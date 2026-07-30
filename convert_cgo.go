//go:build cgo_raft && cgo

// Copyright 2026 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

/*
#cgo CFLAGS: -I${SRCDIR}/c/include -I${SRCDIR}/c/src
#include <stdlib.h>
#include "raft/raft.h"
#include "alloc.h"
*/
import "C"

import (
	"fmt"
	"math"
	"runtime"
	"unsafe"

	"google.golang.org/protobuf/proto"

	"go.etcd.io/raft/v3/quorum"
	pb "go.etcd.io/raft/v3/raftpb"
	"go.etcd.io/raft/v3/tracker"
)

// cInputArena owns only temporary C descriptor storage. Byte payload pointers
// inside those descriptors remain borrowed Go pointers and are kept alive by
// the wrapper until the single semantic C call returns.
type cInputArena struct {
	allocations []unsafe.Pointer
	pinner      runtime.Pinner
}

func (a *cInputArena) alloc(count, size uintptr) (unsafe.Pointer, error) {
	if count == 0 {
		return nil, nil
	}
	if size == 0 || count > ^uintptr(0)/size {
		return nil, errCOutOfMemory
	}
	p := C.raft_calloc(C.size_t(count), C.size_t(size))
	if p == nil {
		return nil, errCOutOfMemory
	}
	a.allocations = append(a.allocations, p)
	return p, nil
}

func (a *cInputArena) free() {
	for i := len(a.allocations) - 1; i >= 0; i-- {
		C.free(a.allocations[i])
	}
	a.allocations = nil
	a.pinner.Unpin()
}

func cBool(v bool) C.bool {
	return C.bool(v)
}

func borrowedBytes(b []byte) *C.uint8_t {
	if len(b) == 0 {
		return nil
	}
	return (*C.uint8_t)(unsafe.Pointer(unsafe.SliceData(b)))
}

func (a *cInputArena) setByteView(dst *C.raft_byte_view_t, src []byte) {
	if len(src) != 0 {
		data := unsafe.SliceData(src)
		a.pinner.Pin(data)
		dst.data = (*C.uint8_t)(unsafe.Pointer(data))
	}
	dst.len = C.size_t(len(src))
	dst.is_nil = cBool(src == nil)
}

func (a *cInputArena) setUint64View(dst *C.raft_uint64_view_t, src []uint64) {
	if len(src) != 0 {
		items := unsafe.SliceData(src)
		a.pinner.Pin(items)
		dst.items = (*C.uint64_t)(unsafe.Pointer(items))
	}
	dst.len = C.size_t(len(src))
}

func (a *cInputArena) fillProtobufMetadata(
	dst *C.raft_protobuf_metadata_view_t,
	fields C.uint32_t,
	src proto.Message,
) {
	dst.fields = fields
	if src == nil {
		a.setByteView(&dst.unknown_fields, nil)
		return
	}
	a.setByteView(&dst.unknown_fields, src.ProtoReflect().GetUnknown())
}

func (a *cInputArena) fillConfStateView(dst *C.raft_conf_state_view_t, src *pb.ConfState) {
	if src == nil {
		a.fillProtobufMetadata(&dst.protobuf, 0, nil)
		return
	}
	a.setUint64View(&dst.voters, src.Voters)
	a.setUint64View(&dst.voters_outgoing, src.VotersOutgoing)
	a.setUint64View(&dst.learners, src.Learners)
	a.setUint64View(&dst.learners_next, src.LearnersNext)
	dst.auto_leave = cBool(src.GetAutoLeave())
	var fields C.uint32_t
	if src.AutoLeave != nil {
		fields |= C.RAFT_CONF_STATE_PROTO_AUTO_LEAVE
	}
	a.fillProtobufMetadata(&dst.protobuf, fields, src)
}

func (a *cInputArena) fillSnapshotView(dst *C.raft_snapshot_view_t, src *pb.Snapshot) {
	if src == nil {
		a.setByteView(&dst.data, nil)
		a.fillProtobufMetadata(&dst.protobuf, 0, nil)
		return
	}
	a.setByteView(&dst.data, src.Data)
	var snapshotFields C.uint32_t
	if src.Metadata != nil {
		snapshotFields |= C.RAFT_SNAPSHOT_PROTO_METADATA
	}
	a.fillProtobufMetadata(&dst.protobuf, snapshotFields, src)
	metadata := src.GetMetadata()
	if metadata == nil {
		a.fillProtobufMetadata(&dst.metadata.protobuf, 0, nil)
		a.fillConfStateView(&dst.metadata.conf_state, nil)
		return
	}
	dst.metadata.index = C.uint64_t(metadata.GetIndex())
	dst.metadata.term = C.uint64_t(metadata.GetTerm())
	var metadataFields C.uint32_t
	if metadata.ConfState != nil {
		metadataFields |= C.RAFT_SNAPSHOT_METADATA_PROTO_CONF_STATE
	}
	if metadata.Index != nil {
		metadataFields |= C.RAFT_SNAPSHOT_METADATA_PROTO_INDEX
	}
	if metadata.Term != nil {
		metadataFields |= C.RAFT_SNAPSHOT_METADATA_PROTO_TERM
	}
	a.fillProtobufMetadata(&dst.metadata.protobuf, metadataFields, metadata)
	a.fillConfStateView(&dst.metadata.conf_state, metadata.GetConfState())
}

func (a *cInputArena) fillEntryView(dst *C.raft_entry_view_t, src *pb.Entry) {
	if src == nil {
		a.setByteView(&dst.data, nil)
		a.fillProtobufMetadata(&dst.protobuf, 0, nil)
		return
	}
	dst._type = C.raft_entry_type_t(src.GetType())
	dst.term = C.uint64_t(src.GetTerm())
	dst.index = C.uint64_t(src.GetIndex())
	a.setByteView(&dst.data, src.Data)
	var fields C.uint32_t
	if src.Type != nil {
		fields |= C.RAFT_ENTRY_PROTO_TYPE
	}
	if src.Term != nil {
		fields |= C.RAFT_ENTRY_PROTO_TERM
	}
	if src.Index != nil {
		fields |= C.RAFT_ENTRY_PROTO_INDEX
	}
	a.fillProtobufMetadata(&dst.protobuf, fields, src)
}

func makeMessageView(a *cInputArena, src *pb.Message) (*C.raft_message_view_t, error) {
	p, err := a.alloc(1, unsafe.Sizeof(C.raft_message_view_t{}))
	if err != nil {
		return nil, err
	}
	dst := (*C.raft_message_view_t)(p)
	if src == nil {
		a.setByteView(&dst.context, nil)
		a.fillProtobufMetadata(&dst.protobuf, 0, nil)
		return dst, nil
	}

	dst._type = C.raft_message_type_t(src.GetType())
	dst.to = C.uint64_t(src.GetTo())
	dst.from = C.uint64_t(src.GetFrom())
	dst.term = C.uint64_t(src.GetTerm())
	dst.log_term = C.uint64_t(src.GetLogTerm())
	dst.index = C.uint64_t(src.GetIndex())
	dst.commit = C.uint64_t(src.GetCommit())
	dst.vote = C.uint64_t(src.GetVote())
	dst.reject = cBool(src.GetReject())
	dst.reject_hint = C.uint64_t(src.GetRejectHint())
	a.setByteView(&dst.context, src.Context)
	var fields C.uint32_t
	if src.Type != nil {
		fields |= C.RAFT_MESSAGE_PROTO_TYPE
	}
	if src.To != nil {
		fields |= C.RAFT_MESSAGE_PROTO_TO
	}
	if src.From != nil {
		fields |= C.RAFT_MESSAGE_PROTO_FROM
	}
	if src.Term != nil {
		fields |= C.RAFT_MESSAGE_PROTO_TERM
	}
	if src.LogTerm != nil {
		fields |= C.RAFT_MESSAGE_PROTO_LOG_TERM
	}
	if src.Index != nil {
		fields |= C.RAFT_MESSAGE_PROTO_INDEX
	}
	if src.Commit != nil {
		fields |= C.RAFT_MESSAGE_PROTO_COMMIT
	}
	if src.Vote != nil {
		fields |= C.RAFT_MESSAGE_PROTO_VOTE
	}
	if src.Reject != nil {
		fields |= C.RAFT_MESSAGE_PROTO_REJECT
	}
	if src.RejectHint != nil {
		fields |= C.RAFT_MESSAGE_PROTO_REJECT_HINT
	}
	a.fillProtobufMetadata(&dst.protobuf, fields, src)

	if len(src.Entries) != 0 {
		items, allocErr := a.alloc(
			uintptr(len(src.Entries)),
			unsafe.Sizeof(C.raft_entry_view_t{}),
		)
		if allocErr != nil {
			return nil, allocErr
		}
		entries := unsafe.Slice((*C.raft_entry_view_t)(items), len(src.Entries))
		for i, entry := range src.Entries {
			a.fillEntryView(&entries[i], entry)
		}
		dst.entries.items = (*C.raft_entry_view_t)(items)
		dst.entries.len = C.size_t(len(entries))
	}

	if src.Snapshot != nil {
		dst.has_snapshot = cBool(true)
		a.fillSnapshotView(&dst.snapshot, src.Snapshot)
	}

	if len(src.Responses) != 0 {
		items, allocErr := a.alloc(
			uintptr(len(src.Responses)),
			unsafe.Sizeof(C.raft_message_view_t{}),
		)
		if allocErr != nil {
			return nil, allocErr
		}
		responses := unsafe.Slice((*C.raft_message_view_t)(items), len(src.Responses))
		for i, response := range src.Responses {
			temporary, fillErr := makeMessageView(a, response)
			if fillErr != nil {
				return nil, fillErr
			}
			responses[i] = *temporary
		}
		dst.responses.items = (*C.raft_message_view_t)(items)
		dst.responses.len = C.size_t(len(responses))
	}
	return dst, nil
}

func makeConfChangeV2View(
	a *cInputArena, cc pb.ConfChangeI,
) (*C.raft_conf_change_v2_view_t, error) {
	p, err := a.alloc(1, unsafe.Sizeof(C.raft_conf_change_v2_view_t{}))
	if err != nil {
		return nil, err
	}
	dst := (*C.raft_conf_change_v2_view_t)(p)
	var src *pb.ConfChangeV2
	if cc == nil {
		src = &pb.ConfChangeV2{}
	} else {
		src = cc.AsV2()
	}
	dst.transition = C.raft_conf_change_transition_t(src.GetTransition())
	dst.has_transition = cBool(src.Transition != nil)
	a.setByteView(&dst.context, src.Context)
	a.setByteView(&dst.unknown_fields, src.ProtoReflect().GetUnknown())
	if len(src.Changes) == 0 {
		return dst, nil
	}
	items, err := a.alloc(
		uintptr(len(src.Changes)),
		unsafe.Sizeof(C.raft_conf_change_single_t{}),
	)
	if err != nil {
		return nil, err
	}
	changes := unsafe.Slice((*C.raft_conf_change_single_t)(items), len(src.Changes))
	for i, change := range src.Changes {
		if change == nil {
			continue
		}
		changes[i]._type = C.raft_conf_change_type_t(change.GetType())
		changes[i].node_id = C.uint64_t(change.GetNodeId())
		changes[i].has_type = cBool(change.Type != nil)
		changes[i].has_node_id = cBool(change.NodeId != nil)
		a.setByteView(
			&changes[i].unknown_fields,
			change.ProtoReflect().GetUnknown(),
		)
	}
	dst.changes = (*C.raft_conf_change_single_t)(items)
	dst.changes_len = C.size_t(len(changes))
	return dst, nil
}

func makeConfChangeV1View(
	a *cInputArena, src *pb.ConfChange,
) (*C.raft_conf_change_view_t, error) {
	p, err := a.alloc(1, unsafe.Sizeof(C.raft_conf_change_view_t{}))
	if err != nil {
		return nil, err
	}
	dst := (*C.raft_conf_change_view_t)(p)
	if src == nil {
		a.setByteView(&dst.context, nil)
		a.setByteView(&dst.unknown_fields, nil)
		return dst, nil
	}
	dst.id = C.uint64_t(src.GetId())
	dst._type = C.raft_conf_change_type_t(src.GetType())
	dst.node_id = C.uint64_t(src.GetNodeId())
	dst.has_id = cBool(src.Id != nil)
	dst.has_type = cBool(src.Type != nil)
	dst.has_node_id = cBool(src.NodeId != nil)
	a.setByteView(&dst.context, src.Context)
	a.setByteView(&dst.unknown_fields, src.ProtoReflect().GetUnknown())
	return dst, nil
}

func checkedCLen(n C.size_t) int {
	if uint64(n) > uint64(math.MaxInt) {
		panic("raft/cgo: C array length exceeds Go int")
	}
	return int(n)
}

func cOwnedBytes(src C.raft_bytes_t) []byte {
	if bool(src.is_nil) {
		return nil
	}
	n := checkedCLen(src.len)
	if n == 0 {
		return []byte{}
	}
	if src.data == nil {
		panic("raft/cgo: invalid owned bytes")
	}
	in := unsafe.Slice((*byte)(unsafe.Pointer(src.data)), n)
	out := make([]byte, n)
	copy(out, in)
	return out
}

func cProtoHas(fields C.uint32_t, field C.uint32_t) bool {
	return fields&field != 0
}

func cSetUnknown(dst proto.Message, src C.raft_bytes_t) {
	unknown := cOwnedBytes(src)
	if len(unknown) != 0 {
		dst.ProtoReflect().SetUnknown(unknown)
	}
}

func cEntry(src *C.raft_entry_t) *pb.Entry {
	if src == nil {
		return nil
	}
	out := &pb.Entry{Data: cOwnedBytes(src.data)}
	typ := pb.EntryType(src._type)
	term := uint64(src.term)
	index := uint64(src.index)
	if cProtoHas(src.protobuf.fields, C.RAFT_ENTRY_PROTO_TYPE) {
		out.Type = &typ
	}
	if cProtoHas(src.protobuf.fields, C.RAFT_ENTRY_PROTO_TERM) {
		out.Term = &term
	}
	if cProtoHas(src.protobuf.fields, C.RAFT_ENTRY_PROTO_INDEX) {
		out.Index = &index
	}
	cSetUnknown(out, src.protobuf.unknown_fields)
	return out
}

func cEntryVec(src C.raft_entry_vec_t) []*pb.Entry {
	n := checkedCLen(src.len)
	if n == 0 {
		return nil
	}
	rows := unsafe.Slice(src.items, n)
	out := make([]*pb.Entry, n)
	for i := range rows {
		out[i] = cEntry(&rows[i])
	}
	return out
}

func cUint64Vec(src C.raft_uint64_vec_t) []uint64 {
	n := checkedCLen(src.len)
	if n == 0 {
		return nil
	}
	rows := unsafe.Slice(src.items, n)
	out := make([]uint64, n)
	for i := range rows {
		out[i] = uint64(rows[i])
	}
	return out
}

func cConfState(src *C.raft_conf_state_t) *pb.ConfState {
	if src == nil {
		return nil
	}
	out := &pb.ConfState{
		Voters:         cUint64Vec(src.voters),
		VotersOutgoing: cUint64Vec(src.voters_outgoing),
		Learners:       cUint64Vec(src.learners),
		LearnersNext:   cUint64Vec(src.learners_next),
	}
	if cProtoHas(
		src.protobuf.fields,
		C.RAFT_CONF_STATE_PROTO_AUTO_LEAVE,
	) {
		autoLeave := bool(src.auto_leave)
		out.AutoLeave = &autoLeave
	}
	cSetUnknown(out, src.protobuf.unknown_fields)
	return out
}

func cSnapshot(src *C.raft_snapshot_t) *pb.Snapshot {
	if src == nil {
		return nil
	}
	out := &pb.Snapshot{Data: cOwnedBytes(src.data)}
	if cProtoHas(
		src.protobuf.fields,
		C.RAFT_SNAPSHOT_PROTO_METADATA,
	) {
		metadata := &pb.SnapshotMetadata{}
		if cProtoHas(
			src.metadata.protobuf.fields,
			C.RAFT_SNAPSHOT_METADATA_PROTO_CONF_STATE,
		) {
			metadata.ConfState = cConfState(&src.metadata.conf_state)
		}
		if cProtoHas(
			src.metadata.protobuf.fields,
			C.RAFT_SNAPSHOT_METADATA_PROTO_INDEX,
		) {
			index := uint64(src.metadata.index)
			metadata.Index = &index
		}
		if cProtoHas(
			src.metadata.protobuf.fields,
			C.RAFT_SNAPSHOT_METADATA_PROTO_TERM,
		) {
			term := uint64(src.metadata.term)
			metadata.Term = &term
		}
		cSetUnknown(metadata, src.metadata.protobuf.unknown_fields)
		out.Metadata = metadata
	}
	cSetUnknown(out, src.protobuf.unknown_fields)
	return out
}

func cMessage(src *C.raft_message_t) *pb.Message {
	if src == nil {
		return nil
	}
	typ := pb.MessageType(src._type)
	to := uint64(src.to)
	from := uint64(src.from)
	term := uint64(src.term)
	logTerm := uint64(src.log_term)
	index := uint64(src.index)
	commit := uint64(src.commit)
	vote := uint64(src.vote)
	reject := bool(src.reject)
	rejectHint := uint64(src.reject_hint)
	out := &pb.Message{
		Entries: cEntryVec(src.entries),
		Context: cOwnedBytes(src.context),
	}
	if cProtoHas(src.protobuf.fields, C.RAFT_MESSAGE_PROTO_TYPE) {
		out.Type = &typ
	}
	if cProtoHas(src.protobuf.fields, C.RAFT_MESSAGE_PROTO_TO) {
		out.To = &to
	}
	if cProtoHas(src.protobuf.fields, C.RAFT_MESSAGE_PROTO_FROM) {
		out.From = &from
	}
	if cProtoHas(src.protobuf.fields, C.RAFT_MESSAGE_PROTO_TERM) {
		out.Term = &term
	}
	if cProtoHas(src.protobuf.fields, C.RAFT_MESSAGE_PROTO_LOG_TERM) {
		out.LogTerm = &logTerm
	}
	if cProtoHas(src.protobuf.fields, C.RAFT_MESSAGE_PROTO_INDEX) {
		out.Index = &index
	}
	if cProtoHas(src.protobuf.fields, C.RAFT_MESSAGE_PROTO_COMMIT) {
		out.Commit = &commit
	}
	if cProtoHas(src.protobuf.fields, C.RAFT_MESSAGE_PROTO_VOTE) {
		out.Vote = &vote
	}
	if cProtoHas(src.protobuf.fields, C.RAFT_MESSAGE_PROTO_REJECT) {
		out.Reject = &reject
	}
	if cProtoHas(src.protobuf.fields, C.RAFT_MESSAGE_PROTO_REJECT_HINT) {
		out.RejectHint = &rejectHint
	}
	if bool(src.has_snapshot) {
		out.Snapshot = cSnapshot(&src.snapshot)
	}
	n := checkedCLen(src.responses.len)
	if n != 0 {
		rows := unsafe.Slice(src.responses.items, n)
		out.Responses = make([]*pb.Message, n)
		for i := range rows {
			out.Responses[i] = cMessage(&rows[i])
		}
	}
	cSetUnknown(out, src.protobuf.unknown_fields)
	return out
}

func cMessageVec(src C.raft_message_vec_t) []*pb.Message {
	n := checkedCLen(src.len)
	if n == 0 {
		return nil
	}
	rows := unsafe.Slice(src.items, n)
	out := make([]*pb.Message, n)
	for i := range rows {
		out[i] = cMessage(&rows[i])
	}
	return out
}

func cHardState(src *C.raft_hard_state_t) *pb.HardState {
	if src == nil {
		return nil
	}
	term := uint64(src.term)
	vote := uint64(src.vote)
	commit := uint64(src.commit)
	return &pb.HardState{Term: &term, Vote: &vote, Commit: &commit}
}

func cSoftState(src *C.raft_soft_state_t) *SoftState {
	if src == nil {
		return nil
	}
	return &SoftState{
		Lead:      uint64(src.lead),
		RaftState: StateType(src.raft_state),
	}
}

func cReady(src *C.raft_ready_t) Ready {
	var out Ready
	if bool(src.has_soft_state) {
		out.SoftState = cSoftState(&src.soft_state)
	}
	if bool(src.has_hard_state) {
		out.HardState = cHardState(&src.hard_state)
	}
	out.Entries = cEntryVec(src.entries)
	out.CommittedEntries = cEntryVec(src.committed_entries)
	out.Messages = cMessageVec(src.messages)
	out.MustSync = bool(src.must_sync)
	if bool(src.has_snapshot) {
		out.Snapshot = cSnapshot(&src.snapshot)
	}
	n := checkedCLen(src.read_states.len)
	if n != 0 {
		rows := unsafe.Slice(src.read_states.items, n)
		out.ReadStates = make([]ReadState, n)
		for i := range rows {
			out.ReadStates[i] = ReadState{
				Index:      uint64(rows[i].index),
				RequestCtx: cOwnedBytes(rows[i].request_ctx),
			}
		}
	}
	return out
}

func cBasicStatus(src *C.raft_basic_status_t) BasicStatus {
	return BasicStatus{
		ID:             uint64(src.id),
		HardState:      cHardState(&src.hard_state),
		SoftState:      *cSoftState(&src.soft_state),
		Applied:        uint64(src.applied),
		LeadTransferee: uint64(src.lead_transferee),
	}
}

func idSet(ids []uint64) map[uint64]struct{} {
	if len(ids) == 0 {
		return nil
	}
	out := make(map[uint64]struct{}, len(ids))
	for _, id := range ids {
		out[id] = struct{}{}
	}
	return out
}

func cTrackerConfig(src *C.raft_conf_state_t) tracker.Config {
	voters := idSet(cUint64Vec(src.voters))
	outgoing := idSet(cUint64Vec(src.voters_outgoing))
	return tracker.Config{
		Voters: quorum.JointConfig{
			quorum.MajorityConfig(voters),
			quorum.MajorityConfig(outgoing),
		},
		AutoLeave:    bool(src.auto_leave),
		Learners:     idSet(cUint64Vec(src.learners)),
		LearnersNext: idSet(cUint64Vec(src.learners_next)),
	}
}

func cProgress(src *C.raft_progress_t) tracker.Progress {
	return tracker.Progress{
		Match:            uint64(src.match_index),
		Next:             uint64(src.next_index),
		State:            tracker.StateType(src.state),
		PendingSnapshot:  uint64(src.pending_snapshot),
		RecentActive:     bool(src.recent_active),
		MsgAppFlowPaused: bool(src.message_flow_paused),
		Inflights:        nil,
		IsLearner:        bool(src.is_learner),
	}
}

func cStatusProgress(src *C.raft_status_progress_t) tracker.Progress {
	progress := cProgress(&src.snapshot.progress)
	inflights := tracker.NewInflights(
		checkedCLen(src.inflights.size),
		uint64(src.inflights.max_bytes),
	)
	items := unsafe.Slice(
		src.inflights.items,
		checkedCLen(src.inflights.len),
	)
	for i := range items {
		inflights.Add(uint64(items[i].index), uint64(items[i].bytes))
	}
	progress.Inflights = inflights
	return progress
}

func cProgressType(src C.raft_progress_type_t) (ProgressType, error) {
	switch int(src) {
	case int(C.RAFT_PROGRESS_PEER):
		return ProgressTypePeer, nil
	case int(C.RAFT_PROGRESS_LEARNER):
		return ProgressTypeLearner, nil
	default:
		return 0, fmt.Errorf("raft/cgo: unknown progress type %d", int(src))
	}
}

func keepAlive(v any) {
	runtime.KeepAlive(v)
}
