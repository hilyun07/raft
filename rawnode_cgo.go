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

void raft_go_storage_ops_init(raft_storage_ops_t *ops, uintptr_t handle);
*/
import "C"

import (
	"errors"
	"fmt"
	"math"
	"runtime"
	"runtime/cgo"
	"unsafe"

	pb "go.etcd.io/raft/v3/raftpb"
	"go.etcd.io/raft/v3/tracker"
)

// RawNode is a thread-unsafe Go owner of an opaque C RawNode.
type RawNode struct {
	p *C.raft_raw_node_t

	id                 uint64
	logger             Logger
	asyncStorageWrites bool

	storageHandle cgo.Handle
	pendingReady  *C.raft_ready_t
}

// ProgressType indicates the type of replica a Progress corresponds to.
type ProgressType byte

const (
	ProgressTypePeer ProgressType = iota
	ProgressTypeLearner
)

// NewRawNode instantiates the opt-in C-backed RawNode. The C core implements
// election, replication, progress tracking, quorum calculation, membership
// changes, and asynchronous storage writes. Remaining unsupported features
// stay explicit errors.
func NewRawNode(config *Config) (*RawNode, error) {
	return newRawNode(config, nil)
}

// newRawNode's optional observer is a narrow lifecycle-test seam. It observes
// the opaque integer handle after creation without changing ownership; the
// constructor still deletes that handle on every subsequent failure.
func newRawNode(
	config *Config, observeStorageHandle func(cgo.Handle),
) (*RawNode, error) {
	if config == nil {
		return nil, fmt.Errorf("%w: nil Config", errCInvalidArgument)
	}
	if err := config.validate(); err != nil {
		return nil, err
	}
	if uint64(config.ElectionTick) > math.MaxUint32 ||
		uint64(config.HeartbeatTick) > math.MaxUint32 {
		return nil, fmt.Errorf("%w: tick count exceeds uint32", errCInvalidArgument)
	}

	bridge := &storageBridge{storage: config.Storage}
	handle := cgo.NewHandle(bridge)
	if observeStorageHandle != nil {
		observeStorageHandle(handle)
	}
	var ops C.raft_storage_ops_t
	C.raft_go_storage_ops_init(&ops, C.uintptr_t(handle))
	cc := C.raft_config_t{
		id:                             C.uint64_t(config.ID),
		election_tick:                  C.uint32_t(config.ElectionTick),
		heartbeat_tick:                 C.uint32_t(config.HeartbeatTick),
		applied:                        C.uint64_t(config.Applied),
		async_storage_writes:           cBool(config.AsyncStorageWrites),
		max_size_per_message:           C.uint64_t(config.MaxSizePerMsg),
		max_committed_size_per_ready:   C.uint64_t(config.MaxCommittedSizePerReady),
		max_uncommitted_entries_size:   C.uint64_t(config.MaxUncommittedEntriesSize),
		max_inflight_messages:          C.size_t(config.MaxInflightMsgs),
		max_inflight_bytes:             C.uint64_t(config.MaxInflightBytes),
		check_quorum:                   cBool(config.CheckQuorum),
		pre_vote:                       cBool(config.PreVote),
		read_only_option:               C.raft_read_only_option_t(config.ReadOnlyOption),
		disable_proposal_forwarding:    cBool(config.DisableProposalForwarding),
		disable_conf_change_validation: cBool(config.DisableConfChangeValidation),
		step_down_on_removal:           cBool(config.StepDownOnRemoval),
	}
	var rawNode *C.raft_raw_node_t
	rc := C.raft_raw_node_new(&cc, &ops, &rawNode)
	if err := decodeCError(rc); err != nil {
		handle.Delete()
		return nil, err
	}
	rn := &RawNode{
		p:                  rawNode,
		id:                 config.ID,
		logger:             config.Logger,
		asyncStorageWrites: config.AsyncStorageWrites,
		storageHandle:      handle,
	}
	runtime.SetFinalizer(rn, (*RawNode).finalize)
	return rn, nil
}

func (rn *RawNode) finalize() {
	rn.destroy()
}

// destroy is intentionally package-private because the public Go RawNode API
// has no Close method. The finalizer is the fallback for direct RawNode users;
// a later end-to-end Node integration may call this deterministic hook after
// the actor has stopped.
func (rn *RawNode) destroy() {
	if rn == nil || rn.p == nil {
		return
	}
	rn.discardPendingReady()
	C.raft_raw_node_destroy(rn.p)
	rn.p = nil
	rn.storageHandle.Delete()
	rn.storageHandle = 0
	runtime.SetFinalizer(rn, nil)
}

func (rn *RawNode) discardPendingReady() {
	if rn.pendingReady == nil {
		return
	}
	C.raft_ready_destroy(rn.pendingReady)
	rn.pendingReady = nil
}

func (rn *RawNode) ensureOpen() error {
	if rn == nil || rn.p == nil {
		return ErrStopped
	}
	return nil
}

func (rn *RawNode) panicOnError(operation string, err error) {
	if err == nil {
		return
	}
	if rn != nil && rn.logger != nil {
		rn.logger.Panicf("%s: %v", operation, err)
	}
	panic(fmt.Errorf("%s: %w", operation, err))
}

func (rn *RawNode) ID() uint64 {
	return rn.id
}

func (rn *RawNode) HasProgress(id uint64) bool {
	if rn.ensureOpen() != nil {
		return false
	}
	return bool(C.raft_raw_node_has_progress(rn.p, C.uint64_t(id)))
}

func (rn *RawNode) AsyncStorageWritesEnabled() bool {
	return rn.asyncStorageWrites
}

func (rn *RawNode) Logger() Logger {
	return rn.logger
}

func (rn *RawNode) Tick() {
	rn.panicOnError("Tick", rn.ensureOpen())
	C.raft_raw_node_tick(rn.p)
}

func (rn *RawNode) TickQuiesced() {
	rn.panicOnError("TickQuiesced", rn.ensureOpen())
	C.raft_raw_node_tick_quiesced(rn.p)
}

func (rn *RawNode) Campaign() error {
	if err := rn.ensureOpen(); err != nil {
		return err
	}
	return decodeCError(C.raft_raw_node_campaign(rn.p))
}

func (rn *RawNode) Propose(data []byte) error {
	if err := rn.ensureOpen(); err != nil {
		return err
	}
	rc := C.raft_raw_node_propose_from_parts(
		rn.p,
		borrowedBytes(data),
		C.size_t(len(data)),
		cBool(data == nil),
	)
	keepAlive(data)
	return decodeCError(rc)
}

func (rn *RawNode) ProposeConfChange(cc pb.ConfChangeI) error {
	if err := rn.ensureOpen(); err != nil {
		return err
	}
	if cc == nil {
		return decodeCError(C.raft_raw_node_propose_conf_change(rn.p, nil))
	}
	var arena cInputArena
	defer arena.free()
	if legacy, ok := cc.AsV1(); ok {
		view, err := makeConfChangeV1View(&arena, legacy)
		if err != nil {
			return err
		}
		rc := C.raft_raw_node_propose_conf_change_v1(rn.p, view)
		keepAlive(cc)
		return decodeCError(rc)
	}
	view, err := makeConfChangeV2View(&arena, cc)
	if err != nil {
		return err
	}
	rc := C.raft_raw_node_propose_conf_change(rn.p, view)
	keepAlive(cc)
	return decodeCError(rc)
}

func (rn *RawNode) ApplyConfChange(cc pb.ConfChangeI) *pb.ConfState {
	rn.panicOnError("ApplyConfChange", rn.ensureOpen())
	var arena cInputArena
	defer arena.free()
	view, err := makeConfChangeV2View(&arena, cc)
	rn.panicOnError("ApplyConfChange", err)

	out := (*C.raft_conf_state_t)(C.calloc(1, C.size_t(unsafe.Sizeof(C.raft_conf_state_t{}))))
	if out == nil {
		rn.panicOnError("ApplyConfChange", errCOutOfMemory)
	}
	defer C.free(unsafe.Pointer(out))
	defer C.raft_conf_state_free(out)

	rc := C.raft_raw_node_apply_conf_change(rn.p, view, out)
	keepAlive(cc)
	rn.panicOnError("ApplyConfChange", decodeCError(rc))
	return cConfState(out)
}

func (rn *RawNode) Step(message *pb.Message) error {
	if message == nil {
		return errors.New("cannot step with nil message")
	}
	if IsLocalMsg(message.GetType()) && !IsLocalMsgTarget(message.GetFrom()) {
		return ErrStepLocalMsg
	}
	return rn.step(message, false)
}

func (rn *RawNode) stepForNode(message *pb.Message) error {
	if message == nil {
		return errors.New("cannot step with nil message")
	}
	return rn.step(message, true)
}

func (rn *RawNode) step(message *pb.Message, forNode bool) error {
	if err := rn.ensureOpen(); err != nil {
		return err
	}
	var arena cInputArena
	defer arena.free()
	view, err := makeMessageView(&arena, message)
	if err != nil {
		return err
	}
	var rc C.int
	if forNode {
		rc = C.raft_raw_node_step_for_node(rn.p, view)
	} else {
		rc = C.raft_raw_node_step(rn.p, view)
	}
	keepAlive(message)
	return decodeCError(rc)
}

func (rn *RawNode) Ready() Ready {
	rn.panicOnError("Ready", rn.ensureOpen())
	// A readyWithoutAccept preview is read-only and carries no obligation.
	// Match Go RawNode by allowing a later preview/Ready to replace it.
	rn.discardPendingReady()
	var ready *C.raft_ready_t
	rc := C.raft_raw_node_ready(rn.p, &ready)
	if err := decodeCError(rc); err != nil {
		if ready != nil {
			C.raft_ready_destroy(ready)
		}
		rn.panicOnError("Ready", err)
	}
	defer C.raft_ready_destroy(ready)
	return cReady(ready)
}

func (rn *RawNode) readyWithoutAccept() Ready {
	rn.panicOnError("readyWithoutAccept", rn.ensureOpen())
	// node.run may lose the Ready-channel select and preview again after
	// processing another event. The old C-owned preview was never accepted.
	rn.discardPendingReady()
	var ready *C.raft_ready_t
	rc := C.raft_raw_node_ready_without_accept(rn.p, &ready)
	if err := decodeCError(rc); err != nil {
		if ready != nil {
			C.raft_ready_destroy(ready)
		}
		rn.panicOnError("readyWithoutAccept", err)
	}
	result := cReady(ready)
	rn.pendingReady = ready
	return result
}

func (rn *RawNode) acceptReady(_ Ready) {
	rn.panicOnError("acceptReady", rn.ensureOpen())
	if rn.pendingReady == nil {
		rn.panicOnError("acceptReady", errors.New("no outstanding Ready preview"))
	}
	ready := rn.pendingReady
	rn.pendingReady = nil
	rc := C.raft_raw_node_accept_ready(rn.p, ready)
	C.raft_ready_destroy(ready)
	rn.panicOnError("acceptReady", decodeCError(rc))
}

func (rn *RawNode) HasReady() bool {
	if rn.ensureOpen() != nil {
		return false
	}
	return bool(C.raft_raw_node_has_ready(rn.p))
}

func (rn *RawNode) Advance(_ Ready) {
	rn.panicOnError("Advance", rn.ensureOpen())
	if rn.asyncStorageWrites {
		rn.panicOnError(
			"Advance",
			errors.New("Advance must not be called when using AsyncStorageWrites"),
		)
	}
	rn.panicOnError("Advance", decodeCError(C.raft_raw_node_advance(rn.p)))
}

func (rn *RawNode) BasicStatus() BasicStatus {
	rn.panicOnError("BasicStatus", rn.ensureOpen())
	var status C.raft_basic_status_t
	rn.panicOnError(
		"BasicStatus",
		decodeCError(C.raft_raw_node_basic_status(rn.p, &status)),
	)
	return cBasicStatus(&status)
}

func (rn *RawNode) Status() Status {
	rn.panicOnError("Status", rn.ensureOpen())
	status := (*C.raft_status_t)(C.calloc(1, C.size_t(unsafe.Sizeof(C.raft_status_t{}))))
	if status == nil {
		rn.panicOnError("Status", errCOutOfMemory)
	}
	defer C.free(unsafe.Pointer(status))
	defer C.raft_status_free(status)
	rn.panicOnError(
		"Status",
		decodeCError(C.raft_raw_node_status(rn.p, status)),
	)

	out := Status{
		BasicStatus: cBasicStatus(&status.basic),
		Config:      cTrackerConfig(&status.conf_state),
	}
	if out.RaftState == StateLeader {
		out.Progress = make(map[uint64]tracker.Progress, checkedCLen(status.progress_len))
	}
	n := checkedCLen(status.progress_len)
	if n != 0 {
		rows := unsafe.Slice(status.progress, n)
		if out.Progress == nil {
			out.Progress = make(map[uint64]tracker.Progress, n)
		}
		for i := range rows {
			out.Progress[uint64(rows[i].id)] = cProgress(&rows[i].progress)
		}
	}
	return out
}

func (rn *RawNode) WithProgress(
	visitor func(id uint64, typ ProgressType, pr tracker.Progress),
) {
	rn.panicOnError("WithProgress", rn.ensureOpen())
	var rows *C.raft_progress_snapshot_t
	var count C.size_t
	rc := C.raft_raw_node_progress_snapshot(rn.p, &rows, &count)
	rn.panicOnError("WithProgress", decodeCError(rc))
	defer C.raft_progress_snapshot_array_free(rows, count)

	snapshots := unsafe.Slice(rows, checkedCLen(count))
	for i := range snapshots {
		typ, err := cProgressType(snapshots[i]._type)
		rn.panicOnError("WithProgress", err)
		progress := cProgress(&snapshots[i].progress)
		progress.Inflights = nil
		visitor(uint64(snapshots[i].id), typ, progress)
	}
}

func (rn *RawNode) ReportUnreachable(id uint64) {
	rn.panicOnError("ReportUnreachable", rn.ensureOpen())
	rn.panicOnError(
		"ReportUnreachable",
		decodeCError(C.raft_raw_node_report_unreachable(rn.p, C.uint64_t(id))),
	)
}

func (rn *RawNode) ReportSnapshot(id uint64, status SnapshotStatus) {
	rn.panicOnError("ReportSnapshot", rn.ensureOpen())
	rn.panicOnError(
		"ReportSnapshot",
		decodeCError(C.raft_raw_node_report_snapshot(
			rn.p,
			C.uint64_t(id),
			C.raft_snapshot_status_t(status),
		)),
	)
}

func (rn *RawNode) TransferLeader(transferee uint64) {
	rn.panicOnError("TransferLeader", rn.ensureOpen())
	rn.panicOnError(
		"TransferLeader",
		decodeCError(C.raft_raw_node_transfer_leader(
			rn.p,
			C.uint64_t(transferee),
		)),
	)
}

func (rn *RawNode) ForgetLeader() error {
	if err := rn.ensureOpen(); err != nil {
		return err
	}
	return decodeCError(C.raft_raw_node_forget_leader(rn.p))
}

func (rn *RawNode) ReadIndex(requestContext []byte) {
	rn.panicOnError("ReadIndex", rn.ensureOpen())
	rc := C.raft_raw_node_read_index_from_parts(
		rn.p,
		borrowedBytes(requestContext),
		C.size_t(len(requestContext)),
		cBool(requestContext == nil),
	)
	keepAlive(requestContext)
	rn.panicOnError("ReadIndex", decodeCError(rc))
}

func (rn *RawNode) Bootstrap(peers []Peer) error {
	if err := rn.ensureOpen(); err != nil {
		return err
	}
	if len(peers) == 0 {
		return errors.New("must provide at least one peer to Bootstrap")
	}
	var arena cInputArena
	defer arena.free()
	p, err := arena.alloc(uintptr(len(peers)), unsafe.Sizeof(C.raft_peer_view_t{}))
	if err != nil {
		return err
	}
	rows := unsafe.Slice((*C.raft_peer_view_t)(p), len(peers))
	for i := range peers {
		rows[i].id = C.uint64_t(peers[i].ID)
		arena.setByteView(&rows[i].context, peers[i].Context)
	}
	rc := C.raft_raw_node_bootstrap(rn.p, (*C.raft_peer_view_t)(p), C.size_t(len(rows)))
	keepAlive(peers)
	return decodeCError(rc)
}

// MustSync preserves the public Go helper in the tagged build. Commit changes
// alone do not require a synchronous write.
func MustSync(st, prevst *pb.HardState, entsnum int) bool {
	return entsnum != 0 ||
		st.GetVote() != prevst.GetVote() ||
		st.GetTerm() != prevst.GetTerm()
}
