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

import (
	"bytes"
	"errors"
	"reflect"
	"testing"

	pb "go.etcd.io/raft/v3/raftpb"
	"go.etcd.io/raft/v3/tracker"
	"google.golang.org/protobuf/proto"
)

func cgoSkeletonConfig() *Config {
	return &Config{
		ID:              1,
		ElectionTick:    10,
		HeartbeatTick:   1,
		Storage:         NewMemoryStorage(),
		MaxSizePerMsg:   1024,
		MaxInflightMsgs: 16,
	}
}

func TestCGoRawNodeMinimalCoreLifecycleAndBoundary(t *testing.T) {
	cfg := cgoSkeletonConfig()
	storage := cfg.Storage.(*MemoryStorage)
	rn, err := NewRawNode(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if rn.ID() != cfg.ID {
		t.Fatalf("ID() = %d, want %d", rn.ID(), cfg.ID)
	}
	if rn.Logger() != cfg.Logger {
		t.Fatal("Logger() did not retain the validated Go logger")
	}
	if rn.AsyncStorageWritesEnabled() {
		t.Fatal("unexpected AsyncStorageWrites")
	}
	if rn.HasReady() {
		t.Fatal("new empty RawNode unexpectedly has Ready work")
	}
	if rn.HasProgress(1) {
		t.Fatal("new empty RawNode unexpectedly reports progress")
	}

	basic := rn.BasicStatus()
	if basic.ID != cfg.ID || basic.Lead != None || basic.RaftState != StateFollower {
		t.Fatalf("unexpected BasicStatus: %+v", basic)
	}
	status := rn.Status()
	if status.ID != cfg.ID || len(status.Progress) != 0 {
		t.Fatalf("unexpected Status: %+v", status)
	}

	if err := rn.ProposeConfChange(&pb.ConfChangeV2{}); !errors.Is(err, ErrProposalDropped) {
		t.Fatalf("ProposeConfChange error = %v, want proposal dropped", err)
	}
	if err := rn.Propose(nil); !errors.Is(err, ErrProposalDropped) {
		t.Fatalf("proposal without leader = %v, want proposal dropped", err)
	}

	cgoBootstrapAndApply(
		t, rn, storage, []Peer{{ID: 1, Context: []byte{}}})
	if !rn.HasProgress(1) {
		t.Fatal("bootstrapped RawNode does not report local progress")
	}
	if err := rn.Campaign(); err != nil {
		t.Fatalf("Campaign: %v", err)
	}
	if err := rn.Propose([]byte("value")); err != nil {
		t.Fatalf("Propose: %v", err)
	}
	if !rn.HasReady() {
		t.Fatal("campaign and proposal did not produce Ready")
	}
	if err := rn.Step(&pb.Message{Type: pb.MsgHup.Enum()}); !errors.Is(err, ErrStepLocalMsg) {
		t.Fatalf("local Step error = %v, want ErrStepLocalMsg", err)
	}
	if err := rn.stepForNode(&pb.Message{Type: pb.MsgHup.Enum()}); err != nil {
		t.Fatalf("node-internal local Step error = %v", err)
	}

	rd := rn.Ready()
	if rd.SoftState == nil || rd.SoftState.RaftState != StateLeader {
		t.Fatalf("Ready SoftState = %+v, want leader", rd.SoftState)
	}
	if rd.HardState.GetTerm() != 2 || rd.HardState.GetVote() != 1 {
		t.Fatalf("Ready HardState = %+v, want term=2 vote=1", rd.HardState)
	}
	if len(rd.Entries) != 2 || len(rd.CommittedEntries) != 2 {
		t.Fatalf("Ready entries=%d committed=%d, want no-op/proposal", len(rd.Entries), len(rd.CommittedEntries))
	}
	if err := storage.Append(rd.Entries); err != nil {
		t.Fatal(err)
	}
	if !IsEmptyHardState(rd.HardState) {
		if err := storage.SetHardState(rd.HardState); err != nil {
			t.Fatal(err)
		}
	}
	rn.Advance(rd)

	visited := 0
	rn.WithProgress(func(id uint64, typ ProgressType, pr tracker.Progress) {
		visited++
		if id != 1 || typ != ProgressTypePeer {
			t.Fatalf("progress row = (%d,%v), want local peer", id, typ)
		}
		if pr.Inflights != nil {
			t.Fatal("WithProgress exposed inflights")
		}
	})
	if visited != 1 {
		t.Fatalf("WithProgress visited %d rows, want 1", visited)
	}
	if status := rn.Status(); status.RaftState != StateLeader || len(status.Progress) != 1 {
		t.Fatalf("unexpected leader Status: %+v", status)
	}
	if err := rn.ForgetLeader(); err != nil {
		t.Fatalf("ForgetLeader: %v", err)
	}

	rn.destroy()
	if err := rn.Campaign(); !errors.Is(err, ErrStopped) {
		t.Fatalf("Campaign after destroy = %v, want ErrStopped", err)
	}
}

func TestCGoRawNodeConfigValidation(t *testing.T) {
	cfg := cgoSkeletonConfig()
	cfg.ID = None
	if _, err := NewRawNode(cfg); err == nil {
		t.Fatal("NewRawNode accepted RAFT_NONE")
	}

	cfg = cgoSkeletonConfig()
	cfg.Storage = nil
	if _, err := NewRawNode(cfg); err == nil {
		t.Fatal("NewRawNode accepted nil Storage")
	}
}

func TestCGoRawNodeReadIndexOwnsRequestContext(t *testing.T) {
	rn, storage := prepareReadIndexParityLeader(
		t, ReadOnlySafe, false)
	requestContext := []byte("owned")
	rn.ReadIndex(requestContext)
	requestContext[0] = 'X'

	heartbeatReady := rn.Ready()
	heartbeat := lastReadIndexHeartbeat(
		t, heartbeatReady.Messages, 2)
	heartbeatContext := append([]byte(nil), heartbeat.GetContext()...)
	persistReadIndexReady(t, rn, storage, heartbeatReady)
	if err := rn.Step(&pb.Message{
		Type:    pb.MsgHeartbeatResp.Enum(),
		To:      new(uint64(1)),
		From:    new(uint64(2)),
		Term:    new(uint64(2)),
		Context: heartbeatContext,
	}); err != nil {
		t.Fatal(err)
	}
	readReady := rn.Ready()
	if len(readReady.ReadStates) != 1 ||
		!bytes.Equal(readReady.ReadStates[0].RequestCtx, []byte("owned")) {
		t.Fatalf("owned ReadState = %+v", readReady.ReadStates)
	}
	persistReadIndexReady(t, rn, storage, readReady)
}

func cgoPersistAndAdvance(t *testing.T, rn *RawNode, storage *MemoryStorage, rd Ready) {
	t.Helper()
	if err := storage.Append(rd.Entries); err != nil {
		t.Fatal(err)
	}
	if !IsEmptyHardState(rd.HardState) {
		if err := storage.SetHardState(rd.HardState); err != nil {
			t.Fatal(err)
		}
	}
	rn.Advance(rd)
}

func cgoBootstrapAndApply(
	t *testing.T, rn *RawNode, storage *MemoryStorage, peers []Peer,
) {
	t.Helper()
	if err := rn.Bootstrap(peers); err != nil {
		t.Fatal(err)
	}
	cgoPersistAndAdvance(t, rn, storage, rn.Ready())
}

func cgoSingleLeader(t *testing.T) (*RawNode, *MemoryStorage) {
	t.Helper()
	cfg := cgoSkeletonConfig()
	storage := cfg.Storage.(*MemoryStorage)
	rn, err := NewRawNode(cfg)
	if err != nil {
		t.Fatal(err)
	}
	cgoBootstrapAndApply(t, rn, storage, []Peer{{ID: 1}})
	if err := rn.Campaign(); err != nil {
		t.Fatal(err)
	}
	cgoPersistAndAdvance(t, rn, storage, rn.Ready())
	return rn, storage
}

type cgoSnapshotStorage struct {
	*MemoryStorage
	snapshotErr   error
	snapshotCalls int
}

func (s *cgoSnapshotStorage) Snapshot() (*pb.Snapshot, error) {
	s.snapshotCalls++
	if s.snapshotErr != nil {
		return nil, s.snapshotErr
	}
	return s.MemoryStorage.Snapshot()
}

func cgoCompactedSnapshotStorage(t *testing.T) *cgoSnapshotStorage {
	t.Helper()
	storage := &cgoSnapshotStorage{MemoryStorage: NewMemoryStorage()}
	if err := storage.ApplySnapshot(&pb.Snapshot{
		Data: []byte("snapshot-data"),
		Metadata: &pb.SnapshotMetadata{
			Index:     new(uint64(5)),
			Term:      new(uint64(1)),
			ConfState: &pb.ConfState{Voters: []uint64{1, 2}},
		},
	}); err != nil {
		t.Fatal(err)
	}
	return storage
}

func cgoElectSnapshotLeader(t *testing.T, storage Storage) *RawNode {
	t.Helper()
	cfg := cgoSkeletonConfig()
	cfg.Storage = storage
	rn, err := NewRawNode(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := rn.Campaign(); err != nil {
		rn.destroy()
		t.Fatal(err)
	}
	if err := rn.Step(&pb.Message{
		Type: pb.MsgVoteResp.Enum(),
		To:   new(uint64(1)),
		From: new(uint64(2)),
		Term: new(uint64(1)),
	}); err != nil {
		rn.destroy()
		t.Fatal(err)
	}
	return rn
}

func cgoRejectCompactedAppend(t *testing.T, rn *RawNode) error {
	t.Helper()
	return rn.Step(&pb.Message{
		Type:       pb.MsgAppResp.Enum(),
		To:         new(uint64(1)),
		From:       new(uint64(2)),
		Term:       new(uint64(1)),
		Index:      new(uint64(5)),
		Reject:     new(true),
		RejectHint: new(uint64(0)),
	})
}

func cgoProgressFor(t *testing.T, rn *RawNode, id uint64) tracker.Progress {
	t.Helper()
	var found bool
	var progress tracker.Progress
	rn.WithProgress(func(gotID uint64, _ ProgressType, pr tracker.Progress) {
		if gotID == id {
			found = true
			progress = pr
		}
	})
	if !found {
		t.Fatalf("progress for %d not found", id)
	}
	return progress
}

func TestCGoRawNodeSnapshotSendAndReport(t *testing.T) {
	storage := cgoCompactedSnapshotStorage(t)
	rn := cgoElectSnapshotLeader(t, storage)
	defer rn.destroy()

	if err := cgoRejectCompactedAppend(t, rn); err != nil {
		t.Fatal(err)
	}
	rd := rn.Ready()
	var snapMessage *pb.Message
	for _, message := range rd.Messages {
		if message.GetType() == pb.MsgSnap && message.GetTo() == 2 {
			snapMessage = message
		}
	}
	if snapMessage == nil || snapMessage.GetSnapshot() == nil {
		t.Fatalf("Ready messages contain no snapshot: %+v", rd.Messages)
	}
	if rd.Snapshot != nil {
		t.Fatalf("outbound MsgSnap also appeared as local Ready.Snapshot: %+v", rd.Snapshot)
	}
	if got := snapMessage.GetSnapshot(); got.GetMetadata().GetIndex() != 5 ||
		got.GetMetadata().GetTerm() != 1 ||
		!bytes.Equal(got.Data, []byte("snapshot-data")) ||
		!reflect.DeepEqual(got.GetMetadata().GetConfState().Voters, []uint64{1, 2}) {
		t.Fatalf("outbound snapshot = %+v", got)
	}
	if storage.snapshotCalls != 1 {
		t.Fatalf("Snapshot calls = %d, want 1", storage.snapshotCalls)
	}
	cgoPersistAndAdvance(t, rn, storage.MemoryStorage, rd)

	progress := cgoProgressFor(t, rn, 2)
	if progress.State != tracker.StateSnapshot || progress.PendingSnapshot != 5 {
		t.Fatalf("snapshot progress = %+v", progress)
	}
	rn.ReportSnapshot(2, SnapshotFailure)
	progress = cgoProgressFor(t, rn, 2)
	if progress.State != tracker.StateProbe || progress.PendingSnapshot != 0 ||
		progress.Next != 1 || !progress.MsgAppFlowPaused {
		t.Fatalf("failed snapshot progress = %+v", progress)
	}

	if err := rn.Step(&pb.Message{
		Type: pb.MsgHeartbeatResp.Enum(),
		To:   new(uint64(1)),
		From: new(uint64(2)),
		Term: new(uint64(1)),
	}); err != nil {
		t.Fatal(err)
	}
	progress = cgoProgressFor(t, rn, 2)
	if progress.State != tracker.StateSnapshot || progress.PendingSnapshot != 5 {
		t.Fatalf("retried snapshot progress = %+v", progress)
	}
	rn.ReportSnapshot(2, SnapshotFinish)
	progress = cgoProgressFor(t, rn, 2)
	if progress.State != tracker.StateProbe || progress.PendingSnapshot != 0 ||
		progress.Next != 6 || !progress.MsgAppFlowPaused {
		t.Fatalf("finished snapshot progress = %+v", progress)
	}
}

func TestCGoRawNodeSnapshotTemporarilyUnavailableRetries(t *testing.T) {
	storage := cgoCompactedSnapshotStorage(t)
	storage.snapshotErr = ErrSnapshotTemporarilyUnavailable
	rn := cgoElectSnapshotLeader(t, storage)
	defer rn.destroy()

	if err := cgoRejectCompactedAppend(t, rn); err != nil {
		t.Fatal(err)
	}
	if progress := cgoProgressFor(t, rn, 2); progress.State != tracker.StateProbe ||
		progress.Next != 1 {
		t.Fatalf("temporary snapshot progress = %+v", progress)
	}
	storage.snapshotErr = nil
	if err := rn.Step(&pb.Message{
		Type: pb.MsgHeartbeatResp.Enum(),
		To:   new(uint64(1)),
		From: new(uint64(2)),
		Term: new(uint64(1)),
	}); err != nil {
		t.Fatal(err)
	}
	rd := rn.Ready()
	var found bool
	for _, message := range rd.Messages {
		found = found || message.GetType() == pb.MsgSnap
	}
	if !found || storage.snapshotCalls != 2 {
		t.Fatalf("retry found=%v Snapshot calls=%d", found, storage.snapshotCalls)
	}
}

func TestCGoRawNodeSnapshotStorageErrorPropagates(t *testing.T) {
	storage := cgoCompactedSnapshotStorage(t)
	storage.snapshotErr = ErrUnavailable
	rn := cgoElectSnapshotLeader(t, storage)
	defer rn.destroy()
	if err := cgoRejectCompactedAppend(t, rn); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("snapshot storage error = %v, want ErrUnavailable", err)
	}
}

func TestCGoRawNodeLegacyConfChangeProposal(t *testing.T) {
	rn, _ := cgoSingleLeader(t)
	defer rn.destroy()

	cc := &pb.ConfChange{
		Id:      new(uint64(9)),
		Type:    pb.ConfChangeRemoveNode.Enum(),
		NodeId:  new(uint64(2)),
		Context: []byte("legacy"),
	}
	if err := rn.ProposeConfChange(cc); err != nil {
		t.Fatal(err)
	}
	rd := rn.Ready()
	if len(rd.Entries) != 1 {
		t.Fatalf("entries = %d, want 1", len(rd.Entries))
	}
	entry := rd.Entries[0]
	if entry.GetType() != pb.EntryConfChange {
		t.Fatalf("entry type = %v, want EntryConfChange", entry.GetType())
	}
	wantData, err := proto.Marshal(cc)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(entry.Data, wantData) {
		t.Fatalf("entry data = %x, want %x", entry.Data, wantData)
	}
	var decoded pb.ConfChange
	if err := proto.Unmarshal(entry.Data, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.GetId() != 9 ||
		decoded.GetType() != pb.ConfChangeRemoveNode ||
		decoded.GetNodeId() != 2 ||
		string(decoded.Context) != "legacy" {
		t.Fatalf("decoded legacy change = %+v", decoded)
	}
}

func TestCGoRawNodeConfChangeV2PreservesScalarPresence(t *testing.T) {
	rn, _ := cgoSingleLeader(t)
	defer rn.destroy()

	cc := &pb.ConfChangeV2{
		Transition: pb.ConfChangeTransitionAuto.Enum(),
		Changes: []*pb.ConfChangeSingle{{
			Type:   pb.ConfChangeAddNode.Enum(),
			NodeId: new(uint64(0)),
		}},
		Context: []byte{},
	}
	if err := rn.ProposeConfChange(cc); err != nil {
		t.Fatal(err)
	}
	rd := rn.Ready()
	if len(rd.Entries) != 1 ||
		rd.Entries[0].GetType() != pb.EntryConfChangeV2 {
		t.Fatalf("V2 proposal entries = %+v", rd.Entries)
	}
	wantData, err := proto.Marshal(cc)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(rd.Entries[0].Data, wantData) {
		t.Fatalf("V2 entry data = %x, want %x", rd.Entries[0].Data, wantData)
	}
}

func TestCGoRawNodeAllowsOnlyOnePendingConfChange(t *testing.T) {
	rn, _ := cgoSingleLeader(t)
	defer rn.destroy()

	first := &pb.ConfChange{
		Type:   pb.ConfChangeAddNode.Enum(),
		NodeId: new(uint64(2)),
	}
	second := &pb.ConfChange{
		Type:   pb.ConfChangeAddLearnerNode.Enum(),
		NodeId: new(uint64(3)),
	}
	if err := rn.ProposeConfChange(first); err != nil {
		t.Fatal(err)
	}
	if err := rn.ProposeConfChange(second); err != nil {
		t.Fatal(err)
	}
	rd := rn.Ready()
	if len(rd.Entries) != 2 {
		t.Fatalf("entries = %d, want 2", len(rd.Entries))
	}
	if rd.Entries[0].GetType() != pb.EntryConfChange ||
		rd.Entries[1].GetType() != pb.EntryNormal ||
		rd.Entries[1].Data != nil {
		t.Fatalf("pending conf entries = %+v", rd.Entries)
	}
}

func TestCGoRawNodeApplySimpleMembershipChanges(t *testing.T) {
	cfg := cgoSkeletonConfig()
	rn, err := NewRawNode(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer rn.destroy()
	if err := rn.Bootstrap([]Peer{{ID: 1}}); err != nil {
		t.Fatal(err)
	}

	addLearner := &pb.ConfChange{
		Type:   pb.ConfChangeAddLearnerNode.Enum(),
		NodeId: new(uint64(2)),
	}
	state := rn.ApplyConfChange(addLearner)
	if !reflect.DeepEqual(state.Voters, []uint64{1}) ||
		!reflect.DeepEqual(state.Learners, []uint64{2}) {
		t.Fatalf("add learner ConfState = %+v", state)
	}
	if !rn.HasProgress(2) {
		t.Fatal("learner has no progress")
	}
	var learnerType ProgressType
	rn.WithProgress(func(id uint64, typ ProgressType, _ tracker.Progress) {
		if id == 2 {
			learnerType = typ
		}
	})
	if learnerType != ProgressTypeLearner {
		t.Fatalf("node 2 progress type = %v, want learner", learnerType)
	}

	zero := &pb.ConfChange{
		Type:   pb.ConfChangeRemoveNode.Enum(),
		NodeId: new(uint64(0)),
	}
	state = rn.ApplyConfChange(zero)
	if !reflect.DeepEqual(state.Voters, []uint64{1}) ||
		!reflect.DeepEqual(state.Learners, []uint64{2}) {
		t.Fatalf("zero-ID no-op ConfState = %+v", state)
	}

	promote := &pb.ConfChange{
		Type:   pb.ConfChangeAddNode.Enum(),
		NodeId: new(uint64(2)),
	}
	state = rn.ApplyConfChange(promote)
	if !reflect.DeepEqual(state.Voters, []uint64{1, 2}) ||
		len(state.Learners) != 0 {
		t.Fatalf("promote ConfState = %+v", state)
	}

	remove := &pb.ConfChange{
		Type:   pb.ConfChangeRemoveNode.Enum(),
		NodeId: new(uint64(2)),
	}
	state = rn.ApplyConfChange(remove)
	if !reflect.DeepEqual(state.Voters, []uint64{1}) ||
		rn.HasProgress(2) {
		t.Fatalf("remove ConfState = %+v, hasProgress=%v", state, rn.HasProgress(2))
	}

	enterJoint := &pb.ConfChangeV2{
		Transition: pb.ConfChangeTransitionJointExplicit.Enum(),
		Changes: []*pb.ConfChangeSingle{
			{Type: pb.ConfChangeRemoveNode.Enum(), NodeId: new(uint64(1))},
			{Type: pb.ConfChangeAddNode.Enum(), NodeId: new(uint64(2))},
		},
	}
	state = rn.ApplyConfChange(enterJoint)
	if state.GetAutoLeave() ||
		!reflect.DeepEqual(state.Voters, []uint64{2}) ||
		!reflect.DeepEqual(state.VotersOutgoing, []uint64{1}) {
		t.Fatalf("enter-joint ConfState = %+v", state)
	}
	state = rn.ApplyConfChange(&pb.ConfChangeV2{})
	if len(state.VotersOutgoing) != 0 ||
		!reflect.DeepEqual(state.Voters, []uint64{2}) ||
		rn.HasProgress(1) {
		t.Fatalf("leave-joint ConfState = %+v", state)
	}
}

func TestCGoRawNodeJointAutoLeaveProposal(t *testing.T) {
	rn, storage := cgoSingleLeader(t)
	defer rn.destroy()

	joint := &pb.ConfChangeV2{
		Changes: []*pb.ConfChangeSingle{
			{Type: pb.ConfChangeAddNode.Enum(), NodeId: new(uint64(2))},
			{Type: pb.ConfChangeAddLearnerNode.Enum(), NodeId: new(uint64(3))},
		},
	}
	if err := rn.ProposeConfChange(joint); err != nil {
		t.Fatal(err)
	}
	rd := rn.Ready()
	if len(rd.CommittedEntries) != 1 ||
		rd.CommittedEntries[0].GetType() != pb.EntryConfChangeV2 {
		t.Fatalf("joint committed entries = %+v", rd.CommittedEntries)
	}
	wantData, err := proto.Marshal(joint)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(rd.CommittedEntries[0].Data, wantData) {
		t.Fatalf("joint entry data = %x, want %x", rd.CommittedEntries[0].Data, wantData)
	}
	state := rn.ApplyConfChange(joint)
	if !state.GetAutoLeave() ||
		!reflect.DeepEqual(state.Voters, []uint64{1, 2}) ||
		!reflect.DeepEqual(state.VotersOutgoing, []uint64{1}) ||
		!reflect.DeepEqual(state.Learners, []uint64{3}) {
		t.Fatalf("joint ConfState = %+v", state)
	}

	cgoPersistAndAdvance(t, rn, storage, rd)
	if !rn.HasReady() {
		t.Fatal("Advance did not propose automatic joint exit")
	}
	leaveReady := rn.Ready()
	var leave *pb.Entry
	for _, entry := range leaveReady.Entries {
		if entry.GetType() == pb.EntryConfChangeV2 {
			leave = entry
		}
	}
	if leave == nil {
		t.Fatalf("automatic leave entries = %+v", leaveReady.Entries)
	}
	if leave.Data != nil {
		t.Fatalf("automatic leave data = %v, want nil", leave.Data)
	}
}

func TestCGoRawNodeLeaderRemoval(t *testing.T) {
	for _, stepDown := range []bool{false, true} {
		t.Run(map[bool]string{false: "remain-leader", true: "step-down"}[stepDown], func(t *testing.T) {
			cfg := cgoSkeletonConfig()
			cfg.StepDownOnRemoval = stepDown
			storage := cfg.Storage.(*MemoryStorage)
			rn, err := NewRawNode(cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer rn.destroy()
			cgoBootstrapAndApply(t, rn, storage, []Peer{{ID: 1}})
			if err := rn.Campaign(); err != nil {
				t.Fatal(err)
			}
			cgoPersistAndAdvance(t, rn, storage, rn.Ready())

			rn.ApplyConfChange(&pb.ConfChange{
				Type:   pb.ConfChangeAddNode.Enum(),
				NodeId: new(uint64(2)),
			})
			state := rn.ApplyConfChange(&pb.ConfChange{
				Type:   pb.ConfChangeRemoveNode.Enum(),
				NodeId: new(uint64(1)),
			})
			if !reflect.DeepEqual(state.Voters, []uint64{2}) ||
				rn.HasProgress(1) {
				t.Fatalf("leader removal ConfState = %+v", state)
			}
			wantState := StateLeader
			if stepDown {
				wantState = StateFollower
			}
			if got := rn.BasicStatus().RaftState; got != wantState {
				t.Fatalf("state after removal = %v, want %v", got, wantState)
			}
			if err := rn.Propose([]byte("removed")); !errors.Is(err, ErrProposalDropped) {
				t.Fatalf("removed leader proposal = %v, want dropped", err)
			}
		})
	}
}

func TestCGoRawNodeRestoresJointConfState(t *testing.T) {
	storage := NewMemoryStorage()
	if err := storage.ApplySnapshot(&pb.Snapshot{
		Metadata: &pb.SnapshotMetadata{
			Index: new(uint64(5)),
			Term:  new(uint64(2)),
			ConfState: &pb.ConfState{
				Voters:         []uint64{1, 2},
				VotersOutgoing: []uint64{1, 3},
				Learners:       []uint64{4},
				LearnersNext:   []uint64{3},
				AutoLeave:      new(true),
			},
		},
	}); err != nil {
		t.Fatal(err)
	}
	if _, state, err := storage.InitialState(); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(state.Voters, []uint64{1, 2}) {
		t.Fatalf("storage ConfState = %+v", state)
	}
	cfg := cgoSkeletonConfig()
	cfg.Storage = storage
	cfg.Applied = 5
	rn, err := NewRawNode(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer rn.destroy()

	status := rn.Status()
	_, in1 := status.Config.Voters[0][1]
	_, in2 := status.Config.Voters[0][2]
	_, out1 := status.Config.Voters[1][1]
	_, out3 := status.Config.Voters[1][3]
	if len(status.Config.Voters[0]) != 2 || !in1 || !in2 ||
		len(status.Config.Voters[1]) != 2 || !out1 || !out3 ||
		!status.Config.AutoLeave {
		t.Fatalf("restored config = %+v", status.Config)
	}
	for _, id := range []uint64{1, 2, 3, 4} {
		if !rn.HasProgress(id) {
			t.Fatalf("missing restored progress for %d", id)
		}
	}
	var types = map[uint64]ProgressType{}
	rn.WithProgress(func(id uint64, typ ProgressType, _ tracker.Progress) {
		types[id] = typ
	})
	if types[1] != ProgressTypePeer ||
		types[2] != ProgressTypePeer ||
		types[3] != ProgressTypePeer ||
		types[4] != ProgressTypeLearner {
		t.Fatalf("restored progress types = %+v", types)
	}
}
