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
	"reflect"
	"testing"

	"google.golang.org/protobuf/proto"

	pb "go.etcd.io/raft/v3/raftpb"
)

func statusConfigParitySet(ids ...uint64) map[uint64]struct{} {
	if ids == nil {
		return nil
	}
	out := make(map[uint64]struct{}, len(ids))
	for _, id := range ids {
		out[id] = struct{}{}
	}
	return out
}

func requireStatusConfigParity(
	t *testing.T,
	got Status,
	incoming []uint64,
	outgoing []uint64,
	learners []uint64,
	learnersNext []uint64,
) {
	t.Helper()
	wantIncoming := statusConfigParitySet(incoming...)
	if incoming == nil {
		// Config.Clone always preserves the non-nil incoming voter map made by
		// MakeProgressTracker, including for the initial empty configuration.
		wantIncoming = map[uint64]struct{}{}
	}
	wantOutgoing := statusConfigParitySet(outgoing...)
	wantLearners := statusConfigParitySet(learners...)
	wantLearnersNext := statusConfigParitySet(learnersNext...)
	if !reflect.DeepEqual(
		map[uint64]struct{}(got.Config.Voters[0]),
		wantIncoming,
	) || !reflect.DeepEqual(
		map[uint64]struct{}(got.Config.Voters[1]),
		wantOutgoing,
	) || !reflect.DeepEqual(got.Config.Learners, wantLearners) ||
		!reflect.DeepEqual(got.Config.LearnersNext, wantLearnersNext) ||
		got.Config.AutoLeave {
		t.Fatalf(
			"Status.Config = %+v, want incoming=%v outgoing=%v learners=%v learnersNext=%v AutoLeave=false",
			got.Config,
			wantIncoming,
			wantOutgoing,
			wantLearners,
			wantLearnersNext,
		)
	}
}

func newStatusConfigParityRawNode(
	t *testing.T,
	confState *pb.ConfState,
) (*RawNode, *MemoryStorage) {
	t.Helper()
	storage := NewMemoryStorage()
	applied := uint64(0)
	if confState != nil {
		if err := storage.ApplySnapshot(&pb.Snapshot{
			Metadata: &pb.SnapshotMetadata{
				Index:     new(uint64(1)),
				Term:      new(uint64(1)),
				ConfState: proto.Clone(confState).(*pb.ConfState),
			},
		}); err != nil {
			t.Fatal(err)
		}
		if err := storage.SetHardState(&pb.HardState{
			Term:   new(uint64(1)),
			Commit: new(uint64(1)),
		}); err != nil {
			t.Fatal(err)
		}
		applied = 1
	}
	rn, err := NewRawNode(&Config{
		ID:              1,
		ElectionTick:    10,
		HeartbeatTick:   1,
		Storage:         storage,
		Applied:         applied,
		MaxSizePerMsg:   1024,
		MaxInflightMsgs: 16,
	})
	if err != nil {
		t.Fatal(err)
	}
	return rn, storage
}

func persistStatusConfigParityReady(
	t *testing.T,
	rn *RawNode,
	storage *MemoryStorage,
	ready Ready,
) {
	t.Helper()
	if !IsEmptyHardState(ready.HardState) {
		if err := storage.SetHardState(ready.HardState); err != nil {
			t.Fatal(err)
		}
	}
	if err := storage.Append(ready.Entries); err != nil {
		t.Fatal(err)
	}
	rn.Advance(ready)
}

func campaignStatusConfigParityLeader(
	t *testing.T,
) (*RawNode, *MemoryStorage) {
	t.Helper()
	rn, storage := newStatusConfigParityRawNode(
		t, &pb.ConfState{Voters: []uint64{1}})
	if err := rn.Campaign(); err != nil {
		t.Fatal(err)
	}
	for iterations := 0; rn.HasReady(); iterations++ {
		if iterations >= 8 {
			t.Fatal("campaign Ready did not drain")
		}
		ready := rn.Ready()
		persistStatusConfigParityReady(t, rn, storage, ready)
	}
	if status := rn.BasicStatus(); status.RaftState != StateLeader {
		t.Fatalf("campaign status = %+v, want leader", status)
	}
	return rn, storage
}

func campaignStatusConfigParityUntilLeader(
	t *testing.T,
	rn *RawNode,
	storage *MemoryStorage,
) {
	t.Helper()
	if err := rn.Campaign(); err != nil {
		t.Fatal(err)
	}
	for iterations := 0; rn.BasicStatus().RaftState != StateLeader; iterations++ {
		if iterations >= 8 || !rn.HasReady() {
			t.Fatalf("campaign did not elect leader: %+v", rn.BasicStatus())
		}
		ready := rn.Ready()
		persistStatusConfigParityReady(t, rn, storage, ready)
	}
}

func TestStatusConfigInitialAndNormalParity(t *testing.T) {
	empty, _ := newStatusConfigParityRawNode(t, nil)
	requireStatusConfigParity(t, empty.Status(), nil, nil, nil, nil)

	normal, _ := newStatusConfigParityRawNode(
		t, &pb.ConfState{
			Voters:   []uint64{1, 2},
			Learners: []uint64{3},
		},
	)
	requireStatusConfigParity(
		t, normal.Status(), []uint64{1, 2}, nil, []uint64{3}, nil)
}

func TestStatusConfigJointAutoLeaveProjectionParity(t *testing.T) {
	tests := []struct {
		name      string
		confState *pb.ConfState
	}{
		{
			name: "auto leave",
			confState: &pb.ConfState{
				Voters:         []uint64{1, 2},
				VotersOutgoing: []uint64{1, 3},
				Learners:       []uint64{4},
				LearnersNext:   []uint64{3},
				AutoLeave:      new(true),
			},
		},
		{
			name: "manual leave",
			confState: &pb.ConfState{
				Voters:         []uint64{1, 2},
				VotersOutgoing: []uint64{1, 3},
				Learners:       []uint64{4},
				LearnersNext:   []uint64{3},
				AutoLeave:      new(false),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rn, storage := newStatusConfigParityRawNode(t, tt.confState)
			requireStatusConfigParity(
				t,
				rn.Status(),
				[]uint64{1, 2},
				[]uint64{1, 3},
				[]uint64{4},
				[]uint64{3},
			)
			_, stored, err := storage.InitialState()
			if err != nil {
				t.Fatal(err)
			}
			if stored.GetAutoLeave() != tt.confState.GetAutoLeave() {
				t.Fatalf(
					"stored ConfState AutoLeave = %v, want %v",
					stored.GetAutoLeave(),
					tt.confState.GetAutoLeave(),
				)
			}
		})
	}
}

func TestStatusConfigExplicitJointChangeParity(t *testing.T) {
	rn, _ := newStatusConfigParityRawNode(
		t, &pb.ConfState{Voters: []uint64{1}})
	state := rn.ApplyConfChange(&pb.ConfChangeV2{
		Transition: pb.ConfChangeTransitionJointExplicit.Enum(),
		Changes: []*pb.ConfChangeSingle{{
			Type:   pb.ConfChangeAddLearnerNode.Enum(),
			NodeId: new(uint64(2)),
		}},
	})
	if state.GetAutoLeave() {
		t.Fatalf("explicit joint ConfState = %+v, want manual leave", state)
	}
	requireStatusConfigParity(
		t, rn.Status(), []uint64{1}, []uint64{1}, []uint64{2}, nil)
}

func TestStatusConfigAutomaticLeaveParity(t *testing.T) {
	rn, storage := campaignStatusConfigParityLeader(t)
	enter := &pb.ConfChangeV2{
		Transition: pb.ConfChangeTransitionJointImplicit.Enum(),
		Changes: []*pb.ConfChangeSingle{{
			Type:   pb.ConfChangeAddLearnerNode.Enum(),
			NodeId: new(uint64(2)),
		}},
	}
	if err := rn.ProposeConfChange(enter); err != nil {
		t.Fatal(err)
	}
	ready := rn.Ready()
	persistStatusConfigParityReady(t, rn, storage, ready)

	ready = rn.Ready()
	var appliedEnter bool
	for _, entry := range ready.CommittedEntries {
		if entry.GetType() != pb.EntryConfChangeV2 {
			continue
		}
		var change pb.ConfChangeV2
		if err := proto.Unmarshal(entry.GetData(), &change); err != nil {
			t.Fatal(err)
		}
		state := rn.ApplyConfChange(&change)
		if !state.GetAutoLeave() {
			t.Fatalf("entered ConfState = %+v, want AutoLeave", state)
		}
		appliedEnter = true
	}
	if !appliedEnter {
		t.Fatalf("no committed enter-joint entry in Ready %+v", ready)
	}
	jointStatus := rn.Status()
	requireStatusConfigParity(
		t, jointStatus, []uint64{1}, []uint64{1}, []uint64{2}, nil)
	persistStatusConfigParityReady(t, rn, storage, ready)

	if !rn.HasReady() {
		t.Fatal("automatic leave proposal was not generated")
	}
	leaveReady := rn.Ready()
	var proposedLeave bool
	for _, entry := range leaveReady.Entries {
		if entry.GetType() == pb.EntryConfChangeV2 &&
			len(entry.GetData()) == 0 {
			proposedLeave = true
		}
	}
	if !proposedLeave {
		t.Fatalf("automatic leave Ready = %+v", leaveReady)
	}
	persistStatusConfigParityReady(t, rn, storage, leaveReady)

	leaveReady = rn.Ready()
	var appliedLeave bool
	for _, entry := range leaveReady.CommittedEntries {
		if entry.GetType() != pb.EntryConfChangeV2 ||
			len(entry.GetData()) != 0 {
			continue
		}
		state := rn.ApplyConfChange(&pb.ConfChangeV2{})
		if state.GetAutoLeave() ||
			len(state.GetVotersOutgoing()) != 0 {
			t.Fatalf("left ConfState = %+v", state)
		}
		appliedLeave = true
	}
	if !appliedLeave {
		t.Fatalf("no committed leave-joint entry in Ready %+v", leaveReady)
	}
	requireStatusConfigParity(
		t, rn.Status(), []uint64{1}, nil, []uint64{2}, nil)

	// The previously returned Status owns an independent configuration copy.
	requireStatusConfigParity(
		t, jointStatus, []uint64{1}, []uint64{1}, []uint64{2}, nil)
}

func TestStatusConfigSnapshotRestartAndNodeParity(t *testing.T) {
	confState := &pb.ConfState{
		Voters:         []uint64{1, 2},
		VotersOutgoing: []uint64{1, 3},
		Learners:       []uint64{4},
		LearnersNext:   []uint64{3},
		AutoLeave:      new(true),
	}
	first, storage := newStatusConfigParityRawNode(t, confState)
	requireStatusConfigParity(
		t,
		first.Status(),
		[]uint64{1, 2},
		[]uint64{1, 3},
		[]uint64{4},
		[]uint64{3},
	)

	cfg := &Config{
		ID:              1,
		ElectionTick:    10,
		HeartbeatTick:   1,
		Storage:         storage,
		Applied:         1,
		MaxSizePerMsg:   1024,
		MaxInflightMsgs: 16,
	}
	restarted, err := NewRawNode(cfg)
	if err != nil {
		t.Fatal(err)
	}
	requireStatusConfigParity(
		t,
		restarted.Status(),
		[]uint64{1, 2},
		[]uint64{1, 3},
		[]uint64{4},
		[]uint64{3},
	)

	node := RestartNode(cfg)
	defer node.Stop()
	requireStatusConfigParity(
		t,
		node.Status(),
		[]uint64{1, 2},
		[]uint64{1, 3},
		[]uint64{4},
		[]uint64{3},
	)
}

func TestStatusConfigLiveSnapshotRestoreParity(t *testing.T) {
	rn, _ := newStatusConfigParityRawNode(
		t, &pb.ConfState{Voters: []uint64{1, 2}})
	snapshot := &pb.Snapshot{
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
	}
	if err := rn.Step(&pb.Message{
		Type:     pb.MsgSnap.Enum(),
		To:       new(uint64(1)),
		From:     new(uint64(2)),
		Term:     new(uint64(3)),
		Snapshot: snapshot,
	}); err != nil {
		t.Fatal(err)
	}
	requireStatusConfigParity(
		t,
		rn.Status(),
		[]uint64{1, 2},
		[]uint64{1, 3},
		[]uint64{4},
		[]uint64{3},
	)
	ready := rn.Ready()
	if ready.Snapshot == nil ||
		!ready.Snapshot.GetMetadata().GetConfState().GetAutoLeave() {
		t.Fatalf("Ready snapshot ConfState = %+v", ready.Snapshot)
	}
}

func TestStatusConfigTermLeadershipAndLearnerParity(t *testing.T) {
	rn, storage := campaignStatusConfigParityLeader(t)
	before := rn.Status()
	requireStatusConfigParity(t, before, []uint64{1}, nil, nil, nil)

	term := rn.BasicStatus().GetTerm()
	if err := rn.Step(&pb.Message{
		Type: pb.MsgHeartbeatResp.Enum(),
		From: new(uint64(1)),
		To:   new(uint64(1)),
		Term: new(term + 1),
	}); err != nil {
		t.Fatal(err)
	}
	if status := rn.BasicStatus(); status.RaftState != StateFollower ||
		status.GetTerm() != term+1 {
		t.Fatalf("step-down status = %+v", status)
	}
	requireStatusConfigParity(t, rn.Status(), []uint64{1}, nil, nil, nil)

	if err := rn.Campaign(); err != nil {
		t.Fatal(err)
	}
	for iterations := 0; rn.HasReady(); iterations++ {
		if iterations >= 8 {
			t.Fatal("re-election Ready did not drain")
		}
		ready := rn.Ready()
		persistStatusConfigParityReady(t, rn, storage, ready)
	}
	if status := rn.BasicStatus(); status.RaftState != StateLeader {
		t.Fatalf("re-election status = %+v", status)
	}

	addLearner := &pb.ConfChange{
		Type:   pb.ConfChangeAddLearnerNode.Enum(),
		NodeId: new(uint64(2)),
	}
	state := rn.ApplyConfChange(addLearner)
	if !reflect.DeepEqual(state.GetLearners(), []uint64{2}) {
		t.Fatalf("learner ConfState = %+v", state)
	}
	learnerStatus := rn.Status()
	requireStatusConfigParity(
		t, learnerStatus, []uint64{1}, nil, []uint64{2}, nil)

	state = rn.ApplyConfChange(&pb.ConfChange{
		Type:   pb.ConfChangeAddNode.Enum(),
		NodeId: new(uint64(2)),
	})
	if !reflect.DeepEqual(state.GetVoters(), []uint64{1, 2}) {
		t.Fatalf("promoted ConfState = %+v", state)
	}
	requireStatusConfigParity(
		t, rn.Status(), []uint64{1, 2}, nil, nil, nil)

	state = rn.ApplyConfChange(&pb.ConfChange{
		Type:   pb.ConfChangeRemoveNode.Enum(),
		NodeId: new(uint64(2)),
	})
	if !reflect.DeepEqual(state.GetVoters(), []uint64{1}) ||
		rn.HasProgress(2) {
		t.Fatalf("removed ConfState = %+v", state)
	}
	requireStatusConfigParity(t, rn.Status(), []uint64{1}, nil, nil, nil)

	// Earlier snapshots remain unchanged across term, leadership, promotion,
	// and removal.
	requireStatusConfigParity(t, before, []uint64{1}, nil, nil, nil)
	requireStatusConfigParity(
		t, learnerStatus, []uint64{1}, nil, []uint64{2}, nil)
}

func TestStatusConfigAutoLeaveSurvivesLeadershipParity(t *testing.T) {
	rn, storage := newStatusConfigParityRawNode(
		t, &pb.ConfState{Voters: []uint64{1}})
	state := rn.ApplyConfChange(&pb.ConfChangeV2{
		Transition: pb.ConfChangeTransitionJointImplicit.Enum(),
		Changes: []*pb.ConfChangeSingle{{
			Type:   pb.ConfChangeAddLearnerNode.Enum(),
			NodeId: new(uint64(2)),
		}},
	})
	if !state.GetAutoLeave() {
		t.Fatalf("implicit joint ConfState = %+v, want AutoLeave", state)
	}
	requireStatusConfigParity(
		t, rn.Status(), []uint64{1}, []uint64{1}, []uint64{2}, nil)

	campaignStatusConfigParityUntilLeader(t, rn, storage)
	requireStatusConfigParity(
		t, rn.Status(), []uint64{1}, []uint64{1}, []uint64{2}, nil)

	term := rn.BasicStatus().GetTerm()
	if err := rn.Step(&pb.Message{
		Type: pb.MsgHeartbeatResp.Enum(),
		From: new(uint64(1)),
		To:   new(uint64(1)),
		Term: new(term + 1),
	}); err != nil {
		t.Fatal(err)
	}
	if status := rn.BasicStatus(); status.RaftState != StateFollower ||
		status.GetTerm() != term+1 {
		t.Fatalf("step-down status = %+v", status)
	}
	requireStatusConfigParity(
		t, rn.Status(), []uint64{1}, []uint64{1}, []uint64{2}, nil)

	campaignStatusConfigParityUntilLeader(t, rn, storage)
	requireStatusConfigParity(
		t, rn.Status(), []uint64{1}, []uint64{1}, []uint64{2}, nil)
}
