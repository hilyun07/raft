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
)

func newDemotedLeaderTestRawNode(t *testing.T, stepDownOnRemoval bool) (*RawNode, *MemoryStorage) {
	t.Helper()

	storage := NewMemoryStorage()
	if err := storage.ApplySnapshot(&pb.Snapshot{
		Metadata: &pb.SnapshotMetadata{
			ConfState: &pb.ConfState{Voters: []uint64{1, 2}},
			Index:     new(uint64(1)),
			Term:      new(uint64(1)),
		},
	}); err != nil {
		t.Fatalf("ApplySnapshot: %v", err)
	}
	if err := storage.SetHardState(&pb.HardState{
		Term:   new(uint64(1)),
		Commit: new(uint64(1)),
	}); err != nil {
		t.Fatalf("SetHardState: %v", err)
	}

	rn, err := NewRawNode(&Config{
		ID:                1,
		ElectionTick:      10,
		HeartbeatTick:     1,
		Storage:           storage,
		Applied:           1,
		MaxSizePerMsg:     1024,
		MaxInflightMsgs:   16,
		StepDownOnRemoval: stepDownOnRemoval,
		Logger:            discardLogger,
	})
	if err != nil {
		t.Fatalf("NewRawNode: %v", err)
	}
	return rn, storage
}

func persistDemotedLeaderReady(t *testing.T, rn *RawNode, storage *MemoryStorage, rd Ready) {
	t.Helper()
	if !IsEmptyHardState(rd.HardState) {
		if err := storage.SetHardState(rd.HardState); err != nil {
			t.Fatalf("SetHardState: %v", err)
		}
	}
	if !IsEmptySnap(rd.Snapshot) {
		if err := storage.ApplySnapshot(rd.Snapshot); err != nil {
			t.Fatalf("ApplySnapshot: %v", err)
		}
	}
	if err := storage.Append(rd.Entries); err != nil {
		t.Fatalf("Append: %v", err)
	}
	rn.Advance(rd)
}

func drainDemotedLeaderReady(t *testing.T, rn *RawNode, storage *MemoryStorage) {
	t.Helper()
	for i := 0; i < 16 && rn.HasReady(); i++ {
		persistDemotedLeaderReady(t, rn, storage, rn.Ready())
	}
	if rn.HasReady() {
		t.Fatal("RawNode still has Ready after bounded drain")
	}
}

func electDemotedLeaderTestRawNode(t *testing.T, rn *RawNode, storage *MemoryStorage) {
	t.Helper()
	if err := rn.Campaign(); err != nil {
		t.Fatalf("Campaign: %v", err)
	}
	drainDemotedLeaderReady(t, rn, storage)

	term := rn.BasicStatus().GetTerm()
	if err := rn.Step(&pb.Message{
		Type: pb.MsgVoteResp.Enum(),
		From: new(uint64(2)),
		To:   new(uint64(1)),
		Term: new(term),
	}); err != nil {
		t.Fatalf("Step(MsgVoteResp): %v", err)
	}
	drainDemotedLeaderReady(t, rn, storage)
	if state := rn.Status().RaftState; state != StateLeader {
		t.Fatalf("state after election = %v, want %v", state, StateLeader)
	}
}

func demoteLeaderForParity(t *testing.T, rn *RawNode) pb.ConfState {
	t.Helper()
	return *rn.ApplyConfChange((&pb.ConfChange{
		Type:   pb.ConfChangeAddLearnerNode.Enum(),
		NodeId: new(uint64(1)),
	}).AsV2())
}

func readyContainsProposal(rd Ready, data []byte) bool {
	for _, ent := range rd.Entries {
		if ent.GetType() == pb.EntryNormal && bytes.Equal(ent.Data, data) {
			return true
		}
	}
	return false
}

func assertSelfLearnerProgress(t *testing.T, rn *RawNode) {
	t.Helper()
	if !rn.HasProgress(1) {
		t.Fatal("demoted leader lost its local Progress")
	}
	var (
		seen bool
		typ  ProgressType
		pr   tracker.Progress
	)
	rn.WithProgress(func(id uint64, progressType ProgressType, progress tracker.Progress) {
		if id == 1 {
			seen = true
			typ = progressType
			pr = progress
		}
	})
	if !seen {
		t.Fatal("WithProgress did not expose local Progress")
	}
	if typ != ProgressTypeLearner || !pr.IsLearner {
		t.Fatalf("local Progress = (%v, %+v), want learner", typ, pr)
	}
}

func TestVoterLeaderProposalAdmissionParity(t *testing.T) {
	rn, storage := newDemotedLeaderTestRawNode(t, false)
	electDemotedLeaderTestRawNode(t, rn, storage)

	before, err := storage.LastIndex()
	if err != nil {
		t.Fatalf("LastIndex before proposal: %v", err)
	}
	data := []byte("voter proposal")
	if err := rn.Propose(data); err != nil {
		t.Fatalf("Propose: %v", err)
	}
	if !rn.HasReady() {
		t.Fatal("accepted proposal did not produce Ready")
	}
	rd := rn.Ready()
	if !readyContainsProposal(rd, data) {
		t.Fatalf("Ready entries %v do not contain proposal %q", rd.Entries, data)
	}
	persistDemotedLeaderReady(t, rn, storage, rd)
	after, err := storage.LastIndex()
	if err != nil {
		t.Fatalf("LastIndex after proposal: %v", err)
	}
	if after != before+1 {
		t.Fatalf("last index = %d, want %d", after, before+1)
	}
}

func TestRemovedLeaderProposalAdmissionParity(t *testing.T) {
	rn, storage := newDemotedLeaderTestRawNode(t, false)
	electDemotedLeaderTestRawNode(t, rn, storage)

	cs := rn.ApplyConfChange((&pb.ConfChange{
		Type:   pb.ConfChangeRemoveNode.Enum(),
		NodeId: new(uint64(1)),
	}).AsV2())
	if !reflect.DeepEqual(cs.Voters, []uint64{2}) || len(cs.Learners) != 0 {
		t.Fatalf("ConfState after removal = %+v", cs)
	}
	if state := rn.Status().RaftState; state != StateLeader {
		t.Fatalf("state after removal = %v, want %v", state, StateLeader)
	}
	if rn.HasProgress(1) {
		t.Fatal("removed leader retained local Progress")
	}

	before, err := storage.LastIndex()
	if err != nil {
		t.Fatalf("LastIndex before proposal: %v", err)
	}
	if err := rn.Propose([]byte("removed proposal")); !errors.Is(err, ErrProposalDropped) {
		t.Fatalf("Propose error = %v, want %v", err, ErrProposalDropped)
	}
	drainDemotedLeaderReady(t, rn, storage)
	after, err := storage.LastIndex()
	if err != nil {
		t.Fatalf("LastIndex after proposal: %v", err)
	}
	if after != before {
		t.Fatalf("removed leader changed last index from %d to %d", before, after)
	}
}

func TestDemotedLeaderProposalAdmissionParity(t *testing.T) {
	rn, storage := newDemotedLeaderTestRawNode(t, false)
	electDemotedLeaderTestRawNode(t, rn, storage)

	cs := demoteLeaderForParity(t, rn)
	if !reflect.DeepEqual(cs.Voters, []uint64{2}) ||
		!reflect.DeepEqual(cs.Learners, []uint64{1}) {
		t.Fatalf("ConfState after demotion = %+v", cs)
	}
	status := rn.Status()
	if status.RaftState != StateLeader {
		t.Fatalf("state after demotion = %v, want %v", status.RaftState, StateLeader)
	}
	if _, ok := status.Config.Voters.IDs()[1]; ok {
		t.Fatalf("demoted leader remains in voter config: %s", status.Config)
	}
	if _, ok := status.Config.Learners[1]; !ok {
		t.Fatalf("demoted leader absent from learner config: %s", status.Config)
	}
	if pr, ok := status.Progress[1]; !ok || !pr.IsLearner {
		t.Fatalf("Status Progress[1] = (%+v, %v), want learner", pr, ok)
	}
	assertSelfLearnerProgress(t, rn)

	before, err := storage.LastIndex()
	if err != nil {
		t.Fatalf("LastIndex before proposal: %v", err)
	}
	data := []byte("learner leader proposal")
	if err := rn.Propose(data); err != nil {
		t.Fatalf("Propose: %v", err)
	}
	if !rn.HasReady() {
		t.Fatal("accepted proposal did not produce Ready")
	}
	rd := rn.Ready()
	if !readyContainsProposal(rd, data) {
		t.Fatalf("Ready entries %v do not contain proposal %q", rd.Entries, data)
	}
	persistDemotedLeaderReady(t, rn, storage, rd)
	after, err := storage.LastIndex()
	if err != nil {
		t.Fatalf("LastIndex after proposal: %v", err)
	}
	if after != before+1 {
		t.Fatalf("last index = %d, want %d", after, before+1)
	}
}

func TestDemotedLeaderStepDownProposalParity(t *testing.T) {
	rn, storage := newDemotedLeaderTestRawNode(t, true)
	electDemotedLeaderTestRawNode(t, rn, storage)

	cs := demoteLeaderForParity(t, rn)
	if !reflect.DeepEqual(cs.Voters, []uint64{2}) ||
		!reflect.DeepEqual(cs.Learners, []uint64{1}) {
		t.Fatalf("ConfState after demotion = %+v", cs)
	}
	if state := rn.Status().RaftState; state != StateFollower {
		t.Fatalf("state after demotion = %v, want %v", state, StateFollower)
	}
	assertSelfLearnerProgress(t, rn)

	before, err := storage.LastIndex()
	if err != nil {
		t.Fatalf("LastIndex before proposal: %v", err)
	}
	if err := rn.Propose([]byte("stepped-down proposal")); !errors.Is(err, ErrProposalDropped) {
		t.Fatalf("Propose error = %v, want %v", err, ErrProposalDropped)
	}
	drainDemotedLeaderReady(t, rn, storage)
	after, err := storage.LastIndex()
	if err != nil {
		t.Fatalf("LastIndex after proposal: %v", err)
	}
	if after != before {
		t.Fatalf("stepped-down node changed last index from %d to %d", before, after)
	}
}
