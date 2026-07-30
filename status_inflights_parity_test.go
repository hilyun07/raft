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
	"strings"
	"testing"

	pb "go.etcd.io/raft/v3/raftpb"
	"go.etcd.io/raft/v3/tracker"
)

func persistStatusInflightsParityReady(
	t *testing.T,
	rn *RawNode,
	storage *MemoryStorage,
	ready Ready,
) {
	t.Helper()
	if !IsEmptySnap(ready.Snapshot) {
		if err := storage.ApplySnapshot(ready.Snapshot); err != nil {
			t.Fatal(err)
		}
	}
	if err := storage.Append(ready.Entries); err != nil {
		t.Fatal(err)
	}
	if !IsEmptyHardState(ready.HardState) {
		if err := storage.SetHardState(ready.HardState); err != nil {
			t.Fatal(err)
		}
	}
	rn.Advance(ready)
}

func drainStatusInflightsParityReady(
	t *testing.T,
	rn *RawNode,
	storage *MemoryStorage,
) {
	t.Helper()
	for iterations := 0; rn.HasReady(); iterations++ {
		if iterations >= 8 {
			t.Fatal("RawNode Ready did not drain")
		}
		persistStatusInflightsParityReady(t, rn, storage, rn.Ready())
	}
}

func prepareStatusInflightsParityLeader(
	t *testing.T,
) (*RawNode, *MemoryStorage) {
	t.Helper()
	storage := NewMemoryStorage()
	if err := storage.ApplySnapshot(&pb.Snapshot{
		Metadata: &pb.SnapshotMetadata{
			Index:     new(uint64(1)),
			Term:      new(uint64(1)),
			ConfState: &pb.ConfState{Voters: []uint64{1, 2}},
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
	rn, err := NewRawNode(&Config{
		ID:               1,
		ElectionTick:     10,
		HeartbeatTick:    1,
		Storage:          storage,
		Applied:          1,
		MaxSizePerMsg:    1,
		MaxInflightMsgs:  2,
		MaxInflightBytes: 4,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := rn.Campaign(); err != nil {
		t.Fatal(err)
	}
	persistStatusInflightsParityReady(t, rn, storage, rn.Ready())
	if err := rn.Step(&pb.Message{
		Type: pb.MsgVoteResp.Enum(),
		To:   new(uint64(1)),
		From: new(uint64(2)),
		Term: new(uint64(2)),
	}); err != nil {
		t.Fatal(err)
	}
	persistStatusInflightsParityReady(t, rn, storage, rn.Ready())
	if err := rn.Step(&pb.Message{
		Type:  pb.MsgAppResp.Enum(),
		To:    new(uint64(1)),
		From:  new(uint64(2)),
		Term:  new(uint64(2)),
		Index: new(uint64(2)),
	}); err != nil {
		t.Fatal(err)
	}
	drainStatusInflightsParityReady(t, rn, storage)
	if status := rn.Status(); status.RaftState != StateLeader {
		t.Fatalf("campaign did not elect leader: %+v", status)
	}
	return rn, storage
}

func TestRawNodeStatusInflightsParity(t *testing.T) {
	rn, _ := prepareStatusInflightsParityLeader(t)
	if err := rn.Propose([]byte("a")); err != nil {
		t.Fatal(err)
	}
	if err := rn.Propose([]byte("b")); err != nil {
		t.Fatal(err)
	}

	before := rn.Status()
	peer, ok := before.Progress[2]
	if !ok {
		t.Fatalf("Status omitted peer progress: %+v", before.Progress)
	}
	if peer.State != tracker.StateReplicate ||
		peer.Match != 2 || peer.Next != 5 ||
		!peer.MsgAppFlowPaused {
		t.Fatalf("unexpected peer progress before ack: %+v", peer)
	}
	if peer.Inflights == nil ||
		peer.Inflights.Count() != 2 ||
		!peer.Inflights.Full() {
		t.Fatalf("unexpected peer inflights before ack: %+v", peer)
	}
	if got := peer.String(); !strings.Contains(got, "inflight=2[full]") {
		t.Fatalf("peer progress string %q omitted full inflight window", got)
	}
	mutableInflights := peer.Inflights.Clone()
	mutableInflights.FreeLE(3)
	if mutableInflights.Count() != 1 || mutableInflights.Full() {
		t.Fatalf("copied inflights lost its acknowledgement boundary")
	}
	self, ok := before.Progress[1]
	if !ok || self.Inflights == nil || self.Inflights.Count() != 0 {
		t.Fatalf("unexpected local progress inflights: %+v", self)
	}

	visited := 0
	rn.WithProgress(func(_ uint64, _ ProgressType, progress tracker.Progress) {
		visited++
		if progress.Inflights != nil {
			t.Fatal("WithProgress unexpectedly exposed inflights")
		}
	})
	if visited != 2 {
		t.Fatalf("WithProgress visited %d rows, want 2", visited)
	}

	if err := rn.Step(&pb.Message{
		Type:  pb.MsgAppResp.Enum(),
		To:    new(uint64(1)),
		From:  new(uint64(2)),
		Term:  new(uint64(2)),
		Index: new(uint64(3)),
	}); err != nil {
		t.Fatal(err)
	}
	afterFirstAck := rn.Status().Progress[2]
	if afterFirstAck.Match != 3 || afterFirstAck.Next != 5 ||
		afterFirstAck.MsgAppFlowPaused {
		t.Fatalf("unexpected peer progress after first ack: %+v", afterFirstAck)
	}
	if afterFirstAck.Inflights == nil ||
		afterFirstAck.Inflights.Count() != 1 ||
		afterFirstAck.Inflights.Full() {
		t.Fatalf("unexpected peer inflights after first ack: %+v", afterFirstAck)
	}
	if peer.Inflights.Count() != 2 || !peer.Inflights.Full() {
		t.Fatalf("earlier Status inflights changed after ack: %+v", peer)
	}

	if err := rn.Step(&pb.Message{
		Type:  pb.MsgAppResp.Enum(),
		To:    new(uint64(1)),
		From:  new(uint64(2)),
		Term:  new(uint64(2)),
		Index: new(uint64(4)),
	}); err != nil {
		t.Fatal(err)
	}
	afterSecondAck := rn.Status().Progress[2]
	if afterSecondAck.Match != 4 ||
		afterSecondAck.Inflights == nil ||
		afterSecondAck.Inflights.Count() != 0 ||
		afterSecondAck.Inflights.Full() {
		t.Fatalf("unexpected peer inflights after second ack: %+v", afterSecondAck)
	}
}
