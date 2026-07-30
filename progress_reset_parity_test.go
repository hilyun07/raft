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
	"testing"

	pb "go.etcd.io/raft/v3/raftpb"
	"go.etcd.io/raft/v3/tracker"
)

func newProgressResetParityRawNode(
	t *testing.T,
) (*RawNode, *MemoryStorage) {
	t.Helper()
	storage := NewMemoryStorage()
	if err := storage.ApplySnapshot(&pb.Snapshot{
		Metadata: &pb.SnapshotMetadata{
			Index:     new(uint64(3)),
			Term:      new(uint64(1)),
			ConfState: &pb.ConfState{Voters: []uint64{1, 2}},
		},
	}); err != nil {
		t.Fatal(err)
	}
	if err := storage.SetHardState(&pb.HardState{
		Term:   new(uint64(1)),
		Commit: new(uint64(3)),
	}); err != nil {
		t.Fatal(err)
	}
	rn, err := NewRawNode(&Config{
		ID:              1,
		ElectionTick:    10,
		HeartbeatTick:   1,
		Storage:         storage,
		Applied:         3,
		MaxSizePerMsg:   1024,
		MaxInflightMsgs: 16,
	})
	if err != nil {
		t.Fatal(err)
	}
	return rn, storage
}

func progressResetParitySnapshot(
	rn *RawNode,
) map[uint64]tracker.Progress {
	progress := make(map[uint64]tracker.Progress)
	rn.WithProgress(func(id uint64, _ ProgressType, pr tracker.Progress) {
		progress[id] = pr
	})
	return progress
}

func assertProgressResetParity(
	t *testing.T,
	progress tracker.Progress,
	match uint64,
	next uint64,
) {
	t.Helper()
	if progress.Match != match ||
		progress.Next != next ||
		progress.State != tracker.StateProbe ||
		progress.PendingSnapshot != 0 ||
		progress.RecentActive ||
		progress.MsgAppFlowPaused {
		t.Fatalf("reset Progress = %+v, want match=%d next=%d probe and inactive",
			progress, match, next)
	}
	if progress.Inflights != nil {
		t.Fatalf("WithProgress returned Inflights: %+v", progress.Inflights)
	}
}

func drainProgressResetParityReady(
	t *testing.T,
	rn *RawNode,
	storage *MemoryStorage,
) {
	t.Helper()
	for iterations := 0; rn.HasReady(); iterations++ {
		if iterations >= 8 {
			t.Fatal("RawNode Ready did not drain")
		}
		ready := rn.Ready()
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
}

func TestProgressResetParity(t *testing.T) {
	rn, storage := newProgressResetParityRawNode(t)

	initial := progressResetParitySnapshot(rn)
	assertProgressResetParity(t, initial[1], 3, 4)
	assertProgressResetParity(t, initial[2], 0, 4)

	if err := rn.Campaign(); err != nil {
		t.Fatal(err)
	}
	drainProgressResetParityReady(t, rn, storage)
	if status := rn.BasicStatus(); status.RaftState != StateCandidate {
		t.Fatalf("campaign status = %+v, want candidate", status)
	}

	term := rn.BasicStatus().GetTerm()
	if err := rn.Step(&pb.Message{
		Type: pb.MsgVoteResp.Enum(),
		From: new(uint64(2)),
		To:   new(uint64(1)),
		Term: new(term),
	}); err != nil {
		t.Fatal(err)
	}
	if status := rn.BasicStatus(); status.RaftState != StateLeader {
		t.Fatalf("election status = %+v, want leader", status)
	}

	if err := rn.Step(&pb.Message{
		Type: pb.MsgHeartbeatResp.Enum(),
		From: new(uint64(2)),
		To:   new(uint64(1)),
		Term: new(term),
	}); err != nil {
		t.Fatal(err)
	}
	leaderProgress := rn.Status().Progress[2]
	if !leaderProgress.RecentActive {
		t.Fatal("heartbeat response did not mark peer active")
	}

	if err := rn.Step(&pb.Message{
		Type:   pb.MsgHeartbeat.Enum(),
		From:   new(uint64(2)),
		To:     new(uint64(1)),
		Term:   new(term + 1),
		Commit: new(uint64(3)),
	}); err != nil {
		t.Fatal(err)
	}
	if status := rn.BasicStatus(); status.RaftState != StateFollower {
		t.Fatalf("step-down status = %+v, want follower", status)
	}

	afterStepDown := progressResetParitySnapshot(rn)
	assertProgressResetParity(t, afterStepDown[1], 4, 5)
	assertProgressResetParity(t, afterStepDown[2], 0, 5)
}
