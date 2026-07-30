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

func newAppendRejectionParityRawNode(
	t *testing.T,
	id uint64,
	lastIndex uint64,
	tailTerm uint64,
	hardTerm uint64,
	vote uint64,
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
	entries := make([]*pb.Entry, 0, lastIndex-1)
	for index := uint64(2); index <= lastIndex; index++ {
		entries = append(entries, &pb.Entry{
			Index: new(index),
			Term:  new(tailTerm),
		})
	}
	if err := storage.Append(entries); err != nil {
		t.Fatal(err)
	}
	if err := storage.SetHardState(&pb.HardState{
		Term:   new(hardTerm),
		Vote:   new(vote),
		Commit: new(uint64(1)),
	}); err != nil {
		t.Fatal(err)
	}
	rn, err := NewRawNode(&Config{
		ID:              id,
		ElectionTick:    10,
		HeartbeatTick:   1,
		Storage:         storage,
		Applied:         1,
		MaxSizePerMsg:   1 << 20,
		MaxInflightMsgs: 16,
	})
	if err != nil {
		t.Fatal(err)
	}
	return rn, storage
}

func persistAppendRejectionParityReady(
	t *testing.T,
	rn *RawNode,
	storage *MemoryStorage,
	ready Ready,
) {
	t.Helper()
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

func findAppendRejectionParityMessage(
	t *testing.T,
	messages []*pb.Message,
	messageType pb.MessageType,
	to uint64,
) *pb.Message {
	t.Helper()
	for _, message := range messages {
		if message.GetType() == messageType && message.GetTo() == to {
			return message
		}
	}
	t.Fatalf("no %s message to %d in %+v", messageType, to, messages)
	return nil
}

func prepareAppendRejectionParityLeader(
	t *testing.T,
) (*RawNode, *MemoryStorage, *pb.Message) {
	t.Helper()
	leader, storage := newAppendRejectionParityRawNode(
		t, 1, 100, 2, 1, None)
	if err := leader.Campaign(); err != nil {
		t.Fatal(err)
	}
	campaignReady := leader.Ready()
	findAppendRejectionParityMessage(
		t, campaignReady.Messages, pb.MsgVote, 2)
	persistAppendRejectionParityReady(
		t, leader, storage, campaignReady)

	if err := leader.Step(&pb.Message{
		Type: pb.MsgVoteResp.Enum(),
		To:   new(uint64(1)),
		From: new(uint64(2)),
		Term: new(uint64(2)),
	}); err != nil {
		t.Fatal(err)
	}
	leaderReady := leader.Ready()
	initialAppend := findAppendRejectionParityMessage(
		t, leaderReady.Messages, pb.MsgApp, 2)
	if initialAppend.GetIndex() != 100 ||
		initialAppend.GetLogTerm() != 2 {
		t.Fatalf("initial append = %+v", initialAppend)
	}
	persistAppendRejectionParityReady(
		t, leader, storage, leaderReady)
	if progress := leader.Status().Progress[2]; progress.Next != 101 ||
		progress.State != tracker.StateProbe {
		t.Fatalf("initial follower progress = %+v", progress)
	}
	return leader, storage, initialAppend
}

func TestRawNodeAppendRejectionLogTermParity(t *testing.T) {
	t.Run("optimized divergent tail", func(t *testing.T) {
		leader, leaderStorage, initialAppend :=
			prepareAppendRejectionParityLeader(t)
		follower, followerStorage :=
			newAppendRejectionParityRawNode(
				t, 2, 99, 1, 2, 1)

		if err := follower.Step(initialAppend); err != nil {
			t.Fatal(err)
		}
		rejectionReady := follower.Ready()
		rejection := findAppendRejectionParityMessage(
			t, rejectionReady.Messages, pb.MsgAppResp, 1)
		if !rejection.GetReject() ||
			rejection.GetIndex() != 100 ||
			rejection.GetRejectHint() != 99 ||
			rejection.GetLogTerm() != 1 {
			t.Fatalf("rejection = %+v", rejection)
		}
		persistAppendRejectionParityReady(
			t, follower, followerStorage, rejectionReady)

		if err := leader.Step(rejection); err != nil {
			t.Fatal(err)
		}
		if progress := leader.Status().Progress[2]; progress.Next != 2 {
			t.Fatalf("optimized follower progress = %+v", progress)
		}
		probeReady := leader.Ready()
		probe := findAppendRejectionParityMessage(
			t, probeReady.Messages, pb.MsgApp, 2)
		if probe.GetIndex() != 1 ||
			probe.GetLogTerm() != 1 ||
			len(probe.GetEntries()) == 0 ||
			probe.GetEntries()[len(probe.GetEntries())-1].GetIndex() != 101 {
			t.Fatalf("optimized probe = %+v", probe)
		}
		persistAppendRejectionParityReady(
			t, leader, leaderStorage, probeReady)

		if err := follower.Step(probe); err != nil {
			t.Fatal(err)
		}
		successReady := follower.Ready()
		success := findAppendRejectionParityMessage(
			t, successReady.Messages, pb.MsgAppResp, 1)
		if success.GetReject() || success.GetIndex() != 101 {
			t.Fatalf("probe response = %+v", success)
		}
		persistAppendRejectionParityReady(
			t, follower, followerStorage, successReady)

		if err := leader.Step(success); err != nil {
			t.Fatal(err)
		}
		progress := leader.Status().Progress[2]
		if progress.State != tracker.StateReplicate ||
			progress.Match != 101 ||
			progress.Next != 102 {
			t.Fatalf("converged follower progress = %+v", progress)
		}
	})

	t.Run("zero log term fallback", func(t *testing.T) {
		leader, storage, _ :=
			prepareAppendRejectionParityLeader(t)
		if err := leader.Step(&pb.Message{
			Type:       pb.MsgAppResp.Enum(),
			To:         new(uint64(1)),
			From:       new(uint64(2)),
			Term:       new(uint64(2)),
			Index:      new(uint64(100)),
			Reject:     new(true),
			RejectHint: new(uint64(99)),
		}); err != nil {
			t.Fatal(err)
		}
		if progress := leader.Status().Progress[2]; progress.Next != 100 {
			t.Fatalf("zero-term follower progress = %+v", progress)
		}
		probeReady := leader.Ready()
		probe := findAppendRejectionParityMessage(
			t, probeReady.Messages, pb.MsgApp, 2)
		if probe.GetIndex() != 99 || probe.GetLogTerm() != 2 {
			t.Fatalf("zero-term probe = %+v", probe)
		}
		persistAppendRejectionParityReady(
			t, leader, storage, probeReady)
	})
}
