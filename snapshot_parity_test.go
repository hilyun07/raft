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
	"reflect"
	"testing"

	pb "go.etcd.io/raft/v3/raftpb"
	"go.etcd.io/raft/v3/tracker"
)

// TestRawNodeSnapshotSendParity runs the leader-side compacted-log fallback
// against both RawNode implementations.
func TestRawNodeSnapshotSendParity(t *testing.T) {
	storage := NewMemoryStorage()
	if err := storage.ApplySnapshot(&pb.Snapshot{
		Data: []byte("transport"),
		Metadata: &pb.SnapshotMetadata{
			Index:     new(uint64(5)),
			Term:      new(uint64(1)),
			ConfState: &pb.ConfState{Voters: []uint64{1, 2}},
		},
	}); err != nil {
		t.Fatal(err)
	}
	rn, err := NewRawNode(&Config{
		ID:              1,
		ElectionTick:    10,
		HeartbeatTick:   1,
		Storage:         storage,
		MaxSizePerMsg:   1024,
		MaxInflightMsgs: 16,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := rn.Campaign(); err != nil {
		t.Fatal(err)
	}
	campaignReady := rn.Ready()
	if err := storage.Append(campaignReady.Entries); err != nil {
		t.Fatal(err)
	}
	if err := storage.SetHardState(campaignReady.HardState); err != nil {
		t.Fatal(err)
	}
	rn.Advance(campaignReady)
	if err := rn.Step(&pb.Message{
		Type: pb.MsgVoteResp.Enum(),
		To:   new(uint64(1)),
		From: new(uint64(2)),
		Term: new(uint64(1)),
	}); err != nil {
		t.Fatal(err)
	}
	if err := rn.Step(&pb.Message{
		Type:       pb.MsgAppResp.Enum(),
		To:         new(uint64(1)),
		From:       new(uint64(2)),
		Term:       new(uint64(1)),
		Index:      new(uint64(5)),
		Reject:     new(true),
		RejectHint: new(uint64(0)),
	}); err != nil {
		t.Fatal(err)
	}

	rd := rn.Ready()
	var message *pb.Message
	for _, candidate := range rd.Messages {
		if candidate.GetType() == pb.MsgSnap && candidate.GetTo() == 2 {
			message = candidate
		}
	}
	if message == nil || message.GetSnapshot() == nil {
		t.Fatalf("Ready messages contain no MsgSnap: %+v", rd.Messages)
	}
	if rd.Snapshot != nil {
		t.Fatalf("outbound MsgSnap also appeared as Ready.Snapshot: %+v", rd.Snapshot)
	}
	if got := message.GetSnapshot(); got.GetMetadata().GetIndex() != 5 ||
		got.GetMetadata().GetTerm() != 1 ||
		!bytes.Equal(got.Data, []byte("transport")) ||
		!reflect.DeepEqual(got.GetMetadata().GetConfState().Voters,
			[]uint64{1, 2}) {
		t.Fatalf("outbound snapshot = %+v", got)
	}
	var before tracker.Progress
	rn.WithProgress(func(id uint64, _ ProgressType, progress tracker.Progress) {
		if id == 2 {
			before = progress
		}
	})
	if before.State != tracker.StateSnapshot || before.PendingSnapshot != 5 {
		t.Fatalf("progress before report = %+v", before)
	}
	if err := storage.Append(rd.Entries); err != nil {
		t.Fatal(err)
	}
	if err := storage.SetHardState(rd.HardState); err != nil {
		t.Fatal(err)
	}
	rn.Advance(rd)

	rn.ReportSnapshot(2, SnapshotFailure)
	var after tracker.Progress
	rn.WithProgress(func(id uint64, _ ProgressType, progress tracker.Progress) {
		if id == 2 {
			after = progress
		}
	})
	if after.State != tracker.StateProbe || after.PendingSnapshot != 0 ||
		after.Next != 1 || !after.MsgAppFlowPaused {
		t.Fatalf("progress after failure = %+v", after)
	}
}

// TestRawNodeSnapshotRestoreParity runs unchanged against the default Go
// implementation and the cgo_raft backend. It is a compact differential
// contract for the externally visible restore trace.
func TestRawNodeSnapshotRestoreParity(t *testing.T) {
	storage := NewMemoryStorage()
	rn, err := NewRawNode(&Config{
		ID:              2,
		ElectionTick:    10,
		HeartbeatTick:   1,
		Storage:         storage,
		MaxSizePerMsg:   1024,
		MaxInflightMsgs: 16,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := rn.Bootstrap([]Peer{{ID: 1}, {ID: 2}}); err != nil {
		t.Fatal(err)
	}

	snapshot := &pb.Snapshot{
		Data: []byte("opaque-state"),
		Metadata: &pb.SnapshotMetadata{
			Index:     new(uint64(5)),
			Term:      new(uint64(1)),
			ConfState: &pb.ConfState{Voters: []uint64{1, 2, 3}},
		},
	}
	if err := rn.Step(&pb.Message{
		Type:     pb.MsgSnap.Enum(),
		To:       new(uint64(2)),
		From:     new(uint64(1)),
		Term:     new(uint64(2)),
		Snapshot: snapshot,
	}); err != nil {
		t.Fatal(err)
	}
	rd := rn.Ready()
	if rd.Snapshot == nil ||
		rd.Snapshot.GetMetadata().GetIndex() != 5 ||
		rd.Snapshot.GetMetadata().GetTerm() != 1 ||
		!bytes.Equal(rd.Snapshot.Data, []byte("opaque-state")) ||
		!reflect.DeepEqual(rd.Snapshot.GetMetadata().GetConfState().Voters,
			[]uint64{1, 2, 3}) {
		t.Fatalf("Ready snapshot = %+v", rd.Snapshot)
	}
	if rd.HardState.GetTerm() != 2 || rd.HardState.GetCommit() != 5 {
		t.Fatalf("Ready HardState = %+v", rd.HardState)
	}
	if len(rd.Entries) != 0 || len(rd.CommittedEntries) != 0 {
		t.Fatalf("Ready entries=%+v committed=%+v", rd.Entries, rd.CommittedEntries)
	}
	var response *pb.Message
	for _, message := range rd.Messages {
		if message.GetType() == pb.MsgAppResp && message.GetTo() == 1 {
			response = message
		}
	}
	if response == nil || response.GetIndex() != 5 || response.GetReject() {
		t.Fatalf("snapshot response = %+v", response)
	}
	if got := rn.Status(); len(got.Config.Voters[0]) != 3 {
		t.Fatalf("restored config = %+v", got.Config)
	} else {
		for _, id := range []uint64{1, 2, 3} {
			if _, ok := got.Config.Voters[0][id]; !ok {
				t.Fatalf("restored config = %+v", got.Config)
			}
		}
	}
	if applied := rn.BasicStatus().Applied; applied != 0 {
		t.Fatalf("applied before Advance = %d, want 0", applied)
	}

	if err := storage.ApplySnapshot(rd.Snapshot); err != nil {
		t.Fatal(err)
	}
	if err := storage.SetHardState(rd.HardState); err != nil {
		t.Fatal(err)
	}
	rn.Advance(rd)
	if applied := rn.BasicStatus().Applied; applied != 5 {
		t.Fatalf("applied after Advance = %d, want 5", applied)
	}
	if rn.HasReady() {
		t.Fatal("snapshot remained Ready after persistence and Advance")
	}

	entry := &pb.Entry{
		Type:  pb.EntryNormal.Enum(),
		Term:  new(uint64(2)),
		Index: new(uint64(6)),
		Data:  []byte("after"),
	}
	if err := rn.Step(&pb.Message{
		Type:    pb.MsgApp.Enum(),
		To:      new(uint64(2)),
		From:    new(uint64(1)),
		Term:    new(uint64(2)),
		LogTerm: new(uint64(1)),
		Index:   new(uint64(5)),
		Commit:  new(uint64(6)),
		Entries: []*pb.Entry{entry},
	}); err != nil {
		t.Fatal(err)
	}
	rd = rn.Ready()
	if rd.Snapshot != nil || len(rd.CommittedEntries) != 1 ||
		rd.CommittedEntries[0].GetIndex() != 6 {
		t.Fatalf("post-snapshot Ready = %+v", rd)
	}
	if err := storage.Append(rd.Entries); err != nil {
		t.Fatal(err)
	}
	if err := storage.SetHardState(rd.HardState); err != nil {
		t.Fatal(err)
	}
	rn.Advance(rd)
	if applied := rn.BasicStatus().Applied; applied != 6 {
		t.Fatalf("post-snapshot applied = %d, want 6", applied)
	}
}
