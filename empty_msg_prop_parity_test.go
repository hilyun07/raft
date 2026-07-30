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
	"errors"
	"testing"
	"time"

	pb "go.etcd.io/raft/v3/raftpb"
)

func newEmptyMsgPropParityRawNode(
	t *testing.T,
	voters []uint64,
	maxUncommitted uint64,
) (*RawNode, *MemoryStorage) {
	t.Helper()
	storage := NewMemoryStorage()
	if err := storage.ApplySnapshot(&pb.Snapshot{
		Metadata: &pb.SnapshotMetadata{
			Index:     new(uint64(1)),
			Term:      new(uint64(1)),
			ConfState: &pb.ConfState{Voters: voters},
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
		ID:                        1,
		ElectionTick:              10,
		HeartbeatTick:             1,
		Storage:                   storage,
		Applied:                   1,
		MaxSizePerMsg:             1024,
		MaxUncommittedEntriesSize: maxUncommitted,
		MaxInflightMsgs:           16,
		Logger:                    discardLogger,
	})
	if err != nil {
		t.Fatal(err)
	}
	return rn, storage
}

func persistEmptyMsgPropParityReady(
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

func drainEmptyMsgPropParityReady(
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
		persistEmptyMsgPropParityReady(t, rn, storage, ready)
	}
}

func campaignEmptyMsgPropParityLeader(
	t *testing.T,
) (*RawNode, *MemoryStorage) {
	t.Helper()
	rn, storage := newEmptyMsgPropParityRawNode(
		t, []uint64{1}, 1024)
	if err := rn.Campaign(); err != nil {
		t.Fatal(err)
	}
	drainEmptyMsgPropParityReady(t, rn, storage)
	if status := rn.BasicStatus(); status.RaftState != StateLeader {
		t.Fatalf("campaign status = %+v, want leader", status)
	}
	return rn, storage
}

func captureEmptyMsgPropParityPanic(fn func()) (recovered any) {
	defer func() {
		recovered = recover()
	}()
	fn()
	return nil
}

func TestLeaderZeroEntryMsgPropPanicsParity(t *testing.T) {
	tests := []struct {
		name    string
		entries []*pb.Entry
	}{
		{name: "nil entries", entries: nil},
		{name: "empty entries", entries: make([]*pb.Entry, 0)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rn, _ := campaignEmptyMsgPropParityLeader(t)
			recovered := captureEmptyMsgPropParityPanic(func() {
				_ = rn.Step(&pb.Message{
					Type:    pb.MsgProp.Enum(),
					To:      new(uint64(1)),
					From:    new(uint64(1)),
					Entries: tt.entries,
				})
			})
			if recovered == nil {
				t.Fatal("leader zero-entry MsgProp did not panic")
			}
		})
	}
}

func TestRemovedLeaderZeroEntryMsgPropPanicsParity(t *testing.T) {
	rn, _ := campaignLeadershipParityLeader(t, false, false)
	state := rn.ApplyConfChange(&pb.ConfChangeV2{
		Changes: []*pb.ConfChangeSingle{{
			Type:   pb.ConfChangeRemoveNode.Enum(),
			NodeId: new(uint64(1)),
		}},
	})
	for _, id := range state.GetVoters() {
		if id == 1 {
			t.Fatalf("local voter remained after removal: %+v", state)
		}
	}
	if rn.BasicStatus().RaftState != StateLeader {
		t.Fatalf("removed leader state = %+v status=%+v",
			state, rn.BasicStatus())
	}
	if err := rn.Step(&pb.Message{
		Type:    pb.MsgProp.Enum(),
		To:      new(uint64(1)),
		From:    new(uint64(1)),
		Entries: []*pb.Entry{{}},
	}); !errors.Is(err, ErrProposalDropped) {
		t.Fatalf("removed leader nonempty proposal error = %v, want dropped",
			err)
	}
	recovered := captureEmptyMsgPropParityPanic(func() {
		_ = rn.Step(&pb.Message{
			Type: pb.MsgProp.Enum(),
			To:   new(uint64(1)),
			From: new(uint64(1)),
		})
	})
	if recovered == nil {
		t.Fatal("removed leader zero-entry MsgProp did not panic")
	}
}

func TestNodeLeaderZeroEntryMsgPropPanicsParity(t *testing.T) {
	rn, _ := campaignEmptyMsgPropParityLeader(t)
	n := newNode(rn)
	panicc := make(chan any, 1)
	go func() {
		defer func() {
			recovered := recover()
			if n.rn != nil {
				n.rn.destroy()
				n.rn = nil
			}
			close(n.done)
			panicc <- recovered
		}()
		n.run()
	}()

	err := n.Step(t.Context(), &pb.Message{Type: pb.MsgProp.Enum()})
	if err != nil && !errors.Is(err, ErrStopped) {
		t.Fatalf("Node.Step error = %v", err)
	}
	select {
	case recovered := <-panicc:
		if recovered == nil {
			t.Fatal("Node actor exited without panicking")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Node actor did not surface empty MsgProp invariant failure")
	}
}

func TestFollowerAndCandidateEmptyMsgPropParity(t *testing.T) {
	t.Run("follower forwards zero entries", func(t *testing.T) {
		for _, entries := range [][]*pb.Entry{
			nil,
			make([]*pb.Entry, 0),
		} {
			rn, storage := newEmptyMsgPropParityRawNode(
				t, []uint64{1, 2}, 1024)
			if err := rn.Step(&pb.Message{
				Type:   pb.MsgHeartbeat.Enum(),
				To:     new(uint64(1)),
				From:   new(uint64(2)),
				Term:   new(uint64(1)),
				Commit: new(uint64(1)),
			}); err != nil {
				t.Fatal(err)
			}
			drainEmptyMsgPropParityReady(t, rn, storage)
			if status := rn.BasicStatus(); status.Lead != 2 {
				t.Fatalf("follower status = %+v, want leader 2", status)
			}
			if err := rn.Step(&pb.Message{
				Type:    pb.MsgProp.Enum(),
				To:      new(uint64(1)),
				From:    new(uint64(1)),
				Entries: entries,
			}); err != nil {
				t.Fatal(err)
			}
			ready := rn.Ready()
			forwarded := findLeadershipMessage(
				t, ready.Messages, pb.MsgProp, 2)
			if len(forwarded.GetEntries()) != 0 {
				t.Fatalf("forwarded entries = %+v, want zero",
					forwarded.GetEntries())
			}
		}
	})

	t.Run("follower forwards empty-data entry", func(t *testing.T) {
		rn, storage := newEmptyMsgPropParityRawNode(
			t, []uint64{1, 2}, 1024)
		if err := rn.Step(&pb.Message{
			Type:   pb.MsgHeartbeat.Enum(),
			To:     new(uint64(1)),
			From:   new(uint64(2)),
			Term:   new(uint64(1)),
			Commit: new(uint64(1)),
		}); err != nil {
			t.Fatal(err)
		}
		drainEmptyMsgPropParityReady(t, rn, storage)
		if err := rn.Step(&pb.Message{
			Type: pb.MsgProp.Enum(),
			To:   new(uint64(1)),
			From: new(uint64(1)),
			Entries: []*pb.Entry{{
				Type: pb.EntryNormal.Enum(),
				Data: []byte{},
			}},
		}); err != nil {
			t.Fatal(err)
		}
		ready := rn.Ready()
		forwarded := findLeadershipMessage(
			t, ready.Messages, pb.MsgProp, 2)
		if len(forwarded.GetEntries()) != 1 ||
			forwarded.GetEntries()[0].Data == nil ||
			len(forwarded.GetEntries()[0].GetData()) != 0 {
			t.Fatalf("forwarded empty-data entry = %+v",
				forwarded.GetEntries())
		}
	})

	t.Run("candidate drops both forms", func(t *testing.T) {
		tests := []struct {
			name    string
			entries []*pb.Entry
		}{
			{name: "zero entries"},
			{name: "one empty entry", entries: []*pb.Entry{{}}},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				rn, storage := newEmptyMsgPropParityRawNode(
					t, []uint64{1, 2}, 1024)
				if err := rn.Campaign(); err != nil {
					t.Fatal(err)
				}
				drainEmptyMsgPropParityReady(t, rn, storage)
				if status := rn.BasicStatus(); status.RaftState != StateCandidate {
					t.Fatalf("campaign status = %+v, want candidate", status)
				}
				err := rn.Step(&pb.Message{
					Type:    pb.MsgProp.Enum(),
					To:      new(uint64(1)),
					From:    new(uint64(1)),
					Entries: tt.entries,
				})
				if !errors.Is(err, ErrProposalDropped) {
					t.Fatalf("candidate proposal error = %v, want dropped",
						err)
				}
			})
		}
	})
}

func TestLeaderEmptyDataEntryReadyParity(t *testing.T) {
	rn, _ := campaignEmptyMsgPropParityLeader(t)
	beforeTerm := rn.BasicStatus().HardState.GetTerm()

	if err := rn.Propose(nil); err != nil {
		t.Fatal(err)
	}
	if err := rn.Propose([]byte{}); err != nil {
		t.Fatal(err)
	}
	ready := rn.Ready()
	if len(ready.Entries) != 2 {
		t.Fatalf("Ready entries = %+v, want two", ready.Entries)
	}
	for i, entry := range ready.Entries {
		if entry.GetTerm() != beforeTerm ||
			entry.GetIndex() != uint64(i+3) ||
			entry.GetType() != pb.EntryNormal {
			t.Fatalf("Ready entry %d = %+v", i, entry)
		}
	}
	if ready.Entries[0].Data != nil {
		t.Fatalf("nil proposal Data = %#v, want nil",
			ready.Entries[0].Data)
	}
	if ready.Entries[1].Data == nil ||
		len(ready.Entries[1].Data) != 0 {
		t.Fatalf("present-empty proposal Data = %#v",
			ready.Entries[1].Data)
	}
}

func TestLeaderMixedAndMetadataOnlyEntriesParity(t *testing.T) {
	rn, _ := campaignEmptyMsgPropParityLeader(t)
	term := rn.BasicStatus().HardState.GetTerm()
	entries := []*pb.Entry{
		{
			Term:  new(uint64(99)),
			Index: new(uint64(99)),
			Data:  nil,
		},
		{
			Type: pb.EntryNormal.Enum(),
			Data: []byte{},
		},
		{
			Data: []byte("x"),
		},
	}
	if err := rn.Step(&pb.Message{
		Type:    pb.MsgProp.Enum(),
		To:      new(uint64(1)),
		From:    new(uint64(1)),
		Entries: entries,
	}); err != nil {
		t.Fatal(err)
	}
	ready := rn.Ready()
	if len(ready.Entries) != len(entries) {
		t.Fatalf("Ready entries = %+v", ready.Entries)
	}
	for i, entry := range ready.Entries {
		if entry.GetTerm() != term ||
			entry.GetIndex() != uint64(i+3) {
			t.Fatalf("Ready entry %d = %+v", i, entry)
		}
	}
	if ready.Entries[0].Data != nil ||
		ready.Entries[1].Data == nil ||
		string(ready.Entries[2].GetData()) != "x" {
		t.Fatalf("Ready Data shapes = %#v %#v %#v",
			ready.Entries[0].Data,
			ready.Entries[1].Data,
			ready.Entries[2].Data)
	}
	if ready.Entries[1].Type == nil {
		t.Fatal("metadata-only normal Entry lost explicit Type presence")
	}
}

func TestEmptyDataEntriesMaxUncommittedParity(t *testing.T) {
	rn, storage := newEmptyMsgPropParityRawNode(
		t, []uint64{1}, 4)
	if err := rn.Campaign(); err != nil {
		t.Fatal(err)
	}
	drainEmptyMsgPropParityReady(t, rn, storage)
	if err := rn.Propose([]byte("large")); err != nil {
		t.Fatal(err)
	}
	if err := rn.Step(&pb.Message{
		Type: pb.MsgProp.Enum(),
		To:   new(uint64(1)),
		From: new(uint64(1)),
		Entries: []*pb.Entry{
			{Data: nil},
			{Data: []byte{}},
		},
	}); err != nil {
		t.Fatalf("all-empty batch was rejected: %v", err)
	}
	if err := rn.Step(&pb.Message{
		Type: pb.MsgProp.Enum(),
		To:   new(uint64(1)),
		From: new(uint64(1)),
		Entries: []*pb.Entry{
			{Data: nil},
			{Data: []byte("x")},
		},
	}); !errors.Is(err, ErrProposalDropped) {
		t.Fatalf("mixed batch error = %v, want proposal dropped", err)
	}
	ready := rn.Ready()
	if len(ready.Entries) != 3 {
		t.Fatalf("Ready entries = %+v, want oversized plus two empty",
			ready.Entries)
	}
}

func TestEmptyDataConfigurationEntryParity(t *testing.T) {
	rn, storage := campaignEmptyMsgPropParityLeader(t)

	if err := rn.Step(&pb.Message{
		Type: pb.MsgProp.Enum(),
		To:   new(uint64(1)),
		From: new(uint64(1)),
		Entries: []*pb.Entry{{
			Type: pb.EntryConfChangeV2.Enum(),
			Data: nil,
		}},
	}); err != nil {
		t.Fatal(err)
	}
	ready := rn.Ready()
	if len(ready.Entries) != 1 ||
		ready.Entries[0].GetType() != pb.EntryNormal ||
		ready.Entries[0].Data != nil {
		t.Fatalf("non-joint empty ConfChangeV2 Ready = %+v",
			ready.Entries)
	}
	persistEmptyMsgPropParityReady(t, rn, storage, ready)
	drainEmptyMsgPropParityReady(t, rn, storage)

	if err := rn.Step(&pb.Message{
		Type: pb.MsgProp.Enum(),
		To:   new(uint64(1)),
		From: new(uint64(1)),
		Entries: []*pb.Entry{{
			Type: pb.EntryConfChange.Enum(),
			Data: []byte{},
		}},
	}); err != nil {
		t.Fatal(err)
	}
	ready = rn.Ready()
	if len(ready.Entries) != 1 ||
		ready.Entries[0].GetType() != pb.EntryConfChange ||
		ready.Entries[0].Data == nil ||
		len(ready.Entries[0].Data) != 0 {
		t.Fatalf("empty ConfChange Ready = %+v", ready.Entries)
	}
}
