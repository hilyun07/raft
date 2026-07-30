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

func captureInvalidInputInvariantPanic(fn func()) (recovered any) {
	defer func() {
		recovered = recover()
	}()
	fn()
	return nil
}

func invalidInputInvariantConfig(
	t *testing.T,
	confState *pb.ConfState,
) *Config {
	t.Helper()
	storage := NewMemoryStorage()
	if err := storage.ApplySnapshot(&pb.Snapshot{
		Metadata: &pb.SnapshotMetadata{
			Index:     new(uint64(1)),
			Term:      new(uint64(1)),
			ConfState: confState,
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
	return &Config{
		ID:              1,
		ElectionTick:    10,
		HeartbeatTick:   1,
		Storage:         storage,
		Applied:         1,
		MaxSizePerMsg:   1024,
		MaxInflightMsgs: 16,
		Logger:          discardLogger,
	}
}

func TestMalformedConfStateClassificationParity(t *testing.T) {
	tests := []struct {
		name      string
		confState *pb.ConfState
		wantPanic bool
	}{
		{
			name:      "empty",
			confState: &pb.ConfState{},
		},
		{
			name: "simple",
			confState: &pb.ConfState{
				Voters: []uint64{1, 2, 3},
			},
		},
		{
			name: "legal joint demotion",
			confState: &pb.ConfState{
				Voters:         []uint64{1, 3},
				VotersOutgoing: []uint64{1, 2},
				LearnersNext:   []uint64{2},
			},
		},
		{
			name: "legal joint reference example",
			confState: &pb.ConfState{
				Voters:         []uint64{1, 2, 3},
				VotersOutgoing: []uint64{1, 2, 4, 6},
				Learners:       []uint64{5},
				LearnersNext:   []uint64{4},
				AutoLeave:      new(true),
			},
		},
		{
			name: "incoming voter is staged learner",
			confState: &pb.ConfState{
				Voters:         []uint64{1, 2, 3},
				VotersOutgoing: []uint64{1, 2},
				LearnersNext:   []uint64{2},
			},
			wantPanic: true,
		},
		{
			name: "duplicate voter",
			confState: &pb.ConfState{
				Voters: []uint64{1, 1},
			},
			wantPanic: true,
		},
		{
			name: "voter learner overlap",
			confState: &pb.ConfState{
				Voters:   []uint64{1, 2},
				Learners: []uint64{2},
			},
			wantPanic: true,
		},
		{
			name: "staged learner missing from outgoing voters",
			confState: &pb.ConfState{
				Voters:         []uint64{1, 2},
				VotersOutgoing: []uint64{1},
				LearnersNext:   []uint64{2},
			},
			wantPanic: true,
		},
		{
			name: "learner outgoing voter overlap",
			confState: &pb.ConfState{
				Voters:         []uint64{1, 2},
				VotersOutgoing: []uint64{1, 3},
				Learners:       []uint64{3},
			},
			wantPanic: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var rn *RawNode
			var err error
			recovered := captureInvalidInputInvariantPanic(func() {
				rn, err = NewRawNode(
					invalidInputInvariantConfig(t, tt.confState),
				)
			})
			if tt.wantPanic {
				if recovered == nil {
					t.Fatalf("NewRawNode accepted malformed ConfState: %+v",
						tt.confState)
				}
				return
			}
			if recovered != nil {
				t.Fatalf("NewRawNode panicked for valid ConfState %+v: %v",
					tt.confState, recovered)
			}
			if err != nil {
				t.Fatalf("NewRawNode valid ConfState: %v", err)
			}
			if rn == nil {
				t.Fatal("NewRawNode returned a nil RawNode")
			}
		})
	}
}

func newHeartbeatInvariantParityRawNode(
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
	if err := storage.Append([]*pb.Entry{
		{Index: new(uint64(2)), Term: new(uint64(2))},
		{Index: new(uint64(3)), Term: new(uint64(2))},
	}); err != nil {
		t.Fatal(err)
	}
	if err := storage.SetHardState(&pb.HardState{
		Term:   new(uint64(2)),
		Commit: new(uint64(1)),
	}); err != nil {
		t.Fatal(err)
	}
	rn, err := NewRawNode(&Config{
		ID:              1,
		ElectionTick:    10,
		HeartbeatTick:   1,
		Storage:         storage,
		Applied:         1,
		MaxSizePerMsg:   1024,
		MaxInflightMsgs: 16,
		Logger:          discardLogger,
	})
	if err != nil {
		t.Fatal(err)
	}
	return rn, storage
}

func heartbeatInvariantMessage(commit uint64) *pb.Message {
	return &pb.Message{
		Type:   pb.MsgHeartbeat.Enum(),
		To:     new(uint64(1)),
		From:   new(uint64(2)),
		Term:   new(uint64(2)),
		Commit: new(commit),
	}
}

func TestHeartbeatCommitValidCasesParity(t *testing.T) {
	tests := []struct {
		name             string
		commit           uint64
		wantCommit       uint64
		wantEntryIndices []uint64
	}{
		{
			name:       "stale commit does not decrease",
			commit:     0,
			wantCommit: 1,
		},
		{
			name:             "advance below last index",
			commit:           2,
			wantCommit:       2,
			wantEntryIndices: []uint64{2},
		},
		{
			name:             "exact last index boundary",
			commit:           3,
			wantCommit:       3,
			wantEntryIndices: []uint64{2, 3},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rn, _ := newHeartbeatInvariantParityRawNode(t)
			if err := rn.Step(heartbeatInvariantMessage(tt.commit)); err != nil {
				t.Fatal(err)
			}
			if got := rn.BasicStatus().HardState.GetCommit(); got != tt.wantCommit {
				t.Fatalf("committed index = %d, want %d",
					got, tt.wantCommit)
			}
			ready := rn.Ready()
			if len(ready.CommittedEntries) != len(tt.wantEntryIndices) {
				t.Fatalf("committed entries = %+v, want indices %v",
					ready.CommittedEntries, tt.wantEntryIndices)
			}
			for i, want := range tt.wantEntryIndices {
				if got := ready.CommittedEntries[i].GetIndex(); got != want {
					t.Fatalf("committed entry %d index = %d, want %d",
						i, got, want)
				}
			}
		})
	}
}

func TestHeartbeatCommitPastLastIndexPanicsParity(t *testing.T) {
	rn, _ := newHeartbeatInvariantParityRawNode(t)
	recovered := captureInvalidInputInvariantPanic(func() {
		_ = rn.Step(heartbeatInvariantMessage(4))
	})
	if recovered == nil {
		t.Fatal("heartbeat Commit greater than lastIndex did not panic")
	}
}

func TestNodeHeartbeatCommitPastLastIndexPanicsParity(t *testing.T) {
	rn, _ := newHeartbeatInvariantParityRawNode(t)
	n := newNode(rn)
	panicc := make(chan any, 1)
	go func() {
		defer func() {
			recovered := recover()
			close(n.done)
			panicc <- recovered
		}()
		n.run()
	}()

	err := n.Step(t.Context(), heartbeatInvariantMessage(4))
	if err != nil && !errors.Is(err, ErrStopped) {
		t.Fatalf("Node.Step error = %v", err)
	}
	select {
	case recovered := <-panicc:
		if recovered == nil {
			t.Fatal("Node actor exited without panicking")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Node actor did not surface heartbeat invariant failure")
	}
}

func TestMalformedSnapshotConfStatePanicsParity(t *testing.T) {
	rn, _ := newHeartbeatInvariantParityRawNode(t)
	malformed := &pb.ConfState{
		Voters:         []uint64{1, 2, 3},
		VotersOutgoing: []uint64{1, 2},
		LearnersNext:   []uint64{2},
	}
	recovered := captureInvalidInputInvariantPanic(func() {
		_ = rn.Step(&pb.Message{
			Type: pb.MsgSnap.Enum(),
			To:   new(uint64(1)),
			From: new(uint64(2)),
			Term: new(uint64(2)),
			Snapshot: &pb.Snapshot{
				Metadata: &pb.SnapshotMetadata{
					Index:     new(uint64(4)),
					Term:      new(uint64(2)),
					ConfState: malformed,
				},
			},
		})
	})
	if recovered == nil {
		t.Fatal("snapshot restore accepted malformed ConfState")
	}
}
