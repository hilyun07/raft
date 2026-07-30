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

	pb "go.etcd.io/raft/v3/raftpb"
)

func newMaxUncommittedEntriesSizeParityRawNode(
	t *testing.T,
) (*RawNode, *MemoryStorage) {
	t.Helper()
	storage := NewMemoryStorage()
	if err := storage.ApplySnapshot(&pb.Snapshot{
		Metadata: &pb.SnapshotMetadata{
			Index:     new(uint64(1)),
			Term:      new(uint64(1)),
			ConfState: &pb.ConfState{Voters: []uint64{1}},
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
		MaxUncommittedEntriesSize: 4,
		MaxInflightMsgs:           16,
	})
	if err != nil {
		t.Fatal(err)
	}
	return rn, storage
}

func drainMaxUncommittedEntriesSizeParityReady(
	t *testing.T,
	rn *RawNode,
	storage *MemoryStorage,
) {
	t.Helper()
	for iterations := 0; rn.HasReady(); iterations++ {
		if iterations >= 8 {
			t.Fatal("RawNode Ready did not drain")
		}
		rd := rn.Ready()
		if !IsEmptyHardState(rd.HardState) {
			if err := storage.SetHardState(rd.HardState); err != nil {
				t.Fatal(err)
			}
		}
		if err := storage.Append(rd.Entries); err != nil {
			t.Fatal(err)
		}
		rn.Advance(rd)
	}
}

func TestMaxUncommittedEntriesSizeParity(t *testing.T) {
	rn, storage := newMaxUncommittedEntriesSizeParityRawNode(t)
	if err := rn.Campaign(); err != nil {
		t.Fatal(err)
	}
	drainMaxUncommittedEntriesSizeParityReady(t, rn, storage)
	if status := rn.BasicStatus(); status.RaftState != StateLeader {
		t.Fatalf("campaign did not elect the single node: %+v", status)
	}

	oversized := []byte("large")
	belowLimit := []byte("ok")

	if err := rn.Propose(oversized); err != nil {
		t.Fatalf("first oversized proposal was rejected: %v", err)
	}
	if err := rn.Propose(oversized); !errors.Is(err, ErrProposalDropped) {
		t.Fatalf("second oversized proposal error = %v, want proposal dropped", err)
	}
	if err := rn.Propose(belowLimit); !errors.Is(err, ErrProposalDropped) {
		t.Fatalf("proposal on oversized uncommitted tail error = %v, want proposal dropped", err)
	}
	if err := rn.Propose([]byte{}); err != nil {
		t.Fatalf("empty proposal was rejected: %v", err)
	}

	drainMaxUncommittedEntriesSizeParityReady(t, rn, storage)

	if err := rn.Propose(belowLimit); err != nil {
		t.Fatalf("first below-limit proposal was rejected: %v", err)
	}
	if err := rn.Propose(belowLimit); err != nil {
		t.Fatalf("proposal reaching the exact limit was rejected: %v", err)
	}
	if err := rn.Propose([]byte("x")); !errors.Is(err, ErrProposalDropped) {
		t.Fatalf("proposal exceeding occupied limit error = %v, want proposal dropped", err)
	}

	drainMaxUncommittedEntriesSizeParityReady(t, rn, storage)

	if err := rn.Propose(oversized); err != nil {
		t.Fatalf("oversized proposal after commit was rejected: %v", err)
	}
}
