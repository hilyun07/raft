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
)

// TestRawNodeElectionTimeoutRangeParity exercises the externally visible
// lower and upper bounds against both the Go and C-backed RawNode.
func TestRawNodeElectionTimeoutRangeParity(t *testing.T) {
	const electionTick = 10

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
		ID:              1,
		ElectionTick:    electionTick,
		HeartbeatTick:   1,
		Storage:         storage,
		Applied:         1,
		MaxSizePerMsg:   1024,
		MaxInflightMsgs: 16,
	})
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < electionTick-1; i++ {
		rn.Tick()
	}
	if status := rn.BasicStatus(); status.RaftState != StateFollower {
		t.Fatalf("election before lower bound: %+v", status)
	}

	for i := electionTick - 1; i < 2*electionTick-1; i++ {
		rn.Tick()
		if rn.BasicStatus().RaftState != StateFollower {
			break
		}
	}
	if status := rn.BasicStatus(); status.RaftState != StateCandidate {
		t.Fatalf("no election by upper bound: %+v", status)
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
	if status := rn.BasicStatus(); status.RaftState != StateLeader {
		t.Fatalf("single-node election did not complete: %+v", status)
	}
}
