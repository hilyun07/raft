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

func newCandidateVoteOwnershipParityRawNode(
	t *testing.T,
) (*RawNode, *MemoryStorage) {
	return newCandidateVoteOwnershipParityRawNodeWithConfState(
		t,
		&pb.ConfState{Voters: []uint64{1, 2, 3, 4, 5}},
	)
}

func newCandidateVoteOwnershipParityRawNodeWithConfState(
	t *testing.T,
	confState *pb.ConfState,
) (*RawNode, *MemoryStorage) {
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
	rn, err := NewRawNode(&Config{
		ID:              1,
		ElectionTick:    10,
		HeartbeatTick:   1,
		Storage:         storage,
		Applied:         1,
		MaxSizePerMsg:   1024,
		MaxInflightMsgs: 16,
	})
	if err != nil {
		t.Fatal(err)
	}
	return rn, storage
}

func drainCandidateVoteOwnershipReady(
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

func campaignCandidateVoteOwnership(
	t *testing.T,
	rn *RawNode,
	storage *MemoryStorage,
) {
	t.Helper()
	if err := rn.Campaign(); err != nil {
		t.Fatal(err)
	}
	drainCandidateVoteOwnershipReady(t, rn, storage)
	if status := rn.BasicStatus(); status.RaftState != StateCandidate {
		t.Fatalf("campaign status = %+v, want candidate", status)
	}
}

func stepCandidateVoteOwnership(
	t *testing.T,
	rn *RawNode,
	from uint64,
	reject bool,
) {
	t.Helper()
	term := rn.BasicStatus().GetTerm()
	if err := rn.Step(&pb.Message{
		Type:   pb.MsgVoteResp.Enum(),
		To:     new(uint64(1)),
		From:   new(from),
		Term:   new(term),
		Reject: new(reject),
	}); err != nil {
		t.Fatal(err)
	}
}

func applyCandidateVoterChange(
	t *testing.T,
	rn *RawNode,
	changeType pb.ConfChangeType,
	id uint64,
) {
	t.Helper()
	rn.ApplyConfChange(&pb.ConfChange{
		Type:   changeType.Enum(),
		NodeId: new(id),
	})
}

func TestCandidateVoteOwnershipParity(t *testing.T) {
	t.Run("grant survives remove and re-add", func(t *testing.T) {
		rn, storage := newCandidateVoteOwnershipParityRawNode(t)
		campaignCandidateVoteOwnership(t, rn, storage)
		stepCandidateVoteOwnership(t, rn, 2, false)

		applyCandidateVoterChange(t, rn, pb.ConfChangeRemoveNode, 2)
		if rn.HasProgress(2) {
			t.Fatal("removed voter retained Progress")
		}
		applyCandidateVoterChange(t, rn, pb.ConfChangeAddNode, 2)
		if !rn.HasProgress(2) {
			t.Fatal("re-added voter has no Progress")
		}

		stepCandidateVoteOwnership(t, rn, 3, false)
		if status := rn.BasicStatus(); status.RaftState != StateLeader {
			t.Fatalf("retained grant election status = %+v, want leader",
				status)
		}
	})

	t.Run("rejection survives remove and re-add", func(t *testing.T) {
		rn, storage := newCandidateVoteOwnershipParityRawNode(t)
		campaignCandidateVoteOwnership(t, rn, storage)
		stepCandidateVoteOwnership(t, rn, 2, true)
		applyCandidateVoterChange(t, rn, pb.ConfChangeRemoveNode, 2)
		applyCandidateVoterChange(t, rn, pb.ConfChangeAddNode, 2)

		stepCandidateVoteOwnership(t, rn, 3, true)
		if status := rn.BasicStatus(); status.RaftState != StateCandidate {
			t.Fatalf("two-rejection status = %+v, want candidate", status)
		}
		stepCandidateVoteOwnership(t, rn, 4, true)
		if status := rn.BasicStatus(); status.RaftState != StateFollower {
			t.Fatalf("retained rejection election status = %+v, want follower",
				status)
		}
	})

	t.Run("new campaign resets retained vote", func(t *testing.T) {
		rn, storage := newCandidateVoteOwnershipParityRawNode(t)
		campaignCandidateVoteOwnership(t, rn, storage)
		stepCandidateVoteOwnership(t, rn, 2, false)
		applyCandidateVoterChange(t, rn, pb.ConfChangeRemoveNode, 2)
		applyCandidateVoterChange(t, rn, pb.ConfChangeAddNode, 2)

		campaignCandidateVoteOwnership(t, rn, storage)
		stepCandidateVoteOwnership(t, rn, 3, false)
		if status := rn.BasicStatus(); status.RaftState != StateCandidate {
			t.Fatalf("stale vote survived new campaign: %+v", status)
		}
		stepCandidateVoteOwnership(t, rn, 4, false)
		if status := rn.BasicStatus(); status.RaftState != StateLeader {
			t.Fatalf("fresh quorum election status = %+v, want leader", status)
		}
	})

	t.Run("removed vote is not counted", func(t *testing.T) {
		rn, storage := newCandidateVoteOwnershipParityRawNode(t)
		campaignCandidateVoteOwnership(t, rn, storage)
		stepCandidateVoteOwnership(t, rn, 2, false)
		applyCandidateVoterChange(t, rn, pb.ConfChangeRemoveNode, 2)

		stepCandidateVoteOwnership(t, rn, 3, false)
		if status := rn.BasicStatus(); status.RaftState != StateCandidate {
			t.Fatalf("removed vote affected quorum: %+v", status)
		}
		stepCandidateVoteOwnership(t, rn, 4, false)
		if status := rn.BasicStatus(); status.RaftState != StateLeader {
			t.Fatalf("current-voter quorum status = %+v, want leader", status)
		}
	})

	t.Run("learner vote counts only after promotion", func(t *testing.T) {
		rn, storage := newCandidateVoteOwnershipParityRawNodeWithConfState(
			t,
			&pb.ConfState{
				Voters:   []uint64{1, 3, 4},
				Learners: []uint64{2},
			},
		)
		campaignCandidateVoteOwnership(t, rn, storage)

		stepCandidateVoteOwnership(t, rn, 2, false)
		if status := rn.BasicStatus(); status.RaftState != StateCandidate {
			t.Fatalf("learner vote affected quorum before promotion: %+v",
				status)
		}

		applyCandidateVoterChange(t, rn, pb.ConfChangeAddNode, 2)
		stepCandidateVoteOwnership(t, rn, 3, false)
		if status := rn.BasicStatus(); status.RaftState != StateLeader {
			t.Fatalf("retained learner vote after promotion = %+v, want leader",
				status)
		}
	})
}
