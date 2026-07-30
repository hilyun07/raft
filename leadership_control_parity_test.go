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
	"testing"

	pb "go.etcd.io/raft/v3/raftpb"
)

func newLeadershipParityNode(
	t *testing.T,
	id uint64,
	preVote bool,
	checkQuorum bool,
) (*RawNode, *MemoryStorage) {
	t.Helper()
	storage := NewMemoryStorage()
	if err := storage.ApplySnapshot(&pb.Snapshot{
		Metadata: &pb.SnapshotMetadata{
			Index:     new(uint64(1)),
			Term:      new(uint64(1)),
			ConfState: &pb.ConfState{Voters: []uint64{1, 2, 3}},
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
		ID:              id,
		ElectionTick:    10,
		HeartbeatTick:   1,
		Storage:         storage,
		Applied:         1,
		MaxSizePerMsg:   1024,
		MaxInflightMsgs: 16,
		CheckQuorum:     checkQuorum,
		PreVote:         preVote,
		ReadOnlyOption:  ReadOnlySafe,
	})
	if err != nil {
		t.Fatal(err)
	}
	return rn, storage
}

func findLeadershipMessage(
	t *testing.T,
	messages []*pb.Message,
	messageType pb.MessageType,
	to uint64,
) *pb.Message {
	t.Helper()
	for i := len(messages) - 1; i >= 0; i-- {
		message := messages[i]
		if message.GetType() == messageType && message.GetTo() == to {
			return message
		}
	}
	t.Fatalf("no %s message to %d in %+v", messageType, to, messages)
	return nil
}

func acceptLeadershipReady(
	t *testing.T,
	rn *RawNode,
	storage *MemoryStorage,
) Ready {
	t.Helper()
	if !rn.HasReady() {
		t.Fatal("RawNode has no Ready")
	}
	rd := rn.Ready()
	persistReadIndexReady(t, rn, storage, rd)
	return rd
}

func campaignLeadershipParityLeader(
	t *testing.T,
	preVote bool,
	checkQuorum bool,
) (*RawNode, *MemoryStorage) {
	t.Helper()
	rn, storage := newLeadershipParityNode(
		t, 1, preVote, checkQuorum)
	if err := rn.Campaign(); err != nil {
		t.Fatal(err)
	}
	if preVote {
		status := rn.BasicStatus()
		if status.RaftState != StatePreCandidate ||
			status.HardState.GetTerm() != 1 ||
			status.HardState.GetVote() != None {
			t.Fatalf("pre-campaign status = %+v", status)
		}
		rd := acceptLeadershipReady(t, rn, storage)
		preVoteRequest := findLeadershipMessage(
			t, rd.Messages, pb.MsgPreVote, 2)
		if preVoteRequest.GetTerm() != 2 {
			t.Fatalf("pre-vote request = %+v", preVoteRequest)
		}
		if err := rn.Step(&pb.Message{
			Type: pb.MsgPreVoteResp.Enum(),
			To:   new(uint64(1)),
			From: new(uint64(2)),
			Term: new(uint64(2)),
		}); err != nil {
			t.Fatal(err)
		}
		status = rn.BasicStatus()
		if status.RaftState != StateCandidate ||
			status.HardState.GetTerm() != 2 ||
			status.HardState.GetVote() != 1 {
			t.Fatalf("post-pre-vote status = %+v", status)
		}
	} else {
		status := rn.BasicStatus()
		if status.RaftState != StateCandidate ||
			status.HardState.GetTerm() != 2 {
			t.Fatalf("campaign status = %+v", status)
		}
	}

	rd := acceptLeadershipReady(t, rn, storage)
	voteRequest := findLeadershipMessage(t, rd.Messages, pb.MsgVote, 2)
	if voteRequest.GetTerm() != 2 {
		t.Fatalf("vote request = %+v", voteRequest)
	}
	if err := rn.Step(&pb.Message{
		Type: pb.MsgVoteResp.Enum(),
		To:   new(uint64(1)),
		From: new(uint64(2)),
		Term: new(uint64(2)),
	}); err != nil {
		t.Fatal(err)
	}
	status := rn.BasicStatus()
	if status.RaftState != StateLeader ||
		status.Lead != 1 ||
		status.HardState.GetTerm() != 2 {
		t.Fatalf("leader status = %+v", status)
	}
	acceptLeadershipReady(t, rn, storage)
	return rn, storage
}

func TestRawNodePreVoteElectionParity(t *testing.T) {
	campaignLeadershipParityLeader(t, true, false)
}

func TestRawNodePreVoteRejectsStaleCandidateParity(t *testing.T) {
	storage := NewMemoryStorage()
	if err := storage.ApplySnapshot(&pb.Snapshot{
		Metadata: &pb.SnapshotMetadata{
			Index:     new(uint64(2)),
			Term:      new(uint64(3)),
			ConfState: &pb.ConfState{Voters: []uint64{1, 2, 3}},
		},
	}); err != nil {
		t.Fatal(err)
	}
	if err := storage.SetHardState(&pb.HardState{
		Term:   new(uint64(3)),
		Commit: new(uint64(2)),
	}); err != nil {
		t.Fatal(err)
	}
	rn, err := NewRawNode(&Config{
		ID:              1,
		ElectionTick:    10,
		HeartbeatTick:   1,
		Storage:         storage,
		Applied:         2,
		MaxSizePerMsg:   1024,
		MaxInflightMsgs: 16,
		PreVote:         true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := rn.Step(&pb.Message{
		Type:    pb.MsgPreVote.Enum(),
		To:      new(uint64(1)),
		From:    new(uint64(2)),
		Term:    new(uint64(4)),
		LogTerm: new(uint64(1)),
		Index:   new(uint64(1)),
	}); err != nil {
		t.Fatal(err)
	}
	status := rn.BasicStatus()
	if status.RaftState != StateFollower ||
		status.HardState.GetTerm() != 3 ||
		status.HardState.GetVote() != None {
		t.Fatalf("stale pre-vote changed status = %+v", status)
	}
	rd := acceptLeadershipReady(t, rn, storage)
	response := findLeadershipMessage(
		t, rd.Messages, pb.MsgPreVoteResp, 2)
	if !response.GetReject() || response.GetTerm() != 3 {
		t.Fatalf("stale pre-vote response = %+v", response)
	}
}

func TestRawNodePreVoteCheckQuorumForgetLeaderParity(t *testing.T) {
	rn, storage := newLeadershipParityNode(t, 2, true, true)
	if err := rn.Step(&pb.Message{
		Type:   pb.MsgHeartbeat.Enum(),
		To:     new(uint64(2)),
		From:   new(uint64(1)),
		Term:   new(uint64(2)),
		Commit: new(uint64(1)),
	}); err != nil {
		t.Fatal(err)
	}
	acceptLeadershipReady(t, rn, storage)

	preVote := &pb.Message{
		Type:    pb.MsgPreVote.Enum(),
		To:      new(uint64(2)),
		From:    new(uint64(3)),
		Term:    new(uint64(3)),
		LogTerm: new(uint64(1)),
		Index:   new(uint64(1)),
	}
	if err := rn.Step(preVote); err != nil {
		t.Fatal(err)
	}
	if rn.HasReady() {
		t.Fatal("active leader lease answered disruptive pre-vote")
	}
	if err := rn.ForgetLeader(); err != nil {
		t.Fatal(err)
	}
	if status := rn.BasicStatus(); status.Lead != None {
		t.Fatalf("leader after ForgetLeader = %d", status.Lead)
	}
	acceptLeadershipReady(t, rn, storage)

	if err := rn.Step(preVote); err != nil {
		t.Fatal(err)
	}
	rd := acceptLeadershipReady(t, rn, storage)
	response := findLeadershipMessage(
		t, rd.Messages, pb.MsgPreVoteResp, 3)
	if response.GetReject() || response.GetTerm() != 3 {
		t.Fatalf("post-forget pre-vote response = %+v", response)
	}
	if status := rn.BasicStatus(); status.HardState.GetTerm() != 2 {
		t.Fatalf("pre-vote changed local term: %+v", status)
	}
}

func TestRawNodePreVoteCheckQuorumActivityParity(t *testing.T) {
	rn, _ := campaignLeadershipParityLeader(t, true, true)
	if err := rn.Step(&pb.Message{
		Type:  pb.MsgAppResp.Enum(),
		To:    new(uint64(1)),
		From:  new(uint64(2)),
		Term:  new(uint64(2)),
		Index: new(uint64(2)),
	}); err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 10; i++ {
		rn.Tick()
	}
	if status := rn.BasicStatus(); status.RaftState != StateLeader {
		t.Fatalf("active leader stepped down: %+v", status)
	}
	if err := rn.Step(&pb.Message{
		Type: pb.MsgHeartbeatResp.Enum(),
		To:   new(uint64(1)),
		From: new(uint64(2)),
		Term: new(uint64(2)),
	}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		rn.Tick()
	}
	if status := rn.BasicStatus(); status.RaftState != StateLeader {
		t.Fatalf("heartbeat-active leader stepped down: %+v", status)
	}
	for i := 0; i < 10; i++ {
		rn.Tick()
	}
	if status := rn.BasicStatus(); status.RaftState != StateFollower ||
		status.Lead != None {
		t.Fatalf("inactive leader status = %+v", status)
	}
}

func TestRawNodeLeadershipTransferParity(t *testing.T) {
	rn, storage := campaignLeadershipParityLeader(t, true, true)
	for _, id := range []uint64{2, 3} {
		if err := rn.Step(&pb.Message{
			Type:  pb.MsgAppResp.Enum(),
			To:    new(uint64(1)),
			From:  new(id),
			Term:  new(uint64(2)),
			Index: new(uint64(2)),
		}); err != nil {
			t.Fatal(err)
		}
	}
	acceptLeadershipReady(t, rn, storage)

	rn.TransferLeader(2)
	if status := rn.BasicStatus(); status.LeadTransferee != 2 {
		t.Fatalf("transfer status = %+v", status)
	}
	rd := acceptLeadershipReady(t, rn, storage)
	findLeadershipMessage(t, rd.Messages, pb.MsgTimeoutNow, 2)
	if err := rn.Propose([]byte("blocked")); !errors.Is(
		err, ErrProposalDropped) {
		t.Fatalf("proposal during transfer = %v", err)
	}
	for i := 0; i < 10; i++ {
		rn.Tick()
	}
	status := rn.BasicStatus()
	if status.RaftState != StateLeader ||
		status.Lead != 1 ||
		status.LeadTransferee != None {
		t.Fatalf("timed-out transfer status = %+v", status)
	}
}

func TestRawNodeLeadershipTransferAfterCatchUpParity(t *testing.T) {
	rn, storage := campaignLeadershipParityLeader(t, false, false)
	for _, id := range []uint64{2, 3} {
		if err := rn.Step(&pb.Message{
			Type:  pb.MsgAppResp.Enum(),
			To:    new(uint64(1)),
			From:  new(id),
			Term:  new(uint64(2)),
			Index: new(uint64(2)),
		}); err != nil {
			t.Fatal(err)
		}
	}
	acceptLeadershipReady(t, rn, storage)

	if err := rn.Propose([]byte("lag")); err != nil {
		t.Fatal(err)
	}
	acceptLeadershipReady(t, rn, storage)
	if err := rn.Step(&pb.Message{
		Type:  pb.MsgAppResp.Enum(),
		To:    new(uint64(1)),
		From:  new(uint64(2)),
		Term:  new(uint64(2)),
		Index: new(uint64(3)),
	}); err != nil {
		t.Fatal(err)
	}
	acceptLeadershipReady(t, rn, storage)

	rn.TransferLeader(3)
	if status := rn.BasicStatus(); status.LeadTransferee != 3 {
		t.Fatalf("lagging transfer status = %+v", status)
	}
	if rn.HasReady() {
		rd := acceptLeadershipReady(t, rn, storage)
		for _, message := range rd.Messages {
			if message.GetType() == pb.MsgTimeoutNow &&
				message.GetTo() == 3 {
				t.Fatal("lagging transferee received early TimeoutNow")
			}
		}
	}
	if err := rn.Step(&pb.Message{
		Type:  pb.MsgAppResp.Enum(),
		To:    new(uint64(1)),
		From:  new(uint64(3)),
		Term:  new(uint64(2)),
		Index: new(uint64(3)),
	}); err != nil {
		t.Fatal(err)
	}
	rd := acceptLeadershipReady(t, rn, storage)
	findLeadershipMessage(t, rd.Messages, pb.MsgTimeoutNow, 3)
}

func TestRawNodeTransferForwardingAndTimeoutNowParity(t *testing.T) {
	rn, storage := newLeadershipParityNode(t, 2, true, true)
	if err := rn.Step(&pb.Message{
		Type:   pb.MsgHeartbeat.Enum(),
		To:     new(uint64(2)),
		From:   new(uint64(1)),
		Term:   new(uint64(2)),
		Commit: new(uint64(1)),
	}); err != nil {
		t.Fatal(err)
	}
	acceptLeadershipReady(t, rn, storage)

	if err := rn.Step(&pb.Message{
		Type: pb.MsgTransferLeader.Enum(),
		To:   new(uint64(2)),
		From: new(uint64(3)),
	}); err != nil {
		t.Fatal(err)
	}
	rd := acceptLeadershipReady(t, rn, storage)
	forwarded := findLeadershipMessage(
		t, rd.Messages, pb.MsgTransferLeader, 1)
	if forwarded.GetFrom() != 3 || forwarded.GetTerm() != 2 {
		t.Fatalf("forwarded transfer = %+v", forwarded)
	}

	if err := rn.Step(&pb.Message{
		Type: pb.MsgTimeoutNow.Enum(),
		To:   new(uint64(2)),
		From: new(uint64(1)),
		Term: new(uint64(2)),
	}); err != nil {
		t.Fatal(err)
	}
	status := rn.BasicStatus()
	if status.RaftState != StateCandidate ||
		status.HardState.GetTerm() != 3 {
		t.Fatalf("TimeoutNow status = %+v", status)
	}
	rd = acceptLeadershipReady(t, rn, storage)
	vote := findLeadershipMessage(t, rd.Messages, pb.MsgVote, 1)
	if !bytes.Equal(vote.GetContext(), []byte(campaignTransfer)) {
		t.Fatalf("transfer vote context = %q", vote.GetContext())
	}
	for _, message := range rd.Messages {
		if message.GetType() == pb.MsgPreVote {
			t.Fatalf("TimeoutNow used PreVote: %+v", rd.Messages)
		}
	}

	receiver, receiverStorage := newLeadershipParityNode(
		t, 3, true, true)
	if err := receiver.Step(&pb.Message{
		Type:   pb.MsgHeartbeat.Enum(),
		To:     new(uint64(3)),
		From:   new(uint64(1)),
		Term:   new(uint64(2)),
		Commit: new(uint64(1)),
	}); err != nil {
		t.Fatal(err)
	}
	acceptLeadershipReady(t, receiver, receiverStorage)
	forcedVote := findLeadershipMessage(t, rd.Messages, pb.MsgVote, 3)
	if err := receiver.Step(forcedVote); err != nil {
		t.Fatal(err)
	}
	if status := receiver.BasicStatus(); status.HardState.GetTerm() != 3 ||
		status.HardState.GetVote() != 2 {
		t.Fatalf("forced transfer vote status = %+v", status)
	}
	voteReady := acceptLeadershipReady(t, receiver, receiverStorage)
	response := findLeadershipMessage(
		t, voteReady.Messages, pb.MsgVoteResp, 2)
	if response.GetReject() {
		t.Fatalf("forced transfer vote rejected: %+v", response)
	}
}

func TestRawNodeCampaignBlockedByUnappliedConfChangeParity(t *testing.T) {
	storage := NewMemoryStorage()
	if err := storage.ApplySnapshot(&pb.Snapshot{
		Metadata: &pb.SnapshotMetadata{
			Index:     new(uint64(1)),
			Term:      new(uint64(1)),
			ConfState: &pb.ConfState{Voters: []uint64{1, 2, 3}},
		},
	}); err != nil {
		t.Fatal(err)
	}
	if err := storage.Append([]*pb.Entry{{
		Type:  pb.EntryConfChange.Enum(),
		Term:  new(uint64(1)),
		Index: new(uint64(2)),
	}}); err != nil {
		t.Fatal(err)
	}
	if err := storage.SetHardState(&pb.HardState{
		Term:   new(uint64(1)),
		Commit: new(uint64(2)),
	}); err != nil {
		t.Fatal(err)
	}
	rn, err := NewRawNode(&Config{
		ID:              2,
		ElectionTick:    10,
		HeartbeatTick:   1,
		Storage:         storage,
		Applied:         1,
		MaxSizePerMsg:   1024,
		MaxInflightMsgs: 16,
		PreVote:         true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := rn.Campaign(); err != nil {
		t.Fatal(err)
	}
	if status := rn.BasicStatus(); status.RaftState != StateFollower ||
		status.HardState.GetTerm() != 1 {
		t.Fatalf("campaign crossed unapplied config: %+v", status)
	}
	if err := rn.Step(&pb.Message{
		Type: pb.MsgTimeoutNow.Enum(),
		To:   new(uint64(2)),
		From: new(uint64(1)),
		Term: new(uint64(1)),
	}); err != nil {
		t.Fatal(err)
	}
	if status := rn.BasicStatus(); status.RaftState != StateFollower ||
		status.HardState.GetTerm() != 1 {
		t.Fatalf("transfer campaign crossed unapplied config: %+v", status)
	}
}
