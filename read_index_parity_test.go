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
	"testing"

	pb "go.etcd.io/raft/v3/raftpb"
)

func newReadIndexParityNode(
	t *testing.T,
	id uint64,
	option ReadOnlyOption,
	checkQuorum bool,
) (*RawNode, *MemoryStorage) {
	t.Helper()
	return newReadIndexParityNodeWithVoters(
		t, id, option, checkQuorum, []uint64{1, 2, 3})
}

func newReadIndexParityNodeWithVoters(
	t *testing.T,
	id uint64,
	option ReadOnlyOption,
	checkQuorum bool,
	voters []uint64,
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
		ID:              id,
		ElectionTick:    10,
		HeartbeatTick:   1,
		Storage:         storage,
		Applied:         1,
		MaxSizePerMsg:   1024,
		MaxInflightMsgs: 16,
		CheckQuorum:     checkQuorum,
		ReadOnlyOption:  option,
	})
	if err != nil {
		t.Fatal(err)
	}
	return rn, storage
}

func persistReadIndexReady(
	t *testing.T,
	rn *RawNode,
	storage *MemoryStorage,
	rd Ready,
) {
	t.Helper()
	if len(rd.Entries) != 0 {
		if err := storage.Append(rd.Entries); err != nil {
			t.Fatal(err)
		}
	}
	if !IsEmptyHardState(rd.HardState) {
		if err := storage.SetHardState(rd.HardState); err != nil {
			t.Fatal(err)
		}
	}
	if rd.Snapshot != nil {
		if err := storage.ApplySnapshot(rd.Snapshot); err != nil {
			t.Fatal(err)
		}
	}
	rn.Advance(rd)
}

func campaignReadIndexParityLeader(
	t *testing.T,
	option ReadOnlyOption,
	checkQuorum bool,
) (*RawNode, *MemoryStorage) {
	t.Helper()
	rn, storage := newReadIndexParityNode(t, 1, option, checkQuorum)
	if err := rn.Campaign(); err != nil {
		t.Fatal(err)
	}
	persistReadIndexReady(t, rn, storage, rn.Ready())
	if err := rn.Step(&pb.Message{
		Type: pb.MsgVoteResp.Enum(),
		To:   new(uint64(1)),
		From: new(uint64(2)),
		Term: new(uint64(2)),
	}); err != nil {
		t.Fatal(err)
	}
	leaderReady := rn.Ready()
	if leaderReady.SoftState == nil ||
		leaderReady.SoftState.RaftState != StateLeader {
		t.Fatalf("leader Ready SoftState = %+v", leaderReady.SoftState)
	}
	persistReadIndexReady(t, rn, storage, leaderReady)
	return rn, storage
}

func prepareReadIndexParityLeader(
	t *testing.T,
	option ReadOnlyOption,
	checkQuorum bool,
) (*RawNode, *MemoryStorage) {
	t.Helper()
	rn, storage := campaignReadIndexParityLeader(
		t, option, checkQuorum)
	if err := rn.Step(&pb.Message{
		Type:  pb.MsgAppResp.Enum(),
		To:    new(uint64(1)),
		From:  new(uint64(2)),
		Term:  new(uint64(2)),
		Index: new(uint64(2)),
	}); err != nil {
		t.Fatal(err)
	}
	persistReadIndexReady(t, rn, storage, rn.Ready())
	status := rn.BasicStatus()
	if status.RaftState != StateLeader ||
		status.HardState.GetTerm() != 2 ||
		status.HardState.GetCommit() != 2 {
		t.Fatalf("prepared leader status = %+v", status)
	}
	return rn, storage
}

func lastReadIndexHeartbeat(
	t *testing.T, messages []*pb.Message, to uint64,
) *pb.Message {
	t.Helper()
	for i := len(messages) - 1; i >= 0; i-- {
		message := messages[i]
		if message.GetType() == pb.MsgHeartbeat &&
			message.GetTo() == to &&
			len(message.GetContext()) != 0 {
			return message
		}
	}
	t.Fatalf("no read-index heartbeat to %d in %+v", to, messages)
	return nil
}

func TestRawNodeReadIndexReadyAndAppliedBarrierParity(t *testing.T) {
	rn, storage := newReadIndexParityNodeWithVoters(
		t, 1, ReadOnlySafe, false, []uint64{1})
	if err := rn.Campaign(); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 4 && rn.HasReady(); i++ {
		persistReadIndexReady(t, rn, storage, rn.Ready())
	}
	if status := rn.BasicStatus(); status.RaftState != StateLeader {
		t.Fatalf("singleton status = %+v", status)
	}

	if err := rn.Propose([]byte("write")); err != nil {
		t.Fatal(err)
	}
	requestContext := []byte("barrier")
	rn.ReadIndex(requestContext)

	rd := rn.readyWithoutAccept()
	if len(rd.ReadStates) != 1 {
		t.Fatalf("ReadStates = %+v", rd.ReadStates)
	}
	readState := rd.ReadStates[0]
	if !bytes.Equal(readState.RequestCtx, []byte("barrier")) {
		t.Fatalf("request context = %q", readState.RequestCtx)
	}
	if got := rn.BasicStatus().Applied; got > readState.Index {
		t.Fatalf("applied before Ready acceptance = %d, read index = %d",
			got, readState.Index)
	}
	if !rn.HasReady() {
		t.Fatal("read state drained by Ready preview")
	}
	if err := storage.Append(rd.Entries); err != nil {
		t.Fatal(err)
	}
	if !IsEmptyHardState(rd.HardState) {
		if err := storage.SetHardState(rd.HardState); err != nil {
			t.Fatal(err)
		}
	}
	rn.acceptReady(rd)
	if rn.HasReady() {
		t.Fatal("accepted read state remained Ready")
	}
	rn.Advance(rd)
	if got := rn.BasicStatus().Applied; got < readState.Index {
		t.Fatalf("applied after Advance = %d, read index = %d",
			got, readState.Index)
	}
}

func TestRawNodeReadIndexNilEmptyContextParity(t *testing.T) {
	rn, storage := newReadIndexParityNodeWithVoters(
		t, 1, ReadOnlySafe, false, []uint64{1})
	if err := rn.Campaign(); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 4 && rn.HasReady(); i++ {
		persistReadIndexReady(t, rn, storage, rn.Ready())
	}

	rn.ReadIndex(nil)
	nilReady := rn.Ready()
	if len(nilReady.ReadStates) != 1 ||
		nilReady.ReadStates[0].RequestCtx != nil {
		t.Fatalf("nil ReadStates = %+v", nilReady.ReadStates)
	}
	persistReadIndexReady(t, rn, storage, nilReady)

	rn.ReadIndex([]byte{})
	emptyReady := rn.Ready()
	if len(emptyReady.ReadStates) != 1 ||
		emptyReady.ReadStates[0].RequestCtx == nil ||
		len(emptyReady.ReadStates[0].RequestCtx) != 0 {
		t.Fatalf("present-empty ReadStates = %+v", emptyReady.ReadStates)
	}
	persistReadIndexReady(t, rn, storage, emptyReady)
}

func TestRawNodeSafeReadIndexQuorumAndDuplicateContextParity(t *testing.T) {
	rn, storage := prepareReadIndexParityLeader(
		t, ReadOnlySafe, false)
	requestContext := []byte("duplicate")
	rn.ReadIndex(requestContext)
	rn.ReadIndex(requestContext)

	heartbeatReady := rn.Ready()
	if len(heartbeatReady.ReadStates) != 0 {
		t.Fatalf("ReadStates returned before quorum: %+v",
			heartbeatReady.ReadStates)
	}
	heartbeat := lastReadIndexHeartbeat(
		t, heartbeatReady.Messages, 2)
	if len(heartbeat.GetContext()) != 8 {
		t.Fatalf("heartbeat context = %x", heartbeat.GetContext())
	}
	context := append([]byte(nil), heartbeat.GetContext()...)
	persistReadIndexReady(t, rn, storage, heartbeatReady)

	if err := rn.Step(&pb.Message{
		Type:    pb.MsgHeartbeatResp.Enum(),
		To:      new(uint64(1)),
		From:    new(uint64(2)),
		Term:    new(uint64(2)),
		Context: context,
	}); err != nil {
		t.Fatal(err)
	}
	readReady := rn.Ready()
	if len(readReady.ReadStates) != 2 {
		t.Fatalf("ReadStates = %+v, want duplicate requests", readReady.ReadStates)
	}
	for _, state := range readReady.ReadStates {
		if state.Index != 2 ||
			!bytes.Equal(state.RequestCtx, []byte("duplicate")) {
			t.Fatalf("ReadState = %+v", state)
		}
	}
	persistReadIndexReady(t, rn, storage, readReady)
}

func TestRawNodeReadIndexWaitsForCurrentTermCommitParity(t *testing.T) {
	rn, storage := campaignReadIndexParityLeader(
		t, ReadOnlySafe, false)
	rn.ReadIndex([]byte("postponed"))
	if rn.HasReady() {
		rd := rn.Ready()
		t.Fatalf("new leader returned work before current-term commit: %+v", rd)
	}

	if err := rn.Step(&pb.Message{
		Type:  pb.MsgAppResp.Enum(),
		To:    new(uint64(1)),
		From:  new(uint64(2)),
		Term:  new(uint64(2)),
		Index: new(uint64(2)),
	}); err != nil {
		t.Fatal(err)
	}
	heartbeatReady := rn.Ready()
	if len(heartbeatReady.ReadStates) != 0 {
		t.Fatalf("postponed read returned before heartbeat quorum: %+v",
			heartbeatReady.ReadStates)
	}
	heartbeat := lastReadIndexHeartbeat(
		t, heartbeatReady.Messages, 2)
	heartbeatContext := append([]byte(nil), heartbeat.GetContext()...)
	persistReadIndexReady(t, rn, storage, heartbeatReady)
	if err := rn.Step(&pb.Message{
		Type:    pb.MsgHeartbeatResp.Enum(),
		To:      new(uint64(1)),
		From:    new(uint64(2)),
		Term:    new(uint64(2)),
		Context: heartbeatContext,
	}); err != nil {
		t.Fatal(err)
	}
	readReady := rn.Ready()
	if len(readReady.ReadStates) != 1 ||
		readReady.ReadStates[0].Index != 2 ||
		!bytes.Equal(
			readReady.ReadStates[0].RequestCtx, []byte("postponed")) {
		t.Fatalf("postponed ReadStates = %+v", readReady.ReadStates)
	}
	persistReadIndexReady(t, rn, storage, readReady)
}

func TestRawNodeFollowerReadIndexForwardingParity(t *testing.T) {
	leader, leaderStorage := prepareReadIndexParityLeader(
		t, ReadOnlySafe, false)
	follower, followerStorage := newReadIndexParityNode(
		t, 2, ReadOnlySafe, false)

	if err := follower.Step(&pb.Message{
		Type:   pb.MsgHeartbeat.Enum(),
		To:     new(uint64(2)),
		From:   new(uint64(1)),
		Term:   new(uint64(2)),
		Commit: new(uint64(1)),
	}); err != nil {
		t.Fatal(err)
	}
	persistReadIndexReady(t, follower, followerStorage, follower.Ready())

	context := []byte("follower")
	follower.ReadIndex(context)
	forwardReady := follower.Ready()
	var forwarded *pb.Message
	for _, message := range forwardReady.Messages {
		if message.GetType() == pb.MsgReadIndex {
			forwarded = message
		}
	}
	if forwarded == nil || forwarded.GetTo() != 1 ||
		forwarded.GetFrom() != 2 || forwarded.GetTerm() != 0 ||
		!bytes.Equal(forwarded.GetEntries()[0].GetData(), []byte("follower")) {
		t.Fatalf("forwarded ReadIndex = %+v", forwarded)
	}
	persistReadIndexReady(t, follower, followerStorage, forwardReady)
	if err := leader.Step(forwarded); err != nil {
		t.Fatal(err)
	}

	heartbeatReady := leader.Ready()
	heartbeat := lastReadIndexHeartbeat(
		t, heartbeatReady.Messages, 3)
	heartbeatContext := append([]byte(nil), heartbeat.GetContext()...)
	persistReadIndexReady(t, leader, leaderStorage, heartbeatReady)
	if err := leader.Step(&pb.Message{
		Type:    pb.MsgHeartbeatResp.Enum(),
		To:      new(uint64(1)),
		From:    new(uint64(3)),
		Term:    new(uint64(2)),
		Context: heartbeatContext,
	}); err != nil {
		t.Fatal(err)
	}
	responseReady := leader.Ready()
	var response *pb.Message
	for _, message := range responseReady.Messages {
		if message.GetType() == pb.MsgReadIndexResp &&
			message.GetTo() == 2 {
			response = message
		}
	}
	if response == nil || response.GetIndex() != 2 ||
		!bytes.Equal(response.GetEntries()[0].GetData(), []byte("follower")) {
		t.Fatalf("ReadIndex response = %+v", response)
	}
	persistReadIndexReady(t, leader, leaderStorage, responseReady)
	if err := follower.Step(response); err != nil {
		t.Fatal(err)
	}
	readReady := follower.Ready()
	if len(readReady.ReadStates) != 1 ||
		readReady.ReadStates[0].Index != 2 ||
		!bytes.Equal(
			readReady.ReadStates[0].RequestCtx, []byte("follower")) {
		t.Fatalf("follower ReadStates = %+v", readReady.ReadStates)
	}
	persistReadIndexReady(t, follower, followerStorage, readReady)
}

func TestRawNodeReadIndexLostOnLeaderStepdownParity(t *testing.T) {
	rn, storage := prepareReadIndexParityLeader(
		t, ReadOnlySafe, false)
	rn.ReadIndex([]byte("lost"))
	heartbeatReady := rn.Ready()
	heartbeat := lastReadIndexHeartbeat(
		t, heartbeatReady.Messages, 2)
	delayedContext := append([]byte(nil), heartbeat.GetContext()...)
	persistReadIndexReady(t, rn, storage, heartbeatReady)

	if err := rn.Step(&pb.Message{
		Type: pb.MsgHeartbeat.Enum(),
		To:   new(uint64(1)),
		From: new(uint64(3)),
		Term: new(uint64(3)),
	}); err != nil {
		t.Fatal(err)
	}
	stepdownReady := rn.Ready()
	persistReadIndexReady(t, rn, storage, stepdownReady)
	if err := rn.Step(&pb.Message{
		Type:    pb.MsgHeartbeatResp.Enum(),
		To:      new(uint64(1)),
		From:    new(uint64(2)),
		Term:    new(uint64(2)),
		Context: delayedContext,
	}); err != nil {
		t.Fatal(err)
	}
	if rn.HasReady() {
		rd := rn.Ready()
		if len(rd.ReadStates) != 0 {
			t.Fatalf("lost request produced ReadStates: %+v", rd.ReadStates)
		}
		persistReadIndexReady(t, rn, storage, rd)
	}
}

func TestRawNodeLeaseReadAndCheckQuorumParity(t *testing.T) {
	rn, storage := prepareReadIndexParityLeader(
		t, ReadOnlyLeaseBased, true)
	rn.ReadIndex([]byte("lease"))
	rd := rn.Ready()
	if len(rd.ReadStates) != 1 ||
		rd.ReadStates[0].Index != 2 ||
		!bytes.Equal(rd.ReadStates[0].RequestCtx, []byte("lease")) {
		t.Fatalf("lease ReadStates = %+v", rd.ReadStates)
	}
	for _, message := range rd.Messages {
		if message.GetType() == pb.MsgHeartbeat &&
			len(message.GetContext()) != 0 {
			t.Fatalf("lease read emitted safe-read heartbeat: %+v", message)
		}
	}
	persistReadIndexReady(t, rn, storage, rd)

	// The no-op acknowledgement counts as activity in the first interval.
	// With no further peer activity, CheckQuorum steps down in the second.
	for i := 0; i < 20; i++ {
		rn.Tick()
	}
	if status := rn.BasicStatus(); status.RaftState != StateFollower ||
		status.Lead != None {
		t.Fatalf("status after inactive lease = %+v", status)
	}
	rn.ReadIndex([]byte("dropped"))
	if rn.HasReady() {
		rd = rn.Ready()
		if len(rd.ReadStates) != 0 {
			t.Fatalf("leaderless lease read returned %+v", rd.ReadStates)
		}
		persistReadIndexReady(t, rn, storage, rd)
	}
}

func TestRawNodeCheckQuorumFollowerLeaseParity(t *testing.T) {
	rn, storage := newReadIndexParityNode(
		t, 2, ReadOnlyLeaseBased, true)
	if err := rn.Step(&pb.Message{
		Type: pb.MsgHeartbeat.Enum(),
		To:   new(uint64(2)),
		From: new(uint64(1)),
		Term: new(uint64(1)),
	}); err != nil {
		t.Fatal(err)
	}
	persistReadIndexReady(t, rn, storage, rn.Ready())
	if err := rn.ForgetLeader(); err != nil {
		t.Fatal(err)
	}
	if lead := rn.BasicStatus().Lead; lead != 1 {
		t.Fatalf("lease follower forgot leader %d", lead)
	}

	vote := &pb.Message{
		Type:    pb.MsgVote.Enum(),
		To:      new(uint64(2)),
		From:    new(uint64(3)),
		Term:    new(uint64(2)),
		Index:   new(uint64(1)),
		LogTerm: new(uint64(1)),
	}
	if err := rn.Step(vote); err != nil {
		t.Fatal(err)
	}
	if status := rn.BasicStatus(); status.HardState.GetTerm() != 1 ||
		status.HardState.GetVote() != None ||
		status.Lead != 1 {
		t.Fatalf("vote was not suppressed during lease: %+v", status)
	}
	for i := 0; i < 10; i++ {
		rn.TickQuiesced()
	}
	if err := rn.Step(vote); err != nil {
		t.Fatal(err)
	}
	if status := rn.BasicStatus(); status.HardState.GetTerm() != 2 ||
		status.HardState.GetVote() != 3 ||
		status.Lead != None {
		t.Fatalf("vote after lease expiration = %+v", status)
	}
}
