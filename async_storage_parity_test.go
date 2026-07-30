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
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	pb "go.etcd.io/raft/v3/raftpb"
)

func newAsyncStorageParityNode(
	t *testing.T, storage Storage, applied uint64,
) *RawNode {
	return newAsyncStorageParityNodeWithMax(
		t, storage, applied, 1024)
}

func newAsyncStorageParityNodeWithMax(
	t *testing.T,
	storage Storage,
	applied uint64,
	maxCommittedSize uint64,
) *RawNode {
	t.Helper()
	rn, err := NewRawNode(&Config{
		ID:                       1,
		ElectionTick:             10,
		HeartbeatTick:            1,
		Storage:                  storage,
		Applied:                  applied,
		AsyncStorageWrites:       true,
		MaxSizePerMsg:            1024,
		MaxCommittedSizePerReady: maxCommittedSize,
		MaxInflightMsgs:          16,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !rn.AsyncStorageWritesEnabled() {
		t.Fatal("AsyncStorageWritesEnabled returned false")
	}
	return rn
}

func findAsyncStorageMessage(
	t *testing.T, rd Ready, typ pb.MessageType,
) *pb.Message {
	t.Helper()
	var found *pb.Message
	for _, message := range rd.Messages {
		if message.GetType() != typ {
			continue
		}
		if found != nil {
			t.Fatalf("multiple %s messages in %+v", typ, rd.Messages)
		}
		found = message
	}
	if found == nil {
		t.Fatalf("no %s message in %+v", typ, rd.Messages)
	}
	return found
}

func persistAsyncAppend(
	t *testing.T, storage *MemoryStorage, message *pb.Message,
) {
	t.Helper()
	if message.GetType() != pb.MsgStorageAppend ||
		message.GetTo() != LocalAppendThread {
		t.Fatalf("not local append work: %+v", message)
	}
	hardState := &pb.HardState{
		Term:   new(message.GetTerm()),
		Vote:   new(message.GetVote()),
		Commit: new(message.GetCommit()),
	}
	if !IsEmptyHardState(hardState) {
		if err := storage.SetHardState(hardState); err != nil {
			t.Fatal(err)
		}
	}
	if !IsEmptySnap(message.GetSnapshot()) {
		if len(message.GetEntries()) != 0 {
			t.Fatalf("append work combines snapshot and entries: %+v", message)
		}
		if err := storage.ApplySnapshot(message.GetSnapshot()); err != nil {
			t.Fatal(err)
		}
		return
	}
	if err := storage.Append(message.GetEntries()); err != nil {
		t.Fatal(err)
	}
}

func routeAsyncResponses(
	t *testing.T, rn *RawNode, message *pb.Message,
) []*pb.Message {
	t.Helper()
	var remote []*pb.Message
	for _, response := range message.GetResponses() {
		if response.GetTo() != rn.ID() {
			remote = append(remote, response)
			continue
		}
		if err := rn.Step(response); err != nil {
			t.Fatalf("step response %+v: %v", response, err)
		}
	}
	return remote
}

func completeAsyncAppend(
	t *testing.T,
	rn *RawNode,
	storage *MemoryStorage,
	message *pb.Message,
) []*pb.Message {
	t.Helper()
	persistAsyncAppend(t, storage, message)
	return routeAsyncResponses(t, rn, message)
}

func completeAsyncApply(
	t *testing.T, rn *RawNode, message *pb.Message,
) {
	t.Helper()
	if message.GetType() != pb.MsgStorageApply ||
		message.GetTo() != LocalApplyThread {
		t.Fatalf("not local apply work: %+v", message)
	}
	if got := routeAsyncResponses(t, rn, message); len(got) != 0 {
		t.Fatalf("apply work produced remote responses: %+v", got)
	}
}

func TestAsyncStorageWritesProtocolParity(t *testing.T) {
	storage := NewMemoryStorage()
	rn := newAsyncStorageParityNode(t, storage, 0)
	if err := rn.Bootstrap([]Peer{{ID: 1}}); err != nil {
		t.Fatal(err)
	}

	bootstrapReady := rn.Ready()
	if len(bootstrapReady.Entries) != 1 ||
		len(bootstrapReady.CommittedEntries) != 0 ||
		!bootstrapReady.MustSync {
		t.Fatalf("bootstrap Ready entries=%d committed=%d",
			len(bootstrapReady.Entries),
			len(bootstrapReady.CommittedEntries))
	}
	bootstrapAppend := findAsyncStorageMessage(
		t, bootstrapReady, pb.MsgStorageAppend)
	if bootstrapAppend.GetTo() != LocalAppendThread ||
		!IsLocalMsgTarget(bootstrapAppend.GetTo()) {
		t.Fatalf("bootstrap append target = %d", bootstrapAppend.GetTo())
	}
	if err := rn.Step(bootstrapAppend); !errors.Is(err, ErrStepLocalMsg) {
		t.Fatalf("stepping local work = %v, want ErrStepLocalMsg", err)
	}
	if got := completeAsyncAppend(
		t, rn, storage, bootstrapAppend); len(got) != 0 {
		t.Fatalf("bootstrap append remote responses = %+v", got)
	}

	bootstrapApplyReady := rn.Ready()
	if len(bootstrapApplyReady.Entries) != 0 ||
		len(bootstrapApplyReady.CommittedEntries) != 1 {
		t.Fatalf("bootstrap apply Ready entries=%d committed=%d",
			len(bootstrapApplyReady.Entries),
			len(bootstrapApplyReady.CommittedEntries))
	}
	bootstrapApply := findAsyncStorageMessage(
		t, bootstrapApplyReady, pb.MsgStorageApply)
	completeAsyncApply(t, rn, bootstrapApply)
	if got := rn.BasicStatus().Applied; got != 1 {
		t.Fatalf("applied after bootstrap = %d, want 1", got)
	}

	if err := rn.Campaign(); err != nil {
		t.Fatal(err)
	}
	campaignReady := rn.Ready()
	if campaignReady.SoftState == nil ||
		campaignReady.SoftState.RaftState != StateCandidate {
		t.Fatalf("campaign soft state = %+v", campaignReady.SoftState)
	}
	if !campaignReady.MustSync {
		t.Fatal("campaign Ready does not require sync")
	}
	campaignAppend := findAsyncStorageMessage(
		t, campaignReady, pb.MsgStorageAppend)
	if campaignAppend.Term == nil || campaignAppend.Vote == nil ||
		campaignAppend.Commit == nil {
		t.Fatalf("campaign append HardState fields are absent: %+v",
			campaignAppend)
	}
	if got := len(campaignAppend.GetResponses()); got != 1 ||
		campaignAppend.GetResponses()[0].GetType() != pb.MsgVoteResp {
		t.Fatalf("campaign responses = %+v", campaignAppend.GetResponses())
	}
	if got := rn.BasicStatus().RaftState; got != StateCandidate {
		t.Fatalf("state before campaign durability = %v", got)
	}
	completeAsyncAppend(t, rn, storage, campaignAppend)
	if got := rn.BasicStatus().RaftState; got != StateLeader {
		t.Fatalf("state after campaign durability = %v", got)
	}

	leaderReady := rn.Ready()
	if len(leaderReady.Entries) != 1 ||
		len(leaderReady.CommittedEntries) != 0 ||
		!leaderReady.MustSync {
		t.Fatalf("leader Ready entries=%d committed=%d",
			len(leaderReady.Entries), len(leaderReady.CommittedEntries))
	}
	leaderAppend := findAsyncStorageMessage(
		t, leaderReady, pb.MsgStorageAppend)
	if got := leaderAppend.GetResponses(); len(got) != 2 ||
		got[0].GetType() != pb.MsgAppResp ||
		got[1].GetType() != pb.MsgStorageAppendResp ||
		got[1].GetIndex() != leaderReady.Entries[0].GetIndex() {
		t.Fatalf("leader append responses = %+v", got)
	}
	completeAsyncAppend(t, rn, storage, leaderAppend)

	commitReady := rn.Ready()
	if len(commitReady.CommittedEntries) != 1 {
		t.Fatalf("committed no-op entries = %+v", commitReady.CommittedEntries)
	}
	if commitReady.MustSync {
		t.Fatal("commit-only Ready unexpectedly requires sync")
	}
	commitAppend := findAsyncStorageMessage(
		t, commitReady, pb.MsgStorageAppend)
	commitApply := findAsyncStorageMessage(
		t, commitReady, pb.MsgStorageApply)
	if commitReady.Messages[len(commitReady.Messages)-2].GetTo() !=
		LocalAppendThread ||
		commitReady.Messages[len(commitReady.Messages)-1].GetTo() !=
			LocalApplyThread {
		t.Fatalf("local message order = %+v", commitReady.Messages)
	}
	// Different local targets may complete independently.
	completeAsyncApply(t, rn, commitApply)
	if got := rn.BasicStatus().Applied; got != 2 {
		t.Fatalf("applied after no-op = %d, want 2", got)
	}
	completeAsyncAppend(t, rn, storage, commitAppend)

	rn.ReadIndex([]byte("read"))
	readReady := rn.Ready()
	if len(readReady.ReadStates) != 1 ||
		readReady.ReadStates[0].Index != 2 ||
		string(readReady.ReadStates[0].RequestCtx) != "read" {
		t.Fatalf("async ReadIndex Ready = %+v", readReady)
	}

	if err := rn.Propose([]byte("first")); err != nil {
		t.Fatal(err)
	}
	firstReady := rn.Ready()
	firstAppend := findAsyncStorageMessage(
		t, firstReady, pb.MsgStorageAppend)
	if err := rn.Propose([]byte("second")); err != nil {
		t.Fatal(err)
	}
	secondReady := rn.Ready()
	secondAppend := findAsyncStorageMessage(
		t, secondReady, pb.MsgStorageAppend)
	if len(firstAppend.GetEntries()) != 1 ||
		len(secondAppend.GetEntries()) != 1 ||
		firstAppend.GetEntries()[0].GetIndex()+1 !=
			secondAppend.GetEntries()[0].GetIndex() {
		t.Fatalf("pipelined append entries first=%+v second=%+v",
			firstAppend.GetEntries(), secondAppend.GetEntries())
	}
	firstAck := firstAppend.GetResponses()[len(firstAppend.GetResponses())-1]
	secondAck := secondAppend.GetResponses()[len(secondAppend.GetResponses())-1]
	if firstAck.GetType() != pb.MsgStorageAppendResp ||
		secondAck.GetType() != pb.MsgStorageAppendResp ||
		firstAck.GetIndex()+1 != secondAck.GetIndex() {
		t.Fatalf("pipelined acknowledgements first=%+v second=%+v",
			firstAck, secondAck)
	}
	completeAsyncAppend(t, rn, storage, firstAppend)
	completeAsyncAppend(t, rn, storage, secondAppend)

	proposalCommitReady := rn.Ready()
	if got := len(proposalCommitReady.CommittedEntries); got != 2 {
		t.Fatalf("committed proposal count = %d", got)
	}
	proposalCommitAppend := findAsyncStorageMessage(
		t, proposalCommitReady, pb.MsgStorageAppend)
	proposalCommitApply := findAsyncStorageMessage(
		t, proposalCommitReady, pb.MsgStorageApply)
	completeAsyncAppend(t, rn, storage, proposalCommitAppend)
	completeAsyncApply(t, rn, proposalCommitApply)
	if got := rn.BasicStatus().Applied; got != 4 {
		t.Fatalf("applied after proposals = %d, want 4", got)
	}

	confChange := &pb.ConfChange{
		Type:   pb.ConfChangeAddNode.Enum(),
		NodeId: new(uint64(2)),
	}
	if err := rn.ProposeConfChange(confChange); err != nil {
		t.Fatal(err)
	}
	confAppendReady := rn.Ready()
	completeAsyncAppend(
		t, rn, storage,
		findAsyncStorageMessage(
			t, confAppendReady, pb.MsgStorageAppend))
	confCommitReady := rn.Ready()
	confCommitAppend := findAsyncStorageMessage(
		t, confCommitReady, pb.MsgStorageAppend)
	confCommitApply := findAsyncStorageMessage(
		t, confCommitReady, pb.MsgStorageApply)
	if len(confCommitApply.GetEntries()) != 1 ||
		confCommitApply.GetEntries()[0].GetType() != pb.EntryConfChange {
		t.Fatalf("configuration apply work = %+v", confCommitApply)
	}
	state := rn.ApplyConfChange(confChange)
	if !reflect.DeepEqual(state.Voters, []uint64{1, 2}) {
		t.Fatalf("configuration state = %+v", state)
	}
	completeAsyncAppend(t, rn, storage, confCommitAppend)
	completeAsyncApply(t, rn, confCommitApply)
	if got := rn.BasicStatus().Applied; got != 5 {
		t.Fatalf("applied after configuration = %d, want 5", got)
	}
}

func TestAsyncStorageWritesSnapshotAndTermChangeParity(t *testing.T) {
	storage := NewMemoryStorage()
	initial := &pb.Snapshot{
		Metadata: &pb.SnapshotMetadata{
			Index:     new(uint64(1)),
			Term:      new(uint64(1)),
			ConfState: &pb.ConfState{Voters: []uint64{1, 2}},
		},
	}
	if err := storage.ApplySnapshot(initial); err != nil {
		t.Fatal(err)
	}
	if err := storage.SetHardState(&pb.HardState{
		Term:   new(uint64(1)),
		Commit: new(uint64(1)),
	}); err != nil {
		t.Fatal(err)
	}
	rn := newAsyncStorageParityNode(t, storage, 1)
	incomingSnapshot := &pb.Snapshot{
		Data: []byte("snapshot-owned"),
		Metadata: &pb.SnapshotMetadata{
			Index:     new(uint64(5)),
			Term:      new(uint64(2)),
			ConfState: &pb.ConfState{Voters: []uint64{1, 2}},
		},
	}
	if err := rn.Step(&pb.Message{
		Type:     pb.MsgSnap.Enum(),
		To:       new(uint64(1)),
		From:     new(uint64(2)),
		Term:     new(uint64(2)),
		Snapshot: incomingSnapshot,
	}); err != nil {
		t.Fatal(err)
	}
	snapshotReady := rn.Ready()
	snapshotAppend := findAsyncStorageMessage(
		t, snapshotReady, pb.MsgStorageAppend)
	incomingSnapshot.Data[0] = 'X'
	if string(snapshotAppend.GetSnapshot().Data) != "snapshot-owned" {
		t.Fatalf("snapshot append did not own data: %q",
			snapshotAppend.GetSnapshot().Data)
	}
	responses := snapshotAppend.GetResponses()
	if len(responses) != 2 ||
		responses[0].GetType() != pb.MsgAppResp ||
		responses[0].GetTo() != 2 ||
		responses[1].GetType() != pb.MsgStorageAppendResp ||
		responses[1].GetFrom() != LocalAppendThread ||
		responses[1].GetSnapshot() == nil {
		t.Fatalf("snapshot append responses = %+v", responses)
	}
	if got := rn.BasicStatus().Applied; got != 1 {
		t.Fatalf("applied before snapshot completion = %d", got)
	}

	if err := rn.Step(&pb.Message{
		Type:   pb.MsgHeartbeat.Enum(),
		To:     new(uint64(1)),
		From:   new(uint64(2)),
		Term:   new(uint64(3)),
		Commit: new(uint64(5)),
	}); err != nil {
		t.Fatal(err)
	}
	higherTermReady := rn.Ready()
	higherAppend := findAsyncStorageMessage(
		t, higherTermReady, pb.MsgStorageAppend)
	if got := rn.BasicStatus(); got.HardState.GetTerm() != 3 ||
		got.Applied != 1 {
		t.Fatalf("status before old snapshot response = %+v", got)
	}

	remote := completeAsyncAppend(
		t, rn, storage, snapshotAppend)
	if len(remote) != 1 ||
		remote[0].GetType() != pb.MsgAppResp ||
		remote[0].GetTo() != 2 {
		t.Fatalf("snapshot remote routing = %+v", remote)
	}
	if got := rn.BasicStatus().Applied; got != 5 {
		t.Fatalf("lower-term snapshot response applied = %d, want 5", got)
	}
	completeAsyncAppend(t, rn, storage, higherAppend)
	if got := storage.hardState.GetTerm(); got != 3 {
		t.Fatalf("persisted term = %d, want 3", got)
	}
}

func TestAsyncStorageWritesHardStatePresenceParity(t *testing.T) {
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
	if err := storage.SetHardState(&pb.HardState{
		Term:   new(uint64(1)),
		Commit: new(uint64(1)),
	}); err != nil {
		t.Fatal(err)
	}
	rn := newAsyncStorageParityNode(t, storage, 1)
	if err := rn.Step(&pb.Message{
		Type:    pb.MsgApp.Enum(),
		To:      new(uint64(1)),
		From:    new(uint64(2)),
		Term:    new(uint64(1)),
		LogTerm: new(uint64(1)),
		Index:   new(uint64(1)),
		Commit:  new(uint64(1)),
	}); err != nil {
		t.Fatal(err)
	}
	rd := rn.Ready()
	if !IsEmptyHardState(rd.HardState) {
		t.Fatalf("unexpected HardState update: %+v", rd.HardState)
	}
	appendWork := findAsyncStorageMessage(
		t, rd, pb.MsgStorageAppend)
	if appendWork.Term != nil || appendWork.Vote != nil ||
		appendWork.Commit != nil {
		t.Fatalf("empty append HardState fields are present: %+v",
			appendWork)
	}
	if len(appendWork.GetResponses()) != 1 ||
		appendWork.GetResponses()[0].GetType() != pb.MsgAppResp {
		t.Fatalf("barrier-only append responses = %+v",
			appendWork.GetResponses())
	}
	if remote := completeAsyncAppend(
		t, rn, storage, appendWork); len(remote) != 1 ||
		remote[0].GetTo() != 2 {
		t.Fatalf("barrier-only append routing = %+v", remote)
	}
}

func TestAsyncStorageWritesAdvancePanicsParity(t *testing.T) {
	rn := newAsyncStorageParityNode(t, NewMemoryStorage(), 0)
	defer func() {
		if recover() == nil {
			t.Fatal("Advance did not panic with AsyncStorageWrites")
		}
	}()
	rn.Advance(Ready{})
}

func TestAsyncStorageWritesEntryAcknowledgementTermParity(t *testing.T) {
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
	if err := storage.SetHardState(&pb.HardState{
		Term:   new(uint64(1)),
		Commit: new(uint64(1)),
	}); err != nil {
		t.Fatal(err)
	}
	rn := newAsyncStorageParityNode(t, storage, 1)
	if err := rn.Step(&pb.Message{
		Type:    pb.MsgApp.Enum(),
		To:      new(uint64(1)),
		From:    new(uint64(2)),
		Term:    new(uint64(2)),
		LogTerm: new(uint64(1)),
		Index:   new(uint64(1)),
		Commit:  new(uint64(1)),
		Entries: []*pb.Entry{{
			Type:  pb.EntryNormal.Enum(),
			Term:  new(uint64(2)),
			Index: new(uint64(2)),
			Data:  []byte("old"),
		}},
	}); err != nil {
		t.Fatal(err)
	}
	oldReady := rn.Ready()
	oldAppend := findAsyncStorageMessage(
		t, oldReady, pb.MsgStorageAppend)
	oldAck := oldAppend.GetResponses()[len(oldAppend.GetResponses())-1]
	if oldAck.GetType() != pb.MsgStorageAppendResp ||
		oldAck.GetTerm() != 2 || oldAck.GetIndex() != 2 ||
		oldAck.GetLogTerm() != 2 {
		t.Fatalf("old append acknowledgement = %+v", oldAck)
	}

	if err := rn.Step(&pb.Message{
		Type:   pb.MsgHeartbeat.Enum(),
		To:     new(uint64(1)),
		From:   new(uint64(2)),
		Term:   new(uint64(3)),
		Commit: new(uint64(1)),
	}); err != nil {
		t.Fatal(err)
	}
	newReady := rn.Ready()
	newAppend := findAsyncStorageMessage(
		t, newReady, pb.MsgStorageAppend)
	newAck := newAppend.GetResponses()[len(newAppend.GetResponses())-1]
	if newAck.GetType() != pb.MsgStorageAppendResp ||
		newAck.GetTerm() != 3 || newAck.GetIndex() != 2 ||
		newAck.GetLogTerm() != 2 {
		t.Fatalf("reattesting append acknowledgement = %+v", newAck)
	}
	if reflect.DeepEqual(oldAck, newAck) {
		t.Fatal("term change did not produce a new append acknowledgement")
	}

	completeAsyncAppend(t, rn, storage, oldAppend)
	completeAsyncAppend(t, rn, storage, newAppend)
}

func asyncNodeReady(t *testing.T, node Node) Ready {
	t.Helper()
	select {
	case ready := <-node.Ready():
		return ready
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for async Node Ready")
		return Ready{}
	}
}

func routeAsyncNodeResponses(
	t *testing.T, ctx context.Context, node Node, message *pb.Message,
) {
	t.Helper()
	for _, response := range message.GetResponses() {
		if response.GetTo() != 1 {
			t.Fatalf("unexpected remote response in single-node test: %+v",
				response)
		}
		if err := node.Step(ctx, response); err != nil {
			t.Fatalf("step Node response %+v: %v", response, err)
		}
	}
}

func TestAsyncStorageWritesNodeActorParity(t *testing.T) {
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
	node := RestartNode(&Config{
		ID:                       1,
		ElectionTick:             10,
		HeartbeatTick:            1,
		Storage:                  storage,
		Applied:                  1,
		AsyncStorageWrites:       true,
		MaxSizePerMsg:            1024,
		MaxCommittedSizePerReady: 1024,
		MaxInflightMsgs:          16,
	})
	defer node.Stop()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if err := node.Campaign(ctx); err != nil {
		t.Fatal(err)
	}

	campaignReady := asyncNodeReady(t, node)
	campaignAppend := findAsyncStorageMessage(
		t, campaignReady, pb.MsgStorageAppend)
	persistAsyncAppend(t, storage, campaignAppend)
	routeAsyncNodeResponses(t, ctx, node, campaignAppend)

	leaderReady := asyncNodeReady(t, node)
	leaderAppend := findAsyncStorageMessage(
		t, leaderReady, pb.MsgStorageAppend)
	if len(leaderReady.Entries) != 1 {
		t.Fatalf("leader Node Ready entries = %+v", leaderReady.Entries)
	}
	persistAsyncAppend(t, storage, leaderAppend)
	routeAsyncNodeResponses(t, ctx, node, leaderAppend)

	commitReady := asyncNodeReady(t, node)
	commitAppend := findAsyncStorageMessage(
		t, commitReady, pb.MsgStorageAppend)
	commitApply := findAsyncStorageMessage(
		t, commitReady, pb.MsgStorageApply)
	if len(commitReady.CommittedEntries) != 1 {
		t.Fatalf("Node committed entries = %+v", commitReady.CommittedEntries)
	}
	persistAsyncAppend(t, storage, commitAppend)
	routeAsyncNodeResponses(t, ctx, node, commitAppend)
	routeAsyncNodeResponses(t, ctx, node, commitApply)
	if got := node.Status().Applied; got != 2 {
		t.Fatalf("Node applied = %d, want 2", got)
	}
}

func TestAsyncStorageWritesApplyPaginationParity(t *testing.T) {
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
	rn := newAsyncStorageParityNodeWithMax(
		t, storage, 1, 2048)
	if err := rn.Campaign(); err != nil {
		t.Fatal(err)
	}
	campaignAppend := findAsyncStorageMessage(
		t, rn.Ready(), pb.MsgStorageAppend)
	completeAsyncAppend(t, rn, storage, campaignAppend)
	leaderAppend := findAsyncStorageMessage(
		t, rn.Ready(), pb.MsgStorageAppend)
	completeAsyncAppend(t, rn, storage, leaderAppend)
	noopCommit := rn.Ready()
	completeAsyncAppend(
		t, rn, storage,
		findAsyncStorageMessage(t, noopCommit, pb.MsgStorageAppend))
	completeAsyncApply(
		t, rn,
		findAsyncStorageMessage(t, noopCommit, pb.MsgStorageApply))

	blob := make([]byte, 1024)
	var pendingApply []*pb.Message
	for i := 0; i < 3; i++ {
		blob[0] = byte(i + 1)
		if err := rn.Propose(append([]byte(nil), blob...)); err != nil {
			t.Fatal(err)
		}
		appendReady := rn.Ready()
		completeAsyncAppend(
			t, rn, storage,
			findAsyncStorageMessage(
				t, appendReady, pb.MsgStorageAppend))

		commitReady := rn.Ready()
		var appendWork *pb.Message
		var applyWork *pb.Message
		for _, message := range commitReady.Messages {
			switch message.GetType() {
			case pb.MsgStorageAppend:
				appendWork = message
			case pb.MsgStorageApply:
				applyWork = message
			}
		}
		if appendWork != nil {
			completeAsyncAppend(t, rn, storage, appendWork)
		}
		if i < 2 {
			if applyWork == nil ||
				len(applyWork.GetEntries()) != 1 {
				t.Fatalf("proposal %d apply work = %+v", i, applyWork)
			}
			pendingApply = append(pendingApply, applyWork)
		} else if applyWork != nil {
			t.Fatalf("third apply escaped outstanding quota: %+v",
				applyWork)
		}
	}
	if rn.HasReady() {
		t.Fatal("third apply became ready before prior apply completion")
	}

	completeAsyncApply(t, rn, pendingApply[0])
	thirdApplyReady := rn.Ready()
	thirdApply := findAsyncStorageMessage(
		t, thirdApplyReady, pb.MsgStorageApply)
	if len(thirdApply.GetEntries()) != 1 ||
		thirdApply.GetEntries()[0].GetIndex() !=
			pendingApply[1].GetEntries()[0].GetIndex()+1 {
		t.Fatalf("third apply work = %+v", thirdApply)
	}
	// The apply target still completes in emission order.
	completeAsyncApply(t, rn, pendingApply[1])
	completeAsyncApply(t, rn, thirdApply)
	if got := rn.BasicStatus().Applied; got != 5 {
		t.Fatalf("applied after pagination = %d, want 5", got)
	}
}
