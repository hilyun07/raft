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
	"reflect"
	"testing"

	pb "go.etcd.io/raft/v3/raftpb"
	"go.etcd.io/raft/v3/tracker"
)

func staleMsgAppRespProgress(
	t *testing.T, rn *RawNode, id uint64,
) tracker.Progress {
	t.Helper()
	progress, ok := rn.Status().Progress[id]
	if !ok {
		t.Fatalf("no progress for %d", id)
	}
	return progress
}

func assertStaleMsgAppRespIgnored(
	t *testing.T, rn *RawNode, before tracker.Progress,
) {
	t.Helper()
	after := staleMsgAppRespProgress(t, rn, 2)
	before.RecentActive = true
	if !reflect.DeepEqual(after, before) {
		t.Fatalf("stale response changed progress: before=%+v after=%+v",
			before, after)
	}
	if rn.HasReady() {
		t.Fatal("stale response generated Ready")
	}
}

func TestRawNodeStaleMsgAppRespParity(t *testing.T) {
	tests := []struct {
		name    string
		reject  bool
		logTerm uint64
	}{
		{name: "successful"},
		{name: "rejected without LogTerm", reject: true},
		{
			name:    "rejected with LogTerm",
			reject:  true,
			logTerm: 1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rn, _ := prepareReportUnreachableLeader(t)
			rn.ReportUnreachable(2)
			before := staleMsgAppRespProgress(t, rn, 2)
			if before.State != tracker.StateProbe ||
				before.Match == 0 ||
				before.MsgAppFlowPaused {
				t.Fatalf("precondition progress = %+v", before)
			}

			message := &pb.Message{
				Type:       pb.MsgAppResp.Enum(),
				To:         new(uint64(1)),
				From:       new(uint64(2)),
				Term:       new(rn.BasicStatus().HardState.GetTerm()),
				Index:      new(before.Match - 1),
				Reject:     new(tt.reject),
				RejectHint: new(before.Match - 1),
			}
			if tt.logTerm != 0 {
				message.LogTerm = new(tt.logTerm)
			}
			if err := rn.Step(message); err != nil {
				t.Fatal(err)
			}
			assertStaleMsgAppRespIgnored(t, rn, before)
		})
	}
}

func TestRawNodeDuplicateProbeMsgAppRespParity(t *testing.T) {
	rn, storage := prepareReportUnreachableLeader(t)
	rn.ReportUnreachable(2)
	before := staleMsgAppRespProgress(t, rn, 2)
	if before.State != tracker.StateProbe {
		t.Fatalf("precondition progress = %+v", before)
	}

	if err := rn.Step(&pb.Message{
		Type:  pb.MsgAppResp.Enum(),
		To:    new(uint64(1)),
		From:  new(uint64(2)),
		Term:  new(rn.BasicStatus().HardState.GetTerm()),
		Index: new(before.Match),
	}); err != nil {
		t.Fatal(err)
	}
	after := staleMsgAppRespProgress(t, rn, 2)
	if after.State != tracker.StateReplicate ||
		after.Match != before.Match ||
		after.Next <= before.Next {
		t.Fatalf("equal-index probe response was not actionable: before=%+v after=%+v",
			before, after)
	}
	ready := acceptLeadershipReady(t, rn, storage)
	findLeadershipMessage(t, ready.Messages, pb.MsgApp, 2)
}

func TestRawNodeDuplicateAndOutOfOrderMsgAppRespInflightsParity(
	t *testing.T,
) {
	rn, storage := prepareReportUnreachableLeader(t)
	term := rn.BasicStatus().HardState.GetTerm()

	if err := rn.Step(&pb.Message{
		Type:  pb.MsgAppResp.Enum(),
		To:    new(uint64(1)),
		From:  new(uint64(2)),
		Term:  new(term),
		Index: new(uint64(3)),
	}); err != nil {
		t.Fatal(err)
	}
	acceptLeadershipReady(t, rn, storage)

	for _, data := range [][]byte{[]byte("four"), []byte("five")} {
		if err := rn.Propose(data); err != nil {
			t.Fatal(err)
		}
		acceptLeadershipReady(t, rn, storage)
	}
	before := staleMsgAppRespProgress(t, rn, 2)
	if before.State != tracker.StateReplicate ||
		before.Match != 3 ||
		before.Next != 6 ||
		before.Inflights == nil ||
		before.Inflights.Count() != 2 {
		t.Fatalf("inflight precondition progress = %+v", before)
	}

	if err := rn.Step(&pb.Message{
		Type:  pb.MsgAppResp.Enum(),
		To:    new(uint64(1)),
		From:  new(uint64(2)),
		Term:  new(term),
		Index: new(uint64(4)),
	}); err != nil {
		t.Fatal(err)
	}
	acceptLeadershipReady(t, rn, storage)
	afterFirst := staleMsgAppRespProgress(t, rn, 2)
	if afterFirst.Match != 4 ||
		afterFirst.Inflights == nil ||
		afterFirst.Inflights.Count() != 1 {
		t.Fatalf("first response progress = %+v", afterFirst)
	}

	if err := rn.Step(&pb.Message{
		Type:  pb.MsgAppResp.Enum(),
		To:    new(uint64(1)),
		From:  new(uint64(2)),
		Term:  new(term),
		Index: new(uint64(4)),
	}); err != nil {
		t.Fatal(err)
	}
	assertStaleMsgAppRespIgnored(t, rn, afterFirst)

	if err := rn.Step(&pb.Message{
		Type:  pb.MsgAppResp.Enum(),
		To:    new(uint64(1)),
		From:  new(uint64(2)),
		Term:  new(term),
		Index: new(uint64(5)),
	}); err != nil {
		t.Fatal(err)
	}
	acceptLeadershipReady(t, rn, storage)
	afterSecond := staleMsgAppRespProgress(t, rn, 2)
	if afterSecond.Match != 5 ||
		afterSecond.Inflights == nil ||
		afterSecond.Inflights.Count() != 0 {
		t.Fatalf("second response progress = %+v", afterSecond)
	}

	if err := rn.Step(&pb.Message{
		Type:  pb.MsgAppResp.Enum(),
		To:    new(uint64(1)),
		From:  new(uint64(2)),
		Term:  new(term),
		Index: new(uint64(4)),
	}); err != nil {
		t.Fatal(err)
	}
	assertStaleMsgAppRespIgnored(t, rn, afterSecond)
}

func TestRawNodeOldTermMsgAppRespAfterProgressResetParity(t *testing.T) {
	rn, storage := prepareReportUnreachableLeader(t)
	oldTerm := rn.BasicStatus().HardState.GetTerm()

	if err := rn.Step(&pb.Message{
		Type:   pb.MsgHeartbeat.Enum(),
		To:     new(uint64(1)),
		From:   new(uint64(2)),
		Term:   new(oldTerm + 1),
		Commit: new(uint64(2)),
	}); err != nil {
		t.Fatal(err)
	}
	acceptLeadershipReady(t, rn, storage)
	if err := rn.Campaign(); err != nil {
		t.Fatal(err)
	}
	acceptLeadershipReady(t, rn, storage)
	newTerm := rn.BasicStatus().HardState.GetTerm()
	if err := rn.Step(&pb.Message{
		Type: pb.MsgVoteResp.Enum(),
		To:   new(uint64(1)),
		From: new(uint64(2)),
		Term: new(newTerm),
	}); err != nil {
		t.Fatal(err)
	}
	acceptLeadershipReady(t, rn, storage)

	before := staleMsgAppRespProgress(t, rn, 2)
	if before.State != tracker.StateProbe {
		t.Fatalf("reset progress = %+v", before)
	}
	if err := rn.Step(&pb.Message{
		Type:  pb.MsgAppResp.Enum(),
		To:    new(uint64(1)),
		From:  new(uint64(2)),
		Term:  new(oldTerm),
		Index: new(uint64(3)),
	}); err != nil {
		t.Fatal(err)
	}
	after := staleMsgAppRespProgress(t, rn, 2)
	if !reflect.DeepEqual(after, before) {
		t.Fatalf("old-term response changed reset progress: before=%+v after=%+v",
			before, after)
	}
	if rn.HasReady() {
		t.Fatal("old-term response generated Ready")
	}
}
