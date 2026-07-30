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
	"time"

	pb "go.etcd.io/raft/v3/raftpb"
	"go.etcd.io/raft/v3/tracker"
)

func reportUnreachableProgress(
	t *testing.T, rn *RawNode, id uint64,
) tracker.Progress {
	t.Helper()
	var progress tracker.Progress
	var found bool
	rn.WithProgress(func(
		progressID uint64, _ ProgressType, row tracker.Progress,
	) {
		if progressID == id {
			progress = row
			found = true
		}
	})
	if !found {
		t.Fatalf("no progress for %d", id)
	}
	return progress
}

func prepareReportUnreachableLeader(
	t *testing.T,
) (*RawNode, *MemoryStorage) {
	t.Helper()
	rn, storage := campaignLeadershipParityLeader(
		t, false, false)
	term := rn.BasicStatus().HardState.GetTerm()
	if err := rn.Step(&pb.Message{
		Type:  pb.MsgAppResp.Enum(),
		To:    new(uint64(1)),
		From:  new(uint64(2)),
		Term:  new(term),
		Index: new(uint64(2)),
	}); err != nil {
		t.Fatal(err)
	}
	acceptLeadershipReady(t, rn, storage)
	if progress := reportUnreachableProgress(t, rn, 2); progress.State != tracker.StateReplicate ||
		progress.Match != 2 {
		t.Fatalf("caught-up progress = %+v", progress)
	}

	if err := rn.Propose([]byte("possibly lost")); err != nil {
		t.Fatal(err)
	}
	acceptLeadershipReady(t, rn, storage)
	if rn.HasReady() {
		t.Fatal("leader retained Ready before unreachable report")
	}
	return rn, storage
}

func assertReportUnreachableTransition(
	t *testing.T, before, after tracker.Progress,
) {
	t.Helper()
	if before.State != tracker.StateReplicate {
		t.Fatalf("before state = %v, want replicate", before.State)
	}
	if after.State != tracker.StateProbe {
		t.Fatalf("after state = %v, want probe", after.State)
	}
	if after.Match != before.Match ||
		after.Next != after.Match+1 ||
		after.PendingSnapshot != 0 ||
		after.MsgAppFlowPaused ||
		after.RecentActive != before.RecentActive ||
		after.IsLearner != before.IsLearner {
		t.Fatalf("progress transition before=%+v after=%+v",
			before, after)
	}
	if before.Next <= after.Next {
		t.Fatalf("optimistic Next did not regress: before=%d after=%d",
			before.Next, after.Next)
	}
}

func TestRawNodeReportUnreachableParity(t *testing.T) {
	rn, storage := prepareReportUnreachableLeader(t)
	before := reportUnreachableProgress(t, rn, 2)

	rn.ReportUnreachable(2)
	after := reportUnreachableProgress(t, rn, 2)
	assertReportUnreachableTransition(t, before, after)
	if rn.HasReady() {
		t.Fatal("ReportUnreachable generated immediate Ready")
	}

	rn.ReportUnreachable(2)
	if repeated := reportUnreachableProgress(t, rn, 2); !reflect.DeepEqual(repeated, after) {
		t.Fatalf("report in probe state changed progress: before=%+v after=%+v",
			after, repeated)
	}
	if rn.HasReady() {
		t.Fatal("repeated ReportUnreachable generated Ready")
	}

	rn.ReportUnreachable(99)
	if unknown := reportUnreachableProgress(t, rn, 2); !reflect.DeepEqual(unknown, after) {
		t.Fatalf("unknown peer changed progress: before=%+v after=%+v",
			after, unknown)
	}
	if rn.HasReady() {
		t.Fatal("unknown-peer ReportUnreachable generated Ready")
	}

	term := rn.BasicStatus().HardState.GetTerm()
	if err := rn.Step(&pb.Message{
		Type: pb.MsgHeartbeatResp.Enum(),
		To:   new(uint64(1)),
		From: new(uint64(2)),
		Term: new(term),
	}); err != nil {
		t.Fatal(err)
	}
	probeReady := acceptLeadershipReady(t, rn, storage)
	probe := findLeadershipMessage(
		t, probeReady.Messages, pb.MsgApp, 2)
	if probe.GetIndex() != after.Match {
		t.Fatalf("probe index = %d, want confirmed match %d",
			probe.GetIndex(), after.Match)
	}
	acknowledged := probe.GetIndex() + uint64(len(probe.GetEntries()))
	if err := rn.Step(&pb.Message{
		Type:  pb.MsgAppResp.Enum(),
		To:    new(uint64(1)),
		From:  new(uint64(2)),
		Term:  new(term),
		Index: new(acknowledged),
	}); err != nil {
		t.Fatal(err)
	}
	if recovered := reportUnreachableProgress(t, rn, 2); recovered.State != tracker.StateReplicate {
		t.Fatalf("probe response did not recover replication: %+v",
			recovered)
	}
}

func TestRawNodeReportUnreachableFollowerNoopParity(t *testing.T) {
	rn, _ := newLeadershipParityNode(t, 1, false, false)
	before := reportUnreachableProgress(t, rn, 2)
	if rn.BasicStatus().RaftState != StateFollower {
		t.Fatalf("state = %v, want follower",
			rn.BasicStatus().RaftState)
	}

	rn.ReportUnreachable(2)
	after := reportUnreachableProgress(t, rn, 2)
	if !reflect.DeepEqual(after, before) {
		t.Fatalf("follower progress changed: before=%+v after=%+v",
			before, after)
	}
	if rn.HasReady() {
		t.Fatal("follower ReportUnreachable generated Ready")
	}
}

func TestNodeReportUnreachableParity(t *testing.T) {
	rn, _ := prepareReportUnreachableLeader(t)
	before := reportUnreachableProgress(t, rn, 2)
	node := newNode(rn)
	go node.run()
	defer node.Stop()

	node.ReportUnreachable(2)
	status := node.Status()
	after, ok := status.Progress[2]
	if !ok {
		t.Fatalf("Node status has no progress for 2: %+v",
			status.Progress)
	}
	assertReportUnreachableTransition(t, before, after)

	select {
	case ready := <-node.Ready():
		t.Fatalf("Node ReportUnreachable generated Ready: %+v", ready)
	case <-time.After(50 * time.Millisecond):
	}
}
