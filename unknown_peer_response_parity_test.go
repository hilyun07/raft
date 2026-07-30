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
	"reflect"
	"testing"

	pb "go.etcd.io/raft/v3/raftpb"
)

func newUnknownPeerResponseParityConfig(
	t *testing.T,
) (*Config, *MemoryStorage) {
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
	}, storage
}

func TestRawNodeUnknownPeerResponseFilteringParity(t *testing.T) {
	config, _ := newUnknownPeerResponseParityConfig(t)
	rn, err := NewRawNode(config)
	if err != nil {
		t.Fatal(err)
	}

	before := rn.BasicStatus()
	unknown := &pb.Message{
		Type: pb.MsgAppResp.Enum(),
		To:   new(uint64(1)),
		From: new(uint64(3)),
		Term: new(before.GetTerm() + 10),
	}
	if err := rn.Step(unknown); !errors.Is(err, ErrStepPeerNotFound) {
		t.Fatalf("RawNode.Step unknown response error = %v", err)
	}
	if err := rn.stepForNode(unknown); !errors.Is(
		err, ErrStepPeerNotFound,
	) {
		t.Fatalf("RawNode.stepForNode unknown response error = %v", err)
	}
	if after := rn.BasicStatus(); !reflect.DeepEqual(after, before) {
		t.Fatalf("unknown response changed state: before=%+v after=%+v",
			before, after)
	}

	localStorageResponse := &pb.Message{
		Type: pb.MsgStorageApplyResp.Enum(),
		To:   new(uint64(1)),
		From: new(uint64(LocalApplyThread)),
	}
	if err := rn.Step(localStorageResponse); err != nil {
		t.Fatalf("RawNode.Step local storage response error = %v", err)
	}
	if err := rn.stepForNode(localStorageResponse); err != nil {
		t.Fatalf("RawNode.stepForNode local storage response error = %v", err)
	}

	known := &pb.Message{
		Type: pb.MsgAppResp.Enum(),
		To:   new(uint64(1)),
		From: new(uint64(2)),
		Term: new(before.GetTerm() + 1),
	}
	if err := rn.Step(known); err != nil {
		t.Fatalf("RawNode.Step known response error = %v", err)
	}
	if status := rn.BasicStatus(); status.GetTerm() != known.GetTerm() {
		t.Fatalf("known response was not processed: %+v", status)
	}

	known.Term = new(known.GetTerm() + 1)
	if err := rn.stepForNode(known); err != nil {
		t.Fatalf("RawNode.stepForNode known response error = %v", err)
	}
	if status := rn.BasicStatus(); status.GetTerm() != known.GetTerm() {
		t.Fatalf("known Node-path response was not processed: %+v", status)
	}
}

func TestNodeUnknownPeerResponseFilteringParity(t *testing.T) {
	config, _ := newUnknownPeerResponseParityConfig(t)
	n := RestartNode(config)
	defer n.Stop()

	before := n.Status()
	unknown := &pb.Message{
		Type: pb.MsgAppResp.Enum(),
		To:   new(uint64(1)),
		From: new(uint64(3)),
		Term: new(before.GetTerm() + 10),
	}
	if err := n.Step(t.Context(), unknown); err != nil {
		t.Fatalf("Node.Step unknown response error = %v", err)
	}
	if after := n.Status(); !reflect.DeepEqual(
		after.BasicStatus, before.BasicStatus,
	) {
		t.Fatalf("unknown Node response changed state: before=%+v after=%+v",
			before.BasicStatus, after.BasicStatus)
	}

	known := &pb.Message{
		Type: pb.MsgAppResp.Enum(),
		To:   new(uint64(1)),
		From: new(uint64(2)),
		Term: new(before.GetTerm() + 1),
	}
	if err := n.Step(t.Context(), known); err != nil {
		t.Fatalf("Node.Step known response error = %v", err)
	}
	if status := n.Status(); status.GetTerm() != known.GetTerm() {
		t.Fatalf("known Node response was not processed: %+v", status)
	}
}
