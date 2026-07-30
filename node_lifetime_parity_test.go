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

func newNodeLifetimeParityNode(
	t *testing.T, asyncStorageWrites bool,
) (Node, *MemoryStorage) {
	t.Helper()
	storage := NewMemoryStorage()
	node := StartNode(&Config{
		ID:                 1,
		ElectionTick:       10,
		HeartbeatTick:      1,
		Storage:            storage,
		AsyncStorageWrites: asyncStorageWrites,
		MaxSizePerMsg:      ^uint64(0),
		MaxInflightMsgs:    16,
		Logger:             discardLogger,
	}, []Peer{{ID: 1}})
	return node, storage
}

func receiveNodeLifetimeReady(t *testing.T, node Node) Ready {
	t.Helper()
	select {
	case ready := <-node.Ready():
		return ready
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for Ready")
		return Ready{}
	}
}

func persistNodeLifetimeReady(
	t *testing.T, storage *MemoryStorage, ready Ready,
) {
	t.Helper()
	if !IsEmptySnap(ready.Snapshot) {
		if err := storage.ApplySnapshot(ready.Snapshot); err != nil {
			t.Fatal(err)
		}
	}
	if len(ready.Entries) != 0 {
		if err := storage.Append(ready.Entries); err != nil {
			t.Fatal(err)
		}
	}
	if !IsEmptyHardState(ready.HardState) {
		if err := storage.SetHardState(ready.HardState); err != nil {
			t.Fatal(err)
		}
	}
}

func requireNodeLifetimeReturns(t *testing.T, operation func()) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		operation()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("operation did not return")
	}
}

func findNodeLifetimeStorageAppend(
	t *testing.T, ready Ready,
) *pb.Message {
	t.Helper()
	for _, message := range ready.Messages {
		if message.GetType() != pb.MsgStorageAppend {
			continue
		}
		if len(message.GetResponses()) == 0 {
			t.Fatal("MsgStorageAppend has no response")
		}
		return message
	}
	t.Fatal("Ready has no MsgStorageAppend")
	return nil
}

func persistNodeLifetimeStorageAppend(
	t *testing.T, storage *MemoryStorage, message *pb.Message,
) {
	t.Helper()
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
	if !IsEmptySnap(message.Snapshot) {
		if err := storage.ApplySnapshot(message.Snapshot); err != nil {
			t.Fatal(err)
		}
		return
	}
	if err := storage.Append(message.Entries); err != nil {
		t.Fatal(err)
	}
}

func TestNodeLifetimeNormalLifecycleParity(t *testing.T) {
	node, storage := newNodeLifetimeParityNode(t, false)

	ready := receiveNodeLifetimeReady(t, node)
	persistNodeLifetimeReady(t, storage, ready)
	node.Advance()
	node.Tick()

	if err := node.Step(t.Context(), &pb.Message{
		Type:   pb.MsgHeartbeat.Enum(),
		To:     new(uint64(1)),
		From:   new(uint64(2)),
		Term:   new(uint64(2)),
		Commit: new(uint64(1)),
	}); err != nil {
		t.Fatal(err)
	}
	ready = receiveNodeLifetimeReady(t, node)
	persistNodeLifetimeReady(t, storage, ready)
	node.Advance()
	node.Stop()
}

func TestNodeLifetimePostStopParity(t *testing.T) {
	node, _ := newNodeLifetimeParityNode(t, false)
	node.Stop()

	if err := node.Step(t.Context(), &pb.Message{
		Type: pb.MsgHeartbeat.Enum(),
		From: new(uint64(2)),
	}); !errors.Is(err, ErrStopped) {
		t.Fatalf("Step after Stop = %v, want ErrStopped", err)
	}
	if err := node.Step(t.Context(), &pb.Message{
		Type: pb.MsgHup.Enum(),
	}); err != nil {
		t.Fatalf("unexpected remote local-message filter result: %v", err)
	}
	requireNodeLifetimeReturns(t, node.Tick)
	requireNodeLifetimeReturns(t, node.Advance)
	requireNodeLifetimeReturns(t, node.Stop)

	select {
	case ready, ok := <-node.Ready():
		t.Fatalf(
			"Ready after Stop was readable: ready=%+v open=%t",
			ready,
			ok,
		)
	default:
	}
	if status := node.Status(); !reflect.DeepEqual(status, Status{}) {
		t.Fatalf("Status after Stop = %+v, want empty", status)
	}
	if state := node.ApplyConfChange(&pb.ConfChangeV2{}); state != nil {
		t.Fatalf("ApplyConfChange after Stop = %+v, want nil", state)
	}
}

func TestNodeLifetimeStopUnblocksPendingWorkParity(t *testing.T) {
	node, _ := newNodeLifetimeParityNode(t, false)
	proposalResult := make(chan error, 1)
	go func() {
		proposalResult <- node.Propose(
			context.Background(), []byte("pending"))
	}()

	node.Stop()
	select {
	case err := <-proposalResult:
		if !errors.Is(err, ErrStopped) {
			t.Fatalf("pending Propose = %v, want ErrStopped", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("pending Propose did not return after Stop")
	}
}

func TestNodeLifetimeAsyncStorageResponseAfterStopParity(t *testing.T) {
	node, storage := newNodeLifetimeParityNode(t, true)
	appendMessage := findNodeLifetimeStorageAppend(
		t, receiveNodeLifetimeReady(t, node))
	persistNodeLifetimeStorageAppend(t, storage, appendMessage)
	response := appendMessage.GetResponses()[len(appendMessage.GetResponses())-1]

	complete := make(chan error, 1)
	release := make(chan struct{})
	go func() {
		<-release
		complete <- node.Step(context.Background(), response)
	}()
	node.Stop()
	close(release)

	select {
	case err := <-complete:
		if !errors.Is(err, ErrStopped) {
			t.Fatalf(
				"storage response after Stop = %v, want ErrStopped",
				err,
			)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("storage response did not return after Stop")
	}
}

func TestNodeLifetimeAsyncStorageResponseStopRaceParity(t *testing.T) {
	const iterations = 100
	for i := 0; i < iterations; i++ {
		node, storage := newNodeLifetimeParityNode(t, true)
		appendMessage := findNodeLifetimeStorageAppend(
			t, receiveNodeLifetimeReady(t, node))
		persistNodeLifetimeStorageAppend(t, storage, appendMessage)
		response := appendMessage.GetResponses()[len(appendMessage.GetResponses())-1]
		start := make(chan struct{})
		stepResult := make(chan error, 1)
		stopDone := make(chan struct{})

		go func() {
			<-start
			stepResult <- node.Step(context.Background(), response)
		}()
		go func() {
			<-start
			node.Stop()
			close(stopDone)
		}()
		close(start)

		select {
		case err := <-stepResult:
			if err != nil && !errors.Is(err, ErrStopped) {
				t.Fatalf("iteration %d: Step = %v", i, err)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("iteration %d: Step did not return", i)
		}
		select {
		case <-stopDone:
		case <-time.After(5 * time.Second):
			t.Fatalf("iteration %d: Stop did not return", i)
		}
		node.Stop()
	}
}
