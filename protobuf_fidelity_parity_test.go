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

	"google.golang.org/protobuf/proto"

	pb "go.etcd.io/raft/v3/raftpb"
)

func protobufFidelityUnknown(tag byte, value byte) []byte {
	// Fields 100 through 115 have two-byte varint keys whose first byte is
	// tag and whose second byte is 0x06. All examples use varint wire type.
	return []byte{tag, 0x06, value}
}

func protobufFidelitySetUnknown(message proto.Message, unknown []byte) {
	message.ProtoReflect().SetUnknown(append([]byte(nil), unknown...))
}

func requireProtobufFidelityEqual(
	t *testing.T,
	want proto.Message,
	got proto.Message,
) {
	t.Helper()
	if !proto.Equal(want, got) {
		t.Fatalf("protobuf mismatch:\nwant: %v\ngot:  %v", want, got)
	}
	if wantSize, gotSize := proto.Size(want), proto.Size(got); wantSize != gotSize {
		t.Fatalf("proto.Size = %d, want %d", gotSize, wantSize)
	}
	options := proto.MarshalOptions{Deterministic: true}
	wantBytes, err := options.Marshal(want)
	if err != nil {
		t.Fatal(err)
	}
	gotBytes, err := options.Marshal(got)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(wantBytes, gotBytes) {
		t.Fatalf("protobuf bytes = %x, want %x", gotBytes, wantBytes)
	}
}

func newProtobufFidelityRawNode(
	t *testing.T,
	id uint64,
	voters ...uint64,
) (*RawNode, *MemoryStorage) {
	t.Helper()
	storage := NewMemoryStorage()
	if err := storage.ApplySnapshot(&pb.Snapshot{
		Metadata: &pb.SnapshotMetadata{
			ConfState: &pb.ConfState{Voters: voters},
			Index:     new(uint64(1)),
			Term:      new(uint64(1)),
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
	rawNode, err := NewRawNode(&Config{
		ID:              id,
		ElectionTick:    10,
		HeartbeatTick:   1,
		Storage:         storage,
		Applied:         1,
		MaxSizePerMsg:   ^uint64(0),
		MaxInflightMsgs: 16,
	})
	if err != nil {
		t.Fatal(err)
	}
	return rawNode, storage
}

func persistProtobufFidelityReady(
	t *testing.T,
	storage *MemoryStorage,
	ready Ready,
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

func drainProtobufFidelityReady(
	t *testing.T,
	rawNode *RawNode,
	storage *MemoryStorage,
) {
	t.Helper()
	for i := 0; rawNode.HasReady(); i++ {
		if i == 20 {
			t.Fatal("RawNode did not quiesce")
		}
		ready := rawNode.Ready()
		persistProtobufFidelityReady(t, storage, ready)
		rawNode.Advance(ready)
	}
}

func requireProtobufFidelityPanic(t *testing.T, operation func()) {
	t.Helper()
	defer func() {
		if recover() == nil {
			t.Fatal("operation did not panic")
		}
	}()
	operation()
}

func TestProtobufFidelityProposalEntryParity(t *testing.T) {
	normal := pb.EntryNormal
	for _, test := range []struct {
		name  string
		entry *pb.Entry
	}{
		{
			name:  "absent-default-type-and-nil-data",
			entry: &pb.Entry{},
		},
		{
			name: "present-default-type-and-empty-data",
			entry: &pb.Entry{
				Type: &normal,
				Data: []byte{},
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			rawNode, storage := newProtobufFidelityRawNode(t, 1, 1)
			if err := rawNode.Campaign(); err != nil {
				t.Fatal(err)
			}
			drainProtobufFidelityReady(t, rawNode, storage)

			protobufFidelitySetUnknown(
				test.entry,
				protobufFidelityUnknown(0xa0, 7),
			)
			want := proto.Clone(test.entry).(*pb.Entry)
			if err := rawNode.Step(&pb.Message{
				Type:    pb.MsgProp.Enum(),
				From:    new(uint64(1)),
				Entries: []*pb.Entry{test.entry},
			}); err != nil {
				t.Fatal(err)
			}
			ready := rawNode.Ready()
			if len(ready.Entries) != 1 {
				t.Fatalf("Ready.Entries length = %d, want 1", len(ready.Entries))
			}
			got := ready.Entries[0]
			want.Term = new(got.GetTerm())
			want.Index = new(got.GetIndex())
			requireProtobufFidelityEqual(t, want, got)
		})
	}
}

func TestProtobufFidelityForwardedMessageParity(t *testing.T) {
	rawNode, storage := newProtobufFidelityRawNode(t, 1, 1, 2)
	if err := rawNode.Step(&pb.Message{
		Type:   pb.MsgHeartbeat.Enum(),
		To:     new(uint64(1)),
		From:   new(uint64(2)),
		Term:   new(uint64(2)),
		Commit: new(uint64(1)),
	}); err != nil {
		t.Fatal(err)
	}
	drainProtobufFidelityReady(t, rawNode, storage)

	normal := pb.EntryNormal
	zero := uint64(0)
	entry := &pb.Entry{Type: &normal, Data: []byte{}}
	protobufFidelitySetUnknown(
		entry,
		protobufFidelityUnknown(0xa8, 8),
	)
	message := &pb.Message{
		Type:    pb.MsgProp.Enum(),
		From:    new(uint64(1)),
		Term:    &zero,
		Index:   &zero,
		Entries: []*pb.Entry{entry},
		Context: []byte{},
	}
	protobufFidelitySetUnknown(
		message,
		protobufFidelityUnknown(0xb0, 9),
	)
	want := proto.Clone(message).(*pb.Message)
	want.To = new(uint64(2))

	if err := rawNode.Step(message); err != nil {
		t.Fatal(err)
	}
	ready := rawNode.Ready()
	if len(ready.Messages) != 1 {
		t.Fatalf("Ready.Messages length = %d, want 1", len(ready.Messages))
	}
	requireProtobufFidelityEqual(t, want, ready.Messages[0])
}

func TestProtobufFidelitySnapshotParity(t *testing.T) {
	rawNode, _ := newProtobufFidelityRawNode(t, 1, 1, 2)
	autoLeave := false
	confState := &pb.ConfState{
		Voters:    []uint64{1, 2},
		AutoLeave: &autoLeave,
	}
	metadata := &pb.SnapshotMetadata{
		ConfState: confState,
		Index:     new(uint64(5)),
		Term:      new(uint64(3)),
	}
	protobufFidelitySetUnknown(
		metadata,
		protobufFidelityUnknown(0xc0, 10),
	)
	snapshot := &pb.Snapshot{
		Data:     []byte{},
		Metadata: metadata,
	}
	protobufFidelitySetUnknown(
		snapshot,
		protobufFidelityUnknown(0xc8, 11),
	)

	if err := rawNode.Step(&pb.Message{
		Type:     pb.MsgSnap.Enum(),
		To:       new(uint64(1)),
		From:     new(uint64(2)),
		Term:     new(uint64(3)),
		Snapshot: snapshot,
	}); err != nil {
		t.Fatal(err)
	}
	ready := rawNode.Ready()
	if IsEmptySnap(ready.Snapshot) {
		t.Fatal("Ready omitted restored snapshot")
	}
	requireProtobufFidelityEqual(t, snapshot, ready.Snapshot)
}

func TestProtobufFidelityConfStateUnknownInvariantParity(t *testing.T) {
	t.Run("initial-state", func(t *testing.T) {
		storage := NewMemoryStorage()
		confState := &pb.ConfState{Voters: []uint64{1}}
		protobufFidelitySetUnknown(
			confState,
			protobufFidelityUnknown(0xa0, 1),
		)
		if err := storage.ApplySnapshot(&pb.Snapshot{
			Metadata: &pb.SnapshotMetadata{
				ConfState: confState,
				Index:     new(uint64(1)),
				Term:      new(uint64(1)),
			},
		}); err != nil {
			t.Fatal(err)
		}
		requireProtobufFidelityPanic(t, func() {
			_, _ = NewRawNode(&Config{
				ID:              1,
				ElectionTick:    10,
				HeartbeatTick:   1,
				Storage:         storage,
				Applied:         1,
				MaxSizePerMsg:   ^uint64(0),
				MaxInflightMsgs: 16,
			})
		})
	})

	t.Run("received-snapshot", func(t *testing.T) {
		rawNode, _ := newProtobufFidelityRawNode(t, 1, 1, 2)
		confState := &pb.ConfState{Voters: []uint64{1, 2}}
		protobufFidelitySetUnknown(
			confState,
			protobufFidelityUnknown(0xa0, 2),
		)
		requireProtobufFidelityPanic(t, func() {
			_ = rawNode.Step(&pb.Message{
				Type: pb.MsgSnap.Enum(),
				To:   new(uint64(1)),
				From: new(uint64(2)),
				Term: new(uint64(3)),
				Snapshot: &pb.Snapshot{
					Metadata: &pb.SnapshotMetadata{
						ConfState: confState,
						Index:     new(uint64(5)),
						Term:      new(uint64(3)),
					},
				},
			})
		})
	})
}

func TestProtobufFidelityConfChangeProposalParity(t *testing.T) {
	t.Run("v1", func(t *testing.T) {
		rawNode, storage := newProtobufFidelityRawNode(t, 1, 1)
		if err := rawNode.Campaign(); err != nil {
			t.Fatal(err)
		}
		drainProtobufFidelityReady(t, rawNode, storage)

		change := &pb.ConfChange{
			Type:    pb.ConfChangeAddNode.Enum(),
			NodeId:  new(uint64(2)),
			Context: []byte{},
		}
		protobufFidelitySetUnknown(
			change,
			protobufFidelityUnknown(0xd0, 13),
		)
		if err := rawNode.ProposeConfChange(change); err != nil {
			t.Fatal(err)
		}
		ready := rawNode.Ready()
		if len(ready.Entries) != 1 {
			t.Fatalf("Ready.Entries length = %d, want 1", len(ready.Entries))
		}
		decoded := &pb.ConfChange{}
		if err := proto.Unmarshal(ready.Entries[0].Data, decoded); err != nil {
			t.Fatal(err)
		}
		requireProtobufFidelityEqual(t, change, decoded)
	})

	t.Run("v2-and-nested-change", func(t *testing.T) {
		rawNode, storage := newProtobufFidelityRawNode(t, 1, 1)
		if err := rawNode.Campaign(); err != nil {
			t.Fatal(err)
		}
		drainProtobufFidelityReady(t, rawNode, storage)

		single := &pb.ConfChangeSingle{
			Type:   pb.ConfChangeAddNode.Enum(),
			NodeId: new(uint64(2)),
		}
		protobufFidelitySetUnknown(
			single,
			protobufFidelityUnknown(0xd8, 14),
		)
		change := &pb.ConfChangeV2{
			Transition: pb.ConfChangeTransitionAuto.Enum(),
			Changes:    []*pb.ConfChangeSingle{single},
			Context:    []byte{},
		}
		protobufFidelitySetUnknown(
			change,
			protobufFidelityUnknown(0xe0, 15),
		)
		if err := rawNode.ProposeConfChange(change); err != nil {
			t.Fatal(err)
		}
		ready := rawNode.Ready()
		if len(ready.Entries) != 1 {
			t.Fatalf("Ready.Entries length = %d, want 1", len(ready.Entries))
		}
		decoded := &pb.ConfChangeV2{}
		if err := proto.Unmarshal(ready.Entries[0].Data, decoded); err != nil {
			t.Fatal(err)
		}
		requireProtobufFidelityEqual(t, change, decoded)
	})
}

func TestProtobufFidelityGeneratedVoteMessagePresenceParity(t *testing.T) {
	rawNode, _ := newProtobufFidelityRawNode(t, 1, 1, 2)
	if err := rawNode.Campaign(); err != nil {
		t.Fatal(err)
	}
	ready := rawNode.Ready()
	if len(ready.Messages) != 1 {
		t.Fatalf("Ready.Messages length = %d, want 1", len(ready.Messages))
	}
	requireProtobufFidelityEqual(t, &pb.Message{
		Type:    pb.MsgVote.Enum(),
		To:      new(uint64(2)),
		From:    new(uint64(1)),
		Term:    new(uint64(2)),
		LogTerm: new(uint64(1)),
		Index:   new(uint64(1)),
	}, ready.Messages[0])
}

func TestProtobufFidelityAsyncStorageMessagePresenceParity(t *testing.T) {
	storage := NewMemoryStorage()
	rawNode := newAsyncStorageParityNode(t, storage, 0)
	if err := rawNode.Bootstrap([]Peer{{ID: 1}}); err != nil {
		t.Fatal(err)
	}

	ready := rawNode.Ready()
	if len(ready.Entries) != 1 {
		t.Fatalf("Ready.Entries length = %d, want 1", len(ready.Entries))
	}
	last := ready.Entries[len(ready.Entries)-1]
	want := &pb.Message{
		Type:    pb.MsgStorageAppend.Enum(),
		To:      new(LocalAppendThread),
		From:    new(uint64(1)),
		Term:    new(ready.GetTerm()),
		Vote:    new(ready.GetVote()),
		Commit:  new(ready.GetCommit()),
		Entries: ready.Entries,
		Responses: []*pb.Message{{
			Type:    pb.MsgStorageAppendResp.Enum(),
			To:      new(uint64(1)),
			From:    new(LocalAppendThread),
			Term:    new(ready.GetTerm()),
			LogTerm: new(last.GetTerm()),
			Index:   new(last.GetIndex()),
		}},
	}
	got := findAsyncStorageMessage(t, ready, pb.MsgStorageAppend)
	requireProtobufFidelityEqual(t, want, got)
}
