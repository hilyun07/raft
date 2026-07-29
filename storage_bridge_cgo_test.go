//go:build cgo_raft && cgo

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
	"runtime/cgo"
	"testing"

	"google.golang.org/protobuf/proto"

	pb "go.etcd.io/raft/v3/raftpb"
)

type callbackTestStorage struct {
	initialState func() (*pb.HardState, *pb.ConfState, error)
	entries      func(lo, hi, maxSize uint64) ([]*pb.Entry, error)
	term         func(index uint64) (uint64, error)
	firstIndex   func() (uint64, error)
	lastIndex    func() (uint64, error)
	snapshot     func() (*pb.Snapshot, error)
}

func (s *callbackTestStorage) InitialState() (*pb.HardState, *pb.ConfState, error) {
	if s.initialState == nil {
		return nil, &pb.ConfState{}, nil
	}
	return s.initialState()
}

func (s *callbackTestStorage) Entries(lo, hi, maxSize uint64) ([]*pb.Entry, error) {
	if s.entries == nil {
		return nil, nil
	}
	return s.entries(lo, hi, maxSize)
}

func (s *callbackTestStorage) Term(index uint64) (uint64, error) {
	if s.term == nil {
		return 0, nil
	}
	return s.term(index)
}

func (s *callbackTestStorage) FirstIndex() (uint64, error) {
	if s.firstIndex == nil {
		return 0, nil
	}
	return s.firstIndex()
}

func (s *callbackTestStorage) LastIndex() (uint64, error) {
	if s.lastIndex == nil {
		return 0, nil
	}
	return s.lastIndex()
}

func (s *callbackTestStorage) Snapshot() (*pb.Snapshot, error) {
	if s.snapshot == nil {
		return &pb.Snapshot{}, nil
	}
	return s.snapshot()
}

func storageTestUint64(value uint64) *uint64 {
	return &value
}

func storageTestBool(value bool) *bool {
	return &value
}

func newStorageCallbackHandle(storage Storage) (cgo.Handle, *storageBridge) {
	bridge := &storageBridge{storage: storage}
	return cgo.NewHandle(bridge), bridge
}

func assertStorageHandleDeleted(t *testing.T, handle cgo.Handle) {
	t.Helper()
	panicked := false
	func() {
		defer func() {
			panicked = recover() != nil
		}()
		_ = handle.Value()
	}()
	if !panicked {
		t.Fatal("cgo.Handle remained valid after ownership cleanup")
	}
}

func TestCGoStorageScalarCallbacks(t *testing.T) {
	storage := &callbackTestStorage{
		term: func(index uint64) (uint64, error) {
			if index != 7 {
				t.Fatalf("Term index = %d, want 7", index)
			}
			return 11, nil
		},
		firstIndex: func() (uint64, error) { return 3, nil },
		lastIndex:  func() (uint64, error) { return 9, nil },
	}
	handle, _ := newStorageCallbackHandle(storage)
	defer handle.Delete()

	if code, term := callStorageTerm(handle, 7); storageCallbackError(code) != nil || term != 11 {
		t.Fatalf("Term callback = (%d, %d, %v)", code, term, storageCallbackError(code))
	}
	if code, index := callStorageFirstIndex(handle); storageCallbackError(code) != nil || index != 3 {
		t.Fatalf("FirstIndex callback = (%d, %d, %v)", code, index, storageCallbackError(code))
	}
	if code, index := callStorageLastIndex(handle); storageCallbackError(code) != nil || index != 9 {
		t.Fatalf("LastIndex callback = (%d, %d, %v)", code, index, storageCallbackError(code))
	}
}

func TestCGoStorageInitialStateCallbackCopiesState(t *testing.T) {
	hardState := &pb.HardState{
		Term:   storageTestUint64(4),
		Vote:   storageTestUint64(2),
		Commit: storageTestUint64(3),
	}
	confState := &pb.ConfState{
		Voters:         []uint64{1, 2},
		VotersOutgoing: []uint64{1, 3},
		Learners:       []uint64{4},
		LearnersNext:   []uint64{5},
		AutoLeave:      storageTestBool(true),
	}
	storage := &callbackTestStorage{
		initialState: func() (*pb.HardState, *pb.ConfState, error) {
			return hardState, confState, nil
		},
	}
	handle, _ := newStorageCallbackHandle(storage)
	defer handle.Delete()

	result := callStorageInitialState(handle)
	defer result.free()
	if err := storageCallbackError(result.code); err != nil {
		t.Fatal(err)
	}

	confState.Voters[0] = 99
	gotHardState, gotConfState := result.values()
	if !proto.Equal(gotHardState, hardState) {
		t.Fatalf("HardState = %v, want %v", gotHardState, hardState)
	}
	wantVoters := []uint64{1, 2}
	if !reflect.DeepEqual(gotConfState.Voters, wantVoters) {
		t.Fatalf("ConfState voters = %v, want %v", gotConfState.Voters, wantVoters)
	}
	if !reflect.DeepEqual(gotConfState.VotersOutgoing, []uint64{1, 3}) ||
		!reflect.DeepEqual(gotConfState.Learners, []uint64{4}) ||
		!reflect.DeepEqual(gotConfState.LearnersNext, []uint64{5}) ||
		!gotConfState.GetAutoLeave() {
		t.Fatalf("unexpected ConfState: %v", gotConfState)
	}
}

func TestCGoStorageEntriesCallbackDeepCopiesAndPreservesNil(t *testing.T) {
	normal := pb.EntryNormal
	entries := []*pb.Entry{
		{Type: &normal, Term: storageTestUint64(1), Index: storageTestUint64(1), Data: nil},
		{Type: &normal, Term: storageTestUint64(1), Index: storageTestUint64(2), Data: []byte{}},
		{Type: &normal, Term: storageTestUint64(2), Index: storageTestUint64(3), Data: []byte{1, 2, 3}},
	}
	storage := &callbackTestStorage{
		entries: func(lo, hi, maxSize uint64) ([]*pb.Entry, error) {
			if lo != 1 || hi != 4 || maxSize != 1024 {
				t.Fatalf("Entries args = (%d, %d, %d)", lo, hi, maxSize)
			}
			return entries, nil
		},
	}
	handle, _ := newStorageCallbackHandle(storage)
	defer handle.Delete()

	result := callStorageEntries(handle, 1, 4, 1024)
	defer result.free()
	if err := storageCallbackError(result.code); err != nil {
		t.Fatal(err)
	}

	entries[2].Data[0] = 9
	got := result.values()
	if len(got) != 3 {
		t.Fatalf("Entries length = %d, want 3", len(got))
	}
	if got[0].Data != nil {
		t.Fatalf("nil Entry.Data became %#v", got[0].Data)
	}
	if got[1].Data == nil || len(got[1].Data) != 0 {
		t.Fatalf("empty non-nil Entry.Data became %#v", got[1].Data)
	}
	if !reflect.DeepEqual(got[2].Data, []byte{1, 2, 3}) {
		t.Fatalf("deep-copied Entry.Data = %v", got[2].Data)
	}
}

func TestCGoStorageSnapshotCallbackCopiesAndPreservesNil(t *testing.T) {
	confState := &pb.ConfState{
		Voters:    []uint64{1, 2},
		Learners:  []uint64{3},
		AutoLeave: storageTestBool(false),
	}
	snapshot := &pb.Snapshot{
		Data: []byte{4, 5, 6},
		Metadata: &pb.SnapshotMetadata{
			ConfState: confState,
			Index:     storageTestUint64(8),
			Term:      storageTestUint64(6),
		},
	}
	storage := &callbackTestStorage{
		snapshot: func() (*pb.Snapshot, error) { return snapshot, nil },
	}
	handle, _ := newStorageCallbackHandle(storage)
	defer handle.Delete()

	result := callStorageSnapshot(handle)
	if err := storageCallbackError(result.code); err != nil {
		t.Fatal(err)
	}
	snapshot.Data[0] = 9
	confState.Voters[0] = 99
	got := result.value()
	result.free()
	if !reflect.DeepEqual(got.Data, []byte{4, 5, 6}) ||
		got.GetMetadata().GetIndex() != 8 ||
		got.GetMetadata().GetTerm() != 6 ||
		!reflect.DeepEqual(got.GetMetadata().GetConfState().Voters, []uint64{1, 2}) {
		t.Fatalf("unexpected Snapshot copy: %v", got)
	}

	for _, test := range []struct {
		name string
		data []byte
		nil  bool
	}{
		{name: "nil", data: nil, nil: true},
		{name: "empty", data: []byte{}, nil: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			snapshot.Data = test.data
			result := callStorageSnapshot(handle)
			defer result.free()
			if err := storageCallbackError(result.code); err != nil {
				t.Fatal(err)
			}
			got := result.value().Data
			if (got == nil) != test.nil || len(got) != 0 {
				t.Fatalf("Snapshot.Data = %#v, want nil=%t", got, test.nil)
			}
		})
	}
}

func TestCGoStorageCallbackErrorAndPanicMapping(t *testing.T) {
	var termErr error
	var snapshotErr error
	storage := &callbackTestStorage{
		term:     func(uint64) (uint64, error) { return 0, termErr },
		snapshot: func() (*pb.Snapshot, error) { return nil, snapshotErr },
	}
	handle, bridge := newStorageCallbackHandle(storage)
	defer handle.Delete()

	for _, test := range []struct {
		name string
		err  error
		want error
	}{
		{name: "compacted", err: ErrCompacted, want: ErrCompacted},
		{name: "unavailable", err: ErrUnavailable, want: ErrUnavailable},
		{name: "unknown", err: errors.New("storage failed"), want: errCFatal},
	} {
		t.Run(test.name, func(t *testing.T) {
			termErr = test.err
			code, _ := callStorageTerm(handle, 1)
			if got := storageCallbackError(code); !errors.Is(got, test.want) {
				t.Fatalf("callback error = %v, want %v", got, test.want)
			}
		})
	}

	snapshotErr = ErrSnapshotTemporarilyUnavailable
	result := callStorageSnapshot(handle)
	result.free()
	if got := storageCallbackError(result.code); !errors.Is(got, ErrSnapshotTemporarilyUnavailable) {
		t.Fatalf("Snapshot callback error = %v", got)
	}

	storage.term = func(uint64) (uint64, error) {
		panic("callback panic")
	}
	code, _ := callStorageTerm(handle, 1)
	if got := storageCallbackError(code); !errors.Is(got, errCCallbackPanic) {
		t.Fatalf("panic callback error = %v, want %v", got, errCCallbackPanic)
	}
	if bridge.recordedError() == nil {
		t.Fatal("callback panic diagnostic was not retained")
	}
}

func TestCGoStorageCallbackRejectsInvalidGoResults(t *testing.T) {
	tests := []struct {
		name string
		run  func(cgo.Handle) int
	}{
		{
			name: "nil ConfState",
			run: func(handle cgo.Handle) int {
				result := callStorageInitialState(handle)
				defer result.free()
				return result.code
			},
		},
		{
			name: "nil Entry",
			run: func(handle cgo.Handle) int {
				result := callStorageEntries(handle, 1, 2, 1)
				defer result.free()
				return result.code
			},
		},
		{
			name: "nil Snapshot",
			run: func(handle cgo.Handle) int {
				result := callStorageSnapshot(handle)
				defer result.free()
				return result.code
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			storage := &callbackTestStorage{
				initialState: func() (*pb.HardState, *pb.ConfState, error) {
					return nil, nil, nil
				},
				entries:  func(uint64, uint64, uint64) ([]*pb.Entry, error) { return []*pb.Entry{nil}, nil },
				snapshot: func() (*pb.Snapshot, error) { return nil, nil },
			}
			handle, _ := newStorageCallbackHandle(storage)
			defer handle.Delete()
			if got := storageCallbackError(test.run(handle)); !errors.Is(got, errCInvalidArgument) {
				t.Fatalf("callback error = %v, want %v", got, errCInvalidArgument)
			}
		})
	}
}

func TestCGoStorageHandleLifecycle(t *testing.T) {
	storage := &callbackTestStorage{
		term: func(uint64) (uint64, error) { return 17, nil },
	}
	cfg := cgoSkeletonConfig()
	cfg.Storage = storage
	rn, err := NewRawNode(cfg)
	if err != nil {
		t.Fatal(err)
	}
	handle := rn.storageHandle
	if code, term := callStorageTerm(handle, 1); storageCallbackError(code) != nil || term != 17 {
		t.Fatalf("live handle callback = (%d, %d, %v)", code, term, storageCallbackError(code))
	}

	rn.destroy()
	if code, _ := callStorageTerm(handle, 1); !errors.Is(storageCallbackError(code), errCCallbackPanic) {
		t.Fatalf("deleted handle callback = %v, want callback panic", storageCallbackError(code))
	}
	assertStorageHandleDeleted(t, handle)
}

func TestCGoStorageHandleDeletedWhenCConstructionFails(t *testing.T) {
	cfg := cgoSkeletonConfig()
	// Go's current Config.validate does not reject unknown ReadOnlyOption
	// values, while the C public constructor correctly does. This reaches the
	// post-handle C-construction failure path without allocation fault
	// injection.
	cfg.ReadOnlyOption = ReadOnlyOption(99)
	var observed cgo.Handle
	if _, err := newRawNode(cfg, func(handle cgo.Handle) {
		observed = handle
	}); !errors.Is(err, errCInvalidArgument) {
		t.Fatalf("NewRawNode error = %v, want %v", err, errCInvalidArgument)
	}
	if observed == 0 {
		t.Fatal("storage handle was not created before the C constructor")
	}
	assertStorageHandleDeleted(t, observed)
}
