// Copyright 2026 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package raft

import (
	"context"
	"errors"
	"testing"
	"time"

	pb "go.etcd.io/raft/v3/raftpb"
)

type fatalStorageParityStorage struct {
	*MemoryStorage
	initialStateErr error
	entriesErr      error
	termErr         error
	firstIndexErr   error
	lastIndexErr    error
	snapshotErr     error
	snapshotCalls   int
}

func (s *fatalStorageParityStorage) InitialState() (*pb.HardState, *pb.ConfState, error) {
	if s.initialStateErr != nil {
		return nil, nil, s.initialStateErr
	}
	return s.MemoryStorage.InitialState()
}

func (s *fatalStorageParityStorage) Entries(
	lo, hi, maxSize uint64,
) ([]*pb.Entry, error) {
	if s.entriesErr != nil {
		return nil, s.entriesErr
	}
	return s.MemoryStorage.Entries(lo, hi, maxSize)
}

func (s *fatalStorageParityStorage) LastIndex() (uint64, error) {
	if s.lastIndexErr != nil {
		return 0, s.lastIndexErr
	}
	return s.MemoryStorage.LastIndex()
}

func (s *fatalStorageParityStorage) Term(index uint64) (uint64, error) {
	if s.termErr != nil {
		return 0, s.termErr
	}
	return s.MemoryStorage.Term(index)
}

func (s *fatalStorageParityStorage) FirstIndex() (uint64, error) {
	if s.firstIndexErr != nil {
		return 0, s.firstIndexErr
	}
	return s.MemoryStorage.FirstIndex()
}

func (s *fatalStorageParityStorage) Snapshot() (*pb.Snapshot, error) {
	s.snapshotCalls++
	if s.snapshotErr != nil {
		return nil, s.snapshotErr
	}
	return s.MemoryStorage.Snapshot()
}

func fatalStorageParityConfig(storage Storage) *Config {
	return &Config{
		ID:              1,
		ElectionTick:    10,
		HeartbeatTick:   1,
		Storage:         storage,
		MaxSizePerMsg:   noLimit,
		MaxInflightMsgs: 8,
		Logger:          discardLogger,
	}
}

func captureFatalStorageParityPanic(fn func()) (recovered any) {
	defer func() {
		recovered = recover()
	}()
	fn()
	return nil
}

func fatalStorageParityWithSnapshot(
	t *testing.T, index uint64,
) *fatalStorageParityStorage {
	t.Helper()
	storage := &fatalStorageParityStorage{
		MemoryStorage: NewMemoryStorage(),
	}
	if err := storage.ApplySnapshot(&pb.Snapshot{
		Metadata: &pb.SnapshotMetadata{
			Index:     new(index),
			Term:      new(uint64(1)),
			ConfState: &pb.ConfState{Voters: []uint64{1, 2}},
		},
	}); err != nil {
		t.Fatal(err)
	}
	return storage
}

func fatalStorageParityElectLeader(
	t *testing.T, rn *RawNode, storage *fatalStorageParityStorage,
) {
	t.Helper()
	if err := rn.Campaign(); err != nil {
		t.Fatal(err)
	}
	ready := rn.Ready()
	if err := storage.Append(ready.Entries); err != nil {
		t.Fatal(err)
	}
	if !IsEmptyHardState(ready.HardState) {
		if err := storage.SetHardState(ready.HardState); err != nil {
			t.Fatal(err)
		}
	}
	rn.Advance(ready)
	if err := rn.Step(&pb.Message{
		Type: pb.MsgVoteResp.Enum(),
		To:   new(uint64(1)),
		From: new(uint64(2)),
		Term: new(uint64(1)),
	}); err != nil {
		t.Fatal(err)
	}
	if got := rn.Status().RaftState; got != StateLeader {
		t.Fatalf("state = %v, want leader", got)
	}
}

func fatalStorageParityVote() *pb.Message {
	return &pb.Message{
		Type:    pb.MsgVote.Enum(),
		To:      new(uint64(1)),
		From:    new(uint64(2)),
		Term:    new(uint64(1)),
		LogTerm: new(uint64(1)),
		Index:   new(uint64(1)),
	}
}

func TestBootstrapStorageErrorParity(t *testing.T) {
	sentinel := errors.New("bootstrap last-index failure")
	storage := &fatalStorageParityStorage{
		MemoryStorage: NewMemoryStorage(),
	}
	rn, err := NewRawNode(fatalStorageParityConfig(storage))
	if err != nil {
		t.Fatal(err)
	}

	storage.lastIndexErr = sentinel
	if err := rn.Bootstrap([]Peer{{ID: 1}}); !errors.Is(err, sentinel) {
		t.Fatalf("Bootstrap error = %v, want sentinel", err)
	}
	storage.lastIndexErr = nil
	if err := rn.Bootstrap([]Peer{{ID: 1}}); err != nil {
		t.Fatalf("Bootstrap after recoverable failure: %v", err)
	}
}

func TestSnapshotTemporaryBootstrapErrorParity(t *testing.T) {
	storage := &fatalStorageParityStorage{
		MemoryStorage: NewMemoryStorage(),
	}
	rn, err := NewRawNode(fatalStorageParityConfig(storage))
	if err != nil {
		t.Fatal(err)
	}

	storage.lastIndexErr = ErrSnapshotTemporarilyUnavailable
	if err := rn.Bootstrap([]Peer{{ID: 1}}); !errors.Is(
		err, ErrSnapshotTemporarilyUnavailable,
	) {
		t.Fatalf("Bootstrap error = %v, want snapshot-temporary", err)
	}
	storage.lastIndexErr = nil
	if err := rn.Bootstrap([]Peer{{ID: 1}}); err != nil {
		t.Fatalf("Bootstrap after recoverable failure: %v", err)
	}
}

func TestSnapshotTemporaryConstructorErrorsPanicParity(t *testing.T) {
	for _, test := range []struct {
		name string
		set  func(*fatalStorageParityStorage)
	}{
		{
			name: "InitialState",
			set: func(storage *fatalStorageParityStorage) {
				storage.initialStateErr = ErrSnapshotTemporarilyUnavailable
			},
		},
		{
			name: "FirstIndex",
			set: func(storage *fatalStorageParityStorage) {
				storage.firstIndexErr = ErrSnapshotTemporarilyUnavailable
			},
		},
		{
			name: "LastIndex",
			set: func(storage *fatalStorageParityStorage) {
				storage.lastIndexErr = ErrSnapshotTemporarilyUnavailable
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			storage := &fatalStorageParityStorage{
				MemoryStorage: NewMemoryStorage(),
			}
			test.set(storage)
			if recovered := captureFatalStorageParityPanic(func() {
				_, _ = NewRawNode(fatalStorageParityConfig(storage))
			}); recovered == nil {
				t.Fatalf("Storage.%s snapshot-temporary error did not panic", test.name)
			}
		})
	}
}

func TestUnexpectedTermStorageErrorPanicsParity(t *testing.T) {
	sentinel := errors.New("term failure")
	storage := &fatalStorageParityStorage{
		MemoryStorage: NewMemoryStorage(),
	}
	if err := storage.ApplySnapshot(&pb.Snapshot{
		Metadata: &pb.SnapshotMetadata{
			Index:     new(uint64(1)),
			Term:      new(uint64(1)),
			ConfState: &pb.ConfState{Voters: []uint64{1, 2}},
		},
	}); err != nil {
		t.Fatal(err)
	}
	rn, err := NewRawNode(fatalStorageParityConfig(storage))
	if err != nil {
		t.Fatal(err)
	}
	storage.termErr = sentinel

	if recovered := captureFatalStorageParityPanic(func() {
		_ = rn.Step(&pb.Message{
			Type:    pb.MsgVote.Enum(),
			To:      new(uint64(1)),
			From:    new(uint64(2)),
			Term:    new(uint64(1)),
			LogTerm: new(uint64(1)),
			Index:   new(uint64(1)),
		})
	}); recovered == nil {
		t.Fatal("unexpected Storage.Term error did not panic")
	}
}

func TestSnapshotTemporaryLogLookupErrorsPanicParity(t *testing.T) {
	for _, test := range []struct {
		name string
		set  func(*fatalStorageParityStorage)
	}{
		{
			name: "Term",
			set: func(storage *fatalStorageParityStorage) {
				storage.termErr = ErrSnapshotTemporarilyUnavailable
			},
		},
		{
			name: "FirstIndex",
			set: func(storage *fatalStorageParityStorage) {
				storage.firstIndexErr = ErrSnapshotTemporarilyUnavailable
			},
		},
		{
			name: "LastIndex",
			set: func(storage *fatalStorageParityStorage) {
				storage.lastIndexErr = ErrSnapshotTemporarilyUnavailable
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			storage := fatalStorageParityWithSnapshot(t, 1)
			rn, err := NewRawNode(fatalStorageParityConfig(storage))
			if err != nil {
				t.Fatal(err)
			}
			test.set(storage)
			vote := fatalStorageParityVote()
			if recovered := captureFatalStorageParityPanic(func() {
				_ = rn.Step(vote)
			}); recovered == nil {
				t.Fatalf("Storage.%s snapshot-temporary error did not panic", test.name)
			}
			if recovered := captureFatalStorageParityPanic(func() {
				_ = rn.Step(vote)
			}); recovered == nil {
				t.Fatalf("repeated Storage.%s failure did not panic", test.name)
			}
		})
	}
}

func TestSnapshotTemporaryEntriesErrorPanicsInReadyParity(t *testing.T) {
	storage := fatalStorageParityWithSnapshot(t, 1)
	if err := storage.Append([]*pb.Entry{{
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
	rn, err := NewRawNode(fatalStorageParityConfig(storage))
	if err != nil {
		t.Fatal(err)
	}
	storage.entriesErr = ErrSnapshotTemporarilyUnavailable

	if recovered := captureFatalStorageParityPanic(func() {
		_ = rn.Ready()
	}); recovered == nil {
		t.Fatal("Storage.Entries snapshot-temporary error did not panic in Ready")
	}
	if recovered := captureFatalStorageParityPanic(func() {
		_ = rn.Ready()
	}); recovered == nil {
		t.Fatal("repeated Storage.Entries failure did not panic")
	}
}

func TestSnapshotTemporarySnapshotErrorRetriesParity(t *testing.T) {
	storage := fatalStorageParityWithSnapshot(t, 5)
	rn, err := NewRawNode(fatalStorageParityConfig(storage))
	if err != nil {
		t.Fatal(err)
	}
	fatalStorageParityElectLeader(t, rn, storage)

	storage.snapshotErr = ErrSnapshotTemporarilyUnavailable
	if err := rn.Step(&pb.Message{
		Type:       pb.MsgAppResp.Enum(),
		To:         new(uint64(1)),
		From:       new(uint64(2)),
		Term:       new(uint64(1)),
		Index:      new(uint64(5)),
		Reject:     new(true),
		RejectHint: new(uint64(0)),
	}); err != nil {
		t.Fatalf("temporary Snapshot error = %v", err)
	}
	if storage.snapshotCalls != 1 {
		t.Fatalf("Snapshot calls = %d, want 1", storage.snapshotCalls)
	}

	storage.snapshotErr = nil
	if err := rn.Step(&pb.Message{
		Type: pb.MsgHeartbeatResp.Enum(),
		To:   new(uint64(1)),
		From: new(uint64(2)),
		Term: new(uint64(1)),
	}); err != nil {
		t.Fatalf("snapshot retry = %v", err)
	}
	rd := rn.Ready()
	var found bool
	for _, message := range rd.Messages {
		found = found || message.GetType() == pb.MsgSnap
	}
	if !found || storage.snapshotCalls != 2 {
		t.Fatalf("retry found=%v Snapshot calls=%d", found, storage.snapshotCalls)
	}
}

func TestUnexpectedSnapshotStorageErrorPanicsParity(t *testing.T) {
	storage := fatalStorageParityWithSnapshot(t, 5)
	rn, err := NewRawNode(fatalStorageParityConfig(storage))
	if err != nil {
		t.Fatal(err)
	}
	fatalStorageParityElectLeader(t, rn, storage)

	storage.snapshotErr = errors.New("snapshot failure")
	if recovered := captureFatalStorageParityPanic(func() {
		_ = rn.Step(&pb.Message{
			Type:       pb.MsgAppResp.Enum(),
			To:         new(uint64(1)),
			From:       new(uint64(2)),
			Term:       new(uint64(1)),
			Index:      new(uint64(5)),
			Reject:     new(true),
			RejectHint: new(uint64(0)),
		})
	}); recovered == nil {
		t.Fatal("unexpected Snapshot error did not panic")
	}
}

func TestNodeUnexpectedTermStorageErrorPanicsParity(t *testing.T) {
	sentinel := errors.New("node term failure")
	storage := &fatalStorageParityStorage{
		MemoryStorage: NewMemoryStorage(),
	}
	if err := storage.ApplySnapshot(&pb.Snapshot{
		Metadata: &pb.SnapshotMetadata{
			Index:     new(uint64(1)),
			Term:      new(uint64(1)),
			ConfState: &pb.ConfState{Voters: []uint64{1, 2}},
		},
	}); err != nil {
		t.Fatal(err)
	}
	rn, err := NewRawNode(fatalStorageParityConfig(storage))
	if err != nil {
		t.Fatal(err)
	}
	n := newNode(rn)
	panicc := make(chan any, 1)
	go func() {
		defer func() {
			recovered := recover()
			close(n.done)
			panicc <- recovered
		}()
		n.run()
	}()
	storage.termErr = sentinel
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err = n.Step(ctx, &pb.Message{
		Type:    pb.MsgVote.Enum(),
		To:      new(uint64(1)),
		From:    new(uint64(2)),
		Term:    new(uint64(1)),
		LogTerm: new(uint64(1)),
		Index:   new(uint64(1)),
	})
	if err != nil && !errors.Is(err, ErrStopped) {
		t.Fatalf("Node.Step error = %v", err)
	}
	select {
	case recovered := <-panicc:
		if recovered == nil {
			t.Fatal("Node actor exited without panicking")
		}
	case <-ctx.Done():
		t.Fatal("Node actor did not surface fatal storage error")
	}
}

func TestNodeSnapshotTemporaryFromTermPanicsParity(t *testing.T) {
	storage := fatalStorageParityWithSnapshot(t, 1)
	rn, err := NewRawNode(fatalStorageParityConfig(storage))
	if err != nil {
		t.Fatal(err)
	}
	n := newNode(rn)
	panicc := make(chan any, 1)
	go func() {
		defer func() {
			recovered := recover()
			close(n.done)
			panicc <- recovered
		}()
		n.run()
	}()
	storage.termErr = ErrSnapshotTemporarilyUnavailable
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err = n.Step(ctx, fatalStorageParityVote())
	if err != nil && !errors.Is(err, ErrStopped) {
		t.Fatalf("Node.Step error = %v", err)
	}
	select {
	case recovered := <-panicc:
		if recovered == nil {
			t.Fatal("Node actor exited without panicking")
		}
	case <-ctx.Done():
		t.Fatal("Node actor did not surface snapshot-temporary Term error")
	}
}
