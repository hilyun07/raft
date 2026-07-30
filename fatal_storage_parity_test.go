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
	lastIndexErr error
	termErr      error
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
