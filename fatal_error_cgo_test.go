//go:build cgo_raft && cgo

// Copyright 2026 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package raft

import (
	"errors"
	"runtime/cgo"
	"testing"

	pb "go.etcd.io/raft/v3/raftpb"
)

func cgoCapturePanic(fn func()) (recovered any) {
	defer func() {
		recovered = recover()
	}()
	fn()
	return nil
}

func TestCGoConstructorAllocationFailurePanicsAndDeletesHandle(
	t *testing.T,
) {
	cfg := cgoSkeletonConfig()
	cfg.Logger = discardLogger
	var observed cgo.Handle

	cgoFailAllocationAfter(0)
	recovered := cgoCapturePanic(func() {
		_, _ = newRawNode(cfg, func(handle cgo.Handle) {
			observed = handle
		})
	})
	cgoResetAllocationFailure()
	if recovered == nil {
		t.Fatal("C constructor allocation failure did not panic")
	}
	if observed == 0 {
		t.Fatal("storage handle was not observed")
	}
	assertStorageHandleDeleted(t, observed)
}

func TestCGoReadyAllocationFailurePanicsAndStaysTerminal(t *testing.T) {
	cfg := cgoSkeletonConfig()
	cfg.Logger = discardLogger
	rn, err := NewRawNode(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer rn.destroy()
	if err := rn.Bootstrap([]Peer{{ID: 1}}); err != nil {
		t.Fatal(err)
	}

	cgoFailAllocationAfter(0)
	recovered := cgoCapturePanic(func() {
		_ = rn.Ready()
	})
	cgoResetAllocationFailure()
	if recovered == nil {
		t.Fatal("Ready allocation failure did not panic")
	}
	if recovered := cgoCapturePanic(func() {
		_ = rn.Campaign()
	}); recovered == nil {
		t.Fatal("Ready allocation failure was not sticky")
	}
	if recovered := cgoCapturePanic(func() {
		rn.TickQuiesced()
	}); recovered == nil {
		t.Fatal("TickQuiesced ignored the sticky allocation failure")
	}
}

func TestCGoInputArenaAllocationFailurePanicsAndCleansUp(t *testing.T) {
	cfg := cgoSkeletonConfig()
	cfg.Logger = discardLogger
	rn, err := NewRawNode(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer rn.destroy()
	change := &pb.ConfChangeV2{
		Changes: []*pb.ConfChangeSingle{{
			Type:   pb.ConfChangeAddNode.Enum(),
			NodeId: new(uint64(1)),
		}},
	}

	cgoFailAllocationAfter(0)
	recovered := cgoCapturePanic(func() {
		_ = rn.ProposeConfChange(change)
	})
	cgoResetAllocationFailure()
	if recovered == nil {
		t.Fatal("input arena allocation failure did not panic")
	}
	if err := rn.Propose(nil); !errors.Is(err, ErrProposalDropped) {
		t.Fatalf("RawNode after pre-call allocation cleanup = %v", err)
	}
}

func TestCGoAsyncReadyNestedAllocationFailurePanics(t *testing.T) {
	var sawNestedFailure bool

	for failAfter := uint64(1); failAfter < 24; failAfter++ {
		cfg := cgoSkeletonConfig()
		cfg.Logger = discardLogger
		cfg.AsyncStorageWrites = true
		rn, err := NewRawNode(cfg)
		if err != nil {
			t.Fatal(err)
		}
		if err := rn.Bootstrap([]Peer{{ID: 1}}); err != nil {
			rn.destroy()
			t.Fatal(err)
		}

		cgoFailAllocationAfter(failAfter)
		recovered := cgoCapturePanic(func() {
			_ = rn.Ready()
		})
		cgoResetAllocationFailure()
		if recovered != nil {
			sawNestedFailure = true
		}
		rn.destroy()
	}
	if !sawNestedFailure {
		t.Fatal("no nested async Ready allocation was fault-injected")
	}
}

func TestCGoNodePathStorageCallbackPanicIsFatal(t *testing.T) {
	storage := &callbackTestStorage{
		initialState: func() (*pb.HardState, *pb.ConfState, error) {
			return &pb.HardState{},
				&pb.ConfState{Voters: []uint64{1, 2}},
				nil
		},
		lastIndex: func() (uint64, error) {
			return 1, nil
		},
		term: func(uint64) (uint64, error) {
			panic("term callback panic")
		},
	}
	cfg := cgoSkeletonConfig()
	cfg.Logger = discardLogger
	cfg.Storage = storage
	rn, err := NewRawNode(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer rn.destroy()
	vote := &pb.Message{
		Type:    pb.MsgVote.Enum(),
		To:      new(uint64(1)),
		From:    new(uint64(2)),
		Term:    new(uint64(1)),
		LogTerm: new(uint64(1)),
		Index:   new(uint64(1)),
	}

	if recovered := cgoCapturePanic(func() {
		_ = rn.stepForNode(vote)
	}); recovered == nil {
		t.Fatal("Node-path storage callback panic did not panic")
	}
	if recovered := cgoCapturePanic(func() {
		_ = rn.stepForNode(vote)
	}); recovered == nil {
		t.Fatal("Node-path callback failure was not sticky")
	}
}

func TestCGoBootstrapStorageErrorRemainsRecoverableBeforeMutation(
	t *testing.T,
) {
	sentinel := errors.New("bootstrap last-index failure")
	fail := false
	storage := &callbackTestStorage{
		lastIndex: func() (uint64, error) {
			if fail {
				return 0, sentinel
			}
			return 0, nil
		},
	}
	cfg := cgoSkeletonConfig()
	cfg.Logger = discardLogger
	cfg.Storage = storage
	rn, err := NewRawNode(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer rn.destroy()

	fail = true
	if err := rn.Bootstrap([]Peer{{ID: 1}}); !errors.Is(err, sentinel) {
		t.Fatalf("Bootstrap error = %v, want sentinel", err)
	}
	fail = false
	if err := rn.Bootstrap([]Peer{{ID: 1}}); err != nil {
		t.Fatalf("Bootstrap after recoverable storage error: %v", err)
	}
}
