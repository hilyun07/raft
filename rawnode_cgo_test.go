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
	"testing"

	pb "go.etcd.io/raft/v3/raftpb"
	"go.etcd.io/raft/v3/tracker"
)

func cgoSkeletonConfig() *Config {
	return &Config{
		ID:              1,
		ElectionTick:    10,
		HeartbeatTick:   1,
		Storage:         NewMemoryStorage(),
		MaxSizePerMsg:   1024,
		MaxInflightMsgs: 16,
	}
}

func TestCGoRawNodeMinimalCoreLifecycleAndBoundary(t *testing.T) {
	cfg := cgoSkeletonConfig()
	storage := cfg.Storage.(*MemoryStorage)
	rn, err := NewRawNode(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if rn.ID() != cfg.ID {
		t.Fatalf("ID() = %d, want %d", rn.ID(), cfg.ID)
	}
	if rn.Logger() != cfg.Logger {
		t.Fatal("Logger() did not retain the validated Go logger")
	}
	if rn.AsyncStorageWritesEnabled() {
		t.Fatal("unexpected AsyncStorageWrites")
	}
	if rn.HasReady() {
		t.Fatal("new empty RawNode unexpectedly has Ready work")
	}
	if rn.HasProgress(1) {
		t.Fatal("new empty RawNode unexpectedly reports progress")
	}

	basic := rn.BasicStatus()
	if basic.ID != cfg.ID || basic.Lead != None || basic.RaftState != StateFollower {
		t.Fatalf("unexpected BasicStatus: %+v", basic)
	}
	status := rn.Status()
	if status.ID != cfg.ID || len(status.Progress) != 0 {
		t.Fatalf("unexpected Status: %+v", status)
	}

	for name, err := range map[string]error{
		"ProposeConfChange": rn.ProposeConfChange(&pb.ConfChangeV2{}),
	} {
		if !errors.Is(err, errCNotImplemented) {
			t.Fatalf("%s error = %v, want not implemented", name, err)
		}
	}
	if err := rn.Propose(nil); !errors.Is(err, ErrProposalDropped) {
		t.Fatalf("proposal without leader = %v, want proposal dropped", err)
	}

	if err := rn.Bootstrap([]Peer{{ID: 1, Context: []byte{}}}); err != nil {
		t.Fatalf("Bootstrap: %v", err)
	}
	if !rn.HasProgress(1) {
		t.Fatal("bootstrapped RawNode does not report local progress")
	}
	if err := rn.Campaign(); err != nil {
		t.Fatalf("Campaign: %v", err)
	}
	if err := rn.Propose([]byte("value")); err != nil {
		t.Fatalf("Propose: %v", err)
	}
	if !rn.HasReady() {
		t.Fatal("campaign and proposal did not produce Ready")
	}
	if err := rn.Step(&pb.Message{Type: pb.MsgHup.Enum()}); !errors.Is(err, ErrStepLocalMsg) {
		t.Fatalf("local Step error = %v, want ErrStepLocalMsg", err)
	}
	if err := rn.stepForNode(&pb.Message{Type: pb.MsgHup.Enum()}); err != nil {
		t.Fatalf("node-internal local Step error = %v", err)
	}

	rd := rn.Ready()
	if rd.SoftState == nil || rd.SoftState.RaftState != StateLeader {
		t.Fatalf("Ready SoftState = %+v, want leader", rd.SoftState)
	}
	if rd.HardState.GetTerm() != 2 || rd.HardState.GetVote() != 1 {
		t.Fatalf("Ready HardState = %+v, want term=2 vote=1", rd.HardState)
	}
	if len(rd.Entries) < 3 || len(rd.CommittedEntries) < 3 {
		t.Fatalf("Ready entries=%d committed=%d, want bootstrap/no-op/proposal", len(rd.Entries), len(rd.CommittedEntries))
	}
	if err := storage.Append(rd.Entries); err != nil {
		t.Fatal(err)
	}
	if !IsEmptyHardState(rd.HardState) {
		if err := storage.SetHardState(rd.HardState); err != nil {
			t.Fatal(err)
		}
	}
	rn.Advance(rd)

	visited := 0
	rn.WithProgress(func(id uint64, typ ProgressType, pr tracker.Progress) {
		visited++
		if id != 1 || typ != ProgressTypePeer {
			t.Fatalf("progress row = (%d,%v), want local peer", id, typ)
		}
		if pr.Inflights != nil {
			t.Fatal("WithProgress exposed inflights")
		}
	})
	if visited != 1 {
		t.Fatalf("WithProgress visited %d rows, want 1", visited)
	}
	if status := rn.Status(); status.RaftState != StateLeader || len(status.Progress) != 1 {
		t.Fatalf("unexpected leader Status: %+v", status)
	}
	if err := rn.ForgetLeader(); err != nil {
		t.Fatalf("ForgetLeader: %v", err)
	}

	rn.destroy()
	if err := rn.Campaign(); !errors.Is(err, ErrStopped) {
		t.Fatalf("Campaign after destroy = %v, want ErrStopped", err)
	}
}

func TestCGoRawNodeConfigValidation(t *testing.T) {
	cfg := cgoSkeletonConfig()
	cfg.ID = None
	if _, err := NewRawNode(cfg); err == nil {
		t.Fatal("NewRawNode accepted RAFT_NONE")
	}

	cfg = cgoSkeletonConfig()
	cfg.Storage = nil
	if _, err := NewRawNode(cfg); err == nil {
		t.Fatal("NewRawNode accepted nil Storage")
	}
}
