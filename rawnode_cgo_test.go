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

func TestCGoRawNodeSkeletonLifecycleAndBoundary(t *testing.T) {
	cfg := cgoSkeletonConfig()
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
		t.Fatal("inert skeleton unexpectedly has Ready work")
	}
	if rn.HasProgress(1) {
		t.Fatal("inert skeleton unexpectedly reports progress")
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
		"Campaign":          rn.Campaign(),
		"Propose-nil":       rn.Propose(nil),
		"Propose-empty":     rn.Propose([]byte{}),
		"ProposeConfChange": rn.ProposeConfChange(&pb.ConfChangeV2{}),
		"Step": rn.Step(&pb.Message{
			Type: pb.MsgApp.Enum(),
			From: new(uint64(2)),
			To:   new(uint64(1)),
		}),
		"Bootstrap":    rn.Bootstrap([]Peer{{ID: 1, Context: []byte{}}}),
		"ForgetLeader": rn.ForgetLeader(),
	} {
		if !errors.Is(err, errCNotImplemented) {
			t.Fatalf("%s error = %v, want not implemented", name, err)
		}
	}
	if err := rn.Step(&pb.Message{Type: pb.MsgHup.Enum()}); !errors.Is(err, ErrStepLocalMsg) {
		t.Fatalf("local Step error = %v, want ErrStepLocalMsg", err)
	}
	if err := rn.stepForNode(&pb.Message{Type: pb.MsgHup.Enum()}); !errors.Is(err, errCNotImplemented) {
		t.Fatalf("node-internal local Step error = %v, want not implemented", err)
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
