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

import "testing"

func TestCGoNodeStopDestroysRawNode(t *testing.T) {
	publicNode, _ := newNodeLifetimeParityNode(t, false)
	node := publicNode.(*node)
	rawNode := node.rn
	handle := rawNode.storageHandle

	if rawNode.p == nil || handle == 0 {
		t.Fatal("Node does not own a live C RawNode")
	}
	node.Stop()

	if node.rn != nil {
		t.Fatal("stopped Node retained its RawNode owner")
	}
	if rawNode.p != nil {
		t.Fatal("Node.Stop did not destroy the C RawNode")
	}
	if rawNode.pendingReady != nil {
		t.Fatal("Node.Stop retained a pending C Ready")
	}
	if rawNode.storageHandle != 0 {
		t.Fatal("Node.Stop retained its storage cgo.Handle")
	}

	handleDeleted := false
	func() {
		defer func() {
			handleDeleted = recover() != nil
		}()
		_ = handle.Value()
	}()
	if !handleDeleted {
		t.Fatal("Node.Stop did not delete the storage cgo.Handle")
	}

	// Both owner-side cleanup hooks remain idempotent.
	node.Stop()
	rawNode.destroy()
}
