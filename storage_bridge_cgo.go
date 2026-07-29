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

/*
#cgo CFLAGS: -I${SRCDIR}/c/include -I${SRCDIR}/c/src
#include <stdlib.h>
#include "raft/raft.h"
*/
import "C"

import (
	"fmt"
	"runtime/cgo"
	"sync"
	"unsafe"

	pb "go.etcd.io/raft/v3/raftpb"
)

type storageBridge struct {
	storage Storage

	mu      sync.Mutex
	lastErr error
}

func (b *storageBridge) recordError(err error) {
	if err == nil {
		return
	}
	b.mu.Lock()
	b.lastErr = err
	b.mu.Unlock()
}

func bridgeFromHandle(handle C.uintptr_t) *storageBridge {
	return cgo.Handle(handle).Value().(*storageBridge)
}

func recordStorageCallbackPanic(handle C.uintptr_t, recovered any) {
	// An invalid/deleted handle can itself panic. The callback still reports the
	// panic code even when there is no live bridge on which to retain details.
	defer func() { _ = recover() }()
	bridgeFromHandle(handle).recordError(
		fmt.Errorf("raft/cgo: storage callback panic: %v", recovered),
	)
}

func copyBytesToC(dst *C.raft_bytes_t, src []byte) bool {
	*dst = C.raft_bytes_t{}
	dst.is_nil = cBool(src == nil)
	dst.len = C.size_t(len(src))
	if len(src) == 0 {
		return true
	}
	dst.data = (*C.uint8_t)(C.malloc(C.size_t(len(src))))
	if dst.data == nil {
		*dst = C.raft_bytes_t{}
		dst.is_nil = cBool(true)
		return false
	}
	copy(unsafe.Slice((*byte)(unsafe.Pointer(dst.data)), len(src)), src)
	return true
}

func copyUint64sToC(dst *C.raft_uint64_vec_t, src []uint64) bool {
	*dst = C.raft_uint64_vec_t{}
	if len(src) == 0 {
		return true
	}
	elementSize := unsafe.Sizeof(C.uint64_t(0))
	if uintptr(len(src)) > ^uintptr(0)/elementSize {
		return false
	}
	size := uintptr(len(src)) * elementSize
	dst.items = (*C.uint64_t)(C.malloc(C.size_t(size)))
	if dst.items == nil {
		return false
	}
	dst.len = C.size_t(len(src))
	rows := unsafe.Slice(dst.items, len(src))
	for i, id := range src {
		rows[i] = C.uint64_t(id)
	}
	return true
}

func copyConfStateToC(dst *C.raft_conf_state_t, src *pb.ConfState) bool {
	*dst = C.raft_conf_state_t{}
	if src == nil {
		return true
	}
	dst.auto_leave = cBool(src.GetAutoLeave())
	if !copyUint64sToC(&dst.voters, src.Voters) ||
		!copyUint64sToC(&dst.voters_outgoing, src.VotersOutgoing) ||
		!copyUint64sToC(&dst.learners, src.Learners) ||
		!copyUint64sToC(&dst.learners_next, src.LearnersNext) {
		C.raft_conf_state_free(dst)
		return false
	}
	return true
}

func copyEntryToC(dst *C.raft_entry_t, src *pb.Entry) bool {
	*dst = C.raft_entry_t{}
	if src == nil {
		dst.data.is_nil = cBool(true)
		return true
	}
	dst._type = C.raft_entry_type_t(src.GetType())
	dst.term = C.uint64_t(src.GetTerm())
	dst.index = C.uint64_t(src.GetIndex())
	return copyBytesToC(&dst.data, src.Data)
}

func copySnapshotToC(dst *C.raft_snapshot_t, src *pb.Snapshot) bool {
	*dst = C.raft_snapshot_t{}
	if src == nil {
		dst.data.is_nil = cBool(true)
		return true
	}
	if !copyBytesToC(&dst.data, src.Data) {
		return false
	}
	metadata := src.GetMetadata()
	if metadata != nil {
		dst.metadata.index = C.uint64_t(metadata.GetIndex())
		dst.metadata.term = C.uint64_t(metadata.GetTerm())
		if !copyConfStateToC(&dst.metadata.conf_state, metadata.GetConfState()) {
			C.raft_snapshot_free(dst)
			return false
		}
	}
	return true
}

//export goRaftStorageInitialState
func goRaftStorageInitialState(
	handle C.uintptr_t,
	hardState *C.raft_hard_state_t,
	confState *C.raft_conf_state_t,
) (result C.int) {
	if hardState == nil || confState == nil {
		return C.RAFT_ERR_INVALID_ARGUMENT
	}
	*hardState = C.raft_hard_state_t{}
	*confState = C.raft_conf_state_t{}
	defer func() {
		if recovered := recover(); recovered != nil {
			C.raft_conf_state_free(confState)
			recordStorageCallbackPanic(handle, recovered)
			result = C.RAFT_ERR_PANIC_FROM_GO_CALLBACK
		}
	}()

	bridge := bridgeFromHandle(handle)
	hs, cs, err := bridge.storage.InitialState()
	if err != nil {
		bridge.recordError(err)
		return storageErrorCode(err)
	}
	hardState.term = C.uint64_t(hs.GetTerm())
	hardState.vote = C.uint64_t(hs.GetVote())
	hardState.commit = C.uint64_t(hs.GetCommit())
	if !copyConfStateToC(confState, cs) {
		return C.RAFT_ERR_OUT_OF_MEMORY
	}
	return C.RAFT_OK
}

//export goRaftStorageEntries
func goRaftStorageEntries(
	handle C.uintptr_t,
	lo C.uint64_t,
	hi C.uint64_t,
	maxSize C.uint64_t,
	out *C.raft_entry_vec_t,
) (result C.int) {
	if out == nil {
		return C.RAFT_ERR_INVALID_ARGUMENT
	}
	*out = C.raft_entry_vec_t{}
	defer func() {
		if recovered := recover(); recovered != nil {
			C.raft_entry_vec_free(out)
			recordStorageCallbackPanic(handle, recovered)
			result = C.RAFT_ERR_PANIC_FROM_GO_CALLBACK
		}
	}()

	bridge := bridgeFromHandle(handle)
	entries, err := bridge.storage.Entries(uint64(lo), uint64(hi), uint64(maxSize))
	if err != nil {
		bridge.recordError(err)
		return storageErrorCode(err)
	}
	if len(entries) == 0 {
		return C.RAFT_OK
	}
	elementSize := unsafe.Sizeof(C.raft_entry_t{})
	if uintptr(len(entries)) > ^uintptr(0)/elementSize {
		return C.RAFT_ERR_OUT_OF_MEMORY
	}
	out.items = (*C.raft_entry_t)(C.calloc(C.size_t(len(entries)), C.size_t(unsafe.Sizeof(C.raft_entry_t{}))))
	if out.items == nil {
		return C.RAFT_ERR_OUT_OF_MEMORY
	}
	out.len = C.size_t(len(entries))
	rows := unsafe.Slice(out.items, len(entries))
	for i, entry := range entries {
		if !copyEntryToC(&rows[i], entry) {
			C.raft_entry_vec_free(out)
			return C.RAFT_ERR_OUT_OF_MEMORY
		}
	}
	return C.RAFT_OK
}

//export goRaftStorageTerm
func goRaftStorageTerm(
	handle C.uintptr_t, index C.uint64_t, out *C.uint64_t,
) (result C.int) {
	if out == nil {
		return C.RAFT_ERR_INVALID_ARGUMENT
	}
	*out = 0
	defer func() {
		if recovered := recover(); recovered != nil {
			recordStorageCallbackPanic(handle, recovered)
			result = C.RAFT_ERR_PANIC_FROM_GO_CALLBACK
		}
	}()
	bridge := bridgeFromHandle(handle)
	term, err := bridge.storage.Term(uint64(index))
	if err != nil {
		bridge.recordError(err)
		return storageErrorCode(err)
	}
	*out = C.uint64_t(term)
	return C.RAFT_OK
}

//export goRaftStorageFirstIndex
func goRaftStorageFirstIndex(
	handle C.uintptr_t, out *C.uint64_t,
) (result C.int) {
	if out == nil {
		return C.RAFT_ERR_INVALID_ARGUMENT
	}
	*out = 0
	defer func() {
		if recovered := recover(); recovered != nil {
			recordStorageCallbackPanic(handle, recovered)
			result = C.RAFT_ERR_PANIC_FROM_GO_CALLBACK
		}
	}()
	bridge := bridgeFromHandle(handle)
	index, err := bridge.storage.FirstIndex()
	if err != nil {
		bridge.recordError(err)
		return storageErrorCode(err)
	}
	*out = C.uint64_t(index)
	return C.RAFT_OK
}

//export goRaftStorageLastIndex
func goRaftStorageLastIndex(
	handle C.uintptr_t, out *C.uint64_t,
) (result C.int) {
	if out == nil {
		return C.RAFT_ERR_INVALID_ARGUMENT
	}
	*out = 0
	defer func() {
		if recovered := recover(); recovered != nil {
			recordStorageCallbackPanic(handle, recovered)
			result = C.RAFT_ERR_PANIC_FROM_GO_CALLBACK
		}
	}()
	bridge := bridgeFromHandle(handle)
	index, err := bridge.storage.LastIndex()
	if err != nil {
		bridge.recordError(err)
		return storageErrorCode(err)
	}
	*out = C.uint64_t(index)
	return C.RAFT_OK
}

//export goRaftStorageSnapshot
func goRaftStorageSnapshot(
	handle C.uintptr_t, out *C.raft_snapshot_t,
) (result C.int) {
	if out == nil {
		return C.RAFT_ERR_INVALID_ARGUMENT
	}
	*out = C.raft_snapshot_t{}
	defer func() {
		if recovered := recover(); recovered != nil {
			C.raft_snapshot_free(out)
			recordStorageCallbackPanic(handle, recovered)
			result = C.RAFT_ERR_PANIC_FROM_GO_CALLBACK
		}
	}()
	bridge := bridgeFromHandle(handle)
	snapshot, err := bridge.storage.Snapshot()
	if err != nil {
		bridge.recordError(err)
		return storageErrorCode(err)
	}
	if !copySnapshotToC(out, snapshot) {
		return C.RAFT_ERR_OUT_OF_MEMORY
	}
	return C.RAFT_OK
}
