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

int raft_go_storage_call_initial_state(uintptr_t handle,
                                       raft_hard_state_t *hard_state,
                                       raft_conf_state_t *conf_state);
int raft_go_storage_call_entries(uintptr_t handle,
                                 uint64_t lo,
                                 uint64_t hi,
                                 uint64_t max_size,
                                 raft_entry_vec_t *entries);
int raft_go_storage_call_term(uintptr_t handle,
                              uint64_t index,
                              uint64_t *term);
int raft_go_storage_call_first_index(uintptr_t handle, uint64_t *index);
int raft_go_storage_call_last_index(uintptr_t handle, uint64_t *index);
int raft_go_storage_call_snapshot(uintptr_t handle,
                                  raft_snapshot_t *snapshot);
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

func (b *storageBridge) recordedError() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.lastErr
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
			*hardState = C.raft_hard_state_t{}
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
	if cs == nil {
		err := fmt.Errorf("raft/cgo: Storage.InitialState returned nil ConfState")
		bridge.recordError(err)
		return C.RAFT_ERR_INVALID_ARGUMENT
	}
	hardState.term = C.uint64_t(hs.GetTerm())
	hardState.vote = C.uint64_t(hs.GetVote())
	hardState.commit = C.uint64_t(hs.GetCommit())
	if !copyConfStateToC(confState, cs) {
		*hardState = C.raft_hard_state_t{}
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
		if entry == nil {
			C.raft_entry_vec_free(out)
			err := fmt.Errorf("raft/cgo: Storage.Entries returned nil entry at offset %d", i)
			bridge.recordError(err)
			return C.RAFT_ERR_INVALID_ARGUMENT
		}
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
	if snapshot == nil {
		err := fmt.Errorf("raft/cgo: Storage.Snapshot returned nil Snapshot")
		bridge.recordError(err)
		return C.RAFT_ERR_INVALID_ARGUMENT
	}
	if !copySnapshotToC(out, snapshot) {
		return C.RAFT_ERR_OUT_OF_MEMORY
	}
	return C.RAFT_OK
}

// The helpers below drive the exported callbacks through the C callback
// table. They keep pointer-bearing result descriptors in C memory and exist so
// the bridge can be tested before the consensus core begins consuming
// Storage. They are unexported and available only in the opt-in cgo build.

func storageCallbackError(code int) error {
	return decodeCError(C.int(code))
}

type storageInitialStateCallbackResult struct {
	code      int
	hardState *C.raft_hard_state_t
	confState *C.raft_conf_state_t
}

func callStorageInitialState(handle cgo.Handle) storageInitialStateCallbackResult {
	result := storageInitialStateCallbackResult{
		hardState: (*C.raft_hard_state_t)(C.calloc(1, C.size_t(unsafe.Sizeof(C.raft_hard_state_t{})))),
		confState: (*C.raft_conf_state_t)(C.calloc(1, C.size_t(unsafe.Sizeof(C.raft_conf_state_t{})))),
	}
	if result.hardState == nil || result.confState == nil {
		result.code = int(C.RAFT_ERR_OUT_OF_MEMORY)
		result.free()
		return result
	}
	result.code = int(C.raft_go_storage_call_initial_state(
		C.uintptr_t(handle), result.hardState, result.confState,
	))
	return result
}

func (r *storageInitialStateCallbackResult) values() (*pb.HardState, *pb.ConfState) {
	return cHardState(r.hardState), cConfState(r.confState)
}

func (r *storageInitialStateCallbackResult) free() {
	if r.confState != nil {
		C.raft_conf_state_free(r.confState)
		C.free(unsafe.Pointer(r.confState))
		r.confState = nil
	}
	if r.hardState != nil {
		C.free(unsafe.Pointer(r.hardState))
		r.hardState = nil
	}
}

type storageEntriesCallbackResult struct {
	code    int
	entries *C.raft_entry_vec_t
}

func callStorageEntries(
	handle cgo.Handle, lo, hi, maxSize uint64,
) storageEntriesCallbackResult {
	result := storageEntriesCallbackResult{
		entries: (*C.raft_entry_vec_t)(C.calloc(1, C.size_t(unsafe.Sizeof(C.raft_entry_vec_t{})))),
	}
	if result.entries == nil {
		result.code = int(C.RAFT_ERR_OUT_OF_MEMORY)
		return result
	}
	result.code = int(C.raft_go_storage_call_entries(
		C.uintptr_t(handle),
		C.uint64_t(lo),
		C.uint64_t(hi),
		C.uint64_t(maxSize),
		result.entries,
	))
	return result
}

func (r *storageEntriesCallbackResult) values() []*pb.Entry {
	if r.entries == nil {
		return nil
	}
	return cEntryVec(*r.entries)
}

func (r *storageEntriesCallbackResult) free() {
	if r.entries == nil {
		return
	}
	C.raft_entry_vec_free(r.entries)
	C.free(unsafe.Pointer(r.entries))
	r.entries = nil
}

func callStorageScalar(
	handle cgo.Handle,
	call func(*C.uint64_t) C.int,
) (int, uint64) {
	out := (*C.uint64_t)(C.calloc(1, C.size_t(unsafe.Sizeof(C.uint64_t(0)))))
	if out == nil {
		return int(C.RAFT_ERR_OUT_OF_MEMORY), 0
	}
	defer C.free(unsafe.Pointer(out))
	code := int(call(out))
	return code, uint64(*out)
}

func callStorageTerm(handle cgo.Handle, index uint64) (int, uint64) {
	return callStorageScalar(handle, func(out *C.uint64_t) C.int {
		return C.raft_go_storage_call_term(
			C.uintptr_t(handle), C.uint64_t(index), out,
		)
	})
}

func callStorageFirstIndex(handle cgo.Handle) (int, uint64) {
	return callStorageScalar(handle, func(out *C.uint64_t) C.int {
		return C.raft_go_storage_call_first_index(C.uintptr_t(handle), out)
	})
}

func callStorageLastIndex(handle cgo.Handle) (int, uint64) {
	return callStorageScalar(handle, func(out *C.uint64_t) C.int {
		return C.raft_go_storage_call_last_index(C.uintptr_t(handle), out)
	})
}

type storageSnapshotCallbackResult struct {
	code     int
	snapshot *C.raft_snapshot_t
}

func callStorageSnapshot(handle cgo.Handle) storageSnapshotCallbackResult {
	result := storageSnapshotCallbackResult{
		snapshot: (*C.raft_snapshot_t)(C.calloc(1, C.size_t(unsafe.Sizeof(C.raft_snapshot_t{})))),
	}
	if result.snapshot == nil {
		result.code = int(C.RAFT_ERR_OUT_OF_MEMORY)
		return result
	}
	result.code = int(C.raft_go_storage_call_snapshot(
		C.uintptr_t(handle), result.snapshot,
	))
	return result
}

func (r *storageSnapshotCallbackResult) value() *pb.Snapshot {
	return cSnapshot(r.snapshot)
}

func (r *storageSnapshotCallbackResult) free() {
	if r.snapshot == nil {
		return
	}
	C.raft_snapshot_free(r.snapshot)
	C.free(unsafe.Pointer(r.snapshot))
	r.snapshot = nil
}
