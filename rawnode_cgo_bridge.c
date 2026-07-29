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

#include "raft/raft.h"
#include "_cgo_export.h"

// The C skeleton lives in a separate subtree so default pure-Go builds never
// require a C compiler. This tagged translation unit incorporates it only for
// the opt-in cgo binding build.
#include "raw_node.c"

void raft_go_storage_ops_init(raft_storage_ops_t *ops, uintptr_t handle) {
    if (ops == NULL) {
        return;
    }
    *ops = (raft_storage_ops_t){
        .handle = handle,
        .initial_state = goRaftStorageInitialState,
        .entries = goRaftStorageEntries,
        .term = goRaftStorageTerm,
        .first_index = goRaftStorageFirstIndex,
        .last_index = goRaftStorageLastIndex,
        .snapshot = goRaftStorageSnapshot,
    };
}

// These small call-through helpers exercise the actual C callback table
// without requiring the still-inert consensus core. They are intentionally
// private to the tagged Go binding and are not part of raft/raft.h.
int raft_go_storage_call_initial_state(uintptr_t handle,
                                       raft_hard_state_t *hard_state,
                                       raft_conf_state_t *conf_state) {
    raft_storage_ops_t ops;
    raft_go_storage_ops_init(&ops, handle);
    return ops.initial_state(ops.handle, hard_state, conf_state);
}

int raft_go_storage_call_entries(uintptr_t handle,
                                 uint64_t lo,
                                 uint64_t hi,
                                 uint64_t max_size,
                                 raft_entry_vec_t *entries) {
    raft_storage_ops_t ops;
    raft_go_storage_ops_init(&ops, handle);
    return ops.entries(ops.handle, lo, hi, max_size, entries);
}

int raft_go_storage_call_term(uintptr_t handle,
                              uint64_t index,
                              uint64_t *term) {
    raft_storage_ops_t ops;
    raft_go_storage_ops_init(&ops, handle);
    return ops.term(ops.handle, index, term);
}

int raft_go_storage_call_first_index(uintptr_t handle, uint64_t *index) {
    raft_storage_ops_t ops;
    raft_go_storage_ops_init(&ops, handle);
    return ops.first_index(ops.handle, index);
}

int raft_go_storage_call_last_index(uintptr_t handle, uint64_t *index) {
    raft_storage_ops_t ops;
    raft_go_storage_ops_init(&ops, handle);
    return ops.last_index(ops.handle, index);
}

int raft_go_storage_call_snapshot(uintptr_t handle,
                                  raft_snapshot_t *snapshot) {
    raft_storage_ops_t ops;
    raft_go_storage_ops_init(&ops, handle);
    return ops.snapshot(ops.handle, snapshot);
}
