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

raft_storage_ops_t raft_go_storage_ops(uintptr_t handle) {
    raft_storage_ops_t ops = {
        .handle = handle,
        .initial_state = goRaftStorageInitialState,
        .entries = goRaftStorageEntries,
        .term = goRaftStorageTerm,
        .first_index = goRaftStorageFirstIndex,
        .last_index = goRaftStorageLastIndex,
        .snapshot = goRaftStorageSnapshot,
    };
    return ops;
}
