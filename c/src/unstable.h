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

#ifndef ETCD_RAFT_UNSTABLE_H
#define ETCD_RAFT_UNSTABLE_H

#include "raft/raft.h"

typedef struct raft_unstable {
    bool has_snapshot;
    raft_snapshot_t snapshot;
    bool snapshot_in_progress;

    raft_entry_vec_t entries;
    uint64_t offset;
    uint64_t offset_in_progress;
} raft_unstable_t;

void raft_unstable_init(raft_unstable_t *unstable, uint64_t offset);
void raft_unstable_free(raft_unstable_t *unstable);

bool raft_unstable_maybe_first_index(const raft_unstable_t *unstable,
                                     uint64_t *index);
bool raft_unstable_maybe_last_index(const raft_unstable_t *unstable,
                                    uint64_t *index);
bool raft_unstable_maybe_term(const raft_unstable_t *unstable,
                              uint64_t index,
                              uint64_t *term);

int raft_unstable_slice(const raft_unstable_t *unstable,
                        uint64_t lo,
                        uint64_t hi,
                        raft_entry_vec_t *out);
int raft_unstable_next_entries(const raft_unstable_t *unstable,
                               raft_entry_vec_t *out);
int raft_unstable_next_snapshot(const raft_unstable_t *unstable,
                                bool *has_snapshot,
                                raft_snapshot_t *out);
int raft_unstable_snapshot(const raft_unstable_t *unstable,
                           bool *has_snapshot,
                           raft_snapshot_t *out);
void raft_unstable_accept_in_progress(raft_unstable_t *unstable);

void raft_unstable_stable_to(raft_unstable_t *unstable,
                             uint64_t index,
                             uint64_t term);
void raft_unstable_stable_snap_to(raft_unstable_t *unstable, uint64_t index);
int raft_unstable_restore(raft_unstable_t *unstable,
                          const raft_snapshot_t *snapshot);
int raft_unstable_truncate_and_append(raft_unstable_t *unstable,
                                      const raft_entry_t *entries,
                                      size_t entry_count);

#endif  // ETCD_RAFT_UNSTABLE_H
