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

#ifndef ETCD_RAFT_LOG_H
#define ETCD_RAFT_LOG_H

#include "unstable.h"

typedef struct raft_log {
    raft_storage_ops_t storage;
    raft_unstable_t unstable;

    uint64_t committed;
    uint64_t applying;
    uint64_t applied;

    uint64_t max_applying_entries_size;
    uint64_t applying_entries_size;
    bool applying_entries_paused;
} raft_log_t;

typedef int (*raft_log_scan_fn)(void *context,
                                const raft_entry_t *entries,
                                size_t entry_count);

int raft_log_init(raft_log_t *log,
                  const raft_storage_ops_t *storage,
                  uint64_t max_applying_entries_size);
void raft_log_free(raft_log_t *log);

int raft_log_first_index(raft_log_t *log, uint64_t *index);
int raft_log_last_index(raft_log_t *log, uint64_t *index);
int raft_log_last_term(raft_log_t *log, uint64_t *term);
int raft_log_term(raft_log_t *log, uint64_t index, uint64_t *term);
bool raft_log_match_term(raft_log_t *log, uint64_t index, uint64_t term);
int raft_log_zero_term_on_out_of_bounds(int result,
                                        uint64_t term,
                                        uint64_t *out);

int raft_log_slice(raft_log_t *log,
                   uint64_t lo,
                   uint64_t hi,
                   uint64_t max_size,
                   raft_entry_vec_t *out);
int raft_log_entries(raft_log_t *log,
                     uint64_t from,
                     uint64_t max_size,
                     raft_entry_vec_t *out);
int raft_log_scan(raft_log_t *log,
                  uint64_t lo,
                  uint64_t hi,
                  uint64_t page_size,
                  raft_log_scan_fn visit,
                  void *context);

int raft_log_append(raft_log_t *log,
                    const raft_entry_t *entries,
                    size_t entry_count,
                    uint64_t *last_index);
int raft_log_find_conflict(raft_log_t *log,
                           const raft_entry_t *entries,
                           size_t entry_count,
                           uint64_t *conflict_index);
int raft_log_find_conflict_by_term(raft_log_t *log,
                                   uint64_t index,
                                   uint64_t term,
                                   uint64_t *conflict_index,
                                   uint64_t *conflict_term);
int raft_log_maybe_append(raft_log_t *log,
                          uint64_t previous_index,
                          uint64_t previous_term,
                          uint64_t leader_committed,
                          const raft_entry_t *entries,
                          size_t entry_count,
                          bool *appended,
                          uint64_t *last_new_index);

int raft_log_commit_to(raft_log_t *log, uint64_t to_commit);
int raft_log_maybe_commit(raft_log_t *log,
                          uint64_t index,
                          uint64_t term,
                          bool *committed);
int raft_log_applied_to(raft_log_t *log, uint64_t index, uint64_t size);
int raft_log_accept_applying(raft_log_t *log,
                             uint64_t index,
                             uint64_t size,
                             bool allow_unstable);

bool raft_log_has_next_committed_entries(const raft_log_t *log,
                                         bool allow_unstable);
int raft_log_next_committed_entries(raft_log_t *log,
                                    bool allow_unstable,
                                    raft_entry_vec_t *out);
bool raft_log_has_next_unstable_entries(const raft_log_t *log);
bool raft_log_has_next_or_in_progress_unstable_entries(const raft_log_t *log);
int raft_log_next_unstable_entries(const raft_log_t *log,
                                   raft_entry_vec_t *out);
bool raft_log_has_next_unstable_snapshot(const raft_log_t *log);
bool raft_log_has_next_or_in_progress_snapshot(const raft_log_t *log);
int raft_log_next_unstable_snapshot(const raft_log_t *log,
                                    bool *has_snapshot,
                                    raft_snapshot_t *out);
int raft_log_snapshot(raft_log_t *log, raft_snapshot_t *out);

void raft_log_accept_unstable(raft_log_t *log);
void raft_log_stable_to(raft_log_t *log, uint64_t index, uint64_t term);
void raft_log_stable_snap_to(raft_log_t *log, uint64_t index);
int raft_log_restore(raft_log_t *log, const raft_snapshot_t *snapshot);
bool raft_log_is_up_to_date(raft_log_t *log,
                            uint64_t candidate_index,
                            uint64_t candidate_term);

// Internal size helper. The current raft_entry_t lacks protobuf scalar
// presence bits, so zero-valued scalar fields are treated as absent.
uint64_t raft_log_entry_encoding_size(const raft_entry_t *entry);

#endif  // ETCD_RAFT_LOG_H
