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

#ifndef ETCD_RAFT_TRACKER_H
#define ETCD_RAFT_TRACKER_H

#include "raft/raft.h"

typedef enum raft_vote_result_internal {
    RAFT_VOTE_PENDING = 0,
    RAFT_VOTE_LOST = 1,
    RAFT_VOTE_WON = 2,
} raft_vote_result_internal_t;

typedef struct raft_inflight_internal {
    uint64_t index;
    uint64_t bytes;
} raft_inflight_internal_t;

typedef struct raft_inflights_internal {
    size_t start;
    size_t count;
    uint64_t bytes;
    size_t size;
    uint64_t max_bytes;
    raft_inflight_internal_t *buffer;
    size_t capacity;
} raft_inflights_internal_t;

typedef struct raft_progress_internal {
    uint64_t id;
    uint64_t match_index;
    uint64_t next_index;
    uint64_t sent_commit;
    raft_progress_state_t state;
    uint64_t pending_snapshot;
    bool recent_active;
    bool message_flow_paused;
    bool is_learner;
    raft_inflights_internal_t inflights;
    bool vote_recorded;
    bool vote_granted;
} raft_progress_internal_t;

typedef struct raft_progress_tracker {
    raft_conf_state_t config;
    raft_progress_internal_t *progress;
    size_t progress_len;
    size_t max_inflight_messages;
    uint64_t max_inflight_bytes;
} raft_progress_tracker_t;

int raft_inflights_init(raft_inflights_internal_t *in,
                        size_t size,
                        uint64_t max_bytes);
void raft_inflights_free(raft_inflights_internal_t *in);
int raft_inflights_clone(raft_inflights_internal_t *dst,
                         const raft_inflights_internal_t *src);
void raft_inflights_reset(raft_inflights_internal_t *in);
bool raft_inflights_full(const raft_inflights_internal_t *in);
int raft_inflights_add(raft_inflights_internal_t *in,
                       uint64_t index,
                       uint64_t bytes);
void raft_inflights_free_le(raft_inflights_internal_t *in, uint64_t index);

int raft_progress_init(raft_progress_internal_t *progress,
                       uint64_t id,
                       uint64_t next,
                       bool is_learner,
                       size_t max_inflight_messages,
                       uint64_t max_inflight_bytes);
void raft_progress_free(raft_progress_internal_t *progress);
int raft_progress_clone(raft_progress_internal_t *dst,
                        const raft_progress_internal_t *src);
void raft_progress_reset_state(raft_progress_internal_t *progress,
                               raft_progress_state_t state);
void raft_progress_become_probe(raft_progress_internal_t *progress);
void raft_progress_become_replicate(raft_progress_internal_t *progress);
void raft_progress_become_snapshot(raft_progress_internal_t *progress,
                                   uint64_t snapshot_index);
int raft_progress_sent_entries(raft_progress_internal_t *progress,
                               size_t entry_count,
                               uint64_t bytes);
bool raft_progress_can_bump_commit(const raft_progress_internal_t *progress,
                                   uint64_t index);
void raft_progress_sent_commit(raft_progress_internal_t *progress,
                               uint64_t commit);
bool raft_progress_maybe_update(raft_progress_internal_t *progress,
                                uint64_t index);
bool raft_progress_maybe_decr_to(raft_progress_internal_t *progress,
                                 uint64_t rejected,
                                 uint64_t match_hint);
bool raft_progress_is_paused(const raft_progress_internal_t *progress);

int raft_tracker_init(raft_progress_tracker_t *tracker,
                      size_t max_inflight_messages,
                      uint64_t max_inflight_bytes);
void raft_tracker_free(raft_progress_tracker_t *tracker);
int raft_tracker_clone(raft_progress_tracker_t *dst,
                       const raft_progress_tracker_t *src);
void raft_tracker_swap(raft_progress_tracker_t *left,
                       raft_progress_tracker_t *right);

raft_progress_internal_t *raft_tracker_find(raft_progress_tracker_t *tracker,
                                            uint64_t id);
const raft_progress_internal_t *raft_tracker_find_const(
    const raft_progress_tracker_t *tracker, uint64_t id);
bool raft_tracker_has_progress(const raft_progress_tracker_t *tracker,
                               uint64_t id);
bool raft_tracker_is_voter(const raft_progress_tracker_t *tracker,
                           uint64_t id);
bool raft_tracker_is_singleton(const raft_progress_tracker_t *tracker);
bool raft_tracker_is_joint(const raft_progress_tracker_t *tracker);

int raft_tracker_add_progress(raft_progress_tracker_t *tracker,
                              uint64_t id,
                              uint64_t next,
                              bool is_learner);
void raft_tracker_remove_progress(raft_progress_tracker_t *tracker,
                                  uint64_t id);

void raft_tracker_reset_votes(raft_progress_tracker_t *tracker);
void raft_tracker_record_vote(raft_progress_tracker_t *tracker,
                              uint64_t id,
                              bool granted);
raft_vote_result_internal_t raft_tracker_vote_result(
    const raft_progress_tracker_t *tracker);
uint64_t raft_tracker_committed(const raft_progress_tracker_t *tracker);
bool raft_tracker_quorum_active(const raft_progress_tracker_t *tracker);

int raft_tracker_conf_state_copy(const raft_progress_tracker_t *tracker,
                                 raft_conf_state_t *out);
int raft_tracker_progress_snapshot(const raft_progress_tracker_t *tracker,
                                   raft_progress_snapshot_t **out,
                                   size_t *out_len);

bool raft_id_vec_contains(const raft_uint64_vec_t *vec, uint64_t id);
int raft_id_vec_insert(raft_uint64_vec_t *vec, uint64_t id);
void raft_id_vec_remove(raft_uint64_vec_t *vec, uint64_t id);
int raft_id_vec_copy(raft_uint64_vec_t *dst,
                     const raft_uint64_vec_t *src);
int raft_id_vec_union(const raft_uint64_vec_t *left,
                      const raft_uint64_vec_t *right,
                      raft_uint64_vec_t *out);

#endif  // ETCD_RAFT_TRACKER_H
