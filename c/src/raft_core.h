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

#ifndef ETCD_RAFT_CORE_H
#define ETCD_RAFT_CORE_H

#include "log.h"

typedef struct raft_basic_progress_internal {
    uint64_t id;
    uint64_t match_index;
    uint64_t next_index;
    raft_progress_state_t state;
    uint64_t pending_snapshot;
    bool recent_active;
    bool message_flow_paused;
    bool is_learner;
    bool vote_recorded;
    bool vote_granted;
} raft_basic_progress_internal_t;

// Minimal Phase 7 state machine. This deliberately does not contain the full
// tracker/quorum/read-only/confchange machinery.
typedef struct raft {
    uint64_t id;
    uint64_t term;
    uint64_t vote;
    uint64_t lead;
    uint64_t lead_transferee;
    raft_state_t state;

    raft_log_t *log;
    raft_conf_state_t conf_state;
    raft_basic_progress_internal_t *progress;
    size_t progress_len;

    raft_message_vec_t messages;

    uint32_t election_timeout;
    uint32_t heartbeat_timeout;
    uint32_t randomized_election_timeout;
    uint32_t election_elapsed;
    uint32_t heartbeat_elapsed;

    uint64_t max_size_per_message;
    uint64_t max_uncommitted_entries_size;
    uint64_t uncommitted_size;
    bool disable_proposal_forwarding;

    // Sticky fatal/callback/storage failure produced by a void Tick call.
    int error;
} raft_t;

int raft_core_init(raft_t *raft,
                   const raft_config_t *config,
                   raft_log_t *log,
                   const raft_storage_ops_t *storage);
void raft_core_free(raft_t *raft);

int raft_core_bootstrap(raft_t *raft,
                        const raft_peer_view_t *peers,
                        size_t peer_count);
int raft_core_tick(raft_t *raft);
void raft_core_tick_quiesced(raft_t *raft);
int raft_core_campaign(raft_t *raft);
int raft_core_propose(raft_t *raft, const raft_byte_view_t *data);
int raft_core_step(raft_t *raft, const raft_message_view_t *message);

void raft_core_hard_state(const raft_t *raft, raft_hard_state_t *state);
void raft_core_soft_state(const raft_t *raft, raft_soft_state_t *state);
bool raft_core_has_progress(const raft_t *raft, uint64_t id);
int raft_core_progress_snapshot(const raft_t *raft,
                                raft_progress_snapshot_t **out,
                                size_t *out_len);
int raft_core_conf_state_copy(const raft_t *raft, raft_conf_state_t *out);
int raft_core_ready_messages_copy(const raft_t *raft,
                                  raft_message_vec_t *out);
void raft_core_clear_messages(raft_t *raft);
void raft_core_reduce_uncommitted(raft_t *raft, uint64_t payload_size);

#endif  // ETCD_RAFT_CORE_H
