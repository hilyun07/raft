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

#ifndef ETCD_RAFT_READ_ONLY_H
#define ETCD_RAFT_READ_ONLY_H

#include "tracker.h"

typedef struct raft_read_ack_internal {
    uint64_t id;
    uint64_t index;
} raft_read_ack_internal_t;

typedef struct raft_read_index_request_internal {
    raft_message_t request;
    uint64_t index;
} raft_read_index_request_internal_t;

typedef struct raft_read_only_internal {
    raft_read_only_option_t option;
    raft_read_ack_internal_t *acks;
    size_t ack_len;
    raft_read_index_request_internal_t *requests;
    size_t request_len;
    uint64_t confirmed_reads;
} raft_read_only_internal_t;

int raft_read_only_init(raft_read_only_internal_t *read_only,
                        raft_read_only_option_t option);
void raft_read_only_reset(raft_read_only_internal_t *read_only);
void raft_read_only_free(raft_read_only_internal_t *read_only);

int raft_read_only_add_request(raft_read_only_internal_t *read_only,
                               uint64_t commit_index,
                               const raft_message_view_t *request);
int raft_read_only_recv_ack(raft_read_only_internal_t *read_only,
                            uint64_t from,
                            const raft_byte_view_t *context);
bool raft_read_only_heartbeat_position(
    const raft_read_only_internal_t *read_only,
    uint64_t *position);
int raft_read_only_confirmed(
    const raft_read_only_internal_t *read_only,
    const raft_progress_tracker_t *tracker,
    size_t *count,
    uint64_t *new_confirmed_reads);
const raft_read_index_request_internal_t *raft_read_only_request_at(
    const raft_read_only_internal_t *read_only,
    size_t at);
void raft_read_only_advance(raft_read_only_internal_t *read_only,
                            size_t count,
                            uint64_t new_confirmed_reads);

#endif  // ETCD_RAFT_READ_ONLY_H
