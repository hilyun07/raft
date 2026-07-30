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

#ifndef ETCD_RAFT_CONFCHANGE_H
#define ETCD_RAFT_CONFCHANGE_H

#include "tracker.h"

typedef struct raft_decoded_conf_change {
    raft_conf_change_transition_t transition;
    raft_conf_change_single_t *changes;
    size_t changes_len;
} raft_decoded_conf_change_t;

void raft_decoded_conf_change_free(raft_decoded_conf_change_t *change);

int raft_confchange_decode_entry(raft_entry_type_t entry_type,
                                 const raft_byte_view_t *data,
                                 raft_decoded_conf_change_t *out);

int raft_confchange_encode_v2(const raft_conf_change_v2_view_t *change,
                              raft_bytes_t *out);
int raft_confchange_encode_v1(const raft_conf_change_view_t *change,
                              raft_bytes_t *out);

bool raft_confchange_is_leave_joint(
    const raft_decoded_conf_change_t *change);
bool raft_confchange_wants_enter_joint(
    const raft_decoded_conf_change_t *change,
    bool *auto_leave);

int raft_confchange_check_invariants(
    const raft_progress_tracker_t *tracker);
int raft_confchange_restore(raft_progress_tracker_t *tracker,
                            const raft_conf_state_t *state,
                            uint64_t last_index);
int raft_confchange_apply(raft_progress_tracker_t *tracker,
                          const raft_decoded_conf_change_t *change,
                          uint64_t last_index);
int raft_confchange_apply_view(raft_progress_tracker_t *tracker,
                               const raft_conf_change_v2_view_t *change,
                               uint64_t last_index);

#endif  // ETCD_RAFT_CONFCHANGE_H
