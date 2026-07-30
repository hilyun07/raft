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

#include "raft_core.h"

#include <stdlib.h>
#include <string.h>

#define RAFT_ALLOC_REPLACE_STDLIB
#include "alloc.h"

typedef enum core_campaign_type {
    CORE_CAMPAIGN_PRE_ELECTION,
    CORE_CAMPAIGN_ELECTION,
    CORE_CAMPAIGN_TRANSFER,
} core_campaign_type_t;

static const uint8_t core_campaign_transfer_context[] =
    "CampaignTransfer";

static uint64_t core_min_u64(uint64_t left, uint64_t right) {
    return left < right ? left : right;
}

static void core_put_le64(uint8_t *data, uint64_t value) {
    size_t i;
    for (i = 0; i < 8; ++i) {
        data[i] = (uint8_t)(value >> (8U * i));
    }
}

static bool core_array_valid(const void *items, size_t len) {
    return len == 0 || items != NULL;
}

static bool core_hard_state_empty(const raft_hard_state_t *state) {
    return state->term == 0 && state->vote == RAFT_NONE &&
           state->commit == 0;
}

bool raft_result_is_terminal(int result) {
    return result == RAFT_ERR_STORAGE_COMPACTED ||
           result == RAFT_ERR_STORAGE_UNAVAILABLE ||
           result == RAFT_ERR_PANIC_FROM_GO_CALLBACK ||
           result == RAFT_ERR_OUT_OF_MEMORY ||
           result == RAFT_ERR_FATAL;
}

int raft_core_latch_error(raft_t *raft, int result) {
    if (raft != NULL && raft->error == RAFT_OK &&
        raft_result_is_terminal(result)) {
        raft->error = result;
    }
    return result;
}

static int core_uint64_vec_copy(raft_uint64_vec_t *dst,
                                const raft_uint64_vec_t *src) {
    if (dst == NULL || src == NULL ||
        !core_array_valid(src->items, src->len)) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    memset(dst, 0, sizeof(*dst));
    if (src->len == 0) {
        return RAFT_OK;
    }
    if (src->len > SIZE_MAX / sizeof(*dst->items)) {
        return RAFT_ERR_OUT_OF_MEMORY;
    }
    dst->items = malloc(src->len * sizeof(*dst->items));
    if (dst->items == NULL) {
        return RAFT_ERR_OUT_OF_MEMORY;
    }
    memcpy(dst->items, src->items, src->len * sizeof(*dst->items));
    dst->len = src->len;
    return RAFT_OK;
}

static int core_conf_state_copy(raft_conf_state_t *dst,
                                const raft_conf_state_t *src) {
    int result;

    if (dst == NULL || src == NULL) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    memset(dst, 0, sizeof(*dst));
    result = core_uint64_vec_copy(&dst->voters, &src->voters);
    if (result != RAFT_OK) {
        goto fail;
    }
    result = core_uint64_vec_copy(&dst->voters_outgoing,
                                  &src->voters_outgoing);
    if (result != RAFT_OK) {
        goto fail;
    }
    result = core_uint64_vec_copy(&dst->learners, &src->learners);
    if (result != RAFT_OK) {
        goto fail;
    }
    result = core_uint64_vec_copy(&dst->learners_next,
                                  &src->learners_next);
    if (result != RAFT_OK) {
        goto fail;
    }
    dst->auto_leave = src->auto_leave;
    return RAFT_OK;

fail:
    raft_conf_state_free(dst);
    return result;
}

static int core_entry_copy(raft_entry_t *dst, const raft_entry_t *src) {
    int result;

    if (dst == NULL || !raft_entry_valid(src)) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    memset(dst, 0, sizeof(*dst));
    dst->data.is_nil = true;
    dst->type = src->type;
    dst->term = src->term;
    dst->index = src->index;
    result = raft_bytes_copy(&dst->data, &src->data);
    if (result != RAFT_OK) {
        raft_entry_free(dst);
    }
    return result;
}

static int core_entry_vec_copy(raft_entry_vec_t *dst,
                               const raft_entry_vec_t *src) {
    size_t i;

    if (dst == NULL || src == NULL ||
        !core_array_valid(src->items, src->len)) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    memset(dst, 0, sizeof(*dst));
    if (src->len == 0) {
        return RAFT_OK;
    }
    if (src->len > SIZE_MAX / sizeof(*dst->items)) {
        return RAFT_ERR_OUT_OF_MEMORY;
    }
    dst->items = calloc(src->len, sizeof(*dst->items));
    if (dst->items == NULL) {
        return RAFT_ERR_OUT_OF_MEMORY;
    }
    dst->len = src->len;
    for (i = 0; i < src->len; ++i) {
        int result = core_entry_copy(&dst->items[i], &src->items[i]);
        if (result != RAFT_OK) {
            raft_entry_vec_free(dst);
            return result;
        }
    }
    return RAFT_OK;
}

static int core_snapshot_copy(raft_snapshot_t *dst,
                              const raft_snapshot_t *src) {
    int result;

    if (dst == NULL || !raft_snapshot_valid(src)) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    memset(dst, 0, sizeof(*dst));
    dst->data.is_nil = true;
    result = raft_bytes_copy(&dst->data, &src->data);
    if (result != RAFT_OK) {
        goto fail;
    }
    result = core_conf_state_copy(&dst->metadata.conf_state,
                                  &src->metadata.conf_state);
    if (result != RAFT_OK) {
        goto fail;
    }
    dst->metadata.index = src->metadata.index;
    dst->metadata.term = src->metadata.term;
    return RAFT_OK;

fail:
    raft_snapshot_free(dst);
    return result;
}

static int core_message_copy(raft_message_t *dst,
                             const raft_message_t *src) {
    size_t i;
    int result;

    if (dst == NULL || !raft_message_valid(src)) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    memset(dst, 0, sizeof(*dst));
    dst->context.is_nil = true;
    dst->snapshot.data.is_nil = true;
    dst->type = src->type;
    dst->to = src->to;
    dst->from = src->from;
    dst->term = src->term;
    dst->log_term = src->log_term;
    dst->index = src->index;
    dst->commit = src->commit;
    dst->vote = src->vote;
    dst->reject = src->reject;
    dst->reject_hint = src->reject_hint;
    result = raft_bytes_copy(&dst->context, &src->context);
    if (result != RAFT_OK) {
        goto fail;
    }
    result = core_entry_vec_copy(&dst->entries, &src->entries);
    if (result != RAFT_OK) {
        goto fail;
    }
    dst->has_snapshot = src->has_snapshot;
    if (src->has_snapshot) {
        result = core_snapshot_copy(&dst->snapshot, &src->snapshot);
        if (result != RAFT_OK) {
            goto fail;
        }
    }
    if (src->responses.len != 0) {
        if (src->responses.len >
            SIZE_MAX / sizeof(*dst->responses.items)) {
            result = RAFT_ERR_OUT_OF_MEMORY;
            goto fail;
        }
        dst->responses.items =
            calloc(src->responses.len, sizeof(*dst->responses.items));
        if (dst->responses.items == NULL) {
            result = RAFT_ERR_OUT_OF_MEMORY;
            goto fail;
        }
        dst->responses.len = src->responses.len;
        for (i = 0; i < src->responses.len; ++i) {
            result = core_message_copy(&dst->responses.items[i],
                                       &src->responses.items[i]);
            if (result != RAFT_OK) {
                goto fail;
            }
        }
    }
    return RAFT_OK;

fail:
    raft_message_free(dst);
    return result;
}

static void core_message_init(raft_message_t *message,
                              raft_message_type_t type) {
    memset(message, 0, sizeof(*message));
    message->type = type;
    message->context.is_nil = true;
    message->snapshot.data.is_nil = true;
}

static int core_message_vec_push(raft_message_vec_t *messages,
                                 raft_message_t *message) {
    raft_message_t *items;

    if (messages == NULL || message == NULL ||
        !raft_message_valid(message)) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    if (messages->len == SIZE_MAX ||
        messages->len + 1 > SIZE_MAX / sizeof(*items)) {
        return RAFT_ERR_OUT_OF_MEMORY;
    }
    items = realloc(messages->items,
                    (messages->len + 1) * sizeof(*items));
    if (items == NULL) {
        return RAFT_ERR_OUT_OF_MEMORY;
    }
    messages->items = items;
    messages->items[messages->len] = *message;
    ++messages->len;
    memset(message, 0, sizeof(*message));
    return RAFT_OK;
}

static int core_read_state_vec_push(raft_read_state_vec_t *states,
                                    raft_read_state_t *state) {
    raft_read_state_t *items;

    if (states == NULL || state == NULL ||
        !raft_bytes_valid(&state->request_ctx)) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    if (states->len == SIZE_MAX ||
        states->len + 1 > SIZE_MAX / sizeof(*items)) {
        return RAFT_ERR_OUT_OF_MEMORY;
    }
    items = realloc(states->items,
                    (states->len + 1) * sizeof(*items));
    if (items == NULL) {
        return RAFT_ERR_OUT_OF_MEMORY;
    }
    states->items = items;
    states->items[states->len] = *state;
    ++states->len;
    memset(state, 0, sizeof(*state));
    return RAFT_OK;
}

static int core_send(raft_t *raft, raft_message_t *message) {
    bool after_append;
    bool vote_message;

    if (raft == NULL || message == NULL || !raft_message_valid(message)) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    if (message->from == RAFT_NONE) {
        message->from = raft->id;
    }
    vote_message = message->type == RAFT_MSG_VOTE ||
                   message->type == RAFT_MSG_VOTE_RESP ||
                   message->type == RAFT_MSG_PRE_VOTE ||
                   message->type == RAFT_MSG_PRE_VOTE_RESP;
    if (vote_message) {
        if (message->term == 0) {
            return RAFT_ERR_FATAL;
        }
    } else {
        if (message->term != 0) {
            return RAFT_ERR_FATAL;
        }
        if (message->type != RAFT_MSG_PROP &&
            message->type != RAFT_MSG_READ_INDEX) {
            message->term = raft->term;
        }
    }
    after_append = message->type == RAFT_MSG_APP_RESP ||
                   message->type == RAFT_MSG_VOTE_RESP ||
                   message->type == RAFT_MSG_PRE_VOTE_RESP;
    if (message->to == RAFT_NONE ||
        (message->to == raft->id && !after_append)) {
        return RAFT_ERR_FATAL;
    }
    return core_message_vec_push(
        after_append ? &raft->messages_after_append
                     : &raft->messages,
        message);
}

static raft_progress_internal_t *core_find_progress(
    raft_t *raft, uint64_t id) {
    return raft == NULL ? NULL : raft_tracker_find(&raft->tracker, id);
}

static bool core_is_voter(const raft_t *raft, uint64_t id) {
    return raft != NULL && raft_tracker_is_voter(&raft->tracker, id);
}

static bool core_promotable(const raft_t *raft) {
    return core_is_voter(raft, raft->id) &&
           !raft_log_has_next_or_in_progress_snapshot(raft->log);
}

static int core_set_configuration(raft_t *raft,
                                  const raft_conf_state_t *state) {
    uint64_t last_index;
    int result;

    if (raft == NULL || state == NULL) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    result = raft_log_last_index(raft->log, &last_index);
    if (result != RAFT_OK) {
        return result;
    }
    return raft_confchange_restore(
        &raft->tracker, state, last_index);
}

static int core_reset(raft_t *raft, uint64_t term) {
    uint64_t last_index;
    size_t i;
    int result;

    if (raft->term != term) {
        raft->term = term;
        raft->vote = RAFT_NONE;
    }
    raft->lead = RAFT_NONE;
    raft->lead_transferee = RAFT_NONE;
    raft->election_elapsed = 0;
    raft->heartbeat_elapsed = 0;
    raft->randomized_election_timeout =
        (uint64_t)raft->election_timeout +
        (uint64_t)raft_random_uniform(
            &raft->random, raft->election_timeout);
    raft->uncommitted_size = 0;
    raft_read_only_reset(&raft->read_only);
    result = raft_log_last_index(raft->log, &last_index);
    if (result != RAFT_OK) {
        return result;
    }
    if (last_index == UINT64_MAX) {
        return RAFT_ERR_FATAL;
    }
    raft_tracker_reset_votes(&raft->tracker);
    for (i = 0; i < raft->tracker.progress_len; ++i) {
        raft_progress_internal_t *progress =
            &raft->tracker.progress[i];
        progress->match_index = 0;
        progress->next_index = last_index + 1;
        raft_progress_reset_state(
            progress, RAFT_PROGRESS_STATE_PROBE);
        progress->recent_active = progress->id == raft->id;
    }
    return RAFT_OK;
}

static int core_become_follower(raft_t *raft,
                                uint64_t term,
                                uint64_t lead) {
    int result = core_reset(raft, term);
    if (result != RAFT_OK) {
        return result;
    }
    raft->lead = lead;
    raft->state = RAFT_STATE_FOLLOWER;
    return RAFT_OK;
}

static int core_become_candidate(raft_t *raft) {
    raft_progress_internal_t *self;
    int result;

    if (raft->state == RAFT_STATE_LEADER || raft->term == UINT64_MAX) {
        return RAFT_ERR_FATAL;
    }
    result = core_reset(raft, raft->term + 1);
    if (result != RAFT_OK) {
        return result;
    }
    raft->vote = raft->id;
    raft->state = RAFT_STATE_CANDIDATE;
    self = core_find_progress(raft, raft->id);
    if (self == NULL || self->is_learner) {
        return RAFT_ERR_FATAL;
    }
    return RAFT_OK;
}

static int core_become_pre_candidate(raft_t *raft) {
    raft_progress_internal_t *self;

    if (raft->state == RAFT_STATE_LEADER) {
        return RAFT_ERR_FATAL;
    }
    raft_tracker_reset_votes(&raft->tracker);
    raft->lead = RAFT_NONE;
    raft->state = RAFT_STATE_PRE_CANDIDATE;
    self = core_find_progress(raft, raft->id);
    if (self == NULL || self->is_learner) {
        return RAFT_ERR_FATAL;
    }
    return RAFT_OK;
}

static uint64_t core_payload_size(const raft_entry_t *entries,
                                  size_t entry_count) {
    uint64_t size = 0;
    size_t i;
    for (i = 0; i < entry_count; ++i) {
        uint64_t payload = (uint64_t)entries[i].data.len;
        if (UINT64_MAX - size < payload) {
            return UINT64_MAX;
        }
        size += payload;
    }
    return size;
}

static int core_maybe_commit(raft_t *raft, bool *changed) {
    if (changed == NULL) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    *changed = false;
    return raft_log_maybe_commit(
        raft->log,
        raft_tracker_committed(&raft->tracker),
        raft->term,
        changed);
}

static int core_append_entries(raft_t *raft,
                               const raft_entry_view_t *entries,
                               size_t entry_count) {
    raft_entry_t *owned;
    raft_message_t response;
    uint64_t last_index;
    uint64_t payload_size;
    size_t i;
    int result;

    if (entry_count == 0 || entries == NULL) {
        return RAFT_ERR_FATAL;
    }
    result = raft_log_last_index(raft->log, &last_index);
    if (result != RAFT_OK) {
        return result;
    }
    if (last_index == UINT64_MAX ||
        entry_count > UINT64_MAX - last_index ||
        entry_count > SIZE_MAX / sizeof(*owned)) {
        return RAFT_ERR_FATAL;
    }
    owned = calloc(entry_count, sizeof(*owned));
    if (owned == NULL) {
        return RAFT_ERR_OUT_OF_MEMORY;
    }
    for (i = 0; i < entry_count; ++i) {
        result = raft_entry_copy_from_view(&owned[i], &entries[i]);
        if (result != RAFT_OK) {
            raft_entry_array_free(owned, entry_count);
            return result;
        }
        owned[i].term = raft->term;
        owned[i].index = last_index + 1 + (uint64_t)i;
    }
    payload_size = core_payload_size(owned, entry_count);
    if (raft->uncommitted_size > 0 && payload_size > 0 &&
        (payload_size > raft->max_uncommitted_entries_size ||
         raft->uncommitted_size >
             raft->max_uncommitted_entries_size - payload_size)) {
        raft_entry_array_free(owned, entry_count);
        return RAFT_ERR_PROPOSAL_DROPPED;
    }
    result = raft_log_append(
        raft->log, owned, entry_count, &last_index);
    raft_entry_array_free(owned, entry_count);
    if (result != RAFT_OK) {
        return result;
    }
    raft->uncommitted_size += payload_size;
    core_message_init(&response, RAFT_MSG_APP_RESP);
    response.to = raft->id;
    response.index = last_index;
    result = core_send(raft, &response);
    raft_message_free(&response);
    return result;
}

static int core_append_noop(raft_t *raft) {
    const raft_entry_view_t entry = {
        .type = RAFT_ENTRY_NORMAL,
        .data = {NULL, 0, true},
    };
    return core_append_entries(raft, &entry, 1);
}

static int core_become_leader(raft_t *raft) {
    raft_progress_internal_t *self;
    uint64_t last_index;
    int result;

    if (raft->state == RAFT_STATE_FOLLOWER) {
        return RAFT_ERR_FATAL;
    }
    result = core_reset(raft, raft->term);
    if (result != RAFT_OK) {
        return result;
    }
    raft->lead = raft->id;
    raft->state = RAFT_STATE_LEADER;
    self = core_find_progress(raft, raft->id);
    if (self == NULL || self->is_learner) {
        return RAFT_ERR_FATAL;
    }
    result = raft_log_last_index(raft->log, &last_index);
    if (result != RAFT_OK) {
        return result;
    }
    raft->pending_conf_index = last_index;
    self->match_index = last_index;
    raft_progress_become_replicate(self);
    self->recent_active = true;
    return core_append_noop(raft);
}

static int core_send_vote_request(raft_t *raft,
                                  uint64_t to,
                                  raft_message_type_t type,
                                  uint64_t term,
                                  core_campaign_type_t campaign_type) {
    raft_message_t message;
    raft_byte_view_t transfer_context = {
        core_campaign_transfer_context,
        sizeof(core_campaign_transfer_context) - 1,
        false,
    };
    uint64_t last_index;
    uint64_t last_term;
    int result;

    if (type != RAFT_MSG_VOTE && type != RAFT_MSG_PRE_VOTE) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    result = raft_log_last_index(raft->log, &last_index);
    if (result != RAFT_OK) {
        return result;
    }
    result = raft_log_term(raft->log, last_index, &last_term);
    if (result != RAFT_OK) {
        return result;
    }
    core_message_init(&message, type);
    message.to = to;
    message.term = term;
    message.index = last_index;
    message.log_term = last_term;
    if (campaign_type == CORE_CAMPAIGN_TRANSFER) {
        result = raft_bytes_copy_from_view(
            &message.context, &transfer_context);
        if (result != RAFT_OK) {
            raft_message_free(&message);
            return result;
        }
    }
    result = core_send(raft, &message);
    raft_message_free(&message);
    return result;
}

static int core_maybe_send_snapshot(
    raft_t *raft, raft_progress_internal_t *progress) {
    raft_message_t message;
    int result;

    if (raft == NULL || progress == NULL ||
        progress->id == raft->id) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    if (!progress->recent_active) {
        return RAFT_OK;
    }

    core_message_init(&message, RAFT_MSG_SNAP);
    message.to = progress->id;
    result = raft_log_snapshot(raft->log, &message.snapshot);
    if (result == RAFT_ERR_SNAPSHOT_TEMPORARILY_UNAVAILABLE) {
        raft_message_free(&message);
        return RAFT_OK;
    }
    if (result != RAFT_OK) {
        raft_message_free(&message);
        return result;
    }
    if (message.snapshot.metadata.index == 0) {
        raft_message_free(&message);
        return RAFT_ERR_FATAL;
    }
    message.has_snapshot = true;
    raft_progress_become_snapshot(
        progress, message.snapshot.metadata.index);
    result = core_send(raft, &message);
    raft_message_free(&message);
    return result;
}

static int core_send_append(raft_t *raft,
                            raft_progress_internal_t *progress) {
    raft_message_t message;
    uint64_t entry_bytes = 0;
    uint64_t previous_index;
    uint64_t previous_term;
    size_t entry_count;
    size_t i;
    int result;

    if (progress == NULL || progress->id == raft->id) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    if (raft_progress_is_paused(progress)) {
        return RAFT_OK;
    }
    if (progress->next_index == 0) {
        progress->next_index = 1;
    }
    previous_index = progress->next_index - 1;
    result = raft_log_term(raft->log, previous_index, &previous_term);
    if (result != RAFT_OK) {
        if (result == RAFT_ERR_STORAGE_COMPACTED ||
            result == RAFT_ERR_STORAGE_UNAVAILABLE) {
            return core_maybe_send_snapshot(raft, progress);
        }
        return result;
    }
    core_message_init(&message, RAFT_MSG_APP);
    message.to = progress->id;
    message.index = previous_index;
    message.log_term = previous_term;
    message.commit = raft->log->committed;
    if (progress->state != RAFT_PROGRESS_STATE_REPLICATE ||
        !raft_inflights_full(&progress->inflights)) {
        result = raft_log_entries(raft->log,
                                  progress->next_index,
                                  raft->max_size_per_message,
                                  &message.entries);
        if (result != RAFT_OK) {
            raft_message_free(&message);
            if (result == RAFT_ERR_STORAGE_COMPACTED ||
                result == RAFT_ERR_STORAGE_UNAVAILABLE) {
                return core_maybe_send_snapshot(raft, progress);
            }
            return result;
        }
    }
    for (i = 0; i < message.entries.len; ++i) {
        uint64_t size =
            (uint64_t)message.entries.items[i].data.len;
        entry_bytes = UINT64_MAX - entry_bytes < size
                          ? UINT64_MAX
                          : entry_bytes + size;
    }
    entry_count = message.entries.len;
    result = core_send(raft, &message);
    if (result == RAFT_OK) {
        result = raft_progress_sent_entries(
            progress, entry_count, entry_bytes);
    }
    if (result == RAFT_OK) {
        raft_progress_sent_commit(
            progress, raft->log->committed);
    }
    raft_message_free(&message);
    return result;
}

static int core_send_pending_entries(
    raft_t *raft, raft_progress_internal_t *progress) {
    uint64_t last_index;
    int result;

    result = raft_log_last_index(raft->log, &last_index);
    if (result != RAFT_OK) {
        return result;
    }
    while (progress->next_index <= last_index &&
           !raft_progress_is_paused(progress)) {
        uint64_t previous_next = progress->next_index;
        result = core_send_append(raft, progress);
        if (result != RAFT_OK) {
            return result;
        }
        if (progress->next_index == previous_next) {
            break;
        }
    }
    return RAFT_OK;
}

static int core_broadcast_append(raft_t *raft) {
    size_t i;
    for (i = 0; i < raft->tracker.progress_len; ++i) {
        int result;
        if (raft->tracker.progress[i].id == raft->id) {
            continue;
        }
        result = core_send_append(
            raft, &raft->tracker.progress[i]);
        if (result != RAFT_OK) {
            return result;
        }
    }
    return RAFT_OK;
}

static int core_send_heartbeat(raft_t *raft,
                               raft_progress_internal_t *progress,
                               const raft_byte_view_t *context) {
    raft_message_t message;
    int result;

    if (progress == NULL || progress->id == raft->id ||
        context == NULL || !raft_byte_view_valid(context)) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    core_message_init(&message, RAFT_MSG_HEARTBEAT);
    message.to = progress->id;
    message.commit =
        core_min_u64(progress->match_index, raft->log->committed);
    result = raft_bytes_copy_from_view(&message.context, context);
    if (result != RAFT_OK) {
        raft_message_free(&message);
        return result;
    }
    result = core_send(raft, &message);
    if (result == RAFT_OK) {
        raft_progress_sent_commit(
            progress,
            core_min_u64(
                progress->match_index, raft->log->committed));
    }
    raft_message_free(&message);
    return result;
}

static int core_broadcast_heartbeat(raft_t *raft) {
    uint8_t encoded_position[8];
    raft_byte_view_t context = {NULL, 0, true};
    uint64_t position;
    size_t i;

    if (raft_read_only_heartbeat_position(
            &raft->read_only, &position)) {
        core_put_le64(encoded_position, position);
        context.data = encoded_position;
        context.len = sizeof(encoded_position);
        context.is_nil = false;
    }
    for (i = 0; i < raft->tracker.progress_len; ++i) {
        int result;
        if (raft->tracker.progress[i].id == raft->id) {
            continue;
        }
        result = core_send_heartbeat(
            raft, &raft->tracker.progress[i], &context);
        if (result != RAFT_OK) {
            return result;
        }
    }
    return RAFT_OK;
}

typedef struct core_conf_change_scan {
    bool found;
} core_conf_change_scan_t;

static int core_scan_conf_changes(void *context,
                                  const raft_entry_t *entries,
                                  size_t entry_count) {
    core_conf_change_scan_t *scan = context;
    size_t i;

    if (scan == NULL ||
        (entry_count != 0 && entries == NULL)) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    for (i = 0; i < entry_count; ++i) {
        if (entries[i].type == RAFT_ENTRY_CONF_CHANGE ||
            entries[i].type == RAFT_ENTRY_CONF_CHANGE_V2) {
            scan->found = true;
        }
    }
    return RAFT_OK;
}

static int core_has_unapplied_conf_changes(
    raft_t *raft, bool *has_changes) {
    core_conf_change_scan_t scan = {false};
    int result;

    if (raft == NULL || has_changes == NULL) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    *has_changes = false;
    if (raft->log->applied >= raft->log->committed) {
        return RAFT_OK;
    }
    if (raft->log->committed == UINT64_MAX) {
        return RAFT_ERR_FATAL;
    }
    result = raft_log_scan(
        raft->log,
        raft->log->applied + 1,
        raft->log->committed + 1,
        raft->log->max_applying_entries_size,
        core_scan_conf_changes,
        &scan);
    if (result != RAFT_OK) {
        return result;
    }
    *has_changes = scan.found;
    return RAFT_OK;
}

static int core_campaign(raft_t *raft, core_campaign_type_t campaign_type);
static int core_send_vote_response(raft_t *raft,
                                   uint64_t to,
                                   uint64_t term,
                                   raft_message_type_t type,
                                   bool reject);

static int core_campaign(raft_t *raft, core_campaign_type_t campaign_type) {
    raft_message_type_t vote_type;
    uint64_t term;
    size_t i;
    int result;

    if (!core_promotable(raft)) {
        return RAFT_OK;
    }
    if (campaign_type == CORE_CAMPAIGN_PRE_ELECTION) {
        if (raft->term == UINT64_MAX) {
            return RAFT_ERR_FATAL;
        }
        result = core_become_pre_candidate(raft);
        vote_type = RAFT_MSG_PRE_VOTE;
        term = raft->term + 1;
    } else {
        result = core_become_candidate(raft);
        vote_type = RAFT_MSG_VOTE;
        term = raft->term;
    }
    if (result != RAFT_OK) {
        return result;
    }
    for (i = 0; i < raft->tracker.progress_len; ++i) {
        raft_progress_internal_t *progress =
            &raft->tracker.progress[i];
        if (!raft_tracker_is_voter(
                &raft->tracker, progress->id)) {
            continue;
        }
        if (progress->id == raft->id) {
            result = core_send_vote_response(
                raft,
                raft->id,
                term,
                vote_type == RAFT_MSG_PRE_VOTE
                    ? RAFT_MSG_PRE_VOTE_RESP
                    : RAFT_MSG_VOTE_RESP,
                false);
        } else {
            result = core_send_vote_request(
                raft,
                progress->id,
                vote_type,
                term,
                campaign_type);
        }
        if (result != RAFT_OK) {
            return result;
        }
    }
    return RAFT_OK;
}

static int core_hup(raft_t *raft, core_campaign_type_t campaign_type) {
    bool has_changes;
    int result;

    if (raft->state == RAFT_STATE_LEADER ||
        !core_promotable(raft)) {
        return RAFT_OK;
    }
    result = core_has_unapplied_conf_changes(
        raft, &has_changes);
    if (result != RAFT_OK) {
        return result;
    }
    return has_changes ? RAFT_OK
                       : core_campaign(raft, campaign_type);
}

static int core_send_vote_response(raft_t *raft,
                                   uint64_t to,
                                   uint64_t term,
                                   raft_message_type_t type,
                                   bool reject) {
    raft_message_t response;
    int result;

    if (type != RAFT_MSG_VOTE_RESP &&
        type != RAFT_MSG_PRE_VOTE_RESP) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    core_message_init(&response, type);
    response.to = to;
    response.term = term;
    response.reject = reject;
    result = core_send(raft, &response);
    raft_message_free(&response);
    return result;
}

static int core_handle_vote(raft_t *raft,
                            const raft_message_view_t *message) {
    bool pre_vote = message->type == RAFT_MSG_PRE_VOTE;
    raft_message_type_t response_type =
        pre_vote ? RAFT_MSG_PRE_VOTE_RESP : RAFT_MSG_VOTE_RESP;
    bool can_vote =
        raft->vote == message->from ||
        (raft->vote == RAFT_NONE && raft->lead == RAFT_NONE) ||
        (pre_vote && message->term > raft->term);
    bool up_to_date;
    bool grant;
    uint64_t response_term;
    int result;

    result = raft_log_is_up_to_date(
        raft->log,
        message->index,
        message->log_term,
        &up_to_date);
    if (result != RAFT_OK) {
        return result;
    }
    grant = can_vote && up_to_date &&
            raft_is_valid_node_id(message->from);
    response_term = grant ? message->term : raft->term;

    if (grant && !pre_vote) {
        raft->vote = message->from;
        raft->election_elapsed = 0;
    }
    return core_send_vote_response(
        raft,
        message->from,
        response_term,
        response_type,
        !grant);
}

static int core_handle_vote_response(raft_t *raft,
                                     const raft_message_view_t *message) {
    raft_progress_internal_t *progress =
        core_find_progress(raft, message->from);
    raft_vote_result_internal_t vote_result;
    raft_message_type_t expected_type;
    int result;

    if (raft->state == RAFT_STATE_PRE_CANDIDATE) {
        expected_type = RAFT_MSG_PRE_VOTE_RESP;
    } else if (raft->state == RAFT_STATE_CANDIDATE) {
        expected_type = RAFT_MSG_VOTE_RESP;
    } else {
        return RAFT_OK;
    }
    if (message->type != expected_type || progress == NULL ||
        progress->is_learner) {
        return RAFT_OK;
    }
    raft_tracker_record_vote(
        &raft->tracker, message->from, !message->reject);
    vote_result = raft_tracker_vote_result(&raft->tracker);
    if (vote_result == RAFT_VOTE_WON) {
        if (raft->state == RAFT_STATE_PRE_CANDIDATE) {
            return core_campaign(raft, CORE_CAMPAIGN_ELECTION);
        }
        result = core_become_leader(raft);
        return result == RAFT_OK ? core_broadcast_append(raft)
                                 : result;
    }
    if (vote_result == RAFT_VOTE_LOST) {
        return core_become_follower(raft, raft->term, RAFT_NONE);
    }
    return RAFT_OK;
}

static int core_owned_entries_from_view(
    const raft_entry_view_vec_t *view, raft_entry_vec_t *owned) {
    size_t i;

    if (view == NULL || owned == NULL ||
        !core_array_valid(view->items, view->len)) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    memset(owned, 0, sizeof(*owned));
    if (view->len == 0) {
        return RAFT_OK;
    }
    if (view->len > SIZE_MAX / sizeof(*owned->items)) {
        return RAFT_ERR_OUT_OF_MEMORY;
    }
    owned->items = calloc(view->len, sizeof(*owned->items));
    if (owned->items == NULL) {
        return RAFT_ERR_OUT_OF_MEMORY;
    }
    owned->len = view->len;
    for (i = 0; i < view->len; ++i) {
        int result =
            raft_entry_copy_from_view(&owned->items[i], &view->items[i]);
        if (result != RAFT_OK) {
            raft_entry_vec_free(owned);
            return result;
        }
    }
    return RAFT_OK;
}

static int core_send_append_response(raft_t *raft,
                                     uint64_t to,
                                     uint64_t index,
                                     bool reject,
                                     uint64_t reject_hint,
                                     uint64_t log_term) {
    raft_message_t response;
    int result;

    core_message_init(&response, RAFT_MSG_APP_RESP);
    response.to = to;
    response.index = index;
    response.reject = reject;
    response.reject_hint = reject_hint;
    response.log_term = log_term;
    result = core_send(raft, &response);
    raft_message_free(&response);
    return result;
}

static int core_handle_append(raft_t *raft,
                              const raft_message_view_t *message) {
    raft_entry_vec_t entries;
    uint64_t last_new_index;
    uint64_t last_index;
    uint64_t hint_index;
    uint64_t hint_term;
    bool appended;
    int result;

    if (message->index < raft->log->committed) {
        return core_send_append_response(raft,
                                         message->from,
                                         raft->log->committed,
                                         false,
                                         0,
                                         0);
    }
    result = core_owned_entries_from_view(&message->entries, &entries);
    if (result != RAFT_OK) {
        return result;
    }
    result = raft_log_maybe_append(raft->log,
                                   message->index,
                                   message->log_term,
                                   message->commit,
                                   entries.items,
                                   entries.len,
                                   &appended,
                                   &last_new_index);
    raft_entry_vec_free(&entries);
    if (result != RAFT_OK) {
        return result;
    }
    if (appended) {
        return core_send_append_response(raft,
                                         message->from,
                                         last_new_index,
                                         false,
                                         0,
                                         0);
    }
    result = raft_log_last_index(raft->log, &last_index);
    if (result != RAFT_OK) {
        return result;
    }
    hint_index = core_min_u64(message->index, last_index);
    result = raft_log_find_conflict_by_term(raft->log,
                                            hint_index,
                                            message->log_term,
                                            &hint_index,
                                            &hint_term);
    if (result != RAFT_OK) {
        return result;
    }
    return core_send_append_response(raft,
                                     message->from,
                                     message->index,
                                     true,
                                     hint_index,
                                     hint_term);
}

static int core_handle_heartbeat(raft_t *raft,
                                 const raft_message_view_t *message) {
    raft_message_t response;
    uint64_t last_index;
    int result;

    result = raft_log_last_index(raft->log, &last_index);
    if (result != RAFT_OK) {
        return result;
    }
    result = raft_log_commit_to(
        raft->log, core_min_u64(message->commit, last_index));
    if (result != RAFT_OK) {
        return result;
    }
    core_message_init(&response, RAFT_MSG_HEARTBEAT_RESP);
    response.to = message->from;
    result = raft_bytes_copy_from_view(&response.context,
                                       &message->context);
    if (result != RAFT_OK) {
        raft_message_free(&response);
        return result;
    }
    result = core_send(raft, &response);
    raft_message_free(&response);
    return result;
}

static bool core_snapshot_vec_contains(const raft_uint64_vec_t *ids,
                                       uint64_t id) {
    size_t i;

    if (ids == NULL || !core_array_valid(ids->items, ids->len)) {
        return false;
    }
    for (i = 0; i < ids->len; ++i) {
        if (ids->items[i] == id) {
            return true;
        }
    }
    return false;
}

static bool core_snapshot_contains_id(const raft_conf_state_t *state,
                                      uint64_t id) {
    return state != NULL &&
           (core_snapshot_vec_contains(&state->voters, id) ||
            core_snapshot_vec_contains(&state->learners, id) ||
            core_snapshot_vec_contains(
                &state->voters_outgoing, id));
}

static int core_restore_snapshot(raft_t *raft,
                                 const raft_snapshot_t *snapshot,
                                 bool *restored) {
    raft_progress_tracker_t replacement;
    bool matches;
    int result;

    if (raft == NULL || !raft_snapshot_valid(snapshot) ||
        restored == NULL) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    *restored = false;
    if (snapshot->metadata.index <= raft->log->committed) {
        return RAFT_OK;
    }
    if (raft->state != RAFT_STATE_FOLLOWER) {
        return RAFT_OK;
    }
    if (!core_snapshot_contains_id(
            &snapshot->metadata.conf_state, raft->id)) {
        return RAFT_OK;
    }
    result = raft_log_match_term(
        raft->log,
        snapshot->metadata.index,
        snapshot->metadata.term,
        &matches);
    if (result != RAFT_OK) {
        return result;
    }
    if (matches) {
        return raft_log_commit_to(
            raft->log, snapshot->metadata.index);
    }
    if (snapshot->metadata.index == UINT64_MAX) {
        return RAFT_ERR_FATAL;
    }

    memset(&replacement, 0, sizeof(replacement));
    result = raft_tracker_init(
        &replacement,
        raft->tracker.max_inflight_messages,
        raft->tracker.max_inflight_bytes);
    if (result != RAFT_OK) {
        return result;
    }
    result = raft_confchange_restore(
        &replacement,
        &snapshot->metadata.conf_state,
        snapshot->metadata.index);
    if (result != RAFT_OK) {
        raft_tracker_free(&replacement);
        return result == RAFT_ERR_OUT_OF_MEMORY ? result
                                                : RAFT_ERR_FATAL;
    }
    result = raft_log_restore(raft->log, snapshot);
    if (result != RAFT_OK) {
        raft_tracker_free(&replacement);
        return result;
    }
    raft_tracker_swap(&raft->tracker, &replacement);
    raft_tracker_free(&replacement);
    if (raft->lead_transferee != RAFT_NONE &&
        !raft_tracker_is_voter(
            &raft->tracker, raft->lead_transferee)) {
        raft->lead_transferee = RAFT_NONE;
    }
    *restored = true;
    return RAFT_OK;
}

static int core_handle_snapshot(raft_t *raft,
                                const raft_message_view_t *message) {
    raft_snapshot_t snapshot;
    uint64_t response_index;
    bool restored;
    int result;

    memset(&snapshot, 0, sizeof(snapshot));
    snapshot.data.is_nil = true;
    if (message->has_snapshot) {
        result = raft_snapshot_copy_from_view(
            &snapshot, &message->snapshot);
        if (result != RAFT_OK) {
            return result;
        }
    }
    result = core_restore_snapshot(raft, &snapshot, &restored);
    if (result == RAFT_OK) {
        if (restored) {
            result = raft_log_last_index(
                raft->log, &response_index);
        } else {
            response_index = raft->log->committed;
        }
    }
    if (result == RAFT_OK) {
        result = core_send_append_response(raft,
                                           message->from,
                                           response_index,
                                           false,
                                           0,
                                           0);
    }
    raft_snapshot_free(&snapshot);
    return result;
}

static int core_read_index_owned_view(const raft_message_t *message,
                                      raft_entry_view_t *entry,
                                      raft_message_view_t *view) {
    if (message == NULL || entry == NULL || view == NULL ||
        message->type != RAFT_MSG_READ_INDEX ||
        message->entries.len != 1 ||
        message->entries.items == NULL) {
        return RAFT_ERR_FATAL;
    }
    memset(entry, 0, sizeof(*entry));
    entry->type = message->entries.items[0].type;
    entry->term = message->entries.items[0].term;
    entry->index = message->entries.items[0].index;
    entry->data.data = message->entries.items[0].data.data;
    entry->data.len = message->entries.items[0].data.len;
    entry->data.is_nil = message->entries.items[0].data.is_nil;

    memset(view, 0, sizeof(*view));
    view->type = message->type;
    view->to = message->to;
    view->from = message->from;
    view->term = message->term;
    view->entries.items = entry;
    view->entries.len = 1;
    view->context.data = message->context.data;
    view->context.len = message->context.len;
    view->context.is_nil = message->context.is_nil;
    return RAFT_OK;
}

static int core_response_to_read_index_request(
    raft_t *raft,
    const raft_message_t *request,
    uint64_t read_index) {
    raft_message_t response;
    raft_read_state_t state;
    int result;

    if (raft == NULL || request == NULL ||
        request->type != RAFT_MSG_READ_INDEX ||
        request->entries.len != 1 ||
        request->entries.items == NULL) {
        return RAFT_ERR_FATAL;
    }
    if (request->from == RAFT_NONE || request->from == raft->id) {
        memset(&state, 0, sizeof(state));
        state.request_ctx.is_nil = true;
        state.index = read_index;
        result = raft_bytes_copy(
            &state.request_ctx, &request->entries.items[0].data);
        if (result != RAFT_OK) {
            raft_read_state_free(&state);
            return result;
        }
        result = core_read_state_vec_push(&raft->read_states, &state);
        raft_read_state_free(&state);
        return result;
    }

    core_message_init(&response, RAFT_MSG_READ_INDEX_RESP);
    response.to = request->from;
    response.index = read_index;
    result = core_entry_vec_copy(&response.entries, &request->entries);
    if (result == RAFT_OK) {
        result = core_send(raft, &response);
    }
    raft_message_free(&response);
    return result;
}

static int core_response_to_read_index_view(
    raft_t *raft,
    const raft_message_view_t *request,
    uint64_t read_index) {
    raft_message_t owned;
    int result;

    if (request == NULL || request->type != RAFT_MSG_READ_INDEX ||
        request->entries.len != 1 ||
        request->entries.items == NULL) {
        return RAFT_ERR_FATAL;
    }
    result = raft_message_copy_from_view(&owned, request);
    if (result != RAFT_OK) {
        return result;
    }
    result =
        core_response_to_read_index_request(raft, &owned, read_index);
    raft_message_free(&owned);
    return result;
}

static int core_send_read_index_response(
    raft_t *raft, const raft_message_view_t *message) {
    uint8_t encoded_position[8];
    raft_byte_view_t context;
    uint64_t position;
    int result;

    if (raft->read_only.option == RAFT_READ_ONLY_LEASE_BASED) {
        return core_response_to_read_index_view(
            raft, message, raft->log->committed);
    }
    result = raft_read_only_add_request(
        &raft->read_only, raft->log->committed, message);
    if (result != RAFT_OK) {
        return result;
    }
    if (!raft_read_only_heartbeat_position(
            &raft->read_only, &position)) {
        return RAFT_ERR_FATAL;
    }
    core_put_le64(encoded_position, position);
    context = (raft_byte_view_t){
        .data = encoded_position,
        .len = sizeof(encoded_position),
        .is_nil = false,
    };
    result = raft_read_only_recv_ack(
        &raft->read_only, raft->id, &context);
    if (result != RAFT_OK) {
        return result;
    }
    return core_broadcast_heartbeat(raft);
}

static int core_committed_entry_in_current_term(
    raft_t *raft, bool *committed) {
    uint64_t term;
    int result;

    if (raft == NULL || committed == NULL) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    *committed = false;
    result = raft_log_term(raft->log, raft->log->committed, &term);
    if (result == RAFT_ERR_STORAGE_COMPACTED ||
        result == RAFT_ERR_STORAGE_UNAVAILABLE) {
        return RAFT_OK;
    }
    if (result != RAFT_OK) {
        return result;
    }
    *committed = term == raft->term;
    return RAFT_OK;
}

static int core_pending_read_index_push(
    raft_t *raft, const raft_message_view_t *message) {
    raft_message_t owned;
    int result = raft_message_copy_from_view(&owned, message);
    if (result != RAFT_OK) {
        return result;
    }
    result = core_message_vec_push(
        &raft->pending_read_index_messages, &owned);
    raft_message_free(&owned);
    return result;
}

static int core_release_pending_read_index_messages(raft_t *raft) {
    bool committed;
    int result;

    if (raft->pending_read_index_messages.len == 0) {
        return RAFT_OK;
    }
    result = core_committed_entry_in_current_term(raft, &committed);
    if (result != RAFT_OK || !committed) {
        return result;
    }
    while (raft->pending_read_index_messages.len != 0) {
        raft_message_t *message =
            &raft->pending_read_index_messages.items[0];
        raft_entry_view_t entry;
        raft_message_view_t view;

        result = core_read_index_owned_view(message, &entry, &view);
        if (result == RAFT_OK) {
            result = core_send_read_index_response(raft, &view);
        }
        if (result != RAFT_OK) {
            return result;
        }
        raft_message_free(message);
        if (raft->pending_read_index_messages.len > 1) {
            memmove(
                message,
                message + 1,
                (raft->pending_read_index_messages.len - 1) *
                    sizeof(*message));
        }
        --raft->pending_read_index_messages.len;
    }
    free(raft->pending_read_index_messages.items);
    raft->pending_read_index_messages.items = NULL;
    return RAFT_OK;
}

static int core_advance_read_only(raft_t *raft) {
    uint64_t new_confirmed_reads;
    size_t count;
    int result;

    result = raft_read_only_confirmed(
        &raft->read_only,
        &raft->tracker,
        &count,
        &new_confirmed_reads);
    if (result != RAFT_OK) {
        return result;
    }
    while (count != 0) {
        const raft_read_index_request_internal_t *request =
            raft_read_only_request_at(&raft->read_only, 0);
        uint64_t next_confirmed =
            raft->read_only.confirmed_reads + 1;
        if (request == NULL) {
            return RAFT_ERR_FATAL;
        }
        result = core_response_to_read_index_request(
            raft, &request->request, request->index);
        if (result != RAFT_OK) {
            return result;
        }
        raft_read_only_advance(
            &raft->read_only, 1, next_confirmed);
        --count;
    }
    if (raft->read_only.confirmed_reads != new_confirmed_reads) {
        return RAFT_ERR_FATAL;
    }
    return RAFT_OK;
}

static int core_step_follower(raft_t *raft,
                              const raft_message_view_t *message) {
    switch (message->type) {
        case RAFT_MSG_PROP: {
            raft_message_t forwarded;
            int result;
            if (raft->lead == RAFT_NONE ||
                raft->disable_proposal_forwarding) {
                return RAFT_ERR_PROPOSAL_DROPPED;
            }
            result = raft_message_copy_from_view(&forwarded, message);
            if (result != RAFT_OK) {
                return result;
            }
            forwarded.to = raft->lead;
            forwarded.from =
                message->from == RAFT_NONE ? raft->id : message->from;
            result = core_send(raft, &forwarded);
            raft_message_free(&forwarded);
            return result;
        }
        case RAFT_MSG_APP:
            raft->election_elapsed = 0;
            raft->lead = message->from;
            return core_handle_append(raft, message);
        case RAFT_MSG_HEARTBEAT:
            raft->election_elapsed = 0;
            raft->lead = message->from;
            return core_handle_heartbeat(raft, message);
        case RAFT_MSG_SNAP:
            raft->election_elapsed = 0;
            raft->lead = message->from;
            return core_handle_snapshot(raft, message);
        case RAFT_MSG_TRANSFER_LEADER: {
            raft_message_t forwarded;
            int result;
            if (raft->lead == RAFT_NONE) {
                return RAFT_OK;
            }
            result = raft_message_copy_from_view(&forwarded, message);
            if (result != RAFT_OK) {
                return result;
            }
            forwarded.to = raft->lead;
            result = core_send(raft, &forwarded);
            raft_message_free(&forwarded);
            return result;
        }
        case RAFT_MSG_FORGET_LEADER:
            if (raft->read_only.option ==
                RAFT_READ_ONLY_LEASE_BASED) {
                return RAFT_OK;
            }
            raft->lead = RAFT_NONE;
            return RAFT_OK;
        case RAFT_MSG_TIMEOUT_NOW:
            return core_hup(raft, CORE_CAMPAIGN_TRANSFER);
        case RAFT_MSG_READ_INDEX: {
            raft_message_t forwarded;
            int result;
            if (raft->lead == RAFT_NONE) {
                return RAFT_OK;
            }
            result = raft_message_copy_from_view(&forwarded, message);
            if (result != RAFT_OK) {
                return result;
            }
            forwarded.to = raft->lead;
            forwarded.from =
                message->from == RAFT_NONE ? raft->id : message->from;
            result = core_send(raft, &forwarded);
            raft_message_free(&forwarded);
            return result;
        }
        case RAFT_MSG_READ_INDEX_RESP:
            if (message->entries.len != 1 ||
                message->entries.items == NULL) {
                return RAFT_OK;
            } else {
                raft_read_state_t state;
                int result;
                memset(&state, 0, sizeof(state));
                state.request_ctx.is_nil = true;
                state.index = message->index;
                result = raft_bytes_copy_from_view(
                    &state.request_ctx,
                    &message->entries.items[0].data);
                if (result == RAFT_OK) {
                    result = core_read_state_vec_push(
                        &raft->read_states, &state);
                }
                raft_read_state_free(&state);
                return result;
            }
        default:
            return RAFT_OK;
    }
}

static int core_step_candidate(raft_t *raft,
                               const raft_message_view_t *message) {
    switch (message->type) {
        case RAFT_MSG_PROP:
            return RAFT_ERR_PROPOSAL_DROPPED;
        case RAFT_MSG_APP: {
            int result = core_become_follower(
                raft, raft->term, message->from);
            return result == RAFT_OK
                       ? core_handle_append(raft, message)
                       : result;
        }
        case RAFT_MSG_HEARTBEAT: {
            int result = core_become_follower(
                raft, raft->term, message->from);
            return result == RAFT_OK
                       ? core_handle_heartbeat(raft, message)
                       : result;
        }
        case RAFT_MSG_SNAP: {
            int result = core_become_follower(
                raft, raft->term, message->from);
            return result == RAFT_OK
                       ? core_handle_snapshot(raft, message)
                       : result;
        }
        case RAFT_MSG_VOTE_RESP:
        case RAFT_MSG_PRE_VOTE_RESP:
            return core_handle_vote_response(raft, message);
        default:
            return RAFT_OK;
    }
}

static int core_prepare_proposal_entries(
    raft_t *raft,
    const raft_entry_view_vec_t *source,
    raft_entry_view_t **out) {
    raft_entry_view_t *entries;
    uint64_t last_index;
    size_t i;
    int result;

    if (raft == NULL || source == NULL || out == NULL ||
        !core_array_valid(source->items, source->len)) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    *out = NULL;
    if (source->len == 0 ||
        source->len > SIZE_MAX / sizeof(*entries)) {
        return source->len == 0 ? RAFT_ERR_PROPOSAL_DROPPED
                                : RAFT_ERR_OUT_OF_MEMORY;
    }
    result = raft_log_last_index(raft->log, &last_index);
    if (result != RAFT_OK) {
        return result;
    }
    entries = calloc(source->len, sizeof(*entries));
    if (entries == NULL) {
        return RAFT_ERR_OUT_OF_MEMORY;
    }
    memcpy(entries,
           source->items,
           source->len * sizeof(*entries));
    for (i = 0; i < source->len; ++i) {
        raft_decoded_conf_change_t decoded;
        bool already_pending;
        bool already_joint;
        bool wants_leave;

        if (entries[i].type != RAFT_ENTRY_CONF_CHANGE &&
            entries[i].type != RAFT_ENTRY_CONF_CHANGE_V2) {
            continue;
        }
        memset(&decoded, 0, sizeof(decoded));
        result = raft_confchange_decode_entry(
            entries[i].type, &entries[i].data, &decoded);
        if (result != RAFT_OK) {
            free(entries);
            return RAFT_ERR_FATAL;
        }
        already_pending =
            raft->pending_conf_index > raft->log->applied;
        already_joint = raft_tracker_is_joint(&raft->tracker);
        wants_leave = raft_confchange_is_leave_joint(&decoded);
        if (!raft->disable_conf_change_validation &&
            (already_pending ||
             (already_joint && !wants_leave) ||
             (!already_joint && wants_leave))) {
            entries[i].type = RAFT_ENTRY_NORMAL;
            entries[i].data =
                (raft_byte_view_t){NULL, 0, true};
        } else {
            if (last_index == UINT64_MAX ||
                (uint64_t)i >= UINT64_MAX - last_index) {
                raft_decoded_conf_change_free(&decoded);
                free(entries);
                return RAFT_ERR_FATAL;
            }
            raft->pending_conf_index =
                last_index + (uint64_t)i + 1;
        }
        raft_decoded_conf_change_free(&decoded);
    }
    *out = entries;
    return RAFT_OK;
}

static int core_handle_append_response(
    raft_t *raft, const raft_message_view_t *message) {
    raft_progress_internal_t *progress =
        core_find_progress(raft, message->from);
    bool committed;
    bool updated;
    int result;

    if (progress == NULL) {
        return RAFT_OK;
    }
    progress->recent_active = true;
    if (message->reject) {
        uint64_t next_probe_index = message->reject_hint;
        if (message->log_term > 0) {
            uint64_t next_probe_term;
            result = raft_log_find_conflict_by_term(
                raft->log,
                message->reject_hint,
                message->log_term,
                &next_probe_index,
                &next_probe_term);
            if (result != RAFT_OK) {
                return result;
            }
        }
        if (!raft_progress_maybe_decr_to(
                progress,
                message->index,
                next_probe_index)) {
            return RAFT_OK;
        }
        if (progress->state == RAFT_PROGRESS_STATE_REPLICATE) {
            raft_progress_become_probe(progress);
        }
        return core_send_append(raft, progress);
    }
    updated = raft_progress_maybe_update(
        progress, message->index);
    if (!updated &&
        progress->match_index == message->index &&
        progress->state == RAFT_PROGRESS_STATE_PROBE) {
        updated = true;
    }
    if (updated) {
        if (progress->state == RAFT_PROGRESS_STATE_PROBE) {
            raft_progress_become_replicate(progress);
        } else if (progress->state ==
                   RAFT_PROGRESS_STATE_SNAPSHOT) {
            uint64_t first_index;
            result = raft_log_first_index(
                raft->log, &first_index);
            if (result != RAFT_OK) {
                return result;
            }
            if (progress->match_index == UINT64_MAX ||
                progress->match_index + 1 >= first_index) {
                raft_progress_become_probe(progress);
                raft_progress_become_replicate(progress);
            }
        } else if (progress->state ==
                   RAFT_PROGRESS_STATE_REPLICATE) {
            raft_inflights_free_le(
                &progress->inflights, message->index);
        }
    }
    result = core_maybe_commit(raft, &committed);
    if (result != RAFT_OK) {
        return result;
    }
    if (committed) {
        result = core_release_pending_read_index_messages(raft);
        if (result != RAFT_OK) {
            return result;
        }
        result = core_broadcast_append(raft);
    } else if (message->from != raft->id &&
               raft_progress_can_bump_commit(
                   progress, raft->log->committed)) {
        result = core_send_append(raft, progress);
    }
    if (result != RAFT_OK) {
        return result;
    }
    if (message->from != raft->id) {
        result = core_send_pending_entries(raft, progress);
        if (result != RAFT_OK) {
            return result;
        }
    }
    if (updated && message->from == raft->lead_transferee) {
        uint64_t last_index;
        result = raft_log_last_index(raft->log, &last_index);
        if (result != RAFT_OK) {
            return result;
        }
        if (progress->match_index == last_index) {
            raft_message_t timeout_now;
            core_message_init(&timeout_now, RAFT_MSG_TIMEOUT_NOW);
            timeout_now.to = message->from;
            result = core_send(raft, &timeout_now);
            raft_message_free(&timeout_now);
            return result;
        }
    }
    return RAFT_OK;
}

static int core_send_timeout_now(raft_t *raft, uint64_t to) {
    raft_message_t message;
    int result;

    core_message_init(&message, RAFT_MSG_TIMEOUT_NOW);
    message.to = to;
    result = core_send(raft, &message);
    raft_message_free(&message);
    return result;
}

static int core_handle_transfer_leader(
    raft_t *raft, const raft_message_view_t *message) {
    raft_progress_internal_t *progress =
        core_find_progress(raft, message->from);
    uint64_t last_index;
    int result;

    if (progress == NULL || progress->is_learner) {
        return RAFT_OK;
    }
    if (raft->lead_transferee != RAFT_NONE) {
        if (raft->lead_transferee == message->from) {
            return RAFT_OK;
        }
        raft->lead_transferee = RAFT_NONE;
    }
    if (message->from == raft->id) {
        return RAFT_OK;
    }
    result = raft_log_last_index(raft->log, &last_index);
    if (result != RAFT_OK) {
        return result;
    }
    raft->election_elapsed = 0;
    raft->lead_transferee = message->from;
    if (progress->match_index == last_index) {
        return core_send_timeout_now(raft, message->from);
    }
    return core_send_append(raft, progress);
}

static int core_step_leader(raft_t *raft,
                            const raft_message_view_t *message) {
    switch (message->type) {
        case RAFT_MSG_BEAT:
            return core_broadcast_heartbeat(raft);
        case RAFT_MSG_CHECK_QUORUM: {
            size_t i;
            if (!raft_tracker_quorum_active(&raft->tracker)) {
                int result = core_become_follower(
                    raft, raft->term, RAFT_NONE);
                if (result != RAFT_OK) {
                    return result;
                }
            }
            for (i = 0; i < raft->tracker.progress_len; ++i) {
                if (raft->tracker.progress[i].id != raft->id) {
                    raft->tracker.progress[i].recent_active = false;
                }
            }
            return RAFT_OK;
        }
        case RAFT_MSG_PROP: {
            raft_entry_view_t *entries = NULL;
            int result;
            if (!core_is_voter(raft, raft->id) ||
                raft->lead_transferee != RAFT_NONE) {
                return RAFT_ERR_PROPOSAL_DROPPED;
            }
            result = core_prepare_proposal_entries(
                raft, &message->entries, &entries);
            if (result == RAFT_OK) {
                result = core_append_entries(
                    raft, entries, message->entries.len);
            }
            free(entries);
            if (result != RAFT_OK) {
                return result;
            }
            return core_broadcast_append(raft);
        }
        case RAFT_MSG_READ_INDEX: {
            bool committed;
            int result;
            if (message->entries.len != 1 ||
                message->entries.items == NULL) {
                return RAFT_ERR_FATAL;
            }
            if (raft_tracker_is_singleton(&raft->tracker)) {
                return core_response_to_read_index_view(
                    raft, message, raft->log->committed);
            }
            result = core_committed_entry_in_current_term(
                raft, &committed);
            if (result != RAFT_OK) {
                return result;
            }
            if (!committed) {
                return core_pending_read_index_push(raft, message);
            }
            return core_send_read_index_response(raft, message);
        }
        case RAFT_MSG_APP_RESP:
            return core_handle_append_response(raft, message);
        case RAFT_MSG_HEARTBEAT_RESP: {
            raft_progress_internal_t *progress =
                core_find_progress(raft, message->from);
            uint64_t last_index;
            int result;
            if (progress == NULL) {
                return RAFT_OK;
            }
            progress->recent_active = true;
            progress->message_flow_paused = false;
            result = raft_log_last_index(raft->log, &last_index);
            if (result != RAFT_OK) {
                return result;
            }
            if (progress->match_index < last_index ||
                progress->state == RAFT_PROGRESS_STATE_PROBE) {
                result = core_send_append(raft, progress);
                if (result != RAFT_OK) {
                    return result;
                }
            }
            if (raft->read_only.option != RAFT_READ_ONLY_SAFE ||
                message->context.len == 0) {
                return RAFT_OK;
            }
            result = raft_read_only_recv_ack(
                &raft->read_only,
                message->from,
                &message->context);
            return result == RAFT_OK
                       ? core_advance_read_only(raft)
                       : result;
        }
        case RAFT_MSG_SNAP_STATUS: {
            raft_progress_internal_t *progress =
                core_find_progress(raft, message->from);
            if (progress == NULL ||
                progress->state != RAFT_PROGRESS_STATE_SNAPSHOT) {
                return RAFT_OK;
            }
            if (message->reject) {
                progress->pending_snapshot = 0;
            }
            raft_progress_become_probe(progress);
            progress->message_flow_paused = true;
            return RAFT_OK;
        }
        case RAFT_MSG_UNREACHABLE: {
            raft_progress_internal_t *progress =
                core_find_progress(raft, message->from);
            if (progress != NULL &&
                progress->state ==
                    RAFT_PROGRESS_STATE_REPLICATE) {
                raft_progress_become_probe(progress);
            }
            return RAFT_OK;
        }
        case RAFT_MSG_TRANSFER_LEADER:
            return core_handle_transfer_leader(raft, message);
        case RAFT_MSG_FORGET_LEADER:
            return RAFT_OK;
        default:
            return RAFT_OK;
    }
}

int raft_core_init(raft_t *raft,
                   const raft_config_t *config,
                   raft_log_t *log,
                   const raft_storage_ops_t *storage) {
    raft_hard_state_t hard_state;
    raft_conf_state_t conf_state;
    uint64_t last_index;
    int result;

    if (raft == NULL || config == NULL || log == NULL || storage == NULL ||
        storage->initial_state == NULL) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    memset(raft, 0, sizeof(*raft));
    result = raft_tracker_init(&raft->tracker,
                               config->max_inflight_messages,
                               config->max_inflight_bytes);
    if (result != RAFT_OK) {
        return result;
    }
    raft->id = config->id;
    raft->log = log;
    raft->election_timeout = config->election_tick;
    raft->heartbeat_timeout = config->heartbeat_tick;
    raft_random_init(&raft->random, config->id);
    raft->max_size_per_message = config->max_size_per_message;
    raft->max_uncommitted_entries_size =
        config->max_uncommitted_entries_size;
    raft->disable_proposal_forwarding =
        config->disable_proposal_forwarding;
    raft->disable_conf_change_validation =
        config->disable_conf_change_validation;
    raft->step_down_on_removal = config->step_down_on_removal;
    raft->check_quorum = config->check_quorum;
    raft->pre_vote = config->pre_vote;
    raft->lead = RAFT_NONE;
    raft->vote = RAFT_NONE;
    raft->lead_transferee = RAFT_NONE;
    raft->state = RAFT_STATE_FOLLOWER;
    result = raft_read_only_init(
        &raft->read_only, config->read_only_option);
    if (result != RAFT_OK) {
        raft_core_free(raft);
        return result;
    }

    memset(&hard_state, 0, sizeof(hard_state));
    memset(&conf_state, 0, sizeof(conf_state));
    result = storage->initial_state(
        storage->handle, &hard_state, &conf_state);
    if (result != RAFT_OK) {
        raft_conf_state_free(&conf_state);
        raft_core_free(raft);
        return result;
    }
    result = core_set_configuration(raft, &conf_state);
    raft_conf_state_free(&conf_state);
    if (result != RAFT_OK) {
        raft_core_free(raft);
        return result;
    }
    if (hard_state.vote != RAFT_NONE &&
        !raft_is_valid_node_id(hard_state.vote)) {
        raft_core_free(raft);
        return RAFT_ERR_FATAL;
    }
    if (!core_hard_state_empty(&hard_state)) {
        result = raft_log_last_index(log, &last_index);
        if (result != RAFT_OK) {
            raft_core_free(raft);
            return result;
        }
        if (hard_state.commit < log->committed ||
            hard_state.commit > last_index) {
            raft_core_free(raft);
            return RAFT_ERR_FATAL;
        }
        log->committed = hard_state.commit;
        raft->term = hard_state.term;
        raft->vote = hard_state.vote;
    }
    if (config->applied != 0) {
        result = raft_log_applied_to(log, config->applied, 0);
        if (result != RAFT_OK) {
            raft_core_free(raft);
            return result;
        }
    }
    result = core_reset(raft, raft->term);
    if (result != RAFT_OK) {
        raft_core_free(raft);
        return result;
    }
    raft->state = RAFT_STATE_FOLLOWER;
    return RAFT_OK;
}

void raft_core_free(raft_t *raft) {
    if (raft == NULL) {
        return;
    }
    raft_read_only_free(&raft->read_only);
    raft_message_vec_free(&raft->pending_read_index_messages);
    raft_read_state_vec_free(&raft->read_states);
    raft_tracker_free(&raft->tracker);
    raft_message_vec_free(&raft->messages_after_append);
    raft_message_vec_free(&raft->messages);
    memset(raft, 0, sizeof(*raft));
}

static size_t core_varint_size(uint64_t value) {
    size_t size = 1;
    while (value >= 0x80) {
        value >>= 7;
        ++size;
    }
    return size;
}

static uint8_t *core_put_varint(uint8_t *out, uint64_t value) {
    while (value >= 0x80) {
        *out++ = (uint8_t)(value | 0x80);
        value >>= 7;
    }
    *out++ = (uint8_t)value;
    return out;
}

static int core_bootstrap_entry(raft_entry_t *entry,
                                const raft_peer_view_t *peer,
                                uint64_t index) {
    size_t size = 2 + 1 + core_varint_size(peer->id);
    uint8_t *cursor;

    if (!peer->context.is_nil) {
        if (peer->context.len > SIZE_MAX - size - 1 ||
            core_varint_size((uint64_t)peer->context.len) >
                SIZE_MAX - size - 1 - peer->context.len) {
            return RAFT_ERR_OUT_OF_MEMORY;
        }
        size += 1 + core_varint_size((uint64_t)peer->context.len) +
                peer->context.len;
    }
    memset(entry, 0, sizeof(*entry));
    entry->type = RAFT_ENTRY_CONF_CHANGE;
    entry->term = 1;
    entry->index = index;
    entry->data.is_nil = false;
    entry->data.data = malloc(size);
    if (entry->data.data == NULL) {
        return RAFT_ERR_OUT_OF_MEMORY;
    }
    entry->data.len = size;
    cursor = entry->data.data;
    *cursor++ = 0x10;  // optional ConfChange.Type, explicitly present.
    *cursor++ = 0;
    *cursor++ = 0x18;  // optional ConfChange.NodeId.
    cursor = core_put_varint(cursor, peer->id);
    if (!peer->context.is_nil) {
        *cursor++ = 0x22;  // optional ConfChange.Context.
        cursor = core_put_varint(cursor, (uint64_t)peer->context.len);
        if (peer->context.len != 0) {
            memcpy(cursor, peer->context.data, peer->context.len);
            cursor += peer->context.len;
        }
    }
    if ((size_t)(cursor - entry->data.data) != size) {
        raft_entry_free(entry);
        return RAFT_ERR_FATAL;
    }
    return RAFT_OK;
}

static int core_bootstrap_once(raft_t *raft,
                               const raft_peer_view_t *peers,
                               size_t peer_count,
                               bool *mutation_started) {
    raft_entry_t *entries;
    raft_conf_state_t state;
    uint64_t storage_last;
    uint64_t ignored;
    size_t i;
    size_t j;
    int result;

    if (mutation_started == NULL) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    *mutation_started = false;
    if (raft == NULL || peers == NULL || peer_count == 0 ||
        peer_count > UINT64_MAX ||
        peer_count > SIZE_MAX / sizeof(*entries)) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    if (raft->error != RAFT_OK) {
        return raft->error;
    }
    if (raft->tracker.progress_len != 0) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    result = raft->log->storage.last_index(
        raft->log->storage.handle, &storage_last);
    if (result != RAFT_OK) {
        return result;
    }
    if (storage_last != 0) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    for (i = 0; i < peer_count; ++i) {
        if (!raft_is_valid_node_id(peers[i].id) ||
            !raft_byte_view_valid(&peers[i].context)) {
            return RAFT_ERR_INVALID_ARGUMENT;
        }
        for (j = 0; j < i; ++j) {
            if (peers[j].id == peers[i].id) {
                return RAFT_ERR_INVALID_ARGUMENT;
            }
        }
    }
    *mutation_started = true;
    result = core_become_follower(raft, 1, RAFT_NONE);
    if (result != RAFT_OK) {
        return result;
    }
    entries = calloc(peer_count, sizeof(*entries));
    if (entries == NULL) {
        return RAFT_ERR_OUT_OF_MEMORY;
    }
    for (i = 0; i < peer_count; ++i) {
        result = core_bootstrap_entry(
            &entries[i], &peers[i], (uint64_t)i + 1);
        if (result != RAFT_OK) {
            raft_entry_array_free(entries, peer_count);
            return result;
        }
    }
    result = raft_log_append(
        raft->log, entries, peer_count, &ignored);
    raft_entry_array_free(entries, peer_count);
    if (result != RAFT_OK) {
        return result;
    }
    memset(&state, 0, sizeof(state));
    state.voters.items = calloc(peer_count, sizeof(*state.voters.items));
    if (state.voters.items == NULL) {
        return RAFT_ERR_OUT_OF_MEMORY;
    }
    state.voters.len = peer_count;
    for (i = 0; i < peer_count; ++i) {
        state.voters.items[i] = peers[i].id;
    }
    result = core_set_configuration(raft, &state);
    raft_conf_state_free(&state);
    if (result != RAFT_OK) {
        return result;
    }
    return raft_log_commit_to(raft->log, (uint64_t)peer_count);
}

int raft_core_bootstrap(raft_t *raft,
                        const raft_peer_view_t *peers,
                        size_t peer_count) {
    bool mutation_started;
    int result =
        core_bootstrap_once(raft, peers, peer_count, &mutation_started);

    return mutation_started ? raft_core_latch_error(raft, result) : result;
}

int raft_core_tick(raft_t *raft) {
    int result = RAFT_OK;

    if (raft == NULL) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    if (raft->error != RAFT_OK) {
        return raft->error;
    }
    if (raft->state == RAFT_STATE_LEADER) {
        ++raft->heartbeat_elapsed;
        ++raft->election_elapsed;
        if (raft->election_elapsed >= raft->election_timeout) {
            raft_message_view_t check;
            raft->election_elapsed = 0;
            if (raft->check_quorum) {
                memset(&check, 0, sizeof(check));
                check.type = RAFT_MSG_CHECK_QUORUM;
                check.from = raft->id;
                check.context.is_nil = true;
                result = core_step_leader(raft, &check);
                if (result != RAFT_OK) {
                    goto done;
                }
            }
            if (raft->state == RAFT_STATE_LEADER &&
                raft->lead_transferee != RAFT_NONE) {
                raft->lead_transferee = RAFT_NONE;
            }
            if (raft->state != RAFT_STATE_LEADER) {
                goto done;
            }
        }
        if (raft->heartbeat_elapsed >= raft->heartbeat_timeout) {
            raft->heartbeat_elapsed = 0;
            result = core_broadcast_heartbeat(raft);
        }
    } else {
        ++raft->election_elapsed;
        if (core_promotable(raft) &&
            raft->election_elapsed >=
                raft->randomized_election_timeout) {
            raft->election_elapsed = 0;
            result = core_hup(
                raft,
                raft->pre_vote
                    ? CORE_CAMPAIGN_PRE_ELECTION
                    : CORE_CAMPAIGN_ELECTION);
        }
    }
done:
    return raft_core_latch_error(raft, result);
}

void raft_core_tick_quiesced(raft_t *raft) {
    if (raft != NULL && raft->error == RAFT_OK &&
        raft->election_elapsed != UINT64_MAX) {
        ++raft->election_elapsed;
    }
}

int raft_core_campaign(raft_t *raft) {
    int result;

    if (raft == NULL) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    if (raft->error != RAFT_OK) {
        return raft->error;
    }
    result = core_hup(
        raft,
        raft->pre_vote
            ? CORE_CAMPAIGN_PRE_ELECTION
            : CORE_CAMPAIGN_ELECTION);
    return raft_core_latch_error(raft, result);
}

static int core_propose_entry(raft_t *raft,
                              raft_entry_type_t type,
                              const raft_byte_view_t *data) {
    raft_entry_view_t entry;
    raft_message_view_t message;

    if (raft == NULL || !raft_byte_view_valid(data)) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    memset(&entry, 0, sizeof(entry));
    entry.type = type;
    entry.data = *data;
    memset(&message, 0, sizeof(message));
    message.type = RAFT_MSG_PROP;
    message.from = raft->id;
    message.entries.items = &entry;
    message.entries.len = 1;
    return raft_core_step(raft, &message);
}

int raft_core_propose(raft_t *raft, const raft_byte_view_t *data) {
    return core_propose_entry(raft, RAFT_ENTRY_NORMAL, data);
}

int raft_core_propose_conf_change_v1(
    raft_t *raft, const raft_conf_change_view_t *change) {
    raft_bytes_t encoded;
    raft_byte_view_t view;
    int result;

    if (raft == NULL || change == NULL) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    if (raft->error != RAFT_OK) {
        return raft->error;
    }
    memset(&encoded, 0, sizeof(encoded));
    result = raft_confchange_encode_v1(change, &encoded);
    if (result != RAFT_OK) {
        return raft_core_latch_error(raft, result);
    }
    view = (raft_byte_view_t){
        .data = encoded.data,
        .len = encoded.len,
        .is_nil = encoded.is_nil,
    };
    result = core_propose_entry(
        raft, RAFT_ENTRY_CONF_CHANGE, &view);
    raft_bytes_free(&encoded);
    return result;
}

int raft_core_propose_conf_change_v2(
    raft_t *raft, const raft_conf_change_v2_view_t *change) {
    raft_bytes_t encoded;
    raft_byte_view_t view;
    int result;

    if (raft == NULL) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    if (raft->error != RAFT_OK) {
        return raft->error;
    }
    if (change == NULL) {
        view = (raft_byte_view_t){NULL, 0, true};
        return core_propose_entry(
            raft, RAFT_ENTRY_CONF_CHANGE_V2, &view);
    }
    memset(&encoded, 0, sizeof(encoded));
    result = raft_confchange_encode_v2(change, &encoded);
    if (result != RAFT_OK) {
        return raft_core_latch_error(raft, result);
    }
    view = (raft_byte_view_t){
        .data = encoded.data,
        .len = encoded.len,
        .is_nil = encoded.is_nil,
    };
    result = core_propose_entry(
        raft, RAFT_ENTRY_CONF_CHANGE_V2, &view);
    raft_bytes_free(&encoded);
    return result;
}

static int core_apply_conf_change_once(
    raft_t *raft,
    const raft_conf_change_v2_view_t *change,
    raft_conf_state_t *out) {
    raft_progress_internal_t *self;
    uint64_t last_index;
    bool committed;
    size_t i;
    int result;

    if (raft == NULL || change == NULL || out == NULL) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    memset(out, 0, sizeof(*out));
    result = raft_log_last_index(raft->log, &last_index);
    if (result != RAFT_OK) {
        return result;
    }
    result = raft_confchange_apply_view(
        &raft->tracker, change, last_index);
    if (result != RAFT_OK) {
        return result;
    }
    result = raft_tracker_conf_state_copy(&raft->tracker, out);
    if (result != RAFT_OK) {
        return result;
    }

    self = core_find_progress(raft, raft->id);
    if (raft->state == RAFT_STATE_LEADER &&
        (self == NULL || self->is_learner)) {
        if (raft->step_down_on_removal) {
            result = core_become_follower(
                raft, raft->term, RAFT_NONE);
        }
        return result;
    }
    if (raft->state != RAFT_STATE_LEADER ||
        raft->tracker.config.voters.len == 0) {
        return RAFT_OK;
    }

    result = core_maybe_commit(raft, &committed);
    if (result != RAFT_OK) {
        return result;
    }
    if (committed) {
        result = core_broadcast_append(raft);
    } else {
        for (i = 0;
             result == RAFT_OK &&
             i < raft->tracker.progress_len;
             ++i) {
            raft_progress_internal_t *progress =
                &raft->tracker.progress[i];
            if (progress->id != raft->id) {
                result = core_send_append(raft, progress);
            }
        }
    }
    if (result == RAFT_OK &&
        raft->lead_transferee != RAFT_NONE &&
        !raft_tracker_is_voter(
            &raft->tracker, raft->lead_transferee)) {
        raft->lead_transferee = RAFT_NONE;
    }
    return result;
}

int raft_core_apply_conf_change(
    raft_t *raft,
    const raft_conf_change_v2_view_t *change,
    raft_conf_state_t *out) {
    if (raft != NULL && raft->error != RAFT_OK) {
        return raft->error;
    }
    return raft_core_latch_error(
        raft, core_apply_conf_change_once(raft, change, out));
}

int raft_core_maybe_auto_leave(raft_t *raft, uint64_t applied) {
    int result;

    if (raft == NULL) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    if (!raft->tracker.config.auto_leave ||
        applied < raft->pending_conf_index ||
        raft->state != RAFT_STATE_LEADER) {
        return RAFT_OK;
    }
    result = raft_core_propose_conf_change_v2(raft, NULL);
    return result == RAFT_ERR_PROPOSAL_DROPPED ? RAFT_OK : result;
}

static int core_apply_storage_snapshot(
    raft_t *raft, const raft_snapshot_view_t *snapshot) {
    uint64_t index;
    int result;

    if (raft == NULL || snapshot == NULL) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    index = snapshot->metadata.index;
    raft_log_stable_snap_to(raft->log, index);
    result = raft_log_applied_to(raft->log, index, 0);
    return result == RAFT_OK
               ? raft_core_maybe_auto_leave(raft, index)
               : result;
}

static int core_handle_storage_apply_response(
    raft_t *raft, const raft_entry_view_vec_t *entries) {
    uint64_t applying_size = 0;
    uint64_t payload_size = 0;
    size_t i;
    int result;

    if (raft == NULL || entries == NULL ||
        !core_array_valid(entries->items, entries->len)) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    if (entries->len == 0) {
        return RAFT_OK;
    }
    for (i = 0; i < entries->len; ++i) {
        const raft_entry_view_t *entry = &entries->items[i];
        raft_entry_t sizing_entry;
        uint64_t entry_size;
        uint64_t entry_payload = (uint64_t)entry->data.len;

        memset(&sizing_entry, 0, sizeof(sizing_entry));
        sizing_entry.type = entry->type;
        sizing_entry.term = entry->term;
        sizing_entry.index = entry->index;
        sizing_entry.data.len = entry->data.len;
        entry_size = raft_log_entry_encoding_size(&sizing_entry);
        if (UINT64_MAX - applying_size < entry_size ||
            UINT64_MAX - payload_size < entry_payload) {
            return RAFT_ERR_FATAL;
        }
        applying_size += entry_size;
        payload_size += entry_payload;
    }
    result = raft_log_applied_to(
        raft->log,
        entries->items[entries->len - 1].index,
        applying_size);
    if (result != RAFT_OK) {
        return result;
    }
    raft_core_reduce_uncommitted(raft, payload_size);
    return raft_core_maybe_auto_leave(
        raft, entries->items[entries->len - 1].index);
}

static int core_step_once(raft_t *raft,
                          const raft_message_view_t *message) {
    int result;

    if (raft == NULL || !raft_message_view_valid(message)) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    if (raft->error != RAFT_OK) {
        return raft->error;
    }
    if (message->type == RAFT_MSG_STORAGE_APPEND ||
        message->type == RAFT_MSG_STORAGE_APPLY) {
        return RAFT_ERR_NOT_IMPLEMENTED;
    }
    if (message->term != 0 && message->term > raft->term) {
        bool vote_request =
            message->type == RAFT_MSG_VOTE ||
            message->type == RAFT_MSG_PRE_VOTE;
        bool forced_vote =
            vote_request &&
            !message->context.is_nil &&
            message->context.len ==
                sizeof(core_campaign_transfer_context) - 1 &&
            memcmp(message->context.data,
                   core_campaign_transfer_context,
                   sizeof(core_campaign_transfer_context) - 1) == 0;
        bool in_lease =
            raft->check_quorum && raft->lead != RAFT_NONE &&
            raft->election_elapsed < raft->election_timeout;
        if (vote_request && !forced_vote && in_lease) {
            return RAFT_OK;
        }
        if (message->type != RAFT_MSG_PRE_VOTE &&
            (message->type != RAFT_MSG_PRE_VOTE_RESP ||
             message->reject)) {
            uint64_t lead =
                message->type == RAFT_MSG_APP ||
                        message->type == RAFT_MSG_HEARTBEAT ||
                        message->type == RAFT_MSG_SNAP
                    ? message->from
                    : RAFT_NONE;
            result = core_become_follower(
                raft, message->term, lead);
            if (result != RAFT_OK) {
                return result;
            }
        }
    } else if (message->term != 0 && message->term < raft->term) {
        if ((raft->check_quorum || raft->pre_vote) &&
            (message->type == RAFT_MSG_APP ||
             message->type == RAFT_MSG_HEARTBEAT)) {
            return core_send_append_response(
                raft, message->from, 0, false, 0, 0);
        }
        if (message->type == RAFT_MSG_PRE_VOTE) {
            return core_send_vote_response(
                raft,
                message->from,
                raft->term,
                RAFT_MSG_PRE_VOTE_RESP,
                true);
        }
        if (message->type == RAFT_MSG_STORAGE_APPEND_RESP &&
            message->has_snapshot) {
            return core_apply_storage_snapshot(
                raft, &message->snapshot);
        }
        return RAFT_OK;
    }
    if (message->type == RAFT_MSG_STORAGE_APPEND_RESP) {
        if (message->index != 0) {
            raft_log_stable_to(
                raft->log, message->index, message->log_term);
        }
        if (message->has_snapshot) {
            return core_apply_storage_snapshot(
                raft, &message->snapshot);
        }
        return RAFT_OK;
    }
    if (message->type == RAFT_MSG_STORAGE_APPLY_RESP) {
        return core_handle_storage_apply_response(
            raft, &message->entries);
    }
    if (message->type == RAFT_MSG_HUP) {
        return core_hup(
            raft,
            raft->pre_vote
                ? CORE_CAMPAIGN_PRE_ELECTION
                : CORE_CAMPAIGN_ELECTION);
    }
    if (message->type == RAFT_MSG_VOTE ||
        message->type == RAFT_MSG_PRE_VOTE) {
        return core_handle_vote(raft, message);
    }
    switch (raft->state) {
        case RAFT_STATE_FOLLOWER:
            return core_step_follower(raft, message);
        case RAFT_STATE_CANDIDATE:
        case RAFT_STATE_PRE_CANDIDATE:
            return core_step_candidate(raft, message);
        case RAFT_STATE_LEADER:
            return core_step_leader(raft, message);
        default:
            return RAFT_ERR_FATAL;
    }
}

int raft_core_step(raft_t *raft, const raft_message_view_t *message) {
    return raft_core_latch_error(raft, core_step_once(raft, message));
}

void raft_core_hard_state(const raft_t *raft, raft_hard_state_t *state) {
    if (raft == NULL || state == NULL) {
        return;
    }
    state->term = raft->term;
    state->vote = raft->vote;
    state->commit = raft->log->committed;
}

void raft_core_soft_state(const raft_t *raft, raft_soft_state_t *state) {
    if (raft == NULL || state == NULL) {
        return;
    }
    state->lead = raft->lead;
    state->raft_state = raft->state;
}

bool raft_core_has_progress(const raft_t *raft, uint64_t id) {
    return raft != NULL &&
           raft_tracker_has_progress(&raft->tracker, id);
}

int raft_core_progress_snapshot(const raft_t *raft,
                                raft_progress_snapshot_t **out,
                                size_t *out_len) {
    if (raft == NULL || out == NULL || out_len == NULL) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    return raft_tracker_progress_snapshot(
        &raft->tracker, out, out_len);
}

int raft_core_status_progress_snapshot(
    const raft_t *raft,
    raft_status_progress_t **out,
    size_t *out_len) {
    if (raft == NULL || out == NULL || out_len == NULL) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    return raft_tracker_status_progress_snapshot(
        &raft->tracker, out, out_len);
}

int raft_core_conf_state_copy(const raft_t *raft, raft_conf_state_t *out) {
    if (raft == NULL || out == NULL) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    return raft_tracker_conf_state_copy(&raft->tracker, out);
}

int raft_core_ready_messages_copy(const raft_t *raft,
                                  raft_message_vec_t *out) {
    size_t i;

    if (raft == NULL || out == NULL) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    memset(out, 0, sizeof(*out));
    if (raft->messages.len == 0) {
        return RAFT_OK;
    }
    if (raft->messages.len > SIZE_MAX / sizeof(*out->items)) {
        return RAFT_ERR_OUT_OF_MEMORY;
    }
    out->items = calloc(raft->messages.len, sizeof(*out->items));
    if (out->items == NULL) {
        return RAFT_ERR_OUT_OF_MEMORY;
    }
    out->len = raft->messages.len;
    for (i = 0; i < raft->messages.len; ++i) {
        int result =
            core_message_copy(&out->items[i], &raft->messages.items[i]);
        if (result != RAFT_OK) {
            raft_message_vec_free(out);
            return result;
        }
    }
    return RAFT_OK;
}

int raft_core_ready_messages_after_append_copy(
    const raft_t *raft, raft_message_vec_t *out) {
    size_t i;

    if (raft == NULL || out == NULL) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    memset(out, 0, sizeof(*out));
    if (raft->messages_after_append.len == 0) {
        return RAFT_OK;
    }
    if (raft->messages_after_append.len >
        SIZE_MAX / sizeof(*out->items)) {
        return RAFT_ERR_OUT_OF_MEMORY;
    }
    out->items = calloc(
        raft->messages_after_append.len, sizeof(*out->items));
    if (out->items == NULL) {
        return RAFT_ERR_OUT_OF_MEMORY;
    }
    out->len = raft->messages_after_append.len;
    for (i = 0; i < raft->messages_after_append.len; ++i) {
        int result = core_message_copy(
            &out->items[i],
            &raft->messages_after_append.items[i]);
        if (result != RAFT_OK) {
            raft_message_vec_free(out);
            return result;
        }
    }
    return RAFT_OK;
}

int raft_core_ready_read_states_copy(const raft_t *raft,
                                     raft_read_state_vec_t *out) {
    size_t i;

    if (raft == NULL || out == NULL) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    memset(out, 0, sizeof(*out));
    if (raft->read_states.len == 0) {
        return RAFT_OK;
    }
    if (raft->read_states.len > SIZE_MAX / sizeof(*out->items)) {
        return RAFT_ERR_OUT_OF_MEMORY;
    }
    out->items = calloc(
        raft->read_states.len, sizeof(*out->items));
    if (out->items == NULL) {
        return RAFT_ERR_OUT_OF_MEMORY;
    }
    out->len = raft->read_states.len;
    for (i = 0; i < raft->read_states.len; ++i) {
        int result;
        out->items[i].index = raft->read_states.items[i].index;
        out->items[i].request_ctx.is_nil = true;
        result = raft_bytes_copy(
            &out->items[i].request_ctx,
            &raft->read_states.items[i].request_ctx);
        if (result != RAFT_OK) {
            raft_read_state_vec_free(out);
            return result;
        }
    }
    return RAFT_OK;
}

void raft_core_clear_messages(raft_t *raft) {
    if (raft != NULL) {
        raft_message_vec_free(&raft->messages);
    }
}

void raft_core_clear_messages_after_append(raft_t *raft) {
    if (raft != NULL) {
        raft_message_vec_free(&raft->messages_after_append);
    }
}

void raft_core_clear_read_states(raft_t *raft) {
    if (raft != NULL) {
        raft_read_state_vec_free(&raft->read_states);
    }
}

void raft_core_reduce_uncommitted(raft_t *raft, uint64_t payload_size) {
    if (raft == NULL) {
        return;
    }
    if (raft->uncommitted_size > payload_size) {
        raft->uncommitted_size -= payload_size;
    } else {
        raft->uncommitted_size = 0;
    }
}
