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

static uint64_t core_min_u64(uint64_t left, uint64_t right) {
    return left < right ? left : right;
}

static bool core_array_valid(const void *items, size_t len) {
    return len == 0 || items != NULL;
}

static bool core_hard_state_empty(const raft_hard_state_t *state) {
    return state->term == 0 && state->vote == RAFT_NONE &&
           state->commit == 0;
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

static int core_send(raft_t *raft, raft_message_t *message) {
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
    if (message->to == RAFT_NONE || message->to == raft->id) {
        return RAFT_ERR_FATAL;
    }
    return core_message_vec_push(&raft->messages, message);
}

static int core_progress_compare(const void *left, const void *right) {
    const raft_basic_progress_internal_t *a = left;
    const raft_basic_progress_internal_t *b = right;
    if (a->id < b->id) {
        return -1;
    }
    if (a->id > b->id) {
        return 1;
    }
    return 0;
}

static raft_basic_progress_internal_t *core_find_progress(
    raft_t *raft, uint64_t id) {
    size_t i;
    if (raft == NULL) {
        return NULL;
    }
    for (i = 0; i < raft->progress_len; ++i) {
        if (raft->progress[i].id == id) {
            return &raft->progress[i];
        }
    }
    return NULL;
}

static const raft_basic_progress_internal_t *core_find_progress_const(
    const raft_t *raft, uint64_t id) {
    size_t i;
    if (raft == NULL) {
        return NULL;
    }
    for (i = 0; i < raft->progress_len; ++i) {
        if (raft->progress[i].id == id) {
            return &raft->progress[i];
        }
    }
    return NULL;
}

static size_t core_voter_count(const raft_t *raft) {
    size_t count = 0;
    size_t i;
    for (i = 0; i < raft->progress_len; ++i) {
        if (!raft->progress[i].is_learner) {
            ++count;
        }
    }
    return count;
}

static size_t core_quorum(const raft_t *raft) {
    size_t voters = core_voter_count(raft);
    return voters / 2 + 1;
}

static bool core_is_voter(const raft_t *raft, uint64_t id) {
    const raft_basic_progress_internal_t *progress =
        core_find_progress_const(raft, id);
    return progress != NULL && !progress->is_learner;
}

static bool core_promotable(const raft_t *raft) {
    return core_is_voter(raft, raft->id) &&
           !raft_log_has_next_or_in_progress_snapshot(raft->log);
}

static int core_set_configuration(raft_t *raft,
                                  const raft_conf_state_t *state) {
    raft_conf_state_t copied;
    raft_basic_progress_internal_t *progress = NULL;
    uint64_t last_index;
    size_t count;
    size_t i;
    size_t j;
    int result;

    if (raft == NULL || state == NULL ||
        !core_array_valid(state->voters.items, state->voters.len) ||
        !core_array_valid(state->voters_outgoing.items,
                          state->voters_outgoing.len) ||
        !core_array_valid(state->learners.items, state->learners.len) ||
        !core_array_valid(state->learners_next.items,
                          state->learners_next.len)) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    if (state->voters_outgoing.len != 0 ||
        state->learners_next.len != 0 || state->auto_leave) {
        return RAFT_ERR_NOT_IMPLEMENTED;
    }
    if (state->voters.len > SIZE_MAX - state->learners.len) {
        return RAFT_ERR_OUT_OF_MEMORY;
    }
    count = state->voters.len + state->learners.len;
    result = raft_log_last_index(raft->log, &last_index);
    if (result != RAFT_OK) {
        return result;
    }
    if (last_index == UINT64_MAX) {
        return RAFT_ERR_FATAL;
    }
    memset(&copied, 0, sizeof(copied));
    result = core_conf_state_copy(&copied, state);
    if (result != RAFT_OK) {
        return result;
    }
    if (count != 0) {
        if (count > SIZE_MAX / sizeof(*progress)) {
            raft_conf_state_free(&copied);
            return RAFT_ERR_OUT_OF_MEMORY;
        }
        progress = calloc(count, sizeof(*progress));
        if (progress == NULL) {
            raft_conf_state_free(&copied);
            return RAFT_ERR_OUT_OF_MEMORY;
        }
    }
    for (i = 0; i < count; ++i) {
        uint64_t id = i < state->voters.len
                          ? state->voters.items[i]
                          : state->learners.items[i - state->voters.len];
        if (!raft_is_valid_node_id(id)) {
            free(progress);
            raft_conf_state_free(&copied);
            return RAFT_ERR_FATAL;
        }
        for (j = 0; j < i; ++j) {
            if (progress[j].id == id) {
                free(progress);
                raft_conf_state_free(&copied);
                return RAFT_ERR_FATAL;
            }
        }
        progress[i].id = id;
        progress[i].next_index = last_index + 1;
        progress[i].state = RAFT_PROGRESS_STATE_PROBE;
        progress[i].is_learner = i >= state->voters.len;
    }
    if (count > 1) {
        qsort(progress, count, sizeof(*progress), core_progress_compare);
    }
    free(raft->progress);
    raft_conf_state_free(&raft->conf_state);
    raft->progress = progress;
    raft->progress_len = count;
    raft->conf_state = copied;
    return RAFT_OK;
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
    raft->randomized_election_timeout = raft->election_timeout;
    raft->uncommitted_size = 0;
    result = raft_log_last_index(raft->log, &last_index);
    if (result != RAFT_OK) {
        return result;
    }
    if (last_index == UINT64_MAX) {
        return RAFT_ERR_FATAL;
    }
    for (i = 0; i < raft->progress_len; ++i) {
        raft_basic_progress_internal_t *progress = &raft->progress[i];
        progress->match_index = 0;
        progress->next_index = last_index + 1;
        progress->state = RAFT_PROGRESS_STATE_PROBE;
        progress->pending_snapshot = 0;
        progress->recent_active = progress->id == raft->id;
        progress->message_flow_paused = false;
        progress->vote_recorded = false;
        progress->vote_granted = false;
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
    raft_basic_progress_internal_t *self;
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
    self->vote_recorded = true;
    self->vote_granted = true;
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
    uint64_t candidate = 0;
    size_t quorum = core_quorum(raft);
    size_t i;

    if (changed == NULL) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    *changed = false;
    if (quorum == 0 || core_voter_count(raft) == 0) {
        return RAFT_OK;
    }
    for (i = 0; i < raft->progress_len; ++i) {
        size_t matched = 0;
        size_t j;
        uint64_t index;
        if (raft->progress[i].is_learner) {
            continue;
        }
        index = raft->progress[i].match_index;
        for (j = 0; j < raft->progress_len; ++j) {
            if (!raft->progress[j].is_learner &&
                raft->progress[j].match_index >= index) {
                ++matched;
            }
        }
        if (matched >= quorum && index > candidate) {
            candidate = index;
        }
    }
    return raft_log_maybe_commit(
        raft->log, candidate, raft->term, changed);
}

static int core_append_entries(raft_t *raft,
                               const raft_entry_view_t *entries,
                               size_t entry_count) {
    raft_entry_t *owned;
    raft_basic_progress_internal_t *self;
    uint64_t last_index;
    uint64_t payload_size;
    size_t i;
    int result;
    bool committed;

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
    if (payload_size > raft->max_uncommitted_entries_size ||
        raft->uncommitted_size >
            raft->max_uncommitted_entries_size - payload_size) {
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
    self = core_find_progress(raft, raft->id);
    if (self == NULL) {
        return RAFT_ERR_PROPOSAL_DROPPED;
    }
    self->match_index = last_index;
    self->next_index =
        last_index == UINT64_MAX ? UINT64_MAX : last_index + 1;
    result = core_maybe_commit(raft, &committed);
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
    raft_basic_progress_internal_t *self;
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
    self->match_index = last_index;
    self->next_index =
        last_index == UINT64_MAX ? UINT64_MAX : last_index + 1;
    self->state = RAFT_PROGRESS_STATE_REPLICATE;
    self->recent_active = true;
    return core_append_noop(raft);
}

static int core_send_vote_request(raft_t *raft, uint64_t to) {
    raft_message_t message;
    uint64_t last_index;
    uint64_t last_term;
    int result;

    result = raft_log_last_index(raft->log, &last_index);
    if (result != RAFT_OK) {
        return result;
    }
    result = raft_log_term(raft->log, last_index, &last_term);
    if (result != RAFT_OK) {
        return result;
    }
    core_message_init(&message, RAFT_MSG_VOTE);
    message.to = to;
    message.term = raft->term;
    message.index = last_index;
    message.log_term = last_term;
    result = core_send(raft, &message);
    raft_message_free(&message);
    return result;
}

static int core_send_append(raft_t *raft,
                            raft_basic_progress_internal_t *progress) {
    raft_message_t message;
    uint64_t previous_index;
    uint64_t previous_term;
    int result;

    if (progress == NULL || progress->id == raft->id) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    if (progress->next_index == 0) {
        progress->next_index = 1;
    }
    previous_index = progress->next_index - 1;
    result = raft_log_term(raft->log, previous_index, &previous_term);
    if (result != RAFT_OK) {
        return result;
    }
    core_message_init(&message, RAFT_MSG_APP);
    message.to = progress->id;
    message.index = previous_index;
    message.log_term = previous_term;
    message.commit = raft->log->committed;
    result = raft_log_entries(raft->log,
                              progress->next_index,
                              raft->max_size_per_message,
                              &message.entries);
    if (result != RAFT_OK) {
        raft_message_free(&message);
        return result;
    }
    result = core_send(raft, &message);
    raft_message_free(&message);
    return result;
}

static int core_broadcast_append(raft_t *raft) {
    size_t i;
    for (i = 0; i < raft->progress_len; ++i) {
        int result;
        if (raft->progress[i].id == raft->id) {
            continue;
        }
        result = core_send_append(raft, &raft->progress[i]);
        if (result != RAFT_OK) {
            return result;
        }
    }
    return RAFT_OK;
}

static int core_send_heartbeat(raft_t *raft,
                               raft_basic_progress_internal_t *progress) {
    raft_message_t message;
    int result;

    if (progress == NULL || progress->id == raft->id) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    core_message_init(&message, RAFT_MSG_HEARTBEAT);
    message.to = progress->id;
    message.commit =
        core_min_u64(progress->match_index, raft->log->committed);
    result = core_send(raft, &message);
    raft_message_free(&message);
    return result;
}

static int core_broadcast_heartbeat(raft_t *raft) {
    size_t i;
    for (i = 0; i < raft->progress_len; ++i) {
        int result;
        if (raft->progress[i].id == raft->id) {
            continue;
        }
        result = core_send_heartbeat(raft, &raft->progress[i]);
        if (result != RAFT_OK) {
            return result;
        }
    }
    return RAFT_OK;
}

static int core_campaign_election(raft_t *raft) {
    size_t i;
    int result;

    if (raft->state == RAFT_STATE_LEADER || !core_promotable(raft)) {
        return RAFT_OK;
    }
    result = core_become_candidate(raft);
    if (result != RAFT_OK) {
        return result;
    }
    if (core_quorum(raft) == 1) {
        return core_become_leader(raft);
    }
    for (i = 0; i < raft->progress_len; ++i) {
        if (raft->progress[i].is_learner ||
            raft->progress[i].id == raft->id) {
            continue;
        }
        result = core_send_vote_request(raft, raft->progress[i].id);
        if (result != RAFT_OK) {
            return result;
        }
    }
    return RAFT_OK;
}

static int core_send_vote_response(raft_t *raft,
                                   uint64_t to,
                                   uint64_t term,
                                   bool reject) {
    raft_message_t response;
    int result;

    core_message_init(&response, RAFT_MSG_VOTE_RESP);
    response.to = to;
    response.term = term;
    response.reject = reject;
    result = core_send(raft, &response);
    raft_message_free(&response);
    return result;
}

static int core_handle_vote(raft_t *raft,
                            const raft_message_view_t *message) {
    bool can_vote =
        raft->vote == RAFT_NONE || raft->vote == message->from;
    bool up_to_date = raft_log_is_up_to_date(
        raft->log, message->index, message->log_term);
    bool grant = can_vote && up_to_date &&
                 raft_is_valid_node_id(message->from);

    if (grant) {
        raft->vote = message->from;
        raft->election_elapsed = 0;
    }
    return core_send_vote_response(
        raft, message->from, message->term, !grant);
}

static int core_handle_vote_response(raft_t *raft,
                                     const raft_message_view_t *message) {
    raft_basic_progress_internal_t *progress =
        core_find_progress(raft, message->from);
    size_t granted = 0;
    size_t rejected = 0;
    size_t quorum = core_quorum(raft);
    size_t i;
    int result;

    if (raft->state != RAFT_STATE_CANDIDATE || progress == NULL ||
        progress->is_learner) {
        return RAFT_OK;
    }
    if (!progress->vote_recorded) {
        progress->vote_recorded = true;
        progress->vote_granted = !message->reject;
    }
    for (i = 0; i < raft->progress_len; ++i) {
        if (raft->progress[i].is_learner ||
            !raft->progress[i].vote_recorded) {
            continue;
        }
        if (raft->progress[i].vote_granted) {
            ++granted;
        } else {
            ++rejected;
        }
    }
    if (granted >= quorum) {
        result = core_become_leader(raft);
        if (result != RAFT_OK) {
            return result;
        }
        return core_broadcast_append(raft);
    }
    if (rejected >= quorum) {
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
        case RAFT_MSG_FORGET_LEADER:
            raft->lead = RAFT_NONE;
            return RAFT_OK;
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
        case RAFT_MSG_VOTE_RESP:
            return core_handle_vote_response(raft, message);
        default:
            return RAFT_OK;
    }
}

static int core_handle_append_response(
    raft_t *raft, const raft_message_view_t *message) {
    raft_basic_progress_internal_t *progress =
        core_find_progress(raft, message->from);
    uint64_t last_index;
    bool committed;
    int result;

    if (progress == NULL) {
        return RAFT_OK;
    }
    progress->recent_active = true;
    if (message->reject) {
        uint64_t next = message->reject_hint == UINT64_MAX
                            ? UINT64_MAX
                            : message->reject_hint + 1;
        if (next >= progress->next_index && progress->next_index > 1) {
            next = progress->next_index - 1;
        }
        if (next <= progress->match_index) {
            next = progress->match_index == UINT64_MAX
                       ? UINT64_MAX
                       : progress->match_index + 1;
        }
        if (next == 0) {
            next = 1;
        }
        progress->next_index = next;
        progress->state = RAFT_PROGRESS_STATE_PROBE;
        return core_send_append(raft, progress);
    }
    if (message->index > progress->match_index) {
        progress->match_index = message->index;
        progress->next_index =
            message->index == UINT64_MAX ? UINT64_MAX
                                         : message->index + 1;
        progress->state = RAFT_PROGRESS_STATE_REPLICATE;
    }
    result = core_maybe_commit(raft, &committed);
    if (result != RAFT_OK) {
        return result;
    }
    if (committed) {
        return core_broadcast_append(raft);
    }
    result = raft_log_last_index(raft->log, &last_index);
    if (result != RAFT_OK) {
        return result;
    }
    if (progress->match_index < last_index) {
        return core_send_append(raft, progress);
    }
    return RAFT_OK;
}

static int core_step_leader(raft_t *raft,
                            const raft_message_view_t *message) {
    switch (message->type) {
        case RAFT_MSG_BEAT:
            return core_broadcast_heartbeat(raft);
        case RAFT_MSG_PROP: {
            int result = core_append_entries(
                raft, message->entries.items, message->entries.len);
            if (result != RAFT_OK) {
                return result;
            }
            return core_broadcast_append(raft);
        }
        case RAFT_MSG_APP_RESP:
            return core_handle_append_response(raft, message);
        case RAFT_MSG_HEARTBEAT_RESP: {
            raft_basic_progress_internal_t *progress =
                core_find_progress(raft, message->from);
            uint64_t last_index;
            int result;
            if (progress == NULL) {
                return RAFT_OK;
            }
            progress->recent_active = true;
            result = raft_log_last_index(raft->log, &last_index);
            if (result != RAFT_OK) {
                return result;
            }
            if (progress->match_index < last_index ||
                progress->state == RAFT_PROGRESS_STATE_PROBE) {
                return core_send_append(raft, progress);
            }
            return RAFT_OK;
        }
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
    raft->id = config->id;
    raft->log = log;
    raft->election_timeout = config->election_tick;
    raft->heartbeat_timeout = config->heartbeat_tick;
    raft->randomized_election_timeout = config->election_tick;
    raft->max_size_per_message = config->max_size_per_message;
    raft->max_uncommitted_entries_size =
        config->max_uncommitted_entries_size;
    raft->disable_proposal_forwarding =
        config->disable_proposal_forwarding;
    raft->lead = RAFT_NONE;
    raft->vote = RAFT_NONE;
    raft->lead_transferee = RAFT_NONE;
    raft->state = RAFT_STATE_FOLLOWER;

    memset(&hard_state, 0, sizeof(hard_state));
    memset(&conf_state, 0, sizeof(conf_state));
    result = storage->initial_state(
        storage->handle, &hard_state, &conf_state);
    if (result != RAFT_OK) {
        raft_conf_state_free(&conf_state);
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
    raft_conf_state_free(&raft->conf_state);
    free(raft->progress);
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

int raft_core_bootstrap(raft_t *raft,
                        const raft_peer_view_t *peers,
                        size_t peer_count) {
    raft_entry_t *entries;
    raft_conf_state_t state;
    uint64_t storage_last;
    uint64_t ignored;
    size_t i;
    size_t j;
    int result;

    if (raft == NULL || peers == NULL || peer_count == 0 ||
        peer_count > UINT64_MAX ||
        peer_count > SIZE_MAX / sizeof(*entries)) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    if (raft->progress_len != 0) {
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
            result = core_campaign_election(raft);
        }
    }
    if (result == RAFT_ERR_FATAL ||
        result == RAFT_ERR_OUT_OF_MEMORY ||
        result == RAFT_ERR_PANIC_FROM_GO_CALLBACK) {
        raft->error = result;
    }
    return result;
}

void raft_core_tick_quiesced(raft_t *raft) {
    if (raft != NULL && raft->election_elapsed != UINT32_MAX) {
        ++raft->election_elapsed;
    }
}

int raft_core_campaign(raft_t *raft) {
    if (raft == NULL) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    if (raft->error != RAFT_OK) {
        return raft->error;
    }
    return core_campaign_election(raft);
}

int raft_core_propose(raft_t *raft, const raft_byte_view_t *data) {
    raft_entry_view_t entry;
    int result;

    if (raft == NULL || !raft_byte_view_valid(data)) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    if (raft->error != RAFT_OK) {
        return raft->error;
    }
    memset(&entry, 0, sizeof(entry));
    entry.type = RAFT_ENTRY_NORMAL;
    entry.data = *data;
    if (raft->state == RAFT_STATE_LEADER) {
        result = core_append_entries(raft, &entry, 1);
        if (result != RAFT_OK) {
            return result;
        }
        return core_broadcast_append(raft);
    }
    if (raft->state == RAFT_STATE_FOLLOWER &&
        raft->lead != RAFT_NONE &&
        !raft->disable_proposal_forwarding) {
        raft_message_t message;
        core_message_init(&message, RAFT_MSG_PROP);
        message.to = raft->lead;
        message.from = raft->id;
        message.entries.items = calloc(1, sizeof(*message.entries.items));
        if (message.entries.items == NULL) {
            return RAFT_ERR_OUT_OF_MEMORY;
        }
        message.entries.len = 1;
        result = raft_entry_copy_from_view(
            &message.entries.items[0], &entry);
        if (result == RAFT_OK) {
            result = core_send(raft, &message);
        }
        raft_message_free(&message);
        return result;
    }
    return RAFT_ERR_PROPOSAL_DROPPED;
}

int raft_core_step(raft_t *raft, const raft_message_view_t *message) {
    int result;

    if (raft == NULL || !raft_message_view_valid(message)) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    if (raft->error != RAFT_OK) {
        return raft->error;
    }
    if (message->type == RAFT_MSG_PRE_VOTE ||
        message->type == RAFT_MSG_PRE_VOTE_RESP ||
        message->type == RAFT_MSG_SNAP ||
        message->type == RAFT_MSG_UNREACHABLE ||
        message->type == RAFT_MSG_SNAP_STATUS ||
        message->type == RAFT_MSG_CHECK_QUORUM ||
        message->type == RAFT_MSG_TRANSFER_LEADER ||
        message->type == RAFT_MSG_TIMEOUT_NOW ||
        message->type == RAFT_MSG_READ_INDEX ||
        message->type == RAFT_MSG_READ_INDEX_RESP ||
        message->type == RAFT_MSG_STORAGE_APPEND ||
        message->type == RAFT_MSG_STORAGE_APPEND_RESP ||
        message->type == RAFT_MSG_STORAGE_APPLY ||
        message->type == RAFT_MSG_STORAGE_APPLY_RESP) {
        return RAFT_ERR_NOT_IMPLEMENTED;
    }
    if (message->term != 0 && message->term > raft->term) {
        uint64_t lead =
            message->type == RAFT_MSG_APP ||
                    message->type == RAFT_MSG_HEARTBEAT
                ? message->from
                : RAFT_NONE;
        result = core_become_follower(raft, message->term, lead);
        if (result != RAFT_OK) {
            return result;
        }
    } else if (message->term != 0 && message->term < raft->term) {
        if (message->type == RAFT_MSG_VOTE) {
            return core_send_vote_response(
                raft, message->from, raft->term, true);
        }
        if (message->type == RAFT_MSG_APP ||
            message->type == RAFT_MSG_HEARTBEAT) {
            return core_send_append_response(
                raft, message->from, 0, true, 0, 0);
        }
        return RAFT_OK;
    }
    if (message->type == RAFT_MSG_HUP) {
        return core_campaign_election(raft);
    }
    if (message->type == RAFT_MSG_VOTE) {
        return core_handle_vote(raft, message);
    }
    switch (raft->state) {
        case RAFT_STATE_FOLLOWER:
            return core_step_follower(raft, message);
        case RAFT_STATE_CANDIDATE:
            return core_step_candidate(raft, message);
        case RAFT_STATE_LEADER:
            return core_step_leader(raft, message);
        default:
            return RAFT_ERR_NOT_IMPLEMENTED;
    }
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
    return core_find_progress_const(raft, id) != NULL;
}

int raft_core_progress_snapshot(const raft_t *raft,
                                raft_progress_snapshot_t **out,
                                size_t *out_len) {
    raft_progress_snapshot_t *snapshots;
    size_t i;

    if (raft == NULL || out == NULL || out_len == NULL) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    *out = NULL;
    *out_len = 0;
    if (raft->progress_len == 0) {
        return RAFT_OK;
    }
    if (raft->progress_len > SIZE_MAX / sizeof(*snapshots)) {
        return RAFT_ERR_OUT_OF_MEMORY;
    }
    snapshots = calloc(raft->progress_len, sizeof(*snapshots));
    if (snapshots == NULL) {
        return RAFT_ERR_OUT_OF_MEMORY;
    }
    for (i = 0; i < raft->progress_len; ++i) {
        const raft_basic_progress_internal_t *src = &raft->progress[i];
        raft_progress_snapshot_t *dst = &snapshots[i];
        dst->id = src->id;
        dst->type =
            src->is_learner ? RAFT_PROGRESS_LEARNER : RAFT_PROGRESS_PEER;
        dst->progress.match_index = src->match_index;
        dst->progress.next_index = src->next_index;
        dst->progress.state = src->state;
        dst->progress.pending_snapshot = src->pending_snapshot;
        dst->progress.recent_active = src->recent_active;
        dst->progress.message_flow_paused = src->message_flow_paused;
        dst->progress.is_learner = src->is_learner;
    }
    *out = snapshots;
    *out_len = raft->progress_len;
    return RAFT_OK;
}

int raft_core_conf_state_copy(const raft_t *raft, raft_conf_state_t *out) {
    if (raft == NULL || out == NULL) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    return core_conf_state_copy(out, &raft->conf_state);
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

void raft_core_clear_messages(raft_t *raft) {
    if (raft != NULL) {
        raft_message_vec_free(&raft->messages);
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
