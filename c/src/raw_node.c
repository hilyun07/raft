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

#include "raft_internal.h"

#include <stdlib.h>
#include <string.h>

// validity, option checker

bool raft_is_none_id(uint64_t id) {
    return id == RAFT_NONE;
}

bool raft_is_local_target_id(uint64_t id) {
    return id == RAFT_LOCAL_APPEND_THREAD ||
           id == RAFT_LOCAL_APPLY_THREAD;
}

bool raft_is_valid_node_id(uint64_t id) {
    return !raft_is_none_id(id) && !raft_is_local_target_id(id);
}

static bool storage_ops_valid(const raft_storage_ops_t *storage) {
    return storage != NULL && storage->initial_state != NULL &&
           storage->entries != NULL && storage->term != NULL &&
           storage->first_index != NULL && storage->last_index != NULL &&
           storage->snapshot != NULL;
}

static bool read_only_option_valid(raft_read_only_option_t option) {
    return option == RAFT_READ_ONLY_SAFE ||
           option == RAFT_READ_ONLY_LEASE_BASED;
}

static bool config_valid(const raft_config_t *config) {
    if (config == NULL || !raft_is_valid_node_id(config->id) ||
        config->heartbeat_tick == 0 ||
        config->election_tick <= config->heartbeat_tick ||
        config->max_inflight_messages == 0 ||
        !read_only_option_valid(config->read_only_option)) {
        return false;
    }
    if (config->max_inflight_bytes != 0 &&
        config->max_inflight_bytes < config->max_size_per_message) {
        return false;
    }
    if (config->read_only_option == RAFT_READ_ONLY_LEASE_BASED &&
        !config->check_quorum) {
        return false;
    }
    return true;
}

static bool byte_shape_valid(const uint8_t *data,
                             size_t len,
                             bool is_nil) {
    if (is_nil) {
        return data == NULL && len == 0;
    }
    if (len == 0) {
        return data == NULL;
    }
    return data != NULL;
}

bool raft_byte_view_valid(const raft_byte_view_t *view) {
    return view != NULL &&
           byte_shape_valid(view->data, view->len, view->is_nil);
}

bool raft_bytes_valid(const raft_bytes_t *bytes) {
    return bytes != NULL &&
           byte_shape_valid(bytes->data, bytes->len, bytes->is_nil);
}

static bool conf_change_type_valid(raft_conf_change_type_t type) {
    return type >= RAFT_CONF_CHANGE_ADD_NODE &&
           type <= RAFT_CONF_CHANGE_ADD_LEARNER_NODE;
}

static bool conf_change_transition_valid(
    raft_conf_change_transition_t transition) {
    return transition >= RAFT_CONF_CHANGE_TRANSITION_AUTO &&
           transition <= RAFT_CONF_CHANGE_TRANSITION_JOINT_EXPLICIT;
}

// A zero node ID is an etcd compatibility no-op when a configuration change
// is applied. The C-only local target sentinels remain invalid membership IDs.
static bool public_conf_change_v2_valid(
    const raft_conf_change_v2_view_t *conf_change) {
    size_t i;

    if (conf_change == NULL ||
        !raft_byte_view_valid(&conf_change->context) ||
        !conf_change_transition_valid(conf_change->transition)) {
        return false;
    }
    if (conf_change->changes_len != 0 && conf_change->changes == NULL) {
        return false;
    }
    for (i = 0; i < conf_change->changes_len; ++i) {
        const raft_conf_change_single_t *change = &conf_change->changes[i];
        if (!conf_change_type_valid(change->type) ||
            (change->node_id != RAFT_NONE &&
             !raft_is_valid_node_id(change->node_id))) {
            return false;
        }
    }
    return true;
}

static bool public_conf_change_v1_valid(
    const raft_conf_change_view_t *conf_change) {
    return conf_change != NULL &&
           raft_byte_view_valid(&conf_change->context) &&
           conf_change_type_valid(conf_change->type) &&
           (conf_change->node_id == RAFT_NONE ||
            raft_is_valid_node_id(conf_change->node_id));
}

static bool message_type_valid(raft_message_type_t type) {
    return type >= RAFT_MSG_HUP && type <= RAFT_MSG_FORGET_LEADER;
}

static bool message_type_is_local(raft_message_type_t type) {
    switch (type) {
        case RAFT_MSG_HUP:
        case RAFT_MSG_BEAT:
        case RAFT_MSG_UNREACHABLE:
        case RAFT_MSG_SNAP_STATUS:
        case RAFT_MSG_CHECK_QUORUM:
        case RAFT_MSG_STORAGE_APPEND:
        case RAFT_MSG_STORAGE_APPEND_RESP:
        case RAFT_MSG_STORAGE_APPLY:
        case RAFT_MSG_STORAGE_APPLY_RESP:
            return true;
        default:
            return false;
    }
}

static bool entry_type_valid(raft_entry_type_t type) {
    return type >= RAFT_ENTRY_NORMAL && type <= RAFT_ENTRY_CONF_CHANGE_V2;
}

static bool array_shape_valid(const void *items, size_t len) {
    return len == 0 || items != NULL;
}

static bool uint64_view_valid(const raft_uint64_view_t *view) {
    return view != NULL && array_shape_valid(view->items, view->len);
}

static bool uint64_vec_valid(const raft_uint64_vec_t *vec) {
    return vec != NULL && array_shape_valid(vec->items, vec->len);
}

static bool conf_state_view_valid(const raft_conf_state_view_t *state) {
    return state != NULL &&
           uint64_view_valid(&state->voters) &&
           uint64_view_valid(&state->voters_outgoing) &&
           uint64_view_valid(&state->learners) &&
           uint64_view_valid(&state->learners_next);
}

static bool conf_state_valid(const raft_conf_state_t *state) {
    return state != NULL &&
           uint64_vec_valid(&state->voters) &&
           uint64_vec_valid(&state->voters_outgoing) &&
           uint64_vec_valid(&state->learners) &&
           uint64_vec_valid(&state->learners_next);
}

bool raft_entry_view_valid(const raft_entry_view_t *entry) {
    return entry != NULL && entry_type_valid(entry->type) &&
           raft_byte_view_valid(&entry->data);
}

bool raft_entry_valid(const raft_entry_t *entry) {
    return entry != NULL && entry_type_valid(entry->type) &&
           raft_bytes_valid(&entry->data);
}

bool raft_snapshot_view_valid(const raft_snapshot_view_t *snapshot) {
    return snapshot != NULL &&
           raft_byte_view_valid(&snapshot->data) &&
           conf_state_view_valid(&snapshot->metadata.conf_state);
}

bool raft_snapshot_valid(const raft_snapshot_t *snapshot) {
    return snapshot != NULL &&
           raft_bytes_valid(&snapshot->data) &&
           conf_state_valid(&snapshot->metadata.conf_state);
}

bool raft_message_view_valid(const raft_message_view_t *message) {
    size_t i;

    if (message == NULL || !message_type_valid(message->type) ||
        !raft_byte_view_valid(&message->context) ||
        !array_shape_valid(message->entries.items, message->entries.len) ||
        !array_shape_valid(message->responses.items, message->responses.len)) {
        return false;
    }
    for (i = 0; i < message->entries.len; ++i) {
        if (!raft_entry_view_valid(&message->entries.items[i])) {
            return false;
        }
    }
    if (message->has_snapshot &&
        !raft_snapshot_view_valid(&message->snapshot)) {
        return false;
    }
    for (i = 0; i < message->responses.len; ++i) {
        if (!raft_message_view_valid(&message->responses.items[i])) {
            return false;
        }
    }
    return true;
}

bool raft_message_valid(const raft_message_t *message) {
    size_t i;

    if (message == NULL || !message_type_valid(message->type) ||
        !raft_bytes_valid(&message->context) ||
        !array_shape_valid(message->entries.items, message->entries.len) ||
        !array_shape_valid(message->responses.items, message->responses.len)) {
        return false;
    }
    for (i = 0; i < message->entries.len; ++i) {
        if (!raft_entry_valid(&message->entries.items[i])) {
            return false;
        }
    }
    if (message->has_snapshot && !raft_snapshot_valid(&message->snapshot)) {
        return false;
    }
    for (i = 0; i < message->responses.len; ++i) {
        if (!raft_message_valid(&message->responses.items[i])) {
            return false;
        }
    }
    return true;
}

static bool peers_valid(const raft_peer_view_t *peers, size_t peer_count) {
    size_t i;

    if (peer_count == 0 || peers == NULL) {
        return false;
    }
    for (i = 0; i < peer_count; ++i) {
        if (!raft_is_valid_node_id(peers[i].id) ||
            !raft_byte_view_valid(&peers[i].context)) {
            return false;
        }
    }
    return true;
}

// copy methods

int raft_bytes_copy_from_view(raft_bytes_t *dst,
                              const raft_byte_view_t *src) {
    if (dst == NULL) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    memset(dst, 0, sizeof(*dst));
    dst->is_nil = true;
    if (!raft_byte_view_valid(src)) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    if (src->len == 0) {
        dst->is_nil = src->is_nil;
        return RAFT_OK;
    }
    dst->data = malloc(src->len);
    if (dst->data == NULL) {
        return RAFT_ERR_OUT_OF_MEMORY;
    }
    memcpy(dst->data, src->data, src->len);
    dst->len = src->len;
    dst->is_nil = false;
    return RAFT_OK;
}

int raft_bytes_copy(raft_bytes_t *dst, const raft_bytes_t *src) {
    raft_byte_view_t view;

    if (dst == NULL || src == NULL || dst == src ||
        !raft_bytes_valid(src)) {
        if (dst != NULL && dst != src) {
            memset(dst, 0, sizeof(*dst));
            dst->is_nil = true;
        }
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    view.data = src->data;
    view.len = src->len;
    view.is_nil = src->is_nil;
    return raft_bytes_copy_from_view(dst, &view);
}


static int uint64_vec_copy_from_view(raft_uint64_vec_t *dst,
                                     const raft_uint64_view_t *src) {
    if (dst == NULL) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    memset(dst, 0, sizeof(*dst));
    if (!uint64_view_valid(src)) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
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

static int conf_state_copy_from_view(raft_conf_state_t *dst,
                                     const raft_conf_state_view_t *src) {
    int result;

    if (dst == NULL) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    memset(dst, 0, sizeof(*dst));
    if (!conf_state_view_valid(src)) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    result = uint64_vec_copy_from_view(&dst->voters, &src->voters);
    if (result != RAFT_OK) {
        goto fail;
    }
    result = uint64_vec_copy_from_view(&dst->voters_outgoing,
                                       &src->voters_outgoing);
    if (result != RAFT_OK) {
        goto fail;
    }
    result = uint64_vec_copy_from_view(&dst->learners, &src->learners);
    if (result != RAFT_OK) {
        goto fail;
    }
    result = uint64_vec_copy_from_view(&dst->learners_next,
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

int raft_entry_copy_from_view(raft_entry_t *dst,
                              const raft_entry_view_t *src) {
    int result;

    if (dst == NULL) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    memset(dst, 0, sizeof(*dst));
    if (!raft_entry_view_valid(src)) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    dst->type = src->type;
    dst->term = src->term;
    dst->index = src->index;
    result = raft_bytes_copy_from_view(&dst->data, &src->data);
    if (result != RAFT_OK) {
        raft_entry_free(dst);
    }
    return result;
}

int raft_snapshot_copy_from_view(raft_snapshot_t *dst,
                                 const raft_snapshot_view_t *src) {
    int result;

    if (dst == NULL) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    memset(dst, 0, sizeof(*dst));
    if (!raft_snapshot_view_valid(src)) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    result = raft_bytes_copy_from_view(&dst->data, &src->data);
    if (result != RAFT_OK) {
        goto fail;
    }
    result = conf_state_copy_from_view(&dst->metadata.conf_state,
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

static int entry_vec_copy_from_view(raft_entry_vec_t *dst,
                                    const raft_entry_view_vec_t *src) {
    size_t i;
    int result;

    if (dst == NULL) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    memset(dst, 0, sizeof(*dst));
    if (src == NULL || !array_shape_valid(src->items, src->len)) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    if (src->len == 0) {
        return RAFT_OK;
    }
    dst->items = calloc(src->len, sizeof(*dst->items));
    if (dst->items == NULL) {
        return RAFT_ERR_OUT_OF_MEMORY;
    }
    dst->len = src->len;
    for (i = 0; i < src->len; ++i) {
        result = raft_entry_copy_from_view(&dst->items[i], &src->items[i]);
        if (result != RAFT_OK) {
            raft_entry_vec_free(dst);
            return result;
        }
    }
    return RAFT_OK;
}

int raft_message_copy_from_view(raft_message_t *dst,
                                const raft_message_view_t *src) {
    size_t i;
    int result;

    if (dst == NULL) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    memset(dst, 0, sizeof(*dst));
    if (!raft_message_view_valid(src)) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }

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

    result = raft_bytes_copy_from_view(&dst->context, &src->context);
    if (result != RAFT_OK) {
        goto fail;
    }
    result = entry_vec_copy_from_view(&dst->entries, &src->entries);
    if (result != RAFT_OK) {
        goto fail;
    }
    dst->has_snapshot = src->has_snapshot;
    if (src->has_snapshot) {
        result = raft_snapshot_copy_from_view(&dst->snapshot,
                                              &src->snapshot);
        if (result != RAFT_OK) {
            goto fail;
        }
    }
    if (src->responses.len != 0) {
        dst->responses.items =
            calloc(src->responses.len, sizeof(*dst->responses.items));
        if (dst->responses.items == NULL) {
            result = RAFT_ERR_OUT_OF_MEMORY;
            goto fail;
        }
        dst->responses.len = src->responses.len;
        for (i = 0; i < src->responses.len; ++i) {
            result = raft_message_copy_from_view(
                &dst->responses.items[i], &src->responses.items[i]);
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

// destructor method
void raft_bytes_free(raft_bytes_t *bytes) {
    if (bytes == NULL) {
        return;
    }
    free(bytes->data);
    bytes->data = NULL;
    bytes->len = 0;
    bytes->is_nil = true;
}

void raft_uint64_vec_free(raft_uint64_vec_t *vec) {
    if (vec == NULL) {
        return;
    }
    free(vec->items);
    memset(vec, 0, sizeof(*vec));
}

void raft_entry_free(raft_entry_t *entry) {
    if (entry == NULL) {
        return;
    }
    raft_bytes_free(&entry->data);
    memset(entry, 0, sizeof(*entry));
}

void raft_entry_array_free(raft_entry_t *entries, size_t len) {
    size_t i;

    if (entries == NULL) {
        return;
    }
    for (i = 0; i < len; ++i) {
        raft_entry_free(&entries[i]);
    }
    free(entries);
}

void raft_entry_vec_free(raft_entry_vec_t *vec) {
    if (vec == NULL) {
        return;
    }
    raft_entry_array_free(vec->items, vec->len);
    memset(vec, 0, sizeof(*vec));
}

void raft_conf_state_free(raft_conf_state_t *conf_state) {
    if (conf_state == NULL) {
        return;
    }
    raft_uint64_vec_free(&conf_state->voters);
    raft_uint64_vec_free(&conf_state->voters_outgoing);
    raft_uint64_vec_free(&conf_state->learners);
    raft_uint64_vec_free(&conf_state->learners_next);
    memset(conf_state, 0, sizeof(*conf_state));
}

void raft_snapshot_free(raft_snapshot_t *snapshot) {
    if (snapshot == NULL) {
        return;
    }
    raft_bytes_free(&snapshot->data);
    raft_conf_state_free(&snapshot->metadata.conf_state);
    memset(snapshot, 0, sizeof(*snapshot));
}

void raft_message_free(raft_message_t *message) {
    if (message == NULL) {
        return;
    }
    raft_entry_vec_free(&message->entries);
    raft_snapshot_free(&message->snapshot);
    raft_bytes_free(&message->context);
    raft_message_vec_free(&message->responses);
    memset(message, 0, sizeof(*message));
}

void raft_message_array_free(raft_message_t *messages, size_t len) {
    size_t i;

    if (messages == NULL) {
        return;
    }
    for (i = 0; i < len; ++i) {
        raft_message_free(&messages[i]);
    }
    free(messages);
}

void raft_message_vec_free(raft_message_vec_t *vec) {
    if (vec == NULL) {
        return;
    }
    raft_message_array_free(vec->items, vec->len);
    memset(vec, 0, sizeof(*vec));
}

void raft_read_state_free(raft_read_state_t *read_state) {
    if (read_state == NULL) {
        return;
    }
    raft_bytes_free(&read_state->request_ctx);
    memset(read_state, 0, sizeof(*read_state));
}

void raft_read_state_vec_free(raft_read_state_vec_t *vec) {
    size_t i;

    if (vec == NULL) {
        return;
    }
    if (vec->items != NULL) {
        for (i = 0; i < vec->len; ++i) {
            raft_read_state_free(&vec->items[i]);
        }
    }
    free(vec->items);
    memset(vec, 0, sizeof(*vec));
}

void raft_ready_free(raft_ready_t *ready) {
    if (ready == NULL) {
        return;
    }
    raft_read_state_vec_free(&ready->read_states);
    raft_entry_vec_free(&ready->entries);
    raft_snapshot_free(&ready->snapshot);
    raft_entry_vec_free(&ready->committed_entries);
    raft_message_vec_free(&ready->messages);
    memset(ready, 0, sizeof(*ready));
}

void raft_ready_destroy(raft_ready_t *ready) {
    if (ready == NULL) {
        return;
    }
    raft_ready_free(ready);
    free(ready);
}

void raft_status_free(raft_status_t *status) {
    if (status == NULL) {
        return;
    }
    raft_conf_state_free(&status->conf_state);
    raft_tracker_status_progress_snapshot_free(
        status->progress, status->progress_len);
    memset(status, 0, sizeof(*status));
}

static bool soft_state_equal(const raft_soft_state_t *left,
                             const raft_soft_state_t *right) {
    return left->lead == right->lead &&
           left->raft_state == right->raft_state;
}

static bool hard_state_equal(const raft_hard_state_t *left,
                             const raft_hard_state_t *right) {
    return left->term == right->term && left->vote == right->vote &&
           left->commit == right->commit;
}

static bool message_type_is_response(raft_message_type_t type) {
    switch (type) {
        case RAFT_MSG_APP_RESP:
        case RAFT_MSG_VOTE_RESP:
        case RAFT_MSG_HEARTBEAT_RESP:
        case RAFT_MSG_UNREACHABLE:
        case RAFT_MSG_READ_INDEX_RESP:
        case RAFT_MSG_PRE_VOTE_RESP:
        case RAFT_MSG_STORAGE_APPEND_RESP:
        case RAFT_MSG_STORAGE_APPLY_RESP:
            return true;
        default:
            return false;
    }
}

static uint64_t entry_vec_encoding_size(const raft_entry_vec_t *entries) {
    uint64_t size = 0;
    size_t i;
    for (i = 0; i < entries->len; ++i) {
        uint64_t entry_size =
            raft_log_entry_encoding_size(&entries->items[i]);
        if (UINT64_MAX - size < entry_size) {
            return UINT64_MAX;
        }
        size += entry_size;
    }
    return size;
}

static void raw_message_init(raft_message_t *message,
                             raft_message_type_t type) {
    memset(message, 0, sizeof(*message));
    message->type = type;
    message->context.is_nil = true;
    message->snapshot.data.is_nil = true;
}

static int raw_message_vec_push(raft_message_vec_t *messages,
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

static int raw_entry_vec_copy(raft_entry_vec_t *dst,
                              const raft_entry_vec_t *src) {
    size_t i;

    if (dst == NULL || src == NULL ||
        (src->len != 0 && src->items == NULL)) {
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
        raft_entry_view_t view = {
            .type = src->items[i].type,
            .term = src->items[i].term,
            .index = src->items[i].index,
            .data = {
                .data = src->items[i].data.data,
                .len = src->items[i].data.len,
                .is_nil = src->items[i].data.is_nil,
            },
        };
        int result =
            raft_entry_copy_from_view(&dst->items[i], &view);
        if (result != RAFT_OK) {
            raft_entry_vec_free(dst);
            return result;
        }
    }
    return RAFT_OK;
}

static int raw_snapshot_copy(raft_snapshot_t *dst,
                             const raft_snapshot_t *src) {
    raft_snapshot_view_t view;

    if (dst == NULL || src == NULL) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    memset(&view, 0, sizeof(view));
    view.data.data = src->data.data;
    view.data.len = src->data.len;
    view.data.is_nil = src->data.is_nil;
    view.metadata.index = src->metadata.index;
    view.metadata.term = src->metadata.term;
    view.metadata.conf_state.voters.items =
        src->metadata.conf_state.voters.items;
    view.metadata.conf_state.voters.len =
        src->metadata.conf_state.voters.len;
    view.metadata.conf_state.voters_outgoing.items =
        src->metadata.conf_state.voters_outgoing.items;
    view.metadata.conf_state.voters_outgoing.len =
        src->metadata.conf_state.voters_outgoing.len;
    view.metadata.conf_state.learners.items =
        src->metadata.conf_state.learners.items;
    view.metadata.conf_state.learners.len =
        src->metadata.conf_state.learners.len;
    view.metadata.conf_state.learners_next.items =
        src->metadata.conf_state.learners_next.items;
    view.metadata.conf_state.learners_next.len =
        src->metadata.conf_state.learners_next.len;
    view.metadata.conf_state.auto_leave =
        src->metadata.conf_state.auto_leave;
    return raft_snapshot_copy_from_view(dst, &view);
}

static int raw_new_storage_append_response(
    raft_raw_node_t *raw_node,
    const raft_ready_t *ready,
    raft_message_t *response) {
    int result;

    raw_message_init(response, RAFT_MSG_STORAGE_APPEND_RESP);
    response->to = raw_node->raft.id;
    response->from = RAFT_LOCAL_APPEND_THREAD;
    response->term = raw_node->raft.term;
    if (raft_log_has_next_or_in_progress_unstable_entries(
            &raw_node->log)) {
        result = raft_log_last_index(
            &raw_node->log, &response->index);
        if (result == RAFT_OK) {
            result = raft_log_last_term(
                &raw_node->log, &response->log_term);
        }
        if (result != RAFT_OK) {
            raft_message_free(response);
            return result;
        }
    }
    if (ready->has_snapshot) {
        response->has_snapshot = true;
        result = raw_snapshot_copy(
            &response->snapshot, &ready->snapshot);
        if (result != RAFT_OK) {
            raft_message_free(response);
            return result;
        }
    }
    return RAFT_OK;
}

static bool raw_need_storage_append_response(
    const raft_raw_node_t *raw_node,
    const raft_ready_t *ready) {
    return raft_log_has_next_or_in_progress_unstable_entries(
               &raw_node->log) ||
           ready->has_snapshot;
}

static int raw_new_storage_append(
    raft_raw_node_t *raw_node,
    const raft_ready_t *ready,
    raft_message_t *message) {
    raft_message_t response;
    int result;

    raw_message_init(message, RAFT_MSG_STORAGE_APPEND);
    message->to = RAFT_LOCAL_APPEND_THREAD;
    message->from = raw_node->raft.id;
    result = raw_entry_vec_copy(
        &message->entries, &ready->entries);
    if (result != RAFT_OK) {
        goto fail;
    }
    if (ready->has_hard_state) {
        message->term = ready->hard_state.term;
        message->vote = ready->hard_state.vote;
        message->commit = ready->hard_state.commit;
    }
    if (ready->has_snapshot) {
        message->has_snapshot = true;
        result = raw_snapshot_copy(
            &message->snapshot, &ready->snapshot);
        if (result != RAFT_OK) {
            goto fail;
        }
    }
    result = raft_core_ready_messages_after_append_copy(
        &raw_node->raft, &message->responses);
    if (result != RAFT_OK) {
        goto fail;
    }
    if (raw_need_storage_append_response(raw_node, ready)) {
        result = raw_new_storage_append_response(
            raw_node, ready, &response);
        if (result != RAFT_OK) {
            goto fail;
        }
        result = raw_message_vec_push(
            &message->responses, &response);
        raft_message_free(&response);
        if (result != RAFT_OK) {
            goto fail;
        }
    }
    return RAFT_OK;

fail:
    raft_message_free(message);
    return result;
}

static int raw_new_storage_apply(
    raft_raw_node_t *raw_node,
    const raft_ready_t *ready,
    raft_message_t *message) {
    raft_message_t response;
    int result;

    raw_message_init(message, RAFT_MSG_STORAGE_APPLY);
    message->to = RAFT_LOCAL_APPLY_THREAD;
    message->from = raw_node->raft.id;
    result = raw_entry_vec_copy(
        &message->entries, &ready->committed_entries);
    if (result != RAFT_OK) {
        goto fail;
    }
    raw_message_init(&response, RAFT_MSG_STORAGE_APPLY_RESP);
    response.to = raw_node->raft.id;
    response.from = RAFT_LOCAL_APPLY_THREAD;
    result = raw_entry_vec_copy(
        &response.entries, &ready->committed_entries);
    if (result != RAFT_OK) {
        raft_message_free(&response);
        goto fail;
    }
    result = raw_message_vec_push(
        &message->responses, &response);
    raft_message_free(&response);
    if (result != RAFT_OK) {
        goto fail;
    }
    return RAFT_OK;

fail:
    raft_message_free(message);
    return result;
}

static int raw_append_synchronous_after_append_messages(
    raft_raw_node_t *raw_node, raft_ready_t *ready) {
    raft_message_vec_t delayed = {0};
    size_t i;
    int result;

    result = raft_core_ready_messages_after_append_copy(
        &raw_node->raft, &delayed);
    if (result != RAFT_OK) {
        return result;
    }
    for (i = 0; i < delayed.len; ++i) {
        if (delayed.items[i].to == raw_node->raft.id) {
            continue;
        }
        result = raw_message_vec_push(
            &ready->messages, &delayed.items[i]);
        if (result != RAFT_OK) {
            raft_message_vec_free(&delayed);
            return result;
        }
    }
    raft_message_vec_free(&delayed);
    return RAFT_OK;
}

static int raw_append_async_storage_messages(
    raft_raw_node_t *raw_node, raft_ready_t *ready) {
    raft_message_t message;
    bool need_append =
        ready->entries.len != 0 || ready->has_hard_state ||
        ready->has_snapshot ||
        raw_node->raft.messages_after_append.len != 0;
    int result;

    if (need_append) {
        result = raw_new_storage_append(
            raw_node, ready, &message);
        if (result != RAFT_OK) {
            return result;
        }
        result = raw_message_vec_push(
            &ready->messages, &message);
        raft_message_free(&message);
        if (result != RAFT_OK) {
            return result;
        }
    }
    if (ready->committed_entries.len != 0) {
        result = raw_new_storage_apply(
            raw_node, ready, &message);
        if (result != RAFT_OK) {
            return result;
        }
        result = raw_message_vec_push(
            &ready->messages, &message);
        raft_message_free(&message);
        if (result != RAFT_OK) {
            return result;
        }
    }
    return RAFT_OK;
}

static bool raft_must_sync(const raft_hard_state_t *state,
                           const raft_hard_state_t *previous,
                           size_t entry_count);

static int raw_node_build_ready(raft_raw_node_t *raw_node,
                                raft_ready_t **out) {
    raft_ready_t *ready;
    raft_soft_state_t soft_state;
    raft_hard_state_t hard_state;
    int result;

    if (out == NULL) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    *out = NULL;
    if (raw_node == NULL) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    if (raw_node->raft.error != RAFT_OK) {
        return raw_node->raft.error;
    }
    ready = calloc(1, sizeof(*ready));
    if (ready == NULL) {
        return RAFT_ERR_OUT_OF_MEMORY;
    }

    raft_core_soft_state(&raw_node->raft, &soft_state);
    if (!soft_state_equal(&soft_state, &raw_node->previous_soft_state)) {
        ready->has_soft_state = true;
        ready->soft_state = soft_state;
    }
    raft_core_hard_state(&raw_node->raft, &hard_state);
    if (!hard_state_equal(&hard_state, &raw_node->previous_hard_state)) {
        ready->has_hard_state = true;
        ready->hard_state = hard_state;
    }
    result = raft_log_next_unstable_entries(
        &raw_node->log, &ready->entries);
    if (result == RAFT_OK) {
        result = raft_log_next_unstable_snapshot(
            &raw_node->log, &ready->has_snapshot, &ready->snapshot);
    }
    if (result == RAFT_OK) {
        result = raft_log_next_committed_entries(
            &raw_node->log,
            !raw_node->config.async_storage_writes,
            &ready->committed_entries);
    }
    if (result == RAFT_OK) {
        result = raft_core_ready_read_states_copy(
            &raw_node->raft, &ready->read_states);
    }
    if (result == RAFT_OK) {
        result = raft_core_ready_messages_copy(
            &raw_node->raft, &ready->messages);
    }
    if (result == RAFT_OK) {
        result = raw_node->config.async_storage_writes
                     ? raw_append_async_storage_messages(
                           raw_node, ready)
                     : raw_append_synchronous_after_append_messages(
                           raw_node, ready);
    }
    if (result != RAFT_OK) {
        raft_ready_destroy(ready);
        return result;
    }
    ready->must_sync =
        raft_must_sync(&hard_state,
                       &raw_node->previous_hard_state,
                       ready->entries.len);
    ready->opaque_token = raw_node->ready_generation;
    *out = ready;
    return RAFT_OK;
}

void raft_raw_node_destroy(raft_raw_node_t *raw_node) {
    if (raw_node == NULL) {
        return;
    }
    raft_message_vec_free(&raw_node->steps_on_advance);
    raft_core_free(&raw_node->raft);
    raft_log_free(&raw_node->log);
    memset(raw_node, 0, sizeof(*raw_node));
    free(raw_node);
}

// public api in bootstrap.go
int raft_raw_node_bootstrap(raft_raw_node_t *raw_node,
                            const raft_peer_view_t *peers,
                            size_t peer_count) {
    int result;
    if (raw_node == NULL || !peers_valid(peers, peer_count)) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    result = raft_core_bootstrap(&raw_node->raft, peers, peer_count);
    if (result == RAFT_OK) {
        memset(&raw_node->previous_hard_state,
               0,
               sizeof(raw_node->previous_hard_state));
    }
    return result;
}

// public api starts here

int raft_raw_node_new(const raft_config_t *config,
                      const raft_storage_ops_t *storage,
                      raft_raw_node_t **out) {
    raft_raw_node_t *raw_node;
    uint64_t max_applying_size;
    int result;

    if (out == NULL) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    *out = NULL;
    if (!config_valid(config) || !storage_ops_valid(storage)) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    raw_node = calloc(1, sizeof(*raw_node));
    if (raw_node == NULL) {
        return RAFT_ERR_OUT_OF_MEMORY;
    }
    raw_node->abi_version = RAFT_RAW_NODE_ABI_VERSION;
    raw_node->config = *config;
    if (raw_node->config.max_committed_size_per_ready == 0) {
        raw_node->config.max_committed_size_per_ready =
            raw_node->config.max_size_per_message;
    }
    if (raw_node->config.max_uncommitted_entries_size == 0) {
        raw_node->config.max_uncommitted_entries_size = UINT64_MAX;
    }
    if (raw_node->config.max_inflight_bytes == 0) {
        raw_node->config.max_inflight_bytes = UINT64_MAX;
    }
    raw_node->storage = *storage;
    max_applying_size =
        raw_node->config.max_committed_size_per_ready;
    result = raft_log_init(
        &raw_node->log, &raw_node->storage, max_applying_size);
    if (result != RAFT_OK) {
        free(raw_node);
        return result;
    }
    result = raft_core_init(&raw_node->raft,
                            &raw_node->config,
                            &raw_node->log,
                            &raw_node->storage);
    if (result != RAFT_OK) {
        raft_log_free(&raw_node->log);
        free(raw_node);
        return result;
    }
    raft_core_soft_state(
        &raw_node->raft, &raw_node->previous_soft_state);
    raft_core_hard_state(
        &raw_node->raft, &raw_node->previous_hard_state);
    raw_node->ready_generation = 1;
    *out = raw_node;
    return RAFT_OK;
}

// logger, hasprogress, id, asyncstoragewritesenabled dualized into c and go

void raft_raw_node_tick(raft_raw_node_t *raw_node) {
    int result;
    if (raw_node == NULL || raw_node->raft.error != RAFT_OK) {
        return;
    }
    result = raft_core_tick(&raw_node->raft);
    if (result != RAFT_OK) {
        raw_node->raft.error = result;
    }
}

void raft_raw_node_tick_quiesced(raft_raw_node_t *raw_node) {
    if (raw_node != NULL) {
        raft_core_tick_quiesced(&raw_node->raft);
    }
}

int raft_raw_node_campaign(raft_raw_node_t *raw_node) {
    return raw_node == NULL ? RAFT_ERR_INVALID_ARGUMENT
                            : raft_core_campaign(&raw_node->raft);
}

int raft_raw_node_propose(raft_raw_node_t *raw_node,
                          const raft_byte_view_t *data) {
    if (raw_node == NULL || !raft_byte_view_valid(data)) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    return raft_core_propose(&raw_node->raft, data);
}

int raft_raw_node_propose_from_parts(raft_raw_node_t *raw_node,
                                     const uint8_t *data,
                                     size_t len,
                                     bool is_nil) {
    const raft_byte_view_t view = {
        .data = data,
        .len = len,
        .is_nil = is_nil,
    };
    return raft_raw_node_propose(raw_node, &view);
}

int raft_raw_node_propose_conf_change(
    raft_raw_node_t *raw_node,
    const raft_conf_change_v2_view_t *conf_change) {
    if (raw_node == NULL ||
        (conf_change != NULL &&
         !public_conf_change_v2_valid(conf_change))) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    return raft_core_propose_conf_change_v2(
        &raw_node->raft, conf_change);
}

int raft_raw_node_propose_conf_change_v1(
    raft_raw_node_t *raw_node,
    const raft_conf_change_view_t *conf_change) {
    if (raw_node == NULL || !public_conf_change_v1_valid(conf_change)) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    return raft_core_propose_conf_change_v1(
        &raw_node->raft, conf_change);
}

int raft_raw_node_apply_conf_change(
    raft_raw_node_t *raw_node,
    const raft_conf_change_v2_view_t *conf_change,
    raft_conf_state_t *conf_state) {
    if (conf_state == NULL) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    memset(conf_state, 0, sizeof(*conf_state));
    if (raw_node == NULL || !public_conf_change_v2_valid(conf_change)) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    return raft_core_apply_conf_change(
        &raw_node->raft, conf_change, conf_state);
}

int raft_raw_node_step(raft_raw_node_t *raw_node,
                       const raft_message_view_t *message) {
    if (raw_node == NULL || !raft_message_view_valid(message)) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    if (message_type_is_local(message->type) &&
        !raft_is_local_target_id(message->from)) {
        return RAFT_ERR_STEP_LOCAL_MSG;
    }
    return raft_raw_node_step_for_node(raw_node, message);
}

int raft_raw_node_step_for_node(raft_raw_node_t *raw_node,
                                const raft_message_view_t *message) {
    if (raw_node == NULL || !raft_message_view_valid(message)) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    // This is the lower-level Node actor/core entry point. It intentionally
    // bypasses public local-message rejection, but retains RawNode's
    // unknown-peer response filtering. Message IDs require message-aware
    // validation: RAFT_NONE is valid for documented local-origin paths, and
    // RAFT_LOCAL_* is valid for matching asynchronous-storage responses.
    if (message_type_is_response(message->type) &&
        !raft_is_local_target_id(message->from) &&
        !raft_core_has_progress(&raw_node->raft, message->from)) {
        return RAFT_ERR_STEP_PEER_NOT_FOUND_OR_IGNORED;
    }
    return raft_core_step(&raw_node->raft, message);
}

int raft_raw_node_ready(raft_raw_node_t *raw_node, raft_ready_t **ready) {
    int result = raw_node_build_ready(raw_node, ready);
    if (result != RAFT_OK) {
        return result;
    }
    result = raft_raw_node_accept_ready(raw_node, *ready);
    if (result != RAFT_OK) {
        raft_ready_destroy(*ready);
        *ready = NULL;
    }
    return result;
}

int raft_raw_node_ready_without_accept(raft_raw_node_t *raw_node,
                                       raft_ready_t **ready) {
    return raw_node_build_ready(raw_node, ready);
}

static bool raft_must_sync(const raft_hard_state_t *st,
                           const raft_hard_state_t *prev,
                           size_t entry_count)
{
    /*
     * This helper is normally called internally with non-NULL pointers.
     * If you prefer fail-fast style, replace these NULL checks with asserts.
     */
    if (st == NULL || prev == NULL) {
        return false;
    }

    return entry_count != 0 ||
           st->term != prev->term ||
           st->vote != prev->vote;
}

static int raw_capture_steps_on_advance(
    raft_raw_node_t *raw_node, const raft_ready_t *ready) {
    raft_message_vec_t delayed = {0};
    raft_message_t response;
    size_t i;
    int result;

    if (raw_node->steps_on_advance.len != 0 || ready == NULL) {
        return RAFT_ERR_FATAL;
    }
    result = raft_core_ready_messages_after_append_copy(
        &raw_node->raft, &delayed);
    if (result != RAFT_OK) {
        return result;
    }
    for (i = 0; i < delayed.len; ++i) {
        if (delayed.items[i].to != raw_node->raft.id) {
            continue;
        }
        result = raw_message_vec_push(
            &raw_node->steps_on_advance, &delayed.items[i]);
        if (result != RAFT_OK) {
            raft_message_vec_free(&delayed);
            raft_message_vec_free(
                &raw_node->steps_on_advance);
            return result;
        }
    }
    raft_message_vec_free(&delayed);
    if (raw_need_storage_append_response(raw_node, ready)) {
        result = raw_new_storage_append_response(
            raw_node, ready, &response);
        if (result != RAFT_OK) {
            goto fail;
        }
        result = raw_message_vec_push(
            &raw_node->steps_on_advance, &response);
        raft_message_free(&response);
        if (result != RAFT_OK) {
            goto fail;
        }
    }
    if (ready->committed_entries.len != 0) {
        raw_message_init(
            &response, RAFT_MSG_STORAGE_APPLY_RESP);
        response.to = raw_node->raft.id;
        response.from = RAFT_LOCAL_APPLY_THREAD;
        result = raw_entry_vec_copy(
            &response.entries, &ready->committed_entries);
        if (result != RAFT_OK) {
            raft_message_free(&response);
            goto fail;
        }
        result = raw_message_vec_push(
            &raw_node->steps_on_advance, &response);
        raft_message_free(&response);
        if (result != RAFT_OK) {
            goto fail;
        }
    }
    return RAFT_OK;

fail:
    raft_message_vec_free(&raw_node->steps_on_advance);
    return result;
}

// helper function
int raft_raw_node_accept_ready(raft_raw_node_t *raw_node,
                               const raft_ready_t *ready) {
    const raft_entry_t *last;
    uint64_t applying_size;
    int result;

    if (raw_node == NULL || ready == NULL) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    if ((!raw_node->config.async_storage_writes &&
         raw_node->ready_accepted) ||
        ready->opaque_token != raw_node->ready_generation) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    if ((ready->entries.len != 0 && ready->entries.items == NULL) ||
        (ready->read_states.len != 0 &&
         ready->read_states.items == NULL) ||
        (ready->committed_entries.len != 0 &&
         ready->committed_entries.items == NULL) ||
        (ready->messages.len != 0 && ready->messages.items == NULL)) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }

    if (!raw_node->config.async_storage_writes) {
        result = raw_capture_steps_on_advance(raw_node, ready);
        if (result != RAFT_OK) {
            return result;
        }
    }
    applying_size = entry_vec_encoding_size(&ready->committed_entries);
    if (ready->committed_entries.len != 0) {
        last =
            &ready->committed_entries.items[
                ready->committed_entries.len - 1];
        result = raft_log_accept_applying(
            &raw_node->log,
            last->index,
            applying_size,
            !raw_node->config.async_storage_writes);
        if (result != RAFT_OK) {
            goto fail;
        }
    }

    if (ready->has_soft_state) {
        raw_node->previous_soft_state = ready->soft_state;
    }
    if (ready->has_hard_state) {
        raw_node->previous_hard_state = ready->hard_state;
    }
    if (ready->read_states.len != 0) {
        raft_core_clear_read_states(&raw_node->raft);
    }
    raft_core_clear_messages(&raw_node->raft);
    raft_core_clear_messages_after_append(&raw_node->raft);
    raft_log_accept_unstable(&raw_node->log);
    raw_node->ready_accepted =
        !raw_node->config.async_storage_writes;
    ++raw_node->ready_generation;
    if (raw_node->ready_generation == 0) {
        raw_node->ready_generation = 1;
    }
    return RAFT_OK;

fail:
    raft_message_vec_free(&raw_node->steps_on_advance);
    return result;
}

bool raft_raw_node_has_ready(const raft_raw_node_t *raw_node) {
    raft_soft_state_t soft_state;
    raft_hard_state_t hard_state;
    if (raw_node == NULL) {
        return false;
    }
    if (raw_node->raft.error != RAFT_OK) {
        return true;
    }
    raft_core_soft_state(&raw_node->raft, &soft_state);
    if (!soft_state_equal(&soft_state, &raw_node->previous_soft_state)) {
        return true;
    }
    raft_core_hard_state(&raw_node->raft, &hard_state);
    return !hard_state_equal(&hard_state,
                             &raw_node->previous_hard_state) ||
           raft_log_has_next_unstable_entries(&raw_node->log) ||
           raft_log_has_next_unstable_snapshot(&raw_node->log) ||
           raft_log_has_next_committed_entries(
               &raw_node->log,
               !raw_node->config.async_storage_writes) ||
           raw_node->raft.read_states.len != 0 ||
           raw_node->raft.messages.len != 0 ||
           raw_node->raft.messages_after_append.len != 0;
}

static int raw_step_after_append_message(
    raft_raw_node_t *raw_node,
    const raft_message_t *message) {
    raft_message_view_t view;
    raft_entry_view_t *entries = NULL;
    size_t i;
    int result;

    if (raw_node == NULL || message == NULL ||
        message->responses.len != 0) {
        return RAFT_ERR_FATAL;
    }
    if (message->entries.len != 0) {
        if (message->entries.len >
            SIZE_MAX / sizeof(*entries)) {
            return RAFT_ERR_OUT_OF_MEMORY;
        }
        entries = calloc(
            message->entries.len, sizeof(*entries));
        if (entries == NULL) {
            return RAFT_ERR_OUT_OF_MEMORY;
        }
        for (i = 0; i < message->entries.len; ++i) {
            entries[i].type = message->entries.items[i].type;
            entries[i].term = message->entries.items[i].term;
            entries[i].index = message->entries.items[i].index;
            entries[i].data.data =
                message->entries.items[i].data.data;
            entries[i].data.len =
                message->entries.items[i].data.len;
            entries[i].data.is_nil =
                message->entries.items[i].data.is_nil;
        }
    }
    memset(&view, 0, sizeof(view));
    view.type = message->type;
    view.to = message->to;
    view.from = message->from;
    view.term = message->term;
    view.log_term = message->log_term;
    view.index = message->index;
    view.commit = message->commit;
    view.vote = message->vote;
    view.reject = message->reject;
    view.reject_hint = message->reject_hint;
    view.entries.items = entries;
    view.entries.len = message->entries.len;
    view.has_snapshot = message->has_snapshot;
    if (message->has_snapshot) {
        view.snapshot.data.data = message->snapshot.data.data;
        view.snapshot.data.len = message->snapshot.data.len;
        view.snapshot.data.is_nil =
            message->snapshot.data.is_nil;
        view.snapshot.metadata.index =
            message->snapshot.metadata.index;
        view.snapshot.metadata.term =
            message->snapshot.metadata.term;
        view.snapshot.metadata.conf_state.voters.items =
            message->snapshot.metadata.conf_state.voters.items;
        view.snapshot.metadata.conf_state.voters.len =
            message->snapshot.metadata.conf_state.voters.len;
        view.snapshot.metadata.conf_state.voters_outgoing.items =
            message->snapshot.metadata.conf_state
                .voters_outgoing.items;
        view.snapshot.metadata.conf_state.voters_outgoing.len =
            message->snapshot.metadata.conf_state
                .voters_outgoing.len;
        view.snapshot.metadata.conf_state.learners.items =
            message->snapshot.metadata.conf_state.learners.items;
        view.snapshot.metadata.conf_state.learners.len =
            message->snapshot.metadata.conf_state.learners.len;
        view.snapshot.metadata.conf_state.learners_next.items =
            message->snapshot.metadata.conf_state
                .learners_next.items;
        view.snapshot.metadata.conf_state.learners_next.len =
            message->snapshot.metadata.conf_state
                .learners_next.len;
        view.snapshot.metadata.conf_state.auto_leave =
            message->snapshot.metadata.conf_state.auto_leave;
    } else {
        view.snapshot.data.is_nil = true;
    }
    view.context.data = message->context.data;
    view.context.len = message->context.len;
    view.context.is_nil = message->context.is_nil;
    result = raft_core_step(&raw_node->raft, &view);
    free(entries);
    return result;
}

int raft_raw_node_advance(raft_raw_node_t *raw_node) {
    int result = RAFT_OK;
    size_t i;

    if (raw_node == NULL) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    if (raw_node->config.async_storage_writes) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    if (!raw_node->ready_accepted) {
        return RAFT_OK;
    }
    for (i = 0; i < raw_node->steps_on_advance.len; ++i) {
        result = raw_step_after_append_message(
            raw_node, &raw_node->steps_on_advance.items[i]);
        if (result != RAFT_OK) {
            raw_node->raft.error = result;
            break;
        }
    }
    raft_message_vec_free(&raw_node->steps_on_advance);
    if (result != RAFT_OK) {
        return result;
    }
    if (result == RAFT_OK) {
        raw_node->ready_accepted = false;
    }
    return result;
}

int raft_raw_node_status(const raft_raw_node_t *raw_node,
                         raft_status_t *status) {
    int result;

    if (raw_node == NULL || status == NULL) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    memset(status, 0, sizeof(*status));
    result = raft_raw_node_basic_status(raw_node, &status->basic);
    if (result != RAFT_OK) {
        memset(status, 0, sizeof(*status));
        return result;
    }
    result = raft_core_conf_state_copy(&raw_node->raft,
                                       &status->conf_state);
    if (result != RAFT_OK) {
        raft_status_free(status);
        return result;
    }
    if (raw_node->raft.state == RAFT_STATE_LEADER) {
        result = raft_core_status_progress_snapshot(
            &raw_node->raft,
            &status->progress,
            &status->progress_len);
        if (result != RAFT_OK) {
            raft_status_free(status);
            return result;
        }
    }
    return RAFT_OK;
}

int raft_raw_node_basic_status(const raft_raw_node_t *raw_node,
                               raft_basic_status_t *status) {
    if (raw_node == NULL || status == NULL) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    memset(status, 0, sizeof(*status));
    status->id = raw_node->config.id;
    raft_core_hard_state(&raw_node->raft, &status->hard_state);
    raft_core_soft_state(&raw_node->raft, &status->soft_state);
    status->applied = raw_node->log.applied;
    status->lead_transferee = raw_node->raft.lead_transferee;
    return RAFT_OK;
}

int raft_raw_node_progress_snapshot(const raft_raw_node_t *raw_node,
                                    raft_progress_snapshot_t **out,
                                    size_t *out_len) {
    if (out == NULL || out_len == NULL) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    *out = NULL;
    *out_len = 0;
    if (raw_node == NULL) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    return raft_core_progress_snapshot(&raw_node->raft, out, out_len);
}

bool raft_raw_node_has_progress(const raft_raw_node_t *raw_node, uint64_t id) {
    if (raw_node == NULL || !raft_is_valid_node_id(id)) {
        return false;
    }
    return raft_core_has_progress(&raw_node->raft, id);
}

void raft_progress_snapshot_array_free(raft_progress_snapshot_t *snapshots,
                                       size_t len) {
    (void)len;
    free(snapshots);
}


int raft_raw_node_report_unreachable(raft_raw_node_t *raw_node, uint64_t id) {
    raft_message_view_t message;

    if (raw_node == NULL || !raft_is_valid_node_id(id)) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    memset(&message, 0, sizeof(message));
    message.type = RAFT_MSG_UNREACHABLE;
    message.from = id;
    message.context.is_nil = true;
    return raft_core_step(&raw_node->raft, &message);
}

int raft_raw_node_report_snapshot(raft_raw_node_t *raw_node,
                                  uint64_t id,
                                  raft_snapshot_status_t status) {
    raft_message_view_t message;

    if (raw_node == NULL || !raft_is_valid_node_id(id) ||
        (status != RAFT_SNAPSHOT_FINISH &&
         status != RAFT_SNAPSHOT_FAILURE)) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    memset(&message, 0, sizeof(message));
    message.type = RAFT_MSG_SNAP_STATUS;
    message.from = id;
    message.reject = status == RAFT_SNAPSHOT_FAILURE;
    message.context.is_nil = true;
    return raft_core_step(&raw_node->raft, &message);
}

int raft_raw_node_transfer_leader(raft_raw_node_t *raw_node,
                                  uint64_t transferee) {
    raft_message_view_t message;

    if (raw_node == NULL || !raft_is_valid_node_id(transferee)) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    memset(&message, 0, sizeof(message));
    message.type = RAFT_MSG_TRANSFER_LEADER;
    message.from = transferee;
    message.context.is_nil = true;
    return raft_core_step(&raw_node->raft, &message);
}

int raft_raw_node_forget_leader(raft_raw_node_t *raw_node) {
    raft_message_view_t message;
    if (raw_node == NULL) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    memset(&message, 0, sizeof(message));
    message.type = RAFT_MSG_FORGET_LEADER;
    message.context.is_nil = true;
    return raft_core_step(&raw_node->raft, &message);
}

int raft_raw_node_read_index(raft_raw_node_t *raw_node,
                             const raft_byte_view_t *request_context) {
    raft_entry_view_t entry;
    raft_message_view_t message;

    if (raw_node == NULL ||
        !raft_byte_view_valid(request_context)) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    memset(&entry, 0, sizeof(entry));
    entry.type = RAFT_ENTRY_NORMAL;
    entry.data = *request_context;
    memset(&message, 0, sizeof(message));
    message.type = RAFT_MSG_READ_INDEX;
    message.entries.items = &entry;
    message.entries.len = 1;
    message.context.is_nil = true;
    return raft_core_step(&raw_node->raft, &message);
}

int raft_raw_node_read_index_from_parts(
    raft_raw_node_t *raw_node,
    const uint8_t *request_context,
    size_t len,
    bool is_nil) {
    const raft_byte_view_t view = {
        .data = request_context,
        .len = len,
        .is_nil = is_nil,
    };
    return raft_raw_node_read_index(raw_node, &view);
}
