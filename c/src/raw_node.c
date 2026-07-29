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

// This validates the strict public C API. A future internal compatibility
// path may still ignore a zero node ID when replaying a Go configuration
// change, but reserved IDs must never enter C membership state.
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
            !raft_is_valid_node_id(change->node_id)) {
            return false;
        }
    }
    return true;
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
    raft_progress_snapshot_array_free(status->progress, status->progress_len);
    memset(status, 0, sizeof(*status));
}

void raft_raw_node_destroy(raft_raw_node_t *raw_node) {
    if (raw_node == NULL) {
        return;
    }
    memset(raw_node, 0, sizeof(*raw_node));
    free(raw_node);
}

// public api in bootstrap.go
int raft_raw_node_bootstrap(raft_raw_node_t *raw_node,
                            const raft_peer_view_t *peers,
                            size_t peer_count) {
    if (raw_node == NULL || !peers_valid(peers, peer_count)) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    return RAFT_ERR_NOT_IMPLEMENTED;
}

// public api starts here

int raft_raw_node_new(const raft_config_t *config,
                      const raft_storage_ops_t *storage,
                      raft_raw_node_t **out) {
    raft_raw_node_t *raw_node;

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
    raw_node->storage = *storage;
    *out = raw_node;
    return RAFT_OK;
}

// logger, hasprogress, id, asyncstoragewritesenabled dualized into c and go

void raft_raw_node_tick(raft_raw_node_t *raw_node) {
    (void)raw_node;
}

void raft_raw_node_tick_quiesced(raft_raw_node_t *raw_node) {
    (void)raw_node;
}

int raft_raw_node_campaign(raft_raw_node_t *raw_node) {
    return raw_node == NULL ? RAFT_ERR_INVALID_ARGUMENT
                            : RAFT_ERR_NOT_IMPLEMENTED;
}

int raft_raw_node_propose(raft_raw_node_t *raw_node,
                          const raft_byte_view_t *data) {
    if (raw_node == NULL || !raft_byte_view_valid(data)) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    return RAFT_ERR_NOT_IMPLEMENTED;
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
    if (raw_node == NULL || !public_conf_change_v2_valid(conf_change)) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    return RAFT_ERR_NOT_IMPLEMENTED;
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
    return RAFT_ERR_NOT_IMPLEMENTED;
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
    // Message IDs require message-aware validation. RAFT_NONE is valid for
    // documented local-origin paths, and RAFT_LOCAL_* is valid for matching
    // asynchronous-storage messages. Do not apply a blanket member-ID check.
    //
    // The public RawNode.Step unknown-response peer check also belongs here.
    // It remains deferred until the C progress tracker exists; rejecting all
    // response messages in this inert skeleton would be incorrect.
    return raft_raw_node_step_for_node(raw_node, message);
}

int raft_raw_node_step_for_node(raft_raw_node_t *raw_node,
                                const raft_message_view_t *message) {
    if (raw_node == NULL || !raft_message_view_valid(message)) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    // This is the lower-level Node actor/core entry point. It intentionally
    // does not call raft_raw_node_step or apply public RawNode.Step checks.
    return RAFT_ERR_NOT_IMPLEMENTED;
}

int raft_raw_node_ready(raft_raw_node_t *raw_node, raft_ready_t **ready) {
    if (ready == NULL) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    *ready = NULL;
    if (raw_node == NULL) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    return RAFT_ERR_NOT_IMPLEMENTED;
}

int raft_raw_node_ready_without_accept(raft_raw_node_t *raw_node,
                                       raft_ready_t **ready) {
    if (ready == NULL) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    *ready = NULL;
    if (raw_node == NULL) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    return RAFT_ERR_NOT_IMPLEMENTED;
}

bool raft_must_sync(const raft_hard_state_t *st,
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

// helper function
int raft_raw_node_accept_ready(raft_raw_node_t *raw_node,
                               const raft_ready_t *ready) {
    if (raw_node == NULL || ready == NULL) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    return RAFT_ERR_NOT_IMPLEMENTED;
}

bool raft_raw_node_has_ready(const raft_raw_node_t *raw_node) {
    (void)raw_node;
    return false;
}

int raft_raw_node_advance(raft_raw_node_t *raw_node) {
    return raw_node == NULL ? RAFT_ERR_INVALID_ARGUMENT
                            : RAFT_ERR_NOT_IMPLEMENTED;
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
    return RAFT_OK;
}

int raft_raw_node_basic_status(const raft_raw_node_t *raw_node,
                               raft_basic_status_t *status) {
    if (raw_node == NULL || status == NULL) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    memset(status, 0, sizeof(*status));
    status->id = raw_node->config.id;
    status->hard_state.vote = RAFT_NONE;
    status->soft_state.lead = RAFT_NONE;
    status->soft_state.raft_state = RAFT_STATE_FOLLOWER;
    status->lead_transferee = RAFT_NONE;
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
    return RAFT_ERR_NOT_IMPLEMENTED;
}

bool raft_raw_node_has_progress(const raft_raw_node_t *raw_node, uint64_t id) {
    if (raw_node == NULL || !raft_is_valid_node_id(id)) {
        return false;
    }
    return false;
}

void raft_progress_snapshot_array_free(raft_progress_snapshot_t *snapshots,
                                       size_t len) {
    (void)len;
    free(snapshots);
}


int raft_raw_node_report_unreachable(raft_raw_node_t *raw_node, uint64_t id) {
    if (raw_node == NULL || !raft_is_valid_node_id(id)) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    return RAFT_ERR_NOT_IMPLEMENTED;
}

int raft_raw_node_report_snapshot(raft_raw_node_t *raw_node,
                                  uint64_t id,
                                  raft_snapshot_status_t status) {
    if (raw_node == NULL || !raft_is_valid_node_id(id) ||
        (status != RAFT_SNAPSHOT_FINISH &&
         status != RAFT_SNAPSHOT_FAILURE)) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    return RAFT_ERR_NOT_IMPLEMENTED;
}

int raft_raw_node_transfer_leader(raft_raw_node_t *raw_node,
                                  uint64_t transferee) {
    if (raw_node == NULL || !raft_is_valid_node_id(transferee)) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    return RAFT_ERR_NOT_IMPLEMENTED;
}

int raft_raw_node_forget_leader(raft_raw_node_t *raw_node) {
    return raw_node == NULL ? RAFT_ERR_INVALID_ARGUMENT
                            : RAFT_ERR_NOT_IMPLEMENTED;
}

int raft_raw_node_read_index(raft_raw_node_t *raw_node,
                             const raft_byte_view_t *request_context) {
    if (raw_node == NULL ||
        !raft_byte_view_valid(request_context)) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    return RAFT_ERR_NOT_IMPLEMENTED;
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
