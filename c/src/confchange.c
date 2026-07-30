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

#include "confchange.h"

#include <stdlib.h>
#include <string.h>

#define RAFT_ALLOC_REPLACE_STDLIB
#include "alloc.h"

typedef struct proto_writer {
    uint8_t *data;
    size_t len;
    size_t cap;
} proto_writer_t;

static bool confchange_array_valid(const void *items, size_t len) {
    return len == 0 || items != NULL;
}

static size_t proto_varint_size(uint64_t value) {
    size_t size = 1;
    while (value >= UINT64_C(0x80)) {
        value >>= 7;
        ++size;
    }
    return size;
}

static int proto_reserve(proto_writer_t *writer, size_t extra) {
    uint8_t *data;
    size_t needed;
    size_t capacity;

    if (writer == NULL || extra > SIZE_MAX - writer->len) {
        return RAFT_ERR_OUT_OF_MEMORY;
    }
    needed = writer->len + extra;
    if (needed <= writer->cap) {
        return RAFT_OK;
    }
    capacity = writer->cap == 0 ? 16 : writer->cap;
    while (capacity < needed) {
        size_t next = capacity > SIZE_MAX / 2 ? SIZE_MAX : capacity * 2;
        if (next <= capacity) {
            capacity = needed;
            break;
        }
        capacity = next;
    }
    data = realloc(writer->data, capacity);
    if (data == NULL) {
        return RAFT_ERR_OUT_OF_MEMORY;
    }
    writer->data = data;
    writer->cap = capacity;
    return RAFT_OK;
}

static int proto_put_varint(proto_writer_t *writer, uint64_t value) {
    int result = proto_reserve(writer, proto_varint_size(value));
    if (result != RAFT_OK) {
        return result;
    }
    while (value >= UINT64_C(0x80)) {
        writer->data[writer->len++] =
            (uint8_t)(value | UINT64_C(0x80));
        value >>= 7;
    }
    writer->data[writer->len++] = (uint8_t)value;
    return RAFT_OK;
}

static int proto_put_key(proto_writer_t *writer,
                         uint32_t field,
                         uint32_t wire_type) {
    return proto_put_varint(
        writer, ((uint64_t)field << 3) | (uint64_t)wire_type);
}

static int proto_put_uint64(proto_writer_t *writer,
                            uint32_t field,
                            uint64_t value) {
    int result = proto_put_key(writer, field, 0);
    return result == RAFT_OK ? proto_put_varint(writer, value) : result;
}

static int proto_put_bytes(proto_writer_t *writer,
                           uint32_t field,
                           const uint8_t *data,
                           size_t len) {
    int result;
    if (len != 0 && data == NULL) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    result = proto_put_key(writer, field, 2);
    if (result == RAFT_OK) {
        result = proto_put_varint(writer, (uint64_t)len);
    }
    if (result == RAFT_OK) {
        result = proto_reserve(writer, len);
    }
    if (result == RAFT_OK && len != 0) {
        memcpy(&writer->data[writer->len], data, len);
        writer->len += len;
    }
    return result;
}

static void proto_writer_free(proto_writer_t *writer) {
    if (writer == NULL) {
        return;
    }
    free(writer->data);
    memset(writer, 0, sizeof(*writer));
}

static int proto_finish(proto_writer_t *writer, raft_bytes_t *out) {
    if (writer == NULL || out == NULL) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    memset(out, 0, sizeof(*out));
    out->data = writer->data;
    out->len = writer->len;
    out->is_nil = false;
    writer->data = NULL;
    writer->len = 0;
    writer->cap = 0;
    return RAFT_OK;
}

static int proto_read_varint(const uint8_t *data,
                             size_t len,
                             size_t *position,
                             uint64_t *out) {
    uint64_t value = 0;
    unsigned shift = 0;

    if (data == NULL || position == NULL || out == NULL) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    while (*position < len && shift < 64) {
        uint8_t byte = data[(*position)++];
        if (shift == 63 && (byte & UINT8_C(0xfe)) != 0) {
            return RAFT_ERR_INVALID_ARGUMENT;
        }
        value |= (uint64_t)(byte & UINT8_C(0x7f)) << shift;
        if ((byte & UINT8_C(0x80)) == 0) {
            *out = value;
            return RAFT_OK;
        }
        shift += 7;
    }
    return RAFT_ERR_INVALID_ARGUMENT;
}

static int proto_read_length(const uint8_t *data,
                             size_t len,
                             size_t *position,
                             size_t *out_len) {
    uint64_t value;
    int result = proto_read_varint(data, len, position, &value);
    if (result != RAFT_OK || value > SIZE_MAX ||
        (size_t)value > len - *position) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    *out_len = (size_t)value;
    return RAFT_OK;
}

static int proto_skip(const uint8_t *data,
                      size_t len,
                      size_t *position,
                      uint32_t wire_type) {
    uint64_t ignored;
    size_t item_len;
    int result;

    switch (wire_type) {
        case 0:
            return proto_read_varint(data, len, position, &ignored);
        case 1:
            if (len - *position < 8) {
                return RAFT_ERR_INVALID_ARGUMENT;
            }
            *position += 8;
            return RAFT_OK;
        case 2:
            result = proto_read_length(data, len, position, &item_len);
            if (result == RAFT_OK) {
                *position += item_len;
            }
            return result;
        case 5:
            if (len - *position < 4) {
                return RAFT_ERR_INVALID_ARGUMENT;
            }
            *position += 4;
            return RAFT_OK;
        default:
            return RAFT_ERR_INVALID_ARGUMENT;
    }
}

static bool confchange_type_valid(raft_conf_change_type_t type) {
    return type >= RAFT_CONF_CHANGE_ADD_NODE &&
           type <= RAFT_CONF_CHANGE_ADD_LEARNER_NODE;
}

static bool confchange_transition_valid(
    raft_conf_change_transition_t transition) {
    return transition >= RAFT_CONF_CHANGE_TRANSITION_AUTO &&
           transition <= RAFT_CONF_CHANGE_TRANSITION_JOINT_EXPLICIT;
}

static int encode_single(const raft_conf_change_single_t *change,
                         proto_writer_t *out) {
    int result = RAFT_OK;
    if (change == NULL || out == NULL ||
        !confchange_type_valid(change->type)) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    if (change->has_type ||
        change->type != RAFT_CONF_CHANGE_ADD_NODE) {
        result = proto_put_uint64(
            out, 1, (uint64_t)change->type);
    }
    if (result == RAFT_OK &&
        (change->has_node_id || change->node_id != 0)) {
        result = proto_put_uint64(out, 2, change->node_id);
    }
    return result;
}

int raft_confchange_encode_v2(const raft_conf_change_v2_view_t *change,
                              raft_bytes_t *out) {
    proto_writer_t writer = {0};
    size_t i;
    int result;

    if (change == NULL || out == NULL ||
        !confchange_transition_valid(change->transition) ||
        !confchange_array_valid(change->changes, change->changes_len) ||
        !raft_byte_view_valid(&change->context)) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    memset(out, 0, sizeof(*out));
    out->is_nil = true;

    if (change->has_transition ||
        change->transition != RAFT_CONF_CHANGE_TRANSITION_AUTO) {
        result = proto_put_uint64(
            &writer, 1, (uint64_t)change->transition);
        if (result != RAFT_OK) {
            goto fail;
        }
    }
    for (i = 0; i < change->changes_len; ++i) {
        proto_writer_t single = {0};
        result = encode_single(&change->changes[i], &single);
        if (result == RAFT_OK) {
            result = proto_put_bytes(
                &writer, 2, single.data, single.len);
        }
        proto_writer_free(&single);
        if (result != RAFT_OK) {
            goto fail;
        }
    }
    if (!change->context.is_nil) {
        result = proto_put_bytes(&writer,
                                 3,
                                 change->context.data,
                                 change->context.len);
        if (result != RAFT_OK) {
            goto fail;
        }
    }
    return proto_finish(&writer, out);

fail:
    proto_writer_free(&writer);
    return result;
}

int raft_confchange_encode_v1(const raft_conf_change_view_t *change,
                              raft_bytes_t *out) {
    proto_writer_t writer = {0};
    int result;

    if (change == NULL || out == NULL ||
        !confchange_type_valid(change->type) ||
        !raft_byte_view_valid(&change->context)) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    memset(out, 0, sizeof(*out));
    out->is_nil = true;
    if (change->has_id || change->id != 0) {
        result = proto_put_uint64(&writer, 1, change->id);
        if (result != RAFT_OK) {
            goto fail;
        }
    }
    result = RAFT_OK;
    if (change->has_type ||
        change->type != RAFT_CONF_CHANGE_ADD_NODE) {
        result = proto_put_uint64(
            &writer, 2, (uint64_t)change->type);
    }
    if (result == RAFT_OK &&
        (change->has_node_id || change->node_id != 0)) {
        result = proto_put_uint64(&writer, 3, change->node_id);
    }
    if (result == RAFT_OK && !change->context.is_nil) {
        result = proto_put_bytes(&writer,
                                 4,
                                 change->context.data,
                                 change->context.len);
    }
    if (result != RAFT_OK) {
        goto fail;
    }
    return proto_finish(&writer, out);

fail:
    proto_writer_free(&writer);
    return result;
}

static int decode_single(const uint8_t *data,
                         size_t len,
                         raft_conf_change_single_t *out) {
    size_t position = 0;
    uint64_t key;
    uint64_t value;
    int result;

    if (out == NULL || (len != 0 && data == NULL)) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    memset(out, 0, sizeof(*out));
    while (position < len) {
        result = proto_read_varint(data, len, &position, &key);
        if (result != RAFT_OK || (key >> 3) == 0) {
            return RAFT_ERR_INVALID_ARGUMENT;
        }
        if ((key >> 3) == 1 && (key & 7) == 0) {
            result = proto_read_varint(data, len, &position, &value);
            if (result != RAFT_OK ||
                value > RAFT_CONF_CHANGE_ADD_LEARNER_NODE) {
                return RAFT_ERR_INVALID_ARGUMENT;
            }
            out->type = (raft_conf_change_type_t)value;
        } else if ((key >> 3) == 2 && (key & 7) == 0) {
            result = proto_read_varint(
                data, len, &position, &out->node_id);
            if (result != RAFT_OK) {
                return result;
            }
        } else {
            result = proto_skip(
                data, len, &position, (uint32_t)(key & 7));
            if (result != RAFT_OK) {
                return result;
            }
        }
    }
    return RAFT_OK;
}

void raft_decoded_conf_change_free(raft_decoded_conf_change_t *change) {
    if (change == NULL) {
        return;
    }
    free(change->changes);
    memset(change, 0, sizeof(*change));
}

static int decode_v1(const raft_byte_view_t *data,
                     raft_decoded_conf_change_t *out) {
    size_t position = 0;
    uint64_t key;
    uint64_t value;
    int result;

    out->changes = calloc(1, sizeof(*out->changes));
    if (out->changes == NULL) {
        return RAFT_ERR_OUT_OF_MEMORY;
    }
    out->changes_len = 1;
    out->transition = RAFT_CONF_CHANGE_TRANSITION_AUTO;
    while (position < data->len) {
        result = proto_read_varint(
            data->data, data->len, &position, &key);
        if (result != RAFT_OK || (key >> 3) == 0) {
            return RAFT_ERR_INVALID_ARGUMENT;
        }
        if ((key >> 3) == 2 && (key & 7) == 0) {
            result = proto_read_varint(
                data->data, data->len, &position, &value);
            if (result != RAFT_OK ||
                value > RAFT_CONF_CHANGE_ADD_LEARNER_NODE) {
                return RAFT_ERR_INVALID_ARGUMENT;
            }
            out->changes[0].type = (raft_conf_change_type_t)value;
        } else if ((key >> 3) == 3 && (key & 7) == 0) {
            result = proto_read_varint(data->data,
                                       data->len,
                                       &position,
                                       &out->changes[0].node_id);
            if (result != RAFT_OK) {
                return result;
            }
        } else {
            result = proto_skip(data->data,
                                data->len,
                                &position,
                                (uint32_t)(key & 7));
            if (result != RAFT_OK) {
                return result;
            }
        }
    }
    return RAFT_OK;
}

static int decoded_changes_append(raft_decoded_conf_change_t *out,
                                  raft_conf_change_single_t change) {
    raft_conf_change_single_t *changes;
    if (out->changes_len == SIZE_MAX ||
        out->changes_len + 1 > SIZE_MAX / sizeof(*changes)) {
        return RAFT_ERR_OUT_OF_MEMORY;
    }
    changes = realloc(
        out->changes, (out->changes_len + 1) * sizeof(*changes));
    if (changes == NULL) {
        return RAFT_ERR_OUT_OF_MEMORY;
    }
    out->changes = changes;
    out->changes[out->changes_len++] = change;
    return RAFT_OK;
}

static int decode_v2(const raft_byte_view_t *data,
                     raft_decoded_conf_change_t *out) {
    size_t position = 0;
    uint64_t key;
    uint64_t value;
    int result;

    out->transition = RAFT_CONF_CHANGE_TRANSITION_AUTO;
    while (position < data->len) {
        result = proto_read_varint(
            data->data, data->len, &position, &key);
        if (result != RAFT_OK || (key >> 3) == 0) {
            return RAFT_ERR_INVALID_ARGUMENT;
        }
        if ((key >> 3) == 1 && (key & 7) == 0) {
            result = proto_read_varint(
                data->data, data->len, &position, &value);
            if (result != RAFT_OK ||
                value > RAFT_CONF_CHANGE_TRANSITION_JOINT_EXPLICIT) {
                return RAFT_ERR_INVALID_ARGUMENT;
            }
            out->transition = (raft_conf_change_transition_t)value;
        } else if ((key >> 3) == 2 && (key & 7) == 2) {
            raft_conf_change_single_t single;
            size_t item_len;
            result = proto_read_length(
                data->data, data->len, &position, &item_len);
            if (result == RAFT_OK) {
                result = decode_single(
                    &data->data[position], item_len, &single);
            }
            if (result == RAFT_OK) {
                result = decoded_changes_append(out, single);
            }
            if (result != RAFT_OK) {
                return result;
            }
            position += item_len;
        } else {
            result = proto_skip(data->data,
                                data->len,
                                &position,
                                (uint32_t)(key & 7));
            if (result != RAFT_OK) {
                return result;
            }
        }
    }
    return RAFT_OK;
}

int raft_confchange_decode_entry(raft_entry_type_t entry_type,
                                 const raft_byte_view_t *data,
                                 raft_decoded_conf_change_t *out) {
    int result;
    if (out == NULL || !raft_byte_view_valid(data)) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    memset(out, 0, sizeof(*out));
    if (entry_type == RAFT_ENTRY_CONF_CHANGE) {
        result = decode_v1(data, out);
    } else if (entry_type == RAFT_ENTRY_CONF_CHANGE_V2) {
        result = decode_v2(data, out);
    } else {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    if (result != RAFT_OK) {
        raft_decoded_conf_change_free(out);
    }
    return result;
}

bool raft_confchange_is_leave_joint(
    const raft_decoded_conf_change_t *change) {
    return change != NULL &&
           change->transition == RAFT_CONF_CHANGE_TRANSITION_AUTO &&
           change->changes_len == 0;
}

bool raft_confchange_wants_enter_joint(
    const raft_decoded_conf_change_t *change,
    bool *auto_leave) {
    if (change == NULL || auto_leave == NULL) {
        return false;
    }
    if (change->transition == RAFT_CONF_CHANGE_TRANSITION_AUTO &&
        change->changes_len <= 1) {
        *auto_leave = false;
        return false;
    }
    *auto_leave =
        change->transition != RAFT_CONF_CHANGE_TRANSITION_JOINT_EXPLICIT;
    return true;
}

static bool vector_has_duplicate_or_reserved(const raft_uint64_vec_t *vec) {
    size_t i;
    if (vec == NULL || !confchange_array_valid(vec->items, vec->len)) {
        return true;
    }
    for (i = 0; i < vec->len; ++i) {
        size_t j;
        if (!raft_is_valid_node_id(vec->items[i])) {
            return true;
        }
        for (j = 0; j < i; ++j) {
            if (vec->items[j] == vec->items[i]) {
                return true;
            }
        }
    }
    return false;
}

int raft_confchange_check_invariants(
    const raft_progress_tracker_t *tracker) {
    raft_uint64_vec_t voters = {0};
    size_t i;
    int result;

    if (tracker == NULL ||
        vector_has_duplicate_or_reserved(&tracker->config.voters) ||
        vector_has_duplicate_or_reserved(
            &tracker->config.voters_outgoing) ||
        vector_has_duplicate_or_reserved(&tracker->config.learners) ||
        vector_has_duplicate_or_reserved(
            &tracker->config.learners_next)) {
        return RAFT_ERR_FATAL;
    }
    result = raft_id_vec_union(&tracker->config.voters,
                               &tracker->config.voters_outgoing,
                               &voters);
    if (result != RAFT_OK) {
        return result;
    }
    for (i = 0; i < voters.len; ++i) {
        if (!raft_tracker_has_progress(tracker, voters.items[i])) {
            result = RAFT_ERR_FATAL;
            goto done;
        }
    }
    for (i = 0; i < tracker->config.learners.len; ++i) {
        uint64_t id = tracker->config.learners.items[i];
        const raft_progress_internal_t *progress =
            raft_tracker_find_const(tracker, id);
        if (progress == NULL || !progress->is_learner ||
            raft_id_vec_contains(&voters, id)) {
            result = RAFT_ERR_FATAL;
            goto done;
        }
    }
    for (i = 0; i < tracker->config.learners_next.len; ++i) {
        uint64_t id = tracker->config.learners_next.items[i];
        const raft_progress_internal_t *progress =
            raft_tracker_find_const(tracker, id);
        if (progress == NULL || progress->is_learner ||
            !raft_id_vec_contains(
                &tracker->config.voters_outgoing, id)) {
            result = RAFT_ERR_FATAL;
            goto done;
        }
    }
    if (!raft_tracker_is_joint(tracker) &&
        (tracker->config.learners_next.len != 0 ||
         tracker->config.auto_leave)) {
        result = RAFT_ERR_FATAL;
        goto done;
    }
    result = RAFT_OK;

done:
    raft_uint64_vec_free(&voters);
    return result;
}

static int tracker_add_config_progress(raft_progress_tracker_t *tracker,
                                       uint64_t id,
                                       uint64_t next,
                                       bool learner) {
    raft_progress_internal_t *progress = raft_tracker_find(tracker, id);
    int result;
    if (progress == NULL) {
        return raft_tracker_add_progress(tracker, id, next, learner);
    }
    if (learner) {
        progress->is_learner = true;
    }
    result = RAFT_OK;
    return result;
}

int raft_confchange_restore(raft_progress_tracker_t *tracker,
                            const raft_conf_state_t *state,
                            uint64_t last_index) {
    raft_progress_tracker_t restored;
    uint64_t next = last_index > 1 ? last_index : 1;
    size_t i;
    int result;

    if (tracker == NULL || state == NULL ||
        !confchange_array_valid(state->voters.items,
                                state->voters.len) ||
        !confchange_array_valid(state->voters_outgoing.items,
                                state->voters_outgoing.len) ||
        !confchange_array_valid(state->learners.items,
                                state->learners.len) ||
        !confchange_array_valid(state->learners_next.items,
                                state->learners_next.len)) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    if (state->voters.len == 0 &&
        (state->voters_outgoing.len != 0 ||
         state->learners.len != 0 ||
         state->learners_next.len != 0)) {
        return RAFT_ERR_FATAL;
    }
    for (i = 0; i < state->learners_next.len; ++i) {
        if (raft_id_vec_contains(
                &state->voters, state->learners_next.items[i])) {
            return RAFT_ERR_FATAL;
        }
    }
    result = raft_tracker_init(&restored,
                               tracker->max_inflight_messages,
                               tracker->max_inflight_bytes);
    if (result != RAFT_OK) {
        return result;
    }
    result = raft_id_vec_copy(
        &restored.config.voters, &state->voters);
    if (result == RAFT_OK) {
        result = raft_id_vec_copy(&restored.config.voters_outgoing,
                                  &state->voters_outgoing);
    }
    if (result == RAFT_OK) {
        result = raft_id_vec_copy(
            &restored.config.learners, &state->learners);
    }
    if (result == RAFT_OK) {
        result = raft_id_vec_copy(&restored.config.learners_next,
                                  &state->learners_next);
    }
    restored.config.auto_leave = state->auto_leave;
    for (i = 0; result == RAFT_OK && i < state->voters.len; ++i) {
        result = tracker_add_config_progress(
            &restored, state->voters.items[i], next, false);
    }
    for (i = 0;
         result == RAFT_OK && i < state->voters_outgoing.len;
         ++i) {
        result = tracker_add_config_progress(
            &restored, state->voters_outgoing.items[i], next, false);
    }
    for (i = 0; result == RAFT_OK && i < state->learners.len; ++i) {
        result = tracker_add_config_progress(
            &restored, state->learners.items[i], next, true);
    }
    for (i = 0;
         result == RAFT_OK && i < state->learners_next.len;
         ++i) {
        result = tracker_add_config_progress(
            &restored, state->learners_next.items[i], next, false);
    }
    if (result == RAFT_OK) {
        result = raft_confchange_check_invariants(&restored);
    }
    if (result != RAFT_OK) {
        raft_tracker_free(&restored);
        return result;
    }
    raft_tracker_swap(tracker, &restored);
    raft_tracker_free(&restored);
    return RAFT_OK;
}

static int confchange_make_voter(raft_progress_tracker_t *tracker,
                                 uint64_t id,
                                 uint64_t next) {
    raft_progress_internal_t *progress = raft_tracker_find(tracker, id);
    int result;
    if (progress == NULL) {
        result = raft_tracker_add_progress(
            tracker, id, next, false);
        if (result != RAFT_OK) {
            return result;
        }
    } else {
        progress->is_learner = false;
    }
    raft_id_vec_remove(&tracker->config.learners, id);
    raft_id_vec_remove(&tracker->config.learners_next, id);
    return raft_id_vec_insert(&tracker->config.voters, id);
}

static void confchange_remove(raft_progress_tracker_t *tracker,
                              uint64_t id) {
    if (!raft_tracker_has_progress(tracker, id)) {
        return;
    }
    raft_id_vec_remove(&tracker->config.voters, id);
    raft_id_vec_remove(&tracker->config.learners, id);
    raft_id_vec_remove(&tracker->config.learners_next, id);
    if (!raft_id_vec_contains(
            &tracker->config.voters_outgoing, id)) {
        raft_tracker_remove_progress(tracker, id);
    }
}

static int confchange_make_learner(raft_progress_tracker_t *tracker,
                                   uint64_t id,
                                   uint64_t next) {
    raft_progress_internal_t *progress = raft_tracker_find(tracker, id);
    int result;
    if (progress == NULL) {
        result = raft_tracker_add_progress(
            tracker, id, next, true);
        if (result != RAFT_OK) {
            return result;
        }
        return raft_id_vec_insert(&tracker->config.learners, id);
    }
    if (progress->is_learner) {
        return RAFT_OK;
    }
    // Remove the node from the incoming configuration while retaining its
    // replication state. This mirrors Changer.makeLearner's save-and-restore
    // of the Progress pointer; demotion must not discard Match/Next or the
    // inflight window.
    raft_id_vec_remove(&tracker->config.voters, id);
    raft_id_vec_remove(&tracker->config.learners, id);
    raft_id_vec_remove(&tracker->config.learners_next, id);
    if (raft_id_vec_contains(
            &tracker->config.voters_outgoing, id)) {
        return raft_id_vec_insert(
            &tracker->config.learners_next, id);
    }
    progress->is_learner = true;
    return raft_id_vec_insert(&tracker->config.learners, id);
}

static int confchange_apply_operations(
    raft_progress_tracker_t *tracker,
    const raft_decoded_conf_change_t *change,
    uint64_t last_index) {
    uint64_t next = last_index > 1 ? last_index : 1;
    size_t i;
    int result = RAFT_OK;

    for (i = 0; i < change->changes_len; ++i) {
        const raft_conf_change_single_t *single = &change->changes[i];
        if (single->node_id == RAFT_NONE) {
            continue;
        }
        if (!raft_is_valid_node_id(single->node_id) ||
            !confchange_type_valid(single->type)) {
            return RAFT_ERR_INVALID_ARGUMENT;
        }
        switch (single->type) {
            case RAFT_CONF_CHANGE_ADD_NODE:
                result = confchange_make_voter(
                    tracker, single->node_id, next);
                break;
            case RAFT_CONF_CHANGE_ADD_LEARNER_NODE:
                result = confchange_make_learner(
                    tracker, single->node_id, next);
                break;
            case RAFT_CONF_CHANGE_REMOVE_NODE:
                confchange_remove(tracker, single->node_id);
                break;
            case RAFT_CONF_CHANGE_UPDATE_NODE:
                break;
            default:
                return RAFT_ERR_INVALID_ARGUMENT;
        }
        if (result != RAFT_OK) {
            return result;
        }
    }
    return tracker->config.voters.len == 0
               ? RAFT_ERR_FATAL
               : RAFT_OK;
}

static size_t vector_symmetric_difference(
    const raft_uint64_vec_t *left,
    const raft_uint64_vec_t *right) {
    size_t count = 0;
    size_t i;
    for (i = 0; i < left->len; ++i) {
        if (!raft_id_vec_contains(right, left->items[i])) {
            ++count;
        }
    }
    for (i = 0; i < right->len; ++i) {
        if (!raft_id_vec_contains(left, right->items[i])) {
            ++count;
        }
    }
    return count;
}

static int confchange_leave_joint(raft_progress_tracker_t *tracker) {
    size_t i;
    if (!raft_tracker_is_joint(tracker)) {
        return RAFT_ERR_FATAL;
    }
    for (i = 0; i < tracker->config.learners_next.len; ++i) {
        uint64_t id = tracker->config.learners_next.items[i];
        raft_progress_internal_t *progress =
            raft_tracker_find(tracker, id);
        int result = raft_id_vec_insert(
            &tracker->config.learners, id);
        if (result != RAFT_OK || progress == NULL) {
            return result != RAFT_OK ? result : RAFT_ERR_FATAL;
        }
        progress->is_learner = true;
    }
    for (i = tracker->config.voters_outgoing.len; i > 0; --i) {
        uint64_t id = tracker->config.voters_outgoing.items[i - 1];
        if (!raft_id_vec_contains(&tracker->config.voters, id) &&
            !raft_id_vec_contains(&tracker->config.learners, id)) {
            raft_tracker_remove_progress(tracker, id);
        }
    }
    raft_uint64_vec_free(&tracker->config.voters_outgoing);
    raft_uint64_vec_free(&tracker->config.learners_next);
    tracker->config.auto_leave = false;
    return RAFT_OK;
}

static int confchange_enter_joint(
    raft_progress_tracker_t *tracker,
    const raft_decoded_conf_change_t *change,
    uint64_t last_index,
    bool auto_leave) {
    int result;
    if (raft_tracker_is_joint(tracker) ||
        tracker->config.voters.len == 0) {
        return RAFT_ERR_FATAL;
    }
    result = raft_id_vec_copy(&tracker->config.voters_outgoing,
                              &tracker->config.voters);
    if (result != RAFT_OK) {
        return result;
    }
    result = confchange_apply_operations(
        tracker, change, last_index);
    if (result == RAFT_OK) {
        tracker->config.auto_leave = auto_leave;
    }
    return result;
}

static int confchange_simple(
    raft_progress_tracker_t *tracker,
    const raft_decoded_conf_change_t *change,
    uint64_t last_index) {
    raft_uint64_vec_t before = {0};
    int result;
    if (raft_tracker_is_joint(tracker)) {
        return RAFT_ERR_FATAL;
    }
    result = raft_id_vec_copy(
        &before, &tracker->config.voters);
    if (result == RAFT_OK) {
        result = confchange_apply_operations(
            tracker, change, last_index);
    }
    if (result == RAFT_OK &&
        vector_symmetric_difference(
            &before, &tracker->config.voters) > 1) {
        result = RAFT_ERR_FATAL;
    }
    raft_uint64_vec_free(&before);
    return result;
}

int raft_confchange_apply(raft_progress_tracker_t *tracker,
                          const raft_decoded_conf_change_t *change,
                          uint64_t last_index) {
    raft_progress_tracker_t updated;
    bool auto_leave;
    int result;

    if (tracker == NULL || change == NULL ||
        !confchange_transition_valid(change->transition) ||
        !confchange_array_valid(change->changes,
                                change->changes_len)) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    result = raft_confchange_check_invariants(tracker);
    if (result != RAFT_OK) {
        return result;
    }
    result = raft_tracker_clone(&updated, tracker);
    if (result != RAFT_OK) {
        return result;
    }
    if (raft_confchange_is_leave_joint(change)) {
        result = confchange_leave_joint(&updated);
    } else if (raft_confchange_wants_enter_joint(
                   change, &auto_leave)) {
        result = confchange_enter_joint(
            &updated, change, last_index, auto_leave);
    } else {
        result = confchange_simple(&updated, change, last_index);
    }
    if (result == RAFT_OK) {
        result = raft_confchange_check_invariants(&updated);
    }
    if (result != RAFT_OK) {
        raft_tracker_free(&updated);
        return result;
    }
    raft_tracker_swap(tracker, &updated);
    raft_tracker_free(&updated);
    return RAFT_OK;
}

int raft_confchange_apply_view(raft_progress_tracker_t *tracker,
                               const raft_conf_change_v2_view_t *change,
                               uint64_t last_index) {
    raft_decoded_conf_change_t decoded;
    int result;
    if (change == NULL ||
        !confchange_transition_valid(change->transition) ||
        !confchange_array_valid(change->changes,
                                change->changes_len)) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    memset(&decoded, 0, sizeof(decoded));
    decoded.transition = change->transition;
    if (change->changes_len != 0) {
        if (change->changes_len >
            SIZE_MAX / sizeof(*decoded.changes)) {
            return RAFT_ERR_OUT_OF_MEMORY;
        }
        decoded.changes = malloc(
            change->changes_len * sizeof(*decoded.changes));
        if (decoded.changes == NULL) {
            return RAFT_ERR_OUT_OF_MEMORY;
        }
        memcpy(decoded.changes,
               change->changes,
               change->changes_len * sizeof(*decoded.changes));
        decoded.changes_len = change->changes_len;
    }
    result = raft_confchange_apply(tracker, &decoded, last_index);
    raft_decoded_conf_change_free(&decoded);
    return result;
}
