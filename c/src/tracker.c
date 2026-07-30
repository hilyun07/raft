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

#include "tracker.h"

#include <stdlib.h>
#include <string.h>

#define RAFT_ALLOC_REPLACE_STDLIB
#include "alloc.h"

static uint64_t tracker_min_u64(uint64_t left, uint64_t right) {
    return left < right ? left : right;
}

static uint64_t tracker_max_u64(uint64_t left, uint64_t right) {
    return left > right ? left : right;
}

static uint64_t tracker_saturating_add_one(uint64_t value) {
    return value == UINT64_MAX ? UINT64_MAX : value + 1;
}

static bool tracker_array_valid(const void *items, size_t len) {
    return len == 0 || items != NULL;
}

static int tracker_u64_compare(const void *left, const void *right) {
    uint64_t a = *(const uint64_t *)left;
    uint64_t b = *(const uint64_t *)right;
    return a < b ? -1 : a > b ? 1 : 0;
}

bool raft_id_vec_contains(const raft_uint64_vec_t *vec, uint64_t id) {
    size_t low = 0;
    size_t high;

    if (vec == NULL || !tracker_array_valid(vec->items, vec->len)) {
        return false;
    }
    high = vec->len;
    while (low < high) {
        size_t middle = low + (high - low) / 2;
        if (vec->items[middle] == id) {
            return true;
        }
        if (vec->items[middle] < id) {
            low = middle + 1;
        } else {
            high = middle;
        }
    }
    return false;
}

int raft_id_vec_insert(raft_uint64_vec_t *vec, uint64_t id) {
    uint64_t *items;
    size_t at = 0;

    if (vec == NULL || !raft_is_valid_node_id(id) ||
        !tracker_array_valid(vec->items, vec->len)) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    while (at < vec->len && vec->items[at] < id) {
        ++at;
    }
    if (at < vec->len && vec->items[at] == id) {
        return RAFT_OK;
    }
    if (vec->len == SIZE_MAX ||
        vec->len + 1 > SIZE_MAX / sizeof(*items)) {
        return RAFT_ERR_OUT_OF_MEMORY;
    }
    items = realloc(vec->items, (vec->len + 1) * sizeof(*items));
    if (items == NULL) {
        return RAFT_ERR_OUT_OF_MEMORY;
    }
    vec->items = items;
    if (at < vec->len) {
        memmove(&vec->items[at + 1],
                &vec->items[at],
                (vec->len - at) * sizeof(*vec->items));
    }
    vec->items[at] = id;
    ++vec->len;
    return RAFT_OK;
}

void raft_id_vec_remove(raft_uint64_vec_t *vec, uint64_t id) {
    size_t at;

    if (vec == NULL || !tracker_array_valid(vec->items, vec->len)) {
        return;
    }
    for (at = 0; at < vec->len; ++at) {
        if (vec->items[at] == id) {
            if (at + 1 < vec->len) {
                memmove(&vec->items[at],
                        &vec->items[at + 1],
                        (vec->len - at - 1) * sizeof(*vec->items));
            }
            --vec->len;
            if (vec->len == 0) {
                free(vec->items);
                vec->items = NULL;
            }
            return;
        }
    }
}

int raft_id_vec_copy(raft_uint64_vec_t *dst,
                     const raft_uint64_vec_t *src) {
    if (dst == NULL || src == NULL ||
        !tracker_array_valid(src->items, src->len)) {
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
    qsort(dst->items, dst->len, sizeof(*dst->items), tracker_u64_compare);
    return RAFT_OK;
}

int raft_id_vec_union(const raft_uint64_vec_t *left,
                      const raft_uint64_vec_t *right,
                      raft_uint64_vec_t *out) {
    size_t i;
    int result;

    if (left == NULL || right == NULL || out == NULL ||
        !tracker_array_valid(left->items, left->len) ||
        !tracker_array_valid(right->items, right->len)) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    memset(out, 0, sizeof(*out));
    for (i = 0; i < left->len; ++i) {
        result = raft_id_vec_insert(out, left->items[i]);
        if (result != RAFT_OK) {
            raft_uint64_vec_free(out);
            return result;
        }
    }
    for (i = 0; i < right->len; ++i) {
        result = raft_id_vec_insert(out, right->items[i]);
        if (result != RAFT_OK) {
            raft_uint64_vec_free(out);
            return result;
        }
    }
    return RAFT_OK;
}

int raft_inflights_init(raft_inflights_internal_t *in,
                        size_t size,
                        uint64_t max_bytes) {
    if (in == NULL || size == 0) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    memset(in, 0, sizeof(*in));
    in->size = size;
    in->max_bytes = max_bytes;
    return RAFT_OK;
}

void raft_inflights_free(raft_inflights_internal_t *in) {
    if (in == NULL) {
        return;
    }
    free(in->buffer);
    memset(in, 0, sizeof(*in));
}

int raft_inflights_clone(raft_inflights_internal_t *dst,
                         const raft_inflights_internal_t *src) {
    if (dst == NULL || src == NULL || src->size == 0 ||
        src->capacity > src->size ||
        !tracker_array_valid(src->buffer, src->capacity)) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    memset(dst, 0, sizeof(*dst));
    *dst = *src;
    dst->buffer = NULL;
    if (src->capacity == 0) {
        return RAFT_OK;
    }
    if (src->capacity > SIZE_MAX / sizeof(*dst->buffer)) {
        memset(dst, 0, sizeof(*dst));
        return RAFT_ERR_OUT_OF_MEMORY;
    }
    dst->buffer = malloc(src->capacity * sizeof(*dst->buffer));
    if (dst->buffer == NULL) {
        memset(dst, 0, sizeof(*dst));
        return RAFT_ERR_OUT_OF_MEMORY;
    }
    memcpy(dst->buffer,
           src->buffer,
           src->capacity * sizeof(*dst->buffer));
    return RAFT_OK;
}

void raft_inflights_reset(raft_inflights_internal_t *in) {
    if (in == NULL) {
        return;
    }
    in->start = 0;
    in->count = 0;
    in->bytes = 0;
}

bool raft_inflights_full(const raft_inflights_internal_t *in) {
    if (in == NULL || in->size == 0) {
        return true;
    }
    return in->count == in->size ||
           (in->max_bytes != 0 && in->bytes >= in->max_bytes);
}

static int inflights_grow(raft_inflights_internal_t *in) {
    raft_inflight_internal_t *buffer;
    size_t new_capacity;
    size_t i;

    if (in->capacity >= in->size) {
        return RAFT_ERR_FATAL;
    }
    new_capacity = in->capacity == 0 ? 1 : in->capacity * 2;
    if (new_capacity < in->capacity || new_capacity > in->size) {
        new_capacity = in->size;
    }
    if (new_capacity > SIZE_MAX / sizeof(*buffer)) {
        return RAFT_ERR_OUT_OF_MEMORY;
    }
    buffer = calloc(new_capacity, sizeof(*buffer));
    if (buffer == NULL) {
        return RAFT_ERR_OUT_OF_MEMORY;
    }
    for (i = 0; i < in->count; ++i) {
        buffer[i] = in->buffer[(in->start + i) % in->capacity];
    }
    free(in->buffer);
    in->buffer = buffer;
    in->capacity = new_capacity;
    in->start = 0;
    return RAFT_OK;
}

int raft_inflights_add(raft_inflights_internal_t *in,
                       uint64_t index,
                       uint64_t bytes) {
    size_t next;
    int result;

    if (in == NULL || in->size == 0 || raft_inflights_full(in)) {
        return RAFT_ERR_FATAL;
    }
    if (in->count != 0) {
        size_t last = (in->start + in->count - 1) % in->capacity;
        if (index < in->buffer[last].index) {
            return RAFT_ERR_FATAL;
        }
    }
    if (in->count == in->capacity) {
        result = inflights_grow(in);
        if (result != RAFT_OK) {
            return result;
        }
    }
    next = (in->start + in->count) % in->capacity;
    in->buffer[next].index = index;
    in->buffer[next].bytes = bytes;
    ++in->count;
    if (UINT64_MAX - in->bytes < bytes) {
        in->bytes = UINT64_MAX;
    } else {
        in->bytes += bytes;
    }
    return RAFT_OK;
}

void raft_inflights_free_le(raft_inflights_internal_t *in, uint64_t index) {
    size_t freed = 0;
    uint64_t bytes = 0;

    if (in == NULL || in->count == 0 ||
        index < in->buffer[in->start].index) {
        return;
    }
    while (freed < in->count) {
        size_t at = (in->start + freed) % in->capacity;
        if (index < in->buffer[at].index) {
            break;
        }
        if (UINT64_MAX - bytes < in->buffer[at].bytes) {
            bytes = UINT64_MAX;
        } else {
            bytes += in->buffer[at].bytes;
        }
        ++freed;
    }
    in->count -= freed;
    in->bytes = bytes > in->bytes ? 0 : in->bytes - bytes;
    in->start = in->count == 0 ? 0 : (in->start + freed) % in->capacity;
}

int raft_progress_init(raft_progress_internal_t *progress,
                       uint64_t id,
                       uint64_t next,
                       bool is_learner,
                       size_t max_inflight_messages,
                       uint64_t max_inflight_bytes) {
    int result;

    if (progress == NULL || !raft_is_valid_node_id(id) ||
        next == 0) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    memset(progress, 0, sizeof(*progress));
    result = raft_inflights_init(&progress->inflights,
                                 max_inflight_messages,
                                 max_inflight_bytes);
    if (result != RAFT_OK) {
        return result;
    }
    progress->id = id;
    progress->next_index = next;
    progress->state = RAFT_PROGRESS_STATE_PROBE;
    progress->is_learner = is_learner;
    progress->recent_active = true;
    return RAFT_OK;
}

int raft_progress_reset(raft_progress_internal_t *progress,
                        uint64_t match,
                        uint64_t next) {
    uint64_t id;
    uint64_t max_inflight_bytes;
    size_t max_inflight_messages;
    bool is_learner;
    int result;

    if (progress == NULL || !raft_is_valid_node_id(progress->id) ||
        next == 0 || match >= next ||
        progress->inflights.size == 0) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    id = progress->id;
    is_learner = progress->is_learner;
    max_inflight_messages = progress->inflights.size;
    max_inflight_bytes = progress->inflights.max_bytes;

    raft_inflights_free(&progress->inflights);
    memset(progress, 0, sizeof(*progress));
    result = raft_inflights_init(&progress->inflights,
                                 max_inflight_messages,
                                 max_inflight_bytes);
    if (result != RAFT_OK) {
        return result;
    }
    progress->id = id;
    progress->match_index = match;
    progress->next_index = next;
    progress->state = RAFT_PROGRESS_STATE_PROBE;
    progress->is_learner = is_learner;
    return RAFT_OK;
}

void raft_progress_free(raft_progress_internal_t *progress) {
    if (progress == NULL) {
        return;
    }
    raft_inflights_free(&progress->inflights);
    memset(progress, 0, sizeof(*progress));
}

int raft_progress_clone(raft_progress_internal_t *dst,
                        const raft_progress_internal_t *src) {
    int result;

    if (dst == NULL || src == NULL) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    memset(dst, 0, sizeof(*dst));
    *dst = *src;
    memset(&dst->inflights, 0, sizeof(dst->inflights));
    result = raft_inflights_clone(&dst->inflights, &src->inflights);
    if (result != RAFT_OK) {
        memset(dst, 0, sizeof(*dst));
    }
    return result;
}

void raft_progress_reset_state(raft_progress_internal_t *progress,
                               raft_progress_state_t state) {
    if (progress == NULL) {
        return;
    }
    progress->message_flow_paused = false;
    progress->pending_snapshot = 0;
    progress->state = state;
    raft_inflights_reset(&progress->inflights);
}

void raft_progress_become_probe(raft_progress_internal_t *progress) {
    uint64_t pending_snapshot;

    if (progress == NULL) {
        return;
    }
    if (progress->state == RAFT_PROGRESS_STATE_SNAPSHOT) {
        pending_snapshot = progress->pending_snapshot;
        raft_progress_reset_state(progress, RAFT_PROGRESS_STATE_PROBE);
        progress->next_index = tracker_max_u64(
            tracker_saturating_add_one(progress->match_index),
            tracker_saturating_add_one(pending_snapshot));
    } else {
        raft_progress_reset_state(progress, RAFT_PROGRESS_STATE_PROBE);
        progress->next_index =
            tracker_saturating_add_one(progress->match_index);
    }
    progress->sent_commit = tracker_min_u64(
        progress->sent_commit, progress->next_index - 1);
}

void raft_progress_become_replicate(raft_progress_internal_t *progress) {
    if (progress == NULL) {
        return;
    }
    raft_progress_reset_state(progress, RAFT_PROGRESS_STATE_REPLICATE);
    progress->next_index =
        tracker_saturating_add_one(progress->match_index);
}

void raft_progress_become_snapshot(raft_progress_internal_t *progress,
                                   uint64_t snapshot_index) {
    if (progress == NULL) {
        return;
    }
    raft_progress_reset_state(progress, RAFT_PROGRESS_STATE_SNAPSHOT);
    progress->pending_snapshot = snapshot_index;
    progress->next_index = tracker_saturating_add_one(snapshot_index);
    progress->sent_commit = snapshot_index;
}

int raft_progress_sent_entries(raft_progress_internal_t *progress,
                               size_t entry_count,
                               uint64_t bytes) {
    int result;

    if (progress == NULL || entry_count > UINT64_MAX) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    switch (progress->state) {
        case RAFT_PROGRESS_STATE_REPLICATE:
            if (entry_count != 0) {
                if ((uint64_t)entry_count >
                    UINT64_MAX - progress->next_index) {
                    return RAFT_ERR_FATAL;
                }
                progress->next_index += (uint64_t)entry_count;
                result = raft_inflights_add(&progress->inflights,
                                            progress->next_index - 1,
                                            bytes);
                if (result != RAFT_OK) {
                    return result;
                }
            }
            progress->message_flow_paused =
                raft_inflights_full(&progress->inflights);
            return RAFT_OK;
        case RAFT_PROGRESS_STATE_PROBE:
            if (entry_count != 0) {
                progress->message_flow_paused = true;
            }
            return RAFT_OK;
        default:
            return RAFT_ERR_FATAL;
    }
}

bool raft_progress_can_bump_commit(const raft_progress_internal_t *progress,
                                   uint64_t index) {
    return progress != NULL &&
           index > progress->sent_commit &&
           progress->sent_commit < progress->next_index - 1;
}

void raft_progress_sent_commit(raft_progress_internal_t *progress,
                               uint64_t commit) {
    if (progress != NULL) {
        progress->sent_commit = commit;
    }
}

bool raft_progress_maybe_update(raft_progress_internal_t *progress,
                                uint64_t index) {
    if (progress == NULL || index <= progress->match_index) {
        return false;
    }
    progress->match_index = index;
    progress->next_index = tracker_max_u64(
        progress->next_index, tracker_saturating_add_one(index));
    progress->message_flow_paused = false;
    return true;
}

bool raft_progress_maybe_decr_to(raft_progress_internal_t *progress,
                                 uint64_t rejected,
                                 uint64_t match_hint) {
    uint64_t next;

    if (progress == NULL || progress->next_index == 0) {
        return false;
    }
    if (progress->state == RAFT_PROGRESS_STATE_REPLICATE) {
        if (rejected <= progress->match_index) {
            return false;
        }
        progress->next_index =
            tracker_saturating_add_one(progress->match_index);
        progress->sent_commit = tracker_min_u64(
            progress->sent_commit, progress->next_index - 1);
        return true;
    }
    if (progress->next_index - 1 != rejected) {
        return false;
    }
    next = tracker_min_u64(
        rejected, tracker_saturating_add_one(match_hint));
    next = tracker_max_u64(
        next, tracker_saturating_add_one(progress->match_index));
    progress->next_index = next;
    progress->sent_commit = tracker_min_u64(
        progress->sent_commit, progress->next_index - 1);
    progress->message_flow_paused = false;
    return true;
}

bool raft_progress_is_paused(const raft_progress_internal_t *progress) {
    if (progress == NULL) {
        return true;
    }
    switch (progress->state) {
        case RAFT_PROGRESS_STATE_PROBE:
        case RAFT_PROGRESS_STATE_REPLICATE:
            return progress->message_flow_paused;
        case RAFT_PROGRESS_STATE_SNAPSHOT:
            return true;
        default:
            return true;
    }
}

int raft_tracker_init(raft_progress_tracker_t *tracker,
                      size_t max_inflight_messages,
                      uint64_t max_inflight_bytes) {
    if (tracker == NULL || max_inflight_messages == 0) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    memset(tracker, 0, sizeof(*tracker));
    tracker->max_inflight_messages = max_inflight_messages;
    tracker->max_inflight_bytes = max_inflight_bytes;
    return RAFT_OK;
}

void raft_tracker_free(raft_progress_tracker_t *tracker) {
    size_t i;

    if (tracker == NULL) {
        return;
    }
    for (i = 0; i < tracker->progress_len; ++i) {
        raft_progress_free(&tracker->progress[i]);
    }
    free(tracker->progress);
    raft_uint64_vec_free(&tracker->votes_granted);
    raft_uint64_vec_free(&tracker->votes_rejected);
    raft_conf_state_free(&tracker->config);
    memset(tracker, 0, sizeof(*tracker));
}

int raft_tracker_clone(raft_progress_tracker_t *dst,
                       const raft_progress_tracker_t *src) {
    size_t i;
    int result;

    if (dst == NULL || src == NULL) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    result = raft_tracker_init(dst,
                               src->max_inflight_messages,
                               src->max_inflight_bytes);
    if (result != RAFT_OK) {
        return result;
    }
    result = raft_id_vec_copy(&dst->config.voters,
                              &src->config.voters);
    if (result == RAFT_OK) {
        result = raft_id_vec_copy(&dst->config.voters_outgoing,
                                  &src->config.voters_outgoing);
    }
    if (result == RAFT_OK) {
        result = raft_id_vec_copy(&dst->config.learners,
                                  &src->config.learners);
    }
    if (result == RAFT_OK) {
        result = raft_id_vec_copy(&dst->config.learners_next,
                                  &src->config.learners_next);
    }
    if (result == RAFT_OK) {
        result = raft_id_vec_copy(&dst->votes_granted,
                                  &src->votes_granted);
    }
    if (result == RAFT_OK) {
        result = raft_id_vec_copy(&dst->votes_rejected,
                                  &src->votes_rejected);
    }
    if (result != RAFT_OK) {
        raft_tracker_free(dst);
        return result;
    }
    dst->config.auto_leave = src->config.auto_leave;
    if (src->progress_len == 0) {
        return RAFT_OK;
    }
    if (src->progress_len > SIZE_MAX / sizeof(*dst->progress)) {
        raft_tracker_free(dst);
        return RAFT_ERR_OUT_OF_MEMORY;
    }
    dst->progress = calloc(src->progress_len, sizeof(*dst->progress));
    if (dst->progress == NULL) {
        raft_tracker_free(dst);
        return RAFT_ERR_OUT_OF_MEMORY;
    }
    dst->progress_len = src->progress_len;
    for (i = 0; i < src->progress_len; ++i) {
        result = raft_progress_clone(&dst->progress[i],
                                     &src->progress[i]);
        if (result != RAFT_OK) {
            raft_tracker_free(dst);
            return result;
        }
    }
    return RAFT_OK;
}

void raft_tracker_swap(raft_progress_tracker_t *left,
                       raft_progress_tracker_t *right) {
    raft_progress_tracker_t temporary;
    if (left == NULL || right == NULL) {
        return;
    }
    temporary = *left;
    *left = *right;
    *right = temporary;
}

raft_progress_internal_t *raft_tracker_find(raft_progress_tracker_t *tracker,
                                            uint64_t id) {
    size_t low = 0;
    size_t high;

    if (tracker == NULL) {
        return NULL;
    }
    high = tracker->progress_len;
    while (low < high) {
        size_t middle = low + (high - low) / 2;
        if (tracker->progress[middle].id == id) {
            return &tracker->progress[middle];
        }
        if (tracker->progress[middle].id < id) {
            low = middle + 1;
        } else {
            high = middle;
        }
    }
    return NULL;
}

const raft_progress_internal_t *raft_tracker_find_const(
    const raft_progress_tracker_t *tracker, uint64_t id) {
    return raft_tracker_find((raft_progress_tracker_t *)tracker, id);
}

bool raft_tracker_has_progress(const raft_progress_tracker_t *tracker,
                               uint64_t id) {
    return raft_tracker_find_const(tracker, id) != NULL;
}

bool raft_tracker_is_voter(const raft_progress_tracker_t *tracker,
                           uint64_t id) {
    return tracker != NULL &&
           (raft_id_vec_contains(&tracker->config.voters, id) ||
            raft_id_vec_contains(&tracker->config.voters_outgoing, id));
}

bool raft_tracker_is_singleton(const raft_progress_tracker_t *tracker) {
    return tracker != NULL && tracker->config.voters.len == 1 &&
           tracker->config.voters_outgoing.len == 0;
}

bool raft_tracker_is_joint(const raft_progress_tracker_t *tracker) {
    return tracker != NULL &&
           tracker->config.voters_outgoing.len != 0;
}

int raft_tracker_add_progress(raft_progress_tracker_t *tracker,
                              uint64_t id,
                              uint64_t next,
                              bool is_learner) {
    raft_progress_internal_t *progress;
    size_t at = 0;
    int result;

    if (tracker == NULL || !raft_is_valid_node_id(id) || next == 0) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    while (at < tracker->progress_len &&
           tracker->progress[at].id < id) {
        ++at;
    }
    if (at < tracker->progress_len &&
        tracker->progress[at].id == id) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    if (tracker->progress_len == SIZE_MAX ||
        tracker->progress_len + 1 >
            SIZE_MAX / sizeof(*tracker->progress)) {
        return RAFT_ERR_OUT_OF_MEMORY;
    }
    progress = realloc(
        tracker->progress,
        (tracker->progress_len + 1) * sizeof(*tracker->progress));
    if (progress == NULL) {
        return RAFT_ERR_OUT_OF_MEMORY;
    }
    tracker->progress = progress;
    if (at < tracker->progress_len) {
        memmove(&tracker->progress[at + 1],
                &tracker->progress[at],
                (tracker->progress_len - at) *
                    sizeof(*tracker->progress));
    }
    memset(&tracker->progress[at], 0, sizeof(tracker->progress[at]));
    result = raft_progress_init(&tracker->progress[at],
                                id,
                                next,
                                is_learner,
                                tracker->max_inflight_messages,
                                tracker->max_inflight_bytes);
    if (result != RAFT_OK) {
        if (at < tracker->progress_len) {
            memmove(&tracker->progress[at],
                    &tracker->progress[at + 1],
                    (tracker->progress_len - at) *
                        sizeof(*tracker->progress));
        }
        return result;
    }
    ++tracker->progress_len;
    return RAFT_OK;
}

void raft_tracker_remove_progress(raft_progress_tracker_t *tracker,
                                  uint64_t id) {
    size_t at;

    if (tracker == NULL) {
        return;
    }
    for (at = 0; at < tracker->progress_len; ++at) {
        if (tracker->progress[at].id == id) {
            raft_progress_free(&tracker->progress[at]);
            if (at + 1 < tracker->progress_len) {
                memmove(&tracker->progress[at],
                        &tracker->progress[at + 1],
                        (tracker->progress_len - at - 1) *
                            sizeof(*tracker->progress));
            }
            --tracker->progress_len;
            if (tracker->progress_len == 0) {
                free(tracker->progress);
                tracker->progress = NULL;
            }
            return;
        }
    }
}

void raft_tracker_reset_votes(raft_progress_tracker_t *tracker) {
    if (tracker == NULL) {
        return;
    }
    raft_uint64_vec_free(&tracker->votes_granted);
    raft_uint64_vec_free(&tracker->votes_rejected);
}

int raft_tracker_record_vote(raft_progress_tracker_t *tracker,
                             uint64_t id,
                             bool granted) {
    if (tracker == NULL || !raft_is_valid_node_id(id)) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    if (raft_id_vec_contains(&tracker->votes_granted, id) ||
        raft_id_vec_contains(&tracker->votes_rejected, id)) {
        return RAFT_OK;
    }
    return raft_id_vec_insert(
        granted ? &tracker->votes_granted : &tracker->votes_rejected,
        id);
}

static raft_vote_result_internal_t tracker_majority_vote_result(
    const raft_progress_tracker_t *tracker,
    const raft_uint64_vec_t *voters) {
    size_t yes = 0;
    size_t missing = 0;
    size_t i;
    size_t quorum;

    if (voters->len == 0) {
        return RAFT_VOTE_WON;
    }
    for (i = 0; i < voters->len; ++i) {
        uint64_t id = voters->items[i];
        if (raft_id_vec_contains(&tracker->votes_granted, id)) {
            ++yes;
        } else if (!raft_id_vec_contains(
                       &tracker->votes_rejected, id)) {
            ++missing;
        }
    }
    quorum = voters->len / 2 + 1;
    if (yes >= quorum) {
        return RAFT_VOTE_WON;
    }
    if (yes + missing >= quorum) {
        return RAFT_VOTE_PENDING;
    }
    return RAFT_VOTE_LOST;
}

raft_vote_result_internal_t raft_tracker_vote_result(
    const raft_progress_tracker_t *tracker) {
    raft_vote_result_internal_t incoming;
    raft_vote_result_internal_t outgoing;

    if (tracker == NULL) {
        return RAFT_VOTE_LOST;
    }
    incoming = tracker_majority_vote_result(
        tracker, &tracker->config.voters);
    outgoing = tracker_majority_vote_result(
        tracker, &tracker->config.voters_outgoing);
    if (incoming == outgoing) {
        return incoming;
    }
    if (incoming == RAFT_VOTE_LOST || outgoing == RAFT_VOTE_LOST) {
        return RAFT_VOTE_LOST;
    }
    return RAFT_VOTE_PENDING;
}

static uint64_t tracker_majority_committed(
    const raft_progress_tracker_t *tracker,
    const raft_uint64_vec_t *voters) {
    uint64_t committed = 0;
    size_t quorum;
    size_t i;

    if (voters->len == 0) {
        return UINT64_MAX;
    }
    quorum = voters->len / 2 + 1;
    for (i = 0; i < voters->len; ++i) {
        const raft_progress_internal_t *progress =
            raft_tracker_find_const(tracker, voters->items[i]);
        uint64_t candidate =
            progress == NULL ? 0 : progress->match_index;
        size_t acknowledged = 0;
        size_t j;

        if (candidate <= committed) {
            continue;
        }
        for (j = 0; j < voters->len; ++j) {
            const raft_progress_internal_t *other =
                raft_tracker_find_const(
                    tracker, voters->items[j]);
            if (other != NULL &&
                other->match_index >= candidate) {
                ++acknowledged;
            }
        }
        if (acknowledged >= quorum) {
            committed = candidate;
        }
    }
    return committed;
}

uint64_t raft_tracker_committed(const raft_progress_tracker_t *tracker) {
    uint64_t incoming;
    uint64_t outgoing;
    if (tracker == NULL) {
        return 0;
    }
    incoming = tracker_majority_committed(
        tracker, &tracker->config.voters);
    outgoing = tracker_majority_committed(
        tracker, &tracker->config.voters_outgoing);
    return tracker_min_u64(incoming, outgoing);
}

static bool tracker_majority_active(
    const raft_progress_tracker_t *tracker,
    const raft_uint64_vec_t *voters) {
    size_t active = 0;
    size_t i;

    if (voters->len == 0) {
        return true;
    }
    for (i = 0; i < voters->len; ++i) {
        const raft_progress_internal_t *progress =
            raft_tracker_find_const(tracker, voters->items[i]);
        if (progress != NULL && progress->recent_active) {
            ++active;
        }
    }
    return active >= voters->len / 2 + 1;
}

bool raft_tracker_quorum_active(const raft_progress_tracker_t *tracker) {
    if (tracker == NULL) {
        return false;
    }
    return tracker_majority_active(
               tracker, &tracker->config.voters) &&
           tracker_majority_active(
               tracker, &tracker->config.voters_outgoing);
}

int raft_tracker_conf_state_copy(const raft_progress_tracker_t *tracker,
                                 raft_conf_state_t *out) {
    int result;

    if (tracker == NULL || out == NULL) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    memset(out, 0, sizeof(*out));
    result = raft_id_vec_copy(&out->voters, &tracker->config.voters);
    if (result == RAFT_OK) {
        result = raft_id_vec_copy(&out->voters_outgoing,
                                  &tracker->config.voters_outgoing);
    }
    if (result == RAFT_OK) {
        result = raft_id_vec_copy(&out->learners,
                                  &tracker->config.learners);
    }
    if (result == RAFT_OK) {
        result = raft_id_vec_copy(&out->learners_next,
                                  &tracker->config.learners_next);
    }
    if (result != RAFT_OK) {
        raft_conf_state_free(out);
        return result;
    }
    out->auto_leave = tracker->config.auto_leave;
    out->protobuf.fields = RAFT_CONF_STATE_PROTO_AUTO_LEAVE;
    out->protobuf.unknown_fields.is_nil = true;
    return RAFT_OK;
}

static void tracker_progress_snapshot_copy(
    raft_progress_snapshot_t *dst,
    const raft_progress_internal_t *src) {
    dst->id = src->id;
    dst->type = src->is_learner
                    ? RAFT_PROGRESS_LEARNER
                    : RAFT_PROGRESS_PEER;
    dst->progress.match_index = src->match_index;
    dst->progress.next_index = src->next_index;
    dst->progress.state = src->state;
    dst->progress.pending_snapshot = src->pending_snapshot;
    dst->progress.recent_active = src->recent_active;
    dst->progress.message_flow_paused =
        src->message_flow_paused;
    dst->progress.is_learner = src->is_learner;
}

static int tracker_inflights_snapshot_copy(
    raft_inflights_snapshot_t *dst,
    const raft_inflights_internal_t *src) {
    size_t i;

    if (dst == NULL || src == NULL || src->size == 0 ||
        src->capacity > src->size || src->count > src->capacity ||
        !tracker_array_valid(src->buffer, src->capacity)) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    memset(dst, 0, sizeof(*dst));
    dst->size = src->size;
    dst->max_bytes = src->max_bytes;
    if (src->count == 0) {
        return RAFT_OK;
    }
    if (src->count > SIZE_MAX / sizeof(*dst->items)) {
        return RAFT_ERR_OUT_OF_MEMORY;
    }
    dst->items = calloc(src->count, sizeof(*dst->items));
    if (dst->items == NULL) {
        return RAFT_ERR_OUT_OF_MEMORY;
    }
    dst->len = src->count;
    for (i = 0; i < src->count; ++i) {
        const raft_inflight_internal_t *item =
            &src->buffer[(src->start + i) % src->capacity];
        dst->items[i].index = item->index;
        dst->items[i].bytes = item->bytes;
    }
    return RAFT_OK;
}

int raft_tracker_progress_snapshot(const raft_progress_tracker_t *tracker,
                                   raft_progress_snapshot_t **out,
                                   size_t *out_len) {
    raft_progress_snapshot_t *snapshots;
    size_t i;

    if (tracker == NULL || out == NULL || out_len == NULL) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    *out = NULL;
    *out_len = 0;
    if (tracker->progress_len == 0) {
        return RAFT_OK;
    }
    if (tracker->progress_len > SIZE_MAX / sizeof(*snapshots)) {
        return RAFT_ERR_OUT_OF_MEMORY;
    }
    snapshots = calloc(tracker->progress_len, sizeof(*snapshots));
    if (snapshots == NULL) {
        return RAFT_ERR_OUT_OF_MEMORY;
    }
    for (i = 0; i < tracker->progress_len; ++i) {
        tracker_progress_snapshot_copy(
            &snapshots[i], &tracker->progress[i]);
    }
    *out = snapshots;
    *out_len = tracker->progress_len;
    return RAFT_OK;
}

void raft_tracker_status_progress_snapshot_free(
    raft_status_progress_t *snapshots, size_t len) {
    size_t i;

    if (snapshots == NULL) {
        return;
    }
    for (i = 0; i < len; ++i) {
        free(snapshots[i].inflights.items);
    }
    free(snapshots);
}

int raft_tracker_status_progress_snapshot(
    const raft_progress_tracker_t *tracker,
    raft_status_progress_t **out,
    size_t *out_len) {
    raft_status_progress_t *snapshots;
    size_t i;
    int result;

    if (tracker == NULL || out == NULL || out_len == NULL) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    *out = NULL;
    *out_len = 0;
    if (tracker->progress_len == 0) {
        return RAFT_OK;
    }
    if (tracker->progress_len > SIZE_MAX / sizeof(*snapshots)) {
        return RAFT_ERR_OUT_OF_MEMORY;
    }
    snapshots = calloc(tracker->progress_len, sizeof(*snapshots));
    if (snapshots == NULL) {
        return RAFT_ERR_OUT_OF_MEMORY;
    }
    for (i = 0; i < tracker->progress_len; ++i) {
        const raft_progress_internal_t *src = &tracker->progress[i];
        tracker_progress_snapshot_copy(
            &snapshots[i].snapshot, src);
        result = tracker_inflights_snapshot_copy(
            &snapshots[i].inflights, &src->inflights);
        if (result != RAFT_OK) {
            raft_tracker_status_progress_snapshot_free(
                snapshots, tracker->progress_len);
            return result;
        }
    }
    *out = snapshots;
    *out_len = tracker->progress_len;
    return RAFT_OK;
}
