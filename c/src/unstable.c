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

#include "unstable.h"

#include <stdlib.h>
#include <string.h>

#define RAFT_ALLOC_REPLACE_STDLIB
#include "alloc.h"

static int unstable_entry_copy(raft_entry_t *dst, const raft_entry_t *src) {
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
    if (result == RAFT_OK) {
        dst->protobuf.fields = src->protobuf.fields;
        result = raft_bytes_copy(
            &dst->protobuf.unknown_fields,
            &src->protobuf.unknown_fields);
    }
    if (result != RAFT_OK) {
        raft_entry_free(dst);
    }
    return result;
}

static int unstable_entry_range_copy(raft_entry_vec_t *dst,
                                     const raft_entry_t *entries,
                                     size_t entry_count) {
    size_t i;

    if (dst == NULL || (entry_count != 0 && entries == NULL)) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    memset(dst, 0, sizeof(*dst));
    if (entry_count == 0) {
        return RAFT_OK;
    }
    if (entry_count > SIZE_MAX / sizeof(*dst->items)) {
        return RAFT_ERR_OUT_OF_MEMORY;
    }
    dst->items = calloc(entry_count, sizeof(*dst->items));
    if (dst->items == NULL) {
        return RAFT_ERR_OUT_OF_MEMORY;
    }
    dst->len = entry_count;
    for (i = 0; i < entry_count; ++i) {
        int result = unstable_entry_copy(&dst->items[i], &entries[i]);
        if (result != RAFT_OK) {
            raft_entry_vec_free(dst);
            return result;
        }
    }
    return RAFT_OK;
}

static int unstable_snapshot_copy(raft_snapshot_t *dst,
                                  const raft_snapshot_t *src) {
    raft_snapshot_view_t view;

    if (dst == NULL || !raft_snapshot_valid(src)) {
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
    view.metadata.conf_state.protobuf.fields =
        src->metadata.conf_state.protobuf.fields;
    view.metadata.conf_state.protobuf.unknown_fields.data =
        src->metadata.conf_state.protobuf.unknown_fields.data;
    view.metadata.conf_state.protobuf.unknown_fields.len =
        src->metadata.conf_state.protobuf.unknown_fields.len;
    view.metadata.conf_state.protobuf.unknown_fields.is_nil =
        src->metadata.conf_state.protobuf.unknown_fields.is_nil;
    view.metadata.protobuf.fields =
        src->metadata.protobuf.fields;
    view.metadata.protobuf.unknown_fields.data =
        src->metadata.protobuf.unknown_fields.data;
    view.metadata.protobuf.unknown_fields.len =
        src->metadata.protobuf.unknown_fields.len;
    view.metadata.protobuf.unknown_fields.is_nil =
        src->metadata.protobuf.unknown_fields.is_nil;
    view.protobuf.fields = src->protobuf.fields;
    view.protobuf.unknown_fields.data =
        src->protobuf.unknown_fields.data;
    view.protobuf.unknown_fields.len =
        src->protobuf.unknown_fields.len;
    view.protobuf.unknown_fields.is_nil =
        src->protobuf.unknown_fields.is_nil;
    return raft_snapshot_copy_from_view(dst, &view);
}

void raft_unstable_init(raft_unstable_t *unstable, uint64_t offset) {
    if (unstable == NULL) {
        return;
    }
    memset(unstable, 0, sizeof(*unstable));
    unstable->offset = offset;
    unstable->offset_in_progress = offset;
}

void raft_unstable_free(raft_unstable_t *unstable) {
    if (unstable == NULL) {
        return;
    }
    raft_entry_vec_free(&unstable->entries);
    raft_snapshot_free(&unstable->snapshot);
    memset(unstable, 0, sizeof(*unstable));
}

bool raft_unstable_maybe_first_index(const raft_unstable_t *unstable,
                                     uint64_t *index) {
    if (unstable == NULL || index == NULL || !unstable->has_snapshot) {
        return false;
    }
    if (unstable->snapshot.metadata.index == UINT64_MAX) {
        return false;
    }
    *index = unstable->snapshot.metadata.index + 1;
    return true;
}

bool raft_unstable_maybe_last_index(const raft_unstable_t *unstable,
                                    uint64_t *index) {
    if (unstable == NULL || index == NULL) {
        return false;
    }
    if (unstable->entries.len != 0) {
        if (unstable->offset >
            UINT64_MAX - (uint64_t)unstable->entries.len + 1) {
            return false;
        }
        *index = unstable->offset + (uint64_t)unstable->entries.len - 1;
        return true;
    }
    if (unstable->has_snapshot) {
        *index = unstable->snapshot.metadata.index;
        return true;
    }
    return false;
}

bool raft_unstable_maybe_term(const raft_unstable_t *unstable,
                              uint64_t index,
                              uint64_t *term) {
    uint64_t last;

    if (unstable == NULL || term == NULL) {
        return false;
    }
    if (index < unstable->offset) {
        if (unstable->has_snapshot &&
            unstable->snapshot.metadata.index == index) {
            *term = unstable->snapshot.metadata.term;
            return true;
        }
        return false;
    }
    if (!raft_unstable_maybe_last_index(unstable, &last) || index > last ||
        index - unstable->offset >= unstable->entries.len) {
        return false;
    }
    *term = unstable->entries.items[index - unstable->offset].term;
    return true;
}

int raft_unstable_slice(const raft_unstable_t *unstable,
                        uint64_t lo,
                        uint64_t hi,
                        raft_entry_vec_t *out) {
    uint64_t upper;

    if (out == NULL) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    memset(out, 0, sizeof(*out));
    if (unstable == NULL || lo > hi ||
        unstable->entries.len > UINT64_MAX - unstable->offset) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    upper = unstable->offset + (uint64_t)unstable->entries.len;
    if (lo < unstable->offset || hi > upper) {
        return RAFT_ERR_FATAL;
    }
    {
        size_t count = (size_t)(hi - lo);
        const raft_entry_t *begin =
            count == 0
                ? NULL
                : unstable->entries.items +
                      (size_t)(lo - unstable->offset);
        return unstable_entry_range_copy(out, begin, count);
    }
}

int raft_unstable_next_entries(const raft_unstable_t *unstable,
                               raft_entry_vec_t *out) {
    uint64_t in_progress;

    if (unstable == NULL || out == NULL ||
        unstable->offset_in_progress < unstable->offset) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    in_progress = unstable->offset_in_progress - unstable->offset;
    if (in_progress > unstable->entries.len) {
        return RAFT_ERR_FATAL;
    }
    {
        size_t count = unstable->entries.len - (size_t)in_progress;
        const raft_entry_t *begin =
            count == 0
                ? NULL
                : unstable->entries.items + (size_t)in_progress;
        return unstable_entry_range_copy(out, begin, count);
    }
}

int raft_unstable_next_snapshot(const raft_unstable_t *unstable,
                                bool *has_snapshot,
                                raft_snapshot_t *out) {
    if (unstable == NULL || has_snapshot == NULL || out == NULL) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    *has_snapshot = false;
    memset(out, 0, sizeof(*out));
    out->data.is_nil = true;
    if (!unstable->has_snapshot || unstable->snapshot_in_progress) {
        return RAFT_OK;
    }
    {
        int result = unstable_snapshot_copy(out, &unstable->snapshot);
        if (result != RAFT_OK) {
            return result;
        }
    }
    *has_snapshot = true;
    return RAFT_OK;
}

int raft_unstable_snapshot(const raft_unstable_t *unstable,
                           bool *has_snapshot,
                           raft_snapshot_t *out) {
    if (unstable == NULL || has_snapshot == NULL || out == NULL) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    *has_snapshot = false;
    memset(out, 0, sizeof(*out));
    out->data.is_nil = true;
    if (!unstable->has_snapshot) {
        return RAFT_OK;
    }
    {
        int result = unstable_snapshot_copy(out, &unstable->snapshot);
        if (result != RAFT_OK) {
            return result;
        }
    }
    *has_snapshot = true;
    return RAFT_OK;
}

void raft_unstable_accept_in_progress(raft_unstable_t *unstable) {
    if (unstable == NULL) {
        return;
    }
    if (unstable->entries.len != 0) {
        uint64_t last =
            unstable->entries.items[unstable->entries.len - 1].index;
        unstable->offset_in_progress =
            last == UINT64_MAX ? UINT64_MAX : last + 1;
    }
    if (unstable->has_snapshot) {
        unstable->snapshot_in_progress = true;
    }
}

void raft_unstable_stable_to(raft_unstable_t *unstable,
                             uint64_t index,
                             uint64_t term) {
    uint64_t found_term;
    size_t remove_count;
    size_t remaining;

    if (unstable == NULL || index == UINT64_MAX ||
        !raft_unstable_maybe_term(unstable, index, &found_term) ||
        index < unstable->offset || found_term != term) {
        return;
    }
    remove_count = (size_t)(index + 1 - unstable->offset);
    if (remove_count > unstable->entries.len) {
        return;
    }
    remaining = unstable->entries.len - remove_count;
    for (size_t i = 0; i < remove_count; ++i) {
        raft_entry_free(&unstable->entries.items[i]);
    }
    if (remaining != 0) {
        memmove(unstable->entries.items,
                unstable->entries.items + remove_count,
                remaining * sizeof(*unstable->entries.items));
    }
    memset(unstable->entries.items + remaining,
           0,
           remove_count * sizeof(*unstable->entries.items));
    unstable->entries.len = remaining;
    unstable->offset = index + 1;
    if (unstable->offset_in_progress < unstable->offset) {
        unstable->offset_in_progress = unstable->offset;
    }
    if (remaining == 0) {
        free(unstable->entries.items);
        unstable->entries.items = NULL;
    }
}

void raft_unstable_stable_snap_to(raft_unstable_t *unstable, uint64_t index) {
    if (unstable == NULL || !unstable->has_snapshot ||
        unstable->snapshot.metadata.index != index) {
        return;
    }
    raft_snapshot_free(&unstable->snapshot);
    unstable->has_snapshot = false;
    unstable->snapshot_in_progress = false;
}

int raft_unstable_restore(raft_unstable_t *unstable,
                          const raft_snapshot_t *snapshot) {
    raft_snapshot_t copied;
    int result;

    if (unstable == NULL || !raft_snapshot_valid(snapshot) ||
        snapshot->metadata.index == UINT64_MAX) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    memset(&copied, 0, sizeof(copied));
    copied.data.is_nil = true;
    result = unstable_snapshot_copy(&copied, snapshot);
    if (result != RAFT_OK) {
        return result;
    }
    raft_entry_vec_free(&unstable->entries);
    raft_snapshot_free(&unstable->snapshot);
    unstable->snapshot = copied;
    unstable->has_snapshot = true;
    unstable->snapshot_in_progress = false;
    unstable->offset = snapshot->metadata.index + 1;
    unstable->offset_in_progress = unstable->offset;
    return RAFT_OK;
}

int raft_unstable_truncate_and_append(raft_unstable_t *unstable,
                                      const raft_entry_t *entries,
                                      size_t entry_count) {
    uint64_t from_index;
    uint64_t upper;
    size_t keep_count;
    raft_entry_vec_t replacement;
    size_t i;

    if (unstable == NULL || entry_count == 0 || entries == NULL) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    for (i = 0; i < entry_count; ++i) {
        if (!raft_entry_valid(&entries[i]) ||
            (i != 0 &&
             (entries[i - 1].index == UINT64_MAX ||
              entries[i].index != entries[i - 1].index + 1))) {
            return RAFT_ERR_INVALID_ARGUMENT;
        }
    }
    from_index = entries[0].index;
    if (unstable->entries.len > UINT64_MAX - unstable->offset) {
        return RAFT_ERR_FATAL;
    }
    upper = unstable->offset + (uint64_t)unstable->entries.len;
    if (from_index == upper) {
        keep_count = unstable->entries.len;
    } else if (from_index <= unstable->offset) {
        keep_count = 0;
    } else if (from_index < upper) {
        keep_count = (size_t)(from_index - unstable->offset);
    } else {
        return RAFT_ERR_FATAL;
    }
    if (keep_count > SIZE_MAX - entry_count ||
        keep_count + entry_count > SIZE_MAX / sizeof(*replacement.items)) {
        return RAFT_ERR_OUT_OF_MEMORY;
    }
    memset(&replacement, 0, sizeof(replacement));
    replacement.len = keep_count + entry_count;
    replacement.items = calloc(replacement.len, sizeof(*replacement.items));
    if (replacement.items == NULL) {
        return RAFT_ERR_OUT_OF_MEMORY;
    }
    for (i = 0; i < keep_count; ++i) {
        int result = unstable_entry_copy(
            &replacement.items[i], &unstable->entries.items[i]);
        if (result != RAFT_OK) {
            raft_entry_vec_free(&replacement);
            return result;
        }
    }
    for (i = 0; i < entry_count; ++i) {
        int result = unstable_entry_copy(
            &replacement.items[keep_count + i], &entries[i]);
        if (result != RAFT_OK) {
            raft_entry_vec_free(&replacement);
            return result;
        }
    }
    raft_entry_vec_free(&unstable->entries);
    unstable->entries = replacement;
    if (from_index <= unstable->offset) {
        unstable->offset = from_index;
        unstable->offset_in_progress = from_index;
    } else if (unstable->offset_in_progress > from_index) {
        unstable->offset_in_progress = from_index;
    }
    return RAFT_OK;
}
