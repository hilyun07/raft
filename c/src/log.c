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

#include "log.h"

#include <stdlib.h>
#include <string.h>

static uint64_t log_varint_size(uint64_t value) {
    uint64_t size = 1;
    while (value >= 0x80) {
        value >>= 7;
        ++size;
    }
    return size;
}

uint64_t raft_log_entry_encoding_size(const raft_entry_t *entry) {
    uint64_t size = 0;

    if (entry == NULL) {
        return 0;
    }
    if (entry->type != RAFT_ENTRY_NORMAL) {
        size += 1 + log_varint_size((uint64_t)entry->type);
    }
    if (entry->term != 0) {
        size += 1 + log_varint_size(entry->term);
    }
    if (entry->index != 0) {
        size += 1 + log_varint_size(entry->index);
    }
    if (entry->data.len != 0) {
        uint64_t length = (uint64_t)entry->data.len;
        uint64_t overhead = 1 + log_varint_size(length);
        if (size > UINT64_MAX - overhead ||
            length > UINT64_MAX - size - overhead) {
            return UINT64_MAX;
        }
        size += overhead + length;
    }
    return size;
}

static uint64_t log_entries_size(const raft_entry_t *entries, size_t count) {
    uint64_t size = 0;
    size_t i;

    for (i = 0; i < count; ++i) {
        uint64_t entry_size = raft_log_entry_encoding_size(&entries[i]);
        if (UINT64_MAX - size < entry_size) {
            return UINT64_MAX;
        }
        size += entry_size;
    }
    return size;
}

static void log_limit_size(raft_entry_vec_t *entries, uint64_t max_size) {
    uint64_t size;
    size_t keep;

    if (entries == NULL || entries->len == 0) {
        return;
    }
    size = raft_log_entry_encoding_size(&entries->items[0]);
    keep = 1;
    while (keep < entries->len) {
        uint64_t entry_size =
            raft_log_entry_encoding_size(&entries->items[keep]);
        if (size > max_size || entry_size > max_size - size) {
            break;
        }
        size += entry_size;
        ++keep;
    }
    for (size_t i = keep; i < entries->len; ++i) {
        raft_entry_free(&entries->items[i]);
    }
    entries->len = keep;
}

static bool log_storage_valid(const raft_storage_ops_t *storage) {
    return storage != NULL && storage->entries != NULL &&
           storage->term != NULL && storage->first_index != NULL &&
           storage->last_index != NULL && storage->snapshot != NULL;
}

static int log_validate_owned_entries(const raft_entry_vec_t *entries) {
    size_t i;

    if (entries == NULL ||
        (entries->len != 0 && entries->items == NULL)) {
        return RAFT_ERR_FATAL;
    }
    for (i = 0; i < entries->len; ++i) {
        if (!raft_entry_valid(&entries->items[i])) {
            return RAFT_ERR_FATAL;
        }
        if (i != 0 &&
            (entries->items[i - 1].index == UINT64_MAX ||
             entries->items[i].index != entries->items[i - 1].index + 1)) {
            return RAFT_ERR_FATAL;
        }
    }
    return RAFT_OK;
}

static uint64_t log_max_appliable_index(const raft_log_t *log,
                                        bool allow_unstable) {
    uint64_t high = log->committed;
    if (!allow_unstable) {
        uint64_t stable =
            log->unstable.offset == 0 ? 0 : log->unstable.offset - 1;
        if (high > stable) {
            high = stable;
        }
    }
    return high;
}

int raft_log_init(raft_log_t *log,
                  const raft_storage_ops_t *storage,
                  uint64_t max_applying_entries_size) {
    uint64_t first_index;
    uint64_t last_index;
    int result;

    if (log == NULL || !log_storage_valid(storage)) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    memset(log, 0, sizeof(*log));
    log->storage = *storage;
    result = log->storage.first_index(log->storage.handle, &first_index);
    if (result != RAFT_OK) {
        memset(log, 0, sizeof(*log));
        return result;
    }
    result = log->storage.last_index(log->storage.handle, &last_index);
    if (result != RAFT_OK) {
        memset(log, 0, sizeof(*log));
        return result;
    }
    if (first_index == 0 || last_index == UINT64_MAX ||
        last_index + 1 < first_index) {
        memset(log, 0, sizeof(*log));
        return RAFT_ERR_FATAL;
    }
    raft_unstable_init(&log->unstable, last_index + 1);
    log->committed = first_index - 1;
    log->applying = first_index - 1;
    log->applied = first_index - 1;
    log->max_applying_entries_size = max_applying_entries_size;
    return RAFT_OK;
}

void raft_log_free(raft_log_t *log) {
    if (log == NULL) {
        return;
    }
    raft_unstable_free(&log->unstable);
    memset(log, 0, sizeof(*log));
}

int raft_log_first_index(raft_log_t *log, uint64_t *index) {
    if (log == NULL || index == NULL) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    if (raft_unstable_maybe_first_index(&log->unstable, index)) {
        return RAFT_OK;
    }
    return log->storage.first_index(log->storage.handle, index);
}

int raft_log_last_index(raft_log_t *log, uint64_t *index) {
    if (log == NULL || index == NULL) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    if (raft_unstable_maybe_last_index(&log->unstable, index)) {
        return RAFT_OK;
    }
    return log->storage.last_index(log->storage.handle, index);
}

int raft_log_term(raft_log_t *log, uint64_t index, uint64_t *term) {
    uint64_t first_index;
    uint64_t last_index;
    int result;

    if (log == NULL || term == NULL) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    *term = 0;
    if (raft_unstable_maybe_term(&log->unstable, index, term)) {
        return RAFT_OK;
    }
    result = raft_log_first_index(log, &first_index);
    if (result != RAFT_OK) {
        return result;
    }
    if (index < first_index - 1) {
        return RAFT_ERR_STORAGE_COMPACTED;
    }
    result = raft_log_last_index(log, &last_index);
    if (result != RAFT_OK) {
        return result;
    }
    if (index > last_index) {
        return RAFT_ERR_STORAGE_UNAVAILABLE;
    }
    result = log->storage.term(log->storage.handle, index, term);
    if (result != RAFT_OK) {
        *term = 0;
    }
    return result;
}

int raft_log_last_term(raft_log_t *log, uint64_t *term) {
    uint64_t index;
    int result;

    if (term == NULL) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    result = raft_log_last_index(log, &index);
    if (result != RAFT_OK) {
        return result;
    }
    return raft_log_term(log, index, term);
}

bool raft_log_match_term(raft_log_t *log, uint64_t index, uint64_t term) {
    uint64_t actual;
    return raft_log_term(log, index, &actual) == RAFT_OK &&
           actual == term;
}

int raft_log_zero_term_on_out_of_bounds(int result,
                                        uint64_t term,
                                        uint64_t *out) {
    if (out == NULL) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    if (result == RAFT_OK) {
        *out = term;
        return RAFT_OK;
    }
    if (result == RAFT_ERR_STORAGE_COMPACTED ||
        result == RAFT_ERR_STORAGE_UNAVAILABLE) {
        *out = 0;
        return RAFT_OK;
    }
    *out = 0;
    return result;
}

static int log_check_slice_bounds(raft_log_t *log,
                                  uint64_t lo,
                                  uint64_t hi) {
    uint64_t first_index;
    uint64_t last_index;
    int result;

    if (lo > hi) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    result = raft_log_first_index(log, &first_index);
    if (result != RAFT_OK) {
        return result;
    }
    if (lo < first_index) {
        return RAFT_ERR_STORAGE_COMPACTED;
    }
    result = raft_log_last_index(log, &last_index);
    if (result != RAFT_OK) {
        return result;
    }
    if (last_index == UINT64_MAX || hi > last_index + 1) {
        return RAFT_ERR_FATAL;
    }
    return RAFT_OK;
}

static int log_append_owned_vectors(raft_entry_vec_t *left,
                                    raft_entry_vec_t *right) {
    raft_entry_t *items;

    if (right->len == 0) {
        return RAFT_OK;
    }
    if (left->len > SIZE_MAX - right->len ||
        left->len + right->len > SIZE_MAX / sizeof(*items)) {
        return RAFT_ERR_OUT_OF_MEMORY;
    }
    items = realloc(left->items,
                    (left->len + right->len) * sizeof(*items));
    if (items == NULL) {
        return RAFT_ERR_OUT_OF_MEMORY;
    }
    left->items = items;
    memcpy(left->items + left->len,
           right->items,
           right->len * sizeof(*right->items));
    left->len += right->len;
    free(right->items);
    right->items = NULL;
    right->len = 0;
    return RAFT_OK;
}

int raft_log_slice(raft_log_t *log,
                   uint64_t lo,
                   uint64_t hi,
                   uint64_t max_size,
                   raft_entry_vec_t *out) {
    uint64_t cut;
    uint64_t stable_size;
    raft_entry_vec_t unstable_entries;
    int result;

    if (out == NULL) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    memset(out, 0, sizeof(*out));
    if (log == NULL) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    result = log_check_slice_bounds(log, lo, hi);
    if (result != RAFT_OK || lo == hi) {
        return result;
    }
    if (lo >= log->unstable.offset) {
        result = raft_unstable_slice(&log->unstable, lo, hi, out);
        if (result == RAFT_OK) {
            log_limit_size(out, max_size);
        }
        return result;
    }

    cut = hi < log->unstable.offset ? hi : log->unstable.offset;
    result = log->storage.entries(
        log->storage.handle, lo, cut, max_size, out);
    if (result != RAFT_OK) {
        raft_entry_vec_free(out);
        return result;
    }
    result = log_validate_owned_entries(out);
    if (result != RAFT_OK) {
        raft_entry_vec_free(out);
        return result;
    }
    if (out->len == 0 || (uint64_t)out->len > cut - lo ||
        out->items[0].index != lo) {
        raft_entry_vec_free(out);
        return RAFT_ERR_FATAL;
    }
    if (hi <= log->unstable.offset || out->len < (size_t)(cut - lo)) {
        return RAFT_OK;
    }
    stable_size = log_entries_size(out->items, out->len);
    if (stable_size >= max_size) {
        return RAFT_OK;
    }
    memset(&unstable_entries, 0, sizeof(unstable_entries));
    result = raft_unstable_slice(
        &log->unstable, log->unstable.offset, hi, &unstable_entries);
    if (result != RAFT_OK) {
        raft_entry_vec_free(out);
        return result;
    }
    log_limit_size(&unstable_entries, max_size - stable_size);
    if (unstable_entries.len == 1 &&
        raft_log_entry_encoding_size(&unstable_entries.items[0]) >
            max_size - stable_size) {
        raft_entry_vec_free(&unstable_entries);
        return RAFT_OK;
    }
    result = log_append_owned_vectors(out, &unstable_entries);
    if (result != RAFT_OK) {
        raft_entry_vec_free(&unstable_entries);
        raft_entry_vec_free(out);
    }
    return result;
}

int raft_log_entries(raft_log_t *log,
                     uint64_t from,
                     uint64_t max_size,
                     raft_entry_vec_t *out) {
    uint64_t last_index;
    int result;

    if (out == NULL) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    memset(out, 0, sizeof(*out));
    result = raft_log_last_index(log, &last_index);
    if (result != RAFT_OK) {
        return result;
    }
    if (from > last_index) {
        return RAFT_OK;
    }
    return raft_log_slice(log, from, last_index + 1, max_size, out);
}

int raft_log_scan(raft_log_t *log,
                  uint64_t lo,
                  uint64_t hi,
                  uint64_t page_size,
                  raft_log_scan_fn visit,
                  void *context) {
    if (log == NULL || visit == NULL) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    while (lo < hi) {
        raft_entry_vec_t entries;
        int result = raft_log_slice(log, lo, hi, page_size, &entries);
        if (result != RAFT_OK) {
            return result;
        }
        if (entries.len == 0) {
            raft_entry_vec_free(&entries);
            return RAFT_ERR_FATAL;
        }
        result = visit(context, entries.items, entries.len);
        lo += (uint64_t)entries.len;
        raft_entry_vec_free(&entries);
        if (result != RAFT_OK) {
            return result;
        }
    }
    return RAFT_OK;
}

int raft_log_append(raft_log_t *log,
                    const raft_entry_t *entries,
                    size_t entry_count,
                    uint64_t *last_index) {
    uint64_t current_last;
    int result;

    if (log == NULL || last_index == NULL ||
        (entry_count != 0 && entries == NULL)) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    if (entry_count == 0) {
        return raft_log_last_index(log, last_index);
    }
    if (entries[0].index == 0 ||
        entries[0].index - 1 < log->committed) {
        return RAFT_ERR_FATAL;
    }
    result = raft_unstable_truncate_and_append(
        &log->unstable, entries, entry_count);
    if (result != RAFT_OK) {
        return result;
    }
    result = raft_log_last_index(log, &current_last);
    if (result == RAFT_OK) {
        *last_index = current_last;
    }
    return result;
}

int raft_log_find_conflict(raft_log_t *log,
                           const raft_entry_t *entries,
                           size_t entry_count,
                           uint64_t *conflict_index) {
    size_t i;

    if (log == NULL || conflict_index == NULL ||
        (entry_count != 0 && entries == NULL)) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    *conflict_index = 0;
    for (i = 0; i < entry_count; ++i) {
        if (!raft_entry_valid(&entries[i]) ||
            (i != 0 &&
             (entries[i - 1].index == UINT64_MAX ||
              entries[i].index != entries[i - 1].index + 1))) {
            return RAFT_ERR_INVALID_ARGUMENT;
        }
        if (!raft_log_match_term(log, entries[i].index, entries[i].term)) {
            *conflict_index = entries[i].index;
            return RAFT_OK;
        }
    }
    return RAFT_OK;
}

int raft_log_find_conflict_by_term(raft_log_t *log,
                                   uint64_t index,
                                   uint64_t term,
                                   uint64_t *conflict_index,
                                   uint64_t *conflict_term) {
    if (log == NULL || conflict_index == NULL || conflict_term == NULL) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    for (;;) {
        uint64_t our_term;
        int result;
        if (index == 0) {
            *conflict_index = 0;
            *conflict_term = 0;
            return RAFT_OK;
        }
        result = raft_log_term(log, index, &our_term);
        if (result != RAFT_OK) {
            *conflict_index = index;
            *conflict_term = 0;
            return RAFT_OK;
        }
        if (our_term <= term) {
            *conflict_index = index;
            *conflict_term = our_term;
            return RAFT_OK;
        }
        --index;
    }
}

int raft_log_maybe_append(raft_log_t *log,
                          uint64_t previous_index,
                          uint64_t previous_term,
                          uint64_t leader_committed,
                          const raft_entry_t *entries,
                          size_t entry_count,
                          bool *appended,
                          uint64_t *last_new_index) {
    uint64_t conflict;
    int result;

    if (log == NULL || appended == NULL || last_new_index == NULL ||
        (entry_count != 0 && entries == NULL) ||
        entry_count > UINT64_MAX - previous_index) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    *appended = false;
    *last_new_index = 0;
    if (!raft_log_match_term(log, previous_index, previous_term)) {
        return RAFT_OK;
    }
    if (entry_count != 0 &&
        (previous_index == UINT64_MAX ||
         entries[0].index != previous_index + 1)) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    *last_new_index = previous_index + (uint64_t)entry_count;
    result = raft_log_find_conflict(
        log, entries, entry_count, &conflict);
    if (result != RAFT_OK) {
        return result;
    }
    if (conflict != 0) {
        uint64_t offset = previous_index + 1;
        size_t start;
        uint64_t ignored;
        if (conflict <= log->committed || conflict < offset ||
            conflict - offset > entry_count) {
            return RAFT_ERR_FATAL;
        }
        start = (size_t)(conflict - offset);
        result = raft_log_append(
            log, entries + start, entry_count - start, &ignored);
        if (result != RAFT_OK) {
            return result;
        }
    }
    result = raft_log_commit_to(
        log,
        leader_committed < *last_new_index
            ? leader_committed
            : *last_new_index);
    if (result != RAFT_OK) {
        return result;
    }
    *appended = true;
    return RAFT_OK;
}

int raft_log_commit_to(raft_log_t *log, uint64_t to_commit) {
    uint64_t last_index;
    int result;

    if (log == NULL) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    if (to_commit <= log->committed) {
        return RAFT_OK;
    }
    result = raft_log_last_index(log, &last_index);
    if (result != RAFT_OK) {
        return result;
    }
    if (to_commit > last_index) {
        return RAFT_ERR_FATAL;
    }
    log->committed = to_commit;
    return RAFT_OK;
}

int raft_log_maybe_commit(raft_log_t *log,
                          uint64_t index,
                          uint64_t term,
                          bool *committed) {
    if (log == NULL || committed == NULL) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    *committed = false;
    if (term != 0 && index > log->committed &&
        raft_log_match_term(log, index, term)) {
        int result = raft_log_commit_to(log, index);
        if (result != RAFT_OK) {
            return result;
        }
        *committed = true;
    }
    return RAFT_OK;
}

int raft_log_applied_to(raft_log_t *log, uint64_t index, uint64_t size) {
    if (log == NULL) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    if (index > log->committed || index < log->applied) {
        return RAFT_ERR_FATAL;
    }
    log->applied = index;
    if (log->applying < index) {
        log->applying = index;
    }
    if (log->applying_entries_size > size) {
        log->applying_entries_size -= size;
    } else {
        log->applying_entries_size = 0;
    }
    log->applying_entries_paused =
        log->applying_entries_size >= log->max_applying_entries_size;
    return RAFT_OK;
}

int raft_log_accept_applying(raft_log_t *log,
                             uint64_t index,
                             uint64_t size,
                             bool allow_unstable) {
    if (log == NULL) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    if (index > log->committed || index < log->applied ||
        index < log->applying ||
        UINT64_MAX - log->applying_entries_size < size) {
        return RAFT_ERR_FATAL;
    }
    log->applying = index;
    log->applying_entries_size += size;
    log->applying_entries_paused =
        log->applying_entries_size >= log->max_applying_entries_size ||
        index < log_max_appliable_index(log, allow_unstable);
    return RAFT_OK;
}

bool raft_log_has_next_committed_entries(const raft_log_t *log,
                                         bool allow_unstable) {
    uint64_t high;
    if (log == NULL || log->applying_entries_paused ||
        log->unstable.has_snapshot ||
        log->applying == UINT64_MAX) {
        return false;
    }
    high = log_max_appliable_index(log, allow_unstable);
    return log->applying + 1 <= high;
}

int raft_log_next_committed_entries(raft_log_t *log,
                                    bool allow_unstable,
                                    raft_entry_vec_t *out) {
    uint64_t high;
    uint64_t max_size;

    if (log == NULL || out == NULL) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    memset(out, 0, sizeof(*out));
    if (!raft_log_has_next_committed_entries(log, allow_unstable)) {
        return RAFT_OK;
    }
    if (log->applying_entries_size >= log->max_applying_entries_size) {
        return RAFT_ERR_FATAL;
    }
    high = log_max_appliable_index(log, allow_unstable);
    max_size =
        log->max_applying_entries_size - log->applying_entries_size;
    return raft_log_slice(log, log->applying + 1, high + 1, max_size, out);
}

bool raft_log_has_next_unstable_entries(const raft_log_t *log) {
    if (log == NULL ||
        log->unstable.offset_in_progress < log->unstable.offset) {
        return false;
    }
    return log->unstable.offset_in_progress - log->unstable.offset <
           log->unstable.entries.len;
}

bool raft_log_has_next_or_in_progress_unstable_entries(const raft_log_t *log) {
    return log != NULL && log->unstable.entries.len != 0;
}

int raft_log_next_unstable_entries(const raft_log_t *log,
                                   raft_entry_vec_t *out) {
    if (log == NULL) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    return raft_unstable_next_entries(&log->unstable, out);
}

bool raft_log_has_next_unstable_snapshot(const raft_log_t *log) {
    return log != NULL && log->unstable.has_snapshot &&
           !log->unstable.snapshot_in_progress;
}

bool raft_log_has_next_or_in_progress_snapshot(const raft_log_t *log) {
    return log != NULL && log->unstable.has_snapshot;
}

int raft_log_next_unstable_snapshot(const raft_log_t *log,
                                    bool *has_snapshot,
                                    raft_snapshot_t *out) {
    if (log == NULL) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    return raft_unstable_next_snapshot(
        &log->unstable, has_snapshot, out);
}

int raft_log_snapshot(raft_log_t *log, raft_snapshot_t *out) {
    bool has_snapshot;
    int result;

    if (log == NULL || out == NULL) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    result = raft_unstable_snapshot(
        &log->unstable, &has_snapshot, out);
    if (result != RAFT_OK) {
        return result;
    }
    if (has_snapshot) {
        return RAFT_OK;
    }
    memset(out, 0, sizeof(*out));
    result = log->storage.snapshot(log->storage.handle, out);
    if (result != RAFT_OK) {
        raft_snapshot_free(out);
        return result;
    }
    if (!raft_snapshot_valid(out)) {
        raft_snapshot_free(out);
        return RAFT_ERR_FATAL;
    }
    return RAFT_OK;
}

void raft_log_accept_unstable(raft_log_t *log) {
    if (log != NULL) {
        raft_unstable_accept_in_progress(&log->unstable);
    }
}

void raft_log_stable_to(raft_log_t *log, uint64_t index, uint64_t term) {
    if (log != NULL) {
        raft_unstable_stable_to(&log->unstable, index, term);
    }
}

void raft_log_stable_snap_to(raft_log_t *log, uint64_t index) {
    if (log != NULL) {
        raft_unstable_stable_snap_to(&log->unstable, index);
    }
}

int raft_log_restore(raft_log_t *log, const raft_snapshot_t *snapshot) {
    int result;
    if (log == NULL || snapshot == NULL) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    result = raft_unstable_restore(&log->unstable, snapshot);
    if (result == RAFT_OK) {
        log->committed = snapshot->metadata.index;
    }
    return result;
}

bool raft_log_is_up_to_date(raft_log_t *log,
                            uint64_t candidate_index,
                            uint64_t candidate_term) {
    uint64_t last_index;
    uint64_t last_term;
    if (raft_log_last_index(log, &last_index) != RAFT_OK ||
        raft_log_term(log, last_index, &last_term) != RAFT_OK) {
        return false;
    }
    return candidate_term > last_term ||
           (candidate_term == last_term &&
            candidate_index >= last_index);
}
