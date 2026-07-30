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

#include <assert.h>
#include <stdlib.h>
#include <string.h>

typedef struct fake_storage {
    uint64_t snapshot_index;
    uint64_t snapshot_term;
    raft_entry_vec_t entries;
    int entries_error;
    int term_error;
    int snapshot_error;
    size_t entries_calls;
} fake_storage_t;

static raft_entry_vec_t make_entries(uint64_t offset,
                                     const uint64_t *terms,
                                     size_t count) {
    raft_entry_vec_t entries = {0};
    entries.items = calloc(count, sizeof(*entries.items));
    assert(count == 0 || entries.items != NULL);
    entries.len = count;
    for (size_t i = 0; i < count; ++i) {
        entries.items[i].type = RAFT_ENTRY_NORMAL;
        entries.items[i].index = offset + (uint64_t)i;
        entries.items[i].term = terms[i];
        entries.items[i].data.is_nil = true;
    }
    return entries;
}

static int copy_entry(raft_entry_t *dst, const raft_entry_t *src) {
    int result;

    memset(dst, 0, sizeof(*dst));
    dst->data.is_nil = true;
    dst->type = src->type;
    dst->term = src->term;
    dst->index = src->index;
    dst->protobuf.fields = src->protobuf.fields;
    result = raft_bytes_copy(&dst->data, &src->data);
    if (result == RAFT_OK) {
        result = raft_bytes_copy(
            &dst->protobuf.unknown_fields,
            &src->protobuf.unknown_fields);
    }
    if (result != RAFT_OK) {
        raft_entry_free(dst);
    }
    return result;
}

static uint64_t storage_first(const fake_storage_t *storage) {
    if (storage->entries.len != 0) {
        return storage->entries.items[0].index;
    }
    return storage->snapshot_index + 1;
}

static uint64_t storage_last(const fake_storage_t *storage) {
    if (storage->entries.len != 0) {
        return storage->entries.items[storage->entries.len - 1].index;
    }
    return storage->snapshot_index;
}

static int fake_initial_state(uintptr_t handle,
                              raft_hard_state_t *hard_state,
                              raft_conf_state_t *conf_state) {
    (void)handle;
    memset(hard_state, 0, sizeof(*hard_state));
    memset(conf_state, 0, sizeof(*conf_state));
    return RAFT_OK;
}

static int fake_entries(uintptr_t handle,
                        uint64_t lo,
                        uint64_t hi,
                        uint64_t max_size,
                        raft_entry_vec_t *out) {
    fake_storage_t *storage = (fake_storage_t *)handle;
    uint64_t first = storage_first(storage);
    uint64_t last = storage_last(storage);
    size_t count;
    uint64_t size = 0;

    memset(out, 0, sizeof(*out));
    ++storage->entries_calls;
    if (storage->entries_error != RAFT_OK) {
        return storage->entries_error;
    }
    if (lo < first) {
        return RAFT_ERR_STORAGE_COMPACTED;
    }
    if (lo > hi || hi > last + 1) {
        return RAFT_ERR_STORAGE_UNAVAILABLE;
    }
    count = (size_t)(hi - lo);
    if (count == 0) {
        return RAFT_OK;
    }
    out->items = calloc(count, sizeof(*out->items));
    if (out->items == NULL) {
        return RAFT_ERR_OUT_OF_MEMORY;
    }
    for (size_t i = 0; i < count; ++i) {
        const raft_entry_t *src =
            &storage->entries.items[(size_t)(lo - first) + i];
        uint64_t entry_size = raft_log_entry_encoding_size(src);
        if (i != 0 && (size > max_size ||
                       entry_size > max_size - size)) {
            break;
        }
        assert(copy_entry(&out->items[out->len], src) == RAFT_OK);
        ++out->len;
        size += entry_size;
    }
    if (out->len == 0) {
        free(out->items);
        out->items = NULL;
    }
    return RAFT_OK;
}

static int fake_term(uintptr_t handle, uint64_t index, uint64_t *term) {
    fake_storage_t *storage = (fake_storage_t *)handle;
    uint64_t first = storage_first(storage);
    uint64_t last = storage_last(storage);

    *term = 0;
    if (storage->term_error != RAFT_OK) {
        return storage->term_error;
    }
    if (index == storage->snapshot_index) {
        *term = storage->snapshot_term;
        return RAFT_OK;
    }
    if (index < first) {
        return RAFT_ERR_STORAGE_COMPACTED;
    }
    if (index > last) {
        return RAFT_ERR_STORAGE_UNAVAILABLE;
    }
    *term = storage->entries.items[index - first].term;
    return RAFT_OK;
}

static int fake_first_index(uintptr_t handle, uint64_t *index) {
    *index = storage_first((fake_storage_t *)handle);
    return RAFT_OK;
}

static int fake_last_index(uintptr_t handle, uint64_t *index) {
    *index = storage_last((fake_storage_t *)handle);
    return RAFT_OK;
}

static int fake_snapshot(uintptr_t handle, raft_snapshot_t *snapshot) {
    fake_storage_t *storage = (fake_storage_t *)handle;
    memset(snapshot, 0, sizeof(*snapshot));
    snapshot->data.is_nil = true;
    if (storage->snapshot_error != RAFT_OK) {
        return storage->snapshot_error;
    }
    snapshot->metadata.index = storage->snapshot_index;
    snapshot->metadata.term = storage->snapshot_term;
    return RAFT_OK;
}

static raft_storage_ops_t storage_ops(fake_storage_t *storage) {
    raft_storage_ops_t ops = {
        .handle = (uintptr_t)storage,
        .initial_state = fake_initial_state,
        .entries = fake_entries,
        .term = fake_term,
        .first_index = fake_first_index,
        .last_index = fake_last_index,
        .snapshot = fake_snapshot,
    };
    return ops;
}

static void storage_init(fake_storage_t *storage,
                         uint64_t snapshot_index,
                         uint64_t snapshot_term,
                         uint64_t entry_offset,
                         const uint64_t *terms,
                         size_t count) {
    memset(storage, 0, sizeof(*storage));
    storage->snapshot_index = snapshot_index;
    storage->snapshot_term = snapshot_term;
    storage->entries = make_entries(entry_offset, terms, count);
}

static void storage_free(fake_storage_t *storage) {
    raft_entry_vec_free(&storage->entries);
    memset(storage, 0, sizeof(*storage));
}

static void assert_entry_range(const raft_entry_vec_t *entries,
                               uint64_t offset,
                               const uint64_t *terms,
                               size_t count) {
    assert(entries->len == count);
    for (size_t i = 0; i < count; ++i) {
        assert(entries->items[i].index == offset + (uint64_t)i);
        assert(entries->items[i].term == terms[i]);
    }
}

typedef struct scan_result {
    uint64_t next_index;
    size_t count;
} scan_result_t;

static int collect_scan(void *context,
                        const raft_entry_t *entries,
                        size_t entry_count) {
    scan_result_t *result = context;

    assert(entry_count != 0);
    for (size_t i = 0; i < entry_count; ++i) {
        assert(entries[i].index == result->next_index);
        ++result->next_index;
        ++result->count;
    }
    return RAFT_OK;
}

static void test_constructor_indexes_terms_and_snapshot(void) {
    const uint64_t stable_terms[] = {2, 2};
    const uint64_t unstable_terms[] = {3, 3};
    fake_storage_t storage;
    raft_storage_ops_t ops;
    raft_log_t log;
    raft_entry_vec_t append;
    raft_snapshot_t snapshot = {0};
    uint64_t value;

    storage_init(&storage, 3, 1, 4, stable_terms, 2);
    ops = storage_ops(&storage);
    assert(raft_log_init(&log, &ops, UINT64_MAX) == RAFT_OK);
    assert(log.committed == 3 && log.applied == 3 && log.applying == 3);
    assert(log.unstable.offset == 6);
    assert(raft_log_first_index(&log, &value) == RAFT_OK && value == 4);
    assert(raft_log_last_index(&log, &value) == RAFT_OK && value == 5);
    assert(raft_log_term(&log, 3, &value) == RAFT_OK && value == 1);
    assert(raft_log_term(&log, 4, &value) == RAFT_OK && value == 2);

    append = make_entries(6, unstable_terms, 2);
    assert(raft_log_append(&log, append.items, append.len, &value) ==
           RAFT_OK);
    assert(value == 7);
    assert(raft_log_last_index(&log, &value) == RAFT_OK && value == 7);
    assert(raft_log_term(&log, 6, &value) == RAFT_OK && value == 3);
    assert(raft_log_term(&log, 8, &value) ==
           RAFT_ERR_STORAGE_UNAVAILABLE);
    assert(raft_log_term(&log, 1, &value) ==
           RAFT_ERR_STORAGE_COMPACTED);

    assert(raft_log_snapshot(&log, &snapshot) == RAFT_OK);
    assert(snapshot.metadata.index == 3);
    assert(snapshot.metadata.term == 1);
    raft_snapshot_free(&snapshot);

    raft_entry_vec_free(&append);
    raft_log_free(&log);
    storage_free(&storage);
}

static void test_slice_across_storage_and_unstable(void) {
    const uint64_t stable_terms[] = {1, 1};
    const uint64_t unstable_terms[] = {2, 2};
    const uint64_t want_terms[] = {1, 1, 2, 2};
    fake_storage_t storage;
    raft_storage_ops_t ops;
    raft_log_t log;
    raft_entry_vec_t append;
    raft_entry_vec_t slice = {0};
    scan_result_t scan = {.next_index = 3};
    uint64_t last;

    storage_init(&storage, 2, 1, 3, stable_terms, 2);
    ops = storage_ops(&storage);
    assert(raft_log_init(&log, &ops, UINT64_MAX) == RAFT_OK);
    append = make_entries(5, unstable_terms, 2);
    assert(raft_log_append(&log, append.items, append.len, &last) ==
           RAFT_OK);

    assert(raft_log_slice(&log, 3, 7, UINT64_MAX, &slice) == RAFT_OK);
    assert_entry_range(&slice, 3, want_terms, 4);
    assert(storage.entries_calls == 1);
    raft_entry_vec_free(&slice);

    assert(raft_log_slice(&log, 3, 7, 0, &slice) == RAFT_OK);
    assert(slice.len == 1 && slice.items[0].index == 3);
    assert(storage.entries_calls == 2);
    raft_entry_vec_free(&slice);

    assert(raft_log_slice(&log, 5, 7, UINT64_MAX, &slice) == RAFT_OK);
    assert_entry_range(&slice, 5, unstable_terms, 2);
    assert(storage.entries_calls == 2);
    raft_entry_vec_free(&slice);

    assert(raft_log_slice(&log, 1, 3, UINT64_MAX, &slice) ==
           RAFT_ERR_STORAGE_COMPACTED);
    raft_entry_vec_free(&slice);

    assert(raft_log_scan(
               &log, 3, 7, UINT64_MAX, collect_scan, &scan) == RAFT_OK);
    assert(scan.count == 4 && scan.next_index == 7);

    raft_entry_vec_free(&append);
    raft_log_free(&log);
    storage_free(&storage);
}

static void test_append_conflict_and_maybe_append(void) {
    const uint64_t base_terms[] = {1, 2, 3};
    const uint64_t incoming_terms[] = {2, 4, 4};
    const uint64_t want_terms[] = {1, 2, 4, 4};
    fake_storage_t storage;
    raft_storage_ops_t ops;
    raft_log_t log;
    raft_entry_vec_t base;
    raft_entry_vec_t incoming;
    raft_entry_vec_t all = {0};
    uint64_t value;
    uint64_t conflict_term;
    bool appended;
    bool up_to_date;

    storage_init(&storage, 0, 0, 1, NULL, 0);
    ops = storage_ops(&storage);
    assert(raft_log_init(&log, &ops, UINT64_MAX) == RAFT_OK);
    base = make_entries(1, base_terms, 3);
    assert(raft_log_append(&log, base.items, base.len, &value) == RAFT_OK);
    incoming = make_entries(2, incoming_terms, 3);
    assert(raft_log_find_conflict(
               &log, incoming.items, incoming.len, &value) == RAFT_OK);
    assert(value == 3);
    assert(raft_log_find_conflict_by_term(
               &log, 3, 2, &value, &conflict_term) == RAFT_OK);
    assert(value == 2 && conflict_term == 2);
    assert(raft_log_is_up_to_date(
               &log, 3, 3, &up_to_date) == RAFT_OK);
    assert(up_to_date);
    assert(raft_log_is_up_to_date(
               &log, 2, 3, &up_to_date) == RAFT_OK);
    assert(!up_to_date);
    assert(raft_log_is_up_to_date(
               &log, 1, 4, &up_to_date) == RAFT_OK);
    assert(up_to_date);
    assert(raft_log_zero_term_on_out_of_bounds(
               RAFT_ERR_STORAGE_UNAVAILABLE, 9, &value) == RAFT_OK);
    assert(value == 0);
    assert(raft_log_zero_term_on_out_of_bounds(
               RAFT_ERR_FATAL, 9, &value) == RAFT_ERR_FATAL);
    assert(value == 0);

    assert(raft_log_maybe_append(
               &log, 1, 1, 4, incoming.items, incoming.len,
               &appended, &value) == RAFT_OK);
    assert(appended && value == 4 && log.committed == 4);
    assert(raft_log_entries(&log, 1, UINT64_MAX, &all) == RAFT_OK);
    assert_entry_range(&all, 1, want_terms, 4);
    raft_entry_vec_free(&all);

    assert(raft_log_maybe_append(
               &log, 1, 9, 4, incoming.items, incoming.len,
               &appended, &value) == RAFT_OK);
    assert(!appended);

    raft_entry_vec_free(&incoming);
    raft_entry_vec_free(&base);
    raft_log_free(&log);
    storage_free(&storage);
}

static void test_commit_apply_and_ready_progress(void) {
    const uint64_t stable_terms[] = {1};
    const uint64_t unstable_terms[] = {1, 1};
    const uint64_t want_all[] = {1, 1};
    fake_storage_t storage;
    raft_storage_ops_t ops;
    raft_log_t log;
    raft_entry_vec_t append;
    raft_entry_vec_t entries = {0};
    uint64_t last;
    bool committed;

    storage_init(&storage, 3, 1, 4, stable_terms, 1);
    ops = storage_ops(&storage);
    assert(raft_log_init(&log, &ops, 100) == RAFT_OK);
    append = make_entries(5, unstable_terms, 2);
    assert(raft_log_append(&log, append.items, append.len, &last) ==
           RAFT_OK);
    assert(raft_log_maybe_commit(&log, 5, 1, &committed) == RAFT_OK);
    assert(committed && log.committed == 5);
    assert(raft_log_commit_to(&log, 4) == RAFT_OK);
    assert(log.committed == 5);
    assert(raft_log_commit_to(&log, 8) == RAFT_ERR_FATAL);

    assert(raft_log_has_next_committed_entries(&log, false));
    assert(raft_log_next_committed_entries(&log, false, &entries) ==
           RAFT_OK);
    assert(entries.len == 1 && entries.items[0].index == 4);
    raft_entry_vec_free(&entries);

    assert(raft_log_has_next_committed_entries(&log, true));
    assert(raft_log_next_committed_entries(&log, true, &entries) ==
           RAFT_OK);
    assert_entry_range(&entries, 4, want_all, 2);
    raft_entry_vec_free(&entries);

    assert(raft_log_accept_applying(&log, 5, 50, true) == RAFT_OK);
    assert(log.applying == 5 && !log.applying_entries_paused);
    assert(raft_log_applied_to(&log, 4, 20) == RAFT_OK);
    assert(log.applied == 4 && log.applying == 5);
    assert(log.applying_entries_size == 30);
    assert(raft_log_applied_to(&log, 6, 0) == RAFT_ERR_FATAL);

    assert(raft_log_has_next_unstable_entries(&log));
    assert(raft_log_next_unstable_entries(&log, &entries) == RAFT_OK);
    assert(entries.len == 2);
    raft_entry_vec_free(&entries);
    raft_log_accept_unstable(&log);
    assert(!raft_log_has_next_unstable_entries(&log));
    assert(raft_log_has_next_or_in_progress_unstable_entries(&log));
    raft_log_stable_to(&log, 5, 1);
    assert(log.unstable.offset == 6);
    assert(log.unstable.entries.len == 1);

    raft_entry_vec_free(&append);
    raft_log_free(&log);
    storage_free(&storage);
}

static void test_restore_snapshot_and_errors(void) {
    const uint64_t terms[] = {1};
    const uint8_t data[] = {1, 2};
    fake_storage_t storage;
    raft_storage_ops_t ops;
    raft_log_t log;
    raft_snapshot_t snapshot = {0};
    raft_snapshot_t next = {0};
    bool has_snapshot;
    uint64_t value;

    storage_init(&storage, 0, 0, 1, terms, 1);
    ops = storage_ops(&storage);
    assert(raft_log_init(&log, &ops, UINT64_MAX) == RAFT_OK);
    snapshot.metadata.index = 8;
    snapshot.metadata.term = 4;
    snapshot.data.data = malloc(sizeof(data));
    assert(snapshot.data.data != NULL);
    memcpy(snapshot.data.data, data, sizeof(data));
    snapshot.data.len = sizeof(data);
    snapshot.data.is_nil = false;

    assert(raft_log_restore(&log, &snapshot) == RAFT_OK);
    snapshot.data.data[0] = 9;
    assert(log.committed == 8);
    assert(raft_log_first_index(&log, &value) == RAFT_OK && value == 9);
    assert(raft_log_last_index(&log, &value) == RAFT_OK && value == 8);
    assert(raft_log_term(&log, 8, &value) == RAFT_OK && value == 4);
    assert(raft_log_has_next_unstable_snapshot(&log));
    assert(raft_log_next_unstable_snapshot(
               &log, &has_snapshot, &next) == RAFT_OK);
    assert(has_snapshot && next.data.data[0] == 1);
    raft_snapshot_free(&next);
    raft_log_accept_unstable(&log);
    assert(!raft_log_has_next_unstable_snapshot(&log));
    assert(raft_log_has_next_or_in_progress_snapshot(&log));
    raft_log_stable_snap_to(&log, 8);
    assert(!raft_log_has_next_or_in_progress_snapshot(&log));

    storage.term_error = RAFT_ERR_STORAGE_UNAVAILABLE;
    assert(raft_log_term(&log, 1, &value) ==
           RAFT_ERR_STORAGE_UNAVAILABLE);
    storage.term_error = RAFT_OK;
    storage.entries_error = RAFT_ERR_STORAGE_COMPACTED;
    {
        raft_entry_vec_t out = {0};
        assert(raft_log_slice(&log, 1, 2, UINT64_MAX, &out) ==
               RAFT_ERR_STORAGE_COMPACTED);
        raft_entry_vec_free(&out);
    }
    storage.snapshot_error = RAFT_ERR_SNAPSHOT_TEMPORARILY_UNAVAILABLE;
    assert(raft_log_snapshot(&log, &next) ==
           RAFT_ERR_SNAPSHOT_TEMPORARILY_UNAVAILABLE);
    raft_snapshot_free(&next);

    raft_snapshot_free(&snapshot);
    raft_log_free(&log);
    storage_free(&storage);
}

static void test_storage_error_classification(void) {
    const uint64_t terms[] = {1};
    fake_storage_t storage;
    raft_storage_ops_t ops;
    raft_log_t log;
    raft_entry_vec_t out = {0};
    bool value;

    storage_init(&storage, 0, 0, 1, terms, 1);
    ops = storage_ops(&storage);
    assert(raft_log_init(&log, &ops, UINT64_MAX) == RAFT_OK);

    storage.term_error = RAFT_ERR_STORAGE_COMPACTED;
    assert(raft_log_match_term(&log, 1, 1, &value) == RAFT_OK);
    assert(!value);
    storage.term_error = RAFT_ERR_STORAGE_UNAVAILABLE;
    assert(raft_log_match_term(&log, 1, 1, &value) == RAFT_OK);
    assert(!value);
    storage.term_error = RAFT_ERR_FATAL;
    assert(raft_log_match_term(&log, 1, 1, &value) == RAFT_ERR_FATAL);
    assert(!value);
    assert(raft_log_is_up_to_date(
               &log, 1, 1, &value) == RAFT_ERR_FATAL);
    assert(!value);

    storage.term_error = RAFT_OK;
    storage.entries_error = RAFT_ERR_STORAGE_UNAVAILABLE;
    assert(raft_log_slice(&log, 1, 2, UINT64_MAX, &out) ==
           RAFT_ERR_FATAL);
    raft_entry_vec_free(&out);
    storage.entries_error = RAFT_ERR_STORAGE_COMPACTED;
    assert(raft_log_slice(&log, 1, 2, UINT64_MAX, &out) ==
           RAFT_ERR_STORAGE_COMPACTED);
    raft_entry_vec_free(&out);

    raft_log_free(&log);
    storage_free(&storage);
}

static void test_protobuf_entry_encoding_size(void) {
    uint8_t unknown[] = {0xa0, 0x06, 0x07};
    raft_entry_t entry = {
        .type = RAFT_ENTRY_NORMAL,
        .data = {NULL, 0, true},
        .protobuf = {
            .unknown_fields = {NULL, 0, true},
        },
    };

    assert(raft_log_entry_encoding_size(&entry) == 0);
    entry.protobuf.fields =
        RAFT_ENTRY_PROTO_TYPE | RAFT_ENTRY_PROTO_TERM |
        RAFT_ENTRY_PROTO_INDEX;
    entry.data.is_nil = false;
    entry.protobuf.unknown_fields =
        (raft_bytes_t){unknown, sizeof(unknown), false};
    // Three present-zero scalar fields, present-empty Data, and three raw
    // unknown bytes.
    assert(raft_log_entry_encoding_size(&entry) == 11);

    entry.protobuf.fields = 0;
    entry.protobuf.unknown_fields =
        (raft_bytes_t){NULL, 0, true};
    entry.data.is_nil = true;
    entry.term = 128;
    assert(raft_log_entry_encoding_size(&entry) == 3);
}

int main(void) {
    test_constructor_indexes_terms_and_snapshot();
    test_slice_across_storage_and_unstable();
    test_append_conflict_and_maybe_append();
    test_commit_apply_and_ready_progress();
    test_restore_snapshot_and_errors();
    test_storage_error_classification();
    test_protobuf_entry_encoding_size();
    return 0;
}
