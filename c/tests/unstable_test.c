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

#include <assert.h>
#include <stdlib.h>
#include <string.h>

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

static raft_snapshot_t make_snapshot(uint64_t index,
                                     uint64_t term,
                                     const uint8_t *data,
                                     size_t data_len) {
    raft_snapshot_t snapshot = {0};
    snapshot.metadata.index = index;
    snapshot.metadata.term = term;
    snapshot.data.is_nil = data == NULL;
    if (data_len != 0) {
        snapshot.data.data = malloc(data_len);
        assert(snapshot.data.data != NULL);
        memcpy(snapshot.data.data, data, data_len);
        snapshot.data.len = data_len;
        snapshot.data.is_nil = false;
    }
    return snapshot;
}

static void assert_terms(const raft_unstable_t *unstable,
                         uint64_t offset,
                         const uint64_t *terms,
                         size_t count) {
    assert(unstable->offset == offset);
    assert(unstable->entries.len == count);
    for (size_t i = 0; i < count; ++i) {
        assert(unstable->entries.items[i].index == offset + (uint64_t)i);
        assert(unstable->entries.items[i].term == terms[i]);
    }
}

static void test_indexes_and_terms(void) {
    const uint64_t terms[] = {2, 3};
    raft_unstable_t unstable;
    uint64_t value = 99;

    raft_unstable_init(&unstable, 5);
    assert(!raft_unstable_maybe_first_index(&unstable, &value));
    assert(!raft_unstable_maybe_last_index(&unstable, &value));

    unstable.snapshot = make_snapshot(4, 1, NULL, 0);
    unstable.has_snapshot = true;
    assert(raft_unstable_maybe_first_index(&unstable, &value));
    assert(value == 5);
    assert(raft_unstable_maybe_last_index(&unstable, &value));
    assert(value == 4);
    assert(raft_unstable_maybe_term(&unstable, 4, &value));
    assert(value == 1);

    unstable.entries = make_entries(5, terms, 2);
    assert(raft_unstable_maybe_last_index(&unstable, &value));
    assert(value == 6);
    assert(raft_unstable_maybe_term(&unstable, 5, &value));
    assert(value == 2);
    assert(raft_unstable_maybe_term(&unstable, 6, &value));
    assert(value == 3);
    assert(!raft_unstable_maybe_term(&unstable, 3, &value));
    assert(!raft_unstable_maybe_term(&unstable, 7, &value));

    raft_unstable_free(&unstable);
}

static void test_progress_and_stable(void) {
    const uint64_t terms[] = {1, 1, 2};
    raft_unstable_t unstable;
    raft_entry_vec_t next = {0};

    raft_unstable_init(&unstable, 5);
    unstable.entries = make_entries(5, terms, 3);
    assert(raft_unstable_next_entries(&unstable, &next) == RAFT_OK);
    assert(next.len == 3);
    raft_entry_vec_free(&next);

    raft_unstable_accept_in_progress(&unstable);
    assert(unstable.offset_in_progress == 8);
    assert(raft_unstable_next_entries(&unstable, &next) == RAFT_OK);
    assert(next.len == 0);
    raft_entry_vec_free(&next);

    raft_unstable_stable_to(&unstable, 5, 2);
    assert(unstable.offset == 5);
    assert(unstable.entries.len == 3);
    raft_unstable_stable_to(&unstable, 5, 1);
    assert(unstable.offset == 6);
    assert(unstable.offset_in_progress == 8);
    assert(unstable.entries.len == 2);
    raft_unstable_stable_to(&unstable, 7, 2);
    assert(unstable.offset == 8);
    assert(unstable.entries.len == 0);

    raft_unstable_free(&unstable);
}

static void test_snapshot_progress_restore_and_stable(void) {
    const uint8_t bytes[] = {1, 2, 3};
    const uint64_t terms[] = {1};
    raft_unstable_t unstable;
    raft_snapshot_t snapshot = make_snapshot(6, 2, bytes, sizeof(bytes));
    raft_snapshot_t next = {0};
    bool has_snapshot = false;

    raft_unstable_init(&unstable, 5);
    unstable.entries = make_entries(5, terms, 1);
    assert(raft_unstable_restore(&unstable, &snapshot) == RAFT_OK);
    assert(unstable.offset == 7);
    assert(unstable.offset_in_progress == 7);
    assert(unstable.entries.len == 0);
    assert(unstable.has_snapshot);

    snapshot.data.data[0] = 9;
    assert(unstable.snapshot.data.data[0] == 1);
    assert(raft_unstable_next_snapshot(
               &unstable, &has_snapshot, &next) == RAFT_OK);
    assert(has_snapshot);
    assert(next.data.data[0] == 1);
    raft_snapshot_free(&next);

    raft_unstable_accept_in_progress(&unstable);
    assert(unstable.snapshot_in_progress);
    assert(raft_unstable_next_snapshot(
               &unstable, &has_snapshot, &next) == RAFT_OK);
    assert(!has_snapshot);
    raft_snapshot_free(&next);

    raft_unstable_stable_snap_to(&unstable, 5);
    assert(unstable.has_snapshot);
    raft_unstable_stable_snap_to(&unstable, 6);
    assert(!unstable.has_snapshot);
    assert(!unstable.snapshot_in_progress);

    raft_snapshot_free(&snapshot);
    raft_unstable_free(&unstable);
}

static void test_truncate_and_append_and_ownership(void) {
    const uint64_t initial_terms[] = {1, 1, 1};
    const uint64_t direct_terms[] = {2, 2};
    const uint64_t direct_want[] = {1, 1, 1, 2, 2};
    const uint64_t replace_terms[] = {3, 3};
    const uint64_t truncate_terms[] = {4, 4};
    const uint64_t truncate_want[] = {3, 4, 4};
    raft_unstable_t unstable;
    raft_entry_vec_t input;
    uint8_t *payload;

    raft_unstable_init(&unstable, 5);
    unstable.entries = make_entries(5, initial_terms, 3);

    input = make_entries(8, direct_terms, 2);
    assert(raft_unstable_truncate_and_append(
               &unstable, input.items, input.len) == RAFT_OK);
    assert_terms(&unstable, 5, direct_want, 5);
    raft_entry_vec_free(&input);

    input = make_entries(4, replace_terms, 2);
    payload = malloc(2);
    assert(payload != NULL);
    payload[0] = 7;
    payload[1] = 8;
    input.items[0].data.data = payload;
    input.items[0].data.len = 2;
    input.items[0].data.is_nil = false;
    assert(raft_unstable_truncate_and_append(
               &unstable, input.items, input.len) == RAFT_OK);
    payload[0] = 9;
    assert(unstable.entries.items[0].data.data[0] == 7);
    assert_terms(&unstable, 4, replace_terms, 2);
    raft_entry_vec_free(&input);

    input = make_entries(5, truncate_terms, 2);
    assert(raft_unstable_truncate_and_append(
               &unstable, input.items, input.len) == RAFT_OK);
    assert_terms(&unstable, 4, truncate_want, 3);
    raft_entry_vec_free(&input);

    raft_unstable_free(&unstable);
}

int main(void) {
    test_indexes_and_terms();
    test_progress_and_stable();
    test_snapshot_progress_restore_and_stable();
    test_truncate_and_append_and_ownership();
    return 0;
}
