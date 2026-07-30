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

#include "raft/raft.h"

#include <assert.h>
#include <stdlib.h>
#include <string.h>

typedef struct snapshot_storage {
    raft_hard_state_t hard_state;
    const uint64_t *voters;
    size_t voter_count;
    uint64_t snapshot_index;
    uint64_t snapshot_term;
    const uint8_t *snapshot_data;
    size_t snapshot_data_len;
    bool snapshot_data_is_nil;
    uint64_t last_index;
    uint64_t stable_term;
    int snapshot_result;
    size_t snapshot_calls;
} snapshot_storage_t;

static int copy_ids(raft_uint64_vec_t *out,
                    const uint64_t *ids,
                    size_t count) {
    memset(out, 0, sizeof(*out));
    if (count == 0) {
        return RAFT_OK;
    }
    out->items = malloc(count * sizeof(*out->items));
    if (out->items == NULL) {
        return RAFT_ERR_OUT_OF_MEMORY;
    }
    memcpy(out->items, ids, count * sizeof(*out->items));
    out->len = count;
    return RAFT_OK;
}

static int storage_initial_state(uintptr_t handle,
                                 raft_hard_state_t *hard_state,
                                 raft_conf_state_t *conf_state) {
    const snapshot_storage_t *storage =
        (const snapshot_storage_t *)(uintptr_t)handle;
    *hard_state = storage->hard_state;
    memset(conf_state, 0, sizeof(*conf_state));
    return copy_ids(&conf_state->voters,
                    storage->voters,
                    storage->voter_count);
}

static uint64_t storage_first(const snapshot_storage_t *storage) {
    return storage->snapshot_index == 0
               ? 1
               : storage->snapshot_index + 1;
}

static int storage_entries(uintptr_t handle,
                           uint64_t lo,
                           uint64_t hi,
                           uint64_t max_size,
                           raft_entry_vec_t *entries) {
    const snapshot_storage_t *storage =
        (const snapshot_storage_t *)(uintptr_t)handle;
    uint64_t first = storage_first(storage);
    size_t count;
    size_t i;
    (void)max_size;

    memset(entries, 0, sizeof(*entries));
    if (lo < first) {
        return RAFT_ERR_STORAGE_COMPACTED;
    }
    if (lo > hi || hi == 0 ||
        hi - 1 > storage->last_index) {
        return RAFT_ERR_STORAGE_UNAVAILABLE;
    }
    if (lo == hi) {
        return RAFT_OK;
    }
    count = (size_t)(hi - lo);
    entries->items = calloc(count, sizeof(*entries->items));
    if (entries->items == NULL) {
        return RAFT_ERR_OUT_OF_MEMORY;
    }
    entries->len = count;
    for (i = 0; i < count; ++i) {
        entries->items[i].type = RAFT_ENTRY_NORMAL;
        entries->items[i].term = storage->stable_term;
        entries->items[i].index = lo + (uint64_t)i;
        entries->items[i].data.is_nil = true;
    }
    return RAFT_OK;
}

static int storage_term(uintptr_t handle,
                        uint64_t index,
                        uint64_t *term) {
    const snapshot_storage_t *storage =
        (const snapshot_storage_t *)(uintptr_t)handle;
    if (index == 0 && storage->snapshot_index == 0) {
        *term = 0;
        return RAFT_OK;
    }
    if (index == storage->snapshot_index &&
        storage->snapshot_index != 0) {
        *term = storage->snapshot_term;
        return RAFT_OK;
    }
    if (index >= storage_first(storage) &&
        index <= storage->last_index) {
        *term = storage->stable_term;
        return RAFT_OK;
    }
    if (index < storage->snapshot_index) {
        return RAFT_ERR_STORAGE_COMPACTED;
    }
    return RAFT_ERR_STORAGE_UNAVAILABLE;
}

static int storage_first_index(uintptr_t handle, uint64_t *index) {
    const snapshot_storage_t *storage =
        (const snapshot_storage_t *)(uintptr_t)handle;
    *index = storage_first(storage);
    return RAFT_OK;
}

static int storage_last_index(uintptr_t handle, uint64_t *index) {
    const snapshot_storage_t *storage =
        (const snapshot_storage_t *)(uintptr_t)handle;
    *index = storage->last_index;
    return RAFT_OK;
}

static int storage_snapshot(uintptr_t handle,
                            raft_snapshot_t *snapshot) {
    snapshot_storage_t *storage =
        (snapshot_storage_t *)(uintptr_t)handle;
    int result;

    ++storage->snapshot_calls;
    memset(snapshot, 0, sizeof(*snapshot));
    snapshot->data.is_nil = true;
    if (storage->snapshot_result != RAFT_OK) {
        return storage->snapshot_result;
    }
    snapshot->metadata.index = storage->snapshot_index;
    snapshot->metadata.term = storage->snapshot_term;
    result = copy_ids(&snapshot->metadata.conf_state.voters,
                      storage->voters,
                      storage->voter_count);
    if (result != RAFT_OK) {
        return result;
    }
    snapshot->data.is_nil = storage->snapshot_data_is_nil;
    if (storage->snapshot_data_len != 0) {
        snapshot->data.data = malloc(storage->snapshot_data_len);
        if (snapshot->data.data == NULL) {
            raft_snapshot_free(snapshot);
            return RAFT_ERR_OUT_OF_MEMORY;
        }
        memcpy(snapshot->data.data,
               storage->snapshot_data,
               storage->snapshot_data_len);
        snapshot->data.len = storage->snapshot_data_len;
        snapshot->data.is_nil = false;
    }
    return RAFT_OK;
}

static raft_storage_ops_t storage_ops(snapshot_storage_t *storage) {
    const raft_storage_ops_t ops = {
        .handle = (uintptr_t)storage,
        .initial_state = storage_initial_state,
        .entries = storage_entries,
        .term = storage_term,
        .first_index = storage_first_index,
        .last_index = storage_last_index,
        .snapshot = storage_snapshot,
    };
    return ops;
}

static raft_config_t config(uint64_t id) {
    const raft_config_t cfg = {
        .id = id,
        .election_tick = 10,
        .heartbeat_tick = 1,
        .max_size_per_message = UINT64_MAX,
        .max_committed_size_per_ready = UINT64_MAX,
        .max_uncommitted_entries_size = UINT64_MAX,
        .max_inflight_messages = 16,
        .read_only_option = RAFT_READ_ONLY_SAFE,
    };
    return cfg;
}

static raft_raw_node_t *new_node(uint64_t id,
                                 snapshot_storage_t *storage) {
    raft_config_t cfg = config(id);
    raft_storage_ops_t ops = storage_ops(storage);
    raft_raw_node_t *node = NULL;
    assert(raft_raw_node_new(&cfg, &ops, &node) == RAFT_OK);
    assert(node != NULL);
    return node;
}

static const raft_message_t *find_message(const raft_ready_t *ready,
                                          raft_message_type_t type,
                                          uint64_t to) {
    size_t i;
    for (i = ready->messages.len; i > 0; --i) {
        const raft_message_t *message = &ready->messages.items[i - 1];
        if (message->type == type && message->to == to) {
            return message;
        }
    }
    return NULL;
}

static raft_progress_t progress_for(raft_raw_node_t *node, uint64_t id) {
    raft_progress_snapshot_t *rows = NULL;
    raft_progress_t progress;
    size_t count = 0;
    size_t i;

    memset(&progress, 0, sizeof(progress));
    assert(raft_raw_node_progress_snapshot(
               node, &rows, &count) == RAFT_OK);
    for (i = 0; i < count; ++i) {
        if (rows[i].id == id) {
            progress = rows[i].progress;
            break;
        }
    }
    assert(i != count);
    raft_progress_snapshot_array_free(rows, count);
    return progress;
}

static void elect_with_two_voters(raft_raw_node_t *node) {
    const raft_message_view_t vote = {
        .type = RAFT_MSG_VOTE_RESP,
        .to = 1,
        .from = 2,
        .term = 2,
        .context = {NULL, 0, true},
    };
    assert(raft_raw_node_campaign(node) == RAFT_OK);
    assert(raft_raw_node_step(node, &vote) == RAFT_OK);
}

static void reject_compacted_append(raft_raw_node_t *node) {
    const raft_message_view_t rejection = {
        .type = RAFT_MSG_APP_RESP,
        .to = 1,
        .from = 2,
        .term = 2,
        .index = 5,
        .reject = true,
        .reject_hint = 0,
        .context = {NULL, 0, true},
    };
    assert(raft_raw_node_step(node, &rejection) == RAFT_OK);
}

static snapshot_storage_t compacted_leader_storage(
    const uint64_t *voters, uint8_t *data, size_t data_len) {
    const snapshot_storage_t storage = {
        .hard_state = {.term = 1, .vote = 1, .commit = 5},
        .voters = voters,
        .voter_count = 2,
        .snapshot_index = 5,
        .snapshot_term = 1,
        .snapshot_data = data,
        .snapshot_data_len = data_len,
        .last_index = 5,
        .stable_term = 1,
        .snapshot_result = RAFT_OK,
    };
    return storage;
}

static void test_leader_snapshot_send_and_report(void) {
    const uint64_t voters[] = {1, 2};
    uint8_t data[] = {'s', 'n', 'a', 'p'};
    snapshot_storage_t storage =
        compacted_leader_storage(voters, data, sizeof(data));
    raft_raw_node_t *node = new_node(1, &storage);
    raft_ready_t *ready = NULL;
    const raft_message_t *message;
    raft_progress_t progress;

    elect_with_two_voters(node);
    reject_compacted_append(node);
    data[0] = 'X';

    assert(raft_raw_node_ready_without_accept(node, &ready) == RAFT_OK);
    assert(!ready->has_snapshot);
    message = find_message(ready, RAFT_MSG_SNAP, 2);
    assert(message != NULL);
    assert(message->has_snapshot);
    assert(message->snapshot.metadata.index == 5);
    assert(message->snapshot.metadata.term == 1);
    assert(message->snapshot.metadata.conf_state.voters.len == 2);
    assert(message->snapshot.data.len == 4);
    assert(message->snapshot.data.data[0] == 's');
    assert(storage.snapshot_calls == 1);
    progress = progress_for(node, 2);
    assert(progress.state == RAFT_PROGRESS_STATE_SNAPSHOT);
    assert(progress.pending_snapshot == 5);
    raft_ready_destroy(ready);
    ready = NULL;

    assert(raft_raw_node_report_snapshot(
               node, 2, RAFT_SNAPSHOT_FAILURE) == RAFT_OK);
    progress = progress_for(node, 2);
    assert(progress.state == RAFT_PROGRESS_STATE_PROBE);
    assert(progress.pending_snapshot == 0);
    assert(progress.next_index == 1);
    assert(progress.message_flow_paused);

    {
        const raft_message_view_t heartbeat_response = {
            .type = RAFT_MSG_HEARTBEAT_RESP,
            .to = 1,
            .from = 2,
            .term = 2,
            .context = {NULL, 0, true},
        };
        assert(raft_raw_node_step(
                   node, &heartbeat_response) == RAFT_OK);
    }
    progress = progress_for(node, 2);
    assert(progress.state == RAFT_PROGRESS_STATE_SNAPSHOT);
    assert(progress.pending_snapshot == 5);
    assert(raft_raw_node_report_snapshot(
               node, 2, RAFT_SNAPSHOT_FINISH) == RAFT_OK);
    progress = progress_for(node, 2);
    assert(progress.state == RAFT_PROGRESS_STATE_PROBE);
    assert(progress.pending_snapshot == 0);
    assert(progress.next_index == 6);
    assert(progress.message_flow_paused);

    raft_raw_node_destroy(node);
}

static void test_snapshot_temporarily_unavailable_retries(void) {
    const uint64_t voters[] = {1, 2};
    uint8_t data[] = {'s'};
    snapshot_storage_t storage =
        compacted_leader_storage(voters, data, sizeof(data));
    raft_raw_node_t *node;
    raft_ready_t *ready = NULL;
    raft_progress_t progress;

    storage.snapshot_result =
        RAFT_ERR_SNAPSHOT_TEMPORARILY_UNAVAILABLE;
    node = new_node(1, &storage);
    elect_with_two_voters(node);
    reject_compacted_append(node);
    assert(storage.snapshot_calls == 1);
    assert(raft_raw_node_ready_without_accept(node, &ready) == RAFT_OK);
    assert(find_message(ready, RAFT_MSG_SNAP, 2) == NULL);
    raft_ready_destroy(ready);
    ready = NULL;
    progress = progress_for(node, 2);
    assert(progress.state == RAFT_PROGRESS_STATE_PROBE);
    assert(progress.next_index == 1);

    storage.snapshot_result = RAFT_OK;
    assert(raft_raw_node_step(
               node,
               &(raft_message_view_t){
                   .type = RAFT_MSG_HEARTBEAT_RESP,
                   .to = 1,
                   .from = 2,
                   .term = 2,
                   .context = {NULL, 0, true},
               }) == RAFT_OK);
    assert(storage.snapshot_calls == 2);
    assert(raft_raw_node_ready_without_accept(node, &ready) == RAFT_OK);
    assert(find_message(ready, RAFT_MSG_SNAP, 2) != NULL);
    raft_ready_destroy(ready);
    raft_raw_node_destroy(node);
}

static void test_append_response_can_abort_snapshot(void) {
    const uint64_t voters[] = {1, 2};
    snapshot_storage_t storage =
        compacted_leader_storage(voters, NULL, 0);
    raft_raw_node_t *node;
    raft_progress_t progress;

    storage.snapshot_data_is_nil = true;
    node = new_node(1, &storage);
    elect_with_two_voters(node);
    reject_compacted_append(node);
    assert(raft_raw_node_step(
               node,
               &(raft_message_view_t){
                   .type = RAFT_MSG_APP_RESP,
                   .to = 1,
                   .from = 2,
                   .term = 2,
                   .index = 4,
                   .context = {NULL, 0, true},
               }) == RAFT_OK);
    progress = progress_for(node, 2);
    assert(progress.state == RAFT_PROGRESS_STATE_SNAPSHOT);
    assert(progress.pending_snapshot == 5);

    assert(raft_raw_node_step(
               node,
               &(raft_message_view_t){
                   .type = RAFT_MSG_APP_RESP,
                   .to = 1,
                   .from = 2,
                   .term = 2,
                   .index = 5,
                   .context = {NULL, 0, true},
               }) == RAFT_OK);
    progress = progress_for(node, 2);
    assert(progress.state == RAFT_PROGRESS_STATE_REPLICATE);
    assert(progress.pending_snapshot == 0);
    assert(progress.match_index == 5);
    assert(progress.next_index == 7);
    raft_raw_node_destroy(node);
}

static void test_snapshot_storage_error_propagates(void) {
    const uint64_t voters[] = {1, 2};
    snapshot_storage_t storage =
        compacted_leader_storage(voters, NULL, 0);
    raft_raw_node_t *node;
    const raft_message_view_t rejection = {
        .type = RAFT_MSG_APP_RESP,
        .to = 1,
        .from = 2,
        .term = 2,
        .index = 5,
        .reject = true,
        .context = {NULL, 0, true},
    };

    storage.snapshot_data_is_nil = true;
    storage.snapshot_result = RAFT_ERR_STORAGE_UNAVAILABLE;
    node = new_node(1, &storage);
    elect_with_two_voters(node);
    assert(raft_raw_node_step(node, &rejection) ==
           RAFT_ERR_STORAGE_UNAVAILABLE);
    raft_raw_node_destroy(node);
}

static void test_follower_restore_ready_advance_and_boundary(void) {
    const uint64_t initial_voters[] = {1, 2};
    const uint64_t restored_voters[] = {2, 1, 3};
    uint8_t input_data[] = {'d', 'a', 't', 'a'};
    uint8_t persisted_data[] = {'d', 'a', 't', 'a'};
    snapshot_storage_t storage = {
        .hard_state = {.term = 3},
        .voters = initial_voters,
        .voter_count = 2,
        .snapshot_data_is_nil = true,
        .snapshot_result = RAFT_OK,
    };
    raft_raw_node_t *node = new_node(2, &storage);
    raft_message_view_t snapshot_message = {
        .type = RAFT_MSG_SNAP,
        .to = 2,
        .from = 1,
        .term = 3,
        .has_snapshot = true,
        .snapshot = {
            .data = {input_data, sizeof(input_data), false},
            .metadata = {
                .conf_state = {
                    .voters = {restored_voters, 3},
                },
                .index = 5,
                .term = 2,
            },
        },
        .context = {NULL, 0, true},
    };
    raft_ready_t *ready = NULL;
    raft_status_t status;
    raft_basic_status_t basic;
    const raft_message_t *response;

    assert(raft_raw_node_step(node, &snapshot_message) == RAFT_OK);
    input_data[0] = 'X';
    assert(raft_raw_node_ready_without_accept(node, &ready) == RAFT_OK);
    assert(ready->has_snapshot);
    assert(find_message(ready, RAFT_MSG_SNAP, 1) == NULL);
    assert(ready->snapshot.metadata.index == 5);
    assert(ready->snapshot.metadata.term == 2);
    assert(ready->snapshot.data.len == 4);
    assert(ready->snapshot.data.data[0] == 'd');
    assert(ready->committed_entries.len == 0);
    assert(!ready->must_sync);
    assert(ready->has_hard_state);
    assert(ready->hard_state.term == 3);
    assert(ready->hard_state.commit == 5);
    response = find_message(ready, RAFT_MSG_APP_RESP, 1);
    assert(response != NULL && !response->reject);
    assert(response->index == 5);

    memset(&status, 0, sizeof(status));
    assert(raft_raw_node_status(node, &status) == RAFT_OK);
    assert(status.conf_state.voters.len == 3);
    assert(status.conf_state.voters.items[0] == 1);
    assert(status.conf_state.voters.items[1] == 2);
    assert(status.conf_state.voters.items[2] == 3);
    raft_status_free(&status);
    assert(raft_raw_node_basic_status(node, &basic) == RAFT_OK);
    assert(basic.applied == 0);

    storage.voters = restored_voters;
    storage.voter_count = 3;
    storage.snapshot_index = 5;
    storage.snapshot_term = 2;
    storage.snapshot_data = persisted_data;
    storage.snapshot_data_len = sizeof(persisted_data);
    storage.snapshot_data_is_nil = false;
    storage.last_index = 5;
    storage.stable_term = 2;
    storage.hard_state =
        (raft_hard_state_t){.term = 3, .commit = 5};

    assert(raft_raw_node_accept_ready(node, ready) == RAFT_OK);
    raft_ready_destroy(ready);
    ready = NULL;
    assert(raft_raw_node_advance(node) == RAFT_OK);
    assert(raft_raw_node_basic_status(node, &basic) == RAFT_OK);
    assert(basic.applied == 5);
    assert(basic.hard_state.commit == 5);
    assert(!raft_raw_node_has_ready(node));

    {
        const raft_entry_view_t entry = {
            .type = RAFT_ENTRY_NORMAL,
            .term = 3,
            .index = 6,
            .data = {(const uint8_t *)"x", 1, false},
        };
        const raft_message_view_t append = {
            .type = RAFT_MSG_APP,
            .to = 2,
            .from = 1,
            .term = 3,
            .log_term = 2,
            .index = 5,
            .commit = 6,
            .entries = {&entry, 1},
            .context = {NULL, 0, true},
        };
        assert(raft_raw_node_step(node, &append) == RAFT_OK);
    }
    assert(raft_raw_node_ready_without_accept(node, &ready) == RAFT_OK);
    assert(!ready->has_snapshot);
    assert(ready->committed_entries.len == 1);
    assert(ready->committed_entries.items[0].index == 6);
    assert(ready->entries.len == 1);
    storage.last_index = 6;
    storage.stable_term = 3;
    storage.hard_state.commit = 6;
    assert(raft_raw_node_accept_ready(node, ready) == RAFT_OK);
    raft_ready_destroy(ready);
    assert(raft_raw_node_advance(node) == RAFT_OK);
    assert(raft_raw_node_basic_status(node, &basic) == RAFT_OK);
    assert(basic.applied == 6);

    raft_raw_node_destroy(node);
}

static void test_matching_snapshot_fast_forwards_commit(void) {
    const uint64_t voters[] = {1, 2};
    const uint64_t snapshot_voters[] = {1, 2, 3};
    snapshot_storage_t storage = {
        .hard_state = {.term = 3, .commit = 3},
        .voters = voters,
        .voter_count = 2,
        .last_index = 5,
        .stable_term = 2,
        .snapshot_data_is_nil = true,
        .snapshot_result = RAFT_OK,
    };
    raft_raw_node_t *node = new_node(2, &storage);
    const raft_message_view_t message = {
        .type = RAFT_MSG_SNAP,
        .to = 2,
        .from = 1,
        .term = 3,
        .has_snapshot = true,
        .snapshot = {
            .data = {NULL, 0, true},
            .metadata = {
                .conf_state = {.voters = {snapshot_voters, 3}},
                .index = 5,
                .term = 2,
            },
        },
        .context = {NULL, 0, true},
    };
    raft_ready_t *ready = NULL;
    raft_status_t status;
    const raft_message_t *response;

    assert(raft_raw_node_step(node, &message) == RAFT_OK);
    assert(raft_raw_node_ready_without_accept(node, &ready) == RAFT_OK);
    assert(!ready->has_snapshot);
    assert(ready->has_hard_state);
    assert(ready->hard_state.commit == 5);
    assert(ready->committed_entries.len == 5);
    response = find_message(ready, RAFT_MSG_APP_RESP, 1);
    assert(response != NULL && response->index == 5);
    memset(&status, 0, sizeof(status));
    assert(raft_raw_node_status(node, &status) == RAFT_OK);
    assert(status.conf_state.voters.len == 2);
    assert(status.conf_state.voters.items[0] == 1);
    assert(status.conf_state.voters.items[1] == 2);
    raft_status_free(&status);
    raft_ready_destroy(ready);
    raft_raw_node_destroy(node);
}

static void test_obsolete_snapshot_is_ignored(void) {
    const uint64_t voters[] = {1, 2};
    snapshot_storage_t storage = {
        .hard_state = {.term = 3, .commit = 5},
        .voters = voters,
        .voter_count = 2,
        .snapshot_index = 5,
        .snapshot_term = 2,
        .last_index = 5,
        .stable_term = 2,
        .snapshot_data_is_nil = true,
        .snapshot_result = RAFT_OK,
    };
    raft_raw_node_t *node = new_node(2, &storage);
    const raft_message_view_t message = {
        .type = RAFT_MSG_SNAP,
        .to = 2,
        .from = 1,
        .term = 3,
        .has_snapshot = true,
        .snapshot = {
            .data = {NULL, 0, true},
            .metadata = {
                .conf_state = {.voters = {voters, 2}},
                .index = 4,
                .term = 1,
            },
        },
        .context = {NULL, 0, true},
    };
    raft_ready_t *ready = NULL;
    const raft_message_t *response;

    assert(raft_raw_node_step(node, &message) == RAFT_OK);
    assert(raft_raw_node_ready_without_accept(node, &ready) == RAFT_OK);
    assert(!ready->has_snapshot);
    response = find_message(ready, RAFT_MSG_APP_RESP, 1);
    assert(response != NULL);
    assert(response->index == 5);
    raft_ready_destroy(ready);
    raft_raw_node_destroy(node);
}

int main(void) {
    test_leader_snapshot_send_and_report();
    test_snapshot_temporarily_unavailable_retries();
    test_append_response_can_abort_snapshot();
    test_snapshot_storage_error_propagates();
    test_follower_restore_ready_advance_and_boundary();
    test_matching_snapshot_fast_forwards_commit();
    test_obsolete_snapshot_is_ignored();
    return 0;
}
