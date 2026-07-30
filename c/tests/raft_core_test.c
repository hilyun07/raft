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
#include "raft_internal.h"

#include <assert.h>
#include <stdlib.h>
#include <string.h>

typedef struct test_storage {
    raft_hard_state_t hard_state;
    const uint64_t *voters;
    size_t voter_count;
    uint64_t last_index;
    uint64_t last_term;
} test_storage_t;

static int test_initial_state(uintptr_t handle,
                              raft_hard_state_t *hard_state,
                              raft_conf_state_t *conf_state) {
    const test_storage_t *storage =
        (const test_storage_t *)(uintptr_t)handle;
    memset(conf_state, 0, sizeof(*conf_state));
    *hard_state = storage->hard_state;
    if (storage->voter_count != 0) {
        conf_state->voters.items =
            calloc(storage->voter_count, sizeof(uint64_t));
        if (conf_state->voters.items == NULL) {
            return RAFT_ERR_OUT_OF_MEMORY;
        }
        memcpy(conf_state->voters.items,
               storage->voters,
               storage->voter_count * sizeof(uint64_t));
        conf_state->voters.len = storage->voter_count;
    }
    return RAFT_OK;
}

static int test_entries(uintptr_t handle,
                        uint64_t lo,
                        uint64_t hi,
                        uint64_t max_size,
                        raft_entry_vec_t *entries) {
    const test_storage_t *storage =
        (const test_storage_t *)(uintptr_t)handle;
    size_t count;
    size_t i;
    (void)max_size;
    memset(entries, 0, sizeof(*entries));
    if (lo > hi || hi == 0 || hi - 1 > storage->last_index) {
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
        entries->items[i].index = lo + (uint64_t)i;
        entries->items[i].term = storage->last_term;
        entries->items[i].type = RAFT_ENTRY_NORMAL;
        entries->items[i].data.is_nil = true;
    }
    return RAFT_OK;
}

static int test_term(uintptr_t handle, uint64_t index, uint64_t *term) {
    const test_storage_t *storage =
        (const test_storage_t *)(uintptr_t)handle;
    if (index == 0) {
        *term = 0;
        return RAFT_OK;
    }
    if (index != storage->last_index) {
        return RAFT_ERR_STORAGE_UNAVAILABLE;
    }
    *term = storage->last_term;
    return RAFT_OK;
}

static int test_first_index(uintptr_t handle, uint64_t *index) {
    (void)handle;
    *index = 1;
    return RAFT_OK;
}

static int test_last_index(uintptr_t handle, uint64_t *index) {
    const test_storage_t *storage =
        (const test_storage_t *)(uintptr_t)handle;
    *index = storage->last_index;
    return RAFT_OK;
}

static int test_snapshot(uintptr_t handle, raft_snapshot_t *snapshot) {
    (void)handle;
    memset(snapshot, 0, sizeof(*snapshot));
    snapshot->data.is_nil = true;
    return RAFT_OK;
}

static raft_storage_ops_t storage_ops(test_storage_t *storage) {
    const raft_storage_ops_t ops = {
        .handle = (uintptr_t)storage,
        .initial_state = test_initial_state,
        .entries = test_entries,
        .term = test_term,
        .first_index = test_first_index,
        .last_index = test_last_index,
        .snapshot = test_snapshot,
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

static raft_raw_node_t *new_node(uint64_t id, test_storage_t *storage) {
    raft_raw_node_t *node = NULL;
    raft_config_t cfg = config(id);
    raft_storage_ops_t ops = storage_ops(storage);
    assert(raft_raw_node_new(&cfg, &ops, &node) == RAFT_OK);
    assert(node != NULL);
    return node;
}

static raft_raw_node_t *new_node_with_config(
    raft_config_t cfg, test_storage_t *storage) {
    raft_raw_node_t *node = NULL;
    raft_storage_ops_t ops = storage_ops(storage);
    assert(raft_raw_node_new(&cfg, &ops, &node) == RAFT_OK);
    assert(node != NULL);
    return node;
}

static void bootstrap_three(raft_raw_node_t *node) {
    const raft_peer_view_t peers[] = {
        {.id = 1, .context = {NULL, 0, true}},
        {.id = 2, .context = {NULL, 0, true}},
        {.id = 3, .context = {NULL, 0, true}},
    };
    assert(raft_raw_node_bootstrap(node, peers, 3) == RAFT_OK);
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
    size_t len = 0;
    size_t i;
    raft_progress_t progress;

    memset(&progress, 0, sizeof(progress));
    assert(raft_raw_node_progress_snapshot(node, &rows, &len) == RAFT_OK);
    for (i = 0; i < len; ++i) {
        if (rows[i].id == id) {
            progress = rows[i].progress;
            raft_progress_snapshot_array_free(rows, len);
            return progress;
        }
    }
    raft_progress_snapshot_array_free(rows, len);
    assert(0 && "progress not found");
    return progress;
}

static int step_owned(raft_raw_node_t *node,
                      const raft_message_t *message,
                      bool public_step) {
    raft_entry_view_t *entries = NULL;
    raft_message_view_t view;
    size_t i;
    int result;

    assert(message != NULL);
    assert(message->responses.len == 0);
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
    view.context.data = message->context.data;
    view.context.len = message->context.len;
    view.context.is_nil = message->context.is_nil;
    if (message->entries.len != 0) {
        entries = calloc(message->entries.len, sizeof(*entries));
        assert(entries != NULL);
        for (i = 0; i < message->entries.len; ++i) {
            entries[i].term = message->entries.items[i].term;
            entries[i].index = message->entries.items[i].index;
            entries[i].type = message->entries.items[i].type;
            entries[i].data.data = message->entries.items[i].data.data;
            entries[i].data.len = message->entries.items[i].data.len;
            entries[i].data.is_nil =
                message->entries.items[i].data.is_nil;
        }
        view.entries.items = entries;
        view.entries.len = message->entries.len;
    }
    result = public_step ? raft_raw_node_step(node, &view)
                         : raft_raw_node_step_for_node(node, &view);
    free(entries);
    return result;
}

static void persist_ready(test_storage_t *storage,
                          const raft_ready_t *ready) {
    if (ready->has_hard_state) {
        storage->hard_state = ready->hard_state;
    }
    if (ready->entries.len != 0) {
        const raft_entry_t *last =
            &ready->entries.items[ready->entries.len - 1];
        storage->last_index = last->index;
        storage->last_term = last->term;
    }
}

static void accept_and_advance(raft_raw_node_t *node,
                               test_storage_t *storage) {
    raft_ready_t *ready = NULL;

    assert(raft_raw_node_ready(node, &ready) == RAFT_OK);
    assert(ready != NULL);
    persist_ready(storage, ready);
    raft_ready_destroy(ready);
    assert(raft_raw_node_advance(node) == RAFT_OK);
}

static void elect_three_node_leader(raft_raw_node_t *node,
                                    test_storage_t *storage,
                                    uint64_t term) {
    const raft_message_view_t vote_response = {
        .type = RAFT_MSG_VOTE_RESP,
        .to = 1,
        .from = 2,
        .term = term,
        .context = {NULL, 0, true},
    };
    assert(raft_raw_node_campaign(node) == RAFT_OK);
    accept_and_advance(node, storage);
    assert(raft_raw_node_step(node, &vote_response) == RAFT_OK);
}

static void commit_leader_noop(raft_raw_node_t *node,
                               test_storage_t *storage,
                               uint64_t term,
                               uint64_t index) {
    const raft_message_view_t append_response = {
        .type = RAFT_MSG_APP_RESP,
        .to = 1,
        .from = 2,
        .term = term,
        .index = index,
        .context = {NULL, 0, true},
    };
    accept_and_advance(node, storage);
    assert(raft_raw_node_step(node, &append_response) == RAFT_OK);
}

static void test_initial_state_and_configuration(void) {
    const uint64_t voters[] = {1};
    test_storage_t storage = {
        .hard_state = {.term = 4, .vote = 1, .commit = 1},
        .voters = voters,
        .voter_count = 1,
        .last_index = 1,
        .last_term = 4,
    };
    raft_raw_node_t *node = new_node(1, &storage);
    raft_basic_status_t status;
    raft_progress_snapshot_t *progress = NULL;
    size_t progress_len = 0;

    assert(raft_raw_node_basic_status(node, &status) == RAFT_OK);
    assert(status.hard_state.term == 4);
    assert(status.hard_state.vote == 1);
    assert(status.hard_state.commit == 1);
    assert(status.soft_state.raft_state == RAFT_STATE_FOLLOWER);
    assert(raft_raw_node_has_progress(node, 1));
    assert(raft_raw_node_progress_snapshot(
               node, &progress, &progress_len) == RAFT_OK);
    assert(progress_len == 1);
    assert(progress[0].id == 1);
    progress[0].progress.match_index = 999;
    raft_progress_snapshot_array_free(progress, progress_len);
    progress = NULL;
    assert(raft_raw_node_progress_snapshot(
               node, &progress, &progress_len) == RAFT_OK);
    assert(progress[0].progress.match_index == 0);
    raft_progress_snapshot_array_free(progress, progress_len);
    raft_raw_node_destroy(node);
}

static void assert_randomized_timeout_in_range(
    const raft_raw_node_t *node) {
    uint64_t lower = node->raft.election_timeout;
    uint64_t upper = lower * UINT64_C(2);

    assert(node->raft.randomized_election_timeout >= lower);
    assert(node->raft.randomized_election_timeout < upper);
}

static void test_randomized_election_timeout_lifecycle(void) {
    const uint64_t voters[] = {1, 2, 3};
    test_storage_t storage = {
        .voters = voters,
        .voter_count = 3,
    };
    raft_raw_node_t *node = new_node(1, &storage);
    raft_message_view_t heartbeat = {
        .type = RAFT_MSG_HEARTBEAT,
        .to = 1,
        .from = 2,
        .context = {NULL, 0, true},
    };
    raft_basic_status_t status;
    uint64_t initial_timeout;
    uint64_t first_reset_timeout = 0;
    uint64_t timeout;
    uint64_t term;
    bool observed_different_timeout = false;

    assert_randomized_timeout_in_range(node);
    initial_timeout = node->raft.randomized_election_timeout;
    raft_raw_node_tick(node);
    assert(node->raft.election_elapsed == 1);
    assert(node->raft.randomized_election_timeout == initial_timeout);

    heartbeat.term = 1;
    assert(raft_raw_node_step(node, &heartbeat) == RAFT_OK);
    timeout = node->raft.randomized_election_timeout;
    assert_randomized_timeout_in_range(node);
    assert(node->raft.election_elapsed == 0);

    raft_raw_node_tick(node);
    assert(node->raft.election_elapsed == 1);
    assert(node->raft.randomized_election_timeout == timeout);
    assert(raft_raw_node_step(node, &heartbeat) == RAFT_OK);
    assert(node->raft.election_elapsed == 0);
    assert(node->raft.randomized_election_timeout == timeout);

    // A fixed private seed makes this reset sequence deterministic. The
    // assertion is therefore not a probabilistic two-draw comparison.
    raft_random_seed(
        &node->raft.random, UINT64_C(0x4d595df4d0f33173));
    for (term = 2; term < 34; ++term) {
        heartbeat.term = term;
        assert(raft_raw_node_step(node, &heartbeat) == RAFT_OK);
        assert_randomized_timeout_in_range(node);
        timeout = node->raft.randomized_election_timeout;
        if (first_reset_timeout == 0) {
            first_reset_timeout = timeout;
        } else if (timeout != first_reset_timeout) {
            observed_different_timeout = true;
        }
        raft_raw_node_tick(node);
        assert(node->raft.randomized_election_timeout == timeout);
    }
    assert(observed_different_timeout);

    // Campaigning enters candidate state through the same reset path.
    assert(raft_raw_node_campaign(node) == RAFT_OK);
    assert_randomized_timeout_in_range(node);
    assert(raft_raw_node_basic_status(node, &status) == RAFT_OK);
    assert(status.soft_state.raft_state == RAFT_STATE_CANDIDATE);
    raft_raw_node_destroy(node);
}

static void test_tick_starts_single_node_election(void) {
    const uint64_t voters[] = {1};
    test_storage_t storage = {
        .voters = voters,
        .voter_count = 1,
    };
    raft_raw_node_t *node = new_node(1, &storage);
    raft_basic_status_t status;
    uint64_t timeout = node->raft.randomized_election_timeout;
    uint64_t i;

    assert_randomized_timeout_in_range(node);
    for (i = 1; i < timeout; ++i) {
        raft_raw_node_tick(node);
    }
    assert(raft_raw_node_basic_status(node, &status) == RAFT_OK);
    assert(status.soft_state.raft_state == RAFT_STATE_FOLLOWER);
    raft_raw_node_tick(node);
    accept_and_advance(node, &storage);
    assert(raft_raw_node_basic_status(node, &status) == RAFT_OK);
    assert(status.soft_state.raft_state == RAFT_STATE_LEADER);
    assert(status.hard_state.term == 1);
    assert(status.hard_state.vote == 1);
    raft_raw_node_destroy(node);
}

static void test_vote_grant_reject_and_higher_term_stepdown(void) {
    const uint64_t voters[] = {1, 2, 3};
    test_storage_t storage = {
        .hard_state = {.term = 1},
        .voters = voters,
        .voter_count = 3,
        .last_index = 3,
        .last_term = 1,
    };
    raft_raw_node_t *node = new_node(1, &storage);
    raft_ready_t *ready = NULL;
    const raft_message_t *response;
    raft_message_view_t vote = {
        .type = RAFT_MSG_VOTE,
        .from = 2,
        .to = 1,
        .term = 2,
        .log_term = 1,
        .index = 3,
        .context = {NULL, 0, true},
    };
    raft_message_view_t heartbeat = {
        .type = RAFT_MSG_HEARTBEAT,
        .from = 2,
        .to = 1,
        .term = 5,
        .context = {NULL, 0, true},
    };
    raft_basic_status_t status;

    assert(raft_raw_node_step(node, &vote) == RAFT_OK);
    assert(raft_raw_node_ready_without_accept(node, &ready) == RAFT_OK);
    response = find_message(ready, RAFT_MSG_VOTE_RESP, 2);
    assert(response != NULL && !response->reject);
    raft_ready_destroy(ready);
    ready = NULL;

    vote.from = 3;
    assert(raft_raw_node_step(node, &vote) == RAFT_OK);
    assert(raft_raw_node_ready_without_accept(node, &ready) == RAFT_OK);
    response = find_message(ready, RAFT_MSG_VOTE_RESP, 3);
    assert(response != NULL && response->reject);
    raft_ready_destroy(ready);
    ready = NULL;

    vote.term = 3;
    vote.index = 2;
    assert(raft_raw_node_step(node, &vote) == RAFT_OK);
    assert(raft_raw_node_ready_without_accept(node, &ready) == RAFT_OK);
    response = find_message(ready, RAFT_MSG_VOTE_RESP, 3);
    assert(response != NULL && response->reject);
    raft_ready_destroy(ready);
    ready = NULL;

    vote.term = 2;
    vote.from = 2;
    vote.index = 3;
    assert(raft_raw_node_step(node, &vote) == RAFT_OK);
    assert(raft_raw_node_ready_without_accept(node, &ready) == RAFT_OK);
    // Lower-term real votes are ignored rather than answered.
    assert(ready->messages.len == 3);
    raft_ready_destroy(ready);
    ready = NULL;

    assert(raft_raw_node_step_for_node(
               node,
               &(raft_message_view_t){
                   .type = RAFT_MSG_HUP,
                   .context = {NULL, 0, true},
               }) == RAFT_OK);
    assert(raft_raw_node_basic_status(node, &status) == RAFT_OK);
    assert(status.soft_state.raft_state == RAFT_STATE_CANDIDATE);
    assert(raft_raw_node_step(node, &heartbeat) == RAFT_OK);
    assert(raft_raw_node_basic_status(node, &status) == RAFT_OK);
    assert(status.soft_state.raft_state == RAFT_STATE_FOLLOWER);
    assert(status.soft_state.lead == 2);
    assert(status.hard_state.term == 5);
    raft_raw_node_destroy(node);
}

static void test_append_heartbeat_and_follower_proposal(void) {
    test_storage_t storage = {0};
    test_storage_t mismatch_storage = {0};
    raft_raw_node_t *node = new_node(1, &storage);
    raft_raw_node_t *mismatch = new_node(1, &mismatch_storage);
    raft_ready_t *ready = NULL;
    raft_entry_view_t entry = {
        .term = 2,
        .index = 4,
        .type = RAFT_ENTRY_NORMAL,
        .data = {(const uint8_t *)"v", 1, false},
    };
    raft_message_view_t append = {
        .type = RAFT_MSG_APP,
        .from = 2,
        .to = 1,
        .term = 2,
        .log_term = 1,
        .index = 3,
        .commit = 4,
        .entries = {&entry, 1},
        .context = {NULL, 0, true},
    };
    raft_message_view_t heartbeat = {
        .type = RAFT_MSG_HEARTBEAT,
        .from = 2,
        .to = 1,
        .term = 2,
        .commit = 4,
        .context = {NULL, 0, true},
    };
    raft_byte_view_t proposal = {
        .data = (const uint8_t *)"forward",
        .len = 7,
        .is_nil = false,
    };
    const raft_message_t *response;
    raft_basic_status_t status;

    bootstrap_three(node);
    bootstrap_three(mismatch);
    assert(raft_raw_node_step(node, &append) == RAFT_OK);
    assert(raft_raw_node_ready_without_accept(node, &ready) == RAFT_OK);
    response = find_message(ready, RAFT_MSG_APP_RESP, 2);
    assert(response != NULL && !response->reject);
    assert(response->index == 4);
    assert(ready->committed_entries.len == 4);
    raft_ready_destroy(ready);
    ready = NULL;
    assert(raft_raw_node_basic_status(node, &status) == RAFT_OK);
    assert(status.soft_state.lead == 2);

    assert(raft_raw_node_step(node, &heartbeat) == RAFT_OK);
    assert(raft_raw_node_ready_without_accept(node, &ready) == RAFT_OK);
    assert(find_message(ready, RAFT_MSG_HEARTBEAT_RESP, 2) != NULL);
    raft_ready_destroy(ready);
    ready = NULL;

    assert(raft_raw_node_propose(node, &proposal) == RAFT_OK);
    assert(raft_raw_node_ready_without_accept(node, &ready) == RAFT_OK);
    assert(find_message(ready, RAFT_MSG_PROP, 2) != NULL);
    raft_ready_destroy(ready);
    ready = NULL;

    append.index = 9;
    append.entries.items = NULL;
    append.entries.len = 0;
    append.commit = 0;
    assert(raft_raw_node_step(mismatch, &append) == RAFT_OK);
    assert(raft_raw_node_ready_without_accept(mismatch, &ready) ==
           RAFT_OK);
    response = find_message(ready, RAFT_MSG_APP_RESP, 2);
    assert(response != NULL && response->reject);
    raft_ready_destroy(ready);
    raft_raw_node_destroy(mismatch);
    raft_raw_node_destroy(node);
}

static void test_commit_only_ready_does_not_require_sync(void) {
    const uint64_t voters[] = {1};
    test_storage_t storage = {
        .hard_state = {.term = 1, .vote = 1, .commit = 1},
        .voters = voters,
        .voter_count = 1,
        .last_index = 2,
        .last_term = 1,
    };
    raft_raw_node_t *node = new_node(1, &storage);
    raft_ready_t *ready = NULL;
    const raft_message_view_t heartbeat = {
        .type = RAFT_MSG_HEARTBEAT,
        .from = 2,
        .to = 1,
        .term = 1,
        .commit = 2,
        .context = {NULL, 0, true},
    };

    assert(raft_raw_node_step(node, &heartbeat) == RAFT_OK);
    assert(raft_raw_node_ready_without_accept(node, &ready) == RAFT_OK);
    assert(ready->has_hard_state);
    assert(ready->hard_state.commit == 2);
    assert(ready->entries.len == 0);
    assert(!ready->must_sync);
    raft_ready_destroy(ready);
    raft_raw_node_destroy(node);
}

static void test_election_replication_and_step_layering(void) {
    const uint64_t voters[] = {1, 2, 3};
    test_storage_t storage[3] = {
        {
            .hard_state = {.term = 1, .commit = 3},
            .voters = voters,
            .voter_count = 3,
            .last_index = 3,
            .last_term = 1,
        },
        {
            .hard_state = {.term = 1, .commit = 3},
            .voters = voters,
            .voter_count = 3,
            .last_index = 3,
            .last_term = 1,
        },
        {
            .hard_state = {.term = 1, .commit = 3},
            .voters = voters,
            .voter_count = 3,
            .last_index = 3,
            .last_term = 1,
        },
    };
    raft_raw_node_t *nodes[3];
    raft_ready_t *ready1 = NULL;
    raft_ready_t *ready2 = NULL;
    const raft_message_t *message;
    raft_message_view_t local = {
        .type = RAFT_MSG_HUP,
        .context = {NULL, 0, true},
    };
    raft_message_view_t unknown_response = {
        .type = RAFT_MSG_APP_RESP,
        .from = 99,
        .to = 1,
        .term = 2,
        .context = {NULL, 0, true},
    };
    raft_message_view_t known_response = {
        .type = RAFT_MSG_HEARTBEAT_RESP,
        .from = 2,
        .to = 1,
        .term = 2,
        .context = {NULL, 0, true},
    };
    raft_byte_view_t proposal = {
        .data = (const uint8_t *)"x",
        .len = 1,
        .is_nil = false,
    };
    size_t i;

    for (i = 0; i < 3; ++i) {
        nodes[i] = new_node((uint64_t)i + 1, &storage[i]);
    }
    assert(raft_raw_node_step(nodes[0], &local) ==
           RAFT_ERR_STEP_LOCAL_MSG);
    assert(raft_raw_node_step_for_node(nodes[0], &local) == RAFT_OK);
    assert(raft_raw_node_ready_without_accept(nodes[0], &ready1) ==
           RAFT_OK);
    assert(ready1->hard_state.term == 2);
    assert(ready1->soft_state.raft_state == RAFT_STATE_CANDIDATE);

    message = find_message(ready1, RAFT_MSG_VOTE, 2);
    assert(step_owned(nodes[1], message, true) == RAFT_OK);
    persist_ready(&storage[0], ready1);
    assert(raft_raw_node_accept_ready(nodes[0], ready1) == RAFT_OK);
    assert(raft_raw_node_advance(nodes[0]) == RAFT_OK);
    assert(raft_raw_node_ready_without_accept(nodes[1], &ready2) ==
           RAFT_OK);
    message = find_message(ready2, RAFT_MSG_VOTE_RESP, 1);
    persist_ready(&storage[1], ready2);
    assert(raft_raw_node_accept_ready(nodes[1], ready2) == RAFT_OK);
    assert(raft_raw_node_advance(nodes[1]) == RAFT_OK);
    assert(step_owned(nodes[0], message, true) == RAFT_OK);
    raft_ready_destroy(ready2);
    ready2 = NULL;
    raft_ready_destroy(ready1);
    ready1 = NULL;

    assert(raft_raw_node_ready_without_accept(nodes[0], &ready1) ==
           RAFT_OK);
    assert(ready1->soft_state.raft_state == RAFT_STATE_LEADER);
    message = find_message(ready1, RAFT_MSG_APP, 2);
    persist_ready(&storage[0], ready1);
    assert(raft_raw_node_accept_ready(nodes[0], ready1) == RAFT_OK);
    assert(raft_raw_node_advance(nodes[0]) == RAFT_OK);
    assert(step_owned(nodes[1], message, true) == RAFT_OK);
    assert(raft_raw_node_ready_without_accept(nodes[1], &ready2) ==
           RAFT_OK);
    message = find_message(ready2, RAFT_MSG_APP_RESP, 1);
    persist_ready(&storage[1], ready2);
    assert(raft_raw_node_accept_ready(nodes[1], ready2) == RAFT_OK);
    assert(raft_raw_node_advance(nodes[1]) == RAFT_OK);
    assert(step_owned(nodes[0], message, true) == RAFT_OK);
    raft_ready_destroy(ready2);
    ready2 = NULL;
    raft_ready_destroy(ready1);
    ready1 = NULL;

    assert(raft_raw_node_propose(nodes[0], &proposal) == RAFT_OK);
    assert(raft_raw_node_ready_without_accept(nodes[0], &ready1) ==
           RAFT_OK);
    assert(ready1->hard_state.commit >= 4);
    assert(find_message(ready1, RAFT_MSG_APP, 2) != NULL);
    raft_ready_destroy(ready1);
    ready1 = NULL;

    raft_raw_node_tick(nodes[0]);
    assert(raft_raw_node_ready_without_accept(nodes[0], &ready1) ==
           RAFT_OK);
    assert(find_message(ready1, RAFT_MSG_HEARTBEAT, 2) != NULL);

    assert(raft_raw_node_step(nodes[0], &unknown_response) ==
           RAFT_ERR_STEP_PEER_NOT_FOUND_OR_IGNORED);
    assert(raft_raw_node_step_for_node(nodes[0], &unknown_response) ==
           RAFT_ERR_STEP_PEER_NOT_FOUND_OR_IGNORED);
    assert(raft_raw_node_step(nodes[0], &known_response) == RAFT_OK);
    assert(raft_raw_node_step_for_node(nodes[0], &known_response) ==
           RAFT_OK);

    raft_ready_destroy(ready1);
    for (i = 0; i < 3; ++i) {
        raft_raw_node_destroy(nodes[i]);
    }
}

static void test_unsupported_configuration_is_explicit(void) {
    test_storage_t storage = {0};
    raft_storage_ops_t ops = storage_ops(&storage);
    raft_raw_node_t *node = NULL;
    raft_config_t cfg = config(1);

    cfg.pre_vote = true;
    assert(raft_raw_node_new(&cfg, &ops, &node) == RAFT_OK);
    raft_raw_node_destroy(node);
    node = NULL;
    cfg = config(1);
    cfg.check_quorum = true;
    assert(raft_raw_node_new(&cfg, &ops, &node) == RAFT_OK);
    raft_raw_node_destroy(node);
    node = NULL;
    cfg = config(1);
    cfg.async_storage_writes = true;
    assert(raft_raw_node_new(&cfg, &ops, &node) == RAFT_OK);
    raft_raw_node_destroy(node);
}

static void test_pre_vote_election_and_stale_log(void) {
    const uint64_t voters[] = {1, 2, 3};
    test_storage_t storage = {
        .hard_state = {.term = 1, .commit = 1},
        .voters = voters,
        .voter_count = 3,
        .last_index = 1,
        .last_term = 1,
    };
    test_storage_t stale_storage = {
        .hard_state = {.term = 3, .commit = 2},
        .voters = voters,
        .voter_count = 3,
        .last_index = 2,
        .last_term = 3,
    };
    raft_config_t cfg = config(1);
    raft_config_t stale_cfg = config(1);
    raft_raw_node_t *node;
    raft_raw_node_t *stale;
    raft_ready_t *ready = NULL;
    raft_basic_status_t status;
    const raft_message_t *message;
    const raft_message_view_t pre_vote_response = {
        .type = RAFT_MSG_PRE_VOTE_RESP,
        .to = 1,
        .from = 2,
        .term = 2,
        .context = {NULL, 0, true},
    };
    const raft_message_view_t vote_response = {
        .type = RAFT_MSG_VOTE_RESP,
        .to = 1,
        .from = 2,
        .term = 2,
        .context = {NULL, 0, true},
    };
    const raft_message_view_t stale_pre_vote = {
        .type = RAFT_MSG_PRE_VOTE,
        .to = 1,
        .from = 2,
        .term = 4,
        .log_term = 1,
        .index = 1,
        .context = {NULL, 0, true},
    };

    cfg.applied = 1;
    cfg.pre_vote = true;
    node = new_node_with_config(cfg, &storage);
    assert(raft_raw_node_campaign(node) == RAFT_OK);
    assert(raft_raw_node_basic_status(node, &status) == RAFT_OK);
    assert(status.soft_state.raft_state ==
           RAFT_STATE_PRE_CANDIDATE);
    assert(status.hard_state.term == 1);
    assert(status.hard_state.vote == RAFT_NONE);
    assert(raft_raw_node_ready_without_accept(node, &ready) == RAFT_OK);
    message = find_message(ready, RAFT_MSG_PRE_VOTE, 2);
    assert(message != NULL && message->term == 2);
    persist_ready(&storage, ready);
    assert(raft_raw_node_accept_ready(node, ready) == RAFT_OK);
    raft_ready_destroy(ready);
    ready = NULL;
    assert(raft_raw_node_advance(node) == RAFT_OK);

    assert(raft_raw_node_step(node, &pre_vote_response) == RAFT_OK);
    assert(raft_raw_node_basic_status(node, &status) == RAFT_OK);
    assert(status.soft_state.raft_state == RAFT_STATE_CANDIDATE);
    assert(status.hard_state.term == 2);
    assert(status.hard_state.vote == 1);
    assert(raft_raw_node_ready_without_accept(node, &ready) == RAFT_OK);
    message = find_message(ready, RAFT_MSG_VOTE, 2);
    assert(message != NULL && message->term == 2);
    persist_ready(&storage, ready);
    assert(raft_raw_node_accept_ready(node, ready) == RAFT_OK);
    raft_ready_destroy(ready);
    ready = NULL;
    assert(raft_raw_node_advance(node) == RAFT_OK);

    assert(raft_raw_node_step(node, &vote_response) == RAFT_OK);
    assert(raft_raw_node_basic_status(node, &status) == RAFT_OK);
    assert(status.soft_state.raft_state == RAFT_STATE_LEADER);
    assert(status.soft_state.lead == 1);
    raft_raw_node_destroy(node);

    stale_cfg.applied = 2;
    stale_cfg.pre_vote = true;
    stale = new_node_with_config(stale_cfg, &stale_storage);
    assert(raft_raw_node_step(stale, &stale_pre_vote) == RAFT_OK);
    assert(raft_raw_node_basic_status(stale, &status) == RAFT_OK);
    assert(status.soft_state.raft_state == RAFT_STATE_FOLLOWER);
    assert(status.hard_state.term == 3);
    assert(status.hard_state.vote == RAFT_NONE);
    assert(raft_raw_node_ready_without_accept(stale, &ready) ==
           RAFT_OK);
    message = find_message(ready, RAFT_MSG_PRE_VOTE_RESP, 2);
    assert(message != NULL && message->reject &&
           message->term == 3);
    raft_ready_destroy(ready);
    raft_raw_node_destroy(stale);
}

static void test_pre_vote_check_quorum_and_forget_leader(void) {
    const uint64_t voters[] = {1, 2, 3};
    test_storage_t storage = {
        .hard_state = {.term = 1, .commit = 1},
        .voters = voters,
        .voter_count = 3,
        .last_index = 1,
        .last_term = 1,
    };
    raft_config_t cfg = config(2);
    raft_raw_node_t *node;
    raft_ready_t *ready = NULL;
    raft_basic_status_t status;
    const raft_message_view_t heartbeat = {
        .type = RAFT_MSG_HEARTBEAT,
        .to = 2,
        .from = 1,
        .term = 2,
        .commit = 1,
        .context = {NULL, 0, true},
    };
    const raft_message_view_t pre_vote = {
        .type = RAFT_MSG_PRE_VOTE,
        .to = 2,
        .from = 3,
        .term = 3,
        .log_term = 1,
        .index = 1,
        .context = {NULL, 0, true},
    };
    size_t message_count;

    cfg.applied = 1;
    cfg.pre_vote = true;
    cfg.check_quorum = true;
    node = new_node_with_config(cfg, &storage);
    assert(raft_raw_node_step(node, &heartbeat) == RAFT_OK);
    assert(raft_raw_node_ready_without_accept(node, &ready) == RAFT_OK);
    message_count = ready->messages.len;
    raft_ready_destroy(ready);
    ready = NULL;

    // An active leader lease suppresses disruptive pre-vote traffic.
    assert(raft_raw_node_step(node, &pre_vote) == RAFT_OK);
    assert(raft_raw_node_ready_without_accept(node, &ready) == RAFT_OK);
    assert(ready->messages.len == message_count);
    raft_ready_destroy(ready);
    ready = NULL;

    assert(raft_raw_node_forget_leader(node) == RAFT_OK);
    assert(raft_raw_node_basic_status(node, &status) == RAFT_OK);
    assert(status.soft_state.lead == RAFT_NONE);
    assert(raft_raw_node_step(node, &pre_vote) == RAFT_OK);
    assert(raft_raw_node_ready_without_accept(node, &ready) == RAFT_OK);
    assert(find_message(ready, RAFT_MSG_PRE_VOTE_RESP, 3) != NULL);
    raft_ready_destroy(ready);
    raft_raw_node_destroy(node);
}

static void test_leadership_transfer_paths(void) {
    const uint64_t voters[] = {1, 2, 3};
    test_storage_t storage = {
        .hard_state = {.term = 1, .commit = 1},
        .voters = voters,
        .voter_count = 3,
        .last_index = 1,
        .last_term = 1,
    };
    raft_config_t cfg = config(1);
    raft_raw_node_t *node;
    raft_ready_t *ready = NULL;
    raft_basic_status_t status;
    const raft_byte_view_t proposal = {
        .data = (const uint8_t *)"blocked",
        .len = 7,
        .is_nil = false,
    };
    const raft_message_view_t catch_up = {
        .type = RAFT_MSG_APP_RESP,
        .to = 1,
        .from = 3,
        .term = 2,
        .index = 2,
        .context = {NULL, 0, true},
    };
    size_t i;

    cfg.applied = 1;
    node = new_node_with_config(cfg, &storage);
    elect_three_node_leader(node, &storage, 2);
    commit_leader_noop(node, &storage, 2, 2);

    assert(raft_raw_node_transfer_leader(node, 2) == RAFT_OK);
    assert(raft_raw_node_basic_status(node, &status) == RAFT_OK);
    assert(status.lead_transferee == 2);
    assert(raft_raw_node_ready_without_accept(node, &ready) == RAFT_OK);
    assert(find_message(ready, RAFT_MSG_TIMEOUT_NOW, 2) != NULL);
    raft_ready_destroy(ready);
    ready = NULL;
    assert(raft_raw_node_propose(node, &proposal) ==
           RAFT_ERR_PROPOSAL_DROPPED);

    for (i = 0; i < cfg.election_tick; ++i) {
        raft_raw_node_tick(node);
    }
    assert(raft_raw_node_basic_status(node, &status) == RAFT_OK);
    assert(status.soft_state.raft_state == RAFT_STATE_LEADER);
    assert(status.lead_transferee == RAFT_NONE);
    raft_raw_node_destroy(node);

    storage.hard_state =
        (raft_hard_state_t){.term = 1, .commit = 1};
    storage.last_index = 1;
    storage.last_term = 1;
    node = new_node_with_config(cfg, &storage);
    elect_three_node_leader(node, &storage, 2);
    assert(raft_raw_node_transfer_leader(node, 3) == RAFT_OK);
    assert(raft_raw_node_ready_without_accept(node, &ready) == RAFT_OK);
    assert(find_message(ready, RAFT_MSG_APP, 3) != NULL);
    raft_ready_destroy(ready);
    ready = NULL;
    assert(raft_raw_node_step(node, &catch_up) == RAFT_OK);
    assert(raft_raw_node_ready_without_accept(node, &ready) == RAFT_OK);
    assert(find_message(ready, RAFT_MSG_TIMEOUT_NOW, 3) != NULL);
    raft_ready_destroy(ready);
    raft_raw_node_destroy(node);
}

static void test_transfer_forwarding_and_forced_campaign(void) {
    const uint64_t voters[] = {1, 2, 3};
    test_storage_t storage = {
        .hard_state = {.term = 1, .commit = 1},
        .voters = voters,
        .voter_count = 3,
        .last_index = 1,
        .last_term = 1,
    };
    raft_config_t cfg = config(2);
    raft_raw_node_t *node;
    raft_ready_t *ready = NULL;
    raft_basic_status_t status;
    const raft_message_t *message;
    const raft_message_view_t heartbeat = {
        .type = RAFT_MSG_HEARTBEAT,
        .to = 2,
        .from = 1,
        .term = 2,
        .commit = 1,
        .context = {NULL, 0, true},
    };
    const raft_message_view_t transfer = {
        .type = RAFT_MSG_TRANSFER_LEADER,
        .to = 2,
        .from = 3,
        .context = {NULL, 0, true},
    };
    const raft_message_view_t timeout_now = {
        .type = RAFT_MSG_TIMEOUT_NOW,
        .to = 2,
        .from = 1,
        .term = 2,
        .context = {NULL, 0, true},
    };

    cfg.applied = 1;
    cfg.pre_vote = true;
    cfg.check_quorum = true;
    node = new_node_with_config(cfg, &storage);
    assert(raft_raw_node_step(node, &heartbeat) == RAFT_OK);
    assert(raft_raw_node_step_for_node(node, &transfer) == RAFT_OK);
    assert(raft_raw_node_ready_without_accept(node, &ready) == RAFT_OK);
    message = find_message(ready, RAFT_MSG_TRANSFER_LEADER, 1);
    assert(message != NULL && message->from == 3 &&
           message->term == 2);
    raft_ready_destroy(ready);
    ready = NULL;

    assert(raft_raw_node_step(node, &timeout_now) == RAFT_OK);
    assert(raft_raw_node_basic_status(node, &status) == RAFT_OK);
    assert(status.soft_state.raft_state == RAFT_STATE_CANDIDATE);
    assert(status.hard_state.term == 3);
    assert(raft_raw_node_ready_without_accept(node, &ready) == RAFT_OK);
    message = find_message(ready, RAFT_MSG_VOTE, 1);
    assert(message != NULL && message->context.len == 16 &&
           memcmp(message->context.data,
                  "CampaignTransfer",
                  16) == 0);
    assert(find_message(ready, RAFT_MSG_PRE_VOTE, 1) == NULL);
    raft_ready_destroy(ready);
    raft_raw_node_destroy(node);
}

static void test_safe_read_index_and_stepdown(void) {
    const uint64_t voters[] = {1, 2, 3};
    test_storage_t storage = {
        .hard_state = {.term = 1, .commit = 1},
        .voters = voters,
        .voter_count = 3,
        .last_index = 1,
        .last_term = 1,
    };
    raft_raw_node_t *node = new_node(1, &storage);
    raft_ready_t *ready = NULL;
    const raft_message_t *heartbeat;
    uint8_t request_context[] = {'r', 'e', 'a', 'd'};
    raft_byte_view_t request = {
        .data = request_context,
        .len = sizeof(request_context),
        .is_nil = false,
    };
    raft_message_view_t heartbeat_response;

    elect_three_node_leader(node, &storage, 2);
    assert(raft_raw_node_read_index(node, &request) == RAFT_OK);
    request_context[0] = 'X';
    assert(raft_raw_node_ready_without_accept(node, &ready) == RAFT_OK);
    assert(ready->read_states.len == 0);
    assert(find_message(ready, RAFT_MSG_HEARTBEAT, 2) == NULL);
    raft_ready_destroy(ready);
    ready = NULL;

    commit_leader_noop(node, &storage, 2, 2);
    assert(raft_raw_node_ready_without_accept(node, &ready) == RAFT_OK);
    heartbeat = find_message(ready, RAFT_MSG_HEARTBEAT, 2);
    assert(heartbeat != NULL);
    assert(!heartbeat->context.is_nil);
    assert(heartbeat->context.len == 8);
    memset(&heartbeat_response, 0, sizeof(heartbeat_response));
    heartbeat_response.type = RAFT_MSG_HEARTBEAT_RESP;
    heartbeat_response.to = 1;
    heartbeat_response.from = 2;
    heartbeat_response.term = 2;
    heartbeat_response.context.data = heartbeat->context.data;
    heartbeat_response.context.len = heartbeat->context.len;
    heartbeat_response.context.is_nil = heartbeat->context.is_nil;
    assert(raft_raw_node_step(node, &heartbeat_response) == RAFT_OK);
    raft_ready_destroy(ready);
    ready = NULL;

    assert(raft_raw_node_ready_without_accept(node, &ready) == RAFT_OK);
    assert(ready->read_states.len == 1);
    assert(ready->read_states.items[0].index == 2);
    assert(ready->read_states.items[0].request_ctx.len == 4);
    assert(ready->read_states.items[0].request_ctx.data[0] == 'r');
    raft_ready_destroy(ready);
    raft_raw_node_destroy(node);

    storage.hard_state.term = 1;
    storage.hard_state.commit = 1;
    storage.last_index = 1;
    storage.last_term = 1;
    node = new_node(1, &storage);
    elect_three_node_leader(node, &storage, 2);
    commit_leader_noop(node, &storage, 2, 2);
    request_context[0] = 'z';
    assert(raft_raw_node_read_index(node, &request) == RAFT_OK);
    assert(raft_raw_node_ready_without_accept(node, &ready) == RAFT_OK);
    heartbeat = find_message(ready, RAFT_MSG_HEARTBEAT, 2);
    assert(heartbeat != NULL);
    memset(&heartbeat_response, 0, sizeof(heartbeat_response));
    heartbeat_response.type = RAFT_MSG_HEARTBEAT_RESP;
    heartbeat_response.to = 1;
    heartbeat_response.from = 2;
    heartbeat_response.term = 2;
    heartbeat_response.context.data = heartbeat->context.data;
    heartbeat_response.context.len = heartbeat->context.len;
    heartbeat_response.context.is_nil = heartbeat->context.is_nil;
    {
        const raft_message_view_t higher_term_heartbeat = {
            .type = RAFT_MSG_HEARTBEAT,
            .to = 1,
            .from = 3,
            .term = 3,
            .context = {NULL, 0, true},
        };
        assert(raft_raw_node_step(
                   node, &higher_term_heartbeat) == RAFT_OK);
    }
    assert(raft_raw_node_step(node, &heartbeat_response) == RAFT_OK);
    raft_ready_destroy(ready);
    ready = NULL;
    assert(raft_raw_node_ready_without_accept(node, &ready) == RAFT_OK);
    assert(ready->read_states.len == 0);
    raft_ready_destroy(ready);
    raft_raw_node_destroy(node);
}

static void test_lease_read_and_check_quorum(void) {
    const uint64_t voters[] = {1, 2, 3};
    test_storage_t storage = {
        .hard_state = {.term = 1, .commit = 1},
        .voters = voters,
        .voter_count = 3,
        .last_index = 1,
        .last_term = 1,
    };
    raft_config_t cfg = config(1);
    raft_raw_node_t *node;
    raft_ready_t *ready = NULL;
    raft_basic_status_t status;
    const uint8_t context_data[] = {'l'};
    const raft_byte_view_t context = {
        .data = context_data,
        .len = sizeof(context_data),
        .is_nil = false,
    };
    size_t i;

    cfg.check_quorum = true;
    cfg.read_only_option = RAFT_READ_ONLY_LEASE_BASED;
    node = new_node_with_config(cfg, &storage);
    elect_three_node_leader(node, &storage, 2);
    commit_leader_noop(node, &storage, 2, 2);
    assert(raft_raw_node_read_index(node, &context) == RAFT_OK);
    assert(raft_raw_node_ready_without_accept(node, &ready) == RAFT_OK);
    assert(ready->read_states.len == 1);
    assert(ready->read_states.items[0].index == 2);
    assert(ready->read_states.items[0].request_ctx.len == 1);
    assert(ready->read_states.items[0].request_ctx.data[0] == 'l');
    raft_ready_destroy(ready);

    // The append response above counts as activity for the first lease
    // interval. A second inactive interval must expire before step-down.
    for (i = 0; i < (size_t)cfg.election_tick * 2; ++i) {
        raft_raw_node_tick(node);
    }
    assert(raft_raw_node_basic_status(node, &status) == RAFT_OK);
    assert(status.soft_state.raft_state == RAFT_STATE_FOLLOWER);
    assert(status.soft_state.lead == RAFT_NONE);
    raft_raw_node_destroy(node);
}

static void test_uncommitted_proposal_limit(void) {
    const uint64_t voters[] = {1};
    test_storage_t storage = {
        .voters = voters,
        .voter_count = 1,
    };
    raft_storage_ops_t ops = storage_ops(&storage);
    raft_config_t cfg = config(1);
    raft_raw_node_t *node = NULL;
    const raft_byte_view_t oversized = {
        .data = (const uint8_t *)"large",
        .len = 5,
        .is_nil = false,
    };
    const raft_byte_view_t below_limit = {
        .data = (const uint8_t *)"ok",
        .len = 2,
        .is_nil = false,
    };
    const raft_byte_view_t one_byte = {
        .data = (const uint8_t *)"x",
        .len = 1,
        .is_nil = false,
    };
    const raft_byte_view_t empty = {
        .data = NULL,
        .len = 0,
        .is_nil = false,
    };
    raft_message_view_t append_response = {
        .type = RAFT_MSG_APP_RESP,
        .to = 1,
        .from = 1,
        .context = {NULL, 0, true},
    };
    size_t iterations;

    cfg.max_uncommitted_entries_size = 4;
    assert(raft_raw_node_new(&cfg, &ops, &node) == RAFT_OK);
    assert(raft_raw_node_campaign(node) == RAFT_OK);
    iterations = 0;
    while (raft_raw_node_has_ready(node)) {
        assert(iterations++ < 8);
        accept_and_advance(node, &storage);
    }
    assert(node->raft.uncommitted_size == 0);

    // An empty uncommitted tail admits one proposal of any size.
    assert(raft_raw_node_propose(node, &oversized) == RAFT_OK);
    assert(node->raft.uncommitted_size == 5);
    assert(raft_raw_node_propose(node, &oversized) ==
           RAFT_ERR_PROPOSAL_DROPPED);
    assert(raft_raw_node_propose(node, &below_limit) ==
           RAFT_ERR_PROPOSAL_DROPPED);
    assert(node->raft.uncommitted_size == 5);

    // Empty entries remain admissible and do not alter the accounting.
    assert(raft_raw_node_propose(node, &empty) == RAFT_OK);
    assert(node->raft.uncommitted_size == 5);

    append_response.term = node->raft.term;
    assert(raft_log_last_index(
               &node->log, &append_response.index) == RAFT_OK);
    assert(raft_raw_node_step(node, &append_response) == RAFT_OK);
    iterations = 0;
    while (raft_raw_node_has_ready(node)) {
        assert(iterations++ < 8);
        accept_and_advance(node, &storage);
    }
    assert(node->raft.uncommitted_size == 0);

    // Ordinary proposals are admitted up to, but not beyond, the limit.
    assert(raft_raw_node_propose(node, &below_limit) == RAFT_OK);
    assert(node->raft.uncommitted_size == 2);
    assert(raft_raw_node_propose(node, &below_limit) == RAFT_OK);
    assert(node->raft.uncommitted_size == 4);
    assert(raft_raw_node_propose(node, &one_byte) ==
           RAFT_ERR_PROPOSAL_DROPPED);
    assert(node->raft.uncommitted_size == 4);

    assert(raft_log_last_index(
               &node->log, &append_response.index) == RAFT_OK);
    assert(raft_raw_node_step(node, &append_response) == RAFT_OK);
    iterations = 0;
    while (raft_raw_node_has_ready(node)) {
        assert(iterations++ < 8);
        accept_and_advance(node, &storage);
    }
    assert(node->raft.uncommitted_size == 0);
    assert(raft_raw_node_propose(node, &oversized) == RAFT_OK);
    assert(node->raft.uncommitted_size == 5);
    raft_raw_node_destroy(node);
}

static void test_report_unreachable_paths(void) {
    const uint64_t voters[] = {1, 2, 3};
    const uint8_t byte = 1;
    const raft_byte_view_t proposal = {&byte, 1, false};
    test_storage_t storage = {
        .hard_state = {.term = 1, .commit = 1},
        .voters = voters,
        .voter_count = 3,
        .last_index = 1,
        .last_term = 1,
    };
    raft_config_t cfg = config(1);
    raft_raw_node_t *node;
    raft_progress_t before;
    raft_progress_t after;
    raft_basic_status_t status;
    const raft_message_view_t append_response_2 = {
        .type = RAFT_MSG_APP_RESP,
        .to = 1,
        .from = 2,
        .term = 2,
        .index = 2,
        .context = {NULL, 0, true},
    };
    const raft_message_view_t append_response_3 = {
        .type = RAFT_MSG_APP_RESP,
        .to = 1,
        .from = 2,
        .term = 2,
        .index = 3,
        .context = {NULL, 0, true},
    };
    const raft_message_view_t unreachable = {
        .type = RAFT_MSG_UNREACHABLE,
        .from = 2,
        .context = {NULL, 0, true},
    };
    const raft_message_view_t storage_append_work = {
        .type = RAFT_MSG_STORAGE_APPEND,
        .from = 1,
        .to = RAFT_LOCAL_APPEND_THREAD,
        .context = {NULL, 0, true},
    };
    const raft_message_view_t storage_apply_work = {
        .type = RAFT_MSG_STORAGE_APPLY,
        .from = 1,
        .to = RAFT_LOCAL_APPLY_THREAD,
        .context = {NULL, 0, true},
    };

    cfg.applied = 1;
    node = new_node_with_config(cfg, &storage);

    before = progress_for(node, 2);
    assert(before.state == RAFT_PROGRESS_STATE_PROBE);
    assert(raft_raw_node_report_unreachable(node, 2) == RAFT_OK);
    after = progress_for(node, 2);
    assert(after.state == before.state);
    assert(after.match_index == before.match_index);
    assert(after.next_index == before.next_index);
    assert(!raft_raw_node_has_ready(node));

    assert(raft_raw_node_report_unreachable(node, 99) == RAFT_OK);
    assert(!raft_raw_node_has_ready(node));
    assert(raft_raw_node_step_for_node(
               node, &storage_append_work) == RAFT_ERR_NOT_IMPLEMENTED);
    assert(raft_raw_node_step_for_node(
               node, &storage_apply_work) == RAFT_ERR_NOT_IMPLEMENTED);

    elect_three_node_leader(node, &storage, 2);
    accept_and_advance(node, &storage);
    assert(raft_raw_node_step(node, &append_response_2) == RAFT_OK);
    accept_and_advance(node, &storage);
    assert(raft_raw_node_basic_status(node, &status) == RAFT_OK);
    assert(status.soft_state.raft_state == RAFT_STATE_LEADER);

    assert(raft_raw_node_propose(node, &proposal) == RAFT_OK);
    accept_and_advance(node, &storage);
    assert(!raft_raw_node_has_ready(node));
    before = progress_for(node, 2);
    assert(before.state == RAFT_PROGRESS_STATE_REPLICATE);
    assert(before.match_index == 2);
    assert(before.next_index == 4);

    assert(raft_raw_node_report_unreachable(node, 2) == RAFT_OK);
    after = progress_for(node, 2);
    assert(after.state == RAFT_PROGRESS_STATE_PROBE);
    assert(after.match_index == before.match_index);
    assert(after.next_index == after.match_index + 1);
    assert(after.pending_snapshot == 0);
    assert(!after.message_flow_paused);
    assert(after.recent_active == before.recent_active);
    assert(after.is_learner == before.is_learner);
    assert(!raft_raw_node_has_ready(node));

    before = after;
    assert(raft_raw_node_report_unreachable(node, 2) == RAFT_OK);
    after = progress_for(node, 2);
    assert(after.state == before.state);
    assert(after.match_index == before.match_index);
    assert(after.next_index == before.next_index);
    assert(!raft_raw_node_has_ready(node));

    assert(raft_raw_node_step(node, &append_response_3) == RAFT_OK);
    accept_and_advance(node, &storage);
    assert(progress_for(node, 2).state ==
           RAFT_PROGRESS_STATE_REPLICATE);

    assert(raft_raw_node_propose(node, &proposal) == RAFT_OK);
    accept_and_advance(node, &storage);
    before = progress_for(node, 2);
    assert(before.state == RAFT_PROGRESS_STATE_REPLICATE);
    assert(raft_raw_node_step_for_node(node, &unreachable) == RAFT_OK);
    after = progress_for(node, 2);
    assert(after.state == RAFT_PROGRESS_STATE_PROBE);
    assert(after.match_index == before.match_index);
    assert(after.next_index == after.match_index + 1);
    assert(!raft_raw_node_has_ready(node));

    raft_raw_node_destroy(node);
}

static void test_async_storage_write_protocol(void) {
    const uint64_t voters[] = {1};
    test_storage_t storage = {
        .hard_state = {.term = 1, .commit = 1},
        .voters = voters,
        .voter_count = 1,
        .last_index = 1,
        .last_term = 1,
    };
    raft_config_t cfg = config(1);
    raft_raw_node_t *node;
    raft_ready_t *ready = NULL;
    const raft_message_t *append;
    const raft_message_t *apply;
    raft_basic_status_t status;
    size_t i;

    cfg.applied = 1;
    cfg.async_storage_writes = true;
    node = new_node_with_config(cfg, &storage);
    assert(raft_raw_node_campaign(node) == RAFT_OK);
    assert(raft_raw_node_ready(node, &ready) == RAFT_OK);
    assert(ready != NULL);
    assert(ready->has_hard_state);
    append = find_message(
        ready, RAFT_MSG_STORAGE_APPEND, RAFT_LOCAL_APPEND_THREAD);
    assert(append != NULL);
    assert(append->entries.len == 0);
    assert(append->responses.len == 1);
    assert(append->responses.items[0].type == RAFT_MSG_VOTE_RESP);
    persist_ready(&storage, ready);
    for (i = 0; i < append->responses.len; ++i) {
        assert(step_owned(node, &append->responses.items[i], true) ==
               RAFT_OK);
    }
    raft_ready_destroy(ready);
    ready = NULL;
    assert(raft_raw_node_advance(node) == RAFT_ERR_INVALID_ARGUMENT);
    assert(raft_raw_node_basic_status(node, &status) == RAFT_OK);
    assert(status.soft_state.raft_state == RAFT_STATE_LEADER);

    assert(raft_raw_node_ready(node, &ready) == RAFT_OK);
    assert(ready->entries.len == 1);
    assert(ready->committed_entries.len == 0);
    append = find_message(
        ready, RAFT_MSG_STORAGE_APPEND, RAFT_LOCAL_APPEND_THREAD);
    assert(append != NULL);
    assert(append->entries.len == 1);
    assert(append->responses.len == 2);
    assert(append->responses.items[0].type == RAFT_MSG_APP_RESP);
    assert(append->responses.items[1].type ==
           RAFT_MSG_STORAGE_APPEND_RESP);
    assert(append->responses.items[1].from ==
           RAFT_LOCAL_APPEND_THREAD);
    assert(append->responses.items[1].index ==
           append->entries.items[0].index);
    persist_ready(&storage, ready);
    for (i = 0; i < append->responses.len; ++i) {
        assert(step_owned(node, &append->responses.items[i], true) ==
               RAFT_OK);
    }
    raft_ready_destroy(ready);
    ready = NULL;

    assert(raft_raw_node_ready(node, &ready) == RAFT_OK);
    assert(ready->entries.len == 0);
    assert(ready->committed_entries.len == 1);
    append = find_message(
        ready, RAFT_MSG_STORAGE_APPEND, RAFT_LOCAL_APPEND_THREAD);
    apply = find_message(
        ready, RAFT_MSG_STORAGE_APPLY, RAFT_LOCAL_APPLY_THREAD);
    assert(append != NULL);
    assert(apply != NULL);
    assert(apply->entries.len == 1);
    assert(apply->responses.len == 1);
    assert(apply->responses.items[0].type ==
           RAFT_MSG_STORAGE_APPLY_RESP);
    persist_ready(&storage, ready);
    assert(step_owned(node, &apply->responses.items[0], true) ==
           RAFT_OK);
    raft_ready_destroy(ready);
    ready = NULL;
    assert(raft_raw_node_basic_status(node, &status) == RAFT_OK);
    assert(status.applied == 2);
    raft_raw_node_destroy(node);
}

int main(void) {
    test_initial_state_and_configuration();
    test_randomized_election_timeout_lifecycle();
    test_tick_starts_single_node_election();
    test_vote_grant_reject_and_higher_term_stepdown();
    test_append_heartbeat_and_follower_proposal();
    test_commit_only_ready_does_not_require_sync();
    test_election_replication_and_step_layering();
    test_unsupported_configuration_is_explicit();
    test_pre_vote_election_and_stale_log();
    test_pre_vote_check_quorum_and_forget_leader();
    test_leadership_transfer_paths();
    test_transfer_forwarding_and_forced_campaign();
    test_safe_read_index_and_stepdown();
    test_lease_read_and_check_quorum();
    test_uncommitted_proposal_limit();
    test_report_unreachable_paths();
    test_async_storage_write_protocol();
    return 0;
}
