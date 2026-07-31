// Copyright 2026 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

#include "raft/raft.h"
#include "alloc.h"

#include <assert.h>
#include <stdlib.h>
#include <string.h>

typedef struct failing_storage {
    uint64_t last_index;
    uint64_t last_term;
    uint64_t voter;
    uint64_t voter2;
    int initial_error;
    int entries_error;
    int term_error;
    int first_error;
    int last_error;
    int snapshot_error;
} failing_storage_t;

static int failing_initial_state(uintptr_t handle,
                                 raft_hard_state_t *hard_state,
                                 raft_conf_state_t *conf_state) {
    failing_storage_t *storage = (failing_storage_t *)handle;

    memset(hard_state, 0, sizeof(*hard_state));
    memset(conf_state, 0, sizeof(*conf_state));
    if (storage->initial_error != RAFT_OK) {
        return storage->initial_error;
    }
    if (storage->voter != RAFT_NONE) {
        size_t voter_count =
            storage->voter2 == RAFT_NONE ? 1 : 2;

        conf_state->voters.items =
            calloc(voter_count, sizeof(uint64_t));
        if (conf_state->voters.items == NULL) {
            return RAFT_ERR_OUT_OF_MEMORY;
        }
        conf_state->voters.items[0] = storage->voter;
        if (voter_count == 2) {
            conf_state->voters.items[1] = storage->voter2;
        }
        conf_state->voters.len = voter_count;
    }
    return RAFT_OK;
}

static int failing_entries(uintptr_t handle,
                           uint64_t lo,
                           uint64_t hi,
                           uint64_t max_size,
                           raft_entry_vec_t *entries) {
    failing_storage_t *storage = (failing_storage_t *)handle;
    size_t count;
    size_t i;

    (void)max_size;
    memset(entries, 0, sizeof(*entries));
    if (storage->entries_error != RAFT_OK) {
        return storage->entries_error;
    }
    if (lo > hi || hi == 0 || hi - 1 > storage->last_index) {
        return RAFT_ERR_STORAGE_UNAVAILABLE;
    }
    count = (size_t)(hi - lo);
    if (count == 0) {
        return RAFT_OK;
    }
    entries->items = calloc(count, sizeof(*entries->items));
    if (entries->items == NULL) {
        return RAFT_ERR_OUT_OF_MEMORY;
    }
    entries->len = count;
    for (i = 0; i < count; ++i) {
        entries->items[i].type = RAFT_ENTRY_NORMAL;
        entries->items[i].term = storage->last_term;
        entries->items[i].index = lo + (uint64_t)i;
        entries->items[i].data.is_nil = true;
    }
    return RAFT_OK;
}

static int failing_term(uintptr_t handle,
                        uint64_t index,
                        uint64_t *term) {
    failing_storage_t *storage = (failing_storage_t *)handle;

    *term = 0;
    if (storage->term_error != RAFT_OK) {
        return storage->term_error;
    }
    if (index == 0) {
        return RAFT_OK;
    }
    if (index > storage->last_index) {
        return RAFT_ERR_STORAGE_UNAVAILABLE;
    }
    *term = storage->last_term;
    return RAFT_OK;
}

static int failing_first_index(uintptr_t handle, uint64_t *index) {
    failing_storage_t *storage = (failing_storage_t *)handle;

    *index = 0;
    if (storage->first_error != RAFT_OK) {
        return storage->first_error;
    }
    *index = 1;
    return RAFT_OK;
}

static int failing_last_index(uintptr_t handle, uint64_t *index) {
    failing_storage_t *storage = (failing_storage_t *)handle;

    *index = 0;
    if (storage->last_error != RAFT_OK) {
        return storage->last_error;
    }
    *index = storage->last_index;
    return RAFT_OK;
}

static int failing_snapshot(uintptr_t handle, raft_snapshot_t *snapshot) {
    failing_storage_t *storage = (failing_storage_t *)handle;

    memset(snapshot, 0, sizeof(*snapshot));
    snapshot->data.is_nil = true;
    return storage->snapshot_error;
}

static raft_storage_ops_t failing_storage_ops(
    failing_storage_t *storage) {
    const raft_storage_ops_t ops = {
        .handle = (uintptr_t)storage,
        .initial_state = failing_initial_state,
        .entries = failing_entries,
        .term = failing_term,
        .first_index = failing_first_index,
        .last_index = failing_last_index,
        .snapshot = failing_snapshot,
    };
    return ops;
}

static raft_config_t failing_config(bool async_storage_writes) {
    const raft_config_t config = {
        .id = 1,
        .election_tick = 2,
        .heartbeat_tick = 1,
        .async_storage_writes = async_storage_writes,
        .max_size_per_message = UINT64_MAX,
        .max_committed_size_per_ready = UINT64_MAX,
        .max_uncommitted_entries_size = UINT64_MAX,
        .max_inflight_messages = 8,
        .max_inflight_bytes = UINT64_MAX,
        .read_only_option = RAFT_READ_ONLY_SAFE,
    };
    return config;
}

static raft_raw_node_t *new_failing_node(
    failing_storage_t *storage, bool async_storage_writes) {
    raft_config_t config = failing_config(async_storage_writes);
    raft_storage_ops_t ops = failing_storage_ops(storage);
    raft_raw_node_t *node = NULL;

    assert(raft_raw_node_new(&config, &ops, &node) == RAFT_OK);
    assert(node != NULL);
    return node;
}

static void test_constructor_failures_cleanup(void) {
    failing_storage_t storage = {0};
    raft_config_t config = failing_config(false);
    raft_storage_ops_t ops = failing_storage_ops(&storage);
    raft_raw_node_t *node = NULL;

    storage.first_error = RAFT_ERR_STORAGE_UNAVAILABLE;
    assert(raft_raw_node_new(&config, &ops, &node) ==
           RAFT_ERR_STORAGE_UNAVAILABLE);
    assert(node == NULL);
    storage.first_error = RAFT_OK;

    storage.first_error =
        RAFT_ERR_SNAPSHOT_TEMPORARILY_UNAVAILABLE;
    assert(raft_raw_node_new(&config, &ops, &node) ==
           RAFT_ERR_SNAPSHOT_TEMPORARILY_UNAVAILABLE);
    assert(node == NULL);
    storage.first_error = RAFT_OK;

    storage.last_error =
        RAFT_ERR_SNAPSHOT_TEMPORARILY_UNAVAILABLE;
    assert(raft_raw_node_new(&config, &ops, &node) ==
           RAFT_ERR_SNAPSHOT_TEMPORARILY_UNAVAILABLE);
    assert(node == NULL);
    storage.last_error = RAFT_OK;

    storage.initial_error =
        RAFT_ERR_SNAPSHOT_TEMPORARILY_UNAVAILABLE;
    assert(raft_raw_node_new(&config, &ops, &node) ==
           RAFT_ERR_SNAPSHOT_TEMPORARILY_UNAVAILABLE);
    assert(node == NULL);
    storage.initial_error = RAFT_OK;

    storage.initial_error = RAFT_ERR_FATAL;
    assert(raft_raw_node_new(&config, &ops, &node) == RAFT_ERR_FATAL);
    assert(node == NULL);
    storage.initial_error = RAFT_OK;

    raft_alloc_fail_after(0);
    assert(raft_raw_node_new(&config, &ops, &node) ==
           RAFT_ERR_OUT_OF_MEMORY);
    raft_alloc_fail_reset();
    assert(node == NULL);
}

static void test_bootstrap_pre_mutation_error_is_returned(void) {
    const raft_peer_view_t peer = {
        .id = 1,
        .context = {NULL, 0, true},
    };
    failing_storage_t storage = {0};
    raft_raw_node_t *node = new_failing_node(&storage, false);

    storage.last_error = RAFT_ERR_STORAGE_UNAVAILABLE;
    assert(raft_raw_node_bootstrap(node, &peer, 1) ==
           RAFT_ERR_STORAGE_UNAVAILABLE);
    assert(raft_raw_node_error(node) == RAFT_OK);

    storage.last_error = RAFT_ERR_FATAL;
    assert(raft_raw_node_bootstrap(node, &peer, 1) == RAFT_ERR_FATAL);
    assert(raft_raw_node_error(node) == RAFT_OK);

    storage.last_error =
        RAFT_ERR_SNAPSHOT_TEMPORARILY_UNAVAILABLE;
    assert(raft_raw_node_bootstrap(node, &peer, 1) ==
           RAFT_ERR_SNAPSHOT_TEMPORARILY_UNAVAILABLE);
    assert(raft_raw_node_error(node) == RAFT_OK);

    storage.last_error = RAFT_OK;
    assert(raft_raw_node_bootstrap(node, &peer, 1) == RAFT_OK);
    raft_raw_node_destroy(node);
}

static void assert_vote_storage_failure(bool for_node, int failure) {
    failing_storage_t storage = {
        .last_index = 1,
        .last_term = 1,
        .voter = 1,
        .voter2 = 2,
    };
    raft_raw_node_t *node = new_failing_node(&storage, false);
    const raft_message_view_t vote = {
        .type = RAFT_MSG_VOTE,
        .to = 1,
        .from = 2,
        .term = 1,
        .log_term = 1,
        .index = 1,
        .context = {NULL, 0, true},
    };
    int result;

    storage.term_error = failure;
    result = for_node
                 ? raft_raw_node_step_for_node(node, &vote)
                 : raft_raw_node_step(node, &vote);
    assert(result == failure);
    assert(raft_raw_node_error(node) == failure);
    assert(raft_raw_node_campaign(node) == failure);
    assert((for_node
                ? raft_raw_node_step_for_node(node, &vote)
                : raft_raw_node_step(node, &vote)) == failure);
    raft_raw_node_destroy(node);
}

static void test_rawnode_and_node_storage_failure(void) {
    assert_vote_storage_failure(false, RAFT_ERR_FATAL);
    assert_vote_storage_failure(true, RAFT_ERR_FATAL);
    assert_vote_storage_failure(
        false, RAFT_ERR_SNAPSHOT_TEMPORARILY_UNAVAILABLE);
    assert_vote_storage_failure(
        true, RAFT_ERR_SNAPSHOT_TEMPORARILY_UNAVAILABLE);
}

static void test_entries_snapshot_temporary_is_terminal(void) {
    failing_storage_t storage = {
        .last_index = 1,
        .last_term = 1,
        .voter = 1,
        .voter2 = 2,
    };
    raft_raw_node_t *node = new_failing_node(&storage, false);
    const raft_message_view_t vote_response = {
        .type = RAFT_MSG_VOTE_RESP,
        .to = 1,
        .from = 2,
        .term = 1,
        .context = {NULL, 0, true},
    };
    const raft_message_view_t append_rejection = {
        .type = RAFT_MSG_APP_RESP,
        .to = 1,
        .from = 2,
        .term = 1,
        .index = 1,
        .reject = true,
        .reject_hint = 0,
        .context = {NULL, 0, true},
    };
    raft_ready_t *ready = NULL;

    assert(raft_raw_node_campaign(node) == RAFT_OK);
    assert(raft_raw_node_ready(node, &ready) == RAFT_OK);
    raft_ready_destroy(ready);
    assert(raft_raw_node_advance(node) == RAFT_OK);
    assert(raft_raw_node_step(node, &vote_response) == RAFT_OK);
    storage.entries_error =
        RAFT_ERR_SNAPSHOT_TEMPORARILY_UNAVAILABLE;
    assert(raft_raw_node_step(node, &append_rejection) ==
           RAFT_ERR_SNAPSHOT_TEMPORARILY_UNAVAILABLE);
    assert(raft_raw_node_error(node) ==
           RAFT_ERR_SNAPSHOT_TEMPORARILY_UNAVAILABLE);
    assert(raft_raw_node_campaign(node) ==
           RAFT_ERR_SNAPSHOT_TEMPORARILY_UNAVAILABLE);
    raft_raw_node_destroy(node);
}

static void test_tick_surfaces_and_latches_storage_failure(void) {
    failing_storage_t storage = {
        .last_index = 1,
        .last_term = 1,
        .voter = 1,
        .voter2 = 2,
    };
    raft_raw_node_t *node = new_failing_node(&storage, false);
    size_t i;
    int result = RAFT_OK;

    assert(raft_raw_node_has_progress(node, 1));
    storage.term_error = RAFT_ERR_FATAL;
    for (i = 0; i < 4 && result == RAFT_OK; ++i) {
        result = raft_raw_node_tick_result(node);
    }
    assert(result == RAFT_ERR_FATAL);
    assert(raft_raw_node_error(node) == RAFT_ERR_FATAL);
    raft_raw_node_tick(node);
    assert(raft_raw_node_error(node) == RAFT_ERR_FATAL);
    raft_raw_node_destroy(node);
}

static void test_ready_allocation_failure_is_terminal(void) {
    const raft_peer_view_t peer = {
        .id = 1,
        .context = {NULL, 0, true},
    };
    failing_storage_t storage = {0};
    raft_raw_node_t *node = new_failing_node(&storage, false);
    raft_ready_t *ready = NULL;

    assert(raft_raw_node_bootstrap(node, &peer, 1) == RAFT_OK);
    raft_alloc_fail_after(0);
    assert(raft_raw_node_ready_without_accept(node, &ready) ==
           RAFT_ERR_OUT_OF_MEMORY);
    raft_alloc_fail_reset();
    assert(ready == NULL);
    assert(raft_raw_node_error(node) == RAFT_ERR_OUT_OF_MEMORY);
    assert(raft_raw_node_ready_without_accept(node, &ready) ==
           RAFT_ERR_OUT_OF_MEMORY);
    assert(ready == NULL);
    raft_raw_node_destroy(node);
}

static void test_async_ready_nested_allocations_cleanup(void) {
    const raft_peer_view_t peer = {
        .id = 1,
        .context = {NULL, 0, true},
    };
    bool saw_nested_failure = false;
    size_t fail_after;

    for (fail_after = 1; fail_after < 32; ++fail_after) {
        failing_storage_t storage = {0};
        raft_raw_node_t *node = new_failing_node(&storage, true);
        raft_ready_t *ready = NULL;
        int result;

        assert(raft_raw_node_bootstrap(node, &peer, 1) == RAFT_OK);
        raft_alloc_fail_after(fail_after);
        result = raft_raw_node_ready_without_accept(node, &ready);
        raft_alloc_fail_reset();
        if (result == RAFT_ERR_OUT_OF_MEMORY) {
            saw_nested_failure = true;
            assert(ready == NULL);
            assert(raft_raw_node_error(node) ==
                   RAFT_ERR_OUT_OF_MEMORY);
        } else {
            assert(result == RAFT_OK);
            raft_ready_destroy(ready);
        }
        raft_raw_node_destroy(node);
    }
    assert(saw_nested_failure);
}

int main(void) {
    test_constructor_failures_cleanup();
    test_bootstrap_pre_mutation_error_is_returned();
    test_rawnode_and_node_storage_failure();
    test_entries_snapshot_temporary_is_terminal();
    test_tick_surfaces_and_latches_storage_failure();
    test_ready_allocation_failure_is_terminal();
    test_async_ready_nested_allocations_cleanup();
    return 0;
}
