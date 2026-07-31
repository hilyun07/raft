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
#include "alloc.h"
#include "raft_internal.h"

#include <assert.h>
#include <stdlib.h>
#include <string.h>

typedef struct test_storage {
    raft_hard_state_t hard_state;
    const uint64_t *voters;
    size_t voter_count;
    const uint64_t *voters_outgoing;
    size_t voter_outgoing_count;
    const uint64_t *learners;
    size_t learner_count;
    const uint64_t *learners_next;
    size_t learner_next_count;
    bool auto_leave;
    uint64_t last_index;
    uint64_t last_term;
} test_storage_t;

static int copy_test_ids(raft_uint64_vec_t *out,
                         const uint64_t *ids,
                         size_t count) {
    if (count == 0) {
        return RAFT_OK;
    }
    out->items = calloc(count, sizeof(*out->items));
    if (out->items == NULL) {
        return RAFT_ERR_OUT_OF_MEMORY;
    }
    memcpy(out->items, ids, count * sizeof(*out->items));
    out->len = count;
    return RAFT_OK;
}

static int test_initial_state(uintptr_t handle,
                              raft_hard_state_t *hard_state,
                              raft_conf_state_t *conf_state) {
    const test_storage_t *storage =
        (const test_storage_t *)(uintptr_t)handle;
    int result;

    memset(conf_state, 0, sizeof(*conf_state));
    *hard_state = storage->hard_state;
    result = copy_test_ids(
        &conf_state->voters, storage->voters, storage->voter_count);
    if (result == RAFT_OK) {
        result = copy_test_ids(&conf_state->voters_outgoing,
                               storage->voters_outgoing,
                               storage->voter_outgoing_count);
    }
    if (result == RAFT_OK) {
        result = copy_test_ids(&conf_state->learners,
                               storage->learners,
                               storage->learner_count);
    }
    if (result == RAFT_OK) {
        result = copy_test_ids(&conf_state->learners_next,
                               storage->learners_next,
                               storage->learner_next_count);
    }
    if (result != RAFT_OK) {
        raft_conf_state_free(conf_state);
        return result;
    }
    conf_state->auto_leave = storage->auto_leave;
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

static void assert_progress_unchanged(
    const raft_progress_internal_t *before,
    const raft_progress_internal_t *after,
    bool response_marks_active) {
    assert(before != NULL);
    assert(after != NULL);
    assert(after->id == before->id);
    assert(after->match_index == before->match_index);
    assert(after->next_index == before->next_index);
    assert(after->sent_commit == before->sent_commit);
    assert(after->state == before->state);
    assert(after->pending_snapshot == before->pending_snapshot);
    assert(after->recent_active ==
           (response_marks_active ? true : before->recent_active));
    assert(after->message_flow_paused ==
           before->message_flow_paused);
    assert(after->is_learner == before->is_learner);
    assert(after->inflights.start == before->inflights.start);
    assert(after->inflights.count == before->inflights.count);
    assert(after->inflights.bytes == before->inflights.bytes);
    assert(after->inflights.size == before->inflights.size);
    assert(after->inflights.max_bytes == before->inflights.max_bytes);
    assert(after->inflights.capacity == before->inflights.capacity);
    if (before->inflights.capacity != 0) {
        assert(memcmp(
                   after->inflights.buffer,
                   before->inflights.buffer,
                   before->inflights.capacity *
                       sizeof(*before->inflights.buffer)) == 0);
    }
}

static const raft_status_progress_t *status_progress_for(
    const raft_status_t *status, uint64_t id) {
    size_t i;

    for (i = 0; i < status->progress_len; ++i) {
        if (status->progress[i].snapshot.id == id) {
            return &status->progress[i];
        }
    }
    assert(0 && "status progress not found");
    return NULL;
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

static void step_vote_response(raft_raw_node_t *node,
                               uint64_t from,
                               bool reject) {
    raft_message_view_t response = {
        .type = RAFT_MSG_VOTE_RESP,
        .to = 1,
        .from = from,
        .term = node->raft.term,
        .reject = reject,
        .context = {NULL, 0, true},
    };
    assert(raft_raw_node_step(node, &response) == RAFT_OK);
}

static void apply_voter_change(raft_raw_node_t *node,
                               raft_conf_change_type_t type,
                               uint64_t id) {
    raft_conf_change_single_t single = {
        .type = type,
        .node_id = id,
    };
    raft_conf_change_v2_view_t change = {
        .transition = RAFT_CONF_CHANGE_TRANSITION_AUTO,
        .changes = &single,
        .changes_len = 1,
        .context = {NULL, 0, true},
    };
    raft_conf_state_t state;

    assert(raft_raw_node_apply_conf_change(node, &change, &state) ==
           RAFT_OK);
    raft_conf_state_free(&state);
}

static void assert_raft_state(raft_raw_node_t *node,
                              raft_state_t expected) {
    raft_basic_status_t status;
    assert(raft_raw_node_basic_status(node, &status) == RAFT_OK);
    assert(status.soft_state.raft_state == expected);
}

static void test_candidate_vote_ownership(void) {
    const uint64_t voters[] = {1, 2, 3, 4, 5};
    const uint64_t learner_voters[] = {1, 3, 4};
    test_storage_t grant_storage = {
        .hard_state = {.term = 1, .commit = 1},
        .voters = voters,
        .voter_count = 5,
        .last_index = 1,
        .last_term = 1,
    };
    test_storage_t reject_storage = grant_storage;
    test_storage_t reset_storage = grant_storage;
    test_storage_t allocation_storage = grant_storage;
    test_storage_t learner_storage = {
        .hard_state = {.term = 1, .commit = 1},
        .voters = learner_voters,
        .voter_count = 3,
        .last_index = 1,
        .last_term = 1,
    };
    raft_raw_node_t *node;

    // A retained grant contributes again when the same voter is re-added.
    node = new_node(1, &grant_storage);
    assert(raft_raw_node_campaign(node) == RAFT_OK);
    assert_raft_state(node, RAFT_STATE_CANDIDATE);
    step_vote_response(node, 2, false);
    apply_voter_change(node, RAFT_CONF_CHANGE_REMOVE_NODE, 2);
    assert(!raft_raw_node_has_progress(node, 2));
    apply_voter_change(node, RAFT_CONF_CHANGE_ADD_NODE, 2);
    assert(raft_raw_node_has_progress(node, 2));
    step_vote_response(node, 3, false);
    assert_raft_state(node, RAFT_STATE_CANDIDATE);
    step_vote_response(node, 4, false);
    assert_raft_state(node, RAFT_STATE_LEADER);
    raft_raw_node_destroy(node);

    // A retained rejection likewise contributes to a lost election.
    node = new_node(1, &reject_storage);
    assert(raft_raw_node_campaign(node) == RAFT_OK);
    step_vote_response(node, 2, true);
    apply_voter_change(node, RAFT_CONF_CHANGE_REMOVE_NODE, 2);
    apply_voter_change(node, RAFT_CONF_CHANGE_ADD_NODE, 2);
    step_vote_response(node, 3, true);
    assert_raft_state(node, RAFT_STATE_CANDIDATE);
    step_vote_response(node, 4, true);
    assert_raft_state(node, RAFT_STATE_FOLLOWER);
    raft_raw_node_destroy(node);

    // A candidate-to-candidate transition starts a new term and clears the
    // retained vote. It takes three fresh grants to win the new election.
    node = new_node(1, &reset_storage);
    assert(raft_raw_node_campaign(node) == RAFT_OK);
    step_vote_response(node, 2, false);
    apply_voter_change(node, RAFT_CONF_CHANGE_REMOVE_NODE, 2);
    apply_voter_change(node, RAFT_CONF_CHANGE_ADD_NODE, 2);
    assert(raft_raw_node_campaign(node) == RAFT_OK);
    assert_raft_state(node, RAFT_STATE_CANDIDATE);
    step_vote_response(node, 3, false);
    step_vote_response(node, 4, false);
    assert_raft_state(node, RAFT_STATE_CANDIDATE);
    step_vote_response(node, 5, false);
    assert_raft_state(node, RAFT_STATE_LEADER);
    raft_raw_node_destroy(node);

    // Failure to allocate independent vote state follows the core's terminal
    // allocation-error propagation path without partially recording a vote.
    node = new_node(1, &allocation_storage);
    assert(raft_raw_node_campaign(node) == RAFT_OK);
    raft_alloc_fail_after(0);
    assert(raft_raw_node_step(
               node,
               &(raft_message_view_t){
                   .type = RAFT_MSG_VOTE_RESP,
                   .to = 1,
                   .from = 2,
                   .term = node->raft.term,
                   .context = {NULL, 0, true},
               }) == RAFT_ERR_OUT_OF_MEMORY);
    raft_alloc_fail_reset();
    assert(raft_raw_node_error(node) == RAFT_ERR_OUT_OF_MEMORY);
    assert(node->raft.tracker.votes_granted.len == 0);
    assert(node->raft.tracker.votes_rejected.len == 0);
    raft_raw_node_destroy(node);

    // A known learner's response is retained but not counted until the
    // current configuration promotes that peer to voter.
    node = new_node(1, &learner_storage);
    apply_voter_change(node, RAFT_CONF_CHANGE_ADD_LEARNER_NODE, 2);
    assert(raft_raw_node_campaign(node) == RAFT_OK);
    step_vote_response(node, 2, false);
    assert_raft_state(node, RAFT_STATE_CANDIDATE);
    apply_voter_change(node, RAFT_CONF_CHANGE_ADD_NODE, 2);
    step_vote_response(node, 3, false);
    assert_raft_state(node, RAFT_STATE_CANDIDATE);
    step_vote_response(node, 4, false);
    assert_raft_state(node, RAFT_STATE_LEADER);
    raft_raw_node_destroy(node);
}

static void assert_fully_reset_progress(
    const raft_progress_internal_t *progress,
    uint64_t id,
    uint64_t match,
    uint64_t next) {
    assert(progress != NULL);
    assert(progress->id == id);
    assert(progress->match_index == match);
    assert(progress->next_index == next);
    assert(progress->sent_commit == 0);
    assert(progress->state == RAFT_PROGRESS_STATE_PROBE);
    assert(progress->pending_snapshot == 0);
    assert(!progress->recent_active);
    assert(!progress->message_flow_paused);
    assert(!progress->is_learner);
    assert(progress->inflights.start == 0);
    assert(progress->inflights.count == 0);
    assert(progress->inflights.bytes == 0);
    assert(progress->inflights.size == 16);
    assert(progress->inflights.max_bytes == UINT64_MAX);
    assert(progress->inflights.buffer == NULL);
    assert(progress->inflights.capacity == 0);
}

static void dirty_progress_for_full_reset(
    raft_progress_internal_t *progress,
    uint64_t match,
    uint64_t next) {
    assert(progress != NULL);
    progress->match_index = match;
    progress->next_index = next;
    progress->sent_commit = next - 1;
    progress->state = RAFT_PROGRESS_STATE_SNAPSHOT;
    progress->pending_snapshot = next - 1;
    progress->recent_active = true;
    progress->message_flow_paused = true;
    assert(raft_inflights_add(
               &progress->inflights, next - 2, 1) == RAFT_OK);
    assert(progress->inflights.buffer != NULL);
}

static void test_raft_progress_reset_semantics(void) {
    const uint64_t voters[] = {1, 2};
    test_storage_t storage = {
        .hard_state = {.term = 1, .commit = 3},
        .voters = voters,
        .voter_count = 2,
        .last_index = 3,
        .last_term = 1,
    };
    raft_raw_node_t *node = new_node(1, &storage);
    raft_progress_internal_t *self =
        raft_tracker_find(&node->raft.tracker, 1);
    raft_progress_internal_t *peer =
        raft_tracker_find(&node->raft.tracker, 2);

    // Construction runs the Raft-wide reset. Self keeps the local last index,
    // while all activity, commit, snapshot, probe, and inflight state is fresh.
    assert_fully_reset_progress(self, 1, 3, 4);
    assert_fully_reset_progress(peer, 2, 0, 4);
    assert(node->raft.pending_conf_index == 0);

    dirty_progress_for_full_reset(self, 1, 8);
    dirty_progress_for_full_reset(peer, 2, 7);
    node->raft.pending_conf_index = 9;

    // Campaign enters candidate state through core_reset.
    assert(raft_raw_node_campaign(node) == RAFT_OK);
    assert_raft_state(node, RAFT_STATE_CANDIDATE);
    assert_fully_reset_progress(self, 1, 3, 4);
    assert_fully_reset_progress(peer, 2, 0, 4);
    assert(node->raft.pending_conf_index == 0);

    raft_raw_node_destroy(node);
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
    assert(progress[0].progress.match_index == 1);
    assert(!progress[0].progress.recent_active);
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

static void test_heartbeat_commit_invariant(void) {
    const uint64_t voters[] = {1, 2};
    test_storage_t storage = {
        .hard_state = {.term = 2, .commit = 1},
        .voters = voters,
        .voter_count = 2,
        .last_index = 3,
        .last_term = 2,
    };
    raft_config_t cfg = config(1);
    raft_message_view_t heartbeat = {
        .type = RAFT_MSG_HEARTBEAT,
        .from = 2,
        .to = 1,
        .term = 2,
        .context = {NULL, 0, true},
    };
    raft_raw_node_t *node;
    raft_basic_status_t status;
    raft_ready_t *ready = NULL;
    uint64_t last_index;

    cfg.applied = 1;

    node = new_node_with_config(cfg, &storage);
    heartbeat.commit = 1;
    assert(raft_raw_node_step(node, &heartbeat) == RAFT_OK);
    assert(raft_raw_node_basic_status(node, &status) == RAFT_OK);
    assert(status.hard_state.commit == 1);
    assert(node->raft.messages.len == 1);
    assert(node->raft.messages.items[0].type ==
           RAFT_MSG_HEARTBEAT_RESP);
    raft_raw_node_destroy(node);

    node = new_node_with_config(cfg, &storage);
    heartbeat.commit = 2;
    assert(raft_raw_node_step(node, &heartbeat) == RAFT_OK);
    assert(raft_raw_node_basic_status(node, &status) == RAFT_OK);
    assert(status.hard_state.commit == 2);
    assert(node->raft.messages.len == 1);
    raft_raw_node_destroy(node);

    node = new_node_with_config(cfg, &storage);
    heartbeat.commit = 3;
    assert(raft_raw_node_step(node, &heartbeat) == RAFT_OK);
    assert(raft_raw_node_basic_status(node, &status) == RAFT_OK);
    assert(status.hard_state.commit == 3);
    assert(node->raft.messages.len == 1);
    raft_raw_node_destroy(node);

    node = new_node_with_config(cfg, &storage);
    heartbeat.commit = 4;
    assert(raft_raw_node_step(node, &heartbeat) == RAFT_ERR_FATAL);
    assert(raft_raw_node_error(node) == RAFT_ERR_FATAL);
    assert(node->raft.log->committed == 1);
    assert(raft_log_last_index(node->raft.log, &last_index) ==
           RAFT_OK);
    assert(last_index == 3);
    assert(node->raft.messages.len == 0);
    assert(raft_raw_node_has_ready(node));
    assert(raft_raw_node_ready_without_accept(node, &ready) ==
           RAFT_ERR_FATAL);
    assert(ready == NULL);
    assert(raft_raw_node_campaign(node) == RAFT_ERR_FATAL);
    assert(node->raft.log->committed == 1);
    raft_raw_node_destroy(node);
}

static void test_append_rejection_log_term_optimization(void) {
    const uint64_t voters[] = {1, 2};
    const struct {
        uint64_t log_term;
        uint64_t next_index;
        uint64_t append_index;
        uint64_t append_log_term;
    } cases[] = {
        {1, 1, 0, 0},
        {0, 100, 99, 2},
    };
    size_t i;

    for (i = 0; i < sizeof(cases) / sizeof(cases[0]); ++i) {
        test_storage_t storage = {
            .hard_state = {.term = 1},
            .voters = voters,
            .voter_count = 2,
        };
        raft_storage_ops_t ops = storage_ops(&storage);
        raft_config_t cfg = config(1);
        raft_raw_node_t *node = NULL;
        raft_entry_t entries[100];
        raft_progress_internal_t *progress;
        raft_ready_t *ready = NULL;
        const raft_message_t *append;
        uint64_t last_index;
        size_t j;
        const raft_message_view_t rejection = {
            .type = RAFT_MSG_APP_RESP,
            .to = 1,
            .from = 2,
            .term = 2,
            .log_term = cases[i].log_term,
            .index = 100,
            .reject = true,
            .reject_hint = 99,
            .context = {NULL, 0, true},
        };

        assert(raft_raw_node_new(&cfg, &ops, &node) == RAFT_OK);
        memset(entries, 0, sizeof(entries));
        for (j = 0; j < sizeof(entries) / sizeof(entries[0]); ++j) {
            entries[j].type = RAFT_ENTRY_NORMAL;
            entries[j].term = 2;
            entries[j].index = (uint64_t)j + 1;
            entries[j].data.is_nil = true;
        }
        assert(raft_log_append(
                   &node->log,
                   entries,
                   sizeof(entries) / sizeof(entries[0]),
                   &last_index) == RAFT_OK);
        assert(last_index == 100);
        assert(raft_raw_node_campaign(node) == RAFT_OK);
        assert(raft_raw_node_step(
                   node,
                   &(raft_message_view_t){
                       .type = RAFT_MSG_VOTE_RESP,
                       .to = 1,
                       .from = 1,
                       .term = 2,
                       .context = {NULL, 0, true},
                   }) == RAFT_OK);
        assert(raft_raw_node_step(
                   node,
                   &(raft_message_view_t){
                       .type = RAFT_MSG_VOTE_RESP,
                       .to = 1,
                       .from = 2,
                       .term = 2,
                       .context = {NULL, 0, true},
                   }) == RAFT_OK);
        raft_core_clear_messages(&node->raft);
        raft_core_clear_messages_after_append(&node->raft);

        progress = raft_tracker_find(&node->raft.tracker, 2);
        assert(progress != NULL);
        assert(progress->state == RAFT_PROGRESS_STATE_PROBE);
        assert(progress->next_index == 101);

        assert(raft_raw_node_step(node, &rejection) == RAFT_OK);
        assert(progress->next_index == cases[i].next_index);
        assert(raft_raw_node_ready_without_accept(node, &ready) ==
               RAFT_OK);
        append = find_message(ready, RAFT_MSG_APP, 2);
        assert(append != NULL);
        assert(append->index == cases[i].append_index);
        assert(append->log_term == cases[i].append_log_term);
        raft_ready_destroy(ready);
        raft_raw_node_destroy(node);
    }
}

static void test_stale_append_response_semantics(void) {
    const uint64_t voters[] = {1, 2, 3};
    const uint8_t proposal_bytes[] = {3, 4, 5};
    test_storage_t storage = {
        .hard_state = {.term = 1, .commit = 1},
        .voters = voters,
        .voter_count = 3,
        .last_index = 1,
        .last_term = 1,
    };
    raft_config_t cfg = config(1);
    raft_raw_node_t *node;
    raft_progress_internal_t *progress;
    raft_progress_internal_t before;
    raft_message_view_t response = {
        .type = RAFT_MSG_APP_RESP,
        .to = 1,
        .from = 2,
        .term = 2,
        .context = {NULL, 0, true},
    };
    raft_byte_view_t proposal = {
        .data = &proposal_bytes[0],
        .len = 1,
        .is_nil = false,
    };
    size_t i;

    cfg.applied = 1;
    node = new_node_with_config(cfg, &storage);
    elect_three_node_leader(node, &storage, 2);
    accept_and_advance(node, &storage);

    response.index = 2;
    assert(raft_raw_node_step(node, &response) == RAFT_OK);
    accept_and_advance(node, &storage);
    progress = raft_tracker_find(&node->raft.tracker, 2);
    assert(progress != NULL);
    assert(progress->state == RAFT_PROGRESS_STATE_REPLICATE);
    assert(progress->match_index == 2);

    assert(raft_raw_node_propose(node, &proposal) == RAFT_OK);
    raft_core_clear_messages(&node->raft);
    raft_core_clear_messages_after_append(&node->raft);
    assert(progress->next_index == 4);
    assert(progress->inflights.count == 1);
    assert(raft_raw_node_report_unreachable(node, 2) == RAFT_OK);
    assert(progress->state == RAFT_PROGRESS_STATE_PROBE);
    assert(progress->match_index == 2);
    assert(progress->next_index == 3);
    assert(!progress->message_flow_paused);
    assert(progress->inflights.count == 0);
    assert(node->raft.messages.len == 0);
    assert(node->raft.messages_after_append.len == 0);

    // A successful response below Match is stale. It marks the peer active,
    // but it must not send the pending entry or pause the probe.
    progress->recent_active = false;
    memset(&before, 0, sizeof(before));
    assert(raft_progress_clone(&before, progress) == RAFT_OK);
    response.index = 1;
    response.reject = false;
    response.reject_hint = 0;
    response.log_term = 0;
    assert(raft_raw_node_step(node, &response) == RAFT_OK);
    assert_progress_unchanged(&before, progress, true);
    assert(node->raft.messages.len == 0);
    assert(node->raft.messages_after_append.len == 0);
    raft_progress_free(&before);

    // Probe rejections are actionable only for Next-1. Both old-format and
    // LogTerm-bearing stale rejections leave all Progress state unchanged.
    for (i = 0; i < 2; ++i) {
        progress->recent_active = false;
        memset(&before, 0, sizeof(before));
        assert(raft_progress_clone(&before, progress) == RAFT_OK);
        response.index = 1;
        response.reject = true;
        response.reject_hint = 1;
        response.log_term = i == 0 ? 0 : 1;
        assert(raft_raw_node_step(node, &response) == RAFT_OK);
        assert_progress_unchanged(&before, progress, true);
        assert(node->raft.messages.len == 0);
        assert(node->raft.messages_after_append.len == 0);
        raft_progress_free(&before);
    }

    // A current zero-LogTerm rejection remains meaningful even if the chosen
    // Next does not numerically change: it sends the outstanding probe.
    response.index = 2;
    response.reject = true;
    response.reject_hint = 1;
    response.log_term = 0;
    assert(raft_raw_node_step(node, &response) == RAFT_OK);
    assert(progress->state == RAFT_PROGRESS_STATE_PROBE);
    assert(progress->match_index == 2);
    assert(progress->next_index == 3);
    assert(progress->message_flow_paused);
    assert(node->raft.messages.len != 0 ||
           node->raft.messages_after_append.len != 0);
    assert(raft_raw_node_has_ready(node));
    raft_core_clear_messages(&node->raft);
    raft_core_clear_messages_after_append(&node->raft);

    // An equal-index success in probe state is deliberately actionable. It
    // recovers replication and sends the pending entry.
    response.index = 2;
    response.reject = false;
    response.reject_hint = 0;
    assert(raft_raw_node_step(node, &response) == RAFT_OK);
    assert(progress->state == RAFT_PROGRESS_STATE_REPLICATE);
    assert(progress->match_index == 2);
    assert(progress->next_index == 4);
    assert(progress->inflights.count == 1);
    assert(raft_raw_node_has_ready(node));
    accept_and_advance(node, &storage);

    response.index = 3;
    assert(raft_raw_node_step(node, &response) == RAFT_OK);
    assert(progress->match_index == 3);
    assert(progress->next_index == 4);
    assert(progress->inflights.count == 0);
    accept_and_advance(node, &storage);

    // A duplicate success in replicate state does not free a later inflight.
    for (i = 1; i < sizeof(proposal_bytes); ++i) {
        proposal.data = &proposal_bytes[i];
        assert(raft_raw_node_propose(node, &proposal) == RAFT_OK);
        accept_and_advance(node, &storage);
    }
    assert(progress->match_index == 3);
    assert(progress->next_index == 6);
    assert(progress->inflights.count == 2);
    response.index = 4;
    assert(raft_raw_node_step(node, &response) == RAFT_OK);
    accept_and_advance(node, &storage);
    assert(progress->match_index == 4);
    assert(progress->next_index == 6);
    assert(progress->inflights.count == 1);

    progress->recent_active = false;
    memset(&before, 0, sizeof(before));
    assert(raft_progress_clone(&before, progress) == RAFT_OK);
    assert(raft_raw_node_step(node, &response) == RAFT_OK);
    assert_progress_unchanged(&before, progress, true);
    assert(progress->inflights.count == 1);
    assert(!raft_raw_node_has_ready(node));
    raft_progress_free(&before);

    // A newer response can arrive first and free all covered inflights. The
    // delayed older response remains inert.
    response.index = 5;
    assert(raft_raw_node_step(node, &response) == RAFT_OK);
    accept_and_advance(node, &storage);
    assert(progress->match_index == 5);
    assert(progress->next_index == 6);
    assert(progress->inflights.count == 0);

    progress->recent_active = false;
    memset(&before, 0, sizeof(before));
    assert(raft_progress_clone(&before, progress) == RAFT_OK);
    response.index = 4;
    assert(raft_raw_node_step(node, &response) == RAFT_OK);
    assert_progress_unchanged(&before, progress, true);
    assert(!raft_raw_node_has_ready(node));
    raft_progress_free(&before);

    // Stale replicate-state rejections do not roll Next back or reset
    // inflights, with or without the Phase 17 LogTerm optimization.
    for (i = 0; i < 2; ++i) {
        progress->recent_active = false;
        memset(&before, 0, sizeof(before));
        assert(raft_progress_clone(&before, progress) == RAFT_OK);
        response.index = i == 0 ? 5 : 4;
        response.reject = true;
        response.reject_hint = 1;
        response.log_term = i == 0 ? 0 : 1;
        assert(raft_raw_node_step(node, &response) == RAFT_OK);
        assert_progress_unchanged(&before, progress, true);
        assert(!raft_raw_node_has_ready(node));
        raft_progress_free(&before);
    }

    // A rejection beyond Match remains actionable. LogTerm still takes the
    // Phase 17 conflict-search path before MaybeDecrTo applies its state guard.
    response.index = 6;
    response.reject = true;
    response.reject_hint = 1;
    response.log_term = 1;
    assert(raft_raw_node_step(node, &response) == RAFT_OK);
    assert(progress->state == RAFT_PROGRESS_STATE_PROBE);
    assert(progress->match_index == 5);
    assert(progress->next_index == 6);
    assert(progress->pending_snapshot == 0);
    assert(!progress->message_flow_paused);
    assert(progress->inflights.count == 0);
    assert(raft_raw_node_has_ready(node));
    accept_and_advance(node, &storage);

    // Snapshot state has the same stale-success guard; PendingSnapshot and
    // every other Progress field remain unchanged.
    raft_progress_become_snapshot(progress, 10);
    progress->recent_active = false;
    memset(&before, 0, sizeof(before));
    assert(raft_progress_clone(&before, progress) == RAFT_OK);
    response.index = 4;
    response.reject = false;
    response.reject_hint = 0;
    response.log_term = 0;
    assert(raft_raw_node_step(node, &response) == RAFT_OK);
    assert_progress_unchanged(&before, progress, true);
    assert(!raft_raw_node_has_ready(node));
    raft_progress_free(&before);

    // A real higher-term transition resets Progress. A response carrying the
    // old term is discarded before leader response handling after re-election.
    assert(raft_raw_node_step(
               node,
               &(raft_message_view_t){
                   .type = RAFT_MSG_HEARTBEAT,
                   .to = 1,
                   .from = 3,
                   .term = 3,
                   .commit = 5,
                   .context = {NULL, 0, true},
               }) == RAFT_OK);
    accept_and_advance(node, &storage);
    assert(raft_raw_node_campaign(node) == RAFT_OK);
    accept_and_advance(node, &storage);
    assert(raft_raw_node_step(
               node,
               &(raft_message_view_t){
                   .type = RAFT_MSG_VOTE_RESP,
                   .to = 1,
                   .from = 2,
                   .term = 4,
                   .context = {NULL, 0, true},
               }) == RAFT_OK);
    accept_and_advance(node, &storage);
    progress = raft_tracker_find(&node->raft.tracker, 2);
    assert(progress != NULL);
    assert(progress->state == RAFT_PROGRESS_STATE_PROBE);
    memset(&before, 0, sizeof(before));
    assert(raft_progress_clone(&before, progress) == RAFT_OK);
    response.term = 2;
    response.index = 5;
    assert(raft_raw_node_step(node, &response) == RAFT_OK);
    assert_progress_unchanged(&before, progress, false);
    assert(!raft_raw_node_has_ready(node));
    raft_progress_free(&before);

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
    uint64_t before_proposal;
    uint64_t after_proposal;

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
    assert(raft_log_last_index(&node->log, &before_proposal) == RAFT_OK);
    assert(raft_raw_node_propose(node, &proposal) ==
           RAFT_ERR_PROPOSAL_DROPPED);
    assert(raft_log_last_index(&node->log, &after_proposal) == RAFT_OK);
    assert(after_proposal == before_proposal);

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

static void test_demoted_leader_proposal_admission(void) {
    const uint64_t voters[] = {1, 2};
    const test_storage_t initial_storage = {
        .hard_state = {.term = 1, .commit = 1},
        .voters = voters,
        .voter_count = 2,
        .last_index = 1,
        .last_term = 1,
    };
    const uint8_t first_data[] = {'o', 'k'};
    const uint8_t second_data[] = {'g', 'o'};
    const uint8_t blocked_data[] = {'x'};
    const raft_byte_view_t first = {
        .data = first_data,
        .len = sizeof(first_data),
        .is_nil = false,
    };
    const raft_byte_view_t second = {
        .data = second_data,
        .len = sizeof(second_data),
        .is_nil = false,
    };
    const raft_byte_view_t blocked = {
        .data = blocked_data,
        .len = sizeof(blocked_data),
        .is_nil = false,
    };
    test_storage_t storage;
    raft_config_t cfg;
    raft_raw_node_t *node;
    raft_progress_internal_t *self;
    raft_ready_t *ready = NULL;
    uint64_t before;
    uint64_t after;

    // A normal voter leader still admits and appends proposals.
    storage = initial_storage;
    cfg = config(1);
    cfg.applied = 1;
    node = new_node_with_config(cfg, &storage);
    elect_three_node_leader(node, &storage, 2);
    assert(raft_log_last_index(&node->log, &before) == RAFT_OK);
    assert(raft_raw_node_propose(node, &first) == RAFT_OK);
    assert(raft_log_last_index(&node->log, &after) == RAFT_OK);
    assert(after == before + 1);
    raft_raw_node_destroy(node);

    // Removing the leader deletes self Progress. With the compatibility
    // default it remains leader, but proposals are still dropped.
    storage = initial_storage;
    cfg = config(1);
    cfg.applied = 1;
    node = new_node_with_config(cfg, &storage);
    elect_three_node_leader(node, &storage, 2);
    apply_voter_change(node, RAFT_CONF_CHANGE_REMOVE_NODE, 1);
    assert_raft_state(node, RAFT_STATE_LEADER);
    assert(!raft_tracker_has_progress(&node->raft.tracker, 1));
    assert(raft_log_last_index(&node->log, &before) == RAFT_OK);
    assert(raft_raw_node_propose(node, &first) ==
           RAFT_ERR_PROPOSAL_DROPPED);
    assert(raft_log_last_index(&node->log, &after) == RAFT_OK);
    assert(after == before);
    raft_raw_node_destroy(node);

    // Demotion retains learner Progress. With StepDownOnRemoval=false, Go
    // continues admitting proposals, including ordinary size enforcement.
    storage = initial_storage;
    cfg = config(1);
    cfg.applied = 1;
    cfg.max_uncommitted_entries_size = 4;
    node = new_node_with_config(cfg, &storage);
    elect_three_node_leader(node, &storage, 2);
    apply_voter_change(
        node, RAFT_CONF_CHANGE_ADD_LEARNER_NODE, 1);
    assert_raft_state(node, RAFT_STATE_LEADER);
    self = raft_tracker_find(&node->raft.tracker, 1);
    assert(self != NULL);
    assert(self->is_learner);
    assert(!raft_tracker_is_voter(&node->raft.tracker, 1));
    assert(raft_log_last_index(&node->log, &before) == RAFT_OK);
    assert(raft_raw_node_propose(node, &first) == RAFT_OK);
    assert(raft_raw_node_propose(node, &second) == RAFT_OK);
    assert(raft_raw_node_propose(node, &blocked) ==
           RAFT_ERR_PROPOSAL_DROPPED);
    assert(raft_log_last_index(&node->log, &after) == RAFT_OK);
    assert(after == before + 2);
    assert(raft_raw_node_ready_without_accept(node, &ready) == RAFT_OK);
    assert(ready->entries.len >= 2);
    assert(ready->entries.items[ready->entries.len - 2].data.len == 2);
    assert(ready->entries.items[ready->entries.len - 1].data.len == 2);
    raft_ready_destroy(ready);
    ready = NULL;
    raft_raw_node_destroy(node);

    // StepDownOnRemoval=true changes only the state transition. The retained
    // learner Progress remains, but follower-without-leader semantics drop.
    storage = initial_storage;
    cfg = config(1);
    cfg.applied = 1;
    cfg.step_down_on_removal = true;
    node = new_node_with_config(cfg, &storage);
    elect_three_node_leader(node, &storage, 2);
    apply_voter_change(
        node, RAFT_CONF_CHANGE_ADD_LEARNER_NODE, 1);
    assert_raft_state(node, RAFT_STATE_FOLLOWER);
    self = raft_tracker_find(&node->raft.tracker, 1);
    assert(self != NULL);
    assert(self->is_learner);
    assert(raft_log_last_index(&node->log, &before) == RAFT_OK);
    assert(raft_raw_node_propose(node, &first) ==
           RAFT_ERR_PROPOSAL_DROPPED);
    assert(raft_log_last_index(&node->log, &after) == RAFT_OK);
    assert(after == before);
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

static raft_raw_node_t *new_single_node_leader(
    test_storage_t *storage, raft_config_t cfg) {
    raft_raw_node_t *node = new_node_with_config(cfg, storage);
    size_t iterations = 0;

    assert(raft_raw_node_campaign(node) == RAFT_OK);
    while (raft_raw_node_has_ready(node)) {
        assert(iterations++ < 8);
        accept_and_advance(node, storage);
    }
    assert(node->raft.state == RAFT_STATE_LEADER);
    return node;
}

static void test_empty_message_proposal_semantics(void) {
    const uint64_t single_voter[] = {1};
    const uint64_t two_voters[] = {1, 2};
    raft_entry_view_t dummy_entry = {
        .type = RAFT_ENTRY_NORMAL,
        .data = {NULL, 0, true},
    };
    size_t i;

    // A leader treats both zero-entry representations as fatal invariants.
    for (i = 0; i < 2; ++i) {
        test_storage_t storage = {
            .hard_state = {.term = 1, .commit = 1},
            .voters = single_voter,
            .voter_count = 1,
            .last_index = 1,
            .last_term = 1,
        };
        raft_config_t cfg = config(1);
        raft_raw_node_t *node;
        raft_message_view_t proposal = {
            .type = RAFT_MSG_PROP,
            .to = 1,
            .from = 1,
            .entries = {
                .items = i == 0 ? NULL : &dummy_entry,
                .len = 0,
            },
            .context = {NULL, 0, true},
        };
        uint64_t before_last;
        uint64_t after_last;

        cfg.applied = 1;
        node = new_single_node_leader(&storage, cfg);
        assert(raft_log_last_index(&node->log, &before_last) == RAFT_OK);
        assert(raft_raw_node_step(node, &proposal) == RAFT_ERR_FATAL);
        assert(node->raft.error == RAFT_ERR_FATAL);
        assert(raft_log_last_index(&node->log, &after_last) == RAFT_OK);
        assert(after_last == before_last);
        assert(raft_raw_node_step(node, &proposal) == RAFT_ERR_FATAL);
        raft_raw_node_destroy(node);
    }

    // A follower with a known leader forwards a zero-entry proposal unchanged.
    {
        test_storage_t storage = {
            .hard_state = {.term = 1, .commit = 1},
            .voters = two_voters,
            .voter_count = 2,
            .last_index = 1,
            .last_term = 1,
        };
        raft_raw_node_t *node = new_node(1, &storage);
        raft_ready_t *ready = NULL;
        const raft_message_view_t heartbeat = {
            .type = RAFT_MSG_HEARTBEAT,
            .to = 1,
            .from = 2,
            .term = 1,
            .commit = 1,
            .context = {NULL, 0, true},
        };
        const raft_message_view_t proposal = {
            .type = RAFT_MSG_PROP,
            .to = 1,
            .from = 1,
            .context = {NULL, 0, true},
        };
        const raft_message_t *forwarded;

        assert(raft_raw_node_step(node, &heartbeat) == RAFT_OK);
        accept_and_advance(node, &storage);
        assert(node->raft.state == RAFT_STATE_FOLLOWER);
        assert(node->raft.lead == 2);
        assert(raft_raw_node_step(node, &proposal) == RAFT_OK);
        assert(raft_raw_node_ready_without_accept(node, &ready) == RAFT_OK);
        forwarded = find_message(ready, RAFT_MSG_PROP, 2);
        assert(forwarded != NULL);
        assert(forwarded->entries.len == 0);
        assert(node->raft.uncommitted_size == 0);
        raft_ready_destroy(ready);
        raft_raw_node_destroy(node);
    }

    // A candidate drops proposals before leader-only empty-message checking.
    {
        test_storage_t storage = {
            .hard_state = {.term = 1, .commit = 1},
            .voters = two_voters,
            .voter_count = 2,
            .last_index = 1,
            .last_term = 1,
        };
        raft_raw_node_t *node = new_node(1, &storage);
        const raft_message_view_t proposal = {
            .type = RAFT_MSG_PROP,
            .to = 1,
            .from = 1,
            .context = {NULL, 0, true},
        };

        assert(raft_raw_node_campaign(node) == RAFT_OK);
        assert(node->raft.state == RAFT_STATE_CANDIDATE);
        assert(raft_raw_node_step(node, &proposal) ==
               RAFT_ERR_PROPOSAL_DROPPED);
        assert(node->raft.error == RAFT_OK);
        raft_raw_node_destroy(node);
    }

    // Go checks for zero entries before checking whether a leader was removed.
    {
        test_storage_t storage = {
            .hard_state = {.term = 1, .commit = 1},
            .voters = two_voters,
            .voter_count = 2,
            .last_index = 1,
            .last_term = 1,
        };
        raft_config_t cfg = config(1);
        raft_raw_node_t *node;
        raft_message_view_t proposal = {
            .type = RAFT_MSG_PROP,
            .to = 1,
            .from = 1,
            .context = {NULL, 0, true},
        };

        cfg.applied = 1;
        node = new_node_with_config(cfg, &storage);
        elect_three_node_leader(node, &storage, 2);
        while (raft_raw_node_has_ready(node)) {
            accept_and_advance(node, &storage);
        }
        apply_voter_change(node, RAFT_CONF_CHANGE_REMOVE_NODE, 1);
        assert(node->raft.state == RAFT_STATE_LEADER);
        assert(!raft_tracker_has_progress(&node->raft.tracker, 1));
        proposal.entries.items = &dummy_entry;
        proposal.entries.len = 1;
        assert(raft_raw_node_step(node, &proposal) ==
               RAFT_ERR_PROPOSAL_DROPPED);
        proposal.entries.items = NULL;
        proposal.entries.len = 0;
        assert(raft_raw_node_step(node, &proposal) == RAFT_ERR_FATAL);
        assert(node->raft.error == RAFT_ERR_FATAL);
        raft_raw_node_destroy(node);
    }
}

static void test_empty_entry_proposal_semantics(void) {
    const uint64_t voters[] = {1};
    const uint8_t payload = 'x';

    // Empty Data is a valid payload. Metadata is cloned, while proposal
    // Term/Index are overwritten with the leader's current values.
    {
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
        raft_entry_view_t entries[] = {
            {
                .type = RAFT_ENTRY_NORMAL,
                .term = 99,
                .index = 99,
                .data = {NULL, 0, true},
                .protobuf = {
                    .fields =
                        RAFT_ENTRY_PROTO_TERM | RAFT_ENTRY_PROTO_INDEX,
                    .unknown_fields = {NULL, 0, true},
                },
            },
            {
                .type = RAFT_ENTRY_NORMAL,
                .data = {NULL, 0, false},
                .protobuf = {
                    .fields = RAFT_ENTRY_PROTO_TYPE,
                    .unknown_fields = {NULL, 0, true},
                },
            },
            {
                .type = RAFT_ENTRY_NORMAL,
                .data = {&payload, 1, false},
                .protobuf = {
                    .unknown_fields = {NULL, 0, true},
                },
            },
        };
        const raft_message_view_t proposal = {
            .type = RAFT_MSG_PROP,
            .to = 1,
            .from = 1,
            .entries = {
                .items = entries,
                .len = sizeof(entries) / sizeof(entries[0]),
            },
            .context = {NULL, 0, true},
        };
        uint64_t before_last;
        size_t i;

        cfg.applied = 1;
        node = new_single_node_leader(&storage, cfg);
        assert(raft_log_last_index(&node->log, &before_last) == RAFT_OK);
        assert(raft_raw_node_step(node, &proposal) == RAFT_OK);
        assert(node->raft.uncommitted_size == 1);
        assert(raft_raw_node_ready_without_accept(node, &ready) == RAFT_OK);
        assert(ready->entries.len == 3);
        for (i = 0; i < ready->entries.len; ++i) {
            assert(ready->entries.items[i].term == node->raft.term);
            assert(ready->entries.items[i].index ==
                   before_last + (uint64_t)i + 1);
        }
        assert(ready->entries.items[0].data.is_nil);
        assert(ready->entries.items[0].data.len == 0);
        assert(!ready->entries.items[1].data.is_nil);
        assert(ready->entries.items[1].data.len == 0);
        assert(ready->entries.items[2].data.len == 1);
        assert(ready->entries.items[2].data.data[0] == payload);
        assert((ready->entries.items[0].protobuf.fields &
                RAFT_ENTRY_PROTO_TERM) != 0);
        assert((ready->entries.items[1].protobuf.fields &
                RAFT_ENTRY_PROTO_TYPE) != 0);
        raft_ready_destroy(ready);
        raft_raw_node_destroy(node);
    }

    // A full uncommitted-size budget still admits any number of entries whose
    // aggregate Data length is zero, but rejects a mixed positive-size batch.
    {
        test_storage_t storage = {
            .hard_state = {.term = 1, .commit = 1},
            .voters = voters,
            .voter_count = 1,
            .last_index = 1,
            .last_term = 1,
        };
        raft_config_t cfg = config(1);
        raft_raw_node_t *node;
        const uint8_t oversized_data[] = {'l', 'a', 'r', 'g', 'e'};
        const raft_byte_view_t oversized = {
            .data = oversized_data,
            .len = sizeof(oversized_data),
            .is_nil = false,
        };
        raft_entry_view_t empty_entries[] = {
            {
                .type = RAFT_ENTRY_NORMAL,
                .data = {NULL, 0, true},
            },
            {
                .type = RAFT_ENTRY_NORMAL,
                .data = {NULL, 0, false},
            },
        };
        raft_entry_view_t mixed_entries[] = {
            {
                .type = RAFT_ENTRY_NORMAL,
                .data = {NULL, 0, true},
            },
            {
                .type = RAFT_ENTRY_NORMAL,
                .data = {&payload, 1, false},
            },
        };
        raft_message_view_t proposal = {
            .type = RAFT_MSG_PROP,
            .to = 1,
            .from = 1,
            .entries = {
                .items = empty_entries,
                .len =
                    sizeof(empty_entries) / sizeof(empty_entries[0]),
            },
            .context = {NULL, 0, true},
        };
        uint64_t before_mixed;
        uint64_t after_mixed;

        cfg.applied = 1;
        cfg.max_uncommitted_entries_size = 4;
        node = new_single_node_leader(&storage, cfg);
        assert(raft_raw_node_propose(node, &oversized) == RAFT_OK);
        assert(node->raft.uncommitted_size == 5);
        assert(raft_raw_node_step(node, &proposal) == RAFT_OK);
        assert(node->raft.uncommitted_size == 5);
        assert(raft_log_last_index(&node->log, &before_mixed) == RAFT_OK);
        proposal.entries.items = mixed_entries;
        proposal.entries.len =
            sizeof(mixed_entries) / sizeof(mixed_entries[0]);
        assert(raft_raw_node_step(node, &proposal) ==
               RAFT_ERR_PROPOSAL_DROPPED);
        assert(node->raft.uncommitted_size == 5);
        assert(raft_log_last_index(&node->log, &after_mixed) == RAFT_OK);
        assert(after_mixed == before_mixed);
        raft_raw_node_destroy(node);
    }

    // Empty configuration-change Data remains meaningful because Entry.Type
    // selects decoding and validation independently of payload size.
    {
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
        raft_entry_view_t entry = {
            .type = RAFT_ENTRY_CONF_CHANGE_V2,
            .data = {NULL, 0, true},
            .protobuf = {
                .fields = RAFT_ENTRY_PROTO_TYPE,
                .unknown_fields = {NULL, 0, true},
            },
        };
        raft_message_view_t proposal = {
            .type = RAFT_MSG_PROP,
            .to = 1,
            .from = 1,
            .entries = {.items = &entry, .len = 1},
            .context = {NULL, 0, true},
        };
        size_t iterations = 0;

        cfg.applied = 1;
        node = new_single_node_leader(&storage, cfg);
        assert(raft_raw_node_step(node, &proposal) == RAFT_OK);
        assert(raft_raw_node_ready(node, &ready) == RAFT_OK);
        assert(ready->entries.len == 1);
        assert(ready->entries.items[0].type == RAFT_ENTRY_NORMAL);
        assert(ready->entries.items[0].data.is_nil);
        persist_ready(&storage, ready);
        raft_ready_destroy(ready);
        assert(raft_raw_node_advance(node) == RAFT_OK);
        while (raft_raw_node_has_ready(node)) {
            assert(iterations++ < 8);
            accept_and_advance(node, &storage);
        }

        entry.type = RAFT_ENTRY_CONF_CHANGE;
        entry.data.is_nil = false;
        assert(raft_raw_node_step(node, &proposal) == RAFT_OK);
        assert(node->raft.pending_conf_index != 0);
        assert(raft_raw_node_ready_without_accept(node, &ready) == RAFT_OK);
        assert(ready->entries.len == 1);
        assert(ready->entries.items[0].type ==
               RAFT_ENTRY_CONF_CHANGE);
        assert(!ready->entries.items[0].data.is_nil);
        assert(ready->entries.items[0].data.len == 0);
        raft_ready_destroy(ready);
        raft_raw_node_destroy(node);
    }
}

static void assert_status_ids(const raft_uint64_vec_t *actual,
                              const uint64_t *expected,
                              size_t expected_len) {
    assert(actual->len == expected_len);
    if (expected_len != 0) {
        assert(actual->items != NULL);
        assert(memcmp(actual->items,
                      expected,
                      expected_len * sizeof(*expected)) == 0);
    }
}

static void assert_status_config(
    raft_raw_node_t *node,
    const uint64_t *voters,
    size_t voter_count,
    const uint64_t *voters_outgoing,
    size_t voter_outgoing_count,
    const uint64_t *learners,
    size_t learner_count,
    const uint64_t *learners_next,
    size_t learner_next_count) {
    raft_status_t status = {0};

    assert(raft_raw_node_status(node, &status) == RAFT_OK);
    assert_status_ids(&status.conf_state.voters,
                      voters,
                      voter_count);
    assert_status_ids(&status.conf_state.voters_outgoing,
                      voters_outgoing,
                      voter_outgoing_count);
    assert_status_ids(&status.conf_state.learners,
                      learners,
                      learner_count);
    assert_status_ids(&status.conf_state.learners_next,
                      learners_next,
                      learner_next_count);
    assert(!status.conf_state.auto_leave);
    raft_status_free(&status);
}

static raft_conf_state_t apply_status_config_change(
    raft_raw_node_t *node,
    raft_conf_change_transition_t transition,
    const raft_conf_change_single_t *changes,
    size_t change_count) {
    const raft_conf_change_v2_view_t change = {
        .transition = transition,
        .changes = changes,
        .changes_len = change_count,
        .context = {NULL, 0, true},
    };
    raft_conf_state_t state = {0};

    assert(raft_raw_node_apply_conf_change(
               node, &change, &state) == RAFT_OK);
    return state;
}

static void test_status_config_autoleave_semantics(void) {
    const uint64_t one[] = {1};
    const uint64_t one_two[] = {1, 2};
    const uint64_t one_three[] = {1, 3};
    const uint64_t two[] = {2};
    const uint64_t three[] = {3};
    const uint64_t three_four[] = {3, 4};
    const uint64_t four[] = {4};

    // The initial empty and ordinary configurations report false AutoLeave.
    {
        test_storage_t empty_storage = {0};
        test_storage_t normal_storage = {
            .hard_state = {.term = 1, .commit = 1},
            .voters = one_two,
            .voter_count = 2,
            .learners = three,
            .learner_count = 1,
            .last_index = 1,
            .last_term = 1,
        };
        raft_raw_node_t *empty = new_node(1, &empty_storage);
        raft_raw_node_t *normal = new_node(1, &normal_storage);

        assert_status_config(
            empty, NULL, 0, NULL, 0, NULL, 0, NULL, 0);
        assert_status_config(normal,
                             one_two,
                             2,
                             NULL,
                             0,
                             three,
                             1,
                             NULL,
                             0);
        raft_raw_node_destroy(normal);
        raft_raw_node_destroy(empty);
    }

    // Restore and restart retain the live tracker's AutoLeave, while Status
    // follows Go Config.Clone and omits it.
    {
        test_storage_t storage = {
            .hard_state = {.term = 2, .commit = 5},
            .voters = one_two,
            .voter_count = 2,
            .voters_outgoing = one_three,
            .voter_outgoing_count = 2,
            .learners = four,
            .learner_count = 1,
            .learners_next = three,
            .learner_next_count = 1,
            .auto_leave = true,
            .last_index = 5,
            .last_term = 2,
        };
        raft_raw_node_t *node = new_node(1, &storage);
        raft_status_t before = {0};
        raft_conf_state_t left;

        assert(node->raft.tracker.config.auto_leave);
        assert_status_config(node,
                             one_two,
                             2,
                             one_three,
                             2,
                             four,
                             1,
                             three,
                             1);
        assert(raft_raw_node_status(node, &before) == RAFT_OK);
        left = apply_status_config_change(
            node, RAFT_CONF_CHANGE_TRANSITION_AUTO, NULL, 0);
        assert(!left.auto_leave);
        assert(left.voters_outgoing.len == 0);
        assert_status_config(
            node, one_two, 2, NULL, 0, three_four, 2, NULL, 0);

        // The earlier status remains a point-in-time owned copy.
        assert_status_ids(&before.conf_state.voters_outgoing,
                          one_three,
                          2);
        assert_status_ids(&before.conf_state.learners_next, three, 1);
        assert(!before.conf_state.auto_leave);
        raft_conf_state_free(&left);
        raft_status_free(&before);
        raft_raw_node_destroy(node);

        node = new_node(1, &storage);
        assert(node->raft.tracker.config.auto_leave);
        assert_status_config(node,
                             one_two,
                             2,
                             one_three,
                             2,
                             four,
                             1,
                             three,
                             1);
        raft_raw_node_destroy(node);
    }

    // Explicit joint consensus stores false, while implicit joint consensus
    // stores true internally. Status reports false in both cases.
    {
        test_storage_t explicit_storage = {
            .hard_state = {.term = 1, .commit = 1},
            .voters = one,
            .voter_count = 1,
            .last_index = 1,
            .last_term = 1,
        };
        raft_raw_node_t *node = new_node(1, &explicit_storage);
        const raft_conf_change_single_t add_learner = {
            .type = RAFT_CONF_CHANGE_ADD_LEARNER_NODE,
            .node_id = 2,
        };
        raft_conf_state_t state = apply_status_config_change(
            node,
            RAFT_CONF_CHANGE_TRANSITION_JOINT_EXPLICIT,
            &add_learner,
            1);

        assert(!state.auto_leave);
        assert(!node->raft.tracker.config.auto_leave);
        assert_status_config(
            node, one, 1, one, 1, two, 1, NULL, 0);
        raft_conf_state_free(&state);
        raft_raw_node_destroy(node);
    }

    // AutoLeave survives term and leadership changes in the live tracker.
    // Once applied progress permits it, the leader proposes the empty V2
    // leave entry. Applying that entry clears both joint state and AutoLeave.
    {
        test_storage_t storage = {
            .hard_state = {.term = 1, .commit = 1},
            .voters = one,
            .voter_count = 1,
            .last_index = 1,
            .last_term = 1,
        };
        raft_raw_node_t *node;
        const raft_conf_change_single_t add_learner = {
            .type = RAFT_CONF_CHANGE_ADD_LEARNER_NODE,
            .node_id = 2,
        };
        raft_conf_state_t state;
        raft_message_view_t higher_term = {
            .type = RAFT_MSG_HEARTBEAT_RESP,
            .to = 1,
            .from = 1,
            .context = {NULL, 0, true},
        };
        raft_ready_t *ready = NULL;
        bool found_leave = false;
        size_t i;

        node = new_node(1, &storage);
        state = apply_status_config_change(
            node,
            RAFT_CONF_CHANGE_TRANSITION_JOINT_IMPLICIT,
            &add_learner,
            1);
        assert(state.auto_leave);
        assert(node->raft.tracker.config.auto_leave);
        assert_status_config(
            node, one, 1, one, 1, two, 1, NULL, 0);
        raft_conf_state_free(&state);

        assert(raft_raw_node_campaign(node) == RAFT_OK);
        accept_and_advance(node, &storage);
        assert(node->raft.state == RAFT_STATE_LEADER);
        higher_term.term = node->raft.term + 1;
        assert(raft_raw_node_step(node, &higher_term) == RAFT_OK);
        assert(node->raft.state == RAFT_STATE_FOLLOWER);
        assert(node->raft.tracker.config.auto_leave);
        assert_status_config(
            node, one, 1, one, 1, two, 1, NULL, 0);

        assert(raft_raw_node_campaign(node) == RAFT_OK);
        accept_and_advance(node, &storage);
        assert(node->raft.state == RAFT_STATE_LEADER);
        assert(node->raft.tracker.config.auto_leave);
        // The configuration was applied directly in this native unit test
        // instead of through a committed log entry. Model that entry as
        // applied before exercising the normal automatic-leave trigger.
        node->raft.pending_conf_index = node->log.applied;
        assert(raft_core_maybe_auto_leave(
                   &node->raft, node->log.applied) == RAFT_OK);
        assert(raft_raw_node_ready_without_accept(
                   node, &ready) == RAFT_OK);
        for (i = 0; i < ready->entries.len; ++i) {
            if (ready->entries.items[i].type ==
                    RAFT_ENTRY_CONF_CHANGE_V2 &&
                ready->entries.items[i].data.len == 0) {
                found_leave = true;
            }
        }
        assert(found_leave);
        raft_ready_destroy(ready);

        state = apply_status_config_change(
            node, RAFT_CONF_CHANGE_TRANSITION_AUTO, NULL, 0);
        assert(!state.auto_leave);
        assert(!node->raft.tracker.config.auto_leave);
        assert_status_config(
            node, one, 1, NULL, 0, two, 1, NULL, 0);
        raft_conf_state_free(&state);
        raft_raw_node_destroy(node);
    }

    // A live snapshot restore rebuilds AutoLeave from ConfState but does not
    // expose it through Status.Config.
    {
        test_storage_t storage = {
            .hard_state = {.term = 1, .commit = 1},
            .voters = one_two,
            .voter_count = 2,
            .last_index = 1,
            .last_term = 1,
        };
        raft_raw_node_t *node = new_node(1, &storage);
        const raft_message_view_t snapshot = {
            .type = RAFT_MSG_SNAP,
            .to = 1,
            .from = 2,
            .term = 3,
            .has_snapshot = true,
            .snapshot = {
                .data = {NULL, 0, true},
                .metadata = {
                    .conf_state = {
                        .voters = {one_two, 2},
                        .voters_outgoing = {one_three, 2},
                        .learners = {four, 1},
                        .learners_next = {three, 1},
                        .auto_leave = true,
                    },
                    .index = 5,
                    .term = 2,
                },
            },
            .context = {NULL, 0, true},
        };

        assert(raft_raw_node_step(node, &snapshot) == RAFT_OK);
        assert(node->raft.tracker.config.auto_leave);
        assert_status_config(node,
                             one_two,
                             2,
                             one_three,
                             2,
                             four,
                             1,
                             three,
                             1);
        raft_raw_node_destroy(node);
    }

    // Learner promotion/removal updates only the relevant membership maps,
    // and an earlier Status remains independent.
    {
        test_storage_t storage = {
            .hard_state = {.term = 1, .commit = 1},
            .voters = one,
            .voter_count = 1,
            .last_index = 1,
            .last_term = 1,
        };
        raft_raw_node_t *node = new_node(1, &storage);
        const raft_conf_change_single_t add_learner = {
            .type = RAFT_CONF_CHANGE_ADD_LEARNER_NODE,
            .node_id = 2,
        };
        const raft_conf_change_single_t promote = {
            .type = RAFT_CONF_CHANGE_ADD_NODE,
            .node_id = 2,
        };
        const raft_conf_change_single_t remove = {
            .type = RAFT_CONF_CHANGE_REMOVE_NODE,
            .node_id = 2,
        };
        raft_conf_state_t state;
        raft_status_t learner_status = {0};

        state = apply_status_config_change(
            node,
            RAFT_CONF_CHANGE_TRANSITION_AUTO,
            &add_learner,
            1);
        raft_conf_state_free(&state);
        assert_status_config(
            node, one, 1, NULL, 0, two, 1, NULL, 0);
        assert(raft_raw_node_status(node, &learner_status) == RAFT_OK);

        state = apply_status_config_change(
            node, RAFT_CONF_CHANGE_TRANSITION_AUTO, &promote, 1);
        raft_conf_state_free(&state);
        assert_status_config(
            node, one_two, 2, NULL, 0, NULL, 0, NULL, 0);

        state = apply_status_config_change(
            node, RAFT_CONF_CHANGE_TRANSITION_AUTO, &remove, 1);
        raft_conf_state_free(&state);
        assert(!raft_raw_node_has_progress(node, 2));
        assert_status_config(
            node, one, 1, NULL, 0, NULL, 0, NULL, 0);

        assert_status_ids(&learner_status.conf_state.voters, one, 1);
        assert_status_ids(
            &learner_status.conf_state.learners, two, 1);
        assert(!learner_status.conf_state.auto_leave);
        raft_status_free(&learner_status);
        raft_raw_node_destroy(node);
    }
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

static void test_status_progress_inflights(void) {
    const uint64_t voters[] = {1, 2};
    const uint8_t first_byte = 'a';
    const uint8_t second_byte = 'b';
    const raft_byte_view_t first_proposal = {
        .data = &first_byte,
        .len = 1,
        .is_nil = false,
    };
    const raft_byte_view_t second_proposal = {
        .data = &second_byte,
        .len = 1,
        .is_nil = false,
    };
    test_storage_t storage = {
        .hard_state = {.term = 1, .commit = 1},
        .voters = voters,
        .voter_count = 2,
        .last_index = 1,
        .last_term = 1,
    };
    raft_config_t cfg = config(1);
    raft_raw_node_t *node;
    raft_status_t before = {0};
    raft_status_t after = {0};
    const raft_status_progress_t *peer;
    raft_progress_internal_t *live_peer;
    const raft_message_view_t vote_response = {
        .type = RAFT_MSG_VOTE_RESP,
        .to = 1,
        .from = 2,
        .term = 2,
        .context = {NULL, 0, true},
    };
    const raft_message_view_t caught_up = {
        .type = RAFT_MSG_APP_RESP,
        .to = 1,
        .from = 2,
        .term = 2,
        .index = 2,
        .context = {NULL, 0, true},
    };
    const raft_message_view_t acknowledge_first = {
        .type = RAFT_MSG_APP_RESP,
        .to = 1,
        .from = 2,
        .term = 2,
        .index = 3,
        .context = {NULL, 0, true},
    };

    cfg.applied = 1;
    cfg.max_size_per_message = 1;
    cfg.max_inflight_messages = 2;
    cfg.max_inflight_bytes = 4;
    node = new_node_with_config(cfg, &storage);

    assert(raft_raw_node_campaign(node) == RAFT_OK);
    accept_and_advance(node, &storage);
    assert(raft_raw_node_step(node, &vote_response) == RAFT_OK);
    accept_and_advance(node, &storage);
    assert(raft_raw_node_step(node, &caught_up) == RAFT_OK);
    accept_and_advance(node, &storage);

    assert(raft_raw_node_propose(node, &first_proposal) == RAFT_OK);
    assert(raft_raw_node_propose(node, &second_proposal) == RAFT_OK);
    live_peer = raft_tracker_find(&node->raft.tracker, 2);
    assert(live_peer != NULL);
    assert(live_peer->state == RAFT_PROGRESS_STATE_REPLICATE);
    assert(live_peer->inflights.count == 2);

    assert(raft_raw_node_status(node, &before) == RAFT_OK);
    peer = status_progress_for(&before, 2);
    assert(peer->snapshot.type == RAFT_PROGRESS_PEER);
    assert(peer->snapshot.progress.state ==
           RAFT_PROGRESS_STATE_REPLICATE);
    assert(peer->snapshot.progress.message_flow_paused);
    assert(peer->inflights.size == 2);
    assert(peer->inflights.max_bytes == 4);
    assert(peer->inflights.len == 2);
    assert(peer->inflights.items != NULL);
    assert(peer->inflights.items[0].index == 3);
    assert(peer->inflights.items[0].bytes == 1);
    assert(peer->inflights.items[1].index == 4);
    assert(peer->inflights.items[1].bytes == 1);
    assert(live_peer->inflights.count == 2);

    assert(raft_raw_node_step(node, &acknowledge_first) == RAFT_OK);
    assert(raft_raw_node_status(node, &after) == RAFT_OK);
    peer = status_progress_for(&after, 2);
    assert(peer->snapshot.progress.match_index == 3);
    assert(peer->snapshot.progress.next_index == 5);
    assert(!peer->snapshot.progress.message_flow_paused);
    assert(peer->inflights.size == 2);
    assert(peer->inflights.max_bytes == 4);
    assert(peer->inflights.len == 1);
    assert(peer->inflights.items != NULL);
    assert(peer->inflights.items[0].index == 4);
    assert(peer->inflights.items[0].bytes == 1);

    // The earlier status remains an independent point-in-time copy.
    peer = status_progress_for(&before, 2);
    assert(peer->inflights.len == 2);
    assert(peer->inflights.items[0].index == 3);
    raft_status_free(&after);
    raft_status_free(&before);
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
    test_candidate_vote_ownership();
    test_raft_progress_reset_semantics();
    test_append_heartbeat_and_follower_proposal();
    test_heartbeat_commit_invariant();
    test_append_rejection_log_term_optimization();
    test_stale_append_response_semantics();
    test_commit_only_ready_does_not_require_sync();
    test_election_replication_and_step_layering();
    test_unsupported_configuration_is_explicit();
    test_pre_vote_election_and_stale_log();
    test_pre_vote_check_quorum_and_forget_leader();
    test_leadership_transfer_paths();
    test_demoted_leader_proposal_admission();
    test_transfer_forwarding_and_forced_campaign();
    test_safe_read_index_and_stepdown();
    test_lease_read_and_check_quorum();
    test_empty_message_proposal_semantics();
    test_empty_entry_proposal_semantics();
    test_status_config_autoleave_semantics();
    test_uncommitted_proposal_limit();
    test_status_progress_inflights();
    test_report_unreachable_paths();
    test_async_storage_write_protocol();
    return 0;
}
