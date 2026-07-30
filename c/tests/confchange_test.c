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

#include <assert.h>
#include <string.h>

static void restore_three(raft_progress_tracker_t *tracker) {
    uint64_t voters[] = {1, 2, 3};
    raft_conf_state_t state = {
        .voters = {voters, 3},
    };
    assert(raft_tracker_init(tracker, 8, 0) == RAFT_OK);
    assert(raft_confchange_restore(tracker, &state, 10) == RAFT_OK);
    assert(raft_confchange_check_invariants(tracker) == RAFT_OK);
}

static int apply_changes(raft_progress_tracker_t *tracker,
                         raft_conf_change_transition_t transition,
                         raft_conf_change_single_t *changes,
                         size_t changes_len) {
    raft_decoded_conf_change_t change = {
        .transition = transition,
        .changes = changes,
        .changes_len = changes_len,
    };
    return raft_confchange_apply(tracker, &change, 10);
}

static void test_simple_and_transactional_changes(void) {
    raft_progress_tracker_t tracker;
    raft_conf_change_single_t add_learner = {
        .type = RAFT_CONF_CHANGE_ADD_LEARNER_NODE,
        .node_id = 4,
    };
    raft_conf_change_single_t promote = {
        .type = RAFT_CONF_CHANGE_ADD_NODE,
        .node_id = 4,
    };
    raft_conf_change_single_t invalid[] = {
        {.type = RAFT_CONF_CHANGE_REMOVE_NODE, .node_id = 1},
        {.type = RAFT_CONF_CHANGE_REMOVE_NODE, .node_id = 2},
        {.type = RAFT_CONF_CHANGE_REMOVE_NODE, .node_id = 3},
        {.type = RAFT_CONF_CHANGE_REMOVE_NODE, .node_id = 4},
    };
    raft_conf_change_single_t noop = {
        .type = RAFT_CONF_CHANGE_REMOVE_NODE,
        .node_id = RAFT_NONE,
    };

    restore_three(&tracker);
    assert(apply_changes(&tracker,
                         RAFT_CONF_CHANGE_TRANSITION_AUTO,
                         &add_learner,
                         1) == RAFT_OK);
    assert(raft_id_vec_contains(&tracker.config.learners, 4));
    assert(raft_tracker_find(&tracker, 4)->is_learner);

    assert(apply_changes(&tracker,
                         RAFT_CONF_CHANGE_TRANSITION_AUTO,
                         &promote,
                         1) == RAFT_OK);
    assert(raft_id_vec_contains(&tracker.config.voters, 4));
    assert(!raft_id_vec_contains(&tracker.config.learners, 4));
    assert(!raft_tracker_find(&tracker, 4)->is_learner);

    assert(apply_changes(&tracker,
                         RAFT_CONF_CHANGE_TRANSITION_AUTO,
                         invalid,
                         4) == RAFT_ERR_FATAL);
    // Changer operations are transactional on failure.
    assert(raft_id_vec_contains(&tracker.config.voters, 1));
    assert(raft_id_vec_contains(&tracker.config.voters, 2));
    assert(raft_id_vec_contains(&tracker.config.voters, 3));
    assert(raft_id_vec_contains(&tracker.config.voters, 4));

    assert(apply_changes(&tracker,
                         RAFT_CONF_CHANGE_TRANSITION_AUTO,
                         &noop,
                         1) == RAFT_OK);
    assert(tracker.config.voters.len == 4);
    raft_tracker_free(&tracker);
}

static void test_joint_demotion_preserves_progress(void) {
    raft_progress_tracker_t tracker;
    raft_conf_change_single_t demote[] = {
        {.type = RAFT_CONF_CHANGE_REMOVE_NODE, .node_id = 3},
        {.type = RAFT_CONF_CHANGE_ADD_LEARNER_NODE, .node_id = 3},
    };
    raft_decoded_conf_change_t leave = {
        .transition = RAFT_CONF_CHANGE_TRANSITION_AUTO,
    };
    raft_progress_internal_t *progress;

    restore_three(&tracker);
    progress = raft_tracker_find(&tracker, 3);
    assert(progress != NULL);
    progress->match_index = 8;
    progress->next_index = 9;

    assert(apply_changes(
               &tracker,
               RAFT_CONF_CHANGE_TRANSITION_JOINT_EXPLICIT,
               demote,
               2) == RAFT_OK);
    assert(raft_tracker_is_joint(&tracker));
    assert(raft_id_vec_contains(&tracker.config.voters_outgoing, 3));
    assert(!raft_id_vec_contains(&tracker.config.voters, 3));
    assert(raft_id_vec_contains(&tracker.config.learners_next, 3));
    progress = raft_tracker_find(&tracker, 3);
    assert(progress != NULL);
    assert(progress->match_index == 8);
    assert(progress->next_index == 9);
    assert(!progress->is_learner);

    assert(raft_confchange_apply(&tracker, &leave, 10) == RAFT_OK);
    assert(!raft_tracker_is_joint(&tracker));
    assert(raft_id_vec_contains(&tracker.config.learners, 3));
    progress = raft_tracker_find(&tracker, 3);
    assert(progress != NULL);
    assert(progress->match_index == 8);
    assert(progress->next_index == 9);
    assert(progress->is_learner);
    raft_tracker_free(&tracker);
}

static void test_configuration_changes_preserve_vote_history(void) {
    raft_conf_change_single_t remove = {
        .type = RAFT_CONF_CHANGE_REMOVE_NODE,
        .node_id = 2,
    };
    raft_conf_change_single_t add = {
        .type = RAFT_CONF_CHANGE_ADD_NODE,
        .node_id = 2,
    };
    raft_progress_tracker_t tracker;

    // A grant survives the transactional remove/add Progress replacement.
    restore_three(&tracker);
    assert(raft_tracker_record_vote(&tracker, 2, true) == RAFT_OK);
    assert(apply_changes(&tracker,
                         RAFT_CONF_CHANGE_TRANSITION_AUTO,
                         &remove,
                         1) == RAFT_OK);
    assert(raft_tracker_find(&tracker, 2) == NULL);
    assert(apply_changes(&tracker,
                         RAFT_CONF_CHANGE_TRANSITION_AUTO,
                         &add,
                         1) == RAFT_OK);
    assert(raft_tracker_find(&tracker, 2) != NULL);
    assert(raft_tracker_record_vote(&tracker, 1, true) == RAFT_OK);
    assert(raft_tracker_vote_result(&tracker) == RAFT_VOTE_WON);
    raft_tracker_free(&tracker);

    // A rejection survives the same replacement.
    restore_three(&tracker);
    assert(raft_tracker_record_vote(&tracker, 2, false) == RAFT_OK);
    assert(apply_changes(&tracker,
                         RAFT_CONF_CHANGE_TRANSITION_AUTO,
                         &remove,
                         1) == RAFT_OK);
    assert(apply_changes(&tracker,
                         RAFT_CONF_CHANGE_TRANSITION_AUTO,
                         &add,
                         1) == RAFT_OK);
    assert(raft_tracker_record_vote(&tracker, 1, false) == RAFT_OK);
    assert(raft_tracker_vote_result(&tracker) == RAFT_VOTE_LOST);
    raft_tracker_free(&tracker);

    // Reset starts a new vote round even after a retained vote was cloned
    // through both configuration changes.
    restore_three(&tracker);
    assert(raft_tracker_record_vote(&tracker, 2, true) == RAFT_OK);
    assert(apply_changes(&tracker,
                         RAFT_CONF_CHANGE_TRANSITION_AUTO,
                         &remove,
                         1) == RAFT_OK);
    assert(apply_changes(&tracker,
                         RAFT_CONF_CHANGE_TRANSITION_AUTO,
                         &add,
                         1) == RAFT_OK);
    raft_tracker_reset_votes(&tracker);
    assert(raft_tracker_record_vote(&tracker, 1, true) == RAFT_OK);
    assert(raft_tracker_vote_result(&tracker) == RAFT_VOTE_PENDING);
    assert(raft_tracker_record_vote(&tracker, 3, true) == RAFT_OK);
    assert(raft_tracker_vote_result(&tracker) == RAFT_VOTE_WON);
    raft_tracker_free(&tracker);

    // A retained vote from a peer that stays removed is history only and is
    // not counted by the current two-voter configuration.
    restore_three(&tracker);
    assert(raft_tracker_record_vote(&tracker, 2, true) == RAFT_OK);
    assert(apply_changes(&tracker,
                         RAFT_CONF_CHANGE_TRANSITION_AUTO,
                         &remove,
                         1) == RAFT_OK);
    assert(raft_tracker_record_vote(&tracker, 1, true) == RAFT_OK);
    assert(raft_tracker_vote_result(&tracker) == RAFT_VOTE_PENDING);
    assert(raft_tracker_record_vote(&tracker, 3, true) == RAFT_OK);
    assert(raft_tracker_vote_result(&tracker) == RAFT_VOTE_WON);
    raft_tracker_free(&tracker);
}

static void test_protobuf_round_trip(void) {
    const uint8_t context[] = {4, 5};
    raft_conf_change_view_t v1 = {
        .id = 9,
        .type = RAFT_CONF_CHANGE_REMOVE_NODE,
        .node_id = 3,
        .context = {context, sizeof(context), false},
    };
    raft_conf_change_single_t singles[] = {
        {.type = RAFT_CONF_CHANGE_ADD_NODE, .node_id = 4},
        {.type = RAFT_CONF_CHANGE_REMOVE_NODE, .node_id = 2},
    };
    raft_conf_change_v2_view_t v2 = {
        .transition = RAFT_CONF_CHANGE_TRANSITION_JOINT_IMPLICIT,
        .changes = singles,
        .changes_len = 2,
        .context = {context, sizeof(context), false},
    };
    raft_bytes_t encoded;
    raft_byte_view_t view;
    raft_decoded_conf_change_t decoded;

    memset(&encoded, 0, sizeof(encoded));
    assert(raft_confchange_encode_v1(&v1, &encoded) == RAFT_OK);
    view = (raft_byte_view_t){
        encoded.data, encoded.len, encoded.is_nil,
    };
    assert(raft_confchange_decode_entry(
               RAFT_ENTRY_CONF_CHANGE, &view, &decoded) == RAFT_OK);
    assert(decoded.changes_len == 1);
    assert(decoded.changes[0].type == RAFT_CONF_CHANGE_REMOVE_NODE);
    assert(decoded.changes[0].node_id == 3);
    raft_decoded_conf_change_free(&decoded);
    raft_bytes_free(&encoded);

    memset(&encoded, 0, sizeof(encoded));
    assert(raft_confchange_encode_v2(&v2, &encoded) == RAFT_OK);
    view = (raft_byte_view_t){
        encoded.data, encoded.len, encoded.is_nil,
    };
    assert(raft_confchange_decode_entry(
               RAFT_ENTRY_CONF_CHANGE_V2, &view, &decoded) == RAFT_OK);
    assert(decoded.transition ==
           RAFT_CONF_CHANGE_TRANSITION_JOINT_IMPLICIT);
    assert(decoded.changes_len == 2);
    assert(decoded.changes[0].node_id == 4);
    assert(decoded.changes[1].node_id == 2);
    raft_decoded_conf_change_free(&decoded);
    raft_bytes_free(&encoded);
}

int main(void) {
    test_simple_and_transactional_changes();
    test_joint_demotion_preserves_progress();
    test_configuration_changes_preserve_vote_history();
    test_protobuf_round_trip();
    return 0;
}
