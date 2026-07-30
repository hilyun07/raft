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

#include <assert.h>

static void add_voter(raft_progress_tracker_t *tracker,
                      uint64_t id,
                      uint64_t match) {
    raft_progress_internal_t *progress;
    assert(raft_tracker_add_progress(tracker, id, 1, false) == RAFT_OK);
    assert(raft_id_vec_insert(&tracker->config.voters, id) == RAFT_OK);
    progress = raft_tracker_find(tracker, id);
    assert(progress != NULL);
    progress->match_index = match;
    progress->next_index = match + 1;
}

static void test_majority_and_joint_quorums(void) {
    raft_progress_tracker_t tracker;
    raft_progress_internal_t *learner;
    const uint64_t matches[] = {10, 10, 8, 7, 7};
    size_t i;

    assert(raft_tracker_init(&tracker, 8, 0) == RAFT_OK);
    for (i = 0; i < 5; ++i) {
        add_voter(&tracker, (uint64_t)i + 1, matches[i]);
    }
    assert(raft_tracker_committed(&tracker) == 8);

    assert(raft_tracker_add_progress(&tracker, 6, 1, true) == RAFT_OK);
    assert(raft_id_vec_insert(&tracker.config.learners, 6) == RAFT_OK);
    learner = raft_tracker_find(&tracker, 6);
    assert(learner != NULL);
    learner->match_index = 100;
    learner->next_index = 101;
    assert(raft_tracker_committed(&tracker) == 8);

    raft_uint64_vec_free(&tracker.config.voters);
    assert(raft_id_vec_insert(&tracker.config.voters, 1) == RAFT_OK);
    assert(raft_id_vec_insert(&tracker.config.voters, 2) == RAFT_OK);
    assert(raft_id_vec_insert(&tracker.config.voters, 3) == RAFT_OK);
    assert(raft_id_vec_insert(
               &tracker.config.voters_outgoing, 3) == RAFT_OK);
    assert(raft_id_vec_insert(
               &tracker.config.voters_outgoing, 4) == RAFT_OK);
    assert(raft_id_vec_insert(
               &tracker.config.voters_outgoing, 5) == RAFT_OK);
    // Incoming commits at 10, outgoing at 7; joint consensus takes the
    // smaller committed index.
    assert(raft_tracker_committed(&tracker) == 7);

    raft_tracker_reset_votes(&tracker);
    raft_tracker_record_vote(&tracker, 1, true);
    raft_tracker_record_vote(&tracker, 2, true);
    assert(raft_tracker_vote_result(&tracker) == RAFT_VOTE_PENDING);
    raft_tracker_record_vote(&tracker, 3, true);
    raft_tracker_record_vote(&tracker, 4, true);
    assert(raft_tracker_vote_result(&tracker) == RAFT_VOTE_WON);

    raft_tracker_reset_votes(&tracker);
    raft_tracker_record_vote(&tracker, 1, true);
    raft_tracker_record_vote(&tracker, 2, true);
    raft_tracker_record_vote(&tracker, 3, false);
    raft_tracker_record_vote(&tracker, 4, false);
    raft_tracker_record_vote(&tracker, 5, false);
    assert(raft_tracker_vote_result(&tracker) == RAFT_VOTE_LOST);
    raft_tracker_free(&tracker);
}

static void test_progress_and_inflights(void) {
    raft_progress_internal_t progress;

    assert(raft_progress_init(&progress, 2, 5, false, 2, 100) == RAFT_OK);
    progress.match_index = 4;
    raft_progress_become_replicate(&progress);
    assert(progress.next_index == 5);
    assert(raft_progress_sent_entries(&progress, 2, 40) == RAFT_OK);
    assert(progress.next_index == 7);
    assert(progress.inflights.count == 1);
    assert(!raft_progress_is_paused(&progress));
    assert(raft_progress_sent_entries(&progress, 1, 60) == RAFT_OK);
    assert(progress.next_index == 8);
    assert(progress.inflights.count == 2);
    assert(raft_progress_is_paused(&progress));

    raft_inflights_free_le(&progress.inflights, 6);
    assert(progress.inflights.count == 1);
    assert(progress.inflights.bytes == 60);
    assert(raft_progress_maybe_update(&progress, 7));
    assert(progress.match_index == 7);
    assert(progress.next_index == 8);
    assert(!raft_progress_is_paused(&progress));

    assert(raft_progress_maybe_decr_to(&progress, 8, 5));
    assert(progress.next_index == 8);
    raft_progress_become_probe(&progress);
    assert(progress.state == RAFT_PROGRESS_STATE_PROBE);
    assert(progress.next_index == 8);
    assert(raft_progress_sent_entries(&progress, 1, 1) == RAFT_OK);
    assert(raft_progress_is_paused(&progress));
    assert(raft_progress_maybe_decr_to(&progress, 7, 3));
    assert(progress.next_index == 8);
    assert(!raft_progress_is_paused(&progress));

    raft_progress_free(&progress);
}

int main(void) {
    test_majority_and_joint_quorums();
    test_progress_and_inflights();
    return 0;
}
