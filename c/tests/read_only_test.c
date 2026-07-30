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

#include "read_only.h"

#include <assert.h>
#include <string.h>

static void put_le64(uint8_t *data, uint64_t value) {
    size_t i;
    for (i = 0; i < 8; ++i) {
        data[i] = (uint8_t)(value >> (8U * i));
    }
}

static raft_message_view_t read_request(uint64_t from,
                                        uint8_t *context,
                                        size_t len,
                                        raft_entry_view_t *entry) {
    raft_message_view_t message;
    memset(entry, 0, sizeof(*entry));
    entry->type = RAFT_ENTRY_NORMAL;
    entry->data.data = context;
    entry->data.len = len;
    entry->data.is_nil = false;
    memset(&message, 0, sizeof(message));
    message.type = RAFT_MSG_READ_INDEX;
    message.from = from;
    message.entries.items = entry;
    message.entries.len = 1;
    message.context.is_nil = true;
    return message;
}

static void add_voter(raft_progress_tracker_t *tracker, uint64_t id) {
    assert(raft_id_vec_insert(&tracker->config.voters, id) == RAFT_OK);
    assert(raft_tracker_add_progress(
               tracker, id, 1, false) == RAFT_OK);
}

static void test_ordered_confirmation_and_deep_copy(void) {
    raft_read_only_internal_t read_only;
    raft_progress_tracker_t tracker;
    raft_entry_view_t entry;
    uint8_t user_context[] = {'s', 'a', 'm', 'e'};
    raft_message_view_t request =
        read_request(2, user_context, sizeof(user_context), &entry);
    uint8_t encoded[8];
    raft_byte_view_t heartbeat_context = {
        .data = encoded,
        .len = sizeof(encoded),
        .is_nil = false,
    };
    uint64_t position;
    uint64_t confirmed;
    size_t count;

    assert(raft_read_only_init(
               &read_only, RAFT_READ_ONLY_SAFE) == RAFT_OK);
    assert(raft_tracker_init(&tracker, 16, UINT64_MAX) == RAFT_OK);
    add_voter(&tracker, 1);
    add_voter(&tracker, 2);
    add_voter(&tracker, 3);

    assert(raft_read_only_add_request(
               &read_only, 7, &request) == RAFT_OK);
    user_context[0] = 'X';
    assert(raft_read_only_request_at(&read_only, 0) != NULL);
    assert(raft_read_only_request_at(
               &read_only, 0)->request.entries.items[0].data.data[0] ==
           's');

    user_context[0] = 's';
    assert(raft_read_only_add_request(
               &read_only, 8, &request) == RAFT_OK);
    assert(raft_read_only_heartbeat_position(
        &read_only, &position));
    assert(position == 2);

    put_le64(encoded, position);
    assert(raft_read_only_recv_ack(
               &read_only, 1, &heartbeat_context) == RAFT_OK);
    assert(raft_read_only_confirmed(
               &read_only, &tracker, &count, &confirmed) == RAFT_OK);
    assert(count == 0);

    assert(raft_read_only_recv_ack(
               &read_only, 2, &heartbeat_context) == RAFT_OK);
    assert(raft_read_only_confirmed(
               &read_only, &tracker, &count, &confirmed) == RAFT_OK);
    assert(count == 2);
    assert(confirmed == 2);
    raft_read_only_advance(&read_only, count, confirmed);
    assert(read_only.request_len == 0);
    assert(read_only.confirmed_reads == 2);

    heartbeat_context.len = 1;
    assert(raft_read_only_recv_ack(
               &read_only, 2, &heartbeat_context) == RAFT_ERR_FATAL);

    raft_read_only_free(&read_only);
    raft_tracker_free(&tracker);
}

static void test_joint_quorum_requires_both_majorities(void) {
    raft_read_only_internal_t read_only;
    raft_progress_tracker_t tracker;
    raft_entry_view_t entry;
    uint8_t user_context[] = {'j'};
    raft_message_view_t request =
        read_request(1, user_context, sizeof(user_context), &entry);
    uint8_t encoded[8];
    raft_byte_view_t context = {
        .data = encoded,
        .len = sizeof(encoded),
        .is_nil = false,
    };
    uint64_t confirmed;
    size_t count;

    assert(raft_read_only_init(
               &read_only, RAFT_READ_ONLY_SAFE) == RAFT_OK);
    assert(raft_tracker_init(&tracker, 16, UINT64_MAX) == RAFT_OK);
    add_voter(&tracker, 1);
    add_voter(&tracker, 2);
    add_voter(&tracker, 3);
    assert(raft_id_vec_insert(
               &tracker.config.voters_outgoing, 1) == RAFT_OK);
    assert(raft_id_vec_insert(
               &tracker.config.voters_outgoing, 3) == RAFT_OK);
    assert(raft_id_vec_insert(
               &tracker.config.voters_outgoing, 4) == RAFT_OK);
    assert(raft_tracker_add_progress(
               &tracker, 4, 1, false) == RAFT_OK);

    assert(raft_read_only_add_request(
               &read_only, 9, &request) == RAFT_OK);
    put_le64(encoded, 1);
    assert(raft_read_only_recv_ack(
               &read_only, 1, &context) == RAFT_OK);
    assert(raft_read_only_recv_ack(
               &read_only, 2, &context) == RAFT_OK);
    assert(raft_read_only_confirmed(
               &read_only, &tracker, &count, &confirmed) == RAFT_OK);
    assert(count == 0);

    assert(raft_read_only_recv_ack(
               &read_only, 3, &context) == RAFT_OK);
    assert(raft_read_only_confirmed(
               &read_only, &tracker, &count, &confirmed) == RAFT_OK);
    assert(count == 1);
    assert(confirmed == 1);

    raft_read_only_free(&read_only);
    raft_tracker_free(&tracker);
}

int main(void) {
    test_ordered_confirmation_and_deep_copy();
    test_joint_quorum_requires_both_majorities();
    return 0;
}
