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

static int storage_initial_state(uintptr_t handle,
                                 raft_hard_state_t *hard_state,
                                 raft_conf_state_t *conf_state) {
    (void)handle;
    memset(hard_state, 0, sizeof(*hard_state));
    memset(conf_state, 0, sizeof(*conf_state));
    return RAFT_OK;
}

static int storage_entries(uintptr_t handle,
                           uint64_t lo,
                           uint64_t hi,
                           uint64_t max_size,
                           raft_entry_vec_t *entries) {
    (void)handle;
    (void)lo;
    (void)hi;
    (void)max_size;
    memset(entries, 0, sizeof(*entries));
    return RAFT_OK;
}

static int storage_term(uintptr_t handle, uint64_t index, uint64_t *term) {
    (void)handle;
    (void)index;
    *term = 0;
    return RAFT_OK;
}

static int storage_first_index(uintptr_t handle, uint64_t *index) {
    (void)handle;
    *index = 1;
    return RAFT_OK;
}

static int storage_last_index(uintptr_t handle, uint64_t *index) {
    (void)handle;
    *index = 0;
    return RAFT_OK;
}

static int storage_snapshot(uintptr_t handle, raft_snapshot_t *snapshot) {
    (void)handle;
    memset(snapshot, 0, sizeof(*snapshot));
    return RAFT_OK;
}

static raft_storage_ops_t test_storage(void) {
    raft_storage_ops_t storage = {
        .handle = 1,
        .initial_state = storage_initial_state,
        .entries = storage_entries,
        .term = storage_term,
        .first_index = storage_first_index,
        .last_index = storage_last_index,
        .snapshot = storage_snapshot,
    };
    return storage;
}

static raft_config_t test_config(void) {
    raft_config_t config = {
        .id = 1,
        .election_tick = 10,
        .heartbeat_tick = 1,
        .max_size_per_message = UINT64_MAX,
        .max_committed_size_per_ready = UINT64_MAX,
        .max_inflight_messages = 1,
        .read_only_option = RAFT_READ_ONLY_SAFE,
    };
    return config;
}

static const raft_byte_view_t *nil_view(void) {
    static const raft_byte_view_t view = {
        .data = NULL,
        .len = 0,
        .is_nil = true,
    };
    return &view;
}

static raft_bytes_t nil_bytes(void) {
    raft_bytes_t bytes = {
        .data = NULL,
        .len = 0,
        .is_nil = true,
    };
    return bytes;
}

static void test_node_id_sentinels(void) {
    assert(RAFT_NONE == UINT64_C(0));
    assert(RAFT_LOCAL_APPEND_THREAD == UINT64_MAX);
    assert(RAFT_LOCAL_APPLY_THREAD == UINT64_MAX - UINT64_C(1));

    assert(raft_is_none_id(RAFT_NONE));
    assert(!raft_is_none_id(1));
    assert(!raft_is_none_id(RAFT_LOCAL_APPEND_THREAD));
    assert(!raft_is_none_id(RAFT_LOCAL_APPLY_THREAD));

    assert(!raft_is_local_target_id(RAFT_NONE));
    assert(!raft_is_local_target_id(1));
    assert(raft_is_local_target_id(RAFT_LOCAL_APPEND_THREAD));
    assert(raft_is_local_target_id(RAFT_LOCAL_APPLY_THREAD));

    assert(!raft_is_valid_node_id(RAFT_NONE));
    assert(!raft_is_valid_node_id(RAFT_LOCAL_APPEND_THREAD));
    assert(!raft_is_valid_node_id(RAFT_LOCAL_APPLY_THREAD));
    assert(raft_is_valid_node_id(1));
    assert(raft_is_valid_node_id(UINT64_MAX - UINT64_C(2)));
}

static void test_wire_enum_values(void) {
    assert(RAFT_ENTRY_NORMAL == 0);
    assert(RAFT_ENTRY_CONF_CHANGE == 1);
    assert(RAFT_ENTRY_CONF_CHANGE_V2 == 2);

    assert(RAFT_MSG_HUP == 0);
    assert(RAFT_MSG_APP == 3);
    assert(RAFT_MSG_SNAP == 7);
    assert(RAFT_MSG_TRANSFER_LEADER == 13);
    assert(RAFT_MSG_READ_INDEX == 15);
    assert(RAFT_MSG_STORAGE_APPEND == 19);
    assert(RAFT_MSG_STORAGE_APPLY_RESP == 22);
    assert(RAFT_MSG_FORGET_LEADER == 23);

    assert(RAFT_CONF_CHANGE_ADD_NODE == 0);
    assert(RAFT_CONF_CHANGE_REMOVE_NODE == 1);
    assert(RAFT_CONF_CHANGE_UPDATE_NODE == 2);
    assert(RAFT_CONF_CHANGE_ADD_LEARNER_NODE == 3);

    assert(RAFT_CONF_CHANGE_TRANSITION_AUTO == 0);
    assert(RAFT_CONF_CHANGE_TRANSITION_JOINT_IMPLICIT == 1);
    assert(RAFT_CONF_CHANGE_TRANSITION_JOINT_EXPLICIT == 2);

    assert(RAFT_STATE_FOLLOWER == 0);
    assert(RAFT_STATE_PRE_CANDIDATE == 3);
    assert(RAFT_PROGRESS_PEER == 0);
    assert(RAFT_PROGRESS_LEARNER == 1);
    assert(RAFT_PROGRESS_STATE_PROBE == 0);
    assert(RAFT_PROGRESS_STATE_SNAPSHOT == 2);
    assert(RAFT_READ_ONLY_SAFE == 0);
    assert(RAFT_READ_ONLY_LEASE_BASED == 1);
    assert(RAFT_SNAPSHOT_FINISH == 1);
    assert(RAFT_SNAPSHOT_FAILURE == 2);
}

static void test_lifecycle_and_minimal_core(void) {
    const uint8_t byte = 1;
    raft_config_t config = test_config();
    raft_storage_ops_t storage = test_storage();
    raft_raw_node_t *raw_node = NULL;
    raft_peer_view_t peer = {
        .id = 1,
        .context = {NULL, 0, true},
    };
    raft_ready_t *ready = NULL;
    raft_basic_status_t basic_status = {0};
    raft_status_t status = {0};
    raft_conf_change_v2_view_t conf_change = {
        .transition = RAFT_CONF_CHANGE_TRANSITION_AUTO,
        .context = {NULL, 0, true},
    };
    raft_conf_state_t conf_state = {0};
    raft_message_view_t message = {
        .type = RAFT_MSG_HUP,
        .context = {NULL, 0, true},
    };
    raft_message_view_t transfer_message = {
        .type = RAFT_MSG_TRANSFER_LEADER,
        .from = 2,
        .to = 1,
        .context = {NULL, 0, true},
    };
    raft_progress_snapshot_t dummy_snapshot = {0};
    raft_progress_snapshot_t *snapshots = &dummy_snapshot;
    size_t snapshot_len = 1;

    assert(raft_raw_node_new(&config, &storage, &raw_node) == RAFT_OK);
    assert(raw_node != NULL);
    assert(!raft_raw_node_has_ready(raw_node));

    raft_raw_node_tick_quiesced(raw_node);

    assert(raft_raw_node_basic_status(raw_node, &basic_status) == RAFT_OK);
    assert(basic_status.id == config.id);
    assert(basic_status.hard_state.vote == RAFT_NONE);
    assert(basic_status.soft_state.lead == RAFT_NONE);
    assert(basic_status.soft_state.raft_state == RAFT_STATE_FOLLOWER);
    assert(basic_status.lead_transferee == RAFT_NONE);

    assert(raft_raw_node_status(raw_node, &status) == RAFT_OK);
    assert(status.basic.id == config.id);
    assert(status.progress == NULL);
    assert(status.progress_len == 0);

    // Membership calls are active in Phase 8. Without a leader, proposals
    // are dropped.
    assert(raft_raw_node_propose_conf_change(raw_node, &conf_change) ==
           RAFT_ERR_PROPOSAL_DROPPED);
    assert(raft_raw_node_read_index(
               raw_node, &(raft_byte_view_t){&byte, 1, 0}) ==
           RAFT_OK);
    assert(raft_raw_node_transfer_leader(raw_node, 2) ==
           RAFT_OK);

    assert(raft_raw_node_bootstrap(raw_node, &peer, 1) == RAFT_OK);
    assert(raft_raw_node_has_progress(raw_node, 1));
    // The committed bootstrap configuration must be applied before this
    // node is eligible to campaign.
    assert(raft_raw_node_campaign(raw_node) == RAFT_OK);
    assert(raft_raw_node_propose(
               raw_node, &(raft_byte_view_t){&byte, 1, 0}) ==
           RAFT_ERR_PROPOSAL_DROPPED);
    assert(raft_raw_node_propose(raw_node, nil_view()) ==
           RAFT_ERR_PROPOSAL_DROPPED);
    assert(raft_raw_node_has_ready(raw_node));

    // Public RawNode.Step rejects a locally generated message whose sender is
    // not a local target. The Node actor helper intentionally bypasses it.
    assert(raft_raw_node_step(raw_node, &message) ==
           RAFT_ERR_STEP_LOCAL_MSG);
    assert(raft_raw_node_step_for_node(raw_node, &message) == RAFT_OK);

    assert(raft_raw_node_ready_without_accept(raw_node, &ready) == RAFT_OK);
    assert(ready != NULL);
    assert(ready->entries.len == 1);
    assert(ready->committed_entries.len == 1);
    assert(ready->has_hard_state);
    assert(ready->hard_state.term == 1);
    assert(ready->hard_state.vote == RAFT_NONE);
    assert(ready->hard_state.commit == 1);
    assert(!ready->has_soft_state);
    assert(raft_raw_node_accept_ready(raw_node, ready) == RAFT_OK);
    raft_ready_destroy(ready);
    ready = NULL;
    assert(raft_raw_node_advance(raw_node) == RAFT_OK);

    assert(raft_raw_node_progress_snapshot(raw_node, &snapshots,
                                           &snapshot_len) == RAFT_OK);
    assert(snapshots != NULL);
    assert(snapshot_len == 1);
    assert(snapshots[0].id == 1);
    assert(snapshots[0].type == RAFT_PROGRESS_PEER);
    raft_progress_snapshot_array_free(snapshots, snapshot_len);
    snapshots = NULL;
    snapshot_len = 0;
    assert(raft_raw_node_forget_leader(raw_node) == RAFT_OK);
    // Go Node.TransferLeadership routes this message through the Node actor
    // helper; it does not require a separate two-ID C RawNode function.
    assert(raft_raw_node_step_for_node(raw_node, &transfer_message) ==
           RAFT_OK);
    assert(raft_raw_node_report_unreachable(raw_node, 2) == RAFT_OK);
    assert(raft_raw_node_report_snapshot(raw_node, 2,
                                         RAFT_SNAPSHOT_FAILURE) ==
           RAFT_OK);

    raft_ready_destroy(ready);
    raft_status_free(&status);
    raft_conf_state_free(&conf_state);
    raft_raw_node_destroy(raw_node);
}

static void test_invalid_arguments(void) {
    const uint8_t byte = 1;
    raft_config_t config = test_config();
    raft_storage_ops_t storage = test_storage();
    raft_raw_node_t *raw_node = NULL;
    raft_ready_t *ready = (raft_ready_t *)(uintptr_t)1;
    raft_basic_status_t basic_status = {0};
    raft_status_t status = {0};
    raft_progress_snapshot_t *snapshots = NULL;
    size_t snapshot_len = 0;
    const uint64_t reserved_ids[] = {
        RAFT_NONE,
        RAFT_LOCAL_APPEND_THREAD,
        RAFT_LOCAL_APPLY_THREAD,
    };
    size_t i;

    assert(raft_raw_node_new(NULL, &storage, &raw_node) ==
           RAFT_ERR_INVALID_ARGUMENT);
    assert(raft_raw_node_new(&config, NULL, &raw_node) ==
           RAFT_ERR_INVALID_ARGUMENT);
    assert(raft_raw_node_new(&config, &storage, NULL) ==
           RAFT_ERR_INVALID_ARGUMENT);

    for (i = 0; i < sizeof(reserved_ids) / sizeof(reserved_ids[0]); ++i) {
        config = test_config();
        config.id = reserved_ids[i];
        assert(raft_raw_node_new(&config, &storage, &raw_node) ==
               RAFT_ERR_INVALID_ARGUMENT);
        assert(raw_node == NULL);
    }

    config = test_config();
    config.read_only_option = RAFT_READ_ONLY_LEASE_BASED;
    assert(raft_raw_node_new(&config, &storage, &raw_node) ==
           RAFT_ERR_INVALID_ARGUMENT);
    config.check_quorum = true;
    assert(raft_raw_node_new(&config, &storage, &raw_node) == RAFT_OK);
    raft_raw_node_destroy(raw_node);
    raw_node = NULL;
    config.read_only_option = (raft_read_only_option_t)99;
    assert(raft_raw_node_new(&config, &storage, &raw_node) ==
           RAFT_ERR_INVALID_ARGUMENT);

    assert(raft_raw_node_campaign(NULL) == RAFT_ERR_INVALID_ARGUMENT);
    assert(raft_raw_node_propose(NULL, nil_view()) ==
           RAFT_ERR_INVALID_ARGUMENT);
    assert(raft_raw_node_propose((raft_raw_node_t *)(uintptr_t)1, NULL) ==
           RAFT_ERR_INVALID_ARGUMENT);
    assert(raft_raw_node_propose(
               (raft_raw_node_t *)(uintptr_t)1,
               &(raft_byte_view_t){NULL, 1, 0}) ==
           RAFT_ERR_INVALID_ARGUMENT);
    assert(raft_raw_node_propose(
               (raft_raw_node_t *)(uintptr_t)1,
               &(raft_byte_view_t){&byte, 1, true}) ==
           RAFT_ERR_INVALID_ARGUMENT);
    assert(raft_raw_node_read_index(
               (raft_raw_node_t *)(uintptr_t)1, NULL) ==
           RAFT_ERR_INVALID_ARGUMENT);
    assert(raft_raw_node_step_for_node(NULL,
                                      &(raft_message_view_t){
                                          .context = {NULL, 0, true},
                                      }) ==
           RAFT_ERR_INVALID_ARGUMENT);
    assert(raft_raw_node_ready(NULL, &ready) == RAFT_ERR_INVALID_ARGUMENT);
    assert(ready == NULL);
    assert(raft_raw_node_ready((raft_raw_node_t *)(uintptr_t)1, NULL) ==
           RAFT_ERR_INVALID_ARGUMENT);
    assert(raft_raw_node_advance(NULL) == RAFT_ERR_INVALID_ARGUMENT);
    assert(raft_raw_node_basic_status(NULL, &basic_status) ==
           RAFT_ERR_INVALID_ARGUMENT);
    assert(raft_raw_node_basic_status((raft_raw_node_t *)(uintptr_t)1, NULL) ==
           RAFT_ERR_INVALID_ARGUMENT);
    assert(raft_raw_node_status(NULL, &status) ==
           RAFT_ERR_INVALID_ARGUMENT);
    assert(raft_raw_node_progress_snapshot(NULL, &snapshots, &snapshot_len) ==
           RAFT_ERR_INVALID_ARGUMENT);
    assert(raft_raw_node_progress_snapshot(
               (raft_raw_node_t *)(uintptr_t)1, NULL, &snapshot_len) ==
           RAFT_ERR_INVALID_ARGUMENT);
    assert(raft_raw_node_progress_snapshot(
               (raft_raw_node_t *)(uintptr_t)1, &snapshots, NULL) ==
           RAFT_ERR_INVALID_ARGUMENT);
    assert(!raft_raw_node_has_progress(NULL, 1));
    assert(!raft_raw_node_has_progress(
        (raft_raw_node_t *)(uintptr_t)1, RAFT_NONE));
    assert(raft_raw_node_forget_leader(NULL) == RAFT_ERR_INVALID_ARGUMENT);
    assert(raft_raw_node_transfer_leader(NULL, 2) ==
           RAFT_ERR_INVALID_ARGUMENT);

    raft_raw_node_tick(NULL);
    raft_raw_node_tick_quiesced(NULL);
    raft_raw_node_destroy(NULL);
}

static void test_node_id_boundary_validation(void) {
    raft_config_t config = test_config();
    raft_storage_ops_t storage = test_storage();
    raft_raw_node_t *raw_node = NULL;
    raft_conf_change_single_t change = {
        .type = RAFT_CONF_CHANGE_ADD_NODE,
        .node_id = 2,
    };
    raft_conf_change_v2_view_t conf_change = {
        .transition = RAFT_CONF_CHANGE_TRANSITION_AUTO,
        .changes = &change,
        .changes_len = 1,
        .context = {NULL, 0, true},
    };
    raft_conf_state_t conf_state = {0};
    raft_message_view_t message = {
        .type = RAFT_MSG_HUP,
        .context = {NULL, 0, true},
    };
    raft_peer_view_t peer = {
        .id = 2,
        .context = {NULL, 0, true},
    };
    const uint64_t reserved_ids[] = {
        RAFT_NONE,
        RAFT_LOCAL_APPEND_THREAD,
        RAFT_LOCAL_APPLY_THREAD,
    };
    size_t i;

    assert(raft_raw_node_new(&config, &storage, &raw_node) == RAFT_OK);

    // Message IDs are contextual. The inert core receives these shapes rather
    // than applying a blanket real-node validation rule.
    message.from = RAFT_NONE;
    message.to = RAFT_NONE;
    assert(raft_raw_node_step(raw_node, &message) ==
           RAFT_ERR_STEP_LOCAL_MSG);
    message.from = RAFT_LOCAL_APPEND_THREAD;
    message.to = RAFT_LOCAL_APPLY_THREAD;
    assert(raft_raw_node_step(raw_node, &message) == RAFT_OK);

    for (i = 0; i < sizeof(reserved_ids) / sizeof(reserved_ids[0]); ++i) {
        change.node_id = reserved_ids[i];
        if (change.node_id == RAFT_NONE) {
            assert(raft_raw_node_propose_conf_change(
                       raw_node, &conf_change) ==
                   RAFT_ERR_PROPOSAL_DROPPED);
        } else {
            assert(raft_raw_node_propose_conf_change(
                       raw_node, &conf_change) ==
                   RAFT_ERR_INVALID_ARGUMENT);
            assert(raft_raw_node_apply_conf_change(
                       raw_node, &conf_change, &conf_state) ==
                   RAFT_ERR_INVALID_ARGUMENT);
        }
        peer.id = reserved_ids[i];
        assert(raft_raw_node_bootstrap(raw_node, &peer, 1) ==
               RAFT_ERR_INVALID_ARGUMENT);
        assert(raft_raw_node_transfer_leader(raw_node, reserved_ids[i]) ==
               RAFT_ERR_INVALID_ARGUMENT);
        assert(raft_raw_node_report_unreachable(raw_node, reserved_ids[i]) ==
               RAFT_ERR_INVALID_ARGUMENT);
        assert(raft_raw_node_report_snapshot(raw_node, reserved_ids[i],
                                             RAFT_SNAPSHOT_FINISH) ==
               RAFT_ERR_INVALID_ARGUMENT);
    }

    change.node_id = 2;
    assert(raft_raw_node_propose_conf_change(raw_node, &conf_change) ==
           RAFT_ERR_PROPOSAL_DROPPED);
    assert(raft_raw_node_apply_conf_change(raw_node, &conf_change,
                                           &conf_state) ==
           RAFT_OK);
    raft_conf_state_free(&conf_state);

    // Empty V2 changes can represent leaving joint configuration and have no
    // member ID to validate.
    conf_change.changes = NULL;
    conf_change.changes_len = 0;
    assert(raft_raw_node_propose_conf_change(raw_node, &conf_change) ==
           RAFT_ERR_PROPOSAL_DROPPED);

    // A non-empty list must provide its change records.
    conf_change.changes_len = 1;
    assert(raft_raw_node_propose_conf_change(raw_node, &conf_change) ==
           RAFT_ERR_INVALID_ARGUMENT);

    // Invalid configuration application is a panic in Go. The C API returns
    // and latches its terminal result so bindings can surface the panic.
    conf_change.changes = NULL;
    conf_change.changes_len = 0;
    assert(raft_raw_node_apply_conf_change(
               raw_node, &conf_change, &conf_state) == RAFT_ERR_FATAL);
    assert(raft_raw_node_error(raw_node) == RAFT_ERR_FATAL);
    assert(raft_raw_node_campaign(raw_node) == RAFT_ERR_FATAL);

    raft_conf_state_free(&conf_state);
    raft_raw_node_destroy(raw_node);
}

static void test_nil_empty_validation(void) {
    uint8_t byte = 1;
    raft_config_t config = test_config();
    raft_storage_ops_t storage = test_storage();
    raft_raw_node_t *raw_node = NULL;
    raft_peer_view_t peer = {
        .id = 2,
        .context = {NULL, 0, 0},
    };
    raft_byte_view_t empty_view = {NULL, 0, 0};
    raft_byte_view_t nonempty_view = {&byte, 1, 0};
    raft_bytes_t nil_owned = {NULL, 0, true};
    raft_bytes_t empty_owned = {NULL, 0, false};
    raft_bytes_t nonempty_owned = {malloc(1), 1, false};

    assert(raft_raw_node_new(&config, &storage, &raw_node) == RAFT_OK);
    assert(nonempty_owned.data != NULL);
    nonempty_owned.data[0] = byte;

    assert(!raft_byte_view_valid(NULL));
    assert(raft_byte_view_valid(nil_view()));
    assert(raft_byte_view_valid(&empty_view));
    assert(raft_byte_view_valid(&nonempty_view));
    assert(!raft_byte_view_valid(&(raft_byte_view_t){NULL, 1, 0}));
    assert(!raft_byte_view_valid(&(raft_byte_view_t){&byte, 0, 0}));
    assert(!raft_byte_view_valid(
        &(raft_byte_view_t){&byte, 1, true}));

    assert(raft_bytes_valid(&nil_owned));
    assert(raft_bytes_valid(&empty_owned));
    assert(raft_bytes_valid(&nonempty_owned));
    assert(!raft_bytes_valid(NULL));
    assert(!raft_bytes_valid(&(raft_bytes_t){NULL, 1, 0}));
    assert(!raft_bytes_valid(
        &(raft_bytes_t){(uint8_t *)(uintptr_t)&byte, 0, 0}));
    assert(!raft_bytes_valid(
        &(raft_bytes_t){(uint8_t *)(uintptr_t)&byte, 1, true}));

    assert(raft_raw_node_propose(raw_node, &empty_view) ==
           RAFT_ERR_PROPOSAL_DROPPED);
    assert(raft_raw_node_propose(raw_node, nil_view()) ==
           RAFT_ERR_PROPOSAL_DROPPED);
    assert(raft_raw_node_propose(
               raw_node, &(raft_byte_view_t){NULL, 1, 0}) ==
           RAFT_ERR_INVALID_ARGUMENT);
    assert(raft_raw_node_propose(
               raw_node,
               &(raft_byte_view_t){&byte, 1, true}) ==
           RAFT_ERR_INVALID_ARGUMENT);
    assert(raft_raw_node_propose_from_parts(raw_node, NULL, 0, true) ==
           RAFT_ERR_PROPOSAL_DROPPED);
    assert(raft_raw_node_propose_from_parts(raw_node, NULL, 0, false) ==
           RAFT_ERR_PROPOSAL_DROPPED);
    assert(raft_raw_node_propose_from_parts(raw_node, NULL, 1, false) ==
           RAFT_ERR_INVALID_ARGUMENT);

    assert(raft_raw_node_read_index(raw_node, &empty_view) ==
           RAFT_OK);
    assert(raft_raw_node_read_index(raw_node, nil_view()) ==
           RAFT_OK);
    assert(raft_raw_node_read_index(
               raw_node, &(raft_byte_view_t){NULL, 1, 0}) ==
           RAFT_ERR_INVALID_ARGUMENT);
    assert(raft_raw_node_read_index_from_parts(raw_node, NULL, 0, true) ==
           RAFT_OK);
    assert(raft_raw_node_read_index_from_parts(raw_node, NULL, 0, false) ==
           RAFT_OK);
    assert(raft_raw_node_read_index_from_parts(raw_node, NULL, 1, false) ==
           RAFT_ERR_INVALID_ARGUMENT);

    assert(raft_raw_node_bootstrap(raw_node, &peer, 1) == RAFT_OK);
    peer.context.data = (const uint8_t *)&peer.id;
    peer.context.len = 1;
    peer.context.is_nil = true;
    assert(raft_raw_node_bootstrap(raw_node, &peer, 1) ==
           RAFT_ERR_INVALID_ARGUMENT);

    raft_bytes_free(&nonempty_owned);
    raft_raw_node_destroy(raw_node);
}

static void test_copy_helpers(void) {
    uint8_t source_data[] = {1, 2, 3};
    uint8_t unknown_data[] = {0xa0, 0x06, 0x07};
    uint64_t voters[] = {1, 2};
    raft_byte_view_t source = {source_data, sizeof(source_data), false};
    raft_bytes_t copied = {0};
    raft_bytes_t copied_again = {0};
    raft_entry_view_t entry_view = {
        .type = RAFT_ENTRY_NORMAL,
        .term = 2,
        .index = 3,
        .data = {source_data, sizeof(source_data), false},
        .protobuf = {
            .fields = RAFT_ENTRY_PROTO_TYPE |
                      RAFT_ENTRY_PROTO_TERM |
                      RAFT_ENTRY_PROTO_INDEX,
            .unknown_fields = {
                unknown_data, sizeof(unknown_data), false,
            },
        },
    };
    raft_entry_t entry = {0};
    raft_snapshot_view_t snapshot_view = {
        .data = {source_data, sizeof(source_data), false},
        .metadata = {
            .conf_state = {
                .voters = {voters, 2},
            },
            .index = 4,
            .term = 2,
            .protobuf = {
                .fields = RAFT_SNAPSHOT_METADATA_PROTO_CONF_STATE |
                          RAFT_SNAPSHOT_METADATA_PROTO_INDEX |
                          RAFT_SNAPSHOT_METADATA_PROTO_TERM,
                .unknown_fields = {
                    unknown_data, sizeof(unknown_data), false,
                },
            },
        },
        .protobuf = {
            .fields = RAFT_SNAPSHOT_PROTO_METADATA,
            .unknown_fields = {
                unknown_data, sizeof(unknown_data), false,
            },
        },
    };
    raft_snapshot_t snapshot = {0};
    raft_message_view_t response_view = {
        .type = RAFT_MSG_APP_RESP,
        .context = {NULL, 0, true},
    };
    raft_message_view_t message_view = {
        .type = RAFT_MSG_APP,
        .from = 1,
        .to = 2,
        .term = 2,
        .entries = {&entry_view, 1},
        .has_snapshot = true,
        .snapshot = {
            .data = {source_data, sizeof(source_data), false},
            .metadata = {
                .conf_state = {
                    .voters = {voters, 2},
                },
                .index = 4,
                .term = 2,
            },
        },
        .context = {source_data, sizeof(source_data), false},
        .responses = {&response_view, 1},
        .protobuf = {
            .fields = RAFT_MESSAGE_PROTO_TYPE |
                      RAFT_MESSAGE_PROTO_TO |
                      RAFT_MESSAGE_PROTO_FROM |
                      RAFT_MESSAGE_PROTO_TERM,
            .unknown_fields = {
                unknown_data, sizeof(unknown_data), false,
            },
        },
    };
    raft_message_t message = {0};

    assert(raft_bytes_copy_from_view(&copied, &source) == RAFT_OK);
    assert(raft_bytes_valid(&copied));
    assert(copied.data != source.data);
    assert(memcmp(copied.data, source.data, source.len) == 0);
    source_data[0] = 9;
    assert(copied.data[0] == 1);

    assert(raft_bytes_copy(&copied_again, &copied) == RAFT_OK);
    assert(copied_again.data != copied.data);
    assert(memcmp(copied_again.data, copied.data, copied.len) == 0);

    raft_bytes_free(&copied);
    raft_bytes_free(&copied_again);
    assert(raft_bytes_copy_from_view(&copied, nil_view()) == RAFT_OK);
    assert(copied.is_nil);
    raft_bytes_free(&copied);
    assert(raft_bytes_copy_from_view(
               &copied, &(raft_byte_view_t){NULL, 0, false}) == RAFT_OK);
    assert(!copied.is_nil);
    raft_bytes_free(&copied);

    assert(raft_entry_view_valid(&entry_view));
    assert(raft_entry_copy_from_view(&entry, &entry_view) == RAFT_OK);
    assert(raft_entry_valid(&entry));
    assert(entry.data.data != entry_view.data.data);
    assert(entry.protobuf.fields == entry_view.protobuf.fields);
    assert(entry.protobuf.unknown_fields.data != unknown_data);
    assert(memcmp(entry.protobuf.unknown_fields.data,
                  unknown_data,
                  sizeof(unknown_data)) == 0);

    assert(raft_snapshot_view_valid(&snapshot_view));
    assert(raft_snapshot_copy_from_view(&snapshot, &snapshot_view) ==
           RAFT_OK);
    assert(raft_snapshot_valid(&snapshot));
    assert(snapshot.data.data != snapshot_view.data.data);
    assert(snapshot.metadata.conf_state.voters.items != voters);
    assert(snapshot.protobuf.unknown_fields.data != unknown_data);
    assert(snapshot.metadata.protobuf.unknown_fields.data != unknown_data);

    assert(raft_message_view_valid(&message_view));
    assert(raft_message_copy_from_view(&message, &message_view) == RAFT_OK);
    assert(raft_message_valid(&message));
    assert(message.context.data != message_view.context.data);
    assert(message.entries.items != NULL);
    assert(message.entries.items[0].data.data != entry_view.data.data);
    assert(message.snapshot.data.data != message_view.snapshot.data.data);
    assert(message.responses.items != NULL);
    assert(message.protobuf.fields == message_view.protobuf.fields);
    assert(message.protobuf.unknown_fields.data != unknown_data);

    raft_entry_free(&entry);
    raft_snapshot_free(&snapshot);
    raft_message_free(&message);
}

static void test_temporary_aggregate_descriptors(void) {
    typedef struct temp_conf_change {
        raft_conf_change_v2_view_t value;
        raft_conf_change_single_t change;
    } temp_conf_change_t;
    typedef struct temp_message {
        raft_message_view_t value;
        raft_entry_view_t entry;
    } temp_message_t;

    uint8_t context[] = {4, 5};
    uint8_t entry_data[] = {6, 7, 8};
    raft_config_t config = test_config();
    raft_storage_ops_t storage = test_storage();
    raft_raw_node_t *raw_node = NULL;
    raft_peer_view_t *peers = calloc(1, sizeof(*peers));
    temp_conf_change_t *conf_change = calloc(1, sizeof(*conf_change));
    temp_message_t *message = calloc(1, sizeof(*message));
    raft_conf_state_t conf_state = {0};

    assert(peers != NULL);
    assert(conf_change != NULL);
    assert(message != NULL);
    assert(raft_raw_node_new(&config, &storage, &raw_node) == RAFT_OK);

    peers[0].id = 1;
    peers[0].context =
        (raft_byte_view_t){context, sizeof(context), false};
    assert(raft_raw_node_bootstrap(raw_node, peers, 1) == RAFT_OK);

    conf_change->change.type = RAFT_CONF_CHANGE_ADD_NODE;
    conf_change->change.node_id = 2;
    conf_change->value.transition = RAFT_CONF_CHANGE_TRANSITION_AUTO;
    conf_change->value.changes = &conf_change->change;
    conf_change->value.changes_len = 1;
    conf_change->value.context =
        (raft_byte_view_t){context, sizeof(context), false};
    assert(raft_raw_node_propose_conf_change(raw_node,
                                             &conf_change->value) ==
           RAFT_ERR_PROPOSAL_DROPPED);
    assert(raft_raw_node_apply_conf_change(raw_node, &conf_change->value,
                                           &conf_state) ==
           RAFT_OK);

    message->entry.type = RAFT_ENTRY_NORMAL;
    message->entry.term = 1;
    message->entry.index = 1;
    message->entry.data =
        (raft_byte_view_t){entry_data, sizeof(entry_data), false};
    message->value.type = RAFT_MSG_APP;
    message->value.from = 2;
    message->value.to = 1;
    message->value.term = 2;
    message->value.entries =
        (raft_entry_view_vec_t){&message->entry, 1};
    message->value.context =
        (raft_byte_view_t){context, sizeof(context), false};
    assert(raft_raw_node_step(raw_node, &message->value) == RAFT_OK);

    // Invalid borrowed nesting is rejected and every temporary allocation can
    // still be released directly; view free functions intentionally do not
    // exist.
    message->value.entries.items = NULL;
    assert(raft_raw_node_step(raw_node, &message->value) ==
           RAFT_ERR_INVALID_ARGUMENT);

    raft_conf_state_free(&conf_state);
    free(message);
    free(conf_change);
    free(peers);
    raft_raw_node_destroy(raw_node);
}

static void test_free_functions(void) {
    raft_bytes_t bytes = {
        .data = malloc(4),
        .len = 4,
        .is_nil = false,
    };
    raft_entry_t *entries = calloc(1, sizeof(*entries));
    raft_message_t *messages = calloc(1, sizeof(*messages));
    raft_progress_snapshot_t *snapshots = calloc(1, sizeof(*snapshots));
    raft_bytes_t empty = {NULL, 0, 0};

    assert(bytes.data != NULL);
    assert(entries != NULL);
    assert(messages != NULL);
    assert(snapshots != NULL);

    entries[0].data.data = malloc(2);
    entries[0].data.len = 2;
    entries[0].data.is_nil = false;
    messages[0].context.data = malloc(3);
    messages[0].context.len = 3;
    messages[0].context.is_nil = false;
    assert(entries[0].data.data != NULL);
    assert(messages[0].context.data != NULL);

    raft_bytes_free(NULL);
    raft_bytes_free(&bytes);
    assert(bytes.data == NULL);
    assert(bytes.len == 0);
    assert(bytes.is_nil);

    raft_bytes_free(&empty);
    assert(empty.data == NULL);
    assert(empty.len == 0);
    assert(empty.is_nil);

    raft_entry_array_free(entries, 1);
    raft_message_array_free(messages, 1);
    raft_progress_snapshot_array_free(snapshots, 1);
    raft_progress_snapshot_array_free(NULL, 0);

    raft_entry_free(&(raft_entry_t){0});
    raft_entry_vec_free(&(raft_entry_vec_t){0});
    raft_snapshot_free(&(raft_snapshot_t){0});
    raft_message_free(&(raft_message_t){0});
    raft_message_vec_free(&(raft_message_vec_t){0});
    raft_read_state_free(&(raft_read_state_t){0});
    raft_read_state_vec_free(&(raft_read_state_vec_t){0});
    raft_conf_state_free(&(raft_conf_state_t){0});
    raft_ready_free(&(raft_ready_t){0});
    raft_status_free(&(raft_status_t){0});

    bytes = nil_bytes();
    raft_bytes_free(&bytes);
    assert(bytes.is_nil);
}

int main(void) {
    test_node_id_sentinels();
    test_wire_enum_values();
    test_lifecycle_and_minimal_core();
    test_invalid_arguments();
    test_node_id_boundary_validation();
    test_nil_empty_validation();
    test_copy_helpers();
    test_temporary_aggregate_descriptors();
    test_free_functions();
    return 0;
}
