#include "node.h"

#include "copy.h"

#include <stdio.h>
#include <string.h>

#define READY_LIMIT 1024
#define PUMP_LIMIT 100000
#define NETWORK_LIMIT 1000000

static raft_config_t node_config(uint64_t id) {
    const raft_config_t config = {
        .id = id,
        .election_tick = 10,
        .heartbeat_tick = 1,
        .max_size_per_message = UINT64_MAX,
        .max_committed_size_per_ready = UINT64_MAX,
        .max_uncommitted_entries_size = UINT64_MAX,
        .max_inflight_messages = 256,
        .read_only_option = RAFT_READ_ONLY_SAFE,
    };
    return config;
}

static int node_open(rafttest_c_node_t *node, bool bootstrap,
                     size_t node_count) {
    raft_config_t config = node_config(node->id);
    raft_storage_ops_t ops = rafttest_c_storage_ops(&node->storage);
    raft_peer_view_t peers[RAFTTEST_C_MAX_NODES];
    raft_status_t status;
    size_t i;
    int result;

    result = raft_raw_node_new(&config, &ops, &node->raw_node);
    if (result != RAFT_OK) {
        return result;
    }
    if (bootstrap) {
        memset(peers, 0, sizeof(peers));
        for (i = 0; i < node_count; ++i) {
            peers[i].id = (uint64_t)i + UINT64_C(1);
            peers[i].context.is_nil = true;
        }
        result = raft_raw_node_bootstrap(node->raw_node, peers,
                                         node_count);
        if (result != RAFT_OK) {
            raft_raw_node_destroy(node->raw_node);
            node->raw_node = NULL;
            return result;
        }
        memset(&status, 0, sizeof(status));
        result = raft_raw_node_status(node->raw_node, &status);
        if (result == RAFT_OK) {
            result = rafttest_c_storage_set_conf_state(
                &node->storage, &status.conf_state);
        }
        raft_status_free(&status);
        if (result != RAFT_OK) {
            raft_raw_node_destroy(node->raw_node);
            node->raw_node = NULL;
            return result;
        }
    }
    node->running = true;
    node->paused = false;
    return RAFT_OK;
}

static int node_process_ready(rafttest_c_cluster_t *cluster,
                              rafttest_c_node_t *node,
                              bool *made_progress) {
    size_t iterations = 0;

    while (node->running && !node->paused &&
           raft_raw_node_has_ready(node->raw_node)) {
        raft_ready_t *ready = NULL;
        size_t i;
        int result;

        if (++iterations > READY_LIMIT) {
            return RAFT_ERR_FATAL;
        }
        result = raft_raw_node_ready_without_accept(node->raw_node,
                                                    &ready);
        if (result != RAFT_OK || ready == NULL) {
            return result == RAFT_OK ? RAFT_ERR_FATAL : result;
        }
        result = rafttest_c_storage_persist_ready(&node->storage, ready);
        if (result != RAFT_OK) {
            raft_ready_destroy(ready);
            return result;
        }
        if (ready->has_snapshot &&
            ready->snapshot.metadata.index > node->applied) {
            node->applied = ready->snapshot.metadata.index;
        }
        if (ready->committed_entries.len != 0) {
            node->applied = ready->committed_entries
                                .items[ready->committed_entries.len - 1]
                                .index;
        }
        for (i = 0; i < ready->messages.len; ++i) {
            result = rafttest_c_network_enqueue(
                &cluster->network, &ready->messages.items[i], cluster->now);
            if (result != RAFT_OK) {
                raft_ready_destroy(ready);
                return result;
            }
        }
        result = raft_raw_node_accept_ready(node->raw_node, ready);
        raft_ready_destroy(ready);
        if (result != RAFT_OK) {
            return result;
        }
        result = raft_raw_node_advance(node->raw_node);
        if (result != RAFT_OK) {
            return result;
        }
        *made_progress = true;
    }
    return RAFT_OK;
}

int rafttest_c_cluster_init(rafttest_c_cluster_t *cluster,
                            size_t node_count) {
    size_t i;
    int result;

    if (cluster == NULL || node_count == 0 ||
        node_count > RAFTTEST_C_MAX_NODES) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    memset(cluster, 0, sizeof(*cluster));
    cluster->node_count = node_count;
    rafttest_c_network_init(&cluster->network, NETWORK_LIMIT,
                            UINT64_C(1), 2);
    for (i = 0; i < node_count; ++i) {
        rafttest_c_node_t *node = &cluster->nodes[i];
        node->id = (uint64_t)i + UINT64_C(1);
        rafttest_c_storage_init(&node->storage);
        rafttest_c_network_connect(&cluster->network, node->id);
        result = node_open(node, true, node_count);
        if (result != RAFT_OK) {
            rafttest_c_cluster_close(cluster);
            return result;
        }
    }
    return rafttest_c_cluster_pump(cluster);
}

void rafttest_c_cluster_close(rafttest_c_cluster_t *cluster) {
    size_t i;

    if (cluster == NULL) {
        return;
    }
    for (i = 0; i < cluster->node_count; ++i) {
        if (cluster->nodes[i].raw_node != NULL) {
            raft_raw_node_destroy(cluster->nodes[i].raw_node);
            cluster->nodes[i].raw_node = NULL;
        }
        rafttest_c_storage_close(&cluster->nodes[i].storage);
    }
    rafttest_c_network_close(&cluster->network);
    memset(cluster, 0, sizeof(*cluster));
}

int rafttest_c_cluster_pump(rafttest_c_cluster_t *cluster) {
    size_t iteration;

    if (cluster == NULL) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    for (iteration = 0; iteration < PUMP_LIMIT; ++iteration) {
        bool progress = false;
        raft_message_t message;
        size_t i;
        int taken;
        int result;

        for (i = 0; i < cluster->node_count; ++i) {
            result = node_process_ready(cluster, &cluster->nodes[i],
                                        &progress);
            if (result != RAFT_OK) {
                return result;
            }
        }
        taken = rafttest_c_network_take(&cluster->network, cluster->now,
                                        &message);
        if (taken < 0) {
            return RAFT_ERR_INVALID_ARGUMENT;
        }
        if (taken == 1) {
            uint64_t to = message.to;
            rafttest_c_message_view_t view;

            if (to == 0 || to > cluster->node_count ||
                !cluster->nodes[to - 1].running) {
                raft_message_free(&message);
                progress = true;
            } else {
                result = rafttest_c_message_view_init(&view, &message);
                if (result == RAFT_OK) {
                    result = raft_raw_node_step_for_node(
                        cluster->nodes[to - 1].raw_node, &view.value);
                    rafttest_c_message_view_close(&view);
                }
                raft_message_free(&message);
                if (result != RAFT_OK &&
                    result != RAFT_ERR_PROPOSAL_DROPPED &&
                    result != RAFT_ERR_STEP_PEER_NOT_FOUND_OR_IGNORED) {
                    return result;
                }
                progress = true;
            }
        }
        if (!progress) {
            return RAFT_OK;
        }
    }
    return RAFT_ERR_FATAL;
}

int rafttest_c_cluster_tick(rafttest_c_cluster_t *cluster) {
    size_t i;
    int result;

    if (cluster == NULL) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    ++cluster->now;
    for (i = 0; i < cluster->node_count; ++i) {
        rafttest_c_node_t *node = &cluster->nodes[i];
        if (!node->running || node->paused) {
            continue;
        }
        result = raft_raw_node_tick_result(node->raw_node);
        if (result != RAFT_OK) {
            return result;
        }
    }
    return rafttest_c_cluster_pump(cluster);
}

static int cluster_leader(const rafttest_c_cluster_t *cluster,
                          size_t *leader_index) {
    uint64_t observed = 0;
    bool leader_running = false;
    size_t i;

    for (i = 0; i < cluster->node_count; ++i) {
        const rafttest_c_node_t *node = &cluster->nodes[i];
        raft_basic_status_t status;
        int result;

        if (!node->running) {
            continue;
        }
        result = raft_raw_node_basic_status(node->raw_node, &status);
        if (result != RAFT_OK) {
            return -result;
        }
        if (status.soft_state.lead == 0) {
            return 0;
        }
        if (observed == 0) {
            observed = status.soft_state.lead;
        } else if (observed != status.soft_state.lead) {
            return 0;
        }
        if (status.id == observed &&
            status.soft_state.raft_state == RAFT_STATE_LEADER) {
            leader_running = true;
            if (leader_index != NULL) {
                *leader_index = i;
            }
        }
    }
    return observed != 0 && leader_running ? 1 : 0;
}

int rafttest_c_cluster_wait_leader(rafttest_c_cluster_t *cluster,
                                   uint64_t max_ticks,
                                   size_t *leader_index) {
    uint64_t i;

    for (i = 0; i <= max_ticks; ++i) {
        int found = cluster_leader(cluster, leader_index);
        int result;

        if (found == 1) {
            return RAFT_OK;
        }
        if (found < 0) {
            return -found;
        }
        result = rafttest_c_cluster_tick(cluster);
        if (result != RAFT_OK) {
            return result;
        }
    }
    return RAFT_ERR_FATAL;
}

static int commit_converged(const rafttest_c_cluster_t *cluster,
                            uint64_t target) {
    uint64_t observed = 0;
    bool have_node = false;
    size_t i;

    for (i = 0; i < cluster->node_count; ++i) {
        const rafttest_c_node_t *node = &cluster->nodes[i];
        raft_basic_status_t status;
        int result;

        if (!node->running) {
            continue;
        }
        result = raft_raw_node_basic_status(node->raw_node, &status);
        if (result != RAFT_OK) {
            return -result;
        }
        if (status.hard_state.commit <= target) {
            return 0;
        }
        if (!have_node) {
            observed = status.hard_state.commit;
            have_node = true;
        } else if (observed != status.hard_state.commit) {
            return 0;
        }
    }
    return have_node ? 1 : 0;
}

int rafttest_c_cluster_wait_commit(rafttest_c_cluster_t *cluster,
                                   uint64_t target,
                                   uint64_t max_ticks) {
    uint64_t i;

    for (i = 0; i <= max_ticks; ++i) {
        int converged = commit_converged(cluster, target);
        int result;

        if (converged == 1) {
            return RAFT_OK;
        }
        if (converged < 0) {
            return -converged;
        }
        result = rafttest_c_cluster_tick(cluster);
        if (result != RAFT_OK) {
            return result;
        }
    }
    return RAFT_ERR_FATAL;
}

int rafttest_c_cluster_propose(rafttest_c_cluster_t *cluster,
                               size_t node_index,
                               const uint8_t *data,
                               size_t len) {
    raft_byte_view_t proposal = {
        .data = data,
        .len = len,
        .is_nil = false,
    };
    uint64_t attempts;

    if (cluster == NULL || node_index >= cluster->node_count ||
        !cluster->nodes[node_index].running ||
        cluster->nodes[node_index].paused || (len != 0 && data == NULL)) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    for (attempts = 0; attempts < 100; ++attempts) {
        int result = raft_raw_node_propose(
            cluster->nodes[node_index].raw_node, &proposal);
        if (result == RAFT_OK) {
            return rafttest_c_cluster_pump(cluster);
        }
        if (result != RAFT_ERR_PROPOSAL_DROPPED) {
            return result;
        }
        result = rafttest_c_cluster_tick(cluster);
        if (result != RAFT_OK) {
            return result;
        }
    }
    return RAFT_ERR_PROPOSAL_DROPPED;
}

int rafttest_c_cluster_stop(rafttest_c_cluster_t *cluster,
                            size_t node_index) {
    rafttest_c_node_t *node;

    if (cluster == NULL || node_index >= cluster->node_count) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    node = &cluster->nodes[node_index];
    if (!node->running || node->raw_node == NULL) {
        return RAFT_ERR_STOPPED;
    }
    rafttest_c_network_disconnect(&cluster->network, node->id);
    raft_raw_node_destroy(node->raw_node);
    node->raw_node = NULL;
    node->running = false;
    node->paused = false;
    return RAFT_OK;
}

int rafttest_c_cluster_restart(rafttest_c_cluster_t *cluster,
                               size_t node_index) {
    rafttest_c_node_t *node;
    int result;

    if (cluster == NULL || node_index >= cluster->node_count) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    node = &cluster->nodes[node_index];
    if (node->running || node->raw_node != NULL) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    result = node_open(node, false, cluster->node_count);
    if (result != RAFT_OK) {
        return result;
    }
    rafttest_c_network_connect(&cluster->network, node->id);
    return rafttest_c_cluster_pump(cluster);
}

int rafttest_c_cluster_pause(rafttest_c_cluster_t *cluster,
                             size_t node_index,
                             bool paused) {
    rafttest_c_node_t *node;

    if (cluster == NULL || node_index >= cluster->node_count) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    node = &cluster->nodes[node_index];
    if (!node->running) {
        return RAFT_ERR_STOPPED;
    }
    node->paused = paused;
    rafttest_c_network_pause(&cluster->network, node->id, paused);
    return paused ? RAFT_OK : rafttest_c_cluster_pump(cluster);
}

void rafttest_c_cluster_dump(const rafttest_c_cluster_t *cluster) {
    size_t i;

    if (cluster == NULL) {
        return;
    }
    fprintf(stderr, "cluster tick=%llu queued=%zu\n",
            (unsigned long long)cluster->now, cluster->network.len);
    for (i = 0; i < cluster->node_count; ++i) {
        const rafttest_c_node_t *node = &cluster->nodes[i];
        if (node->running) {
            raft_basic_status_t status;
            int result = raft_raw_node_basic_status(node->raw_node,
                                                    &status);
            if (result == RAFT_OK) {
                fprintf(stderr,
                        "  node=%llu paused=%d state=%d term=%llu "
                        "lead=%llu commit=%llu applied=%llu\n",
                        (unsigned long long)node->id, (int)node->paused,
                        (int)status.soft_state.raft_state,
                        (unsigned long long)status.hard_state.term,
                        (unsigned long long)status.soft_state.lead,
                        (unsigned long long)status.hard_state.commit,
                        (unsigned long long)node->applied);
            } else {
                fprintf(stderr, "  node=%llu status-error=%d\n",
                        (unsigned long long)node->id, result);
            }
        } else {
            fprintf(stderr, "  node=%llu stopped commit=%llu\n",
                    (unsigned long long)node->id,
                    (unsigned long long)node->storage.hard_state.commit);
        }
    }
}
