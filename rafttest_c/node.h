#ifndef RAFTTEST_C_NODE_H
#define RAFTTEST_C_NODE_H

#include "network.h"
#include "storage.h"

typedef struct rafttest_c_node {
    uint64_t id;
    raft_raw_node_t *raw_node;
    rafttest_c_storage_t storage;
    uint64_t applied;
    bool running;
    bool paused;
} rafttest_c_node_t;

typedef struct rafttest_c_cluster {
    rafttest_c_node_t nodes[RAFTTEST_C_MAX_NODES];
    size_t node_count;
    rafttest_c_network_t network;
    uint64_t now;
} rafttest_c_cluster_t;

int rafttest_c_cluster_init(rafttest_c_cluster_t *cluster,
                            size_t node_count);
void rafttest_c_cluster_close(rafttest_c_cluster_t *cluster);

int rafttest_c_cluster_tick(rafttest_c_cluster_t *cluster);
int rafttest_c_cluster_pump(rafttest_c_cluster_t *cluster);
int rafttest_c_cluster_wait_leader(rafttest_c_cluster_t *cluster,
                                   uint64_t max_ticks,
                                   size_t *leader_index);
int rafttest_c_cluster_wait_commit(rafttest_c_cluster_t *cluster,
                                   uint64_t target,
                                   uint64_t max_ticks);
int rafttest_c_cluster_propose(rafttest_c_cluster_t *cluster,
                               size_t node_index,
                               const uint8_t *data,
                               size_t len);

int rafttest_c_cluster_stop(rafttest_c_cluster_t *cluster,
                            size_t node_index);
int rafttest_c_cluster_restart(rafttest_c_cluster_t *cluster,
                               size_t node_index);
int rafttest_c_cluster_pause(rafttest_c_cluster_t *cluster,
                             size_t node_index,
                             bool paused);

void rafttest_c_cluster_dump(const rafttest_c_cluster_t *cluster);

#endif
