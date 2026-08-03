#ifndef RAFTTEST_C_NETWORK_H
#define RAFTTEST_C_NETWORK_H

#include "raft/raft.h"

#define RAFTTEST_C_MAX_NODES 16

typedef struct rafttest_c_network_item rafttest_c_network_item_t;

typedef struct rafttest_c_network {
    rafttest_c_network_item_t *head;
    rafttest_c_network_item_t *tail;
    size_t len;
    size_t max_len;
    uint64_t sequence;
    uint64_t random_state;
    uint64_t last_due[RAFTTEST_C_MAX_NODES + 1];
    bool connected[RAFTTEST_C_MAX_NODES + 1];
    bool paused[RAFTTEST_C_MAX_NODES + 1];
    uint32_t max_delay_ticks;
} rafttest_c_network_t;

void rafttest_c_network_init(rafttest_c_network_t *network,
                             size_t max_len,
                             uint64_t seed,
                             uint32_t max_delay_ticks);
void rafttest_c_network_close(rafttest_c_network_t *network);
void rafttest_c_network_connect(rafttest_c_network_t *network,
                                uint64_t id);
void rafttest_c_network_disconnect(rafttest_c_network_t *network,
                                   uint64_t id);
void rafttest_c_network_pause(rafttest_c_network_t *network,
                              uint64_t id,
                              bool paused);

int rafttest_c_network_enqueue(rafttest_c_network_t *network,
                               const raft_message_t *message,
                               uint64_t now);
int rafttest_c_network_take(rafttest_c_network_t *network,
                            uint64_t now,
                            raft_message_t *message);

#endif
