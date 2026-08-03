#ifndef RAFTTEST_C_COPY_H
#define RAFTTEST_C_COPY_H

#include "raft/raft.h"

typedef struct rafttest_c_message_view {
    raft_message_view_t value;
    raft_entry_view_t *entries;
    struct rafttest_c_message_view *response_owners;
    raft_message_view_t *responses;
} rafttest_c_message_view_t;

int rafttest_c_conf_state_copy(raft_conf_state_t *dst,
                               const raft_conf_state_t *src);
int rafttest_c_entry_copy(raft_entry_t *dst, const raft_entry_t *src);
int rafttest_c_snapshot_copy(raft_snapshot_t *dst,
                             const raft_snapshot_t *src);
int rafttest_c_message_copy(raft_message_t *dst,
                            const raft_message_t *src);

int rafttest_c_message_view_init(rafttest_c_message_view_t *view,
                                 const raft_message_t *message);
void rafttest_c_message_view_close(rafttest_c_message_view_t *view);

#endif
