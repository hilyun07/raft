#ifndef RAFTTEST_C_STORAGE_H
#define RAFTTEST_C_STORAGE_H

#include "raft/raft.h"

typedef struct rafttest_c_storage {
    raft_hard_state_t hard_state;
    raft_conf_state_t conf_state;
    raft_entry_t *entries;
    size_t entries_len;
    bool has_snapshot;
    raft_snapshot_t snapshot;
} rafttest_c_storage_t;

void rafttest_c_storage_init(rafttest_c_storage_t *storage);
void rafttest_c_storage_close(rafttest_c_storage_t *storage);
raft_storage_ops_t rafttest_c_storage_ops(rafttest_c_storage_t *storage);

int rafttest_c_storage_set_conf_state(rafttest_c_storage_t *storage,
                                      const raft_conf_state_t *state);
int rafttest_c_storage_append(rafttest_c_storage_t *storage,
                              const raft_entry_vec_t *entries);
int rafttest_c_storage_apply_snapshot(rafttest_c_storage_t *storage,
                                      const raft_snapshot_t *snapshot);
int rafttest_c_storage_persist_ready(rafttest_c_storage_t *storage,
                                     const raft_ready_t *ready);

uint64_t rafttest_c_storage_first_index(
    const rafttest_c_storage_t *storage);
uint64_t rafttest_c_storage_last_index(
    const rafttest_c_storage_t *storage);

#endif
