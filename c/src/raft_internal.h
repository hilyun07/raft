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

#ifndef ETCD_RAFT_RAFT_INTERNAL_H
#define ETCD_RAFT_RAFT_INTERNAL_H

#include "log.h"

enum {
    RAFT_RAW_NODE_ABI_VERSION = 10,
};

// The public header intentionally exposes only typedef struct raft_raw_node.
// Consensus state will replace or extend this private skeleton in later phases.
struct raft_raw_node {
    uint32_t abi_version;
    raft_config_t config;
    raft_storage_ops_t storage;
    raft_log_t log;
};

#endif  // ETCD_RAFT_RAFT_INTERNAL_H
