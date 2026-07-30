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

#ifndef ETCD_RAFT_RANDOM_H
#define ETCD_RAFT_RANDOM_H

#include <stdint.h>

typedef struct raft_random {
    uint64_t state;
} raft_random_t;

// Initializes an independently seeded, non-cryptographic random stream.
// stream distinguishes concurrently initialized Raft nodes.
void raft_random_init(raft_random_t *random, uint64_t stream);

// Replaces the stream state. This is private and primarily supports
// deterministic tests of reset-time behavior.
void raft_random_seed(raft_random_t *random, uint64_t seed);

// Returns a uniformly distributed value in [0, upper_bound).
// upper_bound must be nonzero.
uint32_t raft_random_uniform(raft_random_t *random, uint32_t upper_bound);

#endif  // ETCD_RAFT_RANDOM_H
