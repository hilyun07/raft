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

#include "random.h"

#include <stdatomic.h>
#include <stddef.h>
#include <time.h>

static atomic_uint_fast64_t random_sequence = ATOMIC_VAR_INIT(0);

static uint64_t random_mix(uint64_t value) {
    value = (value ^ (value >> 30)) * UINT64_C(0xbf58476d1ce4e5b9);
    value = (value ^ (value >> 27)) * UINT64_C(0x94d049bb133111eb);
    return value ^ (value >> 31);
}

void raft_random_seed(raft_random_t *random, uint64_t seed) {
    if (random != NULL) {
        random->state = seed;
    }
}

void raft_random_init(raft_random_t *random, uint64_t stream) {
    struct timespec now = {0, 0};
    uint64_t sequence;
    uint64_t seed;

    if (random == NULL) {
        return;
    }
    sequence = atomic_fetch_add_explicit(
        &random_sequence, UINT64_C(1), memory_order_relaxed);
    seed = stream;
    seed ^= sequence * UINT64_C(0x9e3779b97f4a7c15);
    seed ^= (uint64_t)(uintptr_t)random;
    if (timespec_get(&now, TIME_UTC) == TIME_UTC) {
        seed ^= (uint64_t)now.tv_sec;
        seed ^= (uint64_t)now.tv_nsec << 32;
    } else {
        seed ^= (uint64_t)clock();
    }
    raft_random_seed(random, random_mix(seed));
}

static uint64_t random_next(raft_random_t *random) {
    random->state += UINT64_C(0x9e3779b97f4a7c15);
    return random_mix(random->state);
}

uint32_t raft_random_uniform(raft_random_t *random, uint32_t upper_bound) {
    uint64_t threshold;
    uint64_t value;

    if (random == NULL || upper_bound == 0) {
        return 0;
    }
    threshold =
        (UINT64_C(0) - (uint64_t)upper_bound) % (uint64_t)upper_bound;
    do {
        value = random_next(random);
    } while (value < threshold);
    return (uint32_t)(value % (uint64_t)upper_bound);
}
