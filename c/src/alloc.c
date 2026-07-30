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

#include "alloc.h"

#include <stdbool.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdlib.h>

static atomic_size_t raft_alloc_failure_countdown =
    ATOMIC_VAR_INIT(SIZE_MAX);

static bool raft_alloc_should_fail(void) {
    size_t remaining =
        atomic_load_explicit(&raft_alloc_failure_countdown,
                             memory_order_relaxed);

    while (remaining != SIZE_MAX) {
        size_t next = remaining == 0 ? SIZE_MAX : remaining - 1;
        if (atomic_compare_exchange_weak_explicit(
                &raft_alloc_failure_countdown,
                &remaining,
                next,
                memory_order_relaxed,
                memory_order_relaxed)) {
            return remaining == 0;
        }
    }
    return false;
}

void *raft_malloc(size_t size) {
    return raft_alloc_should_fail() ? NULL : malloc(size);
}

void *raft_calloc(size_t count, size_t size) {
    return raft_alloc_should_fail() ? NULL : calloc(count, size);
}

void *raft_realloc(void *pointer, size_t size) {
    return raft_alloc_should_fail() ? NULL : realloc(pointer, size);
}

void raft_alloc_fail_after(size_t successful_allocations) {
    atomic_store_explicit(&raft_alloc_failure_countdown,
                          successful_allocations,
                          memory_order_relaxed);
}

void raft_alloc_fail_reset(void) {
    atomic_store_explicit(&raft_alloc_failure_countdown,
                          SIZE_MAX,
                          memory_order_relaxed);
}
