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

#ifndef ETCD_RAFT_ALLOC_H
#define ETCD_RAFT_ALLOC_H

#include <stddef.h>

void *raft_malloc(size_t size);
void *raft_calloc(size_t count, size_t size);
void *raft_realloc(void *pointer, size_t size);

// Test-only fault control. Normal operation keeps fault injection disabled.
// raft_alloc_fail_after(n) makes the allocation after n successful allocation
// attempts fail once, then automatically disables fault injection.
void raft_alloc_fail_after(size_t successful_allocations);
void raft_alloc_fail_reset(void);

#endif  // ETCD_RAFT_ALLOC_H

// This block intentionally remains outside the include guard. The cgo build
// amalgamates alloc.c before the other C sources, so later source-local
// includes must still be able to enable the replacement macros without
// replacing the libc calls inside alloc.c itself.
#ifdef RAFT_ALLOC_REPLACE_STDLIB
#ifndef ETCD_RAFT_ALLOC_STDLIB_REPLACED
#define ETCD_RAFT_ALLOC_STDLIB_REPLACED
#define malloc(size) raft_malloc(size)
#define calloc(count, size) raft_calloc((count), (size))
#define realloc(pointer, size) raft_realloc((pointer), (size))
#endif
#endif
