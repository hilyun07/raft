# C Type-Separated Payload API Result

The C skeleton now expresses ownership and lifetime through C types rather
than runtime flags or kind tags. This is allocation/API plumbing only: no
Raft consensus logic, cgo binding, build tag, or production Go behavior was
implemented or changed.

## Final ownership rule

Borrowed input uses `*_view_t` and is valid only for the duration of the call.
C never frees or retains view pointers. Retained/internal/output data uses
non-view owned types and is recursively freed by C.

There is no `kind`, `is_view`, `OWNED`, `VIEW`, or flags field that selects
ownership at runtime.

## Byte types and nil shape

The final byte types are:

```c
typedef struct raft_byte_view {
    const uint8_t *data;
    size_t len;
    bool is_nil;
} raft_byte_view_t;

typedef struct raft_bytes {
    uint8_t *data;
    size_t len;
    bool is_nil;
} raft_bytes_t;
```

Canonical representations are:

- Go nil: `{NULL, 0, true}`;
- Go empty but non-nil: `{NULL, 0, false}`;
- non-empty: `{non-NULL, positive length, false}`.

`is_nil` records presence only. It does not record ownership. The
`RAFT_BYTES_NIL` and `RAFT_BYTES_OWNED` macros and byte `flags` fields were
removed.

## Aggregate view and owned types

The public header now separates the pointer-bearing object graph:

| Borrowed input | C-owned retained/output |
| --- | --- |
| `raft_byte_view_t` | `raft_bytes_t` |
| `raft_uint64_view_t` | `raft_uint64_vec_t` |
| `raft_conf_state_view_t` | `raft_conf_state_t` |
| `raft_snapshot_metadata_view_t` | `raft_snapshot_metadata_t` |
| `raft_snapshot_view_t` | `raft_snapshot_t` |
| `raft_entry_view_t` / `raft_entry_view_vec_t` | `raft_entry_t` / `raft_entry_vec_t` |
| `raft_message_view_t` / `raft_message_view_vec_t` | `raft_message_t` / `raft_message_vec_t` |

Borrowed Bootstrap and configuration-change inputs are named
`raft_peer_view_t`, `raft_conf_change_view_t`, and
`raft_conf_change_v2_view_t`.

The former output names `raft_progress_view_t` and
`raft_progress_record_t` were insufficiently explicit about observation
semantics. The final type is `raft_progress_snapshot_t`; the returned array is
C-owned point-in-time output and is freed with
`raft_progress_snapshot_array_free`. It contains no borrowed tracker or
inflights pointers.

Ready, Storage callback results, ReadState, Status, and retained message data
use only owned non-view types.

## Public signatures

The payload-bearing RawNode input signatures are:

```c
int raft_raw_node_propose(raft_raw_node_t *rn,
                          const raft_byte_view_t *data);

int raft_raw_node_read_index(raft_raw_node_t *rn,
                             const raft_byte_view_t *ctx);

int raft_raw_node_step(raft_raw_node_t *rn,
                       const raft_message_view_t *message);

int raft_raw_node_step_for_node(raft_raw_node_t *rn,
                                const raft_message_view_t *message);

int raft_raw_node_bootstrap(raft_raw_node_t *rn,
                            const raft_peer_view_t *peers,
                            size_t peer_count);
```

The public Step entry point validates the public RawNode boundary and then
delegates to `raft_raw_node_step_for_node`. The latter is the lower-level Go
Node actor/core entry point and must not call the public function. Both accept
borrowed message views, but their validation layers are intentionally
different.

Configuration-change APIs accept
`const raft_conf_change_v2_view_t *`. Public structs remain pointer-passed;
a null input descriptor is invalid unless an API explicitly says otherwise.

The type-graph correction advanced the marker from version 4 to 5; the
subsequent C-owned Ready/argument-free Advance safety correction advanced it
to 6, the explicit progress-snapshot API rename advanced it to 7, and removal
of the redundant two-ID leadership-transfer declaration advanced it to 8.
The later Phase 4 Node-boundary helper additions leave the current marker at
version 9 for that milestone. Phase 6's private log ownership advances the
current marker to version 10.

## Validation

Public validators distinguish the two sides:

```c
bool raft_byte_view_valid(const raft_byte_view_t *view);
bool raft_bytes_valid(const raft_bytes_t *bytes);
bool raft_entry_view_valid(const raft_entry_view_t *entry);
bool raft_entry_valid(const raft_entry_t *entry);
bool raft_snapshot_view_valid(const raft_snapshot_view_t *snapshot);
bool raft_snapshot_valid(const raft_snapshot_t *snapshot);
bool raft_message_view_valid(const raft_message_view_t *message);
bool raft_message_valid(const raft_message_t *message);
```

Byte validators reject null descriptors, positive length with null data,
non-null zero-length dummy buffers, and positive-length `is_nil` payloads.
Aggregate validators recursively check byte payloads and nested array shapes.

## Deep-copy helpers

The skeleton implements:

```c
int raft_bytes_copy_from_view(raft_bytes_t *dst,
                              const raft_byte_view_t *src);
int raft_bytes_copy(raft_bytes_t *dst,
                    const raft_bytes_t *src);
int raft_entry_copy_from_view(raft_entry_t *dst,
                              const raft_entry_view_t *src);
int raft_snapshot_copy_from_view(raft_snapshot_t *dst,
                                 const raft_snapshot_view_t *src);
int raft_message_copy_from_view(raft_message_t *dst,
                                const raft_message_view_t *src);
```

These allocate and recursively copy byte payloads, Entry arrays, ConfState ID
arrays, Snapshot content, Message arrays, and nested response messages. They
preserve nil-versus-empty and clean partial owned output on failure. They
never store a view pointer into an owned destination.

The helpers require a zero-initialized destination. They implement ownership
conversion only; no copied object is submitted to a Raft state machine in this
phase.

## Free functions

`raft_bytes_free` unconditionally calls `free` on owned `data` and resets to
`{NULL, 0, true}`. Owned Entry, Snapshot, Message, Ready, and Status free paths
recurse through their owned children.

There are no view free functions. Passing borrowed memory in an owned
non-view type violates the API contract.

## Go binding and cgo shims

The Go binding must not allocate pointer-containing descriptors such as
`raft_byte_view_t`, `raft_entry_view_t`, `raft_snapshot_view_t`, or
`raft_message_view_t` in Go memory and pass their addresses to C.

The skeleton provides scalar C shims for the simple byte APIs:

```c
int raft_raw_node_propose_from_parts(raft_raw_node_t *rn,
                                     const uint8_t *data,
                                     size_t len,
                                     bool is_nil);
int raft_raw_node_read_index_from_parts(raft_raw_node_t *rn,
                                        const uint8_t *ctx,
                                        size_t len,
                                        bool is_nil);
```

They construct `raft_byte_view_t` in C storage and call the pointer-based core
API. Aggregate Entry, Snapshot, Message, Peer, and configuration-change inputs
use a single-shot temporary C-allocation strategy: Go allocates/fills the
descriptor graph with `C.malloc`/`C.calloc`, performs one RawNode call, and
frees the temporary graph immediately. No field-by-field builder API is
planned.

All view data remains borrowed only during the C call. If future real logic
retains it, it must first use the copy-from-view helpers.

Ready functions return a C-owned `raft_ready_t *` through
`raft_ready_t **out`; Go converts it in one batch and calls
`raft_ready_destroy`. Advance is `raft_raw_node_advance(rn)` with no Ready
argument, so Go never allocates or reconstructs a pointer-bearing Ready.

## Dynamic ownership tag audit

The C header, implementation, and tests contain none of:

- `RAFT_DATA_VIEW` / `RAFT_DATA_OWNED`;
- `RAFT_ARRAY_VIEW` / `RAFT_ARRAY_OWNED`;
- `raft_data_kind_t` / `raft_array_kind_t`;
- `RAFT_BYTE_VIEW` / `RAFT_BYTE_OWNED`;
- unified `raft_byte_t`;
- `.kind`, `.is_view`, byte ownership flags, `RAFT_BYTES_NIL`, or
  `RAFT_BYTES_OWNED`.

Remaining enums and booleans are semantic, not ownership tags. Examples
include Raft state, MessageType, EntryType, progress peer/learner type,
`has_snapshot`, Ready presence fields, and `is_nil`.

## Files updated

- `c/include/raft/raft.h`
- `c/src/raft_internal.h`
- `c/src/raw_node.c`
- `c/tests/raw_node_skeleton_test.c`
- `prompts/RAFT_C_PORTING_SPEC.md`
- `OUTPUT/RAFTPB_WIRE_COMPAT_FOR_C_PORT.md`
- `OUTPUT/READY_OWNERSHIP_FOR_C_PORT.md`
- `OUTPUT/PHASE2_C_RAWNODE_SKELETON_RESULT.md`
- `OUTPUT/C_BYTE_PAYLOAD_API_CORRECTION_RESULT.md`
- `OUTPUT/C_BYTE_PAYLOAD_POINTER_API_CORRECTION_RESULT.md`
- `OUTPUT/C_BYTE_PAYLOAD_IS_NIL_CORRECTION_RESULT.md`
- `OUTPUT/C_GO_POINTER_SAFE_RAWNODE_API_RESULT.md`
- `OUTPUT/C_PORT_TRACKING_GAPS.md`
- `OUTPUT/RAWNODE_API_PARITY_FOR_C_PORT.md`
- `OUTPUT/C_PORT_FOLLOWUP_DOCS_RESULT.md`

No `C_DYNAMIC_PAYLOAD_KIND_API_RESULT.md` existed, so no such file required
updating.

## Tests

The C skeleton tests cover:

- valid nil, present-empty, and non-empty views and owned bytes;
- invalid null descriptor, null-data/nonzero-length, dummy zero-length, and
  non-empty `is_nil` states;
- byte copy-from-view allocation, nil/empty preservation, and pointer
  independence;
- owned-to-owned byte copying;
- Entry, Snapshot/ConfState, Message, and nested response recursive copies;
- view and owned aggregate validators;
- owned free/reset behavior and null frees;
- Propose/ReadIndex scalar shim behavior;
- temporary C-resident Peer, ConfChange, Entry, and Message descriptors,
  including invalid nesting and cleanup;
- existing lifecycle, sentinel, enum, null-argument, and stub behavior.

Commands:

```sh
make clean-c && make test-c

mkdir -p .tmp .gocache
TMPDIR="$PWD/.tmp" GOTMPDIR="$PWD/.tmp" GOCACHE="$PWD/.gocache" go test ./...
```

Result: both commands passed. The default Go path remains pure-Go and does not
compile or link the C subtree.

## Open questions

- Phase 3 must implement and benchmark the documented single-shot temporary
  C-allocation conversion for complex view graphs without allowing
  Go-allocated pointer-containing descriptors.
- Storage callback output allocation/error cleanup needs a finalized bridge
  contract, though its public output types are now unambiguously owned.
- The recursive Message validator/copy helper follows nested responses; later
  hardening may add explicit depth or allocation limits for hostile native C
  callers.
- ABI versioning must be formalized before any C API stability promise.
