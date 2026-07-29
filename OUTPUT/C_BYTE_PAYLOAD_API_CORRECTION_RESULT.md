# C Byte Payload API Correction Result

The C RawNode skeleton uses one borrowed byte-view type and one C-owned
returned-byte type. The original version of this result used runtime flags;
the current type-separated design below supersedes that intermediate shape.
This was an ABI/type correction only: no
Raft consensus logic, Go binding, cgo path, build tag, or pure-Go behavior was
added or changed.

## Files updated

| File | Correction |
| --- | --- |
| `c/include/raft/raft.h` | Added type-separated view/owned wrappers and aggregate types, changed payload fields and RawNode signatures, and documented canonical shapes and ownership. |
| `c/src/raw_node.c` | Implemented shared validation, recursive view-to-owned copying, unconditional owned freeing, and wrapper-based stub validation. |
| `c/src/raft_internal.h` | Successive wrapper, pointer, type-separation, Ready/Advance, progress-snapshot naming, leadership-transfer cleanup, and Phase 4 boundary-helper corrections leave the current private skeleton marker at version 9. |
| `c/tests/raw_node_skeleton_test.c` | Converted calls/initializers and added canonical-shape, invalid-shape, and free/reset tests. |
| `prompts/RAFT_C_PORTING_SPEC.md` | Updated the port specification examples and ownership rules to use the wrappers. |
| `OUTPUT/RAFTPB_WIRE_COMPAT_FOR_C_PORT.md` | Defined nil/empty/non-empty conversion rules and wrapper use at the raftpb boundary. |
| `OUTPUT/READY_OWNERSHIP_FOR_C_PORT.md` | Defined borrowed-versus-owned lifetime, recursive free, and Go deep-copy rules. |
| `OUTPUT/RAWNODE_API_PARITY_FOR_C_PORT.md` | Recorded wrapper semantics for Propose, ReadIndex, Bootstrap, and `Peer.Context`. |
| `OUTPUT/C_PORT_TRACKING_GAPS.md` | Added wrapper/presence requirements to cross-phase tracking gates. |
| `OUTPUT/PHASE2_C_RAWNODE_SKELETON_RESULT.md` | Reconciled the Phase 2 result with the corrected ABI and tests. |
| `OUTPUT/C_PORT_FOLLOWUP_DOCS_RESULT.md` | Cross-referenced this correction from the earlier documentation result. |

## Public wrapper design

The public header defines:

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

`is_nil` distinguishes Go nil from empty-but-non-nil bytes. Ownership is
determined solely by type: a `raft_byte_view_t` is borrowed and a
`raft_bytes_t` is C-owned. No ownership flag or runtime kind exists. Public
callers pass borrowed descriptors as `const raft_byte_view_t *`, not by value.

Canonical representations are:

- nil: `{NULL, 0, true}`;
- empty but non-nil: `{NULL, 0, false}`;
- non-empty: `{non-NULL, positive length, false}`.

Dummy zero-length allocations are rejected rather than used to encode
presence.

## Signatures changed

The former separate payload parameters were removed:

```c
int raft_raw_node_propose(raft_raw_node_t *rn,
                          const raft_byte_view_t *data);

int raft_raw_node_read_index(raft_raw_node_t *rn,
                             const raft_byte_view_t *request_context);
```

There are no compatibility overloads retaining the old form.
`raft_peer_view_t.context`, legacy ConfChange view context, and ConfChangeV2
view context are borrowed `raft_byte_view_t` fields in enclosing view structs
passed by `const` pointer. The logger callback's call-scoped message is also passed as
`const raft_byte_view_t *`.

## Struct fields changed

The following returned or nested payloads use `raft_bytes_t`:

- `raft_entry_t.data`;
- `raft_message_t.context`;
- `raft_snapshot_t.data`;
- `raft_read_state_t.request_ctx`;
- the same payloads recursively nested under `raft_ready_t` and messages.

This keeps nil/empty shape and allocation ownership adjacent to each payload.

## Helpers and free behavior

The public helpers are:

```c
bool raft_byte_view_valid(const raft_byte_view_t *view);
bool raft_bytes_valid(const raft_bytes_t *bytes);
void raft_bytes_free(raft_bytes_t *bytes);
```

Validation rejects null descriptors, positive length with a null pointer,
non-null data at zero length, and `is_nil` non-empty data. It accepts both
canonical nil and canonical present-empty.

`raft_bytes_free(NULL)` is a no-op. For a non-null wrapper it frees `data`
unconditionally, then resets the wrapper to canonical nil:
`data = NULL`, `len = 0`, and `is_nil = true`. Entry, message,
snapshot, read-state, Ready, and Status free paths consistently recurse
through owned output. Borrowed ConfChange and peer inputs have no free API.

## Obsolete forms

Production C headers, source, and tests contain no `raft_buffer_t`,
`raft_buffer_free`, `is_nil` field, or old Propose/ReadIndex triple
signatures. Documentation mentions the old form only to record that it was
replaced.

The lower-level validation implementation accepts a pointer, length, and
`is_nil` scalar internally; this is private code rather than a runtime
ownership tag.

## Tests

Clean C skeleton:

```sh
make clean-c && make test-c
```

Result: passed with C11, `-Wall -Wextra -Werror -Wpedantic`. Tests cover nil,
present-empty, non-empty, invalid null/positive-length, noncanonical
zero-length data, view-to-owned deep copies, null/owned frees, and
create/destroy/stub behavior.

Default pure-Go suite:

```sh
mkdir -p .tmp .gocache
TMPDIR="$PWD/.tmp" GOTMPDIR="$PWD/.tmp" GOCACHE="$PWD/.gocache" go test ./...
```

Result: all six repository packages passed (cached). The default test path
still does not compile or link the C subtree.

## Open questions

- Phase 3 must define conversion helpers that create nil Go slices for
  `is_nil = true` and distinct non-nil empty slices for canonical empty.
- Storage callback output allocation responsibility still needs a finalized
  contract; callback output uses owned non-view types.
- Allocation-failure cleanup for partially built Ready trees remains work for
  the real output/conversion phases.

The final struct-pointer public API audit and its test results are recorded in
`C_BYTE_PAYLOAD_POINTER_API_CORRECTION_RESULT.md`.
The final type-separated aggregate design is recorded in
`C_TYPE_SEPARATED_PAYLOAD_API_RESULT.md`.
