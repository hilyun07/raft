# C Byte Payload Pointer API Correction Result

The C skeleton follows the project convention that public C APIs never pass
structs by value. Borrowed byte descriptors are passed as
`const raft_byte_view_t *`; returned and owned bytes remain `raft_bytes_t`
fields within output structures. Ownership is now determined by those types,
not runtime flags. No Raft consensus logic, cgo binding, build tag, or
production Go behavior changed.

## Public signatures changed

The final RawNode signatures are:

```c
int raft_raw_node_propose(raft_raw_node_t *raw_node,
                          const raft_byte_view_t *data);

int raft_raw_node_read_index(raft_raw_node_t *raw_node,
                             const raft_byte_view_t *request_context);
```

The public logger callback was corrected for the same convention:

```c
typedef void (*raft_logger_log_fn)(uintptr_t handle,
                                   raft_log_level_t level,
                                   const raft_byte_view_t *message);
```

`raft_raw_node_bootstrap` accepts `const raft_peer_view_t *`; each peer
embeds its borrowed Context descriptor. Configuration-change, Message, Ready,
Status, Storage, and progress structures were already passed through pointers.

This pointer correction originally advanced the private marker to version 4;
the subsequent type-separated aggregate and Ready/Advance safety corrections
advanced it to version 6, the explicit progress-snapshot API rename advanced
it to 7, and removal of the redundant two-ID leadership-transfer declaration
advanced it to 8. The later Phase 4 boundary-helper additions leave the
marker at version 9 for that milestone. Phase 6's private log ownership
advances the current marker to version 10.

## Null and nil semantics

A null descriptor pointer is invalid unless a future API explicitly documents
an optional descriptor. It does not encode a Go nil slice.

- Go nil: a non-null descriptor containing
  `{data = NULL, len = 0, is_nil = true}`.
- Go empty but non-nil: a non-null descriptor containing
  `{data = NULL, len = 0, is_nil = false}`.
- Non-empty: a non-null descriptor containing non-null data, positive length,
  and `is_nil = false`.

Nil-versus-empty is carried only by `is_nil` and never by descriptor pointer
nullness. Ownership is carried by view versus non-view type.

## Validation helper changed

The borrowed-view validator is now:

```c
bool raft_byte_view_valid(const raft_byte_view_t *view);
```

It rejects a null descriptor, a positive length with null data, non-null data
at zero length, and an `is_nil` non-empty payload.
`raft_bytes_valid(const raft_bytes_t *bytes)` was already pointer-based and
continues to reject null.

No optional validator was added because none of the current payload-taking
public APIs has optional descriptor semantics.

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
- `OUTPUT/C_PORT_TRACKING_GAPS.md`
- `OUTPUT/RAWNODE_API_PARITY_FOR_C_PORT.md`
- `OUTPUT/C_PORT_FOLLOWUP_DOCS_RESULT.md`

## Tests updated

The C skeleton tests now pass descriptor addresses and verify:

- `raft_byte_view_valid(NULL)` is false;
- Propose and ReadIndex reject null descriptor pointers;
- canonical nil is accepted through a non-null descriptor with
  `is_nil = true`;
- canonical empty is accepted through a non-null descriptor without
  `is_nil`;
- positive length with null data is rejected;
- the existing noncanonical zero-length, view/owned validation, owned-free,
  and lifecycle/stub cases remain covered.

## Public struct-by-value audit

No struct-by-value argument remains in the public C header.

The only non-pointer typedef arguments that remain are scalar enum values:

- `raft_log_level_t level` in the logger callback;
- `raft_snapshot_status_t status` in `raft_raw_node_report_snapshot`.

These are enums rather than struct descriptors and intentionally remain
passed by value. Integer IDs, handles, sizes, and counts also remain scalar
value arguments.

## Commands and results

Strict C skeleton build/test:

```sh
make clean-c && make test-c
```

Result: passed with C11 and
`-Wall -Wextra -Werror -Wpedantic`.

Default pure-Go suite:

```sh
mkdir -p .tmp .gocache
TMPDIR="$PWD/.tmp" GOTMPDIR="$PWD/.tmp" GOCACHE="$PWD/.gocache" go test ./...
```

Result: passed for every repository package. The default path still does not
compile or link the C subtree.

## Open questions

- Future optional payload fields, if exposed as direct API arguments, should
  use an explicitly named optional validator rather than weakening
  `raft_byte_view_valid`.
- Phase 3 still needs conversion helpers that always allocate/populate a
  descriptor object, including for Go nil and empty slices, for the duration
  of each C call.
- Storage callback allocation and ownership rules remain to be finalized; this
  correction changes only how descriptors are passed, not callback lifetime.

The subsequent type-separated aggregate and cgo-shim rules are recorded in
`C_TYPE_SEPARATED_PAYLOAD_API_RESULT.md`; the binding-safe aggregate,
Ready/Advance, Storage callback, and batching audit is in
`C_GO_POINTER_SAFE_RAWNODE_API_RESULT.md`.
