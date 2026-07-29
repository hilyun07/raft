# WithProgress C Port Audit Result

## Scope and current status

This audit covered only `RawNode.WithProgress`, the C progress output shape,
and the future C-backed Go binding contract.

Before this follow-up, the C skeleton was already callback-free and returned
an owned array shape, but it did not fully satisfy the final design:

- the API was named `raft_raw_node_progress`, which did not make snapshot/copy
  semantics explicit;
- rows were named `raft_progress_record_t`;
- several planning documents still allowed or discussed a synchronous
  visitor callback;
- no focused Go regression test asserted the existing `WithProgress` copy,
  role, and `Inflights` behavior.

After this follow-up, the compile-safe C boundary has the required
snapshot-only shape. The actual C progress tracker and C-backed Go RawNode do
not exist yet, so real C-backed enumeration and Go conversion remain deferred
to their planned core/binding phases. The skeleton returns
`RAFT_ERR_NOT_IMPLEMENTED` rather than incorrectly reporting an empty tracker.

## Files inspected

Implementation and tests:

- `rawnode.go`;
- `rawnode_test.go`;
- `tracker/progress.go`;
- `tracker/state.go`;
- `c/include/raft/raft.h`;
- `c/src/raft_internal.h`;
- `c/src/raw_node.c`;
- `c/tests/raw_node_skeleton_test.c`;
- `c/Makefile` and the repository `Makefile`.

Porting documents:

- `prompts/RAFT_C_PORTING_SPEC.md`;
- `OUTPUT/RAWNODE_API_PARITY_FOR_C_PORT.md`;
- `OUTPUT/READY_OWNERSHIP_FOR_C_PORT.md`;
- `OUTPUT/C_GO_POINTER_SAFE_RAWNODE_API_RESULT.md`;
- `OUTPUT/C_TYPE_SEPARATED_PAYLOAD_API_RESULT.md`;
- `OUTPUT/C_PORT_TRACKING_GAPS.md`;
- `OUTPUT/C_PORT_FOLLOWUP_DOCS_RESULT.md`;
- `OUTPUT/PHASE1_RAWNODE_BOUNDARY_PLAN.md`;
- `OUTPUT/PHASE1_RAWNODE_BOUNDARY_RESULT.md`;
- `OUTPUT/PHASE2_C_RAWNODE_SKELETON_RESULT.md`;
- `OUTPUT/SENTINEL_NODE_IDS_FOR_C_PORT.md`;
- the remaining current C-port config, error, wire-compatibility, and byte
  payload planning/result documents under `OUTPUT/`.

No Go file imports C or defines a C-backend build-tag implementation in the
current repository.

## Final C API decision

The public boundary is now:

```c
typedef struct raft_progress_snapshot {
    uint64_t id;
    raft_progress_type_t type;
    raft_progress_t progress;
} raft_progress_snapshot_t;

int raft_raw_node_progress_snapshot(
    const raft_raw_node_t *raw_node,
    raft_progress_snapshot_t **out,
    size_t *out_len);

void raft_progress_snapshot_array_free(
    raft_progress_snapshot_t *snapshots,
    size_t len);
```

The contract is:

- `WithProgress` is observational;
- a successful implementation returns a C-owned point-in-time copy;
- no row contains a pointer into the internal tracker;
- caller mutation cannot affect tracker state;
- no borrowed `*_view_t` output is used;
- empty success is `*out == NULL` and `*out_len == 0`;
- `id` is a real tracked member ID, never a node-ID sentinel;
- `type` preserves peer versus learner;
- `raft_progress_t` contains the exported scalar progress state, including
  flow-paused and learner state, but deliberately has no live inflights queue;
- the caller frees the array with
  `raft_progress_snapshot_array_free`.

The private skeleton ABI marker moved from 6 to 7 for the public symbol/type
rename. The later leadership-transfer API cleanup moves the current marker to
8, and the Phase 4 Node-boundary helpers move it to 9.

## Future Go C-backed implementation

The public Go method remains:

```go
func (rn *RawNode) WithProgress(
    visitor func(id uint64, typ ProgressType, pr tracker.Progress),
)
```

Its C-backed implementation must:

1. call `raft_raw_node_progress_snapshot` once;
2. map every row into a Go `tracker.Progress`;
3. map `RAFT_PROGRESS_PEER` and `RAFT_PROGRESS_LEARNER` to the existing
   `ProgressType` values;
4. explicitly set `pr.Inflights = nil`;
5. invoke the user visitor in Go;
6. free the C-owned array once, including when a later conversion path exits
   early.

It must not add `raft_raw_node_with_progress(rn, callback, handle)`, call Go
once per peer from C, retain a Go closure handle, or expose an internal C
progress pointer.

This design costs one producer cgo call and one free call per Go
`WithProgress` invocation, rather than one C-to-Go transition per peer.

## Changes made

- Renamed the owned row type from `raft_progress_record_t` to the explicit
  `raft_progress_snapshot_t`.
- Renamed the producer from `raft_raw_node_progress` to
  `raft_raw_node_progress_snapshot`.
- Renamed the matching free function to
  `raft_progress_snapshot_array_free`.
- Added public-header ownership, copy, empty-result, sentinel-ID, and
  no-inflights/no-callback comments.
- Updated `raft_status_t.progress` to use the snapshot row type while retaining
  its separate leader-only `Status.Progress` semantics.
- Kept the skeleton policy: valid calls reset outputs and return
  `RAFT_ERR_NOT_IMPLEMENTED`; invalid output or RawNode pointers return
  `RAFT_ERR_INVALID_ARGUMENT`.
- Updated C tests for the renamed API, output reset, null arguments, and
  null-safe array freeing.
- Added `TestRawNodeWithProgressReturnsCopiesWithoutInflights`, which verifies
  the existing pure-Go peer/learner mapping, `Inflights == nil`, and
  copy-isolation behavior.
- Updated the relevant planning/result documents to prohibit the previously
  discussed callback alternative.

No consensus logic, C tracker implementation, cgo binding, build tags, or
pure-Go production behavior changed.

## Test results

Passed:

```text
make test-c
```

This compiles the C11 skeleton with
`-Wall -Wextra -Werror -Wpedantic` and runs
`c/.build/raw_node_skeleton_test`.

Passed:

```text
go test ./...
```

All packages passed, including the new pure-Go `WithProgress` regression
test.

A source audit found no C progress callback declaration/implementation and no
Go cgo binding.

## Remaining gaps and deferred tests

- `raft_raw_node_progress_snapshot` has no real tracker to enumerate yet; its
  real allocation/copy logic belongs with the Phase 7 C progress tracker, not
  this skeleton-only task.
- The C-backed Go `WithProgress` method and its error decoding belong in the
  future tagged binding phase. There is currently no C-backed Go type on which
  to implement or compile it.
- Runtime C-backed tests for peer/learner conversion, copy isolation, visitor
  execution in Go, and `Inflights == nil` are therefore deferred. They must be
  added when the binding and tracker exist.
- The future C test must obtain a non-empty tracker snapshot, mutate and free
  the returned rows, then obtain another snapshot and verify the internal
  values were unchanged.
- The future Go binding test must ensure the visitor is never entered through
  an exported Go callback from C.

These are implementation-stage gaps, not unresolved API-design questions.
