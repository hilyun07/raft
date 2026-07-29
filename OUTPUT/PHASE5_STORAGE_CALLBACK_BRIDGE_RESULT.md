# Phase 5 Storage Callback Bridge Result

## Outcome

The opt-in C-backed RawNode now has a complete, bridge-level-tested adapter
from the application-provided Go `Storage` interface to the C
`raft_storage_ops_t` callback ABI:

```text
Go Config.Storage
  -> *storageBridge
  -> runtime/cgo.Handle
  -> uintptr_t handle + copied raft_storage_ops_t
  -> C callback
  -> exported Go bridge
  -> Go Storage method
  -> scalar or C-owned deep-copy result
```

No Raft consensus, log, transport, or C-only storage logic was added. The
default build remains pure Go.

## Files and documents inspected

All documents requested by the Phase 5 prompt were present:

- `prompts/RAFT_C_PORTING_SPEC.md`;
- `OUTPUT/PORTING_BASELINE.md`;
- `OUTPUT/PHASE1_RAWNODE_BOUNDARY_PLAN.md`;
- `OUTPUT/PHASE1_RAWNODE_BOUNDARY_RESULT.md`;
- `OUTPUT/C_PORT_FOLLOWUP_DOCS_RESULT.md`;
- `OUTPUT/SENTINEL_NODE_IDS_FOR_C_PORT.md`;
- `OUTPUT/RAWNODE_API_PARITY_FOR_C_PORT.md`;
- `OUTPUT/CONFIG_FIELDS_FOR_C_PORT.md`;
- `OUTPUT/RAFTPB_WIRE_COMPAT_FOR_C_PORT.md`;
- `OUTPUT/READY_OWNERSHIP_FOR_C_PORT.md`;
- `OUTPUT/ERROR_MAPPING_FOR_C_PORT.md`;
- `OUTPUT/C_PORT_TRACKING_GAPS.md`;
- `OUTPUT/PHASE2_C_RAWNODE_SKELETON_RESULT.md`;
- `OUTPUT/C_TYPE_SEPARATED_PAYLOAD_API_RESULT.md`;
- `OUTPUT/C_GO_POINTER_SAFE_RAWNODE_API_RESULT.md`;
- `OUTPUT/WITH_PROGRESS_C_PORT_AUDIT_RESULT.md`;
- `OUTPUT/TRANSFER_LEADERSHIP_API_CLEANUP_RESULT.md`;
- `OUTPUT/STEP_FOR_NODE_LAYERING_DOC_UPDATE_RESULT.md`;
- `OUTPUT/PHASE4_GO_RAWNODE_BINDING_SKELETON_RESULT.md`.

No requested document was missing.

Implementation inspection covered `Storage` and `MemoryStorage`, the tagged
RawNode owner, C/Go conversion and error mapping, the public/private C headers,
the C skeleton constructor/free functions, both Makefiles, and existing
tagged/C tests.

## Files changed

Added:

- `storage_bridge_cgo_test.go`: focused C-to-Go callback, ownership, error,
  panic, and handle-lifecycle tests;
- `OUTPUT/PHASE5_STORAGE_CALLBACK_BRIDGE_RESULT.md`: this result.

Updated:

- `storage_bridge_cgo.go`;
- `rawnode_cgo.go`;
- `rawnode_cgo_bridge.c`;
- `c/include/raft/raft.h`;
- `OUTPUT/CONFIG_FIELDS_FOR_C_PORT.md`;
- `OUTPUT/C_GO_POINTER_SAFE_RAWNODE_API_RESULT.md`;
- `OUTPUT/C_PORT_TRACKING_GAPS.md`;
- `OUTPUT/ERROR_MAPPING_FOR_C_PORT.md`;
- `OUTPUT/PHASE2_C_RAWNODE_SKELETON_RESULT.md`;
- `OUTPUT/PHASE4_GO_RAWNODE_BINDING_SKELETON_RESULT.md`;
- `OUTPUT/READY_OWNERSHIP_FOR_C_PORT.md`.

## Final C Storage ABI

The public ABI retains the repository's vector naming:

```c
typedef struct raft_storage_ops {
    uintptr_t handle;
    raft_storage_initial_state_fn initial_state;
    raft_storage_entries_fn entries;
    raft_storage_term_fn term;
    raft_storage_first_index_fn first_index;
    raft_storage_last_index_fn last_index;
    raft_storage_snapshot_fn snapshot;
} raft_storage_ops_t;
```

The callbacks cover exactly the Go `Storage` interface:

- `InitialState`;
- `Entries`;
- `Term`;
- `FirstIndex`;
- `LastIndex`;
- `Snapshot`.

`raft_raw_node_new` copies the table into the opaque C RawNode. It does not
retain the address of the Go caller's temporary table. The `handle` is the
integer value of a `runtime/cgo.Handle`, never a raw Go pointer.

## Go bridge and C helpers

The exported callback implementations are:

- `goRaftStorageInitialState`;
- `goRaftStorageEntries`;
- `goRaftStorageTerm`;
- `goRaftStorageFirstIndex`;
- `goRaftStorageLastIndex`;
- `goRaftStorageSnapshot`.

`rawnode_cgo_bridge.c` now provides:

```c
void raft_go_storage_ops_init(raft_storage_ops_t *ops, uintptr_t handle);
```

It fills the table with the exported Go function symbols. `NewRawNode`
constructs the handle first, initializes a stack-local table through this
pointer-based helper, and passes the table to the C constructor.

The same C file contains private call-through helpers for bridge tests. Each
helper creates the table in C and invokes one function pointer. This exercises
the real Go-to-C-to-Go call direction without requiring the inert consensus
core to consume Storage yet. These helpers are not part of the public
`raft/raft.h` API.

The `//export` Go file's C preamble contains declarations only. C function
definitions remain in the `.c` translation unit, avoiding duplicate cgo
definitions.

## Handle ownership and lifecycle

The tagged `RawNode` stores the live `cgo.Handle`. Destruction order is:

1. destroy any outstanding C Ready;
2. destroy the C RawNode, after which no C code can invoke Storage;
3. delete the `cgo.Handle`;
4. clear the Go fields and finalizer.

The constructor deletes the handle if `raft_raw_node_new` fails after handle
creation. A focused test reaches this branch using a configuration accepted by
the current Go validator but rejected by strict C enum validation, captures
the handle through the internal lifecycle-test observer, and verifies that the
handle is no longer valid.

Another test invokes `Term` through the live RawNode handle, destroys the
RawNode, then verifies that the deleted handle cannot be resolved. The public
Go RawNode API still has no Close method, so direct RawNode users rely on the
finalizer for eventual cleanup; tests and future Node ownership use the
package-private deterministic `destroy` hook.

## Panic and error handling

Every exported callback installs a panic recovery boundary. A panic never
crosses into C; it returns:

```text
RAFT_ERR_PANIC_FROM_GO_CALLBACK
```

When the handle remains live, the bridge retains a Go diagnostic. Cleanup
defer paths reset/free pointer-bearing output before returning the panic code.

Storage errors map as follows:

```text
ErrCompacted                         -> RAFT_ERR_STORAGE_COMPACTED
ErrUnavailable                       -> RAFT_ERR_STORAGE_UNAVAILABLE
ErrSnapshotTemporarilyUnavailable    -> RAFT_ERR_SNAPSHOT_TEMPORARILY_UNAVAILABLE
other Storage error                  -> RAFT_ERR_FATAL
allocation failure                   -> RAFT_ERR_OUT_OF_MEMORY
invalid output pointer/result shape  -> RAFT_ERR_INVALID_ARGUMENT
```

Invalid result shapes include a nil ConfState from `InitialState`, a nil Entry
inside an Entries result, and a nil successful Snapshot result. Outputs start
zeroed, and every partial allocation path is safe for the matching recursive
free function.

## Ownership and deep-copy rules

`Term`, `FirstIndex`, and `LastIndex` write only scalar results.

`InitialState` writes HardState scalars and deep-copies all ConfState vectors:

- voters;
- outgoing voters;
- learners;
- next learners;
- auto-leave.

`Entries` uses one callback per requested range. It allocates one C-owned
`raft_entry_t` vector, copies all scalar fields, and allocates each non-empty
Entry.Data payload separately. There is no callback per entry.

`Snapshot` allocates one owned Snapshot graph, including metadata, ConfState,
and Data. The callback never returns an application Go slice pointer to C.

Nil-versus-empty bytes are preserved:

```text
nil:               data = NULL, len = 0, is_nil = true
empty non-nil:     data = NULL, len = 0, is_nil = false
non-empty:         data != NULL, len > 0, is_nil = false
```

The C consumer owns successful callback output and must call
`raft_conf_state_free`, `raft_entry_vec_free`, or `raft_snapshot_free` as
appropriate.

## Snapshot large-object caveat

The compatibility bridge copies Snapshot.Data once from Go storage into
C-owned memory. This can be expensive for large snapshots, but it keeps
ownership unambiguous and preserves existing Go Ready/MsgSnap transport
semantics. A later streaming or out-of-band optimization may remove that copy
only if it preserves raftpb compatibility, Ready lifetime, and transport
completion through `ReportSnapshot`.

## Tests added

The tagged bridge tests cover:

- Term callback invocation and returned value;
- FirstIndex and LastIndex scalar output;
- InitialState HardState and every ConfState vector;
- batched Entries conversion and recursive free;
- Entry.Data nil, empty non-nil, and non-empty representations;
- proof that Entry.Data and ConfState vectors are copied, not borrowed;
- Snapshot metadata, ConfState, Data, and recursive free;
- Snapshot.Data nil, empty non-nil, and deep-copy behavior;
- compacted, unavailable, temporary-snapshot, and unknown error mapping;
- callback panic recovery and diagnostic retention;
- rejection of nil ConfState, Entry, and Snapshot results;
- live-handle callback use;
- destroy-before-handle-delete ordering;
- handle deletion on C constructor failure.

The tests allocate pointer-bearing output descriptors in C memory. They do not
pass Go-allocated descriptor structs containing pointers into C.

## Commands run

```text
gofmt -w rawnode_cgo.go storage_bridge_cgo.go storage_bridge_cgo_test.go
```

Completed successfully.

```text
CGO_ENABLED=1 go test -tags=cgo_raft -run '^TestCGoStorage' -v .
```

All focused bridge tests passed.

```text
make test-c
```

The C11 warning-as-error build and skeleton test binary passed.

```text
GOEXPERIMENT=cgocheck2 CGO_ENABLED=1 \
  go test -tags=cgo_raft -run '^TestCGoStorage' .
```

Passed with strict cgo pointer checking.

```text
make test-cgo-raft
```

All tagged packages passed.

```text
mkdir -p .tmp .gocache
TMPDIR=$PWD/.tmp GOTMPDIR=$PWD/.tmp GOCACHE=$PWD/.gocache go test ./...
```

All default pure-Go packages passed.

```text
git diff --check
```

Passed.

## Remaining gaps and deferred work

- The C RawNode constructor still does not invoke `InitialState` as part of
  full Raft initialization.
- Phase 6 now initializes a private C log through `FirstIndex`/`LastIndex`.
  That log consumes batched `Entries`, `Term`, and owned `Snapshot` callback
  results and frees successful owned outputs. Consensus-state consumers and
  end-to-end Ready behavior remain later work.
- Allocation-failure cleanup is implemented, but deterministic OOM fault
  injection across every allocation site remains future sanitizer/fault-test
  work.
- Deadlock/reentrancy policy remains mandatory when the C core starts invoking
  callbacks: do not hold a Go RawNode/Node mutex or a C raft mutex across a
  callback, and Storage must not reenter the same raft node.
- A native C `MemoryStorage` implementation is deferred.
- Snapshot streaming/out-of-band optimization is deferred; this phase retains
  the one-copy compatibility bridge.
- End-to-end callback use by elections, log restoration, Ready, and snapshot
  transport remains tied to the later C consensus/log phases.

## Phase 7 follow-up

The C RawNode constructor now invokes the existing `InitialState` callback
once, consumes its C-owned HardState/ConfState output, and frees the ConfState
graph. Log reads during election and replication continue through the same
batched callback table. No raw Go pointer is stored by the C core; the storage
handle remains an opaque `uintptr_t`.
