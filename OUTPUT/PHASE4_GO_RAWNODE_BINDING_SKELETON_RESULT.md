# Phase 4 Go RawNode Binding Skeleton Result

## Outcome

Phase 4 adds an opt-in Go binding skeleton for the C RawNode under:

```text
cgo_raft
```

The default build remains the existing pure-Go RawNode. Under
`cgo_raft && cgo`, `RawNode` owns an opaque `*C.raft_raw_node_t`, retains the
validated Go ID and Logger, owns a `runtime/cgo.Handle` for Storage, and
performs input/output conversion at the C boundary.

This phase does not add Raft consensus logic. Operations whose C core
semantics do not yet exist continue to return
`RAFT_ERR_NOT_IMPLEMENTED`; Go error-returning methods decode that error, and
void/output-only Go methods panic through the retained Go Logger rather than
pretending the operation succeeded.

## Documents and code inspected

All requested documents existed:

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
- `OUTPUT/TRANSFER_LEADERSHIP_API_CLEANUP_RESULT.md`.

No requested document was missing.

The implementation audit covered `rawnode.go`, `bootstrap.go`, `node.go`,
`raft.go`, `storage.go`, `status.go`, `read_only.go`, the generated raftpb
types, tracker/quorum types, the public/private C headers, C skeleton source,
C tests, and both Makefiles.

## Files added

| File | Purpose |
| --- | --- |
| `rawnode_cgo.go` | Tagged C-backed RawNode owner and complete Go method skeleton. |
| `convert_cgo.go` | C-resident borrowed input descriptors, owned-output conversion, and recursive message/snapshot/entry conversion. |
| `errors_cgo.go` | Central C-to-Go error mapping and shared RawNode Step errors for the tagged build. |
| `storage_bridge_cgo.go` | `cgo.Handle` Storage bridge callbacks, panic recovery, precise storage errors, and C-owned callback output allocation. |
| `rawnode_cgo_bridge.c` | Tagged C translation unit, callback table constructor, and inclusion of the existing C skeleton implementation. |
| `rawnode_cgo_test.go` | Tagged lifecycle, boundary, nil/empty flat-byte, error, status, and validation tests. |
| `OUTPUT/PHASE4_GO_RAWNODE_BINDING_SKELETON_RESULT.md` | This result. |

## Existing files updated

- `rawnode.go` and `bootstrap.go` now use `//go:build !cgo_raft`; their code is
  otherwise unchanged.
- `c/include/raft/raft.h` and `c/src/raw_node.c` add the two minimal Node
  boundary helpers:

  ```c
  raft_raw_node_step_for_node
  raft_raw_node_has_progress
  ```

  The former preserves the public-Step versus Node-internal-Step distinction.
  The latter is scalar and never exposes a progress pointer. Both are honest
  skeletons.
- `c/src/raft_internal.h` used marker version 9 at completion of this phase;
  the Phase 6 log integration advances the current marker to version 10.
- `c/tests/raw_node_skeleton_test.c` covers both helpers.
- The root Makefile adds:

  ```text
  make test-cgo-raft
  ```

- Existing root and `rafttest` semantic tests use `!cgo_raft`. They continue to
  run unchanged by default, but are not run against an intentionally inert C
  core. The tagged build runs the focused binding skeleton tests instead.
- Current API parity, Phase 2, pointer-safety, prior correction, and result
  documents were updated for the new helpers and current ABI marker.

## RawNode method coverage

| Go method/boundary | Tagged binding |
| --- | --- |
| `NewRawNode` | Validates/narrows Config, creates a Storage `cgo.Handle`, builds the C callback table, calls `raft_raw_node_new`, and installs a finalizer. |
| `ID`, `Logger`, `AsyncStorageWritesEnabled` | Return Go-side cached immutable/config metadata. |
| `HasProgress` | Calls scalar `raft_raw_node_has_progress`; currently false because the C tracker is not implemented. |
| `Tick`, `TickQuiesced` | Direct C calls; current C stubs are inert. |
| `Campaign` | Calls the C stub and returns decoded error. |
| `Propose` | One `raft_raw_node_propose_from_parts` call preserving nil versus empty. |
| `ProposeConfChange` | Builds a temporary C-resident V2 descriptor/change array and makes one semantic C call. |
| `ApplyConfChange` | Uses the same safe input path, a C-resident output container, deep-copy to Go ConfState, and C cleanup. |
| `Step` | Rejects unexpected local messages at the public boundary, constructs the complete C-resident message view graph, and calls `raft_raw_node_step` once. |
| `stepForNode` | Uses the same conversion and calls `raft_raw_node_step_for_node` once. |
| `Ready` | Obtains one C-owned Ready, batch-converts it, and destroys it once. |
| `readyWithoutAccept` / `acceptReady` | Retains the exact C-owned preview pointer until acceptance, then destroys it; it never reconstructs a C Ready from Go. |
| `HasReady` | Direct scalar C query; false in the inert skeleton. |
| `Advance` | Preserves `Advance(_ Ready)`, ignores the Go argument, and calls only `raft_raw_node_advance(rn.p)`. |
| `BasicStatus` | Converts fixed-size output. |
| `Status` | Uses a C-resident owned output container, batch-converts Config/progress, and recursively frees it. |
| `WithProgress` | Makes one C snapshot call, converts rows, maps peer/learner, forces `Inflights=nil`, invokes the visitor in Go, and frees once. |
| `ReportUnreachable`, `ReportSnapshot` | Scalar C calls with Go-side panic policy for skeleton/internal failures. |
| `TransferLeader` | Calls only `raft_raw_node_transfer_leader(rn, transferee)`. |
| `ForgetLeader` | Returns the decoded C error. |
| `ReadIndex` | One `raft_raw_node_read_index_from_parts` call preserving nil versus empty. |
| `Bootstrap` | Allocates one C peer descriptor array, preserves each Context’s nil/empty state, calls C once, and frees descriptors immediately. |
| `MustSync` | Preserved in the tagged build; compares entry count, term, and vote, not commit. |

## Public Step versus the Node actor Step path

The two C Step entry points are deliberately layered in one direction:

```text
external C / Go RawNode.Step
  -> raft_raw_node_step
       -> public RawNode.Step validation
       -> raft_raw_node_step_for_node
            -> direct C raft-core Step

Go node.run
  -> RawNode.stepForNode
  -> raft_raw_node_step_for_node
       -> direct C raft-core Step
```

`raft_raw_node_step_for_node` corresponds to the package-internal `r.Step`
calls historically made by `node.run`. It intentionally bypasses public
`RawNode.Step` validation and must not call `raft_raw_node_step`. This allows
the Go actor path to carry locally generated campaign, unreachable/snapshot
status, leadership-routing, and asynchronous-storage messages after its own
routing/filtering.

The C skeleton now enforces public local-message rejection in
`raft_raw_node_step` and then delegates downward. Public rejection of response
messages from unknown non-local peers remains deferred until the C progress
tracker exists; it must be added to `raft_raw_node_step` before the real core
is enabled. The Node actor binding already calls
`raft_raw_node_step_for_node` directly.

## Cgo pointer-safety decisions

The binding never passes the address of a Go-allocated pointer-containing C
descriptor to C.

- Propose and ReadIndex use scalar `*_from_parts` shims. Only pointer-free Go
  byte backing storage is borrowed during the call.
- Step, Bootstrap, and configuration-change inputs allocate descriptor
  structs/arrays with `C.calloc`. Their nested byte pointers may borrow Go
  byte backing arrays only for the single semantic call.
- Aggregate descriptors are freed immediately after return.
- `runtime.KeepAlive` protects the source Go object until C has returned.
- C must recursively deep-copy a view before retention.
- There are no field-by-field C builder calls and no per-entry semantic cgo
  calls.
- Ready, Status, ConfState, and progress results originate in C-owned memory;
  Go deep-copies them and then invokes one matching free/destroy path.

The tagged root test also passes with `GOEXPERIMENT=cgocheck2`.

## Storage bridge lifecycle and ownership

`NewRawNode` creates:

```text
cgo.Handle -> *storageBridge -> application Storage
```

Only the integer handle and C function pointers enter the copied
`raft_storage_ops_t`; no raw Go object pointer is stored in C.

The callbacks cover InitialState, Entries, Term, FirstIndex, LastIndex, and
Snapshot. They:

- recover panics and return `RAFT_ERR_PANIC_FROM_GO_CALLBACK`;
- retain a diagnostic on the Go bridge when a bridge is still reachable;
- map compacted, unavailable, and temporarily-unavailable snapshot errors
  precisely;
- map other Storage errors to fatal;
- deep-copy Entries as one C-owned array per range call;
- deep-copy ConfState and Snapshot graphs into C-owned allocations;
- clean partial allocation on failure.

`RawNode.destroy` destroys any outstanding Ready, then the C RawNode, and only
then deletes the `cgo.Handle`. A finalizer is the fallback because public Go
RawNode has no Close method. A later end-to-end Node integration can call the
package-private deterministic destroy hook after its actor stops.

Phase 5 added a private C call-through harness and focused tagged tests for
all six callbacks, including deep copies, nil/empty preservation, error/panic
mapping, and handle cleanup. Phase 6 now makes the C constructor invoke
`FirstIndex` and `LastIndex` to initialize its internal log. The log consumes
batched `Entries`, scalar `Term`, and owned `Snapshot` callback results as
needed and frees owned results. Full Raft initialization still defers
`InitialState` and consensus-state construction.

## Ready and Advance ownership

Public `Ready()` uses one C producer call, converts the full owned graph in Go,
and calls `raft_ready_destroy` once.

The Node preview path cannot free its C object immediately:

1. `readyWithoutAccept` obtains and converts a C-owned preview and retains its
   C pointer in the Go wrapper.
2. `acceptReady` passes that same C pointer to
   `raft_raw_node_accept_ready`.
3. The wrapper destroys the output object after acceptance.

`Advance(_ Ready)` never rebuilds a C descriptor. It ignores the Go argument
and calls the argument-free C API once. AsyncStorageWrites still rejects
Advance in Go before the C call.

`Ready.MustSync` comes directly from `raft_ready_t.must_sync`.

## WithProgress

The binding uses only:

```c
raft_raw_node_progress_snapshot
raft_progress_snapshot_array_free
```

There is no C-to-Go visitor callback, stored Go closure, borrowed progress
view, live tracker pointer, or exposed inflights queue. The current skeleton
returns `RAFT_ERR_NOT_IMPLEMENTED`, so non-empty runtime conversion waits for
the C tracker phase.

## Leadership transfer and Logger

Leadership transfer uses only:

```c
raft_raw_node_transfer_leader(rn, transferee)
```

The removed two-ID C API was not reintroduced. Go
`Node.TransferLeadership(ctx, lead, transferee)` remains a Go routing
operation whose `MsgTransferLeader` enters through `stepForNode` /
`raft_raw_node_step_for_node`.

The binding retains `Config.Logger` in Go and returns it from `Logger()`. It
does not add or call a C logger accessor. C failures are decoded first and Go
panic policy is applied in Go. No logger or TraceLogger callback bridge is
activated in this phase.

## Error policy

The centralized decoder preserves identity for:

- `ErrStopped`;
- `ErrProposalDropped`;
- `ErrCompacted`;
- `ErrUnavailable`;
- `ErrSnapshotTemporarilyUnavailable`;
- `ErrStepLocalMsg`;
- `ErrStepPeerNotFound`.

Invalid argument, not implemented, OOM, callback panic, fatal, and unknown
codes have distinct Go wrapper errors.

Methods with an `error` result return decoded errors. Methods without an error
result panic through the retained Go Logger for invalid, fatal, or
not-implemented C results. Tick and TickQuiesced cannot report the current
void C stubs.

## Validation

Passed:

```text
gofmt -w <all changed Go files>
go build -tags=cgo_raft ./...
make test-c
make test-cgo-raft
go test ./...
GOEXPERIMENT=cgocheck2 go test -tags=cgo_raft .
git diff --check
```

The default suite passed for all packages. The tagged suite passed the focused
binding tests and all backend-independent subpackage tests. The C11 skeleton
compiled with `-Wall -Wextra -Werror -Wpedantic` and its test binary passed.

## Remaining gaps

- Consensus-dependent C calls still return `RAFT_ERR_NOT_IMPLEMENTED`; no
  election, replication, log, membership, snapshot, read-index, transfer, or
  Ready semantics were invented in Go.
- Tick and TickQuiesced are inert C void stubs and cannot signal
  not-implemented status.
- The C constructor does not yet call Storage as part of consensus
  initialization. Bridge-level C-to-Go callback invocation is covered in
  Phase 5; real-core fatal-state propagation and end-to-end sanitizer tests
  remain deferred.
- `HasProgress` returns false until the C progress tracker exists.
- The C message ABI currently lacks protobuf scalar presence bits. Conversion
  therefore preserves values but cannot yet preserve every absent-versus-zero
  scalar distinction.
- The V2 configuration-change C API does not preserve legacy
  `ConfChange.ID` or select V1 versus V2 entry encoding. That remains an ABI
  and differential-test gap.
- Non-empty Ready, Status progress, WithProgress, ConfState, nested Message,
  and Snapshot output conversion will need runtime coverage when the C core
  can produce them.
- Direct RawNode users rely on a finalizer for eventual C/handle cleanup
  because the existing public API has no Close method. Deterministic Node-owned
  cleanup belongs in the later Node integration phase.
- The pure-Go semantic suites are intentionally excluded under `cgo_raft`
  while the C core is inert. Each later core phase must add C-backed parity
  tests and can re-enable applicable shared tests as semantics become real.
