# Phase 18 C Status Progress Inflights Result

## Outcome

The C-backed `RawNode.Status()` now returns a non-nil, independent
`tracker.Inflights` clone in every leader progress row, matching the original
Go implementation's public Status semantics.

The correction is limited to Status reporting. It does not change the live C
inflight tracker, append flow control, replication, Ready generation,
persistence, or storage behavior. `RawNode.WithProgress()` remains
intentionally scalar-only and continues to report `Inflights == nil`.

This phase fixes only the `Status.Progress` inflight-window mismatch identified
by `OUTPUT/C_PORT_SEMANTIC_COMPLETENESS_AUDIT.md`.

## Original Go behavior

`getStatus` populates `Status.Progress` only while the node is leader. It calls
`getProgressCopy`, which copies every `tracker.Progress` value and replaces
the copied `Inflights` pointer with `pr.Inflights.Clone()`.

The clone contains:

- the configured maximum number of inflight messages;
- the configured maximum number of inflight bytes;
- the current number and aggregate payload size of active append messages;
- the active append records, each containing the last log index and payload
  byte count for that message.

This state supports the public `Count`, `Full`, `Add`, `FreeLE`, `Clone`, and
`Progress.String` behavior without sharing memory with the live Raft tracker.
An earlier Status value therefore remains a point-in-time snapshot after
replication advances.

`RawNode.WithProgress` has different intentional semantics: the original Go
implementation explicitly sets `Inflights` to nil before calling the visitor.

## Previous C behavior

The C tracker already stored the complete equivalent state in
`raft_inflights_internal_t`: `start`, `count`, aggregate `bytes`, configured
`size` and `max_bytes`, and a ring buffer of `(index, bytes)` records.

The information was lost only in the reporting path:

1. `raft_tracker_progress_snapshot` copied scalar `Progress` fields but had no
   inflight representation.
2. `raft_raw_node_status` reused that scalar snapshot format.
3. `cProgress` deliberately constructed Go progress with
   `Inflights: nil`, which is correct for `WithProgress` but not for Status.

Consequently, C-backed Status callers could not inspect the flow-control
window, and methods such as `Progress.String()` or `Inflights.Count()` could
panic.

## Implementation

### Status-only owned C snapshot

`c/include/raft/raft.h` now defines:

- `raft_inflight_snapshot_t`, containing one active `(index, bytes)` record;
- `raft_inflights_snapshot_t`, containing the configured limits and an owned
  array of active records;
- `raft_status_progress_t`, containing the existing scalar progress snapshot
  plus the inflight snapshot.

Only `raft_status_t.progress` uses the full representation. The existing
`raft_progress_snapshot_t` and `raft_raw_node_progress_snapshot` API remain
unchanged for `WithProgress`.

`raft_tracker_status_progress_snapshot` copies each live inflight ring in
logical order from oldest to newest. It stores only active records, rather
than duplicating inactive ring capacity. The configured limits plus those
records are sufficient to reconstruct equivalent public
`tracker.Inflights` behavior.

The snapshot is fully owned:

- no pointer into the live tracker is exposed;
- partial allocation failures recursively clean up completed rows;
- `raft_status_free` releases every nested record array before releasing the
  progress rows.

The scalar field-copy logic was factored into one private helper shared by
the Status and `WithProgress` snapshot builders. No tracker state or behavior
was changed.

### Go reconstruction

`cStatusProgress` in `convert_cgo.go`:

1. converts the existing scalar progress fields;
2. creates a fresh `tracker.Inflights` with the copied `size` and
   `max_bytes`;
3. replays the active `(index, bytes)` records in logical order with `Add`;
4. assigns the independent result to `Progress.Inflights`.

`rawnode_cgo.go` uses this conversion only in `Status()`. Its
`WithProgress()` conversion remains unchanged and continues to return nil
inflights, matching Go.

No duplicate long-lived reporting state was introduced.

## Tests

### Native C

`test_status_progress_inflights` creates a two-voter leader with a two-message
inflight limit, catches the follower up into replicate state, and proposes two
one-byte entries.

It verifies that:

- the status row retains the peer type and scalar replicate progress;
- the full window reports configured limits `size=2` and `max_bytes=4`;
- its active records are `(index=3, bytes=1)` and
  `(index=4, bytes=1)` in logical order;
- constructing Status does not mutate the live tracker;
- acknowledging index 3 leaves only the index 4 record and unpauses flow;
- the earlier status snapshot remains unchanged;
- recursive Status cleanup is accepted by sanitizers and Valgrind.

### Shared Go/C parity

`status_inflights_parity_test.go` runs unchanged against the default Go
RawNode and the C-backed RawNode.

Through only public APIs, it verifies:

- both local and follower Status rows contain non-nil inflights;
- two outstanding appends produce `Count() == 2`, `Full() == true`, and
  `Progress.String()` includes the full inflight window;
- a clone of the exported window has the correct `FreeLE` acknowledgement
  boundary;
- after acknowledgements, the current Status count changes from two to one
  to zero while match and flow-paused fields advance consistently;
- an earlier Status value remains unchanged after the live tracker advances;
- `WithProgress` still reports nil inflights for both rows.

Existing Status, Progress, flow-control, lifecycle, native, default Go, and
C-backed suites continue to pass.

## Files changed

Implementation:

- `c/include/raft/raft.h`;
- `c/src/tracker.h`;
- `c/src/tracker.c`;
- `c/src/raft_core.h`;
- `c/src/raft_core.c`;
- `c/src/raw_node.c`;
- `convert_cgo.go`;
- `rawnode_cgo.go`.

Tests:

- `c/tests/raft_core_test.c`;
- `status_inflights_parity_test.go` (new).

Documentation:

- `OUTPUT/PHASE18_C_STATUS_PROGRESS_INFLIGHTS_RESULT.md`.

## Validation

All commands below passed on 2026-07-31.

Focused native and shared parity tests:

```text
make test-c
go test -count=1 ./... \
  -run '^TestRawNodeStatusInflightsParity$'
CGO_ENABLED=1 go test -count=1 -tags=cgo_raft ./... \
  -run '^TestRawNodeStatusInflightsParity$'
```

Existing Status, Progress, flow-control, and cgo lifecycle tests:

```text
go test -count=1 \
  -run 'Test(RawNodeStatus|RawNodeWithProgressReturnsCopiesWithoutInflights|RawNodeStatusInflightsParity|ProgressLeader|ProgressResumeByHeartbeatResp|ProgressPaused|ProgressFlowControl)$' \
  . ./tracker
CGO_ENABLED=1 go test -count=1 -tags=cgo_raft \
  -run 'Test(RawNodeStatusInflightsParity|CGoRawNodeLifecycle)$' .
```

Complete default and C-backed Go suites:

```text
go test -count=1 ./...
CGO_ENABLED=1 go test -count=1 -tags=cgo_raft ./...
```

Strict cgo pointer checking and race detection:

```text
CGO_ENABLED=1 GOEXPERIMENT=cgocheck2 \
  go test -count=1 -tags=cgo_raft ./...
CGO_ENABLED=1 go test -count=1 -race -tags=cgo_raft ./...
```

AddressSanitizer, LeakSanitizer, and UndefinedBehaviorSanitizer:

```text
make test-c-sanitize
```

Strict Clang C11 build and native tests:

```text
make -C c clean test CC=clang
```

Valgrind with full leak checking ran successfully for all eight native C test
binaries.

Both TLA build variants also passed:

```text
go test -count=1 -tags=with_tla ./...
CGO_ENABLED=1 go test -count=1 -tags='cgo_raft with_tla' ./...
```

The ordinary GCC native build was restored and rerun after the alternate
builds. No test failure, compiler warning, sanitizer finding, Valgrind error,
race, or cgo pointer violation was reported.

## Remaining concerns

- The comprehensive upstream Raft interaction suite remains excluded under
  `cgo_raft` by its existing build constraints. The new backend-neutral
  two-node parity test directly covers the public Status behavior.
- The C status format stores active inflight records in logical order and the
  Go layer reconstructs a functionally equivalent fresh ring. It does not
  preserve inactive ring-buffer slots, which are private implementation
  details and have no public behavioral effect.
- There are no remaining TODOs for this audit finding.
- No other semantic-audit finding was addressed in this phase.
