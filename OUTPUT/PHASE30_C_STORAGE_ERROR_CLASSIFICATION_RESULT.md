# Phase 30: Storage Error Classification Compatibility

## Outcome

Phase 29's storage-error finding was confirmed and fixed.

`ErrSnapshotTemporarilyUnavailable` now has operation-specific behavior:

- when returned by `Storage.Snapshot`, the snapshot-send path consumes it as a
  retryable condition and does not latch an error;
- when the same sentinel escapes `InitialState`, `Term`, `Entries`,
  `FirstIndex`, or `LastIndex`, it is an unexpected storage failure and uses
  the Phase 19 terminal-error policy;
- `RawNode.Bootstrap` retains Go's pre-mutation exception and returns a
  `LastIndex` error without latching it.

Only two production lines of behavior changed:

1. the C terminal-result set now includes
   `RAFT_ERR_SNAPSHOT_TEMPORARILY_UNAVAILABLE`;
2. cgo constructor-fatal classification now includes
   `ErrSnapshotTemporarilyUnavailable`.

The existing snapshot-specific consumer still converts temporary Snapshot
unavailability to success before the terminal latch is reached. The public C
ABI, callback ABI, Storage bridge, snapshot representation, Node architecture,
and async-storage protocol did not change.

No other Phase 29 finding was addressed. In particular, the demoted-leader
proposal behavior was not modified or tested in this phase.

## Reference and source verification

The authoritative reference remains:

```text
etcd-io/raft v3.7.0
commit b867cf13f6bc0dae21204302df97bc2355c3af55
```

The repository's untagged Go implementation is that behavioral reference.
Phase 19 and Phase 29 were read before implementation:

- `OUTPUT/PHASE19_C_FATAL_ERROR_PROPAGATION_RESULT.md`;
- `OUTPUT/PHASE29_C_FINAL_DIFFERENTIAL_AUDIT_RESULT.md`.

The current source independently confirmed the Phase 29 reproduction:

```text
Storage.Term -> ErrSnapshotTemporarilyUnavailable

Go backend:
    RawNode.Step panics

C backend before Phase 30:
    RawNode.Step returns ErrSnapshotTemporarilyUnavailable
    C terminal error remains RAFT_OK
    Node can ignore the ordinary returned Step error
```

The new tests were run against the pre-fix code. Native C observed no sticky
error, and the cgo parity tests observed returned/non-panicking behavior for
constructor and log-lookup paths. The tests passed after the two production
changes.

## Exact Go v3.7.0 semantics

### Storage contract

`storage.go:42-47` states the general rule: an error from a Storage method
makes the Raft instance inoperable. The operation-specific exceptions are
defined by each call site.

`storage.go:91-95` gives
`ErrSnapshotTemporarilyUnavailable` special meaning specifically for
`Storage.Snapshot`: the state machine should wait for the application to
prepare the snapshot and call `Snapshot` again.

No other Storage method documents this sentinel as an expected result.

### Construction

`log.go:73-83` calls `Storage.FirstIndex` and `Storage.LastIndex` while
constructing the log and panics on every error.

`raft.go:439-447` calls `Storage.InitialState` and panics on every error.

Consequently, the snapshot-temporary sentinel from any of these three methods
is a constructor panic, not a retryable result.

### FirstIndex and LastIndex after construction

`log.go:300-319` panics on every error from the stable Storage
`FirstIndex`/`LastIndex` fallback. There is no snapshot-temporary exception.

### Term

`log.go:387-412` treats only `ErrCompacted` and `ErrUnavailable` as expected
term-lookup conditions. Every other Storage.Term error panics.

The vote path at `raft.go:1212-1222` obtains the local last entry ID and term,
so an unexpected Term, FirstIndex, or LastIndex failure must panic rather than
turn into a rejected vote.

### Entries

`log.go:503-520` treats `ErrCompacted` as a recoverable result. It panics for
`ErrUnavailable` and every other error, including
`ErrSnapshotTemporarilyUnavailable`.

Entries retrieval is used by replication, committed-entry construction for
Ready, conflict processing, and log scans. The error may therefore originate
while stepping a message or while constructing Ready.

### Snapshot

`log.go:293-297` returns the Storage.Snapshot result to the caller.

`raft.go:665-678` explicitly consumes
`ErrSnapshotTemporarilyUnavailable`, sends no snapshot, and allows a later
retry. Every other Snapshot error panics.

### Bootstrap exception

`bootstrap.go:34-41` returns any `Storage.LastIndex` error before changing
Raft state. This includes `ErrSnapshotTemporarilyUnavailable`.

Phase 19 deliberately preserved this exception. Phase 30 preserves it as
well.

## Storage-operation compatibility table

| Storage operation | Go v3.7.0 | C before Phase 30 | C after Phase 30 |
| --- | --- | --- | --- |
| `Snapshot` | snapshot-temporary is retryable; other errors panic | snapshot-temporary consumed as retryable | unchanged |
| `Term` | compacted/unavailable are expected; snapshot-temporary panics | returned as a non-terminal error | terminal and sticky |
| `Entries` | compacted may be returned; snapshot-temporary panics | returned as a non-terminal error | terminal and sticky |
| `FirstIndex` during construction | every error panics | constructor returned snapshot-temporary | constructor panics |
| `FirstIndex` after construction | every error panics | returned as a non-terminal error | terminal and sticky |
| `LastIndex` during construction | every error panics | constructor returned snapshot-temporary | constructor panics |
| `LastIndex` after construction | every error panics | returned as a non-terminal error | terminal and sticky |
| `InitialState` | every error panics | constructor returned snapshot-temporary | constructor panics |
| Bootstrap `LastIndex` | return error before mutation | returned and not latched | unchanged |

## Current C/cgo path and original root cause

### Callback transport

`errors_cgo.go:79-90` maps the Go sentinel to:

```text
RAFT_ERR_SNAPSHOT_TEMPORARILY_UNAVAILABLE
```

All callbacks preserve the original Go error for `errors.Is` and diagnostic
detail:

| Callback | Source |
| --- | --- |
| `InitialState` | `storage_bridge_cgo.go:266-304` |
| `Entries` | `storage_bridge_cgo.go:307-359` |
| `Term` | `storage_bridge_cgo.go:361-383` |
| `FirstIndex` | `storage_bridge_cgo.go:385-407` |
| `LastIndex` | `storage_bridge_cgo.go:409-431` |
| `Snapshot` | `storage_bridge_cgo.go:433-465` |

This mapping is intentionally operation-neutral. It transports the exact
result into C; it does not decide whether the result is expected at the
particular call site.

Phase 30 keeps this representation. A bridge test now explicitly verifies
that a non-Snapshot callback still transports the sentinel rather than
collapsing it into a generic error.

### Log and core propagation

The C log returns storage result codes to its callers:

- construction: `c/src/log.c:159-194`;
- FirstIndex/LastIndex: `c/src/log.c:204-222`;
- Term: `c/src/log.c:224-255`;
- Entries slice: `c/src/log.c:367-425`;
- Snapshot: `c/src/log.c:824-850`.

Phase 19's model is “expected conditions are consumed locally; unexpected
conditions become terminal if they escape.” Compacted and unavailable already
use this model.

Before Phase 30, `raft_result_is_terminal` at
`c/src/raft_core.c:52-57` omitted the snapshot-temporary code globally. This
was correct for one caller but incorrect for every other callback.

The actual snapshot consumer already retained operation context:

```text
raft_log_snapshot
    -> core_maybe_send_snapshot
    -> if RAFT_ERR_SNAPSHOT_TEMPORARILY_UNAVAILABLE:
           free partial message
           return RAFT_OK
```

This is at `c/src/raft_core.c:734-740`.

The root cause was therefore an incomplete terminal-on-escape set, not loss of
callback operation context and not an ABI limitation.

### cgo boundary

When a stateful C operation returns an error,
`rawnode_cgo.go:216-236` checks `raft_raw_node_error`. A latched result causes
the wrapper to panic before Node can discard an ordinary Step error.

Construction occurs before a C RawNode exists, so
`rawnode_cgo.go:187-193` has a separate constructor-fatal classification.
That list previously omitted the snapshot-temporary sentinel.

## Implementation

### Terminal-on-escape classification

`c/src/raft_core.c:52-61` now includes:

```c
result == RAFT_ERR_SNAPSHOT_TEMPORARILY_UNAVAILABLE
```

in `raft_result_is_terminal`.

This does not make every Snapshot occurrence fatal. It means that the result
is terminal only if it escapes an operation-specific expected handler and
reaches the Phase 19 latch.

The existing handler in `core_maybe_send_snapshot` consumes the expected
Snapshot result first by returning `RAFT_OK`. Thus the latch never sees the
retryable Snapshot case.

Every unexpected Term, Entries, FirstIndex, or LastIndex escape now records
the first terminal result. Subsequent stateful operations return that same
result.

### Constructor panic classification

`rawnode_cgo.go:187-193` now includes
`ErrSnapshotTemporarilyUnavailable` in `isCConstructorFatal`.

An occurrence from log construction or InitialState therefore uses the same
logger/panic path as the Go reference.

The cgo handle is still deleted on constructor failure before the panic is
raised. No ownership rule changed.

### Bootstrap

No Bootstrap code changed.

`c/src/raft_core.c:2380-2454` retains the `mutation_started` boundary. A
LastIndex result before mutation returns directly without
`raft_core_latch_error`. The Go wrapper consequently returns the original
snapshot-temporary error, and a later Bootstrap can succeed.

### ABI and ownership

- No public Go API changed.
- No public C API or structure changed.
- No ABI marker changed.
- No Storage callback signature changed.
- No allocation, copy, or free rule changed.
- No async-storage work message changed.
- No new error code was introduced.

## Call-site audit

| Path | Storage operation | Snapshot-temporary result after Phase 30 |
| --- | --- | --- |
| `NewRawNode` log construction | FirstIndex/LastIndex | constructor panic |
| `NewRawNode` core construction | InitialState | constructor panic |
| vote/PreVote freshness | LastIndex, FirstIndex, Term | core latches; RawNode/Node panic |
| append construction | LastIndex, Term, Entries | core latches; caller panics |
| append rejection/backtracking | Term, Entries | core latches; caller panics |
| conflict scan | Term, Entries | core latches; caller panics |
| commit/heartbeat invariant checks | LastIndex/Term where required | core latches; caller panics |
| Ready committed-entry construction | FirstIndex/LastIndex/Entries | RawNode latches; Ready panics |
| snapshot fallback send | Snapshot | consumed as retryable; no latch |
| snapshot restore from MsgSnap | no Storage.Snapshot call | not applicable |
| unstable Ready snapshot | no Storage.Snapshot call | not applicable |
| Bootstrap precheck | LastIndex | returned before mutation; no latch |
| async append/apply work | no error-bearing Storage callback | unchanged |
| async append/apply response | no Storage read | unchanged |

There are no other methods in the v3.7.0 Storage interface.

## Tests

### Native C

`c/tests/fatal_error_test.c` now verifies:

- snapshot-temporary constructor failures from FirstIndex, LastIndex, and
  InitialState clean up and return no RawNode;
- Bootstrap returns the sentinel without latching and a retry succeeds;
- Term failure latches identically through RawNode and Node-core entry points;
- the first terminal result is returned by later Campaign and Step calls;
- Entries failure during append backtracking is terminal and sticky.

`c/tests/log_test.c` verifies:

- Term propagates the snapshot-temporary code rather than converting it into
  a nonmatch;
- Entries propagates the code rather than converting it into compacted or
  unavailable;
- the core, rather than the callback bridge, performs terminal-on-escape
  classification.

`c/tests/snapshot_test.c` now explicitly verifies:

- snapshot-temporary fallback leaves `raft_raw_node_error == RAFT_OK`;
- no snapshot message is sent while unavailable;
- a later retry calls Snapshot again and sends the snapshot;
- the terminal result remains clear after retry.

The existing unexpected-Snapshot-error test continues to verify that other
Snapshot errors propagate.

### Backend-neutral Go/C parity

`fatal_storage_parity_test.go` adds:

- `TestSnapshotTemporaryBootstrapErrorParity`;
- `TestSnapshotTemporaryConstructorErrorsPanicParity`;
- `TestSnapshotTemporaryLogLookupErrorsPanicParity`;
- `TestSnapshotTemporaryEntriesErrorPanicsInReadyParity`;
- `TestSnapshotTemporarySnapshotErrorRetriesParity`;
- `TestUnexpectedSnapshotStorageErrorPanicsParity`;
- `TestNodeSnapshotTemporaryFromTermPanicsParity`.

These tests run unchanged against the default Go backend and the
`cgo_raft` backend. They cover:

- constructor panic classification;
- Term, FirstIndex, and LastIndex lookup failure;
- Entries failure during Ready;
- repeated failure;
- Bootstrap's returned-error exception;
- Snapshot retry;
- non-temporary Snapshot panic;
- RawNode and asynchronous Node observability.

### cgo-specific fatal-latch and bridge coverage

`fatal_error_cgo_test.go` adds:

- `TestCGoSnapshotTemporaryTermFailureStaysTerminal`;
- `TestCGoSnapshotTemporaryEntriesFailureStaysTerminal`.

Each test clears the originating Storage fault after the first panic and
verifies that a different stateful operation still panics from the latched
first error.

`storage_bridge_cgo_test.go` extends callback mapping coverage to verify that
the bridge retains the sentinel code for a non-Snapshot callback. This proves
that operation-specific classification happens at the consumer/latch without
losing original error identity.

## Final compatibility matrix

| Scenario | Go v3.7.0 | C after fix | Equivalent? |
| --- | --- | --- | --- |
| Snapshot -> temporary unavailable | return from send attempt; retry later | return from send attempt; retry later | Yes |
| Snapshot -> other storage error | panic | terminal latch and cgo panic | Yes |
| Term -> temporary unavailable | panic | terminal latch and cgo panic | Yes |
| Entries -> temporary unavailable | panic | terminal latch and cgo panic | Yes |
| FirstIndex -> temporary unavailable | panic | constructor panic or stateful terminal panic | Yes |
| LastIndex -> temporary unavailable | panic, except Bootstrap returns | constructor/stateful panic; Bootstrap returns | Yes |
| InitialState -> temporary unavailable | constructor panic | constructor panic | Yes |
| Bootstrap LastIndex -> temporary unavailable | return error before mutation | return error before mutation | Yes |

| Behavior | Expected Go semantics | C semantics after Phase 30 |
| --- | --- | --- |
| Snapshot temporary unavailable | retryable | consumed before terminal latch |
| Unrelated operation returning same sentinel | unexpected fatal/error | terminal-on-escape |
| Fatal latch | instance inoperable after unexpected Storage failure | first terminal code retained |
| Subsequent stateful operation | unsupported/inoperable after panic | returns same terminal code; cgo panics |
| RawNode propagation | panic for unexpected storage error | cgo panic |
| Node propagation | actor panic, not ignored Step error | `stepForNode` sees sticky result and panics |
| Ready propagation | panic for unexpected Entries error | Ready latches and cgo panics |
| Bootstrap pre-mutation failure | return error; retry permitted | return original error; retry permitted |

## Validation

All validation below passed on 2026-07-31.

### Focused regression tests

```text
c/.build/fatal_error_test
c/.build/log_test
c/.build/snapshot_test

go test -count=1 ./... \
  -run 'Test(SnapshotTemporary|UnexpectedSnapshot|NodeSnapshotTemporary)'

CGO_ENABLED=1 go test -count=1 -tags=cgo_raft ./... \
  -run 'Test(SnapshotTemporary|UnexpectedSnapshot|NodeSnapshotTemporary)'

CGO_ENABLED=1 go test -count=1 -tags=cgo_raft ./... \
  -run 'Test(CGoSnapshotTemporary|CGoStorageCallbackError|SnapshotTemporary|UnexpectedSnapshot|NodeSnapshotTemporary)'
```

### Full native and Go suites

```text
make test-c
go test -count=1 ./...
CGO_ENABLED=1 go test -count=1 -tags=cgo_raft ./...
```

All nine native C binaries passed. The cgo-tagged `rafttest` package contains
no tagged test files, as in prior phases.

### Concurrency and cgo checking

```text
go test -race -count=1 ./...
CGO_ENABLED=1 go test -race -count=1 -tags=cgo_raft ./...
CGO_ENABLED=1 GOEXPERIMENT=cgocheck2 \
  go test -count=1 -tags=cgo_raft ./...
```

### ASan, LSan, and UBSan

```text
make test-c-sanitize

ASAN_OPTIONS=detect_leaks=1:halt_on_error=1 \
  CGO_ENABLED=1 go test -asan -count=1 -tags=cgo_raft ./...

CGO_ENABLED=1 \
  CGO_CFLAGS='-O1 -g -fno-omit-frame-pointer -fsanitize=undefined' \
  CGO_LDFLAGS='-fsanitize=undefined' \
  UBSAN_OPTIONS='halt_on_error=1:print_stacktrace=1' \
  go test -count=1 -tags=cgo_raft ./...
```

### Valgrind

All nine clean native C binaries passed:

```text
valgrind --quiet --error-exitcode=99 \
  --leak-check=full --show-leak-kinds=all <test-binary>
```

No leak, invalid access, use-after-free, or double free was reported.

### TLA and compiler builds

```text
go test -count=1 -tags=with_tla ./...
CGO_ENABLED=1 go test -count=1 -tags='cgo_raft with_tla' ./...
make -C c clean test CC=clang
make -C c clean test CC=gcc
```

Both strict C11 compiler builds passed with `-Wall -Wextra -Werror
-Wpedantic`.

Formatting and whitespace checks also passed.

## Files changed

Production:

- `c/src/raft_core.c`;
- `rawnode_cgo.go`.

Tests:

- `c/tests/fatal_error_test.c`;
- `c/tests/log_test.c`;
- `c/tests/snapshot_test.c`;
- `fatal_storage_parity_test.go`;
- `fatal_error_cgo_test.go`;
- `storage_bridge_cgo_test.go`.

Documentation:

- `OUTPUT/PHASE30_C_STORAGE_ERROR_CLASSIFICATION_RESULT.md`.

No build file, public header, protobuf definition, Node implementation,
Progress code, configuration code, or async-storage implementation changed.

## Remaining differences and conclusion

The native C API returns terminal result codes instead of panicking; the cgo
wrapper converts them to Go panics. This is the existing intentional
language-boundary policy from Phase 19.

If a Go caller recovers an unexpected Storage panic, the Go implementation
does not promise continued operation. C explicitly preserves the first
terminal result and refuses later stateful operations. This remains the
documented Phase 19 safety policy.

No remaining `ErrSnapshotTemporarilyUnavailable` operation-classification
mismatch was found:

```text
Storage.Snapshot:
    retryable and not latched

all other Storage operations:
    unexpected and fatal when used by stateful Raft paths

Bootstrap LastIndex precheck:
    returned before mutation and retryable by the caller
```

Phase 30 is complete.
