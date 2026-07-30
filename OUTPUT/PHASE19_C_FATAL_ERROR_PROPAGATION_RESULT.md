# Phase 19 C Fatal Storage and Allocation Error Propagation Result

## Outcome

The C-backed RawNode now treats fatal storage, Go-callback, allocation, and
internal invariant failures as terminal, and the Go wrapper surfaces them as
panics. This matches the original etcd behavior on every normal execution
path: these failures can no longer be mistaken for a vote rejection, an
inactive quorum, a recoverable Step error, or ordinary absence of Ready work.

The first terminal result is latched in the C Raft core. Subsequent stateful
operations return the same result, preventing partially updated state from
being used if a Go caller recovers a panic.

The original Bootstrap exception is preserved. `RawNode.Bootstrap` returns a
`Storage.LastIndex` error that occurs before Bootstrap mutates Raft state; it
does not latch that error. A later Bootstrap attempt may succeed.

This phase fixes only the fatal storage/allocation audit finding. It does not
change successful consensus behavior, storage ordering, flow control,
replication policy, or the async-storage protocol.

## Verification of the audit finding

The repository's untagged Go files remain the authoritative implementation:

- `rawnode.go` and `bootstrap.go`;
- `raft.go`;
- `log.go`;
- `storage.go`;
- `tracker/`;
- `node.go`.

No prior semantic phase had completed this work. The C core had a narrow
`raft.error` field used mainly by the void Tick boundary, and the storage
bridge already translated Go callback failures into C result codes. However,
there was no complete propagation policy:

- several log queries erased storage errors;
- several core entry points returned fatal results without latching them;
- error-returning cgo RawNode methods returned failures that the Go
  implementation would panic on;
- Node's asynchronous receive path intentionally ignores returned Step
  errors, so a returned fatal result could disappear;
- Tick could only expose a failure on a later operation;
- quorum-active reporting allocated a tracker clone and converted OOM into
  `false`, which could incorrectly step down a leader;
- there was no deterministic way to exercise C allocation failures;
- `raft_core_init` leaked initialized core subobjects when
  `Storage.InitialState` failed.

The audit finding was therefore still valid.

## Original Go semantics

### Storage

The original implementation classifies storage results by call site:

| Storage operation | Expected non-success result | Original behavior for other failures |
| --- | --- | --- |
| `FirstIndex`, `LastIndex` during log construction | none | panic |
| `InitialState` during Raft construction | none | panic |
| `Term` | `ErrCompacted`, `ErrUnavailable` can mean an unknown/nonmatching term | panic |
| `Entries` | `ErrCompacted` can trigger retry or snapshot fallback | `ErrUnavailable` and other errors panic |
| `Snapshot` | `ErrSnapshotTemporarilyUnavailable` is retryable | panic |
| Bootstrap's initial `LastIndex` | any returned error | return the error before mutation |

Consequently, `matchTerm` returning false is safe only for compacted or
unavailable terms. An arbitrary storage failure must not become a false match,
a rejected vote, or a less efficient conflict search.

Malformed successful callback output is also fatal. A nil ConfState, nil
entry, malformed entry vector, or nil snapshot cannot be treated as ordinary
public input validation.

### Allocation

Go allocation failure is runtime-fatal. There is no normal error return and no
supported recovery path. The C backend cannot force the Go runtime to abort,
so the closest compatible boundary is:

1. return a precise C OOM result;
2. clean all temporary ownership;
3. latch it for stateful C operations;
4. panic at the Go API boundary.

### RawNode and Node

Fatal failures in the Go Raft core panic through both RawNode and Node. Node
does not need a separate fatal-error channel: a panic unwinds the actor
goroutine instead of becoming a returned Step error.

### Async storage writes

The Raft library emits `MsgStorageAppend` and `MsgStorageApply`; the
application performs the writes and sends their success responses. These
messages do not carry an error field in either backend. If an application
write fails, the application must not send a success response.

The compatibility scope inside this repository is therefore:

- constructing async storage messages and response graphs;
- processing append/apply responses;
- propagating storage reads and allocation failures encountered on those
  paths.

It is not possible or appropriate to add a new write-error protocol in this
phase.

## Failure-source inventory

### C allocation sites

All allocations in the following subsystems now use one private allocator
abstraction:

- unstable entry and snapshot ownership;
- raft-log slices, scans, and append copies;
- tracker configurations, progress maps, inflight rings, and Status copies;
- configuration-change cloning and protobuf encoding;
- read-index queues and acknowledgements;
- Raft messages, entries, pending reads, and broadcast work;
- RawNode Ready objects, synchronous delayed responses, async storage message
  graphs, and status snapshots;
- cgo input arenas and storage-callback output graphs.

Before this phase, these sites returned OOM inconsistently. Some callers
propagated it, some converted it to a boolean, and some could mutate state
before the failure reached Go.

### Storage sites

The complete C storage call set was audited:

- `initial_state`;
- `first_index`;
- `last_index`;
- `term`;
- `entries`;
- `snapshot`.

The Go callback bridge records the original Go error for diagnostics and
`errors.Is` compatibility. Each cgo operation clears stale callback detail
before entering C and consumes detail after the call, so a current storage
failure is attached to the translated result.

### Internal fatal results

Overflow checks, invalid internal transitions, corrupted Ready sequencing,
configuration-application failures, and malformed storage-owned data already
produce `RAFT_ERR_FATAL` at their detection sites. The new terminal latch
ensures these results cannot be ignored after partial state changes.

## Implementation

### Allocation abstraction and deterministic injection

New private files `c/src/alloc.h` and `c/src/alloc.c` provide:

- `raft_malloc`;
- `raft_calloc`;
- `raft_realloc`;
- one-shot test fault injection.

Production behavior delegates directly to libc. Fault injection fails the
allocation after a deterministic number of successful allocation attempts,
then disables itself so cleanup allocations and destructors can run.

Every allocating C subsystem opts into the wrappers with a source-local
macro. The cgo amalgamation includes `alloc.c` once, and direct cgo
allocations also use the wrappers. No allocation ownership rule changed:
memory returned by the wrappers is still released with `free`.

### Log error classification

`raft_log_match_term` and `raft_log_is_up_to_date` now return a result plus a
boolean output.

`raft_log_match_term` converts only compacted and unavailable terms into
`matches=false`. Unexpected storage, callback, OOM, and fatal results
propagate. The same rule is applied to conflict scanning,
`findConflictByTerm`, append matching, commit matching, vote freshness, and
snapshot restoration.

`raft_log_slice` preserves `RAFT_ERR_STORAGE_COMPACTED` for snapshot/retry
behavior, but converts an `Entries` unavailable result to fatal, matching the
Go panic. Malformed storage-owned entry arrays are also fatal.

`RAFT_ERR_SNAPSHOT_TEMPORARILY_UNAVAILABLE` remains retryable and is not
terminal.

### Single core terminal latch

`raft_result_is_terminal` defines the terminal C result set:

- storage compacted or unavailable when they escape an expected handling
  site;
- Go callback panic;
- allocation failure;
- fatal internal error.

`raft_core_latch_error` records the first terminal result. Core Step,
Campaign, Tick, configuration proposal encoding, and configuration
application all pass through this helper.

Bootstrap uses a private `mutation_started` boundary. Its initial storage
query and input validation return directly. Once follower transition begins,
terminal failures are latched.

`raft_core_init` now releases the initialized tracker and read-only state when
`InitialState` fails.

### Allocation-free quorum activity

`raft_tracker_quorum_active` no longer clones the full tracker to count recent
activity. It counts active incoming and outgoing voter majorities directly.

This preserves the same joint-consensus quorum result while removing the only
path where OOM could be translated into `quorum inactive` and cause an
incorrect leader stepdown.

### RawNode, Ready, and Tick

Ready construction and Ready acceptance now latch terminal failures after
destroying partial output graphs. Advance checks the sticky result before
processing delayed responses and latches any terminal response failure.

The existing void `raft_raw_node_tick` remains for C source compatibility. A
new `raft_raw_node_tick_result` entry point lets cgo surface the same failure
on the Tick call that caused it. `raft_raw_node_error` exposes the sticky
terminal result to the binding. TickQuiesced checks that result at the Go
boundary and does not advance the C timer after a terminal failure.

`HasReady` continues to return true for a terminal node. Node therefore enters
the normal Ready path and immediately receives the terminal result rather than
silently idling.

### cgo panic boundary

The Go RawNode API is unchanged.

Before every potentially storage-calling C operation, the wrapper clears old
callback detail. It translates the result afterward and restores the current
Go storage error when available.

Methods that return ordinary Raft control errors still return them:

- proposal dropped;
- local message;
- unknown peer;
- stopped;
- Bootstrap's pre-mutation storage error.

If C reports a sticky terminal result, or cgo input/output allocation itself
fails, the wrapper calls the configured logger's `Panicf`. This applies to
Campaign, Propose, ProposeConfChange, Step, ForgetLeader, Tick, Ready,
Advance, configuration application, status/reporting calls, and constructor
failures as appropriate.

Node requires no duplicate propagation path. Its internal `stepForNode`
wrapper panics before `node.run` can discard the returned error.

### ABI and API impact

- The public Go RawNode and Node APIs are unchanged.
- The existing C RawNode object layout and ABI version are unchanged.
- The C API adds `raft_raw_node_tick_result` and `raft_raw_node_error`.
- The original void `raft_raw_node_tick` remains.
- Private C log helper signatures changed from boolean-only to result plus
  boolean output.
- Native C callers receive terminal result codes and must stop using the
  object; the Go binding converts those results to panics.

## Tests

### Native C

`c/tests/fatal_error_test.c` covers:

- constructor cleanup after `FirstIndex`, `InitialState`, and allocator
  failures;
- Bootstrap storage failure before mutation and a successful retry;
- identical fatal storage handling through public RawNode Step and the Node
  core entry point;
- immediate Tick error reporting and the compatibility void Tick;
- sticky repeated failure behavior;
- Ready OOM cleanup and latching;
- deterministic nested allocation failures while building async storage
  Ready graphs;
- destruction after every injected partial allocation.

`c/tests/log_test.c` verifies:

- compacted and unavailable terms become nonmatches;
- unexpected term failure propagates;
- vote freshness propagates unexpected term failure;
- `Entries` unavailable is fatal;
- `Entries` compacted remains recoverable.

The skeleton test now treats fatal configuration application as terminal and
verifies the sticky result.

### Shared Go/C parity

`fatal_storage_parity_test.go` runs unchanged against both backends. It
verifies:

- Bootstrap returns the original LastIndex error and a retry can succeed;
- unexpected `Storage.Term` failure panics through public RawNode Step;
- the same failure panics the Node actor instead of disappearing on its
  asynchronous receive path.

### C-backed Go boundary

`fatal_error_cgo_test.go` verifies:

- constructor OOM panics and deletes the cgo handle;
- Ready OOM panics and remains sticky;
- cgo input-arena OOM panics and cleans temporary ownership;
- nested async Ready allocation failures panic;
- a Go storage callback panic is terminal through the Node entry path;
- Bootstrap preserves arbitrary Go storage error identity through
  `errors.Is`.

Existing snapshot tests now verify that:

- temporary snapshot unavailability still retries;
- unexpected snapshot unavailability panics and becomes sticky.

Storage bridge tests verify that callback panics, arbitrary errors, malformed
successful outputs, and callback-output allocation failures receive distinct
fatal classifications and clean partial C graphs.

## Files changed

Allocation and build integration:

- `c/src/alloc.h` (new);
- `c/src/alloc.c` (new);
- `c/Makefile`;
- `rawnode_cgo_bridge.c`;
- `c/src/unstable.c`;
- `c/src/confchange.c`;
- `c/src/read_only.c`.

Log, tracker, core, and RawNode:

- `c/src/log.h`;
- `c/src/log.c`;
- `c/src/tracker.c`;
- `c/src/raft_core.h`;
- `c/src/raft_core.c`;
- `c/src/raw_node.c`;
- `c/include/raft/raft.h`.

cgo:

- `convert_cgo.go`;
- `errors_cgo.go`;
- `storage_bridge_cgo.go`;
- `rawnode_cgo.go`.

Tests:

- `c/tests/fatal_error_test.c` (new);
- `c/tests/log_test.c`;
- `c/tests/raw_node_skeleton_test.c`;
- `fatal_error_cgo_test.go` (new);
- `fatal_storage_parity_test.go` (new);
- `rawnode_cgo_test.go`;
- `storage_bridge_cgo_test.go`.

Documentation:

- `OUTPUT/PHASE19_C_FATAL_ERROR_PROPAGATION_RESULT.md`.

## Required fixes versus non-goals

Required and implemented:

- preserve expected compacted/unavailable/temporary-snapshot control flow;
- propagate every unexpected storage result;
- panic at the Go boundary;
- prevent Node from discarding fatal Step results;
- make stateful allocation failures terminal;
- clean partial ownership;
- preserve Bootstrap's pre-mutation return behavior;
- remove allocation from quorum-active calculation.

Optional cleanup deliberately not performed:

- redesigning all C APIs around a richer error object;
- transactional rollback after terminal partial mutation;
- changing the public Ready or Node APIs;
- changing the async storage message protocol;
- refactoring unrelated validation or consensus code.

## Validation

All commands below passed on 2026-07-31.

Native C and both Go backends:

```text
make test-c
go test -count=1 ./...
CGO_ENABLED=1 go test -count=1 -tags=cgo_raft ./...
```

Focused backend-neutral parity:

```text
go test -count=1 ./... \
  -run 'Test(BootstrapStorageErrorParity|UnexpectedTermStorageErrorPanicsParity|NodeUnexpectedTermStorageErrorPanicsParity)$'
CGO_ENABLED=1 go test -count=1 -tags=cgo_raft ./... \
  -run 'Test(BootstrapStorageErrorParity|UnexpectedTermStorageErrorPanicsParity|NodeUnexpectedTermStorageErrorPanicsParity)$'
```

Strict cgo pointer checking and race detection:

```text
CGO_ENABLED=1 GOEXPERIMENT=cgocheck2 \
  go test -count=1 -tags=cgo_raft ./...
CGO_ENABLED=1 go test -count=1 -race -tags=cgo_raft ./...
```

ASan, LSan, and UBSan:

```text
make test-c-sanitize
```

Strict Clang C11 build:

```text
make -C c clean test CC=clang
```

Valgrind full leak checking passed for all nine native C test binaries,
including the allocation-fault suite.

Both TLA build variants passed:

```text
go test -count=1 -tags=with_tla ./...
CGO_ENABLED=1 go test -count=1 -tags='cgo_raft with_tla' ./...
```

The ordinary GCC native build was restored and rerun after alternate builds.
No test failure, compiler warning, sanitizer finding, Valgrind error, race, or
cgo pointer violation was reported.

One intermediate `make test-c` invocation was attempted without cleaning
after a sanitizer build. It failed at link time because the Makefile does not
track CFLAGS changes and reused ASan/UBSan-instrumented objects without their
runtimes. `make clean-c && make test-c` passed, and all later configuration
changes used clean builds. This was a build-artifact issue, not a test or
source failure.

## Remaining concerns and limitations

- A Go runtime OOM normally terminates the process. C OOM is represented as a
  panic so the binding can preserve the Go API. Stateful C failures remain
  sticky if a caller recovers that panic. An allocation failure in a
  pre-call cgo descriptor arena also panics, but does not latch the untouched C
  state.
- Async storage write execution belongs to the application in both
  implementations. There is no write-error response field to propagate; an
  application must withhold success responses after a failed write.
- The native C API returns terminal result codes instead of panicking. This is
  intentional at the language boundary; the cgo wrapper provides the original
  Go panic behavior.
- The comprehensive upstream Raft interaction suite remains excluded under
  the repository's existing `cgo_raft` build constraints. New native,
  cgo-specific, and backend-neutral tests cover the failure paths introduced
  by this phase.
- No other semantic-audit finding was addressed.
