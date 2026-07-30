# Phase 20: cgo RawNode Finalizer Liveness

## Scope

Phase 20 fixes only the lifetime relationship between the Go cgo `RawNode`
wrapper and the C `raft_raw_node_t` that it owns. It does not change Raft
semantics, C ownership, the public API, Node shutdown, protobuf conversion, or
any other final-audit finding.

## Original lifetime bug

The cgo build represents `RawNode` as a Go owner around a separately allocated
C object:

```text
Go *RawNode
    |
    | p
    v
C raft_raw_node_t
```

`newRawNode` installs `(*RawNode).finalize` with
`runtime.SetFinalizer`. The finalizer calls `destroy`, which:

1. frees any pending C Ready preview,
2. calls `raft_raw_node_destroy(rn.p)`,
3. clears `rn.p`,
4. deletes the storage `cgo.Handle`, and
5. removes the finalizer.

`raft_raw_node_destroy` recursively frees the C core and log and finally frees
the `raft_raw_node_t` allocation itself.

Before this phase, several methods made their last effective use of the Go
owner while loading `rn.p` for a cgo call. A Go receiver is not guaranteed to
remain live merely because its native field is still in use. The runtime is
allowed to find the wrapper unreachable at that last use and run its finalizer
while C is still operating on the extracted pointer.

The cgo pointer rules do not supply an additional guarantee here:

- `rn.p` points to C-allocated memory, not to the Go wrapper;
- C receives no Go pointer to `rn`;
- the storage callback table retains only the integer value of a `cgo.Handle`;
- storage callbacks are synchronous C-to-Go callbacks; and
- no C operation retains `rn.p` after returning.

Consequently, a `runtime.KeepAlive(rn)` boundary is required after the native
operation has finished using `rn.p`.

## Complete call-path audit

Every `C.raft_raw_node_*` call in the current Go binding was inspected. In the
table, “shared barrier” means that the C result is passed to
`cOperationError`, whose first operation is now `runtime.KeepAlive(rn)`.

| Call / method | Uses `rn.p` | KeepAlive needed | Current status | Reason |
|---|---:|---:|---|---|
| `newRawNode` / `raft_raw_node_new` | No existing C node yet | No | SAFE, special case | The Go owner and its finalizer do not exist until after construction returns. The local `cgo.Handle` owns the callback bridge during construction. |
| `destroy` / `raft_raw_node_destroy` | Yes | No additional barrier | SAFE, special case | This is the intentional owner-side free. The finalizer receives `rn` as its argument; explicit destruction also uses and clears fields after the C call. |
| `returnOrPanic` / `raft_raw_node_error` | Yes | Yes | FIXED, direct barrier | On a non-terminal error the query could previously be the last use of `rn`. The result is now saved and followed immediately by `runtime.KeepAlive(rn)`. |
| `HasProgress` | Yes | Yes | FIXED, direct barrier | The former direct return had no post-call liveness boundary. |
| `Tick` | Yes | Yes | FIXED, shared barrier | `raft_raw_node_tick_result` returns through `cOperationError`. |
| `TickQuiesced` | Yes | Yes | FIXED, direct barriers | Separate barriers follow its terminal-error query and final void tick so both normal and immediate-panic paths are protected. |
| `Campaign` | Yes | Yes | FIXED, shared barrier | The native result returns through `cOperationError`. |
| `Propose` | Yes | Yes | FIXED, shared barrier | The native result returns through `cOperationError`; the independent input-slice barrier remains unchanged. |
| `ProposeConfChange`, nil/V1/V2 paths | Yes | Yes | FIXED, shared barrier | Every native result returns through `cOperationError`; existing input ownership handling remains unchanged. |
| `ApplyConfChange` | Yes | Yes | FIXED, shared barrier | The native result returns through `cOperationError`. |
| `Step` | Yes | Yes | FIXED, shared barrier | `raft_raw_node_step` returns through `cOperationError`. |
| `stepForNode` | Yes | Yes | FIXED, shared barrier | `raft_raw_node_step_for_node` returns through the same `cOperationError` path. |
| `Ready` | Yes | Yes | FIXED, shared barrier | `raft_raw_node_ready` returns through `cOperationError` before output translation or cleanup. |
| `readyWithoutAccept` | Yes | Yes | FIXED, shared barrier | `raft_raw_node_ready_without_accept` returns through `cOperationError`. |
| `acceptReady` | Yes | Yes | FIXED, shared barrier | `raft_raw_node_accept_ready` returns through `cOperationError`; Ready destruction is independent of `rn.p`. |
| `HasReady` | Yes | Yes | FIXED, direct barrier | The former direct return had no post-call liveness boundary. |
| `Advance` | Yes | Yes | FIXED, shared barrier | The native result returns through `cOperationError`. |
| `BasicStatus` | Yes | Yes | FIXED, shared barrier | The native result returns through `cOperationError` before translating the copied result. |
| `Status` | Yes | Yes | FIXED, shared barrier | The native result returns through `cOperationError`; the returned C status is an independent owned snapshot. |
| `WithProgress` | Yes | Yes | FIXED, shared barrier | The snapshot call returns through `cOperationError`; the returned array is independently owned. |
| `ReportUnreachable` | Yes | Yes | FIXED, shared barrier | The native result returns through `cOperationError`. |
| `ReportSnapshot` | Yes | Yes | FIXED, shared barrier | The native result returns through `cOperationError`. |
| `TransferLeader` | Yes | Yes | FIXED, shared barrier | The native result returns through `cOperationError`. |
| `ForgetLeader` | Yes | Yes | FIXED, shared barrier | The native result returns through `cOperationError`. |
| `ReadIndex` | Yes | Yes | FIXED, shared barrier | The native result returns through `cOperationError`; the independent request-buffer barrier remains unchanged. |
| `Bootstrap` | Yes | Yes | FIXED, shared barrier | The native result returns through `cOperationError`; the independent peer-input barrier remains unchanged. |

`ID`, `AsyncStorageWritesEnabled`, and `Logger` do not use `rn.p`.
`ensureOpen` and `beginCOperation` inspect Go-owned fields only.
`discardPendingReady` frees an independent Ready preview, not the RawNode
pointer. Output cleanup calls for Ready, Status, ConfState, and Progress
snapshots similarly operate on independent C allocations after the RawNode
operation has returned.

## Chosen fix

The implementation uses the existing result-decoding helper as the common
lifetime boundary:

```go
func (rn *RawNode) cOperationError(code C.int) error {
	runtime.KeepAlive(rn)
	// Existing error decoding follows.
}
```

Go evaluates the native operation before invoking `cOperationError`. The
explicit runtime intrinsic therefore makes the wrapper reachable through the
completion of every result-returning native call, including synchronous
storage callbacks and panic/error paths.

The native calls that do not use `cOperationError` received local barriers:

- `returnOrPanic` saves the terminal-error query result, calls
  `runtime.KeepAlive(rn)`, and then applies the original decision;
- `HasProgress` and `HasReady` save their boolean results, keep `rn` alive,
  and return the saved values; and
- `TickQuiesced` keeps `rn` alive after both its terminal-error query and its
  final void native call, protecting the immediate-panic path as well as the
  normal path.

This is smaller than wrapping every C signature in a closure or introducing a
new call abstraction. It also provides an explicit, auditable boundary
without changing call ordering, result decoding, panic behavior, storage
callback handling, or C ownership.

The `returnOrPanic` rewrite preserves its prior short-circuit semantics:
callback panics and allocation failures still panic without making the C
terminal-error query, while other errors still make exactly one query.

## Node, callbacks, and asynchronous work

The Node implementation reaches the C RawNode through `stepForNode`,
`readyWithoutAccept`, `acceptReady`, and the same public RawNode operations.
Those result-returning calls all use the shared barrier. The Node run loop also
normally owns a Go reference to the RawNode, but correctness no longer depends
on that incidental reference.

Storage callbacks execute synchronously before their originating C call
returns. A GC initiated during a callback still observes the future
`runtime.KeepAlive(rn)` boundary, so the finalizer cannot destroy the native
node while the callback returns into C.

Async-storage Ready messages contain independently owned translated data.
Neither the C storage worker protocol nor Go callbacks retain `rn.p`, so there
is no additional post-call lifetime to extend.

## Tests

No new timing-based test was added. Deterministically reproducing the old
window would require a production or test-only blocking hook inside a native
RawNode call, plus explicit finalizer scheduling observation. Go intentionally
does not guarantee when a finalizer runs, so a test based only on concurrent
`runtime.GC`, sleeps, or repeated calls would be probabilistic and flaky.

Existing tests already exercise all public methods named in the finding and
the common result path. Focused cgo coverage included:

- RawNode construction, `HasProgress`, `HasReady`, ticks, proposals, Ready,
  status, membership, snapshots, and destruction;
- storage-handle creation and deterministic deletion by `destroy`;
- Ready and input allocation failure paths; and
- synchronous storage callback and fatal-error paths.

The safety property is additionally checked structurally by the complete
call-site audit above and dynamically by race, cgo pointer checking,
sanitizers, and Valgrind.

## Validation

All commands below passed on 2026-07-31.

Native C, focused cgo, and both complete Go backends:

```text
make clean-c && make test-c
go test -count=1 ./...
CGO_ENABLED=1 go test -count=1 -tags=cgo_raft ./... \
  -run 'TestCGo(RawNode|StorageHandle|Ready|InputArena|AsyncReady)'
CGO_ENABLED=1 go test -count=1 -tags=cgo_raft ./...
```

Race detector and strict cgo pointer checking:

```text
CGO_ENABLED=1 go test -count=1 -race -tags=cgo_raft ./...
CGO_ENABLED=1 GOEXPERIMENT=cgocheck2 \
  go test -count=1 -tags=cgo_raft ./...
```

Native ASan, LeakSanitizer, and UBSan:

```text
make test-c-sanitize
```

cgo ASan/LeakSanitizer and UBSan:

```text
CGO_ENABLED=1 ASAN_OPTIONS=detect_leaks=1:halt_on_error=1 \
  go test -count=1 -asan -tags=cgo_raft ./...

CGO_ENABLED=1 \
  CGO_CFLAGS='-O1 -g -fno-omit-frame-pointer -fsanitize=undefined' \
  CGO_LDFLAGS='-fsanitize=undefined' \
  UBSAN_OPTIONS=halt_on_error=1:print_stacktrace=1 \
  go test -count=1 -tags=cgo_raft ./...
```

Valgrind full leak and error checking passed for all nine native binaries:

- `raw_node_skeleton_test`
- `raft_core_test`
- `snapshot_test`
- `read_only_test`
- `tracker_test`
- `confchange_test`
- `unstable_test`
- `log_test`
- `fatal_error_test`

Both TLA-tagged build variants passed:

```text
go test -count=1 -tags=with_tla ./...
CGO_ENABLED=1 go test -count=1 -tags='cgo_raft with_tla' ./...
```

The strict Clang C11 build and tests passed:

```text
make -C c clean test CC=clang
```

The ordinary GCC C build was restored and rerun after alternate builds.
`gofmt`, `make verify-gofmt`, and `git diff --check` passed. No test failure,
race, cgo pointer violation, use-after-free, double free, leak, sanitizer
finding, Valgrind error, or compiler warning was reported.

## Modified files

- `rawnode_cgo.go`
  - adds the shared result-path lifetime barrier;
  - adds direct barriers for the terminal-error query, `HasProgress`,
    `HasReady`, and `TickQuiesced`; and
  - makes no public API, ABI, C, or Raft behavior change.
- `OUTPUT/PHASE20_C_CGO_RAWNODE_FINALIZER_LIVENESS_RESULT.md`
  - records the analysis, complete call-path inventory, fix, and validation.

No production C file and no test file changed.

## Remaining lifetime-related limitations

- Direct `RawNode` still has no public `Close`, so finalization remains a
  nondeterministic fallback for eventual resource release. A finalizer is not
  guaranteed to run before process exit. This pre-existing ownership choice is
  unchanged.
- Deterministic Node destruction is a separate final-audit finding and was
  intentionally not addressed in Phase 20.
- `RawNode` remains explicitly thread-unsafe. Concurrent use of the
  package-private `destroy` test hook with another method is unsupported.
- There is no deterministic regression test for finalizer timing. The explicit
  runtime barriers are the language-supported correctness mechanism.

No remaining cgo RawNode call path can be prematurely finalized while its
native operation is using `rn.p`.
