# PHASE 25: C Node Lifetime and Deterministic Destruction Result

## Result

The final-audit finding was **confirmed**.

The shared Go `Node` actor already reproduced etcd-io/raft v3.7.0's observable
Stop and post-stop behavior. The mismatch was the lifetime of the cgo-owned
RawNode:

```text
Node.Stop
    -> close(done)
    -> return from run
    -> C RawNode remains reachable through node.rn
    -> destruction waits for a future Go GC/finalizer
```

A caller can legally retain the stopped `Node` indefinitely. Before this
phase, doing so also retained:

- the C `raft_raw_node_t`;
- its Raft core, log, tracker, message, snapshot, and protobuf allocations;
- any unaccepted pending C Ready preview;
- the storage callback bridge; and
- its `runtime/cgo.Handle`.

This was not an immediate use-after-free: the finalizer delayed destruction
rather than destroying too soon. It was nevertheless a real deterministic
resource-lifetime mismatch. `Node.Stop` is the explicit lifecycle boundary,
but it did not release the native resources that the Node exclusively owned.

The fix makes the Node run loop destroy its RawNode before closing `done`.
Consequently, `Stop` does not return until all Node-owned native state and the
storage callback handle have been released.

No public API, C ABI, Raft state transition, storage protocol, or error
behavior changed.

## Exact reference

The compatibility reference is:

```text
etcd-io/raft v3.7.0
commit b867cf13f6bc0dae21204302df97bc2355c3af55
```

The audit inspected the tag's actual `node.go`, including:

- `StartNode`;
- `RestartNode`;
- `newNode`;
- `node.run`;
- `Step` and proposal handling;
- `Tick`;
- `Ready`;
- `Advance`;
- `ApplyConfChange`;
- `Status`;
- reporting and leadership operations; and
- `Stop`.

The v3.7.0 `Node` interface does **not** expose `HasReady`,
`TickQuiesced`, or `Close`. Those are therefore not Node post-stop operations
to compare. `HasReady` and `TickQuiesced` are RawNode operations.

## Reference Node lifecycle

### Creation

`StartNode`:

1. creates a RawNode;
2. bootstraps the supplied peers before starting the actor;
3. creates the Node channels;
4. stores the RawNode in `node.rn`; and
5. starts one `node.run` goroutine.

`RestartNode` performs the same sequence without Bootstrap.

The Node creates:

- unbuffered proposal, receive, configuration, ConfState, Ready, Advance,
  stop, and status channels;
- a buffered tick channel of capacity 128;
- a `done` channel; and
- one actor goroutine.

It does not start a library-owned append worker or apply worker.

### Normal serialization

The run goroutine is the sole normal user of the RawNode. Public methods send
work over channels. The run loop serializes:

- proposal and incoming-message Step operations;
- configuration changes;
- Tick;
- Ready acceptance;
- Advance;
- Status; and
- Stop.

Non-proposal `Node.Step` returns when the unbuffered channel handoff completes,
not after later application effects. `Propose` waits for the proposal result.
This behavior is unchanged.

### Stop

The reference `Stop` sends on `stop` unless `done` is already closed, then
waits for `done`.

The run loop closes `done` in its stop select case and immediately returns.
Thus Stop:

- is synchronous with the actor's shutdown acknowledgement;
- prevents later actor work;
- is safe and idempotent when called repeatedly; and
- does not drain pending messages or Ready work.

The close occurs just before the final return instruction rather than acting
as a separate goroutine join. There is no actor work after the close.

### Pending work

At Stop:

- blocked channel senders select the closed `done` case;
- ordinary Step/Propose calls return `ErrStopped`;
- buffered ticks and unsent Ready state are abandoned with the actor;
- an already delivered Ready remains an application-owned Go value;
- Advance after Stop is ignored; and
- no further Ready send can occur after Stop returns.

The reference closes only `done`. It does not close `readyc` or the other
operation channels.

### Post-stop behavior

| Operation | etcd v3.7.0 behavior |
|---|---|
| ordinary `Step` | returns `ErrStopped` |
| unexpected network-delivered local message | existing pre-channel filter may return nil before checking stopped state |
| `Campaign`, `ForgetLeader`, `ReadIndex` | return `ErrStopped` through Step |
| `Propose` | returns `ErrStopped` |
| `Tick` | returns without changing Raft state; its select may enqueue a now-unused buffered tick |
| `Ready` | returns the same open channel, but no sender remains |
| `Advance` | returns without action |
| `ApplyConfChange` | returns nil |
| `Status` | returns an empty Status |
| reporting/transfer methods | return without action |
| repeated `Stop` | returns without action |

These semantics remain unchanged in both current backends.

## Current C-backed architecture

There is no C Node actor or C Node wrapper. The architecture is:

```text
Go Node
    |
    | owns and serializes
    v
Go cgo RawNode wrapper
    |
    | owns
    +--> C raft_raw_node_t
    |
    +--> Go storageBridge
    |
    +--> runtime/cgo.Handle
    |
    +--> optional pending C Ready preview
```

The Go Node and channel actor are shared between the default and cgo builds.
Only the RawNode implementation differs by build tag.

### C RawNode destruction

The existing package-private cgo `RawNode.destroy`:

1. discards `pendingReady` with `raft_ready_destroy`;
2. calls `raft_raw_node_destroy`;
3. clears the native pointer;
4. deletes the storage `cgo.Handle`;
5. clears the handle; and
6. removes the RawNode finalizer.

`raft_raw_node_destroy` frees the core and log and then the RawNode allocation.
It does not call storage callbacks.

The method is idempotent because it returns immediately after the native
pointer has been cleared.

### Previous ownership mismatch

Before this phase, the cgo Node did not call `destroy`. The run goroutine
returned, but `node.rn` continued to reference the Go RawNode wrapper. The
RawNode finalizer could not run while the stopped Node remained reachable.

Therefore:

```text
logical Node lifetime ended at Stop
native resource lifetime ended only after Node became unreachable and GC ran
```

The finalizer was being used for correctness of resource release, not merely
as a fallback for direct RawNode users.

## Lifecycle comparison

| Lifecycle event | Go v3.7.0 | cgo Node before Phase 25 | Equivalent before? | cgo Node after Phase 25 |
|---|---|---|---:|---|
| Node creation | RawNode, channels, one run goroutine | same plus C RawNode and callback handle | Yes | unchanged |
| RawNode ownership | private `node.rn` | private `node.rn` owns native wrapper | Yes logically | unchanged until Stop |
| Worker startup | no storage workers; one run goroutine | same | Yes | unchanged |
| Step before Stop | serialized through run | same | Yes | unchanged |
| Tick before Stop | buffered channel to run | same | Yes | unchanged |
| Ready before Stop | unbuffered Ready handoff and acceptance | same | Yes | unchanged |
| Advance before Stop | serialized through run | same | Yes | unchanged |
| Stop initiation | stop-channel send or already-done return | same | Yes | unchanged |
| Stop cleanup | Go actor has no explicit native resource | no native cleanup | **No** | C Ready, RawNode, and handle freed |
| Stop return | after `done` closes | after `done`, but C resources retained | **No** for ownership | after deterministic cleanup and `done` |
| Pending work at Stop | dropped/unblocked | same, but pending C Ready retained until GC | **No** | same public behavior; pending C Ready freed |
| Worker termination | run returns | run returns | Yes | unchanged |
| RawNode destruction | ordinary Go GC lifetime | finalizer only | **No** for native ownership | explicit before `done`; finalizer fallback removed |
| ordinary Step after Stop | `ErrStopped` | `ErrStopped` | Yes | unchanged |
| Tick after Stop | no state change | no state change | Yes | unchanged |
| Ready after Stop | open, permanently unsent | same | Yes | unchanged |
| Advance after Stop | no-op | no-op | Yes | unchanged |
| repeated Stop | no-op | no-op | Yes | unchanged |
| final resource cleanup | Go GC | nondeterministic GC/finalizer | **No** | deterministic native cleanup |

## Async storage worker analysis

### No Node-owned worker

`AsyncStorageWrites` changes Ready content. It does not create a library
goroutine.

Ready contains:

- `MsgStorageAppend` for an application append worker;
- `MsgStorageApply` for an application apply worker; and
- response protobuf messages to submit later through `Node.Step`.

The application owns worker creation, cancellation, and storage I/O.

### Ownership of work messages

The cgo Ready conversion deep-copies C messages, entries, snapshots, and
protobuf metadata into Go protobuf objects before the C Ready is released.
An application worker therefore holds no:

- `rn.p`;
- C Ready pointer;
- callback handle;
- C entry buffer; or
- Node channel pointer other than the public Node it invokes.

Storage requests and responses can safely outlive the C RawNode as ordinary Go
values.

### Response racing Stop

There are only two possible serializations.

#### Response first

```text
worker Node.Step(response)
    -> run receives response
    -> run completes C step and any synchronous callback
    -> run returns to select
    -> run receives Stop
    -> destroy RawNode
    -> close(done)
```

The Stop sender waits while the run goroutine finishes the C operation.

#### Stop first

```text
run receives Stop
    -> destroy RawNode
    -> delete callback handle
    -> close(done)
worker Node.Step(response)
    -> selects done
    -> ErrStopped
```

No path permits a response to call the destroyed RawNode.

### Callback ordering

C storage callbacks are synchronous within the cgo RawNode call that triggered
them. Stop is processed by the same run goroutine, so it cannot be selected
until the current operation and callback have returned.

The C destructor does not invoke storage callbacks. The `cgo.Handle` is
deleted only after the C RawNode is destroyed and can no longer initiate one.

Therefore no storage callback can access Node/RawNode state after Stop
returns.

## Chosen fix

The stop select case in `node.run` now performs:

```text
receive Stop
    |
    v
RawNode.destroy()
    |
    +--> destroy pending C Ready
    +--> destroy C RawNode
    +--> delete storage cgo.Handle
    +--> remove finalizer
    |
    v
node.rn = nil
    |
    v
close(done)
    |
    v
return from run
    |
    v
Stop returns
```

The default Go RawNode adds a package-private no-op `destroy` method. This lets
the shared Node actor express one ownership rule without build-specific
branches. It does not change default Go behavior because all of that RawNode's
state is already garbage-collected Go memory.

Setting `node.rn` to nil makes the ownership transfer explicit. No public
post-stop operation dereferences it; those methods use `done`, cached Node
identity/logger fields, or the Ready channel.

No finalizer was added or removed globally. Direct cgo RawNode users still
retain the Phase 20 finalizer fallback. A RawNode owned by Node is now
explicitly destroyed, and `destroy` removes its finalizer.

## Ownership and lifecycle table

| Resource | Created by | Owned by | Used by | Destruction trigger | Synchronization |
|---|---|---|---|---|---|
| Go Node | `newNode` from Start/Restart | caller and run goroutine references | public methods, run | Go GC after caller/run release it | channel actor |
| C Node wrapper | not present | not applicable | not applicable | not applicable | not applicable |
| Node run goroutine | Start/Restart | Node lifecycle | all RawNode operations | stop select returns | `stop`/`done` |
| Go RawNode wrapper | `NewRawNode` | Node until Stop | run goroutine | `node.rn = nil`; then Go GC | exclusive run-loop use |
| C RawNode | `raft_raw_node_new` | cgo RawNode wrapper | synchronous cgo calls | `RawNode.destroy` before `done` | run-loop serialization |
| storageBridge | cgo `NewRawNode` | cgo RawNode wrapper | exported synchronous callbacks | ordinary Go GC after handle/wrapper release | callback call boundary |
| storage `cgo.Handle` | cgo `NewRawNode` | cgo RawNode wrapper | C callback table | deleted after C RawNode destruction | run-loop serialization |
| library storage worker | not present | application responsibility | not applicable | application responsibility | application responsibility |
| storage request | Ready conversion | application worker | storage I/O | Go GC | independent Go protobuf |
| storage response | Ready conversion | application worker until Step | Node Step/run if still live | Go GC | Node channel or `done` |
| Node channels | `newNode` | Go Node | public methods and run | Go GC; only `done` is closed | channel semantics |
| pending C Ready | `readyWithoutAccept` | cgo RawNode | Node Ready preview | accept, replacement, or destroy | run-loop serialization |
| delivered Ready | Ready conversion/application | application | persistence, messaging, apply | Go GC | independent Go values |
| C buffers/metadata | C parent objects | C RawNode/Ready/message parents | synchronous C core/conversion | recursive parent destructor | run-loop plus ownership |

## Explicit final review

### Can any worker access RawNode after destruction?

No. There is no library worker. Application workers own independent Go
messages. Their `Node.Step` either hands the response to the still-running
actor before Stop is processed or observes `done` and returns `ErrStopped`.

### Can any callback access Node state after Stop returns?

No. Callbacks are synchronous within a run-loop C call. Cleanup happens on
that same run goroutine after all earlier calls return. The callback handle is
deleted after the C RawNode is destroyed.

### Can Stop race with storage response delivery?

Yes at the public channel boundary, as in Go, but the race is safe and has only
the two serializations described above.

### Can Stop be called twice?

Yes. The second call observes closed `done` and returns. It does not call the
destructor again.

### Can post-stop operations access destroyed state?

No.

- Step/Propose paths select `done`.
- Tick and Advance return without invoking RawNode.
- Status returns empty.
- ApplyConfChange returns nil.
- Ready returns the permanently unsent channel.
- reporting and transfer methods select `done`.

### Can RawNode be destroyed more than once?

The Node run loop has one stop return path. Repeated Stop does not reenter it.
The cgo `destroy` method itself is also idempotent and removes the finalizer.

### Are resources or goroutines leaked?

No Node-owned goroutine or native allocation remains after Stop. Application
workers remain the application's responsibility, as in the reference.

### Is a finalizer relied upon for correctness?

No for Node-owned RawNode instances. The finalizer remains only the documented
fallback for direct RawNode users, whose public API has no Close method.

## Tests added

### Backend-neutral parity tests

`node_lifetime_parity_test.go` runs against both default Go and cgo backends.

- `TestNodeLifetimeNormalLifecycleParity`
  - StartNode;
  - Ready persistence and Advance;
  - Tick;
  - public Step;
  - another Ready/Advance;
  - Stop.
- `TestNodeLifetimePostStopParity`
  - ordinary Step returns `ErrStopped`;
  - the existing local-message prefilter remains nil;
  - Tick and Advance return;
  - Ready is open but not readable;
  - Status is empty;
  - ApplyConfChange is nil;
  - repeated Stop returns.
- `TestNodeLifetimeStopUnblocksPendingWorkParity`
  - a leaderless blocked proposal is released with `ErrStopped`;
  - the bootstrapped Ready remains unconsumed while Stop occurs.
- `TestNodeLifetimeAsyncStorageResponseAfterStopParity`
  - append work is correctly persisted;
  - its response is held until after Stop;
  - the delayed response receives `ErrStopped`.
- `TestNodeLifetimeAsyncStorageResponseStopRaceParity`
  - append work is persisted;
  - response delivery and Stop begin at one synchronization barrier;
  - either successful pre-stop processing or `ErrStopped` is accepted;
  - both operations must terminate;
  - repeated Stop remains safe;
  - 100 races execute in each test invocation.

The async tests use channel barriers, not sleeps, for the Stop boundary.

### cgo ownership test

`TestCGoNodeStopDestroysRawNode` verifies that Stop:

- clears `node.rn`;
- clears the native RawNode pointer;
- destroys an unsent pending C Ready if present;
- clears and deletes the storage `cgo.Handle`;
- permits repeated Stop; and
- leaves explicit repeated `RawNode.destroy` harmless.

This uses existing package-private state from a cgo-only test. No production
instrumentation was added.

## Validation

All validation below passed on 2026-07-31.

Focused lifecycle and destruction tests:

```text
go test -count=1 -run '^TestNodeLifetime' ./...
CGO_ENABLED=1 go test -count=1 -tags=cgo_raft \
  -run '^(TestNodeLifetime|TestCGoNodeStopDestroysRawNode)' ./...
```

Repeated synchronized Stop/worker stress passed 100 complete test runs for
each backend. Each run itself performs 100 response/Stop races:

```text
go test -count=100 \
  -run '^TestNodeLifetime(StopUnblocksPendingWork|AsyncStorageResponseAfterStop|AsyncStorageResponseStopRace)Parity$' .

CGO_ENABLED=1 go test -count=100 -tags=cgo_raft \
  -run '^(TestNodeLifetime(StopUnblocksPendingWork|AsyncStorageResponseAfterStop|AsyncStorageResponseStopRace)Parity|TestCGoNodeStopDestroysRawNode)$' .
```

Native C and complete Go suites passed:

```text
make -C c test
go test -count=1 ./...
CGO_ENABLED=1 go test -count=1 -tags=cgo_raft ./...
```

Race detector passed for both backends:

```text
go test -count=1 -race ./...
CGO_ENABLED=1 go test -count=1 -race -tags=cgo_raft ./...
```

Strict cgo pointer checking passed:

```text
GOEXPERIMENT=cgocheck2 CGO_ENABLED=1 \
  go test -count=1 -tags=cgo_raft ./...
```

Native ASan, LeakSanitizer, and UBSan passed:

```text
make test-c-sanitize
```

cgo ASan/LeakSanitizer and UBSan passed:

```text
CGO_ENABLED=1 ASAN_OPTIONS=detect_leaks=1:halt_on_error=1 \
  go test -count=1 -asan -tags=cgo_raft ./...

CGO_ENABLED=1 \
  CGO_CFLAGS='-O1 -g -fno-omit-frame-pointer -fsanitize=undefined' \
  CGO_LDFLAGS='-fsanitize=undefined' \
  UBSAN_OPTIONS=halt_on_error=1:print_stacktrace=1 \
  go test -count=1 -tags=cgo_raft ./...
```

Valgrind full leak/error checking passed for all nine native binaries:

- `raw_node_skeleton_test`;
- `raft_core_test`;
- `snapshot_test`;
- `read_only_test`;
- `tracker_test`;
- `confchange_test`;
- `unstable_test`;
- `log_test`; and
- `fatal_error_test`.

Both TLA-tagged variants passed:

```text
go test -count=1 -tags=with_tla ./...
CGO_ENABLED=1 go test -count=1 -tags='cgo_raft with_tla' ./...
```

The clean strict Clang C11 build and native tests passed:

```text
make -C c clean test CC=clang
```

A clean GCC rebuild and native test run subsequently passed:

```text
make -C c clean test CC=gcc
```

Formatting and whitespace checks passed:

```text
make verify-gofmt
git diff --check
```

No test failure, race, cgo pointer violation, deadlock, goroutine leak,
use-after-free, double free, sanitizer finding, Valgrind error, leak, or
compiler warning was reported.

One initial test-harness run intentionally failed during development because
the test acknowledged a `MsgStorageAppend` without first persisting it. That
violated the documented async-storage protocol and caused the reference Go
backend to detect inconsistent stable storage. The test worker was corrected
to persist the append before releasing its response; this was not a product
failure and is not part of the final passing validation.

## Modified files

- `node.go`
  - destroys and releases the Node-owned RawNode before closing `done`.
- `rawnode.go`
  - adds the default-Go no-op implementation of the shared ownership hook.
- `node_lifetime_parity_test.go`
  - adds backend-neutral lifecycle, Stop, post-stop, pending-work, and
    async-storage race coverage.
- `node_lifetime_cgo_test.go`
  - verifies deterministic native destruction and handle deletion.
- `OUTPUT/PHASE25_C_NODE_LIFETIME_RESULT.md`
  - records this analysis, implementation, and validation.

No C source, public header, ABI, protobuf conversion, Raft algorithm, storage
protocol, or other final-audit finding changed.

## Remaining intentional differences and limitations

1. Direct cgo RawNode still has no public Close and therefore retains its Phase
   20 finalizer fallback. This phase changes only Node-owned RawNodes.
2. Node does not own application append/apply workers and therefore does not
   cancel or join them. Their Go work messages remain safe after Stop, and
   response Step calls return `ErrStopped`.
3. Ready remains open but permanently unsent after Stop. This is the exact
   reference behavior; applications must stop their Ready consumer through
   their own lifecycle signal.
4. Node `HasReady`, Node `TickQuiesced`, and Node `Close` do not exist in the
   v3.7.0 interface.

## Final conclusion

The Node lifetime finding was confirmed. Before Phase 25, Stop ended the actor
but left the Node-owned C RawNode and callback handle alive until the entire
stopped Node became unreachable and a finalizer happened to run.

After Phase 25, ownership is:

```text
Node owns RawNode during operation
    -> run loop serializes all access
    -> Stop is selected
    -> RawNode and pending Ready are destroyed exactly once
    -> callback handle is deleted
    -> ownership reference is cleared
    -> done closes
    -> Stop returns
```

No worker or callback can access destroyed state, repeated Stop and every
post-stop operation preserve reference behavior, and the finalizer is no
longer required for correctness of Node resource release.

No Node lifetime or deterministic-destruction compatibility mismatch remains.
