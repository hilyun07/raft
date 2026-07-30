# Phase 12 C Asynchronous Storage Writes Result

## Outcome

Phase 12 implements the current etcd Raft asynchronous storage-write
protocol behind the opt-in `cgo_raft` RawNode. The default build remains the
original pure-Go implementation.

The C backend now supports:

- `Config.AsyncStorageWrites`;
- `MsgStorageAppend` work addressed to `LocalAppendThread`;
- `MsgStorageApply` work addressed to `LocalApplyThread`;
- nested response messages on both work types;
- `MsgStorageAppendResp` and `MsgStorageApplyResp`;
- independent append and apply completion;
- multiple outstanding append and apply requests;
- stable-only committed-entry pagination while apply work is outstanding;
- same-target FIFO emission and pipelining without `Advance`;
- snapshot persistence and completion through the append worker;
- term/ABA protection for entry persistence acknowledgements;
- the async-mode prohibition on `RawNode.Advance`.

This phase also closes the Phase 11 self-acknowledgement timing deviation.
Self Vote/PreVote responses and the leader's self append acknowledgement are
now delayed until the associated Ready append work is complete, in both
synchronous and asynchronous modes.

## Architecture and dependencies

The implementation keeps the existing layering:

```text
Go RawNode / Go Node actor
  -> existing cgo Ready and Message converters
  -> opaque C RawNode
       -> Ready construction and acceptance
       -> raft_core.c
            -> immediate messages
            -> messagesAfterAppend
            -> log/unstable/applying state
            -> tracker, snapshots, reads, membership, leadership
```

In async mode the application-visible processing flow is:

```text
Ready
  +-- ordinary outbound network messages
  +-- MsgStorageAppend -> LocalAppendThread
  |     payload: Entries, HardState, Snapshot
  |     responses:
  |       messagesAfterAppend, in original order
  |       MsgStorageAppendResp to the local node
  |
  +-- MsgStorageApply -> LocalApplyThread
        payload: committed Entries
        response:
          MsgStorageApplyResp to the local node

append/apply worker completes its work
  -> routes each nested response
  -> local response is stepped back into RawNode
  -> C log progress advances
```

Messages to one local target must be processed reliably and in FIFO order.
The append and apply targets are independent and may make progress
concurrently. This is an application integration contract, matching Go;
RawNode creates the ordered work streams but does not create storage worker
goroutines.

Phase 12 depends on:

- Phase 3 recursive Message conversion and owned output cleanup;
- Phases 4-6 storage, unstable-log, Ready, Advance, and pagination support;
- Phase 7 replication, flow control, uncommitted-size accounting, and Node
  integration;
- Phase 8 membership and apply-time auto-leave;
- Phase 9 snapshot persistence and progress;
- Phase 10 ReadIndex and CheckQuorum;
- Phase 11 PreVote and leadership transfer.

The private C RawNode ABI marker advances from 14 to 15 because RawNode now
owns a `steps_on_advance` message vector.

## Original etcd behavior implemented

The implementation was matched against the repository's current Go
`rawnode.go`, `raft.go`, and `node.go`, rather than the early structural
suggestions in `12_async_storage_write.txt`.

The important semantics are:

1. Ready's normal `Entries`, `HardState`, `Snapshot`, and
   `CommittedEntries` fields remain observable in async mode. They are not
   cleared or replaced in the public Ready value.
2. Async mode additionally creates local storage work in `Ready.Messages`.
   Applications process that work instead of directly processing the
   corresponding Ready fields.
3. Responses which are safe only after durable append are held separately
   in `msgsAfterAppend`.
4. `MsgStorageAppend.Responses` contains those delayed responses first and
   the local `MsgStorageAppendResp` last.
5. `MsgStorageApply` contains committed entries and one local
   `MsgStorageApplyResp`.
6. The append response reattests the exact last in-progress unstable
   `(index, term)`. This prevents a delayed acknowledgement from stabilizing
   a rewritten entry at the same index.
7. A lower-term append response does not stabilize entries. Snapshot
   completion is still accepted because the response identifies the
   in-progress snapshot directly.
8. Async Ready acceptance permits new Ready generation immediately and does
   not arm synchronous Advance.
9. Committed-entry selection excludes unstable entries in async mode.
   Consequently application work cannot overtake persistence.
10. The apply response advances applied state, releases committed-entry
    pagination quota and uncommitted proposal bytes, and may trigger
    configuration auto-leave.
11. Calling `Advance` in async mode is invalid.

## C core changes

`raft_t` now owns two output queues:

- `messages` for immediately sendable output;
- `messages_after_append` for output whose delivery requires append
  persistence.

The send path classifies the same response kinds as Go:

- `MsgAppResp`;
- `MsgVoteResp`;
- `MsgPreVoteResp`.

These messages are delayed until append completion. Self-addressed messages
remain forbidden except for these intentional delayed responses.

Election and replication self-accounting are now response-driven:

- a campaign queues its self Vote/PreVote response instead of recording the
  vote immediately;
- a new leader queues its self `MsgAppResp` instead of immediately advancing
  its own progress and commit.

This is required for async ordering and also makes synchronous Ready/Advance
match the original durability boundary.

The core handles `MsgStorageAppendResp` by:

- stabilizing the exact acknowledged entry index and term only when the
  response term is current;
- applying and stabilizing the in-progress snapshot;
- retaining the Go exception which permits snapshot completion after a term
  change.

The core handles `MsgStorageApplyResp` by:

- calculating the payload size of the acknowledged entries;
- advancing the log's applied state;
- reducing uncommitted-entry accounting;
- checking whether an automatic joint-consensus leave should be proposed.

## Ready construction and acceptance

### Asynchronous mode

Ready construction:

- copies immediate messages normally;
- selects committed entries with `allowUnstable=false`;
- creates an owned append work message when Entries, HardState, Snapshot, or
  messages-after-append exist;
- creates an owned apply work message when stable committed entries exist;
- deep-copies all retained entries, snapshots, contexts, and nested
  responses;
- preserves Ready `MustSync` calculation from the HardState and entry
  changes.

Ready acceptance:

- accepts unstable entries and snapshots as in-progress;
- accepts only the selected stable committed entries as applying;
- clears immediate and after-append queues;
- drains read states and records previous SoftState/HardState;
- leaves no accepted-Ready gate and prepares no `stepsOnAdvance`;
- increments the Ready generation so another Ready may be produced
  immediately.

This permits multiple outstanding append/apply work messages while the log
prevents the same work from being re-emitted.

### Synchronous mode

The existing synchronous API is retained, with corrected self-response
timing:

- remote delayed responses are included in the Ready message list;
- self delayed responses and synthetic append/apply responses are captured
  in RawNode's `steps_on_advance`;
- `Advance` steps those messages in Go's exact order, so entry stability,
  snapshot completion, and apply progress use the same response handlers and
  term checks as async mode;
- the accepted Ready remains the only outstanding synchronous Ready.

This preserves the Go contract that the local vote or append acknowledgement
cannot affect consensus before the application has persisted the Ready.

`raft_raw_node_advance` returns `RAFT_ERR_INVALID_ARGUMENT` in async mode.
The Go wrapper retains the original panic behavior.

## Go binding and Node actor

No public Go API was added or changed. Existing Message conversion already
supported:

- local target IDs;
- entries, snapshots, and HardState;
- recursively owned `Responses`.

The cgo RawNode continues to cache `AsyncStorageWrites` for the Go-side
Advance check.

One Node integration correction was required. `node.run` may call
`readyWithoutAccept`, lose the Ready-channel select to another event, and
preview again. A C Ready preview is read-only and has no application
obligation, so the binding now destroys and replaces an old unaccepted
preview rather than panicking. Accepted Ready ownership and synchronous
Advance behavior are unchanged.

The existing C Message ABI stores scalar values without general protobuf
presence bits. The conversion layer now applies the required
`MsgStorageAppend` rule: an all-zero C term/vote/commit tuple is returned to
Go as three absent fields, while a HardState update returns all three fields
present. A valid Raft HardState cannot transition back to all zero values, so
this rule is unambiguous for C-generated append work. The broader inherited
optional-scalar presence gap remains separately tracked.

## Memory ownership

The phase follows the existing view/owned split:

- Go-to-C Message views are borrowed only for the duration of one call;
- Ready and local work messages are C-owned;
- Entries, snapshots, bytes, and nested Responses in local work are
  recursively deep-copied;
- C-owned Ready data is converted to ordinary Go-owned `raftpb` values;
- destroying the C Ready recursively frees every owned payload;
- RawNode separately owns and frees queued `messages_after_append` and
  synchronous `steps_on_advance`;
- no Go pointer is retained by C.

Snapshot tests explicitly mutate the source snapshot after Ready generation
to verify that async work owns its payload independently.

## Files changed

Implementation:

- `c/src/raft_core.h`;
- `c/src/raft_core.c`;
- `c/src/raft_internal.h`;
- `c/src/raw_node.c`;
- `c/include/raft/raft.h`;
- `convert_cgo.go`;
- `rawnode_cgo.go`.

Tests:

- `async_storage_parity_test.go` (new);
- `c/tests/raft_core_test.c`;
- `c/tests/snapshot_test.c`;
- `rawnode_cgo_test.go`.

Tracking documentation:

- `OUTPUT/CONFIG_FIELDS_FOR_C_PORT.md`;
- `OUTPUT/C_PORT_TRACKING_GAPS.md`;
- `OUTPUT/RAFTPB_WIRE_COMPAT_FOR_C_PORT.md`;
- `OUTPUT/RAWNODE_API_PARITY_FOR_C_PORT.md`;
- `OUTPUT/PHASE12_C_ASYNC_STORAGE_WRITES_RESULT.md` (this file).

## Adaptations from `12_async_storage_write.txt`

The high-level protocol requirements were followed directly. Historical
details were adapted as follows:

- The active build tag is the repository's `cgo_raft`, not the prompt's
  illustrative `crawnode`.
- Ready's ordinary persistence/application fields remain populated, matching
  current etcd. The local messages change how applications process Ready;
  they do not erase those fields.
- Storage workers remain application-owned. The C backend generates work and
  consumes responses but does not add a worker/thread subsystem.
- The existing recursive Message ABI was reused; no parallel async-specific
  message representation was introduced.
- The existing C log's unstable and applying markers provide outstanding-work
  tracking; no second request ledger was added.
- `raft_raw_node_advance` uses the established C error convention while the
  Go wrapper provides the externally compatible panic.
- The Node preview lifecycle was corrected in the binding because this is
  where C Ready ownership lives.

No approximation was made to append/apply ordering, delayed responses,
stable-only application, term validation, or the Advance restriction.

## Tests and differential results

`async_storage_parity_test.go` is compiled unchanged against the pure-Go and
`cgo_raft` RawNode implementations. It covers:

- async configuration and bootstrap;
- append work followed by stable-only apply work;
- local target IDs and public Step rejection of local work messages;
- absent versus present HardState fields on append work;
- state gating before and after storage responses;
- delayed self vote and delayed leader self append acknowledgement;
- response order inside append work;
- commit-only versus entry/term `MustSync`;
- independent append and apply completion;
- pipelined append requests without Advance and FIFO indexes;
- ReadIndex in async mode;
- a committed membership change through async apply;
- snapshot ownership and nested remote response routing;
- snapshot completion after a term change;
- entry response reattestation after a term change;
- Advance panic behavior;
- repeated Node actor Ready delivery without Advance;
- apply pagination and quota release across outstanding work.

The direct C test covers local append/apply generation, nested response
ordering, response-driven stability/application, and the native Advance
error. Existing synchronous C and cgo fixtures were updated to persist Ready
before driving the newly delayed self vote/append response.

The shared pure-Go and C-backed scenarios produce equivalent Ready fields,
local work, response ordering, state transitions, pagination, membership,
snapshot, ReadIndex, Node, and panic behavior.

## Validation

All commands below passed on 2026-07-30.

Strict GCC C11 warning-as-error build and all C test binaries:

```text
make test-c
```

AddressSanitizer, LeakSanitizer, and UndefinedBehaviorSanitizer:

```text
make test-c-sanitize
```

Strict Clang C11 warning-as-error build:

```text
make -C c clean test CC=clang
```

Valgrind for all eight C test binaries:

```text
valgrind --quiet --error-exitcode=99 --leak-check=full \
  --errors-for-leak-kinds=all <each C test binary>
```

Default and C-backed Go suites:

```text
go test -count=1 ./...
CGO_ENABLED=1 go test -count=1 -tags=cgo_raft ./...
```

Extended cgo pointer checking:

```text
CGO_ENABLED=1 GOEXPERIMENT=cgocheck2 \
  go test -count=1 -tags=cgo_raft ./...
```

Race detector:

```text
CGO_ENABLED=1 go test -count=1 -race -tags=cgo_raft ./...
```

Both build variants with TLA build tags:

```text
go test -count=1 -tags=with_tla ./...
CGO_ENABLED=1 go test -count=1 -tags='cgo_raft with_tla' ./...
```

`git diff --check` also passed. No sanitizer, Valgrind, race,
strict-compiler, cgo-pointer, or test failure remained.

## Known limitations and remaining TODOs

Async storage writes are implemented without a known semantic deviation in
the tested RawNode protocol. The external application must still honor
etcd's same-target FIFO, reliable-delivery, and durability requirements when
it implements the two local workers.

Remaining repository work is outside this phase:

- exhaustive protobuf optional-scalar presence beyond the
  `MsgStorageAppend` HardState tuple handled here;
- `ReportUnreachable` and `MsgUnreachable`;
- randomized election-timeout parity;
- TraceLogger integration (both `with_tla` variants build and test);
- deterministic allocation-failure injection for the new deep-copy paths;
- longer randomized multi-node async-worker traces across partitions,
  restarts, snapshots, membership changes, and reordered independent worker
  completion;
- higher-level etcd integration after the remaining RawNode gap is closed.

The full upstream interaction harness remains excluded by the repository's
`cgo_raft` build constraints. This phase adds shared scripted differential
coverage for the async protocol, but it does not yet replace that larger
randomized/integration layer.

## Notes for the next session

- Keep `msgsAfterAppend` separate from immediately sendable messages.
- Preserve nested append response order: delayed responses first, local
  append response last.
- Never let committed apply work include unstable entries in async mode.
- Do not arm Advance or block the next Ready after async acceptance.
- Keep entry acknowledgements bound to both index and term.
- Preserve the lower-term snapshot-completion exception.
- Keep self vote and self append progress response-driven in synchronous mode
  as well as async mode.
- Continue allowing unaccepted Node Ready previews to be replaced.
- Treat local append/apply IDs as reserved endpoints, never network peers.
