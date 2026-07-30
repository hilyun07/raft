# Phase 13 C ReportUnreachable Result

## Outcome

Phase 13 implements the final genuinely unfinished public RawNode path:
`ReportUnreachable` and its local `MsgUnreachable` handling.

The opt-in `cgo_raft` backend now matches the original etcd behavior:

- direct `RawNode.ReportUnreachable(id)` reaches the C Raft core;
- `Node.ReportUnreachable(id)` reaches the same core behavior through the
  existing Node actor path;
- a leader moves a reported peer from `StateReplicate` to `StateProbe`;
- unknown peers, non-leaders, already-probing peers, and snapshot-state peers
  are no-ops;
- reporting does not create immediate Ready work or an outbound message;
- subsequent heartbeat/append-response processing restores normal
  replication;
- directly stepping outer `MsgStorageAppend` and `MsgStorageApply` work into
  the core remains explicitly unsupported.

No public API, C ABI, tracker state, allocation, Ready behavior, storage
integration, or Go production code changed.

## Original etcd behavior

The implementation was matched against:

- `RawNode.ReportUnreachable` in `rawnode.go`;
- `Node.ReportUnreachable` and `node.run` in `node.go`;
- `MsgUnreachable` handling in the leader step function in `raft.go`;
- `Progress.ResetState` and `Progress.BecomeProbe` in
  `tracker/progress.go`;
- the upstream unreachable, reordered-message, and heartbeat-recovery tests.

The direct RawNode API constructs a local term-zero
`MsgUnreachable{From: id}` and steps it directly into Raft. It does not use
public `RawNode.Step`, because that API rejects unexpected local messages.

The Node actor sends the same local message through `recvc` and
`stepForNode`.

Only leader state has behavior after progress lookup:

```text
progress missing                 -> no-op
progress StateProbe              -> no-op
progress StateSnapshot           -> no-op
progress StateReplicate          -> BecomeProbe
follower/candidate/pre-candidate -> no-op
```

`ReportUnreachable` does not itself resend append work. A later proposal,
broadcast, or heartbeat response sends a probe from the last confirmed
`Match` index.

## Implementation

### RawNode helper

`raft_raw_node_report_unreachable` now:

1. retains the existing RawNode and real-node-ID validation;
2. creates a stack-owned `raft_message_view_t`;
3. sets `type = RAFT_MSG_UNREACHABLE`;
4. sets `from = id`;
5. marks the empty context as nil;
6. delegates directly to `raft_core_step`.

The message is call-scoped and contains no allocated or retained data.

### Core dispatch

`RAFT_MSG_UNREACHABLE` was removed from the global unsupported-message guard.

The guard still contains:

- `RAFT_MSG_STORAGE_APPEND`;
- `RAFT_MSG_STORAGE_APPLY`.

Those are local worker instructions and must be processed by the application,
not stepped into Raft.

### Leader handling

`core_step_leader` now looks up the reporting peer's progress. If it exists
and is in `RAFT_PROGRESS_STATE_REPLICATE`, it calls the existing
`raft_progress_become_probe`. Every other case returns success without a
state change.

No new transition logic was introduced.

## Transition invariants

The existing C tracker transition already matches Go. For a replicate-state
peer it:

- preserves `match_index`;
- changes state to probe;
- resets `next_index` to `match_index + 1`;
- regresses `sent_commit` to at most `next_index - 1`;
- clears `message_flow_paused`;
- clears `pending_snapshot`;
- resets inflight start, count, and bytes;
- retains the allocated inflight buffer and its configured limits;
- preserves recent activity;
- preserves learner status.

It does not change:

- Raft term, vote, commit, or leader;
- log contents or persistence state;
- election/heartbeat timers;
- CheckQuorum recent activity;
- leadership-transfer state;
- membership configuration;
- Ready or message queues.

## Recovery

The existing C recovery path required no changes:

1. `MsgHeartbeatResp` clears flow pause.
2. Probe state forces an append even when the follower is fully caught up.
3. The append is anchored at the confirmed `Match`.
4. A successful `MsgAppResp` moves progress back to replicate.
5. Delayed rejection handling cannot regress below the confirmed match.

Shared differential coverage verifies this recovery against the pure-Go
implementation.

## Tests

### Shared pure-Go/C parity

`report_unreachable_parity_test.go` compiles unchanged against the pure-Go
and `cgo_raft` RawNode implementations. It covers:

- direct `RawNode.ReportUnreachable`;
- Node actor `ReportUnreachable`;
- replicate-to-probe transition;
- `Match` preservation;
- `Next = Match + 1`;
- flow-pause, pending-snapshot, recent-activity, and learner invariants;
- optimistic `Next` regression;
- follower no-op;
- unknown-peer no-op;
- repeated report while already probing;
- absence of immediate Ready work;
- heartbeat-response probe emission;
- successful append-response recovery to replicate.

### Native C

`c/tests/raft_core_test.c` covers:

- direct C RawNode reporting;
- the lower Node/core message entry path;
- follower, unknown-peer, and probe-state no-ops;
- leader replicate-to-probe behavior;
- no immediate Ready;
- recovery to replicate;
- retained unsupported results for outer storage append/apply work.

`c/tests/snapshot_test.c` verifies that reporting an unreachable
snapshot-state peer leaves its state, pending snapshot, indexes, activity,
pause, and learner fields unchanged.

`c/tests/tracker_test.c` verifies all tracker-only reset invariants, including
inflight count/bytes, retained allocation/configuration, and `sent_commit`.

The previous skeleton assertion was updated from not-implemented to success.

## Files changed

Production:

- `c/src/raw_node.c`;
- `c/src/raft_core.c`.

Tests:

- `report_unreachable_parity_test.go` (new);
- `c/tests/raw_node_skeleton_test.c`;
- `c/tests/raft_core_test.c`;
- `c/tests/snapshot_test.c`;
- `c/tests/tracker_test.c`.

Documentation:

- `OUTPUT/REPORT_UNREACHABLE_ANALYSIS.md`;
- `OUTPUT/RAWNODE_API_PARITY_FOR_C_PORT.md`;
- `OUTPUT/C_PORT_TRACKING_GAPS.md`;
- `OUTPUT/PHASE13_C_REPORT_UNREACHABLE_RESULT.md` (this file).

## Validation

All commands below passed on 2026-07-30.

Strict native C build and all eight C test binaries:

```text
make test-c
```

AddressSanitizer, LeakSanitizer, and UndefinedBehaviorSanitizer:

```text
make test-c-sanitize
```

Strict Clang build:

```text
make -C c clean test CC=clang
```

Valgrind for all eight native C test binaries:

```text
valgrind --quiet --error-exitcode=99 --leak-check=full \
  --errors-for-leak-kinds=all <each C test binary>
```

Default and C-backed Go suites:

```text
go test -count=1 ./...
CGO_ENABLED=1 go test -count=1 -tags=cgo_raft ./...
```

Strict cgo pointer checking:

```text
CGO_ENABLED=1 GOEXPERIMENT=cgocheck2 \
  go test -count=1 -tags=cgo_raft ./...
```

Race detector:

```text
CGO_ENABLED=1 go test -count=1 -race -tags=cgo_raft ./...
```

Both TLA build variants:

```text
go test -count=1 -tags=with_tla ./...
CGO_ENABLED=1 go test -count=1 -tags='cgo_raft with_tla' ./...
```

`git diff --check` also passed.

## Deviations and remaining TODOs

There is no known behavioral deviation in the implemented
ReportUnreachable path.

The existing C boundary rejects reserved/non-real reporting IDs, while Go
would generally reduce them to an unknown-progress no-op. This is the
project's documented peer-reporting boundary policy and was not changed.
Valid but unknown real peer IDs remain no-ops.

No genuine public RawNode operation now returns
`RAFT_ERR_NOT_IMPLEMENTED`. The remaining not-implemented core cases are the
intentional guards against stepping outer local storage work.

Remaining broader work:

- randomized election-timeout parity;
- TraceLogger integration;
- exhaustive protobuf optional-scalar presence;
- deterministic allocation-failure testing for allocation-bearing paths;
- the larger randomized/interaction harness under `cgo_raft`;
- higher-level etcd integration.

## Notes for the next session

- Keep `MsgUnreachable` term zero and local.
- Keep the direct RawNode helper below public `RawNode.Step` validation.
- Do not mark the reported peer inactive for CheckQuorum.
- Do not move probe or snapshot progress through `BecomeProbe` again.
- Do not emit append work directly from the report.
- Preserve the separate `ReportSnapshot` behavior for snapshot transport.
- Keep outer `MsgStorageAppend` and `MsgStorageApply` work out of core Step.
