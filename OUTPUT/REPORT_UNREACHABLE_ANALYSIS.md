# ReportUnreachable / MsgUnreachable Analysis

> Implementation status: completed in Phase 13 after this pre-implementation
> analysis. See `PHASE13_C_REPORT_UNREACHABLE_RESULT.md`.

## Finding

At the time of this analysis, `ReportUnreachable` / `MsgUnreachable` was the
only genuine unfinished public behavior. The C tracker already contained the
required state transition; the missing portion was its RawNode/core wiring.

No implementation changes were made as part of this analysis.

## Original etcd flow

There are two entry paths.

### Direct RawNode path

`RawNode.ReportUnreachable(id)` constructs:

```text
MsgUnreachable{From: id}
```

and steps it directly into the Raft state machine.

It deliberately bypasses public `RawNode.Step`, because `MsgUnreachable` is a
local message.

Source: `rawnode.go:558`.

### Node actor path

`Node.ReportUnreachable(id)` sends the same message through `node.recvc`.
`node.run` passes it through `RawNode.stepForNode`. Errors on this asynchronous
path are ignored because there is no result channel.

Sources:

- `node.go:589`;
- `node.go:399`.

`MsgUnreachable` is classified as both local and response-like:

- calling ordinary `RawNode.Step` with a normal peer as `From` returns
  `ErrStepLocalMsg`;
- `stepForNode` filters unknown peers as `ErrStepPeerNotFound`, which the Node
  actor ignores;
- direct `RawNode.ReportUnreachable` bypasses that filter, and the leader's
  progress lookup makes an unknown peer a no-op.

The message has term zero. It therefore does not participate in term
comparison, elections, or leader changes.

## Leader handling

Only a leader changes state. After looking up `Progress[m.From]`:

- unknown progress: no-op;
- `StateReplicate`: call `Progress.BecomeProbe()`;
- `StateProbe`: no-op;
- `StateSnapshot`: no-op, because snapshot delivery has the separate
  `ReportSnapshot` path;
- learners and voters are treated identically;
- followers, candidates, and pre-candidates ignore the message.

Source: `raft.go:1629`.

`ReportUnreachable` does not immediately send another append. A later
proposal/broadcast or heartbeat response initiates probing.

## Exact BecomeProbe transition

For the relevant `StateReplicate` case, `Progress.BecomeProbe()` performs:

| State | Before | After |
| --- | --- | --- |
| `State` | `StateReplicate` | `StateProbe` |
| `Match` | last confirmed index | unchanged |
| `Next` | optimistically advanced | `Match + 1` |
| `sentCommit` | possibly ahead of `Match` | `min(oldSentCommit, Match)` |
| `MsgAppFlowPaused` | either value | `false` |
| `PendingSnapshot` | normally zero | `0` |
| inflight count/bytes | possibly nonzero/full | reset to zero |
| `RecentActive` | either value | unchanged |
| learner/configuration status | either value | unchanged |

Source: `tracker/progress.go:130`.

The allocated inflight buffer and configured limits are retained. Only its
active window is reset.

The important invariants are:

- optimistic replication is abandoned conservatively;
- `Next` returns to the first unconfirmed index;
- `Match < Next` remains true;
- lost inflight messages no longer consume flow-control capacity;
- previously advertised commit progress may be retransmitted;
- a transport failure does not itself mark the peer inactive for CheckQuorum;
- no log, term, vote, commit, timer, transfer target, or membership state
  changes;
- no Ready, persistence work, or immediate network message is generated.

## Recovery behavior

The surrounding Go behavior ensures that the downgrade is not permanent:

1. A later heartbeat response clears flow pause.
2. Because the progress is `StateProbe`, the leader sends a `MsgApp` even if
   the follower is already fully caught up.
3. The probe is anchored at `Match`.
4. A successful `MsgAppResp` moves the progress back to `StateReplicate`.
5. A delayed rejection is clamped against `Match`, preventing replication
   from regressing below a confirmed index.

This is covered upstream by:

- `TestRecvMsgUnreachable`;
- `TestLogReplicationWithReorderedMessage`;
- `testdata/heartbeat_resp_recovers_from_probing.txt`.

## Current C state

The C tracker already implements the complete transition:

- state reset;
- flow-pause clearing;
- pending-snapshot clearing;
- inflight reset;
- `Next = Match + 1`;
- `sent_commit` regression.

Source: `c/src/tracker.c:376`.

The recovery machinery is also already present:

- `MsgHeartbeatResp` sends an append whenever progress is probing;
- successful `MsgAppResp` moves probe back to replicate;
- rejection handling respects `Match`;
- append flow control understands probe mode.

## Pre-Phase13 missing implementation

Exactly three pieces were absent.

### RawNode helper

`raft_raw_node_report_unreachable` validates the ID and then unconditionally
returns `RAFT_ERR_NOT_IMPLEMENTED`.

Source: `c/src/raw_node.c:1686`.

### Core dispatch

`raft_core_step` rejects every `MsgUnreachable` before role-specific dispatch.

Source: `c/src/raft_core.c:2560`.

### Leader handler

`core_step_leader` has no `RAFT_MSG_UNREACHABLE` case invoking the existing
`raft_progress_become_probe`.

Before Phase 13:

- C-backed direct `RawNode.ReportUnreachable` currently panics through the Go
  error bridge;
- C-backed `Node.ReportUnreachable` silently does nothing because the Node
  actor discards the internal not-implemented error;
- no progress state changes.

## Implemented shape

Phase 13:

- have the RawNode helper construct a local
  `MsgUnreachable{From: id}` and delegate directly to the core;
- remove only `RAFT_MSG_UNREACHABLE` from the global unsupported guard;
- preserve the intentional unsupported guard for directly stepped outer
  `MsgStorageAppend` and `MsgStorageApply` work;
- add the conditional replicate-to-probe case to `core_step_leader`;
- return success for unknown peers, non-leaders, probe state, and snapshot
  state.

No new tracker type, state field, allocation, Ready behavior, storage
integration, or ABI shape is required.

## Boundary compatibility note

C currently rejects reserved/non-real IDs at the reporting API, while Go
generally turns them into an unknown-progress no-op.

The project's existing sentinel-ID specification explicitly requires
peer-reporting APIs to receive a valid real node ID, so retaining this behavior
is consistent with the current C boundary policy.

Go does not contain a special guard against reporting the local node itself.
The C validity check also admits the local node's real ID. A faithful leader
handler therefore should not introduce a new self-specific rejection.

## Recommended validation

Implementation tests should verify:

- leader `StateReplicate` to `StateProbe`;
- exact `Next = Match + 1`;
- inflight count and bytes reset;
- `sent_commit` regression;
- flow pause and pending snapshot reset;
- `Match`, `RecentActive`, learner status, term, commit, and transfer state
  remain unchanged;
- probe, snapshot, unknown-peer, and non-leader no-ops;
- no immediate Ready or outbound message;
- heartbeat-response recovery for an already caught-up follower;
- delayed append rejection after the unreachable transition;
- direct RawNode and Node actor entry paths;
- behavior under both synchronous and asynchronous storage-write modes.
