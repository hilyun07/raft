# Phase 29: Final Differential Audit

## Executive summary

This audit compared the current C-backed implementation with the actual
`etcd-io/raft` v3.7.0 Go implementation. It did not modify production code or
tests.

The result is:

```text
Porting status: NOT COMPLETE
Confirmed remaining compatibility gaps: 2
```

The two confirmed gaps are:

1. `ErrSnapshotTemporarilyUnavailable` is treated as retryable by the C
   boundary even when it is returned by a storage operation other than
   `Storage.Snapshot`. Go treats those uses as unexpected fatal storage
   failures. On an error-returning RawNode path C can return an ordinary error,
   and the asynchronous Node receive path can silently ignore it.
2. A leader demoted to a learner while `StepDownOnRemoval` is false accepts a
   normal proposal in Go but rejects it with `ErrProposalDropped` in C.

The single recommended next phase is:

```text
PHASE 30: Storage Error Domain Classification —
          ErrSnapshotTemporarilyUnavailable
```

It is higher priority because the mismatch crosses Storage, RawNode, Node, and
the Phase 19 fatal-error latch. The demoted-leader proposal mismatch remains a
confirmed lower-priority follow-up; it was not fixed in this audit.

Candidate accounting:

| Classification | Count |
| --- | ---: |
| Investigated | 31 |
| Already fixed by Phase 1-28 | 17 |
| False positive | 3 |
| Intentional/documented difference | 8 |
| Needs verification | 1 |
| Confirmed remaining gap | 2 |

No temporary differential test remains in the repository. The only persistent
change from Phase 29 is this document.

## Audit baseline

### Authoritative reference

The authoritative reference is the signed tag:

```text
etcd-io/raft v3.7.0
commit b867cf13f6bc0dae21204302df97bc2355c3af55
```

Current repository state during the audit:

```text
HEAD       2573344ac003a69f61be39054c284c4880bb2b33
describe   v3.7.0-28-g2573344
merge-base b867cf13f6bc0dae21204302df97bc2355c3af55
```

The peeled `v3.7.0` tag and the merge base are the same commit. The original Go
implementation remains in this repository and is selected by `!cgo_raft`.
The C-backed implementation is selected by `cgo_raft && cgo`.

### Current architecture

The current repository still follows the porting specification:

```text
shared Go Node actor
        |
        v
backend-selected Go RawNode
        |
        v
cgo conversion and Storage callback bridge
        |
        v
opaque C RawNode
        |
        +-- Raft core
        +-- tracker / quorum / configuration
        +-- log / unstable
        +-- read-only state
        +-- Ready and async-storage work construction
```

Important locations:

| Area | Current source |
| --- | --- |
| Go reference RawNode | `rawnode.go`, `bootstrap.go` |
| Shared Go Node actor | `node.go` |
| Go reference core/log | `raft.go`, `log.go`, `tracker/`, `confchange/` |
| cgo RawNode wrapper | `rawnode_cgo.go` |
| cgo protobuf conversion | `convert_cgo.go` |
| cgo Storage bridge | `storage_bridge_cgo.go`, `errors_cgo.go` |
| Public C ABI | `c/include/raft/raft.h` |
| C RawNode | `c/src/raw_node.c` |
| C Raft core | `c/src/raft_core.c` |
| C log/unstable | `c/src/log.c`, `c/src/unstable.c` |
| C tracker/config | `c/src/tracker.c`, `c/src/confchange.c` |
| C read-only state | `c/src/read_only.c` |
| Native tests | `c/tests/` |
| Backend-neutral parity tests | root `*_parity_test.go` files |

## Phase 1-28 resolved-finding inventory

All files matching `OUTPUT/PHASE*.md` were enumerated and read. There is no
file named `PHASE3_*.md`; the work between Phases 2 and 4 is instead recorded
in the ancillary C API, pointer-safety, payload, Ready-ownership, error, and
sentinel-ID documents under `OUTPUT/`.

The phase documents are historical evidence. Every Phase 13-28 semantic claim
listed below was also checked against current source.

| Phase | Area/finding | Historical result | Current status |
| ---: | --- | --- | --- |
| 1 | RawNode boundary and Node decoupling | Shared Node stopped dereferencing Go Raft internals and began using RawNode methods | Present |
| 2 | Public C RawNode skeleton | ABI, ownership conventions, result taxonomy, and full surface skeleton introduced | Superseded by implemented backend; ABI remains |
| 3 | No formal `PHASE3_*.md` | Ancillary boundary corrections were documented separately | No missing claimed phase result |
| 4 | cgo RawNode wrapper skeleton | Go API forwarded through cgo with conversion and finalizer ownership | Present |
| 5 | Storage callback bridge | `cgo.Handle`, synchronous callbacks, deep C-owned results, panic containment | Present |
| 6 | Raft log/unstable | Stable/unstable lookup, slices, truncation, commit/apply, storage fallback | Present |
| 7 | Minimal C Raft core | Elections, follower/leader stepping, append and Ready baseline | Present and extended |
| 8 | Tracker/quorum/config changes | Progress, inflights, joint consensus, learners, configuration application | Present |
| 9 | Snapshot semantics | Send, restore, report, compaction fallback, Ready snapshot ownership | Present |
| 10 | ReadIndex/check-quorum | Safe and lease reads, duplicate contexts, quorum acknowledgement | Present |
| 11 | PreVote/leadership transfer | PreVote, transfer, timeout-now, forget-leader | Present |
| 12 | Async storage writes | Append/apply work messages, responses, persistence ordering, delayed self acknowledgements | Present |
| 13 | `ReportUnreachable` | Replicate-to-probe transition implemented | **ALREADY FIXED** |
| 14 | Election timeout randomization | Reset chooses once in `[ElectionTick, 2*ElectionTick-1]` | **ALREADY FIXED** |
| 15 | Unknown-peer response filtering | RawNode and Node paths share the filter | **ALREADY FIXED** |
| 16 | `MaxUncommittedEntriesSize` | First oversized proposal allowed on an empty uncommitted tail | **ALREADY FIXED** |
| 17 | rejected `MsgAppResp` `LogTerm` | `RejectHint` translated with `findConflictByTerm` | **ALREADY FIXED** |
| 18 | `Status.Progress.Inflights` | Logical inflight snapshot exported | **ALREADY FIXED** |
| 19 | Fatal storage/allocation propagation | First terminal C failure latched and surfaced as Go panic | Mostly present; one newly confirmed call-site classification hole remains |
| 20 | cgo RawNode finalizer liveness | Every `rn.p` call retains `rn` through the native lifetime boundary | **ALREADY FIXED** |
| 21 | Candidate vote ownership | Votes separated from Progress and reset at campaign boundaries | **ALREADY FIXED** for the audited remove/re-add scenario |
| 22 | Progress reset semantics | Match/Next/activity/commit/snapshot/inflight reset aligned | **ALREADY FIXED** |
| 23 | Invalid ConfState and heartbeat invariant | malformed restore rejected; oversized heartbeat commit is fatal | **ALREADY FIXED** |
| 24 | Protobuf object fidelity | supported presence and unknown fields retained | **ALREADY FIXED**; raw-wire limitations remain intentional |
| 25 | Node deterministic destruction | Stop destroys the Node-owned C RawNode before closing `done` | **ALREADY FIXED** |
| 26 | stale successful `MsgAppResp` | follow-up work remains inside Go's actionable-response branch | **ALREADY FIXED** |
| 27 | zero-entry `MsgProp` | leader treats it as fatal; empty-data entries remain valid | **ALREADY FIXED** |
| 28 | `Status.Config.AutoLeave` | reproduces v3.7.0 clone behavior, including false AutoLeave | **ALREADY FIXED** |

### Reverified fixed findings

The following 17 concrete semantic findings were independently rechecked:

| ID | Finding | Current evidence | Classification |
| --- | --- | --- | --- |
| R1 | `ReportUnreachable` | leader `MsgUnreachable` moves replicate Progress to probe | ALREADY FIXED, Phase 13 |
| R2 | election timeout randomization | reset stores one randomized value; ticks do not redraw it | ALREADY FIXED, Phase 14 |
| R3 | unknown-peer response filter | both C RawNode entry points enforce the shared response rule | ALREADY FIXED, Phase 15 |
| R4 | first oversized uncommitted proposal | empty uncommitted tail bypasses the limit once | ALREADY FIXED, Phase 16 |
| R5 | rejected `MsgAppResp.LogTerm` | C invokes conflict-by-term before `maybe_decr_to` | ALREADY FIXED, Phase 17 |
| R6 | Status inflights | logical indexes, bytes, limits, and fullness reconstruct correctly | ALREADY FIXED, Phase 18 |
| R7 | general fatal error latch | storage compacted/unavailable escapes, OOM, callback panic, and fatal errors latch | ALREADY FIXED, Phase 19, subject to C1 below |
| R8 | RawNode finalizer liveness | `cOperationError` or explicit barriers occur after every native use | ALREADY FIXED, Phase 20 |
| R9 | candidate vote ownership | granted/rejected vote vectors have a lifetime independent of Progress | ALREADY FIXED, Phase 21 |
| R10 | Progress reset | full field reset and self Match behavior match Go | ALREADY FIXED, Phase 22 |
| R11 | malformed ConfState | incoming voter and `LearnersNext` overlap is rejected | ALREADY FIXED, Phase 23 |
| R12 | heartbeat commit invariant | follower commits exact requested index or fails fatally | ALREADY FIXED, Phase 23 |
| R13 | protobuf object fidelity | supported object presence and unknown bytes survive conversion | ALREADY FIXED, Phase 24 |
| R14 | Node destruction | actor destroys native ownership before exposing completion | ALREADY FIXED, Phase 25 |
| R15 | stale successful append response | stale success cannot trigger commit/send/transfer follow-up | ALREADY FIXED, Phase 26 |
| R16 | zero-entry proposal | leader failure class and check ordering match Go | ALREADY FIXED, Phase 27 |
| R17 | Status configuration shape | AutoLeave and empty incoming voter set match v3.7.0 Status | ALREADY FIXED, Phase 28 |

## Areas audited

### RawNode public surface

The following current paths were compared with `rawnode.go`,
`bootstrap.go`, and the v3.7.0 core:

- construction and initial state restoration;
- `Tick` and `TickQuiesced`;
- `Campaign`;
- `Propose` and `ProposeConfChange`;
- `ApplyConfChange`;
- public `Step` and package-private Node stepping;
- `HasReady`, `Ready`, preview/accept, and `Advance`;
- `BasicStatus`, `Status`, and `WithProgress`;
- `ReportUnreachable` and `ReportSnapshot`;
- `TransferLeader` and `ForgetLeader`;
- `ReadIndex`;
- `Bootstrap`.

All operations exist in the C ABI and cgo wrapper. The two confirmed gaps are
not missing entry points: they are a storage failure classification error and
one proposal-admission predicate.

### Node

`node.go` is shared by both backends. Start/restart, channel behavior, contexts,
proposal routing, Step, Ready, Advance, Status, leadership transfer,
ForgetLeader, ReadIndex, Stop, repeated Stop, and post-stop behavior therefore
share the same actor implementation.

The backend-sensitive points were checked:

- Node calls only RawNode methods and package-private RawNode helpers.
- Node uses `stepForNode`, retaining unknown-peer filtering while accepting
  generated local messages.
- Phase 25 destruction occurs in the actor before `done` is closed.
- Application-owned async-storage workers do not retain C pointers.
- A terminal C result panics before Node can discard an ordinary Step result,
  except for confirmed finding C1.

### Message handling

| Message group | Result |
| --- | --- |
| `MsgHup`, `MsgBeat`, `MsgCheckQuorum` | No new mismatch found |
| `MsgProp` | One new mismatch after self-demotion; ordinary, empty-entry, forwarding, transfer, and removed-peer cases otherwise match |
| `MsgApp` | matching, append, conflict hint, commit, and rejection paths match |
| `MsgAppResp` | LogTerm optimization and stale/duplicate/out-of-order behavior match after Phases 17 and 26 |
| `MsgVote`, `MsgVoteResp`, `MsgPreVote`, `MsgPreVoteResp` | normal vote, lease, PreVote, term, and quorum behavior match |
| `MsgSnap`, `MsgSnapStatus` | normal restore/report/fallback paths match |
| `MsgHeartbeat`, `MsgHeartbeatResp` | commit invariant, activity, append recovery, and read acknowledgement match |
| `MsgUnreachable` | matches after Phase 13 |
| `MsgReadIndex`, `MsgReadIndexResp` | singleton, safe, lease, forwarded, duplicate-context, and malformed-count behavior match |
| `MsgTransferLeader`, `MsgTimeoutNow`, `MsgForgetLeader` | no new mismatch found |
| `MsgStorageAppendResp`, `MsgStorageApplyResp` | stability/application response processing and ordering match |
| outer `MsgStorageAppend`, `MsgStorageApply` | intentionally handled by application workers and rejected by direct core stepping |
| `MsgStorageError` | not applicable: no such v3.7.0 `raftpb.MessageType` exists |

### Elections and state transitions

Follower/candidate/pre-candidate/leader transitions, term changes, delayed
self-vote persistence, vote reset, random election timeout, check-quorum,
leader-transfer timeout, and snapshot-triggered follower transitions were
checked.

No new normal election transition mismatch was found. Candidate V1 below is
retained as `NEEDS VERIFICATION` because a private C guard differs from Go,
but public filtering and normal campaign/application ordering prevent a
supported distinguishing execution from being established.

### Progress and replication

The audit compared:

- Match, Next, state, probe pause, recent activity;
- pending snapshot and pending configuration index;
- sent commit;
- inflight count, byte limit, freeing, and fullness;
- optimistic update;
- probe/replicate/snapshot transitions;
- rejection, conflict-term backtracking, stale success/rejection;
- eager commit delivery;
- leadership-transfer completion;
- learner replication.

No new replication mismatch was established. Saturating C arithmetic differs
from Go wraparound only at impossible or invariant-violating `uint64` log
boundaries and is not a supported behavioral gap.

### Ready and storage

Ready field construction, soft/hard state, MustSync, entry and message order,
committed-entry pagination, snapshot ownership, read states, preview/accept,
Advance, synchronous delayed responses, and async append/apply work were
checked.

Object ownership and ordering match the intended contract. Confirmed finding
C1 affects the classification of one sentinel when it is returned from the
wrong Storage method; it does not change successful Ready construction.

### Snapshots

Normal snapshot send, temporary unavailability, report status, restore,
fast-forward, local membership check, metadata/ConfState fidelity, stable
snapshot response, and compacted/unavailable fallback were checked.

The valid retryable use of `ErrSnapshotTemporarilyUnavailable` from
`Storage.Snapshot` is correct at `c/src/raft_core.c:732-735`. The gap is that
the same result is also allowed to escape unrelated callbacks.

### Configuration changes

Simple and V2 changes, joint entry/leave, auto-leave, learners,
`LearnersNext`, outgoing voters, promotion/demotion, removal/re-addition,
pending changes, snapshot restore, validation, and Status construction were
checked.

The tracker/configuration result of self-demotion is correct in C. The new
gap is only the later leader proposal predicate.

### Status

Basic Status, leader-only Progress, cloned inflights, voter/learner sets,
outgoing voters, `LearnersNext`, v3.7.0's false Status AutoLeave, non-nil
incoming voter map, snapshot independence, applied index, transfer target,
HardState, and SoftState were checked.

No new Status mismatch was found.

### cgo ownership and protobuf boundary

All current `rn.p` call sites were re-enumerated. Lifetime is covered by
`cOperationError`, explicit `runtime.KeepAlive`, or destruction-only
reasoning. No new finalizer or Stop race was found.

Supported protobuf presence and unknown fields are carried recursively across
Message, Entry, Snapshot, SnapshotMetadata, ConfState, ConfChange, Ready, and
storage-response graphs. The remaining raw-wire limitations are documented
intentional ABI limits.

## Confirmed remaining findings

### C1 — Snapshot-temporary error is not scoped to `Storage.Snapshot`

**Classification:** CONFIRMED

**Severity:** High semantic/operational impact, low expected incidence

**Affected areas:** Storage, RawNode, Node, Ready, constructor error policy,
Phase 19 terminal latch

#### Go v3.7.0 behavior

The Storage contract documents `ErrSnapshotTemporarilyUnavailable` only for
`Storage.Snapshot` (`storage.go:91-95`).

Go classifies Storage errors by call site:

- `InitialState`, constructor `FirstIndex`, and constructor `LastIndex` errors
  panic;
- `Term` accepts only compacted/unavailable as expected lookup conditions and
  panics on every other error (`log.go:387-412`);
- `Entries` accepts only its documented compacted behavior at the relevant
  callers and panics on unexpected results;
- only `Snapshot` treats `ErrSnapshotTemporarilyUnavailable` as retryable.

The minimal reproduced path uses vote freshness. `raft.Step` handles
`MsgVote` at `raft.go:1212-1222`; `raftLog.term` panics on a
`Storage.Term` result of `ErrSnapshotTemporarilyUnavailable`.

#### Current C behavior

The cgo bridge uses one context-free mapper for every Storage callback:

```go
case errors.Is(err, ErrSnapshotTemporarilyUnavailable):
    return C.RAFT_ERR_SNAPSHOT_TEMPORARILY_UNAVAILABLE
```

This is at `errors_cgo.go:79-90` and is called by:

- `goRaftStorageInitialState` at `storage_bridge_cgo.go:286-290`;
- `goRaftStorageEntries` at `storage_bridge_cgo.go:327-331`;
- `goRaftStorageTerm` at `storage_bridge_cgo.go:375-380`;
- `goRaftStorageFirstIndex` at `storage_bridge_cgo.go:399-404`;
- `goRaftStorageLastIndex` at `storage_bridge_cgo.go:423-428`;
- `goRaftStorageSnapshot` at `storage_bridge_cgo.go:448-452`.

`raft_result_is_terminal` at `c/src/raft_core.c:52-57` does not include
`RAFT_ERR_SNAPSHOT_TEMPORARILY_UNAVAILABLE`. This is correct only when the
result is consumed by `core_maybe_send_snapshot` at
`c/src/raft_core.c:732-735`.

When the result escapes `Term`, `Entries`, or an index operation, it is not
latched. `RawNode.Step` therefore returns an ordinary
`ErrSnapshotTemporarilyUnavailable` because `returnOrPanic` at
`rawnode_cgo.go:223-236` sees no sticky C error. Node's receive case at
`node.go:399-403` intentionally ignores ordinary Step errors, so the failure
can disappear and the node can continue.

Constructor behavior also differs: `isCConstructorFatal` at
`rawnode_cgo.go:187-193` does not classify this sentinel as fatal when it
escapes `InitialState`, `FirstIndex`, or `LastIndex`.

Ready, Status, or another no-error Go method may still panic because its
wrapper panics on any returned error, but the C node is not sticky afterward.
That still differs from the Phase 19 terminal policy.

#### Minimal differential reproduction

A temporary backend-neutral probe:

1. restored voters `[1,2]` at snapshot index/term 1;
2. configured `Storage.Term` to return
   `ErrSnapshotTemporarilyUnavailable`;
3. stepped a valid `MsgVote` that requires a last-term lookup;
4. captured panic and returned error.

Observed result:

```text
Go backend:
    panic=true
    returned error=<nil>

cgo_raft backend:
    panic=false
    returned error="snapshot is temporarily unavailable:
                    snapshot is temporarily unavailable"
```

The temporary probe was deleted immediately after confirmation.

#### Why this is not already fixed

Phase 19 correctly stated that storage results must be classified by call
site, but the implemented mapper retained a globally retryable snapshot
sentinel. Existing parity tests use an arbitrary unexpected `Term` error,
which maps to `RAFT_ERR_FATAL`; they do not use the otherwise recognizable
snapshot-temporary sentinel from a non-Snapshot callback.

#### Fix direction for the next phase

Do not redesign the storage bridge. The smallest compatible policy is:

- allow `RAFT_ERR_SNAPSHOT_TEMPORARILY_UNAVAILABLE` to remain retryable only
  when the actual Snapshot send path consumes it;
- make the result terminal when it escapes any other stateful path;
- preserve Bootstrap's special pre-mutation `LastIndex` rule, under which Go
  returns any Storage error;
- make constructor occurrences panic;
- verify RawNode, Node, Ready, constructor, repeated failure, and the valid
  Snapshot retry path.

No fix was made in Phase 29.

### C2 — Demoted leader drops proposals that Go accepts

**Classification:** CONFIRMED

**Severity:** Medium compatibility/liveness impact in a configuration edge
state

**Affected areas:** configuration changes and leader proposal admission

#### Go v3.7.0 behavior

When a leader is removed or demoted, `switchToConfig` detects the missing or
learner self Progress at `raft.go:1987-2005`. With
`StepDownOnRemoval=false`, it returns without stepping down, so the node
remains `StateLeader`.

The subsequent proposal admission test is:

```go
if r.trk.Progress[r.id] == nil {
    return ErrProposalDropped
}
```

at `raft.go:1294-1303`.

A demoted node still has a Progress entry, now marked `IsLearner=true`.
Consequently, the reference leader accepts and appends the proposal.

This behavior is unusual, but it is the actual v3.7.0 public behavior and is
reachable through valid `ApplyConfChange` plus the default false
`StepDownOnRemoval` setting.

#### Current C behavior

C correctly leaves the demoted leader in `StateLeader` when
`step_down_on_removal` is false at `c/src/raft_core.c:2653-2660`.

However, proposal admission tests voter membership:

```c
if (!core_is_voter(raft, raft->id) ||
    raft->lead_transferee != RAFT_NONE) {
    return RAFT_ERR_PROPOSAL_DROPPED;
}
```

at `c/src/raft_core.c:2074-2082`.

`core_is_voter` calls `raft_tracker_is_voter` at
`c/src/raft_core.c:432-434`. A learner is not a voter, even though its
Progress exists. C therefore drops a proposal that Go accepts.

#### Minimal differential reproduction

A temporary backend-neutral probe:

1. restored a two-voter configuration `[1,2]`;
2. elected node 1;
3. applied `ConfChangeAddLearnerNode` for node 1 with
   `StepDownOnRemoval=false`;
4. verified both backends still reported `StateLeader`;
5. called `RawNode.Propose`.

Observed result:

```text
Go backend:
    ConfState voters=[2] learners=[1]
    state=StateLeader
    Propose error=<nil>

cgo_raft backend:
    ConfState voters=[2] learners=[1]
    state=StateLeader
    Propose error=raft proposal dropped
```

The temporary probe was deleted immediately after confirmation.

#### Why this is not already fixed

Phase 8 tested leader removal and intentionally dropped proposals when the
local Progress was absent. It described the check as voter membership and did
not distinguish removal from demotion. Phase 27 preserved the existing
membership-drop ordering but audited zero entries, not this Progress-versus-
voter predicate.

#### Fix direction

The later fix should mirror the reference's exact admission predicate:

- a missing local Progress drops the proposal;
- a retained learner Progress does not, by itself, drop it;
- leadership transfer and all existing proposal-size/configuration checks
  remain unchanged;
- `StepDownOnRemoval=true` still steps down and follows follower semantics.

No fix was made in Phase 29.

## Other candidate classifications

### False positives

| ID | Candidate | Evidence | Classification |
| --- | --- | --- | --- |
| F1 | C does not perform Go's non-follower snapshot defense transition | Every supported Step route makes candidate/pre-candidate a follower before restore; higher-term leader messages also step down; same-term leader `MsgSnap` never calls restore. The differing private defense branch is unreachable through supported dispatch. | FALSE POSITIVE |
| F2 | C HasReady omits Go's explicit nonempty-HardState guard | A reachable Raft HardState cannot transition from a previously observed nonempty state back to the empty state. Both predicates agree on every reachable state. | FALSE POSITIVE |
| F3 | `MsgStorageError` is unimplemented | v3.7.0 defines no `MsgStorageError` enum value. Storage work contains only Append/AppendResp and Apply/ApplyResp. | FALSE POSITIVE / NOT APPLICABLE |

### Intentional and documented differences

| ID | Difference | Classification/rationale |
| --- | --- | --- |
| I1 | Native C result codes, Go wrapper errors/panics, sticky C OOM/fatal results | INTENTIONAL language/ABI error policy |
| I2 | Stricter reserved-ID, enum, nil-descriptor, duplicate-Bootstrap-peer, and `uint32_t` tick validation | INTENTIONAL public C boundary validation |
| I3 | Original protobuf wire order and non-canonical known-field encoding are not retained; future/invalid enum numbers and nil repeated message elements are unsupported | DOCUMENTED C ABI LIMITATION from Phase 24 |
| I4 | Ready, Status, Progress, entries, messages, and snapshots are deep independent copies rather than aliases into live Go objects | INTENTIONAL ownership model |
| I5 | Election PRNG algorithm and seed stream differ while range and reset lifecycle match | INTENTIONAL implementation difference |
| I6 | Logger and TraceLogger callbacks are not bridged; cgo `with_tla` builds do not emit Go internal trace events | DOCUMENTED diagnostic limitation |
| I7 | Direct cgo RawNode has no public Close and uses a finalizer fallback; Node-owned RawNode destruction is deterministic | DOCUMENTED lifecycle limitation |
| I8 | Node scheduling/channels remain Go-owned, and outer async storage work messages are application-worker work rather than core input | INTENTIONAL architecture |

The native Go `NewRawNode` historically panics for invalid Config values while
the cgo wrapper returns validation errors from its error-returning constructor.
This is included in I1/I2 because the binding/error documents explicitly
selected error returns for public validation and constructor panics for
terminal failures. It is not counted as a new consensus compatibility gap.

### Needs verification

| ID | Candidate | Evidence | Classification |
| --- | --- | --- | --- |
| V1 | `core_handle_vote_response` requires a current Progress before recording a vote, while Go core vote bookkeeping is independent | Public RawNode/Node response filtering already rejects a response from an absent peer in both backends. The only possible distinction is a delayed internal self response stepped while self Progress is temporarily absent, followed by re-addition in the same campaign. Normal `hup` refuses to campaign with unapplied configuration changes, and no supported normal-operation reproducer was established. | NEEDS VERIFICATION |

V1 is not a confirmed Phase 21 regression. It should not be implemented
without a supported reproducer.

## Remaining compatibility matrix

| Area | Candidate/result | Evidence | Status | Phase |
| --- | --- | --- | --- | --- |
| RawNode | unexpected snapshot-temporary storage result may return instead of panic | `rawnode_cgo.go:223-236` | **CONFIRMED** | Recommended Phase 30 |
| Node | same result can be ignored by async receive loop | `node.go:399-403` | **CONFIRMED**, part of C1 | Recommended Phase 30 |
| Election | normal vote/PreVote/timeout behavior matches; private response guard remains uncertain | public filters plus `core_handle_vote_response` | NEEDS VERIFICATION | none |
| State transition | self-demoted leader remains leader in both backends when configured | Go `raft.go:1993-2005`; C `raft_core.c:2653-2660` | NO ISSUE by itself | Phase 8 |
| Progress | reset, ownership, inflights, stale responses match | current tracker/core plus parity tests | ALREADY FIXED | 17, 18, 21, 22, 26 |
| Replication | append, rejection, conflict term, state transitions, flow control match | current `stepLeader`/C equivalents | NO NEW ISSUE | 17, 22, 26 |
| Storage | snapshot-temporary sentinel is globally mapped instead of callback-scoped | `errors_cgo.go:79-90` | **CONFIRMED** | Recommended Phase 30 |
| Ready | successful Ready/Advance semantics match; same sentinel may be non-sticky on a failing Ready | RawNode Ready builder/latch | **CONFIRMED**, part of C1 | Recommended Phase 30 |
| Snapshot | actual Snapshot temporary-unavailable retry path matches | `raft_core.c:732-735` | NO ISSUE | Phase 9 |
| Configuration | learner leader proposal predicate differs | Go Progress presence versus C voter membership | **CONFIRMED** | later follow-up |
| Status | all public fields, including inflights and v3.7 AutoLeave shape, match | current conversion/tracker snapshot | ALREADY FIXED | 18, 22, 28 |
| Errors | most fatal classifications match; one sentinel escapes | terminal set excludes snapshot temporary | **CONFIRMED**, part of C1 | Recommended Phase 30 |
| cgo lifetime | KeepAlive and deterministic Node destruction remain correct | all current `rn.p` paths re-audited | ALREADY FIXED | 20, 25 |
| protobuf | supported object fidelity matches; raw-wire exclusions remain | Phase 24 metadata paths | INTENTIONAL LIMITATION | 24 |
| ABI | required RawNode functions exist; validation/error representation differs intentionally | `c/include/raft/raft.h` | INTENTIONAL DIFFERENCE | 2, 4 |
| Message routing | outer storage work rejected; responses step back into Raft | `raft_core.c:2799-2802` | INTENTIONAL DIFFERENCE | 12, 13 |

## TODO, stub, and NOT_IMPLEMENTED audit

The C implementation contains no production `TODO` or `FIXME`.

The only production `RAFT_ERR_NOT_IMPLEMENTED` return is:

```text
c/src/raft_core.c:2799-2802
```

for directly stepping outer `MsgStorageAppend` or `MsgStorageApply` work into
the core. This is intentional routing protection, not a missing subsystem.
Those messages are consumed by application storage workers; only their
responses return to Raft.

Other search results:

- `node.go` contains unchanged upstream TODO comments, not C stubs.
- `errors_cgo.go` retains the ABI mapping for
  `RAFT_ERR_NOT_IMPLEMENTED`.
- `rawnode_cgo.go` has an obsolete comment saying “remaining unsupported
  features”; no RawNode public operation is still a skeleton.
- `c/include/raft/raft.h:63-65` describes NOT_IMPLEMENTED as an unported
  subsystem result. The current use is routing protection, so this is an
  obsolete comment, not an observable semantic gap.
- test names containing “unsupported” exercise the formerly unsupported
  configurations and intentional storage-routing protection.

No genuine unsupported public RawNode operation was found.

## Suspicious divergence patterns checked

| Pattern | Result |
| --- | --- |
| silent storage error conversion | One confirmed snapshot-temporary classification gap; other expected/fatal storage classes match |
| clamping invalid values | heartbeat commit clamp was removed in Phase 23; remaining bounded send-side heartbeat commit matches Go |
| ignored Step results | Node is safe for terminal-latched errors except C1 |
| nil versus empty | supported protobuf and proposal cases match; unsupported nil repeated elements remain documented |
| absent versus present scalar | supported protobuf metadata matches after Phase 24 |
| state retained too long | Progress, votes, pending config, read-only, transfer, and Node ownership match; C2 is an admission predicate, not stale state |
| state reset too aggressively | no remaining Progress reset mismatch found |
| copied public Status aliases | C returns independent snapshots; no shared mutable C state escapes |
| lossy conversion | no supported object-level loss found; raw-wire limitations remain intentional |
| return where Go panics | C1 confirmed; invalid public boundary values are intentional; invariant paths otherwise map to fatal panic |
| panic where Go returns | no new supported normal-operation case found |

## Differential validation

### Temporary differential probes

Two minimal backend-neutral probes were created, run against both backends,
and deleted:

1. non-Snapshot `ErrSnapshotTemporarilyUnavailable` from `Storage.Term`;
2. proposal after self-demotion with `StepDownOnRemoval=false`.

They produced the distinguishing results documented under C1 and C2.

No temporary test file remains.

### Test and tool results

All existing validation executed during Phase 29 passed:

| Validation | Result |
| --- | --- |
| `make test-c` | PASS; all 9 native C binaries |
| `go test -count=1 ./...` | PASS |
| `CGO_ENABLED=1 go test -count=1 -tags=cgo_raft ./...` | PASS; `rafttest` has no tagged tests |
| default `go test -race -count=1 ./...` | PASS |
| cgo `go test -race -count=1 -tags=cgo_raft ./...` | PASS |
| `GOEXPERIMENT=cgocheck2` cgo suite | PASS |
| native ASan + UBSan + leak detection (`make test-c-sanitize`) | PASS |
| cgo `go test -asan` + leak detection | PASS |
| cgo UBSan-instrumented suite | PASS |
| Valgrind full error/leak checking for all 9 native binaries | PASS |
| default `with_tla` suite | PASS |
| cgo `cgo_raft with_tla` suite | PASS |
| strict Clang C11 clean build and tests | PASS |
| strict GCC C11 clean build and tests | PASS |
| `make verify-gofmt` | PASS |
| whitespace/diff checks | PASS |

The green suites do not invalidate C1 or C2:

- existing fatal-storage parity tests use an arbitrary error, which maps to
  `RAFT_ERR_FATAL`, rather than the mis-scoped known sentinel;
- existing leader-removal tests remove self Progress, while C2 requires
  demotion that retains learner Progress;
- much of the original interaction suite and `rafttest` remains build-excluded
  under `cgo_raft`.

## Areas with no new mismatch

No new mismatch was found in:

- ordinary initialization and valid Storage restoration;
- normal follower/candidate/pre-candidate/leader transitions;
- randomized election timeout range/reset lifecycle;
- vote and PreVote lease handling;
- check-quorum and leader-transfer timeout;
- normal proposal, forwarding, configuration proposal, and size accounting;
- append replication and conflict handling;
- snapshot send/restore/report and temporary Snapshot retry;
- safe and lease ReadIndex;
- Ready ordering, persistence gating, and successful Advance;
- async storage work construction and successful response handling;
- simple/joint configuration changes, auto-leave, learners, and restore;
- BasicStatus, Status Config, Progress, and inflights;
- supported protobuf object fidelity;
- cgo pointer lifetimes and Node destruction;
- ownership cleanup, leaks, races, and sanitizer behavior;
- required public C RawNode operations.

## Recommended next phase

Select exactly:

```text
PHASE 30: Storage Error Domain Classification —
          ErrSnapshotTemporarilyUnavailable
```

Recommended scope:

1. verify every callback/call-site combination again;
2. preserve retry only for an actual `Storage.Snapshot` call;
3. preserve Bootstrap's pre-mutation “return any LastIndex error” behavior;
4. make constructor occurrences panic;
5. make escaping stateful occurrences terminal and sticky;
6. verify Node cannot ignore them;
7. add backend-neutral coverage for InitialState, Term, Entries, FirstIndex,
   LastIndex, Snapshot, Bootstrap, Ready, RawNode Step, and Node Step;
8. do not address C2 in the same phase.

This recommendation ranks ahead of C2 because:

- C1 can cause a storage failure to disappear through Node;
- it violates the central Phase 19 safety model;
- it affects several public paths;
- the failure class is precisely identified;
- the fix should remain localized to result classification and constructor
  policy.

After Phase 30, the confirmed learner-leader proposal predicate should be
addressed separately.

## Audit limitations

1. This was a systematic source and targeted differential audit, not an
   exhaustive state-space proof.
2. The cgo build excludes much of the upstream internal interaction suite and
   all tagged `rafttest` tests.
3. Randomized election sequences cannot be compared byte-for-byte because the
   PRNG stream intentionally differs.
4. cgo `with_tla` builds compile and test but do not emit Go's internal trace
   callback stream.
5. The differential probes focused on high-confidence source divergences; no
   broad random differential fuzzer was added.
6. V1 remains explicitly unconfirmed rather than being promoted without a
   supported reproduction.

## Final verdict

```text
The compatibility audit still has two known genuine gaps.

Recommended next task:
PHASE 30 — scope ErrSnapshotTemporarilyUnavailable to Storage.Snapshot
and restore Phase 19 fatal propagation everywhere else.
```

Phase 29 made no production, test, ABI, or build change.
