# Phase 8 C Tracker, Quorum, and Configuration Change Result

## Outcome

Phase 8 adds multi-node progress tracking, majority and joint quorum
calculation, and etcd-compatible membership changes to the C-backed RawNode.
The Go `RawNode` API remains unchanged, and the default build remains pure Go.

The C backend now supports:

- a C-owned progress tracker with probe, replicate, and snapshot progress
  states;
- per-peer inflight message and byte accounting;
- voter-only majority commit calculation;
- majority and joint vote results;
- incoming/outgoing voter configurations;
- learners and staged `LearnersNext`;
- simple and joint `ConfChangeV2` application;
- legacy `ConfChange` proposals with `EntryConfChange` wire compatibility;
- pending-configuration-change proposal validation;
- automatic exit from implicit joint configurations;
- configuration restore from `Storage.InitialState`;
- learner promotion, voter demotion, removal, and zero-NodeID no-op
  compatibility;
- `StepDownOnRemoval` and proposal dropping by a removed leader;
- tracker-backed Status, WithProgress, HasProgress, elections, replication,
  and commit decisions.

ReadIndex, CheckQuorum, PreVote, snapshot transmission/restoration messages,
leadership transfer, ReportUnreachable, and AsyncStorageWrites remain outside
this phase and continue to return explicit unsupported errors.

## Architecture after Phase 8

The active C-backed path remains:

```text
Go RawNode API
  -> rawnode_cgo.go
  -> temporary pinned Go/C input descriptors
  -> public C RawNode API
  -> C raft core
  -> C tracker/quorum/confchange/log/unstable modules
```

The C RawNode remains opaque. No live tracker pointer, progress pointer,
inflight buffer, or Go callback is exposed through the public Go API.
`WithProgress` and Status use copied C-owned snapshots.

The default `!cgo_raft` build continues to compile and use the original Go
implementation.

## Files added

| File | Purpose |
| --- | --- |
| `c/src/tracker.h` | Private progress, inflight, tracker, vote, quorum, and ID-set interfaces. |
| `c/src/tracker.c` | Progress state transitions, inflight windows, sorted progress/config storage, majority/joint votes, commit calculation, and copied snapshots. |
| `c/src/confchange.h` | Private configuration-change codec, restore, invariant, and application interfaces. |
| `c/src/confchange.c` | ConfChange protobuf handling and transactional simple/joint changer semantics. |
| `c/tests/tracker_test.c` | Majority/joint quorum, learner exclusion, progress transition, and inflight tests. |
| `c/tests/confchange_test.c` | Simple/joint change, rollback, zero-ID, learner demotion, and protobuf tests. |

## Files updated

Implementation and ABI:

- `c/include/raft/raft.h`;
- `c/src/raft_internal.h`;
- `c/src/raft_core.h`;
- `c/src/raft_core.c`;
- `c/src/raw_node.c`;
- `c/Makefile`;
- `rawnode_cgo_bridge.c`;
- `convert_cgo.go`;
- `rawnode_cgo.go`.

Tests:

- `c/tests/raw_node_skeleton_test.c`;
- `rawnode_cgo_test.go`.

The private RawNode ABI marker advances from 11 to 12 because the opaque
object now owns the complete tracker and membership state.

## Tracker and progress

`raft_t` now owns one `raft_progress_tracker_t` instead of the Phase 7 simple
progress array and separate ConfState copy.

The tracker owns:

- sorted incoming and outgoing voter ID vectors;
- sorted learner and `LearnersNext` vectors;
- the `AutoLeave` flag;
- sorted progress records;
- maximum inflight message and byte limits.

Each progress record contains:

- Match and Next;
- probe, replicate, or snapshot state;
- pending snapshot and sent-commit state;
- recent activity and message-flow pause state;
- learner status;
- a dynamically grown ring of inflight append records;
- election vote bookkeeping.

Progress cloning is deep, including inflights. This is required because
configuration changes are prepared transactionally and must preserve
replication state when accepted or leave the live tracker unchanged when
rejected.

The append path now:

- pauses probes after a non-empty append;
- advances Next optimistically in replicate state;
- enforces inflight message and payload-byte limits;
- frees acknowledged inflights;
- sends a recovery empty append after a heartbeat response when inflights are
  full;
- pipelines additional size-limited append messages after acknowledgements;
- tracks the highest commit sent to each peer.

## Quorum behavior

Simple majority commit excludes learners and selects the largest Match
acknowledged by a quorum.

Joint commit is:

```text
min(committed(incoming voters), committed(outgoing voters))
```

Votes use the same incoming/outgoing rule:

- both majorities must win for a joint win;
- either majority losing makes the joint vote lose;
- otherwise the vote remains pending.

The candidate self-vote is evaluated through the joint tracker, so a
self-sufficient joint configuration can elect without relying on the older
simple-singleton shortcut.

Election requests are sent to the union of incoming and outgoing voters.
Learners do not vote and do not contribute to commit quorum.

## Configuration restore and invariants

Storage initialization now accepts simple and joint ConfState values.
Restored progress uses `max(lastIndex, 1)` for Next, matching Go
`confchange.Changer`.

The C changer checks:

- all member IDs are real node IDs;
- configuration vectors contain no duplicates;
- every configured voter or learner has progress;
- learners do not overlap either voter set;
- `LearnersNext` members remain voters in the outgoing configuration and are
  not marked as learners yet;
- non-joint configurations have no outgoing voters, staged learners, or
  `AutoLeave`;
- non-empty restored configurations contain incoming voters.

Restore constructs a new tracker and swaps it into place only after all
checks succeed.

## Simple and joint changes

Configuration application mirrors Go `confchange.Changer`:

- `AddNode` adds or promotes a voter;
- `AddLearnerNode` adds a learner or demotes a voter;
- `RemoveNode` removes from the incoming configuration and retains progress
  while the member remains an outgoing voter;
- `UpdateNode` has no tracker effect;
- NodeID zero is ignored for etcd cancellation compatibility;
- removing all incoming voters is rejected;
- a simple change may alter at most one incoming voter;
- multi-change or explicitly joint V2 changes enter joint consensus;
- empty automatic V2 changes leave joint consensus;
- implicit/automatic joint changes set `AutoLeave`;
- explicit joint changes require application-proposed exit.

Voter-to-learner demotion deliberately preserves Match, Next, progress state,
and inflights. If the member is still an outgoing voter, it is staged in
`LearnersNext`; leaving joint consensus then marks it as a learner.

Application is transactional. The live tracker is cloned, changed, checked,
and swapped only on success.

## Proposal behavior

The leader proposal path decodes configuration entries before appending them.
Unless `DisableConfChangeValidation` is set, it converts an invalid concurrent
configuration proposal into a nil normal entry when:

- another configuration change may still be unapplied;
- the configuration is joint and the proposal does not leave joint state;
- the configuration is not joint and the proposal tries to leave joint
  state.

Accepted proposals update `pending_conf_index` to their prospective log
index.

`becomeLeader` conservatively initializes `pending_conf_index` to the current
log tail, matching the Go implementation.

The C protobuf codec supports the fields needed by legacy ConfChange,
ConfChangeSingle, and ConfChangeV2. Optional scalar presence bits in the
borrowed C descriptors preserve Go protobuf output for explicitly present zero
values. Nil and present-empty byte values are also preserved.

The public C ABI retains the specification's V2 proposal entry point and adds
`raft_raw_node_propose_conf_change_v1` so the Go `ConfChangeI` wrapper does
not incorrectly turn a legacy change into `EntryConfChangeV2`. Legacy ID and
context are retained in the proposed bytes.

A NULL V2 proposal descriptor represents the nil-data empty V2 proposal used
by automatic joint exit.

## Applying and switching configurations

`raft_raw_node_apply_conf_change` now applies a V2 descriptor and returns a
deep C-owned ConfState.

After a successful tracker switch:

- a removed or demoted leader steps down when `StepDownOnRemoval` is enabled;
- a removed leader that remains leader cannot accept new proposals because
  it is no longer a voter;
- a leader recalculates commit under the new quorum;
- a changed commit is broadcast;
- otherwise newly added progress records are probed immediately;
- a removed or demoted leadership transferee is cleared.

When Advance applies the entry that established an implicit joint
configuration, the leader proposes a nil-data empty V2 change. Proposal
drops are intentionally retried on a later Advance, matching the Go
auto-leave behavior.

## Go/cgo bridge

The Go public API remains:

```go
func (rn *RawNode) ProposeConfChange(cc pb.ConfChangeI) error
func (rn *RawNode) ApplyConfChange(cc pb.ConfChangeI) *pb.ConfState
```

The proposal wrapper now distinguishes legacy V1 and V2 values and builds the
matching C descriptor.

The extended cgo pointer checker exposed that call-scoped descriptors were
storing unpinned Go payload pointers in C-allocated memory. The input arena
now uses `runtime.Pinner` for every non-empty borrowed byte or uint64 slice.
It frees all temporary C descriptors before unpinning. C still never retains
an input pointer after the semantic call.

The C amalgamation includes tracker and confchange before the raft core.
The standalone C build compiles the same sources as separate objects.

## Adaptations from `08_multinode_membership.txt`

The high-level requirements were followed: real multi-node tracking, quorum
commit, membership changes, learners, joint consensus, and Go API
compatibility now exist behind C RawNode.

Several early implementation details were adapted to the current repository:

- tracker and quorum remain private C core modules rather than becoming
  Go-visible data structures;
- quorum functions live with the tracker because the current core represents
  voter configurations as tracker-owned sorted vectors;
- configuration input continues to use the call-scoped descriptor ownership
  model established by Phases 2 and 4;
- Ready/Advance ownership and Storage callbacks were not redesigned;
- the existing opaque RawNode and build-tag architecture was preserved;
- a supplemental V1 proposal function was added instead of changing the
  specification's existing V2 C function;
- current unsupported subsystems were left explicit rather than being
  approximated as part of membership work.

No working subsystem was replaced solely to match the historical prompt's
proposed file or API layout.

## Tests added and updated

The C tracker tests cover:

- five-voter majority commit;
- learners excluded from commit;
- incoming/outgoing joint commit;
- joint vote pending, win, and loss;
- probe/replicate transitions;
- optimistic Next;
- inflight message and byte limits;
- acknowledgement freeing and rejection handling.

The C configuration tests cover:

- restore of a three-voter configuration;
- add learner and promote voter;
- transactional failure rollback;
- zero-NodeID no-op;
- joint voter demotion through `LearnersNext`;
- progress preservation across demotion and joint exit;
- V1 and V2 protobuf encode/decode.

The tagged Go tests cover:

- legacy proposal entry type, ID, context, and protobuf bytes;
- V2 optional scalar presence and protobuf bytes;
- one-pending-change conversion to a normal entry;
- add learner, promote, remove, and zero-ID application;
- explicit joint enter and leave;
- implicit joint AutoLeave and nil-data exit proposal;
- removed-leader proposal dropping and `StepDownOnRemoval`;
- restart from a joint Storage ConfState;
- copied progress types for voters, staged learners, and learners.

Existing Phase 7 lifecycle tests were updated to expect implemented
membership behavior rather than `RAFT_ERR_NOT_IMPLEMENTED`.

## Validation results

All commands below passed on 2026-07-30.

Strict C build and six-test suite:

```text
make test-c
```

Sanitizers:

```text
make test-c-sanitize
```

This used AddressSanitizer, LeakSanitizer, and UndefinedBehaviorSanitizer with
halt-on-error settings. No findings were reported.

Clang warning-as-error build:

```text
make -C c clean test CC=clang
```

Valgrind:

```text
valgrind --quiet --error-exitcode=99 --leak-check=full <each C test>
```

All six C test binaries passed.

C-backed Go suite:

```text
go test -tags=cgo_raft ./...
```

Extended cgo pointer checking:

```text
CGO_ENABLED=1 GOEXPERIMENT=cgocheck2 go test -tags=cgo_raft ./...
```

Race detector:

```text
go test -race -tags=cgo_raft ./...
```

Default pure-Go suite:

```text
go test ./...
```

`git diff --check` also passed.

## Remaining TODOs

Membership-specific follow-up:

- add a broad automated Go-vs-C trace differential harness for elections,
  rejection recovery, membership transitions, and Ready ordering;
- preserve protobuf unknown fields when a Go configuration object containing
  unknown wire data is re-proposed; known fields and optional scalar presence
  are preserved now;
- add allocation-failure injection for tracker clone, protobuf encoding, and
  ConfState output paths;
- extend long-running randomized multi-node membership tests and fuzz the C
  configuration-change decoder.

Broader porting work still pending:

- randomized election timeout parity;
- snapshot send/receive/restore and snapshot progress;
- CheckQuorum and PreVote;
- ReadIndex and read-only protocols;
- leadership transfer completion;
- ReportUnreachable and ReportSnapshot;
- AsyncStorageWrites and local append/apply threads;
- TraceLogger/`with_tla`.

Snapshot progress state exists in the tracker, but snapshot message behavior
is still explicitly unsupported. CheckQuorum can later reuse
`raft_tracker_quorum_active`; the Config option remains rejected until the
full protocol is ported.

## Notes for the next session

- The tracker and changer own all membership memory. Do not reintroduce a
  second ConfState or progress array in `raft_t`.
- Configuration changes must continue through the transactional clone/check/
  swap path.
- `LearnersNext` progress must remain non-learner until joint exit.
- A legacy Go `ConfChange` must continue to produce `EntryConfChange`, not V2.
- Keep the cgo arena pins alive until after the single semantic C call and
  free C descriptors before unpinning.
- Joint elections and commits must use both voter sets; never infer voter
  status solely from `progress.is_learner`.
- The application, not RawNode, remains responsible for calling
  `ApplyConfChange` only when the corresponding committed entry is applied.
