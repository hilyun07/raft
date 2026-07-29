# Phase 7 C Raft Core Minimal Result

## Outcome

Phase 7 adds the first working C raft state machine behind the opt-in
`cgo_raft` RawNode. The default build remains pure Go. The C backend now
supports a deliberately limited synchronous subset:

- Storage-backed initialization with HardState, ConfState, and
  `Config.Applied`;
- follower, candidate, and leader transitions;
- deterministic election and heartbeat ticking;
- simple-voter elections and vote counting;
- leader no-op append, normal proposals, follower proposal forwarding, and
  the configured uncommitted-payload bound;
- basic append/append-response and heartbeat/heartbeat-response replication;
- bootstrap for simple initial voter sets;
- meaningful Ready/preview/accept/Advance behavior;
- BasicStatus, Status, HasProgress, and copied progress snapshots;
- public RawNode Step validation separated from the Go Node internal Step
  path.

This is not yet a full replacement for the Go v3.7.0 core. Advanced features
listed below remain explicit unsupported operations.

## Inputs inspected

The Phase 7 instructions and `prompts/RAFT_C_PORTING_SPEC.md` were read.
All result/planning files requested by the Phase 7 prompt were present under
`OUTPUT/` and inspected:

- `PORTING_BASELINE.md`;
- both Phase 1 plan/result files;
- the sentinel, RawNode parity, Config, raftpb wire, Ready ownership, error,
  and gap documents;
- the byte/type/pointer-safety correction results;
- the WithProgress, transfer-leadership, and Step-layering results;
- the Phase 2, Phase 4, Phase 5, and Phase 6 result files.

The relevant Go v3.7.0 paths were also inspected: `raft.go`, `rawnode.go`,
`node.go`, `log.go`, `storage.go`, tracker/quorum types, and basic election,
Step, Ready, and storage tests. No requested document was missing; the porting
spec lives in `prompts/` rather than the repository root.

## C files created

| File | Purpose |
| --- | --- |
| `c/src/raft_core.h` | Private minimal `raft_t`, simple progress records, and core entry points. |
| `c/src/raft_core.c` | State transitions, timers, vote/quorum logic, append/heartbeat handling, bootstrap, message ownership, and state snapshots. |
| `c/tests/raft_core_test.c` | Focused Phase 7 initialization, election, vote, proposal, append, heartbeat, Ready, Step-layering, progress-copy, and unsupported-config tests. |

## Files updated

Implementation/build:

- `c/src/raft_internal.h`;
- `c/src/raw_node.c`;
- `c/include/raft/raft.h`;
- `c/Makefile`;
- repository `Makefile`;
- `rawnode_cgo_bridge.c`;
- `rawnode_cgo.go`.

Tests:

- `c/tests/raw_node_skeleton_test.c`;
- `rawnode_cgo_test.go`;
- `storage_bridge_cgo_test.go`.

Documentation:

- `OUTPUT/RAWNODE_API_PARITY_FOR_C_PORT.md`;
- `OUTPUT/READY_OWNERSHIP_FOR_C_PORT.md`;
- `OUTPUT/CONFIG_FIELDS_FOR_C_PORT.md`;
- `OUTPUT/ERROR_MAPPING_FOR_C_PORT.md`;
- `OUTPUT/C_PORT_TRACKING_GAPS.md`;
- Phase 2, Phase 4, Phase 5, and Phase 6 result files;
- `OUTPUT/WITH_PROGRESS_C_PORT_AUDIT_RESULT.md`;
- `OUTPUT/STEP_FOR_NODE_LAYERING_DOC_UPDATE_RESULT.md`.

The private RawNode ABI marker advances from 10 to 11 because the opaque
object now owns a raft core and Ready completion state.

## Minimal raft state

The private `raft_t` owns:

- ID, term, vote, known leader, lead transferee, and role;
- a pointer to the Phase 6 `raft_log_t`;
- a deep-copied simple ConfState;
- a sorted C-owned progress array with Match, Next, state, learner/activity
  scalars, and election vote records;
- a recursively C-owned outbound message vector;
- election/heartbeat elapsed and timeout fields;
- message, uncommitted-payload, and proposal-forwarding configuration;
- a sticky error for failures reached through the void Tick API.

RawNode retains the normalized complete C Config, Storage callback table,
previous SoftState/HardState, Ready generation, accepted-Ready flag, and
stable/apply completion metadata.

No view pointer or Go pointer is retained. Incoming `*_view_t` values are
call-scoped; proposals/messages retained by C are recursively copied into
owned non-view values.

## Initialization and configuration

`raft_raw_node_new` now:

1. performs the existing public Config and callback-table validation;
2. rejects `PreVote`, `CheckQuorum`, lease reads, and
   `AsyncStorageWrites` with `RAFT_ERR_NOT_IMPLEMENTED`;
3. normalizes the documented zero size limits;
4. initializes the Phase 6 log;
5. calls Storage `InitialState`;
6. validates and deep-copies its simple ConfState;
7. loads term, vote, and commit;
8. applies `Config.Applied`;
9. starts as a follower with `lead = RAFT_NONE`;
10. initializes previous state for Ready delta detection.

Joint ConfState (`VotersOutgoing`, `LearnersNext`, or `AutoLeave`) is rejected
as not implemented. Reserved/duplicate membership IDs and invalid HardState
commit ranges fail without entering the core.

## State transitions and election behavior

The core implements minimal equivalents of:

- become follower: reset timers/progress, update term when needed, clear the
  known leader unless supplied;
- become candidate: increment term, vote for self, and record the self vote;
- become leader: reset progress, set self Match/Next to the log tail, and
  append a current-term empty no-op.

`MsgHup` starts a campaign for promotable voters. A single voter becomes
leader immediately. Multi-voter candidates send `MsgVote`, count one response
per voter, become leader at a simple majority, and return to follower after a
majority rejection.

Election timeout is currently deterministic and equal to `ElectionTick`.
Randomized election timeout parity is deferred.

## Implemented message behavior

The minimal core handles:

- `MsgHup`;
- `MsgBeat`;
- `MsgProp`;
- `MsgVote` and `MsgVoteResp`;
- `MsgApp` and `MsgAppResp`;
- `MsgHeartbeat` and `MsgHeartbeatResp`;
- `MsgForgetLeader`.

Vote grant checks term/vote availability and `raft_log_is_up_to_date`.
Append uses Phase 6 `maybe_append`; rejection includes a basic conflict hint.
Follower commit is bounded by available log state. Leader commit uses a simple
voter majority and requires an entry from the current term.

Normal proposals are deep-copied, assigned the current term and consecutive
indexes, appended to unstable storage, and broadcast. A leaderless follower,
or a follower with forwarding disabled, returns
`RAFT_ERR_PROPOSAL_DROPPED`. A follower with a known leader forwards one
owned `MsgProp`. The basic `MaxUncommittedEntriesSize` payload check is active.

## Public Step and Node Step

The required call direction is implemented:

```text
raft_raw_node_step
  -> validate local-message/public unknown-response rules
  -> raft_raw_node_step_for_node
       -> raft_core_step
```

`raft_raw_node_step_for_node` never calls the public function. It therefore
accepts Node-routed local `MsgHup`, while the public path rejects the same
ordinary local-origin shape with `RAFT_ERR_STEP_LOCAL_MSG`. Public response
messages from unknown non-local peers return
`RAFT_ERR_STEP_PEER_NOT_FOUND_OR_IGNORED`.

## Bootstrap

Bootstrap now supports non-empty, unique, real-ID simple voter arrays on empty
storage. It:

- enters term 1 as a follower;
- creates legacy ConfChange entries with `Peer.Context` nil/present-empty
  preservation;
- appends and commits those entries;
- installs simple voter progress immediately.

Full configuration-change application is not implied by bootstrap support.

## Ready and Advance

Ready now returns C-owned copies of:

- changed SoftState;
- changed HardState;
- unstable entries and snapshot, when present at the log layer;
- next committed entries;
- queued outbound messages.

`MustSync` is true for unstable entries or term/vote changes. A commit-only
HardState change does not force sync.

`ready_without_accept` previews using a generation token. `accept_ready`
updates previous states, marks unstable/applying work in progress, drains the
message queue, and stores completion metadata inside RawNode. The returned
Ready graph may then be destroyed. The no-argument `advance` completes
stable/snapshot/applied progress and uncommitted-size accounting without any
Go-allocated Ready descriptor crossing back into C.

Only one accepted Ready may be outstanding. AsyncStorageWrites remains
unsupported.

## Progress and quorum

The minimal tracker is sufficient for simple configurations:

- majority is `voter_count/2 + 1`;
- each member has Match and Next;
- learners are recognized and excluded from election/commit quorum;
- local membership and progress snapshots are accurate;
- Status includes progress only for leaders;
- WithProgress snapshots work on every role.

`raft_raw_node_progress_snapshot` allocates a new row array. Mutating/freeing
that array cannot alter core state. No inflights pointer, live progress pointer,
or C-to-Go visitor callback exists.

Full tracker probe throttling, inflights, max-inflight bytes/messages,
optimistic pipelining, joint quorums, learner promotion, and snapshot progress
remain deferred.

## Error behavior

Phase 7 preserves:

- specific storage callback errors;
- `RAFT_ERR_STEP_LOCAL_MSG`;
- `RAFT_ERR_STEP_PEER_NOT_FOUND_OR_IGNORED`;
- `RAFT_ERR_PROPOSAL_DROPPED`;
- `RAFT_ERR_INVALID_ARGUMENT`;
- `RAFT_ERR_OUT_OF_MEMORY`;
- fatal invariant/callback failures;
- `RAFT_ERR_NOT_IMPLEMENTED` for unsupported advanced semantics.

The void Tick wrapper stores a failure in the core. `HasReady` then reports
work and the next Ready/error-returning operation exposes the sticky error,
rather than silently losing a storage/allocation failure.

## Tests added or updated

The C tests cover:

- reserved Config IDs and InitialState term/vote/commit loading;
- follower/no-leader initialization and copy-isolated progress snapshots;
- election by Campaign and Tick;
- single-voter leadership/no-op;
- multi-voter vote requests and quorum leadership;
- vote grant, duplicate-voter rejection, stale-term rejection, stale-log
  rejection, and higher-term stepdown;
- leader proposals, immediate single-voter commit, proposal forwarding, and
  uncommitted-size dropping;
- matching and mismatching append;
- follower commit advancement and append responses;
- leader quorum commit after append response;
- heartbeat tick, leader observation, and heartbeat response;
- Ready entries, committed entries, messages, HardState, SoftState, and
  `MustSync`;
- commit-only `MustSync == false`;
- preview/accept/no-argument Advance;
- public/internal Step distinction and unknown-response filtering;
- HasProgress, progress type/state, and snapshot copy isolation;
- explicit rejection of unsupported advanced Config modes.

The tagged Go test persists C Ready entries/HardState to `MemoryStorage`,
advances, verifies leader Status, and verifies Go-side WithProgress visitation
with `Inflights == nil`.

## Validation results

All commands below passed on 2026-07-30.

Normal strict C build/tests:

```text
make test-c
```

This compiled the four C tests with C11, `-Wall -Wextra -Werror -Wpedantic`
and ran:

- `raw_node_skeleton_test`;
- `raft_core_test`;
- `unstable_test`;
- `log_test`.

Sanitizers:

```text
make test-c-sanitize
```

This rebuilt and ran all four binaries with:

```text
-O1 -g -fno-omit-frame-pointer -fsanitize=address,undefined
ASAN_OPTIONS=detect_leaks=1:halt_on_error=1
UBSAN_OPTIONS=halt_on_error=1:print_stacktrace=1
```

The sanitizer runtime is now correctly installed/configured. Compilation,
linking, and execution all passed. There were no AddressSanitizer,
LeakSanitizer, or UndefinedBehaviorSanitizer findings.

C-backed tagged Go suite:

```text
TMPDIR=$PWD/.tmp GOTMPDIR=$PWD/.tmp GOCACHE=$PWD/.gocache \
make test-cgo-raft
```

Passed for all packages.

Extended cgo pointer check:

```text
TMPDIR=$PWD/.tmp GOTMPDIR=$PWD/.tmp GOCACHE=$PWD/.gocache \
CGO_ENABLED=1 GOEXPERIMENT=cgocheck2 go test -tags=cgo_raft ./...
```

Passed for all packages.

Default pure-Go suite:

```text
TMPDIR=$PWD/.tmp GOTMPDIR=$PWD/.tmp GOCACHE=$PWD/.gocache \
go test ./...
```

Passed for all packages; the default path did not compile or link C.

Independent memory/compiler checks:

```text
valgrind --quiet --error-exitcode=99 --leak-check=full <each C test>
make -C c clean test CC=clang
git diff --check
```

All four binaries passed Valgrind with no reported leaks/errors. The complete
C suite passed Clang's warning-as-error build. `git diff --check` passed.

## Remaining gaps

The following remain outside this minimal phase:

- randomized election timeout parity;
- full tracker state machine, inflights, flow control, optimistic
  replication, and max-inflight enforcement;
- joint consensus, ConfChange/ConfChangeV2 proposal/application, learner
  promotion/removal, and zero-NodeID cancellation application semantics;
- ReadIndex and read-only safe/lease protocols;
- CheckQuorum and PreVote;
- leadership transfer completion;
- snapshot receive/restore/send and snapshot-status progress;
- ReportUnreachable behavior;
- AsyncStorageWrites and local append/apply response protocol;
- TraceLogger/`with_tla`;
- exhaustive raftpb optional scalar presence;
- allocation-failure injection and a broad Go-vs-C differential harness.

`MsgSnap`, pre-vote, read-index, transfer, snapshot-status/unreachable, and
async-storage message paths return `RAFT_ERR_NOT_IMPLEMENTED` rather than
silently approximating their semantics. Append fallback to snapshot is also
deferred.

## Differential follow-up

A later phase should compare ordered Go/C traces for:

- single-node bootstrap/election/propose/Ready/Advance;
- three-node vote and append quorum;
- stale vote and append conflict/rejection hints;
- HardState/SoftState/Ready equivalence;
- proposal-size accounting;
- randomized timeout and progress-state transitions.
