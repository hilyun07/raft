# Phase 23: Invalid Input and Invariant Compatibility

## Result

Both audit findings were independently confirmed against the current source
and fixed:

1. malformed `ConfState` values could retain an ID in both incoming
   `Voters` and `LearnersNext`; and
2. follower heartbeat handling clamped `Commit` to `lastIndex` instead of
   treating `Commit > lastIndex` as an invariant violation.

The fixes are limited to those two cases. No other final-audit finding was
addressed.

## Compatibility reference

The authoritative reference is the signed `etcd-io/raft` tag:

```text
v3.7.0
b867cf13f6bc0dae21204302df97bc2355c3af55
```

The tag's merge base with the current branch is the same commit.

## Finding A: malformed ConfState

### Go v3.7.0 behavior

Go does not validate a restored `ConfState` with one standalone validator.
The effective validation is the combination of:

1. `confchange.toConfChangeSingle` and `confchange.Restore`
   (`confchange/restore.go:26-155`) reconstructing the configuration through
   ordinary configuration-change operations;
2. the lower-level tracker/configuration invariants in
   `confchange/confchange.go`; and
3. `assertConfStatesEquivalent` comparing the input with the reconstructed
   canonical state at initial construction and snapshot restore
   (`raft.go:471-479`, `raft.go:1921-1936`, `util.go:320-325`).

`LearnersNext` represents a voter that remains in the outgoing configuration
but has already been removed from the incoming configuration. During restore,
Go applies `AddLearnerNode` for every `LearnersNext` ID. `makeLearner` removes
that ID from incoming voters and stages it in `LearnersNext` while it remains
outgoing.

Consequently, an input such as:

```text
Voters:         [1, 2, 3]
VotersOutgoing: [1, 2]
LearnersNext:   [2]
```

reconstructs with peer 2 absent from incoming `Voters`. The reconstructed
state is not equivalent to the input, so Raft construction or snapshot
restore panics. With no other incoming voter, reconstruction can instead fail
earlier because it would remove all voters; the externally observable result
is still a panic.

Duplicates within one repeated field are also rejected by this effective
validation: reconstruction uses maps and deduplicates the ID, after which
`ConfState.Equivalent` detects that the repeated input differs. Voter/learner
overlap, learner/outgoing-voter overlap, and a `LearnersNext` ID absent from
outgoing voters are likewise rejected through reconstruction and/or the
configuration invariants.

Incoming/outgoing voter overlap is legal. A legal staged demotion has the ID
in `VotersOutgoing` and `LearnersNext`, but not in incoming `Voters`.

`RawNode.Bootstrap` does not accept an arbitrary `ConfState`; it constructs
ordinary add-voter configuration entries from a peer list. The malformed
state issue therefore concerns `Storage.InitialState` and snapshot restore,
not the peer-list bootstrap API.

This difference is externally observable as construction or Step success
versus panic, but it requires corrupted, malformed, or adversarial
configuration input. Valid configuration-change processing does not create
the overlapping state.

### C behavior before Phase 23

`raft_confchange_restore` directly copied the four membership vectors and
created Progress entries. `raft_confchange_check_invariants` already rejected:

- duplicates within each vector;
- zero and C-reserved IDs;
- `Learners` intersecting either voter configuration;
- `LearnersNext` IDs not present in outgoing voters;
- `LearnersNext` entries whose Progress was already a learner;
- `LearnersNext` or `AutoLeave` outside a joint configuration; and
- nonempty configurations with no incoming voters.

However, the existing staged-learner invariant only required an ID to be in
`VotersOutgoing` and not already marked as a learner. It did not reproduce
the canonical restore effect that excludes that ID from incoming `Voters`.
The audit example therefore passed C restore.

This was still present in current HEAD before Phase 23. No Phase 19-22 change
had altered this path.

### Implementation

`c/src/confchange.c:647-677` now rejects a restored `ConfState` when any
`LearnersNext` ID also appears in incoming `Voters`.

The check is deliberately restore-specific. The lower-level Go tracker
invariant itself does not contain this exact intersection test; Go rejects
the malformed serialized state through canonical reconstruction and the
subsequent equivalence assertion. Placing the C check at the restore boundary
matches that effective behavior without changing ordinary configuration
change operations or redesigning the tracker.

The check occurs before allocating or installing the replacement tracker, so
failure leaves the destination tracker unchanged and has no new ownership or
cleanup path.

Initial construction maps `RAFT_ERR_FATAL` to the existing cgo constructor
panic. Snapshot restore maps the same result into the existing Phase 19 fatal
latch, and RawNode/Node surface the resulting panic through their established
paths.

### ConfState comparison

| Case | Go v3.7.0 | C after Phase 23 | Equivalent? | Notes |
|---|---|---|---|---|
| Empty `ConfState` | Accepted | Accepted | Yes | Empty configuration is intentionally permitted |
| Simple unique voters | Accepted | Accepted | Yes | Normal restart case |
| Same ID in incoming and outgoing voters | Accepted | Accepted | Yes | Legal retained voter in a joint configuration |
| ID in outgoing voters and `LearnersNext`, absent from incoming voters | Accepted | Accepted | Yes | Legal staged demotion |
| Reference joint example with voters, learner, outgoing voters, staged learner, and `AutoLeave` | Accepted | Accepted | Yes | Matches the v3.7.0 restore test shape |
| ID in `Voters`, `VotersOutgoing`, and `LearnersNext` | Panic | Fatal result mapped to panic | Yes | The confirmed audit case |
| Duplicate ID within `Voters` | Panic | Fatal result mapped to panic | Yes | Go deduplicates during reconstruction and fails equivalence |
| ID in both `Voters` and `Learners` | Panic | Fatal result mapped to panic | Yes | Existing C validation already matched |
| ID in `Learners` and `VotersOutgoing` | Panic | Fatal result mapped to panic | Yes | Existing C validation already matched |
| `LearnersNext` ID absent from `VotersOutgoing` | Panic | Fatal result mapped to panic | Yes | Existing C validation already matched |
| `AutoLeave=true` without a joint configuration | Panic | Fatal result mapped to panic | Yes | Existing C validation already matched |

The C public ABI continues to reject its documented reserved local-thread IDs.
That pre-existing architectural boundary policy was not changed in this
phase.

## Finding B: heartbeat commit invariant

### Go v3.7.0 behavior

`raft.handleHeartbeat` passes the message commit unchanged to
`raftLog.commitTo` (`raft.go:1835-1837`).

`raftLog.commitTo` (`log.go:322-329`):

- does nothing when the requested commit does not advance `committed`;
- advances when `committed < Commit <= lastIndex`; and
- calls the configured logger's panic path when `Commit > lastIndex`.

The panic occurs before `committed` changes and before
`MsgHeartbeatResp` is queued.

A normal v3.7.0 leader limits a heartbeat's commit to what it believes the
follower has matched, so `Commit > follower.lastIndex` is an internal/protocol
invariant violation rather than an ordinary replication condition. It can
arise from malformed/adversarial input or corrupted/inconsistent state. The
difference is externally observable as successful commit/response versus a
terminal panic.

### C behavior before Phase 23

`core_handle_heartbeat` queried `lastIndex` and called:

```c
raft_log_commit_to(log, min(message->commit, last_index))
```

The existing `raft_log_commit_to` already returned `RAFT_ERR_FATAL` for an
out-of-range commit, but the heartbeat clamp made that branch unreachable.
An invalid heartbeat silently committed through `lastIndex`, queued a
heartbeat response, and returned success.

This mismatch was still present in current HEAD before Phase 23.

### Implementation

`c/src/raft_core.c:1187-1206` now passes `message->commit` unchanged to
`raft_log_commit_to`.

No log helper or error mechanism was added. The existing
`raft_log_commit_to` check at `c/src/log.c:635-653` returns
`RAFT_ERR_FATAL`, `raft_core_step` records it in the Phase 19 first-error
latch, and the call returns before constructing or sending
`MsgHeartbeatResp`.

The observable propagation is:

```text
invalid MsgHeartbeat
  -> core_handle_heartbeat
  -> raft_log_commit_to: RAFT_ERR_FATAL
  -> raft_core_step: latch first terminal error
  -> native RawNode.Step: RAFT_ERR_FATAL
  -> cgo RawNode.Step: panic
  -> Go Node actor stepForNode: panic in the actor
```

This reuses the established fatal-error policy. It does not create a second
fatal path. If a cgo caller recovers the panic, subsequent C-backed operations
continue to observe the sticky terminal error, as established in Phase 19.

### Heartbeat comparison

| Case | Go v3.7.0 | C after Phase 23 | Equivalent? | Notes |
|---|---|---|---|---|
| `Commit < committed` | Ignore decrease; send response | Same | Yes | Commit is monotonic |
| `Commit == committed` | No commit change; send response | Same | Yes | Normal stale/equal heartbeat |
| `committed < Commit < lastIndex` | Advance commit; send response | Same | Yes | Ready exposes the newly committed entries |
| `Commit == lastIndex` | Advance to boundary; send response | Same | Yes | Exact valid upper boundary |
| `Commit > lastIndex` | Panic before commit/response | Fatal result mapped to panic before commit/response | Yes | C also latches the terminal result |

Normal leader heartbeat processing, election timer reset, ReadIndex context
echoing, and response construction were not changed.

## Tests added

### Native C

`c/tests/confchange_test.c` adds restore classification coverage for:

- the exact incoming-voter/`LearnersNext` overlap;
- the corresponding legal staged demotion;
- duplicate voters;
- voter/learner overlap;
- a staged learner missing from outgoing voters; and
- the valid joint configuration used by the Go v3.7.0 restore tests.

`c/tests/raft_core_test.c` adds heartbeat coverage for:

- an equal commit;
- a valid advancing commit below `lastIndex`;
- `Commit == lastIndex`; and
- `Commit > lastIndex`.

The invalid native case verifies:

- `RAFT_ERR_FATAL` is returned and latched;
- the committed index remains unchanged;
- `lastIndex` remains unchanged;
- no heartbeat response is queued;
- Ready cannot be fabricated after the failure; and
- subsequent campaign attempts observe the same terminal result.

### Backend-neutral Go/C parity

`invalid_input_invariant_parity_test.go` runs unchanged with the default Go
backend and the `cgo_raft` backend. It verifies:

- matching valid/invalid constructor classification for the ConfState table;
- malformed snapshot restore panics through both backends;
- valid heartbeat commit behavior and committed Ready entries;
- an out-of-range heartbeat panics through both RawNode backends; and
- the Node actor surfaces the invariant violation as a panic in both builds.

`invalid_input_invariant_cgo_test.go` additionally verifies that the
out-of-range heartbeat uses the C fatal latch and remains terminal after the
first recovered panic.

No public testing API was added.

## Validation

All commands below completed successfully.

Focused native and backend-neutral tests:

```text
make -C c .build/confchange_test .build/raft_core_test
c/.build/confchange_test
c/.build/raft_core_test

go test ./... -run \
  'Test(MalformedConfStateClassificationParity|HeartbeatCommitValidCasesParity|HeartbeatCommitPastLastIndexPanicsParity|NodeHeartbeatCommitPastLastIndexPanicsParity|MalformedSnapshotConfStatePanicsParity)$'

CGO_ENABLED=1 go test -tags=cgo_raft ./... -run \
  'Test(MalformedConfStateClassificationParity|HeartbeatCommitValidCasesParity|HeartbeatCommitPastLastIndexPanicsParity|NodeHeartbeatCommitPastLastIndexPanicsParity|MalformedSnapshotConfStatePanicsParity|CGoHeartbeatCommitInvariantUsesFatalLatch)$'
```

Full suites:

```text
make -C c test
go test -count=1 ./...
CGO_ENABLED=1 go test -count=1 -tags=cgo_raft ./...
```

Focused configuration, snapshot, heartbeat, commit, Ready, ReadIndex, and
election regression suites passed against both Go backends.

Race detector:

```text
go test -count=1 -race ./...
CGO_ENABLED=1 go test -count=1 -race -tags=cgo_raft ./...
```

Strict cgo pointer checking:

```text
GOEXPERIMENT=cgocheck2 CGO_ENABLED=1 \
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

Valgrind full leak/error checking passed for all nine native test binaries:

- `raw_node_skeleton_test`
- `raft_core_test`
- `snapshot_test`
- `read_only_test`
- `tracker_test`
- `confchange_test`
- `unstable_test`
- `log_test`
- `fatal_error_test`

Both TLA-tagged variants passed:

```text
go test -count=1 -tags=with_tla ./...
CGO_ENABLED=1 go test -count=1 -tags='cgo_raft with_tla' ./...
```

A clean Clang C11 build and native test run passed:

```text
make -C c clean test CC=clang
```

The ordinary GCC build was subsequently restored from a clean build and the
native suite passed again. `make verify-gofmt` and `git diff --check` also
passed.

No compiler warning, test failure, race, cgo pointer violation, sanitizer
finding, Valgrind error, or leak was reported.

## Modified files

- `c/src/confchange.c`
- `c/src/raft_core.c`
- `c/tests/confchange_test.c`
- `c/tests/raft_core_test.c`
- `invalid_input_invariant_parity_test.go`
- `invalid_input_invariant_cgo_test.go`
- `OUTPUT/PHASE23_C_INVALID_INPUT_INVARIANT_RESULT.md`

## Remaining limitations

No remaining mismatch was found in the two scoped areas:

- restored ConfState classification now matches the verified v3.7.0 cases,
  including the audit reproduction; and
- heartbeat `Commit > lastIndex` now follows the established fatal/panic
  compatibility path while every valid heartbeat case remains unchanged.

The Phase 19 C fatal latch intentionally preserves terminal state if a Go
caller recovers a panic. Go does not have an equivalent recover-and-continue
contract after an internal invariant panic. This previously documented error
model was reused, not changed.

Other final-audit findings are outside Phase 23 and were not modified.
