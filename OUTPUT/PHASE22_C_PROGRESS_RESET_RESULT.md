# Phase 22: C Progress Reset Semantic Compatibility Result

Date: 2026-07-31

## Result

The final-audit Progress reset finding was independently confirmed against the
current source and fixed.

Before this phase, the C Raft-wide reset:

- reset every peer's `Match` to zero instead of retaining the local last index
  for self;
- set self `RecentActive` to true instead of false;
- retained each Progress's prior `sent_commit`;
- retained Raft's prior `pending_conf_index`; and
- cleared active inflights but retained their prior backing allocation.

The implementation now distinguishes the two reset operations present in etcd
v3.7.0:

1. a narrow Progress state transition, which intentionally preserves most
   Progress fields; and
2. the full Progress reconstruction performed by Raft-wide reset.

Only the full reset path changed. Probe, replicate, snapshot, configuration,
learner, Phase 20 cgo lifetime, and Phase 21 vote ownership semantics were not
redesigned.

No remaining Progress reset mismatch was found after the implementation and
final review.

## Compatibility reference

The authoritative reference is the signed `etcd-io/raft` tag `v3.7.0`:

```text
b867cf13f6bc0dae21204302df97bc2355c3af55
```

That commit is also the merge base of the current branch. Relevant reference
locations are:

- `tracker/progress.go`: `Progress.ResetState`, `BecomeProbe`,
  `BecomeReplicate`, and `BecomeSnapshot`;
- `tracker/inflights.go`: `Inflights.reset`;
- `raft.go`: `raft.reset`, `becomeFollower`, `becomeCandidate`,
  `becomePreCandidate`, and `becomeLeader`; and
- `confchange/confchange.go`: `Changer.initProgress`.

The names `resetTo` and `resetProgress` do not occur in this exact version.
The broad reset is implemented inline in `raft.reset` by assigning a new
`tracker.Progress` value to every existing Progress.

## The two reference reset operations

### `Progress.ResetState`

The Go method performs exactly these operations:

```go
pr.MsgAppFlowPaused = false
pr.PendingSnapshot = 0
pr.State = state
pr.Inflights.reset()
```

It intentionally preserves:

- `Match`;
- `Next` until the calling transition adjusts it;
- `sentCommit` until a calling transition adjusts it;
- `RecentActive`;
- `IsLearner`;
- the configured inflight limits; and
- the allocated inflight backing buffer.

`BecomeProbe`, `BecomeReplicate`, and `BecomeSnapshot` call this narrow
operation and then apply their transition-specific `Next`, `sentCommit`, and
snapshot changes.

The pre-Phase 22 C `raft_progress_reset_state` already matched this behavior.
It remains unchanged.

### `raft.reset`

The Go Raft-wide reset does not call only `Progress.ResetState`. It overwrites
each Progress with a new value:

```go
*pr = tracker.Progress{
    Match:     0,
    Next:      r.raftLog.lastIndex() + 1,
    Inflights: tracker.NewInflights(
        r.trk.MaxInflight,
        r.trk.MaxInflightBytes,
    ),
    IsLearner: pr.IsLearner,
}
if id == r.id {
    pr.Match = r.raftLog.lastIndex()
}
```

Zero-value fields produce `StateProbe`, zero `sentCommit`, zero
`PendingSnapshot`, false `RecentActive`, and false `MsgAppFlowPaused`.
`pendingConfIndex`, which is Raft-wide state rather than a Progress field, is
also reset to zero.

The new C `raft_progress_reset` mirrors this full reconstruction while
preserving the C-specific peer ID, learner property, and configured inflight
limits.

## Field-by-field comparison

This table describes the result immediately after the full Raft reset, before
leader-specific initialization.

| Field | Go after reset | C before Phase 22 | C after Phase 22 | Equivalent? | Notes |
|---|---|---|---|---|---|
| Peer identity | Progress remains under the same map key | `id` remained unchanged | `id` remains unchanged | Yes | C stores the map key in the internal Progress. |
| `Match`, self | current local last index | `0` | current local last index | Yes | Leader transition had previously hidden this mismatch by correcting self later. |
| `Match`, other peer | `0` | `0` | `0` | Yes | Unchanged. |
| `Next` | local last index plus one | local last index plus one | local last index plus one | Yes | The C overflow guard remains in place. |
| `State` | `StateProbe` | `StateProbe` | `StateProbe` | Yes | The Go enum zero value is probe. |
| `ProbeSent` | no such v3.7.0 field | no separate field | no separate field | Yes | v3.7.0 uses `MsgAppFlowPaused`. |
| `MsgAppFlowPaused` / `message_flow_paused` | `false` | `false` | `false` | Yes | Cleared by both narrow and full resets. |
| `PendingSnapshot` | `0` | `0` | `0` | Yes | Snapshot transitions retain their separate semantics. |
| `RecentActive` | `false` for every Progress | `true` for self, false for peers | `false` for every Progress | Yes | Leader initialization explicitly sets self true afterward. |
| `sentCommit` / `sent_commit` | `0` | prior value survived | `0` | Yes | Private in Go/C, but affects commit-message decisions. |
| Inflight start | `0` | `0` | `0` | Yes | |
| Inflight count | `0` | `0` | `0` | Yes | |
| Inflight bytes | `0` | `0` | `0` | Yes | |
| Inflight configured message limit | tracker configuration | preserved | preserved | Yes | |
| Inflight configured byte limit | tracker configuration | preserved | preserved | Yes | |
| Inflight backing buffer | fresh, unallocated | prior allocation retained | released and fresh/unallocated | Yes | Growth remains lazy; reset adds no allocation. |
| `IsLearner` | prior value preserved | preserved | preserved | Yes | Configuration ownership is not reset. |
| Candidate votes | tracker vote map reset separately | tracker vote state reset | Phase 21 vote sets reset | Yes | Vote ownership remains independent of Progress. |
| `pendingConfIndex` | `0` | prior value survived | `0` | Yes | This is a field on Raft, not Progress. |

There are no additional meaningful fields in the v3.7.0 Go Progress or the
current C internal Progress.

## Original C behavior and semantic effects

The old `core_reset` manually performed:

```text
Match = 0
Next = lastIndex + 1
raft_progress_reset_state(..., PROBE)
RecentActive = (peer ID == local ID)
```

Because `raft_progress_reset_state` is deliberately narrow, it left
`sent_commit` untouched and retained the inflight backing buffer. The function
also did not clear `pending_conf_index`.

The differences had these consequences:

- follower/candidate `RawNode.WithProgress` exposed self `Match=0` and
  `RecentActive=true`, while Go exposed `Match=lastIndex` and
  `RecentActive=false`;
- a stale `sent_commit` could survive a leadership cycle and influence
  `CanBumpCommit` decisions after the node became leader again;
- stale configuration bookkeeping survived follower/candidate reset, although
  the next leader transition ordinarily overwrote it; and
- the local Progress's activity lifetime did not match the reference.

`Status.Progress` itself is populated only on leaders. The leader transition
already set self `Match` and activity to the expected leader values, which is
why this mismatch was clearest through `WithProgress` on followers and
candidates.

The incorrect self activity was not normally enough to change check-quorum
results: check-quorum runs on leaders, and leader initialization explicitly
marks self active in both implementations. Nevertheless, it was a public
snapshot mismatch and the stale commit state had a later replication effect.

## Implementation

### `c/src/tracker.h`

Added the private full-reset helper:

```c
int raft_progress_reset(raft_progress_internal_t *progress,
                        uint64_t match,
                        uint64_t next);
```

This is an internal interface. There is no public C ABI or Go API change.

### `c/src/tracker.c`

`raft_progress_reset`:

- preserves `id`;
- preserves `is_learner`;
- preserves the configured maximum inflight messages and bytes;
- frees the old inflight backing buffer;
- zeroes the remaining Progress state;
- creates a fresh empty inflight tracker with the same limits;
- sets the supplied `Match` and `Next`; and
- enters `StateProbe`.

The helper performs no new allocation. The inflight tracker grows lazily when
the next leader sends entries, as in Go.

The existing `raft_progress_reset_state` was intentionally left unchanged.

### `c/src/raft_core.c`

`core_reset` now:

- sets `pending_conf_index = 0`;
- supplies `Match=lastIndex` for the local Progress;
- supplies `Match=0` for all other Progress entries;
- supplies `Next=lastIndex+1` for every Progress; and
- invokes the new full-reset helper.

`core_become_leader` remains responsible for:

- entering replicate state for self;
- marking self recently active; and
- conservatively setting `pending_conf_index` to the current last index.

This preserves the exact ordering and division of responsibility used by Go.

## Reset call-site audit

| C operation/call site | Purpose | Go equivalent | Phase 22 action |
|---|---|---|---|
| `core_reset` during construction | establish follower Progress from storage | `newRaft` followed by `becomeFollower`/`raft.reset` | changed to full reset |
| `core_become_follower` | term/state step-down or leader contact | `becomeFollower` calling `raft.reset` | receives corrected full reset |
| `core_become_candidate` | start a real election/new term | `becomeCandidate` calling `raft.reset` | receives corrected full reset |
| `core_become_leader` | initialize new leader | `becomeLeader` calling `raft.reset`, then self replicate/active | receives corrected full reset; leader-specific code unchanged |
| `core_become_pre_candidate` | start pre-election without term reset | `becomePreCandidate` resetting only votes/lead | no Progress reset; unchanged |
| `raft_progress_become_probe` | rejection, unreachable report, or snapshot completion/failure | `Progress.BecomeProbe` | narrow `ResetState`; unchanged |
| `raft_progress_become_replicate` | successful probe or self leader initialization | `Progress.BecomeReplicate` | narrow `ResetState`; unchanged |
| `raft_progress_become_snapshot` | snapshot send begins | `Progress.BecomeSnapshot` | narrow `ResetState`; unchanged |
| `raft_tracker_add_progress` | new voter/learner or restored configuration | `Changer.initProgress` | `RecentActive=true` remains intentional |
| ordinary configuration change clone | transactionally change membership | copied Go Progress map | Progress state preserved; unchanged |
| learner promotion/demotion | change configuration property | mutate/preserve existing Go Progress | replication state preserved; unchanged |
| full snapshot restore | replace tracker with snapshot configuration | replace tracker and run `confchange.Restore` | new Progress entries remain recently active, matching Go |
| check-quorum activity sweep | begin a new activity observation window | clear non-self `RecentActive` | not a Progress reset; unchanged |

All direct calls to `raft_progress_reset_state`,
`raft_progress_become_probe`, `raft_progress_become_replicate`, and
`raft_progress_become_snapshot` were reviewed. Only `core_reset` had been using
the narrow helper where Go performs full reconstruction.

## Tests

### Native tracker test

`c/tests/tracker_test.c::test_full_progress_reset` constructs a non-default
snapshot-state learner Progress with:

- nonzero `Match`, `Next`, `sent_commit`, and `PendingSnapshot`;
- `RecentActive=true`;
- paused message flow;
- two populated inflights and a grown backing buffer; and
- non-default inflight limits.

It verifies every scalar, configuration, activity, snapshot, flow-control,
identity, learner, and ownership field after full reset.

The existing `test_become_probe_resets_optimistic_state` remains as a
counter-test for narrow reset semantics. It verifies that `BecomeProbe`
preserves `RecentActive`, learner status, and the backing buffer while clearing
only the fields Go clears and clamping `sent_commit`.

### Native Raft transition test

`c/tests/raft_core_test.c::test_raft_progress_reset_semantics` verifies:

- construction with a nonempty log gives self `Match=lastIndex`;
- other peers start at `Match=0`;
- all Progress entries are inactive after Raft reset;
- dirty `sent_commit`, snapshot, pause, activity, and inflight state is cleared
  by a real campaign transition;
- self and peer `Match` values differ exactly as in Go;
- inflight limits and ownership remain correct; and
- `pending_conf_index` is zero after construction and campaign reset.

The pre-existing initialization snapshot-copy test was updated from the old
incorrect self `Match=0` expectation to `Match=lastIndex`, and now also checks
that self is inactive.

### Backend-neutral Go/C parity test

`progress_reset_parity_test.go::TestProgressResetParity` runs unchanged against
the default Go and `cgo_raft` backends. It verifies public `WithProgress`
snapshots:

1. after RawNode initialization from a nonempty snapshot;
2. after campaign/election;
3. after a heartbeat response marks a follower active; and
4. after a higher-term heartbeat forces leader-to-follower reset.

After step-down, both implementations expose:

- self `Match` at the current last index and peer `Match=0`;
- common `Next=lastIndex+1`;
- probe state;
- zero pending snapshot;
- false activity; and
- unpaused flow.

The test is deterministic and has no timeout, clock, scheduling, random-value,
or GC dependency. Private `sentCommit` and `pendingConfIndex` are covered by
native tests because the public API intentionally does not expose them.

## Regression review

The full native and Go suites, plus focused test selection, exercised:

- leader election and candidate transitions;
- leader initialization;
- append replication and commit propagation;
- check-quorum behavior;
- leader transfer;
- snapshots and snapshot reporting;
- configuration changes and joint consensus;
- voter/learner addition and promotion;
- inflight flow control and Status inflight copies;
- Ready/Advance behavior;
- Phase 21 candidate vote ownership; and
- Phase 20 cgo lifetime coverage included by the cgo suite.

No regression was observed.

## Validation

All commands below passed against the final Phase 22 source.

Native C, default Go, and cgo-backed full suites:

```text
make clean-c
make test-c
go test -count=1 ./...
CGO_ENABLED=1 go test -count=1 -tags=cgo_raft ./...
```

Focused Progress and reset parity:

```text
go test -count=1 -run '^TestProgressResetParity$' .
CGO_ENABLED=1 go test -count=1 -tags=cgo_raft \
  -run '^TestProgressResetParity$' .
```

Focused Progress, election, replication, configuration, snapshot, inflight,
Ready, quorum, and leader-transfer coverage:

```text
go test -count=1 \
  -run 'Progress|Election|Campaign|Conf|Snapshot|Replic|Leader|Inflight|Ready|Quorum|Transfer' \
  ./...

CGO_ENABLED=1 go test -count=1 -tags=cgo_raft \
  -run 'Progress|Election|Campaign|Conf|Snapshot|Replic|Leader|Inflight|Ready|Quorum|Transfer' \
  ./...
```

The focused native `tracker_test` and `raft_core_test` binaries passed
independently.

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

Valgrind full leak/error checking passed for all nine native binaries:

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

The clean Clang C11 build and native test suite passed:

```text
make -C c clean test CC=clang
```

The ordinary GCC build was restored from a clean tree and all native tests
passed again. `make verify-gofmt` and `git diff --check` passed.

No test failure, compiler warning, race, cgo pointer violation, sanitizer
finding, Valgrind error, leak, double-free, or stale-inflight ownership error
was reported.

## Modified files

- `c/src/tracker.h`
- `c/src/tracker.c`
- `c/src/raft_core.c`
- `c/tests/tracker_test.c`
- `c/tests/raft_core_test.c`
- `progress_reset_parity_test.go`
- `OUTPUT/PHASE22_C_PROGRESS_RESET_RESULT.md`

The pre-existing Phase 20 and Phase 21 changes and documents were preserved.
No other final-audit finding was modified.

## Final review answers

1. **What exactly did Go reset?** On Raft-wide reset it reconstructed each
   Progress with self/peer-specific `Match`, common `Next`, probe state, zero
   commit/snapshot/activity/pause/inflights, and preserved learner status and
   inflight limits. It also zeroed Raft's `pendingConfIndex`.
2. **What did C reset before Phase 22?** It zeroed all `Match` values, set
   `Next`, used the narrow state reset, and marked self active. This cleared
   snapshot/pause/active inflights but retained `sent_commit`, the inflight
   allocation, and `pending_conf_index`.
3. **Which fields differed?** Self `Match`, self `RecentActive`,
   `sent_commit`, `pending_conf_index`, and the full-reset inflight backing
   lifetime.
4. **Which fields intentionally remain unchanged?** Peer identity,
   `IsLearner`, and configured inflight limits survive full reset. Narrow state
   transitions additionally preserve `Match`, `RecentActive`, and other fields
   exactly as Go `Progress.ResetState` does.
5. **Why is final C behavior equivalent?** `core_reset` now uses a dedicated
   full reconstruction with the same self/peer inputs as Go, while all narrow
   transition call sites retain the original compatible behavior.
6. **Does any Progress reset mismatch remain?** No remaining mismatch was
   found in the audited fields or call sites.

## Remaining limitations

No Progress reset limitation remains. C stores peer identity inside its
private Progress while Go uses the Progress map key; preserving the C field is
the equivalent ownership behavior, not a semantic difference.

This phase intentionally does not assess or alter any other final-audit
finding.
