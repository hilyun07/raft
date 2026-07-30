# Phase 21: C Candidate Vote Ownership Result

Date: 2026-07-31

## Scope and result

The candidate vote ownership finding was independently confirmed in the
current source and fixed without changing any other final-audit finding.

Before this phase, the C backend stored `vote_recorded` and `vote_granted`
inside each `raft_progress_internal_t`. Removing a peer's Progress therefore
deleted its vote. Re-adding that peer created a new zeroed Progress and lost
the vote recorded in the current election.

The C tracker now owns vote bookkeeping independently of Progress membership.
Removing and re-adding Progress does not erase the first recorded response for
that peer. Election results still intersect retained vote history with the
current voter configuration, so a peer that remains removed does not affect
the quorum.

No other semantic-audit finding was changed.

## Compatibility reference

The project specification and port history identify `go.etcd.io/raft/v3`
version `v3.7.0` as the authoritative Go reference. The signed local tag
resolves to:

```text
b867cf13f6bc0dae21204302df97bc2355c3af55
```

The current branch has that commit as its merge base. The relevant current Go
sources also retain the reference behavior described below.

## Reference Go ownership and counting semantics

The reference `tracker.ProgressTracker` contains two separate fields:

```go
Progress ProgressMap
Votes    map[uint64]bool
```

The important consequences are:

- `RecordVote(id, vote)` inserts only when `id` has not already voted. The
  first response wins for that voting round.
- `RecordVote` does not require a Progress entry. Vote history is keyed by peer
  ID, not embedded in replication Progress.
- `ResetVotes` replaces `Votes` with an empty map.
- `TallyVotes` determines the election result with
  `p.Voters.VoteResult(p.Votes)`. Consequently, only IDs in the current
  incoming/outgoing voter configuration contribute to the quorum.
- The informational granted/rejected counts separately visit current
  non-learner Progress entries. Those counts do not determine the election
  result.
- `raft.switchToConfig` replaces `r.trk.Config` and `r.trk.Progress`; it does
  not replace `r.trk.Votes`.

Therefore an ordinary configuration change can remove and recreate a
Progress entry without clearing the peer's vote history. If the peer is not a
current voter, that history is not counted. If the same ID becomes a voter
again during the same voting round, its first recorded response is observable
again.

A related consequence is that a response from a known learner is recorded by
the Go candidate path. RawNode's response filter accepts the message because
the learner has Progress. The response does not count while the peer is a
learner, but it counts if the peer is promoted during the same election.

## Vote reset boundaries

The Go `raft.reset` method calls `r.trk.ResetVotes()`. It is used by the
follower, candidate, and leader transitions. `becomePreCandidate` explicitly
calls `ResetVotes` because it does not call the general reset method.

The C reset boundaries now match:

| Transition or event | Go behavior | C behavior |
|---|---|---|
| Initialization | new empty tracker, followed by Raft reset | empty tracker, followed by `core_reset` |
| Follower to candidate | `becomeCandidate` calls `reset(term+1)` | `core_become_candidate` calls `core_reset(term+1)` |
| Candidate to candidate | new campaign calls the same candidate reset | same |
| Pre-candidate entry | `becomePreCandidate` calls `ResetVotes` | `core_become_pre_candidate` calls `raft_tracker_reset_votes` |
| Pre-candidate to candidate | candidate reset starts the real election | same |
| Candidate to leader | `becomeLeader` calls `reset(term)` | `core_become_leader` calls `core_reset(term)` |
| Candidate to follower | `becomeFollower` calls `reset(term)` | `core_become_follower` calls `core_reset(term)` |
| Higher-term/state reset | `reset` clears votes | `core_reset` clears votes |
| Ordinary configuration change | preserves `Votes` | tracker clone preserves vote sets |
| Peer removal/re-addition | preserves history for the ID | preserves history for the ID |
| Full snapshot configuration restore | replaces the tracker | replaces the tracker |

Votes are therefore neither permanent nor owned by a term field in Progress.
They are peer-ID-keyed history whose lifetime is the current pre-election or
election round, bounded by the same Raft state resets as Go.

## Confirmation of the original C mismatch

Before production changes, the current C path was traced as follows:

1. `raft_tracker_record_vote` found the peer's
   `raft_progress_internal_t`.
2. It set that Progress's `vote_recorded` and `vote_granted` fields.
3. `confchange_remove` removed the peer from the voter configuration and
   deleted its Progress when it was not retained by the outgoing voter set.
4. `confchange_make_voter` later created a new zeroed Progress for the same ID.
5. `raft_tracker_vote_result` looked for the vote fields in that new Progress.
   The recorded response was gone.

There was no second vote map or table elsewhere in the C core. No previous
semantic-compatibility phase had partially corrected this ownership.

A read-only native probe against the pre-fix library exercised a three-voter
tracker and replaced peer 2's Progress:

```text
grant remove/re-add result=0 (Go reference: 2)
reject remove/re-add result=0 (Go reference: 1)
```

The C enum values are `PENDING=0`, `LOST=1`, and `WON=2`. Thus the C result
was pending where Go had already won or lost. This confirmed an externally
meaningful election difference rather than a structural difference alone.

## Implementation

### `c/src/tracker.h`

- Removed `vote_recorded` and `vote_granted` from
  `raft_progress_internal_t`.
- Added tracker-owned sorted ID vectors:
  - `votes_granted`
  - `votes_rejected`
- Changed the internal `raft_tracker_record_vote` helper to return a Raft
  result code so allocation failure can follow the existing fatal-error path.

The two vectors reuse the tracker's existing `raft_uint64_vec_t` abstraction.
They represent the Go `map[uint64]bool` without introducing a general-purpose
map or duplicate reporting state.

### `c/src/tracker.c`

- `raft_tracker_record_vote` checks both sets and preserves the first response.
  A new ID is inserted into exactly one set.
- `raft_tracker_vote_result` checks each current incoming/outgoing voter ID
  against the independent vote sets. Retained votes from non-voters are not
  counted.
- `raft_tracker_reset_votes` frees both sets, establishing an empty voting
  round.
- `raft_tracker_clone` copies both sets. This is required because C
  configuration changes clone the whole tracker transactionally before
  swapping it into place.
- `raft_tracker_free` frees both sets.

The clone and insertion paths retain the existing allocation-failure
discipline: partial destinations are freed, source state is unchanged, and an
insertion failure does not partially record a vote.

### `c/src/raft_core.c`

- Candidate/pre-candidate vote handling now propagates
  `raft_tracker_record_vote` failures through the existing core terminal-error
  latch.
- The vote-response path still ignores a response from an unknown ID
  (`Progress == NULL`), preserving RawNode/Node unknown-peer filtering.
- It no longer discards a response merely because the known peer is currently
  a learner. The vote is retained but current voter membership controls
  whether it is counted, matching Go.

No election timeout, quorum algorithm, Progress replication state, public
RawNode API, cgo API, or Node behavior was otherwise changed.

## Scenario behavior after the fix

### Scenario A: granted vote, remove, re-add

The grant remains in `votes_granted`. Removal excludes the ID from the current
quorum. Re-addition makes the retained grant observable again. The election
reaches the same result as Go.

### Scenario B: rejected vote, remove, re-add

The rejection remains in `votes_rejected` and contributes again after
re-addition. A duplicate response cannot overwrite the original rejection.

### Scenario C: election ends and a new election begins

Every candidate/follower/leader reset clears both vectors. A response retained
through configuration changes in the prior election cannot carry into the new
election.

### Scenario D: removed peer is never re-added

History may remain in the vote vectors for the rest of that voting round, but
the result calculation visits only current incoming/outgoing voters. The
removed peer cannot create a win or loss.

### Known learner promoted during an election

The learner's response is retained but is not counted while the ID is outside
the voter configuration. Promotion makes it count without requiring or
accepting a second response, matching Go's ID-keyed vote map.

## Tests added and updated

### Native tracker tests

`c/tests/tracker_test.c` now verifies:

- a grant survives Progress removal and replacement;
- a rejection survives Progress removal and replacement;
- a vote reset clears both independent sets;
- a removed peer's retained history is not counted;
- fresh votes after reset determine the new election;
- allocation failure returns `RAFT_ERR_OUT_OF_MEMORY` without a partial vote;
  and
- all existing vote recording calls check their result.

### Native configuration-change tests

`c/tests/confchange_test.c` now exercises the actual transactional
configuration-change path and verifies:

- grants and rejections survive remove/re-add;
- tracker cloning preserves vote history;
- a later reset clears cloned history; and
- history for a peer that stays removed is not counted.

### Native Raft core tests

`c/tests/raft_core_test.c` now verifies observable candidate state transitions:

- retained grant leads to election victory after remove/re-add;
- retained rejection leads to election loss after remove/re-add;
- candidate-to-candidate campaign reset requires a fresh quorum;
- vote-set allocation failure is latched without partial vote ownership; and
- a known learner response is retained, excluded before promotion, and counted
  after promotion.

### Backend-neutral parity test

`candidate_vote_ownership_parity_test.go` runs unchanged against the default Go
backend and the `cgo_raft` backend. It covers:

- retained grants;
- retained rejections;
- new-campaign reset;
- removed history not being counted; and
- learner history becoming countable only after promotion.

The test uses explicit campaigns, messages, and configuration changes. It has
no clock, timeout, random-value, GC, or scheduling dependency.

## Validation

All commands below passed against the final Phase 21 source.

Native C, default Go, and cgo-backed full suites:

```text
make clean-c
make test-c
go test -count=1 ./...
CGO_ENABLED=1 go test -count=1 -tags=cgo_raft ./...
```

Focused election, vote, campaign, and configuration-change tests passed on
both Go backends:

```text
go test -count=1 -run 'Vote|Election|Campaign|ConfChange|Config' ./...
CGO_ENABLED=1 go test -count=1 -tags=cgo_raft \
  -run 'Vote|Election|Campaign|ConfChange|Config' ./...
```

The focused native `tracker_test`, `confchange_test`, and `raft_core_test`
binaries also passed independently.

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

The clean Clang C11 build and all native tests passed:

```text
make -C c clean test CC=clang
```

The ordinary GCC build was restored with a clean rebuild and all native tests
passed again. `make verify-gofmt` and `git diff --check` passed.

No test failure, compiler warning, race, cgo pointer violation, allocation
cleanup error, sanitizer finding, Valgrind error, or leak was reported.

## Modified files

- `c/src/tracker.h`
- `c/src/tracker.c`
- `c/src/raft_core.c`
- `c/tests/tracker_test.c`
- `c/tests/confchange_test.c`
- `c/tests/raft_core_test.c`
- `candidate_vote_ownership_parity_test.go`
- `OUTPUT/PHASE21_C_CANDIDATE_VOTE_OWNERSHIP_RESULT.md`

The pre-existing Phase 20 changes in `rawnode_cgo.go` and its Phase 20 result
document were preserved and were not modified as part of Phase 21.

## Final review answers

1. **Where does Go store vote state?** In
   `ProgressTracker.Votes`, a peer-ID-keyed map separate from `Progress`.
2. **Where does C store vote state now?** In the tracker-owned
   `votes_granted` and `votes_rejected` ID sets, separate from every Progress.
3. **What was the mismatch?** Deleting C Progress deleted the peer's vote,
   whereas deleting Go Progress leaves the peer-ID-keyed vote map unchanged.
4. **What happens when a voted peer is removed?** Its history is retained but
   is not counted unless the ID remains in an incoming/outgoing voter set.
5. **What happens when it is re-added?** The existing first response becomes
   countable again during the same voting round.
6. **When is vote state reset?** At the same general Raft resets and explicit
   pre-candidate reset as Go, including new campaigns and transitions to
   follower, candidate, or leader.
7. **How are election semantics preserved?** First-response-wins history has
   the Go lifetime, while the current joint voter configuration alone decides
   which history contributes to the quorum.
8. **Are Progress and vote lifetimes independent?** Yes. Configuration
   changes may remove or recreate Progress without altering the vote sets.
9. **Does a candidate vote ownership mismatch remain?** No mismatch was found
   after the implementation and final source-level review.

## Remaining limitations

No remaining candidate vote ownership or reset-lifetime difference was found.
The C representation uses two sorted ID vectors rather than Go's boolean map,
but it has the same observable first-response, reset, membership-filtering,
joint-quorum, and learner-promotion semantics.

This phase intentionally does not assess or alter any other final-audit
finding.
