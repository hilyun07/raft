# Phase 31: Demoted Leader Proposal Admission Compatibility

## Outcome

The Phase 29 finding was independently confirmed and fixed.

In etcd-io/raft v3.7.0, a leader accepts a proposal when its local Progress
exists, even when that Progress has become a learner. Before this phase, C
instead required the local node to remain a voter. Consequently, both
backends retained learner Progress and remained `StateLeader` when
`StepDownOnRemoval=false`, but only C returned `ErrProposalDropped`.

The production change is deliberately one predicate:

```text
before: local node is a voter
after:  local Progress exists
```

The independent leadership-transfer predicate, empty-proposal check,
configuration-change handling, proposal preparation, uncommitted-size
admission, append, replication, Ready, storage, and error behavior are
unchanged.

Phase 30 resolved the first confirmed Phase 29 finding and this phase resolves
the second. No confirmed compatibility finding from the Phase 29 audit remains
unresolved.

## Reference

The authoritative reference is:

```text
etcd-io/raft v3.7.0
commit b867cf13f6bc0dae21204302df97bc2355c3af55
```

The repository's normal, untagged Go implementation is the reference backend;
the C-backed implementation is selected by `cgo_raft && cgo`.

The following historical results were read before implementation:

- `OUTPUT/PHASE29_C_FINAL_DIFFERENTIAL_AUDIT_RESULT.md`;
- `OUTPUT/PHASE30_C_STORAGE_ERROR_CLASSIFICATION_RESULT.md`.

Phase 29 was treated as a hypothesis. Native C and backend-neutral tests were
first run against the pre-fix implementation to reproduce the difference.

## Exact Go v3.7.0 behavior

### Demotion preserves Progress

`confchange/confchange.go:204-227`, `Changer.makeLearner`, performs a direct
demotion by:

1. saving the existing Progress pointer;
2. removing the node from the incoming voter configuration;
3. restoring the saved Progress;
4. marking it `IsLearner=true` when it can immediately become a learner.

Thus Progress membership and voter membership are deliberately different
facts. A directly demoted node is absent from the incoming voter set but still
has a Progress object.

### Configuration switching and StepDownOnRemoval

`raft.go:1979-2005`, `switchToConfig`, installs the reconstructed Config and
Progress map, derives the local learner state from the local Progress, and
detects a removed or demoted leader.

When `StepDownOnRemoval=true`, it becomes a follower. When the option is false,
it returns without changing `StateLeader`. This is true for both removal and
demotion, although only demotion retains local Progress.

### Proposal admission

The exact leader-side predicate at `raft.go:1294-1307` is:

```go
if r.trk.Progress[r.id] == nil {
    return ErrProposalDropped
}
if r.leadTransferee != None {
    return ErrProposalDropped
}
```

Go does not test whether the local node is a voter. Proposal admission is
therefore based on existence of the local Progress:

- missing Progress after complete removal: proposal dropped;
- existing learner Progress after demotion: proposal accepted;
- active leadership transfer: proposal dropped independently.

`RawNode.Propose` constructs the ordinary one-entry `MsgProp`; the leader
predicate above determines admission before the existing configuration-entry,
size, append, and broadcast logic.

## C behavior before the fix

C already matched Go's configuration lifetime:

- `c/src/confchange.c:802-831`, `confchange_make_learner`, retains the existing
  Progress and marks it as a learner for direct demotion;
- `c/src/raft_core.c:2657-2665` finds the retained self Progress and leaves the
  node in `RAFT_STATE_LEADER` when `step_down_on_removal` is false;
- removal deletes self Progress, while demotion retains it with
  `is_learner=true`.

The sole mismatch was the leader `RAFT_MSG_PROP` predicate. It called
`core_is_voter`, which examined voter-set membership. A learner is correctly
not a voter, so C dropped the proposal despite retaining local Progress.

The pre-fix differential result was:

```text
configuration: voters=[2], learners=[1]
local state:   leader
local Progress: present, learner

Go backend:       Propose succeeds and appends
cgo_raft backend: Propose returns ErrProposalDropped
```

The root cause was proposal admission, not Progress ownership,
`ApplyConfChange`, configuration reconstruction, or learner representation.

## Implementation

At `c/src/raft_core.c:2084-2087`, the voter-membership check was replaced with
the existing Progress-presence helper:

```c
if (!raft_tracker_has_progress(&raft->tracker, raft->id) ||
    raft->lead_transferee != RAFT_NONE) {
    return RAFT_ERR_PROPOSAL_DROPPED;
}
```

This directly mirrors the two Go predicates. No public C or Go API changed, no
structure or ABI changed, and no ownership or allocation behavior changed.

### Admission matrix

| Condition | Go v3.7.0 | C before | C after | Equivalent? |
| --- | --- | --- | --- | --- |
| Self Progress exists | eligible for admission | voter status also required | eligible for admission | Yes |
| Self remains voter | proposal accepted | proposal accepted | proposal accepted | Yes |
| Self Progress is learner | proposal accepted while still leader | proposal dropped | proposal accepted | Yes |
| Self Progress is missing | proposal dropped | proposal dropped | proposal dropped | Yes |
| Leadership transfer active | proposal dropped | proposal dropped | proposal dropped | Yes |

### Required edge cases

| Case | Result after Phase 31 |
| --- | --- |
| Normal voter leader | Proposal succeeds and appends one entry |
| Leader removed, `StepDownOnRemoval=false` | Remains leader, self Progress absent, proposal dropped |
| Leader demoted, `StepDownOnRemoval=false` | Remains leader, self Progress retained as learner, proposal succeeds and appends |
| Leader demoted, `StepDownOnRemoval=true` | Becomes follower, learner Progress retained, proposal dropped by follower semantics |
| Leadership transfer active | Proposal dropped and log index unchanged |
| Uncommitted-size limit reached | Existing admission accounting still drops the excess proposal |

## Tests

### Native C

`c/tests/raft_core_test.c` gained
`test_demoted_leader_proposal_admission`, which verifies:

1. an ordinary voter leader accepts a proposal and advances its log;
2. a removed leader with `StepDownOnRemoval=false` remains leader but has no
   self Progress, drops the proposal, and leaves its log unchanged;
3. a demoted leader with `StepDownOnRemoval=false` retains learner Progress,
   is absent from the voter set, accepts two proposals, appends both, exposes
   them through Ready, and still enforces `MaxUncommittedEntriesSize`;
4. the same demotion with `StepDownOnRemoval=true` becomes a follower, retains
   learner Progress, drops a proposal, and leaves its log unchanged.

The existing leadership-transfer native test was strengthened to verify that
the blocked proposal also leaves the last log index unchanged.

Before the production fix, the new native test failed at the first demoted
learner proposal:

```text
raft_raw_node_propose(node, &first) == RAFT_OK
```

It passes after the predicate change.

### Backend-neutral Go/C parity

`demoted_leader_proposal_parity_test.go` adds four tests compiled unchanged
against the default and `cgo_raft` backends:

- `TestVoterLeaderProposalAdmissionParity`;
- `TestRemovedLeaderProposalAdmissionParity`;
- `TestDemotedLeaderProposalAdmissionParity`;
- `TestDemotedLeaderStepDownProposalParity`.

They compare configuration, state, Progress presence and learner
classification, returned proposal errors, Ready entries, and persisted last
index.

Before the production fix:

```text
default Go: all four passed
cgo_raft:  TestDemotedLeaderProposalAdmissionParity failed:
           Propose: raft proposal dropped
```

After the fix, both backends pass all four.

Existing regression coverage was also run for:

- leadership transfer;
- `MaxUncommittedEntriesSize`;
- empty and empty-data proposals from Phase 27;
- configuration changes, joint consensus, learner changes, and leader
  removal through the complete suites.

## Validation

All commands below completed successfully.

### Focused and complete functional suites

```text
make test-c

go test ./... -run \
  'Test(VoterLeaderProposalAdmissionParity|RemovedLeaderProposalAdmissionParity|DemotedLeaderProposalAdmissionParity|DemotedLeaderStepDownProposalParity|RawNodeLeadershipTransferParity|MaxUncommittedEntriesSizeParity)' \
  -count=1

go clean -cache
go test -tags cgo_raft ./... -run \
  'Test(VoterLeaderProposalAdmissionParity|RemovedLeaderProposalAdmissionParity|DemotedLeaderProposalAdmissionParity|DemotedLeaderStepDownProposalParity|RawNodeLeadershipTransferParity|MaxUncommittedEntriesSizeParity)' \
  -count=1

go test ./... -count=1
go test -tags cgo_raft ./... -count=1
```

The cgo cache was explicitly cleaned because `rawnode_cgo_bridge.c` includes
the C implementation and Go's build cache does not independently track every
included C source file.

All nine native C binaries passed. Both complete Go suites passed; the
cgo-tagged `rafttest` package contains no tagged test files, as in prior
phases.

### Concurrency and cgo checking

```text
go test -race ./... -count=1
go test -race -tags cgo_raft ./... -count=1
GOEXPERIMENT=cgocheck2 go test -tags cgo_raft ./... -count=1
```

### ASan, LSan, and UBSan

```text
make test-c-sanitize

ASAN_OPTIONS=detect_leaks=1:halt_on_error=1 \
  CGO_ENABLED=1 go test -asan -tags=cgo_raft ./... -count=1

CGO_ENABLED=1 \
  CGO_CFLAGS='-O1 -g -fno-omit-frame-pointer -fsanitize=undefined' \
  CGO_LDFLAGS='-fsanitize=undefined' \
  UBSAN_OPTIONS='halt_on_error=1:print_stacktrace=1' \
  go test -tags=cgo_raft ./... -count=1
```

The native sanitizer run combined AddressSanitizer, leak detection, and
UndefinedBehaviorSanitizer. The cgo ASan/LSan and cgo UBSan runs also passed.
No invalid access, use-after-free, double free, leak, or undefined behavior was
reported.

### Valgrind

A clean GCC build was run, followed by every native binary:

```text
make -C c clean test CC=gcc

valgrind --quiet --error-exitcode=99 \
  --leak-check=full --show-leak-kinds=all <each-native-test-binary>
```

All nine binaries passed without reported leaks or invalid accesses.

### TLA variants and compiler builds

```text
go test -tags=with_tla ./... -count=1
CGO_ENABLED=1 go test -tags='cgo_raft with_tla' ./... -count=1
make -C c clean test CC=clang
make -C c clean test CC=gcc
```

Both TLA variants passed. The strict C11 builds passed under both compilers
with `-Wall -Wextra -Werror -Wpedantic`.

### Formatting and diff checks

```text
gofmt -w demoted_leader_proposal_parity_test.go
make verify-gofmt
git diff --check
```

All passed.

## Files changed

Production:

- `c/src/raft_core.c`: proposal admission now checks local Progress presence.

Tests:

- `c/tests/raft_core_test.c`: native demotion/removal/step-down/size coverage
  and a stronger leadership-transfer assertion;
- `demoted_leader_proposal_parity_test.go`: backend-neutral public-behavior
  parity coverage.

Documentation:

- `OUTPUT/PHASE31_C_DEMOTED_LEADER_PROPOSAL_RESULT.md`.

## Remaining limitations

No demoted-leader proposal-admission mismatch remains in the audited cases.
No other Phase 29 finding was modified in this phase, and no confirmed Phase
29 compatibility finding remains unresolved after Phases 30 and 31.

The behavior itself remains intentionally unusual because the compatibility
target is the exact etcd v3.7.0 predicate: a demoted leader that is configured
not to step down may append proposals while no longer being a voter. Phase 31
matches that behavior; it does not redesign or reinterpret it.
