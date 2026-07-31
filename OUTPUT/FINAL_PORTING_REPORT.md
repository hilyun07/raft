# Final C RawNode Porting Report

## Executive sign-off

This report is the Phase 32 independent sign-off audit of the C-backed
`RawNode` implementation against the actual `etcd-io/raft` v3.7.0 Go
implementation.

Final classification:

| Classification | Remaining count |
| --- | ---: |
| CONFIRMED COMPATIBILITY BUG | 0 |
| PROBABLE BUG | 0 |
| NEEDS VERIFICATION | 0 |
| INTENTIONAL DIFFERENCE | 8 |

Within the supported project scope, the C backend has achieved semantic parity
with etcd-io/raft v3.7.0. The project is recommended as complete and ready for
normal release review.

Phase 32 did not modify production code or tests. Its only repository change
is this report.

## Reference and audit baseline

The authoritative Go reference is:

```text
etcd-io/raft v3.7.0
commit b867cf13f6bc0dae21204302df97bc2355c3af55
```

The peeled `v3.7.0` tag and the merge base with the audited branch both resolve
to that commit. The audited repository state was:

```text
HEAD       b98b7ca9a140851e379fb36ace63112d4cf63925
describe   v3.7.0-30-gb98b7ca-dirty
```

The dirty state consists of the completed Phase 31 production change, its
native and parity tests, and its result document. Those changes were preserved
and audited as part of the current implementation.

The audit read:

- `prompts/RAFT_C_PORTING_SPEC.md`;
- every `OUTPUT/PHASE*.md` document through Phase 31;
- the ancillary boundary, payload, pointer-safety, Ready-ownership, protobuf,
  error-mapping, sentinel-ID, and prior audit documents in `OUTPUT/`;
- the current Go reference source;
- the current C implementation, public header, cgo bridge, and tests.

Historical phase documents were used as an inventory, not as proof. Current
source and differential test behavior were authoritative.

## Project summary

### Goal

The project preserves the Go etcd/raft `RawNode` and `Node` APIs while replacing
the implementation behind `RawNode` with a C backend selected by the
`cgo_raft` build tag. It is not a rewrite of the surrounding etcd application
or the Go `Node` actor.

### Architecture

```text
shared Go Node actor
        |
        v
build-tag-selected Go RawNode
        |
        v
cgo conversion and Storage callback bridge
        |
        v
opaque C RawNode
        |
        +-- Raft core
        +-- tracker / quorum / configuration changes
        +-- log / unstable state
        +-- read-only state
        +-- Ready and async-storage work construction
```

Important implementation locations:

| Area | Source |
| --- | --- |
| Go reference RawNode | `rawnode.go`, `bootstrap.go` |
| Shared Go Node actor | `node.go` |
| Go reference core and log | `raft.go`, `log.go`, `log_unstable.go` |
| Go tracker/configuration | `tracker/`, `quorum/`, `confchange/` |
| cgo RawNode wrapper | `rawnode_cgo.go` |
| cgo object conversion | `convert_cgo.go` |
| cgo Storage callbacks | `storage_bridge_cgo.go`, `errors_cgo.go` |
| Public C ABI | `c/include/raft/raft.h` |
| C RawNode and Ready | `c/src/raw_node.c` |
| C Raft core | `c/src/raft_core.c` |
| C log and unstable | `c/src/log.c`, `c/src/unstable.c` |
| C tracker/configuration | `c/src/tracker.c`, `c/src/confchange.c` |
| C read-only state | `c/src/read_only.c` |

### Supported scope

The supported compatibility contract is:

- the v3.7.0 Go `RawNode` and `Node` public behavior;
- Raft state, messages, Ready output, Status output, errors, and lifecycle;
- supported v3.7.0 protobuf objects and their presence/unknown-field metadata;
- synchronous Go Storage callbacks and application-owned async-storage workers;
- thread-unsafe RawNode use and serialized Node use, matching the reference.

The typed C API represents semantic protobuf objects. It is not a raw
protobuf-wire relay and does not make private C core helpers public.

## Phase summary

There is no formal `PHASE3_*.md` file. The boundary work between Phases 2 and
4 is recorded in the ancillary C API, payload, pointer-safety, Ready ownership,
error, protobuf, and sentinel-ID documents.

| Phase | Result |
| ---: | --- |
| 1 | Decoupled the shared Go Node actor from direct access to Go Raft internals and defined the RawNode boundary. |
| 2 | Defined the public opaque C RawNode ABI, ownership rules, result taxonomy, and API skeleton. |
| 3 | Recorded ancillary ABI, byte-payload, pointer-safety, sentinel-ID, Ready ownership, and error-mapping corrections; no formal phase result file exists. |
| 4 | Added the build-tagged cgo RawNode wrapper and Go/C object conversion skeleton. |
| 5 | Added the `cgo.Handle` Storage callback bridge, deep-owned callback results, and callback panic containment. |
| 6 | Ported the Raft log and unstable-log subsystem. |
| 7 | Added the baseline C Raft state machine and Ready integration. |
| 8 | Ported Progress, inflights, quorum, learners, joint consensus, and configuration changes. |
| 9 | Implemented snapshot send, restore, status, fallback, and ownership behavior. |
| 10 | Implemented safe and lease-based ReadIndex behavior. |
| 11 | Implemented PreVote, leader transfer, timeout-now, check-quorum interactions, and ForgetLeader. |
| 12 | Implemented async storage append/apply work messages, responses, and ordering. |
| 13 | Fixed `ReportUnreachable` / `MsgUnreachable` behavior. |
| 14 | Fixed election-timeout randomization at reset boundaries. |
| 15 | Fixed unknown-peer response filtering in the Node path. |
| 16 | Fixed first-oversized-proposal `MaxUncommittedEntriesSize` semantics. |
| 17 | Added `RejectHint` plus `LogTerm` conflict-term backtracking. |
| 18 | Fixed `Status.Progress[*].Inflights` reporting. |
| 19 | Added centralized terminal storage/allocation failure latching and cgo propagation. |
| 20 | Added complete cgo RawNode `runtime.KeepAlive` lifetime barriers. |
| 21 | Separated candidate vote ownership from Progress lifetime. |
| 22 | Aligned every Progress reset field and preserved configuration identity. |
| 23 | Fixed malformed ConfState rejection and the heartbeat commit invariant. |
| 24 | Preserved supported protobuf presence and unknown fields across the C boundary. |
| 25 | Added deterministic Node-owned C RawNode destruction during `Stop`. |
| 26 | Fixed stale successful `MsgAppResp` follow-up behavior. |
| 27 | Fixed zero-entry `MsgProp` behavior while preserving empty-data entries. |
| 28 | Fixed `Status.Config.AutoLeave` and complete Status configuration shape. |
| 29 | Performed a fresh differential audit and confirmed two remaining findings. |
| 30 | Made `ErrSnapshotTemporarilyUnavailable` classification storage-operation-aware. |
| 31 | Fixed proposal admission for a leader demoted to a learner. |
| 32 | Re-audited and validated the completed implementation; no new supported-scope mismatch was found. |

## Compatibility fixes

The genuine compatibility findings fixed during the semantic phases were:

1. unreachable-peer reporting did not perform the Go Progress transition;
2. election timeouts were fixed instead of redrawn on timer reset;
3. Node stepping bypassed unknown-peer response filtering;
4. the first oversized uncommitted proposal was incorrectly rejected;
5. rejected append responses ignored the follower's `LogTerm`;
6. Status omitted logical inflight state;
7. unexpected storage/allocation failures could be ignored or treated as
   ordinary Raft conditions;
8. cgo RawNode finalization could race a native call without explicit
   liveness barriers;
9. candidate votes were incorrectly owned by Progress entries;
10. Progress reset left fields with values different from Go;
11. malformed ConfState restore accepted states rejected by Go;
12. an oversized heartbeat commit was clamped instead of treated as an
    invariant failure;
13. supported protobuf presence and unknown fields were lost at the boundary;
14. Node Stop did not deterministically destroy Node-owned native state;
15. stale successful `MsgAppResp` messages could trigger follow-up work;
16. zero-entry `MsgProp` handling differed from Go;
17. Status configuration projection lost the v3.7.0 `AutoLeave` behavior;
18. `ErrSnapshotTemporarilyUnavailable` was treated as globally retryable
    rather than retryable only when returned by `Storage.Snapshot`;
19. a leader demoted to a learner used voter membership instead of local
    Progress existence as its proposal-admission predicate.

Phase 23 contained two independent invariant findings, and Phase 24 addressed
both presence and unknown-field fidelity. All of these fixes remain present
in current source and have regression coverage.

## Public API audit

### RawNode

Every current RawNode operation was compared with the v3.7.0 reference:

| API area | Operations | Result |
| --- | --- | --- |
| Construction | `NewRawNode`, `ID`, `Bootstrap` | Equivalent on valid inputs and storage state |
| Time | `Tick`, `TickQuiesced` | Equivalent timeout/reset behavior |
| Election | `Campaign` | Equivalent Hup, PreVote, pending-conf, and promotability behavior |
| Proposal | `Propose`, `ProposeConfChange` | Equivalent admission, forwarding, empty-entry, size, transfer, and demotion behavior |
| Configuration | `ApplyConfChange` | Equivalent for committed configuration entries and valid rejection discipline |
| Messaging | `Step`, Node-only stepping | Equivalent local-message and unknown-peer filtering |
| Ready | `HasReady`, `Ready`, preview/accept, `Advance` | Equivalent fields, ordering, persistence barriers, and async mode |
| Leadership | `TransferLeader`, `ForgetLeader` | Equivalent |
| Reads | `ReadIndex` | Equivalent safe, lease, forwarding, duplicate-context, and term-barrier behavior |
| Reporting | `ReportUnreachable`, `ReportSnapshot` | Equivalent |
| Observability | `BasicStatus`, `Status`, `WithProgress`, `HasProgress` | Equivalent supported fields and point-in-time ownership |

No RawNode public operation remains a skeleton.

### Node

`node.go` is shared by both backends. Start/restart, proposal routing,
asynchronous Step behavior, contexts, Ready handoff, Advance, configuration
application, Status, transfer, ReadIndex, reports, Stop, repeated Stop, and
post-stop behavior therefore use the same actor.

The backend-sensitive boundaries were separately checked:

- the Node path calls the filtered RawNode stepping entry point;
- terminal C failures panic before an asynchronous Node receive can discard an
  ordinary returned error;
- the actor serializes every C RawNode access;
- Stop destroys pending Ready state and the C RawNode before closing `done`;
- application storage workers hold independent Go protobuf objects and cannot
  retain a C RawNode pointer.

No Node lifecycle, routing, or error-propagation mismatch remains.

## Major subsystem audit

| Area | Compared behavior | Final result |
| --- | --- | --- |
| Elections | randomized timeout, vote/PreVote, leases, quorum, candidate transitions, delayed persisted self vote | No issue |
| Leadership | leader initialization, no-op entry, check quorum, transfer, timeout-now, step-down, ForgetLeader | No issue |
| Replication | append, rejection, conflict hints, conflict-term optimization, stale/duplicate/out-of-order responses | No issue |
| Progress | Match, Next, state, probe pause, activity, pending snapshot, sent commit, learner identity, reset | No issue |
| Flow control | optimistic update, inflight count/bytes/fullness/freeing, probe/replicate/snapshot transitions | No issue |
| Reads | safe and lease ReadIndex, current-term barrier, duplicate contexts, follower forwarding, step-down | No issue |
| Configuration | V1/V2, simple/joint changes, auto-leave, learners, `LearnersNext`, promotion, demotion, removal/re-add | No issue |
| Snapshots | send, temporary unavailability, report, restore, fast-forward, ConfState, compaction fallback | No issue |
| Log | stable/unstable lookup, slicing, append/truncate, conflict, commit/apply, pagination, compaction | No issue |
| Storage | callback mapping, expected conditions, fatal classification, first-error latch, snapshot retry | No issue |
| Ready | SoftState, HardState, MustSync, Entries, CommittedEntries, Messages, Snapshot, ReadStates, ordering | No issue |
| Async storage | append/apply work, local routing, response order, term ABA protection, ownership | No issue |
| Status | Hard/Soft state, Config, Progress, learners, inflights, applied index, transfer target | No issue |
| Protobuf | supported field presence, unknown fields, nested objects, Ready/storage response graphs | No issue in supported object contract |
| cgo lifetime | handles, finalizers, KeepAlive, partial cleanup, Node destruction | No issue |
| Memory ownership | deep copies, destruction, error cleanup, pending Ready, callback objects | No issue |
| Bootstrap | entry construction, commit, configuration, pre-mutation storage error return | No issue |
| Message routing | network/local classification and storage worker response routing | No issue |

## Candidate reproduction and rejection

The final audit rechecked the two Phase 29 confirmed findings and the remaining
Phase 29 hypothesis rather than assuming their later reports were correct.

### Phase 30 storage classification

**Classification: ALREADY FIXED**

Current `raft_result_is_terminal` includes escaped
`RAFT_ERR_SNAPSHOT_TEMPORARILY_UNAVAILABLE`, while
`core_maybe_send_snapshot` consumes that result and returns success only in
the actual Snapshot retry path. Constructor and cgo panic mapping also include
the sentinel. The Phase 30 differential tests pass in both backends.

### Phase 31 demoted-leader proposal

**Classification: ALREADY FIXED**

Go admits a leader proposal when `r.trk.Progress[r.id] != nil`. Current C uses
`raft_tracker_has_progress` at the corresponding leader proposal check.
Removal still drops, retained learner Progress admits, leadership transfer
still drops, and size admission is unchanged. Native and backend-neutral Phase
31 tests pass.

### Candidate vote-response Progress guard

**Classification: OUT OF PROJECT SCOPE**

The private C vote-response helper checks that current Progress exists before
recording a response, while Go's private poll helper only records by peer ID.
This does not distinguish any supported execution:

1. Go RawNode and Node stepping reject an external response while its sender
   has no Progress.
2. C RawNode and Node stepping apply the identical filter before entering the
   C core.
3. A valid application cannot campaign with an unapplied committed
   configuration change.
4. Snapshot-driven reconfiguration first moves a candidate to follower state.
5. The Phase 21 remove/re-add vote scenarios, including grants, rejections,
   election reset, and current-configuration tallying, pass in both backends.

A difference can be manufactured only by bypassing the public RawNode layer
and calling a private core helper directly, or by applying configuration
changes outside the documented committed-entry contract. Neither is a
supported execution path. No production change is warranted.

### Other suspicious candidates

| Candidate | Classification | Reason |
| --- | --- | --- |
| Non-follower private snapshot defense differs | OUT OF PROJECT SCOPE | Every supported `MsgSnap` dispatch reaches restore as a follower; direct private restore invocation is unsupported |
| C HasReady lacks a separate nonempty-HardState guard | ALREADY FIXED | Reachable HardState transitions make the Go and C predicates equivalent |
| Missing `MsgStorageError` handling | OUT OF PROJECT SCOPE | v3.7.0 defines no such message type |
| Outer `MsgStorageAppend`/`MsgStorageApply` return NOT_IMPLEMENTED | INTENTIONAL DIFFERENCE | These are application-worker work items, not Raft-core input; their responses are accepted |

No new candidate produced an observable Go/C difference.

## Remaining differences

The following eight differences are deliberate, documented, and acceptable
within the supported contract.

| ID | Difference | Classification | Compatibility rationale |
| --- | --- | --- | --- |
| I1 | Native C result codes, Go errors/panics, and sticky terminal C state | INTENTIONAL DIFFERENCE | It maps Go's non-recoverable panics into a safe C ABI while cgo restores Go panic behavior |
| I2 | Stricter C boundary validation for reserved IDs, enum ranges, nil descriptors, duplicate Bootstrap peers, and C tick widths | INTENTIONAL DIFFERENCE | Defensive ABI validation does not change valid v3.7.0 Raft executions |
| I3 | Typed C protobuf objects do not retain an original raw wire stream, original known-field ordering, duplicate known-field encodings, or noncanonical encodings | INTENTIONAL DIFFERENCE | The supported contract is semantic object fidelity, which is preserved |
| I4 | Ready, Status, Progress, entries, messages, snapshots, and callback results are deep independent copies | INTENTIONAL DIFFERENCE | This is required by cross-language ownership and is observationally safe |
| I5 | The C election PRNG algorithm and seed stream differ from Go | INTENTIONAL DIFFERENCE | The required range and redraw-on-reset lifecycle match exactly |
| I6 | Logger and TraceLogger callbacks are not bridged | INTENTIONAL DIFFERENCE | These are optional diagnostics; Raft state and TLA-tag builds remain functional |
| I7 | Direct cgo RawNode has no public Close and uses a finalizer fallback; Node-owned RawNode cleanup is deterministic | INTENTIONAL DIFFERENCE | v3.7.0 RawNode itself has no Close API; KeepAlive and Node Stop make supported lifetimes safe |
| I8 | Node scheduling stays Go-owned and async storage workers are application-owned; outer storage-work messages are not stepped into the core | INTENTIONAL DIFFERENCE | This is the specified architecture and preserves ordering and public behavior |

### Explicitly unsupported scope

The following are classified **OUT OF PROJECT SCOPE**, not compatibility bugs:

- arbitrary future or invalid protobuf enum numeric values;
- nil elements inside repeated protobuf message fields;
- direct use of private C core helpers instead of the public C RawNode API;
- concurrent use of thread-unsafe RawNode;
- reentrant Storage callbacks into the same RawNode;
- applying configuration changes that are not committed application entries;
- application storage workers reporting success without actually persisting or
  applying the supplied work;
- behavior after exhausting `uint64_t` log/timer index space or otherwise
  violating documented Raft invariants.

These boundaries are documented in the porting specification or Phase 24/25
reports. None silently changes a supported Raft execution.

## Test coverage

The current repository contains:

- nine native C test binaries;
- 74 native C `test_*` functions;
- 383 Go `Test*` functions across the repository;
- 87 backend-neutral `*Parity` tests in 19 parity files.

Regression coverage exists for every semantic compatibility phase:

| Fix area | Principal regression coverage |
| --- | --- |
| ReportUnreachable and unknown peers | `report_unreachable_parity_test.go`, `unknown_peer_response_parity_test.go`, native core tests |
| Election timeout and leadership | `election_timeout_parity_test.go`, `leadership_control_parity_test.go` |
| Proposal admission and size | `max_uncommitted_entries_size_parity_test.go`, `empty_msg_prop_parity_test.go`, `demoted_leader_proposal_parity_test.go` |
| Rejection/stale replication | `append_rejection_log_term_parity_test.go`, `stale_msg_app_resp_parity_test.go` |
| Progress/votes/Status | `candidate_vote_ownership_parity_test.go`, `progress_reset_parity_test.go`, `status_inflights_parity_test.go`, `status_config_autoleave_parity_test.go` |
| Errors and invariants | `fatal_storage_parity_test.go`, `fatal_error_cgo_test.go`, `invalid_input_invariant_parity_test.go`, native fatal/log/snapshot tests |
| Protobuf fidelity | `protobuf_fidelity_parity_test.go`, conversion and Storage bridge tests |
| Node lifetime | `node_lifetime_parity_test.go`, cgo destruction tests |
| Snapshots and reads | `snapshot_parity_test.go`, `read_index_parity_test.go` |
| Async storage | `async_storage_parity_test.go`, native Ready/core tests |

No new test was added in Phase 32 because the audit found no genuine coverage
gap requiring a behavior change. Private unreachable branches and explicitly
unsupported misuse paths are intentionally not promoted into public parity
contracts.

## Validation

All requested validation completed successfully on 2026-07-31.

### Functional suites

```text
make test-c
go test -count=1 ./...
go clean -cache
CGO_ENABLED=1 go test -count=1 -tags=cgo_raft ./...
```

Results:

- all nine native C binaries passed;
- all default Go packages passed;
- all cgo-backed Go packages passed;
- all 87 backend-neutral parity tests passed in both backends;
- the cgo-tagged `rafttest` package reports no test files by design.

The candidate-vote parity suite was also run explicitly against both
backends and passed.

### Race detector and cgo pointer checking

```text
go test -race -count=1 ./...
CGO_ENABLED=1 go test -race -count=1 -tags=cgo_raft ./...
GOEXPERIMENT=cgocheck2 CGO_ENABLED=1 \
  go test -count=1 -tags=cgo_raft ./...
```

All passed without a race or cgo pointer violation.

### ASan, LSan, and UBSan

```text
make test-c-sanitize

ASAN_OPTIONS=detect_leaks=1:halt_on_error=1 \
  CGO_ENABLED=1 go test -asan -count=1 -tags=cgo_raft ./...

CGO_ENABLED=1 \
  CGO_CFLAGS='-O1 -g -fno-omit-frame-pointer -fsanitize=undefined' \
  CGO_LDFLAGS='-fsanitize=undefined' \
  UBSAN_OPTIONS='halt_on_error=1:print_stacktrace=1' \
  go test -count=1 -tags=cgo_raft ./...
```

The native sanitizer target exercised AddressSanitizer,
LeakSanitizer-compatible leak detection, and UndefinedBehaviorSanitizer.
Native and cgo sanitizer runs passed with no use-after-free, invalid access,
double free, leak, or undefined behavior report.

### Valgrind

A clean strict GCC build passed:

```text
make -C c clean test CC=gcc
```

Every native binary was then run with:

```text
valgrind --quiet --error-exitcode=99 \
  --leak-check=full --show-leak-kinds=all <test-binary>
```

All nine binaries passed without a Valgrind error or leak report.

### TLA build variants

```text
go test -count=1 -tags=with_tla ./...
CGO_ENABLED=1 go test -count=1 -tags='cgo_raft with_tla' ./...
```

Both variants passed. The cgo backend does not emit the optional Go
TraceLogger event stream, as documented under I6.

### Compiler and hygiene checks

```text
make -C c clean test CC=clang
make -C c clean test CC=gcc
make verify-gofmt
git diff --check
```

Both strict C11 compiler builds passed with
`-Wall -Wextra -Werror -Wpedantic`. Go formatting, untracked Go-file
formatting, Markdown trailing-whitespace, and diff whitespace checks passed.

No validation failure was observed.

## Compatibility assessment

### Confirmed compatibility bugs

None.

### Probable bugs

None.

### Needs-verification findings

None. The sole carry-over vote-response hypothesis was resolved as an
unsupported private-core/contract-violation path, while all supported vote
ownership behavior remains covered and equivalent.

### Intentional differences

Eight, enumerated as I1-I8 above. They concern ABI/error representation,
boundary validation, raw-wire representation, ownership, PRNG implementation,
diagnostics, direct RawNode cleanup, and Node/storage-worker architecture.
None changes supported Raft semantics.

### Unsupported behavior

Unsupported invalid-object, private-core, concurrency-misuse,
callback-reentrancy, application-storage-contract, and invariant-exhaustion
cases are explicitly listed above. Raw-wire representation is separately
classified as intentional difference I3. None represents a silent gap in the
supported API.

## Overall conclusion

The C backend is semantically equivalent to etcd-io/raft v3.7.0 within the
supported project scope.

The final sign-off is:

```text
CONFIRMED compatibility bugs remaining: 0
PROBABLE bugs remaining:               0
NEEDS VERIFICATION findings remaining: 0
INTENTIONAL differences remaining:     8

Semantic parity within supported scope: YES
Project completion recommendation:     COMPLETE
```

Further work should be limited to normal release engineering, maintenance,
and explicitly scoped future API work. Another speculative compatibility
feature phase is not recommended without a concrete differential reproducer.
