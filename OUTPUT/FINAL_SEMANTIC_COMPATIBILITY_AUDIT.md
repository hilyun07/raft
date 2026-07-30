# Final Semantic Compatibility Audit

## Go etcd Raft v3.7.0 vs C Backend

Audit date: 2026-07-31

Current repository HEAD:
`5699b5039b31c6288214d1a9781cdb8f041da0f7`
(`v3.7.0-20-g5699b50`)

Authoritative Go reference:
`etcd-io/raft` v3.7.0,
commit `b867cf13f6bc0dae21204302df97bc2355c3af55`

## Final verdict

### Porting status

**NOT COMPLETE**

The six semantic fixes from Phases 14–19 are present and correct in the
current source. However, the audit confirmed additional genuine mismatches,
including public API differences, configuration/election behavior, protobuf
fidelity, and cgo lifetime risks.

No files were modified during the audit itself. The worktree remained clean
throughout that audit.

## Reference and architecture

The authoritative reference is `etcd-io/raft` v3.7.0, commit
`b867cf13f6bc0dae21204302df97bc2355c3af55`.

Evidence:

- That commit is both the resolved `v3.7.0` tag and the merge base of the
  current branch.
- Current HEAD is
  `5699b5039b31c6288214d1a9781cdb8f041da0f7`, described as
  `v3.7.0-20-g5699b50`.
- The original Go implementation remains in the repository behind
  `!cgo_raft`; the C-backed wrapper is selected by `cgo_raft && cgo`.
- `Node` remains the shared Go channel/goroutine actor and delegates to the
  selected RawNode backend.

The current organization remains consistent with the porting specification:

- Go reference/state machine: `raft.go`, `rawnode.go`, `log.go`, `tracker/`,
  and `confchange/`.
- Go cgo binding: `rawnode_cgo.go`, `convert_cgo.go`, and
  `storage_bridge_cgo.go`.
- C public ABI: `c/include/raft/raft.h`.
- C implementation: `c/src/raw_node.c`, `raft_core.c`, `log.c`,
  `unstable.c`, `tracker.c`, `confchange.c`, and `read_only.c`.
- Shared Node actor: `node.go`.

## Genuine remaining bugs

### 1. C-backed RawNode finalizer can run during a cgo call

**Severity:** High impact, low likelihood

- C locations: `rawnode_cgo.go:130`, `rawnode_cgo.go:237`,
  `rawnode_cgo.go:261`, and `rawnode_cgo.go:438`.
- Go reference: `rawnode.go:71`.

The C RawNode is destroyed by a Go finalizer. Methods such as `HasProgress`,
`HasReady`, and `TickQuiesced` make their final access to `rn` before or
during a direct C call, without a later `runtime.KeepAlive(rn)`.

Go permits the finalizer to run once the compiler considers `rn` dead,
potentially destroying `rn.p` while C is still using it. The original Go
backend has no corresponding manually freed native object.

Minimal reproduction direction: repeatedly make one of these methods the
final use of a direct RawNode while forcing concurrent GC/finalization. A hit
can cause use-after-free or handle deletion during native execution.

Suggested direction: keep `rn` alive past every cgo call that depends on
`rn.p`, ideally through a common invocation helper or an explicit
`runtime.KeepAlive(rn)` after the call.

### 2. Protobuf field presence and unknown fields are still lost

**Severity:** Medium, potentially higher for forward compatibility

- C boundary: `convert_cgo.go:124`, `convert_cgo.go:289`, and
  `convert_cgo.go:360`.
- Storage boundary: `storage_bridge_cgo.go:140` and
  `storage_bridge_cgo.go:156`.
- C sizing: `c/src/log.c:32`.
- Go reference cloning and sizing: `raft.go:812` and `util.go:270`.

The C ABI stores known scalar values, but generally has no protobuf
optional-presence bits or unknown-field storage. Go-to-C conversion uses
getters, and C-to-Go conversion normally creates pointers for every scalar,
losing the original shape.

Consequences include:

- Unknown fields on proposed or replicated entries are discarded.
- Absent and explicitly-present zero fields are normalized differently.
- Every C-produced normal entry gets an explicitly-present zero `Type`, even
  where Go leaves it absent.
- C size limiting assumes zero fields are absent, which can differ from
  `proto.Size` for present-zero input.
- `proto.Equal` and serialized bytes can differ even when getters return the
  same values.

Minimal reproduction: step a `MsgApp` or `MsgProp` whose entry carries
unknown bytes or an explicitly-present zero field, then compare the resulting
Ready entry and serialized bytes. Go preserves the shape; C does not.

Suggested direction: extend the boundary representation with necessary
presence and unknown-byte data, or transport opaque serialized protobuf data
where exact preservation is required.

### 3. Malformed ConfState can be accepted

**Severity:** Medium impact, low likelihood

- C restore and invariants: `c/src/confchange.c:570` and
  `c/src/confchange.c:647`.
- Go restore/equivalence enforcement: `raft.go:471`, `raft.go:1921`, and
  `util.go:320`.

C restores the sets directly and does not reject an ID that appears
simultaneously in incoming voters and `LearnersNext`, provided it also
appears in outgoing voters.

Go reconstructs the configuration through configuration-change operations
and then calls `assertConfStatesEquivalent`. That malformed state
reconstructs differently and causes a panic.

Minimal reproduction: initialize or restore a snapshot with an ID in
`Voters`, `VotersOutgoing`, and `LearnersNext`. C accepts it; Go terminates
restoration.

Suggested direction: either restore through equivalent operations or add the
missing equivalence/invariant check before installing the tracker.

### 4. Candidate votes are incorrectly owned by Progress entries

**Severity:** Medium impact, low likelihood

- C representation/removal: `c/src/tracker.h:41`,
  `c/src/tracker.c:741`, and `c/src/tracker.c:767`.
- Go representation: `tracker/tracker.go:114` and
  `tracker/tracker.go:244`.
- Go configuration switch: `raft.go:1979`.

Go stores votes in a separate tracker map. Replacing the configuration and
Progress map does not delete already-recorded votes.

C embeds vote fields inside each Progress. Removing a peer deletes its vote;
re-adding the peer during the same campaign creates a Progress without the
prior vote.

Minimal reproduction:

1. Campaign in a five-voter configuration.
2. Record votes from self and peer 2.
3. Apply removal and re-addition of peer 2 without resetting the election.
4. Receive a vote from peer 3.

Go retains three affirmative votes and wins. C has forgotten peer 2's vote
and remains pending.

Suggested direction: store election votes independently from replication
Progress, mirroring `ProgressTracker.Votes`.

### 5. core_reset does not reproduce Go Progress reset semantics

**Severity:** Low consensus impact, clear public API mismatch

- C: `c/src/raft_core.c:389`.
- Go: `raft.go:781`.

Go resets every Progress with:

- `Match = 0`, except self gets the current last index.
- `RecentActive = false`.
- A fresh zero `sentCommit`.
- `pendingConfIndex = 0`.

C sets all matches to zero and sets self `RecentActive = true`. It also leaves
private `sent_commit` and `pending_conf_index` values in place.

The leader transition later corrects self Match and activity, so ordinary
leader replication is mostly protected. However, follower/candidate
`WithProgress` output differs immediately.

Minimal reproduction: construct a configured RawNode with a nonempty log and
inspect self through `WithProgress` while it is a follower. Go reports
`Match=lastIndex` and `RecentActive=false`; C reports `Match=0` and
`RecentActive=true`.

Suggested direction: mirror the full Go reset, then apply leader-specific
activity only in `core_become_leader`.

### 6. Node.Stop does not release C ownership deterministically

**Severity:** Medium operational issue

- Stop path: `node.go:453`.
- C ownership hook: `rawnode_cgo.go:138`.

The actor exits and closes `done`, but does not call the package-private C
RawNode destruction hook. A retained stopped Node therefore retains the C
graph, cgo handle, storage bridge, and Storage until the entire Node becomes
unreachable and its RawNode finalizer runs.

This does not alter consensus results, but it leaves an explicit native
ownership lifecycle unfinished at the natural Node termination point.

Minimal reproduction: create and stop many Nodes while retaining the stopped
Node values. Native graphs and handles remain live.

Suggested direction: add a build-neutral package-private release hook and
invoke it exactly once while the actor shuts down.

### 7. Stale successful MsgAppResp performs work outside the update branch

**Severity:** Low

- C: `c/src/raft_core.c:1821`.
- Go: `raft.go:1527`.

Go performs commit advancement, eager commit propagation, pending append
sending, and leadership-transfer completion only when the response updates
Progress or recovers a probing peer.

C guards only Progress-state and inflight updates. Commit checking and
pending-send logic execute even when `updated == false`.

A stale duplicate can consequently prompt an extra append or commit-index
message in states where Progress has recently become unpaused, such as after
`ReportUnreachable`.

Suggested direction: move lines 1850–1873 into the existing `updated` branch,
matching Go's control structure.

### 8. Oversized heartbeat commit is clamped instead of treated as an invariant failure

**Severity:** Medium impact under malformed input, low likelihood

- C: `c/src/raft_core.c:1182`.
- Go: `raft.go:1835` and `log.go:322`.

Go calls `commitTo(m.Commit)` and panics if the requested commit exceeds the
follower's last index.

C silently clamps the heartbeat commit to `min(message.commit, lastIndex)`.
A malformed heartbeat can therefore cause C to commit its full local log
while Go terminates on the invariant violation.

Correct leaders do not generate such a heartbeat because heartbeat commits
are bounded by follower Match, so this requires corrupted or malformed input.

Suggested direction: pass the requested commit to `raft_log_commit_to` and
preserve the existing fatal out-of-range result.

### 9. Empty MsgProp has different failure behavior

**Severity:** Low

- C: `c/src/raft_core.c:1710`.
- Go: `raft.go:1294`.

A leader receiving an empty `MsgProp` panics in Go. C returns
`RAFT_ERR_PROPOSAL_DROPPED`, which becomes `ErrProposalDropped`.

Minimal reproduction: campaign a single-node leader and call `RawNode.Step`
with `MsgProp` and no entries.

Suggested direction: classify this as an invariant/fatal error rather than an
ordinary dropped proposal.

### 10. Status.Config snapshot shape differs

**Severity:** Low, reporting-only

- C conversion: `convert_cgo.go:480` and `convert_cgo.go:491`.
- Go Status: `status.go:67`.
- Go `Config.Clone`: `tracker/tracker.go:95`.

The authoritative Go implementation builds `Status.Config` through
`Config.Clone`, which does not copy `AutoLeave`. C reconstructs and exposes
the internal `auto_leave` value. During an auto-leave joint configuration, Go
reports false while C reports true.

C also collapses zero-length ID sets to nil maps, whereas Go preserves a
nonnil empty incoming voter map.

Although C arguably reports the more useful value, it is not equivalent to
the specified v3.7.0 implementation.

Suggested direction: either reproduce v3.7.0 snapshot behavior or explicitly
adopt and document this as an intentional upstream correction.

## Reverification of Phases 13-19

| Finding | Classification | Current evidence |
|---|---|---|
| ReportUnreachable | **NO ISSUE** | Replicate-to-probe handling is present at `c/src/raft_core.c:2044`; follower/unknown cases remain no-ops. |
| Election timeout randomization | **NO ISSUE** | Reset selects once from `[ElectionTick, 2*ElectionTick-1]` at `c/src/raft_core.c:389`; ticks use the stored value. |
| Unknown-peer response filtering | **NO ISSUE** | Shared RawNode/Node filter is at `c/src/raw_node.c:1274`; local storage senders are exempt. |
| MaxUncommittedEntriesSize | **NO ISSUE** | `c/src/raft_core.c:537` permits the first oversized tail and enforces the limit afterward. |
| MsgAppResp LogTerm optimization | **NO ISSUE** | `RejectHint` and `LogTerm` feed `raft_log_find_conflict_by_term` at `c/src/raft_core.c:1796`. |
| Status.Progress.Inflights | **NO ISSUE** | Logical inflight contents and limits are copied at `c/src/tracker.c:965` and reconstructed at `convert_cgo.go:518`. |
| Fatal storage/allocation propagation | **NO ISSUE** for the audited stateful paths | Terminal classification/latching is at `c/src/raft_core.c:52`; cgo panics before Node can discard the result at `rawnode_cgo.go:220`. Expected snapshot unavailability and Bootstrap's pre-mutation storage error remain retryable. |

## Other subsystem results

No further mismatch was found in normal-operation behavior for:

- Campaign, pre-vote, vote rejection, quorum calculation, checkQuorum, leader
  step-down, or transfer timeout.
- Normal MsgApp append/matching, rejection handling, probe/replicate/snapshot
  transitions, inflight flow control, and snapshot reporting.
- Safe and lease ReadIndex processing, duplicate contexts, pending reads, and
  current-term commit barriers.
- Valid simple and joint configuration changes, learner promotion/demotion,
  and auto-leave execution.
- Unstable log acceptance, compaction boundaries, term lookup,
  append/truncate, application pagination, and MaxUncommitted accounting.
- Normal Ready/Advance persistence ordering.
- Async storage message construction, append-before-response ordering, ABA
  protection, apply responses, and stale-term snapshot handling.
- Partial C output allocation cleanup and deep-copy independence of Ready,
  Status Progress, snapshots, messages, and entries.
- Fatal storage propagation: unexpected callback failures do not become
  ordinary term mismatches, vote rejections, compaction, or snapshot fallback.

Directly stepping outer `MsgStorageAppend` or `MsgStorageApply` into the Raft
core remains intentionally unsupported. Those messages are application
storage-worker work items; only their response messages return to Raft.

## Intentional differences

These are documented architectural choices, not bugs:

- Native C APIs return result codes; the Go wrapper maps control errors to Go
  errors and terminal conditions to panics.
- C public boundaries use stricter validation for reserved IDs, enum ranges,
  nil descriptors, duplicate Bootstrap peers, and some malformed inputs.
- Unknown enum values are rejected rather than preserved.
- C outputs are deep-copied into independent Go objects instead of sharing
  protobuf pointers.
- The election-timeout PRNG and seed stream differ from Go, while the range
  and reset lifecycle match.
- The fatal latch preserves the first terminal C result if a Go caller
  recovers a panic.
- Node scheduling, contexts, channels, Ready delivery, and stop/done behavior
  remain implemented in Go.

## Runtime/language differences

- A Go runtime OOM normally terminates the process. C OOM is represented by
  cleanup plus a C result and Go panic.
- A pre-call cgo arena allocation failure cannot latch the untouched C state;
  this is a documented approximation.
- C objects require explicit destruction, unlike garbage-collected Go state.
  The premature-finalization and Node shutdown defects listed above are
  implementation bugs, not unavoidable runtime differences.

## Documented limitations

- Logger and TraceLogger callbacks are not bridged into the C core. This
  affects diagnostics and trace generation, not consensus behavior.
- `with_tla` builds compile and pass, but the C core does not emit the Go
  internal trace event stream.
- The full upstream `rafttest` and much of the original core interaction suite
  are build-excluded under `cgo_raft`.
- Async storage work has no failure response field in either backend.
  Applications must withhold success responses when persistence or apply work
  fails.
- Direct RawNode has no public `Close`; the current finalizer fallback is
  documented. This does not resolve deterministic Node cleanup or finalizer
  liveness.

## Validation status

All executed suites passed:

- `make test-c`
  - All nine native C test binaries passed.
- `go test -count=1 ./...`
  - All default Go packages passed.
- `CGO_ENABLED=1 go test -count=1 -tags=cgo_raft ./...`
  - All available cgo packages passed.
  - `rafttest` reported `[no test files]`.
- Default and `cgo_raft` race-detector suites passed.
- `GOEXPERIMENT=cgocheck2` cgo suite passed.
- Native ASan + UBSan with leak detection passed.
- cgo `go test -asan` with leak detection passed.
- cgo UBSan-instrumented suite passed.
- Valgrind passed for all nine native C test binaries with no errors or leaks.
- Default and `cgo_raft` `with_tla` variants passed.
- Final `git status --short` was empty.

The passing result does not contradict the findings: several relevant
upstream suites are excluded in the cgo build, and current parity tests do not
cover protobuf byte shape, malformed ConfState, vote removal/re-addition,
follower Progress reset, malformed heartbeat commit, or finalizer timing.

## Recommended next action

Fix in this priority order:

1. cgo finalizer liveness.
2. malformed ConfState acceptance and heartbeat commit clamping.
3. protobuf presence/unknown-field fidelity.
4. election vote ownership and full Progress reset semantics.
5. deterministic Node destruction.
6. stale MsgAppResp work.
7. empty MsgProp failure behavior.
8. Status.Config snapshot parity.

After those changes, run a focused final audit plus true differential traces
that compare serialized Ready/messages and status snapshots across the two
backends. The project should not move directly to release cleanup yet.
