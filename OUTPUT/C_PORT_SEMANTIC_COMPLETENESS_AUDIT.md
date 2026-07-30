# C RawNode Semantic Completeness Audit

The deeper audit found real semantic gaps despite the clean
`RAFT_ERR_NOT_IMPLEMENTED` audit. The most important are election
randomization, the missing Node-side unknown-response filter, incorrect
uncommitted-size admission, rejection recovery ignoring `LogTerm`, and
inconsistent fatal storage-error handling.

No code was modified as part of the audit.

## Confirmed semantic bugs

### 1. Node bypasses unknown-peer response filtering — High

- **Original Go:** both `RawNode.Step` and the internal `stepForNode` reject
  response messages from unknown non-local peers before entering the Raft
  core.
- **Current C:** public `raft_raw_node_step()` has the filter, but
  `raft_raw_node_step_for_node()` delegates directly to `raft_core_step()`.
  The Go Node actor uses the latter.
- **Equivalent:** No. An unknown higher-term response can change the local
  term or cause a leader to step down. An unknown `MsgReadIndexResp` can also
  create a spurious `ReadState`.
- **Fix:** apply the same response/unknown-progress check in
  `raft_raw_node_step_for_node()`, retaining the local-storage target
  exemptions.
- **Locations:** original
  [`rawnode.go:147`](../rawnode.go#L147); Node caller
  [`node.go:399`](../node.go#L399); cgo routing
  [`rawnode_cgo.go:279`](../rawnode_cgo.go#L279); public filter
  [`c/src/raw_node.c:1262`](../c/src/raw_node.c#L1262); unfiltered path
  [`c/src/raw_node.c:1283`](../c/src/raw_node.c#L1283); higher-term processing
  [`c/src/raft_core.c:2574`](../c/src/raft_core.c#L2574).

### 2. Election timeout is never randomized — High

- **Original Go:** every reset selects a timeout in
  `[ElectionTick, 2*ElectionTick-1]`.
- **Current C:** `randomized_election_timeout` is always exactly
  `election_timeout`, both initially and after reset.
- **Equivalent:** No. Identically configured nodes started together can
  remain synchronized and repeatedly split votes.
- **Fix:** select a fresh per-reset timeout in the same range, using a
  testable per-node random source.
- **Locations:** original reset
  [`raft.go:781`](../raft.go#L781) and randomization
  [`raft.go:2053`](../raft.go#L2053); C reset
  [`c/src/raft_core.c:370`](../c/src/raft_core.c#L370); C initialization
  [`c/src/raft_core.c:2033`](../c/src/raft_core.c#L2033).

### 3. `MaxUncommittedEntriesSize` admission predicate differs — Medium

- **Original Go:** a proposal is rejected only when the existing uncommitted
  size and new payload are both nonzero and their sum exceeds the limit. A
  single oversized proposal is allowed when the uncommitted tail is empty;
  empty entries are always allowed.
- **Current C:** rejects whenever the new payload alone exceeds the limit or
  the sum would exceed it.
- **Equivalent:** No. A valid first oversized proposal accepted by etcd is
  rejected by C.
- **Fix:** reproduce the upstream three-part predicate, with overflow-safe
  addition.
- **Locations:** original
  [`raft.go:2098`](../raft.go#L2098); C
  [`c/src/raft_core.c:480`](../c/src/raft_core.c#L480).

### 4. Rejected `MsgAppResp` ignores `LogTerm` — Medium

- **Original Go:** when `LogTerm > 0`, the leader calls
  `findConflictByTerm(RejectHint, LogTerm)` before lowering `Next`.
- **Current C:** passes `RejectHint` directly to
  `raft_progress_maybe_decr_to()` even though the C log already has the
  required conflict-by-term helper.
- **Equivalent:** Safety remains intact, but recovery is not equivalent.
  Divergent logs may be probed one entry per round trip instead of
  approximately once per term.
- **Fix:** call `raft_log_find_conflict_by_term()` when
  `message->log_term > 0`, then pass the adjusted hint.
- **Locations:** original leader handling
  [`raft.go:1390`](../raft.go#L1390) and optimized lookup
  [`raft.go:1513`](../raft.go#L1513); original helper
  [`log.go:182`](../log.go#L182); C handler
  [`c/src/raft_core.c:1743`](../c/src/raft_core.c#L1743); existing C helper
  [`c/src/log.c:503`](../c/src/log.c#L503).

### 5. `Status.Progress` loses the inflight window — Medium

- **Original Go:** `Status()` deep-clones every `Progress`, including
  `Inflights`. Only `WithProgress()` deliberately clears `Inflights`.
- **Current C:** the scalar snapshot format omits inflights and is reused for
  both APIs. Consequently, Go `Status().Progress[id].Inflights` is nil.
- **Equivalent:** No. Status callers cannot inspect inflight count/fullness,
  and calling `Progress.String()` or `Inflights.Count()` can panic.
- **Fix:** add a full status-only progress representation or enough snapshot
  data to reconstruct a cloned `tracker.Inflights`; keep `WithProgress`
  intentionally lossy.
- **Locations:** original clone
  [`status.go:44`](../status.go#L44); intentional `WithProgress` behavior
  [`rawnode.go:546`](../rawnode.go#L546); C public shape
  [`c/include/raft/raft.h:385`](../c/include/raft/raft.h#L385); cgo Status
  [`rawnode_cgo.go:381`](../rawnode_cgo.go#L381); scalar conversion
  [`convert_cgo.go:504`](../convert_cgo.go#L504).

### 6. Fatal storage and allocation failures can be downgraded or ignored — High when triggered

This is a group of related failure-path bugs.

- **Original Go:** unexpected storage errors panic; the Storage contract says
  the Raft instance becomes inoperable.
- **Current C:**
  - `raft_log_match_term()` converts every error to `false`.
  - `raft_log_is_up_to_date()` converts storage failure to “candidate is not
    up to date.”
  - `find_conflict_by_term()` treats every error like an unavailable term.
  - Only selected paths latch fatal errors in `raft->error`;
    `raft_core_step()` and campaign paths can return fatal errors without
    latching them.
  - Node's asynchronous receive path ignores the returned error.
  - `raft_tracker_quorum_active()` converts clone/OOM failure to “quorum
    inactive,” potentially stepping down a healthy leader.
- **Equivalent:** No. Fatal storage failure can be misinterpreted as log
  conflict, vote rejection, or noncommit, and the node may continue
  afterward.
- **Fix:**
  - replace error-erasing boolean helpers with `int` plus boolean output;
  - distinguish only expected compacted/unavailable cases;
  - latch fatal/storage/callback/OOM results consistently at mutating entry
    points;
  - make quorum-active calculation allocation-free or return an error
    separately;
  - make Go wrappers panic where the original method cannot legitimately
    return such an error.
- **Locations:** Storage contract
  [`storage.go:42`](../storage.go#L42); original term handling
  [`log.go:387`](../log.go#L387); C match predicate
  [`c/src/log.c:243`](../c/src/log.c#L243); conflict search
  [`c/src/log.c:477`](../c/src/log.c#L477); conflict-by-term
  [`c/src/log.c:503`](../c/src/log.c#L503); up-to-date check
  [`c/src/log.c:806`](../c/src/log.c#L806); C Step
  [`c/src/raft_core.c:2561`](../c/src/raft_core.c#L2561); selective Tick
  latching [`c/src/raft_core.c:2258`](../c/src/raft_core.c#L2258); Node error
  discard [`node.go:399`](../node.go#L399); quorum clone failure
  [`c/src/tracker.c:887`](../c/src/tracker.c#L887).

### 7. Protobuf fidelity is incomplete — Medium

- **Original Go:** proposal entries are cloned as protobuf messages, forwarded
  messages retain their original objects, and `MarshalConfChange` preserves
  protobuf unknown data.
- **Current C:** Message, Entry, Snapshot, and general ConfChange conversion
  carries known fields only. Unknown fields are discarded. Generic optional
  scalar presence is also discarded; C-to-Go reconstruction usually marks
  scalar fields present even if they were absent.
- **Equivalent:** Known Raft values are usually equivalent, but wire behavior
  is not. This affects `proto.Equal`, marshaled output, rolling-version
  compatibility, and entry-size limiting. C's entry-size calculation
  explicitly treats zero scalars as absent while C-to-Go marks them present.
- **Fix:** preserve opaque protobuf unknown bytes and optional-presence bits
  through view, owned, copy, output, and sizing paths—or carry canonical
  serialized protobuf data where appropriate.
- **Locations:** original protobuf clone
  [`raft.go:812`](../raft.go#L812); forwarding
  [`raft.go:1718`](../raft.go#L1718); generated unknown fields
  [`raftpb/raft.pb.go:345`](../raftpb/raft.pb.go#L345); Go-to-C conversion
  [`convert_cgo.go:123`](../convert_cgo.go#L123); C-to-Go reconstruction
  [`convert_cgo.go:288`](../convert_cgo.go#L288); C message/entry ABI
  [`c/include/raft/raft.h:249`](../c/include/raft/raft.h#L249);
  presence-blind sizing
  [`c/src/log.c:29`](../c/src/log.c#L29).

### 8. Stale successful `MsgAppResp` can generate extra work — Low

- **Original Go:** commit calculation, commit-index refresh, pending-entry
  sends, and transfer checks are all inside the “progress updated” branch.
- **Current C:** only the state transition/inflight update is guarded by
  `updated`; commit and send processing runs even for stale successful
  responses.
- **Equivalent:** Usually harmless, but not exact. A stale duplicate may
  generate an extra append or commit-index message and change message
  ordering.
- **Fix:** put commit/bump/send-pending work under the same `updated` condition
  as upstream.
- **Locations:** original
  [`raft.go:1518`](../raft.go#L1518); C
  [`c/src/raft_core.c:1767`](../c/src/raft_core.c#L1767), particularly
  unconditional processing at
  [`c/src/raft_core.c:1796`](../c/src/raft_core.c#L1796).

### 9. Some malformed `ConfState` values are accepted — Medium impact, low likelihood

- **Original Go:** reconstructs the configuration through
  `confchange.Restore()` and then asserts the reconstructed `ConfState` is
  equivalent to the supplied one.
- **Current C:** directly copies the four ID sets. Its invariant checker
  requires `LearnersNext` members to be outgoing voters but does not reject
  overlap with incoming voters.
- **Equivalent:** No for malformed/corrupt storage or snapshots. Go rejects or
  panics; C can install a structurally inconsistent joint configuration.
- **Fix:** either restore through operations like Go or strengthen validation
  and verify reconstructed ConfState equivalence.
- **Locations:** original restore
  [`confchange/restore.go:111`](../confchange/restore.go#L111),
  initialization assertion
  [`raft.go:471`](../raft.go#L471), snapshot assertion
  [`raft.go:1936`](../raft.go#L1936); C restore
  [`c/src/confchange.c:644`](../c/src/confchange.c#L644); C invariants
  [`c/src/confchange.c:567`](../c/src/confchange.c#L567).

### 10. Empty `MsgProp` has different failure behavior — Low

- **Original Go leader:** treats an empty `MsgProp` as an invariant violation
  and panics.
- **Current C:** returns `RAFT_ERR_PROPOSAL_DROPPED`.
- **Equivalent:** No for malformed direct `RawNode.Step` input.
- **Fix:** return/latch `RAFT_ERR_FATAL` so the Go wrapper follows panic
  behavior.
- **Locations:** original
  [`raft.go:1295`](../raft.go#L1295); C preparation
  [`c/src/raft_core.c:1675`](../c/src/raft_core.c#L1675).

### 11. `Node.Stop()` does not deterministically release the C node — Medium operational issue

- **Original Go:** RawNode owns no native allocation or `cgo.Handle`.
- **Current C-backed RawNode:** owns both, but production `Node.Stop()` only
  terminates the actor. `destroy()` is otherwise invoked only by the finalizer
  or tests.
- **Equivalent:** Consensus behavior is unchanged, but lifetime behavior is
  not. A stopped Node that remains referenced retains all C allocations and
  its Go storage handle indefinitely.
- **Fix:** add a package-private release hook implemented in both builds and
  invoke it after `node.run()` is quiescent.
- **Locations:** Node stop
  [`node.go:337`](../node.go#L337); actor exit
  [`node.go:449`](../node.go#L449); C ownership/finalizer
  [`rawnode_cgo.go:127`](../rawnode_cgo.go#L127); destroy hook
  [`rawnode_cgo.go:135`](../rawnode_cgo.go#L135).

## Likely issues requiring further investigation

### 1. Votes are stored inside C Progress objects — Low likelihood

- **Original Go:** votes are a separate `ProgressTracker.Votes` map.
  Replacing the configuration's Progress map does not replace recorded votes.
- **Current C:** `vote_recorded` and `vote_granted` live inside each Progress.
  Removing and re-adding an ID during one joint change recreates the Progress
  and loses that vote.
- **Equivalent:** Normally yes, but likely not for a remove-then-add operation
  applied during an election.
- **Severity:** Low, pathological configuration sequence.
- **Fix if confirmed:** maintain votes separately from Progress or explicitly
  carry vote state across same-ID replacement.
- **Locations:** original tracker layout
  [`tracker/tracker.go:116`](../tracker/tracker.go#L116) and vote methods
  [`tracker/tracker.go:244`](../tracker/tracker.go#L244); C fields
  [`c/src/tracker.h:52`](../c/src/tracker.h#L52); removal/recreation
  [`c/src/tracker.c:681`](../c/src/tracker.c#L681); vote recording
  [`c/src/tracker.c:764`](../c/src/tracker.c#L764).

### 2. `ApplyConfChange` is not failure-transactional after tracker mutation — Medium under OOM

- **Original Go:** allocation failure is unrecoverable; successful application
  returns a ConfState matching the installed tracker.
- **Current C:** swaps in the updated tracker, then allocates the returned
  ConfState. OOM at that point returns an error with membership already
  changed. Later append-message allocation can similarly fail after mutation.
- **Equivalent:** Only if the resulting panic/error is truly terminal. Current
  inconsistent fatal latching makes recovery possible.
- **Fix:** construct output before committing the tracker swap, or latch the
  failure terminally and prevent all further use.
- **Locations:** original apply/switch
  [`raft.go:1951`](../raft.go#L1951); C mutation followed by output allocation
  [`c/src/raft_core.c:2434`](../c/src/raft_core.c#L2434).

## Benign implementation differences

- C uses sorted vectors instead of Go maps for tracker membership. Iteration
  remains deterministic and quorum calculations are equivalent.
- C deep-copies all Ready/message output instead of sharing Go protobuf
  pointers. This follows the project ownership specification; no retained Go
  pointer or use-after-free path was detected.
- `core_reset()` does not reproduce every intermediate Go Progress field
  exactly: self `Match`, `pending_conf_index`, and `sent_commit` are completed
  or refreshed on leader transition. No externally observable divergence was
  found on successful paths.
- Reserved/local-ID validation is stricter at several C public boundaries.
  This is an intentional documented ABI policy for invalid input, not a
  consensus bug.
- Recovering Go callback panics at the cgo boundary is intentional. The gap is
  continuing after such failure, not the recovery firewall itself.
- Logger/TraceLogger bridging remains absent, but this affects diagnostics
  rather than Raft state semantics.

## Areas with no detected gap

For valid inputs and successful storage/allocation paths, no semantic gap was
detected in:

- normal `Ready`, preview, accept, `Advance`, `HasReady`, `MustSync`, and
  self-response ordering;
- synchronous and asynchronous storage-write ordering, including nested
  responses and append/apply worker separation;
- normal Progress transitions, flow control, inflight freeing, snapshot
  progress, `ReportSnapshot`, and the newly implemented
  `ReportUnreachable`;
- valid simple, joint, auto-leave, learner, removal, and leader-removal
  configuration changes;
- snapshot sending, matching-log fast-forward, restore, completion, and Ready
  persistence sequencing;
- ReadIndex Safe and LeaseBased processing, current-term commit gating, joint
  quorum acknowledgements, and ReadState draining;
- PreVote, transfer-triggered elections, CheckQuorum under successful
  allocation, leader transfer, ForgetLeader, and transfer abortion;
- known-field Go-to-C enum/scalar ABI mappings and nil versus present-empty
  byte ownership.

## Validation

All existing suites passed:

```text
go test -count=1 ./...
CGO_ENABLED=1 go test -count=1 -tags=cgo_raft ./...
make -C c test
```

These passes do not invalidate the findings: most of the original detailed
Raft semantic suites are excluded under the `cgo_raft` build tag, as visible
at [`raft_test.go:1`](../raft_test.go#L1) and
[`rawnode_test.go:1`](../rawnode_test.go#L1). A shared differential harness
would be the most effective next audit tool.
