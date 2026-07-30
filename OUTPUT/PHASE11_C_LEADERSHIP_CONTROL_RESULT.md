# Phase 11 C Leadership Control Result

## Outcome

Phase 11 completes PreVote and synchronous leadership transfer behind the
opt-in `cgo_raft` RawNode. It also verifies the Phase 10 CheckQuorum and
ForgetLeader implementation in their PreVote and transfer interactions.
The default build remains the original pure-Go implementation.

The C backend now supports:

- `Config.PreVote`;
- `StatePreCandidate`;
- `MsgPreVote` and `MsgPreVoteResp`;
- future-term PreVote rules without premature local term changes;
- stale-log PreVote rejection;
- PreVote with CheckQuorum leader-lease suppression;
- transition from pre-candidate to the real election;
- `raft_raw_node_transfer_leader`;
- follower forwarding of `MsgTransferLeader`;
- immediate and catch-up-delayed `MsgTimeoutNow`;
- forced transfer elections that skip PreVote;
- the `CampaignTransfer` vote context and CheckQuorum lease bypass;
- proposal dropping while a transfer is active;
- transfer timeout and reset/configuration abort behavior;
- committed-but-unapplied configuration checks before any campaign,
  including a TimeoutNow campaign.

CheckQuorum, heartbeat/append activity tracking, and safe/lease-mode
ForgetLeader behavior were already completed in Phase 10. This phase retains
those implementations and adds their missing PreVote/transfer integration.

## Architecture and dependencies

The implementation keeps the existing architecture:

```text
Go RawNode / Go Node actor
  -> existing cgo conversion and one-ID transfer API
  -> opaque C RawNode
  -> raft_core.c
       -> tracker/joint quorum
       -> log/unstable
       -> confchange
       -> read-only and CheckQuorum state
```

Phase 11 depends on:

- Phase 6 log last-index/term, up-to-date checks, and paged scanning;
- Phase 7 election, Ready, and synchronous Advance behavior;
- Phase 8 tracker membership, joint vote calculation, learner state,
  progress Match/Next, and configuration-change transfer abort;
- Phase 9 snapshot progress and catch-up paths;
- Phase 10 CheckQuorum, recent activity, lease vote suppression, and
  ForgetLeader restrictions.

No new public C type or Go API was required. The private RawNode ABI marker
advances from 13 to 14 because `raft_t` now retains the `pre_vote`
configuration field.

## Files changed

Implementation:

- `c/src/raft_core.h`;
- `c/src/raft_core.c`;
- `c/src/raft_internal.h`;
- `c/src/raw_node.c`.

Tests:

- `c/tests/raft_core_test.c`;
- `c/tests/raw_node_skeleton_test.c`;
- `rawnode_cgo_test.go`;
- `leadership_control_parity_test.go` (new).

Tracking documentation:

- `OUTPUT/CONFIG_FIELDS_FOR_C_PORT.md`;
- `OUTPUT/C_PORT_TRACKING_GAPS.md`;
- `OUTPUT/RAWNODE_API_PARITY_FOR_C_PORT.md`;
- `OUTPUT/PHASE11_C_LEADERSHIP_CONTROL_RESULT.md` (this file).

The production Go binding and Node actor did not change. Their existing
transfer and Step routing was already correct.

## PreVote

The C core now stores `Config.PreVote` and no longer rejects the
configuration during RawNode construction.

Normal timeout and explicit campaigns choose a pre-election when PreVote is
enabled. The pre-election:

1. enters `RAFT_STATE_PRE_CANDIDATE`;
2. clears prior tracker votes and the known leader;
3. leaves the local term and persisted vote unchanged;
4. sends `RAFT_MSG_PRE_VOTE` for `term + 1`;
5. uses the existing incoming/outgoing voter union and joint vote result;
6. enters a real election only after the pre-vote wins.

The real election increments the term, votes locally, sends `RAFT_MSG_VOTE`,
and follows the existing candidate-to-leader path.

Incoming vote handling now matches the current Go rules for both vote kinds:

- a future PreVote request does not change local term or role;
- a granted future PreVote response does not change local term;
- a rejected higher-term PreVote response does move the receiver to the
  rejection's term;
- a stale lower-term PreVote receives a rejection at the receiver's current
  term;
- successful PreVote does not persist a real vote or reset election elapsed;
- real Vote still persists the chosen voter and resets election elapsed;
- PreVote and Vote use their corresponding response types;
- an active CheckQuorum leader lease suppresses both ordinary Vote and
  PreVote requests;
- a transfer vote carrying `CampaignTransfer` bypasses that suppression;
- stale append/heartbeat traffic receives the current-term response when
  either CheckQuorum or PreVote requires it.

Candidate handling accepts only the response matching the current state.
Delayed `MsgPreVoteResp` messages are ignored after the node has become a
real candidate, and delayed `MsgVoteResp` messages are ignored while still a
pre-candidate.

The Phase 8 tracker remains the only vote source, so simple and joint
configurations use the same quorum rules as normal elections.

## Campaign guard

The original Go `hup` path refuses to campaign while committed configuration
entries remain unapplied. The prior C core did not perform this check.

Phase 11 adds a paged scan from `log.applied + 1` through `log.committed`,
using the existing log scan and application-size budget. Either legacy or V2
configuration entries suppresses the campaign without changing role or term.

This applies to:

- explicit `RawNode.Campaign`;
- election-timeout campaigns;
- PreVote campaigns;
- transfer-triggered TimeoutNow campaigns.

Several older C-backed tests campaigned immediately after Bootstrap without
first processing the committed bootstrap Ready. Their fixtures now persist
and Advance the bootstrap Ready before campaigning, matching the Go
application contract instead of weakening the campaign guard.

## Leadership transfer

`raft_raw_node_transfer_leader(rn, transferee)` now constructs the canonical
local `MsgTransferLeader` and steps it directly into the C core. The existing
Go `RawNode.TransferLeader` wrapper already calls this one-ID function.

Leader behavior matches the Go path:

- missing progress and learner targets are ignored;
- transfer to self is a no-op;
- a repeated request for the current target does not extend its timeout;
- a different target aborts the old transfer and starts a new one;
- election elapsed resets when a new transfer starts;
- `lead_transferee` is visible through BasicStatus and Status;
- proposals return `ErrProposalDropped` while transfer is active;
- an up-to-date target receives `MsgTimeoutNow` immediately;
- a lagging target is sent append/catch-up work;
- a successful `MsgAppResp` that reaches the leader's last index causes
  `MsgTimeoutNow`;
- one election timeout aborts an unfinished transfer while the node remains
  leader;
- role/term reset clears the transfer;
- Phase 8 configuration switching continues to clear a removed or demoted
  transferee.

Follower behavior:

- a transfer request with no known leader is dropped;
- otherwise it is forwarded to the known leader with the requested
  transferee preserved in `From`;
- forwarding attaches the current term through the existing send path.

Candidate and pre-candidate roles ignore TimeoutNow, matching Go.

## TimeoutNow and forced elections

A follower receiving `MsgTimeoutNow` invokes the same eligibility and
unapplied-configuration checks as a normal campaign, but selects
`CORE_CAMPAIGN_TRANSFER`.

Transfer campaigns:

- never run a PreVote round, even when `Config.PreVote` is true;
- increment the term and enter the real candidate state;
- attach the exact `CampaignTransfer` context to every Vote request;
- remain blocked for learners, removed nodes, snapshot-busy nodes, and nodes
  with committed-but-unapplied configuration entries.

The higher-term Vote/PreVote handler recognizes this context and bypasses the
CheckQuorum leader lease. Differential coverage verifies that an otherwise
lease-suppressed follower grants the forced transfer vote.

## CheckQuorum and ForgetLeader status

No replacement implementation was introduced.

The existing Phase 10 behavior remains active:

- append and heartbeat responses mark progress active;
- an active joint quorum keeps the leader;
- an inactive joint quorum steps the leader down;
- non-local activity is reset after every quorum check;
- lease reads require CheckQuorum;
- a safe-read follower may forget its current leader;
- a lease-read follower refuses to forget its leader;
- leader and non-follower ForgetLeader paths remain no-ops.

Phase 11 adds coverage showing:

- heartbeat responses keep a CheckQuorum leader active;
- the same leader steps down in the next inactive interval;
- an active leader lease suppresses disruptive PreVote;
- safe-mode ForgetLeader removes that suppression without changing term;
- the subsequent PreVote can be granted.

## Go binding and Node compatibility

The existing production Go binding already had the required shape:

```text
RawNode.TransferLeader(transferee)
  -> raft_raw_node_transfer_leader(rn, transferee)
```

It remains unchanged.

The existing Node actor path also remains unchanged:

```text
Node.TransferLeadership(ctx, lead, transferee)
  -> MsgTransferLeader{From: transferee, To: lead}
  -> RawNode.stepForNode
  -> raft_raw_node_step_for_node
```

Context cancellation and stopped-node behavior remain entirely in Go.
The C unit test exercises transfer forwarding through
`raft_raw_node_step_for_node`, preserving this layering.

`rawnode_cgo_test.go` changed only test setup: bootstrap entries are now
persisted and advanced before a campaign.

## Adaptations from `11_leadership_control.txt`

The high-level behavior was implemented, but historical structural details
were adapted to the current repository:

- the existing tracker is the sole joint quorum and membership source;
- the existing log scan is used for the campaign guard;
- the existing synchronous Ready/Advance ownership model is unchanged;
- the existing C message and status types already covered every required
  field;
- CheckQuorum and ForgetLeader were verified and integrated rather than
  reimplemented;
- the active build tag remains `cgo_raft`;
- the one-ID transfer API and Go Node actor routing were preserved;
- no replacement election, transfer, timer, or binding subsystem was added.

`ReportUnreachable` was not pulled into this phase because it is not required
to implement the requested PreVote/CheckQuorum/transfer/ForgetLeader paths and
remains a separately tracked unsupported RawNode operation.

## Tests

The C core tests now cover:

- pre-candidate state without term/vote mutation;
- future-term PreVote messages;
- transition from pre-candidate to candidate and leader;
- stale-log PreVote rejection;
- CheckQuorum lease suppression of PreVote;
- safe ForgetLeader followed by a granted PreVote;
- immediate TimeoutNow for an up-to-date transferee;
- proposal dropping during transfer;
- transfer timeout;
- append catch-up followed by TimeoutNow;
- follower transfer forwarding through the Node helper;
- TimeoutNow bypassing PreVote;
- exact `CampaignTransfer` Vote context.

`leadership_control_parity_test.go` compiles unchanged under pure Go and
`cgo_raft`. It compares externally visible scripted behavior for:

- full PreVote/pre-candidate/candidate/leader progression;
- stale-candidate rejection without a term change;
- PreVote with CheckQuorum and ForgetLeader;
- active and inactive CheckQuorum intervals;
- up-to-date transfer and timeout abort;
- lagging transfer and catch-up completion;
- follower forwarding;
- TimeoutNow forced election;
- forced-vote lease bypass;
- campaigning blocked by an unapplied configuration change.

The shared assertions compare BasicStatus fields, roles, terms, votes,
leaders, transfer targets, outgoing message type/term/context/rejection,
proposal errors, and Ready transitions.

## Validation

All commands below passed on 2026-07-30.

Strict GCC C11 warning-as-error build and all eight C test binaries:

```text
make test-c
```

AddressSanitizer, LeakSanitizer, and UndefinedBehaviorSanitizer:

```text
make test-c-sanitize
```

Strict Clang C11 warning-as-error build:

```text
make -C c clean test CC=clang
```

Valgrind, independently for all eight C binaries:

```text
valgrind --quiet --error-exitcode=99 --leak-check=full \
  --errors-for-leak-kinds=all <each C test binary>
```

Full C-backed Go suite from a fresh Go build cache:

```text
CGO_ENABLED=1 go test -count=1 -tags=cgo_raft ./...
```

Extended cgo pointer checking:

```text
CGO_ENABLED=1 GOEXPERIMENT=cgocheck2 \
  go test -count=1 -tags=cgo_raft ./...
```

Race detector:

```text
CGO_ENABLED=1 go test -count=1 -race -tags=cgo_raft ./...
```

Default pure-Go suite:

```text
go test -count=1 ./...
```

`git diff --check` also passed.

No sanitizer, Valgrind, race, strict-compiler, cgo-pointer, or test failure
remained.

## Known deviations and remaining TODOs

Leadership-specific inherited limitation:

- The minimal C core records its local election/pre-election vote
  synchronously, while current Go queues the self response in
  `msgsAfterAppend` and accounts for it through Ready/Advance. This timing
  difference predates Phase 11. Network messages still remain subject to the
  synchronous Ready persistence contract, and the shared parity scenarios
  pump Ready/Advance before delivering peer responses, but exact intermediate
  self-vote timing remains a differential gap.

Other remaining work:

- `ReportUnreachable` and `MsgUnreachable`;
- randomized election timeout parity;
- `AsyncStorageWrites` and local append/apply protocols;
- TraceLogger/`with_tla`;
- deterministic allocation-failure injection for campaign message/context
  copies and log scan pages;
- longer randomized multi-node transfer and PreVote traces across
  partitions, membership changes, snapshots, restarts, and message
  reordering;
- higher-level etcd integration once the remaining C RawNode operations are
  enabled.

The strict C transfer API continues to reject reserved IDs, while the Go core
historically treats a zero/unknown transfer target as a no-op. Real unknown
member IDs are accepted by the C boundary and ignored by the core; the
reserved-ID distinction is the existing documented public-ABI policy.

## Notes for the next session

- Keep PreVote term handling ahead of ordinary higher-term follower reset.
- Do not persist or reset election elapsed for a granted PreVote.
- Do not let a transfer-triggered campaign run PreVote.
- Preserve the exact `CampaignTransfer` context; CheckQuorum liveness depends
  on its lease bypass.
- Do not reset `election_elapsed` for a repeated request to the same
  transferee.
- Continue aborting transfer on role reset and after the target leaves the
  voter union.
- Keep campaign eligibility tied to paged committed-but-unapplied
  configuration scanning.
- Keep CheckQuorum and ReadOnlyLeaseBased coupled.
- Keep Node's two-ID API in Go and the canonical C RawNode transfer API
  transferee-only.
