# RawNode API Parity for the C Port

This document audits the public Go `RawNode` surface at commit
`64d31dd6558fe87e313d99ebfbf8574882c27fab` and assigns every operation to the
C-port phase plan. It supplements the existing phase documents; it does not
change production code.

## Phase names used here

| Phase | Responsibility |
| --- | --- |
| Phase 2 | Compile-safe C public/private API skeleton. |
| Phase 3 | Build-tagged Go RawNode binding and conversion skeleton. |
| Phase 4 | Go `Storage` callback bridge and callback error/lifetime rules. |
| Phase 5 | C log and unstable subsystem. |
| Phase 6 | Basic C Raft state machine, election, replication, and Ready generation. |
| Phase 7 | Progress, quorum, membership, `ConfChangeV2`, and joint consensus. |
| Phase 8 | Snapshot send/restore and compaction behavior. |
| Phase 9 | ReadIndex and read-only modes. |
| Phase 10 | PreVote, CheckQuorum, transfer, and ForgetLeader. |
| Phase 11 | AsyncStorageWrites and local storage messages. |
| Phase 12 | Go Node integration and end-to-end compatibility. |

“Stub allowed” means the symbol may compile and return a dedicated
`RAFT_ERR_NOT_IMPLEMENTED` while the C backend remains opt-in. It must not
return success for work it did not perform, and storage-unavailable must not
be reused to mean unimplemented.

## Current log-subsystem implementation note

The repository's later execution prompt names the log/unstable work
“Phase 6,” while the original planning table above called that responsibility
Phase 5. The implemented milestone is unambiguous: the private C
`raft_log_t`/`raft_unstable_t` layer now initializes through Storage
`FirstIndex`/`LastIndex`, batches stable range reads, owns unstable
entries/snapshots, and exposes the bookkeeping primitives needed by Ready.
This does not implement the table's “basic C Raft state machine” milestone;
consensus-dependent RawNode methods remain stubs.

## Required API matrix

| Go operation | C public API required? | Phase 2 stub? | Real semantics phase | Important semantic traps |
| --- | --- | --- | --- | --- |
| `NewRawNode(*Config)` | Yes: `raft_raw_node_new` plus destroy | Lifecycle/validation only | Phases 4-7, then 11 | Validate all config fields before narrowing types; synchronously load Storage initial state; initialize previous Soft/Hard state; never retain raw Go pointers. |
| `Bootstrap([]Peer)` | Yes: `raft_raw_node_bootstrap` | Yes | Phases 5 and 7 | Go builds one temporary C peer array and makes one call; no Go-allocated pointer-bearing peer descriptor. Preserve borrowed Context shape and deep-copy before retention. Storage must be empty and bootstrap term/config entry behavior must match Go. |
| `Tick()` | Yes: `raft_raw_node_tick` | A no-op is acceptable only for an unreachable opt-in skeleton | Phase 6 | Runs role-specific tick logic and may trigger elections/heartbeats. A production no-op would break liveness. |
| `TickQuiesced()` | Yes: `raft_raw_node_tick_quiesced` while Go exposes it | Yes | Phase 6 | Deprecated but public. It increments `electionElapsed` only and deliberately bypasses normal Raft processing. Do not alias it to `Tick`. |
| `Campaign()` | Yes | Yes | Phase 6 | Equivalent to stepping local `MsgHup`; success does not mean leadership was won. |
| `Propose([]byte)` | Yes, with borrowed `const raft_byte_view_t *` plus scalar cgo shim | Yes | Phase 6; limits completed in Phase 7 | A null descriptor is invalid, not nil. Preserve `is_nil` versus canonical present-empty; input retained beyond the call must be copied into owned `raft_bytes_t`. The Go binding must not allocate the descriptor in Go memory. Success means accepted for processing, not committed, and may still return proposal-dropped. |
| `ProposeConfChange(ConfChangeI)` | Yes | Yes | Phase 7 | One temporary C ConfChangeV2 descriptor/change array and one RawNode call. Preserve legacy versus V2 encoding, joint transitions, opaque `is_nil` context, and proposal-time validation. |
| `ApplyConfChange(ConfChangeI)` | Yes | Yes | Phase 7 | Same single-shot temporary C input rule; output ConfState is C-owned and batch-copied to Go. Preserve zero-NodeID cancellation and call only when applying a committed change. |
| `Step(*Message)` | Yes, with `const raft_message_view_t *` | Yes | Phase 6, extended through Phases 7-11 | Go builds the complete temporary C descriptor graph, makes one call to public `raft_raw_node_step`, and frees it. That function performs public RawNode validation and delegates to `raft_raw_node_step_for_node`; never reverse this direction. Reject unexpected local messages and responses from unknown non-local peers. Retained input is recursively copied to owned `raft_message_t`. |
| `Ready()` | Yes | Yes | Phase 6, completed in Phase 11 | Combined preview/accept operation. Must preserve previous-state comparisons, message draining, unstable acceptance, read-state draining, `MustSync`, and deferred completion steps. |
| `readyWithoutAccept()` | Yes for the Go Node binding, public or private C ABI | Yes | Phase 6, completed in Phase 11 | Read-only preview. It must not consume work when the Node actor loses the Ready-channel select. |
| `acceptReady(Ready)` | Yes for the Go Node binding, public or private C ABI | Yes | Phase 6, completed in Phase 11 | Acceptance occurs only after Ready delivery. It records prior states, drains output, accepts unstable work, and prepares advance responses. No intervening mutation is allowed after preview. |
| `HasReady()` | Yes | Skeleton may return false only while backend is unusable | Phase 6, completed in Phase 11 | Must inspect every source of Ready work, including messages-after-append, read states, unstable snapshot/entries, and committed entries. |
| `Advance(Ready)` | Yes: `raft_raw_node_advance(rn)` | Yes | Phase 6, completed in Phase 11 | C tracks accepted completion internally. Go makes one argument-free call and never reconstructs Ready. It must panic/fail if called in async mode. |
| AsyncStorageWrites mode | Yes as config plus Ready behavior; an optional scalar getter can support Node caching | Config field may exist in skeleton | Phase 11 | `Advance` is forbidden; local append/apply targets are not network peers; same-target requests are reliable and ordered; append writes are durable before attached responses. |
| `ReadIndex([]byte)` | Yes, with borrowed `const raft_byte_view_t *` plus scalar cgo shim; returned context uses `raft_bytes_t` | Yes | Phase 9 | Request context is opaque and may be nil/empty, but its descriptor may not be null; preserve `is_nil`, copy before retention, and deep-copy returned context into Go. Request may be lost; returned read is usable only after applied index reaches `ReadState.Index`; lease mode requires CheckQuorum. |
| `Status()` | Yes: `raft_raw_node_status` | Zero/follower stub acceptable | Phases 6 and 7 | Allocating full snapshot: BasicStatus plus cloned config; progress is populated only on leaders. Returned nested memory is C-owned until freed. |
| `BasicStatus()` | Yes: `raft_raw_node_basic_status` | Yes | Phase 6 | Non-allocating snapshot of ID, HardState, SoftState, applied, and transferee. It must not allocate/return progress. Node uses it for proposal gating and logs. |
| `WithProgress(visitor)` | Yes, as `raft_raw_node_progress_snapshot` | Yes | Phase 7 | Observational and not equivalent to `Status.Progress`: Go visits tracked peers/learners on every role, while full Status includes progress only for leaders. Return a C-owned snapshot array, never a live tracker pointer or C-to-Go callback. Do not expose inflight backing storage; emit only real IDs and identify peer versus learner. |
| `ReportUnreachable(id)` | Yes | Yes | Phases 6-7 | ID is a real peer ID; core may no-op for an unknown member. It drives replicate-to-probe transitions. |
| `ReportSnapshot(id,status)` | Yes | Yes | Phase 8 | `SnapshotFinish` is normally a no-op; failure resumes probing. Every completed/failed `MsgSnap` transport must report. |
| `TransferLeader(transferee)` | Yes: `raft_raw_node_transfer_leader(rn, transferee)` | Yes | Phase 10 | This is the canonical C RawNode API. Go RawNode accepts only the transferee. Unknown/zero transferees are core-compatible no-ops, while the strict C wrapper may reject reserved IDs. Transfer may require catch-up and times out. |
| `ForgetLeader()` | Yes | Yes | Phase 10 | Follower clears only its known leader in the current term; leader is a no-op; lease-based read mode refuses/no-ops because forgetting would violate lease assumptions. |

## Additional Phase 1 boundary getters

Phase 1 added methods needed by the Go Node actor even though they were not
part of the historical RawNode API:

| Getter | C/binding treatment |
| --- | --- |
| `ID()` | Cache `Config.ID` in the Go binding/Node or expose a scalar C getter. It is immutable. |
| `HasProgress(id)` | Phase 4 adds scalar `raft_raw_node_has_progress`; the skeleton returns false until Phase 7 owns progress. Node uses it around self-removal; no live pointer or full enumeration is exposed. |
| `AsyncStorageWritesEnabled()` | Cache the validated config value in Go or expose a scalar getter. |
| `Logger()` | Keep Go-side metadata unless a logger callback bridge is explicitly added. Do not expose a Go logger pointer from C. |
| `stepForNode()` | Phase 4 maps this package-private Go binding operation directly to `raft_raw_node_step_for_node`. It corresponds to `node.run`'s historical package-internal `r.Step` calls and bypasses public RawNode Step validation. The Go Node actor retains its routing/filtering responsibilities. It must never be implemented by calling `raft_raw_node_step`. |

## Step entry-point layering

`raft_raw_node_step` and `raft_raw_node_step_for_node` are not interchangeable:

```text
Go RawNode.Step / external C caller
  -> raft_raw_node_step
       -> reject local messages at the public boundary
       -> reject response messages from unknown non-local peers
       -> raft_raw_node_step_for_node
            -> directly step the C raft core

Go node.run
  -> RawNode.stepForNode
  -> raft_raw_node_step_for_node
       -> directly step the C raft core
```

The correct dependency direction is public-to-internal. The Node helper must
not delegate back to the public function, because valid Node-generated local
messages would then be rejected. External C users call
`raft_raw_node_step`; only the Go Node boundary uses
`raft_raw_node_step_for_node`.

The inert skeleton can perform the local-message check now. The
unknown-response peer check needs the later C progress tracker and remains a
documented implementation gate rather than being approximated by rejecting
all response messages.

## Status and progress are three distinct APIs

The C design must preserve three different costs and semantics:

1. `raft_raw_node_basic_status`: fixed-size, allocation-free state.
2. `raft_raw_node_status`: full owned snapshot with cloned configuration and
   leader-only progress, matching Go `Status`.
3. `raft_raw_node_progress_snapshot`: explicit snapshot equivalent to
   `WithProgress`, available regardless of role and identifying peers versus
   learners.

The progress API returns a C-owned point-in-time copy and is the only planned
`WithProgress` boundary. A callback API is deliberately excluded: C must not
call a Go visitor once per peer, store a Go closure handle, or expose a live
tracker pointer. The Go binding makes one snapshot call, converts all rows,
sets each converted `tracker.Progress.Inflights` to nil, invokes the visitor
in Go, and frees the array once. Inflight buffers remain private.

## Bootstrap details

`RawNode.Bootstrap` is observable API behavior, not an optional helper:

- it rejects zero peers;
- it calls `Storage.LastIndex` and rejects non-empty storage;
- it resets the previous HardState to empty so the first Ready persists term
  and vote state;
- it becomes a follower at term 1 with no known leader;
- it serializes one legacy add-node `ConfChange` entry per peer, carrying
  `Peer.Context`;
- entries use term 1 and indexes starting at 1;
- it commits all bootstrap entries and applies each addition internally so
  immediate campaigning works;
- the application still observes and applies those committed config entries.

The public C peer ABI therefore uses `raft_peer_view_t` with an ID plus
embedded borrowed `raft_byte_view_t context`, while the peer array is passed
as `const raft_peer_view_t *`. Bootstrap must deep-copy that view before retaining it;
nil and present-empty are distinguished by `is_nil`, not by a null descriptor
pointer.

## Leadership transfer layering

The Go surfaces intentionally differ:

- `RawNode.TransferLeader(transferee)` injects a transfer request into the
  local state machine. Its canonical C mapping is
  `raft_raw_node_transfer_leader(rn, transferee)`.
- `Node.TransferLeadership(ctx, lead, transferee)` includes `lead` only to
  route the actor message to the believed current leader; context cancellation
  and stopped-node handling remain in Go.

The Go Node layer remains the actor/channel layer in the C-backed integration,
so its mapping is:

```text
Go Node.TransferLeadership(ctx, lead, transferee)
  -> Go Node channel/routing layer
  -> MsgTransferLeader{From: transferee, To: lead}
  -> RawNode.stepForNode / raft_raw_node_step_for_node
```

It does not require a dedicated two-ID C RawNode function. In particular,
`raft_raw_node_transfer_leadership(rn, lead, transferee)` is not part of the
intended public C API and must not be reintroduced as an alias.

## Skeleton and binding gates

Before Phase 3 is considered complete:

- every row marked “Yes” must have a C symbol or a documented binding-only
  treatment;
- stubbed calls must return a distinct not-implemented result;
- the Go binding must map all existing public methods, including deprecated
  `TickQuiesced`;
- default pure-Go behavior must remain unchanged;
- build-tagged compilation must cover every symbol even before semantics are
  complete;
- ownership and error mappings must follow
  `READY_OWNERSHIP_FOR_C_PORT.md` and `ERROR_MAPPING_FOR_C_PORT.md`.

## Current implementation status through Phase 11

The opt-in C backend implements the synchronous RawNode core, tracker and
joint membership changes, snapshot send/restore/report paths, ReadIndex,
`ReadOnlySafe`, `ReadOnlyLeaseBased`, CheckQuorum, and lease-aware
ForgetLeader behavior. Phase 11 adds PreVote and leadership transfer,
including pre-candidate term rules, transfer catch-up and timeout,
`MsgTimeoutNow`, the `CampaignTransfer` lease bypass, follower forwarding,
proposal suppression, and transfer target status.

ReportUnreachable and the async-storage protocol remain explicit
`RAFT_ERR_NOT_IMPLEMENTED` operations. Randomized election timeout parity and
TraceLogger integration also remain open.
