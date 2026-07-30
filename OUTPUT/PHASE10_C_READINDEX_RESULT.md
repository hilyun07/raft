# Phase 10 C ReadIndex and Advanced Read Result

## Outcome

Phase 10 completes the synchronous ReadIndex path behind the opt-in
`cgo_raft` RawNode. The default build remains the original pure-Go
implementation.

The C backend now supports:

- local and forwarded `MsgReadIndex` requests;
- `ReadOnlySafe` quorum confirmation;
- the current etcd ordered read queue and positional heartbeat contexts;
- joint-voter read acknowledgement calculation;
- postponing reads until a new leader commits an entry in its current term;
- local `ReadState` generation and remote `MsgReadIndexResp` handling;
- `Ready.ReadStates` preview, acceptance-time draining, and recursive
  ownership;
- `ReadOnlyLeaseBased` with its required CheckQuorum dependency;
- leader quorum-activity checks and step-down;
- follower vote suppression during the leader lease;
- lease-aware `ForgetLeader`;
- request-context nil/empty preservation and deep ownership across Go, C,
  messages, pending queues, Ready, and Go conversion.

The application remains responsible for waiting until its applied index is at
least `ReadState.Index` before serving the corresponding linearizable read.
Raft returns the barrier; it does not apply entries or execute the read.

## Architecture and dependencies

The implementation reuses the existing architecture:

```text
Go RawNode.ReadIndex
  -> cgo scalar byte shim
  -> raft_raw_node_read_index
  -> C raft core
       -> private read-only queue
       -> Phase 8 tracker/joint quorum
       -> heartbeat request/response path
  -> C Ready.ReadStates
  -> Go-owned Ready.ReadStates
```

Phase 10 depends on:

- Phase 4/5 call-scoped cgo byte descriptors and deep-copy helpers;
- Phase 6 log term lookup, committed/applied tracking, and Ready ownership;
- Phase 7 role transitions, message handling, and synchronous Ready/Advance;
- Phase 8 incoming/outgoing voter configurations and recent-activity
  tracking;
- Phase 9's current repository baseline and complete snapshot behavior.

No Go API change was required. The public C ABI already contained
`raft_raw_node_read_index`, ReadIndex message types, message contexts,
`raft_read_state_t`, `Ready.read_states`, and recursive free functions.
The private RawNode ABI marker advances from 12 to 13 because `raft_t` now
owns read-only, pending-read, read-state, and CheckQuorum state.

## Files added

| File | Purpose |
| --- | --- |
| `c/src/read_only.h` | Private ordered read request, acknowledgement, and confirmation interfaces. |
| `c/src/read_only.c` | C-owned request queue, per-voter maximum acknowledgement positions, little-endian heartbeat position decoding, and joint-quorum confirmation. |
| `c/tests/read_only_test.c` | Focused ordered confirmation, duplicate context, deep-copy, joint quorum, malformed context, reset, and free tests. |
| `read_index_parity_test.go` | Backend-neutral pure-Go/C ReadIndex and CheckQuorum differential scenarios. |
| `OUTPUT/PHASE10_C_READINDEX_RESULT.md` | This handoff. |

## Files updated

Implementation and build:

- `c/src/raft_core.h`;
- `c/src/raft_core.c`;
- `c/src/raft_internal.h`;
- `c/src/raw_node.c`;
- `c/include/raft/raft.h`;
- `c/Makefile`;
- `rawnode_cgo_bridge.c`.

Tests:

- `c/tests/raft_core_test.c`;
- `c/tests/raw_node_skeleton_test.c`;
- `rawnode_cgo_test.go`.

Tracking documentation:

- `OUTPUT/C_PORT_TRACKING_GAPS.md`;
- `OUTPUT/RAWNODE_API_PARITY_FOR_C_PORT.md`;
- `OUTPUT/CONFIG_FIELDS_FOR_C_PORT.md`.

## Current etcd read-only algorithm

The historical Phase 10 prompt describes context-based request tracking.
The current etcd source in this repository has changed to an ordered
positional algorithm, and the C backend follows that current source.

The private read-only state owns:

- an ordered vector of cloned original `MsgReadIndex` requests and their
  receive-time commit indexes;
- a monotonically increasing `confirmed_reads` position;
- the maximum acknowledged position observed from each responding voter;
- the configured safe or lease read option.

For a safe read, the leader:

1. appends the request to the unconfirmed queue;
2. computes `confirmed_reads + queue_length`;
3. encodes that position as eight little-endian heartbeat-context bytes;
4. self-acknowledges the position;
5. broadcasts a heartbeat carrying the current last unconfirmed position.

A heartbeat response updates that peer's maximum acknowledged position.
Confirmation calculates the majority-acknowledged position for incoming
voters and outgoing voters and takes the smaller value. This confirms exactly
the largest prefix accepted by both halves of a joint configuration.

This positional scheme deliberately does not use the application
`RequestCtx` as an internal key. Multiple requests with the same context
remain distinct, and delayed acknowledgements cannot incorrectly associate a
context with a later request.

## Leader, follower, and role-transition behavior

The C core now matches the original role behavior:

- a one-voter leader returns its current committed index immediately;
- a multi-voter leader postpones requests until it has committed an entry in
  its current term;
- a safe multi-voter leader waits for heartbeat quorum confirmation;
- a lease-based leader responds at the current committed index without the
  safe-read heartbeat round;
- a local request appends a local `ReadState`;
- a forwarded request sends `MsgReadIndexResp` with the original entry data;
- a follower with a known leader forwards `MsgReadIndex` without attaching a
  term;
- a follower converts a valid one-entry `MsgReadIndexResp` into a local
  `ReadState`;
- a follower without a known leader drops the request without notification;
- a candidate ignores read requests and responses;
- a role/term reset discards safe reads awaiting heartbeat confirmation;
- delayed old-term acknowledgements do not revive discarded reads.

The separate queue for requests postponed before a current-term commit is
retained across reset, matching the current Go implementation. It is released
only after a leader commits an entry in its current term.

The API intentionally remains lossy. There is no cancellation or negative
response when a request is dropped by a leaderless follower, campaigning
node, step-down, transport loss, or reset.

## CheckQuorum and lease reads

The baseline rejected CheckQuorum, so enabling lease reads safely required
porting the corresponding protocol dependency in this phase.

When CheckQuorum is enabled:

- leader ticks perform a quorum-activity check every election timeout;
- an inactive leader becomes a follower with no known leader;
- after each check, every non-local progress is marked inactive for the next
  interval;
- append and heartbeat responses mark the peer active;
- activity uses the Phase 8 joint-voter quorum rule;
- followers ignore ordinary higher-term vote requests while they have heard
  from a leader within the minimum election timeout;
- the existing `CampaignTransfer` vote context bypasses that suppression;
- stale append/heartbeat traffic produces the current-term response needed
  to free a higher-term isolated node;
- a lease-mode follower refuses to forget its known leader.

`ReadOnlyLeaseBased` remains invalid unless CheckQuorum is true. Its safety,
as in Go, assumes bounded clock/tick drift. The implementation does not
silently turn a lease read into an unguarded immediate read.

PreVote remains unsupported. The CheckQuorum behavior that does not depend on
PreVote is complete and usable independently, matching the Go configuration.

## Ready and memory ownership

Request data follows these ownership transitions:

1. Go passes a borrowed byte slice through the existing C-resident scalar
   shim.
2. If the request is retained or forwarded, C recursively clones its
   `MsgReadIndex` and entry data before returning from the call.
3. Pending and unconfirmed queues own those cloned messages.
4. A local response copies Entry.Data into a C-owned
   `raft_read_state_t.request_ctx`.
5. Ready preview deep-copies the entire read-state vector.
6. The cgo converter copies every context into a new Go slice.
7. Accepting that exact Ready drains the core read-state queue.
8. Destroying the returned Ready frees only the Ready copy.

Nil and present-empty contexts remain distinct at every boundary.

`ReadStates` do not affect `MustSync`, are not persisted, and are not drained
by preview. `HasReady` includes pending output read states. Advance continues
to control stable and applied progress; it does not change the read barrier
itself.

## Adaptations from `10_advanced_read.txt`

The high-level feature and safety requirements were followed. Historical
structural details were adapted as follows:

- the current positional queue/ack algorithm was ported instead of the older
  context-keyed map design;
- the active build tag is `cgo_raft`, not the historical example tag;
- the Phase 8 tracker remains the only membership and quorum source;
- existing message, Ready, and cgo ownership types were reused instead of
  adding replacement public types;
- CheckQuorum was implemented in this phase because lease reads cannot be
  safely enabled without it;
- the synchronous Ready token/accept/Advance design was extended rather than
  passing a Go Ready graph back into C;
- no read execution or automatic application wait was added; that remains the
  application's responsibility, as in etcd.

A non-empty heartbeat context shorter than eight bytes returns
`RAFT_ERR_FATAL`; the corresponding Go implementation panics while decoding
the same malformed protocol input. Both paths fail fast rather than accepting
an ambiguous acknowledgement.

## Tests

The C read-only/module and core tests cover:

- ordered queue positions and maximum per-voter acknowledgements;
- duplicate application contexts remaining distinct;
- copied request data after source mutation;
- simple and joint read quorum confirmation;
- malformed short heartbeat contexts;
- safe reads postponed before the current-term commit;
- no early `ReadState` before heartbeat quorum;
- safe read response index and context;
- reset/step-down before acknowledgement;
- lease response without the safe heartbeat round;
- CheckQuorum leader step-down;
- recursive queue/read-state cleanup.

The backend-neutral Go tests run unchanged under the pure-Go and C-backed
RawNode implementations and cover:

- singleton Ready/read-state behavior;
- nil versus present-empty request contexts;
- Ready preview versus acceptance-time draining;
- the applied-index eligibility rule;
- current-term commit postponement;
- three-voter safe quorum confirmation;
- duplicate contexts and positional acknowledgement;
- follower forwarding and `MsgReadIndexResp`;
- step-down with a delayed old acknowledgement;
- lease reads and inactive-leader step-down;
- follower vote suppression during a lease;
- lease-aware `ForgetLeader`.

The tagged C-specific test additionally mutates the original Go request slice
after `ReadIndex` and verifies that the eventual C-backed `ReadState` retains
the original bytes.

## Validation

All commands below passed on 2026-07-30:

```text
make test-c
make test-c-sanitize
make -C c clean test CC=clang
valgrind --quiet --error-exitcode=99 --leak-check=full \
  --errors-for-leak-kinds=all <each of the eight C test binaries>
make test-cgo-raft
CGO_ENABLED=1 go test -count=1 -tags=cgo_raft ./...
CGO_ENABLED=1 GOEXPERIMENT=cgocheck2 \
  go test -count=1 -tags=cgo_raft ./...
CGO_ENABLED=1 go test -count=1 -race -tags=cgo_raft ./...
go test -count=1 ./...
git diff --check
```

ASan, LeakSanitizer, and UBSan reported no findings. All eight binaries passed
Valgrind without leaks or memory errors. GCC and Clang both passed the strict
C11 warning-as-error build.

## Remaining TODOs and known limitations

Read-specific follow-up:

- add deterministic allocation-failure injection for request cloning,
  acknowledgement growth, read-state output, and Ready conversion;
- add longer randomized multi-node ReadIndex traces with membership changes,
  loss, reordering, restarts, and snapshot boundaries;
- fuzz malformed heartbeat contexts and ReadIndex/response message shapes;
- run higher-level etcd linearizable-read integration tests once more of the
  surrounding C core is enabled there.

Broader C-port limitations remain:

- PreVote and its combined CheckQuorum scenarios;
- leadership transfer;
- ReportUnreachable;
- randomized election timeout parity;
- AsyncStorageWrites and local append/apply thread protocols;
- TraceLogger/`with_tla`.

Lease reads inherit the documented bounded-clock/tick-drift assumption. An
application that serves a read before applying through `ReadState.Index`
still violates the API contract; the Raft layer cannot enforce application
state-machine progress.

## Notes for the next session

- Do not replace positional heartbeat contexts with user `RequestCtx` keys.
- Keep read quorum calculation tied to the tracker-owned incoming/outgoing
  voter configuration.
- Preserve the distinction between postponed-before-current-term-commit
  messages and safe reads already awaiting heartbeat confirmation.
- Reset must discard the latter but currently preserves the former, matching
  Go.
- Read states must remain previewable and drain only when that Ready is
  accepted.
- Lease reads must continue to require CheckQuorum, and lease-mode
  ForgetLeader must remain a no-op.
- RequestCtx is opaque application data and must remain deeply owned with
  nil/present-empty preservation.
