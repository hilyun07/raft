# Phase 1: RawNode Boundary Plan

This document is an inventory and implementation plan only. Phase 1 has not
been implemented, and no C code has been written.

## Goal and scope

Keep `node` as the Go channel/context/select actor, but make `RawNode` the only
synchronous boundary through which it observes or mutates Raft state. After
Phase 1, `node.go` must not dereference `rn.raft`, retain a `*raft` alias, call
`raft` helpers directly, inspect the progress tracker, or read mutable
`RawNode` fields directly.

This phase should not change the public `Node` API, the public behavior of
`RawNode`, or the contents and ordering of `Ready` batches.

## Current `Node` interface

`node.go` currently declares these methods:

```go
type Node interface {
	Tick()
	Campaign(ctx context.Context) error
	Propose(ctx context.Context, data []byte) error
	ProposeConfChange(ctx context.Context, cc pb.ConfChangeI) error
	Step(ctx context.Context, msg *pb.Message) error
	Ready() <-chan Ready
	Advance()
	ApplyConfChange(cc pb.ConfChangeI) *pb.ConfState
	TransferLeadership(ctx context.Context, lead, transferee uint64)
	ForgetLeader(ctx context.Context) error
	ReadIndex(ctx context.Context, rctx []byte) error
	Status() Status
	ReportUnreachable(id uint64)
	ReportSnapshot(id uint64, status SnapshotStatus)
	Stop()
}
```

Phase 1 must leave this interface unchanged.

## Current `RawNode`

### Fields

`RawNode` currently has five unexported fields:

| Field | Type | Role |
| --- | --- | --- |
| `raft` | `*raft` | Underlying Raft state machine. |
| `asyncStorageWrites` | `bool` | Selects synchronous `Advance` handling versus local storage/apply messages. |
| `prevSoftSt` | `*SoftState` | Last accepted volatile state, used to decide whether a `Ready` exposes a new `SoftState`. |
| `prevHardSt` | `*pb.HardState` | Last accepted persistent state, used for `HardState` emission and `MustSync`. |
| `stepsOnAdvance` | `[]*pb.Message` | Local completion messages deferred until `Advance` in synchronous storage mode. |

All five fields must remain private to the RawNode implementation. In
particular, replacing `raft` with an opaque C handle later must not require
changes in `node`.

### Methods

The current exported methods, including `Bootstrap` from `bootstrap.go`, are:

```go
func (rn *RawNode) Bootstrap(peers []Peer) error
func (rn *RawNode) Tick()
func (rn *RawNode) TickQuiesced()
func (rn *RawNode) Campaign() error
func (rn *RawNode) Propose(data []byte) error
func (rn *RawNode) ProposeConfChange(cc pb.ConfChangeI) error
func (rn *RawNode) ApplyConfChange(cc pb.ConfChangeI) *pb.ConfState
func (rn *RawNode) Step(m *pb.Message) error
func (rn *RawNode) Ready() Ready
func (rn *RawNode) HasReady() bool
func (rn *RawNode) Advance(_ Ready)
func (rn *RawNode) Status() Status
func (rn *RawNode) BasicStatus() BasicStatus
func (rn *RawNode) WithProgress(visitor func(id uint64, typ ProgressType, pr tracker.Progress))
func (rn *RawNode) ReportUnreachable(id uint64)
func (rn *RawNode) ReportSnapshot(id uint64, status SnapshotStatus)
func (rn *RawNode) TransferLeader(transferee uint64)
func (rn *RawNode) ForgetLeader() error
func (rn *RawNode) ReadIndex(rctx []byte)
```

The current unexported methods are:

```go
func (rn *RawNode) readyWithoutAccept() Ready
func (rn *RawNode) acceptReady(rd Ready)
func (rn *RawNode) applyUnstableEntries() bool
```

`readyWithoutAccept` and `acceptReady` are intentionally used by the Go actor
and are part of the semantic RawNode boundary even though they are
package-private. The later Go/C binding will need equivalent operations.
`applyUnstableEntries` is an implementation detail and should not be exposed
to `node`.

## Current boundary violations in `node.go`

Line numbers refer to commit
`030244736e0a9abed6b406d6667a522996cd5ee7`. There are two literal
`n.rn.raft` dereferences; the first creates `r`, through which the remaining
`node.run` accesses occur.

| Location | Direct access | Purpose |
| --- | --- | --- |
| `node.go:349` | `r := n.rn.raft` | Retains the underlying `*raft` for the entire actor loop. |
| `node.go:367` | `r.lead` | Detects a leader change. |
| `node.go:368` | `r.hasLeader()` | Enables or disables the proposal channel. |
| `node.go:370` | `r.logger`, `r.id`, `r.lead`, `r.Term` | Logs initial leader election. |
| `node.go:372` | `r.logger`, `r.id`, `r.lead`, `r.Term` | Logs a change from one leader to another. |
| `node.go:376` | `r.logger`, `r.id`, `r.Term` | Logs loss of the current leader. |
| `node.go:379` | `r.lead` | Updates the actor's cached leader. |
| `node.go:388` | `r.id` | Stamps locally submitted proposals with the local node ID. |
| `node.go:389` | `r.Step(m)` | Steps a proposal and returns the result to the waiting proposer. |
| `node.go:395` | `r.trk.Progress[m.GetFrom()]` | Filters response messages from unknown peers. |
| `node.go:399` | `r.Step(m)` | Steps a received message. |
| `node.go:401` | `r.trk.Progress[r.id]` | Records whether the local node had progress before a config change. |
| `node.go:402` | `r.applyConfChange(cc)` | Applies the config change directly to the Raft core. |
| `node.go:412` | `r.trk.Progress[r.id]` | Checks whether the local node has progress after the config change. |
| `node.go:416` | `r.id` | Checks whether the returned `ConfState` still contains the local node. |
| `node.go:437` | `n.rn.asyncStorageWrites` | Decides whether to arm the `Advance` channel. This bypasses Raft internals but still reads a private RawNode field. |
| `node.go:448` | `getStatus(r)` | Constructs status directly from the Raft core. |
| `node.go:463` | `n.rn.raft.logger`, `n.rn.raft.id` | Logs a dropped tick outside `node.run`. |

Calls to `HasReady`, `readyWithoutAccept`, `acceptReady`, `Tick`, and `Advance`
already go through `RawNode`; they are not violations. The two package-private
Ready calls should remain explicit because the actor needs the prepare/accept
split described below.

## Minimal boundary API

### Reuse existing methods

Most violations can be removed without adding API:

- Use `BasicStatus()` for the current leader, term, and leader-present check
  (`Lead != None`) used by proposal gating and leader-change logging.
- Use `Step()` for both proposals and received messages. For received
  messages, continue to ignore `ErrStepPeerNotFound`, preserving the current
  silent filtering behavior. Other receive-path errors are likewise not
  surfaced through the asynchronous `Node.Step` API.
- Use `ApplyConfChange()` instead of `raft.applyConfChange`.
- Use `Status()` instead of `getStatus(r)`.
- Continue to use `HasReady()`, `readyWithoutAccept()`, `acceptReady()`,
  `Advance()`, and `Tick()` as today.

### Add only the missing getters

The following small Go RawNode surface is sufficient:

```go
func (rn *RawNode) ID() uint64
func (rn *RawNode) HasProgress(id uint64) bool
func (rn *RawNode) AsyncStorageWritesEnabled() bool
func (rn *RawNode) Logger() Logger
```

- `ID` supplies the immutable local ID for proposal stamping and missed-tick
  logging without taking a full status snapshot. `newNode` should cache it so
  `Tick` does not call a thread-unsafe RawNode from outside the actor goroutine.
- `HasProgress` preserves the exact pre/post config-change test currently made
  against `trk.Progress`, without exposing the tracker or requiring an
  allocating full `Status`. Although `WithProgress` could be used to synthesize
  this check, that would make the actor iterate tracker data for a boolean
  question and would be a poor fit for the later scalar C boundary.
- `AsyncStorageWritesEnabled` replaces the private field read that controls
  whether the actor waits for `Advance`.
- `Logger` preserves the configured logger and permits the existing leader and
  missed-tick messages without exposing `raft`. The future Go binding can keep
  this as Go-side metadata; it does not require the C core to expose a logger
  pointer.

`ID`, the logger, and the async-mode flag should be copied into immutable
`node` fields during `newNode`. `HasProgress` must only be called by the
single-owner `run` goroutine.

For the eventual C boundary, `ID`, `HasProgress`, and the async-mode flag map
cleanly to scalar C getters. `BasicStatus` and `Status` map to the planned C
status APIs. Logger ownership remains in the Go binding unless a logger bridge
is added later.

No getter should expose `*raft`, `raftLog`, `tracker.Progress`, message queues,
read states, or unstable state.

## Planned `node.run` rewrite

The implementation should make only mechanical routing changes:

1. Remove `r := n.rn.raft`.
2. Read `BasicStatus` at the same point in each loop where leader state is
   currently inspected. Use it to update `propc` and produce the same log
   messages.
3. Stamp proposals with the cached local ID and call `n.rn.Step(m)`, returning
   that error through `pm.result`.
4. Call `n.rn.Step(m)` for received messages. Treat
   `ErrStepPeerNotFound` as the existing ignored unknown-peer response.
5. Around `n.rn.ApplyConfChange(cc)`, use `HasProgress(localID)` for the
   existing before/after self-removal check. Keep the returned `ConfState`
   voter scan and proposal-channel behavior unchanged.
6. Use the cached async flag when deciding whether to arm `advancec`.
7. Answer status requests with `n.rn.Status()`.
8. Use the cached ID and logger for the missed-tick warning.

No consensus decision, message construction rule, or configuration-change
policy moves into `node`.

## Preserving `Ready`/`Advance` semantics

The refactor must retain the current two-stage actor protocol:

1. Only when `advancec == nil` and `HasReady()` is true, call
   `readyWithoutAccept()`. This is a read-only preview. If another select case
   wins, the preview has not consumed messages, read states, or unstable work,
   and the loop may safely build a newer/larger `Ready`.
2. Call `acceptReady(rd)` only after the send on `readyc` succeeds. This keeps
   the acceptance point tied to actual delivery to the application.
3. In synchronous storage mode, arm `advancec` after acceptance and do not
   offer another `Ready` until the application calls `Advance`. `Advance`
   continues to step the completion messages recorded in `stepsOnAdvance`.
4. In async storage mode, never arm `advancec`; clear the actor's local `rd`
   immediately and rely on ordered `MsgStorageAppend`/`MsgStorageApply`
   responses to advance stability and application progress.

All Ready construction and acceptance logic remains inside RawNode. Therefore
the following semantics remain unchanged:

- comparisons against `prevSoftSt` and `prevHardSt`;
- `MustSync` calculation;
- draining of `readStates`, `msgs`, and `msgsAfterAppend`;
- the rule that persistence-dependent messages remain subject to the Ready
  contract requiring the associated state to be persisted before they are
  sent in synchronous mode;
- async local storage/apply message construction and response ordering;
- accepting unstable entries and snapshots;
- committed-entry application accounting;
- deferred self-directed steps performed by `Advance`.

Using `Ready()` directly in `node.run` would accept work before the channel
send succeeds and is explicitly out of scope. The prepare/accept split must
remain.

## Required verification after the refactor

The acceptance command is the complete repository suite:

```sh
TMPDIR="$PWD/.tmp" GOTMPDIR="$PWD/.tmp" GOCACHE="$PWD/.gocache" go test ./...
```

If the default temporary filesystem is still full, run it with a fresh
`TMPDIR` on `/home` and record the environment workaround. No package may
regress from the successful baseline.

The following existing tests are the focused Phase 1 gate:

- Node routing and lifecycle: `TestNodeStep`, `TestNodeStepUnblock`,
  `TestNodeTick`, and `TestNodeStop`.
- Proposal gating and error propagation: `TestNodePropose`,
  `TestNodeProposeConfig`, `TestBlockProposal`,
  `TestNodeProposeWaitDropped`, and `TestNodeProposeAddDuplicateNode`.
- Config changes: `TestNodeProposeAddLearnerNode` and
  `TestRawNodeProposeAndConfChange`.
- Ready/Advance ordering and content: `TestNodeStart`, `TestNodeAdvance`,
  `TestCommitPagination`, `TestRawNodeStart`, and
  `TestRawNodeConsumeReady`.
- Restart/application cursor behavior: `TestNodeRestart`,
  `TestNodeRestartFromSnapshot`, `TestNodeCommitPaginationAfterRestart`,
  `TestRawNodeRestart`, `TestRawNodeRestartFromSnapshot`, and
  `TestRawNodeCommitPaginationAfterRestart`.
- Async storage behavior: `TestCommitPaginationWithAsyncStorageWrites`.
- RawNode filtering and status: `TestRawNodeStep` and
  `TestRawNodeStatus`.

Phase 1 should also add narrow regression coverage for behavior that is
currently implicit in `node.run`:

- an unknown-peer response delivered to a running `Node` is ignored;
- applying a config change that removes the local node preserves the current
  proposal-channel gating behavior;
- the new getters report the configured ID, async mode, and progress
  membership without mutating RawNode state;
- a Ready preview is not accepted when another select case wins, and a
  delivered Ready is accepted exactly once.

Finally, a source-level check should confirm that `node.go` contains no
`rn.raft`, no retained `*raft`, no direct tracker access, no direct
`applyConfChange`, no direct `getStatus`, and no direct read of private RawNode
fields.

## C-port follow-up additions

Phase 1 successfully made the Go Node actor depend on RawNode semantics, but
the later C ABI must track more than the methods directly used by `node.run`.
The complete audit is now in
`OUTPUT/RAWNODE_API_PARITY_FOR_C_PORT.md`.

In particular, the C surface must include Bootstrap and deprecated
TickQuiesced, distinguish BasicStatus from allocating full Status, and provide
an explicit progress enumeration equivalent to WithProgress. Full
`Status.Progress` is not a replacement for WithProgress because Go populates
the former only on leaders while the latter visits tracked replicas on all
roles. The Phase 1 `ID`, `HasProgress`, and async-mode requirements remain
scalar/cache-friendly binding requirements.
