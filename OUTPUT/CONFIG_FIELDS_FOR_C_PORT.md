# Config Fields for the C Port

This document maps the current Go `raft.Config` to the C configuration ABI and
records validation, defaulting, and phase ownership. Validation behavior is
part of compatibility: the C port must not silently choose different limits
or enable unsafe read modes.

## Conversion rule

The Go binding must validate before narrowing a Go `int` to `uint32_t` or
`size_t`. Values that cannot be represented by the C field type return
`RAFT_ERR_INVALID_ARGUMENT`; they must not wrap or truncate. Defaults should
be applied once, in a well-defined layer, so Go and C do not independently
default the same field to different values.

The recommended ownership split is:

- the Go binding validates representation and constructs callback handles;
- the C constructor validates semantic relationships and applies canonical
  defaults;
- the C core stores only the normalized values.

## Field matrix

| Go field | Current C mapping | Validation/default | Semantic notes and implementation phase |
| --- | --- | --- | --- |
| `ID uint64` | `raft_config_t.id` | Required; reject `RAFT_NONE`, `RAFT_LOCAL_APPEND_THREAD`, and `RAFT_LOCAL_APPLY_THREAD`. No default. | Immutable local member ID. Skeleton validation is required in Phase 2; tracker membership uses it in Phase 7. |
| `ElectionTick int` | `election_tick uint32_t` | Must be greater than `HeartbeatTick`; no default. Reject negative or out-of-range conversion. | Election timeout base, with randomized timeout derived by the core. Required by Phase 6. |
| `HeartbeatTick int` | `heartbeat_tick uint32_t` | Must be greater than zero; no default. | Leader heartbeat interval. Required by Phase 6. |
| `Storage Storage` | separate `raft_storage_ops_t` constructor argument | Required and non-nil. Every required callback must be present. | Skeleton checks table shape in Phase 2; Phase 4 implements `cgo.Handle`, callbacks, panic firewall, and error mapping; Phases 5-8 consume it. |
| `Applied uint64` | `applied` | Default zero. Current `Config.validate` does not range-check it, but restart initialization must reject/panic on an applied index inconsistent with the recovered log/commit state. | Prevents already-applied entries from being re-emitted after restart. Used in Phase 5/6 and tested in Phase 12. |
| `MaxSizePerMsg uint64` | `max_size_per_message` | No normalization: `math.MaxUint64` means unlimited; zero means at most one entry per append message. | Replication batching in Phase 6. Do not reinterpret zero as unlimited. |
| `MaxCommittedSizePerReady uint64` | `max_committed_size_per_ready` | If zero, default to normalized `MaxSizePerMsg`. | Limits committed-entry payload across Ready; in async mode the quota spans outstanding, unacknowledged apply messages. Phases 6 and 11. |
| `MaxUncommittedEntriesSize uint64` | `max_uncommitted_entries_size` | If zero, normalize to `math.MaxUint64` (`noLimit`). | Bounds aggregate uncommitted proposal payload. Exceeding it causes `ErrProposalDropped`; leader bookkeeping/reset belongs to Phase 6 and multinode tests to Phase 7. |
| `MaxInflightMsgs int` | `max_inflight_messages size_t` | Must be greater than zero; no default. Reject negative or unrepresentable values. | Caps optimistic append messages per follower. Required when progress/inflights are ported in Phase 7. |
| `MaxInflightBytes uint64` | `max_inflight_bytes` | Zero normalizes to `math.MaxUint64`; otherwise must be at least `MaxSizePerMsg`. | Byte complement to message-count flow control. Phase 7. |
| `CheckQuorum bool` | `check_quorum` | Default false. Required true for lease-based reads. | Leader steps down without recent quorum activity; used by PreVote lease suppression and ForgetLeader assumptions. Phase 10, with read interaction in Phase 9. |
| `PreVote bool` | `pre_vote` | Default false. | Prevents disruptive term increments by partitioned nodes. Phase 10. |
| `ReadOnlyOption` | `read_only_option uint32_t` | Numeric mapping must be `ReadOnlySafe=0`, `ReadOnlyLeaseBased=1`; reject unknown values. Lease-based requires CheckQuorum. | Safe mode confirms through quorum; lease mode relies on bounded clock behavior. Phase 9, with CheckQuorum integration in Phase 10. |
| `DisableProposalForwarding bool` | `disable_proposal_forwarding` | Default false. | Followers drop instead of forward proposals. Phase 6. |
| `AsyncStorageWrites bool` | `async_storage_writes` | Default false. | Changes the entire Ready contract: local storage messages replace direct fields/Advance. Field exists in Phase 2, real behavior belongs to Phase 11. |
| `Logger Logger` | no current C config field | Go nil defaults to package logger. | Keep Go-side for Node/binding logs initially. A C logger callback is optional; if added, use a handle, panic firewall, and no reentrancy. Skeleton consensus must not require it. |
| `DisableConfChangeValidation bool` | `disable_conf_change_validation` | Default false. | Disables best-effort propose-time checks only; apply-time configuration invariants remain mandatory. Phase 7. |
| `StepDownOnRemoval bool` | `step_down_on_removal` | Default false in current Go behavior. | Controls whether a removed/demoted leader immediately becomes follower. Phase 7, with leadership behavior verified in Phase 10. |
| `TraceLogger TraceLogger` | no current C config field | Optional; interface shape depends on `with_tla` build tag. | Documentation/test instrumentation, not consensus input. Decide in Phase 3 binding design; implement only if tracing is required. See below. |

## Current Go validation order and defaults

`Config.validate` performs the following observable sequence:

1. reject local ID zero;
2. reject local-storage target IDs;
3. require positive heartbeat tick;
4. require election tick greater than heartbeat tick;
5. require non-nil Storage;
6. normalize zero `MaxUncommittedEntriesSize` to unlimited;
7. default zero `MaxCommittedSizePerReady` to `MaxSizePerMsg`;
8. require positive `MaxInflightMsgs`;
9. normalize zero `MaxInflightBytes` to unlimited, otherwise require it to be
   at least `MaxSizePerMsg`;
10. default nil Logger;
11. require CheckQuorum for `ReadOnlyLeaseBased`.

The C port should return an explicit invalid-argument error rather than panic
for public constructor validation. Differential tests should compare the
accepted/rejected configuration set and normalized effective values, not Go's
panic mechanism.

## Storage requirements

Storage is required even in the early skeleton ABI so its lifetime can be
designed correctly, but Phase 2 must not pretend to have read it. The
Phase 4/5 tagged binding now defines and tests:

- a `runtime/cgo.Handle` owner in the Go binding;
- callback functions for InitialState, Entries, Term, FirstIndex, LastIndex,
  and Snapshot;
- exact ownership for callback outputs;
- error mapping and panic recovery;
- destruction ordering that prevents callbacks after handle deletion.

`raft_go_storage_ops_init` builds the callback table in C from the integer
handle. `raft_raw_node_new` copies that table into the opaque RawNode, so it
does not retain a pointer to the Go caller's temporary table. Focused Phase 5
tests invoke all six callbacks through the C table without requiring the
consensus core.

Phase 6 makes `raft_raw_node_new` synchronously call `FirstIndex` and
`LastIndex` to initialize its private `raft_log_t`. Later log reads use
batched `Entries`, `Term`, and `Snapshot`; all returned aggregate data is
C-owned and freed after copying/consumption. The later full Raft constructor
must additionally obtain HardState and ConfState through `InitialState`.
Arbitrary storage failures are fatal to that RawNode;
compacted/unavailable errors retain operation-specific meanings.

## Logger and `with_tla`

The build-tag behavior is easy to miss:

- under `//go:build with_tla`, `TraceLogger` has a real `TraceEvent` method and
  the core emits state-machine events;
- under `//go:build !with_tla`, `TraceLogger` is an empty interface and all
  tracing helpers are no-ops.

The C ABI must not embed a Go interface pointer. Options are:

1. keep Logger and TraceLogger entirely in the Go wrapper;
2. add optional callback tables using `cgo.Handle`;
3. compile C tracing hooks only for a corresponding tracing build.

Whichever option is chosen, default and `with_tla` builds must both compile,
and enabling tracing must not change consensus state or Ready contents.

## Phase gates

### Early skeleton

Phase 2 needs the complete fixed-width config shape, ID/tick/inflight
validation, Storage table validation, enum range validation, and a clear rule
for defaults. It may store normalized config without using consensus fields.

### Core and log phases

Phases 5-6 activate Storage, Applied, message-size limits, uncommitted-size
accounting, tick behavior, proposal forwarding, and Ready pagination.

### Membership and advanced phases

Phase 7 activates inflight flow control and config-change flags. Phase 10
activates read-only and CheckQuorum. Phase 11 activates PreVote and its
leadership-transfer/CheckQuorum interactions. The alternate async storage
protocol remains pending.

### Required tests

Use table-driven Go/C differential tests for every invalid combination and
default. Include integer conversion overflow, all reserved IDs, both read-only
modes, zero/unlimited size semantics, and restart behavior for `Applied`.

## Phase 7 implementation status

The C constructor now consumes `InitialState`, applies `Applied`, and
normalizes zero `MaxCommittedSizePerReady`, `MaxUncommittedEntriesSize`, and
`MaxInflightBytes` consistently with the documented rules. The minimal core
uses election/heartbeat ticks, `MaxSizePerMsg`,
`MaxUncommittedEntriesSize`, and `DisableProposalForwarding`.

`MaxInflightMsgs` and `MaxInflightBytes` now drive the Phase 8 tracker and
flow-control implementation. Phase 10 activates CheckQuorum and both read-only
modes; lease reads still require CheckQuorum during validation. Phase 11
activates `PreVote`. `AsyncStorageWrites` remains rejected with
`RAFT_ERR_NOT_IMPLEMENTED`.
