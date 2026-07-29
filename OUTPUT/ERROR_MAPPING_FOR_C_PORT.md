# Error Mapping for the C Port

The C ABI needs errors that describe the failing domain. In particular,
“storage entry unavailable,” “operation not implemented,” “proposal dropped,”
and “fatal callback panic” must not share a value.

This document defines the target taxonomy. Numeric assignments must be fixed
before an external C ABI is declared stable.

## Target taxonomy

| C error | Go counterpart or behavior | Exposure | Required handling |
| --- | --- | --- | --- |
| `RAFT_OK` | `nil` | Public | Operation completed according to its contract. A no-op can return OK only when no-op is the compatible semantic result. |
| `RAFT_ERR_INVALID_ARGUMENT` | Contextual Go validation error or constructor panic | Public | Invalid pointer, enum, range, reserved ID, inconsistent length/pointer pair, or invalid config relationship. Go wrapper should return a descriptive error; constructor wrappers may preserve existing panic behavior at the higher Go API if required. |
| `RAFT_ERR_STOPPED` | `ErrStopped` | Primarily Go Node/binding | Node actor has stopped or a binding-owned backend is closed. RawNode itself currently has no public Stop method; do not invent stopped consensus semantics. |
| `RAFT_ERR_PROPOSAL_DROPPED` | `ErrProposalDropped` | Public | Map by identity so callers can fail fast. Includes proposal rejection from leader state/membership/transfer/uncommitted-size rules. |
| `RAFT_ERR_STORAGE_COMPACTED` | `ErrCompacted` | Storage callback/core and sometimes wrapper | Requested log position predates retained storage. Preserve for log fallback logic; it is not fatal by itself in every call site. |
| `RAFT_ERR_STORAGE_UNAVAILABLE` | `ErrUnavailable` | Storage callback/core | Requested log entry/range is unavailable. Never use this value for an unimplemented C stub. |
| `RAFT_ERR_SNAPSHOT_TEMPORARILY_UNAVAILABLE` | `ErrSnapshotTemporarilyUnavailable` | Storage callback/core | Snapshot preparation can be retried later; not a permanent fatal error. |
| `RAFT_ERR_STEP_LOCAL_MSG` | `ErrStepLocalMsg` | Public RawNode Step | Application/network attempted to step a local-only message without an allowed local target/path. Node's internal step path remains separate. |
| `RAFT_ERR_STEP_PEER_NOT_FOUND_OR_IGNORED` | `ErrStepPeerNotFound` on public RawNode Step; ignored by Node actor where current behavior does so | Public plus compatibility handling | Preserve the public error identity. The Node receive path may intentionally swallow it. Core paths that historically drop unknown senders should remain no-op-compatible rather than manufacturing an error. |
| `RAFT_ERR_PANIC_FROM_GO_CALLBACK` | Go panic recovered at cgo boundary | Internal callback result propagated to outer public call | Capture diagnostic/stack where possible, mark RawNode unusable, unwind without crossing C, and surface a fatal wrapper error. Never continue as if callback succeeded. |
| `RAFT_ERR_OUT_OF_MEMORY` | No existing Go sentinel | Public C allocation failure; wrapper may expose a dedicated Go error | No partial success. Free partial output. Policy must decide whether the Go integration returns an error or treats OOM as fatal, but it must not label it a Go panic or storage unavailable. |
| `RAFT_ERR_FATAL` | Arbitrary fatal Storage error, invariant violation converted at a boundary, corrupted handle/state | Internal and public propagation | Mark node/backend inoperable and require teardown/recovery. Preserve original diagnostic in Go-side state/logging when possible. |

Additional useful codes before ABI freeze:

- `RAFT_ERR_NOT_IMPLEMENTED` for compile-safe skeleton functions;
- `RAFT_ERR_STORAGE_SNAPSHOT_OUT_OF_DATE` if C exposes snapshot-creation
  operations corresponding to Go `ErrSnapOutOfDate`;
- a distinct stale-Ready/generation error if preview/accept tokens can be
  misused by native C callers.

## Skeleton taxonomy resolution

The expanded Phase 2 header now uses the target names directly:

| Earlier name/gap | Resolution in the current skeleton |
| --- | --- |
| `RAFT_ERR_COMPACTED` | Replaced by `RAFT_ERR_STORAGE_COMPACTED`. |
| ambiguous `RAFT_ERR_UNAVAILABLE` | Split into `RAFT_ERR_STORAGE_UNAVAILABLE` and `RAFT_ERR_NOT_IMPLEMENTED`. |
| `RAFT_ERR_LOCAL_MESSAGE` | Replaced by `RAFT_ERR_STEP_LOCAL_MSG`. |
| no peer-not-found code | Added `RAFT_ERR_STEP_PEER_NOT_FOUND_OR_IGNORED`. |
| generic `RAFT_ERR_STORAGE` | Replaced by storage-specific values plus `RAFT_ERR_FATAL`. |
| generic `RAFT_ERR_PANIC` | Split into `RAFT_ERR_PANIC_FROM_GO_CALLBACK` and `RAFT_ERR_FATAL`. |
| no OOM code | Added `RAFT_ERR_OUT_OF_MEMORY`. |

No external binding had consumed the earlier skeleton ABI, so the enum was
replaced directly and the private skeleton ABI marker was advanced. Future
changes must preserve or explicitly version the numeric assignments.

## Go wrapper conversion

The Phase 3 binding should implement one centralized conversion function and
return existing Go sentinel errors by identity where they exist:

```text
RAFT_OK                              -> nil
RAFT_ERR_STOPPED                     -> raft.ErrStopped
RAFT_ERR_PROPOSAL_DROPPED            -> raft.ErrProposalDropped
RAFT_ERR_STORAGE_COMPACTED           -> raft.ErrCompacted
RAFT_ERR_STORAGE_UNAVAILABLE         -> raft.ErrUnavailable
RAFT_ERR_SNAPSHOT_TEMPORARILY_UNAVAILABLE
                                     -> raft.ErrSnapshotTemporarilyUnavailable
RAFT_ERR_STEP_LOCAL_MSG              -> raft.ErrStepLocalMsg
RAFT_ERR_STEP_PEER_NOT_FOUND_OR_IGNORED
                                     -> raft.ErrStepPeerNotFound
```

Invalid argument, not implemented, OOM, callback panic, and fatal failures need
dedicated Go wrapper errors carrying context. Do not map them to one of the
existing storage sentinels merely because no exact Go sentinel exists.

For C APIs with output parameters, any non-OK result means:

- the output is either reset/empty or valid solely for its documented free
  function;
- the Go wrapper must not consume partial data;
- partial C allocations must be freed.

## Callback rules

Storage callbacks return storage-domain errors only:

- `ErrCompacted` -> storage compacted;
- `ErrUnavailable` -> storage unavailable;
- `ErrSnapshotTemporarilyUnavailable` -> temporary snapshot unavailable;
- any other error -> fatal, with Go diagnostic retained;
- panic -> panic-from-Go-callback.

The Phase 5 Go bridge implements and tests these mappings through the actual C
callback table. A nil callback output pointer, nil ConfState from
InitialState, nil Entry element, or nil Snapshot result is
`RAFT_ERR_INVALID_ARGUMENT`. Allocation failure is
`RAFT_ERR_OUT_OF_MEMORY`. Every panic is recovered in the exported callback,
partial owned output is freed/reset, and the panic diagnostic is retained on
the Go bridge when its handle is still valid.

The C core must propagate callback failures to a public call or store a fatal
node state that the next public call reports. It must not drop the error and
continue participating in elections.

### Cgo-safe Storage result ownership

Exported Go Storage callbacks must not return Go-owned Entry/Snapshot memory
for C to retain. Before returning, the callback allocates the result
descriptor graph and payloads in C-owned memory and deep-copies the Go result.
The C core receives owned non-view types and frees them when done.

- InitialState writes fixed scalar state and a C-owned ConfState vector graph.
- Entries returns one C-owned `raft_entry_t` array for the requested range in
  a single callback; it never calls Go once per entry.
- Term, FirstIndex, and LastIndex return scalars.
- Snapshot returns one C-owned `raft_snapshot_t` graph.

No raw Go pointer may remain in any callback result. Allocation failure while
building a callback result maps to `RAFT_ERR_OUT_OF_MEMORY`, with partial C
output freed before returning. A panic remains
`RAFT_ERR_PANIC_FROM_GO_CALLBACK`; it is not an allocation or Storage-domain
error.

## Compatible no-op and drop behavior

Not every ignored input is an error. Differential compatibility requires:

- unknown or invalid leadership transferee messages are often dropped with no
  state change by the Go core; the strict public C convenience API may reject
  reserved IDs before message construction;
- `ConfChange.NodeID == 0` can be an explicit cancellation/no-op marker in the
  internal apply path;
- leader `ForgetLeader`, follower ForgetLeader under lease reads, and
  SnapshotFinish reporting can be no-ops;
- a response from an unknown peer produces `ErrStepPeerNotFound` through
  public RawNode Step, while the Go Node actor's internal receive path ignores
  the result;
- proposal dropping remains an explicit error where Go returns
  `ErrProposalDropped`.

Tests must compare state changes and returned errors separately. “No state
change” does not by itself determine whether an API should return OK.

## Internal-only versus wrapper-visible

| Category | Policy |
| --- | --- |
| Consensus-compatible no-op | Normally `RAFT_OK`; verify exact Go path. |
| Public misuse | `RAFT_ERR_INVALID_ARGUMENT` or a specific Step error. |
| Storage recoverable condition | Specific storage code, interpreted by log/core. Expose only if it reaches the wrapper operation. |
| Node lifecycle stop | `RAFT_ERR_STOPPED`, often synthesized by Go actor rather than C RawNode. |
| Callback panic/invariant/fatal storage | Mark backend fatal and expose a fatal diagnostic. |
| OOM | Explicit OOM; clean partial allocations. |
| Skeleton-only missing behavior | `RAFT_ERR_NOT_IMPLEMENTED`; never `RAFT_OK` or storage unavailable. |

## Required tests

- exhaustive C-code-to-Go-error mapping;
- preservation of Go sentinel identity with `errors.Is`/direct comparison as
  appropriate;
- arbitrary storage error and callback panic fatalization;
- no panic crossing the C ABI;
- OOM injection at every allocating API;
- public Step versus Node-internal unknown-peer behavior;
- config-change zero no-op and strict public validation;
- unknown transfer target differential behavior;
- no unimplemented stub returning a storage-domain error.

For Step errors, the call layering is:

```text
raft_raw_node_step
  -> public local-message and unknown-peer-response validation
  -> raft_raw_node_step_for_node
       -> direct core Step
```

`RAFT_ERR_STEP_LOCAL_MSG` and
`RAFT_ERR_STEP_PEER_NOT_FOUND_OR_IGNORED` belong to the public boundary.
`raft_raw_node_step_for_node` is the Go Node actor helper and must not call the
public function. The Node actor performs its own routing/filtering and invokes
the lower-level helper so legitimate local/internal messages are not rejected.
