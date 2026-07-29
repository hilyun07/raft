# Sentinel Node IDs for the C Port

This document records the node-ID sentinel conventions in the current Go
implementation and proposes an explicit C representation. It is a design
document only; it does not change Go or C production code.

The source inventory below is based on commit
`64d31dd6558fe87e313d99ebfbf8574882c27fab`. Line numbers refer to that
source/worktree.

## Go sentinel constants

The Go implementation declares three `uint64` node-ID sentinels in
`raft.go:36-47`:

| Go name | Numeric value | Meaning |
| --- | ---: | --- |
| `None` | `0` | No real node: for example, no known leader, no vote, or no active leadership-transfer target. |
| `LocalAppendThread` | `math.MaxUint64` (`18446744073709551615`, `0xffffffffffffffff`) | The local stable-storage append worker used by asynchronous storage messages. |
| `LocalApplyThread` | `math.MaxUint64 - 1` (`18446744073709551614`, `0xfffffffffffffffe`) | The local state-machine apply worker used by asynchronous storage messages. |

`IsLocalMsgTarget` in `util.go:67-69` recognizes exactly
`LocalAppendThread` and `LocalApplyThread`. `None` is not a local-message
target.

The local targets are process-internal routing endpoints. They are not Raft
members and must never be sent over the cluster transport as peer IDs.

## Node-ID invariant and current enforcement

A real Raft node ID must satisfy all of the following:

- it is not `None` (`0`);
- it is not `LocalAppendThread`;
- it is not `LocalApplyThread`.

Equivalently, the real-node domain in the current `uint64` representation is
`[1, math.MaxUint64-2]`.

`Config.validate` in `raft.go:293-300` currently enforces both restrictions
for the local `Config.ID`: it rejects `None`, then rejects any ID accepted by
`IsLocalMsgTarget`. `newRaft` calls this validation and panics if it fails.
The future C constructor should report this as
`RAFT_ERR_INVALID_ARGUMENT` instead of relying on a panic.

This validation is not currently centralized for every value that can become
a member ID:

- `RawNode.Bootstrap` in `bootstrap.go:32-79` converts each `Peer.ID` into a
  configuration change without first applying `Config.validate`.
- `confchange.Changer.apply` in `confchange/confchange.go:150-158`
  deliberately ignores `NodeId == 0`. This preserves an etcd convention in
  which a downstream component zeroes a configuration-change node ID to mark
  the change as not applied.
- The configuration-change code does not explicitly reject
  `LocalAppendThread` or `LocalApplyThread`.
- Restored `ConfState` member lists pass through the configuration-change
  machinery and likewise do not have one centralized reserved-ID check.

Therefore, “configuration validation enforces the restrictions” is exactly
true for the local `Config.ID`. The invariant should be made explicit and
enforced more broadly at the C public-API boundary. The Go zero-ID
configuration-change compatibility behavior must still be preserved inside a
Go-compatible core or differential-test adapter.

## `ConfChange.NodeID == 0` is a distinct compatibility convention

Although `ConfChange.NodeID` is syntactically a node-ID field, zero has an
additional app-visible meaning in the configuration-change workflow. An
application may reject or cancel a committed legacy configuration change by
treating it as a no-op. Current etcd integration can represent that decision
downstream by replacing the change's `NodeID` with zero, and
`confchange.Changer.apply` deliberately ignores such a change
(`confchange/confchange.go:150-158`).

This convention must not be generalized:

- a real member ID is still never zero;
- `RAFT_NONE` remains the C spelling for “no node” in leader, vote,
  transferee, and local-origin message fields;
- `ConfChange.NodeID == 0` means “this configuration-change operation was
  cancelled/rejected; do not alter membership” at the narrow compatibility
  boundary;
- zero must never be inserted into voters, learners, progress tracking, or
  snapshot `ConfState`;
- the preferred application behavior remains not calling `ApplyConfChange`
  when it rejects a committed change. The zero-ID rule exists for compatibility
  with paths that pass an explicit no-op change downstream.

C comments should name the distinction directly. For example:

```c
// RAFT_NONE is the general node-ID sentinel. In this compatibility-only
// config-change path, node_id == RAFT_NONE specifically means that the
// application rejected/cancelled the change, so membership is unchanged.
```

The strict public C constructor and typed membership APIs should reject
`RAFT_NONE` as a proposed real member. A separate internal decode/apply path
may accept zero as the legacy cancellation marker for differential
compatibility. It must not call the ordinary real-member validator and then
silently reinterpret arbitrary validation failures as cancellation.

## Meaning and production uses of `None`

`None` is meaningful only when a `uint64` is being used as a node ID. Its
meaning depends on the field or operation.

### Raft state fields

| Field | Meaning when equal to `None` | Important locations |
| --- | --- | --- |
| `raft.lead` | No leader is currently known. | Initialized at `raft.go:451`; cleared by `reset` at `raft.go:786`; queried by `hasLeader` at `raft.go:500`; exposed as `SoftState.Lead`. |
| `raft.Vote` | This node has not voted in the current term. | Cleared when `reset` changes the term at `raft.go:781-785`; checked together with `lead == None` when deciding whether a vote can be granted at `raft.go:1214-1218`; persisted in `HardState.Vote`. |
| `raft.leadTransferee` | No leadership transfer is active. | Tested by the transfer timeout and proposal path at `raft.go:874` and `raft.go:1304`; tested when replacing an active transfer at `raft.go:1641-1650`; cleared by `abortLeaderTransfer` at `raft.go:2061-2063`. |

`switchToConfig` at `raft.go:2030` tests `leadTransferee != 0` rather than
`leadTransferee != None`. It is the same node-ID sentinel check and should use
the named C constant in the port.

`reset` always clears the known leader and aborts any leadership transfer. It
only clears `Vote` when the term changes, preserving a vote within the same
term.

### Leader discovery and state transitions

`becomeFollower(term, None)` means “become a follower without learning a
leader.” Important call sites are:

- initial construction in `raft.go:487`;
- bootstrap at term 1 in `bootstrap.go:52`;
- receipt of a higher-term message that is not `MsgApp`, `MsgHeartbeat`, or
  `MsgSnap`, at `raft.go:1129`;
- leader step-down after a failed quorum check, at `raft.go:1284`;
- a candidate losing an election, at `raft.go:1710`;
- defensive snapshot restoration by a non-follower, at `raft.go:1873`;
- configured leader removal/demotion when step-down-on-removal is enabled, at
  `raft.go:2003`.

By contrast, receiving `MsgApp`, `MsgHeartbeat`, or `MsgSnap` from a leader
passes `m.From` to `becomeFollower`, because that message identifies the
leader.

Other leader-related uses are:

- `becomePreCandidate` explicitly clears `lead` at `raft.go:928`.
- Lease suppression of disruptive votes requires `lead != None` at
  `raft.go:1103`.
- Followers drop proposals, leadership-transfer forwarding, and read-index
  forwarding when `lead == None`, at `raft.go:1721`, `raft.go:1743`, and
  `raft.go:1765`.
- A leader blocks new proposals while `leadTransferee != None`, at
  `raft.go:1304`.
- The election-timeout path aborts a transfer when
  `leadTransferee != None`, at `raft.go:874`.

### `ForgetLeader`

`RawNode.ForgetLeader` creates a `MsgForgetLeader` at
`rawnode.go:573-576`. On a follower, `stepFollower` clears `raft.lead` to
`None` at `raft.go:1748-1757`. The operation is:

- a no-op on a leader (`raft.go:1370-1371`);
- rejected as an operational no-op in `ReadOnlyLeaseBased` mode, because
  forgetting the leader would violate the lease-read assumptions;
- otherwise a no-op when the follower already has `lead == None`.

The Node actor separately caches a leader ID as `lead := None` in
`node.go:355`. It updates that cache from `BasicStatus.Lead` and uses
`None` to control whether proposal input is enabled (`node.go:373-379`).

### `Message.From == None`

The protobuf fields are optional pointers, but generated `GetFrom()` returns
zero both when the field is absent and when it is explicitly set to zero.
Consequently, `Message.From == None` has context-sensitive meanings:

- For a locally created message, it commonly means “sender not stamped yet.”
  `raft.send` fills `Message.From` with `raft.id` when `GetFrom() == None`
  (`raft.go:514-517`).
- `RawNode.ReadIndex` creates `MsgReadIndex` without `From`
  (`rawnode.go:580-588`). In the read-index response path this means that the
  request originated locally.
- Other RawNode/Node operations can construct local protocol messages whose
  sender is supplied by the wrapper or by `raft.send`, depending on the path.
- It must not be interpreted as a real remote sender.

The C port must not apply `raft_is_valid_node_id(message.from)` blindly to
every input message. Local-origin operations need an explicit way to carry the
unset/local-origin state, while inbound network messages should require a
real sender ID.

### `Message.To == None`

`Message.To == None` does not identify an outbound peer.

The clearest deliberate use is `responseToReadIndexReq` at
`raft.go:2072-2086`:

- if `req.From == None` or `req.From == raft.id`, the function appends a local
  `ReadState` and returns an empty `Message`;
- that empty message has `To == None`;
- callers at `raft.go:1357`, `raft.go:1607`, and `raft.go:2158` send the
  response only when `resp.To != None`.

Thus `To == None` can mean “there is no outbound response.” It can also simply
be unset on a local input whose operation does not need a destination.
`raft.send` fills `From`, but it does not fill `To`; an emitted network
message must already have a real destination.

### Voting

`Vote == None` means that no vote has been cast in the current term. The vote
granting condition at `raft.go:1214-1218` permits a new vote when both:

```text
Vote == None && lead == None
```

This is separate from voting for node ID zero: node ID zero is not a valid
candidate. A real vote sets `Vote` to the candidate's non-sentinel node ID;
becoming a candidate sets it to the local `raft.id`.

### Leadership transfer

An active transfer is represented by a real member ID in
`leadTransferee`; `None` means inactive. Starting a transfer stores the
target at `raft.go:1641-1668`, and `abortLeaderTransfer` restores `None`.

`RawNode.TransferLeader(transferee)` encodes the requested transferee in
`MsgTransferLeader.From` (`rawnode.go:568-571`). On a leader,
`stepLeader` first looks up progress for `m.From`. If no progress exists, it
logs and returns `nil` before reaching the transfer logic
(`raft.go:1374-1380`). Under the real-ID invariant, transferee zero therefore
behaves like any unknown member: the core drops it as a no-op.

This behavior creates a deliberate public-API choice for C:

- the C public `raft_raw_node_transfer_leader` function may reject
  `RAFT_NONE` with `RAFT_ERR_INVALID_ARGUMENT`, giving callers an immediate
  and unambiguous result;
- the internal C core message handler should retain the Go-compatible
  unknown-member no-op behavior, including for a zero transferee, when used
  by differential tests;
- an adapter comparing public Go and C wrappers must account for the stricter
  C precondition, or the public C wrapper may need an explicit compatibility
  mode.

The current C skeleton already rejects a zero transferee. This document
proposes retaining that public behavior while keeping the future core
semantics compatible with Go.

## Uses of the local target IDs

The two high-valued sentinels are used only for asynchronous local-storage
routing:

| Message path | `From` | `To` | Source |
| --- | --- | --- | --- |
| `MsgStorageAppend` request | real local node ID | `LocalAppendThread` | `rawnode.go:249-281` |
| `MsgStorageAppendResp` | `LocalAppendThread` | real local node ID | `rawnode.go:289-315` |
| `MsgStorageApply` request | real local node ID | `LocalApplyThread` | `rawnode.go:397-410` |
| `MsgStorageApplyResp` | `LocalApplyThread` | real local node ID | `rawnode.go:413-425` |

`RawNode.Step` and `Node.Step` allow local message types only when the sender
is one of these local targets (`rawnode.go:139-142`,
`node.go:485-488`). `RawNode.Step` also exempts local-target senders from the
normal “known peer” check for response messages (`rawnode.go:149-152`).

These IDs are valid internal message endpoints only in the prescribed
storage-message directions. They are never valid values for `Config.ID`,
cluster membership, a transport peer, a vote, a known leader, or a
leadership-transfer target.

### `WithProgress` IDs

Go `RawNode.WithProgress` visits the progress tracker and reports each tracked
replica as either `ProgressTypePeer` or `ProgressTypeLearner`
(`rawnode.go:542-553`). Every reported key must therefore be a real tracked
member ID.

The C `raft_raw_node_progress_snapshot` API and full status snapshot must not
report any of the following as a normal progress ID:

- `RAFT_NONE`;
- `RAFT_LOCAL_APPEND_THREAD`;
- `RAFT_LOCAL_APPLY_THREAD`.

Finding one of these values in progress state is an internal invariant
violation, not an item for callers to filter out. The C core should reject the
reserved ID before tracker insertion, and C tests should assert that progress
enumeration never emits it. This rule applies to peers and learners alike.
The progress snapshot is a C-owned copy; a C-to-Go visitor callback is not
part of this boundary.

## Zero values that are not `None`

`None` is not a universal spelling for numeric zero. It applies only to
node-ID fields.

| Zero-valued item | Meaning |
| --- | --- |
| `MessageType(0)` | `MsgHup`, a valid local Raft message type (`raftpb/raft.proto:32-33`). It is not `None`. |
| `EntryType(0)` | `EntryNormal`, a valid log-entry type (`raftpb/raft.proto:6-7`). It is not `None`. |
| `Message.Term == 0` | Conventionally marks a local message in `raft.Step` (`raft.go:1097-1100`). Some generated asynchronous apply messages also use term zero because committed entries do not apply under a specific term (`rawnode.go:404`, `rawnode.go:419`). |
| `HardState.Term == 0` or entry term zero | May represent initial/empty state or a message-specific absence. It is not a node-ID sentinel. |
| `Index == 0` | Has log-, storage-, or message-specific meaning. For example, a storage-append response checks `Index != 0` before stabilizing an entry (`raft.go:1197-1200`). It is not a node-ID sentinel. |
| Snapshot metadata index/term zero | `IsEmptySnap` defines an empty snapshot by metadata index zero (`node.go:127-129`). This is snapshot emptiness, not `None`. |
| Zero-valued protobuf scalar getter | May mean that an optional field was absent. It must be interpreted according to that field's domain; only node-ID fields can denote `None`. |

The C port should preserve each field's independent domain semantics instead
of introducing a generic “zero means none” rule.

## Proposed C constants

Add fixed-width macros to the public C header:

```c
// Reserved values in the uint64_t node-ID namespace.
//
// RAFT_NONE means "no node" only in fields whose documented domain is a
// node ID. Numeric zero in terms, indexes, enum values, and snapshot metadata
// has separate field-specific semantics.
//
// The two RAFT_LOCAL_* values are process-local asynchronous-storage
// endpoints. They are never cluster members or network transport peers.
#define RAFT_NONE UINT64_C(0)
#define RAFT_LOCAL_APPEND_THREAD UINT64_MAX
#define RAFT_LOCAL_APPLY_THREAD (UINT64_MAX - UINT64_C(1))
```

Macros or typed `static const uint64_t` values are preferable to a C `enum`
because the local-target constants may not fit the implementation's enum
integer type.

The names intentionally mirror the Go constants. `RAFT_NONE` should be used
only where the value is in the node-ID namespace; ordinary numeric-zero checks
for terms, indexes, enum values, and lengths should remain ordinary
field-specific checks.

## C helpers

The public header exposes typed helper declarations backed by linkable C
functions:

```c
bool raft_is_none_id(uint64_t id);
bool raft_is_local_target_id(uint64_t id);
bool raft_is_valid_node_id(uint64_t id);
```

The name `raft_is_valid_node_id` means “valid in the real-node numeric
domain,” not “known member,” “current voter,” or “valid message endpoint.”
Those properties require separate configuration- and message-aware checks.

## Proposed C public-API validation

Validation should occur before input is retained or passed into the future C
core.

| Boundary | Proposed rule |
| --- | --- |
| `raft_config_t.id` | Require `raft_is_valid_node_id(id)`. Return `RAFT_ERR_INVALID_ARGUMENT` for `RAFT_NONE` or either local target. |
| Bootstrap/initial peer IDs | Require every peer ID to be a valid real node ID. Reject reserved IDs before creating entries or progress records. Duplicate-ID validation is separate but should occur at the same boundary. |
| Restored `raft_conf_state_t` membership | Reject reserved IDs in voters, outgoing voters, learners, and learners-next before constructing the tracker. |
| Add-node/add-learner/update-node changes | Require a valid real node ID. Local targets and `RAFT_NONE` must never enter membership. |
| Remove-node changes | Public typed APIs should reject reserved IDs because neither names a real removable member. The internal compatibility path may treat zero as the Go no-op marker described below. |
| Leave-joint change | An empty change list has no node ID to validate. Do not invent a zero member ID for this operation. |
| `raft_raw_node_transfer_leader` | Reject `RAFT_NONE` and local targets at the public API. Preserve the core's no-op behavior for unknown members for Go compatibility. |
| Peer-reporting APIs | `report_unreachable` and `report_snapshot` should require a valid real node ID; a subsequent membership lookup may still treat a valid-but-unknown node as a no-op, matching core behavior where required. |
| Inbound network message | Require real `From` and `To` IDs at the transport-facing boundary. Do not accept local targets from the network. |
| Internal/local-origin message | Permit `From == RAFT_NONE` or `To == RAFT_NONE` only for documented local construction paths. Permit local targets only for the prescribed asynchronous storage message types and directions. |

### Zero-valued membership-change compatibility

There are two useful layers with different contracts:

1. The public C API should reject a single membership change whose `node_id`
   is `RAFT_NONE`. This catches caller mistakes and prevents zero from becoming
   membership state.
2. The internal configuration-change application path should be capable of
   consuming the Go compatibility representation in which `node_id == 0`
   means “ignore this change.” Differential tests that feed identical encoded
   Go configuration changes require this behavior.

The compatibility exception should be narrow and documented. It must not make
zero a valid peer ID, and it must not apply to the local-target sentinels.

### Transfer-leadership compatibility

The public/core split is similarly useful for transfers:

- public C call with `transferee == RAFT_NONE`: return
  `RAFT_ERR_INVALID_ARGUMENT`;
- public C call with a local-target transferee: return
  `RAFT_ERR_INVALID_ARGUMENT`;
- core handling of a syntactically valid transfer message for an unknown
  member, including a compatibility-injected zero sender: return success and
  make no state change, matching Go;
- known learner or self transferee: preserve the existing Go no-op behavior;
- known eligible follower: begin the normal transfer.

Differential tests should test core compatibility separately from the
stricter public-wrapper validation.

## Commenting rule for future C code

Place the proposed comment block immediately beside the public constants, and
repeat a short field-specific comment where a zero node ID has non-obvious
control-flow meaning. In particular:

- write `id == RAFT_NONE` only for node-ID values;
- do not replace `term == 0`, `index == 0`, enum-zero checks, or empty snapshot
  checks with `RAFT_NONE`;
- use `raft_is_local_target_id` for internal routing checks;
- use `raft_is_valid_node_id` at real-member API boundaries;
- add membership/current-progress checks separately where required.

This keeps “numeric zero” from becoming an untyped, global sentinel concept.

## Implementation status and remaining follow-up

The public C header now contains the three constants and helper declarations,
the C skeleton provides their implementations, and
the current skeleton uses `raft_is_valid_node_id` at its implemented
real-member validation boundaries. Remaining follow-up work is to:

1. add reserved-ID validation utilities for restored configuration states and
   configuration changes without implementing consensus behavior;
2. preserve a narrow internal `ConfChange.NodeID == 0` cancellation path while
   keeping strict public membership validation;
3. ensure the future progress tracker and progress-enumeration API reject all
   reserved IDs;
4. add differential tests for zero-valued
   configuration-change no-ops, unknown transferees, local ReadIndex origin,
   empty read-index responses, and asynchronous storage routing.

No production change is made by this documentation update.
