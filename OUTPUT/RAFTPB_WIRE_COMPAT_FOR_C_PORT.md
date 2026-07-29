# raftpb Wire Compatibility for the C Port

The C structs are an in-process ABI, but the Go binding sits on a protocol
boundary whose canonical representation remains `raftpb`. Converting through
C must preserve the protobuf field meanings, enum numbers, optional-field
behavior, and opaque payload bytes. The C port must not define a subtly
different Raft wire protocol.

## Compatibility rule

At the Go binding boundary:

- inbound Go `raftpb` values are converted to semantically equivalent C
  values;
- outbound C values are deep-copied into ordinary Go-owned `raftpb` values;
- values serialized by Go before and after a C round trip must retain
  compatible field values and enum numbers;
- C enum names may differ, but their numeric values must not;
- unknown future protobuf enum values must be rejected explicitly or
  preserved by a documented forward-compatibility policy, never silently
  remapped.

Direct protobuf serialization in C is optional. If C never serializes
protobuf, the Go binding remains responsible for exact protobuf encoding.

## Type mapping

| Go/protobuf type | C representation requirement |
| --- | --- |
| `raftpb.Message` | Preserve type, to/from IDs, term, log term, index, commit, vote, reject fields, entries, optional snapshot, context, and nested responses. Local storage response lists are part of semantics, not transport-only metadata. |
| `raftpb.Entry` | Preserve type, term, index, and opaque Data bytes. |
| `raftpb.Snapshot` | Preserve Data plus metadata index, term, and full ConfState. Handle nil, empty, and older-peer non-nil empty snapshots. |
| `raftpb.HardState` | Preserve term, vote, and commit plus whether a Ready/message contains an update. A zero-valued state is not automatically an update. |
| `SoftState` | Not a `raftpb` wire message. Represent separately in the C Ready/status ABI with leader ID and Raft state; presence in Ready means “changed.” |
| `raftpb.ConfState` | Preserve voters, learners, outgoing voters, learners-next, and auto-leave. Collection ordering should be deterministic when converted from C sets. |
| `raftpb.ConfChange` | Preserve legacy entry type, type, node ID, Context, and legacy `ID` field. Do not silently convert every V1 change into V2 if doing so changes entry encoding observed by applications. |
| `raftpb.ConfChangeV2` | Preserve transition, ordered change list, and Context. An empty change list with automatic transition is the leave-joint representation. |
| `raftpb.ConfChangeSingle` | Preserve operation type and node ID. Zero node ID has the narrow cancellation/no-op compatibility described in `SENTINEL_NODE_IDS_FOR_C_PORT.md`. |
| `SnapshotStatus` | This is a Go raft API enum rather than a protobuf enum. Preserve `SnapshotFinish=1` and `SnapshotFailure=2` in the C API. |

The C skeleton now distinguishes legacy `raft_conf_change_view_t`, including
its legacy `id`, from `raft_conf_change_v2_view_t`. Phase 3 must still decide whether
the binding passes these structured forms through C or keeps protobuf
marshalling in Go, and must preserve the corresponding `EntryConfChange`
versus `EntryConfChangeV2` encoding.

## Required numeric values

### `MessageType`

| Value | raftpb name |
| ---: | --- |
| 0 | `MsgHup` |
| 1 | `MsgBeat` |
| 2 | `MsgProp` |
| 3 | `MsgApp` |
| 4 | `MsgAppResp` |
| 5 | `MsgVote` |
| 6 | `MsgVoteResp` |
| 7 | `MsgSnap` |
| 8 | `MsgHeartbeat` |
| 9 | `MsgHeartbeatResp` |
| 10 | `MsgUnreachable` |
| 11 | `MsgSnapStatus` |
| 12 | `MsgCheckQuorum` |
| 13 | `MsgTransferLeader` |
| 14 | `MsgTimeoutNow` |
| 15 | `MsgReadIndex` |
| 16 | `MsgReadIndexResp` |
| 17 | `MsgPreVote` |
| 18 | `MsgPreVoteResp` |
| 19 | `MsgStorageAppend` |
| 20 | `MsgStorageAppendResp` |
| 21 | `MsgStorageApply` |
| 22 | `MsgStorageApplyResp` |
| 23 | `MsgForgetLeader` |

Message type zero is valid `MsgHup`; it is not `RAFT_NONE`.

### `EntryType`

| Value | raftpb name |
| ---: | --- |
| 0 | `EntryNormal` |
| 1 | `EntryConfChange` |
| 2 | `EntryConfChangeV2` |

Entry type zero is valid `EntryNormal`; it is not `RAFT_NONE`.

### `ConfChangeType`

| Value | raftpb name |
| ---: | --- |
| 0 | `ConfChangeAddNode` |
| 1 | `ConfChangeRemoveNode` |
| 2 | `ConfChangeUpdateNode` |
| 3 | `ConfChangeAddLearnerNode` |

### `ConfChangeTransition`

| Value | raftpb name |
| ---: | --- |
| 0 | `ConfChangeTransitionAuto` |
| 1 | `ConfChangeTransitionJointImplicit` |
| 2 | `ConfChangeTransitionJointExplicit` |

The transition changes joint-consensus behavior. It is not merely descriptive
metadata and must be validated rather than defaulted for unknown numeric
values.

### `SnapshotStatus`

| Value | Go/C name |
| ---: | --- |
| 1 | `SnapshotFinish` / `RAFT_SNAPSHOT_FINISH` |
| 2 | `SnapshotFailure` / `RAFT_SNAPSHOT_FAILURE` |

## Optional fields and presence

`raft.proto` uses proto2 optional fields. A generated Go getter returns a zero
value for both absent and explicitly-zero scalars, but presence still matters
in several construction and serialization paths.

Examples:

- `Ready.HardState` has presence separate from its numeric contents.
- `MsgStorageAppend` sets term, vote, and commit together when any HardState
  field changed and leaves all three unset otherwise.
- Message snapshot is expected to be non-nil/non-empty for `MsgSnap`, but
  older peers may send a non-nil empty snapshot on other message types.
- `Message.From` and `Message.To` zero have local-origin/empty-response
  meanings on specific internal paths.

The C ABI needs explicit `has_*` bits or message-type-specific conversion
rules wherever presence is observable. Plain zero-initialized scalar structs
are insufficient for lossless generic conversion.

## Nil versus empty byte payloads

The public ABI uses two wrapper types instead of repeating
`(pointer, length, is_nil)` parameters:

```c
typedef struct raft_byte_view {
    const uint8_t *data;
    size_t len;
    bool is_nil;
} raft_byte_view_t;

typedef struct raft_bytes {
    uint8_t *data;
    size_t len;
    bool is_nil;
} raft_bytes_t;
```

Public C APIs do not pass structs by value. A borrowed input is supplied as
`const raft_byte_view_t *`; `raft_bytes_t` represents returned or otherwise
owned bytes inside internal/output structures. Ownership is fixed by the C
type: view data is borrowed and never freed or retained; non-view bytes are
C-owned and always freed by the owned free path. There is no ownership flag,
kind, or view/owned runtime tag. `is_nil` preserves the Go `nil []byte`
distinction only.

The canonical forms are:

| Go shape | `data` | `len` | `is_nil` |
| --- | --- | ---: | --- |
| nil | `NULL` | 0 | `true` |
| empty, non-nil | `NULL` | 0 | `false` |
| non-empty | non-null | positive | `false` |

No dummy zero-length allocation is needed or accepted. The shared
`raft_byte_view_valid` and `raft_bytes_valid` helpers reject at least a null
pointer with a positive length, reject noncanonical non-null zero-length
values, and reject `is_nil` combined with payload data. The wrapper
design reduces repeated parameters and signature drift while applying one
nil/empty policy across all payload-bearing types.

Descriptor pointer nullness is not payload presence. A null
`const raft_byte_view_t *` is an invalid argument unless a particular API
explicitly says it is optional. Go nil is represented by a non-null descriptor
in the nil row above; Go empty-but-non-nil is represented by a non-null
descriptor in the empty row.

The same separation applies recursively. Go-to-C protocol input uses
`raft_entry_view_t`, `raft_snapshot_view_t`, and `raft_message_view_t`;
retained state, Storage output, Ready, and Status use `raft_entry_t`,
`raft_snapshot_t`, and `raft_message_t`. Copy-from-view helpers recursively
allocate owned bytes and arrays, so no view pointer enters retained state.

The Go binding must not allocate pointer-containing view descriptor structs in
Go memory and pass their addresses to C. Scalar-part C shims construct simple
views in C storage. Complex Entry/Snapshot/Message, Peer, and ConfChange input
uses one temporary descriptor graph allocated in C memory, one RawNode call,
and immediate cleanup. The binding must not use Go-resident descriptors or
field-by-field builder calls. If the C core retains input, it first recursively
copies the view graph into owned types.

### `Entry.Data`

Normal proposals, serialized configuration changes, and ReadIndex carrier
entries use this field. Do not assume it is UTF-8. A nil configuration-change
proposal intentionally marshals as `EntryConfChangeV2` with nil data, which
decodes as an empty V2 change.

### `Message.Context`

Context carries protocol-specific opaque data, including heartbeat read-index
contexts and campaign-transfer markers. Preserve bytes exactly and never
NUL-terminate or treat them as strings.

### `Snapshot.Data`

Snapshot data is application-owned opaque state. Empty data can accompany a
valid non-empty snapshot whose metadata index is nonzero. Snapshot emptiness
is determined by metadata index, not payload length.

### ReadIndex request context

`RawNode.ReadIndex` receives a borrowed `const raft_byte_view_t *`, places the
request context in an entry's Data field, and returns it through
`raft_read_state_t.request_ctx` as `raft_bytes_t`. Nil and empty requests may
be application-distinguishable, so conversion must preserve both shape and
bytes.

### `Peer.Context`

`Peer.Context` is not a standalone raftpb wire field.
`raft_peer_view_t.context` is an embedded borrowed descriptor, and the
`raft_peer_view_t` array itself is
passed by `const` pointer; Bootstrap copies the context into each legacy
`ConfChange.Context`, which is then protobuf-marshaled into bootstrap entry
Data. The C implementation must deep-copy it before the call returns if the
operation retains it.

## Conversion and ownership tests

Phase 3 must add table-driven round-trip tests for every enum and message
shape, including:

- all current enum numeric values;
- nil, present-empty, and non-empty payloads;
- legacy V1 and V2 configuration entries;
- leave-joint empty V2 changes;
- full ConfState including outgoing voters and learners-next;
- nested storage-message responses;
- nil, empty, and non-empty snapshots;
- zero-valued but present HardState/message scalar fields.

Phase 12 differential tests should compare serialized protobuf bytes where
deterministic encoding is expected and semantic equality otherwise.
