# PHASE 24: C Protobuf Fidelity Result

## Result

The audit finding was **both a genuine Go-API compatibility bug and a
narrower intentional C-ABI limitation**.

The genuine bug was confirmed. Before this phase, values crossing the cgo
RawNode boundary lost:

- proto2 presence for optional scalar and nested-message fields;
- every protobuf object's raw unknown-field bytes; and
- the corresponding contribution to `proto.Size`, `proto.Equal`, and marshal
  output.

Optional byte fields already preserved nil versus present-empty through
`raft_bytes_t.is_nil`.

The genuine Go-facing mismatch is fixed. For supported Raft protobuf objects,
the C backend now preserves object-level proto2 presence and raw unknown fields
through the complete Go -> C -> Go lifecycle. This includes messages, entries,
snapshots, storage callbacks, Ready output, asynchronous storage messages, and
configuration-change proposal encoding.

The C API remains a typed semantic API. It does not accept or promise to return
an original serialized protobuf wire stream, including original ordering of
known fields, duplicate known-field occurrences, or non-canonical encodings.
That is an intentional limitation, not a remaining RawNode compatibility bug:
the public Go API receives generated protobuf objects rather than original
wire buffers, and the project specification defines the C structures as the
canonical semantic RawNode API.

No protobuf parser or general wire-format implementation was added to the C
Raft core.

## Exact reference

The compatibility reference is:

```text
etcd-io/raft v3.7.0
commit b867cf13f6bc0dae21204302df97bc2355c3af55
```

The audit used the repository's `v3.7.0` versions of:

- `raftpb/raft.proto`;
- generated `raftpb/raft.pb.go`;
- `raft.go`, especially entry cloning and snapshot restoration;
- `log.go`, especially snapshot cloning;
- `rawnode.go`, including asynchronous storage-message construction;
- `raftpb/confstate.go`, especially `ConfState.Equivalent`; and
- the configuration-change proposal and restoration paths.

This is a proto2 schema. Generated Go scalar fields are pointers, generated
nested optional messages are pointers, optional bytes use nil versus non-nil
slices for presence, and every generated message can hold raw unknown bytes.

## Intended compatibility boundary

The project specification establishes two related but different contracts:

1. The existing Go `RawNode` and `Node` behavior is to remain compatible with
   etcd.
2. The public C `RawNode`, message, entry, and snapshot structures are a typed,
   semantic Raft API.

The Go binding is specifically responsible for Go/C conversion, deep copying,
and ownership. Therefore:

- protobuf object properties observable on values returned through the Go API
  are part of Go compatibility;
- consensus behavior alone is not sufficient when `Ready` returns generated
  protobuf objects;
- a byte-for-byte transport API for an original encoded protobuf stream is
  not part of the C API contract; and
- metadata required to reconstruct the reference Go protobuf object must still
  survive while that object is resident in C.

This distinction allowed a small sidecar-metadata fix without turning the C
Raft core into a protobuf implementation.

## Protobuf field inventory

Every message below is unknown-field capable in generated Go code.

| Type | Schema fields | Presence-sensitive fields | Repeated fields | Boundary role |
|---|---|---|---|---|
| `Entry` | optional enum `Type`; optional uint64 `Term`, `Index`; optional bytes `Data` | all four optional fields | none | Step proposals/appends, unstable and stable log, Ready entries, async storage |
| `SnapshotMetadata` | optional `ConfState`; optional uint64 `Index`, `Term` | all three | none | nested in snapshots |
| `Snapshot` | optional bytes `Data`; optional `SnapshotMetadata` | both | none | storage, `MsgSnap`, Ready |
| `Message` | optional enum `Type`; optional uint64 `To`, `From`, `Term`, `LogTerm`, `Index`, `Commit`, `Vote`, `RejectHint`; optional bool `Reject`; optional `Snapshot`; optional bytes `Context` | every non-repeated field | `Entries`, `Responses` | Step, Ready messages, Node routing, async storage |
| `HardState` | optional uint64 `Term`, `Vote`, `Commit` | all three | none | storage initial state and Ready |
| `ConfState` | optional bool `AutoLeave` | `AutoLeave` | `Voters`, `Learners`, `VotersOutgoing`, `LearnersNext` | initial state, snapshots, `ApplyConfChange` |
| `ConfChange` | optional uint64 `Id`, `NodeId`; optional enum `Type`; optional bytes `Context` | all four | none | V1 proposal payload |
| `ConfChangeSingle` | optional enum `Type`; optional uint64 `NodeId` | both | none | nested V2 proposal payload |
| `ConfChangeV2` | optional enum `Transition`; optional bytes `Context` | both | `Changes` | V2 proposal payload |

`Ready`, `SoftState`, `ReadState`, and tracker `Config` are not protobuf
messages. `Ready` carries protobuf `HardState`, entries, snapshot, and
messages, so fidelity of those nested values is externally observable.

For repeated scalar/message fields, nil and empty containers do not have
proto2 field presence and are equivalent under normal protobuf equality and
serialization. Order and element values are preserved. Optional bytes are
different: nil and present-empty are distinct and must be preserved.

## Reference Go behavior

### Presence

The generated Go API distinguishes an absent optional scalar:

```go
&raftpb.Entry{}
```

from the same scalar explicitly present with its default value:

```go
&raftpb.Entry{Type: raftpb.EntryNormal.Enum()}
```

The getters return the same semantic enum value, but `proto.Equal`,
`proto.Size`, and marshal output distinguish them.

The same applies to:

- zero-valued optional integers;
- `Reject: false` when explicitly present;
- `AutoLeave: false` when explicitly present;
- an absent optional nested message versus a present empty nested message; and
- nil optional bytes versus present-empty optional bytes.

### Unknown fields

Generated messages retain raw unknown-field bytes. Cloning retains them, and
marshaling appends those raw bytes after the known fields. Consequently,
unknown fields participate in `proto.Equal`, `proto.Size`, and marshal output.

Important reference paths preserve them:

- `raft.appendEntry` uses `proto.Clone` on proposed entries before overwriting
  `Term` and `Index`;
- the unstable and storage paths clone snapshots;
- follower proposal/read-index forwarding reuses the input message while
  setting routing fields; and
- configuration-change proposals marshal the supplied protobuf object.

### Selectively materialized generated objects

Reference Raft message constructors use protobuf literals. Only fields
assigned by the literal are present. This is observable even when a getter
would return zero for an absent field.

Examples include:

- a vote request has `Type`, `To`, `From`, `Term`, `LogTerm`, and `Index`, but
  does not materialize `Commit`, `Vote`, or rejection fields;
- `MsgStorageAppend` materializes its `Term`/`Vote`/`Commit` tuple together
  only when Ready contains a HardState update;
- `MsgStorageApply` deliberately materializes `Term: 0`; and
- an empty leader no-op entry leaves default `Type` absent while assigning
  `Term` and `Index`.

### Restored ConfState invariant

There is one deliberate reference consequence of unknown fields that is not
ordinary round-trip preservation.

After restoring a configuration during `newRaft` or snapshot restoration, Go
calls `ConfState.Equivalent` between the supplied state and the tracker-built
state. `Equivalent`:

- sorts membership vectors;
- normalizes absent `AutoLeave` to present false; but
- retains unknown fields in both clones.

The tracker-built state has no unknown bytes. Therefore, an otherwise-valid
restored `ConfState` containing unknown fields fails the invariant and Go
panics. The C backend now uses the existing fatal-error policy for the same
case. It does not incorrectly reject absent `AutoLeave`, because that presence
difference is normalized by the reference.

## Previous C representation and loss points

Before this phase, the typed C structures held only decoded semantic values.
The primary loss points were:

- `convert_cgo.go` used getters when constructing C views, collapsing absent
  and present-default scalars;
- C owned copies had no field-presence or unknown-byte storage;
- `cEntry`, `cMessage`, `cSnapshot`, and `cConfState` materialized pointers
  based on fixed assumptions rather than the reference constructor;
- `cMessage` contained a special all-zero `MsgStorageAppend` heuristic that
  could represent only that one optional tuple;
- storage callbacks copied only known values;
- C entry-encoding size counted known non-default fields, not present-default
  fields or unknown bytes; and
- the C configuration-change encoder emitted only known fields.

The result was not only representational. Ready batching uses protobuf entry
encoding size in Go, so an incorrect size can change message/Ready batching
when optional default fields or unknown bytes are present.

## Concrete reproductions

### Reproduction 1: optional presence

The audit compared:

```go
absent := &raftpb.Entry{}
present := &raftpb.Entry{
    Type: raftpb.EntryNormal.Enum(),
    Data: []byte{},
}
```

Both expose default semantic values through getters. Go protobuf behavior
still reports different equality, sizes, and bytes. Before this phase, the
C round trip normalized the scalar pointers and could not reproduce either
object exactly. It now does.

The parity test proposes both forms. The expected reference clone has only
`Term` and `Index` overwritten. Default and cgo backends now return an exactly
equal Ready entry.

### Reproduction 2: unknown field

Tests attach the valid raw unknown field:

```text
a0 06 07
```

This is field 100, varint value 7.

Before this phase, Go -> C -> Go discarded these bytes. The new path copies the
raw bytes opaquely and restores them through `ProtoReflect().SetUnknown`.

The tests exercise unknown bytes on:

- `Message`;
- `Entry`;
- `Snapshot`;
- `SnapshotMetadata`;
- `ConfState` in direct storage conversion;
- `ConfChange`;
- `ConfChangeV2`; and
- nested `ConfChangeSingle`.

### Reproduction 3: ordinary generated message

A generated `MsgVote` and an asynchronous `MsgStorageAppend` plus its response
are compared field-for-field using:

- `proto.Equal`;
- `proto.Size`; and
- deterministic marshal output.

This verifies that the fix does not merely retain exotic metadata; it also
reproduces the reference's ordinary selective field materialization.

### Reproduction 4: unknown restored ConfState

The same otherwise-valid `ConfState` with unknown bytes is used:

- as storage initial state; and
- in a newly accepted snapshot.

Both default Go and cgo backends panic/terminate through their established
fatal paths. This confirms that unknown-field preservation did not silently
weaken the reference configuration invariant.

## Final design

### Metadata attachment

The public C value/view pairs now include small protobuf metadata structures:

```c
typedef struct raft_protobuf_metadata_view {
    uint32_t fields;
    raft_byte_view_t unknown_fields;
} raft_protobuf_metadata_view_t;

typedef struct raft_protobuf_metadata {
    uint32_t fields;
    raft_bytes_t unknown_fields;
} raft_protobuf_metadata_t;
```

`fields` is a type-specific presence mask. It is used only for optional scalar
and nested-message fields. Existing `raft_byte_view_t.is_nil` and
`raft_bytes_t.is_nil` remain the presence mechanism for optional bytes.

The C algorithm does not decode, validate, reorder, or otherwise interpret the
raw unknown bytes. It deep-copies and releases them together with the enclosing
object.

### Why a C-structure extension was chosen

A Go-only side table would have had to track metadata across:

- C message copies;
- unstable-log truncation and replacement;
- snapshot ownership;
- Ready extraction;
- storage callback results;
- async-storage response lifetimes; and
- partial failures.

That would duplicate C ownership and introduce identity/lifetime coupling
outside the canonical C RawNode. Adding metadata to the existing value/view
objects is smaller and easier to audit.

The public function surface is unchanged, but the public value-structure
layout is extended. C consumers must recompile against the updated header. The
private RawNode ABI marker was advanced from 16 to 17 so a stale Go/C build is
rejected rather than silently misinterpreting layouts.

### Generated C objects

C constructors now set presence bits according to the corresponding Go
protobuf literal:

- common send logic marks `Type`, `To`, and `From`;
- term assignment follows the reference message-type rule;
- append, heartbeat, vote, rejection, ReadIndex, and snapshot paths mark their
  exact assigned fields;
- async append HardState tuple presence is explicit rather than inferred from
  values;
- storage apply messages deliberately mark zero `Term`;
- proposal entries preserve input metadata while marking overwritten `Term`
  and `Index`;
- a leader no-op entry retains absent default `Type`; and
- tracker-generated `ConfState` marks `AutoLeave` present, matching Go.

### Configuration-change payloads

The existing small C encoder remains responsible only for known
configuration-change fields. Raw unknown bytes are appended opaquely:

- after V1 known fields;
- inside each V2 nested `ConfChangeSingle`; and
- after V2 known fields.

This matches generated Go marshal structure without adding a generic parser.
The existing decoder continues to skip unknown fields when applying committed
configuration changes, just as semantic application ignores them.

### Exact entry size

`raft_log_entry_encoding_size` now counts:

- present default scalar fields;
- present-empty `Data`;
- non-default known fields;
- raw unknown bytes; and
- the existing outer length-delimited overhead used by batching.

Overflow still follows the existing saturating/error-safe behavior.

## Conversion and ownership audit

| Path | Go -> C | C ownership | C -> Go | Result |
|---|---|---|---|---|
| `RawNode.Step` | recursive borrowed Message/Entry/Snapshot views include masks and unknown bytes | copied only when retained by Raft | Ready conversion restores pointers and unknown bytes | preserved |
| proposal entries | input Entry metadata copied with proposal | owned by log/unstable copies | Ready entry deep-copy | preserved |
| `ProposeConfChange` | known presence plus borrowed unknown bytes | encoded immediately into owned Entry.Data | application decodes ordinary entry bytes | preserved |
| storage `InitialState` | callback allocates owned C values | callback result is freed by existing owner | semantically consumed by core | expected reference behavior |
| storage `Entries` | callback copies Entry metadata | owned result freed by C | may be copied into messages/log paths | preserved |
| storage `Snapshot` | callback copies all nested metadata | owned result freed by C | snapshot/message/Ready conversion restores it | preserved |
| Ready | C owns deep object graph until Ready destruction | parent free recursively frees metadata | Go receives independent byte copies | preserved |
| async storage | message/entry/snapshot copies include metadata | existing message graph owns it | Ready returns normal Go protobuf graph | preserved |

Borrowed Go input slices remain pinned by the existing `cInputArena` through
the native call. Owned unknown-byte buffers use the existing C allocator.
Parent destructors free them recursively. Storage callback construction calls
the same parent cleanup on partial allocation failure. C -> Go conversion
copies unknown bytes before Ready or callback-owned C objects are destroyed.

No Go pointer is retained by C, and the Phase 20 RawNode lifetime model is
unchanged.

## Compatibility classification

### Category A: genuine compatibility bugs, fixed

- Optional scalar presence on Go-visible `Message`, `Entry`, `ConfState`, and
  nested snapshot metadata.
- Optional nested presence for `Snapshot.Metadata` and
  `SnapshotMetadata.ConfState`.
- Unknown bytes on Go-visible messages, entries, snapshots, snapshot metadata,
  storage ConfState values, and configuration-change proposals.
- Selective presence on C-generated Raft and async-storage messages.
- Entry `proto.Size` equivalence used by batching.
- Fatal handling of restored `ConfState` unknown bytes.

### Category B: intentional C ABI limitations

- The C API does not preserve an original complete protobuf wire buffer.
- It does not promise original ordering of known wire fields, duplicate known
  scalar occurrences, or non-canonical known-field encodings.
- C enums remain typed and range-validated; arbitrary future/invalid numeric
  enum values are not a supported C Raft input.
- Nil elements inside repeated message fields are not a supported valid Raft
  input. Normal non-nil repeated elements preserve order and content.

These do not affect ordinary protobuf objects produced or accepted by the
supported etcd/raft v3.7.0 API. They remain explicit limitations rather than a
reason to add protobuf wire machinery to the consensus core.

### Category C: architectural ambiguity, resolved

The old design left unclear whether the C semantic representation permitted
the Go wrapper to normalize protobuf objects. The specification's requirement
to preserve the existing Go RawNode API, combined with Ready returning
protobuf values, makes Go-visible object metadata part of compatibility.

The resolution is:

- object-level presence and unknown fields are guaranteed through the Go
  RawNode/Node boundary;
- raw original-wire preservation is not guaranteed by the typed C API.

## Final compatibility matrix

| Behavior | Go v3.7.0 | C before Phase 24 | Required? | Resolution |
|---|---|---|---|---|
| Optional scalar presence | Distinguishes absent from present-default | Collapsed into value | Yes for Go-visible protobufs | **FIXED** |
| Optional nested presence | Distinguishes nil from present-empty | Usually materialized fixed nested objects | Yes for Go-visible snapshots | **FIXED** |
| Optional byte presence | Distinguishes nil from present-empty | `is_nil` already preserved it | Yes | **NO ISSUE / retained** |
| Unknown fields | Raw bytes retained in generated object | Discarded | Yes when object crosses Go API | **FIXED** |
| `proto.Equal` | Includes presence and unknowns | Could differ | Yes on returned values | **FIXED** |
| `proto.Size` | Includes present-default tags and unknowns | Could undercount | Yes, including entry batching | **FIXED** |
| Marshal output | Reflects object presence and unknown bytes | Could differ | Yes for returned Go object | **FIXED** |
| Original raw wire ordering | Not available once only a semantic object is supplied | Not represented | No C raw-wire contract | **INTENTIONAL LIMITATION** |
| Unmarshal/marshal object round trip | Preserves presence and raw unknown bytes | Metadata was lost through C | Yes for supported objects | **FIXED** |
| Message semantics | Generated literals have selective presence; forwarded unknowns survive | Values worked, representation differed | Yes | **FIXED** |
| Entry semantics | Proposal clone preserves metadata and overwrites Term/Index | Metadata lost | Yes | **FIXED** |
| Snapshot semantics | Clone preserves outer/metadata unknowns and presence | Metadata lost | Yes | **FIXED** |
| Restored ConfState unknowns | Fails `Equivalent` invariant and panics | Previously ignored | Yes | **FIXED** |
| ConfState `AutoLeave` absent | Equivalent to false during restore | Semantic false | Yes | **NO ISSUE / retained** |
| ConfChange proposal bytes | Generated marshal includes presence and unknowns | Only known fields encoded | Yes | **FIXED** |
| HardState metadata from initial storage | Values are consumed into Raft; later HardState is freshly generated | Semantic values consumed | No metadata propagation in reference | **NOT APPLICABLE** |
| Ready protobuf objects | Returned as independent Go protobuf copies | Deep copies normalized metadata | Yes | **FIXED** |
| Future/invalid enum numeric values | Generated protobuf can hold numeric values | Typed C validation rejects unsupported values | Outside typed C v3.7.0 contract | **INTENTIONAL LIMITATION** |

There are no `STILL UNRESOLVED` items within the supported object-level
protobuf compatibility contract.

## Modified files

### Public representation and ABI

- `c/include/raft/raft.h`
  - adds borrowed/owned protobuf metadata;
  - adds per-type presence masks;
  - attaches metadata to Message, Entry, Snapshot,
    SnapshotMetadata, and ConfState;
  - adds opaque unknown-byte views to V1/V2 configuration changes and nested
    V2 changes.
- `c/src/raft_internal.h`
  - advances the RawNode ABI marker to 17.

### C ownership and semantics

- `c/src/raw_node.c`
  - validates, deep-copies, and frees metadata through public RawNode and Ready
    object graphs;
  - preserves metadata in raw copies and async-storage construction.
- `c/src/raft_core.c`
  - preserves metadata through Raft message/log operations;
  - marks fields according to reference Go constructors.
- `c/src/unstable.c`
  - deep-copies entry and nested snapshot metadata.
- `c/src/tracker.c`
  - marks tracker-generated `AutoLeave` present.
- `c/src/log.c`, `c/src/log.h`
  - make entry encoding size presence/unknown aware.
- `c/src/confchange.c`
  - appends opaque unknown bytes in V1/V2 encoding;
  - rejects unknown bytes in restored ConfState through the reference fatal
    invariant.

### Go/cgo conversion

- `convert_cgo.go`
  - extracts pointer/nested presence and unknown bytes on Go -> C;
  - restores exact pointer presence and unknown bytes on C -> Go;
  - removes the `MsgStorageAppend` all-zero heuristic.
- `storage_bridge_cgo.go`
  - copies presence and unknown metadata from Go storage callback values into
    owned C values with partial-failure cleanup.

### Tests

- `protobuf_fidelity_parity_test.go`
- `storage_bridge_cgo_test.go`
- `c/tests/raw_node_skeleton_test.c`
- `c/tests/log_test.c`
- `c/tests/confchange_test.c`

No Node destruction, stale `MsgAppResp`, empty `MsgProp`, `Status.Config`, or
other final-audit issue was changed.

## Tests added

### Backend-neutral Go/default-vs-cgo coverage

- `TestProtobufFidelityProposalEntryParity`
  - absent default Type/nil Data;
  - present default Type/present-empty Data;
  - unknown Entry bytes;
  - reference Term/Index overwrite.
- `TestProtobufFidelityForwardedMessageParity`
  - outer and nested Entry unknown bytes;
  - explicitly present zero fields;
  - present-empty Context;
  - forwarded routing mutation only.
- `TestProtobufFidelitySnapshotParity`
  - outer Snapshot and SnapshotMetadata unknown bytes;
  - present-empty Data;
  - nested presence;
  - present-false AutoLeave.
- `TestProtobufFidelityConfStateUnknownInvariantParity`
  - unknown initial ConfState is fatal;
  - unknown received-snapshot ConfState is fatal.
- `TestProtobufFidelityConfChangeProposalParity`
  - V1 top-level unknown bytes;
  - V2 top-level and nested-change unknown bytes.
- `TestProtobufFidelityGeneratedVoteMessagePresenceParity`
  - exact selective presence on an internally generated vote request.
- `TestProtobufFidelityAsyncStorageMessagePresenceParity`
  - exact `MsgStorageAppend` HardState tuple;
  - exact nested `MsgStorageAppendResp` tuple.

Every exact comparison checks `proto.Equal`, `proto.Size`, and deterministic
marshal bytes.

### Storage bridge coverage

`TestCGoStorageCallbacksPreserveProtobufMetadata` directly verifies callback
copying for:

- ConfState unknown bytes;
- absent and present Entry scalars;
- Entry unknown bytes;
- Snapshot unknown bytes;
- SnapshotMetadata presence and unknown bytes; and
- nested ConfState presence and unknown bytes.

The direct callback test is intentional: restored ConfState unknown bytes are
fatal in the reference, but the storage conversion itself must still be a
correct independent deep copy before the core decides how the value is used.

### Native C coverage

- RawNode skeleton tests verify deep-copy independence for message, entry,
  snapshot, nested metadata, presence masks, and unknown bytes.
- Log tests verify an exact protobuf size of 11 bytes for three present-zero
  scalars, present-empty Data, and a three-byte unknown field.
- Configuration-change tests verify top-level and nested unknown-byte encoding,
  ordinary decoding, and fatal rejection of a restored ConfState containing
  unknown bytes.

## Validation

All validation below was rerun after the final ConfState invariant adjustment.

Focused protobuf and conversion tests passed with both backends:

```text
go test -run '^TestProtobufFidelity' ./...
CGO_ENABLED=1 go test -tags=cgo_raft -run '^TestProtobufFidelity' ./...
CGO_ENABLED=1 go test -tags=cgo_raft -run \
  '^(TestProtobufFidelity|TestCGoStorageCallbacksPreserveProtobufMetadata)' \
  ./...
```

Native and full Go suites passed:

```text
make -C c test
go test -count=1 ./...
CGO_ENABLED=1 go test -count=1 -tags=cgo_raft ./...
```

Race detector passed:

```text
go test -count=1 -race ./...
CGO_ENABLED=1 go test -count=1 -race -tags=cgo_raft ./...
```

Strict cgo pointer checking passed:

```text
GOEXPERIMENT=cgocheck2 CGO_ENABLED=1 \
  go test -count=1 -tags=cgo_raft ./...
```

Native ASan, LeakSanitizer, and UBSan passed:

```text
make test-c-sanitize
```

cgo ASan/LeakSanitizer and UBSan passed:

```text
CGO_ENABLED=1 ASAN_OPTIONS=detect_leaks=1:halt_on_error=1 \
  go test -count=1 -asan -tags=cgo_raft ./...

CGO_ENABLED=1 \
  CGO_CFLAGS='-O1 -g -fno-omit-frame-pointer -fsanitize=undefined' \
  CGO_LDFLAGS='-fsanitize=undefined' \
  UBSAN_OPTIONS=halt_on_error=1:print_stacktrace=1 \
  go test -count=1 -tags=cgo_raft ./...
```

Valgrind full leak/error checking passed for all nine native test binaries:

- `raw_node_skeleton_test`;
- `raft_core_test`;
- `snapshot_test`;
- `read_only_test`;
- `tracker_test`;
- `confchange_test`;
- `unstable_test`;
- `log_test`; and
- `fatal_error_test`.

Both TLA-tagged variants passed:

```text
go test -count=1 -tags=with_tla ./...
CGO_ENABLED=1 go test -count=1 -tags='cgo_raft with_tla' ./...
```

The clean strict Clang C11 build and native tests passed:

```text
make -C c clean test CC=clang
```

A clean GCC build and native tests then passed and restored the ordinary build:

```text
make -C c clean test CC=gcc
```

Formatting and whitespace checks passed:

```text
make verify-gofmt
git diff --check
```

No test failure, race, cgo pointer violation, compiler warning, sanitizer
finding, Valgrind error, leak, double free, use-after-free, or partial-cleanup
failure was reported.

## Remaining intentional limitations

The following are explicitly unsupported and do not affect the intended
etcd/raft v3.7.0 object-level contract:

1. The public C structures do not preserve an original raw protobuf wire
   stream.
2. Original ordering/non-canonical representation of known fields is not
   retained.
3. Arbitrary future or invalid numeric enum values are not accepted by the
   range-validated typed C API.
4. Nil elements inside repeated message fields are not valid supported Raft
   input.

For supported protobuf objects, no remaining presence, unknown-field,
`proto.Equal`, `proto.Size`, deterministic marshal, Entry, Message, Snapshot,
ConfState, ConfChange, storage callback, async-storage, or Ready fidelity
mismatch was found.

## Final conclusion

The original finding was:

- a **genuine compatibility bug** for protobuf object metadata observable
  through the Go RawNode/Node API; and
- an **intentional limitation** only for raw serialized-wire properties that
  the typed C API never promised.

The genuine portion is fixed with a narrow metadata sidecar attached to the
existing C values. The C Raft algorithm still operates on typed semantic
fields, existing ownership and fatal-error policies are preserved, and no
general protobuf wire implementation was introduced.

Within the supported etcd-io/raft v3.7.0 API contract, no protobuf
compatibility issue remains.
