# Ready Ownership for the C Port

`Ready` is both a correctness protocol and a cross-language ownership tree.
The C API must preserve the Go Ready/Advance state transition while ensuring
that no Go application or transport retains pointers into freed C memory.

## Ownership model

Recommended rules for C-produced output:

1. A successful Ready function returns a C-owned `raft_ready_t *`, including
   the outer descriptor and nested allocations.
2. The Go binding never allocates the pointer-bearing outer descriptor in Go
   memory.
3. The caller may read but not mutate the returned tree.
4. The Go binding deep-copies the complete tree into Go-owned values.
5. The caller invokes `raft_ready_destroy` exactly once, including on
   conversion failure after partial output.
6. `raft_ready_destroy` recursively frees nested entries, messages, snapshots,
   read-state contexts, and message response trees, then frees the outer
   descriptor. `raft_ready_free` remains the nested/reset helper for native C
   stack-owned containers.
7. The C RawNode retains its own internal state; it never relies on pointers
   into the returned `raft_ready_t` after the API call.

Ready conversion is batched: one cgo call fills a C-resident Ready graph, Go
walks and deep-copies that graph without calling C once per nested field, and
one `raft_ready_destroy` call releases the graph. Entry, Message, Snapshot, and
ReadState conversion must not introduce per-element cgo calls.

For Go-to-C input, C may borrow data only for the duration of the call. Any
proposal, message, configuration context, or Ready identity retained after
return must be copied into C-owned memory.

## Progress observation ownership

`RawNode.WithProgress` is observational and uses a separate owned-output
contract:

```c
int raft_raw_node_progress_snapshot(
    const raft_raw_node_t *rn,
    raft_progress_snapshot_t **out,
    size_t *out_len);

void raft_progress_snapshot_array_free(
    raft_progress_snapshot_t *arr,
    size_t len);
```

The returned array is a C-owned point-in-time copy, not a borrowed view and
not a collection of pointers into the tracker. Empty success is represented
by `NULL, 0`. The progress value intentionally has no inflights pointer,
matching Go `WithProgress`, which clears `Progress.Inflights` before calling
the visitor.

The Go binding obtains the whole array in one call, converts rows to
`tracker.Progress`, maps peer/learner type, sets `Inflights` to nil, calls the
user visitor in Go, then frees the array once. C never invokes the Go visitor,
stores a visitor handle, or exposes internal progress storage.

## Byte wrapper contract

The ABI no longer repeats `(pointer, length, is_nil)` triples and does not
pass structs by value. It uses:

- `const raft_byte_view_t *` for borrowed call-scoped API inputs such as
  Propose and ReadIndex; configuration-change and Peer structs embed the
  descriptor and are themselves passed by `const` pointer;
- `raft_bytes_t` for returned/owned payloads such as Entry.Data,
  Message.Context, Snapshot.Data, and `ReadState.RequestCtx`.

Both contain `data`, `len`, and `bool is_nil`. Nil-versus-empty is represented
only by `is_nil`; ownership is represented only by the type. A byte view is
borrowed and never freed or retained. Non-view `raft_bytes_t` is C-owned and
always freed by the owned free path. There are no ownership flags or runtime
view/owned kinds.

The canonical shapes are nil `{NULL, 0, true}`, present-empty
`{NULL, 0, false}`, and non-empty `{non-NULL, positive length, false}`.
`raft_byte_view_valid` and `raft_bytes_valid` centralize
these rules. An implementation must deep-copy a borrowed view before
retaining it in `raft_bytes_t`.

A null descriptor pointer is invalid and never encodes Go nil. Both nil and
empty Go slices use non-null descriptors; only `is_nil` distinguishes them.
Free and mutation helpers likewise take pointers to their mutable
objects.

`raft_bytes_free` always frees `data` and then resets the wrapper to canonical
nil. Parent free functions recurse through their nested owned types. A
`raft_bytes_t` must therefore never contain Go or borrowed memory.

Pointer-bearing aggregates intentionally duplicate shape:
`raft_entry_view_t`/`raft_entry_t`,
`raft_snapshot_view_t`/`raft_snapshot_t`, and
`raft_message_view_t`/`raft_message_t`. The view graph is call-scoped input;
the non-view graph is retained/output ownership. Recursive copy-from-view
helpers cross that boundary without retaining borrowed pointers.

The Go binding must call C shims/builders rather than allocate
pointer-containing view descriptors in Go memory. The current scalar shims
cover Propose and ReadIndex. Aggregate calls use a single-shot temporary
C-allocation strategy: allocate descriptor graphs with `C.malloc`/`C.calloc`,
fill them, make one RawNode call, then free the descriptors immediately.
Field-by-field cgo builder calls are intentionally avoided.

## Forbidden Go binding patterns

```go
// Forbidden: v is Go memory containing a pointer to Go byte memory.
v := C.raft_byte_view_t{
	data:   (*C.uint8_t)(unsafe.Pointer(unsafe.SliceData(b))),
	len:    C.size_t(len(b)),
	is_nil: C.bool(b == nil),
}
C.raft_raw_node_propose(rn, &v)
```

```go
// Forbidden: the message and its nested descriptor arrays live in Go memory.
m := C.raft_message_view_t{/* pointer-bearing fields */}
C.raft_raw_node_step(rn, &m)
```

Flat bytes instead use one scalar shim call:

```go
// Allowed: ptr addresses pointer-free byte backing storage and is borrowed
// only during this call. The descriptor itself is constructed in C.
C.raft_raw_node_propose_from_parts(
	rn, ptr, C.size_t(len(b)), C.bool(b == nil),
)
```

For Step, Bootstrap, and ConfChange, descriptor structs and arrays live in C
temporary memory. Their byte pointers may address pointer-free Go byte backing
arrays only during the single RawNode call. C does not retain them; the Go
wrapper frees all temporary C descriptors after return.

## Field-by-field contract

| Ready field | Presence and ownership | Required behavior |
| --- | --- | --- |
| `SoftState` | Optional fixed-size value, represented by `has_soft_state`. | Volatile observation only; never persist. Emit only when leader/state differs from the last accepted Ready. Lead uses `RAFT_NONE` when unknown. |
| `HardState` | Optional fixed-size value, represented by `has_hard_state`. | Persist term, vote, and commit before dependent messages. Numeric all-zero and “not present” are distinct states. |
| `Entries` | C-owned vector of `raft_entry_t`; every Data field is owned `raft_bytes_t`. | Append to stable log in order. Treat as read-only. In synchronous mode persist before messages. |
| `CommittedEntries` | C-owned ordered vector. | Apply to the state machine in log order and across Ready batches without gaps, reorder, or duplicates. Config changes are applied through `ApplyConfChange` only when the application applies the committed entry. |
| `Messages` | C-owned vector with recursively owned nested entries, optional snapshots, `raft_bytes_t` context, and responses. | Deep-copy before dispatch. Route local storage targets locally; do not send them over transport. Preserve per-target ordering. |
| `Snapshot` | Optional recursively owned snapshot, represented by `has_snapshot`. | Save/apply according to Ready ordering. Snapshot validity/emptiness is based on metadata index, not Data length. |
| `ReadStates` | C-owned vector with owned `raft_bytes_t` request contexts. | A read completes only after the application has applied through `ReadState.Index`. Context is opaque and must be copied. |
| `MustSync` | Scalar boolean. | If true, HardState/Entries persistence must be durable before dependent work. It does not apply to SoftState. |

## Nested Message ownership

Each `raft_message_t` can own:

- an entry vector and every Entry.Data buffer;
- an optional Snapshot and its Data/ConfState vectors;
- a Context buffer;
- a response-message vector whose messages recursively own the same kinds of
  data.

The free implementation must be recursive and safe for zero-initialized and
partially initialized structures. A conversion failure at any depth must free
the whole C output tree.

The Go binding must allocate new Go protobuf messages, entries, snapshots,
configuration slices, response slices, and byte slices. Pointing a Go
`raftpb.Message` field at C memory with `unsafe.Slice` is not acceptable once
the cgo call or C Ready lifetime ends.

Conversion must inspect `is_nil`: canonical nil becomes a nil Go
slice, canonical present-empty becomes a non-nil empty Go slice, and
non-empty data is copied. The Go wrapper must not retain either a
`raft_bytes_t.data` pointer or a borrowed `raft_byte_view_t.data` pointer.

## `MsgSnap` lifetime

A `MsgSnap` may outlive Ready processing because snapshot transport can be
asynchronous. Therefore:

- the Go binding must deep-copy snapshot metadata and Data before freeing the
  C Ready;
- transport owns the Go copy for as long as the send needs it;
- after transport succeeds or fails, the application calls
  `ReportSnapshot(target, SnapshotFinish|SnapshotFailure)`;
- freeing the C Ready must not invalidate the in-flight Go transport value;
- a valid snapshot may have empty Data, so metadata—not byte length—controls
  whether a snapshot exists.

Failure to report a failed snapshot can leave follower progress paused in
snapshot state.

## Ready preview, acceptance, and Advance

The Go Node actor requires a two-stage operation:

1. `ready_without_accept` builds a read-only preview.
2. If another select case wins, the preview is freed/discarded and no work is
   consumed.
3. Only after the Ready channel send succeeds does `accept_ready` consume the
   corresponding work.
4. In synchronous mode, Node waits for `Advance` before offering the next
   Ready.
5. `Advance` steps completion messages recorded at acceptance and clears the
   outstanding batch.

Nothing may mutate the RawNode between preview and acceptance. The preview
descriptor is allocated in C memory. `accept_ready` receives that same
C-resident object before it is freed; the Go binding never reconstructs it in
Go memory.

The chosen Advance API is:

```c
int raft_raw_node_advance(raft_raw_node_t *rn);
```

Current Go `Advance(_ Ready)` already ignores its Ready argument and operates
on internally recorded `stepsOnAdvance`. The future C RawNode likewise tracks
the accepted Ready internally. After batch conversion, the Go binding can call
`raft_ready_destroy` once and later make one argument-free Advance cgo call.
No pointer-bearing Ready is reconstructed or passed back.

## Unstable entry and snapshot interaction

Accepting a Ready marks the exposed unstable entries/snapshot as in progress;
it does not mean they are durable. In synchronous mode:

- application persistence happens after Ready delivery;
- `Advance` supplies the completion point that allows the RawNode to step
  local append/apply acknowledgements and stabilize/drop internal data.

The port must preserve the current ABA defenses around asynchronous append
responses: an append response's term and last entry identity are checked
before unstable entries are considered stable.

Snapshots likewise remain internally tracked until the appropriate
persistence/application completion path advances them. Freeing a C Ready is a
memory action only; it must not imply acceptance, persistence, application, or
Advance.

## Persistence and delivery ordering

### Synchronous storage mode

For each accepted Ready:

1. persist Snapshot, Entries, and HardState accurately;
2. when `MustSync` is true, make required persistence durable;
3. only then send messages whose correctness depends on that state;
4. apply CommittedEntries in order;
5. call Advance according to the Node/RawNode contract.

The exact relative disk transaction policy is application-specific, but no
dependent message may escape before the term, vote, log entries, and snapshot
state it assumes are saved.

### AsyncStorageWrites mode

Ready carries `MsgStorageAppend` and `MsgStorageApply`:

- same-target messages are processed reliably and in order;
- append work (entries, HardState, snapshot) is durable before attached
  responses are delivered when responses exist;
- apply work applies committed entries in order;
- attached responses are delivered only after the corresponding work;
- messages to `RAFT_LOCAL_APPEND_THREAD` and
  `RAFT_LOCAL_APPLY_THREAD` never go to network transport;
- `Advance` must not be called.

## Go application retention rule

After C-to-Go conversion and `raft_ready_destroy`, Go transport and application
code may retain only Go-owned copies. This includes:

- Entry and Snapshot payload bytes;
- Message.Context and nested response messages;
- ReadState request contexts;
- ConfState slices;
- snapshots queued for asynchronous transport;
- committed entries queued for state-machine application.

The binding should make this guarantee unconditional rather than requiring
each etcd caller to understand C lifetimes.

## Tests required

Phases 3 and 6 should add allocation/failure tests that:

- convert and free every Ready field independently and together;
- inject partial-allocation failure at each nesting level;
- retain Go copies after C Ready free and verify contents;
- discard a preview without accepting it;
- accept exactly one preview and reject stale/double acceptance;
- call Advance after C output memory has already been freed;
- exercise MsgSnap transport completion after Ready free;
- verify synchronous persistence-before-send ordering;
- verify asynchronous same-target ordering and response gating;
- run under ASan/UBSan where available and exercise Go's cgo pointer checks.
