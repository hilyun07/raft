# C/Go Pointer-Safe RawNode API Result

The RawNode C skeleton and binding plan now prohibit passing the address of
Go-allocated pointer-containing descriptor memory into C. Flat byte input uses
C stack shims; aggregate input uses one temporary descriptor graph allocated
in C memory and one RawNode call. Ready output is C-owned, and Advance no
longer accepts a Ready descriptor.

No Raft consensus logic, cgo binding implementation, build tag, or production
Go behavior changed.

## Files updated

- `c/include/raft/raft.h`
- `c/src/raft_internal.h`
- `c/src/raw_node.c`
- `c/tests/raw_node_skeleton_test.c`
- `prompts/RAFT_C_PORTING_SPEC.md`
- `OUTPUT/RAFTPB_WIRE_COMPAT_FOR_C_PORT.md`
- `OUTPUT/READY_OWNERSHIP_FOR_C_PORT.md`
- `OUTPUT/ERROR_MAPPING_FOR_C_PORT.md`
- `OUTPUT/PHASE2_C_RAWNODE_SKELETON_RESULT.md`
- `OUTPUT/RAWNODE_API_PARITY_FOR_C_PORT.md`
- `OUTPUT/C_PORT_TRACKING_GAPS.md`
- `OUTPUT/C_TYPE_SEPARATED_PAYLOAD_API_RESULT.md`
- `OUTPUT/C_GO_POINTER_SAFE_RAWNODE_API_RESULT.md`

No `PHASE3_GO_RAWNODE_BINDING_RESULT.md` or Go C-binding skeleton exists yet.

## Binding safety rule

The Go binding must never pass the address of a Go-allocated C descriptor that
contains Go pointers. The only Go memory passed directly to C may be
pointer-free backing storage, such as `[]byte` data, and only while C reads it
during that call without retaining it.

The type-separated ownership model remains:

- `*_view_t` is borrowed, never freed, and never retained;
- non-view objects are C-owned retained/output data;
- `bool is_nil` preserves nil versus present-empty;
- retained input is recursively copied from view types to owned types;
- no ownership kind, view/owned tag, or ownership flag exists.

## Forbidden Go binding patterns

```go
// Forbidden: v is Go memory containing a pointer into Go byte memory.
v := C.raft_byte_view_t{
	data:   (*C.uint8_t)(unsafe.Pointer(unsafe.SliceData(b))),
	len:    C.size_t(len(b)),
	is_nil: C.bool(b == nil),
}
C.raft_raw_node_propose(rn, &v)
```

```go
// Forbidden: m and its nested arrays are pointer-containing Go memory.
m := C.raft_message_view_t{/* ... */}
C.raft_raw_node_step(rn, &m)
```

Allowed flat-byte flow:

```go
// ptr points to pointer-free byte backing storage borrowed only for the call.
C.raft_raw_node_propose_from_parts(
	rn, ptr, C.size_t(len(b)), C.bool(b == nil),
)
```

The C shim constructs `raft_byte_view_t` on its own stack.

## Flat byte shims

Both required one-call shims are implemented:

```c
int raft_raw_node_propose_from_parts(raft_raw_node_t *rn,
                                     const uint8_t *data,
                                     size_t len,
                                     bool is_nil);

int raft_raw_node_read_index_from_parts(raft_raw_node_t *rn,
                                        const uint8_t *ctx,
                                        size_t len,
                                        bool is_nil);
```

They validate through the regular pointer-based API. Null data with zero
length is valid for both nil and present-empty; null data with positive length
is invalid.

## Aggregate strategy

The skeleton chooses single-shot temporary C descriptors, not a field-by-field
builder.

For Bootstrap, Step, ProposeConfChange, and ApplyConfChange, the Go binding:

1. computes the descriptor/array space;
2. allocates one contiguous C block where practical with
   `C.malloc`/`C.calloc`;
3. fills scalar fields and C-resident pointer-bearing descriptors;
4. points byte fields at pointer-free Go byte backing arrays only for the
   duration of the call;
5. makes exactly one RawNode operation call;
6. immediately frees the temporary descriptor block after return.

The C core treats every `*_view_t` pointer as borrowed. Real logic must use
the implemented recursive copy-from-view helpers before retention.

### Step

The temporary `raft_message_view_t` graph includes C-resident Entry arrays,
optional Snapshot/ConfState descriptors, Context, and nested response
descriptors. No Go-allocated message, entry, snapshot, or array descriptor is
passed to C.

### Bootstrap

The temporary C peer array contains `raft_peer_view_t` records. Each
`Peer.Context` preserves `is_nil`; its data is borrowed only during the single
Bootstrap call.

### ConfChange

The temporary C graph contains `raft_conf_change_v2_view_t`, its
`raft_conf_change_single_t` array, and borrowed Context. It preserves ordered
V2 changes, joint transition, nil/empty Context, and the documented
zero-NodeID cancellation compatibility distinction. Legacy-versus-V2 entry
encoding remains a Phase 3 conversion responsibility.

## Ready and Advance

Ready functions now return a C-owned object:

```c
int raft_raw_node_ready(raft_raw_node_t *rn, raft_ready_t **out);
int raft_raw_node_ready_without_accept(raft_raw_node_t *rn,
                                       raft_ready_t **out);
void raft_ready_destroy(raft_ready_t *ready);
```

Go batch-converts the full C graph to ordinary Go-owned values and calls
`raft_ready_destroy` once. It does not call C per Entry, Message, Snapshot, or
ReadState and does not retain C child pointers.

Preview acceptance receives the same C-resident Ready pointer before it is
destroyed. Advance is:

```c
int raft_raw_node_advance(raft_raw_node_t *rn);
```

The future C RawNode tracks the accepted Ready internally, matching current Go
RawNode behavior. Go does not reconstruct or pass Ready back. This safety
change advanced the private skeleton ABI marker to version 6. The subsequent
explicit progress-snapshot API rename advanced it to 7, and removal of the
redundant two-ID leadership-transfer declaration leaves the current marker at
version 8.

## Output conversion

C-to-Go output always starts from C-owned memory:

- Ready Entries, CommittedEntries, Messages, Snapshot, and ReadStates;
- full Status and ConfState;
- progress snapshot arrays;
- ApplyConfChange ConfState.

Go deep-copies the whole object graph into Go/raftpb values, invokes the
corresponding C free/destroy function once, and retains no C pointer.
BasicStatus is pointer-free fixed-size output and has no nested pointer risk.

## Storage callback ownership

Exported Go Storage callbacks deep-copy results into C-owned non-view objects
before returning:

- InitialState returns scalar HardState plus C-owned ConfState arrays;
- Entries returns one C-owned Entry array per range callback;
- Term, FirstIndex, and LastIndex return scalars;
- Snapshot returns one C-owned Snapshot graph.

C frees callback-owned output when finished. No raw Go pointer survives the
callback. Entries is batch-oriented; C does not callback into Go once per
entry. Callback allocation failure returns `RAFT_ERR_OUT_OF_MEMORY` after
partial C cleanup, while callback panic remains
`RAFT_ERR_PANIC_FROM_GO_CALLBACK`.

## RawNode cgo-safety audit

| RawNode method | Input kind | Go descriptor risk | Required safe path | Status |
| --- | --- | --- | --- | --- |
| NewRawNode | special/config + callbacks | Config is pointer-free; callback state would be risky as a raw Go pointer | Pointer-free config/table; `uintptr_t` `cgo.Handle`; C copies table | API safe; bridge Phase 4 |
| Bootstrap | aggregate | Peer array embeds byte pointers | One temporary C peer block, one Bootstrap call, immediate free | Documented/tested skeleton |
| Tick | scalar | None | Direct call | Safe |
| TickQuiesced | scalar | None | Direct call | Safe |
| Campaign | scalar | None | Direct call | Safe |
| Propose | flat byte | Go `raft_byte_view_t` would contain a Go pointer | `raft_raw_node_propose_from_parts` | Implemented/tested |
| ProposeConfChange | aggregate | Context and changes descriptor graph | One temporary C block, one call, immediate free | Documented/tested skeleton |
| ApplyConfChange | aggregate + output | Same input risk; ConfState output owns arrays | Temporary C input; C-owned output; batch copy/free | Documented/tested skeleton |
| Step | aggregate | Message/Entry/Snapshot/ConfState graph | One temporary C graph, one Step call, immediate free | Documented/tested skeleton |
| Ready | output | Go-allocated Ready would receive C child pointers | C returns owned Ready; batch copy; `raft_ready_destroy` | API implemented; semantics stubbed |
| readyWithoutAccept / acceptReady | output/special | Reconstructed Go Ready would be unsafe | C-owned preview; accept same C pointer | API implemented; semantics stubbed |
| HasReady | output scalar | None | Direct call | Safe |
| Advance | special | Ready reconstruction risk | Argument-free `raft_raw_node_advance(rn)` | Implemented stub |
| ReadIndex | flat byte | Same as Propose | `raft_raw_node_read_index_from_parts` | Implemented/tested |
| Status | output | Status contains C-owned arrays | C-resident output container; batch copy; `raft_status_free` | Contract documented |
| BasicStatus | pointer-free output | No nested pointer risk | One direct call into fixed-size output | Safe |
| WithProgress / progress snapshot | owned output | Returned array contains copied rows | One `raft_raw_node_progress_snapshot` call; batch copy and Go visitor; one `raft_progress_snapshot_array_free` | API implemented; semantics stubbed |
| ReportUnreachable | scalar ID | None | Direct call | Safe |
| ReportSnapshot | scalar ID + enum | None | Direct call | Safe |
| RawNode.TransferLeader | scalar transferee ID | None | Direct `raft_raw_node_transfer_leader` call | Safe |
| ForgetLeader | none | None | Direct call | Safe |

## Cgo call overhead and batching decisions

Go `Node.TransferLeadership(ctx, lead, transferee)` is not a second C
RawNode API. The Go actor constructs
`MsgTransferLeader{From: transferee, To: lead}` and sends the resulting
aggregate through the existing single-shot `raft_raw_node_step` path.

- Propose and ReadIndex use one `*_from_parts` cgo call each.
- Tick, TickQuiesced, Campaign, HasReady, Advance, reporting, transfer, and
  ForgetLeader use one direct call each.
- Step, Bootstrap, ProposeConfChange, and ApplyConfChange make one semantic
  RawNode call per operation. There are no per-field or per-entry builder
  calls.
- A straightforward `C.malloc` temporary implementation adds one allocation
  and one free transition around an aggregate RawNode call: a constant maximum
  of roughly three Go-to-C transitions rather than a field-dependent count.
  One contiguous descriptor block minimizes this cost.
- Phase 3 should benchmark a reusable C scratch block per binding. After
  capacity warm-up it can preserve C-resident descriptors while reducing the
  steady-state aggregate path toward one RawNode transition; borrowed contents
  must still be invalidated after every call and never retained.
- Ready uses one call to obtain the owned graph, direct batched Go reads/copies
  without nested C calls, then one destroy call.
- WithProgress uses one `raft_raw_node_progress_snapshot` producer call, runs
  the user visitor only in Go over converted copies with `Inflights == nil`,
  and makes one array-free call. There is no per-peer C-to-Go callback.
- Storage Entries uses one C-to-Go callback per requested range, returning an
  array, never one callback per entry.

Safety is not traded for a lower call count. Benchmarking cgo transition cost,
temporary allocation, scratch reuse, large Message graphs, and Ready batch
conversion remains a Phase 3/12 TODO.

## Tests

The C skeleton tests now cover:

- Propose/ReadIndex shims for nil and present-empty;
- invalid null data with positive length;
- temporary C-resident Peer, ConfChange, Message, and Entry descriptor graphs;
- invalid nested message shape and temporary cleanup;
- C-owned Ready output-pointer reset, destroy, and argument-free Advance;
- all prior view/owned copy, free, lifecycle, sentinel, enum, and stub cases.

Commands:

```sh
make clean-c && make test-c

mkdir -p .tmp .gocache
TMPDIR="$PWD/.tmp" GOTMPDIR="$PWD/.tmp" GOCACHE="$PWD/.gocache" go test ./...
```

Result: both passed. The default Go path remains pure-Go.

No Go binding exists yet, so `cgocheck` integration tests are recorded as a
Phase 3 requirement rather than executed in this task.

## Remaining questions

- Exact contiguous temporary-block layout and alignment helpers belong in the
  Phase 3 binding implementation.
- Benchmarking must decide whether per-binding reusable C scratch storage is
  worth its complexity.
- Status could later gain a C-owned outer-object constructor/destructor, like
  Ready, to remove separate outer allocation from the Go wrapper.
- Ready preview token/stale-accept error semantics remain to be finalized with
  the real Ready state machine.
