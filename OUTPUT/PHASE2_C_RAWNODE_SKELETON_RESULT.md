# Phase 2: C RawNode Public API Skeleton Result

The C skeleton now reflects the full RawNode surface tracked by the follow-up
planning documents. It remains a compile-safe API boundary with no Raft
election, tracker, quorum, read-only, configuration-change, or Ready/Advance
consensus logic. Phase 6 subsequently added the private `raft_log_t` and
`raft_unstable_t` subsystem described below; this does not make the RawNode
consensus stubs operational.

The existing pure-Go implementation remains the default and does not import
cgo or compile the C subtree.

## Files updated

| File | Change |
| --- | --- |
| `c/include/raft/raft.h` | Expanded the public ABI with opaque RawNode ownership, sentinels, helpers, error taxonomy, raftpb-compatible enums/types, borrowed/owned byte wrappers, callback shapes, complete RawNode declarations, and ownership/free APIs. |
| `c/src/raft_internal.h` | Retained the private RawNode layout; Phase 6 adds an owned internal `raft_log_t` and advances the private marker to version 10. |
| `c/src/raw_node.c` | Added external sentinel/payload validators, recursive view-to-owned deep-copy helpers, owned frees, shallow API/config validation, lifecycle allocation, inert status, and `RAFT_ERR_NOT_IMPLEMENTED` stubs. |
| `c/tests/raw_node_skeleton_test.c` | Expanded public-header-only tests for enum values, sentinels, lifecycle, every stub family, view/owned nil-empty validation, recursive copies, reserved IDs, and owned frees. |
| `OUTPUT/PHASE2_C_RAWNODE_SKELETON_RESULT.md` | Replaced the earlier partial result with this full tracked-surface result. |

The existing `c/Makefile`, root `test-c`/`clean-c` targets, and `.gitignore`
integration continue to provide a separate C build. No Go source, protobuf
source, Storage implementation, or transport code changed.

## Public RawNode ownership

The public header exposes only:

```c
typedef struct raft_raw_node raft_raw_node_t;
```

The private header stores an ABI marker, normalized/copied configuration,
copied storage callback table, and—after Phase 6—an owned private
`raft_log_t`. The log contains C-owned unstable entries/snapshot state and
uses the copied callback table for stable storage access. It contains no Raft
role/election/progress state and no raw Go pointer.

Phase 4 subsequently added two binding-only boundary helpers:

- `raft_raw_node_step_for_node`, the internal Go actor Step mode;
- `raft_raw_node_has_progress`, a scalar query that never exposes a live
  tracker pointer.

The public Step entry point applies the public local-message check and then
delegates to the actor helper:

```text
raft_raw_node_step
  -> public RawNode.Step validation
  -> raft_raw_node_step_for_node
       -> direct core Step
```

`raft_raw_node_step_for_node` corresponds to `node.run`'s package-internal
`r.Step` path. It deliberately does not call `raft_raw_node_step` and does not
apply public Step validation. External C users call
`raft_raw_node_step`; the Go C-backed Node actor calls the helper directly.
The tracker-dependent public check for response messages from unknown
non-local peers remains deferred until the C progress tracker exists. After
the available validation, Step reaches the compile-safe core stub and returns
`RAFT_ERR_NOT_IMPLEMENTED`; HasProgress returns false until the tracker phase.

`raft_raw_node_new` validates pointer/config/callback-table shape, allocates
the handle, and now initializes its log by calling Storage `FirstIndex` and
`LastIndex`. Allocation failure returns `RAFT_ERR_OUT_OF_MEMORY`; callback
errors are propagated. `raft_raw_node_destroy` recursively frees the log,
zeroes and frees the handle, and accepts null.

## Sentinel node IDs

The header defines:

```c
#define RAFT_NONE UINT64_C(0)
#define RAFT_LOCAL_APPEND_THREAD UINT64_MAX
#define RAFT_LOCAL_APPLY_THREAD (UINT64_MAX - UINT64_C(1))
```

It exports linkable helpers:

```c
bool raft_is_none_id(uint64_t id);
bool raft_is_local_target_id(uint64_t id);
bool raft_is_valid_node_id(uint64_t id);
```

Constructor, Bootstrap peer, configuration-change, reporting, and leadership
APIs reject reserved IDs at strict public boundaries. `Step` intentionally
does not apply a blanket real-ID rule because `RAFT_NONE` and local targets
have valid message-specific internal meanings.

The future internal configuration-change implementation must separately
preserve the narrow Go compatibility meaning of `ConfChange.NodeID == 0` as a
cancelled/no-op change. This skeleton does not implement that core path.

## Error taxonomy

The public enum now defines:

- `RAFT_OK`;
- `RAFT_ERR_INVALID_ARGUMENT`;
- `RAFT_ERR_STOPPED`;
- `RAFT_ERR_PROPOSAL_DROPPED`;
- `RAFT_ERR_STORAGE_COMPACTED`;
- `RAFT_ERR_STORAGE_UNAVAILABLE`;
- `RAFT_ERR_SNAPSHOT_TEMPORARILY_UNAVAILABLE`;
- `RAFT_ERR_STEP_LOCAL_MSG`;
- `RAFT_ERR_STEP_PEER_NOT_FOUND_OR_IGNORED`;
- `RAFT_ERR_PANIC_FROM_GO_CALLBACK`;
- `RAFT_ERR_OUT_OF_MEMORY`;
- `RAFT_ERR_FATAL`;
- `RAFT_ERR_NOT_IMPLEMENTED`.

The previous ambiguous `RAFT_ERR_UNAVAILABLE` is no longer used for missing
implementation. Storage-unavailable remains a storage-domain error, while
consensus-dependent skeleton calls return `RAFT_ERR_NOT_IMPLEMENTED`.

No Go error conversion or callback panic firewall was implemented; those
belong to the build-tagged binding and callback phases.

## raftpb-compatible enum skeletons

The public header assigns the current Go/raftpb numeric values for:

- all MessageType values from `MsgHup=0` through `MsgForgetLeader=23`;
- EntryType values `EntryNormal=0`, `EntryConfChange=1`, and
  `EntryConfChangeV2=2`;
- all four ConfChangeType values;
- all three ConfChangeTransition values;
- follower/candidate/leader/pre-candidate state;
- peer/learner progress type;
- probe/replicate/snapshot progress state;
- safe/lease-based read-only option;
- finish/failure snapshot status.

These C names may differ from Go names, but the numeric assignments are
explicitly documented as binding/wire compatibility requirements.

## Public data types

The ABI now includes:

- `raft_message_view_t` and owned `raft_message_t`;
- `raft_entry_view_t` and owned `raft_entry_t`;
- `raft_snapshot_view_t`, owned `raft_snapshot_t`, and their metadata;
- `raft_hard_state_t` and `raft_soft_state_t`;
- `raft_conf_state_view_t` and owned `raft_conf_state_t`;
- legacy `raft_conf_change_view_t`;
- `raft_conf_change_v2_view_t` and `raft_conf_change_single_t`;
- `raft_read_state_t`;
- `raft_ready_t`;
- `raft_basic_status_t` and `raft_status_t`;
- `raft_progress_t` and owned output `raft_progress_snapshot_t`;
- `raft_peer_view_t`;
- `raft_config_t`.

The earlier repeated `(pointer, length, is_nil)` shape and
`raft_buffer_t` placeholder were replaced by two explicit wrappers:

- `const raft_byte_view_t *` for direct borrowed API input, including Propose
  and ReadIndex; configuration and Peer input structs embed the descriptor and
  are themselves passed by `const` pointer;
- `raft_bytes_t` for C-owned returned/nested data, including
  Entry.Data, Message.Context, Snapshot.Data, and ReadState request contexts.

`bool is_nil` distinguishes nil from present-empty without dummy zero-length
allocations. Ownership is not a flag or runtime kind: `raft_byte_view_t` is
borrowed input and `raft_bytes_t` is always C-owned. Shared validation helpers
enforce the canonical forms and recursive copy helpers cross from view to
owned data.

Pointer-bearing aggregates use the same type split:
`raft_entry_view_t`/`raft_entry_t`,
`raft_snapshot_view_t`/`raft_snapshot_t`, and
`raft_message_view_t`/`raft_message_t`. RawNode Step accepts only
`const raft_message_view_t *`; Ready and Storage outputs use owned types.

The corrected payload-taking signatures are:

```c
int raft_raw_node_propose(raft_raw_node_t *raw_node,
                          const raft_byte_view_t *data);
int raft_raw_node_read_index(raft_raw_node_t *raw_node,
                             const raft_byte_view_t *request_context);
int raft_raw_node_step(raft_raw_node_t *raw_node,
                       const raft_message_view_t *message);
int raft_raw_node_step_for_node(raft_raw_node_t *raw_node,
                                const raft_message_view_t *message);
```

There are no legacy overloads with separate pointer, length, and nil
parameters. There are also no struct-by-value public arguments: descriptor
pointer nullness is validated independently and never represents a nil slice.
The public Step function delegates to the lower-level Node helper after public
validation; the helper never delegates in the opposite direction.

`raft_ready_t` has explicit presence flags for SoftState, HardState, and
Snapshot plus an opaque token reserved for a future preview/accept/advance
identity that does not depend on pointers into freed output.

Legacy ConfChange and V2 are distinct types, preserving room for legacy
`ConfChange.ID` and V1-versus-V2 entry encoding.

## BasicStatus, Status, and WithProgress

The skeleton exposes all three distinct shapes:

```c
raft_raw_node_basic_status
raft_raw_node_status
raft_raw_node_progress_snapshot
```

BasicStatus and full Status safely return `RAFT_OK` for the allocated inert
handle. They report the configured ID, follower state, no leader/vote/active
transferee, and empty full configuration/progress.

`raft_raw_node_progress_snapshot` is the role-independent mapping for Go
`WithProgress`. It returns only a C-owned point-in-time copy, has no visitor
callback, exposes no live tracker or inflights pointer, and is paired with
`raft_progress_snapshot_array_free`. The skeleton resets its output
pointer/length and returns `RAFT_ERR_NOT_IMPLEMENTED`; it does not falsely
claim that the tracker is empty. Full `Status.progress` remains separately
documented as leader-only once real semantics exist.

The later Go C-backed method will make one snapshot call, convert the rows,
map peer/learner types, explicitly leave `tracker.Progress.Inflights` nil,
invoke the supplied visitor in Go, and free the C array once.

## Bootstrap and TickQuiesced

The missing symbols are now present:

```c
raft_raw_node_bootstrap
raft_raw_node_tick_quiesced
```

Bootstrap performs only shallow validation: non-null/non-empty peer array,
real peer IDs, and well-formed nil/empty contexts. Valid calls return
`RAFT_ERR_NOT_IMPLEMENTED`.

`tick_quiesced`, like `tick`, is a void inert stub. This is safe only because
the C backend is not enabled. The later core must implement the Go distinction:
normal Tick runs role-specific clock behavior, while TickQuiesced increments
only election elapsed time.

## ForgetLeader and TransferLeader

The API includes:

```c
raft_raw_node_forget_leader
raft_raw_node_transfer_leader(rn, transferee)
```

`raft_raw_node_transfer_leader(rn, transferee)` is the canonical mapping for
Go `RawNode.TransferLeader(transferee)`. It performs reserved-ID/null
validation and otherwise returns `RAFT_ERR_NOT_IMPLEMENTED`.

Go `Node.TransferLeadership(ctx, lead, transferee)` remains a Go
actor/channel-layer routing API. The Go Node constructs
`MsgTransferLeader{From: transferee, To: lead}` and submits it through
`RawNode.stepForNode` / `raft_raw_node_step_for_node`; there is no separate
`raft_raw_node_transfer_leadership` C function.

ForgetLeader likewise validates the handle and returns not implemented. No
leader, lease, transfer, catch-up, or timeout logic was added.

## Ready API and ownership hooks

The full tracked Ready surface is declared and linkable:

```c
raft_raw_node_has_ready
raft_raw_node_ready
raft_raw_node_ready_without_accept
raft_raw_node_accept_ready
raft_raw_node_advance
```

The inert skeleton has no work, so `has_ready` returns false. All other valid
calls return `RAFT_ERR_NOT_IMPLEMENTED`; Ready-producing calls set their
`raft_ready_t **` output to null. Future successful calls allocate and return
the C-owned outer Ready plus its nested graph, released once with
`raft_ready_destroy`.

`raft_raw_node_advance(raw_node)` has no Ready argument. The future C core
tracks accepted completion internally, matching current Go RawNode behavior
and avoiding both reconstruction overhead and a pointer-bearing Go-allocated
Ready passed back into C.

Recursive ownership functions cover:

- `raft_bytes_t` and uint64 vectors;
- entries and entry arrays/vectors;
- snapshots and ConfState;
- messages and message arrays/vectors, including nested responses;
- read states;
- Ready;
- progress-record arrays;
- Status.

Free functions accept zero/partially initialized output. `raft_bytes_free`
always releases owned `data` and then resets the wrapper to canonical nil;
parent functions recursively apply that rule. Borrowed
configuration-change and peer input views have no free function. No conversion
to Go or C-to-Go deep-copy path was implemented.

The skeleton does implement generic view-to-owned copy helpers for bytes,
entries, snapshots, and messages, including nested arrays and partial-failure
cleanup. These are allocation/ownership plumbing only, not consensus logic.

The Go binding must not allocate pointer-containing view descriptors in Go
memory. `raft_raw_node_propose_from_parts` and
`raft_raw_node_read_index_from_parts` construct simple byte views in C
storage. Aggregate APIs use a single-shot temporary C-allocation strategy:
Go fills `C.malloc`/`C.calloc` descriptor graphs, makes one RawNode call, and
frees the descriptors immediately. This avoids unsafe Go descriptor memory
without adding per-field or per-entry cgo builder calls.

## Storage and logger callback ABI

`raft_storage_ops_t` contains an opaque `uintptr_t handle` and callbacks for:

- InitialState;
- Entries;
- Term;
- FirstIndex;
- LastIndex;
- Snapshot.

The constructor copies the table rather than retaining its address. The handle
is suitable for `runtime/cgo.Handle` and must not be a raw Go pointer. Phase 5
implements `raft_go_storage_ops_init`, all six exported Go callbacks, and a
private C call-through harness used by bridge tests. Phase 6 invokes
`FirstIndex`/`LastIndex` during C log construction and lets the private log
consume batched `Entries`, `Term`, and owned `Snapshot` results. Exported
callbacks batch-copy Entries and Snapshot results into C-owned non-view graphs
before returning. Entries is one array result per range callback, not one Go
callback per entry; C frees owned callback results when finished.

`raft_logger_ops_t` reserves an opaque handle plus log callback shape. The
current RawNode does not accept, store, or call it. Logger/TraceLogger bridging
is explicitly deferred, including `with_tla` behavior.

## Stub return policy

The following operations return `RAFT_ERR_NOT_IMPLEMENTED` after shallow
argument validation:

- Bootstrap;
- Campaign;
- Propose and ProposeConfChange;
- ApplyConfChange;
- Step;
- Ready, preview, accept, and Advance;
- ReadIndex;
- progress enumeration;
- ReportUnreachable and ReportSnapshot;
- transferee-only `raft_raw_node_transfer_leader`;
- ForgetLeader.

The following have safe skeleton results:

- `new`/`destroy`: real allocation lifecycle plus private log initialization
  and cleanup (`RAFT_OK` on success);
- `basic_status`/`status`: fixed skeleton state with the log's real applied
  index (`RAFT_OK`);
- `has_ready`: false because the skeleton creates no work;
- `tick`/`tick_quiesced`: void no-op required by their signatures.

The void/false placeholders must not be interpreted as implemented consensus
behavior and cannot be used by an enabled C backend.

## Validation implemented

Only state-machine-independent validation was added:

- null output/input pointers;
- required Storage callback table shape;
- reserved/real node-ID checks;
- positive heartbeat/inflight count and election greater than heartbeat;
- read-only enum and lease-mode/CheckQuorum relationship;
- MaxInflightBytes versus MaxSizePerMessage relationship;
- enum ranges for message and config-change inputs;
- non-null descriptors, canonical `is_nil` byte shapes, and pointer/length
  consistency;
- non-empty Bootstrap peers;
- public V2 change IDs and operation/transition ranges;
- snapshot status range.

No current-membership lookup, quorum rule, joint-consensus transition,
message-locality classification, log range, or Ready semantic validation was
implemented.

## Build integration

The C subtree remains separate:

```sh
make test-c
```

It compiles C11 with:

```text
-Wall -Wextra -Werror -Wpedantic
```

The default root `test` target and Go package graph do not depend on the C
library, cgo, or a C compiler.

## Commands and results

### Clean C skeleton build/test

```sh
make clean-c
make test-c
```

Result: passed. The source compiled from a clean `.build` directory, the
static library linked, and the public-header-only test binary exited
successfully.

### Default pure-Go tests

```sh
mkdir -p .tmp .gocache
TMPDIR="$PWD/.tmp" \
GOTMPDIR="$PWD/.tmp" \
GOCACHE="$PWD/.gocache" \
go test ./...
```

Result:

```text
ok  	go.etcd.io/raft/v3	(cached)
ok  	go.etcd.io/raft/v3/confchange	(cached)
ok  	go.etcd.io/raft/v3/quorum	(cached)
ok  	go.etcd.io/raft/v3/raftpb	(cached)
ok  	go.etcd.io/raft/v3/rafttest	(cached)
ok  	go.etcd.io/raft/v3/tracker	(cached)
```

This confirms that the default Go path remains pure-Go and unaffected.

## Deferred work

- Go/cgo RawNode binding and type conversion;
- storage/logger callbacks and `cgo.Handle` lifecycle;
- callback panic recovery and C-to-Go error mapping;
- protobuf optional scalar presence beyond the byte/presence skeleton;
- bootstrap/log/core/tracker/quorum/confchange/read-only/snapshot semantics;
- Ready preview/accept/Advance token semantics;
- role-independent progress enumeration;
- async local storage message handling;
- build-tagged C backend enablement and differential tests.

## Phase 7 follow-up

The symbols and ownership hooks established here are now backed by a minimal
C raft state machine for bootstrap, ticking, elections, normal proposals,
basic vote/append/heartbeat Step handling, synchronous Ready/Advance, status,
and copied progress snapshots. The earlier stub descriptions remain a record
of Phase 2, not current runtime behavior. Advanced calls still returning
`RAFT_ERR_NOT_IMPLEMENTED` are enumerated in
`PHASE7_C_RAFT_CORE_MINIMAL_RESULT.md`.
