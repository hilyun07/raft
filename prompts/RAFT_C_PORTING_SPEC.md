아래는 Codex에게 그대로 붙여넣을 수 있는 작업 프롬프트입니다.

You are working on porting the etcd-io/raft library to C while keeping it usable from etcd’s existing Go code.

Your goal is not merely to rewrite isolated functions. Design and implement a C-backed Raft library whose canonical low-level API is a C `RawNode` API, while preserving the existing Go `raft.Node` API used by etcd as much as possible.

Primary design target:

```text
C public API:
  raft_raw_node_t
  raft_message_t
  raft_ready_t
  raft_entry_t
  raft_snapshot_t
  raft_storage_ops_t

Go binding:
  type RawNode struct { p *C.raft_raw_node_t }

Go Node:
  channel/context/select actor layer built on top of Go RawNode binding

etcd:
  should continue using the existing Go raft.Node API with minimal or no application-level changes
```

Do not put all C interaction directly inside the Go `node` struct unless absolutely necessary. Prefer:

```text
Go node
  -> Go RawNode binding
      -> C raft_raw_node_t
          -> C raft core
              -> C storage callback ABI
                  -> exported Go bridge
                      -> Go application Storage implementation
```

The Go `Node` layer should remain Go-specific: goroutines, channels, `context.Context`, `Ready()` channel, `Advance()` channel, `Stop()`, and user-facing `Node` interface behavior should stay in Go where possible.

The C `RawNode` should be the canonical synchronous Raft API. The Go `RawNode` should be a thin binding that performs:

1. Go/C type conversion.
2. C error code to Go error conversion.
3. C memory lifetime management.
4. cgo callback bridge setup.
5. No consensus logic if avoidable.

Before implementing, inspect the current repository. Do not assume the exact current source layout or line numbers. Identify:

* current `Node` interface;
* current `RawNode` methods;
* current `Ready` structure and semantics;
* `Storage` interface;
* `raft`, `raftLog`, `unstable`, progress tracker, readOnly, confchange, quorum modules;
* all places where `node.run()` directly accesses `rn.raft` internals.

Important: currently `node.run()` may not treat `RawNode` as fully opaque. Refactor that. The Go `node.run()` should stop reaching into `rn.raft` fields directly. Replace direct field access with methods/getters on Go `RawNode`, backed by C APIs if needed.

C API requirements:

Expose an opaque C type:

```c
typedef struct raft_raw_node raft_raw_node_t;
```

Do not expose the internal struct layout in the public header.

Provide C APIs corresponding semantically to Go `RawNode`:

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

int raft_raw_node_new(const raft_config_t *cfg,
                      const raft_storage_ops_t *storage,
                      raft_raw_node_t **out);

void raft_raw_node_destroy(raft_raw_node_t *rn);

void raft_raw_node_tick(raft_raw_node_t *rn);
int raft_raw_node_campaign(raft_raw_node_t *rn);
int raft_raw_node_propose(raft_raw_node_t *rn,
                          const raft_byte_view_t *data);
int raft_raw_node_propose_conf_change(raft_raw_node_t *rn,
                                      const raft_conf_change_v2_view_t *cc);
int raft_raw_node_step(raft_raw_node_t *rn,
                       const raft_message_view_t *msg);

bool raft_raw_node_has_ready(const raft_raw_node_t *rn);
int raft_raw_node_ready(raft_raw_node_t *rn,
                        raft_ready_t **out);
int raft_raw_node_advance(raft_raw_node_t *rn);

int raft_raw_node_apply_conf_change(raft_raw_node_t *rn,
                                    const raft_conf_change_v2_view_t *cc,
                                    raft_conf_state_t *out);

int raft_raw_node_read_index(raft_raw_node_t *rn,
                             const raft_byte_view_t *ctx);

int raft_raw_node_report_unreachable(raft_raw_node_t *rn,
                                      uint64_t id);

int raft_raw_node_report_snapshot(raft_raw_node_t *rn,
                                  uint64_t id,
                                  raft_snapshot_status_t status);

int raft_raw_node_status(const raft_raw_node_t *rn,
                         raft_status_t *out);
```

If the Go `Node` implementation needs the current Go split between `readyWithoutAccept()` and `acceptReady()`, add C APIs for that semantic split. These can be public or internal/private to the Go binding:

```c
int raft_raw_node_ready_without_accept(raft_raw_node_t *rn,
                                       raft_ready_t **out);

int raft_raw_node_accept_ready(raft_raw_node_t *rn,
                               const raft_ready_t *rd);
```

The C `RawNode` must implement the full `RawNode` semantic boundary, not just the lower raft state machine:

* `HasReady`;
* `Ready`;
* `readyWithoutAccept`;
* `acceptReady`;
* `Advance`;
* `MustSync` calculation;
* `prevSoftState` / `prevHardState` comparison;
* read states draining;
* messages draining;
* `msgsAfterAppend` semantics;
* unstable entries/snapshot accepted handling;
* `stepsOnAdvance`;
* `AsyncStorageWrites` semantics if the current library supports them.

Preserve all important `Ready` semantics:

`Ready` is the output batch from Raft to the application. It may contain:

* `SoftState`: volatile observation only, not persisted.
* `HardState`: persist to stable storage.
* `Entries`: append to stable storage.
* `Snapshot`: save/apply snapshot if non-empty.
* `CommittedEntries`: apply to application state machine in order.
* `Messages`: send to remote peers or route to local storage workers depending on type/target.
* `ReadStates`: complete pending linearizable reads only after local applied index reaches the given read index.
* `MustSync`: indicates whether HardState/Entries persistence must be durable, e.g. fsync.

Safety-critical rules that must be preserved:

1. `HardState`, `Entries`, and `Snapshot` must be persisted accurately.
2. In normal mode, messages that depend on newly accepted entries/votes/terms must not be sent before the required persistent state is saved. Preserve the existing `Messages` / `msgsAfterAppend` ordering semantics.
3. `CommittedEntries` and snapshots must be applied in log order, without reordering, omission, or duplicate application after restart.
4. The `Storage` implementation must return accurate persisted log/snapshot state.
5. Config changes must only be applied through `ApplyConfChange` when the corresponding config-change entry is committed and applied by the application. If the application rejects a config change as a noop, do not call `ApplyConfChange`.
6. Returned `ConfState` must be preserved for future snapshots.
7. `ReadIndex` reads must not be answered until the local applied index is at least the returned `ReadState.Index`.
8. `ReadOnlyLeaseBased`, if supported, must retain its clock/quorum assumptions and `CheckQuorum` requirements.
9. If `AsyncStorageWrites` is supported, local storage messages to the same target must be reliable and in-order. `MsgStorageAppend` must be durable before its response messages are delivered. `Advance()` must not be called in async storage write mode.

Message handling rules:

* Most `Ready.Messages` are remote Raft protocol messages and should be sent to `Message.To`.
* `MsgSnap` requires special snapshot transport handling and snapshot result feedback via `ReportSnapshot`.
* Local/internal messages must not be sent over the network.
* If `AsyncStorageWrites` is enabled, messages targeting `LocalAppendThread` or `LocalApplyThread` must be routed to local storage/apply workers, not to remote peers.
* Responses attached to storage messages must be delivered after the storage/apply operation completes.
* Preserve peer-local message ordering where required. Different remote peers may be sent concurrently, but do not reorder messages to the same local storage target.

Storage callback bridge:

The Go `Storage` interface is application-provided. C cannot directly call a Go interface. Implement a C callback ABI plus Go exported bridge functions.

C-side storage callback table should look like this conceptually:

```c
typedef struct raft_storage_ops {
    uintptr_t handle;

    int (*initial_state)(uintptr_t handle,
                         raft_hard_state_t *hs,
                         raft_conf_state_t *cs);

    int (*entries)(uintptr_t handle,
                   uint64_t lo,
                   uint64_t hi,
                   uint64_t max_size,
                   raft_entry_vec_t *out);

    int (*term)(uintptr_t handle,
                uint64_t index,
                uint64_t *term);

    int (*first_index)(uintptr_t handle,
                       uint64_t *index);

    int (*last_index)(uintptr_t handle,
                      uint64_t *index);

    int (*snapshot)(uintptr_t handle,
                    raft_snapshot_t *out);
} raft_storage_ops_t;
```

On the Go side, use `runtime/cgo.Handle` to store Go bridge objects. Do not store raw Go object pointers in C memory.

Good pattern:

```go
h := cgo.NewHandle(&storageBridge{storage: appStorage})
C.raft_raw_node_new(..., C.uintptr_t(h), ...)
```

Bad pattern:

```c
void *go_storage; // do not store a raw Go object pointer in C
```

The exported Go callback should recover panics at the Go/C boundary and convert them to fatal C error codes. Do not let Go panic cross the C ABI boundary.

Example pattern:

```go
//export raftStorageTermBridge
func raftStorageTermBridge(handle C.uintptr_t,
                           index C.uint64_t,
                           out *C.uint64_t) (rc C.int) {
    defer func() {
        if r := recover(); r != nil {
            // log panic and stack if possible
            rc = C.RAFT_ERR_PANIC
        }
    }()

    b := cgo.Handle(handle).Value().(*storageBridge)

    term, err := b.storage.Term(uint64(index))
    if err != nil {
        return encodeStorageErr(err)
    }

    *out = C.uint64_t(term)
    return C.RAFT_OK
}
```

`recover` here is not normal control flow. It is only a boundary firewall. A panic should usually mark the raft node or process as fatal; do not continue as if nothing happened.

Error mapping must be explicit. Do not collapse all Go errors to one code. At minimum distinguish:

* OK;
* compacted;
* unavailable;
* snapshot temporarily unavailable;
* proposal dropped;
* stopped;
* local message error;
* fatal storage error;
* panic.

Memory ownership rules:

Be strict. Do not let C retain pointers to Go memory after a cgo call returns.

For Go-to-C:

* Public APIs do not pass structs by value. Use `const *_view_t *` for
  pointer-bearing borrowed input. A null descriptor is invalid; Go nil is a
  non-null descriptor with `is_nil = true`.
* The Go binding must not allocate pointer-containing view descriptors in Go
  memory. Call C scalar-part shims that construct the descriptor in C storage.
* If C stores or uses data after the call, deep-copy into C-owned memory.
* C-owned/internal/output bytes use `raft_bytes_t`; ownership is determined by
  this non-view type, never by a flag or runtime kind. `is_nil` preserves Go
  nil versus empty without dummy zero-length allocations.
* Pointer-bearing aggregates follow the same split:
  `raft_entry_view_t`/`raft_entry_t`,
  `raft_snapshot_view_t`/`raft_snapshot_t`, and
  `raft_message_view_t`/`raft_message_t`.
* `Entries`, `Snapshot.Data`, `Entry.Data`, `Message.Context`, and other byte payloads must have explicit ownership.

For C-to-Go:

* Prefer deep-copying C `Ready`, `Message`, `Entry`, and `Snapshot` structures into Go protobuf types, then freeing the C structures.
* Provide explicit `raft_ready_destroy`, `raft_message_free`,
  `raft_entry_vec_free`, and similar functions for C-owned allocations.
* Avoid exposing C-owned memory to Go application code beyond a controlled conversion boundary.

cgo pointer rules:

* Never store raw Go pointers in C structs.
* Use `cgo.Handle` for Go objects that C must refer to later.
* Delete handles only after the C raw node is destroyed and cannot call back anymore.
* Do not delete a handle while callbacks may still be in flight.
* Avoid passing Go values that contain Go pointers into C memory.

Forbidden Go binding patterns:

```go
// Forbidden: Go-allocated descriptor containing a Go data pointer.
v := C.raft_byte_view_t{/* data points into b */}
C.raft_raw_node_propose(rn, &v)

// Forbidden: Go-allocated aggregate descriptor graph.
m := C.raft_message_view_t{/* pointer-bearing fields */}
C.raft_raw_node_step(rn, &m)
```

Flat bytes call `*_from_parts`, which constructs the descriptor in C.
Bootstrap, Step, and ConfChange use one temporary descriptor graph allocated
with `C.malloc`/`C.calloc`, one RawNode cgo call, and immediate descriptor
cleanup. Do not replace this with field-by-field builder cgo calls.

Ready output is allocated in C, batch-copied into Go, and recursively freed
once. `raft_raw_node_advance(rn)` tracks accepted Ready state internally and
does not accept a reconstructed Ready descriptor.

Storage callbacks deep-copy Entries/ConfState/Snapshot results into C-owned
graphs before returning. `Entries` returns a batch array in one callback, not
one callback per entry.

Deadlock and reentrancy rules:

This bridge creates possible Go -> C -> Go call chains. Avoid hidden deadlocks.

* Do not hold a Go `RawNode` or `Node` mutex while calling callback-capable C functions.
* Do not hold a C raft mutex while invoking Go callbacks.
* Go storage callbacks must not call back into the same raft node API (`Status`, `Propose`, `Step`, `Ready`, `Advance`, etc.).
* Go storage callbacks should avoid blocking channel sends/receives.
* Storage callbacks should be short, synchronous storage reads. If a callback may block on disk I/O, ensure no raft-core lock is held.
* Clearly document which C APIs may invoke callbacks.

Logger and TraceLogger:

* `Storage` is mandatory.
* `Logger` bridge is optional but useful.
* `TraceLogger` bridge is optional, and only necessary if the current build supports tracing.
* Logger callbacks should also be guarded against panic and must not create deadlocks.

Go `Node` preservation:

The Go `Node` interface should remain as compatible as possible:

* `Tick`;
* `Campaign`;
* `Propose`;
* `ProposeConfChange`;
* `Step`;
* `Ready`;
* `Advance`;
* `ApplyConfChange`;
* `TransferLeadership`;
* `ForgetLeader`;
* `ReadIndex`;
* `Status`;
* `ReportUnreachable`;
* `ReportSnapshot`;
* `Stop`.

`Node` should remain a Go channel actor. It should serialize access to the underlying C `RawNode`, preserving the current thread-safety behavior of `Node` while `RawNode` remains thread-unsafe.

`RawNode` thread-safety:

* C `raft_raw_node_t` should be documented as thread-unsafe.
* The Go `Node` wrapper provides single-owner serialization.
* The Go `RawNode` binding should not claim to be thread-safe.

Do not assume `Propose()` means committed. Preserve existing semantics:

* `Propose` success means accepted into local processing path, not committed.
* Actual completion is observed through `Ready.CommittedEntries`.
* Proposals and ReadIndex requests may be lost; retry remains application responsibility.

Restart and Storage:

* Preserve `StartNode` / `RestartNode` semantics.
* `RestartNode` must recover membership from `Storage`.
* `Config.Applied` must prevent already-applied entries from being returned again after restart.
* `InitialState`, snapshot metadata, HardState, ConfState, and applied index must remain consistent.

Implementation strategy:

Do the work in small, testable phases.

Phase 1: Inventory and refactor boundaries.

* Identify all direct `rn.raft` access in Go `node.run()`.
* Introduce Go `RawNode` methods/getters to replace direct internal access.
* Ensure tests still pass before adding C.

Phase 2: Define C public/private headers.

* Add opaque `raft_raw_node_t`.
* Add C types for config, message, entry, snapshot, ready, soft/hard state, conf state, status.
* Add explicit free functions.
* Add error enum.

Phase 3: Implement C RawNode skeleton.

* Implement allocation/destroy.
* Stub or port core methods incrementally.
* Add C unit tests where possible.

Phase 4: Port core state machine.

* Port raft core election, step, append, heartbeat, pre-vote, read-index, leadership transfer.
* Port raftLog, unstable, tracker/progress, quorum, confchange, readOnly.
* Preserve deterministic behavior and existing invariants.

Phase 5: Implement Storage callbacks.

* Add C storage callback table.
* Add Go bridge using `cgo.Handle`.
* Add error mapping.
* Add panic guards.
* Add memory copy/free paths.

Phase 6: Implement C Ready/Advance.

* Implement `HasReady`, `Ready`, `readyWithoutAccept`, `acceptReady`, `Advance`.
* Preserve `MustSync`, `msgsAfterAppend`, unstable accepted handling, read states, local storage messages, async storage semantics if supported.

Phase 7: Implement Go RawNode binding.

* `type RawNode struct { p *C.raft_raw_node_t; handles ... }`
* Methods should mostly call corresponding C functions.
* Convert Go protobuf messages/entries/snapshots to C and back.
* Manage C memory.

Phase 8: Rewire Go Node.

* Keep Go `Node` channel actor.
* Make it use Go `RawNode` binding.
* Remove direct dependency on Go `raft` internals.
* Preserve `context.Context`, `ErrStopped`, proposal result handling, status handling, stop/done semantics.

Phase 9: Compatibility tests.

* Run existing raft tests.
* Add tests comparing Go-original behavior and C-backed behavior where possible.
* Test proposal, election, log replication, snapshot send/restore, conf changes, read-index, restart, compaction, storage errors, async storage writes if supported.
* Test cgo bridge failure cases: invalid handle, storage error, panic in callback, memory free paths.

Phase 10: etcd integration.

* Build etcd against the C-backed raft package.
* Run etcd unit/integration tests relevant to raft, WAL, snapshot, membership, lease/read paths.
* Keep etcdserver application-level changes minimal. If any etcd code must change, document exactly why.

Acceptance criteria:

1. Existing Go `raft.Node` API remains source-compatible for etcd as much as possible.
2. C `RawNode` is a real public API, not an internal accident.
3. Go `RawNode` binding maps semantically 1:1 to C `raft_raw_node_*` functions.
4. Go `Node` remains a Go channel actor layered on Go `RawNode`.
5. No raw Go pointers are stored in C memory.
6. All C allocations crossing the boundary have explicit ownership and free functions.
7. Go callback panics are recovered at the boundary and converted to fatal C error codes.
8. Storage errors are mapped accurately.
9. Safety-critical Ready/Advance/storage/message/config-change/read-index rules are preserved.
10. Existing raft behavior tests pass or deviations are documented with justification.
11. etcd can build and run against the C-backed raft package.

When unsure, prefer preserving existing semantics over simplifying the C API. Document any deviation from the current Go raft behavior before implementing it.
