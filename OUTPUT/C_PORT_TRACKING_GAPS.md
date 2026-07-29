# C Port Tracking Gaps

This is the cross-phase checklist for items that are easy to omit while
individual subsystems are ported. “Surfaces” identifies where a mistake would
appear: public C API, Go binding, C core, tests, or documentation.

| Item | Why it matters | Phase placement | Affected surfaces |
| --- | --- | --- | --- |
| `ConfChange.NodeID == 0` cancellation/rejection | Zero is not a member, but existing etcd compatibility paths use it to mean “ignore this config operation.” Treating it as an ordinary invalid ID everywhere breaks differential behavior; accepting it into membership corrupts invariants. | Document/ABI in Phase 2; conversion in Phase 3; real apply semantics in Phase 7. | C API distinction, Go binding, C core, differential tests, docs. |
| `TickQuiesced` | Public RawNode API is deprecated but still source-visible. It increments only election elapsed time and must not be aliased to normal Tick. | Symbol in Phase 2; binding in Phase 3; semantics in Phase 6; parity test in Phase 12. | C API, Go binding, C core, tests. |
| `Bootstrap` and `Peer.Context` | StartNode depends on empty-storage checks, term-1 entries, committed bootstrap config, immediate progress, and contexts encoded in legacy ConfChange entries. | Peer ABI/symbol in Phase 2; copy/marshal in Phase 3; storage/log in Phase 5; membership in Phase 7; Node tests in Phase 12. | C API, binding, log/core, tests, wire/ownership docs. |
| `BasicStatus`, `Status`, and `WithProgress` | They are not one operation: BasicStatus is fixed-size, Status is allocating and leader-only for progress, and WithProgress observes tracker entries on all roles. WithProgress must use one C-owned `raft_raw_node_progress_snapshot` array, never a live pointer or per-peer C-to-Go callback; Go converts rows, leaves Inflights nil, invokes the visitor, and frees once. | ABI in Phase 2; binding/ownership in Phase 3; basic/full status in Phase 6-7; real progress snapshot in Phase 7. | C API, binding, C core, tests, docs. |
| raftpb wire compatibility | Numeric enum drift, lost optional presence, changed ConfChange encoding, or collapsing nil and empty bytes can make peers/applications incompatible even if C unit tests pass. Borrowed `*_view_t` and owned non-view types are distinct; `bool is_nil` preserves nil/empty and null descriptors remain invalid. | ABI inventory/wrappers in Phase 2; converters in Phase 3; maintained in all protocol phases; end-to-end in Phase 12. | C ABI, binding, protocol core, wire tests, docs. |
| `ConfChangeV2` and joint consensus | Multi-change replacement requires dual quorums, learners-next, auto-leave, and explicit/implicit transition semantics. A V1-only port is unsafe/incomplete. | Conversion skeleton in Phase 3; quorum/config implementation in Phase 7; snapshot ConfState in Phase 8. | C API/types, binding, core, tests. |
| Config validation/defaults | Zero has field-specific meanings, integer narrowing can wrap, and lease reads require CheckQuorum. Different defaults cause differential divergence. | Basic constructor checks in Phase 2; binding range checks in Phase 3; full field use in Phases 5-11. | Public API, binding, core, table tests, docs. |
| `IsLocalMsg` | Public RawNode Step rejects unexpected local messages, while Node/internal storage paths legitimately step them. | Numeric table in Phase 2-3; Step behavior Phase 6; async message cases Phase 11. | C core, Go binding, Node integration, tests. |
| `IsResponseMsg` | Responses from unknown peers produce RawNode error/filter behavior; local storage senders are exempt. | Binding/error identity in Phase 3; core filtering in Phases 6-7; local exceptions Phase 11. | C core, binding, errors, differential tests. |
| `IsEmptyHardState` | Ready presence is not the same as numeric zeros. Incorrect emptiness emits/drops persistence work. | ABI presence in Phase 2-3; Ready implementation Phase 6. | C Ready API, converters, core, tests. |
| `IsEmptySnap` | Snapshot emptiness is metadata index zero, not nil Data or term zero. | Converter rules Phase 3; log/snapshot logic Phases 5 and 8. | Binding, C core, Ready ownership, tests. |
| Ready ownership and deep copies | Go transport/application outlives C calls. Borrowed C pointers create use-after-free and violate cgo rules. `*_view_t` input is never retained/freed; non-view output is always owned and recursively freed. C shims must construct pointer-bearing view descriptors outside Go memory. | View/owned/copy/free APIs Phase 2; C-side binding builders Phase 3; callback allocations Phase 4; Ready Phase 6; snapshots Phase 8. | Public API, binding, C allocation code, sanitizers, docs. |
| Ready preview/accept/Advance | Node must not consume a preview until channel delivery succeeds, and Advance must work after C output is freed. | Symbols Phase 2; binding Phase 3; semantics Phase 6; async completion Phase 11. | C API, binding, core, Node actor, tests. |
| Go-allocated pointer-containing descriptors | Passing `&C.raft_message_view_t{...}` or `&C.raft_byte_view_t{...}` when the object is Go memory containing Go pointers can violate cgo pointer rules. | Flat scalar shims and aggregate C-allocation contract Phase 2; binding implementation/cgocheck tests Phase 3. | C shim API, Go binding, tests, docs. |
| Cgo call batching | Per-field/per-entry builder calls are safe but can multiply transition overhead on Step/Bootstrap/ConfChange and Ready conversion. | Single-shot descriptor design Phase 2; implementation/benchmarks Phase 3; end-to-end profiling Phase 12. | Go binding, C conversion helpers, benchmarks, docs. |
| `MaxUncommittedEntriesSize` | It bounds leader memory and intentionally returns proposal-dropped. Omitting it changes overload behavior. | Config in Phase 2-3; accounting in Phase 6; multinode/differential tests Phase 7/12. | Config API, C core, error mapping, tests. |
| Inflights, flow control, optimistic replication | Match/Next alone are insufficient. Probe/replicate/snapshot state, count/byte windows, pause/resume, and rejection hints drive liveness and throughput. | Phase 7. Snapshot-state completion continues in Phase 8. | C core, status/progress ABI, tests, docs. |
| `ReadOnlySafe` versus `ReadOnlyLeaseBased` | Safe mode requires quorum confirmation; lease mode relies on clocks and CheckQuorum. Conflating them can violate linearizability. | Config enum Phase 2-3; ReadIndex Phase 9; CheckQuorum/ForgetLeader interaction Phase 10. | Public config, C core, binding, safety tests. |
| `Peer.Context` nil/empty/lifetime | Bootstrap context is opaque application data copied into legacy ConfChange entries. `raft_peer_view_t.context` is borrowed; losing its `is_nil` shape or retaining its pointer changes committed application input or violates lifetime rules. | Peer ABI Phase 2; C-side descriptor construction/deep-copy and protobuf conversion Phase 3; Bootstrap Phase 5/7. | C API, binding, wire/ownership tests. |
| Explicit error mapping | The expanded skeleton now separates not-implemented, storage-unavailable, peer-not-found, callback panic, OOM, and fatal errors. Phase 3 must preserve their Go mappings and later phases must return them correctly. | Enum established in Phase 2; wrapper mapping Phase 3; callback mapping Phase 4; maintain throughout. | C API, binding, callbacks, core, tests, docs. |
| `with_tla` / `TraceLogger` build behavior | TraceLogger has a real method only under `with_tla`; otherwise it is an empty interface and hooks are no-ops. A hard-coded bridge can fail one build or alter timing/reentrancy. | Decide in Phase 3; optional bridge with logger work; validate in Phase 12. | Go binding/build tags, optional callbacks, tests, docs. |
| AsyncStorageWrites local targets | Local append/apply IDs are reserved endpoints; same-target ordering, durability, response delivery, and prohibition of Advance are correctness rules. | Sentinels in Phase 2; conversion in Phase 3; full behavior Phase 11. | C API/core, binding, application routing, tests. |
| `MsgSnap` completion feedback | Leader progress stays paused while snapshot is outstanding. Failure must be reported after transport completes, which can outlive Ready. | Ownership in Phase 3; snapshot core in Phase 8; integration Phase 12. | Ready/message ABI, binding, transport, C core, tests. |
| Storage callback result ownership/batching | Exported Go callbacks cannot return Go Entry/Snapshot memory for C retention, and Entries must not callback once per entry. | Owned callback ABI Phase 2; batched deep-copy bridge Phase 4; storage/core tests Phases 5/12. | Callback ABI, Go bridge, C ownership/free code, tests, docs. |

## Phase review gates

### Before Phase 3

- Freeze or version enum numeric mappings and error taxonomy.
- Add missing Bootstrap, TickQuiesced, BasicStatus, and progress symbols.
- Decide transfer function naming.
- Keep byte presence conversion aligned with the Phase 2 wrapper contract:
  canonical nil uses `{NULL,0,true}`, present-empty uses
  `{NULL,0,false}`, and
  no dummy zero-length allocation is introduced. Both use non-null descriptor
  pointers; null descriptors are invalid.
- Keep pointer-bearing descriptors in C storage. The Go binding calls scalar
  shims or builds one temporary C descriptor graph and never passes the
  address of a Go-allocated view struct.
- Keep aggregate calls to one RawNode cgo transition per operation; do not
  replace unsafe descriptors with field-by-field builder calls.
- Decide how Ready preview identity survives output memory free.

### Before Phase 7

- Prove that real member IDs exclude all sentinels.
- Preserve the internal zero-NodeID config-change no-op separately from public
  validation.
- Have differential fixtures for ConfChange V1/V2, joint entry/leave, learners,
  progress state, and error behavior.

### Before Phase 11

- Route local targets without network transport.
- Test same-target reliability/order and attached response gating.
- Ensure synchronous Ready/Advance behavior still passes independently.

### Before Phase 12

- Run default and C-backed Go suites.
- Exercise `with_tla` and non-`with_tla` build combinations if tracing remains
  supported.
- Run memory tools and cgo pointer checks.
- Verify every RawNode API row in `RAWNODE_API_PARITY_FOR_C_PORT.md`.
