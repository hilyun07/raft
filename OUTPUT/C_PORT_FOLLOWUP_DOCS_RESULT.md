# C Port Follow-up Documentation Result

This follow-up audited the current Go API and C skeleton against
`prompts/RAFT_C_PORTING_SPEC.md`. It preserves the existing planning/result
documents and adds focused tracking for topics that did not fit cleanly into
them.

## Files created

| File | Purpose |
| --- | --- |
| `OUTPUT/RAWNODE_API_PARITY_FOR_C_PORT.md` | Complete RawNode-to-C API matrix, stub policy, implementation phase, and semantic traps. |
| `OUTPUT/CONFIG_FIELDS_FOR_C_PORT.md` | Go Config to C mapping, validation, defaults, conversion ranges, and phase ownership. |
| `OUTPUT/RAFTPB_WIRE_COMPAT_FOR_C_PORT.md` | raftpb type/enum compatibility, optional presence, and nil-versus-empty payload rules. |
| `OUTPUT/READY_OWNERSHIP_FOR_C_PORT.md` | Recursive C Ready ownership, deep-copy/free rules, preview/accept/Advance lifetime, and persistence ordering. |
| `OUTPUT/ERROR_MAPPING_FOR_C_PORT.md` | Target C error taxonomy, Go mappings, current skeleton gaps, callback errors, and compatible no-op behavior. |
| `OUTPUT/C_PORT_TRACKING_GAPS.md` | Cross-phase checklist of easy-to-miss API, safety, ownership, build-tag, and testing concerns. |
| `OUTPUT/C_PORT_FOLLOWUP_DOCS_RESULT.md` | This result summary. |

## Files updated

| File | Addition |
| --- | --- |
| `OUTPUT/SENTINEL_NODE_IDS_FOR_C_PORT.md` | Distinguished `ConfChange.NodeID == 0` cancellation/rejection from general `RAFT_NONE`, added WithProgress ID invariants, and updated implementation status. |
| `OUTPUT/PHASE2_C_RAWNODE_SKELETON_RESULT.md` | Added “Follow-up additions required” with present/missing C API surface and links to the new audits. |
| `OUTPUT/PHASE1_RAWNODE_BOUNDARY_PLAN.md` | Linked the complete later C API parity requirements and clarified Status versus WithProgress. |
| `OUTPUT/PHASE1_RAWNODE_BOUNDARY_RESULT.md` | Recorded the later C-boundary follow-up without changing the completed Phase 1 result. |

No `PHASE3_GO_RAWNODE_BINDING_RESULT.md` exists yet, so no Phase 3 result file
was updated. The Phase 2 follow-up section records what the eventual Phase 3
result must cover.

## Most important newly tracked risks

1. **Status does not replace WithProgress.** Go full Status includes progress
   only on leaders, while WithProgress enumerates tracked peers/learners on
   every role. The current C status vector alone is not API parity.
2. **Zero has two node-related meanings.** `RAFT_NONE` is the general
   node-ID sentinel, while `ConfChange.NodeID == 0` is also a narrow
   app-visible cancellation/no-op compatibility representation. Zero must
   never enter membership/progress state.
3. **The original skeleton API was incomplete.** Bootstrap, TickQuiesced,
   BasicStatus, and explicit progress enumeration were subsequently added by
   the expanded Phase 2 skeleton.
4. **The original error names were ambiguous.** The expanded Phase 2 skeleton
   now separates storage-unavailable, not-implemented, peer-not-found,
   callback-panic, out-of-memory, and fatal results.
5. **C output lifetime must be decoupled from Advance.** Go must deep-copy and
   free C Ready output immediately, but Node may call Advance later. The final
   C API needs an internal acceptance state or opaque generation rather than
   dereferencing freed Ready payloads.
6. **raftpb compatibility includes presence and payload shape.** Fixed numeric
   fields alone lose proto2 absent-versus-zero information. Legacy ConfChange,
   V2/joint consensus, nested storage responses, and nil/empty bytes require
   explicit conversion tests.
7. **Bootstrap is observable protocol behavior.** It is not just a convenience
   constructor and must preserve peer context, entry encoding, term/index,
   committed state, and immediate progress setup.
8. **Config defaults are semantic.** In particular, zero means different
   things for message size, committed size, uncommitted size, and inflight
   bytes; integer conversion must not wrap.
9. **Build-tag behavior remains a compatibility surface.** `TraceLogger`
   changes interface shape under `with_tla`; default and tracing builds need a
   deliberate binding policy.

## Byte payload API correction

The Phase 2 skeleton subsequently standardized payload conversion on
`const *_view_t *` for borrowed API inputs and non-view types for
retained/output data. Public structs are not passed by value, and a null
descriptor is invalid rather than a nil-slice encoding. `bool is_nil`
preserves nil versus present-empty; ownership comes exclusively from the type,
not a flag or dynamic kind. Recursive copy/free helpers enforce the boundary.
This replaces repeated pointer/length/nil parameters and the earlier generic
buffer placeholder; see
`C_BYTE_PAYLOAD_API_CORRECTION_RESULT.md` and
`C_BYTE_PAYLOAD_POINTER_API_CORRECTION_RESULT.md`, with the final convention
in `C_TYPE_SEPARATED_PAYLOAD_API_RESULT.md` and the binding-safety/performance
audit in `C_GO_POINTER_SAFE_RAWNODE_API_RESULT.md`.

## Open questions for the next implementation phase

- Progress parity is now settled: use only the C-owned
  `raft_raw_node_progress_snapshot` array and free it with
  `raft_progress_snapshot_array_free`. The Go binding invokes the visitor in
  Go; no synchronous C-to-Go progress callback is permitted.
- Leadership transfer mapping is settled:
  `raft_raw_node_transfer_leader(rn, transferee)` is the sole C RawNode helper.
  Go `Node.TransferLeadership(ctx, lead, transferee)` stays in the Go
  actor/channel layer and routes
  `MsgTransferLeader{From: transferee, To: lead}` through
  `raft_raw_node_step`.
- How will Ready preview identity be represented so output can be freed before
  later acceptance/Advance calls?
- Will C message structs gain explicit scalar/byte presence bits, or will
  message-type-specific Go conversion retain protobuf presence?
- The expanded skeleton replaced the unconsumed error enum and advanced its
  private ABI marker. Further renumbering now requires an explicit ABI policy.
- Will legacy `ConfChange.ID` and V1/V2 entry encoding be represented in C or
  remain marshalled in the Go binding?
- Will Logger/TraceLogger remain Go-side, and what build checks are required
  for `with_tla`?
- Should strict public APIs reject zero membership changes while a separate
  internal compatibility decoder accepts zero as cancellation? The documents
  recommend this split.

## Production-code impact

No production C or Go logic was changed by this follow-up documentation task.
The repository already contained C skeleton changes from earlier approved
work; they were inspected but not modified here.
