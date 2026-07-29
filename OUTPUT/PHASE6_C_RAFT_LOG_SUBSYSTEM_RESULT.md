# Phase 6 C Raft Log Subsystem Result

## Outcome

Phase 6 adds a private C implementation of the etcd-io/raft v3.7.0
`raftLog`/`unstable` boundary and connects it to the existing Storage callback
ABI. The C RawNode constructor now initializes this log from Storage
`FirstIndex` and `LastIndex`, and destruction recursively releases all
unstable state.

This is not a consensus implementation. Elections, replication state,
progress/quorum, configuration changes, read-only processing, and actual
RawNode Ready/Advance production remain stubs. The default build remains the
pure-Go implementation.

## Inputs inspected

All requested documents existed:

- `prompts/RAFT_C_PORTING_SPEC.md`;
- `OUTPUT/PORTING_BASELINE.md`;
- `OUTPUT/PHASE1_RAWNODE_BOUNDARY_PLAN.md`;
- `OUTPUT/PHASE1_RAWNODE_BOUNDARY_RESULT.md`;
- `OUTPUT/C_PORT_FOLLOWUP_DOCS_RESULT.md`;
- `OUTPUT/SENTINEL_NODE_IDS_FOR_C_PORT.md`;
- `OUTPUT/RAWNODE_API_PARITY_FOR_C_PORT.md`;
- `OUTPUT/CONFIG_FIELDS_FOR_C_PORT.md`;
- `OUTPUT/RAFTPB_WIRE_COMPAT_FOR_C_PORT.md`;
- `OUTPUT/READY_OWNERSHIP_FOR_C_PORT.md`;
- `OUTPUT/ERROR_MAPPING_FOR_C_PORT.md`;
- `OUTPUT/C_PORT_TRACKING_GAPS.md`;
- `OUTPUT/PHASE2_C_RAWNODE_SKELETON_RESULT.md`;
- `OUTPUT/C_TYPE_SEPARATED_PAYLOAD_API_RESULT.md`;
- `OUTPUT/C_GO_POINTER_SAFE_RAWNODE_API_RESULT.md`;
- `OUTPUT/WITH_PROGRESS_C_PORT_AUDIT_RESULT.md`;
- `OUTPUT/TRANSFER_LEADERSHIP_API_CLEANUP_RESULT.md`;
- `OUTPUT/STEP_FOR_NODE_LAYERING_DOC_UPDATE_RESULT.md`;
- `OUTPUT/PHASE4_GO_RAWNODE_BINDING_SKELETON_RESULT.md`;
- `OUTPUT/PHASE5_STORAGE_CALLBACK_BRIDGE_RESULT.md`.

The implementation audit covered Go `log.go`, `log_unstable.go`,
`storage.go`, `rawnode.go`, `raft.go`, their focused tests, the C public and
private headers, the tagged cgo bridge, and existing C tests/build rules.

## C files created

| File | Purpose |
| --- | --- |
| `c/src/unstable.h` | Private owned unstable-log/snapshot state and helper declarations. |
| `c/src/unstable.c` | Unstable index, term, slice, append/truncate, restore, in-progress, and stabilization behavior. |
| `c/src/log.h` | Private storage-backed log state and helper declarations. |
| `c/src/log.c` | Storage/unstable composition, conflict/append, commit/apply tracking, Ready-facing selection, snapshots, scanning, and size limiting. |
| `c/tests/unstable_test.c` | Focused pure-C unstable ownership and state-transition tests. |
| `c/tests/log_test.c` | Pure-C raft-log tests backed by a batched fake Storage. |

## Existing implementation files updated

- `c/src/raft_internal.h` gives the opaque RawNode an owned `raft_log_t` and
  advances the private implementation marker from 9 to 10.
- `c/src/raw_node.c` initializes the log from the copied Storage table,
  propagates initialization errors, frees the log on destroy, and reports the
  log's applied index through `BasicStatus`.
- `rawnode_cgo_bridge.c` includes the two private implementation units in the
  opt-in cgo amalgamation.
- `c/Makefile` compiles both implementation units and builds/runs the new
  focused test binaries.
- `storage_bridge_cgo_test.go` gives the default test Storage a valid empty-log
  `FirstIndex`, verifies constructor callback use, and verifies constructor
  error propagation plus `cgo.Handle` cleanup.

The public C header was not expanded: the log subsystem is intentionally
private.

## `raft_unstable_t`

The implemented state contains:

- an optional owned snapshot and in-progress bit;
- an owned `raft_entry_vec_t`;
- `offset`;
- `offset_in_progress`.

Implemented helpers cover:

- `raft_unstable_maybe_first_index`;
- `raft_unstable_maybe_last_index`;
- `raft_unstable_maybe_term`;
- `raft_unstable_slice`;
- next-entry and next-snapshot copies;
- current snapshot copy for log fallback;
- `raft_unstable_accept_in_progress`;
- `raft_unstable_stable_to`;
- `raft_unstable_stable_snap_to`;
- `raft_unstable_restore`;
- `raft_unstable_truncate_and_append`.

`stable_to` advances only when both index and term match. Snapshot
stabilization clears only a matching snapshot index. Restore replaces
unstable entries, resets the offset to `snapshot.index + 1`, and installs an
owned snapshot copy. Truncate/append covers direct extension, complete
replacement, and suffix replacement.

## `raft_log_t`

The implemented private state contains:

- a copied `raft_storage_ops_t`;
- `raft_unstable_t`;
- committed, applying, and applied indexes;
- maximum/in-progress applying entry bytes and pause state.

Implemented helpers cover:

- constructor/destructor;
- first index, last index, last term, term, term matching, and the Go
  zero-term-on-compacted/unavailable rule;
- storage/unstable `slice`, `entries`, and paged `scan`;
- append, conflict search, conflict-by-term, and maybe-append;
- commit-to and maybe-commit;
- applied-to and accept-applying;
- next/available committed entries with `allow_unstable`;
- next and in-progress unstable entries/snapshot;
- Ready-style acceptance/stable notifications;
- snapshot lookup and restore;
- candidate log up-to-date comparison;
- protobuf-entry size limiting.

`allow_unstable` remains private and represents the existing
`applyUnstableEntries = !AsyncStorageWrites` decision. It was not added to the
public API.

## Storage-backed access

Construction calls Storage `FirstIndex` and `LastIndex`, initializes the
unstable offset to `lastIndex + 1`, and initializes committed/applying/applied
to `firstIndex - 1`.

Term and range lookup prefer unstable state when applicable and otherwise use
the copied callback table. `Entries(lo, hi, maxSize)` remains one batched
Storage callback, never one callback per entry. A slice crossing the stable
boundary takes the stable prefix from Storage and appends a deep-copied
unstable suffix.

Successful Storage entry arrays and snapshot graphs are C-owned callback
outputs. The log either transfers their owned rows into its returned vector
or frees them after use. It validates returned object shape, starting index,
and entry continuity before consumption.

`InitialState` is intentionally not called by `raft_log_init`: the Go
`newLogWithSize` equivalent needs only first/last indexes. Full Raft
construction must consume InitialState in a later consensus phase.

## Ownership

- Every retained unstable entry and `Entry.Data` is a C-owned deep copy.
- The unstable snapshot owns Data and ConfState vectors recursively.
- View/input pointers are never stored.
- Snapshot restore and append copy input before returning.
- Returned log/unstable vectors and snapshots are owned copies and use the
  existing recursive free functions.
- The C RawNode frees the complete log before the Go binding deletes the
  Storage `cgo.Handle`.
- No Go pointer is retained in `raft_log_t` or `raft_unstable_t`; only the
  integer `cgo.Handle` value inside the copied Storage table remains.

Valgrind leak/error checking passes for all three C test binaries.

## Snapshot behavior

The log:

- resolves the term at the unstable snapshot index;
- derives first/last indexes from snapshot and unstable entries;
- returns an owned unstable snapshot copy before falling back to Storage;
- preserves `RAFT_ERR_SNAPSHOT_TEMPORARILY_UNAVAILABLE`;
- restores committed state to the snapshot index;
- marks an offered snapshot in progress separately from persistence;
- clears the unstable snapshot only after a matching stable notification.

This is internal log/snapshot ownership only. Snapshot transport and follower
progress remain later work.

## Commit, apply, and Ready preparation

`commit_to` is monotonic and returns `RAFT_ERR_FATAL` when asked to commit
beyond the last index. Applying/applied operations reject backward or
out-of-commit movement and track the outstanding encoded-size quota.

The log can now copy out:

- unstable entries for `Ready.Entries`;
- committed entries for `Ready.CommittedEntries`;
- an unstable snapshot for `Ready.Snapshot`.

It can mark unstable work in progress at Ready acceptance and later stabilize
entries by matching index/term or a snapshot by matching index. The public
RawNode Ready, accept, HasReady, and Advance functions are not wired to these
helpers yet and continue to report their previous skeleton behavior.

## Error mapping and invariants

- compacted ranges/terms return `RAFT_ERR_STORAGE_COMPACTED`;
- unavailable ranges/terms return `RAFT_ERR_STORAGE_UNAVAILABLE`;
- a temporarily unavailable snapshot is preserved exactly;
- callback panic, OOM, and fatal results propagate without remapping;
- malformed public/helper arguments return
  `RAFT_ERR_INVALID_ARGUMENT`;
- Go panic-equivalent invariant failures return `RAFT_ERR_FATAL`.

The implementation checks ordered/contiguous entries, valid storage bounds,
append continuity, committed bounds, applied/applying ordering, overflow, and
unstable slice bounds. It does not silently continue after invariant failure.

## Entry-size note

The limiter follows Go's rule that a non-empty request returns at least its
first entry even when the maximum is zero. It accounts for the protobuf
encoding of represented Entry fields, not only payload bytes.

The current C Entry skeleton lacks scalar protobuf presence bits. Consequently
zero-valued scalar fields are counted as absent. Exact size parity must be
revisited if the ABI gains presence-aware scalar fields or generated C
protobuf encoding.

## Tests added and updated

The pure-C tests cover:

- unstable first/last/term resolution with and without snapshots;
- term-matched stabilization, snapshot stabilization, restore, and
  in-progress state;
- direct append, full replacement, suffix truncation, and payload deep copy;
- constructor FirstIndex/LastIndex calls and offset initialization;
- first/last/term composition across Storage, snapshot, and unstable state;
- stable/unstable boundary slices, paged scan, and max-size-zero behavior;
- append, conflict, conflict-by-term, maybe-append, zero-term error handling,
  and candidate up-to-date checks;
- monotonic commit, out-of-range fatal behavior, applied/applying state;
- committed selection with both `allow_unstable` values;
- Ready-style unstable/snapshot acceptance and stabilization;
- snapshot restore and owned-copy behavior;
- compacted, unavailable, and temporarily unavailable snapshot errors;
- batched C-owned fake-Storage entry results and cleanup.

Tagged Go tests verify that C RawNode creation invokes the Go Storage
`FirstIndex`/`LastIndex` callbacks exactly once, does not yet invoke
`InitialState`, propagates a log-initialization error, and deletes the
`cgo.Handle` on constructor failure.

## Commands and results

```text
make -C c clean test
```

Passed both before and after the sanitizer run: the normal C11
warning-as-error build compiled and ran the RawNode skeleton, unstable, and
log test binaries.

```text
make test-cgo-raft
```

Passed.

```text
GOEXPERIMENT=cgocheck2 CGO_ENABLED=1 \
  go test -count=1 -tags=cgo_raft ./...
```

Passed.

```text
mkdir -p .tmp .gocache
TMPDIR=$PWD/.tmp GOTMPDIR=$PWD/.tmp GOCACHE=$PWD/.gocache go test ./...
```

Passed: the default path remained pure Go.

```text
for test_bin in \
  c/.build/raw_node_skeleton_test \
  c/.build/unstable_test \
  c/.build/log_test
do
  valgrind --quiet --leak-check=full --errors-for-leak-kinds=all \
    --error-exitcode=99 "$test_bin"
done
```

All three binaries passed independently with no reported errors.

The sanitizer runtime installation was revalidated with:

```text
ASAN_OPTIONS=detect_leaks=1:halt_on_error=1 \
UBSAN_OPTIONS=print_stacktrace=1:halt_on_error=1 \
make -C c clean test \
  CFLAGS='-O1 -g -fsanitize=address,undefined -fno-omit-frame-pointer'
```

Passed. All C sources and all three test binaries compiled and linked with
ASan/UBSan, and all three binaries ran successfully. There were no
AddressSanitizer, LeakSanitizer, or UndefinedBehaviorSanitizer findings. The
previous missing-runtime linker failure is resolved.

```text
git diff --check
```

Passed.

## Remaining gaps

- RawNode Ready/Advance/HasReady remain stubs despite having the required log
  primitives.
- Full RawNode construction still needs Storage `InitialState`, Config.Applied
  application, HardState recovery, and Raft state-machine initialization.
- No elections, message handling, replication, tracker/quorum, flow control,
  configuration changes, ReadIndex, or async-storage protocol was added.
- The public Step unknown-response check still awaits the C progress tracker.
- Exact entry-size differential tests should accompany any future
  presence-aware raftpb C representation.
- Allocation-failure injection and a Go-vs-C differential log harness remain
  useful later tests. Sanitizer runtime installation is also an environment
  TODO.

These gaps are deferred because addressing them would broaden Phase 6 beyond
the log/unstable/storage-backed subsystem.

## Phase 7 follow-up

Phase 7 now consumes this subsystem from `c/src/raft_core.c`. RawNode
construction calls Storage `InitialState`, loads HardState/ConfState, applies
`Config.Applied`, and initializes the minimal state machine. Ready/Advance now
uses the unstable, committed, accept-in-progress, stable, and applied helpers.
The Phase 6 bullets above remain the historical state at completion of that
phase; current implementation status is recorded in
`PHASE7_C_RAFT_CORE_MINIMAL_RESULT.md`.
