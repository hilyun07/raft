# Phase 9 C Snapshot Result

## Outcome

Phase 9 completes the synchronous snapshot path behind the opt-in
`cgo_raft` RawNode. The default build remains the original pure-Go
implementation.

The C backend now supports:

- leader fallback from append replication to `MsgSnap` when the follower
  needs compacted or unavailable log history;
- Storage `Snapshot()` retrieval through the existing callback bridge;
- retry after `ErrSnapshotTemporarilyUnavailable`;
- snapshot progress state and `ReportSnapshot` success/failure transitions;
- follower and candidate receipt of `MsgSnap`;
- obsolete, matching-log fast-forward, and full snapshot restore paths;
- ConfState restoration through the Phase 8 tracker/conf-change subsystem;
- local snapshot delivery through `Ready.Snapshot`;
- snapshot persistence/application completion through synchronous
  Ready/Advance;
- compaction-aware first/last/term behavior at the snapshot boundary;
- deep ownership across Storage, message, Ready, and Go/cgo conversions.

No etcd backend snapshot streaming was added. The Raft core continues to
treat `Snapshot.Data` as opaque application bytes.

## Architecture and dependencies

Phase 9 reuses the current architecture rather than adding another snapshot
subsystem:

```text
Go RawNode API
  -> rawnode_cgo.go
  -> C RawNode
  -> raft_core.c
       -> tracker/confchange
       -> log/unstable
       -> Storage callback table
```

The implementation depends on:

- Phase 5's `Storage.Snapshot` callback and C-owned callback output;
- Phase 6's snapshot lookup, log restore, unstable ownership, boundary term,
  Ready selection, in-progress, and stabilization helpers;
- Phase 7's Ready token/accept/Advance completion model;
- Phase 8's snapshot/probe progress state and transactional ConfState
  restoration.

The C public ABI already contained Snapshot, Message, Ready, Storage, and
ReportSnapshot types/functions, so no public ABI expansion or private ABI
marker change was required.

## Files changed

Implementation:

- `c/src/raft_core.c`;
- `c/src/raw_node.c`;
- `c/Makefile`.

Tests:

- `c/tests/snapshot_test.c` (new);
- `c/tests/raw_node_skeleton_test.c`;
- `rawnode_cgo_test.go`;
- `snapshot_parity_test.go` (new, runs under both backends).

Documentation:

- `OUTPUT/PHASE9_C_SNAPSHOT_RESULT.md`.

## Leader snapshot send path

`core_send_append` now falls back to the snapshot path when either the
previous term or requested entries cannot be read because they are compacted
or unavailable.

The snapshot path mirrors etcd's `maybeSendSnapshot` behavior:

1. Do nothing when the follower is not recently active.
2. Fetch the current snapshot through `raft_log_snapshot`, which first checks
   an unstable snapshot and then calls Storage.
3. Treat `RAFT_ERR_SNAPSHOT_TEMPORARILY_UNAVAILABLE` as a retryable no-send.
4. Reject an empty snapshot (metadata index zero) as a fatal invariant
   failure.
5. Move the follower progress to snapshot state with the snapshot index.
6. Queue an owned `RAFT_MSG_SNAP` containing Data, metadata, and ConfState.

The outbound snapshot is present only inside `Ready.Messages`. It does not
also appear as the leader's local `Ready.Snapshot`.

Storage compacted/unavailable results from term/entry access select snapshot
fallback. Unexpected errors from snapshot retrieval remain explicit C errors
instead of being silently converted to a transport message.

## Snapshot progress and ReportSnapshot

`raft_raw_node_report_snapshot` now constructs the same local
`MsgSnapStatus` event as the Go RawNode and sends it directly to the C core.
It does not route through the public Step local-message rejection layer.

On a leader:

- a failure clears `PendingSnapshot`, enters probe state from the previous
  Match position, and pauses append flow until a later heartbeat interval;
- a success enters probe state using `pendingSnapshot + 1` when that is
  larger than `Match + 1`, then pauses append flow while waiting for the
  remote append response;
- reports for missing progress or progress not in snapshot state are ignored.

A successful `MsgAppResp` aborts snapshot state only when its updated Match
can resume from the leader's current `FirstIndex`. This preserves etcd's
out-of-band snapshot flexibility and avoids abandoning a still-required
snapshot too early.

## Snapshot receive and restore

Followers handle `MsgSnap` directly. Candidates first become followers, as in
the Go core. A higher-term `MsgSnap` also records its sender as the leader.

The receive path deep-copies the borrowed message snapshot before use and
implements three restore outcomes:

1. **Obsolete snapshot**: when `snapshot.index <= committed`, ignore it and
   acknowledge the current commit index.
2. **Matching-log fast-forward**: when the local log already has the
   snapshot's `(index, term)`, advance commit without replacing the log or
   configuration.
3. **Full restore**: install the snapshot as unstable log state and replace
   membership/progress from its ConfState.

Full restore is accepted only when the local node appears in incoming voters,
outgoing voters, or learners. `LearnersNext` is intentionally not checked
because valid configurations also contain such a member in outgoing voters.
The membership check is order-independent; ConfState vectors need not arrive
sorted.

Tracker replacement is prepared before mutating the live log. The temporary
tracker uses the existing max-inflight settings and
`raft_confchange_restore`, including joint voters, learners,
`LearnersNext`, `AutoLeave`, and invariant checks. The log restore itself is
copy-before-swap. Only after both preparations succeed is the live tracker
replaced, so allocation/configuration failures do not leave a partially
restored node.

An accepted full restore returns an `MsgAppResp` at the restored last index.
An ignored or fast-forwarded snapshot responds at the resulting commit index.

## Ready and Advance

The existing Phase 6/7 Ready path already selected an unstable snapshot,
marked it in progress at Ready acceptance, and stabilized it by index during
Advance. Phase 9 adds the missing application completion:

- accepting a Ready containing a snapshot records its metadata index as the
  snapshot application completion;
- Advance first stabilizes the snapshot and then moves applied/applying to
  that index;
- committed entries remain blocked while an unstable or in-progress snapshot
  exists;
- after the snapshot is persisted/applied and Advance completes, only entries
  after the snapshot index can be returned for application.

`MustSync` continues to match Go: a snapshot alone does not force it. Term or
vote changes and unstable entries still do. Snapshot persistence and
application remain mandatory regardless of `MustSync`.

This phase supports the currently enabled synchronous Ready/Advance mode.
`AsyncStorageWrites` remains explicitly unsupported by the C backend.

## Compaction and boundary behavior

The Phase 6 log behavior is now exercised through the Raft core:

- indexes below Storage `FirstIndex` are not read as entries;
- the snapshot boundary term is available at `FirstIndex - 1`;
- an unstable restored snapshot defines first index as `snapshot.index + 1`
  and last index as at least `snapshot.index`;
- compacted or unavailable append history selects snapshot transport;
- after application persists the snapshot and calls Advance, log access falls
  back consistently to the updated Storage boundary;
- entries at and below the snapshot index are not returned again as committed
  application work.

Storage callback error identities remain preserved. Temporary snapshot
unavailability is the one retryable no-error send outcome.

## Memory ownership

- Storage callback snapshots are complete C-owned graphs.
- Outbound `MsgSnap` owns its Snapshot Data and ConfState recursively.
- Ready construction deep-copies the message graph before accepted messages
  are drained.
- Incoming borrowed snapshot views are deep-copied before retention.
- `raft_log_restore` takes another owned copy for unstable state.
- Go conversion deep-copies C Message/Ready snapshots into Go protobuf
  objects before the C graph is destroyed.
- Existing `raft_snapshot_free`, `raft_message_free`,
  `raft_message_vec_free`, and `raft_ready_destroy` paths release all nested
  allocations.

The focused C tests mutate source snapshot data after send/receive copying and
verify the retained output remains unchanged. Sanitizer and Valgrind runs
cover all success, retry, ignored, fast-forward, and restore cleanup paths.

## Tests

The new pure-C snapshot suite covers:

- compacted append fallback to an outbound snapshot;
- `Ready.Messages` versus local `Ready.Snapshot` separation;
- snapshot metadata, ConfState, Data, and deep-copy behavior;
- temporarily unavailable snapshot retry;
- propagation of a non-temporary Storage snapshot error;
- SnapshotFailure and SnapshotFinish progress transitions;
- append-response abortion of snapshot state at the correct first-index
  boundary;
- incoming snapshot restore and copied input;
- Ready Snapshot and HardState;
- commit/applied values before and after Advance;
- restored, unsorted ConfState input;
- post-snapshot entry application without replay below the snapshot;
- matching-log fast-forward without configuration replacement;
- obsolete snapshot rejection;
- recursive free behavior through destruction and Ready cleanup.

Tagged Go tests cover outbound C-to-Go `MsgSnap` conversion, local/outbound
separation, Storage callback invocation, temporary retry, Storage error
identity, and Go-visible progress after both report statuses.

`snapshot_parity_test.go` is compiled unchanged under both the default
pure-Go build and `cgo_raft`. It compares externally visible scripted traces
for:

- leader compacted-log fallback, outbound `MsgSnap`, snapshot metadata/data/
  ConfState, and progress after SnapshotFailure;
- follower full restore, `Ready.Snapshot`, HardState, response index,
  configuration, applied boundary, and the first post-snapshot entry.

Both implementations satisfy the same assertions.

## Validation

All commands below passed on 2026-07-30:

```text
make test-c
make test-c-sanitize
make -C c clean test CC=clang
valgrind --quiet --error-exitcode=99 --leak-check=full \
  --errors-for-leak-kinds=all <each of the seven C test binaries>
CGO_ENABLED=1 go test -tags=cgo_raft ./...
CGO_ENABLED=1 GOEXPERIMENT=cgocheck2 go test -count=1 -tags=cgo_raft ./...
CGO_ENABLED=1 go test -count=1 -race -tags=cgo_raft ./...
go test ./...
git diff --check
```

ASan, LeakSanitizer, and UBSan reported no findings. Valgrind reported no
leaks or memory errors.

## Adaptations from `09_snapshot.txt`

The high-level behavior was implemented directly, but historical structural
details were adapted:

- the active build tag is `cgo_raft`, not the historical example
  `crawnode`;
- snapshot logic was connected to the existing log, unstable, tracker,
  conf-change, Ready, and Storage abstractions rather than introducing
  replacement modules;
- the public C ABI and Go RawNode wrapper were already snapshot-capable and
  remained stable;
- current synchronous Ready token/completion ownership was extended instead
  of passing Ready graphs back into C;
- unexpected Storage snapshot errors are returned using the project's
  explicit error mapping, while the original Go `maybeSendSnapshot` panics
  for errors other than temporary unavailability;
- no etcd-specific streaming or out-of-band payload protocol was added.

## Remaining TODOs and known limitations

Snapshot-specific follow-up:

- add deterministic allocation-failure injection for snapshot message copy,
  temporary tracker construction, log restore, and Ready conversion;
- add randomized/fuzzed snapshot ConfState and compaction transition tests;
- extend differential testing into longer multi-node transport traces and
  restart cycles;
- run higher-level etcd integration tests once more of the C Raft core is
  enabled in those packages.

Broader C-port limitations remain:

- `AsyncStorageWrites` and local storage-thread protocols;
- CheckQuorum and PreVote;
- ReadIndex/read-only protocols;
- leadership transfer;
- ReportUnreachable;
- randomized election timeout parity;
- TraceLogger/`with_tla`.

Large application snapshots still incur the compatibility bridge's deep
copies. Streaming remains an application/transport concern and must preserve
`MsgSnap` completion feedback through `ReportSnapshot`.

## Notes for the next session

- Keep local `Ready.Snapshot` and outbound `Ready.Messages[*].Snapshot`
  semantically distinct.
- Do not mark an unstable received snapshot stable or applied before its
  accepted Ready is completed.
- Continue restoring membership exclusively through the tracker/conf-change
  ownership layer.
- Preserve the matching-log fast-forward path; it must not replace ConfState.
- SnapshotFinish still waits for an append response before normal replication;
  do not send immediately from the report handler.
- Snapshot Data remains opaque and owned at every retained boundary.
