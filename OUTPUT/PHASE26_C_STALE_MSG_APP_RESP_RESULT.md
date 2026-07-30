# Phase 26: C Stale `MsgAppResp` Compatibility Result

## Result

The final-audit finding was **confirmed** in the current pre-Phase-26 C
implementation.

The etcd v3.7.0 Go implementation performs all successful-response follow-up
work only when the response:

1. advances `Progress.Match`, or
2. acknowledges the existing `Progress.Match` while the peer is in probe
   state, which deliberately recovers the peer to replicate state.

The C implementation already calculated the same `updated` predicate and
correctly guarded Progress state changes and inflight release with it.
However, it continued into commit checking, eager commit propagation, and
pending-entry sending when `updated == false`. A stale successful response
could therefore emit a new `MsgApp` and mutate current Progress flow-control
state.

Phase 26 adds one early return for that case. Rejected-response handling,
Phase 17's `LogTerm` conflict optimization, Phase 22 Progress reset behavior,
and all other leader processing remain unchanged.

After the change, no stale-`MsgAppResp` compatibility difference remains in
the audited behavior.

## Authoritative reference

The authoritative reference is the signed etcd-io/raft tag:

```text
v3.7.0
b867cf13f6bc0dae21204302df97bc2355c3af55
```

The current repository's reference Go files have no differences from that tag
in the relevant paths:

```text
git diff v3.7.0 -- raft.go tracker/progress.go tracker/inflights.go
```

produced no output.

The relevant reference locations are:

- `raft.go:1384-1577`: leader `MsgAppResp` handling;
- `tracker/progress.go:202-253`: `MaybeUpdate` and `MaybeDecrTo`;
- `tracker/inflights.go:97-126`: `Inflights.FreeLE`;
- `raft.go:1100-1180`: term handling before state-specific dispatch.

The corresponding C locations are:

- `c/src/raft_core.c:1900-2008`: `core_handle_append_response`;
- `c/src/tracker.c:516-557`: `raft_progress_maybe_update` and
  `raft_progress_maybe_decr_to`;
- `c/src/tracker.c:302-334`: `raft_inflights_free_le`;
- `c/src/raft_core.c:2783-2887`: term handling and state dispatch.

## Exact Go v3.7.0 behavior

### Common processing

After term handling and leader dispatch, Go looks up the sender's Progress.
An unknown sender has no Progress and is ignored. For a known sender, every
`MsgAppResp` that reaches the leader handler first sets:

```text
Progress.RecentActive = true
```

This happens before distinguishing successful and rejected responses.

A response carrying a lower nonzero term never reaches this code. It is
discarded by the common term path. A higher-term response first makes the
leader step down, after which it is not processed as a leader response.

### Successful responses

Go calculates the effective successful-response predicate as:

```text
Progress.MaybeUpdate(message.Index)
    ||
(Progress.Match == message.Index && Progress.State == StateProbe)
```

`MaybeUpdate` returns false for `Index <= Match`. Otherwise it:

- sets `Match = Index`;
- raises `Next` to at least `Index + 1`;
- clears `MsgAppFlowPaused`;
- returns true.

The equal-index probe exception is important. A heartbeat can put a follower
back in contact with the leader without advancing `Match`; an acknowledgment
of the already known match can then recover probe state to replicate state.
It is not treated as stale.

Only when the predicate is true does Go:

- transition probe to replicate;
- recover an eligible snapshot Progress through probe to replicate;
- call `Inflights.FreeLE(Index)` in replicate state;
- attempt commit advancement;
- release pending read-index work after a commit;
- broadcast a new commit;
- send an eager commit update;
- send additional pending entries;
- complete a leadership transfer when the transferee is caught up.

When the predicate is false, the only change is `RecentActive = true`.

### Rejected responses

Go first computes:

```text
nextProbeIndex = RejectHint
```

When `LogTerm > 0`, it always translates `(RejectHint, LogTerm)` through
`raftLog.findConflictByTerm` before calling `MaybeDecrTo`. This ordering also
applies when `MaybeDecrTo` will subsequently classify the rejection as stale.
Phase 26 deliberately preserves that ordering.

`MaybeDecrTo` has state-dependent rejection guards:

- replicate state: `Index <= Match` is stale; `Index > Match` is actionable
  and moves `Next` to `Match + 1`;
- probe or snapshot state: only `Index == Next - 1` is actionable;
  other indexes are stale.

An actionable replicate rejection then transitions to probe. An actionable
rejection sends one append/probe. A stale rejection does neither.

### Inflights

Go does not free inflights for a successful response that fails the effective
update predicate.

An out-of-order response for a newer acknowledged index is not stale merely
because earlier responses are missing. If it raises `Match`,
`Inflights.FreeLE(Index)` frees all inflights through that index. A later
response for one of those older indexes is stale and frees nothing.

An equal-index response in probe state resets flow-control state as part of
the intentional probe-to-replicate transition. This response is meaningful,
not stale.

Rejected responses do not directly call `FreeLE`. An actionable rejection can
reset inflights indirectly when replicate state becomes probe state. A stale
rejection leaves inflights unchanged.

## Message-field audit

| Field | Used by Go? | Affects Progress? | C after Phase 26 | Notes |
|---|---:|---:|---|---|
| `Type` | Yes | Selects handler | Equivalent | Must be `MsgAppResp`. |
| `Term` | Yes | Indirectly | Equivalent | Common Step logic handles lower/higher terms before leader dispatch. |
| `From` | Yes | Yes | Equivalent | Selects the Progress; unknown peers are ignored by the shared filtering/lookup path. |
| `To` | Routing only | No | Equivalent | Does not participate in Progress staleness. |
| `Index` | Yes | Yes | Equivalent | Acknowledged index on success; rejected append anchor on rejection. Its meaning depends on `Reject` and Progress state. |
| `Reject` | Yes | Yes | Equivalent | Selects `MaybeUpdate` versus `MaybeDecrTo`. |
| `RejectHint` | On rejection | Yes | Equivalent | Supplies the candidate backtracking index. |
| `LogTerm` | On rejection when nonzero | Yes | Equivalent | Refines `RejectHint` through `findConflictByTerm` before `MaybeDecrTo`. |
| `Commit` | No | No | Equivalent | Ignored in `MsgAppResp` handling. |
| `Entries` | No | No | Equivalent | Not part of an append response. |
| `Context` | No | No | Equivalent | Not used by this response path. |

`Index` alone therefore does not define every stale response. The success and
rejection branches apply different predicates, and both depend on current
Progress state.

## C behavior before Phase 26

Before this phase, `core_handle_append_response` did the following for a
successful response:

1. called `raft_progress_maybe_update`;
2. applied the equal-index probe exception;
3. guarded state transitions and `raft_inflights_free_le` with `updated`;
4. regardless of `updated`, called `core_maybe_commit`;
5. regardless of `updated`, potentially sent an eager commit append;
6. regardless of `updated`, called `core_send_pending_entries`.

This was a control-flow mismatch with Go's enclosing `if`.

The concrete reproduction used by the new tests is:

```text
peer Progress is StateReplicate at Match=2
    ↓
leader has sent entry 3
    ↓
ReportUnreachable transitions the peer to StateProbe
    Match=2, Next=3, MsgAppFlowPaused=false
    ↓
stale successful MsgAppResp(Index=1) arrives
```

Go sets `RecentActive` and stops. Before Phase 26, C entered
`core_send_pending_entries`, sent entry 3, and set
`message_flow_paused = true`. The stale response therefore changed current
flow-control state and produced an extra externally visible message.

The mismatch did not by itself violate Raft safety, but it changed message
emission, flow control, and the observable Progress snapshot.

## Implementation

One production file changed:

### `c/src/raft_core.c`

After the existing successful-response `updated` block, Phase 26 adds:

```c
if (!updated) {
    return RAFT_OK;
}
```

This places commit checking, eager commit propagation, pending-entry sending,
and leadership-transfer evaluation behind the same effective predicate as
Go.

No Progress structure, message field, ABI, public API, storage path, cgo path,
or ownership rule changed.

### Preserved Phase 17 behavior

Rejected responses still:

1. initialize the candidate backtracking index from `RejectHint`;
2. call `raft_log_find_conflict_by_term` when `LogTerm > 0`;
3. pass the translated hint to `raft_progress_maybe_decr_to`;
4. act only when `maybe_decr_to` returns true.

The new return is in the non-rejection path and cannot bypass the Phase 17
optimization.

### Preserved Phase 22 behavior

Phase 26 adds no generation number, timestamp, or reset-specific state.

After a real Progress reset:

- an old-term response is discarded by normal term handling;
- a current-term response is judged solely by the same current Progress
  predicates as Go;
- an equal-index response in probe state remains actionable;
- a numerically advancing response remains actionable.

This matches etcd v3.7.0, which has no per-append generation identifier.

## Scenario matrix

| Scenario | Go v3.7.0 | C after Phase 26 | Equivalent? |
|---|---|---|---:|
| Current successful response | Raises `Match`; updates `Next`; performs state/inflight/commit/send follow-up | Same | Yes |
| Stale successful response (`Index < Match`) | Marks active only | Marks active only | Yes |
| Equal successful response in replicate state | Marks active only | Marks active only | Yes |
| Equal successful response in probe state | Recovers to replicate and may send pending entries | Same | Yes |
| Current replicate rejection (`Index > Match`) | Moves `Next` to `Match+1`, becomes probe, sends append | Same | Yes |
| Stale replicate rejection (`Index <= Match`) | Marks active; no Progress rollback | Same | Yes |
| Current probe/snapshot rejection (`Index == Next-1`) | Applies bounded hint, unpauses, sends append | Same | Yes |
| Stale probe/snapshot rejection (`Index != Next-1`) | Marks active; no rollback | Same | Yes |
| Duplicate response | Probe-equal is meaningful; otherwise stale by the branch-specific guard | Same | Yes |
| Out-of-order response | Newer acknowledgment can advance and free covered inflights; delayed older response is inert | Same | Yes |
| `LogTerm == 0` rejection | Uses `RejectHint` directly | Same | Yes |
| `LogTerm > 0` rejection | Translates hint before `MaybeDecrTo` | Same | Yes |
| Stale rejection plus `LogTerm` | Conflict lookup occurs, then `MaybeDecrTo` rejects state mutation | Same | Yes |
| Response after Progress reset | Old term is dropped; current-term response uses current numeric/state guards | Same | Yes |
| Stale response in snapshot state | Preserves state, `PendingSnapshot`, probe flag, commit tracking, and inflights | Same | Yes |
| Stale-success inflight handling | Does not call `FreeLE` and does not send more entries | Same | Yes |

## Tests added

### Native C coverage

`c/tests/raft_core_test.c` adds
`test_stale_append_response_semantics`.

It verifies:

- a stale successful response after `ReportUnreachable`;
- a stale probe rejection with `LogTerm == 0`;
- a stale probe rejection with `LogTerm > 0`;
- a current probe rejection with `LogTerm == 0`;
- the meaningful equal-index probe success;
- normal successful advancement;
- two active inflights;
- freeing only the inflight covered by a current acknowledgment;
- a duplicate success preserving the later inflight;
- an out-of-order newer acknowledgment followed by an older response;
- stale replicate rejections with and without `LogTerm`;
- a current rejection through the `LogTerm` optimization;
- snapshot-state preservation, including `PendingSnapshot`;
- an old-term response after a real higher-term Progress reset and
  re-election.

For ignored responses, the test compares:

- `id`;
- `match_index`;
- `next_index`;
- `sent_commit`;
- Progress state;
- `pending_snapshot`;
- `recent_active`;
- `message_flow_paused`;
- learner status;
- inflight start, count, bytes, limits, capacity, and buffer contents;
- outbound message queues.

### Backend-neutral Go/C parity coverage

`stale_msg_app_resp_parity_test.go` adds:

- `TestRawNodeStaleMsgAppRespParity`;
- `TestRawNodeDuplicateProbeMsgAppRespParity`;
- `TestRawNodeDuplicateAndOutOfOrderMsgAppRespInflightsParity`;
- `TestRawNodeOldTermMsgAppRespAfterProgressResetParity`.

The same source runs against the default Go backend and the `cgo_raft`
backend. It checks public `Status().Progress`, inflight counts, Ready/message
generation, duplicate/out-of-order behavior, both rejection hint formats,
probe recovery, and reset/term interaction.

No timing-sensitive test and no new public API were introduced.

## Validation

All validation below passed on 2026-07-31.

Focused backend-neutral tests:

```text
go test ./... \
  -run 'TestRawNode(StaleMsgAppResp|DuplicateProbeMsgAppResp|DuplicateAndOutOfOrderMsgAppRespInflights|OldTermMsgAppRespAfterProgressReset)Parity' \
  -count=1

CGO_ENABLED=1 go test -tags=cgo_raft ./... \
  -run 'TestRawNode(StaleMsgAppResp|DuplicateProbeMsgAppResp|DuplicateAndOutOfOrderMsgAppRespInflights|OldTermMsgAppRespAfterProgressReset)Parity' \
  -count=1

go test -count=100 \
  -run '^TestRawNode(StaleMsgAppResp|DuplicateProbeMsgAppResp|DuplicateAndOutOfOrderMsgAppRespInflights|OldTermMsgAppRespAfterProgressReset)Parity$' .

CGO_ENABLED=1 go test -count=100 -tags=cgo_raft \
  -run '^TestRawNode(StaleMsgAppResp|DuplicateProbeMsgAppResp|DuplicateAndOutOfOrderMsgAppRespInflights|OldTermMsgAppRespAfterProgressReset)Parity$' .
```

Native C and complete Go suites:

```text
make test-c
go test -count=1 ./...
CGO_ENABLED=1 go test -count=1 -tags=cgo_raft ./...
```

Race detector:

```text
go test -count=1 -race ./...
CGO_ENABLED=1 go test -count=1 -race -tags=cgo_raft ./...
```

Strict cgo pointer checking:

```text
GOEXPERIMENT=cgocheck2 CGO_ENABLED=1 \
  go test -count=1 -tags=cgo_raft ./...
```

Native ASan, LeakSanitizer, and UBSan:

```text
make test-c-sanitize
```

cgo ASan/LeakSanitizer:

```text
CGO_ENABLED=1 ASAN_OPTIONS=detect_leaks=1:halt_on_error=1 \
  go test -count=1 -asan -tags=cgo_raft ./...
```

cgo UBSan:

```text
CGO_ENABLED=1 \
  CGO_CFLAGS='-O1 -g -fno-omit-frame-pointer -fsanitize=undefined' \
  CGO_LDFLAGS='-fsanitize=undefined' \
  UBSAN_OPTIONS=halt_on_error=1:print_stacktrace=1 \
  go test -count=1 -tags=cgo_raft ./...
```

Valgrind full leak/error checking passed for all nine native binaries:

- `raw_node_skeleton_test`;
- `raft_core_test`;
- `snapshot_test`;
- `read_only_test`;
- `tracker_test`;
- `confchange_test`;
- `unstable_test`;
- `log_test`;
- `fatal_error_test`.

Both TLA-tagged variants:

```text
go test -count=1 -tags=with_tla ./...
CGO_ENABLED=1 go test -count=1 -tags='cgo_raft with_tla' ./...
```

Strict clean C11 builds and tests:

```text
make -C c clean test CC=clang
make -C c clean test CC=gcc
```

Formatting and whitespace:

```text
make verify-gofmt
git diff --check
```

## Final answers

1. **What makes a `MsgAppResp` stale in Go?**  
   It depends on the branch and current Progress state. A successful response
   is stale when it neither raises `Match` nor equals `Match` in probe state.
   A replicate rejection is stale at `Index <= Match`; a probe/snapshot
   rejection is stale unless `Index == Next-1`. Lower-term messages are
   discarded even earlier.

2. **Does `Index` alone determine staleness?**  
   No. `Reject`, Progress state, `Match`, `Next`, and message term all matter.

3. **Can a stale response still free inflights?**  
   No, when “stale” means the response fails Go's effective action predicate.
   A newer out-of-order acknowledgment can legitimately free several older
   inflights, but that response advances `Match` and is therefore actionable.

4. **Can a stale rejection still modify `Next`?**  
   No. `MaybeDecrTo` returns false before changing `Next`.

5. **How does `LogTerm` affect stale rejection handling?**  
   The leader still performs `findConflictByTerm` first. The translated hint
   is then passed to `MaybeDecrTo`, whose state-specific guard prevents a
   stale rejection from changing Progress.

6. **How does Progress reset interact with old responses?**  
   Old-term responses are filtered by common term handling. For a response in
   the current term, Go and C both use the reset Progress's current
   `Match`/`Next`/state; neither implementation adds a generation counter.

7. **What was the exact C mismatch?**  
   C performed commit and pending-send work after a successful response even
   when `updated == false`. The pending-send path could emit an append and
   alter current flow-control state.

8. **Does C now match Go in all tested stale-response cases?**  
   Yes. Successful, rejected, duplicate, out-of-order, inflight, `LogTerm`,
   snapshot, and reset/term cases all match.

## Remaining limitations

No stale-`MsgAppResp` semantic mismatch remains.

As in etcd v3.7.0, staleness detection is deliberately best-effort and based
on current term plus Progress indexes/state. There is no per-append sequence
number or generation counter. That is reference behavior, not a remaining C
limitation.

No other final-audit finding was changed in Phase 26.
