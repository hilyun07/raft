# Phase 17 C MsgAppResp LogTerm Conflict Optimization Result

## Outcome

The C leader now uses both `RejectHint` and `LogTerm` when handling a rejected
`MsgAppResp`, matching the original Go implementation.

When a rejection includes a positive `LogTerm`, the leader translates the
follower's hint through its own log before lowering the follower's
`next_index`. When `LogTerm` is zero, the existing raw-`RejectHint` fallback
is preserved.

This phase fixes only the rejected-`MsgAppResp` optimization identified by
`OUTPUT/C_PORT_SEMANTIC_COMPLETENESS_AUDIT.md`.

## Original Go behavior

Append rejection recovery has cooperating follower-side and leader-side
steps.

On the follower:

1. A `MsgApp` fails to match its `(Index, LogTerm)` anchor.
2. The follower starts at `min(message.Index, follower.lastIndex)`.
3. `findConflictByTerm` walks backward to the largest index whose local term
   is less than or equal to the leader's anchor term, or whose term is
   unavailable.
4. The follower responds with:
   - `Index` equal to the rejected append anchor;
   - `RejectHint` equal to the adjusted follower index;
   - `LogTerm` equal to the follower term at that hint, or zero if unknown.

On the leader:

1. `nextProbeIdx` starts as the received `RejectHint`.
2. When `LogTerm > 0`, the leader calls
   `findConflictByTerm(RejectHint, LogTerm)` against its own log.
3. The translated index is passed to `Progress.MaybeDecrTo`.
4. If progress changes, replicate state becomes probe state when necessary
   and the leader immediately sends the next append.

The second lookup skips leader entries whose terms are already known to be
too new to match the follower. A large divergent suffix therefore converges
approximately once per term instead of potentially once per index and
network round trip. This is a performance optimization; the unoptimized
backtracking remains safe.

## Previous C behavior

The C follower already produced optimized `(RejectHint, LogTerm)` pairs in
`core_handle_append`, using `raft_log_find_conflict_by_term`.

The C leader's `core_handle_append_response`, however, passed
`message->reject_hint` directly to `raft_progress_maybe_decr_to` and ignored
`message->log_term`. This discarded the information supplied by the
follower and could cause nearly linear probing across divergent log tails.

## Implementation

The rejected-response branch in `c/src/raft_core.c` now initializes
`next_probe_index` from `RejectHint` and, only when `LogTerm > 0`, calls the
existing log helper:

```c
uint64_t next_probe_index = message->reject_hint;
if (message->log_term > 0) {
    uint64_t next_probe_term;
    result = raft_log_find_conflict_by_term(
        raft->log,
        message->reject_hint,
        message->log_term,
        &next_probe_index,
        &next_probe_term);
    if (result != RAFT_OK) {
        return result;
    }
}
```

`next_probe_index` is then passed to the unchanged
`raft_progress_maybe_decr_to`.

No new production helper was introduced. The existing
`raft_log_find_conflict_by_term` implementation is shared by follower and
leader rejection handling, like the original Go helper.

No unrelated replication, flow-control, inflight, Ready, persistence,
storage, or snapshot logic changed.

## Tests

### Native C

`test_append_rejection_log_term_optimization` constructs a leader with a
100-entry term run and checks two cases:

- with `RejectHint=99` and `LogTerm=1`, the leader skips the incompatible
  run, sets `next_index` to 1, and sends an append anchored at index 0;
- with the same hint and `LogTerm=0`, the leader preserves the legacy
  fallback, sets `next_index` to 100, and sends an append anchored at index
  99.

The existing log tests continue to exercise
`raft_log_find_conflict_by_term` directly.

### Shared Go/C parity

`append_rejection_log_term_parity_test.go` runs unchanged against the
default Go RawNode and the C-backed RawNode.

It creates:

- a leader whose log has index 1 at term 1 and indexes 2 through 100 at term
  2;
- a follower whose log has indexes 1 through 99 at term 1.

The test verifies the complete exchange:

1. the leader first probes `(index=100, term=2)`;
2. the follower rejects with `(RejectHint=99, LogTerm=1)`;
3. the leader changes peer `Next` directly from 101 to 2;
4. the next append is anchored at the common prefix `(index=1, term=1)`;
5. the follower accepts through index 101;
6. leader progress becomes replicate with `Match=101` and `Next=102`.

A separate zero-`LogTerm` scenario verifies that the leader uses the raw
hint and sends the next append from `(index=99, term=2)`.

## Files changed

Implementation:

- `c/src/raft_core.c`.

Tests:

- `c/tests/raft_core_test.c`;
- `append_rejection_log_term_parity_test.go` (new).

Documentation:

- `OUTPUT/PHASE17_C_MSG_APP_RESP_LOG_TERM_RESULT.md`.

## Validation

All commands below passed on 2026-07-31.

Focused native and shared parity tests:

```text
make test-c
go test -count=1 \
  -run '^TestRawNodeAppendRejectionLogTermParity$' .
CGO_ENABLED=1 go test -count=1 -tags=cgo_raft \
  -run '^TestRawNodeAppendRejectionLogTermParity$' .
```

Existing Go rejection and replication tests:

```text
go test -count=1 \
  -run 'Test(FastLogRejection|LeaderAppResp|MsgAppRespWaitReset|RawNodeAppendRejectionLogTermParity)' .
```

Complete default and C-backed Go suites:

```text
go test -count=1 ./...
CGO_ENABLED=1 go test -count=1 -tags=cgo_raft ./...
```

Strict cgo pointer checking and race detection:

```text
CGO_ENABLED=1 GOEXPERIMENT=cgocheck2 \
  go test -count=1 -tags=cgo_raft ./...
CGO_ENABLED=1 go test -count=1 -race -tags=cgo_raft ./...
```

AddressSanitizer, LeakSanitizer, and UndefinedBehaviorSanitizer:

```text
make test-c-sanitize
```

Strict Clang C11 build and native tests:

```text
make -C c clean test CC=clang
```

Valgrind with full leak checking ran successfully for all eight native C test
binaries.

Both TLA build variants also passed:

```text
go test -count=1 -tags=with_tla ./...
CGO_ENABLED=1 go test -count=1 -tags='cgo_raft with_tla' ./...
```

No test failure, compiler warning, sanitizer finding, Valgrind error, race,
or cgo pointer violation was reported.

## Remaining concerns

- The comprehensive upstream Raft interaction suite remains excluded under
  `cgo_raft` by its existing build constraints. The new backend-neutral
  two-node parity scenario directly covers the optimized exchange.
- The existing C helper's broader storage-error classification was not
  changed; that is a separate audit finding outside this task.
- No other semantic-audit finding was addressed in this phase.
