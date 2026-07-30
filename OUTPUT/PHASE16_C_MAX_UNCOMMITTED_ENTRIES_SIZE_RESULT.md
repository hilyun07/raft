# Phase 16 C MaxUncommittedEntriesSize Compatibility Result

## Outcome

The C backend now applies the same `MaxUncommittedEntriesSize` proposal
admission rule as the original Go implementation.

When the uncommitted tail is empty, the first proposal is admitted even if
its payload exceeds the configured limit. Once a positive-size uncommitted
payload exists, later positive-size proposals are rejected when their
addition would exceed the limit. Empty entries remain admissible.

This phase fixes only the `MaxUncommittedEntriesSize` mismatch identified by
`OUTPUT/C_PORT_SEMANTIC_COMPLETENESS_AUDIT.md`.

## Original Go behavior

`raft.appendEntry` calls `increaseUncommittedSize` before appending proposed
entries. The helper rejects a proposal only when:

```text
existing uncommitted size > 0
and new payload size > 0
and existing size + new size > configured limit
```

The existing-size guard intentionally treats the setting as a bound on the
accumulated uncommitted tail rather than a maximum individual-entry size.
This allows progress when an individual proposal is larger than the
configured backlog limit.

The new-size guard ensures zero-payload entries always succeed. Such entries
are required for a leader's initial no-op and for automatically leaving joint
configuration states.

After an accepted append, Go adds the proposal payload to
`uncommittedSize`. Applied committed entries subtract their payload with
saturation at zero. A Raft reset also clears the counter.

## Previous C behavior

The C core performed proposal admission in `core_append_entries`, at the
correct point before `raft_log_append`, but used this stricter rule:

```text
new payload size > configured limit
or existing size + new size > configured limit
```

The first condition rejected an oversized proposal even when
`uncommitted_size` was zero. This differed from etcd's behavior.

The remaining C accounting was already compatible:

- successful appends add the payload to `uncommitted_size`;
- Raft reset clears `uncommitted_size`;
- storage-apply responses subtract committed payload;
- subtraction saturates at zero.

## Implementation

Only the admission predicate in `c/src/raft_core.c` changed:

```c
if (raft->uncommitted_size > 0 && payload_size > 0 &&
    (payload_size > raft->max_uncommitted_entries_size ||
     raft->uncommitted_size >
         raft->max_uncommitted_entries_size - payload_size)) {
    return RAFT_ERR_PROPOSAL_DROPPED;
}
```

The two positive-size guards reproduce the Go policy. The disjunction inside
the predicate is an overflow-safe C expression of:

```text
uncommitted_size + payload_size > max_uncommitted_entries_size
```

No Ready generation, storage interaction, proposal forwarding, log append,
flow control, replication, configuration-change handling, or public API
changed.

## Tests

### Native C

`test_uncommitted_proposal_limit` now verifies:

- a five-byte proposal is accepted with an empty tail and a four-byte limit;
- the counter records the full five-byte payload;
- later oversized and ordinary positive-size proposals are rejected while
  that tail remains uncommitted;
- an empty proposal is accepted without changing the counter;
- application of committed entries reduces the counter to zero;
- two ordinary two-byte proposals are accepted up to the exact limit;
- a further one-byte proposal is rejected;
- accounting returns to zero after those entries are applied;
- another oversized proposal is accepted after the tail drains.

### Shared Go/C parity

`max_uncommitted_entries_size_parity_test.go` runs unchanged against the
default Go RawNode and the C-backed RawNode. Through the public API it
verifies:

- first-oversized-proposal admission;
- later proposal rejection while the oversized tail is outstanding;
- empty proposal admission;
- normal admission up to the configured limit;
- rejection beyond the occupied limit;
- renewed oversized admission after Ready/Advance persists, commits, and
  applies the previous entries.

Existing native replication tests and the complete default and C-backed Go
suites continue to cover ordinary proposal and replication behavior.

## Files changed

Implementation:

- `c/src/raft_core.c`.

Tests:

- `c/tests/raft_core_test.c`;
- `max_uncommitted_entries_size_parity_test.go` (new).

Documentation:

- `OUTPUT/PHASE16_C_MAX_UNCOMMITTED_ENTRIES_SIZE_RESULT.md`.

## Validation

All commands below passed on 2026-07-31.

Focused native and parity tests:

```text
make test-c
go test -count=1 \
  -run '^TestMaxUncommittedEntriesSizeParity$' .
CGO_ENABLED=1 go test -count=1 -tags=cgo_raft \
  -run '^TestMaxUncommittedEntriesSizeParity$' .
```

Existing Go proposal-limit and bounded-growth tests:

```text
go test -count=1 \
  -run 'Test(UncommittedEntryLimit|RawNodeBoundedLogGrowthWithPartition|MaxUncommittedEntriesSizeParity)' .
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

- The comprehensive upstream Raft interaction tests remain excluded under
  `cgo_raft` by their existing build constraints. The new backend-neutral
  parity test directly covers this compatibility rule.
- There are no remaining TODOs for this audit finding.
- No other semantic-audit finding was addressed in this phase.
