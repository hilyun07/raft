# Phase 14 C Election Timeout Randomization Result

## Outcome

The opt-in `cgo_raft` backend now selects a randomized election timeout every
time the Raft election timer is reset, matching the original Go
implementation's lifecycle and range:

```text
[ElectionTick, 2*ElectionTick - 1]
```

The selected timeout remains unchanged while ticks are processed and while
same-term leader traffic merely resets `election_elapsed`. A new value is
selected only through the common state reset used by initialization and
follower, candidate, and leader transitions.

This phase fixes only the randomized-election-timeout semantic gap identified
by `OUTPUT/C_PORT_SEMANTIC_COMPLETENESS_AUDIT.md`.

## Original Go behavior

The current Go implementation centralizes timer randomization in
`(*raft).reset`:

```go
r.electionElapsed = 0
r.resetRandomizedElectionTimeout()
```

`resetRandomizedElectionTimeout` selects:

```go
r.electionTimeout + globalRand.Intn(r.electionTimeout)
```

The reset path is used during initial follower construction and transitions
to follower, candidate, and leader. Becoming a pre-candidate deliberately
does not reset the timer. Follower receipt of append, heartbeat, or snapshot
messages and the grant of a real vote reset only `electionElapsed`; they do
not select a new timeout.

Follower and candidate ticks compare elapsed time with the stored randomized
timeout. Leader ticks continue to use the configured base election timeout
for CheckQuorum and leadership-transfer expiry.

## Previous C behavior

The C core already had the same centralized lifecycle shape, but
`core_reset` assigned:

```c
raft->randomized_election_timeout = raft->election_timeout;
```

`raft_core_init` also preloaded the same fixed value. Consequently every
eligible follower or candidate timed out at exactly `ElectionTick`.

## Implementation

### Private random stream

New private files `c/src/random.h` and `c/src/random.c` provide:

- one independently seeded non-cryptographic stream per `raft_t`;
- a SplitMix64-style state transition;
- unbiased bounded selection through rejection sampling;
- a private explicit seed operation for deterministic native tests.

Automatic initialization mixes the node ID, a process-wide atomic sequence,
the stream address, and C11 time. Raft does not require cryptographic
unpredictability; the stream exists to distribute election deadlines and
avoid synchronized repeated elections.

The module is compiled into the standalone C library and included in the
tagged cgo amalgamation.

### Reset-time selection

`raft_t` now owns `raft_random_t random`. `raft_core_init` initializes the
stream before the initial `core_reset`.

`core_reset` now assigns:

```c
raft->randomized_election_timeout =
    (uint64_t)raft->election_timeout +
    raft_random_uniform(&raft->random, raft->election_timeout);
```

No tick path draws from the stream. The selected value is stable until the
next `core_reset`.

The behavior therefore applies consistently to:

- RawNode initialization;
- Bootstrap's follower reset;
- higher-term follower transitions;
- normal candidate transitions;
- leader transitions;
- candidate loss and other follower resets.

Pre-candidate transitions remain non-resetting, matching Go.

### Internal timer width

`randomized_election_timeout` and `election_elapsed` are now `uint64_t`.
This preserves the full upper range when the accepted `uint32_t`
`ElectionTick` is near its maximum. `TickQuiesced` saturation was updated to
the same width.

The private RawNode ABI marker advances from 15 to 16. No public Go API or
public C ABI type changed.

## Tests

The native C core test now uses private state only for focused timer
assertions. It verifies:

- initialization selects a value within the documented half-open range;
- ordinary ticks do not change the selected timeout;
- a same-term heartbeat resets elapsed time without changing the timeout;
- higher-term follower resets select values in range;
- a fixed test seed produces a deterministic reset sequence containing
  different timeout values;
- campaigning enters candidate state through the reset path and leaves an
  in-range timeout;
- the existing single-node tick election occurs at its selected deadline and
  still completes leadership through Ready/Advance.

`election_timeout_parity_test.go` runs unchanged against both the default Go
RawNode and the C-backed RawNode. It verifies that:

- no election starts before `ElectionTick`;
- an election starts by `2*ElectionTick - 1`;
- the single-node election completes after persisting and advancing Ready.

Existing native and Go suites continue to cover follower, candidate, leader,
PreVote, CheckQuorum, transfer, and Ready transitions.

## Files changed

Implementation and build:

- `c/src/random.h` (new);
- `c/src/random.c` (new);
- `c/src/raft_core.h`;
- `c/src/raft_core.c`;
- `c/src/raft_internal.h`;
- `c/Makefile`;
- `rawnode_cgo_bridge.c`.

Tests:

- `c/tests/raft_core_test.c`;
- `election_timeout_parity_test.go` (new).

Documentation:

- `OUTPUT/PHASE14_C_ELECTION_TIMEOUT_RANDOMIZATION_RESULT.md`.

## Validation

All commands below passed on 2026-07-31.

Strict native C11 build and all eight C test binaries:

```text
make -C c clean test
```

Focused default and C-backed parity test:

```text
go test -count=1 -run '^TestRawNodeElectionTimeoutRangeParity$' .
CGO_ENABLED=1 go test -count=1 -tags=cgo_raft \
  -run '^TestRawNodeElectionTimeoutRangeParity$' .
```

Complete default and C-backed Go suites:

```text
go test -count=1 ./...
CGO_ENABLED=1 go test -count=1 -tags=cgo_raft ./...
```

AddressSanitizer, LeakSanitizer, and UndefinedBehaviorSanitizer:

```text
make test-c-sanitize
```

Strict Clang C11 build:

```text
make -C c clean test CC=clang
```

Valgrind for every native C test binary:

```text
valgrind --quiet --error-exitcode=99 --leak-check=full \
  --errors-for-leak-kinds=all <test-binary>
```

Strict cgo pointer checking and the race detector:

```text
CGO_ENABLED=1 GOEXPERIMENT=cgocheck2 \
  go test -count=1 -tags=cgo_raft ./...
CGO_ENABLED=1 go test -count=1 -race -tags=cgo_raft ./...
```

Both TLA build variants:

```text
go test -count=1 -tags=with_tla ./...
CGO_ENABLED=1 go test -count=1 -tags='cgo_raft with_tla' ./...
```

No compiler warning, test failure, sanitizer finding, Valgrind error, race,
or cgo pointer violation was reported.

## Remaining concerns

- The private PRNG is intentionally non-cryptographic. Election timeout
  selection requires distribution, not secrecy.
- Tests use an explicit private seed and inspect private timer state only in
  the native core suite. No production test hook or public seed field was
  added.
- The larger randomized multi-node interaction harness remains separate
  future work. This phase does not address any other finding from the
  semantic completeness audit.
