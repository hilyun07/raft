# Phase 15 C Unknown-Peer Response Filter Result

## Outcome

The C-backed Node actor path now filters response messages from unknown
non-local peers before they reach the Raft core, matching the original Go
RawNode/Node layering.

The direct RawNode path retains its existing behavior. Valid responses from
tracked peers continue to reach the core, and responses from
`LocalAppendThread` or `LocalApplyThread` remain exempt for asynchronous
storage processing.

This phase fixes only the unknown-peer Node-path mismatch identified by
`OUTPUT/C_PORT_SEMANTIC_COMPLETENESS_AUDIT.md`.

## Original Go behavior

The Go implementation layers the two entry points as follows:

```text
RawNode.Step
  -> reject unexpected local messages
  -> RawNode.stepForNode
       -> reject responses from unknown non-local peers
       -> raft.Step

node.run
  -> RawNode.stepForNode
       -> reject responses from unknown non-local peers
       -> raft.Step
```

The shared `stepForNode` predicate is:

```go
IsResponseMsg(m.GetType()) &&
    !IsLocalMsgTarget(m.GetFrom()) &&
    rn.raft.trk.Progress[m.GetFrom()] == nil
```

It returns `ErrStepPeerNotFound` before the message reaches the core.

`Node.Step` is asynchronous for non-proposal messages. It reports successful
delivery to the actor, while `node.run` intentionally discards the internal
step error because that path has no result channel. The semantic requirement
is therefore that an unknown response cannot change Raft state, not that
`Node.Step` return `ErrStepPeerNotFound` to its caller.

## Previous C behavior

The public C function performed both checks:

```text
raft_raw_node_step
  -> public local-message rejection
  -> unknown-response rejection
  -> raft_raw_node_step_for_node
       -> raft_core_step
```

This made direct Go/C RawNode Step correct.

The Go Node actor, however, calls the binding's package-private
`stepForNode`, which invokes `raft_raw_node_step_for_node` directly. That C
function performed descriptor validation and then called `raft_core_step`
without checking tracker membership.

An unknown higher-term response could consequently cause a follower reset or
leader step-down. Other unknown response types, such as
`MsgReadIndexResp`, could also reach role-specific core handling.

This was an omission rather than intentional behavior. The Phase 1 boundary
result and the pure-Go implementation both require `stepForNode` to retain
unknown-response filtering while bypassing only public local-message
rejection.

## Implementation

The existing response predicate was moved from `raft_raw_node_step` to
`raft_raw_node_step_for_node`:

```c
if (message_type_is_response(message->type) &&
    !raft_is_local_target_id(message->from) &&
    !raft_core_has_progress(&raw_node->raft, message->from)) {
    return RAFT_ERR_STEP_PEER_NOT_FOUND_OR_IGNORED;
}
```

The resulting C layering is:

```text
raft_raw_node_step
  -> validate descriptor
  -> reject unexpected local messages
  -> raft_raw_node_step_for_node
       -> validate descriptor
       -> reject responses from unknown non-local peers
       -> raft_core_step

Go node.run / RawNode.stepForNode
  -> raft_raw_node_step_for_node
       -> validate descriptor
       -> reject responses from unknown non-local peers
       -> raft_core_step
```

The public path still applies the same checks in the same effective order.
The shared lower boundary now performs the tracker-dependent rule once,
without duplicating it.

No message classification, tracker lookup, error code, public API, cgo
conversion, storage-worker routing, or core message handling changed.

The public C header comment was corrected to state that the Node helper
bypasses public local-message rejection but retains the response-peer filter.

## Tests

### Native C

The existing Step-layering test now verifies:

- public `raft_raw_node_step` rejects an unknown `MsgAppResp`;
- `raft_raw_node_step_for_node` rejects the same response with the same error;
- a known peer's `MsgHeartbeatResp` succeeds through both functions;
- existing Node-local `MsgHup` routing remains accepted only through the
  Node helper.

The rest of the native suite continues to exercise local append/apply
responses, election responses, replication responses, snapshots, reads, and
leadership messages.

### Shared Go/C parity

`unknown_peer_response_parity_test.go` runs unchanged against both the
default Go RawNode and the C-backed RawNode. It verifies:

- direct `RawNode.Step` returns `ErrStepPeerNotFound`;
- package-private `RawNode.stepForNode` returns the same error;
- an unknown higher-term response leaves BasicStatus unchanged;
- local apply-thread responses remain accepted through both paths;
- a tracked peer's response is processed through both paths and advances the
  term;
- public `Node.Step` accepts asynchronous delivery of an unknown response but
  the Node state remains unchanged;
- a tracked peer's response through `Node.Step` reaches the core and advances
  the term.

## Files changed

Implementation and API documentation:

- `c/src/raw_node.c`;
- `c/include/raft/raft.h`.

Tests:

- `c/tests/raft_core_test.c`;
- `unknown_peer_response_parity_test.go` (new).

Documentation:

- `OUTPUT/PHASE15_C_UNKNOWN_PEER_RESPONSE_FILTER_RESULT.md`.

## Validation

All commands below passed on 2026-07-31.

Focused native and backend-neutral tests:

```text
make test-c
go test -count=1 \
  -run 'Test(RawNode|Node)UnknownPeerResponseFilteringParity' .
CGO_ENABLED=1 go test -count=1 -tags=cgo_raft \
  -run 'Test(RawNode|Node)UnknownPeerResponseFilteringParity' .
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

Strict Clang C11 build:

```text
make -C c clean test CC=clang
```

Valgrind for all eight native C test binaries:

```text
valgrind --quiet --error-exitcode=99 --leak-check=full \
  --errors-for-leak-kinds=all <test-binary>
```

Both TLA build variants:

```text
go test -count=1 -tags=with_tla ./...
CGO_ENABLED=1 go test -count=1 -tags='cgo_raft with_tla' ./...
```

No test failure, compiler warning, sanitizer finding, Valgrind error, race, or
cgo pointer violation was reported.

## Remaining concerns

- The public `Node.Step` API remains asynchronous for non-proposal messages
  and therefore does not return `ErrStepPeerNotFound`; this is existing etcd
  behavior. Filtering is verified through the absence of state changes.
- The comprehensive upstream interaction suite remains excluded under
  `cgo_raft`; the new shared scenarios provide focused differential coverage.
- No other semantic-audit finding was addressed in this phase.
