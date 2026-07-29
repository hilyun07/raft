# Transfer Leadership C API Cleanup Result

## Outcome

The C RawNode API now has one leadership-transfer helper:

```c
int raft_raw_node_transfer_leader(
    raft_raw_node_t *raw_node,
    uint64_t transferee);
```

It corresponds directly to Go:

```go
RawNode.TransferLeader(transferee)
```

There is no two-ID leadership-transfer function in the public C header,
implementation, test calls, or intended API inventory.

Go `Node.TransferLeadership(ctx, lead, transferee)` remains a distinct
actor/channel-layer routing operation:

```text
Go Node.TransferLeadership(ctx, lead, transferee)
  -> Go Node channel/routing layer
  -> MsgTransferLeader{From: transferee, To: lead}
  -> RawNode.stepForNode / raft_raw_node_step_for_node
```

The Go Node layer remains in Go for the C-backed integration, so its routing
arguments do not justify another C RawNode method.

## Files inspected

Public C surface and implementation:

- `c/include/raft/raft.h`;
- `c/src/raft_internal.h`;
- `c/src/raw_node.c`;
- `c/tests/raw_node_skeleton_test.c`;
- `c/Makefile` and the root `Makefile`.

Go behavior:

- `node.go`;
- `rawnode.go`;
- `rawnode_test.go`;
- the Node and RawNode interfaces and their transfer-message construction
  paths.

Porting documents and generated results:

- `prompts/RAFT_C_PORTING_SPEC.md`;
- `prompts/11_leadership_control.txt`;
- `prompts/13_end_to_end_test.txt`;
- `prompts/03a_sentinel_ids_doc.txt`;
- `OUTPUT/RAWNODE_API_PARITY_FOR_C_PORT.md`;
- `OUTPUT/PHASE2_C_RAWNODE_SKELETON_RESULT.md`;
- `OUTPUT/C_PORT_FOLLOWUP_DOCS_RESULT.md`;
- `OUTPUT/C_GO_POINTER_SAFE_RAWNODE_API_RESULT.md`;
- `OUTPUT/SENTINEL_NODE_IDS_FOR_C_PORT.md`;
- all other current C-port planning and result Markdown files under
  `OUTPUT/`.

## Stale references found

The audit found:

- a public-header declaration of the redundant two-ID C function;
- four C skeleton test calls covering its normal, null, and reserved-ID
  cases;
- Phase 2 result text claiming that both transfer forms belonged to the C
  API;
- a RawNode parity section that had not yet selected a canonical name and
  allowed an alias;
- follow-up and cgo-safety tables that treated Node and RawNode transfer as
  two direct C calls;
- the Phase 10 prompt instructing a future implementation to add the removed
  function.

`c/src/raw_node.c` already contained only the transferee-only implementation,
so no obsolete implementation body had to be deleted.

## Changes made

### C API and tests

- Removed the redundant declaration from `c/include/raft/raft.h`.
- Kept `raft_raw_node_transfer_leader(rn, transferee)` as the sole canonical
  C RawNode helper.
- Replaced the header comment with the explicit Node routing path through
  `raft_raw_node_step_for_node`.
- Removed all C test calls to the two-ID helper.
- Kept normal, null-handle, and reserved-transferee tests for
  `raft_raw_node_transfer_leader`.
- Added a skeleton test for Node-style routing using:

  ```c
  raft_message_view_t message = {
      .type = RAFT_MSG_TRANSFER_LEADER,
      .from = transferee,
      .to = lead,
  };
  raft_raw_node_step_for_node(raw_node, &message);
  ```

- Advanced the private skeleton ABI marker from 7 to 8 because the public
  declaration was removed.

The subsequent Phase 4 Node-boundary helper additions advance the current
private skeleton marker to 9.

### Documentation and future phase instructions

- Updated the RawNode parity table to map only:

  ```text
  Go RawNode.TransferLeader(transferee)
    -> C raft_raw_node_transfer_leader(rn, transferee)
  ```

- Documented Go `Node.TransferLeadership` as a Go channel/routing operation
  whose `MsgTransferLeader` enters the C-backed core through the lower-level
  Node actor Step helper, bypassing public RawNode Step validation.
- Removed the “two transfer forms” explanation.
- Updated the Phase 2 result, follow-up result, cgo-safety audit, and current
  ABI-marker notes.
- Corrected the Phase 10 prompt so later work does not reintroduce the
  redundant function.
- Clarified the sentinel-planning prompt to discuss transferee validation on
  the canonical RawNode helper.

No consensus logic or production Go behavior changed.

## Remaining references

The obsolete C symbol text remains only in negative design statements in:

- `OUTPUT/RAWNODE_API_PARITY_FOR_C_PORT.md`;
- `OUTPUT/PHASE2_C_RAWNODE_SKELETON_RESULT.md`;
- `prompts/11_leadership_control.txt`.

Those references explicitly say that the function is not part of the public C
API and must not be added. They are retained to prevent regression.

References to Go `Node.TransferLeadership` remain in `node.go`, Go tests,
end-to-end planning, and compatibility documentation because that public Go
API is intentionally preserved. Generic references to leadership-transfer
behavior and `MsgTransferLeader` also remain because the consensus feature and
wire message still exist.

## Validation

Passed from a clean C build:

```text
make clean-c
make test-c
```

The C library and public-header-only test compile with:

```text
-std=c11 -Wall -Wextra -Werror -Wpedantic
```

and `c/.build/raw_node_skeleton_test` passes.

Passed:

```text
go test ./...
```

All Go packages passed. The default pure-Go build remains independent of the C
skeleton.

Symbol/source scans confirm:

- `raft_raw_node_transfer_leader` is the only exported C
  leadership-transfer RawNode function;
- there is no two-ID declaration, implementation, or test call;
- remaining obsolete-name mentions are the intentional negative statements
  listed above.
