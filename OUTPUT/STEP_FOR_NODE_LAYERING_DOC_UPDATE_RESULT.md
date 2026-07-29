# Step-for-Node Layering Documentation Update Result

## Outcome

The C Step boundary now has one explicit dependency direction:

```text
Go RawNode.Step / external C caller
  -> raft_raw_node_step
       -> public RawNode.Step validation
       -> raft_raw_node_step_for_node
            -> direct C raft-core Step

Go node.run
  -> RawNode.stepForNode
  -> raft_raw_node_step_for_node
       -> direct C raft-core Step
```

`raft_raw_node_step_for_node` never delegates to
`raft_raw_node_step`. It is the lower-level Go Node boundary corresponding to
the package-internal `r.Step` calls historically made by `node.run`.

The implementation did not fully satisfy this design before this task: the
two C functions were independent `RAFT_ERR_NOT_IMPLEMENTED` stubs. They are
now layered in the intended direction.

## Files inspected

The audit searched every Markdown file under `OUTPUT/` for Step, Node,
RawNode, and internal `r.Step` descriptions. It also inspected:

- `rawnode.go`, `rawnode_cgo.go`, `node.go`, and `util.go`;
- `rawnode_cgo_test.go`;
- `c/include/raft/raft.h`;
- `c/src/raw_node.c` and `c/src/raft_internal.h`;
- `c/tests/raw_node_skeleton_test.c`;
- the root and C Makefiles.

The most directly relevant result documents were:

- `PHASE1_RAWNODE_BOUNDARY_PLAN.md`;
- `PHASE1_RAWNODE_BOUNDARY_RESULT.md`;
- `PHASE2_C_RAWNODE_SKELETON_RESULT.md`;
- `PHASE4_GO_RAWNODE_BINDING_SKELETON_RESULT.md`;
- `RAWNODE_API_PARITY_FOR_C_PORT.md`;
- `TRANSFER_LEADERSHIP_API_CLEANUP_RESULT.md`;
- `C_GO_POINTER_SAFE_RAWNODE_API_RESULT.md`;
- `C_TYPE_SEPARATED_PAYLOAD_API_RESULT.md`;
- `ERROR_MAPPING_FOR_C_PORT.md`;
- `SENTINEL_NODE_IDS_FOR_C_PORT.md`;
- `C_PORT_FOLLOWUP_DOCS_RESULT.md`.

## Implementation and test changes

`c/src/raw_node.c` now:

- validates the public descriptor shape in both entry points;
- rejects Go-local message types at the public Step boundary unless
  `Message.From` is a local append/apply target;
- delegates from `raft_raw_node_step` to
  `raft_raw_node_step_for_node`;
- leaves `raft_raw_node_step_for_node` as the direct inert core stub;
- documents that the lower-level helper must not call the public function.

The local-message list mirrors Go `IsLocalMsg`: Hup, Beat, Unreachable,
SnapshotStatus, CheckQuorum, storage append/apply requests, and their
responses.

`c/include/raft/raft.h` now identifies the intended caller and validation
layer for each function. Its leadership-transfer comment also routes the Go
Node actor's `MsgTransferLeader` through
`raft_raw_node_step_for_node`.

`c/tests/raw_node_skeleton_test.c` now proves the distinction:

- public Step returns `RAFT_ERR_STEP_LOCAL_MSG` for an ordinary/unset-sender
  `MsgHup`;
- Step-for-Node accepts that boundary shape and reaches the inert core,
  returning `RAFT_ERR_NOT_IMPLEMENTED`;
- Node-style leadership-transfer routing uses Step-for-Node.

`rawnode_cgo_test.go` adds the equivalent tagged binding check: public
`RawNode.Step(MsgHup)` returns `ErrStepLocalMsg`, while
`stepForNode(MsgHup)` reaches the C core stub.

No Raft consensus logic was added.

## Markdown/result files updated

- `PHASE1_RAWNODE_BOUNDARY_RESULT.md`;
- `PHASE2_C_RAWNODE_SKELETON_RESULT.md`;
- `PHASE4_GO_RAWNODE_BINDING_SKELETON_RESULT.md`;
- `RAWNODE_API_PARITY_FOR_C_PORT.md`;
- `TRANSFER_LEADERSHIP_API_CLEANUP_RESULT.md`;
- `C_GO_POINTER_SAFE_RAWNODE_API_RESULT.md`;
- `C_PORT_FOLLOWUP_DOCS_RESULT.md`;
- `C_TYPE_SEPARATED_PAYLOAD_API_RESULT.md`;
- `ERROR_MAPPING_FOR_C_PORT.md`;
- `SENTINEL_NODE_IDS_FOR_C_PORT.md`.

The updated documents consistently state that external C callers use
`raft_raw_node_step`, the Go C-backed `node.run` path uses
`raft_raw_node_step_for_node`, and delegation is public-to-internal only.

## Remaining gap

Public Go `RawNode.Step` also rejects response messages from unknown
non-local peers. The C skeleton has no progress tracker yet, so it cannot
perform that membership-dependent check correctly. It is intentionally
deferred to the C progress-tracker/core phase and is documented as a required
check in `raft_raw_node_step` before that backend becomes functional.

The skeleton does not approximate this by rejecting every response message,
because that would reject valid known-peer responses. After the available
public validation, the lower-level function still returns
`RAFT_ERR_NOT_IMPLEMENTED`.

## Commands run

```text
make test-c
```

Passed with the C11 warning-as-error build and C skeleton tests.

```text
make test-cgo-raft
```

Passed for all tagged Go packages.

```text
mkdir -p .tmp .gocache
TMPDIR=$PWD/.tmp GOTMPDIR=$PWD/.tmp GOCACHE=$PWD/.gocache go test ./...
```

Passed for all default pure-Go packages. The default build remains pure Go.
