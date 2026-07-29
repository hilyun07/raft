# Phase 1: RawNode Boundary Result

Phase 1 is complete as a Go-only boundary refactor. No C, cgo, build-tag,
storage, transport, or protobuf changes were introduced.

## Files changed

| File | Change |
| --- | --- |
| `node.go` | Removed direct Raft-core access from the Node actor and cached immutable RawNode configuration needed outside the actor goroutine. |
| `rawnode.go` | Added the minimal boundary getters and a package-private actor step method. |
| `node_test.go` | Removed one test-helper core access and added unknown-peer and self-removal regression tests. |
| `rawnode_test.go` | Added coverage for the new getters and their read-only behavior. |
| `OUTPUT/PHASE1_RAWNODE_BOUNDARY_RESULT.md` | Recorded the Phase 1 implementation and verification result. |

Existing public `Node` and `RawNode` methods were not removed or changed.

## Direct accesses removed from `node.go`

`node.run` no longer retains `r := n.rn.raft`, and `node.go` no longer
dereferences `rn.raft`.

The former accesses were replaced as follows:

| Former access | Replacement |
| --- | --- |
| `r.lead`, `r.hasLeader()`, `r.Term` | `n.rn.BasicStatus()` |
| `r.id` | Immutable `node.id`, initialized from `RawNode.ID()` |
| `r.logger` | Immutable `node.logger`, initialized from `RawNode.Logger()` |
| `r.Step(m)` | `n.rn.stepForNode(m)` |
| `r.trk.Progress[id]` | `n.rn.HasProgress(id)` |
| `r.applyConfChange(cc)` | `n.rn.ApplyConfChange(cc)` |
| `n.rn.asyncStorageWrites` | Immutable `node.asyncStorageWrites`, initialized from `RawNode.AsyncStorageWritesEnabled()` |
| `getStatus(r)` | `n.rn.Status()` |
| `n.rn.raft.logger` and `n.rn.raft.id` in `Tick` | Cached `node.logger` and `node.id` |

The unknown-peer response check now lives behind the RawNode boundary. The
Node receive path continues to ignore its error because that asynchronous path
has no result channel, matching its previous behavior.

## New RawNode boundary methods

The following exported read-only getters were added:

```go
func (rn *RawNode) ID() uint64
func (rn *RawNode) HasProgress(id uint64) bool
func (rn *RawNode) AsyncStorageWritesEnabled() bool
func (rn *RawNode) Logger() Logger
```

One package-private method was added for the Go Node actor:

```go
func (rn *RawNode) stepForNode(m *pb.Message) error
```

`stepForNode` is necessary because public `RawNode.Step` intentionally rejects
local messages received from an application/network caller, while the Node
actor legitimately carries locally generated `MsgHup`, `MsgUnreachable`,
`MsgSnapStatus`, `MsgTransferLeader`, `MsgForgetLeader`, and storage-response
messages on its receive channel. Public `RawNode.Step` retains its existing
local-message validation and delegates to `stepForNode` afterward. Both paths
retain the existing unknown-peer response filtering.

No consensus logic was moved into `node.go`.

For the later C-backed boundary, this distinction maps to two one-way-layered
entry points:

```text
public RawNode.Step
  -> raft_raw_node_step
       -> public validation
       -> raft_raw_node_step_for_node
            -> direct core Step

node.run / RawNode.stepForNode
  -> raft_raw_node_step_for_node
       -> direct core Step
```

The C Node helper corresponds to the old package-internal `r.Step` calls and
must not call back through the public C Step function. Otherwise locally
generated messages that are valid in `node.run` could be rejected by the
public RawNode boundary. The historical pure-Go Phase 1 implementation keeps
unknown-peer filtering behind `stepForNode`; the C design must preserve the
observable filtering while keeping public-only validation out of the Node
actor helper.

## Ready/Advance semantics

The Ready lifecycle is unchanged:

- `node.run` still calls `HasReady` only when no `Advance` is outstanding.
- It still previews with `readyWithoutAccept`.
- It still calls `acceptReady` only after the Ready channel send succeeds.
- Synchronous mode still arms the `Advance` channel.
- Async storage mode still clears the local Ready without arming `Advance`.
- `RawNode.Advance`, `stepsOnAdvance`, message draining, unstable acceptance,
  `MustSync`, and committed-entry accounting were not modified.

## Remaining internal coupling

The following coupling remains intentionally:

- `RawNode` is still the existing Go implementation and contains `raft *raft`.
  Replacing that implementation is a later C-porting phase.
- `node.go` uses the package-private RawNode methods `readyWithoutAccept`,
  `acceptReady`, and `stepForNode`. These are semantic boundary operations
  that a later binding will need to map to private or public C APIs.
- `node` still stores a concrete `*RawNode`; it no longer depends on the
  layout of RawNode or the Go Raft core behind it.
- Tests in the `raft` package still inspect or manipulate `rn.raft` where
  white-box setup/assertions require it. Production `node.go` does not.
- `Logger` remains Go-side metadata, which is suitable for the future Go
  binding without requiring a C logger pointer.

There is no direct tracker, `raftLog`, unstable, read-state, message-queue, or
configuration-changer access in `node.go`.

## Tests

The focused Phase 1 tests were run with:

```sh
TMPDIR=/home/hilyun/raft-phase1-test.fC7wHy \
  go test . \
  -run 'Test(RawNodeBoundaryGetters|NodeIgnoresResponseFromUnknownPeer|BlockProposalAfterSelfRemoval|BlockProposal|NodePropose|NodeProposeConfig|NodeTick|NodeAdvance|CommitPaginationWithAsyncStorageWrites|RawNodeConsumeReady)$' \
  -count=1
```

Result:

```text
ok  	go.etcd.io/raft/v3	0.226s
```

The complete repository suite was then run with:

```sh
TMPDIR=/home/hilyun/raft-phase1-test.fC7wHy go test ./...
```

Result:

```text
ok  	go.etcd.io/raft/v3	0.379s
ok  	go.etcd.io/raft/v3/confchange	0.052s
ok  	go.etcd.io/raft/v3/quorum	0.122s
ok  	go.etcd.io/raft/v3/raftpb	0.004s
ok  	go.etcd.io/raft/v3/rafttest	0.636s
ok  	go.etcd.io/raft/v3/tracker	0.004s
```

The alternate `TMPDIR` was used because the baseline documented that the
default root-backed temporary filesystem was full. The temporary directory was
removed after verification.

## Later C-boundary follow-up

The Go-only refactor is still complete, but a subsequent API audit identified
C-parity work that is outside Phase 1. The C skeleton/binding must additionally
cover Bootstrap, TickQuiesced, BasicStatus, full Status, and a distinct
WithProgress mapping. See `OUTPUT/RAWNODE_API_PARITY_FOR_C_PORT.md` and
`OUTPUT/C_PORT_TRACKING_GAPS.md`.

No change to the completed Phase 1 Go implementation is implied by this
documentation follow-up.
