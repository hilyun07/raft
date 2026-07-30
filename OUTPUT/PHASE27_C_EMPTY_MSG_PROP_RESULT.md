# Phase 27: C Empty `MsgProp` Compatibility Result

## Result

The final-audit finding was **confirmed**, but only for one precisely defined
case:

```text
leader receives MsgProp with zero Entry objects
```

etcd-io/raft v3.7.0 treats this as an invariant violation and calls
`Logger.Panicf`. Before Phase 27, the C leader returned
`RAFT_ERR_PROPOSAL_DROPPED`. The difference also changed ordering for a
removed leader or a leader transferring leadership: Go checks the empty
entry list first and panics, while C previously performed the drop checks
first.

This finding does **not** apply to an `Entry` whose `Data` has length zero.
Such an entry is a valid proposal in Go. The C backend already handled it
correctly:

- it appends one log entry;
- it assigns the current term and next index;
- it preserves all other supported Entry metadata;
- it contributes zero bytes to uncommitted-size accounting;
- it appears in `Ready`;
- and its `Entry.Type` can still give it configuration-change meaning.

Phase 27 adds one leader-side zero-entry invariant check before the existing
membership and leadership-transfer checks. Through the Phase 19 fatal-error
policy, native C reports and latches `RAFT_ERR_FATAL`; the cgo RawNode path
converts that terminal failure to a Go panic. This is the established C/cgo
mapping for a Go invariant panic.

After this change, no empty-proposal semantic mismatch remains in the audited
behavior.

## Authoritative reference

The authoritative reference is the signed etcd-io/raft tag:

```text
v3.7.0
b867cf13f6bc0dae21204302df97bc2355c3af55
```

The relevant reference locations are:

- `raft.go:1294-1350`: leader `MsgProp` validation and
  configuration-change processing;
- `raft.go:812-840`: `appendEntry`;
- `raft.go:1684-1720`: candidate/pre-candidate `MsgProp` handling;
- `raft.go:1720-1737`: follower proposal forwarding;
- `raft.go:2098-2120`: `increaseUncommittedSize`;
- `rawnode.go:89-105`: RawNode proposal construction;
- `node.go:471-501`: Node proposal construction;
- `node.go:512-541`: Node proposal-channel dispatch.

The corresponding C locations are:

- `c/src/raft_core.c:2074-2097`: leader `RAFT_MSG_PROP` handling;
- `c/src/raft_core.c:1823-1898`: proposal copying and
  configuration-change validation;
- `c/src/raft_core.c:573-631`: entry append and uncommitted-size accounting;
- `c/src/raft_core.c:1693-1712`: follower proposal forwarding;
- `c/src/raft_core.c:1792-1794`: candidate proposal rejection;
- `c/src/raft_core.c:2551-2564`: C RawNode `Propose` construction.

The relevant proposal behavior in the repository's Go source remains the
v3.7.0 reference behavior.

## Definitions

The phrase “empty proposal” can describe several different inputs. They do
not have the same semantics.

### A. Zero-entry `MsgProp`

```text
Message.Type = MsgProp
len(Message.Entries) = 0
```

There is no Entry object to append. This is the invariant-violating case when
the message reaches a leader.

### B. One empty-data Entry

```text
Message.Type = MsgProp
len(Message.Entries) = 1
len(Message.Entries[0].Data) = 0
```

There is one real Entry object with a zero-length application payload. This
is valid and creates one log entry.

Both nil `Data` and present-but-empty `Data` have length zero. Phase 24
preserves that optional-bytes presence distinction across the supported
Go/C object conversion.

### C. Multiple empty-data Entries

Every Entry is independently cloned and appended. Each receives its own
index. The entries add zero to the payload-size budget but are not collapsed
into one entry or into no proposal.

### D. Metadata-bearing empty-data Entry

An empty `Data` field does not erase other Entry metadata. In particular:

- `Term` and `Index` supplied by the proposer are overwritten by the leader;
- `Type` remains meaningful;
- supported protobuf presence and unknown-field metadata continue to follow
  Phase 24 behavior.

An empty-data Entry can therefore be an ordinary application entry, a
configuration-change entry, or another supported Entry form according to
its `Type`.

## Exact etcd v3.7.0 behavior

### RawNode and Node construction

`RawNode.Propose(data)` and `Node.Propose(ctx, data)` always construct:

```text
MsgProp
  Entries:
    - Entry{Data: data}
```

Consequently:

- `Propose(nil)` means one Entry with absent/nil Data;
- `Propose([]byte{})` means one Entry with present-empty Data;
- neither call creates a zero-entry `MsgProp`;
- both are valid proposals when normal state-dependent proposal rules allow
  them.

A caller can create the zero-entry form only by directly stepping a manually
constructed `MsgProp`.

### Leader

The v3.7.0 leader performs checks in this order:

1. if `len(m.Entries) == 0`, call `Logger.Panicf`;
2. if the local node no longer has a Progress entry, return
   `ErrProposalDropped`;
3. if leadership transfer is in progress, return `ErrProposalDropped`;
4. validate configuration-change entries;
5. clone, assign term/index, account for payload size, and append;
6. broadcast append messages.

Therefore a zero-entry message panics even when:

- the leader has been removed from the configuration; or
- leadership transfer is in progress.

A nonempty proposal in those states is still dropped normally.

### Follower

A follower does not inspect the entry count.

- With no known leader, it returns `ErrProposalDropped`.
- With proposal forwarding disabled, it returns `ErrProposalDropped`.
- Otherwise, it sets `To` to the known leader and forwards the complete
  `MsgProp`, including a zero-entry message.

The forwarding follower itself does not panic. If the forwarded zero-entry
message reaches the leader, the leader applies its invariant check.

### Candidate and pre-candidate

Candidate and pre-candidate state handlers return `ErrProposalDropped` for
all `MsgProp` messages before any leader-only proposal validation. This is
true for:

- zero-entry messages;
- one empty-data Entry;
- ordinary nonempty proposals.

### Removed peer

A removed non-leader follows its current state handler. A leader whose local
Progress entry has been removed still performs the zero-entry invariant
check before the membership drop check.

### Node asynchronous behavior

`Node.Step` sends proposals through the proposal channel when that channel is
enabled. For a leader, a zero-entry message reaches the actor's leader step
and triggers the same panic. `Node.Propose` is unaffected because it always
constructs one Entry.

When Node has no known leader, the proposal channel is disabled in the
reference actor loop. Context cancellation, Stop, and normal asynchronous
Node scheduling remain Node-level concerns and were not changed in this
phase.

## Nil versus empty repeated Entries

The protobuf `Entries` field is repeated. It has no optional-field presence
bit distinguishing:

```text
Entries == nil
```

from:

```text
Entries != nil && len(Entries) == 0
```

Both have length zero, serialize as no Entry values, and are treated
identically by Raft. Direct Go slice nilness can be observed by code that
inspects an in-memory object, but it is not a distinct protobuf field state
and is outside the object-level fidelity guaranteed by Phase 24.

The C ABI similarly represents both as an entry vector of length zero.
Phase 27 tests both C vector shapes (`items == NULL` and `items != NULL` with
`len == 0`) and both Go slice shapes. They produce the same Raft behavior.

This is different from `Entry.Data`, which is an optional bytes field whose
nil versus present-empty state is preserved.

## Entry cloning and append behavior

For every accepted Entry, Go:

1. clones the protobuf Entry;
2. replaces its `Term` with the leader's current term;
3. replaces its `Index` with the next log index;
4. retains the remaining Entry content and metadata;
5. accounts for `len(Data)`;
6. appends the Entry;
7. arranges the normal self-acknowledgment after persistence.

The C path already performs the equivalent supported-object conversion,
copying, assignment, accounting, append, Ready, and storage behavior.
Phase 27 does not change this path.

Thus:

- one empty-data Entry creates one log entry;
- multiple empty-data Entries create multiple consecutive log entries;
- a mixed batch keeps its exact Entry count and order;
- explicitly present Entry metadata is not discarded because Data is empty.

## `MaxUncommittedEntriesSize`

etcd v3.7.0 computes proposal size as the sum of `len(Entry.Data)`.
Entry count and protobuf envelope size do not contribute.

The admission condition is:

```text
reject only when:
  current uncommitted payload size > 0
  and new aggregate payload size > 0
  and their sum exceeds the configured limit
```

Consequences:

- a zero-entry message never reaches this accounting on a leader because it
  violates the earlier invariant;
- an empty-data Entry contributes zero bytes;
- any number of all-empty Entries can be appended even when a positive
  uncommitted tail already exceeds the limit;
- a mixed batch is charged for only its nonempty payloads;
- the Phase 16 rule still permits the first oversized positive-payload
  proposal while the uncommitted tail is empty;
- once the tail is nonempty, a later mixed batch with positive aggregate
  payload is rejected when it would exceed the limit.

The existing C accounting already matched all of these rules and was not
modified.

## Configuration-change interaction

Entry type, not payload length alone, determines whether an Entry carries a
configuration change.

### Empty `EntryConfChange`

Empty bytes decode as a zero-valued ConfChange V1 object. The leader still
recognizes it as a configuration-change proposal, runs the usual pending and
joint-state checks, and can assign `pendingConfIndex`. It is not reclassified
merely because the payload length is zero.

### Empty `EntryConfChangeV2`

Empty bytes decode as an empty ConfChange V2. Semantically, that form asks to
leave a joint configuration.

- When already joint and otherwise valid, it remains a meaningful leave-joint
  proposal.
- When not joint and validation is enabled, v3.7.0 downgrades it to an empty
  normal Entry because an empty V2 change cannot leave a nonexistent joint
  configuration.

The C decoder and proposal-validation path already implements this behavior.
Phase 27 does not alter configuration-change encoding or Phase 24 metadata.

## Ready and storage behavior

A leader-side zero-entry `MsgProp`:

- changes no log entry;
- changes no commit index;
- generates no new Ready entry;
- generates no storage append;
- terminates through the established invariant-failure path.

An accepted empty-data Entry follows the ordinary proposal lifecycle:

```text
MsgProp
  ↓
clone and assign Term/Index
  ↓
unstable log
  ↓
Ready.Entries
  ↓
storage persistence
  ↓
Advance / append acknowledgment
  ↓
normal commit and Ready.CommittedEntries behavior
```

There is no special no-op storage path based only on `len(Data) == 0`.

## C behavior before Phase 27

Before this phase, the leader performed the membership and leadership-transfer
drop checks before calling `core_prepare_proposal_entries`.

That helper returned `RAFT_ERR_PROPOSAL_DROPPED` when `source->len == 0`.
Accordingly:

```text
normal leader + zero entries
  Go: Panicf
  C:  RAFT_ERR_PROPOSAL_DROPPED
```

For a removed or transferring leader, C returned the same ordinary drop even
earlier. No terminal error was latched, and the C RawNode remained usable.
That differed observably from the Go invariant failure.

All nonzero-entry proposal behavior was already compatible.

## Implementation

One production file changed.

### `c/src/raft_core.c`

At the beginning of the leader's `RAFT_MSG_PROP` case, before membership and
leadership-transfer checks, Phase 27 adds:

```c
if (message->entries.len == 0) {
    return RAFT_ERR_FATAL;
}
```

`raft_core_step` passes this through the existing Phase 19 error latch.
Therefore:

- native C returns `RAFT_ERR_FATAL`;
- the first terminal error remains latched;
- repeated operations report the same terminal failure;
- no partial proposal mutation occurs;
- the cgo RawNode wrapper observes the latched terminal error and panics;
- the cgo Node actor surfaces the same invariant panic.

No ABI, public API, Entry representation, ownership rule, configuration
decoder, size-accounting logic, Ready path, storage path, forwarding path, or
fatal-error mechanism changed.

The helper's length-zero drop remains a defensive internal check, but the
leader handler now enforces the reference invariant before that helper can be
called with zero entries.

## Tests added

### Native C coverage

`c/tests/raft_core_test.c` adds:

- `test_empty_message_proposal_semantics`;
- `test_empty_entry_proposal_semantics`;
- a local single-node leader construction helper used by those tests.

The native tests verify:

- leader zero-entry messages with both null and nonnull zero-length vectors;
- terminal error latching and no log mutation;
- follower forwarding of a zero-entry message;
- candidate rejection without terminal failure;
- the removed-leader ordering distinction;
- a nonempty proposal on a removed leader still being dropped;
- nil Data, present-empty Data, and nonempty Data in one batch;
- term/index reassignment;
- retained supported Entry metadata;
- multiple empty-data entries;
- Phase 16's first-oversized-proposal rule;
- all-empty batches after the size budget is occupied;
- rejection of a mixed positive-size batch over the limit;
- unchanged log and uncommitted size after that rejection;
- empty ConfChange V2 downgrade outside joint state;
- present-empty ConfChange V1 acceptance and `pendingConfIndex`.

### Backend-neutral Go/C parity coverage

`empty_msg_prop_parity_test.go` runs unchanged against the default Go backend
and the `cgo_raft` backend. It adds:

- `TestLeaderZeroEntryMsgPropPanicsParity`;
- `TestRemovedLeaderZeroEntryMsgPropPanicsParity`;
- `TestNodeLeaderZeroEntryMsgPropPanicsParity`;
- `TestFollowerAndCandidateEmptyMsgPropParity`;
- `TestLeaderEmptyDataEntryReadyParity`;
- `TestLeaderMixedAndMetadataOnlyEntriesParity`;
- `TestEmptyDataEntriesMaxUncommittedParity`;
- `TestEmptyDataConfigurationEntryParity`.

These tests compare:

- nil and nonnil empty repeated Entry slices;
- RawNode and Node leader invariant behavior;
- removed-leader ordering;
- follower forwarding;
- candidate rejection;
- nil versus present-empty Data;
- Ready entry count, order, term, index, type, and data;
- mixed and metadata-only Entries;
- uncommitted-size admission;
- empty V1/V2 configuration-change handling.

The focused parity tests passed 100 consecutive runs on both backends. No
timing-based sleep or new public test API was introduced.

## Compatibility matrix

| Scenario | Go v3.7.0 | C after Phase 27 | Equivalent? |
|---|---|---|---:|
| Zero-entry `MsgProp` at leader | Invariant panic | Fatal latch; cgo panic | Yes |
| Nil `MsgProp.Entries` at leader | Same zero-entry invariant panic | Same | Yes |
| Non-nil empty `MsgProp.Entries` at leader | Same zero-entry invariant panic | Same | Yes |
| One empty-data Entry | Valid; appends one Entry | Same | Yes |
| Multiple empty-data Entries | Valid; appends every Entry | Same | Yes |
| Mixed empty/nonempty Entries | Valid subject to aggregate positive payload limit | Same | Yes |
| Metadata-only Entry | Valid; leader overwrites Term/Index and retains supported metadata | Same | Yes |
| Leader | Checks zero entry count before membership/transfer checks | Same | Yes |
| Follower with known leader | Forwards zero-entry and empty-data forms | Same | Yes |
| Follower without leader/forwarding | Drops proposal | Same | Yes |
| Candidate/pre-candidate | Drops all proposal forms | Same | Yes |
| Removed leader | Zero entries panic; nonempty entries drop | Same | Yes |
| Proposal forwarding | Entry count does not alter follower forwarding | Same | Yes |
| `MaxUncommittedEntriesSize` | Sums only Data lengths; aggregate-zero batches always admitted | Same | Yes |
| Empty `EntryConfChange` Data | Decodes zero V1 change and follows normal validation | Same | Yes |
| Empty `EntryConfChangeV2` Data | Leave-joint form; downgraded outside joint when validation is enabled | Same | Yes |
| Ready generation for zero entries | No Ready entry; invariant failure | Same | Yes |
| Ready generation for empty Data | One Ready entry per proposed Entry | Same | Yes |
| Storage handling for empty Data | Ordinary persistence path | Same | Yes |

## Validation

All validation below passed on 2026-07-31.

Focused backend-neutral tests:

```text
go test -count=1 ./... \
  -run 'Test(LeaderZeroEntryMsgPropPanicsParity|RemovedLeaderZeroEntryMsgPropPanicsParity|NodeLeaderZeroEntryMsgPropPanicsParity|FollowerAndCandidateEmptyMsgPropParity|LeaderEmptyDataEntryReadyParity|LeaderMixedAndMetadataOnlyEntriesParity|EmptyDataEntriesMaxUncommittedParity|EmptyDataConfigurationEntryParity)'

CGO_ENABLED=1 go test -count=1 -tags=cgo_raft ./... \
  -run 'Test(LeaderZeroEntryMsgPropPanicsParity|RemovedLeaderZeroEntryMsgPropPanicsParity|NodeLeaderZeroEntryMsgPropPanicsParity|FollowerAndCandidateEmptyMsgPropParity|LeaderEmptyDataEntryReadyParity|LeaderMixedAndMetadataOnlyEntriesParity|EmptyDataEntriesMaxUncommittedParity|EmptyDataConfigurationEntryParity)'

go test -count=100 . \
  -run 'Test(LeaderZeroEntryMsgPropPanicsParity|RemovedLeaderZeroEntryMsgPropPanicsParity|NodeLeaderZeroEntryMsgPropPanicsParity|FollowerAndCandidateEmptyMsgPropParity|LeaderEmptyDataEntryReadyParity|LeaderMixedAndMetadataOnlyEntriesParity|EmptyDataEntriesMaxUncommittedParity|EmptyDataConfigurationEntryParity)'

CGO_ENABLED=1 go test -count=100 -tags=cgo_raft . \
  -run 'Test(LeaderZeroEntryMsgPropPanicsParity|RemovedLeaderZeroEntryMsgPropPanicsParity|NodeLeaderZeroEntryMsgPropPanicsParity|FollowerAndCandidateEmptyMsgPropParity|LeaderEmptyDataEntryReadyParity|LeaderMixedAndMetadataOnlyEntriesParity|EmptyDataEntriesMaxUncommittedParity|EmptyDataConfigurationEntryParity)'
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

cgo ASan and LeakSanitizer:

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

Valgrind leak/error checking passed for all nine native test binaries:

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

Strict clean C11 compiler builds and tests:

```text
make -C c clean test CC=clang
make clean-c && make test-c CC=gcc
```

Formatting and whitespace:

```text
make verify-gofmt
git diff --check
```

## Final answers

1. **Is a zero-entry `MsgProp` valid in Go?**  
   It is not a valid leader proposal: the v3.7.0 leader treats it as an
   invariant violation and panics. State-specific routing still matters: a
   follower may forward it, while a candidate drops it.

2. **Is an empty-data Entry a valid proposal?**  
   Yes. It is a real Entry with a zero-length payload and follows the normal
   append, Ready, storage, replication, and commit path.

3. **Are nil and empty proposal-entry slices observably different?**  
   Not to Raft or protobuf serialization. Both have zero repeated values and
   trigger the same state-dependent behavior. Direct in-memory Go slice
   nilness is not protobuf presence.

4. **Does an empty-data Entry contribute to uncommitted size?**  
   It contributes zero bytes. It still contributes one log entry.

5. **Can an empty-data Entry still represent a configuration change?**  
   Yes. `Entry.Type` selects configuration-change decoding independently of
   Data length. Empty V1 and V2 payloads have the verified zero-value
   configuration-change meanings and validation behavior.

6. **What happens when an empty proposal reaches a follower?**  
   With a known leader and forwarding enabled, the follower forwards it
   unchanged. Without a leader or with forwarding disabled, it returns
   `ErrProposalDropped`. The follower does not run the leader's zero-entry
   invariant check.

7. **What was the exact C mismatch?**  
   The leader returned `RAFT_ERR_PROPOSAL_DROPPED` for a zero-entry message
   instead of treating it as an invariant failure. It also checked removed
   membership and leadership transfer before the zero-entry condition,
   unlike Go.

8. **What changed?**  
   The leader's C `RAFT_MSG_PROP` branch now returns `RAFT_ERR_FATAL` for
   zero entries before any membership or transfer check. Existing fatal
   latching and cgo panic conversion provide the established Go-compatible
   observable behavior.

9. **Does C now match Go for all relevant proposal cases?**  
   Yes. The audited zero-entry, empty-data, state, forwarding, size,
   configuration-change, Ready, and storage cases now match.

## Remaining limitations

No empty-`MsgProp` or empty-data proposal mismatch remains.

Native C expresses the Go `Logger.Panicf` invariant as `RAFT_ERR_FATAL` plus
the central terminal-error latch. The cgo API converts it to a Go panic.
That is the project's established cross-language invariant policy, not a
remaining proposal limitation.

No other final-audit finding was modified in Phase 27.
