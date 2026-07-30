# Phase 28: C `Status.Config.AutoLeave` Compatibility Result

## Result

The final-audit finding was **confirmed** in current HEAD.

The direction of the mismatch is counterintuitive:

- etcd-io/raft v3.7.0 stores the real `AutoLeave` value in the live
  `ProgressTracker.Config`;
- `ProgressTracker.ConfState()` copies that real value into protobuf
  `ConfState`;
- restore and restart rebuild the real value from `ConfState.AutoLeave`;
- but `Status()` obtains its public `Config` through `Config.Clone()`;
- the v3.7.0 `Config.Clone()` implementation does not copy `AutoLeave`.

Consequently, the authoritative Go implementation always reports:

```text
Status().Config.AutoLeave == false
```

This includes the interval in which the live configuration is joint with
automatic leave enabled.

Before Phase 28, the C status path copied the live tracker into a
`raft_conf_state_t` and exposed its real `auto_leave` value. The cgo-backed
Status therefore reported true where the default Go Status reported false.

The final audit also identified one related snapshot-shape difference:
`Config.Clone()` preserves the non-nil empty incoming voter map created by
`MakeProgressTracker`, while the cgo status conversion previously collapsed
that set to nil.

Phase 28 changes only the Status projection:

- native C `raft_status_t` now reports `auto_leave=false`;
- the cgo Status converter reconstructs a non-nil incoming voter map even
  when it is empty.

The live tracker, `ApplyConfChange` results, snapshot `ConfState`, restore,
restart, joint-consensus transitions, automatic-leave behavior, Progress
membership, protobuf metadata, and C ABI are unchanged.

After the change, no `Status.Config` compatibility mismatch remains in the
audited fields or lifecycle scenarios.

## Authoritative reference

The authoritative reference is the signed etcd-io/raft tag:

```text
v3.7.0
b867cf13f6bc0dae21204302df97bc2355c3af55
```

The relevant Go locations are:

- `status.go:67-77`: `getStatus`;
- `rawnode.go`: `RawNode.Status` delegates to `getStatus`;
- `node.go`: Node status requests are answered from RawNode status;
- `tracker/tracker.go:25-62`: runtime `Config`;
- `tracker/tracker.go:95-113`: `Config.Clone`;
- `tracker/tracker.go:119-160`: tracker initialization and `ConfState`;
- `confchange/confchange.go:45-120`: enter/leave joint transitions;
- `confchange/confchange.go:123-345`: simple changes and invariants;
- `confchange/restore.go:119-153`: restore from `ConfState`;
- `raft.go:742-767`: automatic-leave proposal trigger;
- `raft.go:1881-1937`: snapshot restore;
- `raft.go:1951-2005`: configuration application and tracker switch;
- `raftpb/confchange.go`: transition-to-auto-leave selection.

The relevant C/cgo locations are:

- `c/src/tracker.c:973-998`: live tracker-to-ConfState copy;
- `c/src/confchange.c:688-760`: configuration restore;
- `c/src/confchange.c:889-949`: leave/enter joint;
- `c/src/raft_core.c:2694-2717`: configuration application and automatic
  leave trigger;
- `c/src/raw_node.c:1883-1917`: native Status construction;
- `rawnode_cgo.go:484-516`: cgo RawNode Status;
- `convert_cgo.go:634-665`: Status configuration reconstruction;
- `node.go`: Node Status dispatch.

The relevant Go source files have no semantic differences from v3.7.0 in
these paths.

## `Config` versus `ConfState`

`Config` and `ConfState` describe related membership, but they are not the
same public object.

| Concept | Go representation | C representation | Purpose |
|---|---|---|---|
| Incoming voters | `Config.Voters[0]`, a `MajorityConfig` map | sorted `tracker.config.voters` vector | Current incoming voting quorum |
| Outgoing voters | `Config.Voters[1]`, a `MajorityConfig` map | sorted `voters_outgoing` vector | Old quorum retained while joint |
| Learners | `Config.Learners` map | sorted `learners` vector | Active non-voting members |
| Learners-next | `Config.LearnersNext` map | sorted `learners_next` vector | Demotions staged until joint exit |
| AutoLeave | independent `Config.AutoLeave` bool | independent `config.auto_leave` bool | Whether Raft should propose joint exit automatically |
| Progress membership | separate `ProgressTracker.Progress` map | separate tracker Progress array | Replication state for all configured members |
| Stored configuration | protobuf `ConfState` | `raft_conf_state_t` | Snapshot/storage/restart representation |
| Public status configuration | deep-cloned `tracker.Config` | owned Status carrier converted to `tracker.Config` | Point-in-time reporting snapshot |

### Runtime `Config`

The live `tracker.Config` controls quorum, learner roles, and joint-consensus
behavior. Its `AutoLeave` flag is independent state. It cannot be derived
only from the member sets because the same joint membership can use either:

- automatic leave; or
- application-initiated manual leave.

### Protobuf `ConfState`

`ConfState` carries:

- `Voters`;
- `VotersOutgoing`;
- `Learners`;
- `LearnersNext`;
- optional bool `AutoLeave`.

`ProgressTracker.ConfState()` includes the live `AutoLeave` value. In the
v3.7.0-generated object, the tracker-created bool is present even when false.
Phase 24's supported protobuf presence and encoding behavior remains intact.

Snapshots and storage therefore retain the real configuration mode needed to
restart or restore joint consensus correctly.

### Public `Status.Config`

Go `getStatus` executes:

```text
s.Config = r.trk.Config.Clone()
```

The v3.7.0 clone deep-copies:

- incoming voters;
- outgoing voters;
- learners;
- learners-next.

It does not assign `AutoLeave`, so the returned zero value is false.

This is the exact reference behavior regardless of whether the omission was
desirable as an API design. This project uses v3.7.0 as the compatibility
reference and therefore reproduces it.

## AutoLeave state machine

### How the flag is selected

`ConfChangeV2.EnterJoint()` determines joint and automatic-leave behavior:

- `ConfChangeTransitionAuto` with zero changes means leave joint;
- `ConfChangeTransitionAuto` with one change uses a simple non-joint change;
- `ConfChangeTransitionAuto` with multiple changes enters joint and enables
  automatic leave;
- `ConfChangeTransitionJointImplicit` enters joint and enables automatic
  leave;
- `ConfChangeTransitionJointExplicit` enters joint without automatic leave.

`Changer.EnterJoint(autoLeave, ...)` stores that bool in the runtime Config.

### When automatic leave occurs

While `AutoLeave` is true, a leader checks whether application progress has
reached `pendingConfIndex`. Once it has, the leader proposes an empty
ConfChange V2 entry.

Generating, persisting, or committing the automatic-leave proposal does not
itself clear the runtime flag. The flag is cleared when the application
applies the empty ConfChange V2 and `LeaveJoint()` installs the non-joint
configuration.

### State-transition table

| Event | Live AutoLeave before | Live AutoLeave after | Reason |
|---|---:|---:|---|
| Initial empty configuration | false | false | Tracker initialization |
| Initial normal configuration | false | false | Non-joint invariant |
| Simple configuration change | false | false | Simple changes are non-joint |
| Enter joint, transition Auto with multiple changes | false | true | Automatic joint exit selected |
| Enter joint, JointImplicit | false | true | Implicit exit is automatic |
| Enter joint, JointExplicit | false | false | Application must request exit |
| Automatic leave proposal generated | true | true | Proposal is not yet applied |
| Automatic leave entry persisted/committed | true | true | Runtime configuration is still joint |
| Empty V2 leave applied | true or false | false | `LeaveJoint` clears the flag |
| Successful snapshot restore | previous value replaced | snapshot value | Restore reconstructs Config from ConfState |
| Restart | none in new instance | stored ConfState value | Initial restore reconstructs Config |
| Leadership change | unchanged | unchanged | Leadership is not configuration state |
| Term change | unchanged | unchanged | Term reset does not replace Config |
| Progress reset | unchanged | unchanged | Replication Progress resets independently |
| Learner promotion/removal | false in valid simple config | false | No joint-mode change |

At every row in this table, v3.7.0 `Status.Config.AutoLeave` reports false
because Status uses the clone described above.

## C behavior before Phase 28

The pre-Phase-28 path was:

```text
live C tracker.config.auto_leave
    ↓
raft_core_conf_state_copy
    ↓
raft_status_t.conf_state.auto_leave
    ↓
cTrackerConfig
    ↓
Go Status.Config.AutoLeave
```

Every stage copied the live bool. A restored or newly entered automatic joint
configuration therefore produced:

```text
default Go backend: Status.Config.AutoLeave == false
cgo backend:       Status.Config.AutoLeave == true
```

The internal Raft behavior itself was correct. C used the true live flag to
trigger automatic leave and correctly preserved it in ConfState.

The cgo conversion also used a generic `idSet` helper that returns nil for a
zero-length C vector. For the initial empty configuration, this produced:

```text
default Go: Config.Voters[0] = non-nil empty map
cgo:       Config.Voters[0] = nil map
```

No other member-set mismatch was found:

- outgoing voters are nil outside joint state and populated in joint state;
- learners use nil-aware insertion/deletion in Go and empty-vector cleanup in
  C;
- learners-next is nil outside joint state and populated only when needed;
- nonempty memberships contain the same IDs.

## Implementation

Two production files changed.

### `c/src/raw_node.c`

After copying the live tracker configuration into the owned Status carrier,
`raft_raw_node_status` now sets:

```c
status->conf_state.auto_leave = false;
```

This is deliberately placed in Status construction rather than the shared
tracker-to-ConfState helper.

Therefore it does not affect:

- `raft_core_conf_state_copy` in other contexts;
- `ApplyConfChange` return values;
- snapshots;
- storage callbacks;
- restore/restart;
- protobuf metadata or unknown fields;
- automatic-leave execution.

The `raft_status_t` layout and ABI are unchanged.

### `convert_cgo.go`

`cTrackerConfig` now creates an empty map when the incoming voter vector has
length zero:

```go
if voters == nil {
    voters = make(map[uint64]struct{})
}
```

This mirrors the non-nil incoming map initialized by
`MakeProgressTracker` and preserved by `Config.Clone`.

The generic conversion for outgoing voters, learners, and learners-next is
unchanged.

## Entire `Status.Config` field comparison

| Status.Config field | Go v3.7.0 | C after Phase 28 | Equivalent? |
|---|---|---|---:|
| `Voters[0]` | Deep-copied incoming map; non-nil even when empty | New cgo map with same IDs; non-nil when empty | Yes |
| `Voters[1]` | Deep-copied outgoing map; nil outside joint | New cgo map with same IDs; nil outside joint | Yes |
| `Learners` | Deep-copied map; nil when absent | New cgo map with same IDs; nil when absent | Yes |
| `LearnersNext` | Deep-copied map; nil outside joint | New cgo map with same IDs; nil outside joint | Yes |
| `AutoLeave` | False because `Config.Clone` omits it | False in native Status projection and cgo Config | Yes |
| Progress membership | Separate from Config | Separate from Config | Yes |

## Ordering and canonicalization

Go Status exposes membership as maps. Map iteration order is not guaranteed.
No ordered list is part of `tracker.Config`'s public status shape.

C internally stores sorted ID vectors. The cgo bridge converts those vectors
to maps. The resulting membership sets and nil/non-nil shapes match Go;
vector order is not exposed as an ordering guarantee through Go
`Status.Config`.

No additional sorting or API change was introduced.

## Status snapshot ownership

Go `Config.Clone` allocates fresh maps.

The C-backed path has two independent-copy stages:

1. `raft_raw_node_status` allocates owned ID vectors;
2. `cTrackerConfig` allocates new Go maps before the C Status is freed.

Changing configuration after obtaining `s1 := Status()` therefore cannot
retroactively mutate `s1.Config`. Phase 28 preserves that ownership model and
adds explicit native and backend-neutral tests for it.

## Snapshot and restart behavior

### Snapshot restore

On a successful restore:

1. the snapshot `ConfState.AutoLeave` is decoded;
2. configuration validation runs unchanged;
3. the live tracker stores that value;
4. automatic-leave behavior uses the live value;
5. `Status.Config` reports false through its separate projection.

The Ready snapshot still carries the original true `ConfState.AutoLeave`.

### Restart

New Raft construction obtains HardState and ConfState from Storage and runs
the same configuration restore path. A stored automatic joint configuration
therefore restarts with live `AutoLeave=true`, while RawNode and Node Status
both report false exactly like v3.7.0.

## Joint-consensus lifecycle

The verified public sequence is:

```text
apply implicit/automatic enter-joint ConfChangeV2
    ↓
ApplyConfChange result: ConfState.AutoLeave == true
    ↓
Status.Config.AutoLeave == false
Status.Config contains incoming and outgoing voters
    ↓
applied index reaches pendingConfIndex
    ↓
leader proposes empty ConfChangeV2 automatically
    ↓
application applies empty ConfChangeV2
    ↓
ApplyConfChange result: ConfState.AutoLeave == false
Status.Config.AutoLeave == false
Status.Config.Voters[1] == nil
```

For explicit joint consensus, the ApplyConfChange ConfState and Status both
report false, but the joint member maps remain populated until manual leave.

## Interaction with prior phases

### Phase 22

Progress reset still leaves configuration state intact. Phase 28 changes
neither Progress reset nor the live AutoLeave flag.

### Phase 23

All malformed ConfState and configuration invariants remain enforced:

- AutoLeave cannot be true outside joint state;
- learners and voters remain disjoint;
- learners-next must correspond to outgoing voters;
- valid joint configurations continue to restore.

### Phase 24

The status projection does not use or modify snapshot serialization.

Phase 24 behavior remains unchanged for:

- optional `ConfState.AutoLeave` presence;
- unknown fields;
- Entry and Message protobuf metadata;
- ConfChange encoding;
- ABI markers.

### Phase 27

No proposal validation or empty-`MsgProp` path changed.

## Tests added and updated

### Native C coverage

`c/tests/raft_core_test.c` adds
`test_status_config_autoleave_semantics`.

It verifies:

- initial empty configuration;
- ordinary voters and learners;
- restart-equivalent initialization from automatic joint ConfState;
- live internal `auto_leave=true` versus Status false;
- all four member vectors;
- manual leave and learners-next promotion;
- point-in-time Status ownership;
- a second restart from the same stored state;
- explicit joint consensus without automatic leave;
- implicit joint consensus with internal automatic leave;
- survival across higher term, step-down, and re-election;
- automatic empty-V2 leave proposal generation;
- flag clearing only when leave is applied;
- live snapshot restore from automatic joint ConfState;
- learner addition, promotion, removal, and Progress removal;
- earlier Status snapshots remaining unchanged.

The native storage fixture was extended only to represent all valid ConfState
member sets and AutoLeave during initialization. Production storage behavior
was not changed.

### Backend-neutral Go/C parity coverage

`status_config_autoleave_parity_test.go` runs unchanged against the default
Go backend and the `cgo_raft` backend. It adds:

- `TestStatusConfigInitialAndNormalParity`;
- `TestStatusConfigJointAutoLeaveProjectionParity`;
- `TestStatusConfigExplicitJointChangeParity`;
- `TestStatusConfigAutomaticLeaveParity`;
- `TestStatusConfigSnapshotRestartAndNodeParity`;
- `TestStatusConfigLiveSnapshotRestoreParity`;
- `TestStatusConfigTermLeadershipAndLearnerParity`;
- `TestStatusConfigAutoLeaveSurvivesLeadershipParity`.

The tests compare:

- every `Status.Config` member map;
- map nil/non-nil shape;
- `AutoLeave`;
- RawNode and Node Status;
- `ApplyConfChange` ConfState;
- Ready snapshot ConfState;
- automatic leave proposal/application;
- snapshot restore and restart;
- term and leadership changes;
- learners and Progress membership;
- point-in-time ownership.

`rawnode_cgo_test.go` updates the previous cgo-only restored-joint Status
expectation from true to the v3.7.0-compatible false value.

Focused parity tests passed 100 consecutive runs on both backends.

## Scenario compatibility matrix

| Scenario | Go v3.7.0 | C after Phase 28 | Equivalent? |
|---|---|---|---:|
| Initial empty configuration | AutoLeave false; incoming voter map non-nil empty | Same | Yes |
| Normal configuration | AutoLeave false; current members cloned | Same | Yes |
| Joint plus AutoLeave | Live true; Status false | Live true; Status false | Yes |
| Joint without AutoLeave | Live false; Status false | Same | Yes |
| Automatic leave proposal | Live remains true; Status false | Same | Yes |
| Automatic leave applied | Live false; outgoing/next sets cleared; Status false | Same | Yes |
| Snapshot restore | Live flag restored from ConfState; Status false | Same | Yes |
| Restart | Live flag restored from stored ConfState; Status false | Same | Yes |
| Leadership change | Live flag and member maps unchanged; Status false | Same | Yes |
| Term change | Live flag and member maps unchanged; Status false | Same | Yes |
| Progress reset | Live configuration unchanged | Same | Yes |
| Learner promotion | Learner moves to voter; Status snapshot is independent | Same | Yes |
| Learner removal | Learner and Progress removed | Same | Yes |
| Learner demotion in joint config | Staged in LearnersNext until leave | Same | Yes |
| Status snapshot independence | Earlier cloned maps remain unchanged | Same | Yes |
| Node Status | Same RawNode-derived configuration snapshot | Same | Yes |

## Validation

All validation below passed on 2026-07-31.

Focused tests:

```text
go test -count=1 -run '^TestStatusConfig' .

CGO_ENABLED=1 go test -count=1 -tags=cgo_raft \
  -run '^(TestStatusConfig|TestCGoRawNodeRestoresJointConfState)' .

go test -count=100 -run '^TestStatusConfig' .

CGO_ENABLED=1 go test -count=100 -tags=cgo_raft \
  -run '^TestStatusConfig' .
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

1. **Where does Go store AutoLeave?**  
   In the live `tracker.Config` embedded in `ProgressTracker`.

2. **When is AutoLeave set?**  
   `EnterJoint` sets it from the ConfChange V2 transition choice. Automatic
   and joint-implicit transitions set true; joint-explicit sets false.
   Restore/restart set it from stored `ConfState.AutoLeave`.

3. **When is AutoLeave cleared?**  
   `LeaveJoint` clears it when the empty ConfChange V2 is applied. Merely
   proposing or committing automatic leave does not clear it.

4. **Is AutoLeave encoded in ConfState?**  
   Yes. It is an optional protobuf bool, and tracker-generated ConfState
   includes the live value.

5. **Is AutoLeave derived or independently stored?**  
   It is independently stored. Identical joint member sets can be automatic
   or manual, so membership alone is insufficient.

6. **What does Status.Config return during joint consensus?**  
   It returns cloned incoming/outgoing voter and learner maps, but v3.7.0's
   clone reports `AutoLeave=false` even when the live flag is true.

7. **What happens after automatic joint leave?**  
   Applying the empty V2 change removes the outgoing configuration, promotes
   learners-next as required, and clears the live flag. Status continues to
   report false and shows the new non-joint membership.

8. **What happens after snapshot restore?**  
   The live flag is rebuilt from snapshot ConfState and controls future
   automatic leave. Status still reports false through the cloned
   representation.

9. **What was the exact C mismatch?**  
   Native C Status copied and exposed the live `auto_leave`, so cgo Status
   reported true during automatic joint consensus. The cgo converter also
   returned nil for the initial empty incoming voter map.

10. **What changed?**  
    Status construction now clears only its owned output copy's
    `auto_leave`, and the cgo Status converter preserves the non-nil empty
    incoming voter map.

11. **Are all Status.Config fields now compatible?**  
    Yes. Incoming voters, outgoing voters, learners, learners-next,
    AutoLeave, nil/non-nil shape, and snapshot ownership match in all audited
    scenarios.

## Remaining limitations

No `Status.Config` compatibility mismatch remains.

The reference behavior can look surprising because `Status.Config.AutoLeave`
does not reveal the live automatic-leave mode. This is the actual etcd
v3.7.0 behavior produced by `Config.Clone`, and is therefore compatibility
behavior rather than a remaining C limitation.

No other final-audit finding was modified in Phase 28.
