# C 멀티노드 Raft 테스트 계획

## 구현 상태

2026-08-03 기준으로 단계 A부터 E까지의 첫 구현이 완료되었다.
`storage`, `network`, `node` 하네스와 세 멀티노드 테스트가 추가되었고,
`make test-c-multinode` 및 `make test-c`에 연결되었다. 일반 빌드 50회
반복 실행과 ASan/UBSan 실행이 통과했다. 아래 내용은 구현의 설계 근거와
후속 확장 계획으로 유지한다.

## 1. 목적

이 디렉터리의 목표는 `rafttest/node_test.go`가 검증하는 멀티노드
시나리오를 네이티브 C `raft_raw_node_t` API 위에서 재현하는 것이다.

첫 번째 목표는 다음 세 테스트와 의미적으로 대응하는 테스트를 만드는
것이다.

| Go 테스트 | C 테스트가 검증할 동작 |
| --- | --- |
| `TestBasicProgress` | 5노드 리더 선출, 100개 proposal 제출, 모든 노드 commit 수렴 |
| `TestRestart` | 두 follower의 중단과 재시작, 재시작 후 기존 storage를 사용한 catch-up, commit 수렴 |
| `TestPause` | 두 follower의 메시지 처리를 일시 중지하고 재개한 뒤 commit 수렴 |

이 테스트는 실제 TCP나 여러 OS 프로세스를 사용하는 시스템 테스트가
아니다. 첫 구현은 한 프로세스, 한 스레드, 가상 시계 기반의 결정론적
통합 테스트로 만든다. 검증 대상은 C Raft core뿐 아니라 공개 C
`RawNode` 경계, storage callback, Ready 처리 순서, 메시지 전달 및 노드
수명주기이다.

## 2. 현재 상태와 검증 범위

현재 `make test-c`에는 `c/tests/raft_core_test.c`의 제한적인 3노드 선거와
복제 시나리오가 들어 있다. 그러나 지속적으로 tick하는 노드 집합,
비동기 메시지 큐, pause/restart, 전체 노드 commit 수렴을 함께 검증하는
하네스는 없다.

기존 Go 하네스는 `cgo_raft` 백엔드와 API 수준에서 호환된다. 다음과
같이 파일을 직접 지정하면 현재 코드에서도 세 5노드 테스트가 C
백엔드로 통과한다.

```sh
CGO_ENABLED=1 go test -tags=cgo_raft \
  ./rafttest/node.go \
  ./rafttest/network.go \
  ./rafttest/node_test.go \
  -run 'TestBasicProgress|TestRestart|TestPause' \
  -count=1
```

이 결과는 C core가 기존 Go `Node` actor 아래에서 동작한다는 유용한
기준점이다. `rafttest_c`는 여기서 더 나아가 Go actor와 cgo 변환에
의존하지 않고 공개 C API만으로 같은 시나리오를 검증한다.

## 3. Go 하네스와 C 하네스의 대응

```text
rafttest/node_test.go             rafttest_c/node_test.c
        |                                  |
        v                                  v
Go 테스트용 node actor             C 테스트용 node wrapper
        |                                  |
        v                                  v
raft.Node                          raft_raw_node_t
        |                                  |
        +---- Ready 처리                    +---- Ready 처리
        +---- MemoryStorage                 +---- C memory storage
        +---- channel network               +---- C message queue
```

Go의 `raft.Node`는 goroutine, ticker, `Ready()` 채널과 `Advance()` 채널을
제공한다. 공개 C API의 중심은 동기식 `raft_raw_node_t`이므로 C 테스트
하네스가 다음 actor 역할을 명시적으로 구현해야 한다.

1. 각 노드에 주기적으로 `raft_raw_node_tick_result()`를 호출한다.
2. `raft_raw_node_has_ready()`가 참인 동안 Ready를 처리한다.
3. HardState, Entries, Snapshot을 노드별 storage에 반영한다.
4. 영속화가 끝난 뒤 Ready의 outbound messages를 네트워크 큐에 넣는다.
5. `raft_raw_node_accept_ready()`와 `raft_raw_node_advance()`를 올바른
   순서로 호출한다.
6. 목적지에 도착한 메시지를 `raft_raw_node_step_for_node()`로 전달한다.
7. 중단된 노드는 파괴하되 storage는 유지하고, 재시작 시 같은 storage
   callback handle로 `raft_raw_node_new()`를 다시 호출한다.

## 4. 제안 디렉터리 구조

구현 단계에서 다음 구조를 사용한다.

```text
rafttest_c/
  PLAN.md              # 이 문서
  Makefile             # libraft_c.a 링크, test 및 sanitizer target
  copy.h
  copy.c               # owned 객체 복사와 Step용 borrowed view 변환
  storage.h
  storage.c            # MemoryStorage에 대응하는 영속 상태와 callback
  network.h
  network.c            # owned message queue, disconnect/pause/delay/drop
  node.h
  node.c               # RawNode 수명주기, tick, Ready pump
  node_test.c          # BasicProgress, Restart, Pause
```

테스트 하네스 전용 함수는 `rafttest_c_` 접두사를 사용해 공개 Raft API와
이름이 충돌하지 않게 한다.

## 5. 주요 자료구조

### 5.1 Storage

`rafttest_c_storage_t`는 노드가 중단되어도 살아 있는 테스트 소유
객체이다.

최소 필드는 다음과 같다.

```c
typedef struct rafttest_c_storage {
    raft_hard_state_t hard_state;
    raft_conf_state_t conf_state;
    raft_entry_t *entries;
    size_t entries_len;
    size_t entries_cap;
    bool has_snapshot;
    raft_snapshot_t snapshot;
} rafttest_c_storage_t;
```

다음 `raft_storage_ops_t` callback을 모두 구현한다.

- `initial_state`
- `entries`
- `term`
- `first_index`
- `last_index`
- `snapshot`

Ready persistence에는 별도 helper를 둔다.

- `rafttest_c_storage_set_hard_state()`
- `rafttest_c_storage_append()`
- `rafttest_c_storage_apply_snapshot()`

`append`는 index `i`의 entry를 기록할 때 이미 저장된 index `>= i`인
suffix를 먼저 제거해야 한다. callback 출력은 공개 C ABI의 ownership
규칙에 맞게 deep copy하고, 호출자가 `raft_entry_vec_free()` 또는
`raft_snapshot_free()`로 해제할 수 있게 한다.

### 5.2 Node wrapper

```c
typedef struct rafttest_c_node {
    uint64_t id;
    raft_raw_node_t *raw_node;
    rafttest_c_storage_t *storage;
    bool running;
    bool connected;
    bool paused;
    rafttest_c_message_queue_t paused_messages;
} rafttest_c_node_t;
```

storage와 node의 수명을 분리한다. `stop`은 `raft_raw_node_destroy()`만
호출하고 storage는 유지한다. `restart`는 bootstrap 없이 동일 storage로
새 RawNode를 생성한다.

### 5.3 Network

네트워크는 C-owned `raft_message_t`를 보관하는 FIFO queue로 구현한다.
각 queue item은 다음 정보를 가진다.

- 송신 노드와 수신 노드 ID
- 전달 예정 virtual tick
- deep-copied `raft_message_t`
- enqueue 순서 번호

Ready는 `raft_ready_destroy()`와 함께 해제되므로 Ready 안의 message
포인터를 queue에 그대로 보관하면 안 된다. enqueue 시 entries,
snapshot, context, responses와 protobuf metadata를 포함한 전체 메시지를
deep copy해야 한다. dequeue 후에는 borrowed `raft_message_view_t`를
만들어 `raft_raw_node_step_for_node()`에 전달하고 owned message를
`raft_message_free()`로 해제한다.

초기 기능은 다음과 같다.

- 목적지별 연결/차단
- paused 노드에 대한 FIFO buffering
- 고정 seed PRNG
- 선택적 메시지 drop 비율
- 선택적 최대 지연 tick
- 같은 목적지에 대한 안정적인 enqueue 순서

`BasicProgress`, `Restart`, `Pause`에서는 유실률을 0으로 두고 작은
결정론적 지연만 사용한다. fault injection은 이후 테스트에서 활성화한다.

## 6. 결정론적 event loop

실시간 sleep과 pthread는 첫 구현에서 사용하지 않는다. 다음 순서의
virtual event loop를 사용한다.

```text
한 virtual tick:
  1. 실행 중이고 pause되지 않은 모든 노드를 한 번 tick
  2. 각 노드의 Ready를 quiescent 상태까지 처리
  3. 현재 tick에 도착 가능한 network message를 FIFO로 전달
  4. 메시지를 받은 노드의 Ready를 다시 quiescent 상태까지 처리
  5. 리더/commit 수렴 조건 검사
```

한 단계에서 무한히 Ready와 message가 생성되는 버그를 감추지 않도록
다음 상한을 둔다.

- 한 노드의 tick당 Ready 처리 횟수
- 한 virtual tick당 전달 메시지 수
- 리더 선출 최대 tick
- commit 수렴 최대 tick
- 전체 queue 최대 크기

상한을 넘기면 현재 tick, 각 노드의 term/state/lead/commit, queue 길이를
출력하고 테스트를 실패시킨다. `while (true)` 기반의 무제한 대기는
허용하지 않는다.

## 7. Ready 처리 규칙

일반 모드(`async_storage_writes = false`)의 Ready는 다음 순서를 지킨다.

1. `raft_raw_node_ready_without_accept()`로 Ready를 얻는다.
2. `has_hard_state`이면 HardState를 저장한다.
3. `entries`를 안정 storage에 append한다.
4. `has_snapshot`이면 snapshot을 저장/적용한다.
5. outbound messages를 network-owned 객체로 deep copy한다.
6. committed entries를 테스트 state machine에 순서대로 적용한다.
7. `raft_raw_node_accept_ready(raw_node, ready)`를 호출한다.
8. `raft_raw_node_advance(raw_node)`를 호출한다.
9. `raft_ready_destroy(ready)`를 정확히 한 번 호출한다.
10. persistence 이후에만 복사된 메시지의 실제 전달을 허용한다.

첫 세 테스트에는 동적 configuration proposal이 없지만, 향후 확장을
위해 committed `RAFT_ENTRY_CONF_CHANGE`와
`RAFT_ENTRY_CONF_CHANGE_V2`를 적용할 위치를 node wrapper에 둔다.
초기 bootstrap membership과 재시작 시 storage가 반환하는 ConfState의
관계는 별도 assertion으로 기록해, Go 테스트가 가진 느슨한 state
machine 적용 동작을 무비판적으로 복제하지 않게 한다.

## 8. 테스트 시나리오

### 8.1 `test_basic_progress`

1. voter `{1,2,3,4,5}`로 노드 다섯 개를 bootstrap한다.
2. 최대 election tick 안에 모든 실행 노드가 같은 nonzero leader를
   관찰하는지 확인한다.
3. 노드 1을 통해 payload `"somedata"` proposal 100개를 제출한다.
4. event loop를 구동해 모든 노드의 commit index가 같은 값으로
   수렴하고 bootstrap/leader noop을 포함해 100보다 큰지 확인한다.
5. 모든 RawNode와 queue-owned message를 해제한다.

### 8.2 `test_restart`

1. 5노드 클러스터에서 리더를 선출한다.
2. 30개 proposal을 제출한다.
3. follower 하나를 stop하고 30개를 추가 제출한다.
4. 다른 follower 하나를 stop하고 30개를 추가 제출한다.
5. 두 번째 follower를 기존 storage로 restart한다.
6. 30개를 추가 제출한다.
7. 첫 번째 follower를 기존 storage로 restart한다.
8. 모든 노드가 같은 commit index로 수렴하고 목표 index를 넘는지
   확인한다.

중단 시 해당 노드로 향하는 in-flight message는 버리며, 재시작 후 새로
전송되는 heartbeat/append를 통해 catch-up하게 한다.

### 8.3 `test_pause`

1. 5노드 클러스터에서 리더를 선출한다.
2. 30개 proposal 후 노드 2의 message stepping을 pause한다.
3. 30개 proposal 후 노드 3도 pause한다.
4. 30개 proposal 후 노드 3을 resume한다.
5. 30개 proposal 후 노드 2를 resume한다.
6. pause 중 buffer된 메시지를 각 노드별 FIFO 순서로 전달한다.
7. 모든 노드의 commit index 수렴을 확인한다.

pause는 기존 Go 테스트와 마찬가지로 해당 노드 wrapper의 tick과 Ready
처리를 멈추되, 네트워크 수신부는 계속 동작해 inbound message를 노드별
FIFO에 보관하는 동작으로 정의한다. resume 시 보관한 메시지를 먼저
순서대로 step한 뒤 정상 tick/Ready 처리를 재개한다.

## 9. 구현 단계

### 단계 A: storage와 ownership

- 동적 entry log 및 storage callback 구현
- Ready append/truncate 규칙 구현
- nested payload deep-copy/free helper 구현
- storage 단위 테스트 추가
- ASan/UBSan에서 leak, use-after-free, double-free가 없음을 확인

### 단계 B: 단일 노드 Ready pump

- node 생성, bootstrap, tick, Ready 처리 구현
- 단일 노드 campaign/proposal/commit smoke test 작성
- 모든 C API 반환 코드를 assertion 없이 삼키지 않고 진단과 함께 처리

### 단계 C: 5노드 network와 BasicProgress

- deterministic message queue와 event loop 구현
- 리더 선출 및 proposal routing 구현
- `test_basic_progress` 통과

### 단계 D: stop/restart/pause

- storage와 RawNode 수명 분리
- disconnect 및 in-flight message 폐기 규칙 구현
- paused queue와 resume drain 구현
- `test_restart`, `test_pause` 통과

### 단계 E: 자동화

- `rafttest_c/Makefile`에 `test`, `test-sanitize`, `clean` 추가
- 루트 Makefile에 `test-c-multinode` target 추가
- 안정화 후 `test-c`가 native unit test와 멀티노드 테스트를 모두
  실행하도록 dependency 연결
- CI에서 일반 빌드와 sanitizer 빌드를 분리 실행

## 10. 제안 실행 명령

구현 완료 후 목표 인터페이스는 다음과 같다.

```sh
# C 멀티노드 테스트만
make test-c-multinode

# 상세 로그와 특정 테스트
make -C rafttest_c test TEST=basic_progress VERBOSE=1

# 메모리 및 undefined behavior 검사
make -C rafttest_c test-sanitize

# 모든 네이티브 C 테스트
make test-c
```

`test-c`에 연결하기 전까지는 기존 테스트의 의미가 바뀌지 않도록
`test-c-multinode`를 별도 target으로 유지한다.

## 11. 완료 기준

- 세 테스트가 고정 seed로 반복 실행되어 항상 통과한다.
- 각 테스트가 모든 노드의 동일 leader 또는 동일 commit index를
  bounded wait 안에서 확인한다.
- Ready가 요구하는 persistence-before-send 순서를 위반하지 않는다.
- restart가 새로운 빈 storage가 아니라 기존 노드 storage를 사용한다.
- pause 중 메시지가 손실되거나 동일 목적지 안에서 재정렬되지 않는다.
- 정상 종료와 실패 경로 모두에서 RawNode, Ready, message, entry,
  snapshot 메모리가 해제된다.
- `test-sanitize`가 ASan과 UBSan에서 통과한다.
- 기존 `make test-c`와 `make test-cgo-raft`가 계속 통과한다.

## 12. 후속 확장

첫 세 테스트 이후 다음 순서로 확장할 수 있다.

1. 네트워크 partition과 quorum 상실 후 재선출
2. 메시지 drop/delay를 사용한 반복 seed 테스트
3. snapshot 기반의 매우 뒤처진 follower 복구
4. learner 추가와 voter 승격
5. leadership transfer
6. configuration change 적용과 restart
7. `async_storage_writes` 전용 local storage worker queue
8. pthread 기반 stress test
9. 별도 프로세스와 TCP transport를 사용하는 system test

결정론적 단일 스레드 테스트는 이후 stress/system test가 추가되어도
유지한다. 프로토콜 회귀를 재현하고 디버깅하는 기본 테스트로 가장
유용하기 때문이다.
