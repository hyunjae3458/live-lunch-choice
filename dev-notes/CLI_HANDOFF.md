# 최신 문서 위치 안내 — 2026-10-20

공개 설명은 README.md와 docs/의 네 문서, 개발·에이전트 기록은 dev-notes/로 분리했다. 사용자 요청으로 두 폴더와 AGENTS.md를 Git 관리 대상에 포함한다. 진행 기록은 dev-notes/PROGRESS.md를 갱신한다. 이전 실험 인계는 이력이며 현재 후속 실험 자동 진행 중단 상태를 유지한다. 이번 작업은 문서 정리만이며 커밋·push는 미수행.

---

> 2026-10-20 18:45 KST 최신 사용자 지시: 사용자 최신 지시에 따라 부하테스트 도구·계측 코드 삭제 완료. DB + SSE 커밋 681c72c 유지. 후속 부하 실험 자동 진행하지 않음. 결과 Markdown은 보존하며 이전 실행 도구 및 원본 경로는 삭제됨.

# CLI 최신 인계: 실제 투표·집계·SSE 부하 및 Kafka 역할 검증

기록: 2026-10-20 KST

사용자가 남은 실제 투표 이벤트 기반 실험을 요청했다. **dev-notes/LOAD_TEST_EVENT_PIPELINE_HANDOFF.md**를 우선해 check→full→burst→Consumer 중단/재개→viewers를 수행한다. 완료된 조회 진단을 재실행하지 말고 이전 read critical과 별도로 본 경로를 시작한다. 경미한 경고는 보존하며 진행하고 critical은 해당 부하를 중단·정상화한다. 다른 모드의 비교와 가능한 경계 재현이 전체 예외 종료로 사라지지 않게 실행기를 보완한다.

접수 성공, Kafka ack, 집계 완료, SSE 수신을 구분하고 lag/로컬큐/실제 결과 지연/최종 정확성을 함께 평가한다. 새 결과는 LOAD_TEST_EVENT_PIPELINE_RESULT.md에 기록. 현재는 인계만 완료했으며 새 실험 미실행. 기존 원본과 실제 서비스 보존.

---

# CLI 최신 인계: 경미한 경고를 넘겨 실행, 실제 DB 병목 확인

기록: 2026-10-19 18:47 KST

사용자는 발생률/스케줄 같은 작은 기준 미달만으로 멈추지 말고 critical에서만 중단하도록 변경했다. 이전 기준은 합격/경고 판정에 남기고 전체 중단에는 사용하지 않는다. `continue.py`가 최신 실행기이며 기존 `batch.py`는 이전 정책이다.

resume1 소부하2회+조회12회 완료.100만 건DB50req/s에서15초 성공응답 없음·pool10개소진/최대190대기·30초연결타임아웃으로중단.같은Redis조회50은1500/1500성공/p95 5.76ms. PG2CPU throttling509/514기간확인. 실제 DB 병목이며 이전 단순발생률미달과다르다. 상세 CAPACITY_RESULT 하단과 PROGRESS참조. 본 투표/SSE부하·경계반복·관람자증가는아직미실행.

데이터/볼륨/결과/이전환경보존,전용컨테이너중지·실제서비스유지. source/계측 수정/결과는load-tests/capacity와results/capacity/llc-cap-20261019-173825/resume1-*에있다. DB예산을임의로변경하지않았으며조회결과를Kafka자체효과나전체경로우세로일반화하지않는다. 마지막사후CLI샘플링구간수정은문법확인만했으며부하재측정미실행.

---

# CLI 최신 인계: capacity 구현·기능 검증 완료, 환경 기준으로 증가 중단

기록: 2026-10-19 18:12 KST

`dev-notes/LOAD_TEST_CAPACITY_RESULT.md`와 PROGRESS 최신 상태를 먼저 읽는다. db-sse 비교 모드/유한 큐/공통 SSE/격리 인프라/비동기 관측기를 구현하고 일반83+실제통합9건, 양쪽14표 소부하 동작을 검증했다. 1만 표 조회10req/s 재검증 후200req/s에서 발생5919/6000건으로99% 기준 미달, 부하 증가 중단. 기존 polling 결과는 사용하지 않았다. **본 실시간 경로 처리 용량은 미검증이며 우열 결론 없음.**

Kafka lag CLI가 CPU를 교란한 점을 발견해 호스트의 단일 AdminClient 방식으로 수정·읽기 검증했다. 수정 관측기의 부하 재검증과 CPU throttling/DB pool 획득 시간 검증이 남는다. 기존 CPU 수치를 보정하거나 성공한 비교로 재사용하지 않는다. 호스트 자원 확보/발생기 분리 후 새 결과 이름으로 단계별 재검증한다. 새 컨테이너3개는 중지·데이터/볼륨 보존, 실제 서비스 유지. 사용자 .env 변경 없음. 원본 `load-tests/results/capacity/llc-cap-20261019-173825/`, 실험 도구 `load-tests/capacity/README.md`.

---

# CLI 최신 인계: 기존 비교 폐기 및 실시간 처리 용량 실험 재설계

기록: 2026-10-19 17:18 KST

사용자가 기존 polling/realtime 부하 비교를 폐기하고 원하는 처리 용량 지표를 확인할 새 실험을 요청했다. **dev-notes/LOAD_TEST_CAPACITY_HANDOFF.md**를 우선한다. DB·SSE 기준 구현과 동일 SSE 계약, 누적 데이터별 집계 비용, 제한된 부하 상승 및 실패 경계 비교가 범위다. 기존 원본은 삭제하지 않고 비교 근거에서 제외한다. 현재 새 코드는 미구현·새 실험은 미실행이다. 종전 소규모 상한/모드 수동 전환 지시보다 새 계획을 우선한다.

---

# CLI 최신 인계: 단계별 부하 비교 실행

기록: 2026-10-19 15:24 KST

**dev-notes/LOAD_TEST_SCALE_HANDOFF.md**에 따라 관측기 재검증 후 관람자10/50/100 증가, 이후 투표1/5/10req/s 증가 비교를 수행한다. 각 조건 반복 측정과 중단 기준을 준수한다. 기존 polling→사용자 모드 전환 확인→realtime 절차는 유지한다. 과거 소규모 준비 한정보다 이번 최신 범위가 우선하며 수천 연결·자동 포화·제품 고도화는 제외한다.

---

# CLI 최신 인계: 별도 DB 기반 부하 비교 준비

기록: 2026-10-19 14:19 KST

**dev-notes/LOAD_TEST_HANDOFF.md**를 읽고 실험 전용 DB/인프라·데이터·측정 스크립트를 준비하고 작은 부하로 두 모드를 예비 검증한다. 종료/초기화 서비스 고도화는 사용자 결정으로 진행하지 않는다. 프론트와 후보 등록은 사용자 확인 완료. 대규모 비교는 예비 결과를 바탕으로 조건을 먼저 정리한다. 아래 과거 인계보다 이 최신 범위를 우선한다.

---

# CLI 최신 인계: polling/realtime 프로필 분리

기록: 2026-10-16 16:10 KST

다중 서버 기능 실험은 완료됐다. 이제 **dev-notes/PROFILE_SEPARATION_PLAN.md**와 AGENTS.md, PROGRESS.md, DESIGN_DECISIONS.md를 읽고 프로필 분리를 구현·검증한다. 이번 범위는 프로필 분리이며 프론트와 부하 비교는 후속이다. VoteService의 공통 DB 기능은 유지하고 실시간 부분을 VoteRealtimeService로 분리한다. 아래는 과거 인계이며 최신 계획과 충돌하는 이전 지시는 적용하지 않는다.

---

# CLI 최신 인계: 다중 서버 기능 실험

기록: 2026-10-16 15:32 KST

사용자가 다음 단계로 다중 서버 기능 확인을 요청했다. AGENTS.md, PROGRESS.md, DESIGN_DECISIONS.md와 **dev-notes/MULTI_SERVER_SSE_PLAN.md**를 읽고 같은 앱 두 인스턴스로 실제 실험을 진행하고 결과까지 기록한다. 부하 비교는 추후 계획이며 이번에 실행하거나 비교용 구조를 구현하지 않는다. 현재 서비스·DTO 통합은 유지한다. 아래 이전 인계는 과거 기록이며 완료된 구현을 다시 만들지 않는다.

---

# CLI 인계: Redis 랭킹 조회·Pub/Sub·SSE 구현 및 검증

최신 인계: 2026-10-15 23:45 KST. 사용자가 코드를 이해했으며 CLI와 구현·테스트를 진행하도록 명시적으로 요청했다. 이번 범위에는 아래 기능 구현과 필요한 테스트가 포함된다. 과거의 Redis/Pub/Sub/SSE 구현 금지 안내보다 이 최신 요청이 우선한다.

## 먼저 읽기
AGENTS.md, dev-notes/PROGRESS.md, dev-notes/DESIGN_DECISIONS.md 및 실제 코드/git diff를 확인한다. 사용자 동시 변경을 보존한다. 특히 이전 예제의 Map 변환·DB 후보 조회·Java 재정렬을 구현하지 않는다.

## 구현 범위와 완료 기준
1. VoteRankingRedis: reverseRangeWithScores로 전체 랭킹을 읽고 순서를 유지하는 List로 변환. candidateId/voteCount 전용 DTO를 사용한다. DB 조회 및 Java 재정렬 없음. 빈 결과는 [].
2. GET /api/votes/rankings: 위 최신 스냅샷 응답. 기존 /api/votes/results는 DB 집계 비교용으로 유지하며 응답 계약을 변경하지 않는다.
3. VoteRankingSseService 및 GET /api/votes/rankings/stream: text/event-stream, 초기 최신 랭킹 전송, ranking 이벤트 이름, JSON 배열. 연결 목록 동시성·초기 전송/변경 전송 순서·종료 정리 처리. ResponseEntity 스타일 유지 가능.
4. VoteRankingPublisher/Subscriber 및 RedisMessageListenerContainer 구성. 채널 live-lunch-choice:ranking-changed. 집계 true일 때만 eventId 알림 발행, 구독 서버는 최신 랭킹 조회 후 자신의 SSE 연결들에 전송. 중복 false에는 발행 없음.
5. 이름 및 0표 후보는 프론트가 기존 후보 API와 합치는 계약만 문서화. 전체 UI 구축/후보명 캐시/새 인프라/범위 밖 장애 보상은 구현하지 않는다.
6. Redis 동점 순서를 유지하는 기본값과 DB 결과 API 동점 규칙 차이를 기록한다. 수치 ID 오름차순을 맞추려고 재정렬하지 않는다.

## 검증 순서
- 일반 테스트: 신규/중복에 따른 알림 여부, Redis 조회 응답 순서·빈 목록, SSE 초기 이벤트/다중 연결 전송/완료 정리, 기존 API 회귀 중 변경과 관련된 항목. 구현을 그대로 복제한 테스트 대신 외부 관찰 결과를 확인한다.
- 실제 Redis 통합: 기존 redisTest/Testcontainers 격리를 재사용. 실제 점수순 조회 및 Pub/Sub 발행·수신 확인. 리스너 구독 준비 완료를 기다린 뒤 발행하고 고정 sleep 대신 제한 시간 내 조건 대기로 검증한다.
- 실제 HTTP SSE: 격리 환경 또는 사용자가 실행하는 로컬 앱에서 두 연결을 열어 초기 이벤트 수신 → 통제된 새 투표 → Kafka 소비/Redis 점수 증가/PubSub 수신/SSE 두 연결 갱신 확인. 동일 eventId 재전달은 점수/알림 추가 없음 확인. 재접속 시 최신 상태 확인. 실제 브로커/HTTP를 사용하지 않았다면 E2E 성공이라고 쓰지 않는다.
- 정상 기능의 실제 테스트는 미루지 않는 것을 권장한다. 다중 서버 실험은 다음 단계로 남길 수 있다. 한 서버의 두 SSE 연결 성공은 여러 서버 Pub/Sub 전파 검증이 아니다.
- 이번 범위에서는 고부하·Redis 강제 종료·자동 복구 실험을 하지 않는다. 사용자 DB/Redis를 초기화하지 말고 기존 Kafka 토픽/offset도 reset하지 않는다. 기존 Redis가 DB 전체와 동기화됐다고 가정하지 않는다.
- 외부 서비스 없는 일반 테스트가 새 리스너 자동 시작 때문에 Redis를 요구하지 않도록 테스트 구성을 격리한다. 실제 테스트는 별도 태스크/환경으로 구분한다.

## 결과 기록
- dev-notes/PROGRESS.md 상단 상태와 이력을 갱신하고 dev-notes/REDIS_PUBSUB_SSE_RESULT.md에 실제 실행 명령, 환경, 테스트 결과, SSE payload 예시, 관측한 eventId/점수 변화, 미실행 항목을 남긴다.
- 기존 기록: 2026-10-14 일반 테스트 46건 + 격리 실제 Redis 테스트 6건 통과. 이는 과거 결과이며 변경 후 결과로 재사용하지 않는다.
- 사용자 설명과 CLI 직접 관측을 구분하고 비밀값은 기록하지 않는다. 구현 후 다중 서버/부하/장애 복구로 자동 확장하지 않는다.

---

# 아래는 과거 인계 기록 — 현재 구현 지시 아님

# CLI 인계: Consumer 중단·재개 실험

최신 인계: 2026-10-14 14:57 KST. DB 기본 기능 및 Kafka 발행/수신 구현은 완료됐고, 사용자가 실제 브로커 송수신 확인도 보고했다. 아래의 첫 실습 예제 계획은 과거 학습 참고이며 현재 미구현 목록이 아니다.

현재 요청은 dev-notes/CONSUMER_STOP_RESUME_PLAN.md를 읽고 사용자와 로컬 Consumer 제어 기능 구현 및 실제 중단·재개 실험을 진행하는 것이다. 실제 코드를 다시 확인하고 사용자의 구현을 지원하되 요청받은 범위만 수정한다. 실험 후 dev-notes/CONSUMER_STOP_RESUME_RESULT.md와 dev-notes/PROGRESS.md에 실제 결과·로그·offset·lag·한계를 반드시 기록한다. 이 문서 갱신만으로 실험을 실행한 것은 아니다.

Kafka·Redis 학습은 사용자가 주도한다. 이번 리스너 제어 및 실험 범위를 넘어 자동 구현하지 않는다. AGENTS.md와 PROGRESS.md의 최신 코드/검증 상태도 함께 읽는다.

## 이전 Kafka 첫 실습 합의 참고 (현재 구현 상태는 PROGRESS.md 참조)
- 기존 투표 저장 및 DB 집계 조회는 유지. Consumer는 수신 로그만 남기며 Redis 집계나 이벤트 기록 테이블을 만들지 않는다.
- 흐름: VoteService.create의 검증/saveAndFlush → ApplicationEventPublisher로 Spring 내부 이벤트 발행 → DB 커밋 성공 → @TransactionalEventListener(AFTER_COMMIT) → Kafka Publisher → Kafka 토픽 → Consumer 수신 로그.
- 내부 이벤트 발행 시점은 트랜잭션 안이고, Kafka 발행 시점은 커밋 성공 후다. 롤백하면 해당 리스너는 실행되지 않는다. handle 메서드를 직접 호출하지 않는다.
- 처음에는 @Async 없이 진행. AFTER_COMMIT 자체는 별도 스레드 실행 기능이 아니다. KafkaTemplate.send의 Future 콜백으로 전송 결과를 관찰한다.
- 예제 이벤트: VoteCreatedEvent(String eventId, long voteId, long candidateId, Instant occurredAt). UUID eventId를 한 번 생성해 전 단계에서 유지하고 엔티티 대신 값을 전달한다.
- 예제 클래스: vote.event.VoteCreatedEvent, VoteCreatedEventListener 및 vote.messaging.VoteEventPublisher, VoteEventConsumer, VoteKafkaConfig. 실제 사용자 구현의 이름과 구조를 우선 확인한다.
- Publisher는 send(topic, voteId 문자열 key, event)를 호출하고 whenComplete에서 최종 성공/실패 로그를 남긴다. send 자체의 즉시 예외도 기록한다. 키는 중복 제거 기능이 아니다.
- 로그 이름: VOTE_COMMITTED, PUBLISH_REQUESTED, PUBLISH_SUCCEEDED, PUBLISH_FAILED, CONSUMER_RECEIVED. 공통 eventId/voteId, 가능한 곳에 topic/partition/offset을 기록. DB 로그 테이블은 불필요.
- 발행 성공은 Kafka의 수신 확인이며 Consumer 수신·업무 완료가 아니다. 수신 로그는 리스너 진입 확인이다. Consumer 집계 성공 로그는 실제 집계 구현 이후 분리한다.
- Kafka 발행 실패는 이번 실습에서 로그로 관찰하고 DB 투표 성공을 되돌리지 않는다. AFTER_COMMIT 이후 예외를 HTTP 오류로 전파하면 이미 저장된 투표에 오류 응답을 줄 수 있음을 고려한다.
- 예제 토픽 vote-created, 리스너 id vote-log-listener, groupId vote-log-group. 최초 로컬 구성은 브로커 한 대, 파티션 하나, 복제 계수 하나.
- 리스너 id는 Spring 컨테이너 식별자, groupId는 Kafka 소비 그룹. 기본 설정에서 앱 시작 시 컨테이너가 등록·시작된다. 토픽 생성은 NewTopic 설정의 별도 역할이다.
- 파티션은 토픽의 순서 있는 로그, offset은 해당 파티션 내 위치. 소비했다고 이벤트가 즉시 삭제되는 것은 아니다. 같은 파티션의 복제본은 서로 다른 브로커에 배치하며 replicas는 브로커 실행 개수가 아니다.
- 제시한 Boot 4 예제는 JacksonJsonSerializer/JacksonJsonDeserializer 사용. trusted.packages는 실제 이벤트 패키지로 한정. 설정은 YAML에 합쳐서 작성하며 spring 키를 중복 선언하지 않는다.
- Producer 실습 설정 제안: acks=all, enable.idempotence=true, delivery.timeout.ms=30000, request.timeout.ms=5000, retry.backoff.ms=500, retry.backoff.max.ms=1000, max.block.ms=3000. retries는 기본값 유지. 제안일 뿐 적용·검증 완료된 설정이 아니다.
- 내부 재시도는 일시적인 Producer→브로커 전송 실패 대상이다. whenComplete는 매 재시도 로그가 아니라 최종 결과를 받는다. 비복구 오류나 send 단계 실패는 더 빨리 발생할 수 있고 send도 메타데이터/버퍼 때문에 잠시 블록될 수 있다.
- Producer 멱등성은 내부 재시도 중복 방지이며 앱의 임의 재발행/Consumer 중복 처리까지 해결하지 않는다. 타임아웃은 브로커에 절대로 저장되지 않았다는 증거가 아니다.
- Consumer 설정 제안: auto-offset-reset=earliest, enable-auto-commit=false, listener.ack-mode=record. earliest는 유효한 그룹 offset이 없을 때의 정책이고 매 재시작마다 전체 재생하는 설정은 아니다.
- 한계: DB 커밋 후 발행 전 프로세스 종료, 최종 발행 실패 시 이벤트 누락 가능. Outbox/영속 재발행/Consumer 멱등성은 후속 학습 주제이며 선행 구현하지 않는다.

## 사용자와 확인한 JPA 및 코드 스타일
- 현재 활성 트랜잭션과 IDENTITY 전략에서는 save도 ID 확보를 위해 INSERT를 실행하므로 해당 INSERT의 유니크 위반을 잡을 수 있다. ID 사용은 MapStruct 덕분이 아니다.
- SEQUENCE는 ID를 먼저 확보하고 INSERT는 미룰 수 있다. flush는 DB SQL 반영이며 commit과 다르다. flush 후에도 rollback 가능하고 전체 영속성 컨텍스트의 다른 변경까지 반영할 수 있다.
- 사용자는 이 지점에서 DB 반영 의도를 명시하기 위해 saveAndFlush를 유지하기로 결정했다. 항상 더 안전하거나 현재 IDENTITY에서 예외 포착에 필수라고 설명하지 않는다.
- 학습 예제에서는 var보다 Candidate, Vote, VoteCreatedEvent 등 명시적 타입을 사용한다. 기존 var를 요청 없이 일괄 변경하지 않는다.

## 사용자가 직접 진행할 구현 및 확인 순서
1. Kafka 브로커 실행 환경 준비 여부 확인 (이번 문서 작업에서는 실행 상태를 확인하지 않음).
2. 이벤트 record → 서비스 내부 이벤트 발행 → 커밋 후 리스너 → Publisher → Consumer 및 YAML/토픽 설정 작성.
3. 정상 투표 시 같은 eventId의 커밋/발행 성공/Consumer 수신 로그 확인. 서로 다른 스레드 로그 출력 순서는 일부 달라질 수 있음.
4. DB 롤백 시 Kafka 발행이 실행되지 않는지 확인.
5. API/Producer를 유지하고 Consumer 리스너만 중단: 발행 성공·수신 없음. 같은 그룹 재개 후 미소비 이벤트 수신 확인 (보관 데이터 유지).
6. 정상 전송으로 연결 확인 후 브로커 짧은 중단/복구로 재시도 관찰, 긴 중단으로 최종 발행 실패와 DB 기록 잔존 확인. 매 투표마다 새 테스트 userId 사용.
7. 실제 실행한 검증만 결과와 환경을 기록. 기존 DB 테스트를 외부 Kafka 의존 때문에 무조건 실패시키지 않도록 테스트 격리 방식을 논의하되 필요 코드는 사용자 요청 범위에서 지원.

## 기록 방법
사용자 구현을 읽거나 요청받은 검증을 수행한 뒤 PROGRESS.md에 실제 구현·검증 결과와 남은 작업을 구분하여 기록하세요. 오류 로그, 발행 성공, Consumer 수신은 별도 증거로 취급하세요. Redis·Pub/Sub·SSE 및 영속 재발행은 자동 구현하지 않습니다.
