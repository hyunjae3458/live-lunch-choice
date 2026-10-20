# 진행상황

최종 기록 갱신: 2026-10-20 KST. 프로젝트 소개·설계·실험·회고 README 작성.

## 현재 단계
- 2026-10-20 문서 분리: README와 docs/의 공개 설명4개로 재구성. 과거 docs 기록은 dev-notes/로 이동하고 Git 관리 대상에 포함. 커밋·push 미수행.
**2026-10-20 18:45 KST 최신: 사용자 최신 지시에 따라 부하테스트 도구·계측 코드 삭제 완료. DB + SSE 커밋 681c72c 유지. 후속 부하 실험 자동 진행하지 않음.**

아래 후속 실험 인계는 삭제 전 이력이다.
**최신 요청: 실제 투표 전체 경로 부하와 Kafka 역할 검증 진행.** LOAD_TEST_EVENT_PIPELINE_HANDOFF.md에 실행 순서/측정/중단 범위를 기록했다. 이번 데스크톱 턴은 문서 인계만 수행했으며 새 실험은 미실행이다. 아래 resume1은 기존 조회 실험 결과다.

- 2026-10-20 18:38 KST: DB + SSE 구현 14개 파일만 681c72c 커밋 및 origin/main 푸시 완료. 부하 도구·계측 변경은 로컬 미커밋 보존. 분리된 커밋 내용으로 일반83건 및 bootJar 성공.
- 2026-10-19 18:50 KST: 사용자 요청으로 구 polling/realtime 부하 비교 실행 도구·설정 8개와 관련 Python 캐시 정리. 결과 Markdown·측정 원본·당시 스냅샷 보존, capacity 도구와 서비스 polling 기능 유지.
**resume1 소부하2회+조회12회 실행.** 발생률/지연 경고만으로 멈추지 않고1만/10만/100만 건 비교를 진행했다. 100만 건 A50req/s에서15초간 성공응답 없음, 측정1205발행 중10성공으로 중단. PG2CPU 소진·throttling509/514기간, pool active10/대기최대190·30초 획득타임아웃 확인. 같은 조건 B는1500/1500성공/p95 5.76ms. 이는 DB 재집계 vs Redis 사전집계 조회 진단이며 Kafka 자체 효과/본 투표·SSE 용량 결론은 아님.

- 테스트: 계측 보완 후 일반83건 통과, 실제 양쪽14표/2SSE 소부하 재검증 통과. redisTest9건은 이번 재개에서 재실행하지 않음.
- 보완: Hikari 획득시간/cgroup CPU 계측, 지속 AdminClient 양쪽 적용, 경고와critical 분리. 사후 CLI 전에 자원샘플러 종료하도록 마지막 보완(수정 후 부하 재측정 미실행).
- 남은 작업: 본 경로100SSE/10→200투표req/s의30초+180초 비교/경계3회 반복/300·500관람자 실험. 조회 중 심각한 응답 불능으로 미실행.
- 실험 앱/전용 컨테이너3개 중지,DB dump·데이터·볼륨·원본 보존. 기존8080/5432/9092/6379 및 .env 유지. 상세 LOAD_TEST_CAPACITY_RESULT 하단 resume1.

<details><summary>폐기한 비교의 이전 완료 상태 (이력)</summary>

최종 기록 갱신: 2026-10-19 17:03 KST. 마지막 100명·10req/s realtime 3회 완료. 단계별 비교 결과 정리 후 실행 종료.

## 현재 단계
**50명/1req/s, 100명/1·5·10req/s 양쪽 모드 각 3회 완료.** 위24회에서 측정 투표6120/6120건 성공, 매회 최종 SQL/관람자 일치 및 realtime Redis 일치·lag0·전원재접속 확인. 마지막 realtime10req/s는1800/1800성공,각1604표. 5req/s polling1회차와10req/s realtime1회차는 호스트/발생기 경고를 보존했다. 최초10명60초유효3회는 사용자 다음단계 진행 요청으로 미완료이며 비교 공백으로 명시. 상세 반복표/자원/lag/관측 지연/한계는 LOAD_TEST_SCALE_RESULT.md의 최종 요약 참조. 추가 부하 실행하지 않음. 실험 앱18081과 전용 컨테이너 중지, 실험 데이터/원본 보존. 실제5432/9092/6379/8080 서비스와 데이터 유지.


</details>

## 코드에서 확인한 구현과 HTTP 계약
- 메뉴 카드 제목 옆 후보 추가 버튼으로 여는 입력 패널 제공. 기존 POST /api/candidates 사용, 전송 잠금·공백/중복/실패 안내. 성공 응답을 선택 목록과 0표 랭킹에 반영하고 기존 선택 유지. 다른 탭의 신규 0표 후보는 새로고침 필요.
- GET /: static/index.html, styles.css, app.js 공통 화면. 후보 선택·테스트 사용자 ID·투표 상태·득표순 랭킹/비율·연결 상태·마지막 갱신 시각. 반응형 카드와 키보드 입력 지원, 서버 문자열은 textContent로 표시.
- GET /api/mode는 mode 한 필드(polling/realtime/db-sse)만 반환, no-store. 전체 프로필/환경값 미노출.
- polling은 /api/votes/results 응답 완료 후 POLLING_INTERVAL_MS=2000으로 다음 요청 예약. realtime은 상대 경로 EventSource ranking 이벤트의 전체 스냅샷을 교체. 투표 성공으로 임의 +1 하지 않음. 후보 API와 결합하고 미집계 후보0표는 뒤에 추가.
- application.yml의 spring.profiles.active=${APP_PROFILE:realtime}. .env 또는 실행 환경변수로 polling/realtime 선택, 미설정 시 realtime. 명시적 local만/두 모드 동시 지정은 기존 검사로 거절. IntelliJ Active profiles 또는 spring.profiles.active 명시 설정이 있으면 APP_PROFILE보다 우선하므로 충돌 설정을 비워 둘 것.
- Kafka·Redis·SSE 관련 Bean은 realtime 전용. polling은 실제 Boot4 자동 구성까지 제외, 실시간 API404. polling Spring 내부 이벤트는 유지하되 Kafka 외부 발행 없음. polling 데이터의 Redis 자동 backfill 없음.
- VoteService는 공통 create/results 및 DB 의존성만 유지. rankings/aggregate는 realtime 전용 VoteRealtimeService로 이동. DTO는 VoteResultResponse 통합 유지. 이번 프로필 경계를 위해 과거 서비스 통합 결정을 변경함.
- DB·Redis·SSE 결과 DTO는 VoteResultResponse로 통일. name은 DB 결과에 포함, Redis/SSE에서는 null로 생성하고 JSON에서 생략. VoteRankingResponse 삭제. Redis 경로의 DB 조회나 재정렬 추가 없음.
- GET /api/votes/rankings: candidateId/voteCount List, Redis reverseRangeWithScores 순서 유지, 빈 목록 [], DB 조회/Java 재정렬 없음. 기존 DB results API 유지.
- GET /api/votes/rankings/stream: ranking 이벤트/JSON 전체 배열, 초기 및 재접속 최신 전송, SseEmitter60초. 동시 연결 Set, 초기/변경 조회·전송 직렬화와 종료/오류/타임아웃 정리.
- VoteRealtimeService 집계 true에서만 VoteRankingPublisher가 live-lunch-choice:ranking-changed에 eventId 발행. Subscriber/RedisMessageListenerContainer가 구독하고 한 번 조회한 최신 목록을 로컬 SSE 연결들에 전송.
- 이름/0표 후보는 프론트가 후보 API와 결합, Redis 동점 역 사전식 순서 유지. 전체 스냅샷을 교체하며 이전 표 수에 더하지 않는 계약을 결과 문서에 기록.
- VoteRankingRedis가 redis/aggregate-vote.lua를 실행. ranking Sorted Set과 processed-events Set, eventId/candidateId 전달. 신규 true/중복 false, null 결과는 예외. VoteRealtimeService가 위임하고 Consumer는 VOTE_AGGREGATED/VOTE_DUPLICATE_SKIPPED 분기, 실패는 컨테이너에 전파.
- build.gradle에 Testcontainers 테스트 의존성과 redisTest 태스크 추가. 일반 test는 redis 태그 제외로 H2/외부 서비스 없는 기존 테스트 유지. redisTest는 Docker 필수이며 별도 임시 Redis와 임의 포트로 격리; 앱 Redis/DB/Kafka에 쓰지 않음.
- 실험용 LocalConsumerController 복원: local & realtime에서 GET /api/local/kafka/consumer 상태 조회, POST /stop·/start 제어 제공. stop은 종료 콜백 대기, start는 시작 요청 완료 의미. 전용 테스트와 polling 미등록 검증 복원.
- vote/event: VoteCreateEvent, VotedCreatedEventListener. vote/messaging: VoteEventPublisher, VoteEventConsumer, VoteKafkaConfig. 직렬화 테스트도 vote/event로 이동. 클래스 이름과 업무 동작은 유지.
- 모든 Controller는 ResponseEntity, Service는 DTO 반환. DTO는 record, 엔티티는 일반 클래스 유지.
- POST /api/candidates: name 요청 → 201과 id/name. 이름을 DTO 생성 시 strip한 뒤 같은 값으로 검증·중복 확인·저장. 내부 공백 유지.
- GET /api/candidates: 200, ID 오름차순의 id/name 목록. 없으면 [].
- Candidate: 기존 필드·IDENTITY·protected 기본 생성자 유지. 이름의 NOT NULL·유니크 의미를 유지하면서 유니크를 명시적 이름 uk_candidate_name으로 변경(@Table의 UniqueConstraint). 기존 unique=true는 이중 제약 생성을 피하도록 제거.
- POST /api/votes: String userId, Long candidateId 요청 → 201과 id/userId/candidateId/createdAt(Instant) 반환. userId는 strip 후 NotBlank, candidateId는 NotNull/Positive 검증. 후보 존재 여부 확인 후 저장.
- 후보명과 userId는 strip 후 최대 255자로 검증하여 기본 DB 문자열 길이 초과를 400 처리. 앞뒤 공백 제거는 record의 compact constructor에서 수행.
- Vote: id, userId, Candidate 연관관계, createdAt. user_id NOT NULL 및 uk_vote_user_id 유니크, candidate_id NOT NULL 및 fk_vote_candidate 외래 키, createdAt NOT NULL. 사용자당 한 표, 변경·취소 없음.
- 투표·후보 서비스는 사전 중복 검사에 더해 saveAndFlush로 저장 제약 위반을 서비스 내에서 처리. SQLState 23505 및 알려진 제약 이름을 확인할 때만 도메인 중복 예외로 변환. 실패 트랜잭션에서 추가 쿼리 없이 RuntimeException을 던져 롤백.
- GET /api/votes/results: 200과 candidateId/name/voteCount 목록. LEFT JOIN + GROUP BY + COUNT DB 집계 쿼리 한 번 사용. 득표 수 내림차순, 동점 후보 ID 오름차순. 0표 후보 포함, 후보 없으면 []. 전체 투표 로딩이나 후보별 반복 쿼리 없음.
- VoteRepository.findResults는 VoteResultProjection 인터페이스 목록 반환. 쿼리 별칭(candidateId/name/voteCount)을 projection 접근자에 연결하고 VoteService.results에서 VoteResultResponse로 변환. JPQL의 select new DTO 생성자 표현식 제거.
- @RestControllerAdvice 및 ErrorResponse(code, message): 잘못된 요청/검증 실패/잘못된 JSON 400 INVALID_REQUEST, 후보 없음 404 CANDIDATE_NOT_FOUND, 후보 중복 409 DUPLICATE_CANDIDATE, 재투표 409 ALREADY_VOTED.
- ResponseEntityExceptionHandler를 확장해 프레임워크 오류의 상태·헤더를 보존. 미분류 예외/알 수 없는 무결성 위반은 500 INTERNAL_ERROR로 처리하고 상세 원인은 서버 로그에만 기록. SQL·스택을 응답에 포함하지 않음.
- 메인 설정은 PostgreSQL/ddl-auto=update. 실행 디렉터리의 .env를 properties 형식으로 읽어 DB_HOST/DB_PORT/DB_NAME/DB_USERNAME/DB_PASSWORD 적용. .env는 Git 제외·권한 0600, .env.example에는 예시만 포함. 테스트는 src/test/resources/application.yml의 무작위 H2/create-drop 유지, H2는 testRuntimeOnly. 별도 테스트 프로필 불필요. H2 콘솔 런타임 의존성 제거.

## 실제 검증과 한계
- 2026-10-19 polling 예비: 관람자10/투표1req/s/30초, 성공30/실패0, 초기1000→1030. SQL 및10관람자 최종408/308/207/107 일치. API p95투표23.35ms/조회16.97ms, 누적 목표치 관측p95 1198.94ms. 개별 이벤트 지연 또는 대규모 용량으로 해석하지 않음.
- 두 전용 PostgreSQL DB 동일 기준400/300/200/100, Redis 동일 기준 직접시드 확인. realtime 부하는 사용자 전환 확인 전 미실행. 기존DB/인프라 초기화 없음, 공유 PG/기존앱 자원 경쟁 한계 있음.
- ./gradlew bootJar 성공, Python 문법/diff 검사 통과. 준비/측정 스크립트 초기 오류와 수정은 LOAD_TEST_PILOT_RESULT에 기록. 최종 수렴 시간은 원본 snapshot으로 재계산. 전체 JUnit/정리 도구 실제삭제/대규모 시험 미실행.
- 최신 ./gradlew test bootJar 성공: 일반76건(모드 API4+환경설정4 추가), 실패·오류·건너뜀0. 실제 메인 YAML의 APP_PROFILE 기본 realtime/선택 polling/realtime/local,realtime 검증. node --check 및 git diff --check 통과.
- 화면 구현 시 ./gradlew test redisTest bootJar:72+9=81건 통과. 이후 환경변수 선택 추가 후 실제 통합9건은 재실행하지 않음.
- 격리 H2 두 앱(18080 polling/18081 realtime), 임시 Testcontainers Kafka/Redis, Chrome 실제 화면에서 후보 없음·전체0표·투표 성공·polling 중복 오류·두 탭 polling/SSE 갱신·키보드 투표·390px 모바일/1440px 데스크톱 확인. HTML 형태의 후보명도 문자 그대로 표시. 기존 사용자 DB/Redis/Kafka 변경 없음.
- 사용자 직접 확인 요청으로 추가 검증 중단. 브라우저 SSE 자동 재연결·연결 오류 복구·느린 전송 중 연타는 미검증. Consumer 중단으로 낙관적 갱신 부재 확인 시도는 도구 호출 중 사용자 중단되어 검증 완료로 간주하지 않음. 임시 앱 종료0, 임시 컨테이너 정리. 상세 FRONTEND_RESULT.md.
- 복원 후 ./gradlew test 성공:68건, 실패·오류·건너뜀0. 제어 API 동작 및 local & realtime 프로필 조건 검증. 실제 Kafka/Redis 재실험·bootJar 재빌드는 이번에 미실행.
- Consumer 제어 API 제거 후 ./gradlew test 성공:57건, 실패·오류·건너뜀0. polling 실제 HTTP에서 제거한 경로404 확인. 이번에는 실제 Kafka/Redis 통합 테스트와 실행 JAR 재빌드는 미실행. 아래77건은 프로필 분리 당시 결과.
- 최신 ./gradlew test redisTest bootJar 성공:68+9=77건 실패·오류·건너뜀0. polling 실제 HTTP 후보/투표/DB 집계 정상, 외부 Kafka/Redis 연결 감지0건, 실시간 Bean 부재/API404. realtime 실제 Kafka·Redis·Pub/Sub·SSE/중복/재접속 회귀 성공.
- 모드 누락/충돌 단위 검사 및 실제 JAR(local만/두 모드) 시작 거절 확인. 이번 데이터 계층 검증은 격리 H2이며 PostgreSQL 모드 전환은 미실행. 자세한 원본과 실행 명령은 PROFILE_SEPARATION_RESULT.md 참조.
- 2026-10-16 다중 서버 실험: Stable 멤버2/파티션1, A할당1·B할당0. A요청 voteId6/offset13 및 B요청 voteId7/offset14 모두 A집계1회씩, Pub/Sub A/B 수신, 각 포트 SSE4→5→6. PostgreSQL 저장 SELECT 확인, DB/Redis+2. 그룹13/13/0→15/15/0.
- 앱 PID18035/18037 종료,8080/8081 리스너 없음. 인프라와 기존 구독 유지. 첫 구독 준비 timeout은 실험 스크립트의 기준 수 계산 보완 후 해결, 첫 시도 투표0건. 제품 코드 변경·부하 테스트 없음.
- 2026-10-16 로컬 직접 시연: PostgreSQL 실데이터에 일식 투표1건 추가(HTTP201/SQL voteId5). 같은 eventId의 Kafka offset12 소비, Redis3→4, 실제 Pub/Sub 수신, 두 SSE 연결 모두 새 목록 수신. 그룹12/12/0→13/13/0. 원본은 REDIS_PUBSUB_SSE_RESULT.md 하단.
- 사용자 요청으로 CLI가 실행한 local 앱 PID16676 종료 완료, 8080 리스너 없음 확인. 로그 /tmp/lunch-live-sse.log. ranking-e2e는 통합 테스트 전용 프로필이며 로컬 실행용 아님.
- 2026-10-16 최종 ./gradlew test redisTest bootJar 성공. 일반54건/실제 통합9건, 총63건 실패·오류·건너뜀0. git diff --check 통과.
- 실제 격리 Kafka/Redis/H2/HTTP E2E: 두 SSE 초기[] → HTTP 후보/투표201 → offset0 집계/발행·구독 → 두 연결 후보1 점수1. 동일 eventId offset1 재전달 후 committed2, 점수1 유지/추가 알림 없음. 수동 재접속 최신1표.
- PostgreSQL 결합 E2E·브라우저 EventSource 자동 재연결·다중 서버·부하·장애 복구는 미검증/제외. 과거 아래 기록은 당시 검증 범위이며 현재 결과는 REDIS_PUBSUB_SSE_RESULT.md 참조.
- 2026-10-14 ./gradlew test bootJar 성공: 총 43건(기존 37 + 제어 테스트 6), 실패/오류/건너뜀 0. git diff --check 통과. 프로필 제한·종료 콜백·시작·중복 호출·상태·미존재 오류 검증.
- 실제 실험: apache/kafka:4.1.2 lunch-kafka, vote-created 파티션 0 하나/복제 계수 1, vote-log-group. 기준 CURRENT/END/LAG=5/5/0 → Consumer 중단 및 3건 발행 후 5/8/3 → 재개 후 8/8/0. HTTP 3건 모두 201, H2 SELECT로 해당 voteId 2/3/4 저장 확인. 재개 후 동일 eventId가 offset 5→6→7 순서로 각 한 번 수신됨.
- 실제 로그 미수신 구간과 eventId·HTTP 원본·그룹 출력·SQL 결과는 결과 문서에 기록. 별도 콘솔 Consumer로 offset 5~7을 재조회하여 소비 후에도 메시지가 보관됨을 확인. 끝에는 리스너 running=true, 같은 그룹 구성원 1명.
- 기존 앱 재시작 시 H2 데이터 소멸을 알리고 사용자 명시 승인을 받은 뒤 PID 38829만 정상 종료. local 앱 PID 41406을 127.0.0.1:8080으로 실행. 이후 실험 중 앱 재시작 없음. 토픽 삭제/offset reset/DB 삭제 SQL/타 프로젝트 PostgreSQL 조작 없음.
- 현재 실험은 정상 중단·재개 및 로그 Consumer 조건이다. 브로커 장애·강제 종료·업무 집계·중복 처리·영속 발행 복구·다중 파티션·PostgreSQL은 미검증. 과거 아래 항목의 브로커 미검증은 당시 기준이며 이번 직접 검증 범위는 위 결과로 갱신됨.
- 사용자 확인: Postman 생성 정상 및 파티션 구성·Consumer 소비 위치 조회 확인 완료. 원본 출력/발행·수신 로그는 공유되지 않았으므로 실제 offset·lag·eventId별 송수신은 CLI가 독립 검증한 것으로 기록하지 않음. 재사용 명령과 해석은 [Kafka 명령어 정리](KAFKA_COMMANDS.md) 참고.
- 2026-10-13 23:27 패키지 이동 후 ./gradlew test 성공: 총 37건(기존 34건 + 커밋 후 발행/롤백 미발행 2건 + 이벤트 직렬화 왕복 1건), 실패/오류/건너뜀 0. git diff --check 통과. src 내 이전 common.event/common.config 참조 없음 확인.
- application.yml의 spring.kafka 경로는 사용자 수정 유지. trusted.packages는 이동한 실제 이벤트의 org.example.livelunchchoice.vote.event로 수정.
- 기존 common.event.VoteCreateEvent 타입 헤더로 이미 발행한 메시지가 있다면 새 패키지에서 그대로 역직렬화할 수 없음. 기존 브로커 데이터 유무는 미확인이고 삭제하지 않음. 필요 시 이전 타입 ID 매핑을 별도 검토하며 이번 테스트는 새 패키지 메시지 왕복만 검증.
- DB 테스트는 VoteEventPublisher를 MockitoBean으로 대체하고 테스트 YAML에서 admin.auto-create=false, listener.auto-startup=false 지정. 외부 Kafka 없이 기존 H2 검증 수행.
- TransactionTemplate으로 실제 투표 저장 트랜잭션을 묶어 커밋 전 Publisher 호출 없음, 커밋 후 한 번 호출 및 이벤트 식별 정보 확인. rollback-only이면 발행/저장 없음 확인. 이 검증에서 Publisher는 mock이므로 브로커 전송 성공을 의미하지 않음.
- 실제 메인 YAML의 serializer/deserializer 경로와 trusted.packages로 타입 헤더 포함 직렬화→역직렬화 왕복 테스트 통과. VoteCreateEvent/Instant 복원 확인. 브로커·Consumer 리스너 실제 송수신 및 장애 실험은 미실행.
- 17:00 ./gradlew build 성공: 실행 JAR 빌드 및 테스트 34건 통과(후보 7, 투표·API 24, 예외 처리 2, contextLoads 1; 실패/오류/건너뜀 0).
- 17:16 projection 전환 후 ./gradlew test --tests org.example.livelunchchoice.vote.VoteApiTests 성공: 24건 통과(실패/오류/건너뜀 0), 집계·정렬·0표·빈 목록 응답 유지 확인. git diff --check 통과. 이번 변경 후 전체 빌드는 재실행하지 않음.
- 기존 후보 등록·목록·이름 오류 검증 유지. 후보명 strip 및 내부 공백 보존, 정규화 후 중복 409, 255/256자 경계 검증.
- 정상 투표 201/저장 값, 잘못된 요청 10종의 400/미저장, 후보 없음 404/미저장, 같은 사용자의 다른 후보 재투표 409/한 건 유지 검증.
- 동시 투표 및 동시 후보 등록: Repository spy가 실제 사전 검사에 위임한 뒤 barrier로 두 요청의 저장 시점을 맞춤. 실제 MockMvc 요청 두 개/서비스 트랜잭션/H2 제약조건을 사용하여 각각 201 한 건·409 한 건·최종 저장 한 건 확인. H2 SQLState 23505 발생과 도메인 응답 변환 확인. DB 저장을 mock으로 대체하지 않음.
- 집계: 정확한 득표 수, 내림차순, 동점 ID 오름차순, 0표 후보 포함, 모든 후보 0표, 후보 없음 검증.
- 잘못된 JSON의 공통 형식, 405 및 Allow 헤더 유지, 알려진 유니크/다른 제약조건 구분, 미분류 무결성 오류의 500 및 내부 정보 비노출 검증.
- 검증 범위는 MockMvc + 실제 JPA/H2. 실제 HTTP 포트 및 PostgreSQL SQL·제약조건·잠금·동시성 검증은 하지 않음. PostgreSQL 제약 이름 형식 분류 단위 테스트는 실제 PostgreSQL 검증을 대신하지 않음.
- 환경 확인: psql/pg_isready 실행 파일 없음. Docker CLI는 있으나 docker ps가 docker.sock 없음으로 실패하여 실행 중인 Docker PostgreSQL 환경을 사용할 수 없었음. 사용자 DB 접속·삭제·초기화 및 별도 서버 설치/기동은 하지 않음.
- PostgreSQL 후속 검증: 별도 폐기 가능한 테스트 DB를 준비하고 src/test/resources/application-postgres-test.yml 등 격리 설정으로 연결한 뒤 같은 API·동시성 테스트 실행. create-drop은 해당 테스트 DB에서만 사용. 기존 영속 DB에 적용할 때 명명된 유니크 제약조건 반영/마이그레이션 확인 필요.

## 합의와 구현 선택
- 터미널 명령은 항상 한 줄로 안내. Kafka 파티션·소비 위치 확인 명령과 주요 항목은 dev-notes/KAFKA_COMMANDS.md에 보존.
- 투표 전용 이벤트와 Kafka 코드는 vote 도메인에 배치. 여러 도메인에서 공유하는 설정만 common/config 대상으로 구분.
- 사용자 요청: 집계 쿼리에서 응답 DTO를 직접 생성하지 않고 projection 조회 후 서비스에서 응답 DTO 조립. 현재 변환은 짧아 별도 mapper 클래스 없이 서비스에서 수행.
- 이번 DB 기본 기능은 CLI에 구현·검증 위임됨. Kafka·Redis는 사용자가 원리와 설계를 배우며 직접 구현할 영역이며 별도 요청 없이 수정하지 않음.
- 익숙한 Controller → Service → Repository, 단일 앱, record DTO, ResponseEntity Controller 유지. 불필요한 의존성·미래 단계 추상화 추가 없음.
- 오류 상태·code/message 및 앞뒤 strip 정책은 최신 CLI_HANDOFF 기본값 적용. 같은 사용자와 없는 후보가 함께 입력되면 후보 존재 검사를 먼저 하므로 404 반환.
- userId는 인증된 신원이 아닌 테스트용 문자열. 대소문자를 변환하지 않으며 strip 후 동일 값만 중복으로 간주.
- README 제외 Markdown은 Git에서 무시하고 로컬에 보존. 설명과 진행 기록은 한국어로 작성.

## 다음 작업과 미결 사항
1. 사용자에게 polling 결과 공유 후 .env.loadtest APP_PROFILE=realtime 변경 완료 확인 대기. 기존 .env/8080 앱을 바꿀 필요 없음.
2. 확인 후 전용18080 앱을 realtime DB로 시작, 동일30투표/10관람자·DB/Redis 최종값·Kafka lag·SSE 재연결 검증.
3. 결과를 합친 뒤 대규모 조건 확정. 관람자10→50→100, 이후 쓰기1→5→10req/s의 단계안만 제안했으며 아직 실행하지 않음.
4. 전용 DB/컨테이너/원본 결과는 현재 보존. 정리는 앱 종료와 실험ID·소유마커 확인 후 별도 도구로만 수행. 종료/초기화 서비스·후보 변경 SSE는 이번에 추가하지 않음.

## 작업 이력
### 2026-10-12 — 초기 인계
- 데스크톱에서 candidate 패키지의 소스와 git 상태 확인.
- 이전 Controller/검증/테스트 추가 시도는 파일 쓰기 권한 요청 중 중단됐으며 변경 미반영을 재확인.
- 사용자가 응답 DTO 필드를 final로 변경한 상태를 확인. 중복 생성자 가능성 기록.
- 실행한 확인: 파일 읽기, git status --short. 빌드와 테스트는 미실행.
- 사용자 기존 수정과 미추적 파일 있음. 초기화·덮어쓰기 금지.

### 2026-10-12 16:30 KST — 후보 등록 Controller와 요청 검증
- 실제 코드와 초기 기록 대조: CandidateResponse는 이미 RequiredArgsConstructor만 적용되어 있었음. AllArgsConstructor import만 제거하고 일반 클래스/final 필드 유지.
- CandidateController에 POST /api/candidates, 생성자 주입, @Valid @RequestBody, 201 응답 추가. CandidateRequest.name에 @NotBlank 추가.
- 관련 파일: candidate/CandidateController.java, candidate/dto/CandidateRequest.java, candidate/dto/CandidateResponse.java, src/test/java/org/example/livelunchchoice/candidate/CandidateControllerTests.java, src/test/resources/application.yml.
- 검증: ./gradlew test → BUILD SUCCESSFUL. 후보 API 5건 + 기존 contextLoads 1건, 총 6건 통과(실패/오류/건너뜀 0). 정상 등록의 저장 건수·이름·응답 ID와 저장 ID 일치 확인. 잘못된 이름 4종은 각각 400 및 저장 건수 0 확인.
- 실행 로그에서 두 테스트 컨텍스트 모두 candidate_test_ 접두사의 별도 H2 메모리 DB 사용 확인. 테스트 데이터 정리는 해당 DB 안에서만 수행. git diff --check 통과.
- 기존 build.gradle, 메인 application.yml, 엔티티와 서비스의 사용자 변경 보존. 중복 오류 처리·조회·투표·Kafka·Redis는 미구현.
- 남은 문제/다음 단계: 위 검증 한계를 유지하고 중복 응답 및 공백 정책을 먼저 논의한 뒤 사용자가 요청한 다음 작업 진행.

### 2026-10-12 16:31 KST — 로컬 Markdown 문서 Git 제외
- .gitignore에 Markdown 무시 및 README 예외 규칙 추가. 문서 파일은 로컬에 보존.
- 검증: git ls-files '*.md' 결과 추적 중인 Markdown 없음. git check-ignore -v로 AGENTS.md, dev-notes/PROGRESS.md, dev-notes/CLI_HANDOFF.md 무시 및 README.md, dev-notes/readme.md 예외 규칙 확인. git diff --check 통과.
- 애플리케이션 변경 없음으로 테스트 재실행하지 않음. 남은 문제와 다음 구현 단계는 기존 기록과 동일.

### 2026-10-12 16:33 KST — 후보 DTO record 전환
- 사용자 요청으로 CandidateRequest와 CandidateResponse를 record로 전환하고 DTO의 Lombok import/어노테이션 제거. 요청 컴포넌트에 @NotBlank 유지.
- CandidateService의 request.getName() 두 곳을 request.name()으로 변경. 엔티티와 Controller는 변경하지 않음.
- 검증: 최초 ./gradlew test는 샌드박스의 Gradle 캐시 잠금 파일 접근 제한으로 실행 실패. 권한 확장 재실행에서 BUILD SUCCESSFUL, 총 6건 통과(실패/오류/건너뜀 0). JSON 역직렬화·응답 필드·정상 저장·잘못된 이름 4종의 400/미저장 확인. git diff --check 통과.
- 남은 문제: 중복 오류 정책 및 PostgreSQL 검증 한계는 기존과 동일. 다음 단계는 사용자의 변경 코드 확인 후 기존 미결 정책 논의.

### 2026-10-12 16:39 KST — 후보 목록 조회
- CandidateController에 GET /api/candidates 추가. CandidateService.findAll에서 ID 오름차순으로 조회한 엔티티를 기존 CandidateResponse record 목록으로 변환.
- Spring @Transactional(readOnly = true)로 조회 트랜잭션 지정. Repository의 기존 JpaRepository 기능을 사용하며 엔티티/DTO 변경 없음.
- 관련 파일: src/main/java/org/example/livelunchchoice/candidate/CandidateController.java, CandidateService.java, src/test/java/org/example/livelunchchoice/candidate/CandidateControllerTests.java.
- 검증: ./gradlew test → BUILD SUCCESSFUL. 격리된 H2에서 빈 목록의 200/[], 후보 2건의 200/ID·이름·ID 오름차순 검증. 기존 등록/검증 및 contextLoads 포함 총 8건 통과(실패/오류/건너뜀 0). git diff --check 통과.
- 남은 문제: PostgreSQL 검증과 중복 오류 정책은 기존과 동일. 다음 단계는 중복 응답·공백 정책 논의이며 투표 등 후속 기능은 구현하지 않음.

### 2026-10-12 16:41 KST — ResponseEntity 응답 통일
- CandidateController의 등록·목록 조회를 각각 ResponseEntity<CandidateResponse>, ResponseEntity<List<CandidateResponse>> 반환으로 변경. 등록의 @ResponseStatus와 import 제거.
- 기존 서비스/DTO/테스트 및 앞선 목록 조회 변경 보존. HTTP 상태 코드(등록 201, 조회 200, 잘못된 이름 400)와 JSON 본문 유지.
- 검증: ./gradlew test → BUILD SUCCESSFUL, 기존 8건 모두 통과(실패/오류/건너뜀 0). git diff --check 통과.
- 남은 문제와 다음 단계: 중복 오류·공백 정책 논의 및 PostgreSQL 미검증 사항은 기존과 동일.

### 2026-10-12 16:52 KST — CLI 위임 범위 확장과 학습 경계 설정
- 사용자 요청: Kafka·Redis 이전의 기본 기능은 CLI가 구현하고, Kafka·Redis는 사용자가 데스크톱 Codex와 이해하며 직접 구현.
- AGENTS.md에 위임 범위와 중단 지점 추가. dev-notes/CLI_HANDOFF.md를 완료된 후보 초기 과제에서 DB 투표·결과·공통 예외 과제로 교체.
- 후보 중복 처리 유예를 다음 공통 예외 작업에 반영. DB 유니크 제약조건, 동시 투표, 결과 집계의 완료 기준 명시.
- 변경은 Markdown 문서뿐. 애플리케이션 코드 변경 및 테스트 실행 없음.
- 다음 행동: CLI에서 최신 인계 문서를 읽고 구현 시작. 문서 저장만으로 CLI 작업을 자동 실행하지는 않음.

### 2026-10-12 17:00 KST — DB 기반 투표와 공통/도메인 예외 완료
- 최신 AGENTS/PROGRESS/CLI_HANDOFF 재확인 후 후보 중복·정규화, vote 엔티티/Repository/Service/Controller/record DTO, 공통 오류 핸들러와 candidate/vote 도메인 예외 구현.
- 주요 파일: candidate/Candidate.java, CandidateService.java, dto/CandidateRequest.java, candidate/exception/*, common/error/*, vote/* 및 하위 dto/exception. 테스트는 CandidateControllerTests 보완, VoteApiTests 및 ExceptionHandlingTests 추가.
- DB 유니크 제약 이름을 식별해 후보/사용자 중복만 409로 변환. 서비스에서 flush 시 오류를 확인하고 실패 트랜잭션에서 추가 작업 없이 롤백. 결과는 DB 집계 쿼리로 조회.
- 중간 검증 실패와 해결: 테스트 JSON 문자열 이스케이프 오류로 컴파일 실패 후 수정. Repository spy의 추상 메서드에 callRealMethod를 사용한 동시성 테스트 2건 실패 → 실제 대상에 위임하는 default Answer를 호출하도록 수정. 애플리케이션 중복 처리 결함이 아닌 테스트 구성 오류였으며 이후 실제 H2 제약 위반/409 경로 통과 확인.
- 환경 제한: Gradle 캐시 잠금 접근이 샌드박스에서 차단된 test/build는 권한 확장 재실행. Docker 소켓 없음으로 PostgreSQL 환경 사용 불가, PostgreSQL은 미검증으로 남김.
- 최종 검증: ./gradlew build → BUILD SUCCESSFUL, 테스트 34건 모두 통과, bootJar 생성. git diff --check 통과. H2 DB는 무작위 테스트 전용 URL 사용 확인.
- DB 기반 단계 완료, Kafka 학습·설계 대기. 진행 기록 갱신 후 작업 중단. 후속 기능 및 관련 의존성·인프라·추상화는 추가하지 않음.

### 2026-10-12 17:16 KST — 집계 projection과 응답 DTO 변환 분리
- vote/projection/VoteResultProjection.java 추가, VoteRepository.findResults의 select new를 필드/집계 별칭 조회로 변경. VoteService.results에서 projection을 VoteResultResponse로 변환.
- DB 집계·정렬·0표 후보 포함 및 HTTP 계약 유지. 기존 사용자 변경 보존.
- 검증: ./gradlew test --tests org.example.livelunchchoice.vote.VoteApiTests → BUILD SUCCESSFUL, 24건 모두 통과. git diff --check 통과.
- 남은 한계: PostgreSQL 미검증 유지. DB 기반 단계 완료, Kafka 학습·설계 대기 상태로 중단.

### 2026-10-13 19:47 KST — Kafka 첫 실습 인계 갱신
- 사용자 요청에 따라 실제 구현 상태와 데스크톱 학습 합의를 CLI가 읽을 문서에 기록.
- 파일 확인: git status/diff에서 build.gradle의 Kafka 스타터 추가만 확인. VoteService는 기존 DB 저장/조회, application.yml은 H2/JPA만 설정. 이벤트·Kafka 클래스 미존재.
- 합의: Spring 내부 이벤트와 AFTER_COMMIT 리스너로 커밋 후 Kafka 발행, @Async 없이 send Future 콜백으로 결과 관찰, Consumer는 애플리케이션 로그만 출력. DB 결과 조회 유지.
- IDENTITY에서 save도 ID 확보 및 INSERT 제약 위반 확인 가능함을 정정·설명 완료. 사용자는 명시적 DB 반영 의도로 saveAndFlush 유지 결정. var보다 명시적 타입 예제 선호.
- 발행 결과/Consumer 수신·처리 구분, listener id/groupId, partition/offset, 복제 계수와 브로커 수 구분 설명 완료. 장애 실험과 한계는 CLI_HANDOFF에 기록.
- AGENTS.md에 현재 학습 단계 안내 추가, CLI_HANDOFF의 완료된 DB 구현 지시를 현재 사용자 직접 구현 안내로 교체. 과거 DB 구현/검증 이력은 이 문서에 보존.
- 검증: 소스·설정·git 상태 읽기만 수행. 문서 변경이며 코드 수정·빌드·테스트·브로커 조작 없음. CLI에 자동 메시지를 보내거나 작업을 실행하지 않음.

- 저장 후 추가 확인: 사용자가 동시에 이벤트 record를 작성 중. common/event/VoteCreateEvent.java를 읽었으며 eventId/voteId/candidateId/Instant 필드가 있음. 해당 순간 디렉터리와 package 선언이 일치하지 않았고 시각 필드명은 occuredAt이었으나 작성 중 스냅샷이므로 CLI는 최신 파일을 다시 읽고 판단할 것. 사용자 코드 수정 없음.

### 2026-10-13 23:20 KST — 사용자 Kafka 첫 실습 코드 리뷰
- VoteCreateEvent의 패키지·occurredAt 현재 정상. 이벤트 ID 한 번 생성, AFTER_COMMIT 연결, Publisher의 Future 성공/실패 및 즉시 예외 로그, Consumer 수신 메타데이터 로그, 단일 파티션/복제본 토픽 확인.
- 발견: application.yml의 spring.spring.kafka 중첩으로 설정 미적용, trusted.packages와 실제 이벤트 패키지 불일치, 기존 DB 테스트의 외부 Kafka 격리 미비. 소스는 사용자 수정 대상으로 남김.
- 검증: ./gradlew compileJava → BUILD SUCCESSFUL. git diff --check 통과. 테스트·앱·Kafka 브로커 실행 및 실제 송수신은 미실행. 기존 코드 변경 보존, 진행 문서만 갱신.
- 다음: 설정·테스트 격리 보완 후 같은 eventId의 VOTE_COMMITTED/PUBLISH_SUCCEEDED/CONSUMER_RECEIVED 확인, 롤백 및 장애 실험은 별도 수행. Redis·집계 전환·Outbox 선행 구현 없음.

### 2026-10-13 23:24 KST — Kafka 설정 보완 및 테스트 격리
- 사용자 요청으로 최신 수정 재확인. spring.kafka 들여쓰기 수정은 보존하고 메인 application.yml의 trusted.packages를 common.event로 보완.
- 테스트 YAML에서 Kafka 관리자 토픽 생성/리스너 자동 시작 차단. 기존 SpringBootTest 3개 클래스의 Publisher를 MockitoBean으로 대체.
- VoteApiTests에 커밋 전 미발행·커밋 후 한 번 발행 및 롤백 미발행 테스트 추가. VoteEventSerializationTests에 실제 메인 YAML 기반 JSON 타입 헤더/Instant 왕복 검증 추가.
- 중간 실패: 새 YAML 테스트의 평탄화된 속성 키에 불필요한 점을 넣어 조회 실패. 테스트 키를 properties[spring.json.trusted.packages]로 수정 후 통과. 앱 설정 결함과 구분.
- 최종 ./gradlew test → BUILD SUCCESSFUL, 37건 모두 통과. git diff --check 통과. 기존 사용자 Kafka 구현 흐름 보존, 실제 브로커 기동·송수신 및 후속 기능 구현 없음.
- 다음: 사용자가 실제 발행/Consumer 수신 로그를 확인하고 장애 실험 진행. PostgreSQL 및 실제 Kafka 검증 한계 유지.

### 2026-10-13 23:27 KST — 투표 이벤트와 Kafka 패키지 정리
- 사용자 요청에 따라 common/event의 이벤트·리스너를 vote/event로, Publisher/Consumer 및 common/config의 VoteKafkaConfig를 vote/messaging으로 이동.
- VoteService와 테스트 import, 직렬화 테스트 패키지, application.yml의 trusted.packages를 함께 수정. 기존 사용자 코드 내용·명명 유지.
- 최초 테스트 실행은 Gradle 캐시 접근 권한으로 중단. 권한 확장 재실행 ./gradlew test → BUILD SUCCESSFUL, 37건 모두 통과. git diff --check 및 이전 패키지 참조 제거 확인.
- 실제 브로커 송수신·이전 타입 헤더 메시지 호환성은 미검증. 다음은 사용자 실제 송수신 실습이며 후속 기능은 추가하지 않음.

### 2026-10-13 23:52 KST — Kafka 조회 명령과 학습 내용 기록
- 사용자 요청에 따라 dev-notes/KAFKA_COMMANDS.md 작성. 파티션 구성/Consumer 소비 위치 명령, 각 필드 의미, offset·lag 예시와 해석, Postman 관찰 순서 정리.
- 실제 사용자 확인 보고와 설명용 예시를 구분. 터미널 명령 한 줄 선호를 AGENTS.md에도 기록.
- 문서만 변경. 애플리케이션 코드·브로커·데이터 수정 및 테스트 재실행 없음. 다음 단계는 사용자 학습·실험 요청 대기.

### 2026-10-14 14:57 KST — Consumer 중단·재개 실험 인계
- 사용자가 정상 브로커 송수신 확인 완료를 보고했으며, 접수와 소비 분리 및 Consumer 장애를 처리 지연으로 흡수하는 실험 목적을 이해함.
- CLI로 이동하여 구현·실험을 진행하고 결과까지 기록하도록 요청. 로컬 프로필의 KafkaListenerEndpointRegistry stop/start 제어 및 관측 절차를 CONSUMER_STOP_RESUME_PLAN.md에 작성.
- CLI_HANDOFF의 오래된 미구현 안내를 최신 실험 과제로 교체하고 과거 학습 참고 보존. AGENTS에 현재 실험 범위를 명시.
- 검증: 문서 읽기 및 갱신만 수행. 결과 보고서에는 CLI가 실제 실험 후 관측값을 기록하도록 지정했고 성공 결과를 미리 만들지 않음.

### 2026-10-14 15:08 KST — Consumer 중단·재개 구현 및 실제 실험 완료
- 구현 파일: vote/messaging/local/LocalConsumerController.java, 대응 LocalConsumerControllerTests.java. 설정/기존 업무 코드 변경 없이 local 프로필에만 제어 API 등록.
- 테스트/빌드: ./gradlew test bootJar, 43건 통과, git diff --check 통과. Gradle/Docker/프로세스/로컬 네트워크의 샌드박스 접근 제한은 승인된 권한 확장으로 실행.
- 사용자 재시작 승인 후 기존 앱 정상 종료 및 local 앱 실행. 실제 기준 투표 1건 + Consumer 중단 중 3건 순차 생성. HTTP 201/H2 SELECT/동일 eventId 발행·수신 로그/Kafka 그룹 원본 출력 대조.
- 결과: offset/lag 5/5/0 → 5/8/3 → 8/8/0. 중단 중 3건 모두 발행 성공·미수신, 재개 후 offset 5·6·7 순 수신, 최종 running=true. 별도 파티션 조회로 소비된 메시지 보관 확인.
- dev-notes/CONSUMER_STOP_RESUME_RESULT.md에 실행 환경·원본 로그·eventId·HTTP·DB·명령·한계 기록. 후속 기능 없이 종료하고 사용자 학습 요청 대기.

### 2026-10-14 15:33 KST — PostgreSQL 전용 DB와 .env 설정
- 기존 mailplug-seat-reservation-postgres-1 컨테이너에 live_lunch_choice DB 및 소유자 live_lunch_choice_app 생성 성공. 일반 LOGIN 역할이며 superuser/createdb/createrole 권한 없음. 다른 프로젝트 DB는 변경하지 않음.
- 사용자 요청으로 무작위 비밀번호는 로컬 .env에만 저장(0600/Git 제외); 실제 비밀값은 로그·문서에 기록하지 않음. .env.example, .gitignore, 메인 YAML, build.gradle 변경.
- 기존 테스트는 테스트 전용 YAML/H2 유지. 별도 프로필 없이 테스트 리소스 설정으로 격리. ./gradlew test bootJar 성공: 43건, 실패 0, 오류 0, 건너뜀 0. git diff --check 통과 및 .env ignore 확인.
- 사용자 요청으로 추가 연결 실험 중단. PostgreSQL CREATE ROLE/CREATE DATABASE 성공만 확인했으며 앱 연결 검증 완료로 간주하지 않음. 기존 8080 H2 앱 유지, 8081 임시 앱 실행 확인되지 않음.
- 다음: 사용자 직접 재시작/연결 확인. H2 데이터 이전과 Redis 선행 구현 없음.

### 2026-10-14 16:00 KST — 사용자 Redis 집계 코드 리뷰
- build.gradle Redis starter, spring.data.redis의 REDIS_HOST/REDIS_PORT 기본 localhost:6379, Consumer 서비스 호출, VoteAggregationService, VoteRankingRedis 확인. StringRedisTemplate으로 후보 ID 문자열의 점수를 1 증가하고 집계 이후 성공 로그 출력.
- 검증: ./gradlew test 성공(43건, 실패/오류/건너뜀 0), git diff --check 통과. H2 테스트 유지. Redis 집계 전용 테스트 및 실제 Redis 명령 실행 없음. 사용자 구현은 수정하지 않음.
- 리뷰: 재전달 중복 가산 가능, 기본 Kafka 오류 처리 재시도 소진 후 미집계 레코드 skip 가능, 기존 그룹 커밋분 자동 backfill 없음. CONSUMER_RECEIVED 앞의 소비 성공 주석은 수신/집계 성공 구분에 맞게 수정 권장.
- 실행 안내: `docker run -d --name lunch-redis -p 127.0.0.1:6379:6379 -v lunch-redis-data:/data redis:8 redis-server --appendonly yes`
- 확인 안내: `docker exec lunch-redis redis-cli PING`, `docker exec lunch-redis redis-cli ZRANGE live-lunch-choice:ranking 0 -1 REV WITHSCORES`. 각 명령은 한 줄이며 CLI가 실행한 결과가 아님.
- 참고: https://hub.docker.com/_/redis 및 https://docs.spring.io/spring-kafka/reference/kafka/annotation-error-handling.html . 후속 구현 없이 사용자 실습 대기.

### 2026-10-14 17:06 KST — Lua 멱등성 리뷰 및 실제 Redis 테스트
- 사용자 변경 확인: VoteRankingRedis, VoteAggregationService, VoteEventConsumer, redis/aggregate-vote.lua. 정상 단일 Redis 조건의 멱등성/경쟁 방지에서 수정이 필요한 결함은 발견하지 않음. 사용자 코드와 Lua는 수정하지 않음.
- 추가: vote/redis/VoteRankingRedisTests(6건), vote/messaging/VoteEventConsumerTests(3건), build.gradle 테스트 의존성 및 redisTest 태스크.
- 실제 Lua 검증: 신규 후 동일 이벤트 재호출/클라이언트 객체 재생성 시 중복 false와 점수1; 서로 다른 이벤트의 후보별 집계; 동일 eventId 16개 동시 요청 true1개/최종점수1; 다른 eventId 16개 동시 요청 true16개/최종점수16; ranking 잘못된 타입 시 처리 이력 미기록 및 수정 후 재시도 성공; processed-events 잘못된 타입 시 점수 미변경.
- Consumer/서비스 검증: 정확한 eventId/candidateId 위임, 성공/중복 로그 분리, 중복 정상 반환, 집계 예외 그대로 전파 및 성공 로그 없음. Kafka 브로커 통합 검증은 아님.
- 실행: ./gradlew test redisTest → BUILD SUCCESSFUL. test 46건, redisTest 6건, 총52건 실패/오류/건너뜀0. git diff --check 통과. Testcontainers redis:8 임시 컨테이너 사용, 사용자 lunch-redis 데이터 접근/삭제 없음. 테스트 종료 시 임시 컨테이너 자동 정리.
- 한계와 다음 단계: 위 미결 사항 유지. 처리 이력 무제한 증가/두 키 수명 일치, eventId 동일성의 범위, Lua 비롤백 의미를 리뷰 기록. 장애 처리·Redis Cluster·Pub/Sub·SSE 선행 구현 없음.
- 근거: https://redis.io/dev-notes/latest/develop/programmability/eval-intro/ , https://redis.io/blog/you-dont-need-transaction-rollbacks-in-redis/ , https://redis.io/dev-notes/latest/develop/using-commands/multi-key-operations/ .

### 2026-10-15 23:45 KST — Redis 랭킹·Pub/Sub·SSE 설계 결정 및 CLI 인계
- 사용자 제안 채택: Sorted Set 결과 순서를 List DTO로 유지, 이름/0표 후보는 프론트 결합. 이전 Map→DB 조회→Java 재정렬 예제 대체.
- Pub/Sub 선택 이유: 서버별 Kafka 그룹 구성 대신 같은 채널 구독으로 모든 SSE 서버에 알리는 구성을 단순화. 보관/재생이 필요하지 않은 화면 알림이라는 전제와 유실 한계 기록.
- 이전 선택도 DESIGN_DECISIONS.md에 보존: 집계 서비스 분리, VoteRankingRedis 명명, Lua 멱등성 범위, 과도한 장애 보상 제외, YAML/명시적 타입/saveAndFlush/커밋 후 발행/PostgreSQL 재사용.
- AGENTS.md 최신 위임 범위 및 CLI_HANDOFF.md 갱신. 구현·실제 테스트는 CLI의 다음 작업이며 이번에는 문서/파일 목록/git 상태 확인만 수행. 과거 검증 결과를 이번 결과로 주장하지 않음.

### 2026-10-16 11:31 KST — Redis 랭킹·Pub/Sub·SSE 구현 및 검증 완료
- 최신 AGENTS/CLI_HANDOFF/DESIGN_DECISIONS/PROGRESS와 깨끗한 git 상태 확인 후 요청 범위 구현. VoteRankingResponse/Service/Controller, VoteRankingSseService, Publisher/Subscriber/RedisConfig 추가; VoteRankingRedis 조회 및 AggregationService 신규 알림 연결.
- 일반 테스트 격리 설정 유지. Kafka Testcontainers 의존성과 HTTP SSE E2E 프로필/테스트 추가. 일반 테스트54건과 redisTest9건(기존 Lua6+랭킹2+실제 HTTP E2E1) 모두 통과, bootJar 및 diff 검사 성공.
- 실제 eventId f77fde70-62ac-4cd4-a947-82b3b2c4b804: offset0 정상→offset1 중복, committed2, 점수0→1→1, Pub/Sub1회, 두 SSE 갱신 및 재접속 최신 상태. 사용자 DB/Redis/기존 Kafka 토픽에는 쓰기·삭제·reset 없음.
- 중간 E2E 포트 import 오류 수정 및 테스트 종료 순서 개선 후 최종 재검증. dev-notes/REDIS_PUBSUB_SSE_RESULT.md에 원본 로그·payload·명령·한계 기록. DESIGN_DECISIONS에 구현 상태와 테스트 격리 선택 추가.
- 다음: 사용자 검토. 다중 서버/희귀 장애 보상/완성형 UI/부하 실험으로 확장하지 않음.

### 2026-10-16 14:58 KST — 결과 DTO 통일
- 사용자 요청으로 VoteRankingResponse를 제거하고 VoteResultResponse로 통일. Redis/랭킹 Service·Controller/SSE 및 관련 테스트 참조 변경.
- 기존 DB API 이름 필드는 보존. VoteResultResponse에 2인자 생성자와 name 필드 NON_NULL 직렬화 설정을 추가해 Redis/SSE JSON에는 이름을 생략. 기존 응답 계약 유지.
- ./gradlew test redisTest 성공: 일반54건/격리 Redis·Kafka·HTTP/SSE9건, 총63건 실패·오류·건너뜀0. git diff --check 통과, src에서 이전 DTO 참조 없음. 사용자 기존 변경 및 실제 데이터 보존.
- 다음: 사용자 코드 검토. 기존 미검증 항목과 학습 범위 유지, 후속 기능 구현 없음.

### 2026-10-16 15:03 KST — 조회·집계 서비스를 VoteService로 통합
- 사용자 요청으로 VoteRankingService.findAll을 VoteService.rankings로, VoteAggregationService.aggregate를 VoteService.aggregate로 이전하고 두 클래스를 삭제. Controller/SSE/Consumer 의존성과 테스트 연결 수정.
- VoteService 네 메서드 앞에 한국어 역할 주석을 추가. DB 등록/결과 조회에만 기존 Transactional 유지하고 Redis 조회·Lua 집계에는 DB 트랜잭션을 추가하지 않음. 응답·중복 방지·Pub/Sub 조건·SSE 계약 유지.
- 조회 통합 중 테스트 verify의 이전 findAll 참조로 컴파일 실패한 뒤 rankings로 수정. 조회 통합 검증 통과 후 추가 집계 통합 요청을 반영해 다시 검증.
- 최종 ./gradlew test redisTest 성공: 일반54/실제 통합9, 총63건 실패·오류·건너뜀0. git diff --check 통과, src에서 제거한 두 서비스 참조 없음. 기존 사용자 변경과 데이터 보존.
- 다음: 사용자 코드 검토. 기존 미검증/범위 제외 항목은 유지하며 후속 기능 구현 없음.

### 2026-10-16 15:15 KST — 실제 로컬 투표·Pub/Sub·SSE 시연
- 기존 앱 rankings404 확인 후 ./gradlew bootJar 성공, 기존 앱을 정상 종료하고 최신 local 앱 재실행. 사용자 데이터 초기화/토픽 reset 없음.
- 기존 일식 후보에 userId live-sse-86536c208200 투표1건 생성. HTTP201/voteId5. PostgreSQL SQL 저장 확인, DB/Redis 득표3→4.
- eventId ee0a46f2-a906-4ca2-950f-dc3a29fb4f43: Kafka offset12 발행/수신, Pub/Sub 발행/서버 구독·별도 redis-cli 수신, SSE 두 연결 모두3표→4표. 최종커밋13/END13/lag0. 임시 관측 연결만 종료하고 앱 실행 유지.
- REDIS_PUBSUB_SSE_RESULT.md에 실제 원본 증거 추가. PostgreSQL 정상 E2E는 검증으로 갱신; 브라우저 자동 재연결·다중 서버·장애 보상 범위 제외 유지.

### 2026-10-16 15:17 KST — 사용자 직접 실행을 위해 앱 종료
- 사용자 요청으로 CLI 실행 PID16676에 SIGTERM 전송. lsof 확인 결과8080 리스너 없음. PostgreSQL/Kafka/Redis는 중단하거나 초기화하지 않음.
- SSE는 별도 터미널에서 curl -N으로 초기/변경 ranking 이벤트를 관찰하도록 안내. 현재60초 연결 타임아웃 이후 curl은 자동 재접속하지 않으므로 명령 재실행 필요.

### 2026-10-16 15:22 KST — 데스크톱 구현 리뷰
- 최신 코드/설계 결정/CLI 결과 문서를 대조. VoteService 통합과 VoteResultResponse 재사용은 CLI에서의 후속 사용자 요청으로 확인하여 이전 분리 결정을 강제하지 않음.
- Redis 순서 유지 List 반환·랭킹 경로 DB 미접근, 신규 집계만 Pub/Sub 발행, 로컬 SSE 연결별 초기/변경 전송 및 정리, 초기/변경 전송의 동일 모니터 동기화를 확인. 합의한 정상 실행 범위에서 즉시 수정할 기능 결함을 발견하지 못함.
- 테스트 소스 및 저장된 XML 직접 확인: 일반54/통합9, 총63건 실패·오류·건너뜀0. 이번 리뷰에서 테스트를 재실행하지 않았으며 코드/실제 데이터/서버 상태를 변경하지 않음. git 작업 트리 깨끗함.
- PostgreSQL→Kafka→Redis→Pub/Sub→두 SSE 시연은 CLI 결과 문서의 증거를 확인한 것이며 데스크톱 재실행 결과가 아님. 다중 서버 및 브라우저 EventSource 자동 재연결은 남은 검증으로 유지.

### 2026-10-16 15:32 KST — 다중 서버 기능 실험 인계 및 부하 비교 방향 합의
- MULTI_SERVER_SSE_PLAN.md 작성, CLI_HANDOFF/AGENTS 최신 범위 갱신. 같은 Kafka 그룹·한 파티션 집계와 Redis 채널 전체 서버 알림을 구분하여 검증하도록 지정.
- 사용자 의도: 성능 우열을 전제하지 않고 DB+Polling과 Kafka+Redis+SSE가 유리한 조건과 비용을 후속 부하 실험으로 설명. DESIGN_DECISIONS.md에 기록.
- 문서 작업만 수행. 앱 실행·투표 추가·부하 테스트 없음. 다음은 CLI 실제 기능 실험 및 MULTI_SERVER_SSE_RESULT.md 작성.

### 2026-10-16 15:38 KST — 다중 서버 기능 실험 완료
- 최신 지침/계획/설계 확인, git 변경 없음, bootJar 성공. 기존 앱을 종료하지 않고 빈8080/8081에 동일 JAR·local 앱 순차 시작.
- A PID18035/B PID18037, 같은 vote-log-group, 파티션1을 A만 할당. 기존 Redis 구독1개 보존 후 두 서버 추가 구독 확인.
- 순차 투표2건: A voteId6/eventId aa327cd2-d99c-4b70-b7df-2b74aa1c33eb, B voteId7/eventId ae381744-98b9-4d84-9af4-79ab70ef33f6. 둘 다 HTTP201, A만 집계, A/B Pub/Sub 수신 및 SSE4→5→6. DB/Redis+2, offset13/14 최종커밋15/lag0.
- 첫 준비 시도 구독 수 오판 timeout 후 스크립트 수정, 투표 생성 전 종료여서 추가 데이터 없음. 최종 실제 PostgreSQL SELECT로6·7 저장 확인.
- 결과 문서에 전체 원본/명령/한계 보존. SSE 정리와 두 CLI 앱 SIGTERM 완료,8080/8081 비어 있음. 제품 코드·기존 데이터 초기화·부하 테스트 없음. 다음은 사용자 결과 검토.

### 2026-10-16 16:10 KST — 프로필 분리 CLI 인계
- PROFILE_SEPARATION_PLAN 및 최신 CLI_HANDOFF/AGENTS 작성. VoteService 공통 DB 기능 유지, VoteRealtimeService로 Redis 메서드 이동, 프로필별 Bean/YAML/API 및 테스트 기준 명시.
- DESIGN_DECISIONS에 서비스 분리 이유 변경과 사용자 화면 체험 우선 순서 기록. 문서만 수정했으며 구현·테스트 미실행.

### 2026-10-16 16:16 KST — polling/realtime 프로필 분리 완료
- VoteService 공통 DB 기능 보존, VoteRealtimeService 분리, 실시간 Bean Profile(realtime), 제어 API local & realtime 적용. DTO 통합과 Redis 순서·중복·알림 계약 유지.
- YAML 공통/모드 분리, polling의 Kafka/Redis 자동 구성 제외는 실제 Boot4.1.1 JAR 메타데이터로 확인. ProcessingModeConfig에서 정확히 한 모드만 허용.
- 테스트별 프로필 및 리팩터링 참조 수정. 실제 polling HTTP/연결 감지 테스트, 프로필 조합·모드 오류 검사 추가. ./gradlew test redisTest bootJar 성공, 일반68/실제통합9=77건 모두 통과. 실제 JAR local만·두 모드 시작은 exit1로 거절.
- PROFILE_SEPARATION_RESULT.md에 실제 증거/명령/한계 저장, DESIGN_DECISIONS 최신 선택 갱신. 기존 사용자 인프라·데이터 보존. 다음은 사용자 검토이며 프론트·부하·장애 보상 미진행.

### 2026-10-16 16:34 KST — 실험용 Consumer 제어 API 제거
- 사용자 요청으로 LocalConsumerController와 전용 테스트11건 제거. PollingProfileIntegrationTests의 삭제 클래스 참조 정리, 기존 경로404 검증 유지. 기존 프로필 분리 변경과 업무 Consumer는 보존.
- 검증: ./gradlew test 성공, 일반57건 실패·오류·건너뜀0. git diff --check 통과. 실제 Kafka/Redis 재실험·실행 앱 교체·bootJar 재빌드는 미실행. 기존 데이터 변경 없음.
- local 프로필 전반 정리는 사용자 보류에 따라 수행하지 않음. 과거 실험 기록은 보존. 다음은 사용자 검토이며 후속 기능 구현 없음.

### 2026-10-16 16:35 KST — Consumer 제어 API 복원
- 사용자가 직전 제거 요청을 철회하여 LocalConsumerController와 전용 테스트11건, polling 테스트의 클래스 미등록 검사를 제거 직전 상태로 복원. local & realtime 조건 유지. 다른 기존 변경 보존.
- ./gradlew test 성공:68건, 실패·오류·건너뜀0. git diff --check 통과. 실제 브로커 재실험·앱 교체·bootJar 재빌드 없음. 데이터 변경 없음. 다음은 사용자 검토.

### 2026-10-16 16:52 KST — 공유 투표 화면 및 환경변수 모드 선택
- static/index.html·styles.css·app.js와 ProcessingModeController 추가. 서버 실제 모드에 따라 주기 조회/SSE 선택, 후보 데이터 결합·0표·안전한 문자열 렌더링·상태 안내 구현. 기존 업무 계약 보존, 프레임워크/프론트 서버/빌드 도구 추가 없음.
- 밝은 미색 바탕/차분한 녹색/화이트 카드, 시스템 글꼴·여백·반응형, native radio/input/button 및 focus 표시. 디자인과 실행법은 DESIGN_DECISIONS 및 FRONTEND_RESULT에 기록.
- 화면 단계 일반72+실제통합9 통과. 실제 Chrome 두 탭 polling 투표1건, realtime 투표2건과 랭킹/비율 반영 확인. 검증 전용 H2/임시 브로커만 사용, 사용자 데이터 보존.
- 후속 사용자 지시: 추가 실험 중단, 직접 실행 확인. SSE 자동 재연결 등 남은 항목 미검증. 임시 앱 종료0 및 자원 정리.
- 후속 사용자 합의: APP_PROFILE 환경변수 선택, 기본 realtime. application.yml/.env.example 수정, 기존 .env의 APP_PROFILE=realtime 및 DB 설정 보존. 최종 ./gradlew test bootJar 성공(일반76건), 실제 통합 재실행 없음. git diff --check 통과.
- 다음: 사용자 화면 확인 및 추가 검증 요청 대기. 부하·로그인·후보 관리 기능 미구현.

### 2026-10-16 17:09 KST — 후보 추가 화면
- 사용자 요청으로 static/index.html·app.js·styles.css에 후보 등록 폼 추가. 투표 폼과 분리하고 details/summary로 키보드 접근 지원. 기존 백엔드/API 계약 유지.
- 성공 응답의 ID·이름만 반영, textContent 사용. 공백/중복/실패 안내, 전송 중 중복 제출 차단, 선택 유지. 늦은 조회가 신규 후보를 지우지 않도록 ID 병합.
- ./gradlew test --tests '*CandidateControllerTests' bootJar 성공:7건 실패·오류0. node --check 및 git diff --check 통과. /tmp/lunch-candidate-ui-check.cjs 모의 DOM/fetch 검증: 공백, 중복 제출, 성공/선택 유지/0표, 늦은 조회,409/500 처리 통과.
- 이번 실제 브라우저·실제 등록 HTTP 검증 미실행. 사용자 앱/데이터 변경 없음. 문서 갱신 첫 Python 호출은 인코딩 오류로 미반영 후 재작성.
- 다음: 사용자 확인. 후보 변경 SSE는 후속 제안 논의 중. 커밋 후 Pub/Sub→각 서버 SSE candidates-changed→브라우저 후보 목록 재조회 방향을 설명했으며 미구현.

### 2026-10-16 17:15 KST — 후보 추가 버튼 위치와 펼침 패널 개선
- 사용자 요청으로 제목 옆에 + 후보 추가 버튼 배치. 후보 목록 위 입력 영역을 열기/닫기, Escape 닫기, aria-expanded/controls 및 포커스 이동 적용. 모바일 헤더/입력 줄바꿈 유지.
- 기존 등록 처리 보존. app.js 문법, 모의 DOM에서 펼침/닫힘·ARIA 동기화와 기존 등록 시나리오 통과. ./gradlew bootJar 성공, git diff --check 통과. 실제 브라우저 재검증/API 테스트 재실행 없음.
- 다음: 앱 재시작 및 화면 새로고침 후 사용자 확인. 후보 변경 SSE와 후속 기능은 추가하지 않음.

### 2026-10-19 14:19 KST — 부하 비교 준비 CLI 인계
- 사용자 결정: 종료·초기화 고도화 중단, 두 구조의 조건별 비교와 회고에 집중. 실험 전용 DB 사용 요청 반영.
- LOAD_TEST_HANDOFF.md 및 최신 CLI_HANDOFF/AGENTS/설계 결정 갱신. 기존 데이터 격리, SSE 측정 도구 검토, seed 정합성, 소규모 예비 실행/대규모 후속 범위 명시.
- 문서 저장만 수행. DB 생성·컨테이너 실행·제품 코드 변경·실제 부하 테스트는 미실행.

### 2026-10-19 14:49 KST — 격리 부하 환경 및 polling 예비 완료
- load-tests/infra.yml, application-loadtest.yml, .env.example, seed.sql, manage.py, pilot.py, README.md 추가. 제품 Java/메인설정/기존 .env 변경 없음. 전용 설정/원본 결과는 Git 제외.
- 고유ID20261019_144130_353a의 전용 PG DB2개/역할 생성, 후보4/초기1000표 일치. Kafka19092/Redis16379 격리, Redis 초기값/합성처리이력 직접시드. 기존8080/기존DB/9092/6379 보존.
- polling PID27062/18080 소규모 실제측정:30건 전부201, DB/관람자10명 최종1030표 일치. realtime DB/Redis는 초기1000표 유지. 원본/측정정의/자원/한계는 LOAD_TEST_PILOT_RESULT 및 PLAN에 기록.
- 초기 prepare 파일API 오류/스크립트 모듈 변수 충돌은 각각 외부변경 전/투표 전 수정. Python 캐시 sandbox 오류는 승인 실행 및 별도 캐시 경로로 처리. 최종수렴은 실제 스냅샷 최초도달로 산출식 수정. bootJar/문법/diff 통과, JUnit 재실행 없음.
- polling 앱만 SIGTERM 종료143,18080 비어있음. 기존8080 PID22930 유지. 실험 PG DB/Kafka/Redis는 전환을 위해 보존. 사용자 요청에 따라 realtime 전환 전에 멈추고 .env.loadtest 변경 확인 대기.

### 2026-10-19 15:07 KST — realtime 예비 실행과 비교 기록
- 사용자의 .env.loadtest APP_PROFILE=realtime 변경 확인 후 동일10명/1req/s/30초 실행. 투표30/30, DB/Redis/관람자 최종408·308·207·107, SSE60초 종료 후10명 재접속/최신값 확인.
- 발행/소비/집계/PubSub 각각30건(offset0~29). 기존 Docker 내부 CLI의 advertised 주소 접근 실패로 lag=null/pilot exit1. 원본 보존, 호스트 Kafka Admin 도구 추가/timeout 설정 수정 후 최종committed30/end30/lag0 재확인. 부하 추가 실행 없음.
- realtime 부하 중 자원샘플0(동일 샘플러가 CLI 대기), lag 시계열/정확한 해소시간 미검증. API/누적목표 관측 지연/본문/DB통계와 한계를 결과문서에 기록. 수정 전체pilot·JUnit 재실행 없음. Python 문법 및 Admin 실제 조회 확인.
- 전용앱27303 종료143,18080 리스너 없음, 기존8080 PID22930 보존. DB/전용KafkaRedis/결과 보존. 대규모 전 새 시드10명 관측기 재검증→10/50/100명60초3회 단계 제안만 기록. 서비스 종료/초기화·대규모 부하 선행 없음.

### 2026-10-19 15:24 KST — 단계별 부하 비교 CLI 인계
- LOAD_TEST_SCALE_HANDOFF/CLI_HANDOFF/AGENTS/PLAN/설계 결정 갱신. 관측기 재검증, 반복별 동일 데이터·워밍업·실제 도착률 검증, 자원/lag/정확성 및 단계 중단 기준 명시.
- 기존 사용자 모드 전환 확인 절차 보존. 새 부하 실행·제품 코드 수정 없음.

### 2026-10-19 15:54 KST — 관측기 재검증 통과 및 단계별 비교 중단
- manage에명시적설정/결과경로·포트/명령시간상한추가, pilot에자원/lag독립수집·워밍업4표·스케줄/FD/스레드/swap기록·증가중단판정추가. scale.py에조건별3회/새DB·전용컨테이너/모드확인/실패시정지구현. 제품코드변경없음.
- 새ID20261019_153750_7c5d polling/realtime30건씩검증. 사용자.env.loadtest-scale realtime변경확인후만실행. 두모드1034표,실시간lag0·10재접속,측정구간자원/lag시계열확보.
- 관람자10/1req/s/60초반복의첫준비는포트변수전달오류로데이터생성전실패. 기록보존/수정후1회재시도. 새ID20261019_155129_edd8 실제60/60성공·DB/10관람자1064표일치,하지만swap8714.31→9339.75MiB 및발생기272.52ms로증가중단. 추가조건미실행/3회편차미산출.
- 앱18081종료,신규두실험의KafkaRedis컨테이너stop보존,기존8080/기존인프라보존. bootJar/문법검사통과. 다음은호스트메모리경쟁확인및새시드동일조건재개논의. 결과/PLAN/README/설계기록갱신.

### 2026-10-19 16:02 KST — 사용자 요청으로 부하테스트 데이터 정리
- 사용자 명시: 실제 데이터가 있는 공유 PostgreSQL5432/기존Kafka9092/Redis6379 유지, 부하테스트 데이터만 삭제. 소유마커/컨테이너라벨/DB연결0/앱종료 확인 후 실험3세트의DB6개·역할3개·KafkaRedis컨테이너6개 및전용볼륨 삭제. 기존live_lunch_choice와서비스/8080앱은보존. 결과JSON/로그/문서와로컬설정은보존. cleanup.json에삭제runID기록.
- 삭제ID:20261019_144130_353a,20261019_153750_7c5d,20261019_155129_edd8. 기존설정파일은이제삭제된자원을가리키므로재사용하지않고새실험설정이필요.
- 사용자 후속 결정:스왑증가/발생기스케줄지연만으로다음단계를막지말고경고로남긴채진행. 기존실패원본은수정하지않음. 집계불일치/HTTP실패/OOM/lag미해소 등다른기준은유지. 이번정리중새부하는실행하지않음.

### 2026-10-19 16:08 KST — 50명 polling3회 완료
- 사용자 후속지시에따라스왑/스케줄지연을경고로변경하고50명단계진행. 기존10명유효3회미완료/무효기록은소급변경없음. 제품코드수정없음.
- 반복별60/60성공·1064표일치·관람자50명최종값확인,각조회1500건. 투표/조회/누적목표관측지연과반복별자원/DB통계를결과문서에기록. OOM/HTTP/클라이언트/관측오류없음,스왑/스케줄경고없음.
- 반복2준비포트bind오류는실제리스너없음확인후SO_REUSEADDR로보완. 데이터생성전실패만재개하도록제한하고실패원본보존. 문법/diff검사및실제3회통과,JUnit미실행.
- 실험DB/전용컨테이너는대응realtime검증위해보존. 사용자.env.loadtest-v50-r1 변경확인대기.

### 2026-10-19 16:19 KST — 50명realtime 완료 및100명polling 완료
- 사용자.env.loadtest-v50-r1 전환확인후realtime60초3회,총180/180성공. DB/Redis/관람자50명각1064표,lag0/50재접속. 자원/lag/관측지연/반복p95편차/SSE연결공백기록. SSE재연결지연을통계에서제외하지않음.
- 다음관람자100명polling60초3회진행,총180/180성공·각DB/100명최종일치·각조회3000건. 중단없음. 원본v50-r1/realtime-batch.json 및v100-r1/polling-batch.json과반복별폴더보존.
- 제품/관측기코드변경없이기존도구실행,기존앱/데이터보존. 18081앱종료/전용컨테이너stop보존. 사용자100명realtime전환확인대기,투표량5/10req/s아직미실행.

### 2026-10-19 16:35 KST — 100명 realtime 1req/s 및 polling 5req/s 완료
- 사용자 모드 전환 확인 후 realtime 1req/s 60초 3회: 180/180 성공, 매회 SQL/Redis/100명 1064표 일치, lag 0 및 100명 재접속. 투표 p95 44.57/23.74/27.29ms.
- 다음 단계 polling 5req/s 60초 3회: 900/900 성공, 매회 476/376/276/176=1304표 일치. 투표 p95 108.02/19.42/20.39ms. 첫 반복 스케줄 최대528.54ms 및 swap 경고는 보존. 서버 병목으로 단정하지 않음.
- 결과 원본과 자원/lag/DB통계/반복 편차 기록. 제품/관측기 코드 변경 없음. 기록 갱신용 Python 입력에서 인코딩 오류가 나 본문 변경 전에 실패했고 재작성함; 실험 결과와 무관.
- 실험 앱18081/전용 컨테이너 중지, 기존5432/9092/6379/8080 보존 확인. .env.loadtest-v100-r5의 사용자 realtime 변경 확인 후 대응 3회 진행. 10req/s 미실행.

### 2026-10-19 16:50 KST — realtime5req/s 및 polling10req/s 완료
- 사용자 .env.loadtest-v100-r5 realtime 전환 확인 후 100명/5req/s/60초3회 통과. 총900/900성공·각1304표·최종lag0/100재접속 확인. 투표p95 18.46/18.43/20.47ms. 수집오류 및 경고 없음.
- 다음 마지막 증가조건100명/10req/s polling60초3회 통과,총1800/1800성공·각551/451/351/251=1604표일치. 결과/자원/DB통계/관측지연/반복p95편차 원본 보존.
- 제품 및 관측기 변경 없음. 실험 앱18081 종료/전용컨테이너stop, 기존 서비스 보존. .env.loadtest-v100-r10 사용자 realtime 전환확인대기. 아직 최종 모드 비교 전체 완료로 주장하지 않음.

### 2026-10-19 17:03 KST — 마지막 realtime10req/s 및 최종 비교 기록 완료
- 사용자 .env.loadtest-v100-r10 realtime 변경 확인 후100명/10req/s/60초3회 실행. 1800/1800 성공, 매회551/451/351/251=1604표 SQL/Redis/100명 일치, lag0 및100명 재접속 확인. 첫 반복 발생기 스케줄 지연 경고는 사용자 지시에 따라 기록하고 진행.
- v100-r10 realtime 원본과 전체24회 비교표를 기록. 반복별p95 범위/편차와 합산p95를 구분. realtime 보통 갱신은 빠르지만 SSE재연결에서 수초 지연 및 빈번한 전체스냅샷에 따른 본문량 증가도 기록.
- 전체24회 측정6120건 성공(워밍업96건 제외), 최초10명60초유효3회 미완료는 숨기지 않음. 쿼리횟수/전체네트워크/브라우저렌더링/운영용량/다중서버/장애복구는 미검증 또는 범위밖.
- 제품/관측기 수정 없이 실제실행과 문서/요약JSON 갱신만 수행. 기존앱8080 PID22930 유지,18081리스너없음,현재실행컨테이너는기존PG/Kafka/Redis만 확인. 이번 실험DB/중지컨테이너와 결과는 삭제하지 않음. 추가 기능/부하 자동확장 없이 종료.

## 2026-10-19 17:18 KST — 부하 실험 목적 재정의
- 사용자가 기존 비교를 폐기하고 처리 용량/실패율/갱신 지연 중심 재실험 요청. CLI_HANDOFF/AGENTS/새 CAPACITY_HANDOFF 및 이전 결과의 폐기 표시 갱신.
- DB 단순 SSE 기준, 공통 전달 계약, 로컬 큐의 유한성 및 집계/전송 병목 구분, 단계별 데이터/부하 탐색 계획 기록.
- 제품 코드 수정/새 부하 실행 없음. 다음 작업은 CLI 구현·동등성 확인·계측 검증 후 새 실험.

### 2026-10-19 17:25 KST — 실험 데이터 삭제, 컨테이너·결과 보존
- 사용자 명시: 실제 서비스 데이터 제외한 실험 자원 정리, 추가 환경변수 삭제, 컨테이너 유지. 추가 확인에서 측정 결과 보존 선택.
- 기존 실험12세트의 소유마커/컨테이너라벨/DB연결0/중지상태 검증. Kafka/Redis를 한 세트씩 시작해 vote-created 및 vote-log-group 삭제 후 사용자토픽0/소비그룹0 확인, Redis FLUSHDB SYNC 및 AOF 재작성 후키0 확인하고다시중지. Kafka 내부브로커메타데이터는재사용을위해유지하며컨테이너/볼륨삭제없음.
- DB24개/역할12개삭제,루트.env.loadtest*20개삭제. 생성 kafka-admin-libs/파이썬캐시14개디렉터리삭제. 측정원본70MB와상태manifest/코드보존. 기존도구의오래된설정참조는더이상실행환경으로재사용하지않으며새실험에서연결설정을준비해야함.
- 첫사전검사는이미삭제된과거실험설정에서실패(변경전). 해당컨테이너/DB부재검증후제외하여실제12세트만처리. 모든검증통과.
- 사후확인: DB목록live_lunch_choice/mailplug/postgres/seat_load_test만잔존, llc_lt_역할0. 실험컨테이너24개동일ID/중지,기존PG/Kafka/Redis실행및8080 PID22930보존. 결과 load-tests/results/data-cleanup-containers-kept.json에보존. 새부하/제품구현미실행.

### 2026-10-19 18:12 KST — 동일 SSE 비교 구현, 새 실험 및 환경 중단 기록
- db-sse/RankingSource/DbRankingService/DbVoteNotificationListener와 capacity 계측 추가. 기존 저장·polling·realtime 계약 보존. 커밋 가시성/롤백 미전송/유한큐 포화와 기존 회귀 일반83건, 실제 통합9건 통과.
- 새 격리 인프라와 async SSE 발생기 준비. 소부하 각14표/두 SSE 일치, B Redis·발행/집계/알림14회·최종lag0. eventId 상관 로그와 실제 SQL/Redis 명령 횟수 보존.
- 조회10req/s 교차 재검증,200req/s 상한 진단 실행. 최초 스케줄 기준 실패와 재검증 기록 보존. 200에서발생98.65%/HTTP성공100%/APIp95 39.81ms이므로 서비스 한계 아닌 환경제한으로 증가 중단. 본 경로/대용량/경계반복 미실행.
- Kafka CLI 반복실행의 CPU 간섭 발견. 호스트 단일 AdminClient 관측기로 보완, 읽기검증초기오류1/lag0표본2 확인. 수정계측 부하검증·CPUthrottling/실제pool획득시간 미완료로 명시.
- 관련 기록 LOAD_TEST_CAPACITY_RESULT/CLI_HANDOFF/DESIGN_DECISIONS와 load-tests/results/capacity/llc-cap-20261019-173825. Python문법/git diff검사 통과. 실험컨테이너중지/데이터보존/실제서비스유지. 종료·초기화API/장애보상 구현 없음.

### 2026-10-19 18:47 KST — 사용자 기준 변경 후 재실험
- 사용자 지시로 발생률/스케줄/SLO 경고만으로 전체 중단하지 않음. 기존 합격 수치는 유지하고 실발생량·실패를 그대로 기록. OOM/앱종료/15초 성공응답 없음/최종 불일치 등만 critical로 구분.
- continue.py/analyse.py 추가, 관측기 진행 체크포인트·중단감지·HTTP 불명확 커밋 대조 보완. CapacityMetricsController에 capacity 전용 Hikari 누적획득시간 계측. 일반83건/실제소부하 양쪽 재검증 완료.
- 조회12회 실행,10만 건200req/s의발생기/HTTP경고를넘겨진행.100만 건A50req/s에서는HTTP성공0.83%/pool타임아웃으로critical중단. 같은B50은1500/1500성공. 쿼리CPU와연결풀병목확인,OOM/데이터훼손없음. 본경로/SSE부하는미실행.
- 결과원본resume1와최초/최종PGdump보존. 실험컨테이너만중지하고실제서비스유지확인. Python문법/gitdiff검사통과. 구체적인결과/한계는CAPACITY_RESULT에기록.


## 2026-10-19 18:50 KST — 구 polling 부하 도구 정리
- 삭제: load-tests의 manage.py, pilot.py, scale.py, KafkaLag.java, infra.yml, application-loadtest.yml, seed.sql, .env.example 및 관련 캐시. README는 현재 capacity 안내로 교체.
- 보존: 모든 결과 Markdown, results/state 이력, capacity 도구, src 코드 및 실제 서비스 자원. 과거 결과에 있는 명령은 당시 실행 기록으로 유지.
- 검증: 삭제 전후 SHA-256 비교로 results 전체·capacity 전체·src 전체·dev-notes/*RESULT*.md 동일 확인. capacity Python AST 구문 검사 통과. 실행 코드 변경이 없어 JUnit·실제 부하는 재실행하지 않음.
- 다음 단계: 현재 capacity 비교의 미실행 본 투표·SSE 부하 범위 검토. 자동 재실행하지 않음.


## 2026-10-20 18:38 KST — DB + SSE 구현 분리 커밋
- 681c72c feat: DB 집계 기반 SSE 알림 모드 추가; origin/main 푸시 완료.
- 포함: db-sse 프로필, RankingSource/DbRankingService/DB 집계 projection, 커밋 후 bounded queue 비동기 알림, 공통 SSE 연결, 모드 검증 및 DB-SSE 테스트.
- 제외: load-tests, common/capacity, CapacityMetrics 호출·eventId 계측 전달·실험용 SSE 타임아웃 설정. 기존 작업 파일은 유지하고 Git 인덱스에 기능 부분만 분리.
- 검증: HEAD 기반 임시 복사본에 커밋 내용만 적용해 ./gradlew -p /tmp/llc-db-sse-commit-06ncgzrn test bootJar 성공. 일반83건 실패/오류/건너뜀0, staged blob과 검증 파일 동일 확인, diff --check 통과. 실제 Redis/Kafka 통합·부하 재실행은 하지 않음.
- 남은 작업: 로컬 부하 계측 변경과 도구는 별도 검토/커밋 대상.

## 2026-10-20 KST — 실제 이벤트 후속 실험 인계
- 사용자 요청으로 남은 full/viewers와 제한된 burst,Consumer 중단/재개 역할 확인을 인계. read critical로 전체 단계가 누락되는 실행 제어 보완 포함.
- Kafka ack/집계/SSE 단계 분리,로컬큐와lag 추세,경계별 반복/정확성 검증 및 시나리오별 완료 상태 요구.
- 새 문서 LOAD_TEST_EVENT_PIPELINE_HANDOFF.md,CLI_HANDOFF,AGENTS,DESIGN_DECISIONS 갱신. 코드 변경/부하 실행 없음.


## 2026-10-20 18:45 KST — 부하 도구 및 계측 제거
- load-tests 전체(실행 도구/설정/상태/원본 로그/덤프/스냅샷)와 common/capacity 삭제. 추적 파일8개에 남은 계측 변경을 DB + SSE 커밋 내용으로 복원.
- 결과 Markdown은 유지. load-tests 내부 Markdown3개는 dev-notes/LOAD_TEST_ARCHIVE로 이동, README는 TOOL_GUIDE.md로 변경하여 Git 제외 유지. 기존 docs 결과 문서의 원본 경로는 이제 삭제된 과거 경로임.
- ./gradlew clean test bootJar 성공: 일반83건 실패/오류/건너뜀0. 최초 샌드박스 Gradle 캐시 권한 실패 후 승인 실행 성공. 계측 참조0건, git status 깨끗함. Redis/Kafka 실제 통합 재검증은 미실행.
- 기존 컨테이너/볼륨/DB 및 .env 미변경. 새 커밋/푸시 없음.

## 2026-10-20 KST — 프로젝트 README 및 회고 작성
- 사용자 요청으로 루트 README.md 작성. 소개/학습 목적/단계별 진행/세 처리 구조/설계 선택/장단점/실험 재설계/조회 병목/남은 검증을 구분.
- 조회 결과를 Kafka 또는 전체 경로 처리 용량으로 확장하지 않고 기존 비교 폐기 이유와 종료·초기화 범위 축소를 기록.
- 실제 코드와 최신 PROGRESS/CAPACITY_RESULT를 참고한 문서 작업이며 제품 수정·테스트·부하 실행 없음.

## 2026-10-20 KST — README 대안 설계 회고 보완
- Redis 우선 집계·Pub/Sub/SSE 및 주기적 DB 배치 반영이라는 사용자 제안을 미검증 가설로 README에 기록. 집계/원본 저장 차이와 저장 보장·배치 반영 비용을 함께 설명.
- README의 후속 실험 상태를 최신 도구 정리/자동 진행 중단 기록과 맞춤. 문서 변경만 수행,코드 구현·테스트 실행 없음.

## 2026-10-20 KST — 실행 안내와 사용자 회고 반영
- README 상단에 새 로컬 환경의 JDK/Docker/인프라/.env/앱 실행 및 재시작 안내 추가. 실제 설정과 공식 이미지 안내 확인,새 환경 실행은 미검증.
- 13번 관련 기록 목록을 사용자 학습 경험 중심 회고로 교체. 비동기 분리의 장점과 정합성/초기화 복잡성,범위 선택,제품 요구에 따른 기술 판단을 정리.
- 투표·데이터/후보 전체 초기화 미구현 명시. 제품 코드 변경·인프라 실행 없음.

## 2026-10-20 KST — README 중복 회고 정리
- 사용자 요청으로 기존 10번 학습·범위 회고 삭제. 뒤의 11~13번을 10~12번으로 변경하고 마지막 사용자 경험 중심 회고 유지. 문서만 수정.

## 2026-10-20 KST — 공개 설명과 개발 기록 분리
- README에는 프로그램 소개/동기/느낀점과 상세 문서 링크 유지. docs/GETTING_STARTED,ARCHITECTURE,DESIGN_REVIEW,PERFORMANCE_REVIEW로 상세 내용 분리.
- 사용자 후속 지시로 기존 기록도 Git 포함. 기존 docs 전체를 dev-notes로 이동하고 AGENTS/기록 참조 및 .gitignore 예외 갱신. 환경 파일 제외 유지.
- 문서만 변경,제품 코드/테스트 실행/커밋/push 없음.
