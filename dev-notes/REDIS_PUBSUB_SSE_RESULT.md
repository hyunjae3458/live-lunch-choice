# Redis 랭킹·Pub/Sub·SSE 구현 및 검증 결과

기록: 2026-10-16 11:31 KST. 아래 결과는 CLI 직접 실행 결과다. 과거 사용자 실습 결과와 구분한다.

## 구현과 API 계약
- `GET /api/votes/rankings`: Redis Sorted Set 전체를 reverseRangeWithScores로 조회하고 순서대로 candidateId/voteCount record List 반환. DB 조회·Map 재조립·Java 정렬 없음. 빈 데이터는 `[]`.
- `GET /api/votes/rankings/stream`: HTTP 200, text/event-stream, Cache-Control: no-store. 연결마다 SseEmitter(60초), 초기/재접속 최신 스냅샷, 이벤트 이름 ranking, JSON 배열.
- VoteAggregationService는 Lua 집계 true일 때만 VoteRankingPublisher를 호출. `live-lunch-choice:ranking-changed` 채널 본문은 eventId. 중복 false에는 발행 없음.
- RedisMessageListenerContainer와 VoteRankingSubscriber는 알림을 받으면 최신 랭킹 한 번 조회 후 서버의 로컬 SSE 연결에 전송. `ConcurrentHashMap.newKeySet` 및 초기 조회/전송과 변경 조회/전송의 동일 모니터 동기화로 오래된 초기 스냅샷의 역전 방지.
- 완료/오류/타임아웃 콜백 및 전송 실패에서 연결 목록 정리. I/O 전송 실패의 응답 종료는 Servlet/Spring에 맡김. 느린 연결을 위한 별도 큐/부하 최적화는 범위 밖.
- 기존 `/api/votes/results`는 DB 집계 그대로 유지. 동점 DB API는 숫자 ID 오름차순, Redis API는 역 사전식 member 순서 유지. 실제 테스트에서 점수4 ID1 → 점수3 ID2 → 점수3 ID10 확인.

## 프론트 결합 계약
- `/api/candidates`의 후보 ID·이름과 랭킹 ID를 결합한다. 랭킹 배열 순서를 유지하고 누락된 후보는 0표로 뒤에 표시한다.
- SSE는 증가량이 아니라 전체 최신 스냅샷이다. 이전 표 수에 더하지 않고 목록을 교체한다.
- 이름 조회용 DB 접근, 후보명 캐시, 0표 후보 생성, UI는 이번 랭킹 경로에 추가하지 않았다. 기존 DB 투표 전체가 Redis에 들어 있다고 가정하지 않는다.

## 실제 실행 환경 및 명령
- Java17, Spring Boot4.1.1, 임시 Testcontainers Redis `redis:8`, 임시 Kafka `apache/kafka:4.1.2`, 테스트 전용 H2, Spring Boot 랜덤 HTTP 포트. 앱 한 인스턴스의 두 SSE 연결.
- 기존 사용자 PostgreSQL/Redis/Kafka 데이터는 접근·초기화하지 않았다. 테스트의 키 삭제는 해당 테스트가 만든 임시 Redis 안에서만 수행. 기존 앱 재시작, 토픽 삭제, offset reset 없음.
- 실행: `./gradlew test redisTest bootJar` → BUILD SUCCESSFUL (최종 실행 20초). 일반 54건, redisTest 9건, 합계63건, 실패/오류/건너뜀0. `git diff --check` 통과.
- 일반 테스트는 `app.ranking.subscription-enabled=false`, Kafka listener 자동 시작 비활성화, H2 사용으로 외부 서비스 불필요. 실제 검증은 redis 태그/redisTest로 분리.
- 일반 테스트만: `./gradlew test`
- 실제 Redis·Kafka·HTTP/SSE만: `./gradlew redisTest`
- 수동 랭킹 조회: `curl -s http://localhost:8080/api/votes/rankings`
- 수동 SSE 접속(별도 터미널 두 개에서 실행): `curl -N http://localhost:8080/api/votes/rankings/stream`
- 위 curl은 재사용 안내이며 이번 실제 HTTP 검증 클라이언트는 Java HttpClient다. 사용자 8080 앱을 새 빌드로 교체하지 않았다.

## 검증 내용
| 항목 | 직접 관찰 결과 |
| --- | --- |
| 일반 회귀 | 기존 후보·투표·예외·Lua 호출/Consumer 분기 등 포함54건 통과 |
| REST 랭킹 | 배열 순서 유지, 이름 필드 없음, 빈 결과 [], HTTP200 |
| SSE 일반 테스트 | 초기 named JSON, 두 연결 전송, 완료/오류/타임아웃 정리, 한 연결 전송 실패 격리, 초기 조회 실패 정리, 초기/변경 순서 보장 |
| 실제 Redis | Lua6건 기존 회귀 + 빈 랭킹·점수/역사전식 동점 순서2건 통과 |
| Pub/Sub 준비 | isListening 구독 완료 확인 후 투표 시작; 고정 sleep 없이 제한시간 조건 대기 |
| HTTP→Kafka→Redis→Pub/Sub→SSE | 실제 HTTP 후보201·투표201, Kafka offset0 소비, Redis 후보1 점수1, 실제 Pub/Sub 발행/수신, 두 HTTP SSE 연결 각각 갱신 |
| DB 확인 | 격리 H2 VoteRepository.count()=1, DB 결과 API200/voteCount1 |
| 동일 이벤트 재전달 | 같은 eventId를 실제 Kafka offset1로 발행, 그룹 커밋 위치2 확인, 점수1 유지, Publisher/Subscriber 각각 총1회 |
| 추가 알림 없음 | 중복 소비 커밋 후 두 SSE 수신 큐를 각각750ms 제한시간 확인해 추가 ranking 메시지 없음; 무한 시간 보장 실험은 아님 |
| 재접속 | 첫 연결을 닫고 새 HTTP SSE 연결 시 초기 최신1표 수신 |

초기 SSE(두 연결 모두):
```text
event:ranking
data:[]

```
변경 SSE(두 연결 모두), 재접속 초기 SSE:
```text
event:ranking
data:[{"candidateId":1,"voteCount":1}]

```

## 최종 실행 원본 증거
```text
2026-10-16T00:06:11.186+09:00  INFO 13585 --- [    Test worker] o.s.boot.tomcat.TomcatWebServer          : Tomcat started on port 51164 (http) with context path '/'
2026-10-16T00:06:11.929+09:00  INFO 13585 --- [ad | producer-1] o.e.l.vote.messaging.VoteEventPublisher  : PUBLISH_SUCCEEDED eventId=f77fde70-62ac-4cd4-a947-82b3b2c4b804 topic=vote-created partition=0 offset=0
2026-10-16T00:06:11.939+09:00  INFO 13585 --- [-listener-0-C-1] o.e.l.vote.messaging.VoteEventConsumer   : CONSUMER_RECEIVED eventId=f77fde70-62ac-4cd4-a947-82b3b2c4b804 voteId=1 candidateId=1 topic=vote-created partition=0 offset=0
2026-10-16T00:06:11.950+09:00  INFO 13585 --- [-listener-0-C-1] o.e.l.vote.redis.VoteRankingPublisher    : RANKING_CHANGE_PUBLISHED eventId=f77fde70-62ac-4cd4-a947-82b3b2c4b804
2026-10-16T00:06:11.950+09:00  INFO 13585 --- [-listener-0-C-1] o.e.l.vote.messaging.VoteEventConsumer   : VOTE_AGGREGATED eventId=f77fde70-62ac-4cd4-a947-82b3b2c4b804 voteId=1 candidateId=1
2026-10-16T00:06:11.951+09:00  INFO 13585 --- [enerContainer-1] o.e.l.vote.redis.VoteRankingSubscriber   : RANKING_CHANGE_RECEIVED eventId=f77fde70-62ac-4cd4-a947-82b3b2c4b804
2026-10-16T00:06:12.101+09:00  INFO 13585 --- [-listener-0-C-1] o.e.l.vote.messaging.VoteEventConsumer   : CONSUMER_RECEIVED eventId=f77fde70-62ac-4cd4-a947-82b3b2c4b804 voteId=1 candidateId=1 topic=vote-created partition=0 offset=1
2026-10-16T00:06:12.102+09:00  INFO 13585 --- [-listener-0-C-1] o.e.l.vote.messaging.VoteEventConsumer   : VOTE_DUPLICATE_SKIPPED eventId=f77fde70-62ac-4cd4-a947-82b3b2c4b804 voteId=1 candidateId=1
SSE_E2E eventId=f77fde70-62ac-4cd4-a947-82b3b2c4b804 voteId=1 candidateId=1 duplicateOffset=1 committed=2 initial=[] first=[{"candidateId":1,"voteCount":1}] second=[{"candidateId":1,"voteCount":1}] score=0->1->1 HTTP(candidate/vote/rankings/results)=201/201/200/200 DBvotes=1 pubsub=1 reconnect=latest
```

- 동일 eventId 신규 offset0 → 중복 offset1 → 커밋 위치2. Redis 점수 0(키/멤버 없음)→1→1.
- 정상 집계에서만 Pub/Sub 발행/수신 각1회. Pub/Sub/SSE 경로에는 mock 대체가 없으며 publisher/subscriber spy는 실제 메서드 실행을 관찰한다.
- XML/HTML 원본: build/test-results/test, build/test-results/redisTest, build/reports/tests/test, build/reports/tests/redisTest. 후속 실행으로 덮어쓸 수 있어 위 핵심 증거를 문서에 보존했다.

## 중간 문제와 수정
- 첫 E2E 컴파일에서 LocalServerPort import 패키지 오류. @Value의 local.server.port 주입으로 수정 후 통과. 제품 코드 오류가 아닌 테스트 코드 오류.
- 첫 전체 통합 검증은 통과했지만 컨테이너가 앱보다 먼저 종료돼 Kafka 연결 경고와 SSE graceful shutdown 대기 발생. 테스트에 DirtiesContext(AFTER_EACH_TEST_METHOD), 테스트 프로필 server.shutdown=immediate를 적용해 앱을 먼저 종료하도록 보완. 최종 재실행 통과. 운영 서버 종료 정책은 변경하지 않음.

## 미검증·제외 항목
- 실제 PostgreSQL과 이번 SSE를 결합한 E2E는 미실행. 이번 실제 HTTP E2E의 DB는 격리 H2.
- 브라우저 EventSource의 60초 타임아웃 뒤 자동 재연결은 미실행. 수동 HTTP 재접속과 콜백 정리 단위 테스트는 수행.
- 다중 서버 Pub/Sub 전파, 고부하, 느린 연결 성능, 브로커/Redis 장애·프로세스 강제 종료, 자동 복구·보상은 미구현/미실행.
- Pub/Sub은 알림을 보관하지 않고 집계/발행은 별도 호출. 합의한 한계를 유지하며 Outbox/DLT/재집계/클러스터 등으로 확장하지 않음.

## 2026-10-16 15:15 KST — 사용자 로컬 PostgreSQL·Kafka·Redis 실제 시연 추가
- 기존8080 앱에서 rankings404를 확인. 최신 bootJar 빌드 성공 후 기존 PID44558 정상 종료, local 프로필 최신 앱 PID16676을127.0.0.1:8080에 실행. 로그 /tmp/lunch-live-sse.log. 앱은 시연 종료 후 실행 상태 유지.
- 이번에는 임시 테스트 환경이 아닌 기존 로컬 PostgreSQL(live_lunch_choice), lunch-kafka, lunch-redis 사용. 기존 후보·투표·점수 유지하고 확인용 투표1건만 추가했다. 실험 데이터는 삭제하지 않음.
- 일식(candidateId1): DB/Redis3표→4표. 양식1표/한식0표 유지. HTTP201 voteId5, userId live-sse-86536c208200.
- 별도 redis-cli SUBSCRIBE의 구독 확인 응답을 받은 뒤 HTTP SSE 두 연결 초기 상태를 수신하고 POST 실행. 동일 eventId ee0a46f2-a906-4ca2-950f-dc3a29fb4f43의 실제 Pub/Sub 메시지와 두 SSE 갱신 확인.
- Kafka CURRENT/END/LAG=12/12/0→13/13/0, 신규 이벤트 offset12. 처리 이력 SISMEMBER=1.
- PostgreSQL 읽기 전용 SQL SELECT id,user_id,candidate_id FROM votes WHERE id=5 결과: 5 / live-sse-86536c208200 / 1. DB API 결과와 일치.
- 원본 증거 파일 /tmp/lunch-live-sse-evidence.json. 아래에도 보존.
```json
{
  "base": "http://127.0.0.1:8080",
  "candidates": [
    {
      "id": 1,
      "name": "일식"
    },
    {
      "id": 2,
      "name": "양식"
    },
    {
      "id": 3,
      "name": "한식"
    }
  ],
  "candidate": {
    "id": 1,
    "name": "일식"
  },
  "ranking_before": [
    {
      "candidateId": 1,
      "voteCount": 3
    },
    {
      "candidateId": 2,
      "voteCount": 1
    }
  ],
  "db_before": [
    {
      "candidateId": 1,
      "name": "일식",
      "voteCount": 3
    },
    {
      "candidateId": 2,
      "name": "양식",
      "voteCount": 1
    },
    {
      "candidateId": 3,
      "name": "한식",
      "voteCount": 0
    }
  ],
  "score_before": 3.0,
  "group_before": "\nGROUP           TOPIC           PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG             CONSUMER-ID                                                    HOST            CLIENT-ID\nvote-log-group  vote-created    0          12              12              0               consumer-vote-log-group-1-5f5a7ff7-f94b-434b-b38a-f70b425fcbfb /172.17.0.1     consumer-vote-log-group-1\n",
  "subscribe_ack": [
    "subscribe",
    "live-lunch-choice:ranking-changed",
    "1"
  ],
  "sse_initial": [
    [
      {
        "candidateId": 1,
        "voteCount": 3
      },
      {
        "candidateId": 2,
        "voteCount": 1
      }
    ],
    [
      {
        "candidateId": 1,
        "voteCount": 3
      },
      {
        "candidateId": 2,
        "voteCount": 1
      }
    ]
  ],
  "request": {
    "userId": "live-sse-86536c208200",
    "candidateId": 1
  },
  "http_status": 201,
  "vote": {
    "id": 5,
    "userId": "live-sse-86536c208200",
    "candidateId": 1,
    "createdAt": "2026-10-16T06:14:34.922961Z"
  },
  "pubsub": [
    "message",
    "live-lunch-choice:ranking-changed",
    "ee0a46f2-a906-4ca2-950f-dc3a29fb4f43"
  ],
  "eventId": "ee0a46f2-a906-4ca2-950f-dc3a29fb4f43",
  "sse_updated": [
    [
      {
        "candidateId": 1,
        "voteCount": 4
      },
      {
        "candidateId": 2,
        "voteCount": 1
      }
    ],
    [
      {
        "candidateId": 1,
        "voteCount": 4
      },
      {
        "candidateId": 2,
        "voteCount": 1
      }
    ]
  ],
  "score_after": 4.0,
  "ranking_after": [
    {
      "candidateId": 1,
      "voteCount": 4
    },
    {
      "candidateId": 2,
      "voteCount": 1
    }
  ],
  "db_after": [
    {
      "candidateId": 1,
      "name": "일식",
      "voteCount": 4
    },
    {
      "candidateId": 2,
      "name": "양식",
      "voteCount": 1
    },
    {
      "candidateId": 3,
      "name": "한식",
      "voteCount": 0
    }
  ],
  "group_after": "\nGROUP           TOPIC           PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG             CONSUMER-ID                                                    HOST            CLIENT-ID\nvote-log-group  vote-created    0          13              13              0               consumer-vote-log-group-1-5f5a7ff7-f94b-434b-b38a-f70b425fcbfb /172.17.0.1     consumer-vote-log-group-1\n",
  "processed": "1",
  "logs": [
    "2026-10-16T15:14:34.943+09:00  INFO 16676 --- [live-lunch-choice] [http-nio-127.0.0.1-8080-exec-6] o.e.l.v.event.VotedCreatedEventListener  : VOTE_COMMITTED eventId=ee0a46f2-a906-4ca2-950f-dc3a29fb4f43 voteId=5",
    "2026-10-16T15:14:34.943+09:00  INFO 16676 --- [live-lunch-choice] [http-nio-127.0.0.1-8080-exec-6] o.e.l.vote.messaging.VoteEventPublisher  : PUBLISH_REQUESTED eventId=ee0a46f2-a906-4ca2-950f-dc3a29fb4f43 voteId=5",
    "2026-10-16T15:14:35.002+09:00  INFO 16676 --- [live-lunch-choice] [kafka-producer-network-thread | live-lunch-choice-producer-1] o.e.l.vote.messaging.VoteEventPublisher  : PUBLISH_SUCCEEDED eventId=ee0a46f2-a906-4ca2-950f-dc3a29fb4f43 topic=vote-created partition=0 offset=12",
    "2026-10-16T15:14:35.006+09:00  INFO 16676 --- [live-lunch-choice] [vote-log-listener-0-C-1] o.e.l.vote.messaging.VoteEventConsumer   : CONSUMER_RECEIVED eventId=ee0a46f2-a906-4ca2-950f-dc3a29fb4f43 voteId=5 candidateId=1 topic=vote-created partition=0 offset=12",
    "2026-10-16T15:14:35.033+09:00  INFO 16676 --- [live-lunch-choice] [vote-log-listener-0-C-1] o.e.l.vote.redis.VoteRankingPublisher    : RANKING_CHANGE_PUBLISHED eventId=ee0a46f2-a906-4ca2-950f-dc3a29fb4f43",
    "2026-10-16T15:14:35.033+09:00  INFO 16676 --- [live-lunch-choice] [vote-log-listener-0-C-1] o.e.l.vote.messaging.VoteEventConsumer   : VOTE_AGGREGATED eventId=ee0a46f2-a906-4ca2-950f-dc3a29fb4f43 voteId=5 candidateId=1",
    "2026-10-16T15:14:35.034+09:00  INFO 16676 --- [live-lunch-choice] [rankingListenerContainer-1] o.e.l.vote.redis.VoteRankingSubscriber   : RANKING_CHANGE_RECEIVED eventId=ee0a46f2-a906-4ca2-950f-dc3a29fb4f43"
  ],
  "success": true
}
```
- 이 시연으로 PostgreSQL 결합 정상 HTTP E2E는 직접 검증됨. 앞선 미검증 표기는 당시 테스트 범위다. 브라우저 EventSource 자동 재연결·다중 서버·장애 실험은 여전히 미실행.
- ranking-e2e 프로필은 src/test/resources의 통합 테스트 전용이며 이번 로컬 앱은 local 사용. 일반 실행 JAR에는 테스트 프로필이 포함되지 않음.
