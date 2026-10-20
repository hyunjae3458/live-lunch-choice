# Consumer 중단·재개 실험 결과

## 결론

2026-10-14(KST) 실제 로컬 Kafka 실험 성공. Consumer 중단 중에도 새 투표 3건이 각각 HTTP 201로 DB 저장 및 Kafka 발행에 성공했다. 해당 이벤트는 중단 중 수신되지 않았으며 CURRENT-OFFSET=5 유지, LOG-END-OFFSET=8, LAG=3을 관측했다. 재개 후 동일 이벤트가 파티션 0의 offset 5 → 6 → 7 순으로 수신됐고 최종 8/8/0으로 회복했다.

이 결과는 CLI가 HTTP·H2 SQL·앱 로그·Kafka CLI로 직접 확인했다. 과거 사용자 보고와 구분하며 아래 숫자는 예시가 아닌 실제 관측값이다.

## 구현

- `src/main/java/org/example/livelunchchoice/vote/messaging/local/LocalConsumerController.java` 추가.
- `@Profile("local")`, `KafkaListenerEndpointRegistry.getListenerContainer("vote-log-listener")` 사용.
- POST `/api/local/kafka/consumer/stop`: 컨테이너 종료 콜백 완료까지 대기 후 204. 종료 대기 최대 30초, 대기 실패/인터럽트는 503.
- POST `/api/local/kafka/consumer/start`: 실행 중이 아니면 start 호출 후 204. 파티션 할당/처리 완료 응답은 아님.
- GET `/api/local/kafka/consumer`: listenerId/running 반환. 컨테이너 미존재는 404.
- start/stop 명령은 synchronized로 직렬화. Consumer 메서드, Producer, 토픽/그룹 설정 및 업무 로직은 변경하지 않음.
- 테스트 `LocalConsumerControllerTests` 6건: 종료 콜백, 시작, 반복 호출, 상태, 미존재 404, local 프로필 한정 등록.
- `./gradlew test bootJar` 성공: 기존 37건 + 신규 6건 = 43건, 실패/오류/건너뜀 0. 최초 샌드박스 Gradle 캐시 접근 실패 후 권한 확장 재실행. mock 테스트는 실제 실험과 별도 근거다.

## 환경과 준비

| 항목 | 실제 값 |
|---|---|
| 앱 | Spring Boot 4.1.1, Java 17.0.20, local 프로필 |
| 주소 / PID | http://127.0.0.1:8080 / 41406 |
| 브로커 | 기존 lunch-kafka, apache/kafka:4.1.2, localhost:9092 |
| 토픽 / 그룹 / 리스너 | vote-created / vote-log-group / vote-log-listener |
| 파티션 / 복제 계수 / Leader | 1개(0번) / 1 / 브로커 1 |
| DB | jdbc:h2:mem:live_lunch_choice |
| 실험 구간 | 기준 15:05:02 ~ 최종 상태 확인 15:05:54 KST |
| 로그 | /tmp/lunch-consumer-experiment/app.log |

기존 8080 앱 PID 38829와 후보 2건을 확인했다. 새 제어 API 적용에 재시작이 필요하고 H2 데이터가 사라짐을 사용자에게 알렸으며 **사용자가 재시작을 명시적으로 승인**했다. 해당 PID만 TERM으로 정상 종료한 뒤 새 앱을 한 번 실행했다. 실험 시작 후 앱·Producer·브로커는 재시작하지 않았다. 다른 PostgreSQL 컨테이너는 건드리지 않았다. 토픽 삭제, offset reset, SQL DELETE/TRUNCATE 및 사용자 데이터 정리 명령은 실행하지 않았다.

새 앱 후보 목록이 []인 것을 확인하여 실험 후보 `중단재개실험`(id=1)을 등록했다. 이전 Kafka 로그/그룹 offset은 그대로 보존되어 앱 시작 시 기존 committed offset=4에서 참여했다. 기준 투표 한 건으로 offset 4를 소비·커밋한 다음 실험을 시작했다.

실제 앱 실행 명령:

```bash
java -jar build/libs/live-lunch-choice-0.0.1-SNAPSHOT.jar --spring.profiles.active=local --server.address=127.0.0.1 --logging.file.name=/tmp/lunch-consumer-experiment/app.log
```

토픽 조회 명령 및 원본 출력:

```bash
docker exec lunch-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic vote-created
```

```text
Topic: vote-created	TopicId: 8sy5aYN_Q-Wb_09JkDCSyQ	PartitionCount: 1	ReplicationFactor: 1	Configs: min.insync.replicas=1,segment.bytes=1073741824
	Topic: vote-created	Partition: 0	Leader: 1	Replicas: 1	Isr: 1	Elr: 	LastKnownElr:
```

## offset / lag 변화

| 단계 | 관측 시각(KST) | CURRENT-OFFSET | LOG-END-OFFSET | LAG | 활성 구성원 |
|---|---|---:|---:|---:|---:|
| 기준 투표 소비·커밋 후 | 15:05:06.269 | 5 | 5 | 0 | 1 |
| Consumer 중단 후 3건 발행 | 15:05:30.250 | 5 | 8 | 3 | 0 |
| Consumer 재개 후 | 15:05:53.214 | 8 | 8 | 0 | 1 |

기대값은 다른 트래픽 없는 단일 파티션·비트랜잭션 Producer 기준 lag 3이었다. 실제 관측도 3이었다. 그룹 재개 시 Consumer ID는 바뀌었지만 groupId는 유지됐으며 커밋된 위치 5부터 읽었다.

그룹 조회 원본 출력(각 명령 exit=0):

### before-members — 2026-10-14T15:05:02.450661+09:00

```bash
docker exec lunch-kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group vote-log-group --members
```

```text
GROUP           CONSUMER-ID                                                    HOST            CLIENT-ID                 #PARTITIONS     
vote-log-group  consumer-vote-log-group-1-9a4bba58-8a81-4446-be86-91326629533f /172.17.0.1     consumer-vote-log-group-1 1
```

### baseline — 2026-10-14T15:05:06.269338+09:00

```bash
docker exec lunch-kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group vote-log-group
```

```text
GROUP           TOPIC           PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG             CONSUMER-ID                                                    HOST            CLIENT-ID
vote-log-group  vote-created    0          5               5               0               consumer-vote-log-group-1-9a4bba58-8a81-4446-be86-91326629533f /172.17.0.1     consumer-vote-log-group-1
```

### stopped-members — 2026-10-14T15:05:23.787180+09:00

```bash
docker exec lunch-kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group vote-log-group --members
```

```text
GROUP           CONSUMER-ID     HOST            CLIENT-ID       #PARTITIONS     

Consumer group 'vote-log-group' has no active members.
```

### stopped-three-votes — 2026-10-14T15:05:30.250635+09:00

```bash
docker exec lunch-kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group vote-log-group
```

```text
GROUP           TOPIC           PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG             CONSUMER-ID     HOST            CLIENT-ID
vote-log-group  vote-created    0          5               8               3               -               -               -

Consumer group 'vote-log-group' has no active members.
```

### resumed — 2026-10-14T15:05:53.214860+09:00

```bash
docker exec lunch-kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group vote-log-group
```

```text
GROUP           TOPIC           PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG             CONSUMER-ID                                                    HOST            CLIENT-ID
vote-log-group  vote-created    0          8               8               0               consumer-vote-log-group-2-831cd0cb-d992-4184-a9fd-03f9f5eb6467 /172.17.0.1     consumer-vote-log-group-2
```

### resumed-members — 2026-10-14T15:05:54.474832+09:00

```bash
docker exec lunch-kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group vote-log-group --members
```

```text
GROUP           CONSUMER-ID                                                    HOST            CLIENT-ID                 #PARTITIONS     
vote-log-group  consumer-vote-log-group-2-831cd0cb-d992-4184-a9fd-03f9f5eb6467 /172.17.0.1     consumer-vote-log-group-2 1
```

## HTTP 요청·응답 원본

실제 요청은 Python urllib로 순차 수행했다. 아래는 기록한 경로·본문·상태·응답이며 생략 없이 저장한다. POST의 Content-Type은 application/json, 호스트는 localhost:8080이다. GET의 request=null은 요청 본문 없음이다.

```json
[
  {
    "time": "2026-10-14T15:05:00.177297+09:00",
    "path": "/api/local/kafka/consumer",
    "request": null,
    "status": 200,
    "body": "{\"listenerId\":\"vote-log-listener\",\"running\":true}"
  },
  {
    "time": "2026-10-14T15:05:02.619540+09:00",
    "path": "/api/candidates",
    "request": null,
    "status": 200,
    "body": "[]"
  },
  {
    "time": "2026-10-14T15:05:02.755054+09:00",
    "path": "/api/candidates",
    "request": {
      "name": "중단재개실험"
    },
    "status": 201,
    "body": "{\"id\":1,\"name\":\"중단재개실험\"}"
  },
  {
    "time": "2026-10-14T15:05:02.853634+09:00",
    "path": "/api/votes",
    "request": {
      "userId": "stop-resume-20261014-150502-baseline",
      "candidateId": 1
    },
    "status": 201,
    "body": "{\"id\":1,\"userId\":\"stop-resume-20261014-150502-baseline\",\"candidateId\":1,\"createdAt\":\"2026-10-14T06:05:02.801633Z\"}"
  },
  {
    "time": "2026-10-14T15:05:06.296689+09:00",
    "path": "/api/votes/results",
    "request": null,
    "status": 200,
    "body": "[{\"candidateId\":1,\"name\":\"중단재개실험\",\"voteCount\":1}]"
  },
  {
    "time": "2026-10-14T15:05:22.153176+09:00",
    "path": "/api/local/kafka/consumer/stop",
    "request": {},
    "status": 204,
    "body": ""
  },
  {
    "time": "2026-10-14T15:05:22.156491+09:00",
    "path": "/api/local/kafka/consumer",
    "request": null,
    "status": 200,
    "body": "{\"listenerId\":\"vote-log-listener\",\"running\":false}"
  },
  {
    "time": "2026-10-14T15:05:23.813995+09:00",
    "path": "/api/votes",
    "request": {
      "userId": "stop-resume-20261014-150502-1",
      "candidateId": 1
    },
    "status": 201,
    "body": "{\"id\":2,\"userId\":\"stop-resume-20261014-150502-1\",\"candidateId\":1,\"createdAt\":\"2026-10-14T06:05:23.805754Z\"}"
  },
  {
    "time": "2026-10-14T15:05:23.833496+09:00",
    "path": "/api/votes",
    "request": {
      "userId": "stop-resume-20261014-150502-2",
      "candidateId": 1
    },
    "status": 201,
    "body": "{\"id\":3,\"userId\":\"stop-resume-20261014-150502-2\",\"candidateId\":1,\"createdAt\":\"2026-10-14T06:05:23.824148Z\"}"
  },
  {
    "time": "2026-10-14T15:05:23.844473+09:00",
    "path": "/api/votes",
    "request": {
      "userId": "stop-resume-20261014-150502-3",
      "candidateId": 1
    },
    "status": 201,
    "body": "{\"id\":4,\"userId\":\"stop-resume-20261014-150502-3\",\"candidateId\":1,\"createdAt\":\"2026-10-14T06:05:23.841086Z\"}"
  },
  {
    "time": "2026-10-14T15:05:30.273546+09:00",
    "path": "/api/votes/results",
    "request": null,
    "status": 200,
    "body": "[{\"candidateId\":1,\"name\":\"중단재개실험\",\"voteCount\":4}]"
  },
  {
    "time": "2026-10-14T15:05:46.790617+09:00",
    "path": "/api/local/kafka/consumer/start",
    "request": {},
    "status": 204,
    "body": ""
  },
  {
    "time": "2026-10-14T15:05:54.502806+09:00",
    "path": "/api/votes/results",
    "request": null,
    "status": 200,
    "body": "[{\"candidateId\":1,\"name\":\"중단재개실험\",\"voteCount\":4}]"
  },
  {
    "time": "2026-10-14T15:05:54.506463+09:00",
    "path": "/api/local/kafka/consumer",
    "request": null,
    "status": 200,
    "body": "{\"listenerId\":\"vote-log-listener\",\"running\":true}"
  }
]
```

실제 단계 실행 명령(임시 실험 스크립트이며 앱 기능에 포함하지 않음):

```bash
python3 /tmp/lunch-consumer-experiment/run.py baseline
```

```bash
python3 /tmp/lunch-consumer-experiment/run.py stop
```

```bash
python3 /tmp/lunch-consumer-experiment/run.py start
```

## 이벤트별 대조

| 구분 | voteId | eventId | 파티션 / offset | 발행 성공(KST) | Consumer 수신(KST) |
|---|---:|---|---|---|---|
| 기준 | 1 | 1d4cacfa-65ad-4d10-b60c-f585407fb3a5 | 0 / 4 | 15:05:02.870+09:00 | 15:05:02.877+09:00 |
| 중단 중 1 | 2 | c81d3cdb-2bf9-45e8-b051-ca60c1f43eda | 0 / 5 | 15:05:23.843+09:00 | 15:05:49.884+09:00 |
| 중단 중 2 | 3 | 9c7050f0-fa65-47d6-9111-645317c7e776 | 0 / 6 | 15:05:23.844+09:00 | 15:05:49.894+09:00 |
| 중단 중 3 | 4 | 250ca28b-9b1f-4ca7-b01b-6a448a79fdc7 | 0 / 7 | 15:05:23.851+09:00 | 15:05:49.903+09:00 |

중단 완료는 15:05:22.153의 HTTP 204 및 running=false, 15:05:23.787의 그룹 활성 구성원 0명으로 확인했다. 중단 로그 스냅샷(15:05:30.274경)에서 voteId 2/3/4의 발행 성공은 각 1건, 해당 eventId의 CONSUMER_RECEIVED는 0건이었다. 재개 요청 완료(15:05:46.790)까지 수신 로그가 없으며 실제 최초 수신은 파티션 재할당 뒤 15:05:49.884였다. 최종 로그에서도 세 건은 각각 한 번씩 관측했다. 이는 이번 정상 중단 실험의 관측이며 일반적인 중복 없음/정확히 한 번 보장은 아니다.

앱 원본 로그 중 실험 관련 행:

```text
2026-10-14T15:03:54.533+09:00  INFO 41406 --- [live-lunch-choice] [vote-log-listener-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : vote-log-group: partitions assigned: [vote-created-0]
2026-10-14T15:05:02.809+09:00  INFO 41406 --- [live-lunch-choice] [http-nio-127.0.0.1-8080-exec-9] o.e.l.v.event.VotedCreatedEventListener  : VOTE_COMMITTED eventId=1d4cacfa-65ad-4d10-b60c-f585407fb3a5 voteId=1
2026-10-14T15:05:02.810+09:00  INFO 41406 --- [live-lunch-choice] [http-nio-127.0.0.1-8080-exec-9] o.e.l.vote.messaging.VoteEventPublisher  : PUBLISH_REQUESTED eventId=1d4cacfa-65ad-4d10-b60c-f585407fb3a5 voteId=1
2026-10-14T15:05:02.870+09:00  INFO 41406 --- [live-lunch-choice] [kafka-producer-network-thread | live-lunch-choice-producer-1] o.e.l.vote.messaging.VoteEventPublisher  : PUBLISH_SUCCEEDED eventId=1d4cacfa-65ad-4d10-b60c-f585407fb3a5 topic=vote-created partition=0 offset=4
2026-10-14T15:05:02.877+09:00  INFO 41406 --- [live-lunch-choice] [vote-log-listener-0-C-1] o.e.l.vote.messaging.VoteEventConsumer   : CONSUMER_RECEIVED eventId=1d4cacfa-65ad-4d10-b60c-f585407fb3a5 voteId=1 candidateId=1 topic=vote-created partition=0 offset=4
2026-10-14T15:05:22.150+09:00  INFO 41406 --- [live-lunch-choice] [vote-log-listener-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : vote-log-group: Consumer stopped
2026-10-14T15:05:23.811+09:00  INFO 41406 --- [live-lunch-choice] [http-nio-127.0.0.1-8080-exec-9] o.e.l.v.event.VotedCreatedEventListener  : VOTE_COMMITTED eventId=c81d3cdb-2bf9-45e8-b051-ca60c1f43eda voteId=2
2026-10-14T15:05:23.812+09:00  INFO 41406 --- [live-lunch-choice] [http-nio-127.0.0.1-8080-exec-9] o.e.l.vote.messaging.VoteEventPublisher  : PUBLISH_REQUESTED eventId=c81d3cdb-2bf9-45e8-b051-ca60c1f43eda voteId=2
2026-10-14T15:05:23.831+09:00  INFO 41406 --- [live-lunch-choice] [http-nio-127.0.0.1-8080-exec-6] o.e.l.v.event.VotedCreatedEventListener  : VOTE_COMMITTED eventId=9c7050f0-fa65-47d6-9111-645317c7e776 voteId=3
2026-10-14T15:05:23.831+09:00  INFO 41406 --- [live-lunch-choice] [http-nio-127.0.0.1-8080-exec-6] o.e.l.vote.messaging.VoteEventPublisher  : PUBLISH_REQUESTED eventId=9c7050f0-fa65-47d6-9111-645317c7e776 voteId=3
2026-10-14T15:05:23.843+09:00  INFO 41406 --- [live-lunch-choice] [http-nio-127.0.0.1-8080-exec-4] o.e.l.v.event.VotedCreatedEventListener  : VOTE_COMMITTED eventId=250ca28b-9b1f-4ca7-b01b-6a448a79fdc7 voteId=4
2026-10-14T15:05:23.843+09:00  INFO 41406 --- [live-lunch-choice] [http-nio-127.0.0.1-8080-exec-4] o.e.l.vote.messaging.VoteEventPublisher  : PUBLISH_REQUESTED eventId=250ca28b-9b1f-4ca7-b01b-6a448a79fdc7 voteId=4
2026-10-14T15:05:23.843+09:00  INFO 41406 --- [live-lunch-choice] [kafka-producer-network-thread | live-lunch-choice-producer-1] o.e.l.vote.messaging.VoteEventPublisher  : PUBLISH_SUCCEEDED eventId=c81d3cdb-2bf9-45e8-b051-ca60c1f43eda topic=vote-created partition=0 offset=5
2026-10-14T15:05:23.844+09:00  INFO 41406 --- [live-lunch-choice] [kafka-producer-network-thread | live-lunch-choice-producer-1] o.e.l.vote.messaging.VoteEventPublisher  : PUBLISH_SUCCEEDED eventId=9c7050f0-fa65-47d6-9111-645317c7e776 topic=vote-created partition=0 offset=6
2026-10-14T15:05:23.851+09:00  INFO 41406 --- [live-lunch-choice] [kafka-producer-network-thread | live-lunch-choice-producer-1] o.e.l.vote.messaging.VoteEventPublisher  : PUBLISH_SUCCEEDED eventId=250ca28b-9b1f-4ca7-b01b-6a448a79fdc7 topic=vote-created partition=0 offset=7
2026-10-14T15:05:49.864+09:00  INFO 41406 --- [live-lunch-choice] [vote-log-listener-0-C-2] o.s.k.l.KafkaMessageListenerContainer    : vote-log-group: partitions assigned: [vote-created-0]
2026-10-14T15:05:49.884+09:00  INFO 41406 --- [live-lunch-choice] [vote-log-listener-0-C-2] o.e.l.vote.messaging.VoteEventConsumer   : CONSUMER_RECEIVED eventId=c81d3cdb-2bf9-45e8-b051-ca60c1f43eda voteId=2 candidateId=1 topic=vote-created partition=0 offset=5
2026-10-14T15:05:49.894+09:00  INFO 41406 --- [live-lunch-choice] [vote-log-listener-0-C-2] o.e.l.vote.messaging.VoteEventConsumer   : CONSUMER_RECEIVED eventId=9c7050f0-fa65-47d6-9111-645317c7e776 voteId=3 candidateId=1 topic=vote-created partition=0 offset=6
2026-10-14T15:05:49.903+09:00  INFO 41406 --- [live-lunch-choice] [vote-log-listener-0-C-2] o.e.l.vote.messaging.VoteEventConsumer   : CONSUMER_RECEIVED eventId=250ca28b-9b1f-4ca7-b01b-6a448a79fdc7 voteId=4 candidateId=1 topic=vote-created partition=0 offset=7
```

## DB 조회 증거

기존 H2 웹 콘솔(`/h2-console/`)을 통해 실행 중인 동일 앱의 메모리 DB에 접속하여 다음 SELECT를 수행했다. 조회만 수행했으며 별도 DB 확인 API는 추가하지 않았다.

```sql
SELECT ID, USER_ID, CANDIDATE_ID, CREATED_AT FROM VOTES ORDER BY ID;
```

- baseline (2026-10-14T15:05:06.278344+09:00):

```text
H2 Console SELECT ID, USER_ID, CANDIDATE_ID, CREATED_AT FROM VOTES ORDER BY ID;  ID USER_ID CANDIDATE_ID CREATED_AT 1 stop-resume-20261014-150502-baseline 1 2026-10-14 06:05:02.801633+00 (1 row, 0 ms)
```

- stopped (2026-10-14T15:05:30.265816+09:00):

```text
H2 Console SELECT ID, USER_ID, CANDIDATE_ID, CREATED_AT FROM VOTES ORDER BY ID;  ID USER_ID CANDIDATE_ID CREATED_AT 1 stop-resume-20261014-150502-baseline 1 2026-10-14 06:05:02.801633+00 2 stop-resume-20261014-150502-1 1 2026-10-14 06:05:23.805754+00 3 stop-resume-20261014-150502-2 1 2026-10-14 06:05:23.824148+00 4 stop-resume-20261014-150502-3 1 2026-10-14 06:05:23.841086+00 (4 rows, 1 ms)
```

- resumed (2026-10-14T15:05:54.493305+09:00):

```text
H2 Console SELECT ID, USER_ID, CANDIDATE_ID, CREATED_AT FROM VOTES ORDER BY ID;  ID USER_ID CANDIDATE_ID CREATED_AT 1 stop-resume-20261014-150502-baseline 1 2026-10-14 06:05:02.801633+00 2 stop-resume-20261014-150502-1 1 2026-10-14 06:05:23.805754+00 3 stop-resume-20261014-150502-2 1 2026-10-14 06:05:23.824148+00 4 stop-resume-20261014-150502-3 1 2026-10-14 06:05:23.841086+00 (4 rows, 2 ms)
```

기준 시점 1행, 중단 중 및 재개 후 4행을 확인했다. 중단 중 voteId 2/3/4가 이미 DB에 있었고, DB 기반 GET /api/votes/results도 voteCount=4를 반환했다. Consumer는 현재 로그만 출력하므로 결과 조회가 Consumer 재개를 기다리지 않는 것이 현재 설계와 일치한다. DB createdAt의 +00는 UTC이며 실험 로그의 +09는 KST다.

## 소비 후 메시지 보관 확인

그룹 offset을 reset하지 않고 파티션/offset을 직접 지정한 별도 콘솔 Consumer로 세 건을 다시 읽었다. 앱의 vote-log-group을 지정하지 않았다.

```bash
docker exec lunch-kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic vote-created --partition 0 --offset 5 --max-messages 3 --timeout-ms 5000 --property print.partition=true --property print.offset=true --property print.key=true
```

원본 출력:

```text
Partition:0	Offset:5	2	{"eventId":"c81d3cdb-2bf9-45e8-b051-ca60c1f43eda","voteId":2,"candidateId":1,"occurredAt":"2026-10-14T06:05:23.805754Z"}
Partition:0	Offset:6	3	{"eventId":"9c7050f0-fa65-47d6-9111-645317c7e776","voteId":3,"candidateId":1,"occurredAt":"2026-10-14T06:05:23.824148Z"}
Partition:0	Offset:7	4	{"eventId":"250ca28b-9b1f-4ca7-b01b-6a448a79fdc7","voteId":4,"candidateId":1,"occurredAt":"2026-10-14T06:05:23.841086Z"}
Processed a total of 3 messages
```

Consumer가 읽고 커밋해도 메시지가 즉시 삭제되지 않음을 이번 기록에서 직접 확인했다. 장기간 보관 여부는 retention 정책에 달린 별도 사항이다.

## 최종 상태·한계·다음 단계

- 최종 GET 상태 200, `{"listenerId":"vote-log-listener","running":true}`, 그룹 구성원 1명 및 lag=0. 앱과 브로커를 실행 상태로 두고 기준 1건+실험 3건의 H2 데이터도 유지했다.
- 접수/발행과 소비의 실행 의존성이 분리됨을 확인했다. Consumer 중단은 이번 조건에서 접수 실패가 아니라 처리 지연(lag 증가)으로 나타났고 같은 그룹 재개로 따라잡았다.
- HTTP 201은 DB 저장 성공, PUBLISH_SUCCEEDED는 브로커 확인, CONSUMER_RECEIVED는 리스너 진입, CURRENT-OFFSET 증가는 그룹 커밋이라는 별도 증거다. 업무 집계 완료를 뜻하지 않는다.
- 정상 stop/start, 한 파티션·한 브로커, 로그만 남기는 Consumer, H2 조건의 결과다. 브로커 장애·처리 중 강제 종료·재시도/멱등성·정확히 한 번·영속 발행 복구·다중 파티션 순서·PostgreSQL은 미검증이다.
- DB 커밋 후 Kafka 발행 누락 가능성은 그대로다. 이번 실험은 그 문제를 해결하거나 검증한 것이 아니다.
- 제어 API의 30초 종료 대기 실패 경로와 실시간 동시 제어 요청은 이번 실제 실험에서 발생시키지 않았다. start의 204와 running=true만으로 파티션 할당·처리 완료를 판정하지 않았다.
- Docker/프로세스/로컬 네트워크 접근은 샌드박스에서 제한되어 승인된 권한 확장으로 실행했다. 기존 앱 재시작은 별도 사용자 승인 후 수행했다.
- Redis, Pub/Sub, SSE, Outbox, 업무 처리 재시도·멱등성 및 다른 후속 기능은 추가하지 않았다. 다음 실험은 사용자의 별도 학습 요청을 기다린다.
