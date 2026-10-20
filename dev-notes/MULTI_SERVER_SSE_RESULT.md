# 다중 서버 Pub/Sub·SSE 기능 실험 결과

기록: 2026-10-16 15:38 KST. CLI 직접 실행 결과. **성공**. 부하 테스트와 장애 보상은 실행하지 않았다.

## 환경과 준비
- 동일 최신 JAR, 독립 JVM A=127.0.0.1:8080/PID18035, B=127.0.0.1:8081/PID18037. 시작 전 두 포트는 비어 있었으며 사용자 앱 종료 없음.
- 같은 프로젝트 작업 디렉터리/.env/local 프로필, 기존 PostgreSQL live_lunch_choice, lunch-kafka, lunch-redis 공유. 비밀값 출력 없음.
- vote-created 실제 파티션1/복제계수1 확인. groupId vote-log-group 유지. 서버별 client-id만 관측용 multi-sse-A/B로 지정.
- ./gradlew bootJar 성공(UP-TO-DATE). 앱은 순차 시작해 HTTP 응답·Redis 구독 수 증가 확인. 이후 Kafka Stable/멤버2 확인 후 투표.
- 할당: A만 vote-created:0, B는0개. B도 Pub/Sub을 구독하고 자신의 SSE에 전달할 수 있는 구성을 확인.
- 시작 전 기존 Redis 구독1개 보존. A 시작 후2개, B 시작 후3개. 실험 후 다시1개. 기존 구독자의 소유/용도는 확정하지 않음.
- 첫 준비 시도에서는 기존 구독을 고려하지 않고 전체 구독 수=앱 수로 대기해 timeout. 투표는0건, 첫 A(PID17891)는 SIGTERM 정상 종료. 기준 구독 수+실험 앱 수로 검사 보완 후 재실행. 제품 코드 오류 아님.

## 결과 요약
| 요청 서버 | HTTP/voteId | Kafka offset | 실제 집계 서버 | Pub/Sub 수신 | A/B SSE 일식 표 수 |
| --- | --- | --- | --- | --- | --- |
| A:8080 | 201 / 6 | 13 | A 한 번 | A·B 모두 | 4→5 |
| B:8081 | 201 / 7 | 14 | A 한 번 | A·B 모두 | 5→6 |

- 후보1 일식 기준. DB 결과 API4→6, Redis ZSCORE4→5→6: 각각 두 표 증가. 양식1표, 한식 DB0표 유지.
- voteId6 eventId=`aa327cd2-d99c-4b70-b7df-2b74aa1c33eb`
- voteId7 eventId=`ae381744-98b9-4d84-9af4-79ab70ef33f6`
- 각 eventId마다 VOTE_AGGREGATED는 A 로그에서만1회, RANKING_CHANGE_RECEIVED는 A/B 각각 확인. 처리 이력 SISMEMBER는 각각1.
- CURRENT/END/LAG: 13/13/0→15/15/0. 두 투표 순차 실행, 첫 요청의 양쪽 SSE 갱신을 받은 뒤 두 번째 요청.
- SSE는 A8080/B8081에 각각 별도 HTTP 연결. 같은 서버의 두 연결이 아님. 각 연결의 초기 및 변경 ranking JSON 전체 배열을 직접 수신해 비교.
- PostgreSQL 읽기 전용 SELECT로 실제 저장 확인: id6 / multi-sse-A-4d4ac1d6f1d4 / candidate_id1, id7 / multi-sse-B-b149ede1772c / candidate_id1.

최종 두 SSE 연결의 동일 payload:
```text
event:ranking
data:[{"candidateId":1,"voteCount":6},{"candidateId":2,"voteCount":1}]

```

## 실행 명령
빌드: `./gradlew bootJar`

서버 A:
```bash
java -jar build/libs/live-lunch-choice-0.0.1-SNAPSHOT.jar --spring.profiles.active=local --server.address=127.0.0.1 --server.port=8080 --logging.file.name=/tmp/lunch-multi-sse/final/A.log --spring.kafka.client-id=multi-sse-A
```

서버 B:
```bash
java -jar build/libs/live-lunch-choice-0.0.1-SNAPSHOT.jar --spring.profiles.active=local --server.address=127.0.0.1 --server.port=8081 --logging.file.name=/tmp/lunch-multi-sse/final/B.log --spring.kafka.client-id=multi-sse-B
```

실험 실행: `python3 /tmp/lunch-multi-sse/run.py` (투표를 생성하는 스크립트이므로 읽기 전용 확인 명령이 아님).

파티션 확인: `docker exec lunch-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic vote-created`

그룹 상태: `docker exec lunch-kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group vote-log-group --state`

할당 확인: `docker exec lunch-kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group vote-log-group --members --verbose`

소비 위치: `docker exec lunch-kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group vote-log-group`

구독 수: `docker exec lunch-redis redis-cli PUBSUB NUMSUB live-lunch-choice:ranking-changed`

수동 SSE A: `curl -N http://127.0.0.1:8080/api/votes/rankings/stream`

수동 SSE B: `curl -N http://127.0.0.1:8081/api/votes/rankings/stream`

이번 실제 SSE 관측은 Python HTTP 스트림 클라이언트 사용. 위 curl은 재사용 안내이며 브라우저 테스트는 아님.

## 서버별 실제 로그·응답 원본
원본 /tmp/lunch-multi-sse/final/evidence.json, A.log, B.log. 임시 경로 삭제에 대비해 아래 핵심 증거 전체 보존.
```json
{
  "servers": {
    "A": {
      "port": 8080,
      "pid": 18035,
      "command": [
        "java",
        "-jar",
        "build/libs/live-lunch-choice-0.0.1-SNAPSHOT.jar",
        "--spring.profiles.active=local",
        "--server.address=127.0.0.1",
        "--server.port=8080",
        "--logging.file.name=/tmp/lunch-multi-sse/final/A.log",
        "--spring.kafka.client-id=multi-sse-A"
      ]
    },
    "B": {
      "port": 8081,
      "pid": 18037,
      "command": [
        "java",
        "-jar",
        "build/libs/live-lunch-choice-0.0.1-SNAPSHOT.jar",
        "--spring.profiles.active=local",
        "--server.address=127.0.0.1",
        "--server.port=8081",
        "--logging.file.name=/tmp/lunch-multi-sse/final/B.log",
        "--spring.kafka.client-id=multi-sse-B"
      ]
    }
  },
  "rounds": [
    {
      "request_server": "A",
      "request": {
        "userId": "multi-sse-A-4d4ac1d6f1d4",
        "candidateId": 1
      },
      "http_status": 201,
      "vote": {
        "id": 6,
        "userId": "multi-sse-A-4d4ac1d6f1d4",
        "candidateId": 1,
        "createdAt": "2026-10-16T06:36:47.743596Z"
      },
      "sse": {
        "A": [
          {
            "candidateId": 1,
            "voteCount": 5
          },
          {
            "candidateId": 2,
            "voteCount": 1
          }
        ],
        "B": [
          {
            "candidateId": 1,
            "voteCount": 5
          },
          {
            "candidateId": 2,
            "voteCount": 1
          }
        ]
      },
      "score": 5.0,
      "eventId": "aa327cd2-d99c-4b70-b7df-2b74aa1c33eb",
      "logs": {
        "A": [
          "2026-10-16T15:36:47.766+09:00  INFO 18035 --- [live-lunch-choice] [http-nio-127.0.0.1-8080-exec-5] o.e.l.v.event.VotedCreatedEventListener  : VOTE_COMMITTED eventId=aa327cd2-d99c-4b70-b7df-2b74aa1c33eb voteId=6",
          "2026-10-16T15:36:47.767+09:00  INFO 18035 --- [live-lunch-choice] [http-nio-127.0.0.1-8080-exec-5] o.e.l.vote.messaging.VoteEventPublisher  : PUBLISH_REQUESTED eventId=aa327cd2-d99c-4b70-b7df-2b74aa1c33eb voteId=6",
          "2026-10-16T15:36:47.816+09:00  INFO 18035 --- [live-lunch-choice] [kafka-producer-network-thread | multi-sse-A-1] o.e.l.vote.messaging.VoteEventPublisher  : PUBLISH_SUCCEEDED eventId=aa327cd2-d99c-4b70-b7df-2b74aa1c33eb topic=vote-created partition=0 offset=13",
          "2026-10-16T15:36:47.821+09:00  INFO 18035 --- [live-lunch-choice] [vote-log-listener-0-C-1] o.e.l.vote.messaging.VoteEventConsumer   : CONSUMER_RECEIVED eventId=aa327cd2-d99c-4b70-b7df-2b74aa1c33eb voteId=6 candidateId=1 topic=vote-created partition=0 offset=13",
          "2026-10-16T15:36:47.843+09:00  INFO 18035 --- [live-lunch-choice] [vote-log-listener-0-C-1] o.e.l.vote.redis.VoteRankingPublisher    : RANKING_CHANGE_PUBLISHED eventId=aa327cd2-d99c-4b70-b7df-2b74aa1c33eb",
          "2026-10-16T15:36:47.844+09:00  INFO 18035 --- [live-lunch-choice] [vote-log-listener-0-C-1] o.e.l.vote.messaging.VoteEventConsumer   : VOTE_AGGREGATED eventId=aa327cd2-d99c-4b70-b7df-2b74aa1c33eb voteId=6 candidateId=1",
          "2026-10-16T15:36:47.844+09:00  INFO 18035 --- [live-lunch-choice] [rankingListenerContainer-1] o.e.l.vote.redis.VoteRankingSubscriber   : RANKING_CHANGE_RECEIVED eventId=aa327cd2-d99c-4b70-b7df-2b74aa1c33eb"
        ],
        "B": [
          "2026-10-16T15:36:47.844+09:00  INFO 18037 --- [live-lunch-choice] [rankingListenerContainer-1] o.e.l.vote.redis.VoteRankingSubscriber   : RANKING_CHANGE_RECEIVED eventId=aa327cd2-d99c-4b70-b7df-2b74aa1c33eb"
        ]
      },
      "aggregation_servers": [
        "A"
      ],
      "processed": "1"
    },
    {
      "request_server": "B",
      "request": {
        "userId": "multi-sse-B-b149ede1772c",
        "candidateId": 1
      },
      "http_status": 201,
      "vote": {
        "id": 7,
        "userId": "multi-sse-B-b149ede1772c",
        "candidateId": 1,
        "createdAt": "2026-10-16T06:36:48.058902Z"
      },
      "sse": {
        "A": [
          {
            "candidateId": 1,
            "voteCount": 6
          },
          {
            "candidateId": 2,
            "voteCount": 1
          }
        ],
        "B": [
          {
            "candidateId": 1,
            "voteCount": 6
          },
          {
            "candidateId": 2,
            "voteCount": 1
          }
        ]
      },
      "score": 6.0,
      "eventId": "ae381744-98b9-4d84-9af4-79ab70ef33f6",
      "logs": {
        "A": [
          "2026-10-16T15:36:48.124+09:00  INFO 18035 --- [live-lunch-choice] [vote-log-listener-0-C-1] o.e.l.vote.messaging.VoteEventConsumer   : CONSUMER_RECEIVED eventId=ae381744-98b9-4d84-9af4-79ab70ef33f6 voteId=7 candidateId=1 topic=vote-created partition=0 offset=14",
          "2026-10-16T15:36:48.127+09:00  INFO 18035 --- [live-lunch-choice] [vote-log-listener-0-C-1] o.e.l.vote.redis.VoteRankingPublisher    : RANKING_CHANGE_PUBLISHED eventId=ae381744-98b9-4d84-9af4-79ab70ef33f6",
          "2026-10-16T15:36:48.128+09:00  INFO 18035 --- [live-lunch-choice] [vote-log-listener-0-C-1] o.e.l.vote.messaging.VoteEventConsumer   : VOTE_AGGREGATED eventId=ae381744-98b9-4d84-9af4-79ab70ef33f6 voteId=7 candidateId=1",
          "2026-10-16T15:36:48.128+09:00  INFO 18035 --- [live-lunch-choice] [rankingListenerContainer-2] o.e.l.vote.redis.VoteRankingSubscriber   : RANKING_CHANGE_RECEIVED eventId=ae381744-98b9-4d84-9af4-79ab70ef33f6"
        ],
        "B": [
          "2026-10-16T15:36:48.077+09:00  INFO 18037 --- [live-lunch-choice] [http-nio-127.0.0.1-8081-exec-3] o.e.l.v.event.VotedCreatedEventListener  : VOTE_COMMITTED eventId=ae381744-98b9-4d84-9af4-79ab70ef33f6 voteId=7",
          "2026-10-16T15:36:48.077+09:00  INFO 18037 --- [live-lunch-choice] [http-nio-127.0.0.1-8081-exec-3] o.e.l.vote.messaging.VoteEventPublisher  : PUBLISH_REQUESTED eventId=ae381744-98b9-4d84-9af4-79ab70ef33f6 voteId=7",
          "2026-10-16T15:36:48.126+09:00  INFO 18037 --- [live-lunch-choice] [kafka-producer-network-thread | multi-sse-B-1] o.e.l.vote.messaging.VoteEventPublisher  : PUBLISH_SUCCEEDED eventId=ae381744-98b9-4d84-9af4-79ab70ef33f6 topic=vote-created partition=0 offset=14",
          "2026-10-16T15:36:48.128+09:00  INFO 18037 --- [live-lunch-choice] [rankingListenerContainer-2] o.e.l.vote.redis.VoteRankingSubscriber   : RANKING_CHANGE_RECEIVED eventId=ae381744-98b9-4d84-9af4-79ab70ef33f6"
        ]
      },
      "aggregation_servers": [
        "A"
      ],
      "processed": "1"
    }
  ],
  "subscribers_before": 1,
  "group_state": "\nGROUP           COORDINATOR (ID)          ASSIGNMENT-STRATEGY  STATE                #MEMBERS\nvote-log-group  localhost:9092  (1)       range                Stable               2\n",
  "group_members": "\nGROUP           CONSUMER-ID                                        HOST            CLIENT-ID       #PARTITIONS     CURRENT-EPOCH   CURRENT-ASSIGNMENT   TARGET-EPOCH    TARGET-ASSIGNMENT   \nvote-log-group  multi-sse-A-0-049b21b3-3b52-4636-a462-c4404d3208a0 /172.17.0.1     multi-sse-A-0   1               -               vote-created:0       -               -                   \nvote-log-group  multi-sse-B-0-e66d5ebf-f06b-48ab-a4c4-aedfe967cce0 /172.17.0.1     multi-sse-B-0   0               -               -                    -               -                   \n",
  "pubsub_subscribers": "live-lunch-choice:ranking-changed\n3",
  "group_before": "\nGROUP           TOPIC           PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG             CONSUMER-ID                                        HOST            CLIENT-ID\nvote-log-group  vote-created    0          13              13              0               multi-sse-A-0-049b21b3-3b52-4636-a462-c4404d3208a0 /172.17.0.1     multi-sse-A-0\n",
  "candidate": {
    "id": 1,
    "name": "일식"
  },
  "db_before": [
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
  "score_before": 4.0,
  "sse_initial": {
    "A": [
      {
        "candidateId": 1,
        "voteCount": 4
      },
      {
        "candidateId": 2,
        "voteCount": 1
      }
    ],
    "B": [
      {
        "candidateId": 1,
        "voteCount": 4
      },
      {
        "candidateId": 2,
        "voteCount": 1
      }
    ]
  },
  "db_after": [
    {
      "candidateId": 1,
      "name": "일식",
      "voteCount": 6
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
  "score_after": 6.0,
  "group_after": "\nGROUP           TOPIC           PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG             CONSUMER-ID                                        HOST            CLIENT-ID\nvote-log-group  vote-created    0          15              15              0               multi-sse-A-0-049b21b3-3b52-4636-a462-c4404d3208a0 /172.17.0.1     multi-sse-A-0\n",
  "success": true,
  "final_processes": [
    {
      "pid": 18035,
      "returncode": 143
    },
    {
      "pid": 18037,
      "returncode": 143
    }
  ]
}
```

## 정리와 한계
- 관측 SSE 소켓 정리 후 CLI가 시작한 PID18035/18037에 SIGTERM, 두 프로세스 종료코드143 확인. 최종 lsof에서8080/8081 LISTEN 없음.
- 기존 인프라와 기존 Redis 구독은 유지. 확인용 투표6·7은 삭제하지 않고 남김. DB/Redis 초기화, FLUSHALL, 토픽 삭제, offset reset 없음.
- 서비스·DTO 통합 코드 보존, 제품 코드 변경 없음. 이번은 실제 기능 실험과 bootJar 확인만 수행했으며 일반 테스트 전체 재실행 없음(기존63건 결과와 구분).
- 같은 PC 독립 JVM2개에서 정상 전파를 검증했다. 별도 물리 서버·네트워크 분리, 브라우저 자동 재연결, 다중 파티션, 장애 복구·보상·부하·성능 비교는 미실행.
- HTTP를 받는 서버와 Kafka 집계 서버가 달라도 정상임을 B 요청에서 직접 확인. Pub/Sub 수신과 SSE 데이터 대조로 집계하지 않은 B도 알림을 전달함을 확인했다. 절대적인 exactly-once나 장애 내구성을 주장하지 않음.
