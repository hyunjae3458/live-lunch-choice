# polling/realtime 프로필 분리 결과

> 후속 변경(2026-10-16 16:35 KST): Consumer 제어 API를 잠시 제거했으나 사용자 요청으로 제거 직전 상태로 복원했다. 아래 local & realtime 등록 조건을 유지하며 복원 후 일반 테스트68건 통과.

기록: 2026-10-16 16:16 KST. 구현 및 검증 완료. 기존 사용자 앱·인프라·데이터를 중단하거나 초기화하지 않았다.

## 구현
- 공통 VoteService: create/results와 DB Repository/ApplicationEventPublisher만 유지. 투표 검증·저장·트랜잭션·오류 및 VoteResultResponse 계약 유지.
- VoteRealtimeService: 기존 rankings/aggregate 그대로 이전. Redis 조회 순서, Lua 중복 방지, 신규 집계만 Pub/Sub 발행 유지. Consumer/Controller/SSE 호출 대상 변경.
- realtime 전용 @Profile: 커밋 후 리스너, Kafka Publisher/Consumer/토픽 설정, Redis 랭킹/Publisher/Subscriber/구독 설정, VoteRealtimeService, 랭킹 Controller, SSE 서비스.
- LocalConsumerController는 @Profile("local & realtime"). local만 또는 local,polling에서는 미등록.
- application.yml에는 .env/앱명/DB/JPA, application-realtime.yml에는 기존 Redis/Kafka 옵션을 이동. 직렬화·신뢰 패키지·acks·Producer 멱등성·타임아웃·record ack 옵션 보존.
- application-polling.yml은 admin.auto-create=false/listener.auto-startup=false/구독 비활성화. 추가로 Kafka/Redis 자동 구성을 제외하여 클라이언트 Bean 자체를 생성하지 않음. 라이브러리는 동일 JAR에 남음.
- 제외 클래스는 로컬 Boot4.1.1 JAR의 AutoConfiguration.imports에서 확인한 실제 경로: org.springframework.boot.kafka.autoconfigure.KafkaAutoConfiguration 및 org.springframework.boot.data.redis.autoconfigure의 DataRedisAutoConfiguration/DataRedisReactiveAutoConfiguration/DataRedisRepositoriesAutoConfiguration.

## 모드 정책과 실행
polling/realtime 중 정확히 하나를 명시해야 한다. 모드 없음(프로필 미지정 또는 local만 지정)과 동시 활성화는 ProcessingModeConfig에서 Bean 생성 전 시작 오류로 거절한다. default polling을 암묵 적용하지 않음. local은 환경용 보조 프로필이다.

프로젝트 루트에서 실행:

polling: `java -jar build/libs/live-lunch-choice-0.0.1-SNAPSHOT.jar --spring.profiles.active=local,polling`

realtime: `java -jar build/libs/live-lunch-choice-0.0.1-SNAPSHOT.jar --spring.profiles.active=local,realtime`

빌드/검증: `./gradlew test redisTest bootJar`

ranking-e2e는 테스트 보조 프로필이다. 실제 E2E 테스트는 realtime,ranking-e2e를 함께 활성화한다. 일반 realtime 테스트는 Kafka mock/리스너 자동 시작 끔/Redis 구독 비활성 조건으로 외부 서비스를 격리한다.

## 실제 검증 결과
- ./gradlew test redisTest bootJar → BUILD SUCCESSFUL(25초). 일반68건 + 실제 통합9건 =77건, 실패/오류/건너뜀0. git diff --check 통과.
- polling: local,polling으로 랜덤 포트의 실제 HTTP 서버 시작, 격리 H2 사용. 실제 Kafka/Redis 서버 없이 연결 감지용 TCP 소켓을 주소로 지정. 해당 소켓은 Kafka/Redis 프로토콜을 제공하지 않는다.
- 후보 생성201/조회200, 투표201/중복409, DB 결과200·1표, DB 저장1건 확인. 순위/stream/로컬 Consumer 조회·start·stop API는404.
- 실시간 앱 Bean 및 KafkaTemplate/Registry/RedisConnectionFactory/구독 컨테이너 부재 확인. HTTP 처리 후 두 연결 감지 소켓에서 각300ms accept 대기 결과 연결0건. 이는 테스트 관측 구간의 외부 연결 부재이며 모든 환경/무한 시간의 네트워크 보장은 아님.
- realtime: 임시 Testcontainers Redis8/Kafka4.1.2 + 격리 H2 + 실제 랜덤포트 HTTP/SSE. 초기 두 연결[], HTTP 후보/투표201, 정상 집계·Pub/Sub·두 SSE1표, 같은 이벤트 재전달 시1표 유지/추가 알림 없음, 수동 재접속 최신1표 확인.
- Kafka 커밋 후 발행/롤백 미발행 기존 테스트, Lua 동시성/중복, 순위 정렬, SSE 종료/순서 테스트 유지.
- 모드 단위 검사8건: polling/realtime/local,polling/local,realtime 허용; 무프로필/local/polling,realtime/local,polling,realtime 거절. LocalConsumerController 프로필 조합6건 확인.
- 실제 실행 JAR로도 local만 지정 및 polling,realtime 동시 지정 각각 exit1과 명확한 오류 확인. 제품 코드 변경으로 실패한 테스트 없이 첫 전체 검증에서 통과.

실제 HTTP/실시간 로그:
```text
POLLING_HTTP candidate=201 list=200 vote=201 duplicate=409 results=200/count1 realtimeAPIs=404 realtimeBeans=absent KafkaRedisConnections=0
2026-10-16T16:14:04.114+09:00  INFO 19320 --- [enerContainer-1] o.e.l.vote.redis.VoteRankingSubscriber   : RANKING_CHANGE_RECEIVED eventId=16a8e5de-f529-4fea-a8b7-5a13494a8c7d
2026-10-16T16:14:04.268+09:00  INFO 19320 --- [-listener-0-C-1] o.e.l.vote.messaging.VoteEventConsumer   : VOTE_DUPLICATE_SKIPPED eventId=16a8e5de-f529-4fea-a8b7-5a13494a8c7d voteId=1 candidateId=1
SSE_E2E eventId=16a8e5de-f529-4fea-a8b7-5a13494a8c7d voteId=1 candidateId=1 duplicateOffset=1 committed=2 initial=[] first=[{"candidateId":1,"voteCount":1}] second=[{"candidateId":1,"voteCount":1}] score=0->1->1 HTTP(candidate/vote/rankings/results)=201/201/200/200 DBvotes=1 pubsub=1 reconnect=latest
```

실제 JAR 잘못된 모드 실행 결과:
```json
[
  {
    "profiles": "local",
    "exit_code": 1,
    "error": "2026-10-16T16:14:51.805+09:00  WARN 19365 --- [live-lunch-choice] [           main] ConfigServletWebServerApplicationContext : Exception encountered during context initialization - cancelling refresh attempt: java.lang.IllegalStateException: polling 또는 realtime 프로필 중 정확히 하나를 활성화하세요."
  },
  {
    "profiles": "polling,realtime",
    "exit_code": 1,
    "error": "2026-10-16T16:14:52.954+09:00  WARN 19366 --- [live-lunch-choice] [           main] ConfigServletWebServerApplicationContext : Exception encountered during context initialization - cancelling refresh attempt: java.lang.IllegalStateException: polling 또는 realtime 프로필 중 정확히 하나를 활성화하세요."
  }
]
```

## API와 데이터 주의
| 모드 | 공통 후보·투표·DB results | Redis rankings/stream | 로컬 Consumer 제어 |
| --- | --- | --- | --- |
| polling | 유지 | 404 | 404 |
| realtime | 유지 | 활성 | local 동시 활성 시만 제공 |

polling에서도 공통 Spring 내부 VoteCreateEvent는 생성·발행된다. 커밋 후 Kafka 발행 리스너가 없어 외부 Kafka 발행은 없다. 서버가 자동으로 DB를 주기 조회하지 않으며 polling 호출은 향후 클라이언트 책임이다.

polling으로 저장한 투표는 realtime으로 전환해도 Redis에 자동 반영되지 않는다. 자동 backfill/기존 데이터 초기화는 구현하지 않았다.

## 한계 및 제외
- 이번 정상 모드별 HTTP E2E DB는 격리 H2. PostgreSQL에서 모드별 신규 투표/전환 실험은 미실행. 과거 PostgreSQL 시연과 구분한다.
- 실제 모드 누락/충돌 JAR 실행은 시작 실패를 확인한 것이며 업무 API 검증은 아님.
- 실제 다중 서버 회귀, 브라우저 자동 재접속, 장애 보상, 프론트, 부하 비교는 이번에 진행하지 않았다.
- 기존 앱/DB/Redis/Kafka를 종료·초기화하거나 기존 토픽/offset을 변경하지 않았다. 테스트 임시 컨테이너/서버만 사용하고 종료했다.
