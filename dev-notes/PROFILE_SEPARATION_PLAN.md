# polling / realtime 프로필 분리 구현 계획

## 요청과 범위
사용자가 설명을 이해한 뒤 CLI 구현·검증을 요청했다. 이번에는 프로필 분리만 수행한다. 다음은 작은 프론트 화면, 마지막은 구조별 부하 비교다. 화면·부하 스크립트·장애 보상을 이번에 선행 구현하지 않는다.

## 구조
- 같은 코드베이스/빌드에서 공통 후보 기능, 투표 검증·DB 저장, DB 결과 조회를 유지한다. 프로젝트나 저장 로직을 두 벌로 복제하지 않는다.
- VoteService에는 create/results 및 DB Repository/ApplicationEventPublisher 의존성만 남긴다. 기존 DB 트랜잭션과 예외 처리를 보존한다.
- rankings/aggregate를 @Profile("realtime") VoteRealtimeService로 이동한다. VoteRankingRedis/VoteRankingPublisher 주입과 현재 동작 유지. Consumer, 랭킹 Controller, SSE 서비스의 호출 대상도 변경한다.
- 이는 사용자 요청으로 통합했던 과거 결정을 구체적인 프로필 경계 필요에 따라 변경하는 것이다. DTO는 현재 VoteResultResponse 통합 상태 유지.
- 공통 create의 Spring 내부 VoteCreateEvent 생성/발행은 유지 가능. polling에서는 커밋 후 Kafka 발행 리스너가 없어서 외부 발행이 없다. 내부 이벤트 호출 자체도 사라졌다고 설명하지 않는다.

## Bean 활성화
- @Profile("realtime"): VotedCreatedEventListener, VoteEventPublisher, VoteEventConsumer, VoteKafkaConfig, VoteRankingRedis, VoteRankingPublisher, VoteRankingSubscriber, VoteRankingRedisConfig, VoteRealtimeService, VoteRankingSseService, VoteRankingController.
- LocalConsumerController는 @Profile("local & realtime"). 배열 프로필은 OR이므로 사용하지 않는다.
- realtime 설정/Bean이 비활성화돼도 공통 Bean이 이를 필수 주입하여 시작 실패하지 않도록 의존성 점검한다.
- 기존 app.ranking.subscription-enabled 조건은 일반 테스트의 Redis 구독 격리 용도로 유지 가능. 프로필과 조건을 함께 만족할 때만 컨테이너 시작.

## YAML 및 실행
- application.yml: .env import/애플리케이션명/DB/JPA 공통 설정 유지.
- application-realtime.yml: 기존 Kafka producer/consumer/listener 옵션 및 Redis 접속 설정 이동. 기존 직렬화/acks/멱등 Producer/ack-mode/신뢰 패키지를 잃지 않는다.
- application-polling.yml: Kafka admin.auto-create=false, listener.auto-startup=false, app.ranking.subscription-enabled=false. 프로필로 전용 Bean을 제외하는 것이 주된 분리 수단이다.
- Kafka/Redis 라이브러리는 동일 빌드에 남는다. 자동 구성으로 기본 Bean이 존재하는 것과 실제 외부 통신/구독은 구분한다. 필요시 Boot4.1.1 실제 자동 구성 경로를 확인하여 polling에서 제외할 수 있으나 근거 없이 오래된 클래스명을 넣지 않는다.
- 실행 예: --spring.profiles.active=local,polling 또는 --spring.profiles.active=local,realtime. local은 환경, 나머지는 처리 방식이다.
- 두 모드를 함께 켜는 설정은 허용하지 않는 것으로 문서화하고 가능하면 명확한 시작 오류로 거절한다. 모드를 누락했을 때의 동작도 명시한다. 구현 기본안은 둘 다 지정하지 않으면 polling을 선택하도록 default profile 설정을 검토하되 local만 명시하면 default가 적용되지 않는 점을 주의하고, 불명확하면 명시적 모드 선택을 요구하는 방식을 사용한다. 선택을 결과에 기록한다.

## API 계약
- 공통: 기존 후보 API, POST /api/votes, GET /api/votes/results. DB 저장/집계 계약 그대로.
- realtime 전용: GET /api/votes/rankings 및 /stream. polling에서 해당 Controller 미등록으로 404.
- polling의 주기적 호출은 향후 클라이언트/부하 도구가 담당한다. 서버 스케줄러를 새로 만들지 않는다.
- realtime 랭킹의 Redis 순서 유지, DB 미접근, 이름/0표 프론트 결합 및 SSE 전체 스냅샷 계약 유지.

## 검증
1. polling 프로필: Kafka/Redis 서버 없이 앱 시작, 후보·투표 생성과 DB 결과 조회 성공. Kafka 발행 리스너/Consumer/Redis 구독/SSE Bean 부재 확인. rankings/stream 및 로컬 Consumer 제어 API 미노출 확인.
2. realtime 프로필: 기존 집계·중복 미발행·랭킹·PubSub·SSE 초기/변경/재접속 회귀 검증. 기존 Testcontainers Kafka/Redis와 H2 격리 E2E를 재사용한다.
3. 테스트별 프로필 명시. 기존 커밋 후 Kafka 발행 검증과 로컬 Consumer 제어 검증은 realtime 조건을 포함해야 한다. 일반 테스트는 외부 브로커 없이 mock/구독 비활성 설정 유지. redisTest는 실제 외부 연동 검증으로 분리한다.
4. polling/realtime 동시 활성화 거절과 모드 누락 정책을 검증한다. 빈 Bean 정의 검사만으로 외부 통신 부재를 단정하지 말고 Kafka/Redis 없이 실제 시작/HTTP 기능을 확인한다.
5. ./gradlew test redisTest bootJar 등 변경 범위에 적절한 검사 수행. 기존 사용자 앱/DB/Redis/Kafka는 초기화하거나 중단하지 않는다. 임시 환경 사용.

## 데이터 주의 및 완료 기록
- polling으로 저장한 투표는 Kafka에 발행되지 않으므로 같은 DB를 realtime으로 전환해도 Redis에 자동 반영되지 않는다. 이를 문서화하고 비교/회귀는 격리된 데이터 또는 통제된 기준값에서 수행한다. 자동 backfill/사용자 데이터 삭제로 해결하지 않는다.
- dev-notes/PROFILE_SEPARATION_RESULT.md에 실제 변경, 프로필별 Bean/API/연결 결과, 테스트 명령·결과·미검증 항목과 실행 방법을 기록한다. PROGRESS 및 DESIGN_DECISIONS도 실제 구현 상태로 갱신한다.
- 이번 데스크톱 턴은 인계 문서 작성만 하며 구현·테스트 결과를 미리 생성하지 않는다.
