# 로컬 실행 방법

[프로젝트 소개와 회고](../README.md) · [실행 방법](GETTING_STARTED.md) · [구조](ARCHITECTURE.md) · [설계 판단](DESIGN_REVIEW.md) · [성능 실험](PERFORMANCE_REVIEW.md)

애플리케이션은 호스트에서, PostgreSQL·Kafka·Redis는 Docker에서 실행하는 예시입니다. **JDK 17, Git, Docker**를 준비합니다. Gradle은 저장소의 Wrapper를 사용합니다. 아래 명령은 macOS/Linux 터미널 기준이며 Windows에서는 저장소 복사·환경 파일 준비 후 `gradlew.bat bootRun`을 사용할 수 있습니다.

### 1) 저장소와 인프라 준비

```bash
git clone https://github.com/hyunjae3458/live-lunch-choice.git
cd live-lunch-choice
```

새 로컬 환경에서 아래 컨테이너를 생성합니다. 기존에 같은 이름의 컨테이너나 같은 포트를 사용하는 서비스가 있다면 중복 생성하지 말고 기존 개발용 인프라의 연결 정보를 설정합니다.

```bash
docker run -d --name lunch-postgres -p 127.0.0.1:5432:5432 -e POSTGRES_DB=live_lunch_choice -e POSTGRES_USER=live_lunch_choice_app -e POSTGRES_PASSWORD=lunch_local_password -v lunch-postgres-data:/var/lib/postgresql/data postgres:17
docker run -d --name lunch-redis -p 127.0.0.1:6379:6379 -v lunch-redis-data:/data redis:8 redis-server --appendonly yes
docker run -d --name lunch-kafka -p 127.0.0.1:9092:9092 apache/kafka:4.1.2
```

위 비밀번호는 로컬 실행 예시입니다. 변경할 경우 아래 `.env`에도 동일하게 입력합니다. PostgreSQL 이미지의 DB·사용자 생성 설정은 빈 데이터 디렉터리의 최초 초기화에 적용됩니다. 기존 볼륨이 있다면 생성 당시 계정 정보를 사용해야 합니다. [PostgreSQL 이미지 안내](https://hub.docker.com/_/postgres)

Kafka는 공식 단일 브로커 실행 예시를 기반으로 합니다. 애플리케이션이 같은 컴퓨터에서 `localhost:9092`로 접속하는 구성입니다. 앱까지 컨테이너로 실행하는 구성은 이 안내의 범위가 아닙니다. [Kafka Docker 안내](https://kafka.apache.org/41/getting-started/docker/)

### 2) 환경 파일 설정

프로젝트 루트에서 예시 파일을 복사합니다.

```bash
cp .env.example .env
```

위 컨테이너를 사용했다면 `.env`를 다음과 같이 설정합니다. Spring이 properties 형식으로 읽으므로 값에 따옴표나 `export`를 붙이지 않습니다.

```properties
APP_PROFILE=realtime
DB_HOST=localhost
DB_PORT=5432
DB_NAME=live_lunch_choice
DB_USERNAME=live_lunch_choice_app
DB_PASSWORD=lunch_local_password
```

Redis 기본 주소는 `localhost:6379`, Kafka 기본 주소는 `localhost:9092`입니다. 별도 Redis를 쓰면 `.env`에 `REDIS_HOST`와 `REDIS_PORT`를 설정합니다. Kafka 주소를 바꾸려면 `src/main/resources/application-realtime.yml`의 `spring.kafka.bootstrap-servers`와 브로커의 클라이언트 접속 주소를 함께 맞춥니다.

### 3) 앱 실행과 사용

컨테이너가 준비된 후 프로젝트 루트에서 실행합니다. `docker logs lunch-postgres`, `docker logs lunch-kafka`, `docker logs lunch-redis`로 기동 상태를 확인할 수 있습니다.

```bash
./gradlew bootRun
```

브라우저에서 **http://localhost:8080**에 접속합니다. 별도 프론트엔드 실행은 필요하지 않습니다. 후보를 등록한 뒤 테스트용 사용자 ID로 투표합니다. 여러 창에서 결과 갱신을 확인할 수 있으며 추가 투표에는 새로운 사용자 ID를 사용합니다.

| 모드 | `.env` 설정 | 필요한 인프라 | 화면 사용 |
|---|---|---|---|
| 실시간 | `APP_PROFILE=realtime` | PostgreSQL·Kafka·Redis | SSE로 갱신 |
| 폴링 | `APP_PROFILE=polling` | PostgreSQL | 약 2초 간격으로 조회 |
| DB·SSE 비교용 | `APP_PROFILE=db-sse` | PostgreSQL | 기존 UI의 지원 모드가 아니며 API·SSE 실험용 |

모드 변경 후 앱을 재시작합니다. IntelliJ 등에서 활성 프로필을 별도로 지정했다면 `.env`와 충돌하지 않도록 확인합니다. 기존 DB 투표를 Redis로 자동 복구·동기화하는 기능은 없으므로, 이미 투표한 DB에서 모드만 바꾸면 집계가 자동으로 일치하지 않습니다.

앱은 개발 설정인 `ddl-auto: update`로 엔티티에 맞춰 테이블을 생성·갱신합니다. 실행용 DB 자체는 위 컨테이너 설정 등으로 먼저 준비해야 합니다.

### 종료와 다시 실행

앱은 실행 터미널에서 `Ctrl+C`로 종료합니다. 위 예시로 만든 컨테이너만 중지하거나 다시 시작하려면 다음 명령을 사용합니다.

```bash
docker stop lunch-postgres lunch-redis lunch-kafka
docker start lunch-postgres lunch-redis lunch-kafka
```

중지·재시작은 투표 초기화가 아닙니다. PostgreSQL·Redis의 볼륨과 Kafka 컨테이너는 그대로 재사용합니다. 특히 위 Kafka 예시는 별도 데이터 볼륨을 지정하지 않았으므로 컨테이너를 삭제하고 다시 만드는 동작은 단순 재시작과 다릅니다.

**투표 초기화·후보 전체 초기화 API와 버튼은 아직 구현하지 않았습니다.** DB·Redis 일부만 지우면 남은 Kafka 이벤트가 다시 반영될 수 있어, 데이터 삭제를 초기화 절차로 제공하지 않습니다.

이 안내는 코드의 연결 설정과 공식 이미지 실행 방법을 기준으로 작성했습니다. 문서 수정 과정에서 새 컨테이너를 띄우거나 다른 컴퓨터에서 실행 검증한 것은 아닙니다.
