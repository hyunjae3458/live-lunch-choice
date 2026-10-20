# Kafka 파티션과 Consumer 소비 위치 확인

이 프로젝트의 로컬 실습 명령어와 출력 해석을 정리한다. 모든 터미널 명령은 한 줄로 작성한다.

## 실습 대상

| 구분 | 값 |
|---|---|
| Docker 컨테이너 | `lunch-kafka` |
| 브로커 접속 주소 | `localhost:9092` |
| 토픽 | `vote-created` |
| Consumer 그룹 | `vote-log-group` |

`docker exec`는 실행 중인 컨테이너 안에서 Kafka 관리 명령을 실행한다. 컨테이너 이름을 다르게 만들었다면 명령의 `lunch-kafka`를 변경한다. 아래 명령은 조회용이며 파티션 수나 소비 위치를 변경하지 않는다.

## 1. 토픽의 파티션 구성 확인

```bash
docker exec lunch-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic vote-created
```

| 항목 | 의미 |
|---|---|
| `PartitionCount` | 토픽의 전체 파티션 개수 |
| `ReplicationFactor` | 파티션당 복제본 수. 리더 포함 |
| `Partition` | 파티션 번호. 0부터 시작 |
| `Leader` | 해당 파티션의 일반적인 읽기·쓰기를 담당하는 리더 브로커 ID |
| `Replicas` | 복제본이 배치된 브로커 ID 목록 |
| `Isr` | 리더와 동기화된 복제본의 브로커 ID 목록(In-Sync Replicas) |

출력 예시이며 실제 실행 결과를 옮긴 것은 아니다. 브로커 ID는 환경에 따라 다르다.

```text
Topic: vote-created  PartitionCount: 1  ReplicationFactor: 1
Topic: vote-created  Partition: 0  Leader: 1  Replicas: 1  Isr: 1
```

- 파티션은 0번 하나이고 브로커 1번이 리더다.
- 복제본은 리더를 포함하여 하나다. 별도 팔로워 복제본이 있다는 뜻이 아니다.
- `ReplicationFactor: 1`은 개수이고 `Replicas: 1`은 브로커 ID 목록이다.
- 파티션 내부에는 순서가 있지만 서로 다른 파티션을 아우르는 전체 순서는 보장하지 않는다.

## 2. Consumer 그룹의 소비 위치 확인

```bash
docker exec lunch-kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group vote-log-group
```

| 항목 | 의미 |
|---|---|
| `GROUP` | Consumer 그룹 이름 |
| `TOPIC` | 소비 위치를 확인하는 토픽 |
| `PARTITION` | 소비 위치를 확인하는 파티션 번호 |
| `CURRENT-OFFSET` | 그룹이 커밋한 위치. 재개 시 다음에 읽을 offset |
| `LOG-END-OFFSET` | 파티션 끝의 다음 기록 위치 |
| `LAG` | `LOG-END-OFFSET − CURRENT-OFFSET` |
| `CONSUMER-ID` | 해당 파티션을 담당하는 Consumer 식별자 |
| `HOST` | Consumer가 연결된 호스트 |
| `CLIENT-ID` | Consumer 클라이언트 식별자 |

이해를 위한 예시이며 실제 관측값이 아니다.

```text
TOPIC         PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG
vote-created  0          3               5               2
```

단순히 offset 0~4의 메시지가 기록된 경우, 다음 기록 위치는 5다. Consumer 그룹은 다음에 3번부터 읽을 위치까지 커밋했으므로 커밋 기준으로 2만큼 뒤처져 있다. 이미 읽었지만 아직 커밋하지 않은 메시지도 lag에 포함될 수 있다.

- offset은 토픽 전체 공통 번호가 아니라 파티션별 위치다.
- 커밋 offset은 Consumer 그룹별·토픽별·파티션별로 관리된다.
- offset 0, 1, 2를 처리하고 커밋했다면 `CURRENT-OFFSET`은 3이다.
- `LAG = 0`은 커밋 위치가 현재 파티션 끝을 따라잡았다는 뜻이다. 업무 처리가 성공했는지는 애플리케이션의 처리·커밋 방식도 확인해야 한다.
- 최초 커밋 전에는 소비 위치가 `-`로 보일 수 있다.
- 메시지는 소비·커밋 직후 삭제되지 않는다. Kafka의 보관 정책에 따라 유지된다.
- offset은 위치이므로 일반적으로 누적 메시지 수와 항상 같다고 해석하지 않는다.

## 3. Postman 투표와 연결해 관찰

1. 소비 위치 조회 명령으로 현재 offset/lag를 확인한다.
2. Postman에서 새로운 `userId`로 `POST /api/votes`를 호출한다.
3. 앱 로그에서 같은 `eventId`의 `PUBLISH_SUCCEEDED`와 `CONSUMER_RECEIVED`를 확인한다. 201 응답만으로 Kafka 송수신 성공을 판단하지 않는다.
4. 소비 위치 명령을 다시 실행해 offset 증가와 소비 후 lag를 확인한다. 빠르게 처리되면 중간 lag 증가가 관측되지 않고 계속 0일 수도 있다.

## 확인 기록과 한계

- 사용자는 Postman 생성 처리가 정상이라고 알렸고, 파티션 구성 및 Consumer 소비 위치 명령 확인 완료를 알렸다.
- 실제 명령 출력과 eventId별 발행/수신 로그는 이 대화에 제공되지 않았다. 위 출력은 설명용 예시이며 실제 offset·브로커 ID·lag를 검증 결과로 기록하지 않는다.
- 이 문서 작성 중에는 브로커 조작이나 추가 메시지 발행을 수행하지 않았다.

## 참고

- [Kafka 공식 운영 문서](https://kafka.apache.org/43/operations/basic-kafka-operations/)
- [Consumer 그룹 조회 설명](https://kafka.apache.org/42/operations/basic-kafka-operations/)
