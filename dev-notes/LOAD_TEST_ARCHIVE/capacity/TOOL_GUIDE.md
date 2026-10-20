# 동일 SSE 처리 용량 실험 도구

기존 `pilot.py`/`scale.py`와 이전 polling 결과를 사용하지 않는다. `dev-notes/LOAD_TEST_CAPACITY_HANDOFF.md`와 새 결과 문서를 먼저 확인한다.

- `observer.py`: aiohttp 비동기 SSE 파서, 예약된 도착률, 전송 지연·미발행·미관측·drain 기록. 누적 목표 도달 시간은 개별 이벤트 완료 시간이 아니다. HTTP 오류가 있으면 해당 목표와 이벤트 대응을 단정하지 않는다.
- `stub.py`: 발생기 용량 검증 서버(18083). 제품 성능으로 해석하지 않는다.
- `run.py`: 전용 라벨/DB를 검증한 뒤 앱이 꺼진 상태에서만 시드 복구. 각 반복 앱18082를 종료한다. 실제 서비스 설정을 읽지 않는다.
- `batch.py`: 조회 진단 또는 본 경로를 상한 내 탐색하고 발생기 기준 실패 시 증가를 중단한다.
- `recheck.py`: 첫 환경 경고 후 같은 최저 부하 재검증용. 기존 결과 덮어쓰기 금지.
- `KafkaLagSampler.java`: 호스트에서 한 AdminClient를 재사용하는 읽기 전용 lag 관측기. 최초 실험의 반복 Kafka CLI CPU 간섭을 보완했으며, 수정 후 부하 재측정은 미실행.
- `infra.yml`: 전용 PostgreSQL35432/Kafka39092/Redis36379. 기존 컨테이너와 데이터 보존.
- `application-capacity.yml`: 양쪽 동일 heap은 실행기에서512MiB, pool10, SSE900초, DB worker1/큐1024. `capacity`는 계측을 활성화하는 보조 프로필이며 처리 모드와 함께 사용한다.

환경 상태는 Git 제외된 `load-tests/state/capacity/current.json` 한 파일에 보관한다. 실험 비밀번호를 출력하거나 커밋하지 않는다. 컨테이너를 새로 만들 때는 새 run ID와 볼륨을 사용하고, Kafka 볼륨 소유권을 이미지 사용자 UID1000으로 맞춘다. `seed.sql`은 서비스 DB에 직접 실행하지 않는다.

현재 실행 환경에서 개별 검증 예시(결과 이름은 매번 새 이름):

`/tmp/llc-capacity-venv/bin/python load-tests/capacity/run.py --name new-check-db --mode db-sse --seed 10000 --rate 2 --viewers 2 --warmup 2 --duration 5`

`/tmp/llc-capacity-venv/bin/python load-tests/capacity/run.py --name new-check-realtime --mode realtime --seed 10000 --rate 2 --viewers 2 --warmup 2 --duration 5`

검증 의존성은 Python3.9/aiohttp3.13.3, Java17, Docker, 빌드한 bootJar다. 초기 상태·SQL/Redis 통계·요청별 원본·SSE 수신량·JVM/큐·컨테이너 자원·Kafka lag를 결과 폴더에 기록한다. SQL 통계는 `pg_stat_statements` 전후 차이를 사용하며 조회 진단의 워밍업+측정 합계임을 명시한다. Redis INFO도 전후 차이로 계산한다. metrics의 poolPending은 대기자 표본이며 커넥션 획득 시간 자체는 아직 아니다.

`summary.pass`는 자동 수치 판정의 중간 결과다. 최종 결론 전 원본 알림/전송 수와 lag, 마지막60초 적체 증가, 계측 오류·호스트 한계를 별도로 대조한다. 조회 진단은 SSE0이므로 실시간 처리 용량 결론에 사용할 수 없다. 생성기 실패·실행 예외 결과도 삭제하지 않는다.

사용자 변경 후의 실행기는 `continue.py --session 새이름 check/read/full/viewers`다. 발생기/SLO 경고는 기록하며 계속하고, 심각한 응답 불능이나 최종 데이터 불일치에서 중단한다. `analyse.py 세션이름`은 해당 세션의 원본만 요약한다. 기존 `batch.py`는 이전 엄격한 중단 정책이므로 최신 요청 실행에는 사용하지 않는다.

최종 Kafka CLI 조회 전에 자원 샘플러를 종료하도록 보완했다. resume1에 저장된 Docker CPU/RSS 파일 끝부분에는 사후 CLI 검증 비용이 포함될 수 있으며, cgroup-before/after는 해당 CLI 호출 전에 수집되어 이를 포함하지 않는다. 수정 후 새 부하 재측정은 아직 하지 않았다.
