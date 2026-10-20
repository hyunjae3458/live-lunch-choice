> **2026-10-19 17:18 KST 사용자 결정: 비교 근거 폐기.** 이 문서의 polling/realtime 결과는 신규 처리 용량 비교 및 회고의 성능 근거로 사용하지 않는다. 원본 이력만 보존한다. 새 계획: [LOAD_TEST_CAPACITY_HANDOFF.md](LOAD_TEST_CAPACITY_HANDOFF.md).

# 단계별 부하 비교 결과

## 범위와 환경
수정 관측기 재검증 → 관람자10/50/100(1req/s) → 관람자100에서5/10req/s 순서. 증가 조건은 각각60초/유효3회이며100명1req/s 결과는 재사용한다. polling 실행 후 사용자 모드 전환 확인을 받아 realtime 실행. 자동 포화/제품 최적화/초기화 API 없음.

재검증 ID20261019_153750_7c5d. 새 DB live_lunch_choice_lt_20261019_153750_7c5d_polling/realtime, 새Kafka29092/Redis26379, 앱18081. 기존8080/기존DB/9092/6379 및 이전 예비19092/16379와 결과 보존. 공유 PostgreSQL과 이전 유휴 인프라/앱의 자원 경쟁 포함. JVM heap256~512MiB/ActiveProcessorCount2, Hikari10, Kafka1CPU/1GiB, Redis0.5CPU/256MiB. Git HEAD003eeeaed2b0c0ddd16acd97ca1cc0496c13a005; 실제 스크립트/설정 스냅샷과 bootJar SHA256은 code-version.json에 저장.

`.env.loadtest-scale`(Git 제외,0600), state/results는 load-tests/{state,results}/scale-recheck-01. 예전 .env.loadtest와 별도다.

## 측정 정책 및 판정
- 직접시드 후보4/1000표400·300·200·100. 같은4건 HTTP 쓰기 워밍업 및 전원 집계 완료 후401·301·201·101=1004표 기준. 워밍업 삭제/이벤트 재발행 없음.5초 읽기 대기 후 측정.
- 관람자는 거의 동시 접속. polling 응답완료+2초, SSE60초 종료/3초 뒤 재접속. 전체JSON스냅샷, 임계치 관측 지연은 요청 시작 기준이며 개별 이벤트 지연 아님. 관람자별 표본은 상관관계 있음.
- 자원과 Kafka Admin을 별도 스레드로 분리. 외부 명령45초 상한, Admin request5초/API10초. 자원/lag 각각 작업완료 후5초 대기여서 실제 간격은 수집비용만큼 늘어남. lag는 워밍업 집계 후 시작해 아직 커밋이 없는 상태를 측정 실패로 오인하지 않음.
- 측정 중 자원샘플3개 미만/수집오류, realtime lag샘플3개 미만/오류/종료30초 내lag0 미확인은 증가 중단. lag0 관측시간은 조회 비용과 샘플링 해상도를 포함하며 정확한 해소시각 아님.
- 오류3건의 기존 즉시중단은 유지하되 최종 판단은 더 엄격하게 클라이언트 오류가 하나라도 있으면 증가 보류. HTTP실패1% 이상, 투표p95>1초, 최종불일치, OOM 또는 관측 중 swap>512MiB 증가 시 다음 단계 중단. 스케줄 지연>100ms/미발행도 관측기 실패로 보류한다.
- HTTP 본문과 SSE JSON만 포함, 헤더/프레임/TCP/브라우저 렌더링 제외. ps와 Docker CPU 합산 불가. DB통계는 워밍업/최종검증 포함, SQL 쿼리 수가 아님. 발생기스레드/FD/CPU/RSS/스케줄지연 기록.

## 1단계 재검증 결과

### polling
시작 2026-10-19T15:42:48.300253+09:00, valid=True, exact=True, 중단사유=[].

|항목|결과|
|---|---|
|measurement_seconds|30.003466042|
|attempted|30|
|successful|30|
|failed|0|
|actual_vote_rps|0.9998844786133992|
|vote_http_ms|{"count": 30, "p50": 16.504250000000553, "p95": 22.444832999999775, "p99": 26.5540840000007, "max": 26.5540840000007}|
|poll_http_ms|{"count": 150, "p50": 9.938417000000754, "p95": 23.63141700000071, "p99": 28.94208300000045, "max": 30.027916999998183}|
|cumulative_target_from_request_start_ms|{"count": 300, "p50": 1018.8172919999978, "p95": 2015.7766250000009, "p99": 2027.6568329999982, "max": 2032.9923750000028}|
|schedule_delay_ms|{"count": 30, "p50": 4.298291999999648, "p95": 6.835125000002051, "p99": 7.770292000000012, "max": 7.770292000000012}|
|resource_sample_times|[8.847639541, 15.993893624999998, 23.13797525, 30.287580916, 37.421314166]|
|lag_sample_times|[]|
|lag_zero_observed_after_load_seconds|null|
|sse_reconnected_viewers|0|
|final_kafka_lag|null|
|expected|{"1": 409, "2": 309, "3": 208, "4": 108}|
|db_final|{"1": 409, "2": 309, "3": 208, "4": 108}|
|redis_final|null|
|final_observation_after_last_success_ms|92.2003339999975|
|errors|[]|

측정 상대구간 8.273~38.276초. 자원샘플 간격(초): [7.146, 7.144, 7.15, 7.134].
- 공유PG CPU 0.76~8.02%; 메모리 관측값: 178.3MiB, 178.9MiB, 179.1MiB, 179.4MiB, 178.8MiB.
- 전용Kafka CPU 1.82~4.41%; 메모리 관측값: 336.5MiB, 335.6MiB, 335.7MiB, 336.1MiB, 336.7MiB.
- 전용Redis CPU 0.27~0.44%; 메모리 관측값: 12.65MiB, 12.88MiB, 13.09MiB, 12.82MiB, 12.78MiB.
- 프로세스 CPU/RSS, 생성기 스레드/FD/swap, NetIO/BlockIO, DB 통계와 개별 HTTP 결과는 원본에 보존.
- DB통계 전후: [{"xact_commit": 44, "xact_rollback": 0, "blks_read": 5, "blks_hit": 29082, "tup_returned": 23271, "tup_fetched": 9936, "tup_inserted": 1159, "temp_bytes": 0, "deadlocks": 0}, {"xact_commit": 512, "xact_rollback": 0, "blks_read": 5, "blks_hit": 39421, "tup_returned": 215365, "tup_fetched": 15432, "tup_inserted": 1192, "temp_bytes": 0, "deadlocks": 0}].

### realtime
시작 2026-10-19T15:46:49.745668+09:00, valid=True, exact=True, 중단사유=[].

|항목|결과|
|---|---|
|measurement_seconds|30.003868625000003|
|attempted|30|
|successful|30|
|failed|0|
|actual_vote_rps|0.9998710624603662|
|vote_http_ms|{"count": 30, "p50": 19.670040999997696, "p95": 51.127624999999455, "p99": 67.90900000000022, "max": 67.90900000000022}|
|poll_http_ms|{"count": 0, "p50": null, "p95": null, "p99": null, "max": null}|
|cumulative_target_from_request_start_ms|{"count": 300, "p50": 39.0877500000002, "p95": 86.71758300000043, "p99": 107.10949999999997, "max": 110.37708299999949}|
|schedule_delay_ms|{"count": 30, "p50": 4.035958000000228, "p95": 7.463999999998805, "p99": 10.834458000000602, "max": 10.834458000000602}|
|resource_sample_times|[8.637958875, 15.769903583, 22.919690749999997, 30.059347832999997, 36.175001333000004]|
|lag_sample_times|[8.0664415, 13.781621875, 19.468204458, 25.209792957999998, 30.908917416999998]|
|lag_zero_observed_after_load_seconds|0.7825836660000007|
|sse_reconnected_viewers|10|
|final_kafka_lag|0|
|expected|{"1": 409, "2": 309, "3": 208, "4": 108}|
|db_final|{"1": 409, "2": 309, "3": 208, "4": 108}|
|redis_final|{"4": 108, "3": 208, "2": 309, "1": 409}|
|final_observation_after_last_success_ms|14.214792000004195|
|errors|[]|

측정 상대구간 6.461~36.464초. 자원샘플 간격(초): [7.132, 7.15, 7.14, 6.116].
- 공유PG CPU 0.27~6.09%; 메모리 관측값: 176.6MiB, 177.5MiB, 177.5MiB, 177.9MiB, 177.5MiB.
- 전용Kafka CPU 5.24~10.43%; 메모리 관측값: 363.9MiB, 363.1MiB, 380.3MiB, 379.6MiB, 402.1MiB.
- 전용Redis CPU 0.27~0.51%; 메모리 관측값: 13.05MiB, 13.27MiB, 13.25MiB, 13.2MiB, 13.17MiB.
- 프로세스 CPU/RSS, 생성기 스레드/FD/swap, NetIO/BlockIO, DB 통계와 개별 HTTP 결과는 원본에 보존.
- DB통계 전후: [{"xact_commit": 54, "xact_rollback": 0, "blks_read": 5, "blks_hit": 29524, "tup_returned": 28054, "tup_fetched": 10165, "tup_inserted": 1159, "temp_bytes": 0, "deadlocks": 0}, {"xact_commit": 155, "xact_rollback": 0, "blks_read": 5, "blks_hit": 36918, "tup_returned": 35494, "tup_fetched": 14405, "tup_inserted": 1193, "temp_bytes": 0, "deadlocks": 0}].

## 원본과 실행
load-tests/results/scale-recheck-01/{polling,realtime}-pilot/{summary.json,events.jsonl,http.json,votes.json}, 모드별 app.log, code-version.json/code-snapshot. 원본 실패를 성공으로 덮어쓰지 않는다.

`LLC_TEST_ENV=.env.loadtest-scale LLC_TEST_RUN=scale-recheck-01 python3 load-tests/manage.py run`

`LLC_TEST_ENV=.env.loadtest-scale LLC_TEST_RUN=scale-recheck-01 python3 load-tests/pilot.py --viewers 10 --seconds 30 --rate 1`

`LLC_TEST_ENV=.env.loadtest-scale LLC_TEST_RUN=scale-recheck-01 python3 load-tests/manage.py stop`

## 후속 상태
관람자10/50/100 각60초3회와 투표5/10req/s 각60초3회는 아직 미실행. 단회 재검증 결과로 반복 편차/성능 우열/용량을 주장하지 않는다. 양쪽 관측기 통과 후에만 동일 시드와 별도 결과 디렉터리로 실행한다. 수천 연결/다중 서버/제품 최적화/장애복구는 범위 밖.

## 관람자10 / 1req/s / 60초 반복 진행
- 첫 배치 준비는 부모 프로세스에 새 포트 환경변수가 전달되지 않아 Address already in use로 데이터 생성 전 중단. polling-preflight-failed.json 보존. 환경 전달을 수정하고 동일 조건1회 재시도. 제품 문제 아님. 수정 후 실제1회 실행했으나 아래 호스트/발생기 중단 기준에 걸림. 부하 전 오류와 실제 측정 중단을 구분한다.

### 실제 반복1 — 집계 정상, 성능 비교 유효성 미통과 및 증가 중단

|항목|결과|
|---|---|
|모드/관람자/목표|polling / 10명 / 1req/s|
|측정|60.005초, 60건 모두201, 실제0.999916req/s|
|투표API p50/p95/p99|17.61 / 46.72 / 103.27ms|
|조회API|300건, p50/p95/p99 14.11 / 33.47 / 53.95ms|
|누적목표 관측 p50/p95/p99|1036.89 / 1990.99 / 2050.66ms|
|최종값|SQL 및10명 모두416/316/216/116=1064표(시드1000+워밍업4+측정60)|
|HTTP/클라이언트오류|0 / 0|
|마지막 성공응답→전원최종값|601.50ms, 측정 종료 전 모두수신|
|요청스케줄지연 p50/p95/max|4.63 / 7.47 / 272.52ms|
|발생기|측정 중13스레드/숫자FD19개, 미발행0|
|자원 샘플|9개, 수집오류0|
|Kafka lag|polling은 Kafka 미사용으로 해당 없음|
|판정|exact=true, valid=false. 이후반복/부하증가중단|

중단 근거:
- 첫 swap 관측8714.31MiB → 최대9339.75MiB, **+625.44MiB**로 실험 전 정한512MiB 증가 기준 초과. 57.398초 샘플에서 확인, 총swap할당도9216→10240MiB. 후반9243.38MiB로 감소했지만 통과로 바꾸지 않음.
- 요청 index51의 스케줄 지연272.52ms(상대58.717초)로100ms 기준 초과. 평균 도착률이 맞아도 일정한 도착률을 재현했다고 보기 어려움. 실제 요청을 놓치지는 않음.
- 컨테이너 종료 전 PG/Kafka/Redis OOMKilled=false/running=true. 실험 앱은 측정 후 정상적으로 SIGTERM 종료. 메모리/스케줄 지연의 원인을 이 서비스로 단정하지 않음. 공유PC의 기존 앱·인프라·다른 작업 영향을 분리하지 못함.
- 반복2/3, 대응 realtime60초3회,50/100명과5/10req/s 전부미실행. 단회 무효 결과의 p95는 참고값이며3회 편차/모드별 우열 계산을 하지 않음.

자원 관측(측정구간 샘플 기준, 전체 흐름의 원본은events.jsonl):
- 공유PostgreSQL CPU0.24~5.77%, 메모리샘플 195.6MiB, 195.9MiB, 196.2MiB, 196.5MiB, 196.8MiB, 206.2MiB, 196.7MiB, 196.6MiB, 197.1MiB.
- 유휴전용Kafka CPU2.80~5.62%, 메모리샘플 312.1MiB, 312.6MiB, 329.2MiB, 328.6MiB, 329.4MiB, 329.1MiB, 329.4MiB, 330.6MiB, 332.5MiB.
- 유휴전용Redis CPU0.27~0.44%, 메모리샘플 12.63MiB, 12.63MiB, 12.62MiB, 12.62MiB, 12.6MiB, 12.79MiB, 12.77MiB, 13.27MiB, 12.87MiB.
- 앱 psCPU0.5~27.9%, RSS131760~346192KiB.
- 발생기 psCPU0.0~1.3%, RSS12976~15280KiB.
- 자원샘플 상대시각: [8.473, 15.611, 22.72, 29.847, 36.994, 44.12, 51.266, 57.398, 63.552].
- 수집간격: [7.138, 7.108, 7.128, 7.147, 7.126, 7.145, 6.132, 6.154].
- DB통계 전후(워밍업포함, 쿼리횟수 아님): [{"xact_commit": 23, "xact_rollback": 0, "blks_read": 5, "blks_hit": 26962, "tup_returned": 12273, "tup_fetched": 8687, "tup_inserted": 1159, "temp_bytes": 0, "deadlocks": 0}, {"xact_commit": 822, "xact_rollback": 0, "blks_read": 5, "blks_hit": 41325, "tup_returned": 369955, "tup_fetched": 15484, "tup_inserted": 1222, "temp_bytes": 0, "deadlocks": 0}].

## 재검증 lag 확인 상세
realtime 측정중 committed/log-end는6/6→12/12→17/17→23/23→29/29, 모든 샘플lag0. 측정중5개 수집간격5.69~5.74초. 부하 후 첫lag0조회는 종료후0.783초(정확한 처리완료시각 아님), 최종34/34/0. 총34건은 워밍업4+측정30. 자원샘플은 측정중5개이며독립스레드에서6.12~7.15초간격. 두 수집기오류0. SSE10명 재연결후최종값일치. 따라서 기존의 lag명령 대기가 자원샘플을 전부 누락시킨 문제는 이번 작은 조건에서 재검증됐다.

## 종료 상태 및 다음 조치
- 기존8080 PID22930/기존DB/9092/6379/이전예비19092·16379는 그대로. 신규 재검증 및 반복1 앱은 종료,각 전용KafkaRedis는 stop만 수행해 컨테이너·데이터 보존(삭제/볼륨삭제 없음). 실험DB와기록도모두보존. 18081리스너없음 확인.
- 새 모드파일 .env.loadtest-v10-r1은polling 유지. 중단 기준에 걸렸으므로 지금 realtime 전환 요청이나 추가 부하를 진행하지 않음.
- 재개 시 호스트 메모리 경쟁 원인을 먼저 확인하고 사용자와 불필요한 앱/이전 유휴 실험인프라 중지 여부를 정한 뒤, 새 동일 시드로 이 조건부터 유효3회를 다시 확보한다. 사용자 서비스/이전 컨테이너를 임의 종료하지 않았음. 기준을 완화해서 결과를 통과시키지 않음.
- 일반 JUnit 재실행 없음(제품코드변경없음), bootJar성공/파이썬문법검사/실제 HTTP·PostgreSQL·Redis·Kafka·SSE 재검증 수행. 반복도구의realTime분기는 아직 실제 실행하지 않았으며 동일조건60초재연결을포함한검증은미실행.
- 원본: load-tests/results/v10-r1/polling-preflight-failed.json, polling-batch.json, tool-snapshot 및tool-sha256.json; load-tests/results/v10-r1-repeat-1/polling-pilot/{summary.json,events.jsonl,http.json,votes.json}, polling-app.log/polling-runner.log/polling-container-state.txt. 이전파일덮어쓰기없음.

### 2026-10-19 16:02 KST — 사용자 요청으로 부하테스트 데이터 정리
- 사용자 명시: 실제 데이터가 있는 공유 PostgreSQL5432/기존Kafka9092/Redis6379 유지, 부하테스트 데이터만 삭제. 소유마커/컨테이너라벨/DB연결0/앱종료 확인 후 실험3세트의DB6개·역할3개·KafkaRedis컨테이너6개 및전용볼륨 삭제. 기존live_lunch_choice와서비스/8080앱은보존. 결과JSON/로그/문서와로컬설정은보존. cleanup.json에삭제runID기록.
- 삭제ID:20261019_144130_353a,20261019_153750_7c5d,20261019_155129_edd8. 기존설정파일은이제삭제된자원을가리키므로재사용하지않고새실험설정이필요.
- 사용자 후속 결정:스왑증가/발생기스케줄지연만으로다음단계를막지말고경고로남긴채진행. 기존실패원본은수정하지않음. 집계불일치/HTTP실패/OOM/lag미해소 등다른기준은유지. 이번정리중새부하는실행하지않음.

## 2026-10-19 16:08 KST — 관람자50명/1req/s polling 반복

사용자의 다음 단계 진행 지시에 따라 10명60초 유효3회 미완료 이력은 그대로 두고50명으로 진행했다. 이전 기준으로 무효였던 결과를 소급해서 유효로 변경하지 않았다. 스왑512MiB 증가/스케줄100ms 초과는 warnings로만 기록하며 미발행·HTTP/집계·관측오류·OOM·투표p95>1초 등의 중단 기준은 유지한다.

동일 후보4/시드1000표에 워밍업4표 후401/301/201/101을 시작 기준으로 사용. 앱1개, 관람자 거의 동시접속, 응답완료후2초 polling,60초씩. 각 반복에 고유 DB2개/역할/KafkaRedis가 있으며 이전 반복 컨테이너는 중지한 채 보존. 실제 서비스5432/9092/6379/8080과 공유PG 자원 경쟁은 유지. 삭제한 과거 유휴 실험 인프라가 없다는 점에서 이전10명 실험과 환경이 다르므로 단순 배수 비교 불가.

|반복|성공/시도|실제RPS|투표 p50/p95/p99 ms|조회 p50/p95/p99 ms|관측 p50/p95/p99 ms|자원샘플|판정/경고|
|---|---|---|---|---|---|---|---|
|1|60/60|0.99993|14.61/34.41/68.18|11.08/52.50/88.50|1092.18/1983.78/2085.35|9|valid=True, []|
|2|60/60|0.99991|13.51/26.48/209.27|8.34/34.94/71.11|1021.52/1993.13/2019.56|9|valid=True, []|
|3|60/60|0.99998|15.25/21.63/35.99|8.16/45.75/111.47|1024.77/1988.84/2032.98|9|valid=True, []|

### 반복1 상세
- run ID 20261019_160317_f50d, 시작2026-10-19T16:03:23.138512+09:00, 측정60.004초. DB최종{'1': 416, '2': 316, '3': 216, '4': 116}, 모든50명 동일=True, exact=True. 클라이언트오류[].
- 조회수1500, 누적목표 관측수3000, 응답본문308640bytes. 마지막성공→전원최종559.77ms. 스케줄지연{'count': 60, 'p50': 4.065124999996783, 'p95': 5.8039999999977, 'p99': 48.10270800000005, 'max': 48.10270800000005}.
- 샘플 상대시각[9.488152875, 16.589857583, 22.731222583, 28.832957666000002, 35.986777249999996, 42.105197540999995, 49.245401666, 55.384444083, 62.538370457999996]; 간격[7.102, 6.141, 6.102, 7.154, 6.118, 7.14, 6.139, 7.154]; 오류0. lag는polling미사용으로해당없음(0으로간주하지않음).
- 공유PG CPU 0.11~13.79%, 메모리 192.5MiB, 192.7MiB, 193.2MiB, 193.5MiB, 193.3MiB, 193.3MiB, 193.5MiB, 193.5MiB, 193.5MiB.
- 유휴Kafka CPU 2.50~36.89%, 메모리 311.5MiB, 312.7MiB, 329.2MiB, 329.6MiB, 329.5MiB, 332.9MiB, 332MiB, 330.8MiB, 332.9MiB.
- 유휴Redis CPU 0.22~4.58%, 메모리 12.9MiB, 12.87MiB, 12.86MiB, 12.84MiB, 12.82MiB, 12.82MiB, 12.8MiB, 12.78MiB, 12.78MiB.
- 앱 psCPU0.0~58.3%,RSS157344~207696KiB.
- 발생기 psCPU0.0~3.3%,RSS14912~17040KiB.
- 발생기 threads=[53, 53, 53, 53, 53, 53, 53, 53, 53],FD=[59, 59, 59, 59, 59, 59, 59, 59, 59],측정중swap8548.56~8772.56MiB. DB통계전후[{"xact_commit": 23, "xact_rollback": 0, "blks_read": 5, "blks_hit": 27176, "tup_returned": 12357, "tup_fetched": 8771, "tup_inserted": 1159, "temp_bytes": 0, "deadlocks": 0}, {"xact_commit": 2171, "xact_rollback": 0, "blks_read": 5, "blks_hit": 54346, "tup_returned": 1725552, "tup_fetched": 15673, "tup_inserted": 1222, "temp_bytes": 0, "deadlocks": 0}].

### 반복2 상세
- run ID 20261019_160508_2850, 시작2026-10-19T16:05:15.431506+09:00, 측정60.005초. DB최종{'1': 416, '2': 316, '3': 216, '4': 116}, 모든50명 동일=True, exact=True. 클라이언트오류[].
- 조회수1500, 누적목표 관측수3000, 응답본문308640bytes. 마지막성공→전원최종260.51ms. 스케줄지연{'count': 60, 'p50': 3.900417000004097, 'p95': 5.916625000004672, 'p99': 20.36104200000466, 'max': 20.36104200000466}.
- 샘플 상대시각[9.614581958, 16.760508332999997, 22.890663790999998, 30.001830708, 37.166846833, 44.317752041000006, 51.450051666, 58.554105583, 65.706971916]; 간격[7.146, 6.13, 7.111, 7.165, 7.151, 7.132, 7.104, 7.153]; 오류0. lag는polling미사용으로해당없음(0으로간주하지않음).
- 공유PG CPU 0.32~9.86%, 메모리 194.2MiB, 194.3MiB, 194.9MiB, 195MiB, 195.1MiB, 194.9MiB, 194.9MiB, 195.3MiB, 195.2MiB.
- 유휴Kafka CPU 2.31~7.92%, 메모리 312.5MiB, 313.2MiB, 328.7MiB, 329.9MiB, 329.5MiB, 330.2MiB, 332.6MiB, 331MiB, 330.9MiB.
- 유휴Redis CPU 0.20~0.57%, 메모리 12.63MiB, 12.61MiB, 12.59MiB, 12.57MiB, 12.55MiB, 12.54MiB, 12.52MiB, 12.5MiB, 12.48MiB.
- 앱 psCPU0.1~50.4%,RSS158768~223968KiB.
- 발생기 psCPU0.0~2.4%,RSS14560~16592KiB.
- 발생기 threads=[53, 53, 53, 53, 53, 53, 53, 53, 53],FD=[59, 59, 59, 59, 59, 59, 59, 59, 59],측정중swap7706.69~8500.56MiB. DB통계전후[{"xact_commit": 33, "xact_rollback": 0, "blks_read": 5, "blks_hit": 27362, "tup_returned": 12472, "tup_fetched": 8886, "tup_inserted": 1159, "temp_bytes": 0, "deadlocks": 0}, {"xact_commit": 2185, "xact_rollback": 0, "blks_read": 5, "blks_hit": 54717, "tup_returned": 1734798, "tup_fetched": 15851, "tup_inserted": 1222, "temp_bytes": 0, "deadlocks": 0}].

### 반복3 상세
- run ID 20261019_160624_84ea, 시작2026-10-19T16:06:30.691233+09:00, 측정60.001초. DB최종{'1': 416, '2': 316, '3': 216, '4': 116}, 모든50명 동일=True, exact=True. 클라이언트오류[].
- 조회수1500, 누적목표 관측수3000, 응답본문308640bytes. 마지막성공→전원최종550.16ms. 스케줄지연{'count': 60, 'p50': 2.381749999997851, 'p95': 5.646665999996969, 'p99': 6.723624999999345, 'max': 6.723624999999345}.
- 샘플 상대시각[9.420662667, 16.526024, 23.666353, 30.748052917, 36.867696417, 42.966863875, 50.114474959, 57.24912, 64.405211834]; 간격[7.105, 7.14, 7.082, 6.12, 6.099, 7.148, 7.135, 7.156]; 오류0. lag는polling미사용으로해당없음(0으로간주하지않음).
- 공유PG CPU 0.19~15.33%, 메모리 195.6MiB, 195.7MiB, 195.8MiB, 195.9MiB, 196.4MiB, 196.6MiB, 196.6MiB, 196.7MiB, 196.5MiB.
- 유휴Kafka CPU 2.84~11.75%, 메모리 312.8MiB, 312.7MiB, 328.4MiB, 331.5MiB, 329.8MiB, 333.4MiB, 330.2MiB, 330.9MiB, 331.4MiB.
- 유휴Redis CPU 0.25~0.40%, 메모리 12.36MiB, 12.33MiB, 12.54MiB, 12.52MiB, 12.49MiB, 12.49MiB, 12.48MiB, 12.68MiB, 12.67MiB.
- 앱 psCPU0.0~60.9%,RSS189616~308784KiB.
- 발생기 psCPU0.0~3.5%,RSS15456~17024KiB.
- 발생기 threads=[53, 53, 53, 53, 53, 53, 53, 53, 53],FD=[59, 59, 59, 59, 59, 59, 59, 59, 59],측정중swap7674.69~7682.69MiB. DB통계전후[{"xact_commit": 23, "xact_rollback": 0, "blks_read": 5, "blks_hit": 26973, "tup_returned": 12273, "tup_fetched": 8687, "tup_inserted": 1159, "temp_bytes": 0, "deadlocks": 0}, {"xact_commit": 2184, "xact_rollback": 0, "blks_read": 5, "blks_hit": 54271, "tup_returned": 1735836, "tup_fetched": 15179, "tup_inserted": 1222, "temp_bytes": 0, "deadlocks": 0}].

투표 반복별p95 범위21.63~34.41ms, 표본표준편차6.45ms. 전체요청을합친p95가아님.

조회 반복별p95 범위34.94~52.50ms, 표본표준편차8.86ms. 전체요청을합친p95가아님.

누적목표관측 반복별p95 범위1983.78~1993.13ms, 표본표준편차4.68ms. 전체요청을합친p95가아님.

원본은 load-tests/results/v50-r1/polling-batch.json 및 v50-r1-repeat-{1,2,3}/polling-pilot/{summary.json,events.jsonl,http.json,votes.json}, 앱/runner로그·container-state.txt. 스크립트 스냅샷 및해시는v50-r1/tool-snapshot,tool-sha256.json에보존.

현재 realtime50명/60초3회는미실행. 사용자 `.env.loadtest-v50-r1`의 APP_PROFILE=realtime 전환확인후만실행.100명및5/10req/s도미실행. 앱과전용컨테이너는반복종료후중지,데이터는대응realtime실험을위해보존.본표는브라우저렌더링/전체TCP비용을포함하지않으며공유PG CPU와ps CPU를직접합산하지않음.

## 2026-10-19 16:15 KST — 50명 realtime 반복 결과

사용자가 .env.loadtest-v50-r1을 realtime으로 변경한 것을 확인한 뒤 실행했다. 각 polling 반복에 대응하는 미사용 realtime DB/Redis1000표에서 시작. 워밍업4표 후60초 측정, 후보별배분/앱자원/관람자접속분포는polling과동일하다.

|반복|성공/시도|실제RPS|투표p50/p95/p99 ms|관측p50/p95/p99 ms|자원/lag샘플|최종lag/재접속|판정/경고|
|---|---|---|---|---|---|---|---|
|1|60/60|0.99994|16.46/25.77/41.80|37.71/66.36/2952.19|9/11|0/50|True/[]|
|2|60/60|0.99992|18.47/57.49/140.78|41.46/181.26/2226.01|9/11|0/50|True/[]|
|3|60/60|1.00000|17.61/34.05/99.13|39.68/170.59/2149.52|9/11|0/50|True/[]|

### realtime 반복1
- 시작2026-10-19T16:11:44.688671+09:00/run20261019_160317_f50d. SQL{'1': 416, '2': 316, '3': 216, '4': 116}, Redis{'4': 116, '3': 216, '2': 316, '1': 416}, 관람자50명동일=True, exact=True. 오류[].
- 최초전원최종수렴: 종료후0.00ms, 마지막응답후22.16ms. lag0 최초종료후조회0.9490414999999928초(정확한해소시각아님).
- 자원시각[9.697568167, 16.836653542, 23.99856175, 30.091149334, 37.248923959, 44.398957375, 50.536794, 57.671645584000004, 64.781346209],lag시각[7.959650417, 13.601638667, 19.254442625, 24.911424625000002, 30.554567292, 36.235584084, 42.015573625, 47.672466792, 53.315104375000004, 58.978505042, 64.647727417], 수집오류0. 스케줄지연{'count': 60, 'p50': 5.06129099999697, 'p95': 5.937666000001229, 'p99': 8.500749999999613, 'max': 8.500749999999613}.
- 본문HTTP7200+SSE JSON397300bytes. SSE 프레임/헤더/TCP/브라우저렌더링은제외.
- 공유PG CPU0.15~6.30%,메모리 194.7MiB, 195MiB, 195MiB, 196.3MiB, 196.2MiB, 196MiB, 196.1MiB, 196.5MiB, 196.7MiB.
- Kafka CPU4.70~13.07%,메모리 323.5MiB, 323.2MiB, 344MiB, 359.7MiB, 374.3MiB, 375.6MiB, 377MiB, 375.9MiB, 378.3MiB.
- Redis CPU0.31~0.67%,메모리 12.48MiB, 12.92MiB, 12.64MiB, 12.86MiB, 12.83MiB, 12.78MiB, 12.73MiB, 12.7MiB, 12.89MiB.
- 앱 CPU0.2~31.3%,RSS196064~361248KiB.
- 발생기 CPU0.0~2.5%,RSS16928~19440KiB.
- DB통계전후: [{"xact_commit": 37, "xact_rollback": 0, "blks_read": 5, "blks_hit": 27276, "tup_returned": 18506, "tup_fetched": 8704, "tup_inserted": 1159, "temp_bytes": 0, "deadlocks": 0}, {"xact_commit": 253, "xact_rollback": 0, "blks_read": 5, "blks_hit": 36978, "tup_returned": 34599, "tup_fetched": 14125, "tup_inserted": 1223, "temp_bytes": 0, "deadlocks": 0}].
- lag시계열(상대초/CLI행): [{"t": 2.299, "raw": "vote-log-group vote-created 0 4 4 0"}, {"t": 7.96, "raw": "vote-log-group vote-created 0 6 6 0"}, {"t": 13.602, "raw": "vote-log-group vote-created 0 11 11 0"}, {"t": 19.254, "raw": "vote-log-group vote-created 0 17 17 0"}, {"t": 24.911, "raw": "vote-log-group vote-created 0 23 23 0"}, {"t": 30.555, "raw": "vote-log-group vote-created 0 28 28 0"}, {"t": 36.236, "raw": "vote-log-group vote-created 0 34 34 0"}, {"t": 42.016, "raw": "vote-log-group vote-created 0 40 40 0"}, {"t": 47.672, "raw": "vote-log-group vote-created 0 45 45 0"}, {"t": 53.315, "raw": "vote-log-group vote-created 0 51 51 0"}, {"t": 58.979, "raw": "vote-log-group vote-created 0 57 57 0"}, {"t": 64.648, "raw": "vote-log-group vote-created 0 62 62 0"}, {"t": 67.624, "raw": "vote-log-group vote-created 0 64 64 0"}, {"t": 68.233, "raw": "vote-log-group vote-created 0 64 64 0"}].
- SSE60초종료후재연결공백 3.007~3.057초, 50명. 이구간의투표도누적목표관측지연에포함. 브라우저EventSource자체검증은아님.

### realtime 반복2
- 시작2026-10-19T16:13:02.339108+09:00/run20261019_160508_2850. SQL{'1': 416, '2': 316, '3': 216, '4': 116}, Redis{'4': 116, '3': 216, '2': 316, '1': 416}, 관람자50명동일=True, exact=True. 오류[].
- 최초전원최종수렴: 종료후0.00ms, 마지막응답후13.26ms. lag0 최초종료후조회0.8297465409999916초(정확한해소시각아님).
- 자원시각[8.450577583, 15.555483624999999, 22.932144083, 29.075223292, 36.448268583, 43.637808041999996, 50.790633375, 57.936166, 65.044781833],lag시각[7.922294, 13.610506832999999, 19.310016458, 25.427181333, 31.257005833, 36.996057416999996, 42.660814208, 48.347358125, 53.988091625, 59.643380416999996, 65.34318320800001], 수집오류0. 스케줄지연{'count': 60, 'p50': 4.375624999994443, 'p95': 5.68995799999783, 'p99': 72.8862079999999, 'max': 72.8862079999999}.
- 본문HTTP7200+SSE JSON397300bytes. SSE 프레임/헤더/TCP/브라우저렌더링은제외.
- 공유PG CPU0.10~6.49%,메모리 195.1MiB, 196.2MiB, 196MiB, 196.1MiB, 196.4MiB, 196.3MiB, 196.5MiB, 196.5MiB, 196.1MiB.
- Kafka CPU5.78~15.98%,메모리 324.1MiB, 324.6MiB, 339.7MiB, 341.8MiB, 376.2MiB, 376.4MiB, 377MiB, 379.4MiB, 379.2MiB.
- Redis CPU0.31~0.72%,메모리 13.18MiB, 13.13MiB, 13.09MiB, 13.04MiB, 12.98MiB, 12.95MiB, 12.89MiB, 12.81MiB, 12.77MiB.
- 앱 CPU2.0~10.8%,RSS167136~242240KiB.
- 발생기 CPU0.0~2.7%,RSS15648~19456KiB.
- DB통계전후: [{"xact_commit": 37, "xact_rollback": 0, "blks_read": 5, "blks_hit": 27276, "tup_returned": 18506, "tup_fetched": 8704, "tup_inserted": 1159, "temp_bytes": 0, "deadlocks": 0}, {"xact_commit": 250, "xact_rollback": 0, "blks_read": 5, "blks_hit": 36900, "tup_returned": 33695, "tup_fetched": 14116, "tup_inserted": 1222, "temp_bytes": 0, "deadlocks": 0}].
- lag시계열(상대초/CLI행): [{"t": 1.994, "raw": "vote-log-group vote-created 0 4 4 0"}, {"t": 7.922, "raw": "vote-log-group vote-created 0 6 6 0"}, {"t": 13.611, "raw": "vote-log-group vote-created 0 12 12 0"}, {"t": 19.31, "raw": "vote-log-group vote-created 0 17 17 0"}, {"t": 25.427, "raw": "vote-log-group vote-created 0 23 23 0"}, {"t": 31.257, "raw": "vote-log-group vote-created 0 29 29 0"}, {"t": 36.996, "raw": "vote-log-group vote-created 0 35 35 0"}, {"t": 42.661, "raw": "vote-log-group vote-created 0 41 41 0"}, {"t": 48.347, "raw": "vote-log-group vote-created 0 46 46 0"}, {"t": 53.988, "raw": "vote-log-group vote-created 0 52 52 0"}, {"t": 59.643, "raw": "vote-log-group vote-created 0 58 58 0"}, {"t": 65.343, "raw": "vote-log-group vote-created 0 63 63 0"}, {"t": 67.171, "raw": "vote-log-group vote-created 0 64 64 0"}, {"t": 67.791, "raw": "vote-log-group vote-created 0 64 64 0"}].
- SSE60초종료후재연결공백 3.008~3.057초, 50명. 이구간의투표도누적목표관측지연에포함. 브라우저EventSource자체검증은아님.

### realtime 반복3
- 시작2026-10-19T16:14:20.373923+09:00/run20261019_160624_84ea. SQL{'1': 416, '2': 316, '3': 216, '4': 116}, Redis{'4': 116, '3': 216, '2': 316, '1': 416}, 관람자50명동일=True, exact=True. 오류[].
- 최초전원최종수렴: 종료후0.00ms, 마지막응답후21.35ms. lag0 최초종료후조회0.870711208000003초(정확한해소시각아님).
- 자원시각[9.525439084, 16.642175124999998, 22.794318459, 29.996855791999998, 36.115748209, 43.243376334000004, 49.380744459000006, 55.520400042000006, 61.648105917],lag시각[7.7753549170000005, 13.435775584, 19.229835459, 25.230780583999998, 31.124986167, 36.763643875, 42.433458834, 48.120552375, 53.783123584, 59.463462042, 65.11913616700001], 수집오류0. 스케줄지연{'count': 60, 'p50': 4.1678330000038954, 'p95': 5.731083000000581, 'p99': 10.95241700000038, 'max': 10.95241700000038}.
- 본문HTTP7200+SSE JSON397300bytes. SSE 프레임/헤더/TCP/브라우저렌더링은제외.
- 공유PG CPU0.12~7.65%,메모리 194.7MiB, 196.1MiB, 195.9MiB, 196.9MiB, 196.9MiB, 196.5MiB, 196.2MiB, 196.3MiB, 196.2MiB.
- Kafka CPU4.47~15.33%,메모리 323.4MiB, 323.4MiB, 338.7MiB, 341.6MiB, 373.6MiB, 374.4MiB, 375.7MiB, 377.6MiB, 379.1MiB.
- Redis CPU0.21~0.51%,메모리 12.75MiB, 12.72MiB, 12.68MiB, 12.62MiB, 12.6MiB, 12.56MiB, 12.75MiB, 12.71MiB, 12.67MiB.
- 앱 CPU0.6~15.1%,RSS180912~424528KiB.
- 발생기 CPU0.0~0.5%,RSS16240~19200KiB.
- DB통계전후: [{"xact_commit": 37, "xact_rollback": 0, "blks_read": 5, "blks_hit": 27276, "tup_returned": 18506, "tup_fetched": 8704, "tup_inserted": 1159, "temp_bytes": 0, "deadlocks": 0}, {"xact_commit": 248, "xact_rollback": 0, "blks_read": 5, "blks_hit": 36883, "tup_returned": 33684, "tup_fetched": 14113, "tup_inserted": 1221, "temp_bytes": 0, "deadlocks": 0}].
- lag시계열(상대초/CLI행): [{"t": 2.095, "raw": "vote-log-group vote-created 0 4 4 0"}, {"t": 7.775, "raw": "vote-log-group vote-created 0 6 6 0"}, {"t": 13.436, "raw": "vote-log-group vote-created 0 11 11 0"}, {"t": 19.23, "raw": "vote-log-group vote-created 0 17 17 0"}, {"t": 25.231, "raw": "vote-log-group vote-created 0 23 23 0"}, {"t": 31.125, "raw": "vote-log-group vote-created 0 29 29 0"}, {"t": 36.764, "raw": "vote-log-group vote-created 0 35 35 0"}, {"t": 42.433, "raw": "vote-log-group vote-created 0 40 40 0"}, {"t": 48.121, "raw": "vote-log-group vote-created 0 46 46 0"}, {"t": 53.783, "raw": "vote-log-group vote-created 0 52 52 0"}, {"t": 59.463, "raw": "vote-log-group vote-created 0 57 57 0"}, {"t": 65.119, "raw": "vote-log-group vote-created 0 63 63 0"}, {"t": 67.321, "raw": "vote-log-group vote-created 0 64 64 0"}, {"t": 67.962, "raw": "vote-log-group vote-created 0 64 64 0"}].
- SSE60초종료후재연결공백 3.004~3.044초, 50명. 이구간의투표도누적목표관측지연에포함. 브라우저EventSource자체검증은아님.

### 같은50명조건 비교(반복별p95 범위)
- 투표 API polling: 21.63~34.41ms, 반복p95표본표준편차6.45ms. 합산p95가아님.
- 투표 API realtime: 25.77~57.49ms, 반복p95표본표준편차16.45ms. 합산p95가아님.
- 누적목표관측 polling: 1983.78~1993.13ms, 반복p95표본표준편차4.68ms. 합산p95가아님.
- 누적목표관측 realtime: 66.36~181.26ms, 반복p95표본표준편차63.48ms. 합산p95가아님.

원본: load-tests/results/v50-r1/realtime-batch.json 및 v50-r1-repeat-{1,2,3}/realtime-pilot/{summary.json,events.jsonl,http.json,votes.json}, 앱/runner로그와container-state.txt. 유효/경고/실패기록을원본그대로보존.
같은PC/공유PG/시간대편향/추가KafkaRedis자원비용을포함한구조비교이며Kafka또는Redis단독효과로단정하지않음. DB통계는워밍업/검증포함이며쿼리횟수아님.스케줄/스왑경고시해당반복의해석한계를함께읽어야한다.

## 2026-10-19 16:19 KST — 관람자100명/1req/s polling 반복

50명 조건의 양쪽 모드60초3회 통과 후 관람자만100명으로 늘렸다. 10명60초 유효3회 미완료 이력은 보존한다. 이전 기준으로 무효였던 결과를 소급해서 유효로 변경하지 않았다. 스왑512MiB 증가/스케줄100ms 초과는 warnings로만 기록하며 미발행·HTTP/집계·관측오류·OOM·투표p95>1초 등의 중단 기준은 유지한다.

동일 후보4/시드1000표에 워밍업4표 후401/301/201/101을 시작 기준으로 사용. 앱1개, 관람자 거의 동시접속, 응답완료후2초 polling,60초씩. 각 반복에 고유 DB2개/역할/KafkaRedis가 있으며 이전 반복 컨테이너는 중지한 채 보존. 실제 서비스5432/9092/6379/8080과 공유PG 자원 경쟁은 유지. 삭제한 과거 유휴 실험 인프라가 없다는 점에서 이전10명 실험과 환경이 다르므로 단순 배수 비교 불가.

|반복|성공/시도|실제RPS|투표 p50/p95/p99 ms|조회 p50/p95/p99 ms|관측 p50/p95/p99 ms|자원샘플|판정/경고|
|---|---|---|---|---|---|---|---|
|1|60/60|0.99994|14.96/53.85/98.39|5.61/46.68/92.20|1030.19/1959.57/2039.81|9|valid=True, []|
|2|60/60|0.99997|14.56/49.60/235.71|7.34/48.03/212.64|1088.40/1989.97/2225.20|9|valid=True, []|
|3|60/60|0.99997|16.30/39.07/111.24|6.17/64.30/218.58|1103.81/1641.12/2119.08|9|valid=True, []|

### 반복1 상세
- run ID 20261019_161543_b180, 시작2026-10-19T16:15:51.458729+09:00, 측정60.004초. DB최종{'1': 416, '2': 316, '3': 216, '4': 116}, 모든100명 동일=True, exact=True. 클라이언트오류[].
- 조회수3000, 누적목표 관측수6000, 응답본문610140bytes. 마지막성공→전원최종360.40ms. 스케줄지연{'count': 60, 'p50': 5.014333999998399, 'p95': 13.8836669999991, 'p99': 55.23120899999867, 'max': 55.23120899999867}.
- 샘플 상대시각[8.710408042, 14.846802125, 20.999312458, 27.127524583, 34.268160958, 41.438093333, 48.866005042, 56.023128042, 63.118531667]; 간격[6.136, 6.153, 6.128, 7.141, 7.17, 7.428, 7.157, 7.095]; 오류0. lag는polling미사용으로해당없음(0으로간주하지않음).
- 공유PG CPU 0.16~13.02%, 메모리 213.9MiB, 214.2MiB, 214.5MiB, 214.4MiB, 214.4MiB, 214.8MiB, 214.3MiB, 214.7MiB, 215MiB.
- 유휴Kafka CPU 2.51~10.01%, 메모리 311.6MiB, 312.7MiB, 313.1MiB, 328.6MiB, 329.4MiB, 333.3MiB, 332.5MiB, 330.7MiB, 330.4MiB.
- 유휴Redis CPU 0.22~0.60%, 메모리 12.46MiB, 12.44MiB, 12.42MiB, 12.4MiB, 12.39MiB, 12.37MiB, 12.35MiB, 12.32MiB, 12.3MiB.
- 앱 psCPU0.4~81.6%,RSS144032~410640KiB.
- 발생기 psCPU0.0~2.5%,RSS16768~19584KiB.
- 발생기 threads=[103, 103, 103, 103, 103, 103, 103, 103, 103],FD=[109, 109, 109, 109, 109, 109, 109, 109, 109],측정중swap7706.12~7827.88MiB. DB통계전후[{"xact_commit": 23, "xact_rollback": 0, "blks_read": 5, "blks_hit": 27186, "tup_returned": 12365, "tup_fetched": 8779, "tup_inserted": 1159, "temp_bytes": 0, "deadlocks": 0}, {"xact_commit": 3886, "xact_rollback": 0, "blks_read": 5, "blks_hit": 71627, "tup_returned": 3441577, "tup_fetched": 15418, "tup_inserted": 1222, "temp_bytes": 0, "deadlocks": 0}].

### 반복2 상세
- run ID 20261019_161700_40e0, 시작2026-10-19T16:17:06.474196+09:00, 측정60.002초. DB최종{'1': 416, '2': 316, '3': 216, '4': 116}, 모든100명 동일=True, exact=True. 클라이언트오류[].
- 조회수3000, 누적목표 관측수6000, 응답본문610140bytes. 마지막성공→전원최종717.17ms. 스케줄지연{'count': 60, 'p50': 2.7829169999975534, 'p95': 6.284499999999582, 'p99': 18.319709000000017, 'max': 18.319709000000017}.
- 샘플 상대시각[8.892006792, 16.022771792, 23.172602917000003, 29.293265417, 35.458578834, 42.605743417, 49.728007417, 56.889158125, 64.046188334]; 간격[7.131, 7.15, 6.121, 6.165, 7.147, 7.122, 7.161, 7.157]; 오류0. lag는polling미사용으로해당없음(0으로간주하지않음).
- 공유PG CPU 0.38~10.01%, 메모리 231MiB, 231.1MiB, 231.4MiB, 231.3MiB, 232MiB, 232MiB, 231.8MiB, 232.2MiB, 232.1MiB.
- 유휴Kafka CPU 1.99~7.74%, 메모리 312.7MiB, 313.3MiB, 329.8MiB, 331.1MiB, 330.3MiB, 334.2MiB, 330.5MiB, 331.7MiB, 333.6MiB.
- 유휴Redis CPU 0.23~0.42%, 메모리 12.79MiB, 12.76MiB, 12.74MiB, 12.73MiB, 12.7MiB, 12.66MiB, 12.64MiB, 12.61MiB, 12.58MiB.
- 앱 psCPU0.2~51.4%,RSS176144~266048KiB.
- 발생기 psCPU0.0~0.2%,RSS17184~19424KiB.
- 발생기 threads=[103, 103, 103, 103, 103, 103, 103, 103, 103],FD=[109, 109, 109, 109, 109, 109, 109, 109, 109],측정중swap7787.88~7803.88MiB. DB통계전후[{"xact_commit": 23, "xact_rollback": 0, "blks_read": 5, "blks_hit": 26973, "tup_returned": 12273, "tup_fetched": 8687, "tup_inserted": 1159, "temp_bytes": 0, "deadlocks": 0}, {"xact_commit": 3876, "xact_rollback": 0, "blks_read": 5, "blks_hit": 70894, "tup_returned": 3440453, "tup_fetched": 15236, "tup_inserted": 1222, "temp_bytes": 0, "deadlocks": 0}].

### 반복3 상세
- run ID 20261019_161815_9456, 시작2026-10-19T16:18:21.468312+09:00, 측정60.002초. DB최종{'1': 416, '2': 316, '3': 216, '4': 116}, 모든100명 동일=True, exact=True. 클라이언트오류[].
- 조회수3000, 누적목표 관측수6000, 응답본문610140bytes. 마지막성공→전원최종659.66ms. 스케줄지연{'count': 60, 'p50': 2.8487080000019205, 'p95': 5.704833000002907, 'p99': 17.56254100000021, 'max': 17.56254100000021}.
- 샘플 상대시각[8.540638625000001, 15.704806709000001, 21.891479750000002, 29.066226292, 35.186255209, 41.305668834, 48.43845425, 55.600154334, 62.81822075]; 간격[7.164, 6.187, 7.175, 6.12, 6.119, 7.133, 7.162, 7.218]; 오류0. lag는polling미사용으로해당없음(0으로간주하지않음).
- 공유PG CPU 0.41~17.13%, 메모리 250.2MiB, 250.6MiB, 250.6MiB, 250.9MiB, 250.9MiB, 250.6MiB, 251.4MiB, 258.6MiB, 251.6MiB.
- 유휴Kafka CPU 2.03~33.88%, 메모리 310.9MiB, 311.9MiB, 329MiB, 328.3MiB, 329.4MiB, 332.9MiB, 329.5MiB, 330.1MiB, 332.3MiB.
- 유휴Redis CPU 0.18~0.34%, 메모리 12.28MiB, 12.25MiB, 12.45MiB, 12.44MiB, 12.62MiB, 12.59MiB, 12.58MiB, 12.57MiB, 12.55MiB.
- 앱 psCPU0.6~49.0%,RSS165680~399456KiB.
- 발생기 psCPU0.1~3.2%,RSS16640~19664KiB.
- 발생기 threads=[103, 103, 103, 103, 103, 103, 103, 103, 103],FD=[109, 109, 109, 109, 109, 109, 109, 109, 109],측정중swap7753.31~7783.81MiB. DB통계전후[{"xact_commit": 25, "xact_rollback": 0, "blks_read": 5, "blks_hit": 27538, "tup_returned": 12647, "tup_fetched": 9061, "tup_inserted": 1159, "temp_bytes": 0, "deadlocks": 0}, {"xact_commit": 3874, "xact_rollback": 0, "blks_read": 5, "blks_hit": 72200, "tup_returned": 3442261, "tup_fetched": 15827, "tup_inserted": 1222, "temp_bytes": 0, "deadlocks": 0}].

투표 반복별p95 범위39.07~53.85ms, 표본표준편차7.61ms. 전체요청을합친p95가아님.

조회 반복별p95 범위46.68~64.30ms, 표본표준편차9.81ms. 전체요청을합친p95가아님.

누적목표관측 반복별p95 범위1641.12~1989.97ms, 표본표준편차193.23ms. 전체요청을합친p95가아님.

원본은 load-tests/results/v100-r1/polling-batch.json 및 v100-r1-repeat-{1,2,3}/polling-pilot/{summary.json,events.jsonl,http.json,votes.json}, 앱/runner로그·container-state.txt. 스크립트 스냅샷 및해시는v100-r1/tool-snapshot,tool-sha256.json에보존.

현재 realtime100명/60초3회는미실행. 사용자 `.env.loadtest-v100-r1`의 APP_PROFILE=realtime 전환확인후만실행.100명realtime및5/10req/s는미실행. 앱과전용컨테이너는반복종료후중지,데이터는대응realtime실험을위해보존.본표는브라우저렌더링/전체TCP비용을포함하지않으며공유PG CPU와ps CPU를직접합산하지않음.

## 2026-10-19 16:29 KST — v100-r1 realtime 반복 결과

동일 시드1000표+워밍업4표 후 측정. 관람자 수·투표 도착률은 조건명과 summary에 명시. 각 반복은 독립DB/전용KafkaRedis, 기존 실제서비스와 공유PostgreSQL은 유지. 결과표의 p95 편차는 반복별 차이이며 합산요청 p95가 아니다.

|반복|성공/시도|실제 RPS|투표p50/p95/p99 ms|조회p95 ms|누적목표p50/p95/p99 ms|자원/lag샘플|최종lag/재접속|valid/경고|
|---|---|---|---|---|---|---|---|---|
|1|60/60|0.99998|15.80/44.57/526.14|None|38.69/312.08/2258.31|9/11|0/100|True/[]|
|2|60/60|0.99992|15.06/23.74/70.92|None|39.22/161.00/2863.58|9/11|0/100|True/[]|
|3|60/60|0.99997|16.17/27.29/40.17|None|39.54/67.95/2290.67|9/11|0/100|True/[]|

### 반복1 상세
- run20261019_161543_b180, 시작2026-10-19T16:25:11.199767+09:00, 100명/1.0req/s/60.001초. 최종SQL{'1': 416, '2': 316, '3': 216, '4': 116}, Redis{'4': 116, '3': 216, '2': 316, '1': 416}, 전관람자동일=True. 정확성True, 오류[], 중단사유[].
- 조회수0, 누적목표관측수6000; 마지막응답→전원최종22.27904099999023ms, 종료후전원최종0ms. 스케줄지연{'count': 60, 'p50': 3.4157920000019715, 'p95': 5.963792000002854, 'p99': 12.963416999999922, 'max': 12.963416999999922}.
- 자원시각[10.473271291, 17.655049541, 24.77644525, 31.948619041, 39.066050833, 45.213876, 52.369416666, 59.518066041, 65.65736266600001], lag시각[8.3572655, 14.090315916, 19.720823166, 25.48425025, 31.382383583, 37.084606083, 42.763101333, 48.436885958, 54.088368291, 59.762974375, 65.40218491600001]; 수집오류0. 자원간격[7.182, 7.121, 7.172, 7.117, 6.148, 7.156, 7.149, 6.139].
- HTTP본문7200+SSE JSON794600bytes. 헤더/프레임/TCP/브라우저렌더링제외.
- 공유PG CPU0.10~9.23%,메모리 246.6MiB, 246.7MiB, 247.1MiB, 247MiB, 247.1MiB, 247.2MiB, 248.6MiB, 249.3MiB, 249.6MiB.
- Kafka CPU5.79~22.83%,메모리 323.3MiB, 326MiB, 339.1MiB, 373MiB, 382.5MiB, 375.8MiB, 379.4MiB, 378.9MiB, 379.4MiB.
- Redis CPU0.31~0.53%,메모리 12.55MiB, 12.75MiB, 12.69MiB, 12.65MiB, 12.61MiB, 12.57MiB, 12.53MiB, 12.49MiB, 12.44MiB.
- 앱 psCPU0.1~7.9%,RSS112848~324032KiB.
- 발생기 psCPU0.0~1.0%,RSS13616~21440KiB.
- 발생기 스레드/FD/swap: [{"t": 10.473, "threads": 104, "fds": 209, "swap": "vm.swapusage: total = 9216.00M  used = 7806.62M  free = 1409.38M  (encrypted)"}, {"t": 17.655, "threads": 104, "fds": 209, "swap": "vm.swapusage: total = 9216.00M  used = 7798.62M  free = 1417.38M  (encrypted)"}, {"t": 24.776, "threads": 104, "fds": 211, "swap": "vm.swapusage: total = 9216.00M  used = 7798.62M  free = 1417.38M  (encrypted)"}, {"t": 31.949, "threads": 104, "fds": 209, "swap": "vm.swapusage: total = 9216.00M  used = 7774.62M  free = 1441.38M  (encrypted)"}, {"t": 39.066, "threads": 104, "fds": 209, "swap": "vm.swapusage: total = 9216.00M  used = 7774.62M  free = 1441.38M  (encrypted)"}, {"t": 45.214, "threads": 104, "fds": 209, "swap": "vm.swapusage: total = 9216.00M  used = 7774.62M  free = 1441.38M  (encrypted)"}, {"t": 52.369, "threads": 104, "fds": 209, "swap": "vm.swapusage: total = 9216.00M  used = 7774.62M  free = 1441.38M  (encrypted)"}, {"t": 59.518, "threads": 104, "fds": 211, "swap": "vm.swapusage: total = 9216.00M  used = 7750.62M  free = 1465.38M  (encrypted)"}, {"t": 65.657, "threads": 104, "fds": 209, "swap": "vm.swapusage: total = 9216.00M  used = 7750.62M  free = 1465.38M  (encrypted)"}]
- DB통계전후: [{"xact_commit": 41, "xact_rollback": 0, "blks_read": 5, "blks_hit": 27422, "tup_returned": 20312, "tup_fetched": 8734, "tup_inserted": 1159, "temp_bytes": 0, "deadlocks": 0}, {"xact_commit": 303, "xact_rollback": 0, "blks_read": 5, "blks_hit": 37058, "tup_returned": 35676, "tup_fetched": 14122, "tup_inserted": 1222, "temp_bytes": 0, "deadlocks": 0}]. 워밍업/검증포함,SQL횟수아님.
- lag시계열: [{"t": 2.577, "raw": "vote-log-group vote-created 0 4 4 0"}, {"t": 8.357, "raw": "vote-log-group vote-created 0 6 6 0"}, {"t": 14.09, "raw": "vote-log-group vote-created 0 12 12 0"}, {"t": 19.721, "raw": "vote-log-group vote-created 0 17 17 0"}, {"t": 25.484, "raw": "vote-log-group vote-created 0 23 23 0"}, {"t": 31.382, "raw": "vote-log-group vote-created 0 29 29 0"}, {"t": 37.085, "raw": "vote-log-group vote-created 0 35 35 0"}, {"t": 42.763, "raw": "vote-log-group vote-created 0 40 40 0"}, {"t": 48.437, "raw": "vote-log-group vote-created 0 46 46 0"}, {"t": 54.088, "raw": "vote-log-group vote-created 0 52 52 0"}, {"t": 59.763, "raw": "vote-log-group vote-created 0 57 57 0"}, {"t": 65.402, "raw": "vote-log-group vote-created 0 63 63 0"}, {"t": 67.828, "raw": "vote-log-group vote-created 0 64 64 0"}, {"t": 68.469, "raw": "vote-log-group vote-created 0 64 64 0"}]
- 종료후첫lag0조회0.9082012089999978초. 정확한해소시각이아니며샘플간lag최댓값은알수없음.
- SSE재연결 100명, 공백3.021~3.106초. 재연결중관측지연도통계에포함. 실제브라우저EventSource검증과구분.

### 반복2 상세
- run20261019_161700_40e0, 시작2026-10-19T16:26:29.550454+09:00, 100명/1.0req/s/60.005초. 최종SQL{'1': 416, '2': 316, '3': 216, '4': 116}, Redis{'4': 116, '3': 216, '2': 316, '1': 416}, 전관람자동일=True. 정확성True, 오류[], 중단사유[].
- 조회수0, 누적목표관측수6000; 마지막응답→전원최종23.19454199999882ms, 종료후전원최종0ms. 스케줄지연{'count': 60, 'p50': 5.14383299999821, 'p95': 6.041666999999862, 'p99': 8.426582999999432, 'max': 8.426582999999432}.
- 자원시각[9.43583525, 16.665290167000002, 23.813562709000003, 30.911377834000003, 38.076296625, 45.208541084, 51.357400417, 58.503574375, 64.60810875], lag시각[7.880933667, 14.157727584, 19.887779167, 25.660428459000002, 31.287985042000003, 36.92566975, 42.592400917, 48.188911334, 53.834262249999995, 59.47662675, 65.096405709]; 수집오류0. 자원간격[7.229, 7.148, 7.098, 7.165, 7.132, 6.149, 7.146, 6.105].
- HTTP본문7200+SSE JSON794600bytes. 헤더/프레임/TCP/브라우저렌더링제외.
- 공유PG CPU0.16~6.00%,메모리 248.5MiB, 249.3MiB, 249.7MiB, 249.6MiB, 249.7MiB, 250MiB, 249.5MiB, 249.7MiB, 249.7MiB.
- Kafka CPU6.52~13.53%,메모리 323.3MiB, 323.1MiB, 338.7MiB, 359.1MiB, 374.7MiB, 375.1MiB, 378.2MiB, 378.6MiB, 379.5MiB.
- Redis CPU0.36~1.22%,메모리 12.71MiB, 12.67MiB, 12.62MiB, 12.56MiB, 12.54MiB, 12.5MiB, 12.47MiB, 12.42MiB, 12.39MiB.
- 앱 psCPU0.4~53.6%,RSS179440~281792KiB.
- 발생기 psCPU0.0~3.1%,RSS17744~23392KiB.
- 발생기 스레드/FD/swap: [{"t": 9.436, "threads": 104, "fds": 209, "swap": "vm.swapusage: total = 9216.00M  used = 8095.06M  free = 1120.94M  (encrypted)"}, {"t": 16.665, "threads": 104, "fds": 209, "swap": "vm.swapusage: total = 9216.00M  used = 7983.00M  free = 1233.00M  (encrypted)"}, {"t": 23.814, "threads": 104, "fds": 209, "swap": "vm.swapusage: total = 9216.00M  used = 8160.56M  free = 1055.44M  (encrypted)"}, {"t": 30.911, "threads": 104, "fds": 211, "swap": "vm.swapusage: total = 9216.00M  used = 8144.56M  free = 1071.44M  (encrypted)"}, {"t": 38.076, "threads": 104, "fds": 209, "swap": "vm.swapusage: total = 9216.00M  used = 8128.56M  free = 1087.44M  (encrypted)"}, {"t": 45.208, "threads": 104, "fds": 209, "swap": "vm.swapusage: total = 9216.00M  used = 8112.56M  free = 1103.44M  (encrypted)"}, {"t": 51.357, "threads": 104, "fds": 209, "swap": "vm.swapusage: total = 9216.00M  used = 8112.56M  free = 1103.44M  (encrypted)"}, {"t": 58.504, "threads": 104, "fds": 209, "swap": "vm.swapusage: total = 9216.00M  used = 8096.56M  free = 1119.44M  (encrypted)"}, {"t": 64.608, "threads": 104, "fds": 211, "swap": "vm.swapusage: total = 9216.00M  used = 8096.56M  free = 1119.44M  (encrypted)"}]
- DB통계전후: [{"xact_commit": 41, "xact_rollback": 0, "blks_read": 5, "blks_hit": 27422, "tup_returned": 20312, "tup_fetched": 8734, "tup_inserted": 1159, "temp_bytes": 0, "deadlocks": 0}, {"xact_commit": 303, "xact_rollback": 0, "blks_read": 5, "blks_hit": 37072, "tup_returned": 35683, "tup_fetched": 14128, "tup_inserted": 1222, "temp_bytes": 0, "deadlocks": 0}]. 워밍업/검증포함,SQL횟수아님.
- lag시계열: [{"t": 2.175, "raw": "vote-log-group vote-created 0 4 4 0"}, {"t": 7.881, "raw": "vote-log-group vote-created 0 6 6 0"}, {"t": 14.158, "raw": "vote-log-group vote-created 0 12 12 0"}, {"t": 19.888, "raw": "vote-log-group vote-created 0 18 18 0"}, {"t": 25.66, "raw": "vote-log-group vote-created 0 24 24 0"}, {"t": 31.288, "raw": "vote-log-group vote-created 0 29 29 0"}, {"t": 36.926, "raw": "vote-log-group vote-created 0 35 35 0"}, {"t": 42.592, "raw": "vote-log-group vote-created 0 40 41 1"}, {"t": 48.189, "raw": "vote-log-group vote-created 0 46 46 0"}, {"t": 53.834, "raw": "vote-log-group vote-created 0 52 52 0"}, {"t": 59.477, "raw": "vote-log-group vote-created 0 57 57 0"}, {"t": 65.096, "raw": "vote-log-group vote-created 0 63 63 0"}, {"t": 67.44, "raw": "vote-log-group vote-created 0 64 64 0"}, {"t": 68.047, "raw": "vote-log-group vote-created 0 64 64 0"}]
- 종료후첫lag0조회0.8952718749999917초. 정확한해소시각이아니며샘플간lag최댓값은알수없음.
- SSE재연결 100명, 공백3.011~3.076초. 재연결중관측지연도통계에포함. 실제브라우저EventSource검증과구분.

### 반복3 상세
- run20261019_161815_9456, 시작2026-10-19T16:27:47.667397+09:00, 100명/1.0req/s/60.002초. 최종SQL{'1': 416, '2': 316, '3': 216, '4': 116}, Redis{'4': 116, '3': 216, '2': 316, '1': 416}, 전관람자동일=True. 정확성True, 오류[], 중단사유[].
- 조회수0, 누적목표관측수6000; 마지막응답→전원최종30.84629200000677ms, 종료후전원최종0ms. 스케줄지연{'count': 60, 'p50': 4.176999999998543, 'p95': 5.940416999999698, 'p99': 8.870250000001079, 'max': 8.870250000001079}.
- 자원시각[8.463702667, 15.592250332999999, 21.744644958000002, 28.895719625, 36.014297875, 43.155708249999996, 50.311602416999996, 57.465030416999994, 63.581960333], lag시각[7.757549083000001, 13.426232333, 19.064739, 24.684779542, 30.425643542, 36.100243041999995, 41.787779, 47.479561083, 53.141871875, 58.788186708, 64.403578792]; 수집오류0. 자원간격[7.129, 6.152, 7.151, 7.119, 7.141, 7.156, 7.153, 6.117].
- HTTP본문7200+SSE JSON794600bytes. 헤더/프레임/TCP/브라우저렌더링제외.
- 공유PG CPU0.40~6.05%,메모리 248.7MiB, 249.2MiB, 249MiB, 249MiB, 249.1MiB, 249.2MiB, 249.5MiB, 249.4MiB, 249.1MiB.
- Kafka CPU6.48~11.89%,메모리 324.4MiB, 323.6MiB, 340.2MiB, 339.8MiB, 374.7MiB, 375.7MiB, 376.1MiB, 379.6MiB, 377MiB.
- Redis CPU0.32~0.52%,메모리 12.79MiB, 12.75MiB, 12.68MiB, 12.62MiB, 12.57MiB, 12.78MiB, 12.74MiB, 12.69MiB, 12.65MiB.
- 앱 psCPU0.6~8.8%,RSS197776~414160KiB.
- 발생기 psCPU0.0~0.7%,RSS18656~21408KiB.
- 발생기 스레드/FD/swap: [{"t": 8.464, "threads": 104, "fds": 209, "swap": "vm.swapusage: total = 9216.00M  used = 8064.56M  free = 1151.44M  (encrypted)"}, {"t": 15.592, "threads": 104, "fds": 209, "swap": "vm.swapusage: total = 9216.00M  used = 8064.56M  free = 1151.44M  (encrypted)"}, {"t": 21.745, "threads": 104, "fds": 209, "swap": "vm.swapusage: total = 9216.00M  used = 8048.56M  free = 1167.44M  (encrypted)"}, {"t": 28.896, "threads": 104, "fds": 209, "swap": "vm.swapusage: total = 9216.00M  used = 8048.56M  free = 1167.44M  (encrypted)"}, {"t": 36.014, "threads": 104, "fds": 211, "swap": "vm.swapusage: total = 9216.00M  used = 8032.56M  free = 1183.44M  (encrypted)"}, {"t": 43.156, "threads": 104, "fds": 209, "swap": "vm.swapusage: total = 9216.00M  used = 8182.94M  free = 1033.06M  (encrypted)"}, {"t": 50.312, "threads": 104, "fds": 209, "swap": "vm.swapusage: total = 9216.00M  used = 8182.94M  free = 1033.06M  (encrypted)"}, {"t": 57.465, "threads": 104, "fds": 209, "swap": "vm.swapusage: total = 9216.00M  used = 8174.94M  free = 1041.06M  (encrypted)"}, {"t": 63.582, "threads": 104, "fds": 109, "swap": "vm.swapusage: total = 9216.00M  used = 8166.94M  free = 1049.06M  (encrypted)"}]
- DB통계전후: [{"xact_commit": 41, "xact_rollback": 0, "blks_read": 5, "blks_hit": 27422, "tup_returned": 20312, "tup_fetched": 8734, "tup_inserted": 1159, "temp_bytes": 0, "deadlocks": 0}, {"xact_commit": 304, "xact_rollback": 0, "blks_read": 5, "blks_hit": 35918, "tup_returned": 35112, "tup_fetched": 13558, "tup_inserted": 1222, "temp_bytes": 0, "deadlocks": 0}]. 워밍업/검증포함,SQL횟수아님.
- lag시계열: [{"t": 2.057, "raw": "vote-log-group vote-created 0 4 4 0"}, {"t": 7.758, "raw": "vote-log-group vote-created 0 6 6 0"}, {"t": 13.426, "raw": "vote-log-group vote-created 0 11 11 0"}, {"t": 19.065, "raw": "vote-log-group vote-created 0 17 17 0"}, {"t": 24.685, "raw": "vote-log-group vote-created 0 23 23 0"}, {"t": 30.426, "raw": "vote-log-group vote-created 0 28 28 0"}, {"t": 36.1, "raw": "vote-log-group vote-created 0 34 34 0"}, {"t": 41.788, "raw": "vote-log-group vote-created 0 40 40 0"}, {"t": 47.48, "raw": "vote-log-group vote-created 0 45 46 1"}, {"t": 53.142, "raw": "vote-log-group vote-created 0 51 51 0"}, {"t": 58.788, "raw": "vote-log-group vote-created 0 57 57 0"}, {"t": 64.404, "raw": "vote-log-group vote-created 0 62 62 0"}, {"t": 67.287, "raw": "vote-log-group vote-created 0 64 64 0"}, {"t": 67.951, "raw": "vote-log-group vote-created 0 64 64 0"}]
- 종료후첫lag0조회0.8656406249999975초. 정확한해소시각이아니며샘플간lag최댓값은알수없음.
- SSE재연결 100명, 공백3.011~3.074초. 재연결중관측지연도통계에포함. 실제브라우저EventSource검증과구분.

투표 반복p95범위23.74~44.57ms/표본표준편차11.14ms. 합산p95아님.

누적목표관측 반복p95범위67.95~312.08ms/표본표준편차123.21ms. 합산p95아님.

원본: load-tests/results/v100-r1/realtime-batch.json 및 v100-r1-repeat-{1,2,3}/realtime-pilot/{summary.json,events.jsonl,http.json,votes.json}, 앱/runner로그 및 container-state.txt. 로컬공유자원/시간대편향/추가인프라비용을포함하며관측차이를단일기술효과로단정하지않음. 같은투표를여러관람자가관측하므로지연표본은독립적이지않음.

## 2026-10-19 16:34 KST — v100-r5 polling 반복 결과

동일 시드1000표+워밍업4표 후 측정. 관람자 수·투표 도착률은 조건명과 summary에 명시. 각 반복은 독립DB/전용KafkaRedis, 기존 실제서비스와 공유PostgreSQL은 유지. 결과표의 p95 편차는 반복별 차이이며 합산요청 p95가 아니다.

|반복|성공/시도|실제 RPS|투표p50/p95/p99 ms|조회p95 ms|누적목표p50/p95/p99 ms|자원/lag샘플|최종lag/재접속|valid/경고|
|---|---|---|---|---|---|---|---|---|
|1|300/300|4.99972|11.17/108.02/282.71|56.18620799999974|1039.72/1957.71/2143.64|8/0|None/0|True/['발생기 스케줄 지연 100ms 초과', 'swap 512MiB 이상 증가']|
|2|300/300|4.99969|12.04/19.42/30.06|19.96016599999706|1030.24/1908.49/1999.12|9/0|None/0|True/[]|
|3|300/300|4.99991|12.75/20.39/25.89|22.667916999999704|1021.44/1932.01/2008.71|9/0|None/0|True/[]|

### 반복1 상세
- run20261019_162946_29b8, 시작2026-10-19T16:30:09.651357+09:00, 100명/5.0req/s/60.003초. 최종SQL{'1': 476, '2': 376, '3': 276, '4': 176}, RedisNone, 전관람자동일=True. 정확성True, 오류[], 중단사유[].
- 조회수2962, 누적목표관측수30000; 마지막응답→전원최종1400.2583749999928ms, 종료후전원최종1216.1932079999929ms. 스케줄지연{'count': 300, 'p50': 4.470040999990488, 'p95': 8.666666000003431, 'p99': 154.35854100000057, 'max': 528.542833000003}.
- 자원시각[21.210904667, 27.328296167, 34.425281874999996, 41.5327175, 47.645250624999996, 54.73812375, 60.924826292, 68.003505042], lag시각[]; 수집오류0. 자원간격[6.117, 7.097, 7.107, 6.113, 7.093, 6.187, 7.079].
- HTTP본문631062+SSE JSON0bytes. 헤더/프레임/TCP/브라우저렌더링제외.
- 공유PG CPU2.71~5.79%,메모리 269.2MiB, 269.2MiB, 269.8MiB, 269.6MiB, 269.4MiB, 269.9MiB, 269.7MiB, 269.8MiB.
- Kafka CPU2.76~6.51%,메모리 329.9MiB, 329.5MiB, 329.8MiB, 330MiB, 332.7MiB, 330.4MiB, 331.3MiB, 330.8MiB.
- Redis CPU0.18~0.38%,메모리 12.84MiB, 12.79MiB, 12.75MiB, 12.74MiB, 12.72MiB, 12.7MiB, 12.69MiB, 12.67MiB.
- 앱 psCPU2.6~41.5%,RSS176016~236336KiB.
- 발생기 psCPU0.1~1.1%,RSS15248~18752KiB.
- 발생기 스레드/FD/swap: [{"t": 21.211, "threads": 106, "fds": 112, "swap": "vm.swapusage: total = 10240.00M  used = 9566.19M  free = 673.81M  (encrypted)"}, {"t": 27.328, "threads": 106, "fds": 112, "swap": "vm.swapusage: total = 10240.00M  used = 9542.19M  free = 697.81M  (encrypted)"}, {"t": 34.425, "threads": 106, "fds": 112, "swap": "vm.swapusage: total = 10240.00M  used = 9542.19M  free = 697.81M  (encrypted)"}, {"t": 41.533, "threads": 106, "fds": 112, "swap": "vm.swapusage: total = 10240.00M  used = 9510.19M  free = 729.81M  (encrypted)"}, {"t": 47.645, "threads": 106, "fds": 112, "swap": "vm.swapusage: total = 10240.00M  used = 9486.19M  free = 753.81M  (encrypted)"}, {"t": 54.738, "threads": 106, "fds": 112, "swap": "vm.swapusage: total = 10240.00M  used = 9505.44M  free = 734.56M  (encrypted)"}, {"t": 60.925, "threads": 106, "fds": 112, "swap": "vm.swapusage: total = 10240.00M  used = 9631.88M  free = 608.12M  (encrypted)"}, {"t": 68.003, "threads": 106, "fds": 112, "swap": "vm.swapusage: total = 10240.00M  used = 9623.88M  free = 616.12M  (encrypted)"}]
- DB통계전후: [{"xact_commit": 36, "xact_rollback": 0, "blks_read": 5, "blks_hit": 29028, "tup_returned": 19762, "tup_fetched": 9979, "tup_inserted": 1159, "temp_bytes": 0, "deadlocks": 0}, {"xact_commit": 4428, "xact_rollback": 0, "blks_read": 5, "blks_hit": 96766, "tup_returned": 3910906, "tup_fetched": 15529, "tup_inserted": 1463, "temp_bytes": 0, "deadlocks": 0}]. 워밍업/검증포함,SQL횟수아님.
- Kafka lag/SSE는polling미사용으로해당없음. 0으로간주하지않음.

### 반복2 상세
- run20261019_163126_db6c, 시작2026-10-19T16:31:34.480645+09:00, 100명/5.0req/s/60.004초. 최종SQL{'1': 476, '2': 376, '3': 276, '4': 176}, RedisNone, 전관람자동일=True. 정확성True, 오류[], 중단사유[].
- 조회수3000, 누적목표관측수30000; 마지막응답→전원최종1185.7718750000004ms, 종료후전원최종999.1160839999935ms. 스케줄지연{'count': 300, 'p50': 3.214791999997857, 'p95': 5.369874999992419, 'p99': 8.19595900000003, 'max': 16.99445900000285}.
- 자원시각[9.5296305, 15.702722541, 22.844281291, 29.949069791, 37.077216, 44.338749625, 51.486905375, 57.660180874999995, 63.810770624999996], lag시각[]; 수집오류0. 자원간격[6.173, 7.142, 7.105, 7.128, 7.262, 7.148, 6.173, 6.151].
- HTTP본문638700+SSE JSON0bytes. 헤더/프레임/TCP/브라우저렌더링제외.
- 공유PG CPU1.55~13.00%,메모리 269.9MiB, 270.8MiB, 271.6MiB, 271.4MiB, 271MiB, 271.3MiB, 271.3MiB, 271.7MiB, 271.4MiB.
- Kafka CPU2.32~16.12%,메모리 312.1MiB, 313.3MiB, 329.6MiB, 333.1MiB, 329.9MiB, 329.9MiB, 332.4MiB, 331.1MiB, 330.5MiB.
- Redis CPU0.24~0.34%,메모리 12.46MiB, 12.43MiB, 12.64MiB, 12.59MiB, 12.56MiB, 12.55MiB, 12.52MiB, 12.51MiB, 12.48MiB.
- 앱 psCPU2.0~46.4%,RSS196736~284928KiB.
- 발생기 psCPU0.1~3.9%,RSS16688~21008KiB.
- 발생기 스레드/FD/swap: [{"t": 9.53, "threads": 103, "fds": 109, "swap": "vm.swapusage: total = 10240.00M  used = 9633.12M  free = 606.88M  (encrypted)"}, {"t": 15.703, "threads": 103, "fds": 109, "swap": "vm.swapusage: total = 10240.00M  used = 9585.12M  free = 654.88M  (encrypted)"}, {"t": 22.844, "threads": 103, "fds": 109, "swap": "vm.swapusage: total = 10240.00M  used = 9561.12M  free = 678.88M  (encrypted)"}, {"t": 29.949, "threads": 103, "fds": 109, "swap": "vm.swapusage: total = 10240.00M  used = 9561.12M  free = 678.88M  (encrypted)"}, {"t": 37.077, "threads": 103, "fds": 109, "swap": "vm.swapusage: total = 10240.00M  used = 9553.12M  free = 686.88M  (encrypted)"}, {"t": 44.339, "threads": 103, "fds": 109, "swap": "vm.swapusage: total = 10240.00M  used = 9545.12M  free = 694.88M  (encrypted)"}, {"t": 51.487, "threads": 103, "fds": 109, "swap": "vm.swapusage: total = 10240.00M  used = 9537.12M  free = 702.88M  (encrypted)"}, {"t": 57.66, "threads": 103, "fds": 109, "swap": "vm.swapusage: total = 10240.00M  used = 9537.12M  free = 702.88M  (encrypted)"}, {"t": 63.811, "threads": 103, "fds": 109, "swap": "vm.swapusage: total = 10240.00M  used = 9537.12M  free = 702.88M  (encrypted)"}]
- DB통계전후: [{"xact_commit": 23, "xact_rollback": 0, "blks_read": 5, "blks_hit": 27394, "tup_returned": 12453, "tup_fetched": 8867, "tup_inserted": 1159, "temp_bytes": 0, "deadlocks": 0}, {"xact_commit": 4317, "xact_rollback": 0, "blks_read": 5, "blks_hit": 99749, "tup_returned": 3906744, "tup_fetched": 16567, "tup_inserted": 1461, "temp_bytes": 0, "deadlocks": 0}]. 워밍업/검증포함,SQL횟수아님.
- Kafka lag/SSE는polling미사용으로해당없음. 0으로간주하지않음.

### 반복3 상세
- run20261019_163246_81cf, 시작2026-10-19T16:32:53.104195+09:00, 100명/5.0req/s/60.001초. 최종SQL{'1': 476, '2': 376, '3': 276, '4': 176}, RedisNone, 전관람자동일=True. 정확성True, 오류[], 중단사유[].
- 조회수3000, 누적목표관측수30000; 마지막응답→전원최종1400.3713749999918ms, 종료후전원최종1221.6766250000005ms. 스케줄지연{'count': 300, 'p50': 3.423333000000639, 'p95': 5.289040999997496, 'p99': 7.925708000000142, 'max': 61.112749999999494}.
- 자원시각[9.473291417, 15.656094625, 22.751168042, 28.905815041999997, 35.045557875, 42.192004209000004, 49.32214375, 55.476529084, 62.630272625], lag시각[]; 수집오류0. 자원간격[6.183, 7.095, 6.155, 6.14, 7.146, 7.13, 6.154, 7.154].
- HTTP본문638700+SSE JSON0bytes. 헤더/프레임/TCP/브라우저렌더링제외.
- 공유PG CPU0.85~15.53%,메모리 271.3MiB, 271.9MiB, 272.7MiB, 272.6MiB, 272.5MiB, 272.8MiB, 272.4MiB, 272.7MiB, 273.4MiB.
- Kafka CPU1.93~4.20%,메모리 313.6MiB, 314.6MiB, 331MiB, 330MiB, 330.6MiB, 334.9MiB, 331.3MiB, 332.4MiB, 342MiB.
- Redis CPU0.19~0.37%,메모리 12.38MiB, 12.33MiB, 12.52MiB, 12.46MiB, 12.44MiB, 12.43MiB, 12.42MiB, 12.4MiB, 12.38MiB.
- 앱 psCPU3.0~44.3%,RSS240976~326464KiB.
- 발생기 psCPU0.1~1.0%,RSS18208~21264KiB.
- 발생기 스레드/FD/swap: [{"t": 9.473, "threads": 103, "fds": 109, "swap": "vm.swapusage: total = 10240.00M  used = 9417.12M  free = 822.88M  (encrypted)"}, {"t": 15.656, "threads": 103, "fds": 109, "swap": "vm.swapusage: total = 10240.00M  used = 9401.12M  free = 838.88M  (encrypted)"}, {"t": 22.751, "threads": 104, "fds": 110, "swap": "vm.swapusage: total = 10240.00M  used = 9393.12M  free = 846.88M  (encrypted)"}, {"t": 28.906, "threads": 104, "fds": 110, "swap": "vm.swapusage: total = 10240.00M  used = 9385.12M  free = 854.88M  (encrypted)"}, {"t": 35.045, "threads": 104, "fds": 110, "swap": "vm.swapusage: total = 10240.00M  used = 9377.12M  free = 862.88M  (encrypted)"}, {"t": 42.192, "threads": 104, "fds": 110, "swap": "vm.swapusage: total = 10240.00M  used = 9377.12M  free = 862.88M  (encrypted)"}, {"t": 49.322, "threads": 104, "fds": 110, "swap": "vm.swapusage: total = 10240.00M  used = 9377.12M  free = 862.88M  (encrypted)"}, {"t": 55.476, "threads": 104, "fds": 110, "swap": "vm.swapusage: total = 10240.00M  used = 9377.12M  free = 862.88M  (encrypted)"}, {"t": 62.63, "threads": 104, "fds": 110, "swap": "vm.swapusage: total = 10240.00M  used = 9369.12M  free = 870.88M  (encrypted)"}]
- DB통계전후: [{"xact_commit": 23, "xact_rollback": 0, "blks_read": 5, "blks_hit": 27191, "tup_returned": 12369, "tup_fetched": 8783, "tup_inserted": 1159, "temp_bytes": 0, "deadlocks": 0}, {"xact_commit": 4321, "xact_rollback": 0, "blks_read": 5, "blks_hit": 100071, "tup_returned": 3910643, "tup_fetched": 16464, "tup_inserted": 1463, "temp_bytes": 0, "deadlocks": 0}]. 워밍업/검증포함,SQL횟수아님.
- Kafka lag/SSE는polling미사용으로해당없음. 0으로간주하지않음.

투표 반복p95범위19.42~108.02ms/표본표준편차50.88ms. 합산p95아님.

누적목표관측 반복p95범위1908.49~1957.71ms/표본표준편차24.62ms. 합산p95아님.

원본: load-tests/results/v100-r5/polling-batch.json 및 v100-r5-repeat-{1,2,3}/polling-pilot/{summary.json,events.jsonl,http.json,votes.json}, 앱/runner로그 및 container-state.txt. 로컬공유자원/시간대편향/추가인프라비용을포함하며관측차이를단일기술효과로단정하지않음. 같은투표를여러관람자가관측하므로지연표본은독립적이지않음.

## 2026-10-19 16:46 KST — v100-r5 realtime 반복 결과

동일 시드1000표+워밍업4표 후 측정. 관람자 수·투표 도착률은 조건명과 summary에 명시. 각 반복은 독립DB/전용KafkaRedis, 기존 실제서비스와 공유PostgreSQL은 유지. 결과표의 p95 편차는 반복별 차이이며 합산요청 p95가 아니다.

|반복|성공/시도|실제 RPS|투표p50/p95/p99 ms|조회p95 ms|누적목표p50/p95/p99 ms|자원/lag샘플|최종lag/재접속|valid/경고|
|---|---|---|---|---|---|---|---|---|
|1|300/300|4.99997|13.33/18.46/24.71|None|33.49/110.96/2460.35|9/11|0/100|True/[]|
|2|300/300|4.99968|12.25/18.43/22.79|None|32.66/84.64/2475.32|9/11|0/100|True/[]|
|3|300/300|4.99958|12.72/20.47/27.54|None|33.18/142.58/2450.59|9/11|0/100|True/[]|

### 반복1 상세
- run20261019_162946_29b8, 시작2026-10-19T16:42:21.013357+09:00, 100명/5.0req/s/60.000초. 최종SQL{'1': 476, '2': 376, '3': 276, '4': 176}, Redis{'4': 176, '3': 276, '2': 376, '1': 476}, 전관람자동일=True. 정확성True, 오류[], 중단사유[].
- 조회수0, 누적목표관측수30000; 마지막응답→전원최종21.591374999999857ms, 종료후전원최종0ms. 스케줄지연{'count': 300, 'p50': 3.9003749999988457, 'p95': 5.359083000001874, 'p99': 5.90870799999621, 'max': 9.464750000000244}.
- 자원시각[9.432600208, 15.623538333, 22.731458458, 29.854073790999998, 36.982959375, 43.113538916, 50.253341958, 57.400354083, 64.525017166], lag시각[7.733421666, 13.388216125, 19.010295957999997, 24.724863041, 30.367360958, 36.029458125000005, 41.679375583, 47.337615958, 52.984291, 58.812173375, 64.447072958]; 수집오류0. 자원간격[6.191, 7.108, 7.123, 7.129, 6.131, 7.14, 7.147, 7.125].
- HTTP본문35997+SSE JSON3918200bytes. 헤더/프레임/TCP/브라우저렌더링제외.
- 공유PG CPU0.46~8.90%,메모리 270.3MiB, 270.5MiB, 271MiB, 270.8MiB, 270.7MiB, 271.2MiB, 271.3MiB, 271.2MiB, 271.3MiB.
- Kafka CPU7.21~14.55%,메모리 326.6MiB, 359.6MiB, 377.5MiB, 380.2MiB, 382.1MiB, 379.2MiB, 381.9MiB, 388.1MiB, 381.5MiB.
- Redis CPU0.44~1.14%,메모리 12.6MiB, 12.54MiB, 12.69MiB, 12.88MiB, 12.82MiB, 12.77MiB, 13.14MiB, 13.03MiB, 12.64MiB.
- 앱 psCPU10.6~26.9%,RSS267104~413440KiB.
- 발생기 psCPU0.3~2.7%,RSS22608~47712KiB.
- 발생기 스레드/FD/swap: [{"t": 9.433, "threads": 104, "fds": 209, "swap": "vm.swapusage: total = 10240.00M  used = 9770.62M  free = 469.38M  (encrypted)"}, {"t": 15.623, "threads": 104, "fds": 209, "swap": "vm.swapusage: total = 10240.00M  used = 9762.62M  free = 477.38M  (encrypted)"}, {"t": 22.731, "threads": 104, "fds": 209, "swap": "vm.swapusage: total = 10240.00M  used = 9754.62M  free = 485.38M  (encrypted)"}, {"t": 29.854, "threads": 104, "fds": 211, "swap": "vm.swapusage: total = 10240.00M  used = 9754.62M  free = 485.38M  (encrypted)"}, {"t": 36.983, "threads": 104, "fds": 209, "swap": "vm.swapusage: total = 10240.00M  used = 9746.62M  free = 493.38M  (encrypted)"}, {"t": 43.113, "threads": 104, "fds": 209, "swap": "vm.swapusage: total = 10240.00M  used = 9738.62M  free = 501.38M  (encrypted)"}, {"t": 50.253, "threads": 104, "fds": 209, "swap": "vm.swapusage: total = 10240.00M  used = 9738.62M  free = 501.38M  (encrypted)"}, {"t": 57.4, "threads": 104, "fds": 209, "swap": "vm.swapusage: total = 10240.00M  used = 9730.62M  free = 509.38M  (encrypted)"}, {"t": 64.525, "threads": 104, "fds": 209, "swap": "vm.swapusage: total = 10240.00M  used = 9642.62M  free = 597.38M  (encrypted)"}]
- DB통계전후: [{"xact_commit": 47, "xact_rollback": 0, "blks_read": 5, "blks_hit": 27641, "tup_returned": 23021, "tup_fetched": 8779, "tup_inserted": 1159, "temp_bytes": 0, "deadlocks": 0}, {"xact_commit": 524, "xact_rollback": 0, "blks_read": 5, "blks_hit": 41445, "tup_returned": 40796, "tup_fetched": 14791, "tup_inserted": 1460, "temp_bytes": 0, "deadlocks": 0}]. 워밍업/검증포함,SQL횟수아님.
- lag시계열: [{"t": 2.103, "raw": "vote-log-group vote-created 0 4 4 0"}, {"t": 7.733, "raw": "vote-log-group vote-created 0 11 11 0"}, {"t": 13.388, "raw": "vote-log-group vote-created 0 39 39 0"}, {"t": 19.01, "raw": "vote-log-group vote-created 0 67 67 0"}, {"t": 24.725, "raw": "vote-log-group vote-created 0 96 96 0"}, {"t": 30.367, "raw": "vote-log-group vote-created 0 124 124 0"}, {"t": 36.029, "raw": "vote-log-group vote-created 0 152 152 0"}, {"t": 41.679, "raw": "vote-log-group vote-created 0 180 181 1"}, {"t": 47.338, "raw": "vote-log-group vote-created 0 209 209 0"}, {"t": 52.984, "raw": "vote-log-group vote-created 0 237 237 0"}, {"t": 58.812, "raw": "vote-log-group vote-created 0 266 266 0"}, {"t": 64.447, "raw": "vote-log-group vote-created 0 294 294 0"}, {"t": 67.548, "raw": "vote-log-group vote-created 0 304 304 0"}, {"t": 68.149, "raw": "vote-log-group vote-created 0 304 304 0"}, {"t": 70.35, "raw": "vote-log-group vote-created 0 304 304 0"}]
- 종료후첫lag0조회1.1202873329999932초. 정확한해소시각이아니며샘플간lag최댓값은알수없음.
- SSE재연결 100명, 공백3.008~3.092초. 재연결중관측지연도통계에포함. 실제브라우저EventSource검증과구분.

### 반복2 상세
- run20261019_163126_db6c, 시작2026-10-19T16:43:42.093420+09:00, 100명/5.0req/s/60.004초. 최종SQL{'1': 476, '2': 376, '3': 276, '4': 176}, Redis{'4': 176, '3': 276, '2': 376, '1': 476}, 전관람자동일=True. 정확성True, 오류[], 중단사유[].
- 조회수0, 누적목표관측수30000; 마지막응답→전원최종22.643666000007556ms, 종료후전원최종0ms. 스케줄지연{'count': 300, 'p50': 4.548499999998512, 'p95': 5.348499999996648, 'p99': 5.764333000001898, 'max': 8.736708000000704}.
- 자원시각[8.442177, 15.565346083, 21.706614542, 28.860137333, 34.990469167, 42.119559125, 49.250859625, 56.368631708, 63.521348333], lag시각[7.700277166999999, 13.328747208, 18.937129332999998, 24.603206417, 30.249186292, 35.881149583, 41.5293035, 47.185729916999996, 52.851304083, 58.488080042, 64.10204908300001]; 수집오류0. 자원간격[7.123, 6.141, 7.154, 6.13, 7.129, 7.131, 7.118, 7.153].
- HTTP본문36000+SSE JSON3915186bytes. 헤더/프레임/TCP/브라우저렌더링제외.
- 공유PG CPU0.50~5.98%,메모리 271MiB, 270.7MiB, 270.9MiB, 271MiB, 271.5MiB, 271.3MiB, 271.4MiB, 271.1MiB, 271.4MiB.
- Kafka CPU7.19~17.11%,메모리 323.8MiB, 355MiB, 376.8MiB, 375.6MiB, 377.3MiB, 379.8MiB, 380.1MiB, 379.4MiB, 382.5MiB.
- Redis CPU0.46~0.70%,메모리 12.46MiB, 12.91MiB, 12.58MiB, 12.52MiB, 12.68MiB, 12.57MiB, 12.5MiB, 12.43MiB, 12.61MiB.
- 앱 psCPU7.0~26.9%,RSS280992~460944KiB.
- 발생기 psCPU0.3~0.6%,RSS21824~46032KiB.
- 발생기 스레드/FD/swap: [{"t": 8.442, "threads": 104, "fds": 209, "swap": "vm.swapusage: total = 10240.00M  used = 9578.62M  free = 661.38M  (encrypted)"}, {"t": 15.565, "threads": 104, "fds": 209, "swap": "vm.swapusage: total = 10240.00M  used = 9578.62M  free = 661.38M  (encrypted)"}, {"t": 21.707, "threads": 104, "fds": 209, "swap": "vm.swapusage: total = 10240.00M  used = 9578.62M  free = 661.38M  (encrypted)"}, {"t": 28.86, "threads": 104, "fds": 209, "swap": "vm.swapusage: total = 10240.00M  used = 9570.62M  free = 669.38M  (encrypted)"}, {"t": 34.99, "threads": 104, "fds": 209, "swap": "vm.swapusage: total = 10240.00M  used = 9570.62M  free = 669.38M  (encrypted)"}, {"t": 42.12, "threads": 104, "fds": 209, "swap": "vm.swapusage: total = 10240.00M  used = 9570.62M  free = 669.38M  (encrypted)"}, {"t": 49.251, "threads": 104, "fds": 209, "swap": "vm.swapusage: total = 10240.00M  used = 9570.62M  free = 669.38M  (encrypted)"}, {"t": 56.369, "threads": 104, "fds": 209, "swap": "vm.swapusage: total = 10240.00M  used = 9562.62M  free = 677.38M  (encrypted)"}, {"t": 63.521, "threads": 104, "fds": 149, "swap": "vm.swapusage: total = 10240.00M  used = 9562.62M  free = 677.38M  (encrypted)"}]
- DB통계전후: [{"xact_commit": 47, "xact_rollback": 0, "blks_read": 5, "blks_hit": 27641, "tup_returned": 23021, "tup_fetched": 8779, "tup_inserted": 1159, "temp_bytes": 0, "deadlocks": 0}, {"xact_commit": 526, "xact_rollback": 0, "blks_read": 5, "blks_hit": 41247, "tup_returned": 40680, "tup_fetched": 14766, "tup_inserted": 1459, "temp_bytes": 0, "deadlocks": 0}]. 워밍업/검증포함,SQL횟수아님.
- lag시계열: [{"t": 2.04, "raw": "vote-log-group vote-created 0 4 4 0"}, {"t": 7.7, "raw": "vote-log-group vote-created 0 11 11 0"}, {"t": 13.329, "raw": "vote-log-group vote-created 0 39 39 0"}, {"t": 18.937, "raw": "vote-log-group vote-created 0 67 67 0"}, {"t": 24.603, "raw": "vote-log-group vote-created 0 95 95 0"}, {"t": 30.249, "raw": "vote-log-group vote-created 0 123 123 0"}, {"t": 35.881, "raw": "vote-log-group vote-created 0 152 152 0"}, {"t": 41.529, "raw": "vote-log-group vote-created 0 180 180 0"}, {"t": 47.186, "raw": "vote-log-group vote-created 0 208 208 0"}, {"t": 52.851, "raw": "vote-log-group vote-created 0 236 236 0"}, {"t": 58.488, "raw": "vote-log-group vote-created 0 265 265 0"}, {"t": 64.102, "raw": "vote-log-group vote-created 0 293 293 0"}, {"t": 67.548, "raw": "vote-log-group vote-created 0 304 304 0"}, {"t": 68.137, "raw": "vote-log-group vote-created 0 304 304 0"}, {"t": 69.819, "raw": "vote-log-group vote-created 0 304 304 0"}]
- 종료후첫lag0조회1.1227547499999986초. 정확한해소시각이아니며샘플간lag최댓값은알수없음.
- SSE재연결 100명, 공백3.009~3.058초. 재연결중관측지연도통계에포함. 실제브라우저EventSource검증과구분.

### 반복3 상세
- run20261019_163246_81cf, 시작2026-10-19T16:45:03.887277+09:00, 100명/5.0req/s/60.005초. 최종SQL{'1': 476, '2': 376, '3': 276, '4': 176}, Redis{'4': 176, '3': 276, '2': 376, '1': 476}, 전관람자동일=True. 정확성True, 오류[], 중단사유[].
- 조회수0, 누적목표관측수30000; 마지막응답→전원최종21.299333000001752ms, 종료후전원최종0ms. 스케줄지연{'count': 300, 'p50': 3.4206249999986227, 'p95': 5.41758399999992, 'p99': 9.06308400000011, 'max': 22.821167000003584}.
- 자원시각[9.458476291999999, 16.576741875, 23.7154475, 30.856462333, 36.971456875, 43.111327958000004, 50.297075583, 57.461226083, 64.58639762499999], lag시각[7.721637583, 13.374115999999999, 19.022335625, 24.67343275, 30.303942375, 35.929780083000004, 41.574244083, 47.214739125, 52.910168042, 59.003850542, 64.684534292]; 수집오류0. 자원간격[7.118, 7.139, 7.141, 6.115, 6.14, 7.186, 7.164, 7.125].
- HTTP본문36000+SSE JSON3910254bytes. 헤더/프레임/TCP/브라우저렌더링제외.
- 공유PG CPU0.54~9.76%,메모리 271MiB, 270.7MiB, 271.1MiB, 271.4MiB, 271.6MiB, 271.2MiB, 271.3MiB, 271.4MiB, 271.2MiB.
- Kafka CPU4.71~18.23%,메모리 322.8MiB, 357.3MiB, 376.9MiB, 376.8MiB, 380.6MiB, 379.6MiB, 380.6MiB, 382.3MiB, 381MiB.
- Redis CPU0.48~0.65%,메모리 12.55MiB, 12.74MiB, 12.65MiB, 12.56MiB, 12.48MiB, 12.43MiB, 12.82MiB, 12.5MiB, 12.88MiB.
- 앱 psCPU6.7~54.7%,RSS216928~430912KiB.
- 발생기 psCPU0.3~0.7%,RSS22832~41600KiB.
- 발생기 스레드/FD/swap: [{"t": 9.458, "threads": 104, "fds": 209, "swap": "vm.swapusage: total = 10240.00M  used = 9474.62M  free = 765.38M  (encrypted)"}, {"t": 16.577, "threads": 104, "fds": 209, "swap": "vm.swapusage: total = 10240.00M  used = 9474.62M  free = 765.38M  (encrypted)"}, {"t": 23.715, "threads": 104, "fds": 209, "swap": "vm.swapusage: total = 10240.00M  used = 9466.62M  free = 773.38M  (encrypted)"}, {"t": 30.856, "threads": 104, "fds": 209, "swap": "vm.swapusage: total = 10240.00M  used = 9466.62M  free = 773.38M  (encrypted)"}, {"t": 36.971, "threads": 104, "fds": 209, "swap": "vm.swapusage: total = 10240.00M  used = 9466.62M  free = 773.38M  (encrypted)"}, {"t": 43.111, "threads": 104, "fds": 209, "swap": "vm.swapusage: total = 10240.00M  used = 9466.62M  free = 773.38M  (encrypted)"}, {"t": 50.297, "threads": 104, "fds": 209, "swap": "vm.swapusage: total = 10240.00M  used = 9683.88M  free = 556.12M  (encrypted)"}, {"t": 57.461, "threads": 104, "fds": 209, "swap": "vm.swapusage: total = 10240.00M  used = 9635.88M  free = 604.12M  (encrypted)"}, {"t": 64.586, "threads": 104, "fds": 211, "swap": "vm.swapusage: total = 10240.00M  used = 9627.88M  free = 612.12M  (encrypted)"}]
- DB통계전후: [{"xact_commit": 57, "xact_rollback": 0, "blks_read": 5, "blks_hit": 27827, "tup_returned": 23136, "tup_fetched": 8894, "tup_inserted": 1159, "temp_bytes": 0, "deadlocks": 0}, {"xact_commit": 527, "xact_rollback": 0, "blks_read": 5, "blks_hit": 41321, "tup_returned": 40708, "tup_fetched": 14805, "tup_inserted": 1461, "temp_bytes": 0, "deadlocks": 0}]. 워밍업/검증포함,SQL횟수아님.
- lag시계열: [{"t": 2.075, "raw": "vote-log-group vote-created 0 4 4 0"}, {"t": 7.722, "raw": "vote-log-group vote-created 0 11 11 0"}, {"t": 13.374, "raw": "vote-log-group vote-created 0 39 39 0"}, {"t": 19.022, "raw": "vote-log-group vote-created 0 67 67 0"}, {"t": 24.673, "raw": "vote-log-group vote-created 0 95 95 0"}, {"t": 30.304, "raw": "vote-log-group vote-created 0 124 124 0"}, {"t": 35.93, "raw": "vote-log-group vote-created 0 152 152 0"}, {"t": 41.574, "raw": "vote-log-group vote-created 0 180 180 0"}, {"t": 47.215, "raw": "vote-log-group vote-created 0 208 208 0"}, {"t": 52.91, "raw": "vote-log-group vote-created 0 236 237 1"}, {"t": 59.004, "raw": "vote-log-group vote-created 0 267 267 0"}, {"t": 64.685, "raw": "vote-log-group vote-created 0 295 295 0"}, {"t": 67.661, "raw": "vote-log-group vote-created 0 304 304 0"}, {"t": 68.292, "raw": "vote-log-group vote-created 0 304 304 0"}, {"t": 70.534, "raw": "vote-log-group vote-created 0 304 304 0"}]
- 종료후첫lag0조회1.207813250000001초. 정확한해소시각이아니며샘플간lag최댓값은알수없음.
- SSE재연결 100명, 공백3.007~3.081초. 재연결중관측지연도통계에포함. 실제브라우저EventSource검증과구분.

투표 반복p95범위18.43~20.47ms/표본표준편차1.17ms. 합산p95아님.

누적목표관측 반복p95범위84.64~142.58ms/표본표준편차29.01ms. 합산p95아님.

원본: load-tests/results/v100-r5/realtime-batch.json 및 v100-r5-repeat-{1,2,3}/realtime-pilot/{summary.json,events.jsonl,http.json,votes.json}, 앱/runner로그 및 container-state.txt. 로컬공유자원/시간대편향/추가인프라비용을포함하며관측차이를단일기술효과로단정하지않음. 같은투표를여러관람자가관측하므로지연표본은독립적이지않음.

## 2026-10-19 16:50 KST — v100-r10 polling 반복 결과

동일 시드1000표+워밍업4표 후 측정. 관람자 수·투표 도착률은 조건명과 summary에 명시. 각 반복은 독립DB/전용KafkaRedis, 기존 실제서비스와 공유PostgreSQL은 유지. 결과표의 p95 편차는 반복별 차이이며 합산요청 p95가 아니다.

|반복|성공/시도|실제 RPS|투표p50/p95/p99 ms|조회p95 ms|누적목표p50/p95/p99 ms|자원/lag샘플|최종lag/재접속|valid/경고|
|---|---|---|---|---|---|---|---|---|
|1|600/600|9.99961|11.69/17.88/24.15|18.624624999999284|1015.34/1920.97/2003.64|9/0|None/0|True/[]|
|2|600/600|9.99934|11.64/18.03/25.55|18.37625000000287|1020.92/1931.48/2009.13|9/0|None/0|True/[]|
|3|600/600|9.99916|11.11/20.31/25.85|22.93345899999899|1021.34/1930.72/2010.77|9/0|None/0|True/[]|

### 반복1 상세
- run20261019_164632_cb2b, 시작2026-10-19T16:46:38.792098+09:00, 100명/10.0req/s/60.002초. 최종SQL{'1': 551, '2': 451, '3': 351, '4': 251}, RedisNone, 전관람자동일=True. 정확성True, 오류[], 중단사유[].
- 조회수3000, 누적목표관측수60000; 마지막응답→전원최종1253.4739590000045ms, 종료후전원최종1164.6593749999993ms. 스케줄지연{'count': 600, 'p50': 4.350917000003562, 'p95': 5.375999999998271, 'p99': 6.6690420000057316, 'max': 29.098292000001138}.
- 자원시각[9.453819416, 16.550995791000002, 23.697812416, 30.800584125, 36.934453208, 44.063440791, 51.209526041000004, 58.348909125, 65.49361716599999], lag시각[]; 수집오류0. 자원간격[7.097, 7.147, 7.103, 6.134, 7.129, 7.146, 7.139, 7.145].
- HTTP본문674397+SSE JSON0bytes. 헤더/프레임/TCP/브라우저렌더링제외.
- 공유PG CPU1.25~9.51%,메모리 273.4MiB, 274.2MiB, 274.3MiB, 273.9MiB, 274.2MiB, 274.5MiB, 274.5MiB, 274.7MiB, 274.9MiB.
- Kafka CPU2.11~6.60%,메모리 311.5MiB, 313.3MiB, 328.6MiB, 332.6MiB, 329.6MiB, 329.5MiB, 329.8MiB, 330.6MiB, 330.2MiB.
- Redis CPU0.25~0.48%,메모리 12.5MiB, 12.72MiB, 12.71MiB, 12.68MiB, 12.64MiB, 12.88MiB, 12.84MiB, 12.79MiB, 12.75MiB.
- 앱 psCPU6.6~56.6%,RSS361024~425568KiB.
- 발생기 psCPU0.5~4.7%,RSS19616~22416KiB.
- 발생기 스레드/FD/swap: [{"t": 9.454, "threads": 103, "fds": 109, "swap": "vm.swapusage: total = 10240.00M  used = 9579.88M  free = 660.12M  (encrypted)"}, {"t": 16.551, "threads": 103, "fds": 109, "swap": "vm.swapusage: total = 10240.00M  used = 9579.88M  free = 660.12M  (encrypted)"}, {"t": 23.698, "threads": 103, "fds": 109, "swap": "vm.swapusage: total = 10240.00M  used = 9555.88M  free = 684.12M  (encrypted)"}, {"t": 30.801, "threads": 103, "fds": 109, "swap": "vm.swapusage: total = 10240.00M  used = 9555.88M  free = 684.12M  (encrypted)"}, {"t": 36.934, "threads": 103, "fds": 109, "swap": "vm.swapusage: total = 10240.00M  used = 9547.88M  free = 692.12M  (encrypted)"}, {"t": 44.063, "threads": 103, "fds": 109, "swap": "vm.swapusage: total = 10240.00M  used = 9547.88M  free = 692.12M  (encrypted)"}, {"t": 51.209, "threads": 103, "fds": 109, "swap": "vm.swapusage: total = 10240.00M  used = 9539.88M  free = 700.12M  (encrypted)"}, {"t": 58.349, "threads": 103, "fds": 109, "swap": "vm.swapusage: total = 10240.00M  used = 9539.88M  free = 700.12M  (encrypted)"}, {"t": 65.494, "threads": 103, "fds": 109, "swap": "vm.swapusage: total = 10240.00M  used = 9539.88M  free = 700.12M  (encrypted)"}]
- DB통계전후: [{"xact_commit": 23, "xact_rollback": 0, "blks_read": 5, "blks_hit": 26978, "tup_returned": 12277, "tup_fetched": 8691, "tup_inserted": 1159, "temp_bytes": 0, "deadlocks": 0}, {"xact_commit": 4680, "xact_rollback": 0, "blks_read": 5, "blks_hit": 107221, "tup_returned": 4364982, "tup_fetched": 16215, "tup_inserted": 1763, "temp_bytes": 0, "deadlocks": 0}]. 워밍업/검증포함,SQL횟수아님.
- Kafka lag/SSE는polling미사용으로해당없음. 0으로간주하지않음.

### 반복2 상세
- run20261019_164749_18ed, 시작2026-10-19T16:47:55.072575+09:00, 100명/10.0req/s/60.004초. 최종SQL{'1': 551, '2': 451, '3': 351, '4': 251}, RedisNone, 전관람자동일=True. 정확성True, 오류[], 중단사유[].
- 조회수3000, 누적목표관측수60000; 마지막응답→전원최종1337.4872919999916ms, 종료후전원최종1251.0097920000048ms. 스케줄지연{'count': 600, 'p50': 4.374999999999574, 'p95': 5.367208000006229, 'p99': 9.059541000000948, 'max': 17.521625000004093}.
- 자원시각[8.424155792, 15.537676667, 22.628427583, 29.782835083000002, 36.908979042, 44.059938042, 50.195587708, 56.317308000000004, 63.466648792], lag시각[]; 수집오류0. 자원간격[7.114, 7.091, 7.154, 7.126, 7.151, 6.136, 6.122, 7.149].
- HTTP본문674400+SSE JSON0bytes. 헤더/프레임/TCP/브라우저렌더링제외.
- 공유PG CPU1.27~8.83%,메모리 274.6MiB, 275.3MiB, 275.9MiB, 275.5MiB, 275.6MiB, 276.1MiB, 275.9MiB, 276.2MiB, 275.8MiB.
- Kafka CPU2.57~16.32%,메모리 312MiB, 313MiB, 329.3MiB, 332.4MiB, 329.6MiB, 333.1MiB, 330.1MiB, 330.7MiB, 341.9MiB.
- Redis CPU0.20~0.58%,메모리 12.48MiB, 12.69MiB, 12.64MiB, 12.59MiB, 12.56MiB, 12.55MiB, 12.53MiB, 12.52MiB, 12.51MiB.
- 앱 psCPU4.6~37.7%,RSS256512~418032KiB.
- 발생기 psCPU0.6~3.0%,RSS19376~22304KiB.
- 발생기 스레드/FD/swap: [{"t": 8.424, "threads": 103, "fds": 109, "swap": "vm.swapusage: total = 10240.00M  used = 9499.88M  free = 740.12M  (encrypted)"}, {"t": 15.538, "threads": 103, "fds": 109, "swap": "vm.swapusage: total = 10240.00M  used = 9499.88M  free = 740.12M  (encrypted)"}, {"t": 22.628, "threads": 103, "fds": 109, "swap": "vm.swapusage: total = 10240.00M  used = 9491.88M  free = 748.12M  (encrypted)"}, {"t": 29.783, "threads": 103, "fds": 109, "swap": "vm.swapusage: total = 10240.00M  used = 9491.88M  free = 748.12M  (encrypted)"}, {"t": 36.909, "threads": 103, "fds": 109, "swap": "vm.swapusage: total = 10240.00M  used = 9491.88M  free = 748.12M  (encrypted)"}, {"t": 44.06, "threads": 103, "fds": 109, "swap": "vm.swapusage: total = 10240.00M  used = 9491.88M  free = 748.12M  (encrypted)"}, {"t": 50.196, "threads": 103, "fds": 109, "swap": "vm.swapusage: total = 10240.00M  used = 9491.88M  free = 748.12M  (encrypted)"}, {"t": 56.317, "threads": 103, "fds": 109, "swap": "vm.swapusage: total = 10240.00M  used = 9491.88M  free = 748.12M  (encrypted)"}, {"t": 63.467, "threads": 103, "fds": 109, "swap": "vm.swapusage: total = 10240.00M  used = 9491.88M  free = 748.12M  (encrypted)"}]
- DB통계전후: [{"xact_commit": 23, "xact_rollback": 0, "blks_read": 5, "blks_hit": 27186, "tup_returned": 12365, "tup_fetched": 8779, "tup_inserted": 1159, "temp_bytes": 0, "deadlocks": 0}, {"xact_commit": 4674, "xact_rollback": 0, "blks_read": 5, "blks_hit": 108182, "tup_returned": 4362722, "tup_fetched": 16236, "tup_inserted": 1763, "temp_bytes": 0, "deadlocks": 0}]. 워밍업/검증포함,SQL횟수아님.
- Kafka lag/SSE는polling미사용으로해당없음. 0으로간주하지않음.

### 반복3 상세
- run20261019_164906_aa41, 시작2026-10-19T16:49:12.806990+09:00, 100명/10.0req/s/60.005초. 최종SQL{'1': 551, '2': 451, '3': 351, '4': 251}, RedisNone, 전관람자동일=True. 정확성True, 오류[], 중단사유[].
- 조회수3000, 누적목표관측수60000; 마지막응답→전원최종1435.5789589999972ms, 종료후전원최종1343.3702500000068ms. 스케줄지연{'count': 600, 'p50': 3.5061669999976175, 'p95': 5.33500000000231, 'p99': 7.22033399999944, 'max': 71.2672920000017}.
- 자원시각[9.445152334, 16.581182042000002, 23.713785334, 30.795690375000003, 37.937984166999996, 44.063247084, 51.216454792, 58.370981584, 65.511717459], lag시각[]; 수집오류0. 자원간격[7.136, 7.133, 7.082, 7.142, 6.125, 7.153, 7.155, 7.141].
- HTTP본문674397+SSE JSON0bytes. 헤더/프레임/TCP/브라우저렌더링제외.
- 공유PG CPU1.17~9.88%,메모리 277MiB, 277.8MiB, 278.6MiB, 278.5MiB, 278.5MiB, 278.4MiB, 278.9MiB, 279.4MiB, 278.7MiB.
- Kafka CPU2.09~5.36%,메모리 312.6MiB, 313.7MiB, 329.9MiB, 340.3MiB, 331.2MiB, 330.6MiB, 330.7MiB, 331.7MiB, 331MiB.
- Redis CPU0.20~0.42%,메모리 12.13MiB, 12.36MiB, 12.34MiB, 12.33MiB, 12.3MiB, 12.29MiB, 12.26MiB, 12.21MiB, 12.4MiB.
- 앱 psCPU6.9~79.3%,RSS247712~407520KiB.
- 발생기 psCPU0.6~4.2%,RSS18656~22096KiB.
- 발생기 스레드/FD/swap: [{"t": 9.445, "threads": 103, "fds": 109, "swap": "vm.swapusage: total = 10240.00M  used = 9443.88M  free = 796.12M  (encrypted)"}, {"t": 16.581, "threads": 103, "fds": 109, "swap": "vm.swapusage: total = 10240.00M  used = 9411.88M  free = 828.12M  (encrypted)"}, {"t": 23.714, "threads": 103, "fds": 109, "swap": "vm.swapusage: total = 10240.00M  used = 9363.88M  free = 876.12M  (encrypted)"}, {"t": 30.796, "threads": 104, "fds": 110, "swap": "vm.swapusage: total = 10240.00M  used = 9195.81M  free = 1044.19M  (encrypted)"}, {"t": 37.938, "threads": 104, "fds": 110, "swap": "vm.swapusage: total = 10240.00M  used = 9179.81M  free = 1060.19M  (encrypted)"}, {"t": 44.063, "threads": 104, "fds": 110, "swap": "vm.swapusage: total = 10240.00M  used = 9171.81M  free = 1068.19M  (encrypted)"}, {"t": 51.216, "threads": 104, "fds": 110, "swap": "vm.swapusage: total = 10240.00M  used = 9171.81M  free = 1068.19M  (encrypted)"}, {"t": 58.371, "threads": 104, "fds": 110, "swap": "vm.swapusage: total = 10240.00M  used = 9171.81M  free = 1068.19M  (encrypted)"}, {"t": 65.512, "threads": 104, "fds": 110, "swap": "vm.swapusage: total = 10240.00M  used = 9163.81M  free = 1076.19M  (encrypted)"}]
- DB통계전후: [{"xact_commit": 25, "xact_rollback": 0, "blks_read": 5, "blks_hit": 27293, "tup_returned": 12531, "tup_fetched": 8945, "tup_inserted": 1159, "temp_bytes": 0, "deadlocks": 0}, {"xact_commit": 4638, "xact_rollback": 0, "blks_read": 5, "blks_hit": 108328, "tup_returned": 4366923, "tup_fetched": 16410, "tup_inserted": 1763, "temp_bytes": 0, "deadlocks": 0}]. 워밍업/검증포함,SQL횟수아님.
- Kafka lag/SSE는polling미사용으로해당없음. 0으로간주하지않음.

투표 반복p95범위17.88~20.31ms/표본표준편차1.36ms. 합산p95아님.

누적목표관측 반복p95범위1920.97~1931.48ms/표본표준편차5.86ms. 합산p95아님.

원본: load-tests/results/v100-r10/polling-batch.json 및 v100-r10-repeat-{1,2,3}/polling-pilot/{summary.json,events.jsonl,http.json,votes.json}, 앱/runner로그 및 container-state.txt. 로컬공유자원/시간대편향/추가인프라비용을포함하며관측차이를단일기술효과로단정하지않음. 같은투표를여러관람자가관측하므로지연표본은독립적이지않음.

## 2026-10-19 16:59 KST — v100-r10 realtime 반복 결과

동일 시드1000표+워밍업4표 후 측정. 관람자 수·투표 도착률은 조건명과 summary에 명시. 각 반복은 독립DB/전용KafkaRedis, 기존 실제서비스와 공유PostgreSQL은 유지. 결과표의 p95 편차는 반복별 차이이며 합산요청 p95가 아니다.

|반복|성공/시도|실제 RPS|투표p50/p95/p99 ms|조회p95 ms|누적목표p50/p95/p99 ms|자원/lag샘플|최종lag/재접속|valid/경고|
|---|---|---|---|---|---|---|---|---|
|1|600/600|9.99921|10.57/17.20/24.68|None|29.39/151.62/2458.35|9/11|0/100|True/['발생기 스케줄 지연 100ms 초과']|
|2|600/600|9.99910|10.39/17.55/25.24|None|29.02/210.86/2408.16|9/11|0/100|True/[]|
|3|600/600|9.99992|10.38/16.65/28.33|None|28.96/162.26/2426.11|9/11|0/100|True/[]|

### 반복1 상세
- run20261019_164632_cb2b, 시작2026-10-19T16:54:18.416384+09:00, 100명/10.0req/s/60.005초. 최종SQL{'1': 551, '2': 451, '3': 351, '4': 251}, Redis{'4': 251, '3': 351, '2': 451, '1': 551}, 전관람자동일=True. 정확성True, 오류[], 중단사유[].
- 조회수0, 누적목표관측수60000; 마지막응답→전원최종21.297083999996858ms, 종료후전원최종0ms. 스케줄지연{'count': 600, 'p50': 3.881416999995224, 'p95': 5.319541999998734, 'p99': 7.991832999998394, 'max': 126.41545799999676}.
- 자원시각[9.605590708, 15.719081458, 22.856143624999998, 29.993882291, 37.111098291, 44.203874916, 51.350725875, 58.481707916, 64.601169791], lag시각[8.277695875, 13.933781374999999, 19.560855790999998, 25.218820833, 30.870669333, 36.509869166, 42.177093375, 48.096106208, 54.100216916, 59.994106291, 65.638287583]; 수집오류0. 자원간격[6.113, 7.137, 7.138, 7.117, 7.093, 7.147, 7.131, 6.119].
- HTTP본문71997+SSE JSON7813658bytes. 헤더/프레임/TCP/브라우저렌더링제외.
- 공유PG CPU1.02~8.82%,메모리 275.9MiB, 276.4MiB, 276.3MiB, 276.5MiB, 276.6MiB, 276MiB, 276.7MiB, 276.4MiB, 276.3MiB.
- Kafka CPU7.38~16.45%,메모리 359MiB, 384MiB, 378.8MiB, 380.7MiB, 383.6MiB, 382.2MiB, 386.8MiB, 383.5MiB, 389.7MiB.
- Redis CPU0.55~8.10%,메모리 12.72MiB, 12.64MiB, 12.61MiB, 12.76MiB, 12.66MiB, 12.56MiB, 12.49MiB, 12.42MiB, 12.35MiB.
- 앱 psCPU10.9~38.6%,RSS278128~443712KiB.
- 발생기 psCPU0.7~8.4%,RSS24352~79712KiB.
- 발생기 스레드/FD/swap: [{"t": 9.606, "threads": 104, "fds": 209, "swap": "vm.swapusage: total = 11264.00M  used = 10163.62M  free = 1100.38M  (encrypted)"}, {"t": 15.719, "threads": 104, "fds": 209, "swap": "vm.swapusage: total = 11264.00M  used = 10147.62M  free = 1116.38M  (encrypted)"}, {"t": 22.856, "threads": 104, "fds": 209, "swap": "vm.swapusage: total = 11264.00M  used = 10147.62M  free = 1116.38M  (encrypted)"}, {"t": 29.994, "threads": 104, "fds": 209, "swap": "vm.swapusage: total = 11264.00M  used = 10139.56M  free = 1124.44M  (encrypted)"}, {"t": 37.111, "threads": 104, "fds": 209, "swap": "vm.swapusage: total = 11264.00M  used = 10099.56M  free = 1164.44M  (encrypted)"}, {"t": 44.204, "threads": 104, "fds": 209, "swap": "vm.swapusage: total = 11264.00M  used = 10011.56M  free = 1252.44M  (encrypted)"}, {"t": 51.351, "threads": 105, "fds": 210, "swap": "vm.swapusage: total = 11264.00M  used = 9987.56M  free = 1276.44M  (encrypted)"}, {"t": 58.482, "threads": 105, "fds": 210, "swap": "vm.swapusage: total = 11264.00M  used = 9947.56M  free = 1316.44M  (encrypted)"}, {"t": 64.601, "threads": 105, "fds": 210, "swap": "vm.swapusage: total = 11264.00M  used = 9939.56M  free = 1324.44M  (encrypted)"}]
- DB통계전후: [{"xact_commit": 37, "xact_rollback": 0, "blks_read": 5, "blks_hit": 27276, "tup_returned": 18506, "tup_fetched": 8704, "tup_inserted": 1159, "temp_bytes": 0, "deadlocks": 0}, {"xact_commit": 835, "xact_rollback": 0, "blks_read": 5, "blks_hit": 44281, "tup_returned": 37959, "tup_fetched": 14200, "tup_inserted": 1751, "temp_bytes": 0, "deadlocks": 0}]. 워밍업/검증포함,SQL횟수아님.
- lag시계열: [{"t": 2.317, "raw": "vote-log-group vote-created 0 4 4 0"}, {"t": 8.278, "raw": "vote-log-group vote-created 0 20 20 0"}, {"t": 13.934, "raw": "vote-log-group vote-created 0 76 77 1"}, {"t": 19.561, "raw": "vote-log-group vote-created 0 133 133 0"}, {"t": 25.219, "raw": "vote-log-group vote-created 0 189 189 0"}, {"t": 30.871, "raw": "vote-log-group vote-created 0 246 246 0"}, {"t": 36.51, "raw": "vote-log-group vote-created 0 302 302 0"}, {"t": 42.177, "raw": "vote-log-group vote-created 0 359 359 0"}, {"t": 48.096, "raw": "vote-log-group vote-created 0 418 418 0"}, {"t": 54.1, "raw": "vote-log-group vote-created 0 478 478 0"}, {"t": 59.994, "raw": "vote-log-group vote-created 0 537 537 0"}, {"t": 65.638, "raw": "vote-log-group vote-created 0 593 594 1"}, {"t": 68.135, "raw": "vote-log-group vote-created 0 604 604 0"}, {"t": 68.746, "raw": "vote-log-group vote-created 0 604 604 0"}, {"t": 71.357, "raw": "vote-log-group vote-created 0 604 604 0"}, {"t": 77.081, "raw": "vote-log-group vote-created 0 604 604 0"}, {"t": 82.829, "raw": "vote-log-group vote-created 0 604 604 0"}]
- 종료후첫lag0조회1.4397368340000014초. 정확한해소시각이아니며샘플간lag최댓값은알수없음.
- SSE재연결 100명, 공백3.012~3.079초. 재연결중관측지연도통계에포함. 실제브라우저EventSource검증과구분.

### 반복2 상세
- run20261019_164749_18ed, 시작2026-10-19T16:55:53.257299+09:00, 100명/10.0req/s/60.005초. 최종SQL{'1': 551, '2': 451, '3': 351, '4': 251}, Redis{'4': 251, '3': 351, '2': 451, '1': 551}, 전관람자동일=True. 정확성True, 오류[], 중단사유[].
- 조회수0, 누적목표관측수60000; 마지막응답→전원최종22.93466700000124ms, 종료후전원최종0ms. 스케줄지연{'count': 600, 'p50': 4.447541000004662, 'p95': 5.2874999999996675, 'p99': 6.740791000000357, 'max': 29.606457999996394}.
- 자원시각[9.380176, 16.513905625, 23.644207332999997, 30.793466042, 37.922823667, 44.109337333, 51.239183667, 58.371435417, 64.46795275], lag시각[7.688311917, 13.336596458, 19.046998833, 24.72406075, 30.438402292, 36.068204125, 41.740665167, 47.367494, 53.063469500000004, 58.749568375, 64.3902605]; 수집오류0. 자원간격[7.134, 7.13, 7.149, 7.129, 6.187, 7.13, 7.132, 6.097].
- HTTP본문71997+SSE JSON7822700bytes. 헤더/프레임/TCP/브라우저렌더링제외.
- 공유PG CPU1.09~7.03%,메모리 276.4MiB, 276MiB, 276.4MiB, 276.5MiB, 276.1MiB, 276MiB, 276.7MiB, 276.6MiB, 276.7MiB.
- Kafka CPU8.82~28.01%,메모리 357.1MiB, 383MiB, 380.3MiB, 379.9MiB, 380.9MiB, 390.9MiB, 385.6MiB, 384MiB, 384MiB.
- Redis CPU0.59~0.97%,메모리 12.8MiB, 12.72MiB, 13.11MiB, 12.8MiB, 12.96MiB, 12.88MiB, 12.81MiB, 12.71MiB, 12.62MiB.
- 앱 psCPU8.2~39.4%,RSS205856~402592KiB.
- 발생기 psCPU0.5~2.7%,RSS24256~82080KiB.
- 발생기 스레드/FD/swap: [{"t": 9.38, "threads": 104, "fds": 209, "swap": "vm.swapusage: total = 11264.00M  used = 9875.56M  free = 1388.44M  (encrypted)"}, {"t": 16.514, "threads": 104, "fds": 209, "swap": "vm.swapusage: total = 11264.00M  used = 9811.56M  free = 1452.44M  (encrypted)"}, {"t": 23.644, "threads": 104, "fds": 209, "swap": "vm.swapusage: total = 11264.00M  used = 9803.56M  free = 1460.44M  (encrypted)"}, {"t": 30.793, "threads": 104, "fds": 209, "swap": "vm.swapusage: total = 11264.00M  used = 9795.56M  free = 1468.44M  (encrypted)"}, {"t": 37.923, "threads": 104, "fds": 209, "swap": "vm.swapusage: total = 11264.00M  used = 9787.56M  free = 1476.44M  (encrypted)"}, {"t": 44.109, "threads": 105, "fds": 210, "swap": "vm.swapusage: total = 11264.00M  used = 9875.06M  free = 1388.94M  (encrypted)"}, {"t": 51.239, "threads": 105, "fds": 210, "swap": "vm.swapusage: total = 11264.00M  used = 9803.06M  free = 1460.94M  (encrypted)"}, {"t": 58.371, "threads": 105, "fds": 212, "swap": "vm.swapusage: total = 11264.00M  used = 9803.06M  free = 1460.94M  (encrypted)"}, {"t": 64.468, "threads": 105, "fds": 210, "swap": "vm.swapusage: total = 11264.00M  used = 9795.06M  free = 1468.94M  (encrypted)"}]
- DB통계전후: [{"xact_commit": 39, "xact_rollback": 0, "blks_read": 5, "blks_hit": 27349, "tup_returned": 19409, "tup_fetched": 8719, "tup_inserted": 1159, "temp_bytes": 0, "deadlocks": 0}, {"xact_commit": 836, "xact_rollback": 0, "blks_read": 5, "blks_hit": 45134, "tup_returned": 39325, "tup_fetched": 14648, "tup_inserted": 1755, "temp_bytes": 0, "deadlocks": 0}]. 워밍업/검증포함,SQL횟수아님.
- lag시계열: [{"t": 2.026, "raw": "vote-log-group vote-created 0 4 4 0"}, {"t": 7.688, "raw": "vote-log-group vote-created 0 17 17 0"}, {"t": 13.337, "raw": "vote-log-group vote-created 0 73 74 1"}, {"t": 19.047, "raw": "vote-log-group vote-created 0 131 131 0"}, {"t": 24.724, "raw": "vote-log-group vote-created 0 187 187 0"}, {"t": 30.438, "raw": "vote-log-group vote-created 0 245 245 0"}, {"t": 36.068, "raw": "vote-log-group vote-created 0 301 301 0"}, {"t": 41.741, "raw": "vote-log-group vote-created 0 358 358 0"}, {"t": 47.367, "raw": "vote-log-group vote-created 0 414 414 0"}, {"t": 53.063, "raw": "vote-log-group vote-created 0 471 471 0"}, {"t": 58.75, "raw": "vote-log-group vote-created 0 528 528 0"}, {"t": 64.39, "raw": "vote-log-group vote-created 0 584 584 0"}, {"t": 67.843, "raw": "vote-log-group vote-created 0 604 604 0"}, {"t": 68.448, "raw": "vote-log-group vote-created 0 604 604 0"}, {"t": 70.117, "raw": "vote-log-group vote-created 0 604 604 0"}, {"t": 75.813, "raw": "vote-log-group vote-created 0 604 604 0"}, {"t": 81.514, "raw": "vote-log-group vote-created 0 604 604 0"}]
- 종료후첫lag0조회1.4532118329999975초. 정확한해소시각이아니며샘플간lag최댓값은알수없음.
- SSE재연결 100명, 공백3.002~3.061초. 재연결중관측지연도통계에포함. 실제브라우저EventSource검증과구분.

### 반복3 상세
- run20261019_164906_aa41, 시작2026-10-19T16:57:27.812637+09:00, 100명/10.0req/s/60.000초. 최종SQL{'1': 551, '2': 451, '3': 351, '4': 251}, Redis{'4': 251, '3': 351, '2': 451, '1': 551}, 전관람자동일=True. 정확성True, 오류[], 중단사유[].
- 조회수0, 누적목표관측수60000; 마지막응답→전원최종20.123458000000483ms, 종료후전원최종0ms. 스케줄지연{'count': 600, 'p50': 4.163583000000415, 'p95': 5.298290999999011, 'p99': 6.2620830000028604, 'max': 59.358624999998}.
- 자원시각[9.444243541999999, 16.564779917000003, 23.663581167, 30.931502667, 38.07483475, 45.213833625, 52.341021167, 58.457672167, 65.570147917], lag시각[7.848812542, 13.476987667, 19.130619667, 24.803931375, 30.488372375, 36.174193792000004, 41.827500958, 47.475935292, 53.116279792, 58.760506583, 64.526999292]; 수집오류0. 자원간격[7.121, 7.099, 7.268, 7.143, 7.139, 7.127, 6.117, 7.112].
- HTTP본문71991+SSE JSON7818864bytes. 헤더/프레임/TCP/브라우저렌더링제외.
- 공유PG CPU0.85~7.59%,메모리 275.7MiB, 275.9MiB, 276.4MiB, 276.9MiB, 276.7MiB, 276.7MiB, 276.6MiB, 277.1MiB, 276.7MiB.
- Kafka CPU6.60~18.38%,메모리 356.3MiB, 376.9MiB, 379.7MiB, 379MiB, 379.4MiB, 391.3MiB, 384.1MiB, 383.6MiB, 383MiB.
- Redis CPU0.53~1.22%,메모리 12.4MiB, 12.55MiB, 12.47MiB, 12.38MiB, 12.56MiB, 12.45MiB, 12.63MiB, 12.31MiB, 12.5MiB.
- 앱 psCPU8.9~34.9%,RSS241312~393248KiB.
- 발생기 psCPU0.5~5.5%,RSS24176~80912KiB.
- 발생기 스레드/FD/swap: [{"t": 9.444, "threads": 104, "fds": 209, "swap": "vm.swapusage: total = 11264.00M  used = 9699.06M  free = 1564.94M  (encrypted)"}, {"t": 16.565, "threads": 104, "fds": 208, "swap": "vm.swapusage: total = 11264.00M  used = 9699.06M  free = 1564.94M  (encrypted)"}, {"t": 23.664, "threads": 104, "fds": 209, "swap": "vm.swapusage: total = 11264.00M  used = 9691.06M  free = 1572.94M  (encrypted)"}, {"t": 30.931, "threads": 106, "fds": 211, "swap": "vm.swapusage: total = 11264.00M  used = 9779.06M  free = 1484.94M  (encrypted)"}, {"t": 38.075, "threads": 106, "fds": 211, "swap": "vm.swapusage: total = 11264.00M  used = 9763.06M  free = 1500.94M  (encrypted)"}, {"t": 45.214, "threads": 106, "fds": 211, "swap": "vm.swapusage: total = 11264.00M  used = 9739.06M  free = 1524.94M  (encrypted)"}, {"t": 52.341, "threads": 106, "fds": 211, "swap": "vm.swapusage: total = 11264.00M  used = 9731.06M  free = 1532.94M  (encrypted)"}, {"t": 58.458, "threads": 106, "fds": 213, "swap": "vm.swapusage: total = 11264.00M  used = 9723.06M  free = 1540.94M  (encrypted)"}, {"t": 65.57, "threads": 106, "fds": 211, "swap": "vm.swapusage: total = 11264.00M  used = 9723.06M  free = 1540.94M  (encrypted)"}]
- DB통계전후: [{"xact_commit": 39, "xact_rollback": 0, "blks_read": 5, "blks_hit": 27542, "tup_returned": 19492, "tup_fetched": 8802, "tup_inserted": 1159, "temp_bytes": 0, "deadlocks": 0}, {"xact_commit": 829, "xact_rollback": 0, "blks_read": 5, "blks_hit": 45576, "tup_returned": 39519, "tup_fetched": 14882, "tup_inserted": 1750, "temp_bytes": 0, "deadlocks": 0}]. 워밍업/검증포함,SQL횟수아님.
- lag시계열: [{"t": 2.194, "raw": "vote-log-group vote-created 0 4 4 0"}, {"t": 7.849, "raw": "vote-log-group vote-created 0 17 17 0"}, {"t": 13.477, "raw": "vote-log-group vote-created 0 73 73 0"}, {"t": 19.131, "raw": "vote-log-group vote-created 0 130 130 0"}, {"t": 24.804, "raw": "vote-log-group vote-created 0 186 186 0"}, {"t": 30.488, "raw": "vote-log-group vote-created 0 243 243 0"}, {"t": 36.174, "raw": "vote-log-group vote-created 0 300 300 0"}, {"t": 41.828, "raw": "vote-log-group vote-created 0 356 357 1"}, {"t": 47.476, "raw": "vote-log-group vote-created 0 413 413 0"}, {"t": 53.116, "raw": "vote-log-group vote-created 0 469 470 1"}, {"t": 58.761, "raw": "vote-log-group vote-created 0 526 526 0"}, {"t": 64.527, "raw": "vote-log-group vote-created 0 583 584 1"}, {"t": 68.017, "raw": "vote-log-group vote-created 0 604 604 0"}, {"t": 68.623, "raw": "vote-log-group vote-created 0 604 604 0"}, {"t": 70.267, "raw": "vote-log-group vote-created 0 604 604 0"}, {"t": 75.958, "raw": "vote-log-group vote-created 0 604 604 0"}, {"t": 81.684, "raw": "vote-log-group vote-created 0 604 604 0"}]
- 종료후첫lag0조회1.4412592920000122초. 정확한해소시각이아니며샘플간lag최댓값은알수없음.
- SSE재연결 100명, 공백3.005~3.051초. 재연결중관측지연도통계에포함. 실제브라우저EventSource검증과구분.

투표 반복p95범위16.65~17.55ms/표본표준편차0.45ms. 합산p95아님.

누적목표관측 반복p95범위151.62~210.86ms/표본표준편차31.59ms. 합산p95아님.

원본: load-tests/results/v100-r10/realtime-batch.json 및 v100-r10-repeat-{1,2,3}/realtime-pilot/{summary.json,events.jsonl,http.json,votes.json}, 앱/runner로그 및 container-state.txt. 로컬공유자원/시간대편향/추가인프라비용을포함하며관측차이를단일기술효과로단정하지않음. 같은투표를여러관람자가관측하므로지연표본은독립적이지않음.

## 2026-10-19 16:59 KST 최종 단계별 비교 요약

관람자10명60초3회는 최초 중단과 사용자 다음 단계 진행 지시로 미완료다. 해당 무효원본은 그대로 유지했다. 이후50명/1req/s,100명/1req/s,100명/5req/s,100명/10req/s의 양쪽모드 각3회를 아래에 정리한다. 표의 범위는 **반복별 p95의 최솟값~최댓값**이며 전체 요청을 합친 p95가 아니다.

|관람자/투표req/s|모드|성공/시도(3회)|투표 p95 범위ms|누적목표 관측 p95 범위ms|관측 p99 범위ms|응답본문 범위bytes|경고 반복|
|---|---|---|---|---|---|---|---|
|50/1.0|polling|180/180|21.63~34.41|1983.78~1993.13|2019.56~2085.35|308640~308640|[]|
|50/1.0|realtime|180/180|25.77~57.49|66.36~181.26|2149.52~2952.19|404500~404500|[]|
|100/1.0|polling|180/180|39.07~53.85|1641.12~1989.97|2039.81~2225.20|610140~610140|[]|
|100/1.0|realtime|180/180|23.74~44.57|67.95~312.08|2258.31~2863.58|801800~801800|[]|
|100/5.0|polling|900/900|19.42~108.02|1908.49~1957.71|1999.12~2143.64|631062~638700|[1]|
|100/5.0|realtime|900/900|18.43~20.47|84.64~142.58|2450.59~2475.32|3946254~3954197|[]|
|100/10.0|polling|1800/1800|17.88~20.31|1920.97~1931.48|2003.64~2010.77|674397~674400|[]|
|100/10.0|realtime|1800/1800|16.65~17.55|151.62~210.86|2408.16~2458.35|7885655~7894697|[1]|

유효 판정된 위24회에서 측정 투표 총6120건 성공. 워밍업은 매회4건으로 별도이며 이 총수에 포함하지 않았다. 반복별 최종값은1req/s=1064표,5req/s=1304표,10req/s=1604표. 모든DB/관람자최종값과realtime Redis가일치했고실시간매회최종lag0/전원재접속확인. valid는설정한판정기준을통과했다는뜻이며호스트경쟁에영향받지않았다는뜻은아니다.

### 관측으로 말할 수 있는 점과 한계
- 이 조건에서 realtime은 대부분의 누적목표 갱신을 polling의2초주기보다 빠르게 관측했다. 다만SSE60초타임아웃/3초재접속구간은 관측p99를 약수초로 늘렸고 이를제외하지않았다. 항상모든갱신이더빠르다는뜻은아니다.
- 투표HTTP응답지연과결과관측지연은다르다. 같은환경의소규모유한부하이며도착률별API p95가단조롭게증가하지않는다. 첫5req/s polling반복은스왑·발생기지연경고가있어원본과함께해석해야한다.
- SSE는변경시전체스냅샷을모든관람자에게전달하므로투표량이늘면전달횟수/본문량도늘어난다. polling은주기별최신값을가져오므로여러투표가한응답에합쳐진다. SSE가항상더적은트래픽을쓴다고주장하지않음. 이름포함여부등JSON계약차이도있다.
- 최종lag0은각실행종료후적체가해소됐다는증거다. 샘플링사이에순간lag가없었다는보장이나장애시정확히한번처리보장이아니다. 원본시계열/관측간격/수집비용과함께읽는다.
- CPU/메모리/NetIO/DB통계는반복별상세와원본에기록했다. PostgreSQL은공유컨테이너,앱과발생기도같은호스트이며ps CPU와Docker CPU는합산하지않는다. 쿼리횟수/브라우저렌더링/전체네트워크비용은미측정이다.
- 대규모용량·자동포화·수천연결·다중서버·장애복구·제품최적화·종료초기화API는미실행/미구현. 최대100명/10req/s를통과했다고운영환경용량으로일반화하지않는다.

### 완료 상태
요청한 증가조건의실행은종료하며추가부하를자동실행하지않는다. 최초10명60초유효3회미완료는명시적인남은비교공백이다. 기존사용자5432/9092/6379/8080서비스와데이터는보존. 실험앱18081과각전용KafkaRedis컨테이너는중지했고실험DB/컨테이너데이터/원본로그는삭제하지않았다. 이전삭제요청으로정리한과거실험과이번보존자원은구분한다.

### 2026-10-19 17:25 KST — 실험 데이터 삭제, 컨테이너·결과 보존
- 사용자 명시: 실제 서비스 데이터 제외한 실험 자원 정리, 추가 환경변수 삭제, 컨테이너 유지. 추가 확인에서 측정 결과 보존 선택.
- 기존 실험12세트의 소유마커/컨테이너라벨/DB연결0/중지상태 검증. Kafka/Redis를 한 세트씩 시작해 vote-created 및 vote-log-group 삭제 후 사용자토픽0/소비그룹0 확인, Redis FLUSHDB SYNC 및 AOF 재작성 후키0 확인하고다시중지. Kafka 내부브로커메타데이터는재사용을위해유지하며컨테이너/볼륨삭제없음.
- DB24개/역할12개삭제,루트.env.loadtest*20개삭제. 생성 kafka-admin-libs/파이썬캐시14개디렉터리삭제. 측정원본70MB와상태manifest/코드보존. 기존도구의오래된설정참조는더이상실행환경으로재사용하지않으며새실험에서연결설정을준비해야함.
- 첫사전검사는이미삭제된과거실험설정에서실패(변경전). 해당컨테이너/DB부재검증후제외하여실제12세트만처리. 모든검증통과.
- 사후확인: DB목록live_lunch_choice/mailplug/postgres/seat_load_test만잔존, llc_lt_역할0. 실험컨테이너24개동일ID/중지,기존PG/Kafka/Redis실행및8080 PID22930보존. 결과 load-tests/results/data-cleanup-containers-kept.json에보존. 새부하/제품구현미실행.
