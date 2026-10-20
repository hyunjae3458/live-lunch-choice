> **2026-10-19 17:18 KST 사용자 결정: 비교 근거 폐기.** 이 문서의 polling/realtime 결과는 신규 처리 용량 비교 및 회고의 성능 근거로 사용하지 않는다. 원본 이력만 보존한다. 새 계획: [LOAD_TEST_CAPACITY_HANDOFF.md](LOAD_TEST_CAPACITY_HANDOFF.md).

# 소규모 예비 결과 — polling/realtime 실행 및 정확성 검증

2026-10-19. 실험 ID20261019_144130_353a. 사용자 요청대로 polling 실행 후 멈췄고, 사용자의 환경변수 변경 완료 확인을 받은 뒤 realtime을 실행했다. 두 모드의 집계 정확성은 확인했으나 realtime 자원 샘플과 측정 중 lag 관측에 결함이 있어 자원 효율·적체 해소 시간은 비교하지 않는다.

## 실행과 데이터 격리
- 새 PostgreSQL DB2개/전용 역할 생성, 동일 후보4개/초기투표1000건(400/300/200/100) 확인. 기존 PostgreSQL 컨테이너만 재사용했고 기존 live_lunch_choice DB에 쓰기/초기화하지 않음.
- 전용 Kafka19092/Redis16379 컨테이너 생성. Redis 기준1000표, 합성 seed 처리 이력1000개. 초기 데이터는 직접 시드이며 Kafka로 발행한 데이터가 아님.
- 기존8080 앱 PID22930 유지. 실험 polling PID27062/18080은 측정 후 SIGTERM으로 종료(exit143),18080 리스너 없음 확인.
- 실험 DB/전용 인프라는 원본 확인을 위해 보존. 루트 .env 변경 없음, .env.loadtest 현재APP_PROFILE=realtime. realtime PID27303/18080도 검증 후 SIGTERM 종료(exit143), 기존8080 PID22930 유지 및18080 리스너 없음 확인.

실행 명령:

`./gradlew bootJar`

`python3 load-tests/manage.py prepare`

`python3 load-tests/manage.py run`

`python3 load-tests/pilot.py --viewers 10 --seconds 30 --rate 1`

`python3 load-tests/manage.py snapshot`

`python3 load-tests/manage.py stop`

## polling 실제 결과

| 지표 | 결과 |
|---|---:|
| 관람자 / 요청 목표 | 10명 / 투표1req/s |
| 측정 구간 | 30.005초 |
| 투표 시도 / 성공 / 실패 | 30 / 30 / 0 |
| 실제 투표 요청률 | 0.9998req/s |
| 투표 API p50 / p95 / p99 | 16.18 / 23.35 / 130.28ms |
| 결과 조회 수 | 150건 |
| 조회 API p50 / p95 / p99 | 10.01 / 16.97 / 17.85ms |
| 후보 누적 목표치 관측 p50 / p95 / p99 | 1001.63 / 1198.94 / 1215.03ms |
| 누적 목표치 관측 최대 | 2038.56ms |
| HTTP 응답 본문 합계 | 33,720bytes(투표/결과 조회 포함) |
| 측정 종료 후 최종 스냅샷 수렴 | 0ms(종료 이전에 전원 수신) |
| 마지막 성공 응답 → 전원 최종 수신 | 202.26ms |

투표 API p99는30표본에서 최대값이며 첫 쓰기의 초기화 비용이 포함됐다. 관측 지연은 요청 시작 기준/후보별 누적 임계치 기준이며 이벤트 단위 처리 지연이 아니다. 10명 접속 시각을 거의 동시에 맞춘 조건이라 임의의 사용자가 보는 지연 분포로 일반화할 수 없다.

정확성:

| 후보 ID | 초기 | 신규 성공 | 최종 SQL | 관람자10명 |
|---|---:|---:|---:|---:|
|1|400|8|408|408|
|2|300|8|308|308|
|3|200|7|207|207|
|4|100|7|107|107|
|합계|1000|30|1030|1030|

오류0, exact=true. 별도 snapshot으로 **realtime DB는400/300/200/100 및 Redis도동일 기준값 유지** 확인.

## 자원/DB 관측
- 전체 자원 샘플6개, 측정 구간 샘플5개. resource_error0.
- 공유 PostgreSQL CPU0.19~5.52%, 메모리160~161.3MiB. 앱 RSS168,048~337,600KiB, ps CPU0.1~19.1%. 생성기 RSS12,992~16,432KiB, CPU0~0.2%.
- 측정 중 전용 Kafka는 유휴 상태에서도 CPU1.67~5.27%/약334~336MiB, Redis0.23~0.43%/약12.5~12.6MiB. polling이 이 인프라를 사용한 것이 아니라 미리 준비한 유휴 비용이며 같은 PC 경쟁 요인이다.
- 실험 DB 통계 전후: xact_commit42→470, rollback0→0, blks_read5→5, blks_hit29033→38422, tup_returned22387→203333, tup_fetched9940→14911, tup_inserted1159→1189, temp_bytes0/deadlocks0.
- DB 통계 구간은 초기 확인/워밍업/최종 검증 포함. tup_inserted 절대값은 통계상 카탈로그 작업도 포함할 수 있으므로 SQL votes count로 정확성을 별도 확인했다. pg_stat_statements 기반 정확한 집계 쿼리 횟수는 미수집.
- CPU는 로컬/공유 자원/적은 샘플이고 ps와 Docker의 산정 방식이 달라 직접 합산하지 않음. 네트워크는 HTTP 본문과 컨테이너 NetIO 원본만 보존했으며 앱별 TCP 전체 비용을 분리하지 못함.

## 원본과 재현
- load-tests/results/polling-pilot/summary.json
- load-tests/results/polling-pilot/events.jsonl (스냅샷·자원·시점)
- load-tests/results/polling-pilot/http.json (HTTP 상태/지연/본문 크기)
- load-tests/results/polling-pilot/votes.json (성공 voteId/후보/시각)
- load-tests/results/polling-app.log
- load-tests/state/manifest.json 및 baseline.json (비밀값 제외)
- 설정/역할 비밀번호는 .env.loadtest(0600/Git 제외), 문서/로그 노출 없음.

## 실패·수정과 한계
- 첫 prepare는 Path.open(opener=...) 지원 차이로 외부 자원 생성 전에 실패. 내장 open으로 수정 후 정상 준비.
- 스크립트 문법 검사에서 macOS Python 캐시 디렉터리 sandbox 권한 오류 발생. 승인된 실행 및 /tmp 전용 캐시 경로로 검사.
- 첫 pilot은 HTTP 기록 변수명과 http 모듈 충돌로 최초 요청 전 실패. 투표0건, 초기값 유지. 비어 있는 실패 결과 폴더는 polling-preflight-failed로 보존 후 변수 수정하여 실제 측정 진행.
- 최종 수렴 값은 첫 코드에서 확인 루프 종료 시각을 사용했으나, 원본 snapshot 시각으로 재계산해 최초 전원 관측시각으로 수정. 추가 부하 없이 산출식만 바로잡음. 현재 값은 측정 종료0ms/마지막응답 이후202.26ms.
- 제품 Java/업무 API 변경 없음. bootJar 성공, Python 문법/diff 검사. 전체 JUnit 재실행은 하지 않음.
- realtime 실제 실행 결과는 아래 추가 기록 참조. 대규모 비교/용량 판정/부하 중 lag 시계열 및 정확한 해소 시간은 미검증.
- cleanup 도구 구현했지만 실제 삭제 검증 미실행. 기존 데이터를 보존하기 위해 실험 데이터도 중간 확인 동안 유지.

## realtime 실제 결과와 비교

관람자10명/투표1req/s/30초를 동일하게 실행. 30건 모두201, 오류0, 실제0.9999req/s. 두 SQL DB와 Redis, 관람자10명 모두408/308/207/107=1030표. 제품 코드 수정 없음.

| 지표 | polling | realtime |
|---|---:|---:|
| 성공 / 시도 |30/30|30/30|
| 투표 API p50 / p95 / p99(ms)|16.18 / 23.35 / 130.28|21.57 / 40.31 / 238.75|
| 후보 누적 목표 관측 p50 / p95 / p99(ms)|1001.63 / 1198.94 / 1215.03|40.15 / 65.06 / 862.90|
| 측정 중 결과 전달 |조회150응답|SSE300스냅샷|
| 본문 합계(bytes)|33720|44700(HTTP3600 + SSE JSON41100)|
| 마지막 성공 응답→전원 최종값(ms)|202.26|18.12|
| 측정 종료 후 전원 최종값(ms)|0|0|

이번 조건에서 realtime은 관측 반영이 빨랐지만 투표 API 지연과 본문 총량은 더 컸다. 30건의 작은 단회 실행이며 첫 Producer 초기화 포함, 동기화된 관람자 접속 조건이다. 본문은 SSE 프레임/HTTP 헤더/TCP 비용 제외이며 JSON 계약도 다르다. SSE가 항상 적은 트래픽을 쓴다고 해석하지 않는다.

실제 로그:
- PUBLISH_SUCCEEDED / CONSUMER_RECEIVED / VOTE_AGGREGATED / RANKING_CHANGE_PUBLISHED / RANKING_CHANGE_RECEIVED 각각30건.
- 첫 eventId=efcd4503-3f57-4d7a-b940-69c395303f5e, voteId1001, partition0/offset0, 소비2026-10-19 14:58:27.343 KST.
- 마지막 eventId=b6a699e6-1457-4f7d-b24f-9f9fd89badc3, voteId1030, partition0/offset29, 소비14:58:55.590 KST.
- 호스트 Admin 재조회: `vote-log-group vote-created 0 30 30 0` (partition/committed/log-end/lag). 최종 lag0. 조회 시각은 lag-recheck.json에 보존. 부하 종료 직후 lag0이 된 정확한 시점은 미검증.
- 관측기 상대시각 SSE 최초연결0.641~0.643초, EOF60.871~60.873초, 두번째 연결63.884~63.893초. 10명 모두 두번째 연결 직후 최종1030표 스냅샷을 확인. 실제 브라우저 EventSource 자체 검증은 이번에 수행하지 않음.

측정 도구 오류와 검증 구분:
- 최초 pilot은 집계값/재접속은 일치했지만 Docker 안 Kafka CLI가 호스트용 advertised 127.0.0.1:19092에 연결하지 못해 타임아웃. CLI가 오류 문자열을 stdout/exit0으로 내보내 final_lag=null, exact=false 및 pilot exit1이 됨. 원본 summary.json을 성공으로 덮어쓰지 않음.
- KafkaLag.java와 manage.kafka_lag를 추가해 bootJar의 Kafka client/SLF4J 라이브러리로 호스트 읽기 전용 Admin 조회. 첫 Admin 설정은 request timeout보다 작은 API timeout으로 실패했고 request.timeout.ms=5000 지정 후 정상 조회. 투표 추가/재실행 없이 최종lag0 및 SQL/Redis를 다시 확인.
- validation-recheck.json에 별도 최종 검증 근거 저장. 수정된 전체 pilot 재실행은 미실행(초기 데이터가 이미 변했으므로 동일 실험 재실행 거절 유지).
- Kafka CLI 대기로 자원 샘플은 전체3개이나 측정 구간 내0개. realtime CPU·메모리 비교 미검증. 전체 구간의 샘플은 원본에만 참고 보존. 수정한 Admin은 빠른 단독 조회 성공, 부하 중 샘플링 재검증은 후속.
- realtime pg_stat_database 전후 commit70→194, rollback0, read5→5, hit30108→37173, returned35278→42335, fetched10285→14290, inserted1159→1189, temp0/deadlock0. polling과 달리 재접속/실패한 lag 대기까지 포함한 구간이므로 초당 DB 부하로 비교하지 않음.

추가 원본: load-tests/results/realtime-pilot/{summary.json,events.jsonl,http.json,votes.json,lag-recheck.json,validation-recheck.json}, load-tests/results/realtime-app.log. summary의 실패와 후속 재확인 결과를 함께 읽어야 한다.

## 다음 단계와 미검증
먼저 새 동일 시드 환경으로 수정된 관측기10명/1req/s/30초를 재검증해 측정 중 자원 샘플과 lag 시계열을 확보하는 것을 제안한다. 이번에는 추가 부하를 자동 실행하지 않았다. 이후 PLAN의10→50→100명/각60초3회, 쓰기1→5→10req/s 순서로 한 변수를 늘린다. 수백~수천 연결은 비동기 관측기와 전용 자원 준비 후 별도 합의한다. 서비스 종료/초기화·다중 서버·장애 복구·대규모 시험은 이번에 구현/실행하지 않음. 전용 인프라/DB는 검토용으로 보존,18080 앱은 종료.
