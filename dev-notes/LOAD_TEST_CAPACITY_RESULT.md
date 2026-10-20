# DB·SSE / Kafka·Redis·SSE 새 비교 실험

최신 상태: 사용자 요청으로 경미한 경고를 넘겨 resume1 재개. 소부하2회/조회12회 실행 후100만 건 DB 조회50req/s에서15초간 성공 응답 없음·DB 연결 획득 타임아웃으로 중단. 본 투표·SSE 처리 용량은 미검증. 상세는 하단 재개 결과. 이전 polling 결과는 사용하지 않는다.

## 실행 조건 (본 측정 전 고정)
- run: `llc-cap-20261019-173825`, 원본 `load-tests/results/capacity/llc-cap-20261019-173825/`.
- HTTP 성공률 ≥99.9%, 발생률 ≥99%, API p95≤200ms/p99≤500ms. 누적 목표 최초 관측 p95≤500ms/p99≤1초. 미관측 포함, drain≤60초. 최종 SQL/Redis/전원 SSE 일치. 마지막60초 적체 증가면 실패.
- 발생기 스케줄 지연 p99≤20ms 추가 검증 기준. 예약보다100ms 이상 늦거나 in-flight1024면 미발행으로 남기며 따라잡기 폭주를 하지 않는다.
- 양쪽 앱 동일 JVM heap512MiB/ActiveProcessorCount2/DB pool10. host JVM이므로 CPU 하드쿼터는 아님. PostgreSQL2CPU/1GiB, Kafka1CPU/1GiB, Redis0.5CPU/256MiB. 같은 노트북과 Docker VM 자원 경쟁 한계. 추가 인프라 비용 별도 수집.
- 동일 후보4개/균등 seed/인덱스/ANALYZE. A worker1/유한큐1024, B Kafka1파티션. 공통 SSE 전체 스냅샷, 한 알림에 한 번 조회, timeout900초. DB 동점ID오름차순, Redis 동점역사전순은 유지하며 후보별 점수로 검증.
- 조회 진단: 워밍업5초/측정30초, 쓰기·SSE 없음. 본 경로: 워밍업30초/측정180초/drain최대60초.
- API/관측기 monotonic clock, 서버 eventId 단계 epoch millis. 같은 호스트 Docker 시계 사용, 별도 호스트 시계 동기화 실험은 아님. 단계 로그 계측은 양쪽 동일 정책이나 경로별 단계 수 차이가 있음.

## 준비 검증
- 일반83건/실제 Kafka·Redis·HTTP·SSE9건 통과. 새 db-sse 커밋 가시성/롤백 미전송/유한큐 포화 검증 포함.
- 최초 관측기 검증의 임시 서버 Python3.9 asyncio.Lock 생성 루프 오류: 검증 서버 결함으로 제외, 원본 보존. 누락 예약을 누적 목표에 포함하던 관측기 로직도 실제 발행 대상 기준으로 수정. HTTP 오류가 발생하면 누적 목표의 개별 이벤트 의미는 보장하지 않으며 정확성 대조와 함께 실패 처리.
- 수정 후100연결/200req/s:3000/3000 발행·성공, 관측누락0, 스케줄p99 0.77ms. 500연결/200req/s는 발생률32.7%로 발생기/검증 서버/로컬 환경 한계. 이 수치를 서비스 용량으로 사용하지 않음.
- 격리 Kafka 최초 기동: 새 named volume 소유권 오류로 실패. 전용볼륨을 UID1000으로 수정. 초기 realtime 준비 실패는 부하 미실행으로 보존.
- 소부하 동등성: 각 모드14표(워밍업4+측정10), SQL 각2504/2504/2503/2503. 두 클라이언트 일치, realtime Redis 일치·14커밋/발행/집계/알림/스냅샷/브로드캐스트, SSE전송30(초기2+14×2). DB 경로도 로그상14알림/조회/전송 완료. 기존 서비스 데이터 변경 없음.

## 실행 범위
아래 최종 기록이 현재 상태다. 본 경로 용량 비교는 미완료이며 준비·소부하 검증과 구분한다.

## 최종 실행 상태 — 2026-10-19

**DB·SSE 비교 구현과 소부하 동작 계약 검증은 완료. 처리 용량 비교는 환경/발생기 기준 위반으로 중단했으며, 처리 한계와 우열은 미확정이다.** 아래는 새 실험에서 실제 실행한 결과만이다.

### 조회 진단: 초기 1만 표, SSE/쓰기 없음

각 반복 워밍업5초+측정30초. API 값은 ms. A=db-sse의 DB GROUP BY, B=realtime의 Redis 랭킹 조회. 첫 A 실험 후 검증용 서버와 사용이 끝난 Gradle 데몬만 종료하고 동일10req/s에서 B/A→A/B 순서로 재검증했다. 사용자 앱은 종료하지 않았다. 첫 실패를 지우거나 기준을 완화하지 않았다.

| 반복 | 목표 req/s | 측정 발생/성공 | API p50 / p95 / p99 | 발생기 지연 p99 | 판정 |
|---|---:|---:|---:|---:|---|
| A-1 | 10 | 300 / 300 | 10.64 / 27.35 / 79.34 | 23.89 | 발생기 기준 실패 |
| B-2 | 10 | 300 / 300 | 5.42 / 9.54 / 15.91 | 3.88 | 이 조건 수치 기준 통과 |
| A-2 | 10 | 300 / 300 | 10.43 / 15.01 / 41.48 | 5.81 | 이 조건 수치 기준 통과 |
| A-3 | 10 | 300 / 300 | 8.99 / 14.67 / 33.40 | 11.50 | 이 조건 수치 기준 통과 |
| B-3 | 10 | 299 / 299 | 5.21 / 10.77 / 78.68 | 15.27 | 발생률99.67%, 미발행1건 보존 |
| A 상한 진단 | 200 | 5919 / 5919 | 3.24 / 39.81 / 152.82 | 83.84 | 발생률98.65%로 환경 중단 |

- 1만 건의 낮은 조회 부하에서는 양쪽 응답이 기준 안이었다. 200req/s에서는 HTTP 성공률100%였지만 실제 발생률이99% 미만이다. **200req/s를 DB 처리 한계로 판정하지 않는다.** 같은 조건 B, 더 큰 데이터와 본 경로 부하는 실행하지 않았다.
- 상한 진단의 미발행: 측정81건, 워밍업 포함87건. in-flight 최대62. 응답 실패·타임아웃0, SQL 최종1만 표 그대로. 계획된6000건과 실제5919건을 구분한다.
- 정확한 중단 원인은 측정기 발생률/스케줄 기준 위반이다. 호스트 스왑 약9.4→9.6GiB 수준 증가 및 다른 개발 도구의 동시 실행을 관측했다. 메모리 압박은 원인 후보이며 스왑 증가만으로 지연의 인과를 확정하지 않는다. OOM이나 DB 자체 포화를 관측한 것으로 쓰지 않는다.

### SQL·Redis·자원 근거와 한계

- `pg_stat_statements` 전후 차이: A10req/s 각351회(초기조회1+워밍업50+측정300), 총 SQL 실행시간921.73/847.00/901.91ms. A200req/s는6914회(초기1+실제전체6913),8721.56ms, 평균1.26ms/회. 읽은 shared block은 캐시 hit이며 측정 차이상 shared_blks_read0, temp write0. 이는 1만 건의 워밍업된 데이터 조건이다.
- B 조회 중 DB 집계 SQL0회. Redis ZREVRANGE는 B-2 351회/4934μs, B-3 350회/4815μs. Redis 서버 내부 명령 시간이며 HTTP/네트워크 전체 지연과 동일하지 않다.
- A200: PostgreSQL CPU 표본 평균30.65%/최대64.34%(100%=1코어, 설정2코어), 메모리 약94.6→97.7MiB. 앱 GC 표본 증가34ms, poolPending 최대0. 낮은 간격의 표본이므로 짧은 대기를 배제하지 않는다. 원본 docker stats의 ANSI 제어문자는 분석 시 제거했다.
- 최초 계측은 B에서 Kafka lag 확인용 CLI JVM을 주기적으로 Kafka 컨테이너 안에서 실행했다. **해당 Kafka CPU/RSS에는 관측기 비용이 섞였으므로 인프라 비용 비교에 사용할 수 없다.** A에서도 Kafka·Redis를 켜 둔 상태이므로 전체 인프라 비용의 독립적인 A/B 비교도 아니다. CPU/IO/메모리 원본은 보존한다.
- 계측 간섭 발견 후 `KafkaLagSampler.java`로 호스트의 단일 AdminClient를 재사용하도록 수정했고 양쪽에 같은 관측기를 적용하도록 실행기를 바꿨다. 읽기 전용 검증에서 broker 시작 직후 오류 표본1개를 보존하고, 이후 lag0 표본2개/종료0 확인. 준비 완료는 실제 lag 표본으로 확인한다. **수정된 계측기를 넣은 부하 실험은 미실행**이며 이전 CPU 수치를 보정해 재사용하지 않는다.
- CPU throttling, 커넥션 획득 대기 시간 자체, 계측 오버헤드 대조 실험은 미검증. poolPending은 대기자 표본일 뿐 획득 시간 측정이 아니다. JSON summary의 pass는 임시 수치 판정이며 위 환경·계측 한계와 함께 해석한다.

### 소부하 실시간 동작과 eventId

- A14표: 커밋/큐 처리/스냅샷/브로드캐스트14회, 조회17회(초기API1+SSE초기2+변경14), 실제 SSE30개. SQL 최종2504/2504/2503/2503, 두 관람자 일치. 예시eventId `be041ccd-a53b-40fe-835c-58f72ed7316a`.
- B14표: 커밋/발행확인/집계/발행알림/수신알림/스냅샷/브로드캐스트14회, Redis조회17회, 실제 SSE30개. SQL/Redis/두 관람자 모두 동일, 최종 Kafka committed14/end14/lag0. 예시eventId `b42e4bb8-9289-4528-b9e8-2ec6a51a44b4`.
- eventId별 서버 로그를 연결한 commit→broadcast 중앙값/최대는 A10/14ms, B23/681ms(워밍업 포함14건). 이는 소부하 기능 확인용이며 용량 비교 결론으로 사용하지 않는다. `event-correlation.json`에 전체 원본 연결을 보존했다. SSE send 완료는 브라우저 렌더 완료가 아니다.
- 일반 테스트83건·실제 Kafka/Redis/HTTP/SSE 테스트9건 모두 실패/오류/건너뜀0. 마지막 변경은 실험 도구의 lag 관측 방식과 설명이며 별도 Python 문법 및 읽기 전용 관측기 검증을 수행했다.

### 실행 명령과 결과 위치

- 일반/실제 통합: `./gradlew test redisTest bootJar`
- 소부하 예시: `/tmp/llc-capacity-venv/bin/python load-tests/capacity/run.py --name equivalence-realtime-v2 --mode realtime --seed 10000 --rate 2 --viewers 2 --warmup 2 --duration 5`
- 조회 탐색: `/tmp/llc-capacity-venv/bin/python load-tests/capacity/batch.py read`
- 최저 조건 재검증: `/tmp/llc-capacity-venv/bin/python load-tests/capacity/recheck.py`
- 기존 결과 이름은 덮어쓰기 거절. 재실행은 새 run/결과 이름과 정지 컨테이너 기동이 필요하다. 현재 상태 비밀값은 Git 제외된 `load-tests/state/capacity/current.json`에만 보관하며 사용자 `.env`는 변경하지 않았다.
- `analysis.json`, 각 반복의 `summary.json`, `requests.jsonl`, `metrics.json`, `resources.json`, `docker-stats.jsonl`, `pg-before/after.json`, `redis-before/after.txt`, `gc.log`, `app.log`에 원본 저장. 환경/commit/diff/source ZIP/JAR SHA도 같은 run 폴더에 보존.

### 미실행 항목과 다음 단계

- 10만·100만 건 조회 진단, 비교 가능한 최고 통과/최초 실패 경계, 본 경로100관람자/10→200투표req/s의30초+180초 탐색·경계3회 재현, 300/500관람자 서비스 실험은 미실행.
- 마지막60초 lag/queue/관측지연 증가 판정과 본 부하 후60초 drain도 미실행. 소부하 lag0를 본 부하 지속 처리 통과로 대체하지 않는다.
- 다음은 발생기 분리 호스트 또는 호스트 여유 자원 확보 후 수정된 관측기를 재검증하는 단계다. 같은 SLO를 유지하고 단계별 실험을 새 결과로 다시 시작한다. 계측 비용과 CPU throttling/DB pool 획득 시간도 보완 확인해야 한다.
- DB 재집계와 Redis 사전집계의 조회 경로 차이는 확인했지만, 전체 구조의 처리 용량 차이와 Kafka 자체 효과는 확인하지 못했다. **Kafka·Redis가 유리하다는 결론 없음.**
- 종료 상태: 실험 앱18082/검증 서버18083 종료, 이번 전용 컨테이너3개 중지. 데이터·볼륨·과거 결과 보존. 실제 앱8080 PID22930 및 서비스 PostgreSQL5432/Kafka9092/Redis6379 유지. 종료/초기화 서비스 기능·다중 서버·장애 보상은 추가하지 않았다.

## 재개 결과 — 2026-10-19 18:47 KST (resume1)

사용자 최신 지시로 소폭 발생률 미달·스케줄 지연·SLO 초과는 경고로 남기고 계속했다. 기존 합격 기준의 실패를 성공으로 변경하지 않았다. 전용 인프라/기존 앱 예산 유지, 기존 결과를 덮어쓰지 않고 `resume1-*`로 구분했다.

### 실제 실행 범위
- 소부하 동작 재검증2회 + 조회 진단12회 완료. 조회는1만/10만 건에서10·200req/s,100만 건에서10·50req/s를 비교했다. A/B 순서를 교차했다.
- 수정한 지속 AdminClient lag 관측기를 양쪽에 적용했다. capacity 전용 Hikari 연결 획득 횟수·누적 시간과 cgroup CPU throttling 계측을 추가했다. `./gradlew test bootJar` 일반83건 통과. 실제 Kafka/Redis/SSE 소부하 양쪽 재검증 통과. 별도 redisTest9건은 이번 재개에서 다시 실행하지 않았으며 앞선 검증 기록과 구분한다.
- 체크 양쪽 각각14표(워밍업4+측정10), SQL/두 SSE 일치 및 B Redis/lag0 확인. 12개 조회 실험에는 쓰기·SSE가 없고 최종 SQL 시드 건수 유지. 조회 결과의 `최종 정확성=True`는 실시간 부하 정확성 검증을 뜻하지 않는다.
- 다음 표의 API 지연은 실패한 요청의 클라이언트 대기 시간도 포함한다. 마지막 A의 약10.95초를 성공한 서버 처리 시간으로 읽으면 안 된다. 미발행과 조기 중단도 예정 요청 분모에서 제외하지 않았다.

| 실행 | 실제/예정 | HTTP 성공 | API p95 ms | 관측 p95 ms | 발생기 경고 | 최종 정확성 |
|---|---:|---:|---:|---:|---|---|
| resume1-check-s10000-r2-v2-db-sse-1 | 10/10 | 10 | 33.77 | 37 | False | True |
| resume1-check-s10000-r2-v2-realtime-1 | 10/10 | 10 | 38.86 | 62 | False | True |
| resume1-read-s10000-r10-v0-db-sse-1 | 300/300 | 300 | 15.25 | None | False | True |
| resume1-read-s10000-r10-v0-realtime-1 | 300/300 | 300 | 10.51 | None | False | True |
| resume1-read-s10000-r200-v0-db-sse-1 | 6000/6000 | 6000 | 8.05 | None | False | True |
| resume1-read-s10000-r200-v0-realtime-1 | 5979/6000 | 5979 | 3.60 | None | False | True |
| resume1-read-s100000-r10-v0-db-sse-1 | 300/300 | 300 | 34.35 | None | False | True |
| resume1-read-s100000-r10-v0-realtime-1 | 300/300 | 300 | 8.95 | None | False | True |
| resume1-read-s100000-r200-v0-db-sse-1 | 4566/6000 | 4502 | 9305.81 | None | True | True |
| resume1-read-s100000-r200-v0-realtime-1 | 6000/6000 | 6000 | 3.34 | None | False | True |
| resume1-read-s1000000-r10-v0-db-sse-1 | 299/300 | 299 | 1339.62 | None | False | True |
| resume1-read-s1000000-r10-v0-realtime-1 | 300/300 | 300 | 11.49 | None | False | True |
| resume1-read-s1000000-r50-v0-db-sse-1 | 1205/1500 | 10 | 10951.72 | None | True | True |
| resume1-read-s1000000-r50-v0-realtime-1 | 1500/1500 | 1500 | 5.76 | None | False | True |

### 경고를 넘겨 진행한 조건
- 10만 건 A200req/s: 예정6000건 중4566건 발행,4502건 성공. API p95 9305.81ms, in-flight1024 도달. 스케줄 지연p99 3.85ms였으므로 단순히 발생기 CPU가 느린 것으로 설명하지 않는다. 서버 응답 지연으로 진행 중 요청 상한에 도달해 미발행이 생긴 조건이다. 최종 데이터는 유지되어 경고를 기록하고 다음 비교를 진행했다.
- 같은10만 건 B200req/s:6000/6000성공, API p95 3.34ms. 이 차이는 매 조회 DB 집계와 Redis 사전집계 조회의 차이다.

### 실제 중단 사유 — 100만 건 A50req/s
- 15초 동안 HTTP 성공 응답이 없어서 신규 요청을 중단했다. 측정 구간 예정1500건 중1205건 발행,10건 성공(발행 대비0.83%). 워밍업 포함1455건 중146건 성공/1309건 클라이언트 오류. 대부분 오류는10초로 설정된 요청 타임아웃 부근에서 끝났고, 당시 예외 문자열은 비어 있어 원본으로 개별 예외 타입을 확정하지 않는다. 이후 도구는 repr로 타입도 보존하도록 보완했다.
- 30초 측정 예정이었지만 이 조건은 조기 중단됐다. B의 정상30초 결과와 같은 시간 동안 정상 수행한 실험으로 취급하지 않는다.
- PostgreSQL CPU 표본 평균 약203%(100%=1코어),2코어 예산을 거의 사용. cgroup 차이514기간 중509기간 throttled. throttle 시간은 여러 실행 스레드의 누적값이므로 벽시계 지연으로 환산하지 않는다.
- 앱 DB pool active10/idle0, 대기 최대190. 실제 로그: `Connection is not available, request timed out after 30006ms (total=10, active=10, idle=0, waiting=189)`. DB 연결을 얻지 못한 요청이 쌓인 근거다.
- SQL 집계 완료578회, 누적 실행461394.77ms, shared read0/hit4821684. 워밍업·측정·drain 합계이고 동시 SQL의 누적 시간이라 실행 벽시계 시간과 다르다. DB 메모리 약247→248MiB/1GiB, OOM없음. CPU 제한과 연결 풀 대기가 직접 관측된 병목이며 같은 호스트 경쟁 영향까지 완전히 분리한 것은 아니다.
- 메트릭 API도 일부 타임아웃되어 마지막값이 없었다. 분석은 유효한 표본만 사용하며 누락을0이나 음수GC로 계산하지 않는다.
- 같은100만 건 B50req/s는1500/1500성공, API p95 5.76ms, poolPending0, DB 집계 SQL0. 이 결과로 Kafka 이벤트 처리량이나 전체 투표·SSE 경로의 우세를 주장하지 않는다.

### 자원 계측 보완과 해석 범위
- cgroup-before/after는 사후 Kafka CLI 검증 전에 수집돼 CLI 비용을 포함하지 않는다. PostgreSQL CPU 예산 소진 판단은 이 계측과 연결 풀/SQL 로그를 함께 사용했다.
- Docker stats 파일은 종료 검증까지 계속 수집되어 끝부분에 최종 Kafka CLI 비용이 섞일 수 있다. B의 Kafka CPU/RSS 최고값을 부하 구간의 비용으로 사용하지 않는다. 다음 실행부터 사후 CLI 전에 샘플러를 종료하도록 수정했다. 이 마지막 보완 뒤 새 부하 재측정은 하지 않았다.
- poolAcquireNanos는 커넥션 획득의 누적 소요 시간이며 순수 잠금 대기만을 분리한 값이나 p95가 아니다. 측정 API 누락 구간도 있으므로 원본 표본 범위를 함께 확인한다.
- 같은 노트북/같은 Docker VM,2CPU PostgreSQL,앱512MiB·ActiveProcessorCount2·pool10이라는 한정된 결과다. A1만 건200req/s와10만 건10req/s는 관측 기준 통과,10만 건200req/s 실패. 중간50/100이 없어 정확한 경계는 미확정. A100만 건은10req/s부터 지연기준 실패,50에서 심각한 응답 불능. B는 실행한 조회 조건을 통과했지만100만 건100/200은 미실행.

### 남은 작업과 종료 상태
- 100명 SSE를 유지하는 투표10→200req/s의30초 워밍업+180초 측정, 마지막 통과/첫 실패3회 반복,300/500명 관람자 증가는 이번 심각한 응답 불능으로 미실행. 이를 조회 진단으로 대신하지 않는다. 본 경로의 queue/lag 지속성·drain·최종 집계 용량은 여전히 미검증이다.
- 처리 모드·DB CPU/인덱스/서비스 기능을 유리하게 바꾸지 않았다. 이번에는 실험 계측과 실행 제어만 보완했다. 종료/초기화 서비스 기능도 추가하지 않았다.
- 원본 `resume1-*-ledger.json`, `resume1-analysis.json`, `resume1-table.md`, `resume1-critical.json`, 각 반복의 요청/SQL/자원/메트릭/log 및 `cgroup-before/after.json`, `lag.jsonl` 보존. 최초 실행 전/마지막 DB dump도 보관.
- 실행 명령: `/tmp/llc-capacity-venv/bin/python load-tests/capacity/continue.py --session resume1 check` 및 `/tmp/llc-capacity-venv/bin/python load-tests/capacity/continue.py --session resume1 read`. full/viewers는 순차 실행 예정이었지만 read의 critical 종료로 시작되지 않았다.
- 실험 앱18082 및 전용컨테이너3개 중지, 데이터·볼륨·이전 결과 보존. 기존8080 PID22930/5432/9092/6379 정상 유지 확인. 실제서비스 초기화 없음.
