# 구조별 부하 비교 계획

2026-10-19 최신: SCALE_HANDOFF에 따른 새 관측기 재검증 양쪽 통과. LOAD_TEST_SCALE_RESULT.md에 최신 실행 기록. 아래 기존 예비 실험 기록은 보존. 관람자10/50/100 및100명5/10req/s 각60초3회 범위가 승인됐으나 첫60초polling에서swap증가/발생기지연으로중단. 환경경쟁확인후같은조건부터재개논의,이후조건미실행. 중단기준과모드전환확인절차유지.

## 격리와 토폴로지
- 앱1개를 순차 실행. 기존8080(PID22930)은 보존, 실험 앱은127.0.0.1:18080.
- 기존 PostgreSQL18.6 컨테이너 mailplug-seat-reservation-postgres-1/5432를 재사용. 고유 실험 ID 20261019_144130_353a.
- DB: live_lunch_choice_lt_20261019_144130_353a_polling, live_lunch_choice_lt_20261019_144130_353a_realtime. 전용 LOGIN 역할 llc_lt_20261019_144130_353a. superuser/createdb/createrole 없음. 비밀번호는 Git 제외 .env.loadtest(0600)에만 보관.
- Kafka4.1.2 전용 컨테이너/19092, Redis8 전용 컨테이너/16379. 이름과 run ID 라벨 검사. 기존9092/6379와 분리. 토픽/그룹/키/채널은 같은 이름을 사용하지만 물리 인스턴스가 달라 격리됨.
- 실험 설정은 load-tests/application-loadtest.yml, .env.loadtest. 루트 기존 .env/제품 Java 코드 변경 없음. DB 스키마는 validate.
- 앱 JVM Xms256m/Xmx512m/ActiveProcessorCount2, Hikari10. ActiveProcessorCount는 JVM 인식값이지 OS CPU 하드 제한이 아님.
- Kafka CPU1/메모리1GiB(heap256~512MiB), Redis CPU0.5/256MiB/AOF. 공유 PG는 기존 한도 그대로(컨테이너 메모리 약7.748GiB), 다른 DB와 자원 공유.
- 호스트8CPU/16GiB, Docker VM8CPU/약7.748GiB. 기존 앱과 PG/Kafka/Redis도 실행 중. 자원 경쟁을 피한 전용 장비 결과가 아님.

## 데이터와 준비
- 후보 한식/중식/일식/양식4개, 두 DB에 동일한 seed userId1000개. 분포400/300/200/100. 시각도 동일한 고정값.
- 두 DB 모두 PK/후보명·사용자ID 유니크/FK와 votes(candidate_id) 인덱스. 이 실험용 인덱스는 제품에 자동 추가하지 않음.
- 새 DB에만 CREATE/INSERT, prepare는 기존 환경 파일/동명 자원을 덮어쓰지 않음. 실험 DB 주석에 소유 마커.
- Redis 초기 랭킹을 같은 분포로 직접 시드하고 processed-events에 합성 seed-000001~seed-001000 등록. 이 초기 데이터는 실제 Kafka 이벤트 발행/소비를 거친 것이 아니며 소요 비용은 측정에서 제외. 이후 측정 투표만 실제 기존 파이프라인 사용.
- 신규 Kafka 인스턴스라 이전 메시지/커밋 없음. 초기 시드에 대한 이벤트를 뒤늦게 발행하면 중복 집계될 수 있으므로 재발행하지 않는다. 합성 seed ID는 측정 UUID 이벤트와 별개.

## 도구와 시나리오
- Python3 표준 라이브러리 http.client/threads 사용. 추가 패키지 없음. Docker k6 이미지는 있지만 일반 HTTP 측정만으로 SSE 이벤트를 확인할 수 없어 이번 소규모에서는 한 도구로 HTTP/SSE 시각을 수집.
- 관람자10, 쓰기1req/s,30초, 성공 기대30건. 사용자ID에 run ID/mode/index 포함. 후보 고정.
- 관람자별 최초 후보 조회1회와 초기 집계 수신.5초 읽기 워밍업. 쓰기 워밍업 없음(첫 INSERT/Producer 초기 비용 포함).
- polling: HTTP keep-alive 재사용, 응답 완료 후2초 대기, 탭별 요청 중첩 없음.10개 클라이언트를 거의 동시에 시작한 동기화 조건이며 무작위 접속 분포가 아님.
- realtime: 실제 ranking SSE 파싱.60초 서버 타임아웃 뒤3초 대기 후 재접속. 별도 비측정 구간으로 최대70초 연장하여10명 재접속 최신 스냅샷10명 확인 완료. 브라우저 자체 구현 검증과는 구분.
- JSON 차이: polling 결과는 이름 포함/0표 후보 포함, SSE는 집계 후보 ID/표 수. 전송 바이트는 이 계약 차이를 포함.

## 지표와 판정
- HTTP 시도/성공/실패/실제 요청률, p50/p95/p99(nearest rank), 응답 body bytes. 헤더/TCP/TLS/브라우저 렌더링은 미포함.
- 요청 시작부터 관람자별 해당 후보 누적 목표치가 처음 보인 시각. SSE eventId가 없으므로 개별 이벤트와 대응한 처리 지연이라고 해석하지 않음.30투표×10관람자=300관측, 독립된300개 표본은 아님.
- 성공 투표와 직접 SQL 후보별 변화량/총수 일치. 모든 관람자가 최종값 관측. realtime은 Redis 동일성 및 Kafka 최종lag0도 확인.
- 측정 종료 후 최종 스냅샷 최초 관측까지 시간과 마지막 성공 응답 이후 전원 관측시간 별도 기록. 종료 전에 이미 관측되면 전자는0.
- pg_stat_database before/after(워밍업/검증 쿼리 포함), docker stats와 ps 약5초+명령시간 간격 샘플. DB row/block 수는 SQL 실행 횟수 아님. PG CPU/네트워크는 컨테이너 전체이고 다른 DB 트래픽도 포함.
- ps %CPU의 OS별 평균 산정, Docker CPU와의 단위 차이로 합산하지 않음. 생성기 RSS/CPU도 기록. 부하 발생기 포화 한계는 이번 크기로 판정 불가.
- 오류3건이면 추가 투표 중단. 초기 스냅샷25초 미수신/기준값 불일치/최종15초 미수렴은 실패. 최대100관람자/10req/s/120초로 예비 도구 자체 한도. 자동 포화 시험 없음.

## 순서와 중간 확인
1. prepare/build → polling run → pilot → SQL 정확성 → 전용 앱 stop.
2. 사용자에게 polling 결과를 보여주고 .env.loadtest APP_PROFILE=realtime 변경 확인을 받는다. 확인 전 realtime 앱/부하를 실행하지 않음.
3. 같은 run/pilot 명령으로 별도 realtime DB 실행. 기준1000표/Redis1000표 확인 후 같은30투표. lag/최종값/재접속 검증.
4. 두 결과와 한계 기록 후 대규모 조건 확정 논의. 실험 DB/원본 결과는 사용자 검토까지 보존. cleanup은 앱 종료+run ID 확인+DB 주석/컨테이너 라벨 검사 후 전용 자원만 삭제하는 오프라인 도구이며 서비스 기능이 아님.

실행 명령/전용 정리 도구는 load-tests/README.md 참조.

## 단계별 비교 후속 승인 — 아직 실행하지 않음

2026-10-19 사용자 요청으로 아래100명/10req/s까지의 단계별 실행을 인계했다. 최신 구체적 절차·워밍업·중단 기준은 LOAD_TEST_SCALE_HANDOFF.md가 우선한다. 수백~수천 및 다중 서버는 여전히 후속 제안이다.
- realtime30/30·DB/Redis/관람자 일치·최종lag0·재접속 확인 완료. 먼저 새 동일 시드로10명/1req/s/30초를 다시 실행해 수정된 호스트 Admin 관측기의 부하 중 lag/자원 수집을 검증할 것을 제안. 현재 원본은 자원 비교에 부족하며 재실행은 아직 하지 않음.
- 그 뒤 관람자10→50→100, 투표1req/s 고정, 각60초/3회 반복. polling 먼저 완료 후 realtime으로 변경하는 사용자 확인 절차 유지. 매회 새 동일 시드 DB/Redis 기준 사용.
- 다음 단계는 관람자100 고정, 투표1→5→10req/s, 각60초/3회. 조회 증가와 쓰기 증가를 한 번에 바꾸지 않음.
- 수백~수천 SSE 연결 단계는 현재 thread-per-viewer Python 도구로 진행하지 않음. 비동기 SSE 관측기 또는 SSE를 지원하는 검증된 도구를 먼저 준비하고 생성기 CPU/파일디스크립터/메모리 사용 확인.
- 다중 서버는 그다음 앱1→2, 총관람자/총쓰기 동일하게 분산. Kafka 파티션1이면 집계 Consumer는한 서버만 담당하므로 집계 처리량이2배라고 가정하지 않음.
- 앱 예산은 인스턴스당 heap512MiB로 시작. 비교를 강화하려면 별도 PostgreSQL 컨테이너2CPU/1GiB 사용을 논의(현재 공유 PG 한도를 바꾸지 않음). Kafka1CPU/1GiB, Redis0.5CPU/256MiB. 같은 PC 경쟁과 인프라 추가 비용을 별도 비교.
- 단계 중 HTTP 실패1% 이상, p95투표1초 초과, lag가 종료30초 내0으로 회복하지 않음, 최종 불일치, OOM/스왑 급증 시 다음 단계 중지. 이 값은 용량 보장이 아닌 제안 중단 기준.

## 최신 실행 상태 — 2026-10-19 16:08 KST
사용자 지시로 스왑/스케줄 지연을 경고로 변경하고50명 단계 진행. polling60초3회통과,대응realtime은사용자.env.loadtest-v50-r1 전환확인대기. 이후100명·투표5/10req/s미실행. 기존10명유효3회미완료는보존하며단순성능배수비교하지않음. 최신수치/원본은LOAD_TEST_SCALE_RESULT.md참조.

## 2026-10-19 16:19 KST 최신 상태
50명polling/realtime각3회통과후100명polling3회통과. .env.loadtest-v100-r1 사용자realtime전환확인대기. 아직100명realtime및100명5/10req/s는미실행. 새결과와과거환경은유휴인프라/시간대차이가있어용량배수로단정하지않음.

## 2026-10-19 16:35 KST 최신 상태
100명 1req/s 양쪽 3회 통과 후, 100명 5req/s polling 3회 완료. 첫 반복의 스왑/발생기 지연은 경고로 보존. 다음은 .env.loadtest-v100-r5 사용자 realtime 전환 확인 후 같은 조건 3회. 10req/s는 아직 미실행.

## 2026-10-19 16:50 KST 최신 상태
100명5req/s 양쪽3회통과,100명10req/s polling3회통과. 다음은.env.loadtest-v100-r10 사용자realtime전환확인후같은조건3회. 이마지막실행이아직남아있으며더큰부하는범위밖.

## 2026-10-19 17:03 KST 실행 종료
마지막100명10req/s realtime3회 완료. 50명1req/s·100명1/5/10req/s 양쪽각3회 총24회 결과 정리 완료. 최초10명60초유효3회는 미완료로 남김. 더 큰 부하/다중서버/제품최적화는 진행하지 않는다. 기존서비스 유지,실험 앱/전용컨테이너 중지와데이터보존. 최종표와원본은LOAD_TEST_SCALE_RESULT.md 및load-tests/results/scale-comparison-summary.json 참조.
