# 작은 투표 화면 검증 결과

기록: 2026-10-16 16:52 KST. 사용자 추가 실험 중단 요청 반영.

## 구현 및 실행
- src/main/resources/static/index.html, styles.css, app.js. 같은 Spring Boot가 /에서 화면 제공. 별도 프론트 서버/빌드 없음.
- GET /api/mode 응답 {"mode":"polling"} 또는 {"mode":"realtime"}, 환경정보 미노출.
- 프로젝트 루트 .env에서 APP_PROFILE=realtime 또는 APP_PROFILE=polling. 미설정 기본 realtime. 제어 API 사용 시 APP_PROFILE=local,realtime. IntelliJ의 별도 Active profiles는 비워 두어야 .env 선택이 적용됨.
- IntelliJ에서 기존 앱 실행 후 http://localhost:8080/ 접속. 포트8081이면 http://localhost:8081/. 실시간은 기존 Kafka/Redis 실행 필요.
- CLI 실행: `./gradlew bootRun`
- JAR 실행: `java -jar build/libs/live-lunch-choice-0.0.1-SNAPSHOT.jar`
- polling은 완료 후2초 간격 DB 조회, realtime은 SSE 전체 스냅샷 교체. 사용자 ID는 실험용이며 로그인 아님. polling 저장분 자동 Redis backfill 없음.

## 실행한 테스트
- 화면 구현: `./gradlew test redisTest bootJar` 성공. 일반72/실제통합9건 실패·오류·건너뜀0. 모드 API4건, 두 모드 실제 HTTP에서 정적 화면/모드 응답 확인 포함.
- 환경변수 선택 후: `./gradlew test bootJar` 성공. 일반76건 실패·오류·건너뜀0. 실제 메인 YAML 기반 기본 realtime/선택값 검증4건 추가. 이 후 실제통합9건 재실행 없음.
- `node --check src/main/resources/static/app.js`, `git diff --check` 통과.

## 실제 Chrome 검증
- 임시 Java 실행기 /tmp/LunchUiVerification.java: H2 메모리 DB 각각, polling18080/realtime18081, 임시 Testcontainers Kafka4.1.2/Redis8. 사용자 인프라·기존 데이터에 쓰기/초기화 없음.
- 초기 후보0개/투표 비활성/랭킹0 확인. 두 DB에 API로 검증 후보5개 등록(201). 이름·ID는 화면 하드코딩이 아닌 실제 응답 사용.
- 후보5개 전체0표: 각0표·0.0%, 총0. `<b>채식 & 샐러드</b>` 후보 이름은 HTML 요소가 되지 않고 문자 그대로 보임.
- polling 탭1에서 ui-polling-browser-01로 한식 투표: 성공 메시지 후 두 탭 모두 총1, 한식1표100.0%, 나머지0표. 동일 ID 재투표 시 중복 안내, 수치 유지.
- realtime 탭1에서 ui-realtime-browser-01로 일식 투표: 두 탭 모두 총1/일식1표100.0%, Redis 목록 밖 후보는0표로 뒤에 표시.
- 탭2에서 ui-realtime-browser-02로 중식 투표: radio Space/입력 Tab/버튼 Enter 사용. 두 탭 총2, 일식·중식 각각1표50.0%, 나머지0. Redis 동점 순서(일식ID3 다음 중식ID2) 보존.
- 데스크톱1440px 두 카드, 모바일390px 한 열 확인. documentElement.clientWidth=390/scrollWidth=390, 가로 넘침 없음. 모바일 전체 화면 투표/랭킹/상태 문구 확인.
- 브라우저 초기 error/warn 로그 없음 확인. 모든 이후 시점 콘솔의 지속 수집은 미실행.
- 임시 스크린샷: /tmp/lunch-ui-desktop.jpg, /tmp/lunch-ui-mobile.jpg. 서버 로그: /tmp/lunch-ui-server.log.
- 임시 앱 PID21012 정상 종료(exit0), 임시 컨테이너 정리. 첫 background 실행 시 지속되지 않아 foreground 세션으로 재실행한 준비 이력 있음.

## 미검증 및 범위
- 브라우저 EventSource 자동 재연결/연결 오류 복구/느린 요청 중 중복 클릭의 직접 관측은 미완료. 관련 구현은 있으나 검증 성공으로 주장하지 않음. 기존 통합테스트의 수동 HTTP SSE 재접속과 구분.
- Consumer를 잠시 중단해 투표 성공 직후 임의+1 부재를 관측하려던 도구 호출은 사용자 중단. 실행 여부/결과 미확정으로 이 항목 검증 완료 처리하지 않음. 대상은 폐기한 격리 앱뿐.
- 최종 환경변수 설정은 일반 테스트로 검증. 사용자 PostgreSQL에서 새 화면 투표,8080/8081 실제 화면 동시 실험은 미실행. 실제 브라우저 검증은18080/18081에서 수행.
- 모바일은 Chrome viewport이며 실제 모바일 기기/Safari/스크린리더 검증은 아님. 요청 지연 시 polling 미중첩은 구현 검토로 확인했으며 지연 주입 검증 미실행.
- 사용자가 직접 실행을 원해 추가 실험 중단. 로그인/후보관리/부하 테스트/장애 보상 미진행. 다음은 사용자 확인 후 추가 요청 대기.
