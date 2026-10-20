# 부하 비교 도구

현재 실행 도구는 [capacity/README.md](capacity/README.md)를 참고한다. DB·SSE와 Kafka·Redis·SSE를 비교하며 기존 polling 비교와 별개다.

사용자 요청에 따라 이전 polling/realtime 예비·단계별 비교 실행 도구(`manage.py`, `pilot.py`, `scale.py`, `KafkaLag.java`)와 전용 설정·시드를 삭제했다. 서비스의 polling 프로필과 화면 기능은 유지한다.

## 보존한 기록

- [예비 비교 결과](../dev-notes/LOAD_TEST_PILOT_RESULT.md)
- [단계별 비교 결과](../dev-notes/LOAD_TEST_SCALE_RESULT.md)
- [현재 용량 비교 결과](../dev-notes/LOAD_TEST_CAPACITY_RESULT.md)
- `results/`의 측정 원본과 당시 코드 스냅샷, `state/`의 실험 이력은 보존한다. 과거 스냅샷은 실행용 도구가 아니라 기록이다.

과거 결과 문서의 삭제된 스크립트 실행 명령은 당시 이력이며 현재 실행 방법이 아니다. 이번 정리는 파일에 한정하며 DB·컨테이너·볼륨·실제 서비스 환경 설정은 변경하지 않았다.
