# 프로젝트 문서

| 문서 | 내용 |
| --- | --- |
| [요구사항과 데이터 계약](requirements/timeline-leveldb-parser.md) | 처리 범위, LevelDB 입력 조건, Parquet 스키마, 지표 의미, 멱등성과 오류 처리 |
| [아키텍처와 기술 스택](architecture/leveldb-to-parquet.md) | Java 8 의존성, LevelDB/FST 해독, 컴포넌트 구조, 메모리와 검증 기준 |
| [구현 계획](development/implementation-plan.md) | 구현 단계와 검증 체크리스트 |
| [검증 결과](development/validation.md) | Java 8 테스트, 합성 데이터 처리량과 실제 CDP 검증 범위 |
| [CI와 버전 릴리스](development/releases.md) | 브랜치 CI, 태그 전용 JAR 게시와 다음 버전 배포 절차 |

문서는 주제에 따라 `requirements/`, `architecture/`, `development/` 아래에 관리한다. 작성일은 문서 본문에 기록한다.
