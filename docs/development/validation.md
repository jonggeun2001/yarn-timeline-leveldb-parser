# 검증 결과

Java 8에서 테스트 **68개**(단위·컴포넌트 66개, 배포 JAR 2개)가 통과했습니다.

| 환경 | 확인 |
| --- | --- |
| Ubuntu 22.04 x86_64 / Zulu Java 8 | [CI 실행](https://github.com/jonggeun2001/yarn-timeline-leveldb-parser/actions/runs/34319208031) |
| macOS / Rosetta x86_64 Java 8 | 로컬 `./mvnw clean verify` |

## 검증 범위

- Hadoop writer가 만든 LevelDB의 키·FST 복원, SST/WAL 업데이트·삭제 반영
- 파일 누락·손상 거부, 원본 파일 불변, DAG 중복·충돌 처리
- 완료 application 판별, SELECT·CTAS 매핑, null·0·INT64 처리
- Parquet 왕복, 잠금 경합, 원자적 교체와 실패 시 기존 결과 보존
- 배포 JAR의 의존성 포함, 버전 일치, SHA-256 확인

## 합성 데이터 측정

Linux CI에서 작은 DAG 10,000개(90,003개 Key/Value, 약 6 MB)를 약 **2.1초**에 변환했습니다. 최대 Java 힙은 `-Xmx512m`이며 프로세스 기동 시간은 측정에서 제외했습니다.

큰 DAG plan과 대량 카운터를 포함하지 않은 합성 데이터 결과입니다. 처리 시간과 메모리 사용량은 입력 규모에 따라 달라집니다.
