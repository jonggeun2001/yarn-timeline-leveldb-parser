# 구현 검증 결과

검증일: 2026-09-09

## 환경과 명령

- Apple Silicon macOS, Rosetta의 Azul Zulu OpenJDK 8u504 x86_64.
- Maven Wrapper 3.9.16, Hadoop 3.1.1 실제 `RollingLevelDBTimelineStore` writer fixture.
- `JAVA_HOME`을 위 Java 8로 설정한 뒤 `./mvnw clean verify` 실행.
- 애플리케이션과 번들 의존성의 기본 class 파일은 Java 8 bytecode 범위(major ≤ 52). Multi-release JAR의 상위 JVM 전용 경로는 이 검사에서 제외한다.

## 자동 검증

66개 단위·컴포넌트 테스트와 2개 배포 JAR 통합 테스트: 총 **68개, 실패 0, 오류 0, 건너뜀 0**.

| 영역 | 주요 확인 사항 |
| --- | --- |
| 실제 LevelDB 읽기 | 역순 시각 키, 한글, event/filter/otherInfo, 역관계, 원본 파일 불변 |
| DB 일관성 점검 | 필수 WAL, live SST 존재·크기, MANIFEST CRC·본문·부분 header 손상, 32 KiB를 넘는 분할 MANIFEST |
| Native 엔진 의미 | compact된 SST 위 WAL 업데이트와 삭제 표식 반영 |
| 값 해독 | FST LinkedHashMap 구버전 등록, 손상 값 오류, 불필요한 configuration 값 해독 생략 |
| 지표 | DAG/EXTRA_INFO 결합, 중복 제거·충돌 거부, 완료 범위, non-Hive 제외, null·0·INT64, SELECT/CTAS 명시 매핑 |
| Parquet | 20개 컬럼, UTC millisecond, 한글, null, INT64 최댓값, Snappy, footer·행 수 검증 |
| 출력 보호 | 확정 전 실패 보존, 원자 교체 실패 보존, JVM·별도 프로세스 잠금, 소유 표시가 있는 잔여 작업 정리 |
| CLI | Maven 버전 표시, 인자 오류, 완료 ID 정규화·범위, 경로 중첩 거부, 0행 교체, SELECT/CTAS 전체 변환 |
| 배포 JAR | CLI·manifest 버전 일치, 별도 Java 8 프로세스, 1만 DAG 변환과 재실행, 원본 SHA-256 일치, 실패 시 결과 바이트 보존 |

리뷰에서 발견한 WAL 누락, monolithic 감지, JNI 값 중복 복사, 시각·상태 충돌, non-Hive 범위, null application eventInfo, 완료 ID 오버플로, 고아 임시 데이터 정리를 수정하고 회귀 검증했다.

## 합성 처리량

배포 JAR 테스트는 application 하나에 작은 DAG 10,000개를 넣는다. 읽은 Key/Value는 90,003개, key/value 합계는 5,999,153바이트다. 별도 프로세스에 `-Xmx512m`을 지정했다. 확인한 실행에서 내부 측정 시간은 최초 2.564초, 재실행 2.482초였고 출력은 100,055바이트였다. 프로세스 기동 시간은 내부 측정값에 포함하지 않는다.

이 fixture에는 큰 DAG plan과 대량 카운터가 없다. 512 MiB는 **최대 Java 힙 설정**이며 RSS 또는 실제 사용 힙의 측정값이 아니다. 운영 처리량 보장이나 1만 Hive 쿼리 전체의 대표 benchmark로 해석하지 않는다.

## 배포 전 실제 환경 확인

실제 CDP 7.1.7 사본은 제공되지 않아 CDP 패치의 레이아웃·FST 설정, SELECT/CTAS sink 매핑, 완료 메타정보를 대조하지 못했다. Linux 운영 호스트에서 native library 실행, 실제 일일 데이터의 시간·힙/RSS·작업 디스크 사용량도 확인해야 한다.

MANIFEST 검사와 native open 성공만으로 일관된 수집을 증명할 수 없다. 특히 MANIFEST보다 새 WAL이 누락된 경우는 남은 파일만으로 알 수 없다. 실행 중인 운영 DB 수집은 앱 범위에 포함하지 않는다.
