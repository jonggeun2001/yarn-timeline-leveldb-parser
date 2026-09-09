# LevelDB → Parquet 아키텍처

Java 8 배치 프로그램으로, Hadoop `RollingLevelDBTimelineStore`의 로컬 entity DB 사본에서 완료 application의 Hive DAG 지표를 추출한다.
입력은 기간별 `entity-ldb*` DB이며, 출력은 DAG당 한 행의 `result.parquet`이다.

## 처리 구조

```mermaid
flowchart LR
    A[DB 탐색·검증] --> B[작업 사본]
    B --> C[엔티티 복원]
    C --> D[DAG 결합·지표 추출]
    D --> E[Parquet 검증·교체]
```

| 구성 | 주요 컴포넌트 | 책임 |
| --- | --- | --- |
| 실행 | `ParseCommand`, `BatchRunner` | 인자 해석, 처리 순서, 자원 관리 |
| 입력 보호 | `LevelDbCatalog`, `ManifestValidator`, `WorkingCopy` | DB 탐색·검증, 원본과 분리된 사본 생성 |
| 해독 | `LevelDbScanner`, `RollingKeyDecoder`, `FstValueDecoder` | 타입별 키 순회, binary key·FST 값 해독, 엔티티 복원 |
| 지표 | `DagCollector`, `MetricsExtractor`, `ResultRowsResolver`, `DagRecord` | DAG 결합, 필요한 카운터 추출, 출력 값 구성 |
| 출력 | `ParquetOutput`, `OutputTransaction` | 스키마 적용, 출력 잠금, 임시 파일 검증·교체 |

## 저장 형식과 원본 보존

Rolling 키는 entity type, 역순 시작 시각, entity ID, 컬럼과 부가 값으로 구성된다.
Hadoop의 키 인코딩 규칙으로 해독하며, `otherInfo`·event info·primary filter 값에는 FST를 사용한다.
FST 설정은 reference sharing을 끄고 구버전 `LinkedHashMap` class ID 83을 처리한다.

LevelDB JNI iterator가 SST·WAL·삭제 표식을 반영한 최신 유효 Key/Value를 제공한다.
스캐너는 필요한 entity type과 `otherInfo` 필드를 선택하고, 한 엔티티의 연속된 키를 모아 복원한다.
전체 entity DB를 순회하므로 `indexes-ldb`와 `starttime-ldb`는 사용하지 않는다.

입력은 실행 중 변경되지 않는 일관된 DB 사본이어야 한다. DB open의 recovery 쓰기를 원본에서 분리하기 위해 출력 경로 아래에 파일을 실제 복사한다.
hard link를 사용하지 않으며, 한 DB씩 복사·열기·스캔·닫기·정리한다. 애플리케이션은 DB에 put/delete/repair를 호출하지 않는다.
복사 전 원본과 복사 후 작업 DB에서 `CURRENT`, MANIFEST의 CRC·레코드 경계, 필수 WAL, live SST의 존재·크기를 검사한다.
MANIFEST에 아직 기록되지 않은 최신 WAL의 누락은 이 검사로 알아낼 수 없으므로, 입력 사본에 해당 파일도 모두 포함해야 한다.

## DAG 결합과 자원 사용

`TEZ_DAG_ID`와 `TEZ_DAG_EXTRA_INFO`를 DAG ID로 결합하고 application 식별자가 일치하는지 확인한다.
동일 값은 한 번만 유지하고 상충하는 값은 오류로 처리한다. 완료 application 범위를 적용한 뒤 application ID와 DAG ID 순서로 정렬한다.
해독 실패는 실행 오류이며, 선택 지표의 누락만 null로 기록한다. 지표 정의와 결과 행 수 규칙은 [출력 스키마](../reference/schema.md)를 따른다.

메모리에는 DAG별 기본 필드·시각·필요 카운터의 축약 상태를 유지한다. plan/counter 전체 객체는 현재 엔티티 처리 후 보관하지 않는다.
사용량은 DAG 수, 현재 역직렬화 값의 크기, 8 MiB DB 캐시와 Parquet 버퍼에 영향을 받는다.
임시 디스크에는 DB 사본 한 개, MANIFEST 검증용 재기록 파일, 임시 Parquet가 필요하다.

## 출력과 라이브러리 재사용

출력 디렉터리를 잠근 뒤 Snappy Parquet를 임시 파일에 쓰고 닫는다. footer·스키마를 검증한 뒤 원자적으로 `result.parquet`를 교체하며, 교체 전 실패하면 기존 결과를 보존한다.

LevelDB JNI는 DB 조회, iq80의 `LogReader`·`VersionEdit`·`FileChannelLogWriter`는 MANIFEST 검사에 사용한다. Hadoop 모델과 FST로 엔티티를 복원하고, Parquet·Avro로 출력하며, picocli와 Maven Wrapper·Shade로 실행과 패키징을 구성한다.
의존성 버전은 [pom.xml](../../pom.xml), 재사용 코드의 출처·라이선스는 [THIRD-PARTY-NOTICES.md](../../THIRD-PARTY-NOTICES.md), 실행 방법은 [CLI 사용법](../usage/cli.md)에 정리한다.
