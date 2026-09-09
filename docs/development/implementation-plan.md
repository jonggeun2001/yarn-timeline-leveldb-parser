# Timeline LevelDB 파서 구현 계획

> **For agentic workers:** Use test-driven development and independent component review. Keep all documents under topic directories in `docs/`.

**Goal:** Java 8에서 로컬 Rolling LevelDB의 Hive DAG 지표를 추출해 멱등적으로 Parquet를 생성하는 CLI를 구현한다.

**Architecture:** Native LevelDB iterator와 Hadoop 키/FST codec으로 엔티티를 복원한다. 내부 DAG 모델로 결합·완료 범위를 적용한 뒤 Avro Parquet를 임시 파일에 쓰고 원자적으로 교체한다.

**Tech Stack:** Java 8, Maven Wrapper, picocli, leveldbjni, FST, Hadoop TimelineEntity, parquet-avro, JUnit 5.

**Spec:** [요구사항](../requirements/timeline-leveldb-parser.md), [아키텍처](../architecture/leveldb-to-parquet.md)

## 공통 제약

- Java 8 실제 프로세스로 native DB와 Parquet를 검증한다.
- 입력 DB는 작업 사본만 open한다. 손상 입력은 자동 repair하지 않는다.
- 같은 출력 디렉터리의 `result.parquet`를 전량 교체하며 처리 이력을 저장하지 않는다.
- 결과 행 수를 검증할 근거가 없으면 null을 저장한다.
- 완료 application은 YARN_APPLICATION_FINISHED 이벤트 또는 사용자가 명시한 완료 ID 입력으로 한정한다. DAG_FINISHED만으로 application 완료를 판단하지 않는다.
- Git 저장소가 없는 기존 프로젝트 폴더에서 구현한다. 문서는 날짜·도구명 디렉터리 대신 주제별로 배치한다.

## 1. Java 8 빌드와 native fixture

파일: `pom.xml`, `.mvn/wrapper/`, `mvnw`, `.gitignore`, `src/test/java/.../fixture/RollingStoreFixture.java`.

- [x] Maven/Java 8 검증 도구 준비 및 의존성 버전 고정.
- [x] 실제 Hadoop RollingLevelDBTimelineStore가 작성한 테스트 DB 준비.
- [x] native DB를 열고 테스트할 수 있는 빌드 구성 확인.

## 2. LevelDB 입력과 codec

파일: `input/LevelDbCatalog.java`, `input/WorkingCopy.java`, `input/LevelDbScanner.java`, `compat/RollingKeyDecoder.java`, `compat/FstValueDecoder.java`, 해당 테스트.

계약: `List<Path> discover(Path input)`, `scan(Path database, Consumer<TimelineEntity> consumer)`.

- [x] 키의 역순 시각, event/filter/otherInfo와 원본 보존을 검증하는 실패 테스트 작성·실행.
- [x] Hadoop key helper 규칙과 FST 2.50/2.24 호환 설정 적용.
- [x] DB 순회와 엔티티 복원 구현 후 fixture와 대조.
- [x] WAL 최신 값·삭제 표식·누락 파일·손상 입력 테스트.

## 3. DAG 결합과 지표

파일: `domain/DagRecord.java`, `metrics/DagCollector.java`, `metrics/MetricsExtractor.java`, `metrics/ResultRowsResolver.java`, 해당 테스트.

계약: `accept(TimelineEntity entity)`, `List<DagRecord> finish(Set<String> completedIds)`; `DagRecord`는 출력 스키마의 불변 필드 맵을 제공한다.

- [x] DAG 기본/상세 정보 분리, 중복·충돌, application 종료, 카운터 null/0을 검증하는 실패 테스트 작성·실행.
- [x] Tez 0.9.1의 정확한 key/group 기준으로 필드 추출 구현.
- [x] SELECT/CTAS final sink의 명시적으로 검증된 매핑 입력이 있을 때만 resultRows를 채운다. 기본값은 null.
- [x] 순서 무관 결과와 같은 DAG 중복 부재를 검증한다.

## 4. Parquet와 원자적 결과 확정

파일: `output/ParquetOutput.java`, `output/OutputTransaction.java`, `src/main/resources/dag-metrics.avsc`, 해당 테스트.

계약: `write(Path file, List<DagRecord> rows)`, `OutputTransaction.open(Path output)`, `temporaryFile()`, `commit()`.

- [x] null/시각/INT64 왕복과 실패 시 이전 결과 보존 테스트를 먼저 실행한다.
- [x] Snappy Parquet, 명시적 스키마·메타데이터·footer 검증 구현.
- [x] 파일 잠금과 동일 파일시스템 원자적 교체 구현.
- [x] 중복 실행·중단 후 재실행·0행 결과 검증.

## 5. CLI 통합과 배포 검증

파일: `Main.java`, `cli/ParseCommand.java`, `application/BatchRunner.java`, CLI 통합 테스트, `README.md`.

- [x] `--input`, `--output`, 선택적 완료 application ID/행 수 매핑 입력과 종료 코드를 테스트한다.
- [x] 전체 파이프라인 연결, 요약 출력, 오류 문맥과 임시 데이터 정리 구현.
- [x] 같은 입력 재실행·입력 제거 시 결과 교체·원본 불변을 실제 DB와 별도 CLI 프로세스로 확인한다.
- [x] Java 8에서 `./mvnw clean verify`, 배포 JAR 변환, Parquet 읽기 검증을 수행한다.
- [x] 독립 리뷰 결과를 반영하고 사용자용 빌드·사용법·검증 한계를 문서화한다.

검증용 예시는 `assertEquals(120L, row.get("cpuMilliseconds"))`, `assertNull(row.get("resultRows"))`, `assertEquals(beforeHashes, afterHashes)`처럼 코드가 계산한 값을 기대값으로 재사용하지 않는 방식으로 작성한다. 실제 CDP 사본은 제공되지 않았으므로 upstream writer fixture 검증과 구분해 보고한다.

## 구현 결과

단위·컴포넌트 테스트 65개와 배포 JAR 통합 테스트 1개를 Java 8에서 검증했다. 세부 환경과 미검증 범위는 [검증 결과](validation.md)를 따른다. 실제 CDP 사본 및 운영 Linux에서의 검증은 입력과 실행 환경 확보 후 수행한다.
