# Timeline LevelDB Parser

로컬 YARN Timeline Rolling LevelDB 사본에서 완료 application의 Hive/Tez DAG 지표를 추출하는 Java 8 CLI입니다. DAG 하나를 Parquet 한 행으로 저장하며, 실행할 때마다 입력 전체를 다시 계산해 `<output>/result.parquet`를 교체합니다.

## 빌드와 실행

JDK 8을 사용합니다. Maven Wrapper가 Maven 3.9.16을 내려받으므로 최초 빌드에는 네트워크가 필요합니다.

```sh
export JAVA_HOME=/path/to/jdk8
export PATH="$JAVA_HOME/bin:$PATH"
java -version
./mvnw clean verify

java -jar target/timeline-parser.jar \
  --input /data/timeline-copy/2026-09-08 \
  --output /data/parquet/2026-09-08
```

`leveldbjni` 1.8의 native library를 사용하므로 JVM과 native library의 CPU 아키텍처가 맞아야 합니다. 실행 대상은 Linux x86_64 및 macOS x86_64입니다. 현재 개발 검증은 Apple Silicon macOS에서 Rosetta를 통한 x86_64 Java 8로 수행했습니다. Linux 운영 환경과 Java 8보다 높은 JVM은 별도로 검증해야 합니다. ARM64 JVM용 LevelDB JNI로 자동 전환하지 않습니다.

릴리스 JAR는 [GitHub Releases](https://github.com/jonggeun2001/yarn-timeline-leveldb-parser/releases)에서 받을 수 있습니다. `main`·`release`의 push와 PR은 CI 검증만 수행하며, `0.0.1` 같은 버전 태그를 push할 때만 의존성이 포함된 `timeline-parser-<version>-all.jar`와 SHA-256을 게시합니다. [CI와 릴리스 절차](docs/development/releases.md)를 참고하세요.

## 입력 준비

`--input`은 rolling entity DB 하나 또는 여러 `entity-ldb.*` DB를 포함하는 상위 디렉터리입니다. `CURRENT`, 해당 `MANIFEST`, SST와 필요한 WAL 등 DB 파일이 함께 있는 **일관된 로컬 사본**을 준비하고, 실행 중에는 변경하지 않아야 합니다. 개별 `.ldb`/`.sst` 파일, ATS JSON 로그, 원격 URL은 입력 형식에 포함되지 않습니다.

`indexes-ldb.*`와 `starttime-ldb`는 보조 인덱스이며 전체 entity 스캔에는 필요하지 않습니다. 입력과 출력은 같거나 서로의 하위 디렉터리일 수 없습니다.

프로젝트 안에 실제 DB 사본·결과·개인 설정을 보관하려면 Git에서 제외한 `local/` 아래를 사용하세요. 빌드 산출물, IDE·Serena 설정, LevelDB·Parquet 파일, 로그와 자격 증명 파일도 [.gitignore](.gitignore)에서 제외합니다. Maven Wrapper와 소스·테스트·설계 문서는 저장소에 포함합니다.

프로그램은 DB를 출력 디렉터리 아래 `.work-*`에 복사한 후 작업 사본을 엽니다. 원본 DB에 `put`, `delete`, `repair`를 수행하지 않습니다. 복사는 이미 불일치한 DB 사본을 복구하지 않으므로, 파일 누락·손상·해독 오류가 발생하면 실행을 실패시킵니다.

## 완료 application 지정

기본적으로 입력의 `YARN_APPLICATION_FINISHED` 또는 terminal state update 이벤트를 완료 근거로 사용합니다. `DAG_FINISHED`만으로 application 완료를 판단하지 않습니다. DAG가 있지만 일치하는 application 완료 근거가 하나도 없으면 완료 ID 목록을 요구하는 오류가 발생합니다.

외부에서 완료를 확인한 application만 처리하려면 UTF-8 파일에 ID를 한 줄씩 적습니다.

```text
application_1700000000000_0001
application_1700000000000_0002
```

```sh
java -jar target/timeline-parser.jar \
  --input /data/timeline-copy/2026-09-08 \
  --output /data/parquet/2026-09-08 \
  --completed-applications completed-applications.txt
```

이 파일을 지정하면 해당 목록이 기본 이벤트 판별을 대체합니다. 빈 목록은 명시적인 0행 결과를 뜻합니다. 일부 application만 완료 근거가 있으면 나머지는 제외하며, 제외 DAG 수를 콘솔에 표시합니다. 목록의 ID가 실제로 완료되었는지는 사용자가 확인해야 하며, 프로그램이 운영 YARN API를 조회하지 않습니다.

명시적으로 `callerType`이 Hive가 아닌 DAG는 제외합니다. `callerType` 자체가 없으면 DAG를 보존하고 `hiveQueryId=null`로 기록합니다.

## SELECT·CTAS 결과 행 수

기본 `resultRows`는 null입니다. 최종 SELECT 또는 CTAS sink와 정확한 FileSink 카운터의 관계를 확인한 DAG에 대해서만 JSON 매핑을 제공합니다.

```json
{
  "dag_1700000000000_0001_1": {
    "kind": "SELECT_RESULT",
    "counterGroup": "HIVE",
    "counterName": "RECORDS_OUT_0"
  }
}
```

```sh
java -jar target/timeline-parser.jar \
  --input /data/timeline-copy/2026-09-08 \
  --output /data/parquet/2026-09-08 \
  --completed-applications completed-applications.txt \
  --result-rows-mapping result-rows-mapping.json
```

위 `HIVE`/`RECORDS_OUT_0`는 형식 예시입니다. 실제 DAG에서 검증한 그룹·카운터 이름으로 바꾸어야 합니다. `kind`는 `SELECT_RESULT` 또는 `CTAS_WRITE`, `counterName`은 정확한 `RECORDS_OUT_<destinationId>[_<tableName>]` 이름입니다. DAG 상태가 `SUCCEEDED`이고 매핑한 카운터의 값이 0 이상일 때만 기록합니다. 누락된 값을 0으로 만들거나 전체 `OUTPUT_RECORDS`를 합산하지 않습니다.

## 출력과 재실행

출력은 Snappy 압축, 128 MiB row group 목표 크기의 단일 Parquet 파일입니다. 시각은 UTC millisecond timestamp, 숫자 지표는 nullable INT64입니다. [20개 컬럼과 지표 의미](docs/requirements/timeline-leveldb-parser.md#5-parquet-스키마)를 참고하세요.

```text
/data/parquet/2026-09-08/
  result.parquet
  .timeline-parser.lock
```

같은 출력 디렉터리의 동시 실행은 거부합니다. 임시 파일을 닫고 footer·스키마·행 수를 확인한 뒤 동일 파일시스템의 원자적 이동으로 결과를 교체합니다. 교체 전 실패하면 이전 결과를 보존하며, 원자적 교체를 지원하지 않는 파일시스템에서는 실패합니다. 대상 DAG가 없는 정상 입력은 스키마가 있는 0행 파일을 생성합니다.

입력 범위를 줄여 재실행하면 결과도 해당 범위로 전량 교체됩니다. 결과를 누적하거나 처리 이력을 저장하지 않으며, 스케줄러도 포함하지 않습니다. 중단으로 남은 앱 전용 임시 파일과 소유 marker가 있는 작업 사본은 다음 실행이 잠금을 얻은 뒤 정리합니다. `.timeline-parser.lock` 파일은 실행 후에도 유지되며 파일이 있다는 사실만으로 실행 중이라는 뜻은 아닙니다.

중단 후 재실행하면 잠금을 얻은 뒤 `.result-<UUID>.tmp`와 소유 marker가 있는 `.work-<UUID>`만 정리합니다. 다른 파일·디렉터리와 출력 최상위의 심볼릭링크는 보존합니다. 작업 디렉터리 안의 링크도 대상 경로를 따라가지 않습니다. marker 생성 전에 중단되어 소유 표시가 없는 작업 디렉터리는 자동으로 지우지 않습니다.

| 종료 코드 | 의미 |
| --- | --- |
| `0` | 결과 확정 성공 |
| `1` | 입력·해독·출력 처리 오류 |
| `2` | 명령행 인자 오류 |
| `3` | 같은 출력 디렉터리의 잠금 경합 |

## 검증 범위

Java 8에서 자동 테스트 68개가 통과했습니다. [검증 환경과 결과](docs/development/validation.md)에 상세 내용을 기록했습니다.

Apache Hadoop 3.1.1의 실제 RollingLevelDBTimelineStore writer가 만든 fixture와 Java 8 native DB 읽기, Parquet 왕복 및 출력 잠금을 검증 대상으로 사용합니다. **실제 CDP 7.1.7 LevelDB 사본은 아직 제공되지 않았습니다.** CDP 패치에 따른 저장 레이아웃·FST 설정, application 완료 근거, SELECT/CTAS 매핑은 운영 사본으로 확인해야 합니다.

일반 LeveldbTimelineStore의 다른 저장 레이아웃과 실행 중 DB 수집은 지원 범위에 포함하지 않습니다. DAG 없이 FETCH task로 끝난 쿼리는 이 데이터셋에 나타나지 않습니다. 1만 DAG의 작은 합성 fixture는 별도 Java 8 프로세스와 `-Xmx512m`으로 변환·재실행을 검증합니다. 큰 DAG plan/counter를 포함한 하루 단위 처리 시간·메모리·작업 디스크 사용량은 실제 입력 규모로 측정해야 합니다.

[문서 목록](docs/README.md) · [요구사항](docs/requirements/timeline-leveldb-parser.md) · [아키텍처](docs/architecture/leveldb-to-parquet.md) · [구현 계획](docs/development/implementation-plan.md)

프로젝트 라이선스는 [Apache License 2.0](LICENSE)입니다. 재사용한 코드와 의존성의 고지는 [THIRD-PARTY-NOTICES.md](THIRD-PARTY-NOTICES.md)를 참고하세요.
