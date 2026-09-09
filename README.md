# Timeline LevelDB Parser

YARN Timeline의 로컬 Rolling LevelDB 사본에서 Hive DAG 지표를 추출해 Parquet로 저장하는 Java 8 CLI입니다.

## 실행

Java 8, Linux/macOS x86_64 환경에서 [의존성 포함 JAR](https://github.com/jonggeun2001/yarn-timeline-leveldb-parser/releases)를 실행합니다.

```sh
java -jar timeline-parser-0.0.2-all.jar \
  --input ./local/input \
  --output ./local/output
```

결과는 `<output>/result.parquet`이며 DAG 하나당 한 행입니다. 재실행하면 입력 전체로 결과를 교체합니다. 교체 전 실패하면 기존 파일을 보존하고, 같은 출력 경로의 동시 실행은 거부합니다. 처리 대상이 없는 정상 입력은 0행 파일을 만듭니다.

- 입력: `entity-ldb.*` DB 하나 또는 여러 DB를 담은 상위 디렉터리.
- 운영 DB 대신 `CURRENT`, `MANIFEST`, SST, 필요한 WAL을 갖춘 **일관된 사본**을 사용하고 실행 중에는 변경하지 않습니다. `indexes-ldb`, `starttime-ldb`는 필요 없습니다.
- 입력·출력 경로는 서로 겹치지 않아야 합니다. 예제의 `local/`은 Git에서 제외됩니다.

## 선택 옵션

| 옵션 | 용도 |
| --- | --- |
| `--completed-applications FILE` | 직접 완료를 확인한 application ID 목록. UTF-8, 한 줄에 하나 |
| `--result-rows-mapping FILE` | DAG별 결과 행 수 카운터를 우선 지정하는 선택적 JSON override |
| `--help`, `--version` | 도움말, 버전 출력 |

기본적으로 application 종료 이벤트를 확인해 완료된 application만 처리합니다. `--completed-applications`는 이 판별을 목록으로 대체하며, 빈 목록은 0행 결과를 만듭니다. 목록 없이 대상 DAG의 application 완료 근거가 하나도 없으면 오류로 종료합니다.

`resultRows`는 `SUCCEEDED` DAG의 Hive SQL을 분석해 SELECT는 `RECORDS_OUT_0` 계열, CTAS·단일 INSERT INTO·INSERT OVERWRITE(TABLE/DIRECTORY)는 `RECORDS_OUT_1` 계열에서 고릅니다. 해당 번호의 카운터가 하나이고 0 이상일 때 기록하며, 여러 출력 대상·지원하지 않는 SQL·누락·모호한 후보는 null입니다. SQL이 없으면 기존처럼 유일한 숫자 FileSink 카운터를 사용합니다.

`resultRowsKind`는 `FILE_SINK_OUTPUT`, `resultRowsSource`는 원본 카운터 이름입니다. 값은 DAG별 출력 카운터이며, 중간 출력이나 같은 이름의 sink 합산을 포함할 수 있어 쿼리 전체의 최종 결과·테이블 커밋 행 수를 보장하지 않습니다.

특정 DAG의 최종 SELECT·CTAS sink를 검증했다면 다음 JSON을 `--result-rows-mapping`으로 지정해 자동 선택을 덮어쓸 수 있습니다.

```json
{
  "dag_1700000000000_0001_1": {
    "kind": "SELECT_RESULT",
    "counterGroup": "HIVE",
    "counterName": "RECORDS_OUT_0"
  }
}
```

CTAS는 `kind`를 `CTAS_WRITE`로 지정합니다. 그룹·카운터는 해당 DAG의 검증된 값으로 바꾸며, 이름은 `RECORDS_OUT_`으로 시작해야 합니다. 매핑한 DAG는 지정한 카운터만 사용하며, 없거나 값이 유효하지 않으면 null입니다. 매핑하지 않은 DAG에는 자동 추출을 적용합니다. 카운터 구조가 손상되어 후보 전체를 확인할 수 없으면 해당 DAG의 자동 추출을 중단하고 null을 유지합니다.

## 오류 처리와 로그

같은 DAG의 정상 `startTime`·`endTime`이 충돌하면 입력 순서와 관계없이 가장 늦은 시각을 선택하고 경고를 남깁니다. `otherInfo` 간, `DAG_STARTED`·`DAG_FINISHED` 이벤트 간, 두 출처를 병합할 때 모두 적용하며, 같은 값이나 `null`은 경고를 남기지 않습니다. 로그의 시각 값은 UTC epoch 밀리초입니다.

형식이 잘못된 선택 필드·카운터와 시각 외 값의 충돌은 경고 후 해당 값을 null로 처리합니다. 무효화된 값은 나중에 정상값이 들어와도 복구하지 않습니다. 잘못된 DAG ID나 소속 application ID 불일치는 DAG 전체를 제외합니다. 개별 레코드·FST 역직렬화 오류도 해당 레코드·엔터티를 건너뛰고 나머지를 처리합니다.

DB 자체의 검증·읽기에 실패하면 다른 정상 DB는 계속 스캔하지만 최종 결과를 교체하지 않습니다. 엔터티를 폐기한 뒤 결과가 0행인 경우에도 기존 출력을 보존합니다. 처리 대상이 없는 정상 입력은 기존처럼 0행 파일을 만듭니다. 결과 교체 후 임시 파일 정리에 실패하면 경고를 남기고 게시한 결과를 유지합니다.

| 종료 코드 | 의미 |
| --- | --- |
| `0` | 정상 종료. 결과에 경고나 null 값이 있을 수 있음 |
| `1` | DB 실패, 완료 근거 없음, 출력 I/O 오류 등으로 안전 중단. 기존 결과 보존 |
| `2` | CLI 경로·필수 인자, 완료 목록 또는 결과 행 수 매핑 입력 오류 |
| `3` | 출력 경로 잠금 획득 실패 |

CLI 진행상황은 표준 출력으로, WARN·ERROR는 SLF4J와 `slf4j-simple`을 통해 기본적으로 표준 오류로 기록합니다. 로그에는 시각·로거 이름과 추적 가능한 DB·DAG·필드 정보를 포함하며, 예외가 있는 경우 원인 예외도 기록합니다. 파일 저장 등 로그 설정은 [빌드·릴리스](docs/development/releases.md)를 참고하세요.

[출력 컬럼](docs/reference/schema.md) · [빌드·릴리스](docs/development/releases.md)

[Apache License 2.0](LICENSE) · [오픈소스 고지](THIRD-PARTY-NOTICES.md)
