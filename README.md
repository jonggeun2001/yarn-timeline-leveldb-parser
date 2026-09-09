# Timeline LevelDB Parser

YARN Timeline의 로컬 Rolling LevelDB 사본에서 Hive DAG 지표를 추출해 Parquet로 저장하는 Java 8 CLI입니다.

## 실행

Java 8, Linux/macOS x86_64 환경에서 [의존성 포함 JAR](https://github.com/jonggeun2001/yarn-timeline-leveldb-parser/releases)를 실행합니다.

```sh
java -jar timeline-parser-0.0.1-all.jar \
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
| `--result-rows-mapping FILE` | SELECT·CTAS 최종 출력 카운터를 지정한 JSON |
| `--help`, `--version` | 도움말, 버전 출력 |

기본적으로 application 종료 이벤트를 확인해 완료된 application만 처리합니다. `--completed-applications`는 이 판별을 목록으로 대체하며, 빈 목록은 0행 결과를 만듭니다. 목록 없이 대상 DAG의 application 완료 근거가 하나도 없으면 오류로 종료합니다.

`resultRows`는 기본적으로 null입니다. 최종 출력 sink의 카운터를 확인한 DAG만 다음 형식으로 매핑합니다.

```json
{
  "dag_1700000000000_0001_1": {
    "kind": "SELECT_RESULT",
    "counterGroup": "HIVE",
    "counterName": "RECORDS_OUT_0"
  }
}
```

CTAS는 `kind`를 `CTAS_WRITE`로 지정합니다. 그룹·카운터는 해당 DAG의 실제 값으로 바꾸며, 카운터 이름은 `RECORDS_OUT_`으로 시작해야 합니다. 성공한 DAG의 해당 카운터가 0 이상일 때만 행 수를 기록합니다.

[출력 컬럼](docs/reference/schema.md) · [빌드·릴리스](docs/development/releases.md)

[Apache License 2.0](LICENSE) · [오픈소스 고지](THIRD-PARTY-NOTICES.md)
