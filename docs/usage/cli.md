# CLI 사용법

## 옵션

| 옵션 | 설명 |
| --- | --- |
| `--input PATH` | 필수. 로컬 entity DB 또는 상위 디렉터리 |
| `--output PATH` | 필수. `result.parquet`를 저장할 로컬 디렉터리 |
| `--completed-applications FILE` | 완료를 확인한 application ID 목록 |
| `--result-rows-mapping FILE` | 최종 SELECT·CTAS 출력 카운터 매핑 |
| `--help`, `--version` | 도움말, 버전 출력 |

## 완료 application

기본적으로 `YARN_APPLICATION_FINISHED` 또는 application의 종료 상태 이벤트(`FINISHED`, `FAILED`, `KILLED`)를 확인합니다. DAG 종료만으로 application 완료를 판단하지 않습니다.

외부에서 완료를 확인했다면 UTF-8 파일에 ID를 한 줄씩 적습니다. 예: `local/completed.txt`.

```text
application_1700000000000_0001
application_1700000000000_0002
```

`--completed-applications`를 지정하면 이 목록이 기본 이벤트 판별을 대체합니다. 빈 목록은 0행 결과를 뜻합니다. 목록을 지정하지 않았고 대상 DAG의 완료 근거가 하나도 없으면 오류로 종료합니다.

## SELECT·CTAS 결과 행 수

`resultRows`의 기본값은 null입니다. 최종 출력 sink와 카운터의 관계를 확인한 DAG에 대해 매핑 파일을 작성합니다. 예: `local/rows.json`.

```json
{
  "dag_1700000000000_0001_1": {
    "kind": "SELECT_RESULT",
    "counterGroup": "HIVE",
    "counterName": "RECORDS_OUT_0"
  }
}
```

`kind`는 SELECT 결과라면 `SELECT_RESULT`, CTAS 테이블 출력이라면 `CTAS_WRITE`입니다. 위 그룹·카운터 이름은 예시이며 해당 DAG의 값으로 바꿉니다. 성공한 DAG의 매핑된 카운터가 0 이상일 때만 행 수를 기록합니다.

```sh
java -jar timeline-parser-0.0.1-all.jar \
  --input ./local/input --output ./local/output \
  --completed-applications ./local/completed.txt \
  --result-rows-mapping ./local/rows.json
```

## 재실행과 오류

매번 입력 전체를 다시 계산해 `result.parquet`를 원자적으로 교체합니다. 입력에서 빠진 DAG는 결과에서도 빠집니다. 대상 DAG가 없는 정상 입력은 0행 Parquet를 생성합니다.

같은 출력 경로의 동시 실행은 거부합니다. `.timeline-parser.lock`은 실행 후에도 남으며, 중단된 작업의 앱 전용 임시 파일은 다음 실행에서 정리합니다. 스케줄러와 처리 이력은 없습니다.

| 종료 코드 | 의미 |
| --- | --- |
| `0` | 결과 저장 성공 |
| `1` | DB 읽기·해독·출력 오류 |
| `2` | 인자 오류 |
| `3` | 출력 경로 잠금 경합 |

입력 준비는 [README](../../README.md#입력-준비), 컬럼 정의는 [출력 스키마](../reference/schema.md)를 참고하세요.
