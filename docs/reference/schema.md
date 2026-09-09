# 출력 스키마

완료 application의 DAG당 한 행입니다. Hive 쿼리가 여러 DAG를 실행하면 여러 행이며, DAG 없는 FETCH 쿼리는 포함되지 않습니다.
`dagId`·`applicationId`만 필수입니다. 시각은 UTC 밀리초 `TIMESTAMP`, 숫자는 64비트 정수 `INT64`입니다.

| 필드 | 타입 | 의미 |
| --- | --- | --- |
| `dagId` | STRING | DAG 식별자 |
| `applicationId` | STRING | 소속 YARN application 식별자 |
| `user` | STRING | DAG 사용자 |
| `hiveQueryId` | STRING | Hive caller context의 쿼리 식별자 |
| `startTime` | TIMESTAMP | DAG 실행 시작 시각. 제출 시각과 구분 |
| `endTime` | TIMESTAMP | DAG 종료 시각 |
| `resultRows` | INT64 | 자동 선택한 유일 FileSink 카운터 값 또는 DAG별 override 값 |
| `cpuMilliseconds` | INT64 | 누적 CPU 시간, 밀리초 |
| `status` | STRING | DAG 상태. application 상태와 별개 |
| `queueName` | STRING | 실행 큐 |
| `durationMilliseconds` | INT64 | `endTime - startTime`, 밀리초 |
| `gcMilliseconds` | INT64 | 누적 GC 시간, 밀리초 |
| `hdfsBytesRead` | INT64 | HDFS 읽기 바이트 수 |
| `hdfsBytesWritten` | INT64 | HDFS 쓰기 바이트 수 |
| `shuffleBytes` | INT64 | 셔플 바이트 수 |
| `additionalSpillBytesWritten` | INT64 | 추가 spill 쓰기 바이트 수 |
| `totalTasks` | INT64 | 전체 태스크 수 (원본: `numCompletedTasks`) |
| `failedTaskAttempts` | INT64 | 실패한 태스크 시도 수 |
| `resultRowsKind` | STRING | 자동 추출: `FILE_SINK_OUTPUT`; override: `SELECT_RESULT` 또는 `CTAS_WRITE` |
| `resultRowsSource` | STRING | 결과 행 수의 근거 카운터 이름 |

- `startTime`·`endTime`은 `otherInfo`와 `DAG_STARTED`·`DAG_FINISHED` 이벤트에서 수집한 값 중 각각 가장 늦은 시각입니다. 서로 다른 값은 경고를 남기고 병합하며, 입력 순서는 결과에 영향을 주지 않습니다.
- 누락·음수 지표는 `null`이며, 실제 `0`과 구분합니다. 경과시간은 선택된 `endTime - startTime`으로 계산하며, 종료가 시작보다 빠르면 종료 시각·경과시간도 `null`입니다.
- CPU·GC는 DAG가 보고한 태스크 누적값으로, 실제 경과시간과 다릅니다.
- `callerType`이 Hive 이외로 명시되면 제외합니다. 값이 없으면 DAG를 포함하되 `hiveQueryId`는 `null`입니다.

`resultRows`는 `SUCCEEDED` DAG의 모든 그룹에서 이름이 `^RECORDS_OUT_[0-9]+(?:_.+)?$`에 일치하는 카운터를 찾습니다. 서로 다른 `(group, name)` 후보가 정확히 하나이면 0 이상인 원본 INT64 값을 그대로 기록하고, `resultRowsSource`에 카운터 이름을 넣습니다. 후보 없음·여러 후보(다른 그룹의 같은 이름 포함)·음수·비성공 DAG는 null입니다. 숫자 ID가 없는 `RECORDS_OUT_INTERMEDIATE`·`RECORDS_OUT_OPERATOR_*` 및 일반 `OUTPUT_RECORDS`는 제외합니다.

자동 값은 FileSink 카운터의 DAG 집계값으로, 임시 materialization 출력이나 같은 이름의 sink 합산일 수 있어 최종 SELECT/CTAS 커밋 행 수를 보장하지 않습니다.

`--result-rows-mapping`은 검증한 DAG별 `SELECT_RESULT`·`CTAS_WRITE` override입니다. 매핑한 DAG는 지정한 그룹·이름의 카운터만 사용하며, 없거나 음수이면 null을 유지하고 자동 선택으로 대체하지 않습니다.

실행 옵션과 결과 행 수 매핑은 [README](../../README.md)를 참고하세요.
