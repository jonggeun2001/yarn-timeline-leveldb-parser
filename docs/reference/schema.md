# 출력 스키마

`result.parquet`는 완료된 application의 DAG 하나를 한 행으로 저장합니다.
Hive 쿼리 하나가 여러 DAG를 실행하면 여러 행이 됩니다. DAG 없이 FETCH task로 끝난 쿼리는 포함되지 않습니다.

`callerType`이 Hive 이외로 명시된 DAG는 제외합니다. 이 값이 없으면 DAG를 포함하되 `hiveQueryId`는 null로 둡니다.

`dagId`와 `applicationId`는 필수이며, 나머지 필드는 모두 `null`을 허용합니다.
시각은 UTC 기준 밀리초 정밀도의 `TIMESTAMP`, 숫자는 64비트 정수 `INT64`입니다.

| 필드 | 타입 | 의미 |
| --- | --- | --- |
| `dagId` | STRING | DAG 식별자 |
| `applicationId` | STRING | DAG가 속한 YARN application 식별자 |
| `user` | STRING | DAG에 기록된 사용자 |
| `hiveQueryId` | STRING | Hive caller context에서 확인한 쿼리 식별자 |
| `startTime` | TIMESTAMP | DAG 실행 시작 시각. 제출 시각과 구분 |
| `endTime` | TIMESTAMP | DAG 실행 종료 시각 |
| `resultRows` | INT64 | 검증된 최종 SELECT 결과 또는 CTAS 대상 출력 행 수 |
| `cpuMilliseconds` | INT64 | DAG의 `CPU_MILLISECONDS` 카운터 |
| `status` | STRING | DAG 상태. application 상태와 별개 |
| `queueName` | STRING | DAG에 기록된 큐 이름 |
| `durationMilliseconds` | INT64 | `endTime - startTime`, 밀리초 |
| `gcMilliseconds` | INT64 | DAG의 `GC_TIME_MILLIS` 카운터 |
| `hdfsBytesRead` | INT64 | HDFS에서 읽은 바이트 수 (`HDFS_BYTES_READ`) |
| `hdfsBytesWritten` | INT64 | HDFS에 쓴 바이트 수 (`HDFS_BYTES_WRITTEN`) |
| `shuffleBytes` | INT64 | 셔플 바이트 수 (`SHUFFLE_BYTES`) |
| `additionalSpillBytesWritten` | INT64 | 추가 spill 쓰기 바이트 수 (`ADDITIONAL_SPILLS_BYTES_WRITTEN`) |
| `totalTasks` | INT64 | DAG 전체 태스크 수 (`numCompletedTasks` 필드) |
| `failedTaskAttempts` | INT64 | 실패한 태스크 시도 수 (`numFailedTaskAttempts` 필드) |
| `resultRowsKind` | STRING | SELECT는 `SELECT_RESULT`, CTAS는 `CTAS_WRITE` |
| `resultRowsSource` | STRING | 결과 행 수의 근거가 된 정확한 카운터 이름 |

## 누락 값과 시간 해석

- `null`은 값이 없거나 의미를 확정할 수 없다는 뜻입니다. 실제로 기록된 `0`과 구분합니다.
- 저장소에서 값이 0인 카운터를 생략할 수 있으므로, 누락된 카운터를 0으로 채우지 않습니다.
- 음수 숫자 지표는 `null`로 저장합니다. 종료가 시작보다 빠르면 종료 시각과 경과시간을 `null`로 저장합니다.
- CPU·GC 시간은 병렬 태스크의 집계값으로, 실제 경과시간과 다릅니다. DAG 집계에 task·vertex 값을 다시 더하지 않습니다.
- CPU 카운터를 실패한 모든 재시도까지 포함한 전체 자원 사용량으로 해석하지 않습니다.

## SELECT·CTAS 결과 행 수

`resultRows`의 기본값은 `null`입니다. 최종 출력과 정확한 Hive `RECORDS_OUT_...` 카운터의 관계를 확인한 DAG에만 매핑을 제공합니다.
매핑한 그룹·이름의 카운터가 있고, 값이 0 이상이며, DAG 상태가 `SUCCEEDED`일 때만 행 수와 출처를 기록합니다.

SELECT는 최종 결과를 생성한 출력, CTAS는 대상 테이블에 기록한 출력의 행 수입니다.
클라이언트가 실제로 읽은 행 수나 테이블 전체 행 수를 뜻하지 않습니다.
여러 DAG 중 최종 출력을 담당한 DAG에만 매핑하며, 중간 출력이나 전체 `OUTPUT_RECORDS`를 합산하지 않습니다.

매핑 파일 작성과 실행 옵션은 [CLI 사용법](../usage/cli.md)을 참고하세요.
