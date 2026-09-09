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
| `resultRows` | INT64 | 최종 SELECT 결과 또는 CTAS 출력 행 수 |
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
| `resultRowsKind` | STRING | `SELECT_RESULT` 또는 `CTAS_WRITE` |
| `resultRowsSource` | STRING | 결과 행 수의 근거 카운터 이름 |

- 누락·음수 지표는 `null`이며, 실제 `0`과 구분합니다. 종료가 시작보다 빠르면 종료 시각·경과시간도 `null`입니다.
- CPU·GC는 DAG가 보고한 태스크 누적값으로, 실제 경과시간과 다릅니다.
- `callerType`이 Hive 이외로 명시되면 제외합니다. 값이 없으면 DAG를 포함하되 `hiveQueryId`는 `null`입니다.

`resultRows`는 검증된 성공 DAG의 최종 출력만 기록합니다. 클라이언트 수신 행 수나 테이블 전체 행 수가 아니며, 중간 출력·전체 `OUTPUT_RECORDS`를 합산하지 않습니다.

실행 옵션과 결과 행 수 매핑은 [README](../../README.md)를 참고하세요.
