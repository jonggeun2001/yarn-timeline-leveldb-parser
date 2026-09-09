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
| `resultRows` | INT64 | SQL의 출력 대상 번호로 선택한 FileSink 카운터 값 또는 DAG별 override 값 |
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

- 유효한 `startTime`·`endTime`은 `otherInfo`와 `DAG_STARTED`·`DAG_FINISHED` 이벤트에서 수집한 값 중 각각 가장 늦은 시각입니다. 서로 다른 정상값은 경고를 남기고 병합하며, 입력 순서는 결과에 영향을 주지 않습니다.
- 누락 지표는 `null`이며, 실제 `0`과 구분합니다. 선택 필드·카운터의 형식 오류·음수·INT64 범위 초과 또는 시각 외 값의 충돌은 해당 값을 `null`로 무효화하며, 이후 정상값이 있어도 복구하지 않습니다.
- 경과시간은 선택된 `endTime - startTime`으로 계산합니다. 둘 중 하나가 `null`이면 경과시간도 `null`이며, 종료가 시작보다 빠르면 종료 시각·경과시간도 `null`입니다.
- 잘못된 DAG ID나 소속 application ID 불일치가 발견된 DAG는 행 전체를 제외합니다.
- CPU·GC는 DAG가 보고한 태스크 누적값으로, 실제 경과시간과 다릅니다.
- `callerType`이 Hive 이외로 명시되면 제외합니다. 값이 없으면 DAG를 포함하되 `hiveQueryId`는 `null`입니다.

`resultRows`는 `SUCCEEDED` DAG에서 Hive SQL 문법을 분석해 다음 번호를 선택합니다. 테이블명 접미사는 허용합니다.

| SQL | 카운터 |
| --- | --- |
| SELECT, WITH … SELECT | `RECORDS_OUT_0` 계열 |
| CTAS, 단일 INSERT INTO / INSERT OVERWRITE TABLE | `RECORDS_OUT_1` 계열 |
| INSERT OVERWRITE [LOCAL] DIRECTORY | `RECORDS_OUT_1` 계열 |

주석·인용 식별자·CTE·파티션 구문을 지원합니다. 다중 INSERT·여러 SQL 문장·UPDATE/DELETE/MERGE·기타 미지원 또는 해석 불가 SQL은 null입니다. 필요한 번호가 없으면 다른 번호로 대체하지 않습니다.

SQL은 Hive `dagPlan.dagInfo`의 JSON `description`을 우선 사용하고, 없으면 Hive caller context의 `dagPlan.dagContext.description`을 사용합니다. SQL이 없을 때만 기존의 유일 숫자 카운터 선택을 적용합니다. SQL 원문은 출력에 저장하지 않습니다.

모든 그룹에서 해당 번호의 `(group, name)` 후보가 정확히 하나이고 값이 0 이상이면 원본 INT64 값을 기록합니다. 후보 없음·여러 후보·음수·비성공 DAG는 null입니다. `INTERMEDIATE`·`OPERATOR` 및 `OUTPUT_RECORDS`는 제외합니다. ATS는 0인 카운터를 생략할 수 있으므로 누락을 0으로 해석하지 않습니다.

자동 값은 `FILE_SINK_OUTPUT`이며 `resultRowsSource`에 원본 카운터 이름을 기록합니다. 번호 선택은 일반적인 Hive 출력 경로에 근거합니다. 같은 이름의 중간 sink가 합산되거나 여러 DAG가 실행될 수 있으므로 쿼리 전체의 최종 결과·테이블 커밋 행 수를 보장하지 않습니다.

카운터 구조·그룹명·카운터명이 손상되어 후보 전체를 확인할 수 없으면 해당 DAG의 자동 추출을 비활성화하고 `resultRows`·`resultRowsKind`·`resultRowsSource`를 모두 `null`로 유지합니다. 이름을 확인한 카운터의 값만 무효화된 경우에는 후보 수에 계속 포함하므로, 다른 FileSink가 유일한 후보로 잘못 선택되지 않습니다.

`--result-rows-mapping`은 검증한 DAG별 `SELECT_RESULT`·`CTAS_WRITE` override입니다. 매핑한 DAG는 지정한 그룹·이름의 카운터만 사용하며, 없거나 무효화된 값이면 null을 유지하고 자동 선택으로 대체하지 않습니다. 구조 오류로 자동 추출이 비활성화되어도 지정한 카운터가 유효하고 DAG가 성공했다면 override를 적용합니다.

실행 옵션과 결과 행 수 매핑은 [README](../../README.md)를 참고하세요.
