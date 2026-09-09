# Timeline LevelDB Parser

YARN Timeline의 로컬 Rolling LevelDB 사본에서 Hive DAG 지표를 추출해 Parquet로 저장하는 Java 8 CLI입니다.

## 실행

Java 8과 Linux/macOS x86_64 환경이 필요합니다. [Releases](https://github.com/jonggeun2001/yarn-timeline-leveldb-parser/releases)에서 의존성이 포함된 JAR를 내려받아 실행합니다.

```sh
java -jar timeline-parser-0.0.1-all.jar \
  --input ./local/input \
  --output ./local/output
```

결과는 `local/output/result.parquet`이며, DAG 하나당 한 행입니다. 재실행하면 입력 전체로 결과를 교체하고, 교체 전 실패하면 기존 파일을 보존합니다.

## 입력 준비

- 입력은 `entity-ldb.*` DB 하나 또는 여러 DB를 담은 상위 디렉터리입니다.
- `CURRENT`, `MANIFEST`, SST, 필요한 WAL이 모두 있는 일관된 사본을 준비하고 실행 중에는 변경하지 않습니다.
- `indexes-ldb`, `starttime-ldb`는 필요하지 않습니다. 입력과 출력 경로는 서로 겹치지 않아야 합니다.
- 로컬 데이터는 Git에서 제외한 `local/` 아래에 보관할 수 있습니다.

완료된 application만 처리합니다. 완료 정보가 없는 입력은 `--completed-applications`로 확인된 ID 목록을 지정합니다. `resultRows`는 최종 출력 카운터 매핑을 제공할 때 채웁니다. 자세한 예시는 [CLI 사용법](docs/usage/cli.md)을 참고하세요.

## 소스 빌드

```sh
JAVA_HOME=/path/to/jdk8 ./mvnw clean verify
```

실행 JAR는 `target/timeline-parser.jar`에 생성됩니다.

## 문서

- [CLI 사용법](docs/usage/cli.md): 옵션, 완료 ID, SELECT·CTAS 행 수
- [출력 스키마](docs/reference/schema.md): 20개 컬럼과 해석
- [아키텍처](docs/architecture/leveldb-to-parquet.md): 처리 흐름과 구성 요소
- [CI와 릴리스](docs/development/releases.md): 태그 push로 JAR 게시
- [검증 결과](docs/development/validation.md): 테스트 범위와 합성 데이터 측정

[Apache License 2.0](LICENSE) · [오픈소스 고지](THIRD-PARTY-NOTICES.md)
