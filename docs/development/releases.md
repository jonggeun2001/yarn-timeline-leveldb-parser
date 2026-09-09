# 빌드·릴리스

Java 8로 빌드·테스트하면 의존성을 포함한 `target/timeline-parser.jar`가 생성됩니다.

```sh
JAVA_HOME=/path/to/jdk8 ./mvnw clean verify
```

worktree 변경사항은 다음 명령으로 테스트합니다. 이 검증 스크립트는 `./mvnw test`만 실행합니다.

```sh
JAVA_HOME=/path/to/jdk8 bash scripts/verify-worktree.sh
```

운영 오류를 조사할 때는 표준 출력의 진행상황과 표준 오류의 WARN·ERROR를 함께 보관합니다. 기본 로그 설정은 [`simplelogger.properties`](../../src/main/resources/simplelogger.properties)에 있으며, 시각·로거 이름과 원인 예외를 기록합니다. 기존 `slf4j-simple` 설정으로 로그 파일을 지정할 수도 있습니다. JVM 옵션은 `-jar` 앞에 둡니다.

```sh
java -Dorg.slf4j.simpleLogger.logFile=timeline-parser.log \
  -jar target/timeline-parser.jar --input ./local/input --output ./local/output
```

파일을 지정해도 CLI 진행상황은 표준 출력, CLI 오류 요약은 표준 오류로 나옵니다. 오류 유형과 기존 결과 보존 조건은 [README](../../README.md#오류-처리와-로그)를 참고하세요.

`main`·`release` push와 해당 브랜치 대상 PR은 [CI](../../.github/workflows/ci.yml)에서 Java 8 빌드·테스트를 실행합니다. **새 버전 태그 push 시에만** [Release](../../.github/workflows/release.yml)가 JAR와 SHA-256 파일을 게시합니다.

1. `main`에서 `pom.xml` 버전을 변경하고 push해 CI 성공을 확인합니다.
2. 검증한 커밋을 `release`에 반영하고 push합니다.
3. 해당 커밋에 POM과 같은 버전의 태그(`X.Y.Z`, `v` 접두사 없음)를 붙여 push합니다.

```sh
# POM 버전이 0.0.2인 release 커밋에서 실행
git tag -a 0.0.2 -m "Release 0.0.2"
git push origin refs/tags/0.0.2
```

게시한 버전은 덮어쓰지 않습니다. 실패한 draft가 남으면 원인을 해결하고 draft를 정리한 뒤 워크플로를 재실행합니다.
