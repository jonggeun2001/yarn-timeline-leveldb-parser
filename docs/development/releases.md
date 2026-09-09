# 빌드·릴리스

Java 8로 빌드·테스트하면 의존성을 포함한 `target/timeline-parser.jar`가 생성됩니다.

```sh
JAVA_HOME=/path/to/jdk8 ./mvnw clean verify
```

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
