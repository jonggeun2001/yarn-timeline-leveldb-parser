# CI와 버전 릴리스

## 실행 조건

| 이벤트 | 동작 | 공개 산출물 |
| --- | --- | --- |
| `main`, `release` push | Linux x86_64 / Java 8에서 `./mvnw clean verify` | 없음 |
| `main`, `release` 대상 Pull Request | 같은 빌드·테스트 실행 | 없음 |
| `0.0.1` 같은 `X.Y.Z` 태그 push | 버전과 release 브랜치 포함 여부 확인 → 빌드·테스트 → GitHub Release 게시 | 의존성 포함 JAR와 SHA-256 |

일반 CI에서는 검증을 위해 JAR를 빌드하지만 다운로드 artifact나 GitHub Release로 게시하지 않는다. 릴리스 workflow에는 수동 실행 및 브랜치 push 트리거를 두지 않는다.

태그는 `v` 접두사 없는 안정 버전 형식이다. 태그 버전은 `pom.xml`의 프로젝트 버전과 일치해야 하며, 태그가 가리키는 커밋은 원격 `release` 브랜치 이력에 포함되어야 한다. Maven 버전은 빌드 리소스, `--version`, JAR manifest와 Parquet의 `timeline.parser.version`에 함께 반영된다.

## 배포 파일

`0.0.1` 릴리스에는 다음 두 파일을 게시한다.

- `timeline-parser-0.0.1-all.jar`: Maven Shade가 만든 의존성 포함 실행 JAR.
- `timeline-parser-0.0.1-all.jar.sha256`: JAR의 SHA-256 체크섬.

원래의 작은 `original-timeline-parser.jar`는 게시하지 않는다. 모든 테스트가 통과하면 draft release를 만들고 자산을 올린 뒤 공개한다. 이미 존재하는 릴리스를 자동으로 덮어쓰지 않는다.

```sh
sha256sum -c timeline-parser-0.0.1-all.jar.sha256
java -jar timeline-parser-0.0.1-all.jar --version
java -jar timeline-parser-0.0.1-all.jar --input /data/timeline-copy --output /data/parquet
```

Java 8과 지원되는 native 아키텍처는 여전히 필요하다. JAR에 JVM 자체를 포함하지 않는다.

## 다음 버전 배포

1. `main`에서 `pom.xml`의 프로젝트 버전을 변경하고 커밋·push한다.
2. CI 성공을 확인한 커밋을 `release` 브랜치에 반영하고 push한다.
3. 해당 `release` 커밋에 동일 버전의 annotated tag를 생성하고 그 태그만 push한다.
4. Actions의 Release workflow가 성공하고 JAR와 체크섬이 게시됐는지 확인한다.

예를 들어 프로젝트 버전이 `0.0.2`이고 검증된 `release` 커밋을 체크아웃한 상태라면:

```sh
git tag -a 0.0.2 -m "Release 0.0.2"
git push origin refs/tags/0.0.2
```

실패한 실행에서 draft가 남으면 실패 원인을 확인한 뒤 그 draft를 정리하고 재실행한다. 이미 공개된 버전은 수정하거나 태그를 옮기지 않고 새 버전으로 배포한다.

워크플로 정의: [CI](../../.github/workflows/ci.yml), [Release](../../.github/workflows/release.yml). 트리거 정의는 [GitHub Actions 공식 문서](https://docs.github.com/en/actions/reference/workflows-and-actions/workflow-syntax#onpushbranchestagsbranches-ignoretags-ignore)를 따른다.
