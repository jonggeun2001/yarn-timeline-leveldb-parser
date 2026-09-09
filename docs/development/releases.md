# CI와 릴리스

| 이벤트 | 실행 내용 | 게시 파일 |
| --- | --- | --- |
| `main`·`release` push 또는 해당 브랜치 대상 PR | Linux x86_64 / Java 8 빌드·테스트 | 없음 |
| `X.Y.Z` 버전 태그 push | 버전 확인 → 빌드·테스트 → GitHub Release | 의존성 포함 JAR와 SHA-256 |

릴리스는 태그 push 시에만 생성합니다. 태그는 `v` 접두사 없이 POM 버전과 일치해야 하며, 해당 커밋은 원격 `release` 브랜치 이력에 포함되어야 합니다. 이미 게시한 버전은 덮어쓰지 않습니다.

## 새 버전 게시

1. `main`에서 `pom.xml` 버전을 변경해 push하고 CI 성공을 확인합니다.
2. 검증한 커밋을 `release`에 반영해 push합니다.
3. 해당 커밋에 같은 버전의 태그를 붙여 push합니다.

```sh
# POM 버전이 0.0.2인 release 커밋에서 실행
git tag -a 0.0.2 -m "Release 0.0.2"
git push origin refs/tags/0.0.2
```

## 다운로드 확인

[Releases](https://github.com/jonggeun2001/yarn-timeline-leveldb-parser/releases)에서 JAR와 체크섬 파일을 내려받습니다.

```sh
sha256sum -c timeline-parser-0.0.1-all.jar.sha256
java -jar timeline-parser-0.0.1-all.jar --version
```

게시 중 실패해 draft가 남으면 원인을 해결하고 draft를 정리한 뒤 재실행합니다.

워크플로: [CI](../../.github/workflows/ci.yml) · [Release](../../.github/workflows/release.yml)
