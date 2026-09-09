# Third-party notices

이 프로젝트의 라이선스는 [Apache License 2.0](LICENSE)입니다. 아래 소프트웨어의 저작권과 라이선스는 각 권리자에게 있으며, 이 문서는 해당 조건을 변경하지 않습니다.

## 직접 적용한 Apache Hadoop 코드와 규칙

This product includes software developed by The Apache Software Foundation (https://www.apache.org/).

`src/main/java/io/github/timelineparser/compat/RollingKeyDecoder.java`의 binary key 구조와 역순 long 해독은 Apache Hadoop 3.1.1의 `LeveldbUtils`/`GenericObjectMapper`를 바탕으로 이 프로젝트에 맞게 수정했습니다. Hadoop 서버 전체 실행 대신 로컬 rolling entity key 해독에 필요한 부분을 사용하며, 잘못된 문자열·잘린 키의 오류 처리를 추가했습니다.

`src/main/java/io/github/timelineparser/compat/FstValueDecoder.java`의 reference sharing 비활성화와 구버전 LinkedHashMap class ID 83 처리는 같은 버전의 `RollingLevelDBTimelineStore`에서 사용하는 호환 설정을 따릅니다. 원본 프로젝트의 라이선스는 Apache-2.0입니다.

- [Hadoop 3.1.1 LeveldbUtils](https://github.com/apache/hadoop/blob/rel/release-3.1.1/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-applicationhistoryservice/src/main/java/org/apache/hadoop/yarn/server/timeline/util/LeveldbUtils.java)
- [Hadoop 3.1.1 GenericObjectMapper](https://github.com/apache/hadoop/blob/rel/release-3.1.1/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-applicationhistoryservice/src/main/java/org/apache/hadoop/yarn/server/timeline/GenericObjectMapper.java)
- [Hadoop 3.1.1 RollingLevelDBTimelineStore](https://github.com/apache/hadoop/blob/rel/release-3.1.1/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-applicationhistoryservice/src/main/java/org/apache/hadoop/yarn/server/timeline/RollingLevelDBTimelineStore.java)
- [Hadoop 3.1.1 NOTICE](https://github.com/apache/hadoop/blob/rel/release-3.1.1/NOTICE.txt)

Tez 0.9.1과 Hive 3.1.3의 엔티티·카운터 규칙은 [요구사항의 근거](docs/requirements/timeline-leveldb-parser.md#10-근거)에 연결되어 있습니다. 두 실행 엔진을 애플리케이션의 직접 의존성으로 포함하지 않습니다.

## 주요 의존성

직접 의존성과 버전의 기준은 [pom.xml](pom.xml)입니다. 아래 표는 전이 의존성 전체 목록이 아닙니다. 라이브러리에 동봉된 LICENSE/NOTICE와 해당 배포본의 조건도 함께 적용됩니다.

| 소프트웨어 | 버전 | 용도 | 라이선스·출처 |
| --- | --- | --- | --- |
| picocli | 4.7.7 | CLI | [Apache-2.0](https://github.com/remkop/picocli/blob/v4.7.7/LICENSE) |
| leveldbjni-all | 1.8 | native LevelDB 및 JNI/API | [BSD-3-Clause](https://github.com/fusesource/leveldbjni/blob/leveldbjni-1.8/license.txt) |
| FST | 2.50 | rolling 값 역직렬화 | [Apache-2.0, 배포 POM](https://repo.maven.apache.org/maven2/de/ruedigermoeller/fst/2.50/fst-2.50.pom) |
| Apache Hadoop common, yarn-api, mapreduce-client-core | 3.1.1 | Timeline 모델·Parquet 지원 | [Apache-2.0](https://github.com/apache/hadoop/blob/rel/release-3.1.1/LICENSE.txt) |
| Apache Parquet / parquet-avro | 1.16.0 | Parquet 생성·검증 | [Apache-2.0](https://github.com/apache/parquet-java/blob/apache-parquet-1.16.0/LICENSE) |
| Apache Avro | 1.11.4 | 스키마와 record 모델 | [Apache-2.0](https://github.com/apache/avro/blob/release-1.11.4/LICENSE.txt) |
| Jackson | 2.19.2 | JSON 매핑·라이브러리 지원 | [Apache-2.0, 배포 POM](https://repo.maven.apache.org/maven2/com/fasterxml/jackson/core/jackson-databind/2.19.2/jackson-databind-2.19.2.pom) |
| SLF4J API, simple, jcl-over-slf4j | 2.0.17 | 로깅 | [MIT](https://github.com/qos-ch/slf4j/blob/v_2.0.17/LICENSE.txt) |
| Apache Hadoop applicationhistoryservice | 3.1.1 | 테스트 fixture 전용 | [Apache-2.0](https://github.com/apache/hadoop/blob/rel/release-3.1.1/LICENSE.txt) |
| JUnit Jupiter | 5.13.4 | 테스트 전용 | [EPL-2.0, 배포 POM](https://repo.maven.apache.org/maven2/org/junit/jupiter/junit-jupiter/5.13.4/junit-jupiter-5.13.4.pom) |

Parquet의 Snappy 압축에는 전이 의존성인 `org.xerial.snappy:snappy-java:1.1.10.7`이 사용됩니다. Java wrapper는 [Apache-2.0](https://repo.maven.apache.org/maven2/org/xerial/snappy/snappy-java/1.1.10.7/snappy-java-1.1.10.7.pom), 포함된 Google Snappy는 BSD 조건을 따릅니다. [snappy-java NOTICE](https://github.com/xerial/snappy-java/blob/v1.1.10.7/NOTICE)에는 Google Snappy, Apache PureJavaCrc32C 및 정적 링크된 libstdc++의 GCC Runtime Library Exception 고지가 있습니다.

빌드 도구는 Maven Wrapper 3.3.4와 Apache Maven 3.9.16이며 Apache-2.0으로 배포됩니다. JDK는 별도로 설치하고 실행 JAR에 포함하지 않습니다.

현재 빌드의 런타임 전이 의존성은 다음 명령으로 확인할 수 있습니다.

```sh
./mvnw dependency:tree -Dscope=runtime
```

## 비 Apache 라이선스 고지

아래에는 LevelDB JNI, 포함된 native LevelDB/Snappy, SLF4J의 라이선스 고지를 보존합니다. Snappy의 별도 라이선스 benchmark 데이터는 이 프로젝트의 입력 fixture로 사용하지 않습니다.

### LevelDB JNI — BSD-3-Clause

[원문](https://raw.githubusercontent.com/fusesource/leveldbjni/leveldbjni-1.8/license.txt)

```text
Copyright (c) 2011 FuseSource Corp. All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met:

   * Redistributions of source code must retain the above copyright
notice, this list of conditions and the following disclaimer.
   * Redistributions in binary form must reproduce the above
copyright notice, this list of conditions and the following disclaimer
in the documentation and/or other materials provided with the
distribution.
   * Neither the name of FuseSource Corp. nor the names of its
contributors may be used to endorse or promote products derived from
this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
```

### LevelDB — BSD-3-Clause

[원문](https://raw.githubusercontent.com/google/leveldb/v1.15/LICENSE)

```text
Copyright (c) 2011 The LevelDB Authors. All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met:

   * Redistributions of source code must retain the above copyright
notice, this list of conditions and the following disclaimer.
   * Redistributions in binary form must reproduce the above
copyright notice, this list of conditions and the following disclaimer
in the documentation and/or other materials provided with the
distribution.
   * Neither the name of Google Inc. nor the names of its
contributors may be used to endorse or promote products derived from
this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
```

### Google Snappy — BSD-3-Clause

[원문](https://raw.githubusercontent.com/google/snappy/main/COPYING)

```text
Copyright 2011, Google Inc.
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met:

    * Redistributions of source code must retain the above copyright
notice, this list of conditions and the following disclaimer.
    * Redistributions in binary form must reproduce the above
copyright notice, this list of conditions and the following disclaimer
in the documentation and/or other materials provided with the
distribution.
    * Neither the name of Google Inc. nor the names of its
contributors may be used to endorse or promote products derived from
this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
```

### SLF4J — MIT

[원문](https://raw.githubusercontent.com/qos-ch/slf4j/v_2.0.17/LICENSE.txt)

```text
Copyright (c) 2004-2022 QOS.ch Sarl (Switzerland)
All rights reserved.

Permission is hereby granted, free  of charge, to any person obtaining
a  copy  of this  software  and  associated  documentation files  (the
"Software"), to  deal in  the Software without  restriction, including
without limitation  the rights to  use, copy, modify,  merge, publish,
distribute,  sublicense, and/or sell  copies of  the Software,  and to
permit persons to whom the Software  is furnished to do so, subject to
the following conditions:

The  above  copyright  notice  and  this permission  notice  shall  be
included in all copies or substantial portions of the Software.

THE  SOFTWARE IS  PROVIDED  "AS  IS", WITHOUT  WARRANTY  OF ANY  KIND,
EXPRESS OR  IMPLIED, INCLUDING  BUT NOT LIMITED  TO THE  WARRANTIES OF
MERCHANTABILITY,    FITNESS    FOR    A   PARTICULAR    PURPOSE    AND
NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
OF CONTRACT, TORT OR OTHERWISE,  ARISING FROM, OUT OF OR IN CONNECTION
WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
```
