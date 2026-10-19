#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
read -r -a maven <<< "${MAVEN_ARGS:-}"
mvn -q -B "${maven[@]}" -pl dzdcache-server -am test-compile dependency:build-classpath \
  -Dmdep.includeScope=test -Dmdep.outputFile=target/benchmark-classpath.txt
cd dzdcache-server
java -cp "target/test-classes:target/classes:$(cat target/benchmark-classpath.txt)" \
  org.openjdk.jmh.Main -rf json -rff target/benchmark.json "$@"
