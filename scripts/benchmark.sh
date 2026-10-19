#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
./gradlew -q :dzdcache-server:jmh -Pjmh.args="$*"
