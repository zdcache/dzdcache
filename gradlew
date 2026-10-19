#!/bin/sh
# Minimal bootstrap launcher for the OFFICIAL Gradle Wrapper JAR.
# The JAR is fetched on first run with an SHA-256 check because it could not be
# included in this generated archive. Subsequent runs use the verified JAR.
set -eu
HERE=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
JAR="$HERE/gradle/wrapper/gradle-wrapper.jar"
EXPECTED_SHA256=d81e0f23ade952b35e55333dd5f1821585e887c6d24305aeea2fbc8dad564b95
URL=https://services.gradle.org/distributions/gradle-9.7.0-wrapper.jar
if [ ! -f "$JAR" ]; then
  TEMP="$JAR.tmp.$$"
  trap 'rm -f "$TEMP"' EXIT HUP INT TERM
  echo 'Fetching the official Gradle 9.7.0 wrapper JAR (first run only)...' >&2
  if command -v curl >/dev/null 2>&1; then
    curl --fail --location --retry 2 --output "$TEMP" "$URL"
  elif command -v wget >/dev/null 2>&1; then
    wget -O "$TEMP" "$URL"
  else
    echo 'Requires curl or wget to bootstrap Gradle, or a locally installed gradle command.' >&2
    exit 1
  fi
  if command -v sha256sum >/dev/null 2>&1; then
    ACTUAL=$(sha256sum "$TEMP" | cut -d ' ' -f 1)
  elif command -v shasum >/dev/null 2>&1; then
    ACTUAL=$(shasum -a 256 "$TEMP" | cut -d ' ' -f 1)
  else
    echo 'sha256sum or shasum is required to verify the Gradle wrapper.' >&2
    exit 1
  fi
  if [ "$ACTUAL" != "$EXPECTED_SHA256" ]; then
    echo 'SHA-256 mismatch for Gradle wrapper download. Aborting.' >&2
    exit 1
  fi
  mv "$TEMP" "$JAR"
  trap - EXIT HUP INT TERM
fi
if [ -n "${JAVA_HOME:-}" ]; then
  JAVA="$JAVA_HOME/bin/java"
else
  JAVA=java
fi
exec "$JAVA" -Dorg.gradle.appname=gradlew -classpath "$JAR" org.gradle.wrapper.GradleWrapperMain "$@"
