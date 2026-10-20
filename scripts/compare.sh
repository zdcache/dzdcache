#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
case "${1:-}" in
  -quick) plan=quick ;;
  -full) plan=full ;;
  *) echo "Usage: scripts/compare.sh -quick|-full [subject,subject,...]"; exit 1 ;;
esac
args=(-Pbenchmark.plan="$plan")
[[ -n "${2:-}" ]] && args+=(-Pbenchmark.subjects="$2")
./gradlew :dzdcache-benchmark:compare "${args[@]}" --no-configuration-cache
echo "Report: dzdcache-benchmark/build/reports/dzdcache-benchmark/report.md"
