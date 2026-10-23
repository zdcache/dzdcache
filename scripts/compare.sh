#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
report=dzdcache-benchmark/build/reports/dzdcache-benchmark/report.md
case "${1:-}" in
  -quick) plan=quick ;;
  -full) plan=full ;;
  -publish)
    [[ -f "$report" ]] || { echo "No report; run scripts/compare.sh -full first"; exit 1; }
    grep -q "(full run)" "$report" || { echo "Only a full run is published; $report is from another run"; exit 1; }
    grep -q "^| redis-stack" "$report" && grep -q "^| dzdcache-embedded" "$report" || { echo "The report does not cover every subject"; exit 1; }
    cp "$report" docs/BENCHMARK.md
    mkdir -p docs/benchmark
    find docs/benchmark -name '*.svg' -delete
    cp "$(dirname "$report")"/benchmark/*.svg docs/benchmark/
    if ! grep -q "docs/BENCHMARK.md" README.md; then
      python3 - <<'PY'
p = "README.md"
s = open(p).read()
s = s.replace("<!-- benchmark-link -->", "<!-- benchmark-link -->\nThe latest published run is in the [benchmark report](docs/BENCHMARK.md).", 1)
open(p, "w").write(s)
PY
    fi
    echo "Published to docs/BENCHMARK.md; commit and push it to update the site."
    exit 0 ;;
  *) echo "Usage: scripts/compare.sh -quick|-full [subject,subject,...] | -publish"; exit 1 ;;
esac
args=(-Pbenchmark.plan="$plan")
[[ -n "${2:-}" ]] && args+=(-Pbenchmark.subjects="$2")
./gradlew :dzdcache-benchmark:compare "${args[@]}" --no-configuration-cache
echo "Report: dzdcache-benchmark/build/reports/dzdcache-benchmark/report.md"
