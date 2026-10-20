#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."
JAR="${1:-dzdcache-server/build/libs/dzdcache-server.jar}"
DURATION_SECONDS="${DURATION_SECONDS:-600}"
SEED="${SEED:-$RANDOM}"
work=$(mktemp -d)
TOKEN=soak-token
declare -a nodes proxies
cleanup() {
  for pid in "${nodes[@]:-}" "${proxies[@]:-}" "${k6pid:-}"; do [[ -n "$pid" ]] && kill "$pid" 2>/dev/null || true; done
  wait 2>/dev/null || true
  echo "logs kept in $work"
}
trap cleanup EXIT
RANDOM=$SEED
echo "soak for ${DURATION_SECONDS}s, seed $SEED, logs in $work"

start_node() {
  local i=$1 peers=""
  for j in 1 2 3; do [[ "$j" != "$i" ]] && peers="${peers:+$peers,}http://127.0.0.1:707$j"; done
  java -Xmx768m -jar "$JAR" --server.port=808$i --dzdcache.cluster-port=717$i --dzdcache.discovery.mode=static \
    --dzdcache.discovery.node-id=n$i --dzdcache.discovery.self-url=http://127.0.0.1:707$i \
    --dzdcache.discovery.static-peers=$peers --dzdcache.api-token=$TOKEN --dzdcache.cluster-token=soak-cluster \
    >>"$work/n$i.log" 2>&1 &
  nodes[$i]=$!
}
metric() { curl -s --max-time 2 "http://127.0.0.1:808$1/actuator/prometheus" | awk -v m="$2" '$1==m {print $2}'; }
all_current() { for i in 1 2 3; do [[ "$(metric $i dzdcache_current)" == "1.0" ]] || return 1; done; }

for i in 1 2 3; do python3 scripts/soak/fault-proxy.py 707$i 717$i >"$work/proxy$i.log" 2>&1 & proxies[$i]=$!; done
for i in 1 2 3; do start_node $i; done
for _ in $(seq 1 240); do all_current && break; sleep 0.5; done
all_current || { echo "FAIL: the cluster did not start"; exit 1; }

k6 run -q -e BASE_URL=http://127.0.0.1:8081 -e READ_URLS=http://127.0.0.1:8082,http://127.0.0.1:8083 -e TOKEN=$TOKEN \
  -e DURATION="${DURATION_SECONDS}s" -e SIZE=5000 --summary-export "$work/summary.json" scripts/load/search-with-writes.js \
  >"$work/k6.log" 2>&1 &
k6pid=$!

faults=0
end=$(( $(date +%s) + DURATION_SECONDS - 30 ))
while (( $(date +%s) < end )); do
  sleep $(( 5 + RANDOM % 10 ))
  target=$(( 1 + RANDOM % 3 ))
  hold=$(( 3 + RANDOM % 10 ))
  if (( RANDOM % 2 == 0 )) && [[ $target != 1 ]]; then
    echo "$(date +%T) kill -9 n$target for ${hold}s"
    kill -9 "${nodes[$target]}" 2>/dev/null || true
    sleep "$hold"
    start_node "$target"
  else
    echo "$(date +%T) cut inbound cluster traffic to n$target for ${hold}s"
    kill -USR1 "${proxies[$target]}"
    sleep "$hold"
    kill -USR1 "${proxies[$target]}"
  fi
  faults=$((faults + 1))
  for _ in $(seq 1 120); do all_current && break; sleep 0.5; done
done
wait "$k6pid" || true
for _ in $(seq 1 120); do all_current && break; sleep 0.5; done

echo "faults injected: $faults"
python3 - "$work/summary.json" <<'PY'
import json, sys
checks = json.load(open(sys.argv[1]))["root_group"]["checks"]
for name, c in checks.items(): print("  %-45s passes %7d fails %d" % (name, c["passes"], c["fails"]))
stale = checks.get("acknowledged write visible on every node", {"fails": 1})["fails"]
sys.exit(1 if stale else 0)
PY
stale=$?
applied=$(for i in 1 2 3; do metric $i dzdcache_applied_index; done | sort -u | wc -l | tr -d ' ')
entries=$(for i in 1 2 3; do curl -s "http://127.0.0.1:808$i/actuator/prometheus" | grep '^dzdcache_entries'; done | sort -u | wc -l | tr -d ' ')
echo "nodes agree on the applied index: $([[ $applied == 1 ]] && echo yes || echo no); on cached roots: $([[ $entries == 1 ]] && echo yes || echo no)"
[[ $stale == 0 && $applied == 1 && $entries == 1 ]] && echo PASS || { echo FAIL; exit 1; }
