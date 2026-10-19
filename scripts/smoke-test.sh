#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
jar_path="${1:-dzdcache-server/build/libs/dzdcache-server.jar}"
port="${DZD_SMOKE_PORT:-18089}"
work=$(mktemp -d)
node_pid=""
cleanup() {
  if [[ -n "$node_pid" ]]; then kill "$node_pid" 2>/dev/null || true; wait "$node_pid" 2>/dev/null || true; fi
  rm -rf "$work"
}
trap cleanup EXIT
java -jar "$jar_path" --server.address=127.0.0.1 --server.port="$port" \
  --dzdcache.api-token=smoke-test-token --logging.level.root=WARN >"$work/node.log" 2>&1 &
node_pid=$!
base="http://127.0.0.1:$port"
started=false
for ((i=0; i<100; i++)); do
  if curl --silent --fail "$base/actuator/health/readiness" > /dev/null; then started=true; break; fi
  if ! kill -0 "$node_pid" 2>/dev/null; then cat "$work/node.log"; exit 1; fi
  sleep 0.2
done
if [[ "$started" != true ]]; then cat "$work/node.log"; exit 1; fi
request() {
  curl --silent --show-error --fail -H 'Authorization: Bearer smoke-test-token' \
    -H 'Content-Type: application/json' "$@"
}
request "$base/cache/schemas" --data-binary @examples/student-schema.json > "$work/schema.json"
request "$base/cache/entries" --data-binary @examples/put-student.json > "$work/write.json"
result=$(request "$base/cache/search" -d '{"specification":"students","schemaVersion":1,"dimension":"sex","value":"female"}')
[[ "$result" == *'"Anna"'* && "$result" == *'"CS101"'* ]]
empty=$(request "$base/cache/search" -d '{"specification":"students","schemaVersion":1,"dimension":"sex","value":"no-match"}')
[[ "$empty" == '[]' ]]
request "$base/cache/entries/invalidate" -d '{"specification":"students","schemaVersion":1,"identity":42,"revision":2}' > /dev/null
status=$(curl --silent -o /dev/null -w '%{http_code}' -H 'Authorization: Bearer smoke-test-token' \
  -H 'Content-Type: application/json' "$base/cache/entries/get" \
  -d '{"specification":"students","schemaVersion":1,"identity":42}')
[[ "$status" == '404' ]]
request "$base/cache/schemas" --data-binary @examples/student-json.json > "$work/json-schema.json"
request "$base/cache/entries" -d '{"specification":"students-json","schemaVersion":1,"revision":1,"value":{"id":7,"name":"Ola","sex":"male","age":30,"courses":[{"code":"MA201"}]}}' > /dev/null
json_result=$(request "$base/cache/search" -d '{"specification":"students-json","schemaVersion":1,"dimension":"course","value":"MA201","fields":["/name"]}')
[[ "$json_result" == '[{"name":"Ola"}]' ]]
json_range=$(request "$base/cache/search" -d '{"specification":"students-json","schemaVersion":1,"dimension":"age","from":25,"to":35}')
[[ "$json_range" == *'"Ola"'* ]]
echo 'Packaged JAR: runtime compilation, graph search, empty results, and invalidation passed.'
echo 'Packaged JAR: registration without a schema, typed search, and field selection passed.'
