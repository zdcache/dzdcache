#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
KIND="${KIND:-kind}"
CLUSTER="${KIND_CLUSTER:-dzdcache-test}"
NS="${NAMESPACE:-dzdcache-test}"
TOKEN="k8s-test-token"
work=$(mktemp -d)
forwards=()
cleanup() {
  for pid in "${forwards[@]:-}"; do [[ -n "$pid" ]] && kill "$pid" 2>/dev/null || true; done
  rm -rf "$work"
  if [[ "${KEEP:-}" != 1 ]]; then "$KIND" delete cluster --name "$CLUSTER" >/dev/null 2>&1 || true; fi
}
trap cleanup EXIT
fail() { echo "FAIL: $*"; kubectl -n "$NS" get pods -o wide || true; exit 1; }
step() { echo "== $*"; }

forward() {
  local target=$1 port=$2
  kubectl -n "$NS" port-forward "$target" "$port:8080" >"$work/forward-$port.log" 2>&1 &
  forwards+=($!)
  for _ in $(seq 1 50); do curl -s -o /dev/null "http://127.0.0.1:$port/actuator/health/liveness" && return 0; sleep 0.2; done
  fail "port-forward to $target did not start"
}
stop_forwards() { for pid in "${forwards[@]:-}"; do [[ -n "$pid" ]] && kill "$pid" 2>/dev/null || true; done; forwards=(); }
request() {
  curl -s -o "$work/body" -w '%{http_code}' -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' "$@"
}
metric() { curl -s "http://127.0.0.1:$1/actuator/prometheus" | awk -v m="$2" '$1==m {print $2}'; }
PODS=3
forward_pods() { stop_forwards; for i in $(seq 0 $((PODS - 1))); do forward "pod/dzdcache-$i" $((18090 + i)); done; }
master_pod() { for i in $(seq 0 $((PODS - 1))); do [[ "$(metric $((18090 + i)) dzdcache_master)" == "1.0" ]] && { echo "$i"; return; }; done; echo none; }
resize() {
  local status
  status=$(curl -s -o "$work/body" -w '%{http_code}' -X PUT -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
    http://127.0.0.1:18090/cache/cluster -d "$1")
  [[ "$status" == 200 ]] || fail "resize $1 answered $status: $(cat "$work/body")"
}
every_pod_reads() {
  local id=$1 city=$2
  for i in $(seq 0 $((PODS - 1))); do
    local status
    status=$(request "http://127.0.0.1:$((18090 + i))/cache/entries/get" -d "{\"specification\":\"students\",\"schemaVersion\":1,\"identity\":$id}")
    [[ "$status" == 200 ]] && grep -q "\"$city\"" "$work/body" || fail "dzdcache-$i answered $status for $id: $(cat "$work/body")"
  done
}
put() {
  local id=$1 city=$2 port=$3
  local status
  status=$(request "http://127.0.0.1:$port/cache/entries" -d "{\"specification\":\"students\",\"schemaVersion\":1,\"value\":{\"id\":$id,\"name\":\"N$id\",\"sex\":\"female\",\"address\":{\"city\":\"$city\"}}}")
  [[ "$status" == 200 ]] || fail "write of $id answered $status: $(cat "$work/body")"
}
ready() { kubectl -n "$NS" rollout status statefulset/dzdcache --timeout="${1:-240s}" >/dev/null; }

step "kind cluster $CLUSTER"
"$KIND" get clusters 2>/dev/null | grep -qx "$CLUSTER" || "$KIND" create cluster --name "$CLUSTER" --wait 120s
kubectl config use-context "kind-$CLUSTER" >/dev/null
step "image"
docker build --progress=plain -t dzdcache:local . >"$work/docker.log" 2>&1 || { tail -60 "$work/docker.log"; fail "the image did not build"; }
"$KIND" load docker-image dzdcache:local --name "$CLUSTER" >/dev/null
step "deploy"
kubectl create namespace "$NS" --dry-run=client -o yaml | kubectl apply -f - >/dev/null
kubectl -n "$NS" create secret generic dzdcache-auth --from-literal=api-token="$TOKEN" \
  --from-literal=cluster-token="$(openssl rand -hex 32)" --dry-run=client -o yaml | kubectl apply -f - >/dev/null
kubectl -n "$NS" apply -f k8s/dzdcache.yaml >/dev/null
ready || fail "the StatefulSet did not become ready"
forward_pods

step "publish, write and read on every pod"
[[ "$(request http://127.0.0.1:18090/cache/schemas --data-binary @examples/student-schema.json)" == 201 ]] || fail "publish: $(cat "$work/body")"
for id in $(seq 1 20); do put "$id" Oslo 18091; done
every_pod_reads 7 Oslo

step "hard kill of the master pod"
master=$(master_pod)
[[ "$master" != none ]] || fail "no master"
writer=$(( (master + 1) % 3 ))
stop_forwards
kubectl -n "$NS" delete pod "dzdcache-$master" --grace-period=0 --force >/dev/null 2>&1
forward "pod/dzdcache-$writer" 18099
start=$(date +%s)
until [[ "$(request http://127.0.0.1:18099/cache/entries -d '{"specification":"students","schemaVersion":1,"value":{"id":21,"name":"N21","sex":"female","address":{"city":"Bergen"}}}')" == 200 ]]; do
  (( $(date +%s) - start < 60 )) || fail "no write was acknowledged within 60 seconds of the kill: $(cat "$work/body")"
  sleep 0.2
done
echo "   a write was acknowledged $(( $(date +%s) - start )) s after the kill"
ready || fail "dzdcache-$master did not come back"
forward_pods
every_pod_reads 21 Bergen
every_pod_reads 7 Oslo

step "rolling restart"
stop_forwards
kubectl -n "$NS" rollout restart statefulset/dzdcache >/dev/null
ready 300s || fail "the rolling restart did not finish"
forward_pods
every_pod_reads 7 Oslo
every_pod_reads 21 Bergen

step "live resize from three pods to five"
resize '{"size":5}'
stop_forwards
kubectl -n "$NS" scale statefulset dzdcache --replicas=5 >/dev/null
ready 300s || fail "the StatefulSet did not reach five ready pods"
PODS=5
forward_pods
every_pod_reads 7 Oslo
every_pod_reads 21 Bergen
put 22 Trondheim 18094
every_pod_reads 22 Trondheim

step "live resize back to three pods"
resize '{"size":3,"leaving":["dzdcache-3","dzdcache-4"]}'
for _ in $(seq 1 30); do
  [[ "$(kubectl -n "$NS" get pod dzdcache-4 -o jsonpath='{.status.containerStatuses[0].ready}' 2>/dev/null)" != true ]] && break
  sleep 1
done
stop_forwards
kubectl -n "$NS" scale statefulset dzdcache --replicas=3 >/dev/null
ready 300s || fail "the StatefulSet did not settle on three pods"
PODS=3
forward_pods
every_pod_reads 7 Oslo
every_pod_reads 22 Trondheim
request http://127.0.0.1:18090/cache/cluster >/dev/null
grep -q '"size":3' "$work/body" || fail "the cluster does not report size 3: $(cat "$work/body")"

step "every pod deleted at once; the volumes keep the data"
stop_forwards
kubectl -n "$NS" delete pod dzdcache-0 dzdcache-1 dzdcache-2 --grace-period=0 --force >/dev/null 2>&1
sleep 5
ready 300s || fail "the StatefulSet did not come back after every pod was deleted"
forward_pods
every_pod_reads 7 Oslo
every_pod_reads 21 Bergen
every_pod_reads 22 Trondheim
put 23 Stavanger 18091
every_pod_reads 23 Stavanger

echo "PASS: kind cluster $CLUSTER"
