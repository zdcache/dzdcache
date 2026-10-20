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
forward_pods() { stop_forwards; for i in 0 1 2; do forward "pod/dzdcache-$i" $((18090 + i)); done; }
master_pod() { for i in 0 1 2; do [[ "$(metric $((18090 + i)) dzdcache_master)" == "1.0" ]] && { echo "$i"; return; }; done; echo none; }
every_pod_reads() {
  local id=$1 city=$2
  for i in 0 1 2; do
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
docker build -q -t dzdcache:local . >/dev/null
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

step "a changed cluster-size is refused during a rollout"
stop_forwards
kubectl -n "$NS" set env statefulset/dzdcache DZDCACHE_CLUSTER_SIZE=5 >/dev/null
for _ in $(seq 1 90); do
  [[ "$(kubectl -n "$NS" get pod dzdcache-2 -o jsonpath='{.metadata.labels.controller-revision-hash}' 2>/dev/null)" != \
     "$(kubectl -n "$NS" get statefulset dzdcache -o jsonpath='{.status.currentRevision}')" ]] && break
  sleep 2
done
sleep 20
[[ "$(kubectl -n "$NS" get pod dzdcache-2 -o jsonpath='{.status.containerStatuses[0].ready}' 2>/dev/null)" != true ]] \
  || fail "dzdcache-2 with cluster-size 5 became ready"
forward pod/dzdcache-2 18092
curl -s http://127.0.0.1:18092/actuator/health | grep -q "cluster-size 5" || echo "   (health details are not exposed; checked readiness only)"
stop_forwards
for i in 0 1; do
  [[ "$(kubectl -n "$NS" get pod "dzdcache-$i" -o jsonpath='{.status.containerStatuses[0].ready}')" == true ]] || fail "dzdcache-$i stopped serving"
done
kubectl -n "$NS" rollout undo statefulset/dzdcache >/dev/null
kubectl -n "$NS" delete pod dzdcache-2 >/dev/null
ready 300s || fail "the cluster did not recover after the rollback"
forward_pods
every_pod_reads 7 Oslo

echo "PASS: kind cluster $CLUSTER"
