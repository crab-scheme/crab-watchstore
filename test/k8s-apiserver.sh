#!/usr/bin/env bash
# test/k8s-apiserver.sh — cw-lkq.9: kube-apiserver conformance.
#
# A REAL kube-apiserver (registry.k8s.io container) runs with
# --etcd-servers pointed at the crab-watchstore compose cluster, then kubectl
# (or curl) drives it from the host: healthz, namespace + configmap CRUD,
# LIST, and the WATCH path (apiserver watch-cache priming uses serializable
# reads; object watches ride our Watch service). This is THE integration gate
# for "backs a Kubernetes control plane".
set -uo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
COMPOSE="docker compose -f $ROOT/deploy/docker/docker-compose.yml"
KVER="${KVER:-v1.30.0}"
WORK=/tmp/cws-k8s; rm -rf "$WORK"; mkdir -p "$WORK"
PASS=0; FAIL=0
ok(){ PASS=$((PASS+1)); echo "  ok   $1"; }
bad(){ FAIL=$((FAIL+1)); echo "  FAIL $1"; }
check(){ if [ "$2" = "$3" ]; then ok "$1"; else bad "$1  expected=$2 got=$3"; fi; }
cleanup(){ docker rm -f cws-apiserver >/dev/null 2>&1; $COMPOSE down -v >/dev/null 2>&1; }
trap cleanup EXIT

echo "== bring up the crab-watchstore compose cluster =="
$COMPOSE up -d --build >"$WORK/up.log" 2>&1 || { bad "compose up"; exit 1; }
for _ in $(seq 1 40); do
  H=$($COMPOSE ps --format '{{.Health}}' 2>/dev/null | grep -c healthy); [ "$H" = 3 ] && break; sleep 3
done
check "store healthy x3" 3 "$($COMPOSE ps --format '{{.Health}}' | grep -c healthy)"

echo "== discover the leader (apiserver gets the leader endpoint; follower read-forwarding is cw-lkq follow-up) =="
LNAME=""
for m in a b c; do
  J=$(docker exec "$(docker ps -qf name=cws-$m)" curl -s "http://cws-$m:12379/metrics" 2>/dev/null | grep "etcd_server_is_leader 1")
  [ -n "$J" ] && LNAME=$m && break
done
[ -n "$LNAME" ] && ok "leader = cws-$LNAME" || { bad "no leader found"; exit 1; }

echo "== start kube-apiserver $KVER against the store =="
openssl genrsa -out "$WORK/sa.key" 2048 >/dev/null 2>&1
openssl rsa -in "$WORK/sa.key" -pubout -out "$WORK/sa.pub" >/dev/null 2>&1
echo 'cwstoken,admin,admin,"system:masters"' > "$WORK/tokens.csv"
docker run -d --name cws-apiserver --network crab-watchstore_cws \
  -p 6443:6443 -v "$WORK":/keys \
  registry.k8s.io/kube-apiserver:"$KVER" kube-apiserver \
  --etcd-servers=http://cws-$LNAME:2379 \
  --service-cluster-ip-range=10.96.0.0/16 \
  --service-account-issuer=https://kubernetes.default.svc \
  --service-account-key-file=/keys/sa.pub \
  --service-account-signing-key-file=/keys/sa.key \
  --authorization-mode=AlwaysAllow --token-auth-file=/keys/tokens.csv \
  --cert-dir=/tmp --secure-port=6443 >/dev/null 2>&1 \
  && ok "apiserver container started" || { bad "apiserver start"; exit 1; }

A="https://127.0.0.1:6443"
K(){ curl -ks -H "Authorization: Bearer cwstoken" "$@"; }
HEALTHY=""
for _ in $(seq 1 60); do
  [ "$(K $A/healthz 2>/dev/null)" = "ok" ] && { HEALTHY=yes; break; }; sleep 2
done
[ -n "$HEALTHY" ] && ok "apiserver /healthz == ok (store-backed startup complete)" \
  || { bad "apiserver never became healthy"; docker logs cws-apiserver 2>&1 | tail -8; exit 1; }

echo "== CRUD via the Kubernetes API =="
NS=$(K $A/api/v1/namespaces -X POST -H 'Content-Type: application/json' \
  -d '{"apiVersion":"v1","kind":"Namespace","metadata":{"name":"cws-test"}}' | grep -o '"name": *"cws-test"' | head -1)
[ -n "$NS" ] && ok "namespace created" || bad "namespace create"
CM=$(K $A/api/v1/namespaces/cws-test/configmaps -X POST -H 'Content-Type: application/json' \
  -d '{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"demo"},"data":{"k":"v1"}}' | grep -o '"k": *"v1"')
[ -n "$CM" ] && ok "configmap created" || bad "configmap create"
GOT=$(K $A/api/v1/namespaces/cws-test/configmaps/demo | grep -o '"k": *"v1"')
[ -n "$GOT" ] && ok "configmap read back" || bad "configmap get"
RV=$(K $A/api/v1/namespaces/cws-test/configmaps/demo | grep -o '"resourceVersion": *"[0-9]*"' | grep -o '[0-9]*')
[ -n "$RV" ] && [ "$RV" -gt 0 ] && ok "resourceVersion present ($RV — our MVCC revision)" || bad "resourceVersion missing"
N=$(K $A/api/v1/namespaces/cws-test/configmaps | grep -c '"name": *"demo"')
[ "$N" -ge 1 ] && ok "LIST returns the configmap" || bad "LIST"

echo "== WATCH path: update arrives on a streaming watch =="
(K -N "$A/api/v1/namespaces/cws-test/configmaps?watch=1&resourceVersion=$RV" --max-time 20 >"$WORK/watch.json" 2>/dev/null &)
sleep 2
K $A/api/v1/namespaces/cws-test/configmaps/demo -X PATCH \
  -H 'Content-Type: application/merge-patch+json' -d '{"data":{"k":"v2"}}' >/dev/null
sleep 4
grep -q '"type": *"MODIFIED"' "$WORK/watch.json" && grep -q '"k": *"v2"' "$WORK/watch.json" \
  && ok "watch delivered the MODIFIED event with v2" \
  || { bad "watch event missing"; head -c 300 "$WORK/watch.json"; }

echo "== DELETE + compaction-style churn =="
K $A/api/v1/namespaces/cws-test/configmaps/demo -X DELETE >/dev/null 2>&1
D=$(K $A/api/v1/namespaces/cws-test/configmaps/demo | grep -c '"code": *404')
check "configmap deleted (404)" 1 "$D"
for i in 1 2 3 4 5; do
  K $A/api/v1/namespaces/cws-test/configmaps -X POST -H 'Content-Type: application/json' \
    -d "{\"apiVersion\":\"v1\",\"kind\":\"ConfigMap\",\"metadata\":{\"name\":\"churn-$i\"},\"data\":{\"i\":\"$i\"}}" >/dev/null
done
N=$(K $A/api/v1/namespaces/cws-test/configmaps | grep -c '"name": *"churn-')
check "churn objects listed" 5 "$N"

echo; echo "================================================================"
echo "$PASS passed, $FAIL failed"
[ "$FAIL" = 0 ] && echo "K8S APISERVER PROOF: ALL PASS — kube-apiserver runs on crab-watchstore." || exit 1
