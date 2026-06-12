#!/usr/bin/env bash
# test/geo-k8s-e2e.sh — cw-lkq.10: the geographic-aware Kubernetes e2e.
#
# The WAN compose overlay (3 members = 3 regions at 150ms RTT, leader pinned
# to "us-east"/cws-a) backs a REAL kube-apiserver. Drill:
#   1. apiserver healthy on the stretched store; objects created with region
#      topology labels (the scheduler inputs);
#   2. REGION LOSS: SIGKILL the us-east member (the pinned leader's region);
#      measure RTO = time until the apiserver serves a WRITE again (the
#      surviving regions re-elect; client fails over);
#   3. post-loss: data intact, watch delivers, region restarts + leadership
#      auto-returns to us-east (pinning).
set -uo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
COMPOSE="docker compose -f $ROOT/deploy/docker/docker-compose.yml -f $ROOT/deploy/docker/wan.override.yml"
KVER="${KVER:-v1.30.0}"
WORK=/tmp/cws-geo; rm -rf "$WORK"; mkdir -p "$WORK"
PASS=0; FAIL=0
ok(){ PASS=$((PASS+1)); echo "  ok   $1"; }
bad(){ FAIL=$((FAIL+1)); echo "  FAIL $1"; }
cleanup(){ docker rm -f cws-apiserver >/dev/null 2>&1; $COMPOSE down -v >/dev/null 2>&1; }
trap cleanup EXIT

echo "== WAN store up (regions: a=us-east b=eu-west c=ap-south, leader pinned us-east) =="
WAN_DELAY_MS=75 LEADER_REGION=us-east $COMPOSE up -d --build >"$WORK/up.log" 2>&1 || { bad "compose up"; exit 1; }
for _ in $(seq 1 60); do H=$($COMPOSE ps --format '{{.Health}}' | grep -c healthy); [ "$H" = 3 ] && break; sleep 3; done
[ "$($COMPOSE ps --format '{{.Health}}' | grep -c healthy)" = 3 ] && ok "3 regions healthy" || { bad "store"; exit 1; }

echo "== wait for leadership to settle in the pinned region (us-east) =="
SETTLED=""
for _ in $(seq 1 30); do
  docker exec "$(docker ps -qf name=cws-a)" curl -s http://cws-a:12379/metrics 2>/dev/null     | grep -q "etcd_server_is_leader 1" && { SETTLED=y; break; }
  sleep 3
done
[ -n "$SETTLED" ] && ok "leader settled in us-east (pinning)" || bad "leader never settled in us-east"
sleep 5

echo "== kube-apiserver on the stretched store (all endpoints) =="
openssl genrsa -out "$WORK/sa.key" 2048 >/dev/null 2>&1
openssl rsa -in "$WORK/sa.key" -pubout -out "$WORK/sa.pub" >/dev/null 2>&1
echo 'cwstoken,admin,admin,"system:masters"' > "$WORK/tokens.csv"
docker run -d --name cws-apiserver --network crab-watchstore_cws -p 6443:6443 -v "$WORK":/keys \
  registry.k8s.io/kube-apiserver:"$KVER" kube-apiserver \
  --etcd-servers=http://cws-a:2379,http://cws-b:2379,http://cws-c:2379 \
  --service-cluster-ip-range=10.96.0.0/16 \
  --service-account-issuer=https://kubernetes.default.svc \
  --service-account-key-file=/keys/sa.pub --service-account-signing-key-file=/keys/sa.key \
  --authorization-mode=AlwaysAllow --token-auth-file=/keys/tokens.csv \
  --cert-dir=/tmp --secure-port=6443 >/dev/null 2>&1 || { bad "apiserver start"; exit 1; }
A="https://127.0.0.1:6443"
K(){ curl -ks -H "Authorization: Bearer cwstoken" "$@"; }
HEALTHY=""; for _ in $(seq 1 200); do [ "$(K $A/healthz 2>/dev/null)" = "ok" ] && { HEALTHY=y; break; }; sleep 2; done
[ -n "$HEALTHY" ] && ok "apiserver healthy on the WAN store" || { bad "apiserver"; docker logs cws-apiserver 2>&1|tail -4; exit 1; }

echo "== geo topology objects =="
K $A/api/v1/namespaces -X POST -H 'Content-Type: application/json' \
  -d '{"apiVersion":"v1","kind":"Namespace","metadata":{"name":"geo"}}' >/dev/null
for r in us-east eu-west ap-south; do
  K $A/api/v1/nodes -X POST -H 'Content-Type: application/json' \
    -d "{\"apiVersion\":\"v1\",\"kind\":\"Node\",\"metadata\":{\"name\":\"node-$r\",\"labels\":{\"topology.kubernetes.io/region\":\"$r\"}}}" >/dev/null
done
N=$(K "$A/api/v1/nodes?labelSelector=topology.kubernetes.io%2Fregion%3Deu-west" | grep -c '"name": *"node-eu-west"')
[ "$N" -ge 1 ] && ok "region label selector resolves (scheduler input)" || bad "label selector"

echo "== REGION LOSS drill: kill us-east (pinned leader region) =="
T0=$(date +%s)
docker kill -s KILL "$(docker ps -qf name=cws-a)" >/dev/null 2>&1
RTO=""
for i in $(seq 1 120); do
  if K $A/api/v1/namespaces/geo/configmaps -X POST -H 'Content-Type: application/json' \
       -d "{\"apiVersion\":\"v1\",\"kind\":\"ConfigMap\",\"metadata\":{\"name\":\"rto-probe-$i\"},\"data\":{\"t\":\"$i\"}}" 2>/dev/null \
       | grep -q '"rto-probe-'; then RTO=$(( $(date +%s) - T0 )); break; fi
  sleep 1
done
[ -n "$RTO" ] && ok "WRITES recovered after region loss: RTO = ${RTO}s" || { bad "writes never recovered"; }

echo "== post-loss integrity + watch =="
N=$(K $A/api/v1/nodes | grep -c '"node-')
[ "$N" -ge 3 ] && ok "pre-loss objects intact ($N nodes listed)" || bad "objects lost"
(K -N "$A/api/v1/namespaces/geo/configmaps?watch=1" --max-time 15 >"$WORK/w.json" 2>/dev/null &)
sleep 2
K $A/api/v1/namespaces/geo/configmaps -X POST -H 'Content-Type: application/json' \
  -d '{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"after-loss"},"data":{"k":"v"}}' >/dev/null
sleep 4
grep -q '"after-loss"' "$WORK/w.json" && ok "watch delivers after region loss" || bad "watch dead after loss"

echo "== region recovery + leadership returns to us-east =="
docker start "$(docker ps -aqf name=cws-a)" >/dev/null 2>&1 || WAN_DELAY_MS=75 $COMPOSE up -d >/dev/null 2>&1
BACK=""
for _ in $(seq 1 60); do
  docker exec "$(docker ps -qf name=cws-a)" curl -s http://cws-a:12379/metrics 2>/dev/null | grep -q "etcd_server_is_leader 1" && { BACK=y; break; }
  sleep 3
done
[ -n "$BACK" ] && ok "leadership auto-returned to us-east after recovery (pinning)" \
  || bad "leadership did not return to us-east"

echo; echo "================================================================"
echo "$PASS passed, $FAIL failed"
[ "$FAIL" = 0 ] && echo "GEO K8S PROOF: ALL PASS — geographic-aware Kubernetes on a multi-region store." || exit 1
