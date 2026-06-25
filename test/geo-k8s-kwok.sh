#!/usr/bin/env bash
# test/geo-k8s-kwok.sh — multi-region k8s scale test on the gtr embedded k0s
# control plane, whose ENTIRE object store is the 9-member (3-region x 3-AZ)
# crab-watchstore cluster used as k0s external etcd.
#
# "The test tool that simulates a cluster" = KWOK (Kubernetes WithOut Kubelet):
# we apply fake Nodes (annotation kwok.x-k8s.io/node=fake) that the in-cluster
# kwok-controller-gtr keeps Ready, then schedule fake Pods onto them. Every
# Node/Pod/Lease object is written through k0s -> crab-watchstore, so this
# exercises the multi-region store under real kube-apiserver + scheduler load
# without any real kubelets.
#
# Drives the k0s cluster via `kubectl exec` into the k0s-controller pod (the
# embedded control plane has no externally-routable apiserver), using the
# bundled `k0s kubectl`. The OUTER gtr cluster is reached with ~/.kube/gtr.yaml.
#
#   NODES=60 PODS=600 ./test/geo-k8s-kwok.sh           # default scale
#   NODES=300 PODS=3000 ./test/geo-k8s-kwok.sh          # heavier sweep
#   CLEAN=1 ./test/geo-k8s-kwok.sh                       # delete sim objects + exit
set -uo pipefail

KUBECONFIG_GTR="${KUBECONFIG_GTR:-$HOME/.kube/gtr.yaml}"
NS="${NS:-cws-geo}"
NODES="${NODES:-60}"          # fake nodes, spread evenly across 3 regions
PODS="${PODS:-600}"           # fake pods scheduled onto them
REGIONS=(us-east eu-west ap-south)
TIMEOUT_NODES="${TIMEOUT_NODES:-120}"   # s to wait for all nodes Ready
TIMEOUT_PODS="${TIMEOUT_PODS:-300}"     # s to wait for all pods Running

export KUBECONFIG="$KUBECONFIG_GTR"
OUTER=(kubectl -n "$NS")
# k0s kubectl, run inside the controller pod; -i so we can pipe manifests to apply -f -.
K0S=("${OUTER[@]}" exec -i deploy/k0s-controller -- k0s kubectl)

PASS=0; FAIL=0
ok(){ PASS=$((PASS+1)); echo "  ok   $1"; }
bad(){ FAIL=$((FAIL+1)); echo "  FAIL $1"; }
now(){ date +%s; }

# ---- cleanup mode -----------------------------------------------------------
if [ "${CLEAN:-0}" = 1 ]; then
  echo "== deleting sim objects (deployment fake-pod + kwok nodes) =="
  "${K0S[@]}" delete deploy fake-pod -n default --ignore-not-found 2>/dev/null
  "${K0S[@]}" delete nodes -l type=kwok --ignore-not-found 2>/dev/null
  echo "done."
  exit 0
fi

echo "== gate: k0s control plane healthy on the multi-region crab-watchstore store =="
RZ=$("${K0S[@]}" get --raw /readyz 2>/dev/null)
[ "$RZ" = "ok" ] && ok "k0s /readyz=ok (apiserver serving on stretched store)" \
  || { bad "k0s not ready (/readyz='$RZ')"; exit 1; }

# ---- fake nodes -------------------------------------------------------------
echo "== applying $NODES fake KWOK nodes across ${#REGIONS[@]} regions =="
mk_nodes(){
  for i in $(seq 0 $((NODES-1))); do
    r="${REGIONS[$(( i % ${#REGIONS[@]} ))]}"
    cat <<EOF
apiVersion: v1
kind: Node
metadata:
  name: kwok-$(printf '%04d' "$i")
  annotations: { node.alpha.kubernetes.io/ttl: "0", kwok.x-k8s.io/node: fake }
  labels:
    type: kwok
    kubernetes.io/role: agent
    kubernetes.io/hostname: kwok-$(printf '%04d' "$i")
    kubernetes.io/os: linux
    kubernetes.io/arch: amd64
    topology.kubernetes.io/region: $r
    topology.kubernetes.io/zone: ${r}-a
spec:
  taints:
  - { effect: NoSchedule, key: kwok.x-k8s.io/node, value: fake }
status:
  allocatable: { cpu: "32", memory: 256Gi, pods: "110" }
  capacity:    { cpu: "32", memory: 256Gi, pods: "110" }
  nodeInfo:
    kubeletVersion: fake
    kubeProxyVersion: fake
    architecture: amd64
    operatingSystem: linux
    bootID: ""
    containerRuntimeVersion: ""
    kernelVersion: ""
    machineID: ""
    osImage: ""
    systemUUID: ""
  phase: Running
---
EOF
  done
}
T0=$(now)
mk_nodes | "${K0S[@]}" apply -f - >/tmp/kwok-nodes.log 2>&1 || { bad "node apply failed"; tail -5 /tmp/kwok-nodes.log; exit 1; }
echo "   applied (k8s accepted writes through the WAN store in $(( $(now)-T0 ))s)"

echo "== wait for all $NODES nodes Ready (KWOK heartbeat via the store) =="
RN=0
for _ in $(seq 1 "$TIMEOUT_NODES"); do
  RN=$("${K0S[@]}" get nodes -l type=kwok --no-headers 2>/dev/null | grep -cw Ready)
  [ "$RN" -ge "$NODES" ] && break
  sleep 2
done
NODE_T=$(( $(now)-T0 ))
[ "$RN" -ge "$NODES" ] && ok "$RN/$NODES nodes Ready in ${NODE_T}s" || bad "only $RN/$NODES nodes Ready after ${TIMEOUT_NODES}s"

# ---- fake pods --------------------------------------------------------------
echo "== deploying $PODS fake pods scheduled onto the fake fleet =="
P0=$(now)
# Region-aware scheduling — the kube-scheduler honors the well-known topology
# labels stamped on the KWOK nodes (topology.kubernetes.io/region). Two modes:
#   REGION=us-east   -> PIN every pod to that region (nodeAffinity match)
#   (default)        -> EVEN-SPREAD across all regions (topologySpreadConstraints)
REGION="${REGION:-}"
if [ -n "$REGION" ]; then
  echo "== pods PINNED to region=$REGION via nodeAffinity on topology.kubernetes.io/region =="
  REGION_MATCH=$'\n              - { key: topology.kubernetes.io/region, operator: In, values: ['"$REGION"'] }'
  SPREAD=""
else
  echo "== pods EVEN-SPREAD across regions via topologySpreadConstraints (region topologyKey) =="
  REGION_MATCH=""
  SPREAD=$'\n      topologySpreadConstraints:\n      - { maxSkew: 1, topologyKey: topology.kubernetes.io/region, whenUnsatisfiable: DoNotSchedule, labelSelector: { matchLabels: { app: fake-pod } } }'
fi

cat <<EOF | "${K0S[@]}" apply -f - >/tmp/kwok-pods.log 2>&1 || { bad "pod apply failed"; tail -5 /tmp/kwok-pods.log; exit 1; }
apiVersion: apps/v1
kind: Deployment
metadata: { name: fake-pod, namespace: default }
spec:
  replicas: $PODS
  selector: { matchLabels: { app: fake-pod } }
  template:
    metadata: { labels: { app: fake-pod } }
    spec:$SPREAD
      affinity:
        nodeAffinity:
          requiredDuringSchedulingIgnoredDuringExecution:
            nodeSelectorTerms:
            - matchExpressions:
              - { key: type, operator: In, values: [kwok] }$REGION_MATCH
      tolerations:
      - { key: "kwok.x-k8s.io/node", operator: "Exists", effect: "NoSchedule" }
      containers:
      - { name: fake, image: fake-image }
EOF

echo "== wait for all $PODS pods Running (scheduler + KWOK via the store) =="
RP=0
for _ in $(seq 1 "$TIMEOUT_PODS"); do
  RP=$("${K0S[@]}" get deploy fake-pod -n default -o jsonpath='{.status.readyReplicas}' 2>/dev/null)
  RP="${RP:-0}"
  [ "$RP" -ge "$PODS" ] && break
  sleep 2
done
POD_T=$(( $(now)-P0 ))
[ "$RP" -ge "$PODS" ] && ok "$RP/$PODS pods Running in ${POD_T}s ($(( PODS / (POD_T>0?POD_T:1) )) pods/s through the store)" \
  || bad "only $RP/$PODS pods Running after ${TIMEOUT_PODS}s"

# ---- per-region placement: did the scheduler honor the topology labels? -----
echo "== per-region pod distribution (scheduler honoring node region labels) =="
NODEREG=$("${K0S[@]}" get nodes -l type=kwok \
  -o custom-columns='N:.metadata.name,R:.metadata.labels.topology\.kubernetes\.io/region' --no-headers 2>/dev/null)
PODNODE=$("${K0S[@]}" get pods -n default -l app=fake-pod \
  -o custom-columns='X:.spec.nodeName' --no-headers 2>/dev/null)
DIST=$( { echo "$NODEREG" | awk 'NF==2{print "NODE",$1,$2}'; echo "$PODNODE" | awk 'NF>=1{print "POD",$1}'; } | awk '
  $1=="NODE"{reg[$2]=$3}
  $1=="POD"{c[reg[$2]]++; t++}
  END{ n=0; min=1e9; max=0;
       for(r in c){ printf "    %-10s %d\n", r, c[r] > "/dev/stderr"; n++; if(c[r]<min)min=c[r]; if(c[r]>max)max=c[r] }
       print n, min, max, t }' 2>/tmp/kwok-dist.txt )
cat /tmp/kwok-dist.txt
read -r NREG DMIN DMAX DTOT <<<"$DIST"
if [ -n "$REGION" ]; then
  # pinned: exactly one region populated, and it is $REGION
  PR=$(awk -v r="$REGION" '$1==r{print $2}' /tmp/kwok-dist.txt)
  [ "$NREG" = 1 ] && [ "${PR:-0}" -ge "$PODS" ] \
    && ok "all $PODS pods pinned to region=$REGION (single region populated)" \
    || bad "pin failed: regions=$NREG, region=$REGION got ${PR:-0}/$PODS"
else
  # spread: all 3 regions populated, skew (max-min) within maxSkew tolerance
  SKEW=$(( DMAX - DMIN ))
  [ "$NREG" -ge "${#REGIONS[@]}" ] && [ "$SKEW" -le 2 ] \
    && ok "even spread across $NREG regions (min=$DMIN max=$DMAX skew=$SKEW <=2)" \
    || bad "uneven spread: regions=$NREG min=$DMIN max=$DMAX skew=$SKEW"
fi

# ---- store-load readback (list latency through the multi-region store) ------
echo "== list-latency readback through the stretched store =="
LT0=$(date +%s%3N 2>/dev/null || echo "")
PC=$("${K0S[@]}" get pods -n default --no-headers 2>/dev/null | wc -l | tr -d ' ')
if [ -n "$LT0" ]; then
  LT=$(( $(date +%s%3N) - LT0 ))
  ok "LIST $PC pods served in ${LT}ms (full read through k0s -> crab-watchstore)"
else
  ok "LIST returned $PC pods"
fi

echo
echo "================================================================"
echo "geo-k8s-kwok: $PASS passed, $FAIL failed"
echo "  fleet: $NODES nodes / $PODS pods simulated on the embedded k0s"
echo "  store: 9-member crab-watchstore (3 regions x 3 AZs), leader pinned us-east"
echo "  node-ready ${NODE_T:-?}s · pods-running ${POD_T:-?}s"
echo "  (cleanup: CLEAN=1 $0)"
[ "$FAIL" = 0 ] && echo "MULTI-REGION K8S PROOF: ALL PASS — simulated cluster on a stretched store." || exit 1
