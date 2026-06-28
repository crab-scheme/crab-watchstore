#!/usr/bin/env bash
# smoke.sh — the kubectl ladder against the apiserver (which is backed by crab-watchstore).
# Runs kubectl ON the apiserver node (via ssh) so it hits https://127.0.0.1:6443.
set -uo pipefail

TFDIR="$(cd "$(dirname "$0")/terraform" && pwd)"
KEY="$TFDIR/cws-smoke.pem"
API_IP="$(terraform -chdir="$TFDIR" output -raw apiserver_ip)"
K8S_VER="${K8S_VER:-v1.31.4}"
SSH="ssh -i $KEY -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR ubuntu@$API_IP"
KC="sudo docker run --rm --network host -v /etc/cws-k8s/certs:/certs:ro registry.k8s.io/kubectl:${K8S_VER} kubectl --kubeconfig /certs/admin.kubeconfig"

step() { echo ""; echo "=== $1 ==="; }
run()  { $SSH "$KC $*"; }

step "1. apiserver health (the hard gate — boot writes to crab-watchstore)"
run get --raw /healthz?verbose | tail -5 || { echo "apiserver not healthy; check: $SSH 'sudo docker logs --tail 50 kube-apiserver'"; exit 1; }

step "2. namespaces apiserver bootstrapped INTO crab-watchstore"
run get namespaces

step "3. create + read a Deployment (persisted in crab-watchstore)"
run create deployment nginx --image=nginx 2>/dev/null || echo "(already exists)"
run get deploy,rs -o wide

step "4. watch path through crab-watchstore (5s window while we mutate)"
$SSH "$KC get events -A --watch-only & WPID=\$!; sleep 1; \
      $KC label deployment nginx smoke=ok --overwrite >/dev/null 2>&1; \
      $KC scale deployment nginx --replicas=2 >/dev/null 2>&1; \
      sleep 4; kill \$WPID 2>/dev/null" || true

step "5. controller reconcile (Deployment -> ReplicaSet -> Pods Pending, no kubelet)"
run get deploy,rs,pods

echo ""
echo "PASS criteria: /healthz ok, 4 namespaces, the Deployment + a controller-created ReplicaSet,"
echo "and watch events streaming. Pods Pending is EXPECTED (no kubelet)."
