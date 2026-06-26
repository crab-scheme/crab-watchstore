#!/usr/bin/env bash
# setup-apiserver.sh — stand up a standalone kube-apiserver + kube-controller-manager on the
# apiserver EC2 node, backed by the crab-watchstore etcd-gRPC endpoint. Run AFTER `terraform apply`.
#
# This is the fiddliest rung; treat first failures as findings about crab-watchstore<->apiserver
# compatibility, not script bugs. Iterate by re-running (it's idempotent-ish).
set -euo pipefail

TFDIR="$(cd "$(dirname "$0")/terraform" && pwd)"
KEY="$(terraform -chdir="$TFDIR" output -raw ssh_key)"
API_IP="$(terraform -chdir="$TFDIR" output -raw apiserver_ip)"
ETCD="$(terraform -chdir="$TFDIR" output -raw etcd_endpoint)"
K8S_VER="${K8S_VER:-v1.31.4}"
SSH="ssh -i $KEY -o StrictHostKeyChecking=no -o ConnectTimeout=10 ubuntu@$API_IP"

echo ">> apiserver node $API_IP, etcd backend $ETCD, k8s $K8S_VER"

# Remote bootstrap: certs + apiserver + controller-manager as containers.
$SSH "API_IP='$API_IP' ETCD='$ETCD' K8S_VER='$K8S_VER' bash -s" <<'REMOTE'
set -euxo pipefail
sudo install -d /etc/cws-k8s/certs && cd /etc/cws-k8s/certs
gen() { sudo openssl "$@"; }

if [ ! -f ca.crt ]; then
  sudo openssl genrsa -out ca.key 2048
  sudo openssl req -x509 -new -nodes -key ca.key -subj "/CN=cws-k8s-ca" -days 30 -out ca.crt
  # service-account signing key
  sudo openssl genrsa -out sa.key 2048
  sudo openssl rsa -in sa.key -pubout -out sa.pub
  # apiserver serving cert with the SANs apiserver/kubectl will hit
  cat <<EOF | sudo tee san.cnf >/dev/null
[req]
distinguished_name=dn
req_extensions=v3
[dn]
[v3]
subjectAltName=@alt
[alt]
DNS.1=kubernetes
DNS.2=kubernetes.default
DNS.3=kubernetes.default.svc
DNS.4=localhost
IP.1=127.0.0.1
IP.2=10.96.0.1
IP.3=${API_IP}
EOF
  sudo openssl genrsa -out apiserver.key 2048
  sudo openssl req -new -key apiserver.key -subj "/CN=kube-apiserver" -reqexts v3 -config san.cnf -out apiserver.csr
  sudo openssl x509 -req -in apiserver.csr -CA ca.crt -CAkey ca.key -CAcreateserial \
    -extensions v3 -extfile san.cnf -days 30 -out apiserver.crt
  # admin client cert for kubectl/controller-manager (AlwaysAllow authz; cert just authenticates)
  sudo openssl genrsa -out admin.key 2048
  sudo openssl req -new -key admin.key -subj "/CN=admin/O=system:masters" -out admin.csr
  sudo openssl x509 -req -in admin.csr -CA ca.crt -CAkey ca.key -CAcreateserial -days 30 -out admin.crt
fi

# kubeconfig for controller-manager + kubectl (talks to apiserver on localhost)
sudo bash -c 'cat >/etc/cws-k8s/certs/admin.kubeconfig' <<EOF
apiVersion: v1
kind: Config
clusters: [{name: cws, cluster: {server: https://127.0.0.1:6443, certificate-authority: /etc/cws-k8s/certs/ca.crt}}]
users: [{name: admin, user: {client-certificate: /etc/cws-k8s/certs/admin.crt, client-key: /etc/cws-k8s/certs/admin.key}}]
contexts: [{name: cws, context: {cluster: cws, user: admin}}]
current-context: cws
EOF

# --- kube-apiserver (host network so :6443 + the etcd dial work directly) ---
sudo docker rm -f kube-apiserver kube-controller-manager 2>/dev/null || true
sudo docker run -d --name kube-apiserver --restart=always --network host \
  -v /etc/cws-k8s/certs:/certs:ro \
  registry.k8s.io/kube-apiserver:${K8S_VER} kube-apiserver \
    --etcd-servers="${ETCD}" \
    --service-cluster-ip-range=10.96.0.0/24 \
    --bind-address=0.0.0.0 --secure-port=6443 --advertise-address=${API_IP} \
    --authorization-mode=AlwaysAllow \
    --tls-cert-file=/certs/apiserver.crt --tls-private-key-file=/certs/apiserver.key \
    --client-ca-file=/certs/ca.crt \
    --service-account-key-file=/certs/sa.pub \
    --service-account-signing-key-file=/certs/sa.key \
    --service-account-issuer=https://kubernetes.default.svc.cluster.local \
    --api-audiences=api \
    --request-timeout=300s \
    --allow-privileged=true \
    --disable-admission-plugins=ServiceAccount

echo ">> waiting for apiserver /healthz (interpreter + WAN boot may be slow)..."
for i in $(seq 1 60); do
  if sudo docker run --rm --network host -v /etc/cws-k8s/certs:/certs:ro \
       registry.k8s.io/kubectl:${K8S_VER} kubectl --kubeconfig /certs/admin.kubeconfig get --raw /healthz 2>/dev/null | grep -q ok; then
    echo "apiserver healthy"; break
  fi
  echo "  not ready ($i); recent apiserver log:"; sudo docker logs --tail 3 kube-apiserver 2>&1 | sed 's/^/    /'
  sleep 10
done

# --- kube-controller-manager (proves a reconcile loop against crab-watchstore) ---
sudo docker run -d --name kube-controller-manager --restart=always --network host \
  -v /etc/cws-k8s/certs:/certs:ro \
  registry.k8s.io/kube-controller-manager:${K8S_VER} kube-controller-manager \
    --kubeconfig=/certs/admin.kubeconfig \
    --service-account-private-key-file=/certs/sa.key \
    --root-ca-file=/certs/ca.crt \
    --leader-elect=false \
    --controllers=*,-nodeipam,-route,-cloud-node-lifecycle,-node-lifecycle,-ttl,-bootstrapsigner,-tokencleaner
REMOTE

echo ""
echo ">> done. drive it with:  ./smoke.sh   (or tunnel 6443 and use kubectl locally:)"
echo "   ssh -i $KEY -L 6443:127.0.0.1:6443 ubuntu@$API_IP"
