#!/usr/bin/env bash
# test/etcd-mtls-grpc.sh — prove crab-watchstore speaks etcd over MUTUAL TLS with
# REAL etcdctl (cw-u4a.21).
#
# Starts a SINGLE-NODE crab-watchstore whose etcd gRPC client port is served over
# mTLS (server cert + require-and-verify client-cert verifier built from a test CA),
# then drives the real `etcdctl` (3.6.x) against it over https, asserting:
#   (a) etcdctl WITH --cacert/--cert/--key  : put / get / range succeed over mTLS;
#   (b) etcdctl WITHOUT --cert/--key        : REJECTED at the TLS layer
#       (proves require-and-verify);
#   (c) the server saw the client identity  : (grpc-request-peer-identity h) was the
#       client cert's SAN/CN ("etcd-client"), logged by the KV handler.
#
# Certs (CA + server[SAN IP:127.0.0.1,DNS:localhost] + client[CN/SAN etcd-client])
# are generated with openssl into a temp dir, all chaining to the CA.
#
# Usage:  bash test/etcd-mtls-grpc.sh
# Env:    CRABSCHEME = path to the crabscheme binary (default below).
#         ETCDCTL    = path to etcdctl (default: etcdctl on PATH).

set -uo pipefail

# Run from the repo root so node-cluster.scm's relative includes resolve.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"
cd "$REPO_ROOT" || { echo "FATAL: cannot cd to repo root"; exit 1; }

BIN="${CRABSCHEME:-/Users/ztaylor/repos/workspaces/crabscheme/target/release/crabscheme}"
ETCDCTL="${ETCDCTL:-etcdctl}"

# unique port + db dir per run (wall-clock nanoseconds) so back-to-back runs are clean.
TAG="$(date +%s%N)"
PORT="$(( 28000 + (TAG % 4000) ))"
RAFTPORT="$(( 19000 + (TAG % 4000) ))"
DB="/tmp/cws-etcd-mtls-${TAG}"
CERTS="/tmp/cws-etcd-mtls-certs-${TAG}"
EP="https://127.0.0.1:${PORT}"
LOG="/tmp/cws-etcd-mtls-node-${TAG}.log"

PASS=0
FAIL=0
NODE_PID=""

cleanup() {
  [ -n "$NODE_PID" ] && kill "$NODE_PID" 2>/dev/null
  wait "$NODE_PID" 2>/dev/null
  rm -rf "$DB"* "$CERTS" 2>/dev/null
}
trap cleanup EXIT

ok()   { PASS=$((PASS+1)); echo "  ok   $1"; }
bad()  { FAIL=$((FAIL+1)); echo "  FAIL $1"; echo "         $2"; }

assert_contains() {
  if echo "$2" | grep -qF -- "$1"; then ok "$3"; else bad "$3" "expected to contain [$1], got: $(echo "$2" | tr '\n' '|')"; fi
}
assert_eq() {
  if [ "$1" = "$2" ]; then ok "$3"; else bad "$3" "expected [$1], got [$2]"; fi
}

# ---------------------------------------------------------------------------
echo "== generate test PKI (CA + server + client) with openssl =="
mkdir -p "$CERTS"
# CA
openssl req -x509 -newkey rsa:2048 -nodes -keyout "$CERTS/ca.key" -out "$CERTS/ca.crt" \
  -days 2 -subj "/CN=crab-watchstore test CA" \
  -addext "basicConstraints=critical,CA:TRUE" \
  -addext "keyUsage=critical,keyCertSign,cRLSign" 2>/dev/null
# server leaf: SAN IP:127.0.0.1 + DNS:localhost, serverAuth
openssl req -new -newkey rsa:2048 -nodes -keyout "$CERTS/server.key" -out "$CERTS/server.csr" \
  -subj "/CN=crab-watchstore" 2>/dev/null
openssl x509 -req -in "$CERTS/server.csr" -CA "$CERTS/ca.crt" -CAkey "$CERTS/ca.key" \
  -CAcreateserial -out "$CERTS/server.crt" -days 2 \
  -extfile <(printf "subjectAltName=IP:127.0.0.1,DNS:localhost\nextendedKeyUsage=serverAuth\nbasicConstraints=CA:FALSE\n") 2>/dev/null
# client leaf: CN + SAN DNS:etcd-client, clientAuth
openssl req -new -newkey rsa:2048 -nodes -keyout "$CERTS/client.key" -out "$CERTS/client.csr" \
  -subj "/CN=etcd-client" 2>/dev/null
openssl x509 -req -in "$CERTS/client.csr" -CA "$CERTS/ca.crt" -CAkey "$CERTS/ca.key" \
  -CAcreateserial -out "$CERTS/client.crt" -days 2 \
  -extfile <(printf "subjectAltName=DNS:etcd-client\nextendedKeyUsage=clientAuth\nbasicConstraints=CA:FALSE\n") 2>/dev/null
if [ ! -s "$CERTS/server.crt" ] || [ ! -s "$CERTS/client.crt" ]; then
  echo "FATAL: cert generation failed"; exit 1
fi
echo "  CA + server (SAN IP:127.0.0.1,DNS:localhost) + client (CN/SAN etcd-client) generated"
echo "  client cert SAN/CN:"; openssl x509 -in "$CERTS/client.crt" -noout -subject -ext subjectAltName 2>/dev/null | sed 's/^/    /'

# ---------------------------------------------------------------------------
echo
echo "== bring up single-node crab-watchstore over mTLS (etcd gRPC on $EP) =="
"$BIN" run src/node-cluster.scm -- \
  --node a --db "$DB" --cluster "a:127.0.0.1:${RAFTPORT}:${PORT}" \
  --tls-cert "$CERTS/server.crt" --tls-key "$CERTS/server.key" --tls-ca "$CERTS/ca.crt" \
  > "$LOG" 2>&1 &
NODE_PID=$!

up=0
for _ in $(seq 1 80); do
  if grep -q "serving on" "$LOG" 2>/dev/null; then up=1; break; fi
  if ! kill -0 "$NODE_PID" 2>/dev/null; then break; fi
  sleep 0.5
done
if [ "$up" != "1" ]; then
  echo "FATAL: node did not come up. log:"; cat "$LOG"; exit 1
fi
grep "shard 0 ready\|serving on" "$LOG" | sed 's/^/  /'

ECTL=( "$ETCDCTL" --endpoints="$EP" \
  --cacert="$CERTS/ca.crt" --cert="$CERTS/client.crt" --key="$CERTS/client.key" \
  --dial-timeout=5s --command-timeout=5s )

# ---------------------------------------------------------------------------
echo
echo "== (a) etcdctl OVER mTLS: put / get / range =="
out="$( "${ECTL[@]}" put foo bar 2>&1 | grep -v unrecognized )"; echo "$out" | sed 's/^/  $ etcdctl --cacert --cert --key put foo bar -> /'
assert_contains "OK" "$out" "mTLS put foo bar -> OK"
out="$( "${ECTL[@]}" get foo 2>&1 | grep -v unrecognized )"; echo "$out" | sed 's/^/  /'
assert_contains "bar" "$out" "mTLS get foo -> bar"

"${ECTL[@]}" put a 1 >/dev/null 2>&1
"${ECTL[@]}" put b 2 >/dev/null 2>&1
"${ECTL[@]}" put c 3 >/dev/null 2>&1
allout="$( "${ECTL[@]}" get "" --prefix --keys-only 2>&1 | grep -v unrecognized | grep -c . )"
echo "  mTLS range \"\" --prefix --keys-only listed $allout key line(s)"
if [ "$allout" -ge 4 ]; then ok "mTLS range over prefix lists >=4 keys (a,b,c,foo)"; else bad "mTLS range >=4 keys" "got $allout"; fi

js="$( "${ECTL[@]}" get foo -w json 2>&1 | grep -v unrecognized )"
echo "  $js"
assert_contains '"version":1' "$js" "mTLS get -w json shows version"

# ---------------------------------------------------------------------------
echo
echo "== (b) etcdctl WITHOUT a client cert is REJECTED (require-and-verify) =="
# Trusts the server CA, but presents NO client cert. The mTLS verifier must
# reject it at the TLS layer, so the RPC fails (non-zero exit + TLS/cert error).
NOCERT=( "$ETCDCTL" --endpoints="$EP" --cacert="$CERTS/ca.crt" \
  --dial-timeout=5s --command-timeout=5s )
nc_raw="$( "${NOCERT[@]}" get foo 2>&1 )"; nc_rc=$?   # capture etcdctl's own exit
nc_out="$( echo "$nc_raw" | grep -v unrecognized )"
echo "  \$ etcdctl --cacert (NO --cert/--key) get foo  [exit=$nc_rc]"
echo "$nc_out" | sed 's/^/    /'
if [ "$nc_rc" -ne 0 ] || echo "$nc_out" | grep -qiE "certificate required|bad certificate|tls|handshake|remote error|connection (refused|closed|reset)|context deadline|unavailable"; then
  ok "no-client-cert connection rejected at the TLS layer"
else
  bad "no-client-cert connection rejected" "expected a TLS/cert failure, got [exit=$nc_rc] $(echo "$nc_out" | tr '\n' '|')"
fi
# Sanity: the same op WITH the client cert still works (rules out an unrelated outage).
sanity="$( "${ECTL[@]}" get foo 2>&1 | grep -v unrecognized )"
assert_contains "bar" "$sanity" "with client cert the same get still succeeds"

# ---------------------------------------------------------------------------
echo
echo "== (c) the server saw the verified client identity (grpc-request-peer-identity) =="
# The KV handler logs 'grpc-kv: mTLS peer <identity> -> <path>' per call.
idline="$( grep -m1 "grpc-kv: mTLS peer" "$LOG" )"
echo "  node log: $idline"
assert_contains "grpc-kv: mTLS peer etcd-client" "$idline" "peer identity matches client cert SAN/CN (etcd-client)"

echo
echo "================================================================"
echo "$PASS passed, $FAIL failed"
if [ "$FAIL" -gt 0 ]; then
  echo "ETCDCTL mTLS PROOF FAILED"
  echo "--- node log tail ---"; tail -25 "$LOG"
  exit 1
fi
echo "ETCDCTL mTLS PROOF: ALL PASS — crab-watchstore speaks etcd over mutual TLS."
