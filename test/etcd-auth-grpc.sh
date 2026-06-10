#!/usr/bin/env bash
# test/etcd-auth-grpc.sh — prove crab-watchstore enforces etcd auth/RBAC with REAL
# etcdctl (3.6.x) over the gRPC Auth service (cw-u4a.26).
#
# Brings up a SINGLE-NODE crab-watchstore (1 voter = always leader, h2c gRPC) and
# drives the real `etcdctl` auth flow:
#   - bootstrap (auth OFF): user add root / role add root / grant-role / auth enable
#   - as root (auth ON): user add alice / role add dev / grant-permission dev
#       readwrite app/ --prefix / grant-role alice dev
#   - ENFORCEMENT: alice within app/ -> OK ; alice beyond app/ -> permission denied ;
#       no creds -> user-name-empty/unauth ; wrong password -> auth failed ;
#       root -> allowed everywhere ; then auth disable -> no-creds works again.
#
# Usage:  bash test/etcd-auth-grpc.sh
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
PORT="$(( 26000 + (TAG % 3000) ))"
RAFTPORT="$(( 20000 + (TAG % 3000) ))"
DB="/tmp/cws-etcd-auth-${TAG}"
EP="127.0.0.1:${PORT}"
LOG="/tmp/cws-etcd-auth-node-${TAG}.log"
# base etcdctl (no creds); ROOT/ALICE add --user.
ECTL=(  "$ETCDCTL" --endpoints="$EP" --insecure-transport=true --dial-timeout=5s --command-timeout=5s )
ROOT=(  "$ETCDCTL" --endpoints="$EP" --insecure-transport=true --dial-timeout=5s --command-timeout=5s --user root:rootpw )
ALICE=( "$ETCDCTL" --endpoints="$EP" --insecure-transport=true --dial-timeout=5s --command-timeout=5s --user alice:alicepw )

PASS=0
FAIL=0
NODE_PID=""

cleanup() {
  [ -n "$NODE_PID" ] && kill "$NODE_PID" 2>/dev/null
  wait "$NODE_PID" 2>/dev/null
  rm -rf "$DB"* 2>/dev/null
}
trap cleanup EXIT

ok()   { PASS=$((PASS+1)); echo "  ok   $1"; }
bad()  { FAIL=$((FAIL+1)); echo "  FAIL $1"; echo "         $2"; }

# assert that "$2" (actual) contains "$1" (needle, case-insensitive); label "$3".
assert_has() {
  if echo "$2" | grep -qiF -- "$1"; then ok "$3"; else bad "$3" "expected to contain [$1], got: $(echo "$2" | tr '\n' '|')"; fi
}
# assert that "$2" (actual) does NOT contain "$1"; label "$3".
assert_hasnot() {
  if echo "$2" | grep -qiF -- "$1"; then bad "$3" "did NOT expect [$1], got: $(echo "$2" | tr '\n' '|')"; else ok "$3"; fi
}

echo "== bring up single-node crab-watchstore (etcd gRPC on $EP) =="
"$BIN" run src/node-cluster.scm -- \
  --node a --db "$DB" --cluster "a:127.0.0.1:${RAFTPORT}:${PORT}" > "$LOG" 2>&1 &
NODE_PID=$!

up=0
for _ in $(seq 1 80); do
  if grep -q "etcd KV gRPC serving on" "$LOG" 2>/dev/null; then up=1; break; fi
  if ! kill -0 "$NODE_PID" 2>/dev/null; then break; fi
  sleep 0.5
done
if [ "$up" != "1" ]; then
  echo "FATAL: node did not come up. log:"; cat "$LOG"; exit 1
fi
grep "shard 0 ready\|etcd KV gRPC serving on" "$LOG" | sed 's/^/  /'

# ---------------------------------------------------------------------------
echo
echo "== bootstrap (auth OFF): root user + root role + grant + auth enable =="
out="$( "${ECTL[@]}" user add root:rootpw 2>&1 )";          echo "$out" | sed 's/^/  $ etcdctl user add root:rootpw -> /'
assert_has "User root created" "$out" "user add root -> created"
out="$( "${ECTL[@]}" role add root 2>&1 )";                 echo "$out" | sed 's/^/  $ etcdctl role add root -> /'
assert_has "Role root created" "$out" "role add root -> created"
out="$( "${ECTL[@]}" user grant-role root root 2>&1 )";     echo "$out" | sed 's/^/  $ etcdctl user grant-role root root -> /'
assert_has "granted" "$out" "grant-role root root -> granted"
out="$( "${ECTL[@]}" auth enable 2>&1 | grep -v 'level":"warn' )"; echo "$out" | sed 's/^/  $ etcdctl auth enable -> /'
assert_has "Authentication Enabled" "$out" "auth enable -> Authentication Enabled"

# ---------------------------------------------------------------------------
echo
echo "== as root (auth ON): create alice + dev role (readwrite app/ --prefix) =="
out="$( "${ROOT[@]}" user add alice:alicepw 2>&1 )";        echo "$out" | sed 's/^/  $ etcdctl --user root user add alice -> /'
assert_has "User alice created" "$out" "root: user add alice -> created"
out="$( "${ROOT[@]}" role add dev 2>&1 )";                  echo "$out" | sed 's/^/  $ etcdctl --user root role add dev -> /'
assert_has "Role dev created" "$out" "root: role add dev -> created"
out="$( "${ROOT[@]}" role grant-permission dev readwrite app/ --prefix 2>&1 )"; echo "$out" | sed 's/^/  $ etcdctl --user root role grant-permission dev readwrite app\/ --prefix -> /'
assert_has "Role dev updated" "$out" "root: grant-permission dev readwrite app/ --prefix"
out="$( "${ROOT[@]}" user grant-role alice dev 2>&1 )";     echo "$out" | sed 's/^/  $ etcdctl --user root user grant-role alice dev -> /'
assert_has "granted" "$out" "root: grant-role alice dev -> granted"

# ---------------------------------------------------------------------------
echo
echo "== ENFORCEMENT: alice WITHIN app/ prefix is allowed (token auth) =="
out="$( "${ALICE[@]}" put app/x 1 2>&1 | grep -v 'level":"warn' )"; echo "$out" | sed 's/^/  $ etcdctl --user alice put app\/x 1 -> /'
assert_has "OK" "$out" "alice put app/x 1 -> OK"
out="$( "${ALICE[@]}" get app/x 2>&1 | grep -v 'level":"warn' )";   echo "$out" | sed 's/^/  $ etcdctl --user alice get app\/x -> /'
assert_has "app/x" "$out" "alice get app/x -> key"
assert_has "1"     "$out" "alice get app/x -> value 1"

echo
echo "== ENFORCEMENT: alice BEYOND app/ prefix is DENIED (PermissionDenied) =="
out="$( "${ALICE[@]}" put other/y 2 2>&1 | grep -v 'level":"warn' )"; echo "$out" | sed 's/^/  $ etcdctl --user alice put other\/y 2 -> /'
assert_has "permission denied" "$out" "alice put other/y -> permission denied"
assert_hasnot "OK" "$out" "alice put other/y did NOT succeed"
# read beyond perms is denied too
out="$( "${ALICE[@]}" get other/y 2>&1 | grep -v 'level":"warn' )"; echo "$out" | sed 's/^/  $ etcdctl --user alice get other\/y -> /'
assert_has "permission denied" "$out" "alice get other/y -> permission denied"

echo
echo "== ENFORCEMENT: NO credentials with auth enabled is rejected =="
out="$( "${ECTL[@]}" put app/x 1 2>&1 | grep -v 'level":"warn' )"; echo "$out" | sed 's/^/  $ etcdctl (no --user) put app\/x 1 -> /'
assert_hasnot "OK" "$out" "no-creds put did NOT succeed"
if echo "$out" | grep -qiE "user name is empty|invalid auth token|user name empty|authentication|unauthenticated|permission denied"; then
  ok "no-creds put -> auth required (rejected)"
else
  bad "no-creds put -> auth required" "got: $(echo "$out" | tr '\n' '|')"
fi

echo
echo "== ENFORCEMENT: WRONG password fails Authenticate =="
out="$( "$ETCDCTL" --endpoints="$EP" --insecure-transport=true --dial-timeout=5s --command-timeout=5s \
        --user alice:wrongpw get app/x 2>&1 | grep -v 'level":"warn' )"; echo "$out" | sed 's/^/  $ etcdctl --user alice:wrongpw get app\/x -> /'
assert_has "authentication failed" "$out" "wrong password -> authentication failed"

echo
echo "== ENFORCEMENT: root is allowed EVERYWHERE (root role override) =="
out="$( "${ROOT[@]}" put anywhere 1 2>&1 | grep -v 'level":"warn' )"; echo "$out" | sed 's/^/  $ etcdctl --user root put anywhere 1 -> /'
assert_has "OK" "$out" "root put anywhere -> OK"
out="$( "${ROOT[@]}" get other/y 2>&1 | grep -v 'level":"warn' )";   echo "$out" | sed 's/^/  $ etcdctl --user root get other\/y -> /'
assert_hasnot "permission denied" "$out" "root get other/y -> NOT denied"

# ---------------------------------------------------------------------------
echo
echo "== auth DISABLE (as root): no-creds writes work again =="
out="$( "${ROOT[@]}" auth disable 2>&1 | grep -v 'level":"warn' )"; echo "$out" | sed 's/^/  $ etcdctl --user root auth disable -> /'
assert_has "Authentication Disabled" "$out" "auth disable -> Authentication Disabled"
out="$( "${ECTL[@]}" put nocreds ok 2>&1 | grep -v 'level":"warn' )"; echo "$out" | sed 's/^/  $ etcdctl (no --user) put nocreds ok -> /'
assert_has "OK" "$out" "after disable: no-creds put -> OK"
out="$( "${ECTL[@]}" get nocreds 2>&1 | grep -v 'level":"warn' )";   echo "$out" | sed 's/^/  $ etcdctl (no --user) get nocreds -> /'
assert_has "ok" "$out" "after disable: no-creds get -> value"

# ---------------------------------------------------------------------------
echo
echo "================================================================"
echo "$PASS passed, $FAIL failed"
if [ "$FAIL" -gt 0 ]; then
  echo "ETCDCTL AUTH PROOF FAILED"
  echo "--- node log tail ---"; tail -25 "$LOG"
  exit 1
fi
echo "ETCDCTL AUTH PROOF: ALL PASS — crab-watchstore enforces etcd RBAC."
