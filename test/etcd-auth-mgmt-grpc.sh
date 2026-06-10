#!/usr/bin/env bash
# test/etcd-auth-mgmt-grpc.sh — prove crab-watchstore's full Auth management surface
# with REAL etcdctl (3.6.x) over the gRPC Auth service (cw-u4a.27).
#
# Tests the 9 new RPCs beyond .26's 7:
#   UserList / UserGet / UserChangePassword (+ re-auth) /
#   RoleList / RoleGet /
#   RoleRevokePermission (then alice is denied) /
#   UserRevokeRole (then alice loses all access) /
#   RoleDelete / UserDelete
#
# Usage:  bash test/etcd-auth-mgmt-grpc.sh
# Env:    CRABSCHEME = path to the crabscheme binary (default below).
#         ETCDCTL    = path to etcdctl (default: etcdctl on PATH).

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"
cd "$REPO_ROOT" || { echo "FATAL: cannot cd to repo root"; exit 1; }

BIN="${CRABSCHEME:-/Users/ztaylor/repos/workspaces/crabscheme/target/release/crabscheme}"
ETCDCTL="${ETCDCTL:-etcdctl}"

TAG="$(date +%s%N)"
PORT="$(( 26000 + (TAG % 3000) ))"
RAFTPORT="$(( 20000 + (TAG % 3000) ))"
DB="/tmp/cws-etcd-auth-mgmt-${TAG}"
EP="127.0.0.1:${PORT}"
LOG="/tmp/cws-etcd-auth-mgmt-node-${TAG}.log"

ECTL=( "$ETCDCTL" --endpoints="$EP" --insecure-transport=true --dial-timeout=5s --command-timeout=5s )
ROOT=( "$ETCDCTL" --endpoints="$EP" --insecure-transport=true --dial-timeout=5s --command-timeout=5s --user root:rootpw )

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

assert_has() {
  if echo "$2" | grep -qiF -- "$1"; then ok "$3"; else bad "$3" "expected [$1], got: $(echo "$2" | tr '\n' '|')"; fi
}
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
"${ECTL[@]}" user add root:rootpw > /dev/null 2>&1
"${ECTL[@]}" role add root         > /dev/null 2>&1
"${ECTL[@]}" user grant-role root root > /dev/null 2>&1
out="$( "${ECTL[@]}" auth enable 2>&1 | grep -v 'level":"warn' )"
assert_has "Authentication Enabled" "$out" "auth enable -> Authentication Enabled"

# ---------------------------------------------------------------------------
echo
echo "== as root (auth ON): create alice + dev role (readwrite app/ --prefix) + grant =="
"${ROOT[@]}" user add alice:alicepw > /dev/null 2>&1
"${ROOT[@]}" role add dev           > /dev/null 2>&1
"${ROOT[@]}" role grant-permission dev readwrite app/ --prefix > /dev/null 2>&1
"${ROOT[@]}" user grant-role alice dev > /dev/null 2>&1

# seed a KV entry alice can read (proves access before we revoke)
"${ROOT[@]}" put app/x initval > /dev/null 2>&1

ALICE=( "$ETCDCTL" --endpoints="$EP" --insecure-transport=true --dial-timeout=5s --command-timeout=5s --user alice:alicepw )

# ---------------------------------------------------------------------------
echo
echo "== UserList / UserGet =="
out="$( "${ROOT[@]}" user list 2>&1 | grep -v 'level":"warn' )"
echo "$out" | sed 's/^/  $ etcdctl --user root user list -> /'
assert_has "root"  "$out" "user list contains root"
assert_has "alice" "$out" "user list contains alice"

out="$( "${ROOT[@]}" user get alice 2>&1 | grep -v 'level":"warn' )"
echo "$out" | sed 's/^/  $ etcdctl --user root user get alice -> /'
assert_has "alice" "$out" "user get alice -> name present"
assert_has "dev"   "$out" "user get alice -> dev role listed"

# ---------------------------------------------------------------------------
echo
echo "== RoleList / RoleGet =="
out="$( "${ROOT[@]}" role list 2>&1 | grep -v 'level":"warn' )"
echo "$out" | sed 's/^/  $ etcdctl --user root role list -> /'
assert_has "root" "$out" "role list contains root"
assert_has "dev"  "$out" "role list contains dev"

out="$( "${ROOT[@]}" role get dev 2>&1 | grep -v 'level":"warn' )"
echo "$out" | sed 's/^/  $ etcdctl --user root role get dev -> /'
assert_has "dev"  "$out" "role get dev -> name"
assert_has "app/" "$out" "role get dev -> key app/"

# ---------------------------------------------------------------------------
echo
echo "== UserChangePassword: alice re-auth =="
# prove alice can get app/x with OLD password first
out="$( "${ALICE[@]}" get app/x 2>&1 | grep -v 'level":"warn' )"; echo "$out" | sed 's/^/  $ alice:alicepw get app\/x -> /'
assert_has "initval" "$out" "alice:alicepw get app/x -> ok before passwd change"

# change alice's password as root; etcdctl prompts twice (password + confirmation)
out="$( printf 'newpw\nnewpw\n' | "${ROOT[@]}" user passwd alice 2>&1 | grep -v 'level":"warn' )"
echo "$out" | sed 's/^/  $ etcdctl --user root user passwd alice -> /'
assert_has "Password updated" "$out" "user passwd alice -> Password updated"

# old password must fail
out="$( "${ALICE[@]}" get app/x 2>&1 | grep -v 'level":"warn' )"; echo "$out" | sed 's/^/  $ alice:alicepw (OLD) get app\/x after passwd -> /'
assert_has "authentication failed" "$out" "alice:alicepw (old) -> auth failed after passwd change"

# new password must succeed
ALICE_NEW=( "$ETCDCTL" --endpoints="$EP" --insecure-transport=true --dial-timeout=5s --command-timeout=5s --user alice:newpw )
out="$( "${ALICE_NEW[@]}" get app/x 2>&1 | grep -v 'level":"warn' )"; echo "$out" | sed 's/^/  $ alice:newpw get app\/x -> /'
assert_has "initval" "$out" "alice:newpw get app/x -> ok after passwd change"

# ---------------------------------------------------------------------------
echo
echo "== RoleRevokePermission: alice (via dev) is now denied app/ =="
out="$( "${ROOT[@]}" role revoke-permission dev app/ --prefix 2>&1 | grep -v 'level":"warn' )"
echo "$out" | sed 's/^/  $ etcdctl --user root role revoke-permission dev app\/ -> /'
assert_has "dev" "$out" "role revoke-permission dev app/ -> dev updated"

out="$( "${ALICE_NEW[@]}" get app/x 2>&1 | grep -v 'level":"warn' )"; echo "$out" | sed 's/^/  $ alice:newpw get app\/x after perm revoke -> /'
assert_has "permission denied" "$out" "alice get app/x -> permission denied after revoke-perm"

# ---------------------------------------------------------------------------
echo
echo "== UserRevokeRole: alice loses the dev role itself =="
out="$( "${ROOT[@]}" user revoke-role alice dev 2>&1 | grep -v 'level":"warn' )"
echo "$out" | sed 's/^/  $ etcdctl --user root user revoke-role alice dev -> /'
assert_has "dev" "$out" "user revoke-role alice dev -> dev revoked"

# confirm dev is gone from alice's profile
out="$( "${ROOT[@]}" user get alice 2>&1 | grep -v 'level":"warn' )"
echo "$out" | sed 's/^/  $ user get alice after revoke-role -> /'
assert_hasnot "dev" "$out" "user get alice after revoke-role -> dev absent"

# ---------------------------------------------------------------------------
echo
echo "== RoleDelete + UserDelete =="
out="$( "${ROOT[@]}" role delete dev 2>&1 | grep -v 'level":"warn' )"
echo "$out" | sed 's/^/  $ etcdctl --user root role delete dev -> /'
assert_has "dev" "$out" "role delete dev -> dev deleted"

out="$( "${ROOT[@]}" role list 2>&1 | grep -v 'level":"warn' )"
echo "$out" | sed 's/^/  $ role list after role delete -> /'
assert_hasnot "dev"  "$out" "role list -> dev absent after delete"

out="$( "${ROOT[@]}" user delete alice 2>&1 | grep -v 'level":"warn' )"
echo "$out" | sed 's/^/  $ etcdctl --user root user delete alice -> /'
assert_has "alice" "$out" "user delete alice -> alice deleted"

out="$( "${ROOT[@]}" user list 2>&1 | grep -v 'level":"warn' )"
echo "$out" | sed 's/^/  $ user list after user delete -> /'
assert_hasnot "alice" "$out" "user list -> alice absent after delete"

# ---------------------------------------------------------------------------
echo
echo "================================================================"
echo "$PASS passed, $FAIL failed"
if [ "$FAIL" -gt 0 ]; then
  echo "ETCDCTL AUTH MGMT PROOF FAILED"
  echo "--- node log tail ---"; tail -30 "$LOG"
  exit 1
fi
echo "ETCDCTL AUTH MGMT PROOF: ALL PASS — full etcd Auth management service complete."
