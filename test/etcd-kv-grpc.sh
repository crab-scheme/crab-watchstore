#!/usr/bin/env bash
# test/etcd-kv-grpc.sh — prove crab-watchstore speaks etcd with REAL etcdctl (cw-u4a.22).
#
# Starts a SINGLE-NODE crab-watchstore (1 voter = always leader, h2c gRPC client
# port, a wall-clock-unique temp RocksDB) and drives the real `etcdctl` (3.6.x)
# binary against it over cleartext (--insecure-transport), asserting the etcd KV
# surface: put / get / get -w json (revisions+version) / range+prefix / --limit /
# historical --rev / del / --keys-only / --count-only / txn (success+failure) /
# compact (ErrCompacted on a read below the floor).
#
# Usage:  bash test/etcd-kv-grpc.sh
# Env:    CRABSCHEME = path to the crabscheme binary (default below).
#         ETCDCTL    = path to etcdctl (default: etcdctl on PATH).

set -uo pipefail

BIN="${CRABSCHEME:-/Users/ztaylor/repos/workspaces/crabscheme/target/release/crabscheme}"
ETCDCTL="${ETCDCTL:-etcdctl}"

# unique port + db dir per run (wall-clock nanoseconds) so back-to-back runs are clean.
TAG="$(date +%s%N)"
PORT="$(( 24000 + (TAG % 4000) ))"
RAFTPORT="$(( 18000 + (TAG % 4000) ))"
DB="/tmp/cws-etcd-kv-${TAG}"
EP="127.0.0.1:${PORT}"
LOG="/tmp/cws-etcd-kv-node-${TAG}.log"
ECTL=( "$ETCDCTL" --endpoints="$EP" --insecure-transport=true --dial-timeout=5s --command-timeout=5s )

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

# assert that "$2" (actual) contains "$1" (needle); label "$3".
assert_contains() {
  if echo "$2" | grep -qF -- "$1"; then ok "$3"; else bad "$3" "expected to contain [$1], got: $(echo "$2" | tr '\n' '|')"; fi
}
# assert exact-equality of a single value.
assert_eq() {
  if [ "$1" = "$2" ]; then ok "$3"; else bad "$3" "expected [$1], got [$2]"; fi
}

echo "== bring up single-node crab-watchstore (etcd KV gRPC on $EP) =="
"$BIN" run src/node-cluster.scm -- \
  --node a --db "$DB" --cluster "a:127.0.0.1:${RAFTPORT}:${PORT}" > "$LOG" 2>&1 &
NODE_PID=$!

# wait for the serving banner (or node death).
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

echo
echo "== etcdctl: put / get =="
out="$( "${ECTL[@]}" put foo bar 2>&1 )";            echo "$out" | grep -v unrecognized | sed 's/^/  $ etcdctl put foo bar -> /'
assert_contains "OK" "$out" "put foo bar -> OK"
out="$( "${ECTL[@]}" get foo 2>&1 | grep -v unrecognized )"; echo "$out" | sed 's/^/  /'
assert_contains "bar" "$out" "get foo -> bar"

echo
echo "== etcdctl: get -w json shows create/mod revision + version =="
js="$( "${ECTL[@]}" get foo -w json 2>&1 | grep -v unrecognized )"
echo "  $js"
assert_contains '"create_revision":1' "$js" "json create_revision=1"
assert_contains '"mod_revision":1'    "$js" "json mod_revision=1"
assert_contains '"version":1'         "$js" "json version=1"

echo
echo "== etcdctl: overwrite advances version + mod_revision =="
"${ECTL[@]}" put foo baz >/dev/null 2>&1
out="$( "${ECTL[@]}" get foo 2>&1 | grep -v unrecognized )"; echo "$out" | sed 's/^/  /'
assert_contains "baz" "$out" "get foo -> baz (after overwrite)"
js="$( "${ECTL[@]}" get foo -w json 2>&1 | grep -v unrecognized )"
echo "  $js"
assert_contains '"version":2'      "$js" "json version=2 after overwrite"
assert_contains '"mod_revision":2' "$js" "json mod_revision=2 advanced"

echo
echo "== etcdctl: historical read --rev=1 returns the OLD value =="
out="$( "${ECTL[@]}" get foo --rev=1 2>&1 | grep -v unrecognized )"; echo "$out" | sed 's/^/  /'
assert_contains "bar" "$out" "get foo --rev=1 -> bar (historical)"

echo
echo "== etcdctl: range over a prefix + --limit =="
"${ECTL[@]}" put a 1 >/dev/null 2>&1
"${ECTL[@]}" put b 2 >/dev/null 2>&1
"${ECTL[@]}" put c 3 >/dev/null 2>&1
# get the full keyspace (prefix "")
allout="$( "${ECTL[@]}" get "" --prefix 2>&1 | grep -v unrecognized )"
echo "$allout" | sed 's/^/  /'
assert_contains "a" "$allout" "prefix get sees key a"
assert_contains "c" "$allout" "prefix get sees key c"
assert_contains "foo" "$allout" "prefix get sees key foo"
# --limit 2 over the prefix returns only 2 keys (count their key lines)
limout="$( "${ECTL[@]}" get "" --prefix --limit=2 --keys-only 2>&1 | grep -v unrecognized )"
nkeys="$( echo "$limout" | grep -c . )"
echo "  --prefix --limit=2 --keys-only returned $nkeys key line(s):"; echo "$limout" | sed 's/^/    /'
assert_eq "2" "$nkeys" "--limit=2 returns exactly 2 keys"

echo
echo "== etcdctl: --keys-only and --count-only =="
# --keys-only over the whole prefix lists exactly the live keys (a,b,c,foo = 4), no values.
ko="$( "${ECTL[@]}" get "" --prefix --keys-only 2>&1 | grep -v unrecognized | grep -c . )"
echo "  --prefix --keys-only listed $ko key line(s)"
assert_eq "4" "$ko" "--keys-only over prefix lists 4 keys (a,b,c,foo)"
# --count-only needs -w fields (etcdctl rule); the Count field carries the total.
co="$( "${ECTL[@]}" get "" --prefix --count-only -w fields 2>&1 | grep -v unrecognized )"
cnt="$( echo "$co" | grep -i '"Count"' | grep -o '[0-9]*' | head -1 )"
echo "  --count-only -w fields Count = ${cnt:-?}"
if [ -n "$cnt" ] && [ "$cnt" -ge 4 ]; then ok "--count-only reports Count>=4 (got $cnt)"; else bad "--count-only Count>=4" "got [$cnt] :: $(echo "$co" | tr '\n' '|')"; fi

echo
echo "== etcdctl: del foo -> deleted 1, then get is empty =="
dl="$( "${ECTL[@]}" del foo 2>&1 | grep -v unrecognized )"; echo "$dl" | sed 's/^/  $ etcdctl del foo -> /'
assert_eq "1" "$dl" "del foo -> 1"
ge="$( "${ECTL[@]}" get foo 2>&1 | grep -v unrecognized )"
assert_eq "" "$ge" "get foo after del -> empty"

echo
echo "== etcdctl: txn (compare-then-put success + failure branch) =="
"${ECTL[@]}" put tk hello >/dev/null 2>&1
# SUCCESS branch: value(tk) = "hello"  ? then put tk=world : get tk
succ="$( printf 'value("tk") = "hello"\n\nput tk world\n\nget tk\n\n' | "${ECTL[@]}" txn 2>&1 | grep -v unrecognized )"
echo "$succ" | sed 's/^/  [txn-success] /'
assert_contains "SUCCESS" "$succ" "txn compare-true took SUCCESS branch"
tkval="$( "${ECTL[@]}" get tk 2>&1 | grep -v unrecognized )"
assert_contains "world" "$tkval" "txn SUCCESS put tk=world applied"
# FAILURE branch: value(tk) = "nope" ? then put tk=BAD : get tk   (compare false now)
fail="$( printf 'value("tk") = "nope"\n\nput tk BAD\n\nget tk\n\n' | "${ECTL[@]}" txn 2>&1 | grep -v unrecognized )"
echo "$fail" | sed 's/^/  [txn-failure] /'
assert_contains "FAILURE" "$fail" "txn compare-false took FAILURE branch"
tkval2="$( "${ECTL[@]}" get tk 2>&1 | grep -v unrecognized )"
assert_contains "world" "$tkval2" "txn FAILURE did NOT overwrite tk (still world)"

echo
echo "== etcdctl: compact, then a read below the compacted rev errors (ErrCompacted) =="
# current revision (from a get json header)
curhdr="$( "${ECTL[@]}" get tk -w json 2>&1 | grep -v unrecognized )"
currev="$( echo "$curhdr" | grep -o '"revision":[0-9]*' | head -1 | grep -o '[0-9]*' )"
echo "  current revision = $currev"
cmp="$( "${ECTL[@]}" compact "$currev" 2>&1 | grep -v unrecognized )"
echo "$cmp" | sed 's/^/  $ etcdctl compact '"$currev"' -> /'
assert_contains "compacted revision $currev" "$cmp" "compact $currev acknowledged"
# a read at rev=1 (below the compaction floor) must now error with ErrCompacted.
belowrev=1
cerr="$( "${ECTL[@]}" get foo --rev="$belowrev" 2>&1 | grep -v unrecognized )"
echo "  get --rev=$belowrev after compact -> $cerr"
if echo "$cerr" | grep -qiE "compact|OutOfRange|out of range|revision has been compacted"; then
  ok "read below compacted rev surfaces ErrCompacted"
else
  bad "read below compacted rev surfaces ErrCompacted" "got: $cerr"
fi

echo
echo "== etcdctl: endpoint status (Maintenance/Status stub) =="
st="$( "${ECTL[@]}" endpoint status -w json 2>&1 | grep -v unrecognized )"
echo "  $st"
assert_contains '"version":"3.6.0"' "$st" "endpoint status returns version (stub)"

echo
echo "== etcdctl: an unhandled RPC (Auth) returns UNIMPLEMENTED (12) =="
# Lease + Watch ARE bound now (cw-u4a.23); Auth is NOT (cw-u4a.25-.27) — an unknown
# path must come back UNIMPLEMENTED so the client gets a clean gRPC status.
un="$( "${ECTL[@]}" auth status 2>&1 | grep -v unrecognized )"
echo "  auth status -> $un"
if echo "$un" | grep -qiE "Unimplemented|unimplemented method|code = Unimplemented"; then
  ok "unhandled Auth RPC -> UNIMPLEMENTED(12)"
else
  bad "unhandled Auth RPC -> UNIMPLEMENTED(12)" "got: $un"
fi

echo
echo "================================================================"
echo "$PASS passed, $FAIL failed"
if [ "$FAIL" -gt 0 ]; then
  echo "ETCDCTL KV PROOF FAILED"
  echo "--- node log tail ---"; tail -20 "$LOG"
  exit 1
fi
echo "ETCDCTL KV PROOF: ALL PASS — crab-watchstore speaks etcd."
