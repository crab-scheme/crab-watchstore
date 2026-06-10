#!/usr/bin/env bash
# test/etcd-watch-lease-grpc.sh — prove crab-watchstore's STREAMING etcd APIs
# (Watch bidi + Lease unary/KeepAlive bidi) with REAL etcdctl (3.6.x) over h2c
# (cw-u4a.23).
#
# Brings up a SINGLE-NODE crab-watchstore (1 voter = always leader) and drives
# `etcdctl` against it:
#   WATCH  : a live watch sees PUT v1 / PUT v2 / DELETE in order; a --prefix
#            watch; a historical --rev watch that replays past events.
#   LEASE  : grant -> put --lease -> timetolive --keys -> keep-alive (stream) ->
#            list -> revoke -> the lease-attached key is gone.
#
# Usage:  bash test/etcd-watch-lease-grpc.sh
# Env:    CRABSCHEME = path to the crabscheme binary (default below).
#         ETCDCTL    = path to etcdctl (default: etcdctl on PATH).

set -uo pipefail

BIN="${CRABSCHEME:-/Users/ztaylor/repos/workspaces/crabscheme/target/release/crabscheme}"
ETCDCTL="${ETCDCTL:-etcdctl}"

TAG="$(date +%s%N)"
PORT="$(( 28000 + (TAG % 4000) ))"
RAFTPORT="$(( 14000 + (TAG % 4000) ))"
DB="/tmp/cws-etcd-wl-${TAG}"
EP="127.0.0.1:${PORT}"
LOG="/tmp/cws-etcd-wl-node-${TAG}.log"
WDIR="/tmp/cws-etcd-wl-out-${TAG}"
mkdir -p "$WDIR"
ECTL=( "$ETCDCTL" --endpoints="$EP" --insecure-transport=true --dial-timeout=5s --command-timeout=5s )

PASS=0
FAIL=0
NODE_PID=""
BG_PIDS=()

cleanup() {
  for p in "${BG_PIDS[@]}"; do kill "$p" 2>/dev/null; done
  [ -n "$NODE_PID" ] && kill "$NODE_PID" 2>/dev/null
  wait "$NODE_PID" 2>/dev/null
  rm -rf "$DB"* "$WDIR" 2>/dev/null
}
trap cleanup EXIT

ok()   { PASS=$((PASS+1)); echo "  ok   $1"; }
bad()  { FAIL=$((FAIL+1)); echo "  FAIL $1"; echo "         $2"; }

assert_contains() {  # needle haystack label
  if echo "$2" | grep -qF -- "$1"; then ok "$3"; else bad "$3" "want [$1] in: $(echo "$2" | tr '\n' '|')"; fi
}
assert_eq() { if [ "$1" = "$2" ]; then ok "$3"; else bad "$3" "expected [$1], got [$2]"; fi; }

# wait up to $3 tenths-of-a-second for FILE ($1) to contain NEEDLE ($2)
wait_file() {
  for _ in $(seq 1 "$3"); do
    if grep -qF -- "$2" "$1" 2>/dev/null; then return 0; fi
    sleep 0.1
  done
  return 1
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
if [ "$up" != "1" ]; then echo "FATAL: node did not come up. log:"; cat "$LOG"; exit 1; fi
grep "etcd KV gRPC serving on" "$LOG" | sed 's/^/  /'

# ===========================================================================
echo
echo "== WATCH: live watch sees PUT v1 / PUT v2 / DELETE in order =="
# ===========================================================================
WF="$WDIR/watch-live.out"
"${ECTL[@]}" watch foo > "$WF" 2>/dev/null < /dev/null &
WPID=$!; BG_PIDS+=("$WPID")
sleep 1.5   # let the WatchCreate establish (server sends a created ack)

"${ECTL[@]}" put foo v1 >/dev/null 2>&1
"${ECTL[@]}" put foo v2 >/dev/null 2>&1
"${ECTL[@]}" del foo    >/dev/null 2>&1

wait_file "$WF" "DELETE" 40
kill "$WPID" 2>/dev/null
wout="$(cat "$WF")"
echo "$wout" | sed 's/^/  | /'
assert_contains "v1" "$wout" "watch saw PUT v1"
assert_contains "v2" "$wout" "watch saw PUT v2"
assert_contains "DELETE" "$wout" "watch saw DELETE"
# order: v1 line < v2 line < DELETE line
l1="$(grep -n '^v1$'     "$WF" | head -1 | cut -d: -f1)"
l2="$(grep -n '^v2$'     "$WF" | head -1 | cut -d: -f1)"
l3="$(grep -n '^DELETE$' "$WF" | head -1 | cut -d: -f1)"
if [ -n "$l1" ] && [ -n "$l2" ] && [ -n "$l3" ] && [ "$l1" -lt "$l2" ] && [ "$l2" -lt "$l3" ]; then
  ok "events arrived in order (PUT v1 < PUT v2 < DELETE)"
else
  bad "events in order" "lines v1=$l1 v2=$l2 DELETE=$l3"
fi

# ===========================================================================
echo
echo "== WATCH: --prefix watch over 'pre' sees both keys =="
# ===========================================================================
PF="$WDIR/watch-prefix.out"
"${ECTL[@]}" watch --prefix pre > "$PF" 2>/dev/null < /dev/null &
PFPID=$!; BG_PIDS+=("$PFPID")
sleep 1.5
"${ECTL[@]}" put pre/a 1 >/dev/null 2>&1
"${ECTL[@]}" put pre/b 2 >/dev/null 2>&1
wait_file "$PF" "pre/b" 40
kill "$PFPID" 2>/dev/null
pout="$(cat "$PF")"
echo "$pout" | sed 's/^/  | /'
assert_contains "pre/a" "$pout" "prefix watch saw pre/a"
assert_contains "pre/b" "$pout" "prefix watch saw pre/b"

# ===========================================================================
echo
echo "== WATCH: historical --rev replays past events =="
# ===========================================================================
# write three revisions to 'hist', capture the revision of the FIRST write,
# then watch from that revision -> the stream replays the history.
"${ECTL[@]}" put hist h1 >/dev/null 2>&1
hrev="$( "${ECTL[@]}" get hist -w json 2>&1 | grep -v unrecognized | grep -o '"mod_revision":[0-9]*' | head -1 | grep -o '[0-9]*' )"
"${ECTL[@]}" put hist h2 >/dev/null 2>&1
"${ECTL[@]}" put hist h3 >/dev/null 2>&1
echo "  first 'hist' write was revision $hrev; watching --rev=$hrev"
HF="$WDIR/watch-hist.out"
"${ECTL[@]}" watch hist --rev="$hrev" > "$HF" 2>/dev/null < /dev/null &
HPID=$!; BG_PIDS+=("$HPID")
wait_file "$HF" "h3" 40
kill "$HPID" 2>/dev/null
hout="$(cat "$HF")"
echo "$hout" | sed 's/^/  | /'
assert_contains "h1" "$hout" "historical watch replayed h1"
assert_contains "h2" "$hout" "historical watch replayed h2"
assert_contains "h3" "$hout" "historical watch replayed h3"

# ===========================================================================
echo
echo "== LEASE: grant / put --lease / timetolive --keys =="
# ===========================================================================
g="$( "${ECTL[@]}" lease grant 60 2>&1 | grep -v unrecognized )"
echo "  $ etcdctl lease grant 60 -> $g"
assert_contains "granted with TTL(60s)" "$g" "lease grant 60 -> TTL(60s)"
LID="$( echo "$g" | grep -o 'lease [0-9a-f]*' | head -1 | awk '{print $2}' )"
echo "  lease id (hex) = $LID"
if [ -z "$LID" ]; then bad "parse lease id" "from: $g"; fi

pl="$( "${ECTL[@]}" put k v --lease="$LID" 2>&1 | grep -v unrecognized )"
assert_contains "OK" "$pl" "put k v --lease=$LID -> OK"

ttl="$( "${ECTL[@]}" lease timetolive "$LID" --keys 2>&1 | grep -v unrecognized )"
echo "  $ etcdctl lease timetolive $LID --keys -> $ttl"
assert_contains "granted with TTL(60s)" "$ttl" "timetolive shows granted TTL(60s)"
assert_contains "k" "$ttl" "timetolive --keys shows attached key k"

# ===========================================================================
echo
echo "== LEASE: list shows the granted lease =="
# ===========================================================================
ll="$( "${ECTL[@]}" lease list 2>&1 | grep -v unrecognized )"
echo "  $ etcdctl lease list -> $(echo "$ll" | tr '\n' ' ')"
assert_contains "$LID" "$ll" "lease list contains $LID"

# ===========================================================================
echo
echo "== LEASE: keep-alive streams TTL refreshes =="
# ===========================================================================
KF="$WDIR/lease-ka.out"
"${ECTL[@]}" lease keep-alive "$LID" > "$KF" 2>/dev/null < /dev/null &
KPID=$!; BG_PIDS+=("$KPID")
wait_file "$KF" "keepalived" 40
kill "$KPID" 2>/dev/null
kout="$(cat "$KF")"
echo "$kout" | head -3 | sed 's/^/  | /'
assert_contains "keepalived with TTL(60)" "$kout" "keep-alive streamed a TTL(60) refresh"

# ===========================================================================
echo
echo "== LEASE: revoke deletes the attached key =="
# ===========================================================================
rv="$( "${ECTL[@]}" lease revoke "$LID" 2>&1 | grep -v unrecognized )"
echo "  $ etcdctl lease revoke $LID -> $rv"
assert_contains "revoked" "$rv" "lease revoke $LID -> revoked"
gk="$( "${ECTL[@]}" get k 2>&1 | grep -v unrecognized )"
assert_eq "" "$gk" "get k after revoke -> empty (lease-attached key deleted)"
# and the revoked lease drops out of the list
ll2="$( "${ECTL[@]}" lease list 2>&1 | grep -v unrecognized )"
if echo "$ll2" | grep -qF "$LID"; then
  bad "revoked lease gone from list" "still present: $(echo "$ll2" | tr '\n' ' ')"
else
  ok "revoked lease no longer in lease list"
fi

echo
echo "================================================================"
echo "$PASS passed, $FAIL failed"
if [ "$FAIL" -gt 0 ]; then
  echo "ETCDCTL WATCH+LEASE PROOF FAILED"
  echo "--- node log tail ---"; tail -25 "$LOG"
  exit 1
fi
echo "ETCDCTL WATCH+LEASE PROOF: ALL PASS — crab-watchstore streams etcd Watch + Lease."
