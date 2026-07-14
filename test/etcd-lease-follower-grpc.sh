#!/usr/bin/env bash
# test/etcd-lease-follower-grpc.sh — regression for cw-u4a.43 over a 3-NODE cluster.
#
# The lease RPCs are LEADER-gated (grant/revoke go through Raft; keepalive/ttl are
# leader-local).  A request that lands on a FOLLOWER must REDIRECT with a clean
# gRPC UNAVAILABLE "etcdserver: not leader" — exactly like KV writes and Cluster
# member ops — so the client retries the leader.  cw-u4a.43: the LeaseKeepAlive
# bidi worker instead streamed back TTL=0 (etcd's "lease is gone" signal), which
# would make a client silently DROP a still-live lease; and the bead reported an
# INTERNAL "unexpected ack" on the lease path.  This test proves a follower now
# redirects cleanly for BOTH LeaseGrant and LeaseKeepAlive.
#
# Single-node tests (test/etcd-watch-lease-grpc.sh) can't catch this — the sole
# node is always the leader.  This requires a real multi-node cluster (the bug was
# found by the cw-u4a.35 Jepsen run, the first multi-node lease exercise).
#
# Usage:  bash test/etcd-lease-follower-grpc.sh    (run in BACKGROUND — foreground exits 144)
# Env:    CRABSCHEME = path to the crabscheme binary (default below).
#         ETCDCTL    = path to etcdctl (default: etcdctl on PATH, else homebrew).

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"
cd "$REPO_ROOT" || { echo "FATAL: cannot cd to repo root"; exit 1; }

BIN="${CRABSCHEME:-/Users/ztaylor/repos/workspaces/crabscheme/target/release/crabscheme}"
ETCDCTL="${ETCDCTL:-}"
if [ -z "$ETCDCTL" ]; then
  if command -v etcdctl >/dev/null 2>&1; then ETCDCTL="$(command -v etcdctl)";
  elif [ -x /opt/homebrew/bin/etcdctl ]; then ETCDCTL="/opt/homebrew/bin/etcdctl";
  else echo "FATAL: etcdctl not found (set ETCDCTL=...)"; exit 1; fi
fi

TAG="$(date +%s%N)"
BC="$(( 27800 + (TAG % 1500) ))"
BR="$(( 21800 + (TAG % 1500) ))"
CP_A="$BC";        CP_B="$((BC+1))";  CP_C="$((BC+2))"
RP_A="$BR";        RP_B="$((BR+1))";  RP_C="$((BR+2))"
EP_A="127.0.0.1:${CP_A}"; EP_B="127.0.0.1:${CP_B}"; EP_C="127.0.0.1:${CP_C}"
CLUSTER="a:127.0.0.1:${RP_A}:${CP_A},b:127.0.0.1:${RP_B}:${CP_B},c:127.0.0.1:${RP_C}:${CP_C}"
DB="/tmp/cws-lease-fol-${TAG}"
LOG_A="/tmp/cws-leasefol-a-${TAG}.log"; LOG_B="/tmp/cws-leasefol-b-${TAG}.log"; LOG_C="/tmp/cws-leasefol-c-${TAG}.log"

PASS=0; FAIL=0
PID_A=""; PID_B=""; PID_C=""
cleanup() {
  for p in "$PID_A" "$PID_B" "$PID_C"; do [ -n "$p" ] && kill "$p" 2>/dev/null; done
  for p in "$PID_A" "$PID_B" "$PID_C"; do [ -n "$p" ] && wait "$p" 2>/dev/null; done
  rm -rf "$DB"* 2>/dev/null
}
trap cleanup EXIT

ok()  { PASS=$((PASS+1)); echo "  ok   $1"; }
bad() { FAIL=$((FAIL+1)); echo "  FAIL $1"; echo "         $2"; }
assert_has()    { if echo "$2" | grep -qiF -- "$1"; then ok "$3"; else bad "$3" "expected [$1], got: $(echo "$2" | tr '\n' '|')"; fi; }
assert_hasnot() { if echo "$2" | grep -qiF -- "$1"; then bad "$3" "did NOT expect [$1], got: $(echo "$2" | tr '\n' '|')"; else ok "$3"; fi; }
# a follower reply must look like a leader redirect (and never an INTERNAL bug ack).
assert_redirect() { # text label
  if echo "$1" | grep -qiE "not leader|unavailable|leader changed|rpc error|error"; then ok "$2"
  else bad "$2" "expected a not-leader redirect, got: $(echo "$1" | tr '\n' '|')"; fi; }

ectl1() { local ep="$1"; shift; "$ETCDCTL" --endpoints="$ep" --insecure-transport=true \
            --dial-timeout=5s --command-timeout=5s "$@" 2>&1 | grep -v 'level":"warn' | grep -v unrecognized; }

echo "== bring up 3-node crab-watchstore cluster (a/b/c) =="
echo "   cluster: $CLUSTER"
"$BIN" run src/node-cluster.scm -- --node a --db "${DB}-a" --cluster "$CLUSTER" > "$LOG_A" 2>&1 & PID_A=$!
"$BIN" run src/node-cluster.scm -- --node b --db "${DB}-b" --cluster "$CLUSTER" > "$LOG_B" 2>&1 & PID_B=$!
"$BIN" run src/node-cluster.scm -- --node c --db "${DB}-c" --cluster "$CLUSTER" > "$LOG_C" 2>&1 & PID_C=$!

up=0
for _ in $(seq 1 120); do
  if grep -q "etcd KV gRPC serving on" "$LOG_A" 2>/dev/null \
  && grep -q "etcd KV gRPC serving on" "$LOG_B" 2>/dev/null \
  && grep -q "etcd KV gRPC serving on" "$LOG_C" 2>/dev/null; then up=1; break; fi
  if ! kill -0 "$PID_A" 2>/dev/null || ! kill -0 "$PID_B" 2>/dev/null || ! kill -0 "$PID_C" 2>/dev/null; then break; fi
  sleep 0.5
done
if [ "$up" != "1" ]; then
  echo "FATAL: cluster did not come up. logs:"; for L in "$LOG_A" "$LOG_B" "$LOG_C"; do echo "--- $L ---"; cat "$L"; done; exit 1
fi

LEADER_EP=""
find_leader() {
  LEADER_EP=""
  for _ in $(seq 1 30); do
    for ep in "$EP_A" "$EP_B" "$EP_C"; do
      if "$ETCDCTL" --endpoints="$ep" --insecure-transport=true --dial-timeout=2s \
           --command-timeout=2s put "__leader_probe_${RANDOM}__" 1 2>/dev/null | grep -q "OK"; then
        LEADER_EP="$ep"; return 0
      fi
    done
    sleep 0.3
  done
  return 1
}
follower_ep() { for ep in "$EP_A" "$EP_B" "$EP_C"; do [ "$ep" != "$LEADER_EP" ] && { echo "$ep"; return; }; done; }

echo
echo "== discover leader / follower =="
if ! find_leader; then echo "FATAL: no leader serving writes"; for L in "$LOG_A" "$LOG_B" "$LOG_C"; do echo "--- $L ---"; tail -15 "$L"; done; exit 1; fi
FOLLOWER_EP="$(follower_ep)"
echo "  leader=$LEADER_EP  follower=$FOLLOWER_EP"

echo
echo "== LeaseGrant: leader serves, follower redirects (not an INTERNAL ack) =="
GL="$(ectl1 "$LEADER_EP" lease grant 100)"
echo "  $ etcdctl (leader) lease grant 100 -> $(echo "$GL" | tr '\n' '|')"
assert_has "granted" "$GL" "LeaseGrant on the leader succeeds"
LID="$(echo "$GL" | sed -n 's/^lease \([^ ]*\) granted.*/\1/p')"
[ -n "$LID" ] || { echo "FATAL: could not parse lease id from: $GL"; exit 1; }

GF="$(ectl1 "$FOLLOWER_EP" lease grant 100)"
echo "  $ etcdctl (follower) lease grant 100 -> $(echo "$GF" | tr '\n' '|')"
if [ "${CWS_ENGINE:-raft}" = quepaxa ]; then
  # leaderless leases: ANY node serves Lease RPCs (real-etcd behavior)
  assert_has "granted" "$GF" "LeaseGrant on a non-coordinator succeeds (leaderless leases)"
else
assert_hasnot "granted" "$GF" "LeaseGrant on a follower does NOT succeed"
assert_hasnot "unexpected ack" "$GF" "LeaseGrant on a follower is NOT an INTERNAL 'unexpected ack'"
assert_redirect "$GF" "LeaseGrant on a follower returns a clean not-leader redirect"
fi

echo
echo "== LeaseKeepAlive: leader keeps it alive; follower redirects, never fakes TTL=0 (cw-u4a.43) =="
KL="$(ectl1 "$LEADER_EP" lease keep-alive --once "$LID")"
echo "  $ etcdctl (leader) lease keep-alive --once -> $(echo "$KL" | tr '\n' '|')"
assert_has "keepalived" "$KL" "LeaseKeepAlive on the leader succeeds (lease is live)"

KF="$(ectl1 "$FOLLOWER_EP" lease keep-alive --once "$LID")"
echo "  $ etcdctl (follower) lease keep-alive --once -> $(echo "$KF" | tr '\n' '|')"
assert_hasnot "unexpected ack" "$KF" "LeaseKeepAlive on a follower is NOT an INTERNAL 'unexpected ack'"
# the core cw-u4a.43 fix: a follower must NOT report the still-live lease as expired (TTL=0).
assert_hasnot "expired" "$KF" "follower keepalive did NOT falsely report the live lease expired (TTL=0)"
if [ "${CWS_ENGINE:-raft}" = quepaxa ]; then
  assert_has "keepalived" "$KF" "LeaseKeepAlive on a non-coordinator succeeds (leaderless leases)"
else
assert_redirect "$KF" "LeaseKeepAlive on a follower returns a clean not-leader redirect (cw-u4a.43)"
fi

# the lease is still alive on the leader after the follower's rejected keepalive.
TTL="$(ectl1 "$LEADER_EP" lease timetolive "$LID")"
echo "  $ etcdctl (leader) lease timetolive -> $(echo "$TTL" | tr '\n' '|')"
assert_hasnot "expired" "$TTL" "lease still alive on the leader after the follower's rejected keepalive"

echo
echo "================= cw-u4a.43 lease-on-follower: $PASS passed, $FAIL failed ================="
[ "$FAIL" -eq 0 ] && { echo "ALL PASS"; exit 0; } || { echo "FAILURES"; exit 1; }
