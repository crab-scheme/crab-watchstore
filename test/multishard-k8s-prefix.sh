#!/usr/bin/env bash
# test/multishard-k8s-prefix.sh — cw-0v2 (G3): multi-shard routing for the k8s
# keyspace.  Boots a SINGLE-NODE crab-watchstore with --shard-groups 3 and
# proves, with real etcdctl, that prefix-aware routing (src/shard-route.scm):
#
#   1. spreads /registry/pods vs /registry/leases vs /registry/events across
#      DIFFERENT shard groups (observed via independent per-shard revision
#      sequences: a fresh resource's first key gets mod_revision 1, not the
#      continuation of another resource's sequence);
#   2. keeps every key of ONE resource on ONE shard (per-resource revision
#      sequence is dense/monotone);
#   3. serves a per-prefix LIST (Range) for each resource completely, plus a
#      cross-shard whole-keyspace Range via scatter-gather;
#   4. single-key Txn compare-and-swap (the k8s update pattern) works against
#      a mod_revision taken from a prefix LIST (same shard -> coherent revs);
#   5. a per-prefix watch delivers events for its resource (registered at the
#      owning shard, NOT sprayed across groups) and does NOT see other
#      resources' events;
#   6. per-prefix DeleteRange deletes exactly the resource's keys.
#
# Runs the whole battery for BOTH consensus engines: raft and quepaxa.
#
# Usage:  bash test/multishard-k8s-prefix.sh
# Env:    CRABSCHEME = path to crabscheme binary (default below).
#         ETCDCTL    = path to etcdctl (default: etcdctl on PATH).
#         CWS_ENGINES = engines to run (default "raft quepaxa").

set -uo pipefail

BIN="${CRABSCHEME:-/Users/ztaylor/repos/workspaces/crabscheme/target/release/crabscheme}"
ETCDCTL="${ETCDCTL:-etcdctl}"
ENGINES="${CWS_ENGINES:-raft quepaxa}"

PASS=0
FAIL=0
NODE_PID=""

cleanup() {
  [ -n "$NODE_PID" ] && kill "$NODE_PID" 2>/dev/null
  wait "$NODE_PID" 2>/dev/null
  [ -n "${DB:-}" ] && rm -rf "$DB"* 2>/dev/null
}
trap cleanup EXIT

ok()  { PASS=$((PASS+1)); echo "  ok   $1"; }
bad() { FAIL=$((FAIL+1)); echo "  FAIL $1"; echo "         $2"; }

assert_contains() {
  if echo "$2" | grep -qF -- "$1"; then ok "$3"; else bad "$3" "expected to contain [$1], got: $(echo "$2" | tr '\n' '|')"; fi
}
assert_not_contains() {
  if echo "$2" | grep -qF -- "$1"; then bad "$3" "expected NOT to contain [$1], got: $(echo "$2" | tr '\n' '|')"; else ok "$3"; fi
}
assert_eq() {
  if [ "$1" = "$2" ]; then ok "$3"; else bad "$3" "expected [$1], got [$2]"; fi
}

mod_rev() {  # mod_revision of a single key, via -w json
  "${ECTL[@]}" get "$1" -w json 2>/dev/null | grep -v unrecognized \
    | sed -n 's/.*"mod_revision":\([0-9]*\).*/\1/p'
}

run_engine() {
  local ENGINE="$1"
  TAG="$(date +%s%N)"
  PORT="$(( 28000 + (TAG % 4000) ))"
  RAFTPORT="$(( 21000 + (TAG % 4000) ))"
  DB="/tmp/cws-mshard-${ENGINE}-${TAG}"
  EP="127.0.0.1:${PORT}"
  LOG="/tmp/cws-mshard-${ENGINE}-node-${TAG}.log"
  ECTL=( "$ETCDCTL" --endpoints="$EP" --insecure-transport=true --dial-timeout=5s --command-timeout=10s )

  echo
  echo "==== engine=$ENGINE: single node, --shard-groups 3, etcd gRPC on $EP ===="
  "$BIN" run src/node-cluster.scm -- \
    --node a --db "$DB" --cluster "a:127.0.0.1:${RAFTPORT}:${PORT}" \
    --shard-groups 3 --engine "$ENGINE" > "$LOG" 2>&1 &
  NODE_PID=$!

  up=0
  for _ in $(seq 1 80); do
    if grep -q "etcd KV gRPC serving on" "$LOG" 2>/dev/null; then up=1; break; fi
    if ! kill -0 "$NODE_PID" 2>/dev/null; then break; fi
    sleep 0.5
  done
  if [ "$up" != "1" ]; then
    bad "engine=$ENGINE node up" "node did not come up; log tail: $(tail -5 "$LOG" | tr '\n' '|')"
    kill "$NODE_PID" 2>/dev/null; wait "$NODE_PID" 2>/dev/null; NODE_PID=""
    return
  fi
  sleep 1   # let all 3 shard groups elect

  echo "== [$ENGINE] per-prefix watch on /registry/leases/ (registered BEFORE writes) =="
  WDIR="$(mktemp -d)"
  WF="$WDIR/watch-leases.out"
  "${ECTL[@]}" watch --prefix /registry/leases/ > "$WF" 2>/dev/null < /dev/null &
  WPID=$!
  sleep 2

  echo "== [$ENGINE] writes across the three k8s resources =="
  "${ECTL[@]}" put /registry/pods/ns1/web-0 pod-web-0   >/dev/null 2>&1
  "${ECTL[@]}" put /registry/pods/ns1/web-1 pod-web-1   >/dev/null 2>&1
  "${ECTL[@]}" put /registry/pods/ns2/db-0  pod-db-0    >/dev/null 2>&1
  "${ECTL[@]}" put /registry/leases/kube-node-lease/n1 lease-n1 >/dev/null 2>&1
  "${ECTL[@]}" put /registry/events/ns1/web-0.evt ev-web-0     >/dev/null 2>&1

  echo "== [$ENGINE] shard spreading: independent per-shard revision sequences =="
  # 3 pod writes advance ONLY the pods shard (revs 1,2,3). If leases/events
  # shared it, their first key would get mod_revision 4/5; on their OWN shards
  # each starts a fresh sequence at 1.
  pr1="$(mod_rev /registry/pods/ns1/web-0)"
  pr3="$(mod_rev /registry/pods/ns2/db-0)"
  lr="$(mod_rev /registry/leases/kube-node-lease/n1)"
  er="$(mod_rev /registry/events/ns1/web-0.evt)"
  echo "  pods first/last mod_rev = $pr1/$pr3, leases = $lr, events = $er"
  assert_eq "1" "$pr1" "pods shard: first pod at rev 1"
  assert_eq "3" "$pr3" "pods shard: third pod at rev 3 (one resource = one shard, dense seq)"
  assert_eq "1" "$lr"  "leases on their OWN shard (first lease at rev 1, not 4)"
  assert_eq "1" "$er"  "events on their OWN shard (first event at rev 1, not 5)"

  echo "== [$ENGINE] per-prefix LIST routes to the owning shard and is complete =="
  pods="$( "${ECTL[@]}" get /registry/pods/ --prefix --keys-only 2>&1 | grep -v unrecognized | grep -c . )"
  assert_eq "3" "$pods" "LIST /registry/pods/ -> exactly 3 keys"
  leases="$( "${ECTL[@]}" get /registry/leases/ --prefix 2>&1 | grep -v unrecognized )"
  assert_contains "lease-n1" "$leases" "LIST /registry/leases/ sees the lease"
  assert_not_contains "pod-web-0" "$leases" "LIST /registry/leases/ has no pods"

  echo "== [$ENGINE] cross-shard Range (whole keyspace) scatter-gathers all 5 keys =="
  allk="$( "${ECTL[@]}" get /registry/ --prefix --keys-only 2>&1 | grep -v unrecognized | grep -c . )"
  assert_eq "5" "$allk" "LIST /registry/ (spans all shards) -> all 5 keys"

  echo "== [$ENGINE] single-key Txn CAS against a LIST-provided mod_revision =="
  rv="$(mod_rev /registry/leases/kube-node-lease/n1)"
  txout="$( "${ECTL[@]}" txn 2>&1 <<EOF
mod("/registry/leases/kube-node-lease/n1") = "$rv"

put /registry/leases/kube-node-lease/n1 lease-n1-renewed

put /registry/leases/kube-node-lease/n1 CAS-LOST

EOF
)"
  assert_contains "SUCCESS" "$txout" "CAS txn succeeded (compare rev from same shard)"
  out="$( "${ECTL[@]}" get /registry/leases/kube-node-lease/n1 2>&1 | grep -v unrecognized )"
  assert_contains "lease-n1-renewed" "$out" "CAS applied the success op"
  # stale CAS (old rev) must take the failure branch
  txout2="$( "${ECTL[@]}" txn 2>&1 <<EOF
mod("/registry/leases/kube-node-lease/n1") = "$rv"

put /registry/leases/kube-node-lease/n1 MUST-NOT-APPLY

put /registry/leases/kube-node-lease/n1 lease-n1-final

EOF
)"
  assert_contains "FAILURE" "$txout2" "stale CAS txn took the failure branch"

  echo "== [$ENGINE] per-prefix watch saw ONLY the leases events =="
  sleep 2
  kill "$WPID" 2>/dev/null; wait "$WPID" 2>/dev/null
  wout="$(cat "$WF")"
  echo "$wout" | sed 's/^/  | /'
  assert_contains "lease-n1" "$wout" "leases watch saw the lease PUT (owning shard, not group 0)"
  assert_contains "lease-n1-renewed" "$wout" "leases watch saw the CAS update"
  assert_not_contains "pod-web-0" "$wout" "leases watch saw NO pod events"
  assert_not_contains "ev-web-0" "$wout" "leases watch saw NO event-resource events"

  echo "== [$ENGINE] per-prefix DeleteRange deletes exactly the resource =="
  del="$( "${ECTL[@]}" del /registry/events/ --prefix 2>&1 | grep -v unrecognized )"
  assert_eq "1" "$del" "del /registry/events/ --prefix -> 1"
  left="$( "${ECTL[@]}" get /registry/ --prefix --keys-only 2>&1 | grep -v unrecognized | grep -c . )"
  assert_eq "4" "$left" "4 keys remain after events prefix delete"

  rm -rf "$WDIR"
  kill "$NODE_PID" 2>/dev/null; wait "$NODE_PID" 2>/dev/null; NODE_PID=""
  rm -rf "$DB"* 2>/dev/null
}

for e in $ENGINES; do run_engine "$e"; done

echo
echo "$PASS passed, $FAIL failed"
if [ "$FAIL" -gt 0 ]; then exit 1; fi
echo "ALL PASS"
