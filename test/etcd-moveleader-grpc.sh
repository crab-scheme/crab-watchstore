#!/usr/bin/env bash
# test/etcd-moveleader-grpc.sh — cw-u4a.42: REAL leadership transfer over a 3-node
# cluster with etcdctl `move-leader`, exercising the raft.scm TimeoutNow primitive
# end-to-end (grpc Maintenance/MoveLeader -> shard move-leader mailbox -> leader
# emits 'timeout-now -> target campaigns + wins).
#
#   move-leader <follower-id> on the LEADER  -> leadership moves to that follower
#   move-leader on a FOLLOWER endpoint       -> not-leader (the client must retarget)
#   move-leader <bogus-id>                   -> bad leader transferee (FailedPrecondition)
#
# Single-node tests can't reach this (the sole node is always leader); needs a real
# multi-node cluster.  Usage: bash test/etcd-moveleader-grpc.sh  (run in BACKGROUND).

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
BC="$(( 29200 + (TAG % 1500) ))"; BR="$(( 23200 + (TAG % 1500) ))"
CP_A="$BC"; CP_B="$((BC+1))"; CP_C="$((BC+2))"
RP_A="$BR"; RP_B="$((BR+1))"; RP_C="$((BR+2))"
EP_A="127.0.0.1:${CP_A}"; EP_B="127.0.0.1:${CP_B}"; EP_C="127.0.0.1:${CP_C}"
CLUSTER="a:127.0.0.1:${RP_A}:${CP_A},b:127.0.0.1:${RP_B}:${CP_B},c:127.0.0.1:${RP_C}:${CP_C}"
DB="/tmp/cws-moveldr-${TAG}"
LOG_A="/tmp/cws-mvl-a-${TAG}.log"; LOG_B="/tmp/cws-mvl-b-${TAG}.log"; LOG_C="/tmp/cws-mvl-c-${TAG}.log"
PASS=0; FAIL=0; PID_A=""; PID_B=""; PID_C=""
cleanup(){ for p in "$PID_A" "$PID_B" "$PID_C"; do [ -n "$p" ] && kill "$p" 2>/dev/null; done
           for p in "$PID_A" "$PID_B" "$PID_C"; do [ -n "$p" ] && wait "$p" 2>/dev/null; done
           rm -rf "$DB"* 2>/dev/null; }
trap cleanup EXIT
ok(){ PASS=$((PASS+1)); echo "  ok   $1"; }
bad(){ FAIL=$((FAIL+1)); echo "  FAIL $1"; echo "         $2"; }
assert_has(){ if echo "$2" | grep -qiF -- "$1"; then ok "$3"; else bad "$3" "expected [$1], got: $(echo "$2"|tr '\n' '|')"; fi; }
assert_eq(){ if [ "$1" = "$2" ]; then ok "$3"; else bad "$3" "expected [$1] got [$2]"; fi; }
ectl1(){ local ep="$1"; shift; "$ETCDCTL" --endpoints="$ep" --insecure-transport=true --dial-timeout=5s --command-timeout=5s "$@" 2>&1 | grep -v 'level":"warn' | grep -v unrecognized; }
id_of(){ ectl1 "$1" member list | awk -F', ' -v n="$2" '$3==n {print $1}'; }
ep_name(){ case "$1" in "$EP_A") echo a;; "$EP_B") echo b;; "$EP_C") echo c;; esac; }

echo "== bring up 3-node cluster (a/b/c) =="
"$BIN" run src/node-cluster.scm -- --node a --db "${DB}-a" --cluster "$CLUSTER" > "$LOG_A" 2>&1 & PID_A=$!
"$BIN" run src/node-cluster.scm -- --node b --db "${DB}-b" --cluster "$CLUSTER" > "$LOG_B" 2>&1 & PID_B=$!
"$BIN" run src/node-cluster.scm -- --node c --db "${DB}-c" --cluster "$CLUSTER" > "$LOG_C" 2>&1 & PID_C=$!
up=0
for _ in $(seq 1 120); do
  if grep -q "etcd KV gRPC serving on" "$LOG_A" 2>/dev/null && grep -q "etcd KV gRPC serving on" "$LOG_B" 2>/dev/null && grep -q "etcd KV gRPC serving on" "$LOG_C" 2>/dev/null; then up=1; break; fi
  if ! kill -0 "$PID_A" 2>/dev/null || ! kill -0 "$PID_B" 2>/dev/null || ! kill -0 "$PID_C" 2>/dev/null; then break; fi
  sleep 0.5
done
[ "$up" = 1 ] || { echo "FATAL: cluster did not come up"; for L in "$LOG_A" "$LOG_B" "$LOG_C"; do echo "--- $L ---"; cat "$L"; done; exit 1; }

LEADER_EP=""
find_leader(){ LEADER_EP=""
  for _ in $(seq 1 30); do for ep in "$EP_A" "$EP_B" "$EP_C"; do
    if "$ETCDCTL" --endpoints="$ep" --insecure-transport=true --dial-timeout=2s --command-timeout=2s put "__p_${RANDOM}__" 1 2>/dev/null | grep -q OK; then LEADER_EP="$ep"; return 0; fi
  done; sleep 0.3; done; return 1; }
follower_ep(){ for ep in "$EP_A" "$EP_B" "$EP_C"; do [ "$ep" != "$LEADER_EP" ] && { echo "$ep"; return; }; done; }

echo "== discover leader =="
find_leader || { echo "FATAL: no leader"; exit 1; }
OLD_LEADER_EP="$LEADER_EP"
FOLLOWER_EP="$(follower_ep)"; FNAME="$(ep_name "$FOLLOWER_EP")"
FID="$(id_of "$LEADER_EP" "$FNAME")"
echo "  leader=$OLD_LEADER_EP ($(ep_name "$OLD_LEADER_EP"))  ->  transfer to $FNAME (id=$FID) @ $FOLLOWER_EP"

echo
echo "== MoveLeader on the leader -> leadership moves to the target follower =="
OUT="$(ectl1 "$OLD_LEADER_EP" move-leader "$FID")"
echo "  $ etcdctl (leader) move-leader $FID -> $(echo "$OUT" | tr '\n' '|')"
assert_has "Leadership transferred" "$OUT" "etcdctl reports leadership transferred"
# the target follower must now be the leader (it serves writes; the old leader does not).
NEW=""
for _ in $(seq 1 25); do find_leader; [ "$LEADER_EP" = "$FOLLOWER_EP" ] && { NEW="$LEADER_EP"; break; }; sleep 0.4; done
assert_eq "$FOLLOWER_EP" "$NEW" "the target ($FNAME) is now the leader"
assert_eq "1" "$( [ "$LEADER_EP" != "$OLD_LEADER_EP" ] && echo 1 || echo 0 )" "leadership left the original leader"
# writes still commit under the new leader
assert_has "OK" "$(ectl1 "$LEADER_EP" put post-xfer v)" "the new leader commits a write"

echo
echo "== MoveLeader on a FOLLOWER endpoint -> not-leader redirect =="
NLEP="$(follower_ep)"   # a current follower (relative to the NEW leader)
RID="$(id_of "$LEADER_EP" "$(ep_name "$LEADER_EP")")"   # transfer back to the current leader = some valid id
OUT2="$(ectl1 "$NLEP" move-leader "$RID")"
echo "  $ etcdctl (follower) move-leader -> $(echo "$OUT2" | tr '\n' '|')"
if echo "$OUT2" | grep -qiE "not leader|unavailable|leader changed|rpc error|error"; then ok "move-leader on a follower is refused (not-leader)"; else bad "follower move-leader not refused" "$OUT2"; fi

echo
echo "== MoveLeader to a bogus member id -> bad leader transferee =="
OUT3="$(ectl1 "$LEADER_EP" move-leader 123456789)"
echo "  $ etcdctl (leader) move-leader 123456789 -> $(echo "$OUT3" | tr '\n' '|')"
if echo "$OUT3" | grep -qiE "bad leader transferee|transferee|error"; then ok "bogus transferee rejected"; else bad "bogus transferee not rejected" "$OUT3"; fi

echo
echo "================= cw-u4a.42 MoveLeader: $PASS passed, $FAIL failed ================="
[ "$FAIL" -eq 0 ] && { echo "ALL PASS"; exit 0; } || { echo "FAILURES"; exit 1; }
