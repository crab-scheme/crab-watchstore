#!/usr/bin/env bash
# test/cluster-soak.sh — cw-24e.4: the cw-24e epic acceptance gate.
#
# A 3-node cluster under CONTINUOUS etcdctl write load + a prefix watch while:
#   t+~25s  the LEADER is killed (SIGKILL) and restarted 5s later (recovers
#           from its RocksDB data dir, rejoins as follower);
#   t+~55s  one live membership cycle on a follower: member remove -> wipe its
#           data -> restart with --join -> member add (the docs/operations.md
#           recipe) -> it catches up by replication.
#
# Assertions at the end:
#   1. ZERO LOST ACKNOWLEDGED WRITES — every put the client saw "OK" for is
#      readable with its exact value.
#   2. WATCH CONTINUITY — the watcher delivered every acknowledged write
#      (in order; the stream survives leader change by reconnecting, like a
#      real etcd client).
#   3. CONVERGENCE — hashkv identical on all 3 members (incl. the re-added one).
#   4. The cluster ends with ONE leader and serves reads.
set -uo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
ETCDCTL="${ETCDCTL:-$(command -v etcdctl)}"
DUR="${DUR:-90}"                       # seconds of write load
EPS_A=(127.0.0.1:23790 127.0.0.1:23791 127.0.0.1:23792)
EPS="$(IFS=,; echo "${EPS_A[*]}")"
WORK=/tmp/cws-soak; rm -rf "$WORK" /tmp/crab-watchstore-3; mkdir -p "$WORK"
PASS=0; FAIL=0
ok(){ PASS=$((PASS+1)); echo "  ok   $1"; }
bad(){ FAIL=$((FAIL+1)); echo "  FAIL $1"; }
check(){ if [ "$2" = "$3" ]; then ok "$1"; else bad "$1  expected=$2 got=$3"; fi; }
cleanup(){ pkill -f node-cluster.scm 2>/dev/null; pkill -f "watch soak/" 2>/dev/null; }
trap cleanup EXIT

start_member(){ # name [extra-flags...]
  local n="$1"; shift
  ("$ROOT/bin/crab-watchstore" --config "$ROOT/examples/cluster-3/$n.conf" "$@" \
     >>"$WORK/node-$n.log" 2>&1 &)
}
leader_ep(){
  for ep in "${EPS_A[@]}"; do
    local J M L
    J=$("$ETCDCTL" --endpoints="$ep" endpoint status -w json 2>/dev/null)
    M=$(echo "$J" | sed 's/.*"member_id"://;s/[,}].*//')
    L=$(echo "$J" | sed 's/.*"leader"://;s/[,}].*//')
    case "$M" in (*[!0-9]*|"") continue;; esac   # numeric member_id only (an erroring node echoes its error into both fields)
    [ "$M" = "$L" ] && { echo "$ep"; return; }
  done
}
leader_name(){ # a/b/c of the current leader (by endpoint position)
  case "$(leader_ep)" in
    *23790) echo a;; *23791) echo b;; *23792) echo c;; *) echo "";;
  esac
}

echo "== bring up 3 members =="
for n in a b c; do start_member "$n"; done
for n in a b c; do
  for _ in $(seq 1 100); do grep -q "etcd KV gRPC serving on" "$WORK/node-$n.log" 2>/dev/null && break; sleep 0.5; done
done
sleep 2
[ -n "$(leader_ep)" ] && ok "cluster up, leader = $(leader_name)" || { bad "no leader"; exit 1; }

echo "== start watch + write load (${DUR}s) =="
# The server cancels watches on leader loss (reads/watches are leader-gated),
# and etcdctl exits on a server cancel — so reconnect in a loop, replaying from
# rev 1 each time (nothing compacts during the soak). The FINAL connection's
# replay therefore contains every committed soak/ event in revision order; the
# continuity assertion counts distinct values.
(while [ ! -f "$WORK/load-done" ]; do
   "$ETCDCTL" --endpoints="$EPS" watch soak/ --prefix --rev 1 >>"$WORK/watch.out" 2>>"$WORK/watch.err"
   sleep 1
 done &)
sleep 1
(
  i=0
  end=$(( $(date +%s) + DUR ))
  while [ "$(date +%s)" -lt "$end" ]; do
    i=$((i+1))
    if "$ETCDCTL" --endpoints="$EPS" --command-timeout=3s put "soak/k-$i" "v-$i" >/dev/null 2>&1; then
      echo "$i" >> "$WORK/acked.txt"
    fi
  done
) &
LOADPID=$!

sleep 25
echo "== fault 1: kill + restart the leader =="
L1=$(leader_name); echo "  killing leader $L1"
pkill -9 -f "cluster-3/$L1.conf"
sleep 5
start_member "$L1"
sleep 5
L2=$(leader_name)
[ -n "$L2" ] && ok "new leader elected ($L2) after killing $L1" || bad "no leader after kill"

sleep 12
echo "== fault 2: live membership cycle on a follower =="
# pick a follower that is not the current leader
VICTIM=""
for n in a b c; do [ "$n" != "$(leader_name)" ] && VICTIM=$n && break; done
LEP=$(leader_ep)
VID=$("$ETCDCTL" --endpoints="$LEP" member list 2>/dev/null | grep ", $VICTIM," | cut -d, -f1)
echo "  removing member $VICTIM ($VID)"
"$ETCDCTL" --endpoints="$LEP" member remove "$VID" >/dev/null 2>&1 \
  && ok "member remove $VICTIM accepted" || bad "member remove failed"
pkill -9 -f "cluster-3/$VICTIM.conf"; sleep 2
rm -rf "/tmp/crab-watchstore-3/$VICTIM-shard0"
start_member "$VICTIM" --join yes
sleep 4
LEP=$(leader_ep)
RPORT=$(case $VICTIM in a) echo 21790;; b) echo 21791;; c) echo 21792;; esac)
"$ETCDCTL" --endpoints="$LEP" member add "$VICTIM" --peer-urls="http://127.0.0.1:$RPORT" >/dev/null 2>&1 \
  && ok "member add $VICTIM accepted" || bad "member add failed"

wait "$LOADPID" 2>/dev/null
sleep 8   # let replication + a final watch replay settle
touch "$WORK/load-done"; sleep 2; pkill -f "watch soak/" 2>/dev/null
ACKED=$(wc -l < "$WORK/acked.txt" | tr -d ' ')
echo "== load done: $ACKED acknowledged writes =="
[ "$ACKED" -gt 100 ] && ok "meaningful load ($ACKED acked writes)" || bad "too few acked writes ($ACKED)"

echo "== assert 1: zero lost acknowledged writes =="
LOST=0
LEP=$(leader_ep)
while read -r i; do
  V=$("$ETCDCTL" --endpoints="$LEP" --command-timeout=5s get "soak/k-$i" --print-value-only 2>/dev/null | tr -d '\n')
  [ "$V" = "v-$i" ] || { LOST=$((LOST+1)); [ $LOST -le 3 ] && echo "  LOST: soak/k-$i (got '$V')"; }
done < "$WORK/acked.txt"
check "lost acknowledged writes" 0 "$LOST"

echo "== assert 2: watch continuity =="
WSEEN=0
while read -r i; do
  grep -q "^v-$i\$" "$WORK/watch.out" && WSEEN=$((WSEEN+1))
done < "$WORK/acked.txt"
# the watch stream reconnects on leader change; events committed while it was
# down are replayed from its last revision — so every acked write must appear.
check "watch delivered every acked write" "$ACKED" "$WSEEN"

echo "== assert 3: hashkv convergence across all 3 =="
for _ in $(seq 1 20); do
  H1=$("$ETCDCTL" --endpoints="${EPS_A[0]}" endpoint hashkv -w json 2>/dev/null | sed 's/.*"hash"://;s/[,}].*//')
  H2=$("$ETCDCTL" --endpoints="${EPS_A[1]}" endpoint hashkv -w json 2>/dev/null | sed 's/.*"hash"://;s/[,}].*//')
  H3=$("$ETCDCTL" --endpoints="${EPS_A[2]}" endpoint hashkv -w json 2>/dev/null | sed 's/.*"hash"://;s/[,}].*//')
  [ -n "$H1" ] && [ "$H1" = "$H2" ] && [ "$H2" = "$H3" ] && break
  sleep 2
done
check "hashkv a==b" "$H1" "$H2"
check "hashkv b==c (incl. re-added member)" "$H2" "$H3"

echo "== assert 4: one leader, reads served =="
[ -n "$(leader_ep)" ] && ok "leader present at end" || bad "no leader at end"

echo
echo "================================================================"
echo "$PASS passed, $FAIL failed"
[ "$FAIL" = 0 ] && echo "SOAK PROOF: ALL PASS — load + leader kill + membership cycle, zero lost acks." || exit 1
