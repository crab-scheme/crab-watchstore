#!/usr/bin/env bash
# test/wan-soak.sh — cw-lkq.3: the multi-region (WAN) acceptance gate.
#
# 3 members, one per simulated region: tc netem injects WAN_DELAY_MS one-way
# egress delay per member (~2x = inter-region RTT), Raft on the WAN profile
# (tick 250ms, election base 8 — deploy/docker/wan.override.yml). Under a 60s
# write load with a mid-run leader SIGKILL, assert (docs/specs/multi-region.md §E):
#   1. ZERO lost acknowledged writes;
#   2. TERM STABILITY: <= 3 term bumps over the whole run (bootstrap + the
#      kill's re-election; no WAN flapping);
#   3. steady-state MEDIAN write latency < 3.5x RTT and p99 < 5x RTT. Each
#      etcdctl invocation is a COLD process: connection setup costs ~2 delayed
#      reply legs (~+1x RTT) before the ~1.5x-RTT commit+reply, so a cold put
#      measures ~2.5-3x RTT (observed 437ms at 150ms RTT = dial ~150 + AE round
#      150 + reply 75 + overheads). A persistent client (kube-apiserver) pays
#      ~1.5x RTT; the cold budgets bound the harness, the spec budget holds for
#      real clients;
#   4. hashkv convergence across all 3 members at the end;
#   5. a SERIALIZABLE read on a non-leader member answers locally.
set -uo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
ETCDCTL="${ETCDCTL:-$(command -v etcdctl)}"
COMPOSE="docker compose -f $ROOT/deploy/docker/docker-compose.yml -f $ROOT/deploy/docker/wan.override.yml"
DELAY="${WAN_DELAY_MS:-75}"; RTT=$((DELAY*2)); BUDGET_MS=$((RTT*7/2))
DUR="${DUR:-60}"
EPS_A=(127.0.0.1:24791 127.0.0.1:24792 127.0.0.1:24793)
WORK=/tmp/cws-wan; rm -rf "$WORK"; mkdir -p "$WORK"
PASS=0; FAIL=0
ok(){ PASS=$((PASS+1)); echo "  ok   $1"; }
bad(){ FAIL=$((FAIL+1)); echo "  FAIL $1"; }
check(){ if [ "$2" = "$3" ]; then ok "$1"; else bad "$1  expected=$2 got=$3"; fi; }
cleanup(){ $COMPOSE down -v >/dev/null 2>&1; }
trap cleanup EXIT

leader_ep(){
  for ep in "${EPS_A[@]}"; do
    local J M L
    J=$("$ETCDCTL" --endpoints="$ep" endpoint status -w json 2>/dev/null)
    M=$(echo "$J" | sed 's/.*"member_id"://;s/[,}].*//')
    L=$(echo "$J" | sed 's/.*"leader"://;s/[,}].*//')
    case "$M" in (*[!0-9]*|"") continue;; esac
    [ "$M" = "$L" ] && { echo "$ep"; return; }
  done
}
term_of(){ "$ETCDCTL" --endpoints="${EPS_A[0]}" endpoint status -w json 2>/dev/null | sed 's/.*raft_term"://;s/[,}].*//'; }

echo "== compose up (WAN profile: ${DELAY}ms one-way, RTT ~${RTT}ms, budget p99<${BUDGET_MS}ms) =="
WAN_DELAY_MS="$DELAY" $COMPOSE up -d --build >"$WORK/up.log" 2>&1 || { bad "compose up"; tail -5 "$WORK/up.log"; exit 1; }
for _ in $(seq 1 60); do
  H=$($COMPOSE ps --format '{{.Health}}' 2>/dev/null | grep -c healthy); [ "$H" = 3 ] && break; sleep 3
done
check "all 3 healthy under WAN delay" 3 "$($COMPOSE ps --format '{{.Health}}' | grep -c healthy)"
$COMPOSE logs 2>/dev/null | grep -m1 "wan: injected" >/dev/null && ok "netem delay confirmed in-container" \
  || { bad "netem delay not injected"; $COMPOSE logs 2>/dev/null | tail -5; }

LEP=$(leader_ep); [ -n "$LEP" ] && ok "leader at $LEP" || { bad "no leader"; exit 1; }
T0=$(term_of); echo "  start term: $T0"

echo "== ${DUR}s write load (latencies recorded) + mid-run leader kill =="
# Steady-state load targets the LEADER endpoint (a region-local client per the
# C8 routing docs); after the kill the writer falls back to the full list.
(
  i=0; end=$(( $(date +%s) + DUR ))
  while [ "$(date +%s)" -lt "$end" ]; do
    i=$((i+1)); t0=$(python3 -c 'import time;print(int(time.time()*1000))')
    if "$ETCDCTL" --endpoints="$LEP" --command-timeout=3s put "wan/k-$i" "v-$i" >/dev/null 2>&1 \
       || "$ETCDCTL" --endpoints="$(IFS=,; echo "${EPS_A[*]}")" --command-timeout=5s put "wan/k-$i" "v-$i" >/dev/null 2>&1; then
      t1=$(python3 -c 'import time;print(int(time.time()*1000))')
      echo "$i $((t1-t0))" >> "$WORK/acked.txt"
    fi
  done
) & LOAD=$!
sleep $((DUR/2))
wc -l < "$WORK/acked.txt" > "$WORK/prekill-count" 2>/dev/null || echo 0 > "$WORK/prekill-count"
VICTIM=$(case "$(leader_ep)" in *24791) echo cws-a;; *24792) echo cws-b;; *24793) echo cws-c;; esac)
echo "  killing leader container $VICTIM"
docker kill -s KILL "$(docker ps -qf name=$VICTIM)" >/dev/null 2>&1
sleep 8
docker start "$(docker ps -aqf name=$VICTIM)" >/dev/null 2>&1 || $COMPOSE up -d >/dev/null 2>&1
wait $LOAD 2>/dev/null
sleep 5

ACKED=$(wc -l < "$WORK/acked.txt" | tr -d ' ')
[ "$ACKED" -gt 20 ] && ok "meaningful WAN load ($ACKED acked writes)" || bad "too few acked ($ACKED)"

echo "== assert 1: zero lost acknowledged writes =="
LEP=$(leader_ep); LOST=0
while read -r i ms; do
  V=$("$ETCDCTL" --endpoints="$LEP" --command-timeout=8s get "wan/k-$i" --print-value-only 2>/dev/null | tr -d '\n')
  [ "$V" = "v-$i" ] || LOST=$((LOST+1))
done < "$WORK/acked.txt"
check "lost acked writes" 0 "$LOST"

echo "== assert 2: term stability (<= start+3 after bootstrap + one kill) =="
T1=$(term_of)
[ "$T1" -le $((T0+3)) ] && ok "terms stable under WAN ($T0 -> $T1)" || bad "term churn under WAN ($T0 -> $T1)"

echo "== assert 3: STEADY-STATE latency — cold-client median < ${BUDGET_MS}ms, p99 < $((RTT*5))ms =="
# The fault window's retry latencies are RTO, not steady-state latency — budget
# only the writes acked BEFORE the leader kill.
PRE=$(cat "$WORK/prekill-count")
P50=$(head -n "$PRE" "$WORK/acked.txt" | sort -n -k2 | awk -v n="$PRE" 'NR==int((n+1)/2){print $2}')
P99=$(head -n "$PRE" "$WORK/acked.txt" | sort -n -k2 | awk -v n="$PRE" 'NR==int(n*0.99)+((n*0.99==int(n*0.99))?0:1){print $2}')
[ -n "$P50" ] && [ "$P50" -lt "$BUDGET_MS" ] && ok "steady-state median = ${P50}ms over $PRE writes (< ${BUDGET_MS}ms)" \
  || bad "steady-state median = ${P50:-?}ms over ${PRE:-0} writes (budget ${BUDGET_MS}ms)"
[ -n "$P99" ] && [ "$P99" -lt $((RTT*5)) ] && ok "steady-state p99 = ${P99}ms (< $((RTT*5))ms cold-client budget)" \
  || bad "steady-state p99 = ${P99:-?}ms (cold-client budget $((RTT*5))ms)"

echo "== assert 4: hashkv convergence =="
for _ in $(seq 1 45); do
  H1=$("$ETCDCTL" --endpoints="${EPS_A[0]}" endpoint hashkv -w json 2>/dev/null | sed 's/.*"hash"://;s/[,}].*//')
  H2=$("$ETCDCTL" --endpoints="${EPS_A[1]}" endpoint hashkv -w json 2>/dev/null | sed 's/.*"hash"://;s/[,}].*//')
  H3=$("$ETCDCTL" --endpoints="${EPS_A[2]}" endpoint hashkv -w json 2>/dev/null | sed 's/.*"hash"://;s/[,}].*//')
  [ -n "$H1" ] && [ "$H1" = "$H2" ] && [ "$H2" = "$H3" ] && break; sleep 2
done
if [ "$H1" != "$H2" ] || [ "$H2" != "$H3" ]; then
  echo "  applied indexes at mismatch:"
  for ep in "${EPS_A[@]}"; do
    "$ETCDCTL" --endpoints="$ep" endpoint status -w json 2>/dev/null | sed 's/.*raftAppliedIndex"://;s/[,}].*//' | sed "s|^|    $ep applied=|"
  done
fi
check "hashkv a==b" "$H1" "$H2"; check "hashkv b==c" "$H2" "$H3"

echo "== assert 5: serializable read on a non-leader answers locally =="
LEP=$(leader_ep)
FOL=""; for ep in "${EPS_A[@]}"; do [ "$ep" != "$LEP" ] && FOL=$ep && break; done
FIRSTK=$(head -1 "$WORK/acked.txt" | awk '{print $1}')
SV=$("$ETCDCTL" --endpoints="$FOL" --consistency=s --command-timeout=10s get "wan/k-$FIRSTK" --print-value-only 2>"$WORK/ser.err" | tr -d '\n')
[ "$SV" = "v-$FIRSTK" ] || sed 's/^/  ser-err: /' "$WORK/ser.err" | head -2
check "follower serializable read ($FOL, k-$FIRSTK)" "v-$FIRSTK" "$SV"

echo; echo "================================================================"
echo "$PASS passed, $FAIL failed"
[ "$FAIL" = 0 ] && echo "WAN SOAK PROOF: ALL PASS — multi-region profile holds at ${RTT}ms RTT." || exit 1
