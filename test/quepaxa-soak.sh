#!/usr/bin/env bash
# test/quepaxa-soak.sh — Q8 (cw-cqf): the quepaxa engine's failover soak.
#
# 3-node --engine quepaxa cluster under continuous etcdctl write load; the
# COORDINATOR is SIGKILLed mid-run and restarted 10s later. QuePaxa has no
# elections — survivors keep committing via hedged randomized rounds, so the
# availability gap should be ~the hedge delay, not an election timeout.
# Assertions:
#   1. ZERO lost acknowledged writes (every OK'd put readable, exact value);
#   2. writes KEEP SUCCEEDING while the coordinator is dead (>= 1 OK in window);
#   3. hashkv convergence across all 3 members at the end;
#   4. the restarted coordinator serves reads.
set -uo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
BIN="${CRABSCHEME:-/Users/ztaylor/repos/workspaces/crabscheme/target/release/crabscheme}"
ETCDCTL="${ETCDCTL:-$(command -v etcdctl || echo /opt/homebrew/bin/etcdctl)}"
DUR="${DUR:-45}"
TAG="$(date +%s)"
BC=$((25400 + (TAG % 500))); BR=$((21400 + (TAG % 500)))
CL="a:127.0.0.1:$BR:$BC,b:127.0.0.1:$((BR+1)):$((BC+1)),c:127.0.0.1:$((BR+2)):$((BC+2))"
DB=/tmp/cws-qpsoak-$TAG
EPS="127.0.0.1:$BC,127.0.0.1:$((BC+1)),127.0.0.1:$((BC+2))"
PASS=0; FAIL=0
ok(){ PASS=$((PASS+1)); echo "  ok   $1"; }
bad(){ FAIL=$((FAIL+1)); echo "  FAIL $1"; }
declare -a PIDS
start_node(){ # $1=name
  $BIN run src/node-cluster.scm -- --node "$1" --db "$DB-$1" --cluster "$CL" \
    --engine quepaxa --durable yes > "/tmp/qpsoak-$1-$TAG.log" 2>&1 &
  PIDS+=($!)
}
cleanup(){ kill "${PIDS[@]}" 2>/dev/null; wait 2>/dev/null; }
trap cleanup EXIT

start_node a; start_node b; start_node c
sleep 8

ACKED=/tmp/qpsoak-acked-$TAG; : > "$ACKED"
KILL_WIN_OK=0
END=$((SECONDS + DUR)); I=0
COORD_PID="${PIDS[0]}"   # node a = coordinator (first voter)
KILLED=0; RESTART_AT=0
while [ $SECONDS -lt $END ]; do
  I=$((I+1))
  K="soak-$I"; V="v-$I-$TAG"
  if $ETCDCTL --endpoints="$EPS" --dial-timeout=2s --command-timeout=3s \
       put "$K" "$V" >/dev/null 2>&1; then
    echo "$K $V" >> "$ACKED"
    if [ $KILLED -eq 1 ] && [ $RESTART_AT -gt $SECONDS ]; then KILL_WIN_OK=$((KILL_WIN_OK+1)); fi
  fi
  # mid-run: SIGKILL the coordinator, restart after 10s
  if [ $KILLED -eq 0 ] && [ $SECONDS -gt $((END - DUR + 15)) ]; then
    kill -9 "$COORD_PID" 2>/dev/null
    KILLED=1; RESTART_AT=$((SECONDS + 10))
    echo "  -- coordinator (a) SIGKILLed at t=$SECONDS, restart at t=$RESTART_AT"
  fi
  if [ $KILLED -eq 1 ] && [ $RESTART_AT -ne 0 ] && [ $SECONDS -ge $RESTART_AT ]; then
    start_node a; RESTART_AT=0
    echo "  -- coordinator (a) restarted at t=$SECONDS"
  fi
done
sleep 6   # let the restarted node catch up

TOTAL=$(wc -l < "$ACKED" | tr -d ' ')
echo "  -- $TOTAL acknowledged writes; $KILL_WIN_OK acked while coordinator dead"
[ "$TOTAL" -gt 20 ] && ok "write load ran ($TOTAL acked)" || bad "write load too small ($TOTAL)"
[ "$KILL_WIN_OK" -ge 1 ] && ok "writes succeeded WHILE coordinator dead ($KILL_WIN_OK)" \
                         || bad "no write succeeded during coordinator death"

LOST=0
while read -r K V; do
  GOT=$($ETCDCTL --endpoints="127.0.0.1:$((BC+1))" get "$K" --print-value-only 2>/dev/null)
  [ "$GOT" = "$V" ] || { LOST=$((LOST+1)); [ $LOST -le 3 ] && echo "  lost: $K (want $V got '$GOT')"; }
done < "$ACKED"
[ "$LOST" -eq 0 ] && ok "zero lost acknowledged writes" || bad "$LOST acknowledged writes lost"

H1=$($ETCDCTL --endpoints="127.0.0.1:$BC"      endpoint hashkv 2>/dev/null | awk -F', ' '{print $2}')
H2=$($ETCDCTL --endpoints="127.0.0.1:$((BC+1))" endpoint hashkv 2>/dev/null | awk -F', ' '{print $2}')
H3=$($ETCDCTL --endpoints="127.0.0.1:$((BC+2))" endpoint hashkv 2>/dev/null | awk -F', ' '{print $2}')
[ -n "$H1" ] && [ "$H1" = "$H2" ] && [ "$H2" = "$H3" ] \
  && ok "hashkv converged on all 3 ($H1)" || bad "hashkv diverged: $H1 / $H2 / $H3"

RG=$($ETCDCTL --endpoints="127.0.0.1:$BC" get soak-1 --print-value-only 2>/dev/null)
[ "$RG" = "v-1-$TAG" ] && ok "restarted coordinator serves reads" || bad "restarted coordinator read failed ('$RG')"

echo "================================================================"
echo "$PASS passed, $FAIL failed"
if [ $FAIL -eq 0 ]; then echo "QUEPAXA SOAK: ALL PASS"; exit 0; else echo "QUEPAXA SOAK: FAILED"; exit 1; fi
