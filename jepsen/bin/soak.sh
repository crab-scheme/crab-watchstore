#!/usr/bin/env bash
# soak.sh — run the crab-watchstore Jepsen matrix (each workload x fault) repeatedly
# and summarize the :valid? verdicts. Drives the running Docker cluster (jcws-control
# + jcws-n1..n5); bring it up first with:
#   bin/stage-docker.sh
#   docker compose -f docker/docker-compose.yml up -d --build
#
# Env knobs:  TEST_COUNT (iterations per cell, default 1)  TIME_LIMIT (s, default 100)
#
# Configs are the low-concurrency / realistic-client-retry settings that let the
# checkers reach a definitive verdict (single-group actor throughput pushes Knossos
# to :unknown at high load — a measurement limit, not a violation). Elle (append)
# tolerates more concurrency than Knossos.
#
# NOTE: this is the cw-u4a.35 FULL matrix (with nemesis). cw-u4a.34's bar is just the
# no-nemesis register/cas smoke (the first two rows).
set -uo pipefail

CTL=${CTL:-jcws-control}
NODES=${NODES:-n1,n2,n3,n4,n5}
TEST_COUNT=${TEST_COUNT:-1}
TIME_LIMIT=${TIME_LIMIT:-100}
SSH="--username root --ssh-private-key /root/.ssh/id_jepsen"

# workload|fault|extra-args  (faults: none partition kill membership)
# cw-u4a.35 matrix: register/cas under each fault (incl. the membership-change nemesis),
# Elle append, watch and lease. Keep --register-ops small + concurrency low so Knossos
# terminates (the leader-finding throughput floor keeps :ok counts modest; cas overall
# :valid? false with EMPTY :failures is the :stats :info-rate flag, not a violation —
# the Knossos workload checker is :valid? true). See docs/jepsen-validation.md.
MATRIX=(
  "register|none|--concurrency 5 --register-group 1 --register-ops 50"
  "register|partition|--concurrency 5 --register-group 1 --register-ops 50"
  "register|kill|--concurrency 5 --register-group 1 --register-ops 50"
  "register|membership|--concurrency 5 --register-group 1 --register-ops 40"
  "cas|partition|--concurrency 5 --register-group 1 --register-ops 50"
  "cas|kill|--concurrency 5 --register-group 1 --register-ops 50"
  "cas|membership|--concurrency 5 --register-group 1 --register-ops 40"
  "append|none|--concurrency 5"
  "append|kill|--concurrency 5"
  "watch|none|--concurrency 8 --rate 30"
  "watch|kill|--concurrency 8 --rate 30"
  "lease|none|--concurrency 5"
  "lease|kill|--concurrency 5"
)

# CELL_TIMEOUT bounds each cell; pkill reaps any orphaned JVM (a timed-out `lein run`
# wrapper can leave its java grandchild running, which would starve the next cell).
CELL_TIMEOUT=${CELL_TIMEOUT:-260}
run_one() {  # workload fault extra
  local wl=$1 fault=$2 extra=$3
  docker exec "$CTL" bash -c "cd /crab-watchstore/jepsen && timeout $CELL_TIMEOUT lein run test \
    --workload $wl --nemesis $fault --nodes $NODES $SSH \
    --time-limit $TIME_LIMIT $extra 2>&1 | tail -40; pkill -9 -x java 2>/dev/null; true"
}

verdict() {  # reads stdin (lein tail) once, prints true|false|unknown|crash
  local out; out=$(cat)
  if   grep -q "Everything looks good" <<<"$out"; then echo true
  elif grep -q "no anomalies found"    <<<"$out"; then echo unknown
  elif grep -q ":valid? false"         <<<"$out"; then echo false
  else echo crash ; fi
}

echo "crab-watchstore Jepsen soak — TEST_COUNT=$TEST_COUNT TIME_LIMIT=${TIME_LIMIT}s"
declare -a RESULTS ; rc=0
for cell in "${MATRIX[@]}"; do
  IFS='|' read -r wl fault extra <<<"$cell"
  for i in $(seq 1 "$TEST_COUNT"); do
    out=$(run_one "$wl" "$fault" "$extra")
    v=$(printf '%s' "$out" | verdict)
    printf '  %-9s %-10s #%-2s -> %s\n' "$wl" "$fault" "$i" "$v"
    RESULTS+=("$wl/$fault=$v")
    [ "$v" = "false" -o "$v" = "crash" ] && rc=1
  done
done

echo "=== SUMMARY ==="
printf '%s\n' "${RESULTS[@]}"
echo "exit=$rc (0=no anomalies/crashes; 1=anomaly or crash detected)"
exit $rc
