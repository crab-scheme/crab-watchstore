#!/usr/bin/env bash
# bench/sweep-cws.sh — concurrency + shard-count throughput sweep for a local
# 3-node crab-watchstore cluster, the harness behind the cw-mul findings.
#
# Modes:
#   CONC sweep (default):  ./bench/sweep-cws.sh
#       fixed SHARDS, sweep loadgen -conc over $CONCS. Finds the operating point.
#   SHARD sweep:           MODE=shards ./bench/sweep-cws.sh
#       fixed CONC, sweep --shard-groups over $SHARD_LIST. Finds the scale-out
#       optimum (= node count; N>nodes over-shards and collapses on a fixed host).
#
# Env: CRABSCHEME, CONCS (default "16 32 64 128 256 512"), CONC (shard mode, 512),
#      SHARDS (conc mode, 3), SHARD_LIST (shard mode, "1 3 6 9"), DUR (16s), VAL (256).
#
# Reports throughput + summed node %CPU per point. Each point is one fresh
# cluster (clean DB). REQUIRES bench/loadgen built to /tmp/cwsloadgen or $LOADGEN.
set -uo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="${CRABSCHEME:-/Users/ztaylor/repos/workspaces/crabscheme/target/release/crabscheme}"
LOADGEN="${LOADGEN:-/tmp/cwsloadgen}"
MODE="${MODE:-conc}"; DUR="${DUR:-16s}"; VAL="${VAL:-256}"
CONCS="${CONCS:-16 32 64 128 256 512}"; SHARDS="${SHARDS:-3}"
CONC="${CONC:-512}"; SHARD_LIST="${SHARD_LIST:-1 3 6 9}"
CL="a:127.0.0.1:39011:36911,b:127.0.0.1:39012:36912,c:127.0.0.1:39013:36913"
EPS=127.0.0.1:36911,127.0.0.1:36912,127.0.0.1:36913

[ -x "$BIN" ]     || { echo "FATAL: crabscheme not found: $BIN (set CRABSCHEME)"; exit 1; }
[ -x "$LOADGEN" ] || { echo "FATAL: loadgen not found: $LOADGEN — build: (cd bench/loadgen && go build -o $LOADGEN .)"; exit 1; }

one_point() {  # $1=shards $2=conc
  local N=$1 C=$2 OUT; OUT=$(mktemp -d "${TMPDIR:-/tmp}/cws-sweep.XXXXXX")
  pkill -9 -f node-cluster.scm 2>/dev/null; sleep 1
  cd "$ROOT"
  for nm in a b c; do
    "$BIN" run src/node-cluster.scm -- --node "$nm" --db "$OUT/db-$nm" --durable no \
      --shard-groups "$N" --cluster "$CL" >"$OUT/$nm.log" 2>&1 &
  done
  for _ in $(seq 1 180); do
    rc=$(grep -l "etcd KV gRPC serving" "$OUT"/a.log "$OUT"/b.log "$OUT"/c.log 2>/dev/null | wc -l)
    [ "$rc" -eq 3 ] && break; sleep 1
  done
  sleep 4
  "$LOADGEN" -endpoints="$EPS" -shards="$N" -mode=sharded -dur="$DUR" -conc="$C" -valsize="$VAL" \
    >"$OUT/l.txt" 2>/dev/null &
  local LP=$!; sleep "$(( ${DUR%s} / 2 ))"
  local cpu; cpu=$(ps -A -o %cpu,command | grep "[n]ode-cluster.scm" | awk '{s+=$1} END{printf "%.0f", s}')
  wait $LP
  printf "shards=%-2s conc=%-4s %s  nodes_cpu=%s%%\n" "$N" "$C" \
    "$(grep -o 'THROUGHPUT=[0-9]* writes/s (fail-rate=[0-9.]*%)' "$OUT/l.txt")" "$cpu"
  pkill -9 -f node-cluster.scm 2>/dev/null
}

if [ "$MODE" = "shards" ]; then
  echo "# shard sweep @ conc=$CONC (optimum = node count; N>nodes over-shards)"
  for N in $SHARD_LIST; do one_point "$N" "$CONC"; done
else
  echo "# concurrency sweep @ shards=$SHARDS (drive conc>=256 — conc=64 is latency-bound)"
  for C in $CONCS; do one_point "$SHARDS" "$C"; done
fi
