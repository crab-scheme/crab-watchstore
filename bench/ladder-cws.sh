#!/usr/bin/env bash
# bench/ladder-cws.sh — run the check-perf ladder against crab-watchstore only
# (single node, relaxed). For the full head-to-head incl. etcd see vs-etcd.sh.
set -uo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="${CRABSCHEME:-/Users/ztaylor/repos/workspaces/crabscheme/target/release/crabscheme}"
ETCDCTL="${ETCDCTL:-$(command -v etcdctl)}"
LOADS="${LOADS:-s m l}"
OUT="${OUT:-/tmp/cws-ladder}"
PORT=36888; RAFT=32888
rm -rf "$OUT"; mkdir -p "$OUT"
PIDS=()
cleanup(){ for p in "${PIDS[@]:-}"; do kill "$p" 2>/dev/null; done; pkill -f "check perf" 2>/dev/null; }
trap cleanup EXIT

for L in $LOADS; do
  pkill -f node-cluster.scm 2>/dev/null; sleep 1
  rm -rf "$OUT/db-$L"
  "$BIN" run "$ROOT/src/node-cluster.scm" -- \
    --node a --db "$OUT/db-$L" --durable no --shards "${SHARDS:-1}" \
    --cluster "a:127.0.0.1:${RAFT}:${PORT}" >"$OUT/cws-$L.log" 2>&1 &
  PIDS+=($!); SRV=$!
  for _ in $(seq 1 100); do
    grep -q "etcd KV gRPC serving on" "$OUT/cws-$L.log" 2>/dev/null && break
    kill -0 "$SRV" 2>/dev/null || { echo "server died (load=$L)"; tail -5 "$OUT/cws-$L.log"; exit 1; }
    sleep 0.5
  done
  sleep 1
  echo "== load=$L =="
  "$ETCDCTL" --endpoints="127.0.0.1:${PORT}" --dial-timeout=10s --command-timeout=120s \
    check perf --load "$L" 2>&1 | tr '\r' '\n' | grep -E "PASS|FAIL|Throughput|Slowest|Stddev" | sed 's/^.*]//'
  kill "$SRV" 2>/dev/null; wait "$SRV" 2>/dev/null
done
