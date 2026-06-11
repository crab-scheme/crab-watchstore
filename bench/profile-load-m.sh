#!/usr/bin/env bash
# bench/profile-load-m.sh — profile the gRPC write path under check-perf load=m (cw-b5w.1).
# Starts a single-node crab-watchstore (relaxed), drives `etcdctl check perf --load m`,
# and captures `sample` profiles of the server mid-load. Output: $OUT/{cws.log,perf.out,sample*.txt}
set -uo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="${CRABSCHEME:-/Users/ztaylor/repos/workspaces/crabscheme/target/release/crabscheme}"
ETCDCTL="${ETCDCTL:-$(command -v etcdctl)}"
OUT="${OUT:-/tmp/cws-profile-m}"
PORT=36777; RAFT=32777
rm -rf "$OUT"; mkdir -p "$OUT"
PIDS=()
cleanup(){ for p in "${PIDS[@]:-}"; do kill "$p" 2>/dev/null; done; pkill -f "check perf" 2>/dev/null; }
trap cleanup EXIT

rm -rf "$OUT/db"
"$BIN" run "$ROOT/src/node-cluster.scm" -- \
  --node a --db "$OUT/db" --durable no \
  --cluster "a:127.0.0.1:${RAFT}:${PORT}" >"$OUT/cws.log" 2>&1 &
PIDS+=($!); SRV=$!
for _ in $(seq 1 100); do
  grep -q "etcd KV gRPC serving on" "$OUT/cws.log" 2>/dev/null && break
  kill -0 "$SRV" 2>/dev/null || { echo "server died:"; cat "$OUT/cws.log"; exit 1; }
  sleep 0.5
done
sleep 1
echo "server pid=$SRV up on :$PORT"

"$ETCDCTL" --endpoints="127.0.0.1:${PORT}" check perf --load m >"$OUT/perf.out" 2>&1 &
PERF=$!
sleep 8   # let load ramp
for i in 1 2 3; do
  sample "$SRV" 10 -file "$OUT/sample$i.txt" >/dev/null 2>&1
  sleep 2
done
wait "$PERF"
echo "== check perf result =="; cat "$OUT/perf.out"
echo "samples in $OUT/sample{1,2,3}.txt"
