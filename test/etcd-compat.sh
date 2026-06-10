#!/usr/bin/env bash
# test/etcd-compat.sh — Phase 5 capstone: prove REAL etcd clients work against
# crab-watchstore via the go.etcd.io/etcd/client/v3 Go library (cw-u4a.24).
#
# Starts a SINGLE-NODE crab-watchstore (wall-clock-unique port + DB) and runs
# the test/clientv3-compat Go program (clientv3 v3.5.x) against it.  The Go
# program exercises KV / Txn / Watch / Lease / Compact end-to-end and exits
# nonzero on any assertion failure.
#
# Usage:  bash test/etcd-compat.sh
# Env:    CRABSCHEME = path to crabscheme binary (default below)

set -uo pipefail

BIN="${CRABSCHEME:-/Users/ztaylor/repos/workspaces/crabscheme/target/release/crabscheme}"

# Unique port + DB per run (wall-clock nanoseconds).
TAG="$(date +%s%N)"
PORT="$(( 36000 + (TAG % 4000) ))"
RAFTPORT="$(( 32000 + (TAG % 4000) ))"
DB="/tmp/cws-compat-${TAG}"
EP="127.0.0.1:${PORT}"
LOG="/tmp/cws-compat-node-${TAG}.log"

NODE_PID=""

cleanup() {
  [ -n "$NODE_PID" ] && kill "$NODE_PID" 2>/dev/null
  wait "$NODE_PID" 2>/dev/null
  rm -rf "$DB"* 2>/dev/null
}
trap cleanup EXIT

# ---- bring up the single-node crab-watchstore ----
echo "== bring up single-node crab-watchstore (etcd gRPC on $EP) =="
"$BIN" run src/node-cluster.scm -- \
  --node a --db "$DB" --cluster "a:127.0.0.1:${RAFTPORT}:${PORT}" > "$LOG" 2>&1 &
NODE_PID=$!

up=0
for _ in $(seq 1 80); do
  if grep -q "etcd KV gRPC serving on" "$LOG" 2>/dev/null; then up=1; break; fi
  if ! kill -0 "$NODE_PID" 2>/dev/null; then break; fi
  sleep 0.5
done
if [ "$up" != "1" ]; then
  echo "FATAL: crab-watchstore did not start. log:"
  cat "$LOG"
  exit 1
fi
grep "etcd KV gRPC serving on" "$LOG" | sed 's/^/  /'

echo
echo "== running clientv3 Go program against $EP =="
cd "$(dirname "$0")/clientv3-compat"
go run . "$EP"
STATUS=$?

echo
if [ "$STATUS" -eq 0 ]; then
  echo "ETCD COMPAT (clientv3): ALL PASS"
else
  echo "ETCD COMPAT (clientv3): FAILED (exit $STATUS)"
  echo "--- node log tail ---"
  tail -30 "$LOG"
fi

exit "$STATUS"
