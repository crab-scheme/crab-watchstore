#!/usr/bin/env bash
# bench/cluster-3node.sh — launch a local 3-node crab-watchstore cluster and
# leave it running, then print readiness. Used by sweep-cws.sh and for manual
# load tests. Each node gets its own RocksDB dir under $OUT and N independent
# Raft groups (--shard-groups). Leaders spread shard S -> node (S mod 3).
#
#   OUT=/tmp/cws SHARDS=3 DURABLE=no ./bench/cluster-3node.sh
#
# Then drive load with bench/loadgen (built: cd bench/loadgen && go build .):
#   loadgen -endpoints=127.0.0.1:36911,127.0.0.1:36912,127.0.0.1:36913 \
#           -shards=3 -mode=sharded -conc=512 -valsize=256 -dur=20s
#
# KEY MEASUREMENT NOTE (cw-mul): always drive at conc>=256. At conc=64 the
# system is LATENCY-bound (node CPU plateaus ~6 cores while throughput keeps
# climbing with concurrency); conc=64 understates achievable throughput ~60-70%.
set -uo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="${CRABSCHEME:-/Users/ztaylor/repos/workspaces/crabscheme/target/release/crabscheme}"
SHARDS="${SHARDS:-3}"; DURABLE="${DURABLE:-no}"
OUT="${OUT:-$(mktemp -d "${TMPDIR:-/tmp}/cws-3node.XXXXXX")}"
CL="a:127.0.0.1:39011:36911,b:127.0.0.1:39012:36912,c:127.0.0.1:39013:36913"

[ -x "$BIN" ] || { echo "FATAL: crabscheme binary not found: $BIN (set CRABSCHEME)"; exit 1; }
pkill -9 -f node-cluster.scm 2>/dev/null; sleep 1
cd "$ROOT"
for nm in a b c; do
  "$BIN" run src/node-cluster.scm -- \
    --node "$nm" --db "$OUT/db-$nm" --durable "$DURABLE" \
    --shard-groups "$SHARDS" --cluster "$CL" >"$OUT/$nm.log" 2>&1 &
done
for _ in $(seq 1 180); do
  rc=$(grep -l "etcd KV gRPC serving" "$OUT"/a.log "$OUT"/b.log "$OUT"/c.log 2>/dev/null | wc -l)
  [ "$rc" -eq 3 ] && break; sleep 1
done
sleep 3
echo "OUT=$OUT  SHARDS=$SHARDS  DURABLE=$DURABLE"
grep -h "shard .* ready\|gRPC serving" "$OUT"/a.log "$OUT"/b.log "$OUT"/c.log | sort
echo "(cluster left running; pkill -9 -f node-cluster.scm to stop)"
