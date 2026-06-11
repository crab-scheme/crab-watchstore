#!/usr/bin/env bash
# bench/vs-etcd.sh — crab-watchstore vs REAL etcd, the SAME TOOL on both (cw-u4a.36).
#
# Because crab-watchstore speaks the etcd v3 gRPC API, the official etcd client
# `etcdctl` AND etcd's own load generator `etcdctl check perf` drive BOTH servers
# — a genuinely apples-to-apples head-to-head. crab-cache (RESP) could NOT get
# this: no single tool hit both crab-cache and etcd, so etcd was only an
# order-of-magnitude reference. Here the identical gRPC client, identical load,
# identical host hit both.
#
# We EXPECT crab-watchstore to trail etcd — etcd is Go + bbolt + a decade of
# tuning; crab-watchstore is an interpreted-Scheme store on a cooperative green
# actor runtime. The deliverable is an HONEST number; the proof of this project is
# correctness (Jepsen strict-serializable, cw-u4a.35) + full feature parity +
# distribution at a competitive — not necessarily winning — speed, all in CrabScheme.
#
# What it measures (single-node):
#   1. SINGLE-OP ROUND-TRIP LATENCY — hyperfine over `etcdctl put/get/txn/range`.
#      Each etcdctl op is a cold gRPC dial + one RPC; that dial cost is identical
#      for both servers, so the per-op delta is the server.
#   2. SUSTAIN-AT-LOAD LADDER — `etcdctl check perf --load={s,m,l}` against each.
#      check perf is etcd's own *rate-limited* health probe, NOT a max-throughput
#      bench: it drives a fixed target rate (s≈150, m≈1000, l≈8000 writes/s) for
#      60s and PASS/FAILs on sustained latency. So the honest throughput story is
#      "the highest load each server SUSTAINS (PASS) and at what tail latency."
#      crab-watchstore is expected to top out well below etcd — we report where.
#
# Requirements: etcd + etcdctl on PATH (or $ETCD/$ETCDCTL), hyperfine, and a
#   crabscheme built --features stdlib-store,grpc (CRABSCHEME=path).
# Tunables: LOADS (ladder, default "s m l"), DURABLE (no|yes, default no),
#   RUNS (hyperfine runs, default 20), D (value bytes, default 256).
#
# NOTE on macOS: fsync() is not a true durability barrier there (only F_FULLFSYNC
# is), so the durable/relaxed distinction is ~free on darwin for BOTH servers —
# the numbers below isolate store + transport cost, not disk. Run on Linux with
# DURABLE=yes for a real per-write-fsync comparison.
set -uo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="${CRABSCHEME:-/Users/ztaylor/repos/workspaces/crabscheme/target/release/crabscheme}"
ETCD="${ETCD:-$(command -v etcd || true)}"
ETCDCTL="${ETCDCTL:-$(command -v etcdctl || true)}"
HYPERFINE="${HYPERFINE:-$(command -v hyperfine || true)}"
LOADS="${LOADS:-s m l}"; DURABLE="${DURABLE:-no}"; RUNS="${RUNS:-20}"; D="${D:-256}"

[ -x "$BIN" ]       || { echo "FATAL: crabscheme binary not found: $BIN"; exit 1; }
[ -n "$ETCD" ]      || { echo "FATAL: etcd not on PATH (set \$ETCD)"; exit 1; }
[ -n "$ETCDCTL" ]   || { echo "FATAL: etcdctl not on PATH (set \$ETCDCTL)"; exit 1; }
[ -n "$HYPERFINE" ] || { echo "FATAL: hyperfine not on PATH"; exit 1; }

TAG="$(date +%s%N)"
CWS_PORT=$(( 36000 + (TAG % 2000) ))
CWS_RAFT=$(( 32000 + (TAG % 2000) ))
ETCD_CLIENT=2379; ETCD_PEER=2390
CWS_EP="127.0.0.1:${CWS_PORT}"
ETCD_EP="127.0.0.1:${ETCD_CLIENT}"
WORK="$(mktemp -d "${TMPDIR:-/tmp}/cws-vsetcd.XXXXXX")"
PIDS=()
cleanup(){ for p in "${PIDS[@]:-}"; do kill "$p" 2>/dev/null; done
           pkill -f node-cluster.scm 2>/dev/null
           pkill -f "$ETCD --data-dir $WORK" 2>/dev/null
           pkill -f "check perf" 2>/dev/null
           rm -rf "$WORK"; }
trap cleanup EXIT

VAL="$(head -c "$D" </dev/zero | tr '\0' 'x')"
ectl(){ "$ETCDCTL" --endpoints="$1" "${@:2}"; }   # ETCDCTL_API=3 is the default in 3.5+

start_cws(){
  rm -rf "$WORK/cws"
  "$BIN" run "$ROOT/src/node-cluster.scm" -- \
    --node a --db "$WORK/cws" --durable "$DURABLE" \
    --cluster "a:127.0.0.1:${CWS_RAFT}:${CWS_PORT}" >"$WORK/cws.log" 2>&1 &
  PIDS+=($!); local pid="${PIDS[-1]}"
  for _ in $(seq 1 100); do
    grep -q "etcd KV gRPC serving on" "$WORK/cws.log" 2>/dev/null && { sleep 1; return 0; }
    kill -0 "$pid" 2>/dev/null || { echo "  crab-watchstore died on start:"; sed 's/^/    /' "$WORK/cws.log"; return 1; }
    sleep 0.5
  done
  echo "  crab-watchstore did not serve in time:"; tail -20 "$WORK/cws.log" | sed 's/^/    /'; return 1
}

start_etcd(){
  pkill -f "$ETCD --data-dir $WORK" 2>/dev/null; sleep 1; rm -rf "$WORK/etcd"
  "$ETCD" --data-dir "$WORK/etcd" \
    --listen-client-urls "http://${ETCD_EP}" --advertise-client-urls "http://${ETCD_EP}" \
    --listen-peer-urls "http://127.0.0.1:${ETCD_PEER}" \
    --initial-cluster "default=http://127.0.0.1:${ETCD_PEER}" \
    --initial-advertise-peer-urls "http://127.0.0.1:${ETCD_PEER}" \
    --log-level error >"$WORK/etcd.log" 2>&1 &
  PIDS+=($!)
  for _ in $(seq 1 80); do
    ectl "$ETCD_EP" endpoint health 2>/dev/null | grep -q healthy && return 0
    sleep 0.3
  done
  echo "  etcd did not become healthy:"; tail -20 "$WORK/etcd.log" | sed 's/^/    /'; return 1
}

# One timeout-guarded check-perf run, parsed from a FILE (never a live pipe — that
# was what wedged: command-substituting the multi-MB live progress bar). Echoes
# "VERDICT|throughput_w_per_s|slowest_s|stddev_s".
checkperf(){ # endpoint loadlevel
  local ep="$1" load="$2"
  local raw="$WORK/cp-${ep//[:.]/_}-${load}.txt"
  timeout 180 "$ETCDCTL" --endpoints="$ep" check perf --load="$load" >"$raw" 2>&1
  local rc=$?
  local clean="$raw.clean"
  tr '\r' '\n' <"$raw" >"$clean"
  local v t s d
  # Verdict from the reliable summary line ("PASS: Throughput is N writes/s"); the
  # bare trailing "PASS"/"FAIL" line sometimes carries progress-bar residue after the
  # \r-collapse, so derive from the throughput line's prefix instead, with the bare
  # token as a fallback.
  v="$(grep -aoE '(PASS|FAIL): Throughput is [0-9]+ writes/s' "$clean" | tail -1 | grep -oE '^(PASS|FAIL)')"
  [ -z "$v" ] && v="$(grep -aE '^(PASS|FAIL)$' "$clean" | tail -1)"
  t="$(sed -n 's/.*Throughput is \([0-9]*\) writes\/s.*/\1/p' "$clean" | tail -1)"
  s="$(sed -n 's/.*Slowest request took \([0-9.]*\)s.*/\1/p' "$clean" | tail -1)"
  d="$(sed -n 's/.*Stddev is \([0-9.]*\)s.*/\1/p' "$clean" | tail -1)"
  # rc=124 only means TIMEOUT if the verdict never printed: check perf prints its
  # PASS/FAIL summary BEFORE the (slow on cws) 60k-key cleanup DeleteRange, so a
  # kill during cleanup must not poison an already-delivered verdict.
  [ "$rc" = 124 ] && [ -z "$v" ] && v="TIMEOUT"
  printf '%s|%s|%s|%s' "${v:-INCOMPLETE}" "${t:-?}" "${s:-?}" "${d:-?}"
}
load_target(){ case "$1" in s) echo 150;; m) echo 1000;; l) echo 8000;; xl) echo 15000;; *) echo '?';; esac; }

# hyperfine mean (ms) for one etcdctl op against an endpoint.
hf(){ # cmd...
  local json="$WORK/hf-$RANDOM.json"
  "$HYPERFINE" --warmup 3 --runs "$RUNS" --export-json "$json" --shell=bash "$*" >/dev/null 2>&1 || { echo "?"; return; }
  awk 'match($0,/"mean": *([0-9.eE+-]+)/,m){printf "%.1f", m[1]*1000; exit}' "$json"
}

echo "# crab-watchstore vs etcd — same etcdctl on both (single-node)"
echo
echo "Host: \`$(uname -mrs)\`; cores: $(sysctl -n hw.ncpu 2>/dev/null || nproc 2>/dev/null || echo '?'); values ${D}B; DURABLE=$DURABLE."
echo "etcd: \`$("$ETCD" --version 2>/dev/null | head -1)\`; driver: \`$("$ETCDCTL" version 2>/dev/null | head -1)\`."
echo "_The SAME \`etcdctl\` — the official etcd v3 client — drives both servers; crab-watchstore answers the identical gRPC wire._"
echo

echo "## bring-up"
echo
start_etcd && echo "  etcd healthy on $ETCD_EP"            || echo "  _etcd failed to start_"
start_cws  && echo "  crab-watchstore serving on $CWS_EP"  || { echo "  _crab-watchstore failed to start — aborting_"; exit 1; }
echo

# seed a known key + a small prefix for get/range latency
ectl "$ETCD_EP" put hk "$VAL" >/dev/null 2>&1; ectl "$CWS_EP" put hk "$VAL" >/dev/null 2>&1
for i in 0 1 2 3 4 5 6 7 8 9; do
  ectl "$ETCD_EP" put "bench/$i" "$VAL" >/dev/null 2>&1
  ectl "$CWS_EP"  put "bench/$i" "$VAL" >/dev/null 2>&1
done
# txn input for the latency probe: zero compares, one put, zero failure ops (always-true)
printf '\nput txnk "%s"\n\n\n' "$VAL" >"$WORK/txn.in"

echo "## Single-op round-trip latency — hyperfine over etcdctl (mean ms, ${RUNS} runs)"
echo
echo "| op | etcd (ms) | crab-watchstore (ms) |"
echo "|---|---|---|"
echo "| put   | $(hf "$ETCDCTL --endpoints=$ETCD_EP put hk $VAL")            | $(hf "$ETCDCTL --endpoints=$CWS_EP put hk $VAL") |"
echo "| get   | $(hf "$ETCDCTL --endpoints=$ETCD_EP get hk")                 | $(hf "$ETCDCTL --endpoints=$CWS_EP get hk") |"
echo "| range | $(hf "$ETCDCTL --endpoints=$ETCD_EP get --prefix bench/")    | $(hf "$ETCDCTL --endpoints=$CWS_EP get --prefix bench/") |"
echo "| txn   | $(hf "$ETCDCTL --endpoints=$ETCD_EP txn < $WORK/txn.in")          | $(hf "$ETCDCTL --endpoints=$CWS_EP txn < $WORK/txn.in") |"
echo
echo "_Each op is a cold gRPC dial + one RPC; the dial cost is identical for both, so the delta is the server._"
echo

echo "## Sustain-at-load ladder — \`etcdctl check perf --load=L\` (60s/level; PASS = sustained at target tail latency)"
echo
echo "| load (target w/s) | etcd | crab-watchstore |"
echo "|---|---|---|"
cws_done=0
for L in $LOADS; do
  tgt="$(load_target "$L")"
  IFS='|' read -r ev et es ed <<<"$(checkperf "$ETCD_EP" "$L")"
  ecell="**${ev}** · ${et} w/s · slowest ${es}s"
  if [ "$cws_done" = 0 ]; then
    IFS='|' read -r cv ct cs cd <<<"$(checkperf "$CWS_EP" "$L")"
    ccell="**${cv}** · ${ct} w/s · slowest ${cs}s"
    [ "$cv" != PASS ] && cws_done=1   # stop climbing once it can't sustain a level
  else
    ccell="_skipped (did not sustain a lower load)_"
  fi
  echo "| ${L} (~${tgt}) | ${ecell} | ${ccell} |"
done
echo
echo "_check perf is rate-limited: a PASS means \"sustained the target rate at acceptable tail latency,\" not \"max throughput.\"_"
echo "_Raw per-level output: \`$WORK/cp-*.txt\` (removed on cleanup)._"
