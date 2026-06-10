#!/usr/bin/env bash
# test/health-metrics.sh — prove crab-watchstore's endpoint health/status surface (cw-u4a.33):
#   * the standard gRPC grpc.health.v1.Health/Check  -> SERVING            (on the client port)
#   * an HTTP /health   (etcd --listen-metrics-urls faithful)             (dedicated metrics port)
#   * an HTTP /version  ({"etcdserver":..,"etcdcluster":..})
#   * an HTTP /metrics  (Prometheus exposition with REAL per-node gauges)
# over a STATIC 3-NODE cluster (a,b,c) on the live cs-net transport + h2c gRPC client ports,
# each with a dedicated HTTP metrics port (= client-port + 10000).
#
# WHY a separate metrics port: the client port runs the Rust h2c gRPC transport (hardcoded
# application/grpc), which cannot serve plain HTTP — exactly why etcd uses --listen-metrics-urls.
# The gRPC Health service rides the client port (new Scheme handlers, no Rust change).
#
# gRPC client: grpcurl is preferred but not assumed on PATH; we fall back to a tiny grpc-go
# Health client (test/health-probe, the same grpc-go the .24 proof uses — builds offline from
# the module cache).  If neither is available the gRPC Check is SKIPPED with a loud note (the
# HTTP + etcdctl assertions still run).  etcdctl does NOT call the Health service, so it is used
# only for `endpoint health` (Range-probe) + `endpoint status` (.32) — not for Check.
#
# Usage:  bash test/health-metrics.sh        (run in BACKGROUND — foreground exits 144)
# Env:    CRABSCHEME = path to the crabscheme binary (default below).
#         ETCDCTL    = path to etcdctl (default: etcdctl on PATH, else homebrew Cellar).

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"
cd "$REPO_ROOT" || { echo "FATAL: cannot cd to repo root"; exit 1; }

BIN="${CRABSCHEME:-/Users/ztaylor/repos/workspaces/crabscheme/target/release/crabscheme}"
ETCDCTL="${ETCDCTL:-}"
if [ -z "$ETCDCTL" ]; then
  if command -v etcdctl >/dev/null 2>&1; then ETCDCTL="$(command -v etcdctl)";
  elif [ -x /opt/homebrew/bin/etcdctl ]; then ETCDCTL="/opt/homebrew/bin/etcdctl";
  else echo "FATAL: etcdctl not found (set ETCDCTL=...)"; exit 1; fi
fi

# unique tag + ports + db dirs per run so back-to-back spaced runs are clean.
TAG="$(date +%s%N)"
BC="$(( 29200 + (TAG % 1000) ))"          # client-port base (a=BC, b=BC+1, c=BC+2)
BR="$(( 23200 + (TAG % 1000) ))"          # raft-port   base
CP_A="$BC";        CP_B="$((BC+1))";  CP_C="$((BC+2))"
RP_A="$BR";        RP_B="$((BR+1))";  RP_C="$((BR+2))"
MP_A="$((CP_A+10000))"; MP_B="$((CP_B+10000))"; MP_C="$((CP_C+10000))"   # metrics = client + 10000
EP_A="127.0.0.1:${CP_A}"; EP_B="127.0.0.1:${CP_B}"; EP_C="127.0.0.1:${CP_C}"
CLUSTER="a:127.0.0.1:${RP_A}:${CP_A},b:127.0.0.1:${RP_B}:${CP_B},c:127.0.0.1:${RP_C}:${CP_C}"
DB="/tmp/cws-health-${TAG}"
LOG_A="/tmp/cws-health-a-${TAG}.log"; LOG_B="/tmp/cws-health-b-${TAG}.log"; LOG_C="/tmp/cws-health-c-${TAG}.log"
PROBE_BIN="/tmp/cw-health-probe-${TAG}"

PASS=0; FAIL=0
PID_A=""; PID_B=""; PID_C=""

cleanup() {
  for p in "$PID_A" "$PID_B" "$PID_C"; do [ -n "$p" ] && kill "$p" 2>/dev/null; done
  for p in "$PID_A" "$PID_B" "$PID_C"; do [ -n "$p" ] && wait "$p" 2>/dev/null; done
  rm -rf "$DB"* "$PROBE_BIN" 2>/dev/null
}
trap cleanup EXIT

ok()  { PASS=$((PASS+1)); echo "  ok   $1"; }
bad() { FAIL=$((FAIL+1)); echo "  FAIL $1"; echo "         $2"; }
note(){ echo "  NOTE $1"; }
assert_has()    { if echo "$2" | grep -qiF -- "$1"; then ok "$3"; else bad "$3" "expected [$1], got: $(echo "$2" | tr '\n' '|')"; fi; }
assert_eq()     { if [ "$1" = "$2" ]; then ok "$3"; else bad "$3" "expected [$1], got [$2]"; fi; }
assert_gt0()    { if [ -n "$1" ] && [ "$1" -gt 0 ] 2>/dev/null; then ok "$2"; else bad "$2" "expected >0, got [$1]"; fi; }
assert_ge()     { if [ -n "$1" ] && [ "$1" -ge "$2" ] 2>/dev/null; then ok "$3"; else bad "$3" "expected >= $2, got [$1]"; fi; }

# etcdctl against a SINGLE endpoint, warnings/deprecation/unrecognized stripped.
ectl1() { local ep="$1"; shift; "$ETCDCTL" --endpoints="$ep" --insecure-transport=true \
            --dial-timeout=5s --command-timeout=5s "$@" 2>&1 \
            | grep -v 'level":"warn' | grep -v -i 'deprecat' | grep -v unrecognized; }
mcurl() { curl -s --max-time 5 "http://127.0.0.1:${1}${2}"; }                 # mcurl <metrics-port> <path>
json_num() { echo "$1" | grep -oE "\"$2\":\"?[0-9]+" | head -1 | grep -oE '[0-9]+'; }
# value of a Prometheus gauge line "<name> <int>" from a /metrics body.
metric_val() { echo "$1" | grep -E "^$2 " | head -1 | awk '{print $2}'; }

# ---- detect/build the gRPC Health client BEFORE bring-up ----
# A CPU-heavy `go build` mid-run can starve the running nodes' Raft heartbeat and trigger a
# spurious election, so build the probe up front (cluster not yet running).  grpcurl preferred;
# else a tiny grpc-go Health client (builds offline from the module cache); else SKIP the Check
# with a loud note (the HTTP + etcdctl proofs still run).
HEALTH_CLIENT=""
if command -v grpcurl >/dev/null 2>&1; then
  HEALTH_CLIENT="grpcurl"
elif command -v go >/dev/null 2>&1 && [ -d test/health-probe ]; then
  echo "== build the grpc-go Health probe (grpcurl not on PATH) =="
  if (cd test/health-probe && GOFLAGS=-mod=mod go build -o "$PROBE_BIN" . ) >/tmp/cw-probe-build-${TAG}.log 2>&1; then
    HEALTH_CLIENT="goprobe"; echo "  built $PROBE_BIN"
  else
    echo "  NOTE grpc-go health probe failed to build:"; sed 's/^/         /' /tmp/cw-probe-build-${TAG}.log
  fi
  rm -f /tmp/cw-probe-build-${TAG}.log
fi

echo "== bring up 3-node crab-watchstore cluster (a/b/c) — health/metrics endpoints =="
echo "   cluster: $CLUSTER"
echo "   metrics ports: a=$MP_A b=$MP_B c=$MP_C"
"$BIN" run src/node-cluster.scm -- --node a --db "${DB}-a" --cluster "$CLUSTER" > "$LOG_A" 2>&1 & PID_A=$!
"$BIN" run src/node-cluster.scm -- --node b --db "${DB}-b" --cluster "$CLUSTER" > "$LOG_B" 2>&1 & PID_B=$!
"$BIN" run src/node-cluster.scm -- --node c --db "${DB}-c" --cluster "$CLUSTER" > "$LOG_C" 2>&1 & PID_C=$!

up=0
for _ in $(seq 1 120); do
  if grep -q "etcd KV gRPC serving on" "$LOG_A" 2>/dev/null \
  && grep -q "etcd KV gRPC serving on" "$LOG_B" 2>/dev/null \
  && grep -q "etcd KV gRPC serving on" "$LOG_C" 2>/dev/null; then up=1; break; fi
  if ! kill -0 "$PID_A" 2>/dev/null || ! kill -0 "$PID_B" 2>/dev/null || ! kill -0 "$PID_C" 2>/dev/null; then break; fi
  sleep 0.5
done
if [ "$up" != "1" ]; then
  echo "FATAL: cluster did not come up. logs:"; for L in "$LOG_A" "$LOG_B" "$LOG_C"; do echo "--- $L ---"; cat "$L"; done; exit 1
fi
# wait for the dedicated metrics HTTP listeners too (cw-u4a.33 banner).
mup=0
for _ in $(seq 1 60); do
  if grep -q "metrics/health HTTP serving on" "$LOG_A" 2>/dev/null \
  && grep -q "metrics/health HTTP serving on" "$LOG_B" 2>/dev/null \
  && grep -q "metrics/health HTTP serving on" "$LOG_C" 2>/dev/null; then mup=1; break; fi
  sleep 0.5
done
if [ "$mup" != "1" ]; then
  echo "FATAL: metrics HTTP listeners did not come up. logs:"; for L in "$LOG_A" "$LOG_B" "$LOG_C"; do echo "--- $L ---"; tail -20 "$L"; done; exit 1
fi
grep -h "shard 0 ready\|etcd KV gRPC serving on\|metrics/health HTTP serving on" "$LOG_A" "$LOG_B" "$LOG_C" | sed 's/^/  /'

# ---- leader discovery (a write commits ONLY on the leader; followers redirect) ----
LEADER_EP=""
find_leader() {
  LEADER_EP=""
  for _ in $(seq 1 30); do
    for ep in "$EP_A" "$EP_B" "$EP_C"; do
      if "$ETCDCTL" --endpoints="$ep" --insecure-transport=true --dial-timeout=2s \
           --command-timeout=2s put "__leader_probe_${RANDOM}__" 1 2>/dev/null | grep -q "OK"; then
        LEADER_EP="$ep"; return 0
      fi
    done
    sleep 0.3
  done
  return 1
}
echo
echo "== discover the leader endpoint =="
if ! find_leader; then echo "FATAL: no leader serving writes"; for L in "$LOG_A" "$LOG_B" "$LOG_C"; do echo "--- $L ---"; tail -15 "$L"; done; exit 1; fi
LEADER_NAME=""; case "$LEADER_EP" in "$EP_A") LEADER_NAME=a;; "$EP_B") LEADER_NAME=b;; "$EP_C") LEADER_NAME=c;; esac
echo "  leader endpoint = $LEADER_EP  (node $LEADER_NAME)"

# ---- write a few keys so the mvcc gauges are non-trivial, and let them replicate ----
echo
echo "== write keys (so /metrics keys_total/revision are non-trivial) =="
NWROTE=6
for kv in health/k1=a health/k2=b health/k3=c health/k4=d health/k5=e health/k6=f; do
  ectl1 "$LEADER_EP" put "${kv%%=*}" "${kv#*=}" >/dev/null
done
ok "wrote $NWROTE keys to the leader"

# ---- wait until every node is readiness-converged (followers apply the writes) ----
echo
echo "== wait for all 3 nodes to report /health = true (followers catch up) =="
allhealthy=0
for _ in $(seq 1 40); do
  ha="$(mcurl "$MP_A" /health)"; hb="$(mcurl "$MP_B" /health)"; hc="$(mcurl "$MP_C" /health)"
  if echo "$ha" | grep -q '"health":"true"' && echo "$hb" | grep -q '"health":"true"' && echo "$hc" | grep -q '"health":"true"'; then
    allhealthy=1; break; fi
  sleep 0.3
done
assert_eq "1" "$allhealthy" "all 3 nodes report /health true (has-leader + initialized)"

# ===========================================================================
echo
echo "== gRPC grpc.health.v1.Health/Check -> SERVING =="
grpc_check() {  # grpc_check <host:port> -> prints status name, exit 0 iff SERVING
  case "$HEALTH_CLIENT" in
    grpcurl) grpcurl -plaintext -d '{}' "$1" grpc.health.v1.Health/Check 2>&1 | grep -oE 'SERVING|NOT_SERVING|UNKNOWN|SERVICE_UNKNOWN' | head -1 ;;
    goprobe) "$PROBE_BIN" "$1" ;;
    *) return 2 ;;
  esac
}
if [ -z "$HEALTH_CLIENT" ]; then
  note "NO gRPC client available (grpcurl absent + grpc-go probe unavailable) — SKIPPING the gRPC Health/Check assertion (the HTTP + etcdctl proofs still run). etcdctl does NOT call the Health service."
else
  echo "  using gRPC client: $HEALTH_CLIENT"
  # Retry per endpoint until SERVING: readiness = has-leader + caught-up, which dips for a node
  # only during the (rare, brief) re-election window — a healthy cluster settles back to SERVING.
  for ep in "$EP_A" "$EP_B" "$EP_C"; do
    st=""
    for _ in $(seq 1 30); do st="$(grpc_check "$ep")"; [ "$st" = "SERVING" ] && break; sleep 0.3; done
    echo "  Health/Check $ep -> ${st:-<none>}"
    assert_eq "SERVING" "$st" "gRPC Health/Check on $ep reports SERVING"
  done
fi

# ===========================================================================
echo
echo "== HTTP /health (etcd --listen-metrics-urls faithful) =="
HB=""
for _ in $(seq 1 30); do HB="$(mcurl "$MP_A" /health)"; echo "$HB" | grep -q '"health":"true"' && break; sleep 0.3; done
echo "  GET :$MP_A/health -> $HB"
assert_has '"health":"true"' "$HB" "/health on a healthy node contains \"health\":\"true\""

# ===========================================================================
echo
echo "== HTTP /version =="
VB="$(mcurl "$MP_A" /version)"
echo "  GET :$MP_A/version -> $VB"
assert_has 'etcdserver'  "$VB" "/version contains etcdserver"
assert_has 'etcdcluster' "$VB" "/version contains etcdcluster"
assert_has '3.6.0'       "$VB" "/version reports 3.6.0"

# ===========================================================================
echo
echo "== HTTP /metrics (Prometheus exposition; REAL per-node gauges) =="
# fetch until has_leader==1 (dips only during a re-election window) so the value assertions
# below see the steady state.
MET=""
for _ in $(seq 1 30); do MET="$(mcurl "$MP_A" /metrics)"; [ "$(metric_val "$MET" etcd_server_has_leader)" = "1" ] && break; sleep 0.3; done
echo "  --- :$MP_A/metrics (gauge lines) ---"
echo "$MET" | grep -E '^etcd_|^up ' | sed 's/^/    /'
assert_has 'etcd_server_has_leader'                "$MET" "/metrics has etcd_server_has_leader"
assert_has 'etcd_server_raft_term'                 "$MET" "/metrics has etcd_server_raft_term (raft term line)"
assert_has 'etcd_server_raft_index'                "$MET" "/metrics has etcd_server_raft_index (raft index line)"
assert_has 'etcd_debugging_mvcc_current_revision'  "$MET" "/metrics has etcd_debugging_mvcc_current_revision (mvcc revision line)"
assert_has 'etcd_debugging_mvcc_keys_total'        "$MET" "/metrics has etcd_debugging_mvcc_keys_total (mvcc keys line)"
assert_has '# TYPE etcd_server_has_leader gauge'   "$MET" "/metrics emits # HELP/# TYPE metadata"
# values reflect the writes:
HL="$(metric_val "$MET" etcd_server_has_leader)"
RT="$(metric_val "$MET" etcd_server_raft_term)"
RI="$(metric_val "$MET" etcd_server_raft_index)"
KT="$(metric_val "$MET" etcd_debugging_mvcc_keys_total)"
REV="$(metric_val "$MET" etcd_debugging_mvcc_current_revision)"
echo "  values: has_leader=$HL raft_term=$RT raft_index=$RI keys_total=$KT revision=$REV"
assert_eq  "1"        "$HL" "etcd_server_has_leader == 1 (a leader exists)"
assert_gt0 "$RT"      "etcd_server_raft_term > 0"
assert_gt0 "$RI"      "etcd_server_raft_index > 0"
assert_ge  "$KT" "$NWROTE" "etcd_debugging_mvcc_keys_total >= $NWROTE keys written (got $KT)"
assert_gt0 "$REV"     "etcd_debugging_mvcc_current_revision > 0"

# ===========================================================================
echo
echo "== exactly ONE node reports etcd_server_is_leader 1 (single-leader sanity) =="
nlead=0
for _ in $(seq 1 20); do
  c=0
  for mp in "$MP_A" "$MP_B" "$MP_C"; do
    v="$(metric_val "$(mcurl "$mp" /metrics)" etcd_server_is_leader)"
    [ "$v" = "1" ] && c=$((c+1))
  done
  nlead="$c"
  [ "$c" = "1" ] && break
  sleep 0.3
done
echo "  nodes reporting etcd_server_is_leader 1 = $nlead"
assert_eq "1" "$nlead" "exactly one node reports etcd_server_is_leader 1"

# ===========================================================================
echo
echo "== HTTP 404 for an unknown path =="
H404="$(curl -s -o /dev/null -w '%{http_code}' --max-time 5 "http://127.0.0.1:${MP_A}/nope")"
echo "  GET :$MP_A/nope -> HTTP $H404"
assert_eq "404" "$H404" "unknown path returns 404"

# ===========================================================================
echo
echo "== etcdctl endpoint health (Range-probe) + endpoint status (.32) still work =="
# endpoint health does a leader-gated linearizable Range, so target the CURRENT leader.  Retry
# (re-discovering the leader each iteration) to absorb a leadership shift between find_leader and
# the probe.  Assert the literal "is healthy" marker — NOT a bare "healthy", which substring-
# matches the "is unhealthy" FAILURE line.
EH=""; ehealthy=0
for _ in $(seq 1 30); do
  find_leader || { sleep 0.3; continue; }
  EH="$(ectl1 "$LEADER_EP" endpoint health)"
  if echo "$EH" | grep -qiF 'is healthy'; then ehealthy=1; break; fi
  sleep 0.3
done
echo "  endpoint health ($LEADER_EP) -> $EH"
assert_eq "1" "$ehealthy" "etcdctl endpoint health reports healthy (on the leader)"
ES="$(ectl1 "$LEADER_EP" endpoint status -w json)"
ESI="$(json_num "$ES" raftIndex)"
echo "  endpoint status raftIndex = $ESI"
assert_gt0 "$ESI" "etcdctl endpoint status still works (raftIndex > 0)"

# ---------------------------------------------------------------------------
echo
echo "================================================================"
echo "$PASS passed, $FAIL failed"
if [ "$FAIL" -gt 0 ]; then
  echo "HEALTH/METRICS PROOF FAILED"
  echo "--- node log tails ---"; tail -n 20 "$LOG_A" "$LOG_B" "$LOG_C"
  exit 1
fi
echo "HEALTH/METRICS PROOF: ALL PASS — crab-watchstore serves gRPC Health + /health + /version + /metrics."
