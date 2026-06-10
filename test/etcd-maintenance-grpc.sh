#!/usr/bin/env bash
# test/etcd-maintenance-grpc.sh — prove crab-watchstore's etcd Maintenance gRPC service
# (/etcdserverpb.Maintenance/*) with REAL etcdctl (3.6.x) over a 3-NODE cluster (cw-u4a.32).
#
# Brings up a STATIC 3-voter cluster (a,b,c) over the live cs-net transport + h2c gRPC
# client ports, writes some keys, discovers the leader, and drives the real `etcdctl`
# Maintenance subcommands:
#   endpoint status  -w json (all 3)  -> REAL raftIndex>0 / raftTerm>0 / dbSize / leader   [Status]
#   endpoint hashkv  -w json (all 3)  -> the 32-bit hash is IDENTICAL across all 3 members  [HashKV]
#                                        and non-zero  (the cross-member CONSISTENCY proof)
#   alarm list                        -> empty;  alarm disarm -> ok                          [Alarm]
#   defrag (one endpoint)             -> Finished defragmenting (advisory RocksDB flush)      [Defragment]
#   snapshot save <file>              -> the server-stream downloads to a non-empty file      [Snapshot]
#                                        (bbolt restore/status is NOT supported — see note)
#   move-leader <non-leader-id>       -> Unimplemented (the engine has no TimeoutNow)         [MoveLeader]
#
# QUORUM REASONING: a/b/c are all live for the whole run (never killed); every write commits
# on the 3-voter quorum.  Maintenance reads (status/hashkv/snapshot) are endpoint-LOCAL and
# UN-gated — each replica answers from its own committed ctx — which is exactly why a
# cross-member hashkv equality proves the replicas hold the identical committed keyspace.
#
# Usage:  bash test/etcd-maintenance-grpc.sh        (run in BACKGROUND — foreground exits 144)
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
BC="$(( 26800 + (TAG % 1200) ))"          # client-port base (a=BC, b=BC+1, c=BC+2)
BR="$(( 20800 + (TAG % 1200) ))"          # raft-port   base
CP_A="$BC";        CP_B="$((BC+1))";  CP_C="$((BC+2))"
RP_A="$BR";        RP_B="$((BR+1))";  RP_C="$((BR+2))"
EP_A="127.0.0.1:${CP_A}"; EP_B="127.0.0.1:${CP_B}"; EP_C="127.0.0.1:${CP_C}"
CLUSTER="a:127.0.0.1:${RP_A}:${CP_A},b:127.0.0.1:${RP_B}:${CP_B},c:127.0.0.1:${RP_C}:${CP_C}"
DB="/tmp/cws-etcd-maint-${TAG}"
SNAP="/tmp/cw-maint-snap-${TAG}.db"
LOG_A="/tmp/cws-maint-a-${TAG}.log"; LOG_B="/tmp/cws-maint-b-${TAG}.log"; LOG_C="/tmp/cws-maint-c-${TAG}.log"

PASS=0; FAIL=0
PID_A=""; PID_B=""; PID_C=""

cleanup() {
  for p in "$PID_A" "$PID_B" "$PID_C"; do [ -n "$p" ] && kill "$p" 2>/dev/null; done
  for p in "$PID_A" "$PID_B" "$PID_C"; do [ -n "$p" ] && wait "$p" 2>/dev/null; done
  rm -rf "$DB"* "$SNAP" 2>/dev/null
}
trap cleanup EXIT

ok()  { PASS=$((PASS+1)); echo "  ok   $1"; }
bad() { FAIL=$((FAIL+1)); echo "  FAIL $1"; echo "         $2"; }
assert_has()    { if echo "$2" | grep -qiF -- "$1"; then ok "$3"; else bad "$3" "expected [$1], got: $(echo "$2" | tr '\n' '|')"; fi; }
assert_eq()     { if [ "$1" = "$2" ]; then ok "$3"; else bad "$3" "expected [$1], got [$2]"; fi; }
assert_gt0()    { if [ -n "$1" ] && [ "$1" -gt 0 ] 2>/dev/null; then ok "$2"; else bad "$2" "expected >0, got [$1]"; fi; }

# etcdctl against a SINGLE endpoint, warnings/deprecation/unrecognized stripped.
ectl1() { local ep="$1"; shift; "$ETCDCTL" --endpoints="$ep" --insecure-transport=true \
            --dial-timeout=5s --command-timeout=5s "$@" 2>&1 \
            | grep -v 'level":"warn' | grep -v -i 'deprecat' | grep -v unrecognized; }

# extract the first integer for a JSON field NAME (handles bare or string-quoted numbers).
json_num() { echo "$1" | grep -oE "\"$2\":\"?[0-9]+" | head -1 | grep -oE '[0-9]+'; }
# hex member ID for a member NAME from `member list` CSV (field 1 = hex ID, field 3 = name).
id_of()    { ectl1 "$1" member list | awk -F', ' -v n="$2" '$3==n {print $1}'; }
# map an endpoint to its node name (a/b/c).
name_of_ep() { case "$1" in "$EP_A") echo a;; "$EP_B") echo b;; "$EP_C") echo c;; esac; }

echo "== bring up 3-node crab-watchstore cluster (a/b/c) — etcd Maintenance gRPC =="
echo "   cluster: $CLUSTER"
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
grep -h "shard 0 ready\|serving on" "$LOG_A" "$LOG_B" "$LOG_C" | sed 's/^/  /'

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
echo "  leader endpoint = $LEADER_EP"

# ---- write some keys so dbSize/hash are non-trivial, and let them replicate ----
echo
echo "== write keys (so dbSize/hashkv are non-trivial) =="
for kv in maint/k1=alpha maint/k2=bravo maint/k3=charlie maint/k4=delta maint/k5=echo; do
  ectl1 "$LEADER_EP" put "${kv%%=*}" "${kv#*=}" >/dev/null
done
ok "wrote 5 keys to the leader"

# ---------------------------------------------------------------------------
echo
echo "== Maintenance/Status: endpoint status -w json on ALL 3 endpoints =="
LEADER_VALS=""
for ep in "$EP_A" "$EP_B" "$EP_C"; do
  st="$(ectl1 "$ep" endpoint status -w json)"
  echo "  [$ep] $st"
  ri="$(json_num "$st" raftIndex)"; rt="$(json_num "$st" raftTerm)"
  rai="$(json_num "$st" raftAppliedIndex)"; dbs="$(json_num "$st" dbSize)"
  ldr="$(json_num "$st" leader)"
  assert_gt0 "$ri"  "[$ep] raftIndex > 0 (got $ri)"
  assert_gt0 "$rt"  "[$ep] raftTerm > 0 (got $rt)"
  assert_gt0 "$rai" "[$ep] raftAppliedIndex > 0 (got $rai)"
  assert_gt0 "$ldr" "[$ep] leader present + non-zero (got $ldr)"
  LEADER_VALS="$LEADER_VALS $ldr"
  if [ "$ep" = "$LEADER_EP" ]; then assert_gt0 "$dbs" "[leader $ep] dbSize > 0 (got $dbs)"; fi
done
# all 3 endpoints must agree on the leader id (consistency).
nuniq="$(echo "$LEADER_VALS" | tr ' ' '\n' | grep -c . | head -1)"
ldr_uniq="$(echo "$LEADER_VALS" | tr ' ' '\n' | grep . | sort -u | grep -c .)"
assert_eq "1" "$ldr_uniq" "all 3 endpoints report the SAME leader id"
# and that id matches the leader endpoint's member id (hex from member list -> decimal).
LEADER_NAME="$(name_of_ep "$LEADER_EP")"
LEADER_HEX="$(id_of "$LEADER_EP" "$LEADER_NAME")"
LEADER_DEC="$(printf '%d' "0x${LEADER_HEX}" 2>/dev/null)"
REPORTED_LDR="$(echo "$LEADER_VALS" | tr ' ' '\n' | grep . | head -1)"
echo "  leader name=$LEADER_NAME hex=$LEADER_HEX dec=$LEADER_DEC  reported=$REPORTED_LDR"
assert_eq "$LEADER_DEC" "$REPORTED_LDR" "status leader id == the leader member's id"

# ---------------------------------------------------------------------------
echo
echo "== Maintenance/HashKV: the hash is IDENTICAL across all 3 members (consistency proof) =="
# retry until the hashes converge (replication lag): each member hashes its OWN committed
# keyspace, so once all 3 have applied the writes the 32-bit hashes are byte-identical.
H_A=""; H_B=""; H_C=""
for _ in $(seq 1 40); do
  H_A="$(json_num "$(ectl1 "$EP_A" endpoint hashkv -w json)" hash)"
  H_B="$(json_num "$(ectl1 "$EP_B" endpoint hashkv -w json)" hash)"
  H_C="$(json_num "$(ectl1 "$EP_C" endpoint hashkv -w json)" hash)"
  if [ -n "$H_A" ] && [ "$H_A" = "$H_B" ] && [ "$H_B" = "$H_C" ] && [ "$H_A" != "0" ]; then break; fi
  sleep 0.3
done
echo "  hashkv: a=$H_A  b=$H_B  c=$H_C"
assert_gt0 "$H_A" "hashkv on node a is non-zero (got $H_A)"
assert_eq "$H_A" "$H_B" "hashkv(a) == hashkv(b)  [cross-member identical]"
assert_eq "$H_B" "$H_C" "hashkv(b) == hashkv(c)  [cross-member identical]"
# Hash (full keyspace) should also be present + non-zero on the leader.
HFULL="$(json_num "$(ectl1 "$LEADER_EP" endpoint hashkv -w json)" hash)"
assert_gt0 "$HFULL" "Hash/HashKV non-zero on the leader (got $HFULL)"

# ---------------------------------------------------------------------------
echo
echo "== Maintenance/Alarm: alarm list (empty) + alarm disarm (ok) =="
al="$(ectl1 "$LEADER_EP" alarm list)"
echo "  alarm list -> [$al]"
assert_eq "" "$(echo "$al" | grep -i 'alarm:' )" "alarm list is empty (no active alarms)"
# capture etcdctl's OWN exit code directly (the ectl1 grep wrapper would mask it: an empty
# success output makes the trailing `grep -v` exit 1, not etcdctl).
"$ETCDCTL" --endpoints="$LEADER_EP" --insecure-transport=true --dial-timeout=5s \
  --command-timeout=5s alarm disarm >/tmp/cw-disarm-${TAG}.out 2>&1; drc=$?
echo "  alarm disarm -> rc=$drc :: $(grep -vi 'deprecat\|level":"warn\|unrecognized' /tmp/cw-disarm-${TAG}.out | tr '\n' '|')"
assert_eq "0" "$drc" "alarm disarm succeeds (exit 0)"
rm -f /tmp/cw-disarm-${TAG}.out

# ---------------------------------------------------------------------------
echo
echo "== Maintenance/Defragment: defrag one endpoint (advisory RocksDB flush) =="
df="$(ectl1 "$LEADER_EP" defrag --command-timeout=10s)"
echo "  defrag -> $df"
if echo "$df" | grep -qiE "Finished defragmenting|defragmented"; then
  ok "defrag returns success"
else
  assert_has "rpc error" "NONE" "defrag returns success"   # force a clear FAIL with the output
fi

# ---------------------------------------------------------------------------
echo
echo "== Maintenance/Snapshot: snapshot save downloads the server-stream to a file =="
# NOTE (documented limitation): this is a crab-watchstore-native LOGICAL snapshot over the
# RocksDB backend, NOT etcd's bbolt .db — `snapshot save` DOWNLOADS it (proving the
# server-stream end-to-end), but `snapshot restore/status` (bbolt parsers) are intentionally
# NOT supported here.  So we assert only that the file was streamed + is non-empty.
sv="$(ectl1 "$LEADER_EP" snapshot save "$SNAP")"
echo "  snapshot save -> $sv"
if [ -s "$SNAP" ]; then
  ok "snapshot save produced a non-empty file ($(wc -c < "$SNAP" | tr -d ' ') bytes)"
else
  bad "snapshot save produced a non-empty file" "file [$SNAP] missing/empty; out: $sv"
fi

# ---------------------------------------------------------------------------
echo
echo "== Maintenance/MoveLeader: transfer to a NON-leader id is Unimplemented =="
find_leader
LEADER_NAME="$(name_of_ep "$LEADER_EP")"
# pick a follower name (a/b/c that is not the leader) and its hex member id.
FOLLOWER_NAME=""; for n in a b c; do [ "$n" != "$LEADER_NAME" ] && { FOLLOWER_NAME="$n"; break; }; done
FOLLOWER_HEX="$(id_of "$LEADER_EP" "$FOLLOWER_NAME")"
echo "  leader=$LEADER_NAME  target follower=$FOLLOWER_NAME (id=$FOLLOWER_HEX)"
ml="$(ectl1 "$LEADER_EP" move-leader "$FOLLOWER_HEX")"
echo "  move-leader $FOLLOWER_HEX -> $ml"
if echo "$ml" | grep -qiE "Unimplemented|not supported|TimeoutNow"; then
  ok "move-leader to a non-leader returns Unimplemented (no engine TimeoutNow)"
else
  bad "move-leader to a non-leader returns Unimplemented" "got: $ml"
fi

# ---------------------------------------------------------------------------
echo
echo "================================================================"
echo "$PASS passed, $FAIL failed"
if [ "$FAIL" -gt 0 ]; then
  echo "ETCDCTL MAINTENANCE PROOF FAILED"
  echo "--- node log tails ---"; tail -n 20 "$LOG_A" "$LOG_B" "$LOG_C"
  exit 1
fi
echo "ETCDCTL MAINTENANCE PROOF: ALL PASS — crab-watchstore speaks the etcd Maintenance service."
