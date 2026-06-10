#!/usr/bin/env bash
# test/etcd-cluster-grpc.sh — prove crab-watchstore's etcd Cluster gRPC service
# (/etcdserverpb.Cluster/*) with REAL etcdctl (3.6.x) over a 3-NODE cluster (cw-u4a.30).
#
# Brings up a STATIC 3-voter cluster (a,b,c) over the live cs-net transport + h2c gRPC
# client ports, discovers the leader, and drives the real `etcdctl` member subcommands:
#   member list                         -> the 3 REAL replicated members (distinct IDs,
#                                          names a/b/c, isLearner=false)  [real MemberList]
#   member add d  (voter)               -> 4-voter config commits, d appears   [MemberAdd]
#   member add e --learner              -> e appears with isLearner=true        [learner add]
#   member promote <e-ID>               -> e flips to isLearner=false           [MemberPromote]
#   member remove <d-ID>                -> list shrinks, d gone                 [MemberRemove]
#   member add to a FOLLOWER endpoint   -> leader-redirect error, no mutation   [redirect path]
#
# QUORUM REASONING (why this works without live d/e):
#   d and e are DEAD placeholders (no node process) — a real live join+catch-up under load
#   is cw-u4a.31.  Each conf change commits on the LIVE majority {a,b,c}:
#     add d:      voters {a,b,c}->{a,b,c,d}     quorum-of-4 = 3 = {a,b,c}            (commits)
#     add e:      learner (non-voting)          committed on the voter majority      (commits)
#     promote e:  voters {a,b,c,d}->{a,b,c,d,e} quorum-of-5 = 3 = {a,b,c}            (commits)
#     remove d:   voters {a,b,c,d,e}->{a,b,c,e} quorum-of-4 = 3 = {a,b,c}            (commits)
#   So ALL THREE of {a,b,c} must stay live for the whole run (we never kill them); after the
#   promote the cluster runs at EXACTLY quorum (3 of 5), held by the 3 live original voters.
#
# Usage:  bash test/etcd-cluster-grpc.sh        (run in BACKGROUND — foreground exits 144)
# Env:    CRABSCHEME = path to the crabscheme binary (default below).
#         ETCDCTL    = path to etcdctl (default: etcdctl on PATH, else homebrew Cellar).

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"
cd "$REPO_ROOT" || { echo "FATAL: cannot cd to repo root"; exit 1; }

BIN="${CRABSCHEME:-/Users/ztaylor/repos/workspaces/crabscheme/target/release/crabscheme}"
# etcdctl discovery: PATH first, then the homebrew Cellar location (.21/.26 path).
ETCDCTL="${ETCDCTL:-}"
if [ -z "$ETCDCTL" ]; then
  if command -v etcdctl >/dev/null 2>&1; then ETCDCTL="$(command -v etcdctl)";
  elif [ -x /opt/homebrew/bin/etcdctl ]; then ETCDCTL="/opt/homebrew/bin/etcdctl";
  else echo "FATAL: etcdctl not found (set ETCDCTL=...)"; exit 1; fi
fi

# unique tag + ports + db dirs per run so back-to-back spaced runs are clean.
TAG="$(date +%s%N)"
BC="$(( 26200 + (TAG % 1500) ))"          # client-port base (a=BC, b=BC+1, c=BC+2)
BR="$(( 20200 + (TAG % 1500) ))"          # raft-port   base
CP_A="$BC";        CP_B="$((BC+1))";  CP_C="$((BC+2))"
RP_A="$BR";        RP_B="$((BR+1))";  RP_C="$((BR+2))"
EP_A="127.0.0.1:${CP_A}"; EP_B="127.0.0.1:${CP_B}"; EP_C="127.0.0.1:${CP_C}"
CLUSTER="a:127.0.0.1:${RP_A}:${CP_A},b:127.0.0.1:${RP_B}:${CP_B},c:127.0.0.1:${RP_C}:${CP_C}"
DB="/tmp/cws-etcd-cluster-${TAG}"
LOG_A="/tmp/cws-cluster-a-${TAG}.log"; LOG_B="/tmp/cws-cluster-b-${TAG}.log"; LOG_C="/tmp/cws-cluster-c-${TAG}.log"

PASS=0; FAIL=0
PID_A=""; PID_B=""; PID_C=""

cleanup() {
  for p in "$PID_A" "$PID_B" "$PID_C"; do [ -n "$p" ] && kill "$p" 2>/dev/null; done
  for p in "$PID_A" "$PID_B" "$PID_C"; do [ -n "$p" ] && wait "$p" 2>/dev/null; done
  rm -rf "$DB"* 2>/dev/null
}
trap cleanup EXIT

ok()  { PASS=$((PASS+1)); echo "  ok   $1"; }
bad() { FAIL=$((FAIL+1)); echo "  FAIL $1"; echo "         $2"; }
assert_has()    { if echo "$2" | grep -qiF -- "$1"; then ok "$3"; else bad "$3" "expected [$1], got: $(echo "$2" | tr '\n' '|')"; fi; }
assert_hasnot() { if echo "$2" | grep -qiF -- "$1"; then bad "$3" "did NOT expect [$1], got: $(echo "$2" | tr '\n' '|')"; else ok "$3"; fi; }
assert_eq()     { if [ "$1" = "$2" ]; then ok "$3"; else bad "$3" "expected [$1], got [$2]"; fi; }

# etcdctl against a SINGLE endpoint, warnings/unrecognized stripped.
ectl1() { local ep="$1"; shift; "$ETCDCTL" --endpoints="$ep" --insecure-transport=true \
            --dial-timeout=5s --command-timeout=5s "$@" 2>&1 | grep -v 'level":"warn' | grep -v unrecognized; }

# member list (default one-line-per-member CSV) against an endpoint.
mlist()      { ectl1 "$1" member list; }
mcount()     { mlist "$1" | grep -c .; }                                  # members = lines
# hex member ID for a member NAME (field 3 of the CSV is the name, field 1 the hex ID).
id_of()      { mlist "$1" | awk -F', ' -v n="$2" '$3==n {print $1}'; }
# isLearner ("true"/"false") for a member NAME (last CSV field).
learner_of() { mlist "$1" | awk -F', ' -v n="$2" '$3==n {print $NF}'; }

echo "== bring up 3-node crab-watchstore cluster (a/b/c) — etcd Cluster gRPC =="
echo "   cluster: $CLUSTER"
"$BIN" run src/node-cluster.scm -- --node a --db "${DB}-a" --cluster "$CLUSTER" > "$LOG_A" 2>&1 & PID_A=$!
"$BIN" run src/node-cluster.scm -- --node b --db "${DB}-b" --cluster "$CLUSTER" > "$LOG_B" 2>&1 & PID_B=$!
"$BIN" run src/node-cluster.scm -- --node c --db "${DB}-c" --cluster "$CLUSTER" > "$LOG_C" 2>&1 & PID_C=$!

# wait for all 3 "serving on" banners (each prints only AFTER its node learns a leader).
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

# ---- leader discovery: a write commits ONLY on the leader (followers redirect). ----
# Returns the leader's endpoint; retried because a freshly-elected leader needs to commit
# its term's no-op barrier before it serves writes.
LEADER_EP=""
find_leader() {
  LEADER_EP=""
  for _ in $(seq 1 30); do
    for ep in "$EP_A" "$EP_B" "$EP_C"; do
      # short timeout: a follower put returns Unavailable ("not leader") and clientv3
      # retries until the command timeout, so keep the probe budget small.
      if "$ETCDCTL" --endpoints="$ep" --insecure-transport=true --dial-timeout=2s \
           --command-timeout=2s put "__leader_probe_${RANDOM}__" 1 2>/dev/null | grep -q "OK"; then
        LEADER_EP="$ep"; return 0
      fi
    done
    sleep 0.3
  done
  return 1
}
follower_ep() { for ep in "$EP_A" "$EP_B" "$EP_C"; do [ "$ep" != "$LEADER_EP" ] && { echo "$ep"; return; }; done; }

echo
echo "== discover the leader endpoint =="
if ! find_leader; then echo "FATAL: no leader serving writes"; for L in "$LOG_A" "$LOG_B" "$LOG_C"; do echo "--- $L ---"; tail -15 "$L"; done; exit 1; fi
echo "  leader endpoint = $LEADER_EP"

# ---------------------------------------------------------------------------
echo
echo "== member list: the 3 REAL replicated members (a,b,c) =="
out="$(mlist "$LEADER_EP")"; echo "$out" | sed 's/^/  $ etcdctl member list -> /'
assert_eq "3" "$(mcount "$LEADER_EP")" "member list shows exactly 3 members"
assert_has ", a," "$out" "member list contains node a"
assert_has ", b," "$out" "member list contains node b"
assert_has ", c," "$out" "member list contains node c"
# IDs distinct (no name->ID hash collision across a/b/c).
nids="$(mlist "$LEADER_EP" | awk -F', ' '{print $1}' | sort -u | grep -c .)"
assert_eq "3" "$nids" "the 3 member IDs are distinct (no hash collision)"
# all three are voters (isLearner=false).
assert_eq "false" "$(learner_of "$LEADER_EP" a)" "a isLearner=false"
assert_eq "false" "$(learner_of "$LEADER_EP" b)" "b isLearner=false"
assert_eq "false" "$(learner_of "$LEADER_EP" c)" "c isLearner=false"

# ---------------------------------------------------------------------------
echo
echo "== member add d (VOTER) -> 4-voter config commits =="
find_leader
add_d="$(ectl1 "$LEADER_EP" member add d --peer-urls=http://d:2380)"
echo "$add_d" | sed 's/^/  $ etcdctl member add d -> /'
assert_has "added" "$add_d" "member add d -> acknowledged (added)"
assert_eq "4" "$(mcount "$LEADER_EP")" "member list now shows 4 members"
assert_has ", d," "$(mlist "$LEADER_EP")" "member list contains the new node d"
assert_eq "false" "$(learner_of "$LEADER_EP" d)" "d isLearner=false (added as a voter)"

# ---------------------------------------------------------------------------
echo
echo "== member add e --learner -> e is a non-voting learner =="
find_leader
add_e="$(ectl1 "$LEADER_EP" member add e --learner --peer-urls=http://e:2380)"
echo "$add_e" | sed 's/^/  $ etcdctl member add e --learner -> /'
assert_has "added" "$add_e" "member add e --learner -> acknowledged"
assert_eq "5" "$(mcount "$LEADER_EP")" "member list now shows 5 members"
assert_eq "true" "$(learner_of "$LEADER_EP" e)" "e isLearner=true"

# ---------------------------------------------------------------------------
echo
echo "== member promote <e-ID> -> learner flips to voter =="
find_leader
E_ID="$(id_of "$LEADER_EP" e)"
echo "  e member ID (hex) = $E_ID"
prom="$(ectl1 "$LEADER_EP" member promote "$E_ID")"
echo "$prom" | sed 's/^/  $ etcdctl member promote e -> /'
# promote is a two-phase voter-set change; give the post-commit config a beat to surface.
for _ in $(seq 1 10); do [ "$(learner_of "$LEADER_EP" e)" = "false" ] && break; sleep 0.5; find_leader; done
assert_eq "false" "$(learner_of "$LEADER_EP" e)" "e isLearner=false after promote (now a voter)"
assert_eq "5" "$(mcount "$LEADER_EP")" "still 5 members after promote"

# ---------------------------------------------------------------------------
echo
echo "== member remove <d-ID> -> config shrinks, d gone =="
find_leader
D_ID="$(id_of "$LEADER_EP" d)"
echo "  d member ID (hex) = $D_ID"
rem="$(ectl1 "$LEADER_EP" member remove "$D_ID")"
echo "$rem" | sed 's/^/  $ etcdctl member remove d -> /'
assert_has "removed" "$rem" "member remove d -> acknowledged (removed)"
for _ in $(seq 1 10); do [ "$(mcount "$LEADER_EP")" = "4" ] && break; sleep 0.5; find_leader; done
assert_eq "4" "$(mcount "$LEADER_EP")" "member list shrinks to 4 members"
assert_hasnot ", d," "$(mlist "$LEADER_EP")" "node d absent after remove"

# ---------------------------------------------------------------------------
echo
echo "== leader-redirect: a member op on a FOLLOWER endpoint errors (no mutation) =="
find_leader
FOLLOWER_EP="$(follower_ep)"
echo "  leader=$LEADER_EP  follower=$FOLLOWER_EP"
before="$(mcount "$LEADER_EP")"
redir="$(ectl1 "$FOLLOWER_EP" member add gate-x --peer-urls=http://gate-x:2380)"
echo "$redir" | sed 's/^/  $ etcdctl (follower) member add gate-x -> /'
# the follower must NOT serve the change: either a "not leader"/Unavailable error, or at
# minimum no "added" success; and the config is unchanged.
if echo "$redir" | grep -qiE "not leader|unavailable|leader changed|error|rpc error"; then
  ok "member op on a follower returns a leader-redirect / error"
else
  assert_hasnot "added" "$redir" "member op on a follower did not succeed"
fi
assert_eq "$before" "$(mcount "$LEADER_EP")" "follower redirect did NOT mutate the config (gate-x not added)"
assert_hasnot ", gate-x," "$(mlist "$LEADER_EP")" "gate-x absent (redirect prevented the add)"

# ---------------------------------------------------------------------------
echo
echo "================================================================"
echo "$PASS passed, $FAIL failed"
if [ "$FAIL" -gt 0 ]; then
  echo "ETCDCTL CLUSTER PROOF FAILED"
  echo "--- leader log tail ---"; tail -25 "$LOG_A" "$LOG_B" "$LOG_C"
  exit 1
fi
echo "ETCDCTL CLUSTER PROOF: ALL PASS — crab-watchstore speaks the etcd Cluster service."
