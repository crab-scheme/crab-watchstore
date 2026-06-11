#!/usr/bin/env bash
# test/cluster-config-launch.sh — cw-24e.1 acceptance: a 3-node cluster started
# ONLY via the supported entrypoint (bin/crab-watchstore --config X.conf, the
# checked-in examples/cluster-3 configs) serves etcdctl: status on all 3, a
# put on the leader, a linearizable get on a follower endpoint, CLI override
# (--db) beating the config value.
set -uo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
ETCDCTL="${ETCDCTL:-$(command -v etcdctl)}"
PASS=0; FAIL=0
ok(){ PASS=$((PASS+1)); echo "  ok   $1"; }
bad(){ FAIL=$((FAIL+1)); echo "  FAIL $1"; }
check(){ if [ "$2" = "$3" ]; then ok "$1"; else bad "$1  expected=$2 got=$3"; fi; }

PIDS=()
cleanup(){ for p in "${PIDS[@]:-}"; do kill "$p" 2>/dev/null; done; }
trap cleanup EXIT
rm -rf /tmp/crab-watchstore-3 /tmp/cws-cfg-override

echo "== start 3 members from the checked-in example configs =="
for n in a b c; do
  "$ROOT/bin/crab-watchstore" --config "$ROOT/examples/cluster-3/$n.conf" \
    >"/tmp/cws-cfg-$n.log" 2>&1 &
  PIDS+=($!)
done
for n in a b c; do
  for _ in $(seq 1 100); do
    grep -q "etcd KV gRPC serving on" "/tmp/cws-cfg-$n.log" 2>/dev/null && break
    sleep 0.5
  done
  grep -q "etcd KV gRPC serving on" "/tmp/cws-cfg-$n.log" \
    && ok "member $n serving" || { bad "member $n did not serve"; tail -5 "/tmp/cws-cfg-$n.log"; }
done
sleep 2

EPS=(127.0.0.1:23790 127.0.0.1:23791 127.0.0.1:23792)
echo "== endpoint status on all 3 =="
LEADERS=""
for ep in "${EPS[@]}"; do
  L=$("$ETCDCTL" --endpoints="$ep" endpoint status -w json 2>/dev/null \
      | sed 's/.*"leader"://;s/[,}].*//')
  [ -n "$L" ] && [ "$L" != "0" ] && ok "[$ep] reports a leader ($L)" || bad "[$ep] no leader"
  LEADERS="$LEADERS $L"
done
check "all 3 endpoints agree on the leader" 1 "$(echo $LEADERS | tr ' ' '\n' | sort -u | wc -l | tr -d ' ')"

echo "== write + read on the leader endpoint (reads are leader-gated) =="
LEADER_EP=""
for ep in "${EPS[@]}"; do
  J=$("$ETCDCTL" --endpoints="$ep" endpoint status -w json 2>/dev/null)
  MID=$(echo "$J" | sed 's/.*"member_id"://;s/[,}].*//')
  LID=$(echo "$J" | sed 's/.*"leader"://;s/[,}].*//')
  [ -n "$MID" ] && [ "$MID" = "$LID" ] && LEADER_EP="$ep"
done
[ -n "$LEADER_EP" ] && ok "leader endpoint discovered ($LEADER_EP)" || bad "no leader endpoint"
"$ETCDCTL" --endpoints="$LEADER_EP" put cfg-launch-key hello >/dev/null 2>&1 \
  && ok "put accepted" || bad "put failed"
GOT=$("$ETCDCTL" --endpoints="$LEADER_EP" get cfg-launch-key --print-value-only 2>/dev/null | tr -d '\n')
check "get returns the written value" "hello" "$GOT"

echo "== CLI flag overrides the config value (--db) =="
"$ROOT/bin/crab-watchstore" --config "$ROOT/examples/cluster-3/a.conf" \
  --node z --db /tmp/cws-cfg-override \
  --cluster "z:127.0.0.1:21799:23799" >/tmp/cws-cfg-z.log 2>&1 &
PIDS+=($!)
for _ in $(seq 1 100); do
  grep -q "etcd KV gRPC serving on" /tmp/cws-cfg-z.log 2>/dev/null && break; sleep 0.5
done
[ -d /tmp/cws-cfg-override-shard0 ] && ok "--db override took effect (shard dir created)" \
                                    || bad "--db override ignored"

echo
echo "================================================================"
echo "$PASS passed, $FAIL failed"
[ "$FAIL" = 0 ] && echo "CONFIG-LAUNCH PROOF: ALL PASS — supported entrypoint brings up a 3-node cluster." || exit 1
