#!/usr/bin/env bash
# test/docker-cluster.sh — cw-24e.2 acceptance: the compose 3-node cluster
# (deploy/docker, durable=yes, Linux fsync) comes up healthy and serves real
# etcdctl from the HOST: compose healthchecks green, endpoint status agrees on
# one leader, put/get/watch on the leader, hashkv identical across members.
set -uo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
ETCDCTL="${ETCDCTL:-$(command -v etcdctl)}"
COMPOSE="docker compose -f $ROOT/deploy/docker/docker-compose.yml"
PASS=0; FAIL=0
ok(){ PASS=$((PASS+1)); echo "  ok   $1"; }
bad(){ FAIL=$((FAIL+1)); echo "  FAIL $1"; }
check(){ if [ "$2" = "$3" ]; then ok "$1"; else bad "$1  expected=$2 got=$3"; fi; }
EPS=(127.0.0.1:24791 127.0.0.1:24792 127.0.0.1:24793)  # 247xx: clear of the local example-config ports (237xx)

echo "== compose up --build (3 members, durable) =="
$COMPOSE up -d --build >/tmp/cws-compose-up.log 2>&1 || { bad "compose up"; tail -10 /tmp/cws-compose-up.log; exit 1; }
ok "compose up"

echo "== healthchecks reach healthy =="
for _ in $(seq 1 40); do
  H=$($COMPOSE ps --format '{{.Name}} {{.Health}}' 2>/dev/null | grep -c healthy)
  [ "$H" = 3 ] && break; sleep 3
done
check "all 3 containers healthy" 3 "$($COMPOSE ps --format '{{.Name}} {{.Health}}' | grep -c healthy)"

echo "== host etcdctl: endpoint status =="
LEADER_EP=""; LEADERS=""
for ep in "${EPS[@]}"; do
  J=$("$ETCDCTL" --endpoints="$ep" endpoint status -w json 2>/dev/null)
  MID=$(echo "$J" | sed 's/.*"member_id"://;s/[,}].*//')
  LID=$(echo "$J" | sed 's/.*"leader"://;s/[,}].*//')
  [ -n "$LID" ] && [ "$LID" != "0" ] && ok "[$ep] reports a leader" || bad "[$ep] no leader"
  [ -n "$MID" ] && [ "$MID" = "$LID" ] && LEADER_EP="$ep"
  LEADERS="$LEADERS $LID"
done
check "one leader across members" 1 "$(echo $LEADERS | tr ' ' '\n' | sort -u | wc -l | tr -d ' ')"
[ -n "$LEADER_EP" ] && ok "leader endpoint = $LEADER_EP" || bad "leader endpoint not found"

echo "== put/get/watch on the leader =="
"$ETCDCTL" --endpoints="$LEADER_EP" put dkr-k v1 >/dev/null 2>&1 && ok "put" || bad "put"
check "get" "v1" "$("$ETCDCTL" --endpoints="$LEADER_EP" get dkr-k --print-value-only 2>/dev/null | tr -d '\n')"
("$ETCDCTL" --endpoints="$LEADER_EP" watch dkr-k >/tmp/cws-dkr-watch.out 2>&1 &
 WPID=$!; sleep 2
 "$ETCDCTL" --endpoints="$LEADER_EP" put dkr-k v2 >/dev/null 2>&1
 sleep 2; kill $WPID 2>/dev/null)
grep -q "v2" /tmp/cws-dkr-watch.out && ok "watch delivered the event" || bad "watch event missing"

echo "== hashkv identical across all 3 members =="
# A follower applies one AE round behind the commit quorum — retry until all
# three hashes converge (bounded), then assert equality.
for _ in $(seq 1 10); do
  H1=$("$ETCDCTL" --endpoints="${EPS[0]}" endpoint hashkv -w json 2>/dev/null | sed 's/.*"hash"://;s/[,}].*//')
  H2=$("$ETCDCTL" --endpoints="${EPS[1]}" endpoint hashkv -w json 2>/dev/null | sed 's/.*"hash"://;s/[,}].*//')
  H3=$("$ETCDCTL" --endpoints="${EPS[2]}" endpoint hashkv -w json 2>/dev/null | sed 's/.*"hash"://;s/[,}].*//')
  [ -n "$H1" ] && [ "$H1" = "$H2" ] && [ "$H2" = "$H3" ] && break
  sleep 1
done
check "hashkv a==b" "$H1" "$H2"
check "hashkv b==c" "$H2" "$H3"
[ -n "$H1" ] && ok "hashkv non-empty ($H1)" || bad "hashkv empty"

echo
echo "================================================================"
echo "$PASS passed, $FAIL failed"
if [ "${KEEP:-no}" != "yes" ]; then $COMPOSE down -v >/dev/null 2>&1; fi
[ "$FAIL" = 0 ] && echo "DOCKER CLUSTER PROOF: ALL PASS — compose 3-node cluster serves etcdctl from the host." || exit 1
