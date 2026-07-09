#!/usr/bin/env bash
# ab-engine.sh <raft|quepaxa> — one leg of the real-AWS 2-region WAN A/B (cw-7e5).
# Run from deploy/aws-apiserver-smoke/. Assumes instances up, src/ already pushed,
# etcdctl installed on the apiserver node. 5 store nodes, single shard group.
set -euo pipefail
ENGINE="${1:?usage: ab-engine.sh raft|quepaxa}"
KEY=terraform/cws-smoke.pem
SSH() { ssh -i "$KEY" -o StrictHostKeyChecking=no -o ConnectTimeout=10 ubuntu@"$1" "${@:2}"; }
declare -A IP=( [n1]=18.219.175.139 [n2]=3.146.108.47 [n3]=18.190.228.14 [n4]=34.213.13.28 [n5]=16.144.176.102 )
API=18.225.114.210
SPEC="n1:n1:7000:2379,n2:n2:7000:2379,n3:n3:7000:2379,n4:n4:7000:2379,n5:n5:7000:2379"
EPS="${IP[n1]}:2379,${IP[n2]}:2379,${IP[n3]}:2379,${IP[n4]}:2379,${IP[n5]}:2379"

echo "== [$ENGINE] reconfigure + fresh start on 5 nodes =="
for n in n1 n2 n3 n4 n5; do
  SSH "${IP[$n]}" "sudo systemctl stop crabwatchstore; sudo rm -rf /opt/crabwatchstore/data; sudo install -d -o ubuntu /opt/crabwatchstore/data; sudo mkdir -p /etc/systemd/system/crabwatchstore.service.d && printf '[Service]\nExecStart=\nExecStart=/opt/crabwatchstore/crabscheme run src/node-cluster.scm -- --node $n --durable yes --db /opt/crabwatchstore/data/cw --cluster $SPEC --tick-ms 250 --election-ticks 8 --shard-groups 1 --engine $ENGINE\nRestart=always\nRestartSec=10\n' | sudo tee /etc/systemd/system/crabwatchstore.service.d/ab.conf >/dev/null; sudo truncate -s0 /opt/crabwatchstore/node.log; sudo systemctl daemon-reload; sudo systemctl start crabwatchstore" &
done
wait

echo "== [$ENGINE] wait for cluster health =="
for i in $(seq 1 60); do
  H=$(SSH "$API" "etcdctl --endpoints=$EPS endpoint health 2>/dev/null | grep -c 'is healthy'" || true)
  [ "${H:-0}" = 5 ] && { echo "  ok all 5 healthy"; break; }
  [ "$i" = 60 ] && { echo "FATAL: cluster never healthy"; SSH "${IP[n1]}" "tail -30 /opt/crabwatchstore/node.log"; exit 1; }
  sleep 3
done
START_TERM=$(SSH "$API" "etcdctl --endpoints=${IP[n1]}:2379 endpoint status -w json | jq '.[0].Status.raftTerm'")
echo "  start term: $START_TERM"

echo "== [$ENGINE] 60s write load (kill leader at t=20s, dead 10s) =="
SSH "$API" "CWS_EPS=$EPS bash -s" >"/tmp/ab-$ENGINE-load.log" <<'LOAD' &
end=$(( $(date +%s) + 60 )); i=0
while [ "$(date +%s)" -lt "$end" ]; do
  t0=$(date +%s%3N)
  if etcdctl --endpoints="$CWS_EPS" --command-timeout=5s put "ab-k-$i" "v-$i" >/dev/null 2>&1; then
    echo "ok $i $(( $(date +%s%3N) - t0 ))"
  else
    echo "fail $i $(( $(date +%s%3N) - t0 ))"
  fi
  i=$((i+1))
done
LOAD
LOAD_PID=$!
sleep 20
if [ "$ENGINE" = raft ]; then
  LEADER_EP=$(SSH "$API" "etcdctl --endpoints=$EPS endpoint status -w json | jq -r '.[] | select(.Status.leader==.Status.header.member_id) | .Endpoint'" | head -1)
  VICTIM=n1; for n in n1 n2 n3 n4 n5; do [ "${IP[$n]}:2379" = "$LEADER_EP" ] && VICTIM=$n; done
else
  VICTIM=n1  # quepaxa coordinator
fi
echo "  killing $VICTIM (${IP[$VICTIM]}) — SIGKILL, systemd restarts in 10s"
SSH "${IP[$VICTIM]}" "sudo systemctl kill -s SIGKILL crabwatchstore"
wait "$LOAD_PID" || true

ACKED=$(grep -c '^ok' "/tmp/ab-$ENGINE-load.log" || true)
echo "  acked writes: $ACKED"
echo "== [$ENGINE] verify: lost acked writes =="
awk '/^ok/{print $2}' "/tmp/ab-$ENGINE-load.log" | SSH "$API" "CWS_EPS=$EPS bash -c 'LOST=0
while read -r k; do
  V=\$(etcdctl --endpoints=\$CWS_EPS --command-timeout=5s get ab-k-\$k --print-value-only 2>/dev/null)
  [ \"\$V\" = \"v-\$k\" ] || { LOST=\$((LOST+1)); echo \"  LOST ab-k-\$k\"; }
done
echo \"  lost acked writes: \$LOST\"'"
END_TERM=$(SSH "$API" "etcdctl --endpoints=${IP[n2]}:2379 endpoint status -w json | jq '.[0].Status.raftTerm'")
echo "  terms: $START_TERM -> $END_TERM"
echo "== [$ENGINE] hashkv convergence =="
for n in n1 n2 n3 n4 n5; do
  SSH "$API" "etcdctl --endpoints=${IP[$n]}:2379 endpoint hashkv -w json | jq -c '{n:\"$n\",hash:.[0].HashKV.hash}'" || echo "  $n hashkv FAILED"
done
echo "== [$ENGINE] latency (steady-state = pre-kill ops) =="
awk '/^ok/ && $2<'"$(awk '/^ok/{c++} END{print int(c/3)}' "/tmp/ab-$ENGINE-load.log")"' {print $3}' "/tmp/ab-$ENGINE-load.log" | sort -n >"/tmp/ab-$ENGINE-lat.txt"
N=$(wc -l <"/tmp/ab-$ENGINE-lat.txt")
if [ "$N" -gt 0 ]; then
  P99=$(( N*99/100 )); [ "$P99" -lt 1 ] && P99=1
  echo "  median $(sed -n "$(( (N+1)/2 ))p" "/tmp/ab-$ENGINE-lat.txt")ms  p99 $(sed -n "${P99}p" "/tmp/ab-$ENGINE-lat.txt")ms  over $N ops"
else
  echo "  NO steady-state ops recorded"
fi
echo "== [$ENGINE] leg done =="
