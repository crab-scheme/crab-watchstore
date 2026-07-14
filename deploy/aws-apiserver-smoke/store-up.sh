#!/usr/bin/env bash
# store-up.sh <raft|quepaxa> — (re)start the 5-node store fresh under the given
# engine, single shard group. Run from deploy/aws-apiserver-smoke/.
set -euo pipefail
ENGINE="${1:?usage: store-up.sh raft|quepaxa}"
KEY=terraform/cws-smoke.pem
SSH() { ssh -i "$KEY" -o StrictHostKeyChecking=no -o ConnectTimeout=10 ubuntu@"$1" "${@:2}"; }
declare -A IP=( [n1]=18.219.175.139 [n2]=3.146.108.47 [n3]=18.190.228.14 [n4]=34.213.13.28 [n5]=16.144.176.102 )
API=18.225.114.210
SPEC="n1:n1:7000:2379,n2:n2:7000:2379,n3:n3:7000:2379,n4:n4:7000:2379,n5:n5:7000:2379"
EPS="${IP[n1]}:2379,${IP[n2]}:2379,${IP[n3]}:2379,${IP[n4]}:2379,${IP[n5]}:2379"

echo "== [$ENGINE] fresh store on 5 nodes =="
for n in n1 n2 n3 n4 n5; do
  SSH "${IP[$n]}" "sudo systemctl stop crabwatchstore; sudo rm -rf /opt/crabwatchstore/data; sudo install -d -o ubuntu /opt/crabwatchstore/data; sudo mkdir -p /etc/systemd/system/crabwatchstore.service.d && printf '[Service]\nExecStart=\nExecStart=/opt/crabwatchstore/crabscheme run src/node-cluster.scm -- --node $n --durable yes --db /opt/crabwatchstore/data/cw --cluster $SPEC --tick-ms 250 --election-ticks 8 --shard-groups 1 --engine $ENGINE\nRestart=always\nRestartSec=10\n' | sudo tee /etc/systemd/system/crabwatchstore.service.d/ab.conf >/dev/null; sudo truncate -s0 /opt/crabwatchstore/node.log; sudo systemctl daemon-reload; sudo systemctl start crabwatchstore" &
done
wait
for i in $(seq 1 60); do
  H=$(SSH "$API" "etcdctl --endpoints=$EPS endpoint health 2>/dev/null | grep -c 'is healthy'" || true)
  [ "${H:-0}" = 5 ] && { echo "  ok all 5 healthy under $ENGINE"; exit 0; }
  sleep 3
done
echo "FATAL: store never healthy"; SSH "${IP[n1]}" "tail -30 /opt/crabwatchstore/node.log"; exit 1
