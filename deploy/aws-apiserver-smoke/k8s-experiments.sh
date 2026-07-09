#!/usr/bin/env bash
# k8s-experiments.sh — failover + scale experiments against the k3s-on-quepaxa
# cluster. Runs ON the apiserver node (bash -s over ssh). kubectl = k3s kubectl.
set -uo pipefail
K="sudo k3s kubectl"

echo "=== E1 smoke: cluster basics ==="
$K get nodes
$K get ns
$K create deployment nginx --image=nginx 2>/dev/null || echo "(nginx exists)"
$K rollout status deployment nginx --timeout=180s
$K scale deployment nginx --replicas=3
$K rollout status deployment nginx --timeout=180s
$K get pods -o wide | head -6

echo ""
echo "=== E2 failover: 90s configmap churn, coordinator n1 SIGKILLed at t=30 for 10s ==="
# churn loop in background; one line per op: ok/fail + ms
rm -f /tmp/churn.log
( end=$(( $(date +%s) + 90 )); i=0
  while [ "$(date +%s)" -lt "$end" ]; do
    t0=$(date +%s%3N)
    if $K create configmap churn-$i --from-literal=k=v-$i >/dev/null 2>&1; then
      echo "ok $i $(( $(date +%s%3N) - t0 ))" >>/tmp/churn.log
    else
      echo "fail $i $(( $(date +%s%3N) - t0 ))" >>/tmp/churn.log
    fi
    i=$((i+1))
  done ) &
CHURN=$!
sleep 30
echo "  (kill signal is sent from the laptop orchestrator at this mark)"
touch /tmp/kill-mark
wait $CHURN
OK=$(grep -c '^ok' /tmp/churn.log); FAIL=$(grep -c '^fail' /tmp/churn.log)
echo "  churn: $OK ok, $FAIL fail"
echo "  longest fail run: $(awk '{if($1=="fail"){r++;m+=$3;if(r>mr){mr=r;mm=m}}else{r=0;m=0}}END{print mr" ops, ~"mm"ms"}' /tmp/churn.log)"
echo "  post-kill health: $($K get --raw /readyz 2>&1 | tail -1)"

echo ""
echo "=== E3 scale: bulk configmaps + LIST latency at increasing counts ==="
for target in 500 1000 2000; do
  have=$($K get cm --no-headers 2>/dev/null | grep -c '^bulk-') ; t0=$(date +%s%3N)
  i=$have
  while [ $i -lt $target ]; do
    # 20 parallel creates per wave
    for j in $(seq $i $((i+19))); do
      $K create configmap bulk-$j --from-literal=payload="$(head -c 512 /dev/zero | tr '\0' 'x')" >/dev/null 2>&1 &
    done
    wait; i=$((i+20))
  done
  dt=$(( $(date +%s%3N) - t0 ))
  n=$((target-have)); rate=0; [ $dt -gt 0 ] && rate=$(( n*1000/dt ))
  t1=$(date +%s%3N); $K get cm --no-headers >/dev/null 2>&1; lst=$(( $(date +%s%3N) - t1 ))
  t2=$(date +%s%3N); $K get cm bulk-$((target-1)) -o name >/dev/null 2>&1; g1=$(( $(date +%s%3N) - t2 ))
  echo "  @$target cm: created $n in ${dt}ms (~$rate/s), LIST-all ${lst}ms, single GET ${g1}ms"
done
echo ""
echo "=== store-node footprint (n1) is collected by the orchestrator ==="
