#!/usr/bin/env bash
set -e
if [ -n "${WAN_DELAY_MS:-}" ] && [ "${WAN_DELAY_MS:-0}" != "0" ]; then
  tc qdisc add dev eth0 root netem delay "${WAN_DELAY_MS}ms" \
    || { echo "FATAL: tc netem failed (kernel module missing?)"; exit 1; }
  echo "wan: injected ${WAN_DELAY_MS}ms egress delay (inter-member RTT ~$((WAN_DELAY_MS*2))ms)"
fi
exec /opt/cws/bin/crab-watchstore "$@"
