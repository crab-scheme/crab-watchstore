# deploy/docker/Dockerfile — one crab-watchstore member (cw-24e.2).
#
# Build context = the repo root (see docker-compose.yml):
#   docker compose -f deploy/docker/docker-compose.yml up -d --build
#
# Expects jepsen/docker/crabscheme to be a LINUX arm64 crabscheme built with
# --features stdlib-store,grpc (refresh recipe: jepsen/bin/stage-docker.sh).
FROM debian:bookworm-slim

RUN apt-get update \
 && apt-get install -y --no-install-recommends libstdc++6 ca-certificates curl bash iproute2 \
 && rm -rf /var/lib/apt/lists/*

COPY jepsen/docker/crabscheme /opt/cws/crabscheme
COPY src /opt/cws/src
COPY bin /opt/cws/bin
RUN chmod +x /opt/cws/crabscheme /opt/cws/bin/crab-watchstore

ENV CRABSCHEME=/opt/cws/crabscheme
WORKDIR /opt/cws

# 2379 etcd v3 gRPC (h2c) · 7001 Raft · 12379 health/metrics HTTP (client+10000)
EXPOSE 2379 7001 12379
# The member binds the address from its --cluster row (its compose hostname →
# the container IP, NOT loopback), so probe via $(hostname).
HEALTHCHECK --interval=5s --timeout=3s --start-period=20s --retries=5 \
  CMD curl -fsS "http://$(hostname):12379/health" | grep -q '"health":"true"' || exit 1

# WAN_DELAY_MS (cw-lkq.3): one-way egress delay injected via tc netem — with one
# member per "region" every member<->member path is inter-region (RTT = 2x this).
# Needs cap_add: [NET_ADMIN]. Fails LOUD if tc cannot install the qdisc.
COPY deploy/docker/entry.sh /opt/cws/entry.sh
RUN chmod +x /opt/cws/entry.sh
ENTRYPOINT ["/opt/cws/entry.sh"]
CMD ["--config", "/etc/crab-watchstore.conf"]
