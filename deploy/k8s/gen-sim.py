#!/usr/bin/env python3
# gen-sim.py — generate the 9-member region×AZ crab-watchstore topology for the
# geo k8s scale sim on gtr. Emits, to stdout, a multi-doc YAML:
#   - ConfigMap cws-conf9 : per-member node configs + bootstrap.sh
#   - 9 headless Services + 9 Deployments (debian + bundle-wait bootstrap)
# Members: 3 regions × 3 AZs. Region drives Raft locality/leader-region; AZ only
# shapes the latency matrix (applied post-startup by latency-matrix.sh) and the
# k8s node labels. Leader pinned to us-east. Guaranteed-QoS CPU so the noisy host
# can't starve the consensus path.
import sys

REGIONS = ["us-east", "eu-west", "ap-south"]
AZS = ["a", "b", "c"]
# member id m<region-index><az>  e.g. m1a, m1b... ; host = cws-<id>
members = []
for ri, region in enumerate(REGIONS, start=1):
    for az in AZS:
        mid = f"m{ri}{az}"
        members.append(
            {"id": mid, "host": f"cws-{mid}", "region": region, "az": f"{region}-{az}"}
        )

cluster = ",".join(f'{m["id"]}:{m["host"]}:7001:2379:{m["region"]}' for m in members)

# cw-85j local-majority WAN config: the LEADER REGION must hold a STRICT MAJORITY
# of voters so a write commits on the LAN without crossing the WAN. With 3 AZs in
# the leader region, cap voters at 5 = us-east(3) + 2 from other regions; majority
# 3 = the leader's 3 local us-east voters. Everyone else (the 3rd eu-west AZ + all
# ap-south) boots as a non-voting LEARNER: fully replicated for read-locality +
# failover, never on the commit critical path. Passed IDENTICALLY to every member
# so all nodes derive the same genesis voters/learners split.
LEADER_REGION = "us-east"
_leader_voters = [m["id"] for m in members if m["region"] == LEADER_REGION]
_other_ids = [m["id"] for m in members if m["region"] != LEADER_REGION]
# add just enough non-leader voters to reach 5 total (keeps leader region a majority)
voter_ids = set(_leader_voters + _other_ids[: max(0, 5 - len(_leader_voters))])
learners = ",".join(m["id"] for m in members if m["id"] not in voter_ids)

BOOTSTRAP = r"""#!/usr/bin/env bash
set -euo pipefail
export DEBIAN_FRONTEND=noninteractive
apt-get update -qq
apt-get install -y --no-install-recommends libstdc++6 ca-certificates curl bash iproute2 procps >/dev/null
echo ">> bootstrap: waiting for artifact bundle (/opt/cws/.ready)"
while [ ! -f /opt/cws/.ready ]; do sleep 1; done
chmod +x /opt/cws/crabscheme /opt/cws/bin/crab-watchstore
export CRABSCHEME=/opt/cws/crabscheme
# NOTE: inter-member latency is NOT set here (the per-destination matrix is
# installed post-startup by latency-matrix.sh once all peer pod IPs exist).
cd /opt/cws
echo ">> exec crab-watchstore $*"
exec /opt/cws/bin/crab-watchstore "$@"
"""

docs = []
cm = {"a.k": "v"}
# ConfigMap
conf_data = [
    f"  bootstrap.sh: |\n" + "\n".join("    " + l for l in BOOTSTRAP.splitlines())
]
for m in members:
    conf_data.append(
        f'  {m["id"]}.conf: |\n'
        f'    node     {m["id"]}\n'
        f'    db       /data/{m["id"]}\n'
        f"    durable  yes\n"
        f"    cluster  {cluster}\n"
    )
docs.append(
    "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: cws-conf9\n  namespace: cws-geo\ndata:\n"
    + "\n".join(conf_data)
)

for m in members:
    leader_region = "us-east"
    docs.append(f"""apiVersion: v1
kind: Service
metadata: {{ name: {m['host']}, namespace: cws-geo }}
spec:
  clusterIP: None
  publishNotReadyAddresses: true
  selector: {{ app: cws9, member: {m['id']} }}
  ports:
  - {{ name: grpc, port: 2379 }}
  - {{ name: raft, port: 7001 }}
  - {{ name: health, port: 12379 }}""")
    docs.append(f"""apiVersion: apps/v1
kind: Deployment
metadata: {{ name: {m['host']}, namespace: cws-geo }}
spec:
  replicas: 1
  strategy: {{ type: Recreate }}
  selector: {{ matchLabels: {{ app: cws9, member: {m['id']} }} }}
  template:
    metadata: {{ labels: {{ app: cws9, member: {m['id']}, region: {m['region']}, az: {m['az']} }} }}
    spec:
      hostname: {m['host']}
      containers:
      - name: cws
        image: debian:bookworm-slim
        command: ["bash","/cfg/bootstrap.sh"]
        args: ["--config","/etc/crab-watchstore.conf","--tick-ms","250","--election-ticks","10","--locality","{m['region']}","--leader-region","{leader_region}","--learners","{learners}"]
        securityContext: {{ capabilities: {{ add: ["NET_ADMIN"] }} }}
        resources:
          requests: {{ cpu: "1", memory: 256Mi }}
          limits: {{ cpu: "2", memory: 512Mi }}
        ports: [{{ containerPort: 2379 }},{{ containerPort: 7001 }},{{ containerPort: 12379 }}]
        volumeMounts:
        - {{ name: cfg, mountPath: /cfg }}
        - {{ name: conf, mountPath: /etc/crab-watchstore.conf, subPath: {m['id']}.conf }}
        - {{ name: cws, mountPath: /opt/cws }}
        - {{ name: data, mountPath: /data }}
        readinessProbe:
          exec: {{ command: ["bash","-c","curl -fsS http://$(hostname):12379/health | grep -q '\\"health\\":\\"true\\"'"] }}
          initialDelaySeconds: 15
          periodSeconds: 5
          failureThreshold: 18
      volumes:
      - {{ name: cfg, configMap: {{ name: cws-conf9 }} }}
      - {{ name: conf, configMap: {{ name: cws-conf9 }} }}
      - {{ name: cws, emptyDir: {{}} }}
      - {{ name: data, emptyDir: {{}} }}""")

sys.stdout.write("\n---\n".join(docs) + "\n")
