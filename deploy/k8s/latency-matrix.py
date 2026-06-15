#!/usr/bin/env python3
# latency-matrix.py — install a per-destination inter-member latency matrix on
# the 9-member region×AZ crab-watchstore cluster (gtr geo sim).
#
# Models RTT between regions and AZs as faithfully as a single physical node
# allows: each member applies EGRESS delay to each peer's pod IP via tc netem
# (prio qdisc + u32 dst filters), so member<->member RTT = 2 x each-way delay.
# Same-region different-AZ ~ 1ms RTT; inter-region per a realistic matrix.
# Unclassified traffic (apiserver<->etcd, DNS) stays on the default band (0ms).
#
#   KUBECONFIG=~/.kube/gtr.yaml python3 latency-matrix.py
import json
import subprocess
import sys

NS = "cws-geo"
KC = ["kubectl", "-n", NS]

# each-way delay in ms; RTT = 2x. intra-region (diff AZ) = 0.5 => ~1ms RTT.
INTRA_REGION_AZ = 0.5
INTER_REGION_EACHWAY = {
    frozenset(["us-east", "eu-west"]): 40,  # ~80ms RTT
    frozenset(["us-east", "ap-south"]): 60,  # ~120ms RTT
    frozenset(["eu-west", "ap-south"]): 75,  # ~150ms RTT
}


def eachway(r1, r2):
    if r1 == r2:
        return INTRA_REGION_AZ
    return INTER_REGION_EACHWAY[frozenset([r1, r2])]


def main():
    out = subprocess.check_output(KC + ["get", "pods", "-l", "app=cws9", "-o", "json"])
    pods = json.loads(out)["items"]
    # member -> {region, ip, pod}
    info = {}
    for p in pods:
        if p.get("status", {}).get("phase") != "Running":
            continue
        lbl = p["metadata"]["labels"]
        info[lbl["member"]] = {
            "region": lbl["region"],
            "az": lbl["az"],
            "ip": p["status"]["podIP"],
            "pod": p["metadata"]["name"],
        }
    if len(info) < 9:
        print(f"WARN: only {len(info)}/9 members Running", file=sys.stderr)

    for mid, me in info.items():
        # build tc script: prio root, one band per peer, u32 dst filter -> band
        lines = [
            "tc qdisc del dev eth0 root 2>/dev/null || true",
            "tc qdisc add dev eth0 root handle 1: prio bands 16",
        ]
        band = 2
        for pid, peer in info.items():
            if pid == mid:
                continue
            d = eachway(me["region"], peer["region"])
            lines.append(
                f"tc qdisc add dev eth0 parent 1:{band} handle {band}0: netem delay {d}ms"
            )
            lines.append(
                f"tc filter add dev eth0 protocol ip parent 1:0 prio {band} "
                f"u32 match ip dst {peer['ip']}/32 flowid 1:{band}"
            )
            band += 1
        script = "set -e; " + "; ".join(lines)
        try:
            subprocess.run(
                KC + ["exec", me["pod"], "-c", "cws", "--", "bash", "-c", script],
                check=True,
                capture_output=True,
            )
            print(f"  ok   {mid} ({me['az']}): matrix installed for {band-2} peers")
        except subprocess.CalledProcessError as e:
            print(f"  FAIL {mid}: {e.stderr.decode()[:200]}", file=sys.stderr)


if __name__ == "__main__":
    main()
