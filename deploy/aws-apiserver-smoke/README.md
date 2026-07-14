# kube-apiserver smoke test against crab-watchstore (multi-region AWS)

Boot a real `kube-apiserver` (+ `kube-controller-manager`) whose storage backend is a
**geo-distributed, 3-shard crab-watchstore** spanning **us-east-2 + us-west-2**, then drive it
with `kubectl`. This converts "etcd-compatible by Jepsen" into "actually backs Kubernetes,"
and stresses the cross-region multi-Raft + global-revision path under a real control plane.

> **Status: v1, untested infrastructure.** These scripts are a grounded scaffold (real AMIs,
> real launch flags, the proven arm64 build recipe) but have NOT been run end-to-end. Expect to
> iterate. The *point* of the exercise is to find where a real apiserver and crab-watchstore
> disagree — treat the first failure as a finding, not a script bug.

## Topology

```
            us-east-2 (primary)                         us-west-2
  ┌─────────────────────────────────┐        ┌──────────────────────────┐
  │  n1  n2  n3   crab-watchstore    │  Raft  │   n4  n5  crab-watchstore │
  │  (:7000 raft, :2379 etcd-gRPC)   │◄──────►│   (:7000, :2379)          │
  │                                  │  WAN   │                           │
  │  apiserver + controller-manager  │        │                           │
  │     --etcd-servers=http://n1:2379│        │                           │
  └─────────────────────────────────┘        └──────────────────────────┘
        3 store nodes + 1 apiserver                  2 store nodes
```

- **5 store nodes** (`n1..n3` east, `n4..n5` west), `--shard-groups 3 --global-rev yes` — 3 Raft
  groups replicated across the 5 nodes, shard 0 the global-revision authority. Matches the
  Jepsen-validated topology.
- **1 apiserver node** in east, co-located with the majority so writes commit on a local quorum;
  the west nodes replicate cross-region (real WAN ~50–70 ms east↔west).
- Native arm64 binary on Graviton (`c7g`), host networking, **plaintext** Raft/etcd over
  security-group-restricted public IPs (throwaway smoke test — see Security caveats).

## Honest caveats (read before trusting the result)

- **2 regions ≠ geo-HA.** A Raft group whose quorum sits in one region cannot survive losing that
  region. This demonstrates cross-region *replication + compatibility*, not 3-region fault
  tolerance. Real geo-HA needs a third region (or witness).
- **The rev allocator is a global coordination point.** Shard 0 owns the revision line; every
  globally-ordered op funnels through its leader. Watch whether cross-region placement of shard 0
  tanks write latency — that's the central architectural question this test exists to surface.
- **Interpreter perf.** crab-watchstore is interpreted CrabScheme. apiserver is chatty
  (Txn-with-compare on every write, watch resyncs, periodic compaction). Boot may be slow; the
  apiserver flags below set generous storage timeouts. If apiserver never reaches `/healthz`,
  that's the finding.
- **Plaintext consensus over the public internet** is fine ONLY for a locked-down throwaway. For
  anything real, enable crab-watchstore's mTLS/QUIC transport and put the nodes on peered VPCs.

## Prereqs

- AWS SSO logged in; profile `stigen-io-tasks/sandbox/AdministratorAccess` (account `022499027873`).
- `terraform`, `aws` CLI, `docker` (for the arm64 build), `kubectl`, `jq`, an SSH client.
- Your public IP (for the SSH/apiserver security-group allowlist):
  `curl -s https://checkip.amazonaws.com`

## Run it

```bash
export AWS_PROFILE='stigen-io-tasks/sandbox/AdministratorAccess'
cd deploy/aws-apiserver-smoke

# 1. Build the arm64-linux crabscheme binary + stage binary+src to S3.
#    (Apple Silicon runs linux/arm64 natively; ~10–25 min the first time.)
./build-and-stage.sh                       # creates/uses bucket cws-smoke-<account>-<region>

# 2. Provision both regions + bring up the 5-node store (terraform user-data
#    pulls the artifact and starts each node with the full --cluster spec).
cd terraform
terraform init
terraform apply -var "allowed_cidr=$(curl -s https://checkip.amazonaws.com)/32"
#    Outputs: node IPs, the assembled cluster spec, the apiserver IP, ssh hints.

# 3. Wait for the store to form quorum, then stand up apiserver + controllers.
cd ..
./setup-apiserver.sh                       # reads terraform output; ssh's to the apiserver node

# 4. Smoke ladder (apiserver boot → kubectl CRUD → watch → controller reconcile).
./smoke.sh

# 5. Tear it ALL down (instances, EIPs, SGs, key). S3 bucket left for reuse.
cd terraform && terraform destroy -var "allowed_cidr=0.0.0.0/0"
```

## What success looks like (the test ladder)

1. **apiserver reaches `/healthz` ok** — the hard gate. Boot does dozens of Txn-with-compare
   writes + bootstraps namespaces/RBAC/the `kubernetes` service against crab-watchstore.
2. `kubectl get ns` → `default kube-system kube-public kube-node-lease`.
3. `kubectl create deployment nginx --image=nginx` persists; `kubectl get deploy`.
4. `kubectl get events -w` during mutation → **exercises the cross-shard watch path**.
5. `kube-controller-manager` running → Deployment→ReplicaSet created (reconcile loop read/wrote/
   watched through crab-watchstore). Pods stay `Pending` (no kubelet) — expected, still a pass.

## Cost / teardown

~6 × `c7g.large` ≈ $0.43/hr + EBS ≈ negligible → a few hours is **< $2**. `terraform destroy`
removes everything except the S3 artifact bucket (kept so re-runs skip the rebuild; delete it
manually with `aws s3 rb --force` when done). Everything is tagged `project=cws-apiserver-smoke`.
