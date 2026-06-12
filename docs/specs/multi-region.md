# Spec: Multi-region crab-watchstore (epic cw-mr)

Status: DRAFT (spec for the cw-mr epic) · Date: 2026-06-11
Prereqs: epics cw-u4a (etcd parity, Jepsen-validated), cw-b5w (perf, ~1.5× etcd),
cw-24e (3-node ops: launcher/compose/ops-guide/soak). ADR 0005 (revision domain).

## Goal

Run crab-watchstore as the coordination store for **geographically-aware
Kubernetes**: each region hosts a full local replica set; the cluster spans
regions CockroachDB-style (locality-labelled replicas, leaseholder/leader
placement, locality-aware reads), while preserving the etcd v3 contract that
kube-apiserver depends on.

The target deployment:

```
region us-east           region eu-west            region ap-south
┌──────────────────┐     ┌──────────────────┐      ┌──────────────────┐
│ voter e1  voter e2│     │ voter w1         │      │ learner s1        │
│ (leader pinned)   │◄───►│ (failover voter)  │◄───►│ (read/watch only) │
│ kube-apiserver(s) │     │ kube-apiserver(s) │      │ kube-apiserver(s) │
└──────────────────┘     └──────────────────┘      └──────────────────┘
   writes: local            writes: 1 WAN RTT         writes: 2-region RTT
   reads:  local            reads:  LOCAL (serializable) / leader (linearizable)
   watch:  local            watch:  LOCAL              watch: LOCAL
```

## What CockroachDB-style means here — and what it does not

CRDB shards the keyspace into ranges with **per-range Raft groups** placed by
locality. ADR 0005 §A/§C established that splitting the etcd **revision
domain** breaks the API contract (monotonic header.revision, watch ordering,
Range@rev, Txn atomicity, compaction) — kube-apiserver relies on all of these
(resourceVersion IS our revision). So:

- **Writes stay a single Raft group** (one revision domain, linearizable,
  CP across regions — like CRDB's per-range groups, there is exactly one
  leaseholder; unlike CRDB there is one "range"). Write latency from a
  non-leader region is one WAN round trip — the same as stretched etcd, and
  the price of linearizability under partition (CAP: we keep C).
- **Reads, watches, and replica placement go locality-aware** — this is where
  the CRDB ideas land: locality labels, leader/leaseholder pinning, follower
  reads, non-voting (learner) read replicas in far regions.
- **Geo-partitioned keyspaces** (CRDB's table localities) map to **one
  crab-watchstore cluster per k8s cluster** (k8s clusters are already the
  partition unit) plus a thin global service layer — NOT to sharding one
  store's revision domain.

## Workstreams

### A. WAN-ready consensus (the substrate must tolerate 50–300 ms RTTs)
1. **Configurable Raft timing**: `--tick-ms` / `--election-ticks` /
   `--heartbeat-ticks` (etcd's `--heartbeat-interval`/`--election-timeout`).
   Today: 120 ms tick, 4–10-tick stagger — an 80 ms WAN RTT eats most of the
   window. Defaults stay LAN; a WAN profile ships in the example configs.
2. **Leader placement**: `--leader-region` preference — a leader outside the
   preferred region transfers back (MoveLeader/TimeoutNow exists) once a
   preferred-region voter is caught up. CheckQuorum/PreVote (already in) keep
   WAN flapping down; add a soak gate that asserts term stability at 150 ms
   injected RTT.
3. **WAN soak**: extend test/cluster-soak.sh with `tc netem`-style injected
   latency (compose supports it) — leader kill + membership cycle at 100–200 ms
   inter-region RTT, zero lost acks, bounded write latency.

### B. Locality-aware reads + watches (the CRDB payoff)
4. **Honor `serializable` ranges**: serve Range with `serializable=true` from
   the LOCAL replica's committed state (no ReadIndex round) — this is exactly
   etcd's semantics kube-apiserver uses for most LIST/GETs
   (`--etcd-servers-overrides` aside, apiserver issues serializable reads for
   watch cache priming). Decoded-but-ignored today; the read seam exists
   (hashkv already reads locally).
5. **Follower watch serving**: watches register on the LOCAL replica (already
   replicated REV-CF) instead of leader-gating; progress-notify driven from
   local applied. Watch ordering is per-replica revision order — same
   guarantee, no WAN hop. (Leader-gated stays for `--linearizable` watch
   creation barriers.)
6. **Learner read replicas**: `--join`-style non-voting members that serve
   serializable reads + watches in regions that should not affect quorum
   (CRDB non-voting replicas). MemberAdd --learner exists; gate reads on
   learner applied-watermark freshness (staleness bound surfaced in /metrics).

### C. Locality topology + placement
7. **Locality labels**: `region`/`zone` in member config (`locality us-east/a`),
   exposed in MemberList (etcd has no field — use member name prefix or the
   metadata convention) + /metrics; cluster spec gains
   `name:host:raftport:clientport[:region]`.
8. **Locality-aware client routing doc**: per-region endpoint lists for
   kube-apiserver (local replicas first), gRPC health checks for failover;
   the ops guide gains a multi-region chapter (bootstrap order, region loss
   runbook, leader-region failover drill).

### D. Kubernetes integration (the point of it all)
9. **kube-apiserver conformance**: run a real apiserver (kind/k3s external-etcd
   mode) against crab-watchstore; pass its startup + CRUD + watch path; document
   the compaction cadence apiserver expects (`--etcd-compaction-interval`).
10. **Geo-aware k8s topology**: one stretched control plane backed by the
    multi-region store, kubelets labelled `topology.kubernetes.io/region`;
    prove scheduling respects topology while the store survives a region loss
    (region-down drill = quorum regions keep serving; learner region degrades
    to read-only of last-applied state).
11. **Per-cluster federation pattern** (CRDB "table locality" analogue):
    document + example for N regional k8s clusters each on a local
    crab-watchstore, with a global service-discovery prefix replicated via a
    stretched store — the recommended topology when regions must write locally.

### E. Acceptance gates
- WAN soak (A3) green at 150 ms RTT: zero lost acks, term stable, p99 write
  < 2.5× RTT, serializable read p99 < 5 ms in every region.
- Jepsen register/append re-run with latency-injected partitions (existing
  suite, netem nemesis) — linearizable writes + serializable-read staleness
  bounded.
- kube-apiserver e2e (D9) green; region-loss drill (D10) documented with
  measured RTO.

## Non-goals (v1)
- Sharding the revision domain / true multi-Raft (ADR 0005 §C stands).
- Active-active writes per region (requires CRDB-style per-range leaseholders;
  use the D11 federation pattern instead).
- Cross-region encryption (mTLS exists; WAN tunnels are deployment concerns).
