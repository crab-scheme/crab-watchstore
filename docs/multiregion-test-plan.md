# Multiregion Kubernetes Test Plan — crab-watchstore

Goal: prove a **single stretched Kubernetes cluster** — apiservers in 3 AWS
regions, one geo-distributed crab-watchstore (quepaxa, leaderless) as the
datastore — at **100k pods / 500 nodes**, surviving region loss.

This is the topology stock etcd fundamentally can't do well: etcd's Raft
leader pins every write to one region (2×WAN RTT for remote apiservers, and
leader region loss = full write outage until election). Quepaxa's leaderless
fast path commits at any replica in 1 majority round-trip. That asymmetry is
the thesis; the plan exists to demonstrate it honestly.

Status quo (2026-07-11): 5000 pods / 3 control planes / single region,
zero restarts, tuned leader-election. Known ceiling: single shard actor,
5k-pod LIST blocks the shard 2.8s, per-row range cost ~0.55ms.

---

## 1. Scale targets (what 100k/500 actually demands)

Derived from etcd's published envelope for a 5000-node/150k-pod cluster,
scaled to our 500-node/100k-pod target, plus what we've measured:

| Dimension | Target | Why / derivation |
|---|---|---|
| Pods (KWOK-backed) | 100,000 | headline |
| Nodes | 500 (≥8 real, rest KWOK) | 500 node Leases @10s renew = **50 writes/s floor**, forever |
| Steady write rate | ≥ 300 w/s sustained | leases + status updates + events + controller resyncs (etcd guidance ~10k w/s at 5k nodes; 500 nodes ≈ 1k w/s peak, 300 sustained) |
| Burst write rate | ≥ 1,500 w/s for 5 min | 10k-pod rollout storm (create+schedule+status ≈ 3–4 writes/pod) |
| Watch event fanout | ≥ 5,000 events/s | burst events × (3 apiservers + controllers per event) |
| Full-keyspace LIST (pods) | 100k rows ≈ 350 MB | 1.36 delegator digest LISTs run every ~5 min per resource **per apiserver** — this is the wall (see §2) |
| Paginated LIST (limit=500) | p99 < 1 s per page | reflector relist path |
| DB size | ~2–4 GB live, compaction keeps history bounded | etcd default quota is 2 GB, recommends 8 GB max |
| Write latency, local-region apiserver | p99 < 100 ms | k8s defaults assume etcd p99 ~100ms |
| Write latency, remote-region apiserver | p99 < 1×WAN-RTT + 50 ms | the quepaxa claim: no leader detour |
| Lease renew (Txn) | p99 < 5 s ALWAYS | untuned kubelet/controller deadlines; we should NOT need the 60s election hack at the end state |
| Region loss RTO | control plane writable < 30 s, zero pod evictions from healthy regions | headline chaos claim |
| Restarts in any 1 h soak window | 0 across all apiservers | our established bar |

**Hard math on the LIST wall:** at today's 0.55 ms/row on the shard thread,
a 100k-row LIST blocks all writes for **~55 s** — every lease in the cluster
expires. This is not tunable around; it gates everything (§2, G1/G2).

## 2. Phase 0 — Engineering gates (must land before scaling past 10k)

All tracked under cw-001; ordered by leverage:

- **G1 Reads-off-thread.** Serve Range/LIST from a RocksDB+MVCC snapshot on
  a reader pool; shard actor only sequences writes and hands out snapshot
  handles. Kills the entire "LIST blocks lease" class. Constraint learned
  the hard way: do NOT re-attempt pinned-revision chunking through
  `mvcc-range` (ca79c2c revert — 3.2s→31.5s).
- **G2 Per-row cost 0.55 ms → <0.05 ms.** Profile says key-unescape +
  record-decode interpreted byte loops. Candidates: native
  `bytevector-index`/unescape builtin in crabscheme (same playbook as the
  32× `subbv` win), batched iterator that returns N rows per hop.
- **G3 Multi-shard for k8s keyspace.** `--shard-groups N` exists; verify
  prefix→shard routing spreads `/registry/pods` vs leases vs events, and
  that Txn (compare-and-swap on single keys) and per-prefix watch never
  span shards. Leases and events on their own shard is the cheap 80%.
- **G4 Watch-path fairness.** Fix the do-create-holds-events-hostage await
  (a slow watcher register must not delay other watchers' buffered events
  on the same stream).
- **G5 LIST pagination conformance.** Verify limit/continue behaves exactly
  like etcd at 100k keys (apiserver relies on it); add a conformance test.
- **G6 Compaction at scale.** Window compaction (92efd88) proven at 5k;
  verify cost is O(window) not O(keyspace) at 100k with 5-min k3s cadence.

Exit criterion for Phase 0: single-region, **put-during-LIST probe shows
<50 ms write stall during a full 100k-row LIST**, and the leader-election
tuning (60/40/8) can be REMOVED at 10k pods with zero restarts.

## 3. Phase 1 — Single-region ladder to 100k

Extend the proven staged ladder (staged5k.sh): rungs **10k → 25k → 50k →
100k**, KWOK nodes scaled with pods (50 fake nodes per 10k pods, /24-cidr
math), +500–2000/stage with readyz gating.

Per-rung gate (all must hold 30 min):
restarts=0 · running ≥ target−0.5% · lease-renew p99 <5s ·
put-during-LIST stall <50ms · digest-sweep LIST duration recorded ·
store RSS/CPU recorded · compaction duration recorded.

Instrumentation to build once: CWS_PROF histogram dump (write hop chain,
range rows/s), plus a `bench/k8sload` driver that replays the measured
write mix (leases/status/events) directly against gRPC so we can find the
store's ceiling independent of k3s.

## 4. Phase 2 — Stretch across real regions

Topology: **5 store nodes = 2 × us-east-2, 2 × us-west-2, 1 × eu-west-1**
(majority=3 reachable from US pair without EU, ~25ms us-east↔us-west fast
path). 3 apiservers, one per region, each pointed at its **local** store
nodes first in `--datastore-endpoint`. Real RTTs (~25/60/90 ms), real
egress cost (~$0.02/GB — watch fanout dominates; budget it).

Prereqs: cross-region SG plumbing on 2379 (public IPs, as today) or ideally
the QUIC/mTLS transport; NTP sanity; kwok controller per region (fix the
node1-only single point).

Measure, per region: write p50/p99 (expect ≈ local-majority RTT, NOT
2×RTT-to-a-leader), watch event propagation lag region→region, LIST p99,
scheduler throughput scheduling to remote-region nodes. Then re-run the
Phase 1 ladder to 100k on the stretched topology.

**The money graph:** write p99 per region, quepaxa vs. a real stretched
etcd baseline (§4b) on identical instances/regions.

### 4b. etcd baseline (measured, same topology)

Stand up stock etcd (latest 3.6.x) as **5 members 2/2/1 across the same
three regions on identical instance types**, k3s `--datastore-endpoint`
pointed at it, and run the same stretched ladder rung(s) and the same
Phase 3 chaos scenarios. Expect etcd to need `--heartbeat-interval` /
`--election-timeout` retuning for 90 ms RTTs (document exactly what stock
requires — that's part of the result). Capture:

- write p50/p99 per region (leader-local vs remote apiservers),
- behavior on **leader-region kill** — write-outage duration through
  re-election vs quepaxa's no-outage claim,
- max pod rung both stores reach on the same fleet before gates fail,
- ops friction notes (tuning required, quota/compaction config).

Same harness, same metrics, same gates — the comparison is only honest if
nothing differs but the store.

## 5. Phase 3 — Region-failure chaos

At 100k steady state on the stretched cluster:

1. **Region kill (us-west-2):** stop both store nodes + apiserver
   simultaneously. Gates: remaining apiservers writable <30 s (quepaxa
   still has majority 3/5), zero restarts elsewhere, no healthy-region pod
   evictions, kubelet leases in survivor regions never lapse. Restore;
   nodes rejoin + catch up (snapshot path) without a write stall.
2. **Minority-region isolation (eu-west-1, SG-based partition):** cluster
   unaffected; isolated apiserver goes read-degraded gracefully; heals.
3. **Rolling region failure:** kill us-east-2 after us-west-2 restore —
   proves no hidden home-region dependency.
4. **Apiserver-only region loss** (store survives): agents client-side LB
   to surviving servers — extends the proven 68s failover, target <70 s
   with tuning removed per Phase 0.

## 6. Phase 4 — Jepsen over WAN

Rerun the existing suites (register, counter, cas, **Elle append**) with
the 5 Jepsen store nodes split 2/2/1 and `tc netem` delay matching real
measured RTTs (25/60/90 ms ± jitter), nemesis = region partitions +
node kills. Pass bar: same verdicts as the LAN run (Elle
strict-serializable, 0 counter anomalies). Known limitation carries over:
~19 ops/s single-shard → also run one multi-shard config to smoke G3.
This is netem-on-the-Jepsen-rig by design — real-region Jepsen adds cost,
not signal, since we take real-RTT measurements in Phase 2.

## 7. Phase 5 — Sustained churn soak

On the stretched 100k cluster, **6 h minimum**:
continuous rollout storm (10% of pods rolling at all times via deployment
image bumps), scale oscillation ±10k, one region kill/restore at hour 3.
Gates: restarts=0 outside the injected fault, store RSS drift <10%/h after
hour 1, compaction keeps revision window bounded, watch caches never pin
("Too large resource version" absent from logs), lease p99 <5 s throughout.

## 8. Fleet & budget sketch

- Store: 5 × c7g.xlarge (bump from large — 100k needs the headroom), 3 regions.
- Control: 3 × c7g.xlarge apiservers (RSS was ~1 GB at 5k; 100k informers need 8–16 GB).
- Real nodes: 8 × c7g.medium agents (2–3 per region) + KWOK for the rest.
- Ballpark: ~$3.5/h fleet + cross-region egress (measure at 10k stretched
  before committing to 100k soak). Phases are teardown-friendly; only
  soaks need long-running fleet.

## 9. Order of execution & review checkpoints

P0 gates → 10k single-region (checkpoint: remove election tuning) →
100k single-region (checkpoint: review ceiling data, right-size fleet) →
stretch at 10k (checkpoint: WAN latency data = the thesis check) →
**etcd baseline at 10k stretched** (checkpoint: the comparison graph) →
100k stretched → chaos both stores → Jepsen-WAN → soak. Each checkpoint is a
user-review point before spending the next tranche.

Out of scope (explicitly): cluster federation / per-region clusters,
membership-change Jepsen (static topology limitation stands).
