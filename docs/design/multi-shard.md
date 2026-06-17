# cw-ivt — Multi-Raft-group sharding (design)

bead: cw-ivt · branch: `feat/multi-shard` (off fix/cw-geo-perf, the EXP6-19 baseline)

## Why

Single-shard throughput plateaus at ~4297 w/s (EXP19, ~57% of etcd) — every write
serializes through one shard actor + one Raft group + one leader. All single-shard
levers are exhausted (EXP6-20; the WAL was perf-neutral). The only ceiling-raiser is
**N independent Raft groups** with keys partitioned across them: writes to different
keys land on different groups → different leaders (the election stagger already
rotates the short timeout by shard index, `shard-actor.scm:134`, so leadership
spreads across nodes) → **parallel consensus across cores/nodes ≈ linear scale-out**.

## Substrate is already multi-shard-ready

Multi-shard was deliberately STRIPPED from the crab-cache port (`node-cluster.scm:13`,
"a single shard '0' is enough"), but the machinery survived:
- `shard-actor` is fully parameterized by `shard-key`; its DB path, election stagger,
  and published table keys (`ws-shard-{pid,role,leader,commit,applied}` keyed by
  `"node:shard"`) are all per-shard.
- `peer-poller` takes `shard-keys` (PLURAL) and routes each inbound `ws-engine`/
  `ws-fwd*`/`ws-snap` frame to the right local replica by shard-key (`local-pid sk`).
- The Raft frame tag carries the shard-key, so cross-node replication is already
  shard-demuxed.

So the consensus + transport layers need ~no change. The work is at the edges:
spawn N shards, and route client ops to the owning shard.

## Changes

1. **`node-cluster.scm`**: `--shard-groups N` (default 1 = today's behavior). Spawn N
   `shard-main` actors with keys `"0".."N-1"`, each its own DB path
   (`<db>-shard<i>`); pass the full `("0".."N-1")` list to the peer-poller (it already
   loops them). voters/learners identical across groups (every node replicates every
   group, like etcd-style range replicas — NOT disjoint placement, so any node can
   serve any key and failover is per-group).
2. **`grpc-kv.scm`**: today a worker holds ONE `shard-pid`. Replace with a shard
   RESOLVER: `(shard-pid-for key) = (table-lookup 'ws-shard-pid (node ":" (hash(key) mod N)))`,
   cached per-N. `ask-shard`/`shard-write`/`shard-range`/`shard-prev` take the key →
   resolve → send. Hash = a stable byte hash of the user key (FNV-1a over the key bytes).
3. **`grpc-router.scm`**: unchanged for streaming/auth pinning; unary KV ops already
   reach a worker, which now self-routes by key.

## etcd-compat divergences (the hard part — scope explicitly)

- **Cross-shard Txn**: an etcd `Txn` over keys in different shards spans Raft groups →
  NOT atomic under independent per-group Raft. **Phase 1 decision: single-shard txns
  only** — hash all of a txn's compare+op keys; if they map to one shard, route the
  whole txn there (atomic, correct); if they span shards, REJECT with a clear error
  (or, behind a flag, fall back to routing the whole DB to shard 0 = no parallelism).
  True cross-shard atomicity (2PC / a txn-coordinator group) is a later phase, big.
- **Cross-shard Range** (`[a,z)` spanning shards): scatter-gather across the covered
  shards + merge-sort + a synthesized revision. Phase 2. Phase 1: a Range whose
  `[key,range-end)` spans shards is served by scatter-gather read (reads are easier
  than txns — no atomicity needed beyond per-shard linearizability; the merged result
  is a valid snapshot-union). Single-key Range trivially routes to one shard.
- **Global revision**: etcd has ONE monotonic revision; per-shard revisions are
  independent. Clients relying on a global mod_revision ordering across keys in
  different shards will see per-shard revisions. Document as a known divergence
  (acceptable for k8s-style per-key usage; not for global-ordering consumers).
- **MemberList/Status**: report per-shard or aggregate; pick aggregate for compat.

## Phases

- **P1 — N-shard spawn + key-hash routing (single-key ops + single-shard txn).**
  `--shard-groups`, the grpc-kv resolver, txn single-shard guard. Measure scale-out:
  N=3 across a 3-node cluster should approach ~N× the single-shard write throughput
  (leadership spreads → parallel). Reject cross-shard txn. Cross-shard range → P2.
- **P2 — cross-shard Range scatter-gather** (merge across covered shards).
- **P3 — cross-shard txn** (coordinator/2PC) OR finalize the documented restriction.
- **P4 — rebalancing/dynamic shard count** (likely out of scope; fixed N at boot).

## Validation

- Per-shard correctness reuses the existing Jepsen harness (each group is an
  independent Raft group; the register/cas workloads over many keys now exercise
  multiple groups). Run register no-nemesis + kill at N=3.
- Throughput: 3-node, `--shard-groups {1,3,6}`, `check perf --load=l` — expect
  near-linear up to min(groups, cores, nodes). This is the headline measurement.

## Status — IMPLEMENTED + scale-out VALIDATED (cw-ivt / cw-gx4 / cw-mul)

Multi-Raft-group sharding shipped, plus per-shard pollers (see
`per-shard-pollers.md`). **Scale-out confirmed at the correct operating point**
(`loadgen -conc >= 256` — `conc=64` is latency-bound and understates throughput
~60-70%): at conc=512, N=3 = ~9.3k w/s = **2.0× over N=1 (~4.7k), beating etcd's
7562 on the same host.** Optimum shard count = node count; N>nodes over-shards
and collapses (dedicated thread per group oversubscribes the cores). The earlier
"no scale-out (N=3==N=1)" reading was a conc=64 artifact. Reproduce with
`bench/sweep-cws.sh`. WAL (feat/raft-wal) is orthogonal (perf-neutral).
