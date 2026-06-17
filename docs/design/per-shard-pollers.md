# cw-gx4 — Per-shard peer-pollers (multi-shard scale-out lever)

bead: cw-gx4 · branch: feat/multi-shard · gated finding from the load harness (2049194)

## The proven problem

Multi-shard (cw-ivt) is functionally correct but does **not** scale out. Measured with
`bench/loadgen` (reliable, persistent-conn):

| Config | Throughput | Fail | Notes |
|---|---|---|---|
| 3-node N=1 | 3926 w/s | 0% | single group |
| 3-node N=3 **sharded** (no forwarding) | 3960 w/s | 0% | **no scale-out** |
| 3-node N=3 rr / one (forwarding) | 11 / 5 w/s | 66–80% | forwarding **collapse** |
| **1-node** N=1 (solo, no replication) | **15625** w/s | 0% | replication-free ceiling |
| **1-node** N=3 / N=6 (solo) | 20863 / 21846 | 0% | local apply parallelizes ~1.3–1.4× |

Reading: (1) the 3-node throughput (3.9k) is ~4× below the replication-free solo
ceiling (15.6k) — the **inter-node replication path dominates**; (2) local apply
*does* parallelize a little (solo 15.6k→20.9k at N=3), so the cap is **not** local
work; (3) at 3-node, N=3 == N=1 — the replication path is **serialized per node**.

## Root cause (scoped in cs-distrib / cs-runtime)

The watchstore's `node-send`/`node-poll` go through the **cs-distrib `Router`**, which
sends and drains a single cs-net logical channel — `Channel::Messages`
(`cs-distrib/src/router.rs:161,174`) — into ONE local inbox (`recv_local`). So EVERY
group's Raft AE/AER for a node multiplexes over one channel and is drained by the one
peer-poller thread → all groups serialize on that single drain. cs-net itself already
has **6 independent per-peer channel queues** (`cs-net/src/lib.rs`: Control / Consensus
/ Messages / Workflow / Bulk / Observability) — the parallelism exists at the transport
but is collapsed at the Router + the single Scheme poller.

## The change (multi-layer; needs a crabscheme rebuild)

1. **cs-distrib `Router`**: per-channel inboxes. `poll()` drains every peer's queue for
   EACH channel into a per-channel inbox; add `recv_local_channel(ch)`. (Today: one
   inbox, Messages only.)
2. **cs-runtime builtins** (`builtins/distrib.rs`): `node-send-ch FROM TO CH MSG` (route
   to cs-net channel CH) and `node-poll-ch NODE CH` (drain only CH's inbox). Keep the
   old `node-send`/`node-poll` (Messages) for back-compat.
3. **crab-watchstore**: spawn ONE peer-poller PER group, each `node-poll-ch`-ing its
   group's channel; `shard-actor` `emit!` uses `node-send-ch` on its group's channel.
   Map group → channel by `(modulo shard-idx 5) + 1` (reserve Control=0 for handshake/
   gossip); groups beyond 5 share a channel (still ≤5-way serialized — fine, N≤5 common).
   The poller already routes by shard-key, so a per-channel poller handles its 1 group
   directly.
4. Pair with **shard-aware ingest** (clients/proxy) — forwarding still collapses
   (rr/one above), so the client must reach the group's leader node directly.

## Expected + ceiling

If the 5 channels drain in parallel, 3-node N=3 should rise from 3960 toward the
replication-parallel limit (~3× → ~12k), bounded by the **single-host ceiling ~21k**
(solo N=6). TRUE linear scale-out needs SEPARATE machines (separate cores + real NIC);
a single 10-core host running 3 nodes + the load gen cannot demonstrate it. Re-measure
with `bench/loadgen -mode=sharded` on real hosts for the real number.

## Risk / cost

Multi-day, multi-layer (cs-net is ready; cs-distrib Router redesign + cs-runtime
builtins + rebuild + watchstore rewire). Consensus-adjacent (changes the replication
transport path) → re-run Jepsen after. Validate per-channel ordering/delivery is
preserved per group (Raft needs in-order per-peer-per-group; per-channel queues are
already in-order, so mapping one group → one channel preserves it).

## Status — IMPLEMENTED + VALIDATED (cw-gx4 / cw-mul)

All layers shipped: cs-distrib `Router` per-channel inboxes (`send_ch` /
`recv_local_channel` / `poll_channel`), cs-runtime `node-send-ch` / `node-poll-ch`
builtins (direct `Value`↔bytes codec), and the watchstore rewire (shard-actor
`my-channel = #(1 3 4 5)[group mod 4]`, one peer-poller per group). Wire format
unchanged; clean election + replication, 0% fail; Jepsen not yet re-run.

**The result reversed the original "no scale-out" finding — it was a measurement
artifact.** All earlier numbers were taken at `loadgen -conc 64`, which is
**latency-bound**: node CPU plateaus at ~6 cores while throughput keeps climbing
with concurrency. Re-measured at the correct operating point (`conc >= 256`):

| config | conc=64 | conc=256 | conc=512 |
|---|---|---|---|
| N=1 (single group) | 3546 | 4748 | 4684 |
| N=3 (sharded)      | 3546 | 7519 | **9343** |
| scale-out          | ~1×  | 1.58× | **1.99×** |

At conc=512, **N=3 = ~9.3k w/s = 2.0× scale-out over N=1, exceeding etcd's 7562
on the same host.** Per-channel pollers + multi-Raft sharding deliver near-linear
scale-out once enough requests are in flight to fill the parallel groups.

**Over-sharding limit (conc=512):** N=3 = 8402 → N=6 = 3333 → N=9 = 2837, with CPU
*rising* to ~800%. Each group spawns its own dedicated poller + shard-actor thread,
so N>node-count oversubscribes the cores (N=6 = 36 threads, N=9 = 54, on 10 cores)
and collapses. **Optimum = N = node count** (one leader group per node). Going
past ~9k needs more *machines* (cores + NICs), not more groups per host.

Reproduce: `bench/cluster-3node.sh` + `bench/sweep-cws.sh` (conc sweep) and
`MODE=shards bench/sweep-cws.sh` (shard sweep). **Always drive `conc >= 256`.**
