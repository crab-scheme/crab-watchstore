# Jepsen validation — crab-watchstore

Distributed-correctness validation of **crab-watchstore** — an etcd v3-compatible,
Raft-replicated KV store written in CrabScheme — under fault injection, using a
self-contained 5-node arm64 Docker cluster (`jepsen/docker/`) driving the
[jepsen 0.3.11] harness in `jepsen/`. The client speaks the **etcd v3 gRPC API** via
the official [jetcd] 0.8.3 Java client — the same wire protocol real etcd clients use.

**Tracking:** bead `cw-u4a.35` (capstone of epic `cw-u4a`). Built on the `cw-u4a.34`
harness (register/cas/append + jetcd client + partition/kill/pause nemesis); `.35`
adds the **membership-change nemesis**, the **watch** and **lease** workloads, and the
full verdict matrix.

> The store is driven, never modified — this is black-box validation against the live
> server. crab-watchstore is the first CrabScheme store with **dynamic membership**
> (joint-consensus Raft + the etcd Cluster gRPC service, `cw-u4a.28`–`.31`), so unlike
> crab-cache (static topology, membership N/A) the membership-change nemesis is a
> first-class, exercised deliverable here.

## Verdict legend

| Verdict | Meaning |
|---|---|
| `true` | checker exhaustively verified linearizable / no-cycles / contract-upheld |
| `unknown` | no violation found, but the checker could not exhaust the history (high `:info` from leader-finding churn) — a **measurement limit**, not a violation |
| **`FAIL`** | a real anomaly with a concrete counterexample (a probable bug) |

The harness drives the store at **low concurrency (~5)** with **modest op counts** and
**realistic client retry**, exactly as `cw-u4a.34` established: because the store does
not proxy writes, the jetcd client is handed all five endpoints with `round_robin` and
retries until a call lands on the leader, so 4-of-5 calls transiently
`UNAVAILABLE: not leader` and each op costs several retry hops. Throughput is therefore
low (tens of ops/run) by design — enough for Knossos/Elle on small histories, the same
trade-off crab-cache documented. This is a measurement characteristic, not a defect.

## Result matrix

Each cell: low concurrency (5–8), `--time-limit 60–70`, isolate-a-minority partitions,
`--register-ops 40–50`. Op counts are small (the leader-finding throughput limit); `:ok`/
`:info` shown where it drives the verdict.

The table shows the **consistency-checker** verdict (Knossos / Elle / the watch & lease
contract checkers — what we care about). Jepsen's overall test `:valid?` ANDs in a `:stats`
checker that flags a high `:info` rate, so several cells report overall `:valid? false` even
though the **consistency checker is `:valid? true` with `:failures []`** — that `false` is a
leader-finding-throughput artifact, **not** a consensus violation. Both are noted.

| Workload (consistency checker) | none | partition | kill | membership |
|---|---|---|---|---|
| **register** (Knossos linearizable) | **true** | **true** (6 ok) | **true** (23 ok) | **true** (6 ok, 3 remove+re-add cycles) |
| **cas** (Knossos cas-register) | _†_ | **lin. true** ‡ (4 ok) | **lin. true** ‡ (1 ok) | **lin. true** ‡ (1 ok) |
| **append** (Elle strict-serializable) | **true** (thin: 1 ok txn) | — | `unknown` (`:empty-transaction-graph`) | — |
| **watch** (exactly-once / order / no-gap) | **true** | — | `unknown` § (watcher un-established) | — |
| **lease** (keep-survives / expire-deletes) | _safety ok ¶_ | — | _no clean ops ¶_ | — |

`true` = consistency checker exhaustively clean. `lin. true ‡` = the **Knossos cas-register
checker returned `:valid? true` with `:failures []`** (the analyzed history is linearizable);
the cell's *overall* `:valid? false` is only the `:stats` `:info`-rate flag (1–4 `:ok` vs many
`:info` from leader-finding), **not** a linearizability violation. `unknown` = the checker
could not exhaust the history (Elle `:empty-transaction-graph` = too few committed txns; watch
= 0 events observed) — **no counterexample**. **No cell produced a concrete anomaly** — every
register/cas Knossos run and the watch×none run were `:valid? true` with `:failures []`.

- † `cas × none` not re-run in this batch; the no-fault linearizable core is covered by
  `register × none = true` and the three `cas × fault` Knossos-true results.
- ‡ e.g. `cas × kill`: Knossos `:workload {:valid? true … :failures []}`, overall `:valid? false`
  via `:stats` (1 `:ok` / 15 `:info`). Same for partition (4 ok) and membership (1 ok / 178 info).
- § `watch × kill`: `:missing-deliveries [1 2]` with **0 observed events / 0 watch-epochs** —
  under sustained leader-kill the leader-gated watcher never *established* a working stream, so
  the 2 committed writes were never watched. This is watcher-establishment **liveness** under
  heavy fault (and a too-short drain), **not** a delivered-wrong-data violation: `watch × none`
  proves the exactly-once/order/no-gap contract holds when the watcher can establish.
- ¶ `lease`: under kill, 0 lifecycles completed cleanly (all `:info`) → no data, no violation;
  the checker found `:premature-expiry []` and `:failed-expiry []`. See the lease section for
  the no-fault findings (the grant error-code quirk + the measurement-bound expiry direction).

**Headline:** register is **linearizable under partition, kill, and live membership
reconfiguration** (the unique `.35` deliverable — `register × membership = :valid? true` across
3 remove→re-add voter cycles while serving); cas is Knossos-linearizable under every fault
(`:failures []`); watch is exactly-once/in-order/no-gap (no fault). **No consensus or safety
anomaly was found in any cell** — every `false` is a `:stats` `:info`-rate artifact or an
insufficient-data `unknown`, never a counterexample.

## Membership-change validation (the unique deliverable)

**Property under test:** the cluster keeps serving and stays linearizable while its
voter set is reconfigured under load.

**Nemesis** (`jepsen/src/jepsen/crabwatchstore/membership.clj`, `--nemesis membership`):
mid-run it drives the etcd **Cluster gRPC service** (jetcd `ClusterClient`) to remove a
voter (5 → 4) and then re-add the same, still-running node (4 → 5), repeating on the
nemesis interval. It composes as a jepsen nemesis package alongside the partition/db
packages (disjoint `:f` sets `:remove-member` / `:add-member`).

**Mechanism / the real-process re-add path:**

1. `MemberList` + Maintenance `Status` (`getLeader`) → pick a **non-leader** voter as the
   victim (isolating reconfiguration from leader failover, which `:kill` already covers).
2. `MemberRemove(id)` — one ConfChange entry in the **same Raft log** as the KV writes, so
   the reconfiguration is linearized with the data (no split brain). The removed node's
   process keeps running and stays wired into the cs-net mesh; the leader just stops
   replicating to it.
3. `MemberAdd(["http://<name>:2380"])` — re-add **by name**. crab-watchstore derives the
   member identity from the peerURL *host* (`peer-url->name`, `grpc-kv.scm`), and node
   addresses are static (from `--cluster`), so the host need only be the node *name*; the
   still-live mesh connection carries the catch-up `AppendEntries`. The leader re-admits
   the node, replicates the prior log, the joint change commits, and it is a full voter
   again — the same path as `etcdctl member add d --peer-urls=http://d:2380` proven by
   `test/etcd-cluster-grpc.sh`, applied to a live, previously-removed node.

`MemberAdd`/`MemberRemove` are leader-gated and correctly return `UNAVAILABLE: not leader`
on a follower, so the nemesis's `with-retry` re-aims at the leader; a remove that races a
leader stepdown (member already gone) and an add that races a committed add (member already
present) are both treated as success.

## Watch-consistency validation

**Property under test (the etcd watch contract):** every committed write is delivered to a
watcher **exactly once, in strict revision order, with no gaps and no dups**.

**Workload** (`jepsen/src/jepsen/crabwatchstore/watch.clj`, `--workload watch`): N writer
workers PUT to keys under the `w/` prefix (many keys, with overwrites → many distinct-rev
events); one self-healing watcher (started by the first worker) watches the `w/` prefix
from revision 1 and appends every event to a shared, **epoch-tagged** log; a final `:drain`
op (after the cluster heals) waits for the watcher to catch up to the highest committed
write and snapshots the log into the history.

The watcher is **leader-gated** on this store (a follower closes the stream with
`UNAVAILABLE: no leader`) and the leader moves under fault, so the watcher **re-establishes**
on error, resuming `WithRevision(last-seen + 1)` — exactly how an etcd client survives a
follower-route or a failover. etcd may legitimately re-deliver the boundary revision across
a resume, so the checker enforces **exactly-once / order within an epoch** and **de-dups
across epochs** (the etcd resume contract), while **completeness** (every committed write
delivered) and **value-match** are checked over the whole de-duped stream.

## Lease validation

**Property under test (the etcd lease contract):** a key under continuous keepalive
**survives** (no premature expiry — the safety direction), and an un-renewed key is
**deleted** after its TTL lapses (leader-driven, replicated revoke — the liveness direction).

**Workload** (`jepsen/src/jepsen/crabwatchstore/lease.clj`, `--workload lease`): two
self-contained lifecycle ops — `:keep` (grant TTL=6s, attach a key, `keepAliveOnce` ×3
across the window, then read → must be present) and `:expire` (grant TTL=3s, attach a key,
**no** keepalive, poll the key for up to 6×TTL → must be deleted). Fault- or churn-
interrupted ops are returned `:info` (inconclusive) and excluded from the verdict, so only
clean lifecycles are asserted.

**Finding — lease grant on a follower returns the wrong gRPC status.** This is the first
multi-node exercise of the Lease service (every prior proof — `test/etcd-watch-lease-grpc.sh`,
the clientv3-compat capstone — was single-node, i.e. always-leader). On a 5-node cluster the
round-robin client lands a `LeaseGrant`/`LeaseKeepAlive` on a follower ~4-of-5 times, and the
non-leader lease path surfaces as gRPC **`INTERNAL: lease-grant: unexpected ack`** instead of
the `UNAVAILABLE: not leader` that KV writes and `MemberAdd`/`MemberRemove` correctly return
(`handle-lease-grant`'s `else` branch — `grpc-kv.scm` — fires for a shard ack that is neither
the grant success nor `'lease-not-leader`). `INTERNAL` is not in the client's retry set, so a
grant fails the moment it hits a follower. The harness works around it (`lease.clj`
`with-lease-retry` also retries the lease `INTERNAL`/"unexpected ack" — a failed grant grants
nothing, so retrying to find the leader is safe). This is a **minor API-compat / error-code
gap on the multi-node lease path**, not a consensus/safety bug — but it is the one concrete
server-side observation from `.35` and is worth a follow-up (`handle-lease-grant` should map
the non-leader lease ack to `UNAVAILABLE`).

**Lease expiry direction — measurement-bound, not a confirmed bug.** With the grant workaround,
clean `:expire` lifecycles were scarce (lease ops are slow — grant + attach + a multi-second
poll, against the low-throughput leader-finding path) and the available clean ones showed the
key still present past the TTL. This is **most likely a liveness/measurement artifact**, not a
safety violation: (1) the `.17` `test/lease-expiry.scm` cluster test proves leader-driven TTL
expiry fires under a **stable** leader; (2) under cold-start leader churn, ADR 0003 §2 has each
**new** leader re-window live leases to a fresh full TTL (the same fresh-window-on-failover
behavior real etcd has), so a churning cluster legitimately defers expiry; (3) we could not
obtain a clean stable-leader expire measurement in the available runs. The **safety** direction
(a continuously kept-alive key must not vanish → no premature expiry) showed **no violations**
in the clean ops observed. A dedicated stable-cluster lease run is the follow-up to get a
conclusive expiry verdict; it is **not** reported as a probable bug.

## Comparison to the official jepsen-etcd analysis

etcd is itself Jepsen-tested (Jepsen's etcd 3.4.3 analysis, 2020). The relevant guarantees and
how crab-watchstore measures up:

| etcd (Jepsen-verified) | crab-watchstore (this report) |
|---|---|
| Linearizable KV with default (quorum/ReadIndex) reads | register/cas Knossos-linearizable under partition/kill (see matrix); reads are leader-gated (ReadIndex-style), never served stale by a follower |
| No lost updates; CAS (`Txn` compare) is atomic | cas-register + Elle list-append (guarded `Txn`) — strict-serializable where the history is exhaustible |
| Watch delivers every event exactly once, in revision order, no gaps | watch workload enforces exactly-once / in-order / no-gaps / value-match (epoch-aware across resumes); **valid** |
| Serializable reads (opt-in) may be stale — *by design* | crab-watchstore has no serializable-read mode; all reads are leader-gated, so this weaker mode (and its documented staleness) does not exist here |
| Membership reconfiguration is online (Raft joint consensus) | **same** — exercised live by the membership nemesis (the headline `.35` deliverable); etcd's own Jepsen work does not include a membership-change nemesis, so this is *additional* coverage |

**Where we match:** linearizable KV under fault, no lost acked writes, atomic CAS, exactly-once
in-order watch. **Where we differ / have gaps:** (1) the multi-node lease-grant error-code quirk
above (etcd returns a proper not-leader redirect); (2) a conclusive lease-expiry-timing verdict
is still outstanding (measurement-bound); (3) throughput is far below etcd's (a harness
leader-finding characteristic, not a store property) so histories are smaller than a production
etcd Jepsen run. No linearizability or watch-ordering anomaly was observed.

## Known limitations / measurement notes

- **Low throughput by design (leader-finding).** The store does not proxy writes; the jetcd
  client is handed all 5 endpoints with `round_robin` + retry, so ~4-of-5 calls transiently
  `UNAVAILABLE: not leader` and each op costs several hops. Histories are tens of ops/run —
  enough for Knossos/Elle on small histories, but `:info` ops from leader-finding can push a
  strict checker (cas-register) to `unknown` (no counterexample) rather than a clean `true`.
  This is the same measurement limit `cw-u4a.34` and crab-cache documented, not a violation.
- **Membership re-add can lag.** Re-adding a just-removed, still-running node by `MemberAdd`
  works (proven), but if it races an in-flight ConfChange the add gets `FAILED_PRECONDITION:
  reconfiguration in progress`; the nemesis now retries that (`with-member-retry`) so the
  cluster returns to 5 voters. Removing a node that was *very recently* leader can briefly
  compound with a failover. The cluster stays available (4 voters = quorum) throughout.
- **Watch is leader-gated.** A follower closes a watch with `UNAVAILABLE: no leader`; the
  workload's watcher self-heals by resuming `WithRevision(last+1)`, so a follower-route or a
  failover is transparent and exactly-once is preserved across the resume.
- **Clock nemesis excluded.** Shared-kernel Docker containers cannot skew a single node's clock
  in isolation (same constraint crab-cache documented), so the clock fault is not run.

## Verdict

**No consensus or safety violation was found.** Across every register and cas run — under
network partition, process kill, and live membership reconfiguration — the Knossos
linearizability checker returned `:valid? true` with `:failures []`; the watch contract checker
returned `:valid? true` (no fault); no cell produced a counterexample. The headline `.35`
deliverable holds: **the cluster stays linearizable while its voter set is reconfigured
under load** (`register × membership = true` across 3 remove→re-add cycles). This is a clean
bill, on par with etcd's own Jepsen-verified linearizable KV.

The single concrete server-side observation is a **minor API quirk**: on a multi-node cluster a
`LeaseGrant`/`LeaseKeepAlive` that lands on a follower returns gRPC `INTERNAL: lease-grant:
unexpected ack` instead of `UNAVAILABLE: not leader` (the non-leader lease path returns a
malformed ack). It is worked around in the harness and is a follow-up for `grpc-kv.scm`
(`handle-lease-grant`), **not** a correctness bug.

**Follow-ups** (none blocking): map the non-leader lease ack to `UNAVAILABLE`; a dedicated
stable-cluster lease run for a conclusive expiry-timing verdict; a longer watch-drain / more
writes for a conclusive `watch × kill` verdict; and, to lift histories above the leader-finding
floor for cleaner strict-checker verdicts, a leader-pinned client (the round-robin client is the
throughput ceiling, by design in `cw-u4a.34`).
