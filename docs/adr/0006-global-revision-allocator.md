# ADR 0006 — Synthetic global revision allocator for multi-Raft-group scale-out (cw-kp0)

Status: PROPOSED (Phase 1 keystone implemented + proven; Phases 2–5 pending raft.scm sign-off + Jepsen)
Date: 2026-06-25
Supersedes: the option-C deferral in ADR 0005, **for the global-scale goal only** (epic cw-7cn).
Bead: cw-kp0.

## Context

ADR 0005 chose option B (one Raft group, parallel apply workers) for v1 and explicitly
deferred option C (N independent Raft groups + a revision-allocation scheme) as
"etcd→TiKV-grade complexity… defer until B's ceiling is measured." We have now measured it
(`docs/perf-vs-etcd.md`, `docs/design/wan-multi-region.md`): the single Raft group is a hard
write ceiling — one sequencer, one cross-region quorum RTT per write (~3.1k w/s @100ms RTT).
For the massively-scalable, globally-deployed goal (epic cw-7cn) that ceiling must fall, which
means option C. The multi-Raft-group **substrate already exists** on `feat/multi-shard`
(`--shard-groups N`, per-group pollers, shard-demuxed Raft frames, the grpc-kv shard resolver —
see `docs/design/multi-shard.md`); what is missing is the piece that makes N groups produce an
etcd-faithful revision domain. That piece is this ADR.

## The semantic wall (from ADR 0005 §"ONE revision domain")

etcd exposes a single, totally-ordered revision sequence; clients depend on five invariants:
(1) `header.revision` monotonic across all responses; (2) watch delivers in revision order with
no missing events, incl. a prefix watch spanning shards; (3) range@R is a consistent cross-key
snapshot; (4) txn is multi-key-atomic at one R; (5) compaction is "< R" globally. Independent
per-shard counters (ADR 0005 option A) break all five — which is why `--shard-groups` defaults
to 1 today.

## Decision: lease-batched global allocator + low-watermark

**Allocator.** One authority holds `high` = the highest revision granted (a replicated counter
owned by a designated rev-leader group). A shard about to commit a group-commit batch of K
writes requests a contiguous block of exactly K revisions: `(rev-grant high K) → [lo, lo+K)`.
Per-batch grants sized to the batch mean a settled batch uses **all** its revisions — no
intra-batch holes. Grants are serialized and strictly increasing, so blocks are globally ordered
and non-overlapping ⇒ the revision sequence is monotonic (invariant 1).

**Low-watermark (the keystone).** A cross-shard watcher may release an event at revision R only
once **no shard can still produce an event ≤ R**. That bound is W:
- a shard with an in-flight batch starting at `lo` constrains W at `lo-1`;
- an **idle** shard does **not** pin W at its last write (its next grant is above `high`), so a
  single idle shard cannot freeze the watermark — the trap a naive `min(last-rev)` would fall into.

⇒ `W = (min over in-flight shards of lo-1)`, or `high` when none are in-flight. The prefix watch
buffers per-shard event streams and releases everything ≤ W in revision order (invariant 2). A
range@R / ReadIndex gates on `W ≥ R` so no read is served across a gap (invariant 3).

**Phase 1 (this ADR) implements + proves the allocator + watermark + watcher-merge in isolation**
— `src/rev-allocator.scm` (pure: no Raft/IO) with `test/rev-allocator.scm` (17 checks). The
decisive test: shards committing **out of wall-clock order** (a higher-block shard commits before
a lower-block shard) still yield strictly in-revision-order delivery, and an idle shard never
freezes the watermark. This de-risks the hard correctness property before any consensus-core change.

## Phased plan (each consensus phase gets its own Jepsen pass — child cw-nzm)

- **Phase 1 — allocator core (DONE, proven):** pure algorithm + self-check.
- **Phase 2 — wire the authority:** replicate `high` on a rev-leader (reuse a shard group or a
  tiny dedicated group); shards request per-batch grants in the group-commit path
  (`shard-actor.scm`). Lease blocks to amortize the grant RTT. **Touches raft.scm — needs sign-off.**
- **Phase 3 — watch revision-merge + read watermark:** k-way merge across shard watch streams
  gated by W (`grpc-watch.scm`); ReadIndex/range wait on `W ≥ R`.
- **Phase 4 — cross-shard txn + compaction coordinator:** multi-key txn at one R across groups
  (claim-and-drain, ADR 0005 §B.4 generalized to groups); global "compact < R".
- **Phase 5 — failover + Jepsen-at-scale:** N-group elections, rev-leader failover (the authority
  counter must survive leader loss — it is Raft-replicated, so it does); validate the five
  invariants under partition/kill/membership at 10k-node scale (cw-nzm).

## Alternatives rejected

- **HLC timestamps as revisions:** monotonic + comparable, but not the dense int64 etcd clients
  read from `header.revision`, and gives no clean watermark for in-order watch. Rejected.
- **Per-write grants (block size 1):** no holes, trivially correct, but one allocator RTT per
  write — reintroduces the very serialization we are removing. The per-**batch** grant keeps the
  group-commit amortization (cw-b5w.3) while staying hole-free.
- **Independent per-shard revisions (ADR 0005 option A):** breaks all five invariants. Rejected.

## Consequences

- Write throughput scales with shard-group count (parallel consensus across leaders/cores/nodes),
  the epic cw-7cn write ceiling.
- Cost: cross-shard coordination on the write path (the grant) and watch path (the merge); the
  watermark adds latency bounded by the slowest in-flight batch. Idle shards are free.
- Risk: this is consensus-core. Phases 2/4/5 must not ship without Jepsen (cw-nzm) — the same bar
  ADR 0005 set ("the Jepsen workloads are the final referee").
