# QuePaxa consensus engine for crab-watchstore (option 3)

**Branch:** `feat/quepaxa`. **Epic:** see beads (`bd list | grep quepaxa`).
**Sources:** QuePaxa (SOSP'23, Tennage et al., EPFL DEDIS), Cloudflare Meerkat
(blog.cloudflare.com/meerkat-introduction), dedis/quepaxa reference impl (Go).

## Why

Our multi-region (us-east-2/us-west-2) shards already needed a hand-tuned "WAN
election profile" and append/match clamps because Raft's timeout-driven elections
thrash on jittery WAN links. QuePaxa keeps Multi-Paxos/Raft's 1-RTT normal case
but replaces timeout-based leader election with a randomized asynchronous core +
**hedging delays**: a mis-tuned hedge costs latency, never liveness. Meerkat is
Cloudflare running exactly this for a global control plane.

## Architecture recap (what exists)

- `src/raft.scm` — PURE engine: `(node, input) -> (node' . outputs)`, alist node,
  tagged-list messages, log entries `(term . command)`, deterministic in-file
  cluster simulator. Vendored contract from crabscheme `lib/consensus/raft.scm`.
- `src/server/shard-actor.scm` — the driver: ticks from peer-poller, `('engine
  from rpc)` frames in, `emit!` outputs over node-send; consumes `raft-leader?`,
  `raft-applied`, `raft-term`, `raft-commit`, snapshots (`ship-snaps!`),
  ReadIndex round bookkeeping, leader-region transfer, CheckQuorum/PreVote.
- `src/server/peer-poller.scm` — transport bridge; **tag-agnostic** (forwards any
  `ws-engine` frame), so new message types need no transport work.
- Consumers of leadership: get-fast gating, gr-writer/rev-allocator refill,
  metrics tables, moveleader, watch progress (leader-only apply path).

## QuePaxa engine design (new `src/quepaxa.scm`)

Same purity contract as raft.scm. Per **log slot** an independent consensus
instance; instances pipeline like Multi-Paxos.

- **Recorder** (per replica, per slot): the "interval summary register" — a tiny
  state object storing, per round, the max-ranked proposal seen at each of 4
  lock-step steps. One RPC: `(rec slot round step proposal)` →
  `(recr slot round step F A)` where F = max proposal recorded at this step so
  far, A = aggregate (max of the previous step's recorded set). Dumb, fast,
  no timeouts — this is what makes followers trivial.
- **Proposer** (per replica): drives rounds. Round r, steps 0–3:
  - S0: submit proposal with rank = random ∈ [1, MAX-1]; the round's
    **coordinator** (current leader analogue) uses rank = MAX → if a quorum of
    recorders saw only the coordinator's proposal, **decide in 1 RTT (fast
    path)** — byte-equivalent latency to Raft's healthy-leader case.
  - S1/S2: spread best / gather common (ensures agreement despite races).
  - S3: if the best-known proposal provably reached a quorum, decide; else
    next round with fresh randomness. Terminates with probability 1 — no
    dueling-leader livelock, no election.
- **Hedging**: a non-coordinator proposer holds its proposal for `hedge` ticks;
  if no decision observed, it enters the round too. `hedge` is a latency knob
  only (safe at any value including 0). Replaces election-timeout, PreVote,
  CheckQuorum, timeout-now — all deleted on this path.
- **Decided log → apply**: identical downstream shape to raft
  (`commit`/`applied`/`sm`, entries `(epoch . command)`), so mvcc apply-fn,
  ctx-save-applied!, watch pipeline are untouched.

## What must be re-derived (not free)

1. **Snapshot/compaction**: keep `base`/`base-term` semantics; a replica whose
   slots were compacted requests a store snapshot (reuse ws-snap protocol).
2. **Linearizable reads**: no leader ⇒ no ReadIndex. Meerkat's approach: a
   consistent read is a no-op log entry (1 consensus round). Optimization
   (later): coordinator lease. `range-serializable` path unaffected.
3. **Leadership-shaped consumers**: map "current coordinator" onto
   `raft-leader?` consumers (get-fast, gr-writer, moveleader→coordinator
   reassignment, metrics role). Coordinator is a *preference*, not a safety
   role — reassignment is just a config write.
4. **Membership**: static config first (our shards are fixed-topology today);
   joint-consensus parity deferred to a follow-up bead.

## Task list (beads, dependency order)

- **Q1 core**: `src/quepaxa.scm` single-slot recorder+proposer, 4-step rounds,
  random ranks, fast path; deterministic sim tests: agreement, validity,
  progress under reorder/dup/loss, fast-path-is-1-RTT assertion.
- **Q2 SMR log**: multi-slot pipeline, commit/apply, batch propose
  (`qp-propose-batch`), no-op barrier, decided-log ≡ raft log shape.
- **Q3 hedging**: tick-driven hedge schedule in the pure engine
  (`qp-tick st` returns hedge entries when due); coordinator preference field.
- **Q4 snapshots**: base/base-term compaction + store-snapshot catch-up parity,
  reusing ws-snap messages.
- **Q5 lin-reads**: consistent read as no-op slot; wire to the existing
  read-ok/pending-read plumbing in shard-actor.
- **Q6 driver seam**: engine dispatch in shard-actor (`engine 'raft|'quepaxa`
  from cluster spec per shard-group); route `('engine from rpc)` by tag prefix;
  coordinator→leader mapping for all leader consumers.
- **Q7 differential**: sim harness driving BOTH engines over identical op/fault
  traces; assert identical applied command sequences.
- **Q8 conformance**: full `test/etcd-*-grpc.sh` suites + watch-scale-bench with
  the quepaxa flag on; wan-soak.sh A/B raft vs quepaxa.
- **Q9 AWS WAN A/B**: 5-node 2-region cluster, forced coordinator flaps +
  induced latency jitter; measure commit p99 + blackout windows vs raft.
- **Q10 bandit tuning** (stretch): online coordinator + hedge-delay selection
  (ε-greedy per shard); optional, after Q9 proves the static win.
- **Q11 membership parity** (deferred): conf-change on the quepaxa path.

Raft stays the default engine throughout; quepaxa is opt-in per shard-group
until Q8/Q9 are green.
