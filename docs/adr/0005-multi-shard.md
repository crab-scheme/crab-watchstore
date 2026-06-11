# ADR 0005 — Multi-shard keyspace: design note (cw-b5w.4)

Status: PROPOSED (awaiting review — cw-b5w.4 requires this note reviewed before code)
Date: 2026-06-11

## Problem

After cw-b5w.2 (native codec) + cw-b5w.3 (group commit), a single node sustains
~2,400–2,700 w/s at load=l pressure with PASSING tail latency; etcd does ~8,000 w/s.
The remaining ceiling is the **single shard actor**: every write serializes through
one VM-tier actor that runs decode-dispatch, Raft propose (state-alist rebuild +
log append per entry), apply (MVCC encode + RocksDB batch), and watch fan-out.
Profiling under load=l (`/tmp/cws-profile-l`, sample2) shows the process is not
I/O-bound — on-CPU time is VM dispatch + alloc churn inside that one actor, with
`b_append`/alist rebuilds (the pure-functional Raft state) newly visible.

cw-b5w.4 proposes N shard actors / N Raft groups per node, crab-cache-style
(CRC16 multi-Raft). This note works through what that does to **etcd semantics**
before any code, as the bead requires.

## The semantic wall: etcd has ONE revision domain

Everything hard about sharding this store reduces to one fact: the etcd v3 API
exposes a single, totally-ordered, gapless-per-write revision sequence, and its
clients depend on it in ways Redis clients never depended on anything:

1. **`header.revision`** must be monotonically non-decreasing across ALL responses
   a client sees, regardless of key.
2. **Watch** carries `start_revision` + guarantees in-revision-order, no-gap
   delivery per watcher — including a watcher on a prefix spanning what would be
   multiple shards. Progress notifications + compaction errors are revision-based.
3. **Range at revision=R** must be a consistent snapshot across keys (a multi-key
   range crossing shards at one R).
4. **Txn** compares + mutates multiple keys atomically at one revision.
5. **Compaction** is "compact everything < R" — one R for the whole store.

Per-shard independent revisions break all five for any client that touches more
than one shard. jetcd/clientv3 *will* notice (the Jepsen suite's register/append
checkers literally assert these).

## Options considered

### A. Independent per-shard revisions (crab-cache model) — REJECTED
Each shard its own Raft group + its own revision counter; `header.revision` is
the owning shard's. Fastest (zero cross-shard coordination) but visibly violates
1/2/3/4/5 above. crab-cache got away with it because Redis has no revision
concept. An "etcd-compatible" store that fails `watch --prefix` ordering is not
etcd-compatible. Rejected outright.

### B. Global revision sequencer + sharded apply — VIABLE, the real contender
Keep ONE Raft group (one log, one revision domain) exactly as today — but split
the **apply + serving** path by key-hash across N worker actors:

- The shard actor stays the single sequencer: it batches proposals (cw-b5w.3),
  assigns revisions, appends to the one log. This is cheap — the profile shows
  decode/apply/MVCC dominates, not the propose bookkeeping itself.
- Committed entries are handed to N **apply workers** by key-hash; each owns its
  RocksDB column-family slice and its slice of the watch registry. Revisions are
  already assigned, so workers can apply *in parallel* as long as per-key order
  holds (hash partitioning gives that for free) and a revision watermark
  (`min(applied)` across workers) gates reads + watch emission so nothing is
  served above a gap.
- Range/Txn/watch semantics are untouched: one revision domain, snapshots at R
  remain meaningful, a prefix watch merges worker streams by revision (workers
  emit in revision order; the merge is a k-way pick-min on a small k).

This attacks the measured bottleneck (per-write CPU in one actor) without
breaking any etcd invariant. It is also strictly less risky: raft.scm and the
revision model stay untouched; the change is shard-actor-internal (a worker pool
+ a watermark), the same shape as crab-cache's conn/shard split.

### C. N Raft groups + a revision-allocation group — REJECTED for v1
True multi-Raft with a separate sequencer group allocating revision blocks.
Restores write parallelism INCLUDING the log/fsync path, but: cross-shard Txn
needs 2PC across groups, watch needs a cross-group revision merge with holes
(allocated-but-unused revisions), compaction needs a coordinator, and failover
semantics multiply (N elections). This is etcd→TiKV-grade complexity for a gap
that B likely closes most of. Defer until B's ceiling is measured.

### D. Do nothing / micro-optimize the single actor — fallback
The profile still shows alist-rebuild + log-append costs (`b_append`) inside
propose, and the apply path deep-clones values. A vector-backed raft state or
mutable log buffer is a few hours' work for maybe +20–40%. Worth doing
opportunistically inside B, not as the headline.

## Recommendation

**Option B** — one Raft group / one revision domain (semantics intact), N
hash-partitioned apply+serve workers behind a revision watermark, plus the D
micro-fixes where they fall out naturally. Concretely:

1. Extract apply (`mvcc-apply`) + per-key watch fan-out into `apply-worker.scm`,
   N instances, key-hash routed, each with its own RocksDB CF set.
2. Shard actor: assign revision at commit (already true), dispatch batch slices
   to workers, track `applied-watermark = min(worker applied)`.
3. Reads (Range/ReadIndex gate) wait on watermark ≥ ReadIndex; watch emission
   merges worker streams by revision (k-way, k=N).
4. Cross-shard Txn: execute on the sequencer by claiming all touched workers'
   slices for that revision (workers drain to the Txn's revision, then the Txn
   applies serially). Correct, simple; Txns are not the hot path.
5. N default = 4 (matches the ~3× gap; tune by ladder).

Acceptance: ladder load=l; all etcdctl suites + watch/lease contract tests;
the Jepsen register/append/watch workloads are the final referee (they encode
exactly the invariants this design must preserve).

## Open questions for review

- Is the watch k-way merge acceptable in Scheme for v1, or should watch fan-out
  stay on the sequencer (workers apply, sequencer emits watch events from the
  committed batch it already has in hand — simpler, slightly more sequencer CPU)?
- Worker count N: fixed config vs `--shards N` CLI (interacts with cw-24e.1's
  config file).
- Does RocksDB CF-per-worker pay for itself vs shared CF + key-prefix? (CF gives
  per-worker flush/iterator isolation; prefix is zero-migration.)
