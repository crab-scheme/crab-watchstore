# Performance — crab-watchstore vs etcd

A single-node performance head-to-head between **crab-watchstore** — an etcd v3-compatible,
Raft-replicated KV store written in CrabScheme — and **real etcd 3.6**, driven by the
**same official `etcdctl` client** against both. Harness: [`bench/vs-etcd.sh`](../bench/vs-etcd.sh).

**Tracking:** bead `cw-u4a.36` (Phase 9 of epic `cw-u4a`). Builds on the `cw-u4a.24`
etcd-client compatibility smoke (real `go.etcd.io/etcd/client/v3` against crab-watchstore).

> **The honest framing up front.** crab-watchstore is an *interpreted-Scheme* store running on
> a cooperative green-actor runtime; etcd is Go + bbolt with a decade of tuning. We expect
> crab-watchstore to **trail** on raw throughput, and it does — by roughly an order of
> magnitude on sustained concurrent writes. The deliverable of this project is **correctness**
> (Jepsen strict-serializable under partition/kill/membership churn — see
> [jepsen-validation.md](jepsen-validation.md)), **full etcd-v3 feature parity**, and
> **distribution at a competitive — not necessarily winning — speed**. The numbers below are
> reported without spin.

## The measurement is itself a result

crab-cache (the prior CrabScheme store, RESP wire) could only ever cite etcd as an
*order-of-magnitude reference*: no single load tool spoke both RESP and gRPC, so the
comparison was apples-to-oranges (see `crab-cache/bench/vs-etcd.sh`).

crab-watchstore speaks the **etcd v3 gRPC API**, so the *identical* official tooling —
`etcdctl` for single ops and `etcdctl check perf` (etcd's own load generator) — drives
**both** servers, on the same host, with the same load profiles. The fact that the real
etcd client and its benchmark run **unmodified** against a store written in CrabScheme is the
first headline. Every number here is the same wire protocol, same client, same machine.

## Host & method

- Host: `Darwin 25.2.0 arm64`, 10 cores; 256-byte values; single node.
- etcd `3.6.12`; driver `etcdctl 3.6.12`; latency via `hyperfine` (20 runs/op).
- **Relaxed regime** (`DURABLE=no`): the meaningful same-host number. macOS `fsync()` is not a
  true durability barrier (only `F_FULLFSYNC` is), so a per-write-fsync ("durable") row on
  darwin is ~free for **both** servers and would mislead. These results isolate **store +
  transport** cost, not disk. Run `DURABLE=yes` on Linux for a real durable-write comparison.

## Single-op round-trip latency

Each `etcdctl` invocation is a cold gRPC dial + one RPC. That dial cost is **identical** for
both servers, so the per-op delta is the server's own work. Mean of 20 runs, milliseconds:

| op | etcd (ms) | crab-watchstore (ms) | |
|---|---|---|---|
| put   | 12.2 | 7.6 | crab-watchstore **faster** |
| get   | 7.8  | 7.6 | parity |
| range (prefix, 10 keys) | 7.1 | 8.2 | etcd faster by ~1 ms |
| txn (1 put) | 11.8 | 8.7 | crab-watchstore **faster** |

_(2026-06-11 re-run after the cw-b5w perf epic; the original cw-u4a.36 numbers were put
10.4/6.7, get 6.0/7.1, range 6.2/7.5, txn 11.0/7.4 — same shape, `get` moved to parity
after the .38 O(log n) point-read seek + the native codec.)_

**crab-watchstore is competitive on single-op latency — and faster on writes.** At one
operation at a time the cold-dial cost dominates, so the interpreted server's per-op overhead
barely shows; etcd's writes also carry slightly more machinery (proposal → bbolt commit) than
crab-watchstore's in-memory single-node Raft append, which is why `put`/`txn` favor
crab-watchstore here.

## Sustained throughput — `etcdctl check perf` ladder

`etcdctl check perf` is etcd's own **rate-limited health probe**: it drives a fixed target
write rate for 60 s with N concurrent clients and **PASS/FAIL**s on whether the cluster
sustained it at acceptable tail latency. So a PASS means *"sustained the target rate,"* not
*"max throughput."* We climb the ladder until each server can no longer sustain a level:

| load (target writes/s, clients) | etcd | crab-watchstore |
|---|---|---|
| `s` (~150 w/s, 50 clients)  | **PASS** · 150 w/s  | **PASS** · 151 w/s |
| `m` (~1000 w/s, 200 clients) | **PASS** · 1000 w/s · slowest 0.063 s | **PASS** · 1000 w/s |
| `l` (~8000 w/s, 500 clients) | **PASS** · 7935 w/s · slowest 0.075 s | **FAIL** — sustains **~1,200 w/s** (see correction) |

**2026-06-11, after the cw-b5w perf epic** (the original cw-u4a.36 run did not sustain
`load=m`; see history below). crab-watchstore now **PASSes `load=m`** — measured solo it
holds 1000 w/s with a **37 ms** worst case (etcd's same-day solo tail: ~40–63 ms) — and at
`load=l` pressure it sustains **~2,700 w/s** against the 8,000 target (`bench/ladder-cws.sh`,
three runs 1.9k–2.7k). The sustained-write gap to etcd is **~6–7×**, down from ~8×+ pre-epic
(see the cw-b5w.7 correction below — the interim ~2.7k/~3× figure was inflated by a
broken Raft tick clock). Tail-latency numbers in the combined back-to-back run above are noisier than the
solo measurements (both servers + population phases share the box); the solo ladder + the
profiling doc (`docs/perf-profile-load-m.md`) carry the clean per-level tails.

What moved it (the cw-b5w epic):
1. **cw-u4a.38** — O(log n) RocksDB point reads (`store-seek`): `load=m` went from
   not-sustaining to PASS before this epic even started measuring.
2. **cw-b5w.2** — native etcdserverpb codec builtins for Put/Range (the interpreted
   per-byte proto codec was the #1 on-CPU cost): `load=l` pressure 1.9k w/s.
3. **cw-b5w.3** — group commit (one AE/commit/settle round per mailbox batch of up to 64
   proposals): 2.7k w/s and the `load=m` tail collapsed 124 ms → 37 ms.
4. **cw-b5w.4** — parallel apply workers (ADR 0005 B) shipped + proven correct, but
   regressed `load=l` on darwin-arm64 (barrier round-trips + the SingleThreaded-RocksDB
   mutex), so the default stays `--shards 1`; retune issue open.

**Correction (cw-b5w.7):** every number measured between cw-b5w.3 and the tick fix ran
on a broken Raft tick clock (idle-iteration-paced; ~700–1000 ticks/s instead of 8/s),
which churned multi-node elections AND acted as an accidental commit pump on the ladder.
With wall-clock ticks (the fix): `load=m` still PASSES (989 w/s, slowest 0.42 s) and
`load=l` sustains **~1,180 w/s** — the honest post-epic single-node ceiling. Batch-cap
sweep: 64 ≫ 8 (454 w/s), so group commit itself is real and kept; the extra ~1.5k w/s
under the flood was the artifact. Idle 3-node clusters now hold term 1 indefinitely
(previously +2–4 terms/s).

**Update (cw-b5w.6):** the parallel-apply investigation found the real lock: the
crabscheme store registry held its Mutex across every RocksDB operation
(SingleThreaded mode), serializing ALL store access process-wide. With
MultiThreaded RocksDB + Arc handles (ops run unlocked), the SERIAL path jumped
`load=l` **~1,180 → 5,202 w/s** (slowest 0.15 s) — the sustained-write gap to
etcd (~7,900 w/s) is now **~1.5×**. `--shards 4` measures 3,606 w/s (the
per-batch barrier still costs more than parallel apply saves), so the default
remains `--shards 1`. Full suite battery + the cw-24e soak (zero lost acks
under leader kill + membership cycle) green on the new store layer.

Historical (cw-u4a.36, pre-epic): `s` PASS (slowest 13 ms vs etcd 24 ms), `m` did not
sustain, `l` skipped — i.e. a single-node ceiling between 150 and 1000 w/s.

## Interpretation

- **Why latency is competitive:** at low concurrency the request is dominated by the gRPC
  round-trip; the server's per-op CPU (proto decode → Raft append → MVCC put → proto encode,
  all in interpreted Scheme on the VM tier) is small next to it. crab-watchstore's
  pure-in-memory single-node Raft append even edges etcd's bbolt write path on `put`/`txn`.
- **Why throughput trails (and by how much now):** under 200+ concurrent clients the
  per-request interpreter cost stops hiding behind the network. The cw-b5w epic removed the
  two biggest serial costs — the interpreted protobuf codec (native builtins, cw-b5w.2) and
  the per-proposal Raft round (group commit, cw-b5w.3) — taking the gap from ~8×+ to ~3×.
  What remains is the single shard actor's per-write VM execution (Raft state alist rebuilds,
  MVCC bookkeeping) — see `docs/perf-profile-load-m.md` for the ranked profile.
- **The ceiling is still the single sequencer.** Parallel apply workers (cw-b5w.4 /
  ADR 0005 option B) are implemented and correctness-proven but currently net-negative on
  darwin-arm64 (the process-global SingleThreaded RocksDB mutex re-serializes worker writes);
  they stay behind `--shards` pending a retune. True multi-Raft was rejected for v1 — it
  breaks etcd's single-revision-domain semantics (ADR 0005 §A/§C).

## What this does and does not claim

- **Does:** the official etcd client and load generator run unmodified against crab-watchstore;
  single-op latency is on par with etcd (writes faster, reads at parity); crab-watchstore
  sustains etcd's own `load=m` health bar (1000 w/s) with a better solo tail, and trails
  etcd ~3× at `load=l` (2.7k vs 7.9k w/s).
- **Does not:** claim durable-write parity (not measured on a real fsync barrier — Linux TODO),
  multi-shard/multi-node scaling, large-value or read-heavy mixes, or max-throughput via a
  persistent-connection benchmark (check perf is rate-limited; a saturating gRPC bench is future
  work). Numbers are single-host and inherently noisy (±~1 ms latency, ±a few % throughput).

## Reproduce

```bash
# needs etcd + etcdctl + hyperfine on PATH, and a crabscheme built --features stdlib-store,grpc
CRABSCHEME=/path/to/crabscheme bash bench/vs-etcd.sh            # default: LOADS="s m l", relaxed
LOADS="s m" DURABLE=yes RUNS=40 bash bench/vs-etcd.sh           # Linux durable-write comparison
```

The harness brings up a fresh single-node crab-watchstore and a fresh single-member etcd on
unique ports, seeds a key, runs the hyperfine latency sweep, then the check-perf ladder
(stopping crab-watchstore's climb at the first level it cannot sustain), and tears both down.
