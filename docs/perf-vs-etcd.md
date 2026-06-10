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
| put   | 10.4 | 6.7 | crab-watchstore **faster** |
| get   | 6.0  | 7.1 | etcd faster by ~1 ms |
| range (prefix, 10 keys) | 6.2 | 7.5 | etcd faster by ~1.3 ms |
| txn (1 put) | 11.0 | 7.4 | crab-watchstore **faster** |

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
| `s` (~150 w/s, 50 clients)  | **PASS** · 151 w/s · slowest 0.024 s  | **PASS** · 151 w/s · slowest 0.013 s |
| `m` (~1000 w/s, 200 clients) | **PASS** · 1000 w/s · slowest 0.177 s | **did not sustain** (timed out at 80 s) |
| `l` (~8000 w/s, 500 clients) | **PASS** · 7971 w/s · slowest 0.104 s | _skipped (did not sustain `m`)_ |

**crab-watchstore sustains ~150 writes/s at a tail latency on par with etcd's**
(13 ms vs 24 ms at `load=s` in this run), but **cannot sustain 1000 writes/s**, where etcd is comfortable
all the way to ~8000 writes/s. So on **sustained concurrent write throughput** crab-watchstore
trails by roughly **one order of magnitude** (single-node ceiling between 150 and 1000 w/s vs
etcd's ~8000+), while its **tail latency at a load it can sustain is on par with etcd's**.

## Interpretation

- **Why latency is competitive:** at low concurrency the request is dominated by the gRPC
  round-trip; the server's per-op CPU (proto decode → Raft append → MVCC put → proto encode,
  all in interpreted Scheme on the VM tier) is small next to it. crab-watchstore's
  pure-in-memory single-node Raft append even edges etcd's bbolt write path on `put`/`txn`.
- **Why throughput trails:** under 200+ concurrent clients the per-request interpreter cost
  stops hiding behind the network. Every request runs Scheme on the cooperative green-actor
  runtime (one shard actor serializes the Raft group), so the server saturates at a far lower
  rate than etcd's compiled, batched, multi-core write pipeline. This is the same lesson
  crab-cache surfaced (actor bodies on the VM tier, native fast-paths for the hot reply path) —
  the sustained-throughput lever is a native/batched request path, not the consensus core.
- **The ceiling is the single shard.** crab-watchstore runs one shard actor per node here;
  the Raft group is single-writer by design. Throughput scales with shards/nodes (as crab-cache
  showed), which this single-node, single-shard probe does not exercise.

## What this does and does not claim

- **Does:** the official etcd client and load generator run unmodified against crab-watchstore;
  single-op latency is on par with etcd (writes faster); sustained single-node write throughput
  is ~1 order of magnitude below etcd at a comparable tail latency.
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
