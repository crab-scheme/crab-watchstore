# crab-watchstore

**An etcd v3 API-compatible distributed store written in [CrabScheme](https://github.com/crab-scheme/crabscheme).**

gRPC-native (Protocol Buffers v3), full etcd v3 feature set including Watch streams,
Transactions (Mini-transactions), Auth, Leases, and dynamic cluster membership — all
orchestration written in Scheme.

> **Status: build-out complete.** All 9 phases (KV · Watch · Txn · Lease · Auth/RBAC · gRPC
> wire · dynamic membership · maintenance/health · validation) are implemented, wire-compatible
> with real `etcdctl` / `clientv3`, and **Jepsen-validated linearizable** under partition, kill,
> and live membership churn. See the [exit report](docs/exit-report.md).

## What it is

crab-watchstore is a **showcase for the CrabScheme language**, in the same spirit as
[crab-cache](https://github.com/crab-scheme/crab-cache). The thesis:

> CrabScheme's actors, consensus stack, group-commit store substrate, and native-FFI
> story are expressive enough to implement a **correct, linearizable, wire-compatible**
> etcd v3 store with a small amount of Scheme.

It ports crab-cache's **Jepsen-validated Raft** and **group-commit RocksDB store
substrate** (AS-3 failover, AS-4 crash recovery, linearizable reads via ReadIndex) and
replaces the Redis RESP2 front-end with a gRPC/Protobuf etcd v3 API.

## Two-repo architecture

| This repo — **crab-watchstore** | CrabScheme interpreter repo |
|---|---|
| etcd v3 semantics (KV, Watch, Txn, Auth, Lease) | `cs-grpc` — gRPC + HTTP/2 + Protobuf codec (native) |
| Raft/cluster orchestration (ported from crab-cache) | `cs-store` — RocksDB FFI binding |
| All wire protocol handling in Scheme | `cs-consensus` — Raft core + durable log |

The interpreter must be built with `--features stdlib-store` (enables `cs-store`
and `cs-consensus`).

Minimum crabscheme revision: `db19e5a` (branch `feat/cw-71k-native-unescape`) —
`src/mvcc.scm` uses the native `bytevector-nul-unescape` builtin added there
(cw-71k, G2); older binaries fail with `undefined variable: bytevector-nul-unescape`.

## Build & Run

```sh
# Build the runtime once (in the crabscheme repo):
cargo build -p cs-cli --features stdlib-store,grpc --release
export CRABSCHEME=/path/to/crabscheme/target/release/crabscheme

# Start a single-node cluster (serves the etcd v3 gRPC API on the client port):
$CRABSCHEME run src/node-cluster.scm -- \
  --node a --db /tmp/cws-a --cluster a:127.0.0.1:7001:2379

# Drive it with real etcd tooling:
etcdctl --endpoints=127.0.0.1:2379 put hello world
etcdctl --endpoints=127.0.0.1:2379 get hello
```

## Test

```sh
# Run the smoke test:
$CRABSCHEME run test/smoke.scm

# Run all tests:
bash bin/test.sh
```

## Beads epic

Issue tracking uses [beads](https://github.com/gastownhall/beads) (`bd`). The epic for
this scaffold task is **`cw-u4a`**; run `bd prime` for full workflow context.

## Repository layout

```
crab-watchstore/
├── src/                  # CrabScheme source — all etcd semantics (~7.7k lines)
│   ├── mvcc.scm raft.scm proto.scm auth.scm txn.scm watch.scm …
│   ├── server/           # gRPC KV/Watch handlers, shard actor, metrics HTTP server
│   └── commands/          # command helpers
├── test/                 # 30 Scheme conformance tests + 10 etcd gRPC integration scripts
├── bench/                # vs-etcd.sh head-to-head harness
├── jepsen/               # 5-node Docker linearizability suite (jetcd client)
├── proto/                # .proto definitions (etcd v3 API)
└── docs/                 # exit-report, etcd-compat, jepsen-validation, perf-vs-etcd, ADRs
```

## Relationship to CrabScheme and crab-cache

crab-watchstore runs *on* the [`crabscheme`](https://github.com/crab-scheme/crabscheme)
runtime and depends on the same native modules as crab-cache (`cs-store`, `cs-consensus`).
crab-cache's Raft/store substrate is Jepsen-validated: linearizable under partition/kill
(register workload), and durable under `kill -9` (group-commit WAL fsync). This store
inherits that validated base.

## Documentation

- [**Exit report**](docs/exit-report.md) — epic close-out: what was built, the proofs, the full coverage matrix, the crab-cache reuse manifest
- [etcd v3 compatibility](docs/etcd-compat.md) — per-RPC coverage + proof corpus
- [Jepsen validation](docs/jepsen-validation.md) — linearizability verdict matrix
- [Performance vs etcd](docs/perf-vs-etcd.md) — honest head-to-head
- [ADRs](docs/adr/) — MVCC · Watch · Lease · Auth design decisions

## License

Dual-licensed under MIT or Apache-2.0, matching the CrabScheme project.
