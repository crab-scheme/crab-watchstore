# crab-watchstore

**An etcd v3 API-compatible distributed store written in [CrabScheme](https://github.com/crab-scheme/crabscheme).**

gRPC-native (Protocol Buffers v3), full etcd v3 feature set including Watch streams,
Transactions (Mini-transactions), Auth, Leases, and dynamic cluster membership — all
orchestration written in Scheme.

> **Status: Phase 0 — scaffolding.** Build/run/test harness in place. No etcd logic yet.

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

## Build & Run

```sh
# Build the runtime once (in the crabscheme repo):
cargo build -p cs-cli --features stdlib-store --release
export CRABSCHEME=/path/to/crabscheme/target/release/crabscheme

# Start a node (skeleton — prints banner and exits):
bash bin/run.sh
# or:
$CRABSCHEME run src/node-watchstore.scm -- --port 2379
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
├── src/                  # CrabScheme source (node entry, future: server/, commands/)
│   ├── server/           # connection handlers (future)
│   └── commands/         # KV/Watch/Txn/Auth/Lease handlers (future)
├── test/                 # Scheme conformance tests + harness
├── bench/                # benchmark harness (future)
├── proto/                # .proto definitions (etcd v3 API)
└── docs/                 # measurements + design docs (future)
```

## Relationship to CrabScheme and crab-cache

crab-watchstore runs *on* the [`crabscheme`](https://github.com/crab-scheme/crabscheme)
runtime and depends on the same native modules as crab-cache (`cs-store`, `cs-consensus`).
crab-cache's Raft/store substrate is Jepsen-validated: linearizable under partition/kill
(register workload), and durable under `kill -9` (group-commit WAL fsync). This store
inherits that validated base.

## License

Dual-licensed under MIT or Apache-2.0, matching the CrabScheme project.
