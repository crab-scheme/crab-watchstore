# Jepsen tests for crab-watchstore

Formal correctness testing for crab-watchstore — an **etcd v3-compatible**,
Raft-replicated KV store written in CrabScheme — under fault injection (real network
partitions, process kills, pauses) with consistency verdicts from
[Knossos](https://github.com/jepsen-io/knossos) (linearizability) and
[Elle](https://github.com/jepsen-io/elle) (strict serializability).

The client drives the store over the **etcd v3 gRPC API** using the official
[jetcd](https://github.com/etcd-io/jetcd) Java client — the same wire protocol real
etcd clients use. Adapted from crab-cache's Jepsen harness (which used RESP/carmine);
the consensus-substrate `db`/`nemesis`/`core` scaffolding is shared, the client is
rewritten for etcd gRPC semantics.

> **Status (cw-u4a.34):** the harness + the **no-nemesis register/cas linearizable
> smoke**. The full nemesis matrix + Elle verdict + watch/lease consistency workloads
> are **cw-u4a.35** (the `watch`/`lease` workloads here are clearly-marked stubs).

## What it checks

| Workload (`--workload`) | Ops | Checker | Catches |
|---|---|---|---|
| `register` | `Range` / `Put` over many keys | Knossos linearizable register | stale reads, lost acked writes, split-brain |
| `cas` | `Range` / `Put` / `Txn(value-compare)` | Knossos linearizable **cas-register** | the above, tighter linearization |
| `append` | atomic list-append via guarded `Txn` | Elle (strict-serializable) | dependency-cycle anomalies |
| `watch` | — | — | **stub → cw-u4a.35** |
| `lease` | — | — | **stub → cw-u4a.35** |

There is **no `counter` workload** (etcd has no atomic `INCR`; `cas` and the
`Txn`-based `append` are the analogue), and **no `--shards`** (crab-watchstore is a
single Raft group).

## The client (jetcd over h2c)

`src/jepsen/crabwatchstore/client.clj`:

- **All endpoints + round-robin.** crab-watchstore does **not** proxy writes — a
  *follower* rejects every KV mutation (and leader-gated `Range`) with gRPC
  `UNAVAILABLE(14)` `etcdserver: not leader`. So, exactly as etcd's own clientv3 does
  (and as the cw-u4a.24 clientv3 proof relied on), the jetcd client is given **all
  five** `http://nX:2379` endpoints and `round_robin` balancing, then retries until a
  call lands on the leader. `round_robin` is set explicitly: gRPC's default
  `pick_first` would pin every call to one endpoint and `UNAVAILABLE` forever if it
  were a follower.
- **h2c, not TLS.** The endpoint scheme is `http://` → jetcd selects cleartext
  HTTP/2. An `https://` scheme would start a TLS handshake the h2c server can't answer.
  (The store *can* serve mTLS — `--tls-cert` etc. — but the default node is h2c.)
- **Ops:** read = `KV.get` → first KeyValue's value; write = `KV.put`; cas[old→new] =
  `KV.txn().If(Cmp value EQUAL old).Then(Op.put new).commit()` → `isSucceeded`.
- **Retry policy:** a transient `UNAVAILABLE`/not-leader means the op was rejected
  *before* entering the Raft log (never applied) → bounded retry against another
  endpoint. A client-side timeout or any other error propagates → jepsen `:info`
  (unknown). A clean cas-failure (`isSucceeded=false`) is a definite `:fail`.

## Running with Docker (the supported path on Apple Silicon)

A self-contained, arm64-native cluster: a `control` container (runs `lein` + the
jetcd client) plus `n1`..`n5` DB nodes, all on one compose network resolving each
other by name.

```bash
# 1. Stage the node build context: copies src/ and checks docker/crabscheme.
#    docker/crabscheme must be a LINUX arm64 crabscheme built with
#    --features stdlib-store,grpc — the macOS release binary will NOT run in the
#    Linux node container. Build one in a container (see bin/stage-docker.sh header):
bin/stage-docker.sh

# 2. Bring up control + n1..n5 (builds both images).
docker compose -f docker/docker-compose.yml up -d --build

# 3. Run a test from the control node.
docker compose -f docker/docker-compose.yml exec control \
  lein run test --workload register --nemesis none \
    --nodes n1,n2,n3,n4,n5 --username root \
    --ssh-private-key /root/.ssh/id_jepsen \
    --concurrency 10 --register-group 1 --register-ops 50 --time-limit 30

# soak.sh runs the (cw-u4a.35) matrix and summarizes verdicts.
```

Each DB node serves etcd gRPC on `:2379` (h2c) and Raft on `:7000`; it logs
`node nX: etcd KV gRPC serving on nX:2379 ...` once a leader is known.

## Running on bare hosts

For 5 SSH-reachable Linux hosts (resolvable as `n1`..`n5`) with `iptables`/`pkill`:

```bash
CRABSCHEME=/path/to/linux/crabscheme NODES="n1 n2 n3 n4 n5" SSH_USER=root \
  ./bin/sync-nodes.sh
lein run test --workload register --nemesis none --nodes n1,n2,n3,n4,n5 ...
```

The `db` layer starts `crabscheme run src/node-cluster.scm` from `/opt/crabwatchstore`,
wiping RocksDB MVCC state on `setup!` and each fresh test — but **not** on a nemesis
kill/restart (that's the crash-recovery + Raft-rejoin path under test).

## Options

On top of Jepsen's built-ins (`--nodes`/`--nodes-file`, `--concurrency`,
`--time-limit`, `--test-count`, `--username`, `--ssh-private-key`, ...):

| Option | Default | Meaning |
|---|---|---|
| `--workload` | `register` | `register` / `cas` / `append` / `watch`* / `lease`* |
| `--nemesis` | `partition` | subset of `partition,kill,pause`, or `none` / `all` |
| `--[no-]durable` | `durable` | fsync every write (RocksDB durable mode) |
| `--rate` | `50` | approx requests/sec/thread |
| `--register-group` | `2` | register/cas: worker threads per key |
| `--register-ops` | `100` | register/cas: ops per key |
| `--append-keys` | `8` | append: independent list keys |
| `--append-txn-len` | `4` | append: max micro-ops per transaction |

`*` stub → cw-u4a.35. Results land in `store/<test>/<timestamp>/`; `results.edn` holds
the verdict (`:valid? true|false|:unknown`).

## Findings from cw-u4a.34 bring-up (carry into cw-u4a.35)

- **`grpc-serve` needs a numeric IP, not a hostname.** The Rust h2c transport binds
  its address as a literal `SocketAddr` and does NOT resolve names — unlike Raft's
  `node-listen`, which accepted `n1:7000`. A name-as-host `--cluster` spec made the
  gRPC server fail to bind (`invalid socket address syntax`), the client port never
  opened, and every RPC hung until timeout. `db.clj` therefore resolves each node
  name to its IP for the host field (`node-ip`); the node identity stays the name.
  This is the one server-adjacent quirk found; it is fully worked around in the
  harness (the spec is the harness's responsibility) — not a correctness bug.
- **This is the FIRST multi-node gRPC exercise of the store.** Every existing proof
  (`test/etcd-kv-grpc.sh`, the `clientv3-compat` capstone) is single-node. The
  multi-node write path works: the leader commits a Put through Raft (~100ms),
  followers return `UNAVAILABLE: etcdserver: not leader` promptly, and jetcd's
  `round_robin` + `with-retry` lands on the leader.
- **Leader-finding via round_robin + app-retry; do NOT use jetcd's `waitForReady
  false` / a high `retryMaxAttempts`.** Both were tried and REGRESSED (2 `:ok` / 30
  `:info`) — they fail RPCs before the picker cycles to the leader. jetcd's defaults
  plus the `with-retry` re-issue loop are what work (register: ~10 `:ok` / ~4 `:info`).
- **`cas` reaches `:valid? true` only on keys with no `:info` op.** A handful of ops
  per run still exhaust leader-finding (likely first-RPC subchannel warm-up) and
  return `:info`; under the strict `cas-register` model each `:info` leaves that key
  `:unknown` → overall `:valid? false` with **empty `:failures`** (no counterexample).
  This is the same Knossos-under-load measurement limit crab-cache documented, NOT a
  violation — `register` (the simpler `register` model) tolerates the `:info` and is
  the clean proof. cw-u4a.35 should prime connections in `open!` and/or lower
  concurrency to drive `:info` to zero for a conclusive `cas`/Elle verdict.

## Layout

```
project.clj                          deps: jepsen 0.3.11, io.etcd/jetcd-core 0.8.3
bin/stage-docker.sh                  stage docker/ build context (src + binary check)
bin/sync-nodes.sh                    provision binary + src onto bare DB nodes
bin/soak.sh                          run the cw-u4a.35 matrix, summarize verdicts
src/jepsen/crabwatchstore/
  core.clj                           test map + CLI + nemesis wiring
  db.clj                             start/stop/kill/pause/logs for a node-cluster proc
  client.clj                         jetcd etcd-v3 client (h2c, round-robin, Txn-CAS)
  register.clj                       linearizable read/write register
  cas.clj                            linearizable cas-register (Txn value-compare)
  append.clj                         Elle list-append (guarded multi-key Txn RMW)
  watch.clj                          STUB → cw-u4a.35
  lease.clj                          STUB → cw-u4a.35
docker/                              self-contained 5-node arm64 cluster
```
