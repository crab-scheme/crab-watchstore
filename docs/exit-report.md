# crab-watchstore — epic `cw-u4a` exit report

**An etcd v3 API-compatible, Raft-replicated, linearizable distributed key-value store
written in [CrabScheme](https://github.com/crab-scheme/crabscheme).** This is the close-out
for epic `cw-u4a`: what was built, the evidence it works, the coverage matrix, the consistency
model, what was reused from [crab-cache](https://github.com/crab-scheme/crab-cache), and the
known limitations.

> **Status: build-out complete.** All 37 build tasks across 9 phases (Phase 0 scaffold →
> Phase 9 validation) are done and closed. Five P2/P3 follow-ups remain tracked (see
> [Limitations](#limitations--deferred-follow-ups)); none block the feature set or correctness.

## The thesis, and the result

> CrabScheme's actors, consensus stack, group-commit store substrate, and native-FFI story are
> expressive enough to implement a **correct, linearizable, wire-compatible** etcd v3 store with
> a small amount of Scheme.

**Confirmed.** crab-watchstore is **~7,700 lines of Scheme** implementing the entire etcd v3
data plane, Watch, Lease, Txn, Auth/RBAC, dynamic membership, maintenance, and the gRPC/protobuf
wire — on top of a **Jepsen-hardened Raft core reused verbatim** from crab-cache. Three
independent proofs back the claim:

| Proof | Result | Where |
|---|---|---|
| **Wire compatibility** | Real `etcdctl` 3.6.12 **and** the `go.etcd.io/etcd/client/v3` Go library drive the store unmodified across KV/Txn/Watch/Lease/Auth/Cluster/Maintenance | [etcd-compat.md](etcd-compat.md), `test/etcd-*.sh`, `test/clientv3-compat/` |
| **Correctness** | **Jepsen CLEAN BILL** — strict-serializable (Elle append), register/CAS linearizable under partition, process kill, **and live membership reconfiguration**; 0 counterexamples across 24 results sets | [jepsen-validation.md](jepsen-validation.md) |
| **Performance** | Honest head-to-head vs real etcd via the **same `etcdctl`**: single-op latency on par (faster on writes); sustained write throughput ~1 order of magnitude behind | [perf-vs-etcd.md](perf-vs-etcd.md) |

## Phases delivered

| Phase | Scope | Key deliverables |
|---|---|---|
| **0** Substrate | scaffold + consensus base | raft.scm reused verbatim, durable-KV FFI, 3-voter sim + real-TCP cluster |
| **1** MVCC | etcd's data identity | `src/mvcc.scm` — global revision, per-key `{create,mod,version,value,lease}`, revision history, tombstones, read-at-revision, compaction; ADR-0001 |
| **2** KV + Txn | the core API | Range/Put/DeleteRange/Txn/Compact; mini-transactions (compare/success/failure); `src/txn.scm` |
| **3** Watch | revision-anchored streams | `src/watch.scm` — live + prefix + historical replay (buffered-before-CREATED); ADR-0002 |
| **4** Lease | TTL + attachment | grant/revoke/keepalive/ttl/leases; leader-driven expiry proposed through Raft; ADR-0003 |
| **5** gRPC wire | **the core goal** | pure-Scheme proto3 codec (`src/proto.scm`); Rust h2c+trailers transport (crabscheme `feat/grpc-h2c-transport`); mTLS + peer-identity; **proven with etcdctl AND clientv3** |
| **6** Auth/RBAC | etcd security | `src/auth.scm` — Argon2id, users/roles, range-containment authz, token + cert-CN; ADR-0004 |
| **7** Membership | **the hard feature crab-cache skipped** | joint-consensus Raft (ConfChange, joint quorum, learners) + the Cluster gRPC service; add/remove/promote a voter on a **live** cluster |
| **8** Maintenance + health | operability | Status / Hash / HashKV / Snapshot / Defragment / Alarm; `grpc.health.v1.Health`; HTTP `/health` `/version` `/metrics` |
| **9** Validation | the proof | etcdctl + clientv3 compat, Jepsen (incl. membership nemesis), perf-vs-etcd, this report |

## etcd v3 API coverage matrix

Legend: **✓ Supported** · **◑ Partial** (works for the common path, caveat noted) · **✗ Not implemented** (returns a clean gRPC status, never a TCP reset).

### `etcdserverpb.KV`
| RPC | | Notes |
|---|---|---|
| `Range` | ✓ | prefix, range, limit+more, historical `WithRev`, keys-only, count-only, sort; **linearizable** (leader-gated) |
| `Put` | ✓ | lease attach, prev-kv |
| `DeleteRange` | ✓ | prefix, prev-kv |
| `Txn` | ✓ | compare value/version/create/mod/lease; success + failure branches; multi-op |
| `Compact` | ✓ | `ErrCompacted` (`OUT_OF_RANGE` 11) below floor |

### `etcdserverpb.Watch`
| RPC | | Notes |
|---|---|---|
| `Watch` (bidi) | ✓ | created-ack; in-order live PUT/DELETE; prefix; historical replay; cancel; teardown on half-close. Jepsen: exactly-once / in-order / no-gap |

### `etcdserverpb.Lease`
| RPC | | Notes |
|---|---|---|
| `LeaseGrant` | ◑ | works; on a **follower** returns `INTERNAL` instead of `UNAVAILABLE`-not-leader (bug `cw-u4a.43`) |
| `LeaseRevoke` / `LeaseTimeToLive` / `LeaseLeases` | ✓ | atomic attached-key delete; TTL + attached keys; active-lease list |
| `LeaseKeepAlive` (bidi) | ◑ | works on the leader; same follower-routing caveat as grant |

### `etcdserverpb.Auth` — **all ✓** (Phase 6)
`AuthEnable`/`AuthDisable`/`Authenticate`; `UserAdd`/`Get`/`List`/`Delete`/`ChangePassword`/`GrantRole`/`RevokeRole`; `RoleAdd`/`Get`/`List`/`Delete`/`GrantPermission`/`RevokePermission`. Argon2id hashing, range-containment authorization, token + client-cert-CN identity. Proof: `test/etcd-auth-grpc.sh`, `test/etcd-auth-mgmt-grpc.sh`.

### `etcdserverpb.Cluster` — Phase 7 (joint-consensus)
| RPC | | Notes |
|---|---|---|
| `MemberList` | ✓ | real config view; uint64 ID↔name bijection (FNV-1a == node stable-id) |
| `MemberAdd` (voter / learner) | ✓ | two-phase joint change; learner = single-phase |
| `MemberRemove` / `MemberPromote` | ✓ | remove (incl. the leader) / promote learner→voter, all on a **live** cluster |
| `MemberUpdate` | ◑ | no-op — peer addresses are static in this deployment model |

### `etcdserverpb.Maintenance`
| RPC | | Notes |
|---|---|---|
| `Status` | ✓ | real raft scalars (term/index/applied/leader), db size, key count |
| `Hash` / `HashKV` | ✓ | deterministic FNV-1a-32 over canonical scan order — **identical across members** |
| `Defragment` | ✓ | advisory store flush |
| `Snapshot` | ◑ | logical-keyspace stream, 512-aligned + sha256 trailer so `etcdctl snapshot save` downloads & verifies; restore is **native, not bbolt** |
| `Alarm` / `AlarmList` / `AlarmDisarm` | ◑ | leader-local, not yet Raft-replicated (`cw-u4a.42`) |
| `MoveLeader` | ✗ | the Raft engine has no `TimeoutNow` leadership-transfer primitive (`cw-u4a.42`) |
| `Downgrade` | ✗ | version downgrade protocol not modeled |

### `grpc.health.v1.Health` + HTTP — **all ✓** (Phase 8)
`Health/Check` + `Health/Watch` on the client port; a hand-rolled HTTP/1.1 server on a dedicated metrics port serving `/health`, `/version` (`3.6.0`), and Prometheus `/metrics` (has-leader, is-leader, raft term/index/applied, mvcc revision/db-size/keys). Proof: `test/health-metrics.sh`.

## Consistency model

| Property | Guarantee |
|---|---|
| Reads | **Linearizable** — `Range` is leader-gated; the inherited Raft core provides ReadIndex/CheckQuorum/PreVote so no stale or split-brain reads |
| Writes | **Total order** — every mutation passes through a single Raft log; exactly-once across leader stepdown (crab-cache's `cc-cri` fix, inherited) |
| Watch | Revision-anchored, strictly increasing; historical replay before live with no gap |
| Lease expiry | Leader-driven deadline, revocation **proposed through Raft** → consistent across the cluster |
| Membership | Joint-consensus (Ongaro §4.3) — a config change needs a majority of **both** the old and new voter sets; no split-brain across a reconfiguration |

**Jepsen verdict (`cw-u4a.35`):** register / CAS linearizable and append **strict-serializable** under no-fault, network partition, process kill, **and live membership churn**. 0 counterexamples; every "overall false" in the raw results is a Knossos/Elle measurement limit (`:info`-rate, empty transaction graph), not a safety violation.

## crab-cache reuse manifest

The point of the two-store strategy is leverage: crab-watchstore proves the substrate is reusable
across very different systems (a Redis cache vs an etcd store).

**Reused verbatim / near-verbatim from crab-cache:**
- **`src/raft.scm`** — the Jepsen-validated Raft core (election, replication, commit, ReadIndex,
  CheckQuorum, PreVote, §5.4.2 no-op, snapshots), including the `cc-idc` read-path and `cc-cri`
  exactly-once-on-stepdown fixes. Extended **only additively** with joint-consensus membership
  (Phase 7) — a feature crab-cache never needed.
- The **group-commit RocksDB store substrate** (durable WAL, crash recovery), the **spawn-source
  green-actor** model, and the **`node-cluster.scm` full-mesh** bootstrap.
- The **test harness** machinery and the **Jepsen** scaffolding (ported, with the RESP/carmine
  client replaced by a jetcd etcd-v3 gRPC client).

**Net-new for crab-watchstore (the etcd-specific work):**
- **MVCC** (`src/mvcc.scm`, ~1,165 lines) — etcd's identity; the single largest new component.
- **The entire gRPC/protobuf wire** — a pure-Scheme proto3 codec (`src/proto.scm`, ~953 lines)
  plus a Rust h2c+trailers transport bridge in the crabscheme runtime
  (`feat/grpc-h2c-transport`: `cs-web/src/grpc.rs` + `cs-runtime/src/builtins/grpc.rs`), the only
  net-new Rust, registered on both the walker and VM tiers.
- **Watch source, Lease, Txn translation, Auth/RBAC, dynamic-membership wiring** (shard-actor
  mailbox + Cluster gRPC service), and the **maintenance/health** surface.

> **The 7,700 lines of Scheme contain 100% of the etcd semantics.** The only net-new Rust is the
> generic gRPC transport (shared interpreter infrastructure, not store logic) — the same
> native-FFI-at-the-edges pattern crab-cache established.

## CrabScheme as a language stress-test — findings fed back

Building a second, very different system surfaced real interpreter work, filed against crabscheme:
- **`cw-u4a.41`** — bitwise/shift operations are not bignum-aware (wrap at i64); the protobuf
  varint codec worked around it with bignum arithmetic.
- The gRPC h2c transport (h2/HPACK/trailers) was implemented in Rust by design — too large for
  Scheme and it touches the interpreter — landing the green-actor-compatible primops on **both**
  evaluator tiers.
- The cooperative green-actor runtime sustains concurrent gRPC streams correctly under Jepsen
  load (membership, watch, lease workloads) — the consensus + transport stack is sound; the
  throughput ceiling is the interpreted request path, not the actor model.

## Test corpus

- **10 etcd gRPC integration scripts** (`test/etcd-*.sh`, `test/health-metrics.sh`) driving the
  store with **real `etcdctl` 3.6.12** + a Go `clientv3` program: KV, Watch, Lease, mTLS, Auth,
  Auth-mgmt, Cluster, Maintenance, Health/metrics, full clientv3 compat.
- **30 Scheme conformance tests** (`test/*.scm`) covering MVCC (encoding/range/txn/compact/edge/
  integration), Watch, Lease, Txn, Raft (CQ/PreVote, compaction-backoff, **membership** 73/0),
  membership over the live cluster (cluster 36/0, load 40/0), and sim-cluster.
- **Jepsen** (`jepsen/`) — 5-node arm64 Docker, jetcd client, register/CAS/append/watch/lease/
  membership workloads under partition/kill/pause/reconfiguration nemeses.

## ADR index

| ADR | Subject |
|---|---|
| [0001](adr/0001-mvcc-data-model.md) | MVCC data model — revision counter, per-key history, RocksDB key encoding, read-at-revision, compaction |
| [0002](adr/0002-watch.md) | Watch subsystem — revision-anchored events, historical replay, buffered-before-CREATED ordering |
| [0003](adr/0003-lease.md) | Lease subsystem — TTL replication, leader-driven expiry through Raft, key attachment |
| [0004](adr/0004-auth.md) | Auth / RBAC model — NS-AUTH namespace, Argon2id, range-containment authorization |

## Limitations & deferred follow-ups

All tracked as open beads under `cw-u4a`; none block the feature set or the correctness proofs.

| Bead | Pri | Item |
|---|---|---|
| `cw-u4a.40` | P2 | Don't bump the global revision on zero-effect ops (exact etcd parity) |
| `cw-u4a.38` | P3 | `kv-seek`: O(log n) RocksDB seek for latest-≤-readRev (ADR-0001 optional perf) |
| `cw-u4a.41` | P3 | crabscheme: bitwise/shift not bignum-aware (i64 wrap) — interpreter fix |
| `cw-u4a.42` | P3 | Maintenance faithfulness: real `MoveLeader` (engine `TimeoutNow`) + Raft-replicated alarms |
| `cw-u4a.43` | P3 | Lease grant/keepalive on a follower returns `INTERNAL` instead of `UNAVAILABLE`-not-leader |

**Not measured / out of scope here:** durable-write parity on a real fsync barrier (macOS fsync
isn't one — Linux run is future work), multi-shard / multi-node throughput scaling, large-value
and read-heavy perf mixes, and a saturating persistent-connection throughput benchmark.

## Landing status

All crab-watchstore commits are **local on `master`** (this repo has no remote configured). The
crabscheme-side gRPC transport lives on the local `feat/grpc-h2c-transport` branch (with the
release binary rebuilt `--features stdlib-store,grpc`). Landing/publishing is pending explicit
go-ahead; the conservative beads profile is in effect (local commits + bead close only).
