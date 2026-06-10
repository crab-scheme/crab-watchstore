# etcd v3 API Compatibility — crab-watchstore

This document records the etcd v3 gRPC API surface supported by crab-watchstore,
the evidence that proves each section, and the consistency guarantees.

## Proof corpus

| Proof | What it exercises | Status |
|-------|-------------------|--------|
| `test/etcd-kv-grpc.sh` | etcdctl CLI — KV.Put/Range/Del/Txn/Compact + Maintenance/Status | ALL PASS (25 assertions) |
| `test/etcd-watch-lease-grpc.sh` | etcdctl CLI — Watch (live/prefix/historical) + Lease (Grant/Put/TTL/KeepAlive/List/Revoke) | ALL PASS (18 assertions) |
| `test/etcd-mtls-grpc.sh` | etcdctl over mutual TLS — same KV surface, peer-identity logged | ALL PASS (7 assertions) |
| `test/etcd-auth-grpc.sh` | etcdctl CLI — Auth enable/Authenticate + RBAC enforcement | ALL PASS (22 assertions) |
| `test/etcd-auth-mgmt-grpc.sh` | etcdctl CLI — User/Role management RPCs | ALL PASS (21 assertions) |
| `test/etcd-cluster-grpc.sh` | etcdctl CLI — Cluster Member{List,Add,Remove,Promote} on a live cluster | ALL PASS (23 assertions) |
| `test/etcd-maintenance-grpc.sh` | etcdctl CLI — Status/Hash/HashKV/Defragment/Snapshot/Alarm | ALL PASS (25 assertions) |
| `test/health-metrics.sh` | grpc.health.v1.Health + HTTP /health, /version, /metrics | ALL PASS (24 assertions) |
| `test/clientv3-compat/main.go` | **go.etcd.io/etcd/client/v3 library (v3.5.17)** — KV/Txn/Watch/Lease/Compact via the actual Go etcd client | ALL PASS (see matrix below) |

The clientv3 program (`test/clientv3-compat`) is the Phase 5 capstone.  It uses
the real etcd client Go library — the same one production applications use — not
just the CLI binary.  Run via `bash test/etcd-compat.sh`.

## API coverage matrix

### KV service (`etcdserverpb.KV`)

| RPC | Status | clientv3 options exercised | Proof |
|-----|--------|---------------------------|-------|
| `Put` | **Supported** | plain, `WithLease`, `WithPrevKV` | etcdctl + clientv3 |
| `Range` | **Supported** | plain, `WithPrefix`, `WithRange`, `WithLimit` (more=true), `WithRev` (historical), `WithKeysOnly`, `WithCountOnly`, `WithSort(SortByKey, SortDescend)` | etcdctl + clientv3 |
| `DeleteRange` | **Supported** | plain, `WithPrefix`, `WithPrevKV` | etcdctl + clientv3 |
| `Txn` | **Supported** | `Compare(Value/Version/Create/Mod/Lease)`, success branch, failure branch, multi-op success, nested ranges | etcdctl + clientv3 |
| `Compact` | **Supported** | plain; verified `ErrCompacted` (gRPC `OUT_OF_RANGE` 11) on read below floor; read at floor succeeds | etcdctl + clientv3 |

All KV reads are **linearizable** via leader gating: the shard actor refuses Range
requests when not leader (returns `GRPC_UNAVAILABLE`).  Single-node mode is
always leader.

### Watch service (`etcdserverpb.Watch`)

| RPC | Status | Features | Proof |
|-----|--------|----------|-------|
| `Watch` (bidi streaming) | **Supported** | Created ack; live PUT/DELETE events in order; `WithPrefix` range watch; `WithRev` historical replay (buffered-before-CREATED ordering); watch cancel; stream teardown on client half-close | etcdctl + clientv3 |

Watch events are **revision-anchored**: the server buffers historical events
before sending the CREATED ack, so the client observes a consistent replay
followed by live events without gaps.  `start_revision` wire-to-internal
off-by-one adaptation is handled in `src/server/grpc-watch.scm`.

### Lease service (`etcdserverpb.Lease`)

| RPC | Status | Notes | Proof |
|-----|--------|-------|-------|
| `LeaseGrant` | **Supported** | auto-assign ID (id=0); TTL round-tripped | etcdctl + clientv3 |
| `LeaseRevoke` | **Supported** | deletes all attached keys atomically | etcdctl + clientv3 |
| `LeaseTimeToLive` | **Supported** | `WithAttachedKeys`: granted TTL + remaining TTL + key list | etcdctl + clientv3 |
| `LeaseKeepAlive` (bidi streaming) | **Supported** | stream-per-connection; two keepalive response round-trips proven; TTL=0 for expired leases | etcdctl + clientv3 |
| `LeaseLeases` | **Supported** | returns all active lease IDs; revoked lease absent | etcdctl + clientv3 |

Lease expiry is leader-driven: the shard's deadline timer fires at the leader
and proposes the revocation through Raft.

### Auth service (`etcdserverpb.Auth`) — Phase 6

| RPC | Status | Notes |
|-----|--------|-------|
| `AuthEnable` / `AuthDisable` / `Authenticate` | **Supported** | Argon2id; token issued on Authenticate; `AuthEnable` flips enforcement |
| `UserAdd` / `UserGet` / `UserList` / `UserDelete` / `UserChangePassword` / `UserGrantRole` / `UserRevokeRole` | **Supported** | full user lifecycle |
| `RoleAdd` / `RoleGet` / `RoleList` / `RoleDelete` / `RoleGrantPermission` / `RoleRevokePermission` | **Supported** | range-containment authorization |

Authorization is enforced per request via the auth token **or** the client-certificate CN
(over mTLS). Users/roles live in an internal `NS-AUTH` namespace invisible to `Range`/`Watch`.
Proof: `test/etcd-auth-grpc.sh` (RBAC, 22 assertions), `test/etcd-auth-mgmt-grpc.sh` (mgmt RPCs,
21 assertions).

### Cluster service (`etcdserverpb.Cluster`) — Phase 7 (joint-consensus membership)

| RPC | Status | Notes |
|-----|--------|-------|
| `MemberList` | **Supported** | real config view; uint64 ID↔name bijection (FNV-1a == node stable-id) |
| `MemberAdd` | **Supported** | add a voter (two-phase joint change) or a learner (single-phase) on a **live** cluster |
| `MemberRemove` / `MemberPromote` | **Supported** | remove a voter (incl. the leader; it steps down after Cnew commits) / promote learner→voter |
| `MemberUpdate` | **Partial (no-op)** | peer addresses are static in this deployment model |

Membership changes use joint consensus (Ongaro §4.3): a change requires a majority of **both**
the old and new voter sets, so there is no split-brain across a reconfiguration. Proof:
`test/etcd-cluster-grpc.sh` (list→add voter→add learner→promote→remove→follower-redirect, 23
assertions) and the Jepsen membership nemesis (`cw-u4a.35`).

### Maintenance service (`etcdserverpb.Maintenance`) — Phase 8

| RPC | Status | Notes |
|-----|--------|-------|
| `Status` | **Supported** | real raft scalars (term/index/applied/leader), db size, key count |
| `Hash` / `HashKV` | **Supported** | deterministic FNV-1a-32 over canonical scan order — identical across members |
| `Defragment` | **Supported** | advisory store flush |
| `Snapshot` | **Partial** | logical-keyspace stream, 512-aligned + sha256 trailer so `etcdctl snapshot save` downloads & verifies; restore is **native, not bbolt** |
| `Alarm` / `AlarmList` / `AlarmDisarm` | **Partial** | leader-local, not yet Raft-replicated (`cw-u4a.42`) |
| `MoveLeader` | **Not yet** | the Raft engine has no `TimeoutNow` leadership-transfer primitive (`cw-u4a.42`) |
| `Downgrade` | **Not yet** | version downgrade protocol not modeled |

Also served (Phase 8): `grpc.health.v1.Health/Check` + `Health/Watch` on the client port, and a
plain-HTTP `/health` `/version` `/metrics` (Prometheus) listener on a dedicated metrics port.
Proof: `test/etcd-maintenance-grpc.sh` (25 assertions), `test/health-metrics.sh` (24 assertions).

## Consistency model

| Property | Guarantee |
|----------|-----------|
| Read consistency | Linearizable — Range is leader-gated; no stale reads |
| Write ordering | Total order via Raft; all mutations pass through a single Raft log |
| Watch ordering | Revision-anchored; events delivered in strictly increasing revision order |
| Watch historical | Buffered-before-CREATED: client sees CREATED ack, then historical replay, then live events without gaps |
| Lease expiry | Leader-driven deadline; expiry proposed through Raft (consistent across cluster) |
| Compact floor | Reads below the compaction floor return `GRPC_OUT_OF_RANGE(11)` with the canonical etcd message |

## clientv3-compat test coverage summary

The `test/clientv3-compat/main.go` program (run via `test/etcd-compat.sh`) proves
the following with the real `go.etcd.io/etcd/client/v3` v3.5.17 library:

**KV section (18 assertions):**
- Put/Get value round-trip
- Overwrite: version=2, mod_revision > create_revision
- `WithRev` historical read returns original value
- `WithPrefix` range scan
- `WithLimit(2)` truncation with `more=true`
- `WithRange` lexicographic half-open range
- `WithKeysOnly` returns empty values
- `WithCountOnly` returns count with no Kvs
- `WithSort(SortByKey, SortDescend)` descending key order
- `WithPrevKV` on Put returns previous value
- `WithPrevKV` on Delete returns deleted value
- `WithPrefix` bulk delete + confirmation

**Txn section (8 assertions):**
- Succeeded=true when compare matches; mutation applied
- Succeeded=false when compare fails; no mutation
- Multi-op success branch (two puts in one Txn)

**Watch section (12 assertions):**
- CREATED ack received for live watch (requires `clientv3.WithCreatedNotify()` — by
  default clientv3 consumes the CREATED ack internally to gate the channel return;
  crab-watchstore correctly sends it, and the ack IS delivered when the flag is set)
- PUT / DELETE events in order (wv1, wv2, DELETE)
- Prefix watch CREATED ack + both key events
- Historical watch with `WithRev` replays h1, h2, h3

**Library behaviour finding (not a bug in crab-watchstore):**
The clientv3 `Watch()` function blocks internally until the server's CREATED ack is
received (the ack gates the channel return via an internal `retc` channel).  The ack
is only forwarded to the user-facing `WatchChan` when `clientv3.WithCreatedNotify()`
is supplied.  Without it, the ack is consumed silently — but the watch IS established
before `Watch()` returns.  No server-side fix needed.

**Lease section (11 assertions):**
- Grant returns non-zero ID + correct TTL
- Put with lease attaches key
- TimeToLive: granted TTL, remaining TTL > 0, attached key present
- KeepAlive: 2 response round-trips with TTL > 0
- Leases() contains the active lease
- Revoke: attached key deleted, lease absent from Leases()

**Compact section (4 assertions):**
- Compact(curRev) succeeds
- Get(WithRev(1)) below floor returns ErrCompacted (gRPC OUT_OF_RANGE)
- Get(WithRev(curRev)) at floor returns correct value
