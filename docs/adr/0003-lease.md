# ADR 0003 — Lease subsystem

- **Status:** Accepted
- **Date:** 2026-06-09
- **Tracking:** `cw-u4a.16` (this ADR), implemented by `.17` (expiry actor + LEASE-REVOKE apply), `.18` (keepalive bidi stream + tests), `.23` (gRPC `Lease` service binding).
- **Builds on:** [ADR 0001 — MVCC data model](0001-mvcc-data-model.md). The lease→keys index (`NS-LEASE`, `0x03 ‖ u64be(leaseId) ‖ K`, §8 there) and the inline `lease` field on every KeyValue record are Lease's storage substrate; the **attach/detach** machinery is **already implemented and validated** by `cw-u4a.6` (`mvcc-put!` / `mvcc-delete-range!` in `src/mvcc.scm`). This ADR adds the lease *object*, *expiry*, *keepalive*, and the *replicated revoke* on top of that index. Leader-gating + actor-tick patterns mirror [ADR 0002 — Watch](0002-watch.md).
- **Validated by:** [`test/lease-poc.scm`](../../test/lease-poc.scm) — 28 assertions that the **lease→keys index** behaves exactly as the revoke path needs, over the REAL `.6` write path (`mvcc-apply` PUT/DEL): attach groups a lease's keys, reattach moves a key with **no stale prior entry**, `lease=0` detaches, `DeleteRange` detaches, and the **revoke scan** (`mvcc-lease-keys`, the only new read helper) returns exactly a lease's current attached set. All green, twice back-to-back. The live expiry/keepalive/revoke-apply machinery is `.17`/`.18`, tested there.

## Context

crab-watchstore is an etcd v3 API-compatible store. A **lease** is etcd's TTL mechanism for *automatic key deletion*: a client grants a lease with a time-to-live, attaches one or more keys to it, and keeps it alive with periodic heartbeats. If the client stops (crash, partition, shutdown) the lease **expires** and **every key attached to it is deleted** — the foundation of etcd-based service registration, leader election (the leader holds a lease; losing it relinquishes leadership), and distributed locks. This **replaces** crab-cache's Redis-style per-key logical-clock TTL with etcd's lease-grouped, replicated, linearizable-revoke model.

Lease has a property no other subsystem here does: it couples a **wall-clock deadline** (intrinsically a real-time, leader-local concern — clocks differ across replicas) to a **linearizable state change** (the revoke must delete the same keys at the same revision on every replica). Getting that split right is the crux of this ADR.

We build on the **already-built MVCC substrate** (ADR 0001) and the **already-built attach/detach** (`cw-u4a.6`):

- **Lease→keys index** — `NS-LEASE` (`0x03 ‖ u64be(leaseId) ‖ K → ()`, ADR 0001 §8). `u64be(leaseId)` is order-preserving so each lease's keys group disjointly; a single `kv-scan` of `0x03 ‖ u64be(id)` enumerates exactly that lease's keys. This is what makes revoke **O(keys-on-lease)**. Proven disjoint in `test/mvcc-encoding-poc.scm` ("lease 100 vs 101 group disjointly") and exercised end-to-end in `test/lease-poc.scm`.
- **Inline `lease` field** — every KeyValue record carries its lease id (ADR 0001 §4), so a read returns the lease with no second lookup, and the apply path knows a key's *prior* lease to maintain the index.
- **Attach/detach (already done, `mvcc-put!` / `mvcc-delete-range!`):** `PUT k v lease` adds `0x03 ‖ lease ‖ K` and **removes a stale prior lease entry** when the key's lease changed; `lease=0` detaches; `DeleteRange` removes the victim's lease entry. `test/lease-poc.scm` is the validation that this index machinery is sound (the stale-removal especially, which the revoke scan depends on).
- **The apply seam** — `mvcc-apply ctx cmd` runs inside the shard actor's group-commit batch (`src/server/shard-actor.scm`, `apply-fn`); it is the single replicated point where keyspace state changes. **The revoke must go through here** so every replica deletes the same keys at the same revision.
- **The leader tick** — the shard actor already runs a heartbeat/election `(tick)` (it drives Raft heartbeats, bounds ack latency, and `progress_notify` for Watch). **The expiry scan rides this same tick** — no new timer subsystem.
- **`current-second`** — the project's wall-clock primitive (used as `(exact (round (* 1000000 (current-second))))` for test dir tags). It is the clock the leader-local deadline reads.

### What this ADR must pin down

1. The **lease object + replicated storage** (granted TTL must survive leader change).
2. **THE CRUX — expiry**: the granted TTL is replicated, but the live **deadline** is leader-local; expiry proposes a **`("LEASE-REVOKE" id)`** Raft entry so the revoke is **linearizable** (same keys, same revision, every replica), and the new leader re-derives deadlines on failover.
3. **Keepalive** as a leader-local deadline reset (**not** a Raft entry).
4. **attach/detach** (already implemented in `.6` — formalized here, plus the put-to-dead-lease validation).
5. The **API surface** mapping (`.17`/`.18`/`.23`).
6. **Revision semantics** (grant/keepalive don't bump; revoke bumps once).
7. The **consumers** table.

## Decision

### 1. Lease object + replicated storage

A **lease** is a tiny object: `{ id, granted_ttl }`. The live deadline is **not** part of it (see §2 — it is leader-local). `LeaseGrant(ttl, id)` with `id = 0` ⇒ the leader auto-assigns a fresh non-zero id (a monotone counter persisted as a meta scalar — see below); a non-zero `id` is used as given (etcd allows client-chosen ids).

**Where lease metadata lives (must be REPLICATED).** The granted TTL must survive a leader change — a new leader has to know each live lease's TTL to re-derive its deadline. So the lease record is written to the **durable, replicated store** under the existing `NS-LEASE` namespace, distinguished from the lease→keys index entries by a **sentinel empty key** (`K = ""`):

```
lease-meta key   :  0x03 ‖ u64be(leaseId) ‖ <empty>     (i.e. 0x03 ‖ u64be(id), length exactly 9)
lease-meta value :  u64be(granted_ttl_seconds)          (a self-describing fixed-width scalar)
```

This reuses the `0x03` lease prefix (no new namespace tag) and sits at the **head** of the lease's own prefix group: the sentinel key `0x03 ‖ u64be(id)` is a byte-prefix of every index entry `0x03 ‖ u64be(id) ‖ K` (since any real attached key `K` is non-empty), so it sorts **first** within the lease's group and a `kv-scan` of `lease-prefix(id)` returns the meta entry followed by the attached keys. The revoke-scan read helper (`mvcc-lease-keys`, below) and `LeaseTimeToLive`'s key-listing therefore **skip the length-9 sentinel** and return only the real (`K`-non-empty) attached keys — a one-line length guard.

> **Why a sentinel key under `0x03`, not a new namespace tag or the MVCC keyspace?** (a) Reusing `0x03` keeps every lease's metadata + index entries **physically adjacent** in one prefix group — granting, revoking, and "does lease `id` exist?" are all reads/writes within `lease-prefix(id)`. (b) It keeps lease metadata **out of the MVCC `NS-KEY` keyspace**, so a grant does **not** create a KeyValue version and does **not** bump the revision (§6) — leases are not keyspace mutations, exactly as in etcd. (c) The empty-key sentinel is unambiguous: a real attached key is never empty (an empty user-key can't be PUT), so `length == 9` ⇔ "this is the meta entry". A separate tag (e.g. `0x04`) would also work but fragments the lease's data across two prefix groups for no benefit. See Alternatives.

**Lease existence = the meta entry exists.** "Is lease `id` live?" is `kv-exists? (0x03 ‖ u64be(id))`. Grant writes the meta entry; revoke deletes it (along with all index entries — §2). This is the replicated source of truth a new leader scans on failover.

**Auto-assigned ids.** `id = 0` on grant ⇒ allocate `next = (lease-id-counter) + 1`, persist the counter as a meta scalar `0x00 ‖ "lease-id-seq" → u64be(next)` (`NS-META`, alongside `current-rev`/`compact-rev`), and use `next`. The counter write rides the same group-commit batch as the grant; it bumps **only on grant**, never reused, so ids are unique across the cluster's life even across leader changes (the counter is replicated like every other applied write). etcd derives ids from a similar monotone source; a fixed-width replicated counter is the order-preserving, crash-safe equivalent.

**`mvcc-lease-keys` (the only new read helper, added this task).** A **pure read** over `lease-prefix(id)` that returns the list of currently-attached user-keys (skipping the length-9 meta sentinel). It is exactly what `("LEASE-REVOKE" id)` iterates (§2) and what `LeaseTimeToLive(...keys)` returns. The lease **write** path is untouched — that is `.6`, already validated; this helper is the read side `lease-poc.scm` exercises.

### 2. THE CRUX — expiry & the deadline-vs-replicated-TTL split

This is the heart of the ADR. The tension: a TTL deadline is a **wall-clock** fact and replicas' clocks differ, but the revoke (delete all the lease's keys) must be **linearizable** — identical keys, identical revision, on every replica. crab-watchstore resolves it by splitting the lease into a **replicated part** and a **leader-local part**:

| | What | Where | Replicated? |
|---|---|---|---|
| **Granted TTL** | `granted_ttl_seconds` | lease-meta entry `0x03 ‖ u64be(id)` (§1) | **YES** — in the Raft log / durable store |
| **Live deadline** | `deadline = now + ttl` (wall-clock) | leader-local in-memory map `id → deadline` | **NO** — leader-only, never replicated |

**The leader-side expiry scan (rides the actor tick).** The leader holds an in-memory map `lease-deadlines : id → deadline-second`. On each `(tick)` (the same heartbeat the shard actor already runs — §6 of ADR 0002 reuses it for `progress_notify`; here it drives expiry), the leader reads `now := (current-second)` and finds every lease whose `deadline ≤ now`. For each expired `id`, **it does not delete locally** — it **proposes** a Raft command:

```
("LEASE-REVOKE" id)      ; id as decimal-ASCII bytevector, same convention as the PUT leaseId arg
```

through the normal propose path (`raft-propose` in `shard-actor.scm`, exactly like a client `PUT`/`DEL`). To avoid re-proposing the same expiry every tick until it commits, the leader removes `id` from `lease-deadlines` (or marks it "revoke in flight") at propose time; the authoritative removal of the lease meta + keys happens when the entry **applies**.

**The `LEASE-REVOKE` command + its apply (one revision, every replica).** When the committed `("LEASE-REVOKE" id)` entry reaches `mvcc-apply` on **every** replica (leader and followers alike, in the group-commit batch), the apply:

1. bumps `current-rev` once → `main = prev + 1` (the revoke is one keyspace-effecting Raft entry = one revision — §6);
2. enumerates the lease's keys via `mvcc-lease-keys ctx id` (the validated revoke scan);
3. for each key, writes a **TOMBSTONE** KEY-CF version at `main.sub` (sub increments per key) **and** a REV-CF `DELETE` event — i.e. the *exact same per-key delete `mvcc-delete-range!` already performs* (so Watch sees a `DELETE`, reads after `main` see the key gone, and the key's own `0x03 ‖ id ‖ K` index entry is removed by that delete path);
4. deletes the **lease-meta** entry `0x03 ‖ u64be(id)` (the lease no longer exists);
5. persists the bumped `current-rev` — all in the one batch, under one fsync.

Because this is a **single committed Raft entry applied by every replica at the same log position**, all replicas delete **the same keys at the same revision** `main`. That is the linearizable revoke. The implementation is `.17` (`mvcc-apply` gains a `"LEASE-REVOKE"` case that loops `mvcc-lease-keys` and tombstones each, mirroring `mvcc-delete-range!`'s per-victim body); this ADR fixes the command shape, the single-revision contract, and that it goes through the apply seam.

**On leader change — re-derive deadlines from the persisted TTL (fresh full window).** A new leader has the **replicated TTL** (every live lease's meta entry is durable) but **not** the old leader's in-memory deadlines. On becoming leader, it **scans the `NS-LEASE` meta entries** (every `0x03 ‖ u64be(id)` sentinel), and for each live lease seeds `lease-deadlines[id] := (current-second) + granted_ttl` — i.e. a **fresh, full TTL window** from the moment of leadership. This is **etcd's default behavior**: on leader change a lease effectively gets its full TTL again (the deadline is not checkpointed by default), trading a possibly-longer-than-nominal survival for zero clock-coordination. A client that was keeping the lease alive simply continues; one that died will have its (re-windowed) lease expire one full TTL after the new leader takes over. *(Followers track no deadlines — they only apply `LEASE-REVOKE` entries; only the leader runs the scan.)*

> **Future option — lease checkpointing (tighter remaining-TTL).** etcd optionally replicates periodic *remaining-TTL checkpoints* (`LeaseCheckpoint`) through Raft so a new leader resumes near the *actual* remaining time instead of granting a fresh full window. crab-watchstore can add this later as a periodic `("LEASE-CHECKPOINT" id remaining)` entry the leader proposes on a coarse interval; the new leader would seed `deadline := now + min(granted_ttl, last_checkpoint_remaining)`. **Out of scope for the MVP** (it adds replicated write traffic proportional to live-lease count); the fresh-full-window default is correct and simple. Noted so `.17`'s re-derivation leaves room for it.

**Why ONLY the leader runs expiry.** (a) Expiry is the one place a *wall-clock* decision enters the system; if every replica independently decided "expired now" off its own clock, they would revoke at **different revisions** (or not at all on a lagging clock) — non-linearizable, divergent state. Funneling the decision through **one** node (the leader) that then **replicates the consequence** (the `LEASE-REVOKE` entry) is what makes the revoke agree everywhere. (b) Only the leader proposes to Raft anyway; a follower cannot commit an entry. (c) This mirrors the Watch design exactly (ADR 0002 §4: "only the leader applies + acks, so only it notifies") — the leader is already the single locus of client-facing, time-sensitive work. A follower that becomes leader picks up expiry via the failover re-derivation above; a leader that steps down simply stops ticking expiry (its in-memory deadlines are discarded, harmless — the TTLs are safe in the replicated meta entries).

### 3. Keepalive

`LeaseKeepAlive(id)` is the client's "I'm still here" heartbeat. It **resets the leader-local deadline** and is **NOT a Raft entry**:

- The leader sets `lease-deadlines[id] := (current-second) + granted_ttl` (a full fresh window) and replies the **granted TTL** (`LeaseKeepAliveResponse{ id, ttl = granted_ttl }`). etcd's response carries the (granted) TTL so the client knows the interval at which to renew (typically `ttl/3`).
- **No Raft round.** The lease's *existence* is already replicated (the meta entry from grant); a keepalive changes only the **leader-local deadline**, which is leader-local by design (§2). Replicating every keepalive would put the hottest lease operation on the consensus path for **zero correctness gain** — the deadline isn't replicated anyway, and on failover it's re-derived fresh (§2). This is exactly why etcd keepalives don't go through Raft. (This is also the consistency the §6 "no rev bump" rule reflects: keepalive has no keyspace effect.)
- **Validation.** A keepalive for an `id` whose meta entry **does not exist** (never granted, or already revoked) replies a TTL of **0** — etcd's signal that the lease is gone and the client must stop/re-grant.
- **On a non-leader: reject / redirect.** Keepalive must hit the leader (it owns the deadline map). A non-leader replies a redirect marker (`('lease-not-leader . LEADER)`, mirroring the read path's `'tryagain` and Watch's `'watch-not-leader`) so the streaming binding (`.23`) re-targets the current leader. A keepalive served on a stale node would reset a deadline no one consults.

`LeaseKeepAlive` is a **bidi stream** in the gRPC API (the client sends `id`s, the server streams TTL responses) — the streaming actor for it is `.18`/`.23`; this ADR fixes that it is a leader-local deadline reset with a granted-TTL response and a non-leader redirect.

### 4. attach / detach (already implemented in `.6` — formalized)

A key is bound to a lease by the **`lease` argument on a PUT**, and unbound by `lease=0`, a different lease, or a delete. This is **already implemented and validated** (`mvcc-put!` / `mvcc-delete-range!`, `test/mvcc-apply.scm` + `test/lease-poc.scm`); formalized here as the lease contract:

- **Attach** — `PUT k v lease` (lease ≠ 0): the KeyValue record stores `lease` inline, and `0x03 ‖ lease ‖ K → ()` is added to the index. (lease-poc: "L1 index = {k1,k2,k3}".)
- **Reattach** — `PUT k v lease2` where `k` previously had `lease1 ≠ lease2`: `mvcc-put!` **removes the stale** `0x03 ‖ lease1 ‖ K` and adds `0x03 ‖ lease2 ‖ K`. The stale-removal is load-bearing — without it `lease1`'s revoke scan would still hit `k`. (lease-poc: "k2 leaves L1, appears under L2 (no stale L1)".)
- **Detach** — `PUT k v 0` (or omitted lease): the prior lease entry is removed and none added; the record's `lease` becomes 0. (lease-poc: "k1 detached".)
- **Delete detaches** — `DeleteRange` over `k` tombstones it **and** removes its `0x03 ‖ lease ‖ K` entry. (lease-poc: "DEL k3 → k3 leaves its lease index".)

> **Put-to-nonexistent/expired-lease must error (etcd parity) — `.17` validation.** In etcd, `Put(key, value, WithLease(id))` for an `id` that does not exist (never granted, or already revoked/expired) **fails** with `ErrLeaseNotFound` — the key is **not** written. The current `.6` `mvcc-put!` does **not** validate the lease (it was built before the lease object existed). **`.17` adds this check at the apply boundary**: before attaching, verify the lease meta entry `0x03 ‖ u64be(id)` exists (`kv-exists?`); if not, the PUT returns an error result (`(cons 'err-lease-not-found id)`) and writes nothing. This keeps a key from being silently orphaned to a dead lease (which would never be revoked and would leak). The check is a single `kv-exists?` on the apply path, cheap and replicated (every replica sees the same meta state). *(Recorded as a `.17` deliverable so `.6`'s validated write path is not changed by this design task.)*

### 5. API surface (implemented in `.17`/`.18`/`.23`)

Each etcd Lease RPC maps to the model above:

| RPC | Model mapping | Bumps rev? |
|---|---|---|
| **`LeaseGrant(ttl, id)`** | id=0 ⇒ auto-assign from `lease-id-seq`; write lease-meta `0x03‖u64be(id) → u64be(ttl)`; leader seeds `lease-deadlines[id] = now+ttl`. Returns `{id, ttl}`. (§1) | **No** (§6) — side-namespace meta write, no keyspace mutation |
| **`LeaseRevoke(id)`** | Propose `("LEASE-REVOKE" id)`; apply tombstones all `mvcc-lease-keys(id)` + deletes the meta entry at one revision. *Explicit* client revoke takes the **same** replicated path as expiry (§2). Returns `{}`. | **Yes** — one bump for the deletes (§6) |
| **`LeaseKeepAlive(id)`** (bidi stream) | Leader resets `lease-deadlines[id] = now+granted_ttl`; replies `{id, ttl=granted_ttl}` (0 if the lease is gone). Non-leader redirects. **No Raft.** (§3) | **No** (§6) — leader-local deadline only |
| **`LeaseTimeToLive(id, keys?)`** | Leader reads `granted_ttl` from the meta entry and `remaining = deadline - now` from `lease-deadlines` (`< 0` / absent ⇒ lease gone). With `keys=true`, also returns `mvcc-lease-keys(id)` (the attached keys). Returns `{ttl=remaining, grantedTTL, keys?}`. Leader-local (the remaining-TTL is the deadline, leader-only); non-leader redirects. | **No** — pure read |
| **`LeaseLeases()`** | List all live lease ids by scanning `NS-LEASE` for length-9 sentinel keys (`0x03‖u64be(id)`), decoding the id. Returns `{leases:[{id}...]}`. | **No** — pure read |

`LeaseGrant`/`LeaseRevoke`/`LeaseTimeToLive`/`LeaseLeases` are **unary**; `LeaseKeepAlive` is a **bidi stream**. `.17` implements grant + the expiry actor + the `LEASE-REVOKE` apply (and the put-to-dead-lease check, §4); `.18` implements the keepalive stream + `LeaseTimeToLive`/`LeaseLeases` reads + the lease tests; `.23` binds all five over gRPC streaming framing.

### 6. Revision semantics

Consistent with `cw-u4a.40`'s rule ("advance the revision only on a keyspace *effect*"):

- **`LeaseGrant` does NOT bump the revision.** A grant is not a keyspace mutation — it writes a lease-meta scalar in the `NS-LEASE` side namespace (§1), never a `NS-KEY` KeyValue version. This is why the meta entry lives **outside** the MVCC keyspace: a grant must be *applied* (durably, replicated) but must **not** advance `current-rev`. etcd agrees — `LeaseGrant` returns the *current* header revision, unchanged. *(The `lease-id-seq` counter write also doesn't bump.)*
- **`LeaseKeepAlive` does NOT bump.** It is leader-local, doesn't even apply through Raft, touches no keyspace state (§3).
- **`LeaseRevoke` (and expiry) bumps the revision exactly ONCE.** The revoke deletes ≥1 key — a real keyspace effect — so it advances `current-rev` by **one** `main` (the deletes are sub-revisions `main.0, main.1, …` within that single entry), exactly like a multi-key `DeleteRange`. A revoke of a lease with **zero** attached keys (all already detached/deleted) deletes nothing and, per `cw-u4a.40`, should **not** bump — it only removes the (already-empty) meta entry; `.17` applies the same "effect ⇒ bump" guard the Txn/DEL paths use.
- **`LeaseTimeToLive` / `LeaseLeases` are pure reads** — no bump.

This keeps the revision a faithful count of keyspace-mutating transactions: grants/keepalives are invisible to it, a revoke is one mutation. *(The rev-bump-only-on-effect rule for the revoke is the same guard `.40` installs for zero-match DELs; this ADR states the lease side of it.)*

### 7. Consumers

| Task | Implements / uses |
|---|---|
| `cw-u4a.16` (this ADR + model) | Decides lease object + replicated meta storage (§1), the deadline-vs-TTL split + `LEASE-REVOKE`-via-Raft (§2), keepalive-as-leader-local (§3), the attach/detach contract (§4), the API mapping (§5), revision semantics (§6). Adds the **pure-read** `mvcc-lease-keys` (the revoke scan) to `src/mvcc.scm`; **validated by `test/lease-poc.scm`** over the real `.6` index. |
| `cw-u4a.17` Lease expiry (replicated revoke) | The **leader-local `lease-deadlines` map** + the **expiry scan off the actor tick** (§2: `now := current-second`, propose `("LEASE-REVOKE" id)` for `deadline ≤ now`); the **`LEASE-REVOKE` apply case** in `mvcc-apply` (loop `mvcc-lease-keys` → tombstone each + delete meta, one revision — §2/§6); **`LeaseGrant`** (meta write + `lease-id-seq` + seed deadline — §1); the **failover re-derivation** (scan meta entries → fresh-window deadlines — §2); the **put-to-dead-lease `kv-exists?` check** (§4). Uses `mvcc-lease-keys` (this ADR) + the existing `mvcc-delete-range!` per-key tombstone body. |
| `cw-u4a.18` Lease keepalive bidi + tests | The **keepalive deadline reset** + granted-TTL response + non-leader redirect (§3); **`LeaseTimeToLive`** (granted+remaining, optional keys) and **`LeaseLeases`** (sentinel scan) reads (§5); the **lease test suite** — grant/attach/revoke/expiry/keepalive + revoke-deletes-all-keys-at-one-revision (mirrors `test/lease-poc.scm`'s structure, adds the live expiry/keepalive seam the way `.15` adds Watch's live half). |
| `cw-u4a.23` gRPC `Lease` service | Binds **`LeaseGrant`/`LeaseRevoke`/`LeaseTimeToLive`/`LeaseLeases`** (unary) + **`LeaseKeepAlive`** (bidi stream) over the gRPC framing (`.20`) to the `.17`/`.18` actors; carries the leader-redirect markers (§3) back to the client as the appropriate gRPC status. |
| `cw-u4a.40` rev-on-effect | The revoke's "bump only if ≥1 key deleted" guard (§6) is the same zero-effect rule `.40` installs for DEL/Txn. |

## Alternatives considered

1. **Replicate the live deadline (or run expiry on every replica off local clocks).** Tempting for "HA expiry", but replicas' wall clocks differ, so independent expiry decisions would revoke at **different revisions** (or diverge if a clock lags) — non-linearizable, the exact failure leases must avoid. Rejected: the deadline stays **leader-local** and only the linearizable *consequence* (the `LEASE-REVOKE` entry) is replicated (§2). This is etcd's model.
2. **Revoke locally on the leader (delete keys directly), then let normal MVCC replication carry the deletes.** The leader *could* just call `mvcc-delete-range!` for each key. But then the deletes are an ordinary leader-side mutation, and there is no single Raft entry that *names the revoke* — making "every replica deleted exactly the lease's keys at the same revision" an emergent property of N separate deletes rather than one atomic, named, replicated decision. Rejected in favor of one `("LEASE-REVOKE" id)` entry whose apply is identical on every replica (§2) — simpler to reason about, idempotent on replay, and matches how every other write here is one Raft entry.
3. **Keepalive through Raft (replicate each heartbeat).** Would make the deadline replicated and survive failover exactly. Rejected: it puts the **hottest** lease op on the consensus path for no correctness gain (the deadline is re-derived fresh on failover anyway, §2), and etcd deliberately keeps keepalives off Raft for the same reason. The fresh-full-window-on-failover default (§2) is the accepted trade.
4. **Lease metadata in the MVCC `NS-KEY` keyspace (a real key per lease).** Would let `Range` see leases and reuse the KeyValue record. Rejected: a grant would then create a KeyValue version and **bump the revision** (§6), contradicting etcd (grants don't advance the revision) and `cw-u4a.40`; it also entangles lease lifecycle with compaction/tombstones. The `NS-LEASE` sentinel meta entry (§1) keeps leases out of the revisioned keyspace.
5. **A separate `0x04` namespace tag for lease metadata** (instead of the `0x03` sentinel key). Clean separation, and equally correct. Rejected only for locality: the sentinel keeps a lease's meta + index entries in **one** contiguous prefix group (`lease-prefix(id)` covers both), so grant/revoke/exists are all one prefix's worth of I/O; a second tag fragments it for no real benefit. (If a future need arises to scan *all* lease metadata without touching index entries, the `0x04` tag becomes attractive — noted.)
6. **Lease checkpointing in the MVP** (replicate remaining-TTL so failover resumes near actual remaining time). Rejected for v1: it adds replicated write traffic proportional to live-lease count for a refinement (tighter post-failover TTL) that the fresh-full-window default makes unnecessary for correctness. Kept as an explicit §2 future option.

## Consequences

**Positive**
- The lease→keys index foundation the whole revoke path stands on is **proven over the real `.6` write machinery** by `test/lease-poc.scm` (28/28, twice back-to-back): attach groups, reattach moves with no stale entry, `lease=0`/`DeleteRange` detach, and the revoke scan returns exactly a lease's current set.
- **Linearizable revoke for free:** because revoke is one `("LEASE-REVOKE" id)` Raft entry applied identically on every replica, all replicas delete the same keys at the same revision — no clock coordination, inheriting the substrate's Jepsen-validated apply/commit path.
- **Zero change to the validated `.6` write path** in this task: the only `src/mvcc.scm` edit is the pure-read `mvcc-lease-keys`. The lease object/expiry/keepalive/revoke-apply are additive (`.17`/`.18`), and the meta entry reuses the existing `NS-LEASE` namespace (no schema churn).
- Grant/keepalive **don't perturb the revision** (§6), so the revision stays a faithful keyspace-mutation count and lease traffic doesn't inflate it — etcd parity.
- Expiry **reuses the existing actor tick** and the leader-gating pattern Watch already established — no new timer subsystem, no new race surface (the leader's single green thread serializes expiry-propose with apply, like Watch's registry).

**Negative / trade-offs**
- **Fresh-full-window on leader change** means a lease can survive up to ~one extra TTL after a failover (the new leader re-windows it). This is etcd's default and acceptable; lease checkpointing (§2 future option) is the remedy if tighter bounds are needed.
- Expiry granularity is **one tick**: a lease can outlive its nominal deadline by up to the tick interval before the leader notices and proposes the revoke (plus one Raft round-trip to commit). Fine for second-granularity TTLs; the tick already runs, so no extra cost.
- The leader holds an **in-memory `lease-deadlines` map** (O(live-leases)) and rescans it each tick. Cheap for realistic lease counts; a future heap/timer-wheel keyed by deadline would make the per-tick scan O(expired) instead of O(live) if very large lease populations arrive (same shape as Watch's deferred interval-tree).
- **Watch + Lease are leader-only.** A leadership change discards the old leader's deadline map (re-derived on the new leader) and any in-flight keepalive streams (the client reconnects/redirects) — standard etcd client behavior, not transparently HA across failover.
- A `LEASE-REVOKE` that races a concurrent `PUT`-attach to the same lease is resolved by **Raft order**: whichever entry commits first wins; if the PUT-attach commits after the revoke (and the put-to-dead-lease check, §4, sees the meta already deleted), it errors `ErrLeaseNotFound` — the key is not orphaned. The single apply thread per replica makes this deterministic.
