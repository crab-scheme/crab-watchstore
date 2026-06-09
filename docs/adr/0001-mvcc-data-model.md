# ADR 0001 — MVCC data model

- **Status:** Accepted
- **Date:** 2026-06-09
- **Tracking:** `cw-u4a.5` (this ADR), implemented by `cw-u4a.6` (apply-fn), consumed by `.7` (Range), `.8` (compaction), `.12/.13/.15` (Watch), `.17` (lease).
- **Validated by:** [`test/mvcc-encoding-poc.scm`](../../test/mvcc-encoding-poc.scm) — 37 assertions on the byte sort order this design depends on, all green.

## Context

crab-watchstore is an etcd v3 API-compatible store. etcd's data model is **multi-version concurrency control (MVCC)** over a single monotone revision counter:

- A **global revision** advances on every write transaction; within one transaction each individual operation gets a **sub-revision**. etcd writes this as `main.sub`.
- Each key carries `KeyValue{key, create_revision, mod_revision, version, value, lease}`. `version` is the per-key write count since creation (resets to 1 on create-after-delete); `create_revision`/`mod_revision` are the global revisions at create / last-modify.
- **Deletes** write a *tombstone* version (not a physical erase), so a `Range` at a revision after the delete returns nothing and **Watch** can emit a `DELETE` event.
- **Compact(rev)** drops superseded history at or below `rev`; reads below the compact revision fail with `ErrCompacted`.
- **Watch** streams events **in revision order** from a caller-supplied `start_revision`.

We must implement this on the **already-built, Jepsen-validated substrate** (ported from crab-cache):

- [`src/encoding.scm`](../../src/encoding.scm) — order-preserving byte helpers (`u64->bytes`/`bytes->u64` 8-byte big-endian, `s64->order-bytes`, `subbv`, …).
- [`src/store-ctx.scm`](../../src/store-ctx.scm) — a durable-KV API over **one RocksDB column family** (an ordered LSM): `kv-get`/`kv-put!`/`kv-del!`/`kv-exists?`, ordered prefix `kv-scan`/`kv-scan-count`, group-commit `ctx-flush!`, and applied-index persistence `ctx-save-applied!`/`ctx-load-applied`.
- [`src/server/shard-actor.scm`](../../src/server/shard-actor.scm) — the apply-fn seam. Mutations run *inside* the group-commit batch; the Raft applied-index is persisted via `ctx-save-applied!` **in the same batch**; `cw-u4a.6` swaps the stub apply-fn for the MVCC apply.

RocksDB stores bytes in **lexicographic key order**, and `kv-scan` does an ordered forward prefix iteration (`store-iter` → `raw.seek(prefix)` then iterate while `starts_with(prefix)`, ascending). The WAL is shared across the whole DB, and `store-flush-wal db #t` issues **one fsync** that durably persists every write since the last fsync.

### The crux: the two-ordering problem

The hot etcd queries demand **two incompatible sort orders** over the same data:

| Query | Wants data ordered by |
|---|---|
| `Range [k1,k2)` at `readRev`, point read, latest-version | **user-key**, then revision |
| `Watch` replay from `start_revision` | **revision** |

RocksDB is a *single* ordered keyspace. We cannot have both orders in one index, so we maintain **two namespaces** populated atomically in the same write batch.

## Decision

### 1. Two namespaces in one column family (prefix-tagged)

We keep the substrate's single-CF design and split the keyspace by a **1-byte leading namespace tag**. Each namespace gets an independent, disjoint, ordered byte range within the one CF (proven in the POC: `META < KEY < REV < LEASE`), so a prefix scan of one never bleeds into another. Sharing **one CF means sharing one WAL**, which is what makes a multi-namespace write atomic under a single fsync (see Crash-consistency).

| Tag | Byte | Namespace | Key → Value |
|---|---|---|---|
| `NS-META`  | `0x00` | meta scalars | `0x00 ‖ name` → `u64` (current-rev / compact-rev) |
| `NS-KEY`   | `0x01` | **key-ordered store** | `0x01 ‖ u64be(len K) ‖ K ‖ INV(rev16)` → `KeyValue` record |
| `NS-REV`   | `0x02` | **revision-ordered index** | `0x02 ‖ rev16` → event record |
| `NS-LEASE` | `0x03` | lease→keys index | `0x03 ‖ u64be(leaseId) ‖ K` → `()` |

> **Why prefix-tag, not real column families?** RocksDB CFs exist (`store-cf-create`) and would give physical separation, but the ported `store-ctx.scm` hardcodes one CF per shard (`shard-ctx-cf`) across every `kv-*` op, and `store-flush-wal` is DB-wide regardless. Prefix tags get the two independent orderings **with zero substrate changes** and the same one-fsync atomicity. Switching `NS-KEY`/`NS-REV` to real CFs later is a transparent change (CFs share the DB WAL too). See Alternatives.

### 2. Revision model

- **`rev16` = `u64be(main) ‖ u64be(sub)`** — 16 bytes. `u64be` is unsigned-monotone, so lexicographic order on `rev16` equals etcd's numeric `main.sub` order: **main dominates (high 8 bytes), sub breaks ties** (POC: `1.999 < 2.0`, `7.10 < 7.11`).
- **main** bumps **once per applied Txn** (one Raft entry = one Txn = one main revision); **sub** increments per write op within that Txn, starting at 0. A read-only Txn does not bump main.
- **current-rev** and **compact-rev** are persisted as meta scalars: `kv-put!(0x00‖"current-rev", u64->bytes(main))` and `kv-put!(0x00‖"compact-rev", …)`. They are written **in the same group-commit batch** as that Txn's record writes and the `ctx-save-applied!`, so the fsync makes (revision, records, applied-index) durable together. On restart the apply-fn loads current-rev/compact-rev from these meta keys exactly as the actor already loads the applied-index via `ctx-load-applied`; the Raft log then replays only entries **above** the persisted applied-index, so revisions are never double-allocated.

### 3. KEY-CF key — `0x01 ‖ u64be(len K) ‖ K ‖ INV(rev16)`

This layout makes all three key-side queries a single bounded ordered scan:

- **`u64be(len K)` length prefix** — guarantees a key that is a byte-prefix of another (e.g. `"a"` vs `"ab"`) still groups **disjointly**: `len("a")=1 < len("ab")=2` forces *all* of `"a"`'s versions before *any* of `"ab"`'s (POC: "all 'a' versions < all 'ab' versions"; prefix-of hazard rejected). Without it, `"a"‖rev` could interleave with `"ab"`'s range.
- **`INV(rev16)` = bitwise complement of `rev16`** — stores revisions in **DESCENDING** on-disk order (newest first) within a key group. A *newer* revision yields a *smaller* on-disk key (POC: "rev 5.0 sorts before rev 3.0"). The `kv-scan` of a key's prefix therefore returns versions **newest → oldest**.

The composite key cleanly separates concerns: across different keys the user-key bytes dominate the rev bytes that follow, so **`enc(k1, anyRev) < enc(k2, anyRev)` whenever `k1 < k2`** regardless of revision (POC: "k_aaa(rev 1000000.0) < k_abc(rev 0.0)").

This makes each query efficient:

- **(a) latest visible version of K ≤ `readRev`** — `kv-scan` the prefix `0x01‖u64be(len K)‖K`; the result is newest→oldest, so walk it and take the **first record whose decoded rev ≤ readRev**. Bounded by the number of versions of K. *(The encoding also supports a true O(log n) RocksDB seek to `0x01‖len‖K‖INV(readRev16)` — the first on-disk key ≥ that seek is exactly the answer (POC: "seek(5.0) after 7.0", "seek(5.0) ≤ 4.0 (4.0 is first hit)"). `kv-scan` doesn't expose an arbitrary seek-key today; a thin `kv-seek` is a cheap, optional `.7`/perf follow-up the layout already accommodates.)*
- **(b) Range `[k1,k2)` at `readRev`** — scan forward from `0x01‖u64be(len k1)‖k1`; because cross-key order is by user-key, this is one contiguous byte range; within each key group take the first version ≤ readRev (as in (a)), skipping tombstones.
- **(c) point read of K at current rev** — special case of (a) with `readRev = current-rev`; the very first scan hit (newest) wins unless it is a tombstone.

### 4. KeyValue record serialization (self-describing, NO protobuf)

The protobuf codec (`cw-u4a.19`) does not exist yet, and storage must not depend on it. We use a **length-prefixed field blob** — simple, self-describing, binary-safe, decoded with `subbv`/`bytes->u64` only:

```
record :=
  u8     tag           ; 0 = VALUE (live), 1 = TOMBSTONE (delete)
  u64be  create_revision   ; main rev at create (0 in a tombstone)
  u64be  mod_revision      ; main rev of THIS write
  u64be  version           ; per-key write count; 1 on (re)create, +1 each update, 0 in tombstone
  u64be  lease             ; lease id, 0 = none
  u64be  value_len
  bytes  value             ; value_len raw bytes (empty in a tombstone)
```

Fixed-width fields keep it trivial; `value_len` makes the value binary-safe and lets the record carry the lease id inline (no second lookup on read). (This is the *storage* record; it is unrelated to the Raft-command wire shape — the documented "node-send can't carry nested lists" constraint is on the Raft AppendEntries path, where the etcd Txn is serialized as a flat RESP-style blob, not here.)

### 5. REV-CF key — `0x02 ‖ rev16` (PLAIN, ascending)

Watch replays **oldest → newest**, so this index stores **plain** (non-inverted) `rev16` ascending (POC: `1.0 < 1.1 < 2.0 < 100.0`). The value is an **event record**:

```
event :=
  u8     kind         ; 0 = PUT, 1 = DELETE
  u64be  key_len
  bytes  key
  u64be  value_len
  bytes  value        ; new value on PUT; empty on DELETE
  u64be  mod_revision ; = the rev16.main of this event (redundant-but-handy)
```

- **(c) all events in `(compactRev, readRev]`** — a forward range scan `[0x02‖rev16(compactRev, +1) , 0x02‖rev16(readRev)]`; because plain `rev16` is ascending and monotone, this is one contiguous ordered scan delivering events in exactly the order Watch must stream them (POC: "compact 5.0 < event 5.1", "event 9.3 < next 9.4"). A new Watch from `start_revision` seeks `0x02‖rev16(start_revision)`; live notification is layered on top by `.13`.

Every write op appends one event here, keyed by its own `main.sub`, so the REV index *is* the revision-ordered event log. (sub is preserved so a multi-op Txn's events keep their intra-Txn order.)

### 6. Tombstones

`DeleteRange` writes, for each matched key K, a **new KEY-CF version** `0x01‖len‖K‖INV(rev16)` whose record has `tag = TOMBSTONE` (`create_revision = 0`, `version = 0`, no value). Because INV makes it the newest (smallest) on-disk key for K:

- a read of K at any `readRev ≥ deleteRev` hits the tombstone first → returns **nothing**;
- the matching **REV-CF** event `kind = DELETE` lets Watch emit a `DELETE`.

`version` reset: a subsequent `Put` to a tombstoned K is a *create* — its record sets `create_revision = newRev` and `version = 1` (not "previous version + 1"), per etcd semantics. The apply-fn detects this by seeing the newest existing version is a tombstone (or no version exists).

### 7. Compaction (implemented by `cw-u4a.8`)

`Compact(rev)` is **physical removal of superseded history** at or below `rev`, recorded by a logical guard:

- **Logical:** set the `compact-rev` meta key to `rev` (in a group-commit batch). Reads with `readRev < compact-rev` return **`ErrCompacted`**; a Watch `start_revision < compact-rev` is rejected the same way. This is the *enforced* contract and is cheap/atomic.
- **Physical (KEY-CF):** for each key K, keep the **single newest version with rev ≤ `rev`** (the one a read at `rev` would see) plus every version `> rev`; `kv-del!` the older superseded versions. A key whose newest-version-at-`rev` is a tombstone *and* has no versions `> rev` is fully removed (the tombstone is collectible).
- **Physical (REV-CF):** `kv-del!` every event with `rev16.main ≤ rev` (a forward range scan from the `NS-REV` tag up to `0x02‖rev16(rev)`), since Watch can no longer start below `compact-rev`.

Physical deletes ride the same group-commit batch as the `compact-rev` bump. Logical-before-physical ordering means a crash mid-compaction is safe: `compact-rev` gates reads regardless of how much physical GC completed, and re-running compaction is idempotent.

### 8. Lease linkage (implemented by `cw-u4a.17`)

- The KeyValue record carries `lease` inline (field above), so a read returns the lease id with no extra lookup.
- A **lease→keys index** `0x03 ‖ u64be(leaseId) ‖ K → ()` makes revoke **O(keys-on-lease)**: a single `kv-scan` of prefix `0x03‖u64be(leaseId)` enumerates exactly that lease's keys (POC: lease 100 vs 101 group disjointly and order by id), each of which is then tombstoned. Attaching/detaching a lease adds/removes the corresponding index entry in the write batch.

## Consumers

| Task | Uses |
|---|---|
| `cw-u4a.6` MVCC apply-fn | current-rev/compact-rev meta keys (§2), KEY-CF writes + record (§3,§4), REV-CF event append (§5), tombstones (§6), lease index writes (§8); all in the existing group-commit batch + `ctx-save-applied!` |
| `cw-u4a.7` Range | KEY-CF read paths (a)/(b)/(c) (§3); tombstone skip (§6); `ErrCompacted` gate (§7) |
| `cw-u4a.8` Compaction | `compact-rev` meta key + physical GC of KEY-CF & REV-CF (§7) |
| `cw-u4a.12/.13/.15` Watch | REV-CF forward range `(compactRev, readRev]` (§5); event record (§5); `ErrCompacted` gate (§7) |
| `cw-u4a.17` Lease | inline `lease` field (§4); lease→keys index for O(lease) revoke (§8) |
| `cw-u4a.9` MVCC unit tests | every encoder/record above; mirrors `test/mvcc-encoding-poc.scm` |

## Alternatives considered

1. **Two real RocksDB column families (KEY-CF, REV-CF).** Cleaner physical separation and per-CF tuning. Rejected for v1 because the ported substrate hardcodes one CF per shard across all `kv-*` ops and `store-flush-wal` is DB-wide anyway — prefix tags give the same two orderings and the same one-fsync atomicity with zero substrate churn. Migrating to CFs later is transparent (CFs still share the DB WAL).
2. **ASCENDING revisions in KEY-CF (plain `rev16`, not INV).** Then "latest ≤ readRev" is a *reverse* scan or a seek-then-prev. Rejected: `kv-scan` only iterates forward; INV makes "newest first" a plain forward scan, so the answer is the first qualifying hit with the existing substrate.
3. **A single combined index keyed by `(rev, key)` or `(key, rev)` only.** Either Range or Watch then becomes a full scan + in-memory re-sort. Rejected — defeats the point of an ordered store; the dual-namespace cost is one extra small write per op.
4. **Protobuf-encoded records now.** No codec exists (`cw-u4a.19`), and coupling storage to it inverts the dependency. Rejected in favor of the length-prefixed blob (§4); a future migration is a record-version bump on the `tag` byte.
5. **Variable-length / varint revisions.** Smaller keys, but varints are **not** order-preserving under lexicographic compare, breaking the whole scheme. Rejected — fixed-width `u64be` is mandatory for sort correctness.

## Consequences

**Positive**
- Range, point reads, read-at-revision, Watch replay, and lease revoke are each a **single bounded ordered scan** — validated by `test/mvcc-encoding-poc.scm` (37/37).
- All encoders reuse the existing `encoding.scm` helpers; no new Rust, no protobuf dependency, no substrate changes.
- Revision + records + applied-index commit atomically under the existing group-commit fsync, inheriting crab-cache's Jepsen-validated crash-consistency.

**Negative / trade-offs**
- Every write op does **two namespace writes** (KEY + REV) plus possible meta/lease writes — more write amplification than a non-MVCC store (intrinsic to etcd semantics).
- History grows until `Compact`; unbounded without it (etcd has the same property; `.8` addresses it).
- `kv-scan` materializes a whole prefix group into a Scheme list, so a key with pathologically many uncompacted versions, or a huge Range, costs memory proportional to the group/range size. The INV layout supports a future streaming `kv-seek` (Decision §3 (a)) to bound the per-key cost to one record.
- Prefix tags share one CF, so the two namespaces can't be tuned/compacted independently at the RocksDB level (acceptable; revisited only if a CF migration is warranted).

## Crash-consistency (atomic revision + records + applied-index)

This is why MVCC writes are safe under `kill -9`:

1. The apply-fn runs **inside** the actor's group-commit batch for one committed Raft entry (= one Txn). For that entry it issues, all with `sync=#f` (immediate to the shared WAL buffer, no per-write fsync):
   - the KEY-CF record write(s) (§3,§4),
   - the REV-CF event append(s) (§5),
   - the `current-rev` (and on compaction, `compact-rev`) meta-key update (§2),
   - any lease-index writes (§8),
   - **`ctx-save-applied!`** — the new Raft applied-index+term (the substrate already does this in the same batch).
2. The actor then issues **one `ctx-flush!` → `store-flush-wal db #t` = one fsync**, which durably persists *every* write since the last flush. Because the WAL is shared DB-wide, **all of the above land or none do** — a single atomic durability barrier.
3. A waiter is never acked until that fsync returns (the existing group-commit ack gate). So a client `PUT` is acknowledged only once its record, its Watch event, the bumped revision, and the applied-index are **all** durable together.
4. On restart: the actor reads the applied-index via `ctx-load-applied` and the apply-fn reads current-rev/compact-rev from the meta keys (the same persisted batch). The Raft log replays only entries **above** the applied-index, so no revision is re-allocated and no record/event is double-written — recovery and rejoin are idempotent, exactly as in the ported crab-cache substrate.

A crash *between* step 1 and step 2 loses the entire un-fsync'd batch atomically (the WAL never recorded a partial Txn as durable), and Raft re-delivers that committed entry on rejoin → it re-applies cleanly. There is no window in which the revision counter advances on disk without its records, or records exist without the matching applied-index.
