# ADR 0002 — Watch subsystem

- **Status:** Accepted
- **Date:** 2026-06-09
- **Tracking:** `cw-u4a.12` (this ADR), implemented by `.13` (Watch backend: event log + registry), `.14` (per-conn streaming actor: replay+live), `.15` (Watch tests), `.23` (gRPC `Watch` bidi stream).
- **Builds on:** [ADR 0001 — MVCC data model](0001-mvcc-data-model.md). REV-CF (`0x02 ‖ rev16` → event record, §5 there) is Watch's source of truth; this ADR adds the Watch semantics on top of it.
- **Validated by:** [`test/watch-replay-poc.scm`](../../test/watch-replay-poc.scm) — 19 assertions that the **historical-replay query** (`mvcc-watch-events`, the foundation of §3) returns exactly the right events in strict revision order over the REAL REV-CF log written by `mvcc-apply`, including ErrCompacted. All green (twice back-to-back). The **live half** (§3 handoff, registry, streaming actor) is implemented and tested by `.13`/`.14`/`.15`.

## Context

crab-watchstore is an etcd v3 API-compatible store. **Watch** is the etcd feature a client uses to receive a stream of mutations to a key or key-range, *in revision order*, starting from a caller-supplied `start_revision`. It is the backbone of every etcd consumer (Kubernetes informers, leader election, config reload): correctness here means a watcher **never misses an event and never sees one twice**, even across the boundary between historical catch-up and live streaming.

etcd's reference implementation is `watchableStore` (mvcc/watchable_store.go), which classifies every watcher as **`unsynced`** (still catching up from history) or **`synced`** (caught up, receiving live events as they are applied). This ADR specifies crab-watchstore's version of that model **over the green-actor architecture** and on the **already-built MVCC substrate** of ADR 0001.

The substrate gives us exactly what Watch needs and nothing it must fight:

- **REV-CF** (`0x02 ‖ rev16` → event record, ADR 0001 §5) is a **revision-ordered, append-only event log**: every Put/DeleteRange op appends one event keyed by its own `main.sub`, PLAIN-ascending, so a forward scan yields events in precisely Watch's stream order (oldest→newest, intra-Txn `sub` order preserved). The decode + accessors already exist in `src/mvcc.scm`:
  - `event-decode` → `#(kind key value mod-rev)`; accessors `ev-kind` (`EV-PUT`=0 / `EV-DELETE`=1), `ev-key`, `ev-value`, `ev-mod-rev`.
- **KEY-CF** (`0x01 ‖ len ‖ K ‖ INV(rev16)` → KeyValue record, ADR 0001 §3/§4) holds the **full KeyValue** for any key at any revision; `mvcc-get-latest ctx K [at-rev]` resolves the visible version at-or-below a revision (skips tombstones). Record accessors: `kv-rec-create-rev`, `kv-rec-mod-rev`, `kv-rec-version`, `kv-rec-lease`, `kv-rec-value`, `kv-rec-tombstone?`.
- **Revision counters**: `mvcc-current-rev` (advances once per applied Txn), `mvcc-compact-rev` (the read/replay floor).
- **The apply seam**: `mvcc-apply ctx cmd` runs *inside* the shard actor's group-commit batch (`src/server/shard-actor.scm`, `apply-fn`) and is the single point where every event is produced. **Watch notification must hang off this exact point** — there is no other place an event comes into existence.

### What this ADR must pin down

The hard part is **§3, the gap-free replay→live handoff**: a watch from `start_revision ≤ current` must replay `(start_revision, current]` from REV-CF and then stream live events with **no gap and no duplicate** across the transition — including events applied *concurrently* with the replay. Everything else (watcher model, response shape, event sufficiency, notification path, ErrCompacted, progress, fragmentation, cancellation) is in service of getting that seam right on the actor model.

## Decision

### 1. Watcher model and WatchResponse shape

A **Watcher** is the unit a client creates with a `WatchCreateRequest`. crab-watchstore models it as a record carried by the per-connection streaming actor (`.14`):

| Field | Meaning |
|---|---|
| `watch_id` | server-assigned (or client-supplied) id; scopes every response and the `WatchCancelRequest`. Unique **per stream**. |
| `key` | the watched key (start of range). |
| `range_end` | range semantics **identical to Range/`mvcc-range`** (ADR 0001 §3, reused here verbatim): `#f`/empty ⇒ single key; `#u8(0)` ⇒ to-end-of-keyspace; `key=#u8(0)` + `range_end=#u8(0)` ⇒ all keys; **prefix** = `key` + `range_end = prefix-range-end(key)` (the last non-`0xFF` byte incremented). One matcher (`range-in-range?`) serves Watch and Range so they can never disagree. |
| `start_revision` | first revision the client has **not** seen. `0` ⇒ **current/future-only** (no historical replay; created `synced`). `>0` ⇒ replay `(start_revision, current]` then go live. Per etcd, this bound is **exclusive** at the low end (start at `start_revision`, deliver revs `> start_revision`)… see note. |
| `filters` | a set of `NOPUT` / `NODELETE` — drop PUT / DELETE events respectively at the source. |
| `prev_kv` | when set, each event also carries the KeyValue **as of the revision immediately before this event** (`prev_kv`); see §2. |
| `progress_notify` | when set, the server periodically emits an **empty** WatchResponse carrying only the current `header.revision` so an idle watcher can checkpoint progress (§6). |
| `fragment` | when set, the server may split one logical WatchResponse across multiple frames to respect the gRPC max message size (§6). |

> **`start_revision` boundary convention.** etcd delivers events with `mod_revision >= start_revision`; clients resume by setting `start_revision = lastSeen + 1`, so in practice the first delivered event has `mod_revision > lastSeen`. crab-watchstore's replay query treats `start_revision` as an **exclusive** lower bound on `mod_revision` (deliver `mod_rev > start_revision`) — i.e. the caller passes the *last revision already seen*. `.14`/`.23` adapt the etcd-wire `start_revision` to this internal contract by passing `start_revision - 1` (the gRPC field is inclusive). The POC and `mvcc-watch-events` use the internal exclusive form. This is a single, documented `±1` at the wire boundary; pinning it here prevents an off-by-one that would drop or duplicate the boundary revision.

A **WatchResponse** (one frame to the client stream) carries:

| Field | Source |
|---|---|
| `watch_id` | the watcher it belongs to. |
| `header.revision` | the store's `current-rev` **at the moment this response was produced** (the watcher's high-water mark / progress point). |
| `events[]` | zero or more `Event{type, kv, prev_kv?}` (§2), **revision-ascending**. |
| `created` | `#t` on the first response acknowledging a `WatchCreateRequest` (etcd sends an immediate empty created-response with `header.revision = current`). |
| `canceled` + `cancel_reason` | `#t` when the watch ends — explicit cancel, or compaction passing it (§5). |
| `compact_revision` | set alongside `canceled` when the cause is ErrCompacted, so the client knows the floor to re-establish above (§5). |
| `fragment` | `#t` on every non-final frame of a fragmented logical response (§6). |

### 2. Event sufficiency — keep the lean event, reconstruct at delivery (option b). **Recommended.**

An etcd `Event` needs the **full new KeyValue** — `key, create_revision, mod_revision, version, value, lease` — plus, optionally, `prev_kv`. The current REV-CF event record (ADR 0001 §5) carries **only** `kind, key, value, mod_rev`. It is therefore **NOT sufficient** on its own: it is missing `create_revision`, `version`, and `lease`.

Two ways to close the gap:

- **(a) ENRICH the REV-CF event** to store the full KeyValue inline (add `create_rev`, `version`, `lease` to `event-encode`/`event-decode`). Delivery becomes a pure REV-CF read — fastest, fewest lookups. **Cost:** it changes the on-disk *write* format produced by `mvcc-apply` (`src/mvcc.scm` / cw-u4a.6) and would need a record-version bump and migration. It also still cannot carry `prev_kv` (that is the *previous* version, inherently a second lookup).
- **(b) KEEP the lean event and RECONSTRUCT** the full KeyValue + `prev_kv` by KEY-CF lookups at delivery time. **No format change.** For an event at `(K, modRev)`:
  - **new KeyValue** = `mvcc-get-latest ctx K modRev` — the visible version at exactly this event's revision gives `create_rev`, `version`, `lease`, `value` (for a DELETE the event itself supplies the tombstone fact; the KeyValue is the tombstone with `version=0`).
  - **`prev_kv`** = `mvcc-get-latest ctx K (modRev - 1)` — the version visible immediately **before** this event's revision (or absent if `K` was created at `modRev`). This is the precise etcd `prev_kv` semantics: the value the key held just before the change.

  **Cost:** one (or two, with `prev_kv`) extra KEY-CF point reads per delivered event. Each is a single bounded prefix scan (ADR 0001 §3 (a)).

**Decision: adopt (b) for the MVP** — it requires **zero change to the validated write path** and to ADR 0001's event format, keeps this task a pure additive read, and `prev_kv` *needs* the second lookup under either option anyway. **Enriching the event (a) is recorded as a deliberate `.13` follow-up / perf option**: if delivery-time reconstruction shows up as a hotspot under load, `.13` may add the full-KeyValue fields to the REV-CF record behind a record-version bump (the `kind` byte is the version hook, exactly as ADR 0001 §4 anticipates for KeyValue records). Until then, the lean event + KEY-CF reconstruction is the contract. *(This task does not change `event-encode`; it only adds the pure-read `mvcc-watch-events`.)*

### 3. THE CRUX — gap-free replay→live handoff (the `unsynced`→`synced` model)

A watch with `start_revision ∈ (compact_rev, current]` must deliver, **exactly once and in revision order**, every in-range event with `mod_rev > start_revision` — both the ones already on disk *and* the ones applied **while replay is running**. The mechanism, crab-watchstore's actor-native rendering of etcd's `unsynced`→`synced`:

```
REGISTER-BEFORE-REPLAY   The streaming actor (.14) registers the watcher in the
  (close the seam first)  shard's watch registry as UNSYNCED, recording
                          delivered_rev = start_revision, BEFORE issuing any replay
                          read.  From this instant, apply-side notification (§4) for
                          a NEW live event does NOT push to this watcher; it only
                          advances a marker / is buffered (see DISPATCH).  Registering
                          first is what guarantees no live event can slip through the
                          gap between "finished replay" and "started listening".

REPLAY-TO-SNAPSHOT       Read snap_rev := current-rev once, then run the replay query
                          mvcc-watch-events(ctx, delivered_rev, key, range_end,
                          filters) and stream the result.  After delivering an event
                          at rev r, set delivered_rev := r.  Replay covers
                          (start_revision, snap_rev].

CATCH-UP LOOP            current-rev may have advanced past snap_rev during replay
  (drain the tail)        (new applies).  While delivered_rev < current-rev, re-run
                          the replay query from delivered_rev to the new current-rev
                          and deliver, advancing delivered_rev each time.  Each pass
                          strictly shrinks the gap (REV-CF is append-only and
                          monotone), so it terminates.

PROMOTE-AT-BOUNDARY      When delivered_rev == current-rev with NO unreplayed event
                          in flight, atomically (on the registry-owner actor's single
                          thread) flip the watcher to SYNCED.  The boundary is the
                          watcher's own delivered_rev: it has seen everything <=
                          delivered_rev exactly once.

LIVE                      A SYNCED watcher receives each live event from §4 iff
                          ev.mod_rev > delivered_rev, then advances delivered_rev :=
                          ev.mod_rev.  The strict ">" is the de-dup guard: any event
                          already delivered by replay/catch-up (rev <= delivered_rev)
                          is dropped, so the seam is exactly-once on both sides.
```

**Why there is no gap and no duplicate.** The registry-owner is a **single green actor** (the shard actor, §4) — registration, the `delivered_rev` marker, and the apply-side dispatch all run on **one thread**, so "register as unsynced" and "an event is applied" are strictly serialized; there is no concurrent window. An event with `rev = r` is delivered by **exactly one** of {replay query (if `r ≤` the catch-up's last snapshot), live dispatch (if `r >` `delivered_rev` at promotion)} because the single `delivered_rev` high-water mark is the **sole** boundary both halves test against (`replay: r > start..delivered`; `live: r > delivered`) and it advances monotonically through the same value. The catch-up loop closes the snapshot-skew (events applied during replay) before promotion, so the live side never has to reach below `delivered_rev`. This is exactly etcd's `unsynced`→`synced` promotion, with the `synced` boundary computed as **the watcher's delivered-revision high-water mark** rather than a global one.

> **DISPATCH variant (what an unsynced watcher does with a concurrent live event).** Two equivalent realizations, both safe because the registry-owner is single-threaded:
> - **(i) Marker-only (chosen for MVP):** while `unsynced`, §4 does **not** deliver to the watcher at all; the catch-up loop is what drains everything up to `current-rev`. Simplest; correct because the loop re-reads REV-CF (the durable truth) right up to the promotion instant.
> - **(ii) Buffer-and-merge:** while `unsynced`, §4 appends the live event to a small per-watcher buffer; at promotion the buffer is merged after the replayed tail, de-duped by `mod_rev > delivered_rev`. Avoids the final re-scan. `.13` may adopt (ii) if the re-scan is measurable; the contract (exactly-once, ordered) is identical.

`mvcc-watch-events` (added in this task, validated by the POC) is the **replay query** used by REPLAY-TO-SNAPSHOT and the CATCH-UP LOOP. Its exactly-the-right-events-in-revision-order behavior over the real log is what makes the whole handoff sound; the POC proves that half. The live half (registry, the single-threaded dispatch, promotion, the `>` de-dup) is `.13`/`.14`, tested by `.15`.

### 4. Notification path — apply-side dispatch off `mvcc-apply`, registry on the shard actor

Events come into existence in exactly one place: `apply-fn` → `mvcc-apply` in `src/server/shard-actor.scm`, inside the group-commit batch. The notification path hangs off that point:

- **Where the registry lives — the shard actor.** The watch registry (the set of active watchers, keyed by `watch_id`, with each watcher's `key`/`range_end`/`filters`/`delivered_rev`/sync-state) is owned by the **shard actor** that owns the MVCC state. It is the natural home because (a) it is the single thread on which events are produced, so dispatch needs no locking and the §3 seam is automatically race-free; (b) only the **leader** applies and acks, so only it notifies — followers apply for durability but have no client watchers. The per-connection **streaming actors** (`.14`) are the *consumers*: each registers its watcher(s) with the shard registry and receives delivered events on its mailbox, then frames them onto its gRPC stream. This fits the green-actor model directly — registry-owner ↔ streaming-actor is the same mailbox `send` pattern the shard already uses for client acks.
- **Dispatch (MVP): per-event linear scan of active watchers.** After `mvcc-apply` produces the batch's events (it already returns the applied result; `.13` extends the apply seam to also surface the concrete events, or re-reads them from REV-CF for the just-applied `(old-rev, new-rev]` window — a bounded scan), the registry-owner iterates active **synced** watchers and, for each whose `range-in-range?` matches the event key and whose filters pass, `send`s the event to that watcher's streaming actor (which then does §2 reconstruction + framing). `unsynced` watchers are handled per §3 DISPATCH. **A linear scan over active watchers per event is acceptable for the MVP** and is explicitly called out as the first optimization target.
- **Future: interval-tree / range index.** etcd indexes watchers by key-interval so an event dispatches to only the overlapping watchers in `O(log n + matches)`. crab-watchstore can add the same (an interval tree, or a prefix trie for the common prefix-watch case) behind the identical `range-in-range?` predicate. **Noted as future work**, not in the MVP.

### 5. ErrCompacted

Compaction (ADR 0001 §7, `mvcc-compact`) sets `compact-rev` and physically GCs REV-CF events `≤ compact_rev`. Watch respects this floor on both creation and mid-stream:

- **At creation:** `start_revision > 0 && start_revision < compact_rev` ⇒ the watch **cannot be served historically** (the events are gone). The server replies a **canceled** WatchResponse with `compact_revision = compact_rev` set, and the watch is **not** established. The client must re-establish above `compact_rev` (typically by doing a fresh Range at `compact_rev` to rebuild state, then watching from there). `mvcc-watch-events` returns `(cons 'err-compacted compact-rev)` for this case — **validated by the POC** (`start_revision 1 < compact 3 → err-compacted 3`; `start_revision 0` and `start_revision == compact_rev` are *not* rejected).
- **Mid-stream (compaction passes a running watch):** if a `Compact(rev)` is applied while a watch's `delivered_rev < rev` and the watch is still `unsynced` (replaying below `rev`), its remaining historical events have been GC'd. The registry-owner detects `compact_rev > delivered_rev` for an unsynced watcher and **cancels it** with `compact_revision = compact_rev` — same client recovery. A **synced** watcher is, by definition, at `delivered_rev == current-rev ≥ compact_rev` (compaction never exceeds current), so it is never affected — it keeps receiving live events normally.

### 6. progress_notify, response fragmentation, cancellation

- **`progress_notify`.** A watcher may be idle for a long time (no in-range mutations) yet the client wants to know the stream is alive and where it is, so it can resume without replaying from stale. When set, the server periodically (etcd: ~every 10 min, or on demand via `WatchProgressRequest`) emits an **empty** WatchResponse (`events = []`) with `header.revision = current-rev`. crab-watchstore drives this from the shard actor's existing **tick** (the same heartbeat that already bounds ack latency): on a tick, any `synced` watcher with `progress_notify` and no events since its last progress gets an empty response advancing its `header.revision` to `current-rev`. Cheap; reuses the tick the actor already runs.
- **Response fragmentation (`fragment`).** A single revision can touch many keys (a large `DeleteRange`, or a Txn with many ops), so one logical WatchResponse's `events[]` can exceed the gRPC **max message size** (default 4 MiB). When the watcher set `fragment`, the server splits the response into multiple frames **on event boundaries**, every non-final frame flagged `fragment = #t`, the final frame `fragment = #f` (or unset); the client reassembles by `watch_id` until it sees an unflagged frame. All fragments of one logical response **share `header.revision`** so the client treats them as one atomic revision. `.23` (the gRPC binding) owns the byte-size accounting against the configured max-message-size; `.14` produces the ordered event list it fragments.
- **Cancellation.** A client sends a `WatchCancelRequest{watch_id}` (multiplexed on the same bidi stream). The streaming actor removes the watcher from the shard registry and replies a WatchResponse with `canceled = #t` for that `watch_id`. Server-initiated cancels (ErrCompacted §5; stream teardown when the gRPC connection drops — the streaming actor exits and deregisters all its watchers) take the same path. Because deregistration runs on the registry-owner's single thread, a cancel concurrent with an in-flight dispatch is serialized — no use-after-cancel.

### 7. Consumers

| Task | Implements / uses |
|---|---|
| `cw-u4a.13` Watch backend (event log + registry) | The **watch registry** on the shard actor (§4); the **apply-side dispatch** off `mvcc-apply` (§4); the §3 `unsynced`→`synced` machinery incl. the catch-up loop + `delivered_rev` high-water mark + the `>` de-dup; uses `mvcc-watch-events` (this ADR, §3) for replay/catch-up; the §2 KeyValue + `prev_kv` reconstruction via `mvcc-get-latest`; the ErrCompacted gate (§5). **May** adopt the event-enrichment option (§2 (a)) and/or buffer-and-merge dispatch (§3 (ii)) if profiling warrants — both are recorded here as deliberate options, not requirements. |
| `cw-u4a.14` streaming actor (per-conn replay+live) | The **per-connection green streaming actor**: registers watcher(s) with the shard registry, runs REGISTER→REPLAY→CATCH-UP→PROMOTE→LIVE (§3), frames `WatchResponse`s (§1), drives `progress_notify` + fragmentation (§6), handles `WatchCancelRequest` (§6). Adapts the etcd-wire inclusive `start_revision` to the internal exclusive contract (§1 note). |
| `cw-u4a.15` Watch tests | The **live half** §3 proves end-to-end: concurrent-apply-during-replay delivers exactly once (the gap-free seam), de-dup at the boundary, `prev_kv`, NOPUT/NODELETE live, mid-stream ErrCompacted cancel (§5), `progress_notify`, fragmentation, cancellation. Mirrors the structure of `test/watch-replay-poc.scm` (which already locks the historical half). |
| `cw-u4a.23` gRPC `Watch` bidi stream | The **`Watch` RPC**: the bidirectional stream multiplexing `WatchCreate`/`WatchCancel`/`WatchProgress` requests and `WatchResponse`s by `watch_id` (§1); the max-message-size accounting for fragmentation (§6); the inclusive→exclusive `start_revision` boundary adaptation (§1 note). Sits on top of `.14`'s streaming actor. |

## Alternatives considered

1. **Enrich the REV-CF event with the full KeyValue now (§2 (a)) instead of reconstructing.** Faster delivery (pure REV-CF read, no KEY-CF lookup). Rejected for the MVP because it changes the validated `mvcc-apply` write format (a record-version bump + migration), couples this Watch-design task to a storage-format change it was told **not** to make, and still cannot supply `prev_kv` without a second lookup. Kept as an explicit `.13` perf option behind the `kind`-byte version hook.
2. **Replay entirely from KEY-CF (per-key version history) instead of REV-CF.** KEY-CF can reconstruct any key's history, but it is **key-ordered**, so assembling a *revision-ordered* multi-key stream means scanning every key in range and merge-sorting by `mod_rev` in memory — exactly the re-sort REV-CF exists to avoid (ADR 0001's two-ordering rationale). Rejected; REV-CF is the revision-ordered log by construction.
3. **A global `synced` revision shared by all watchers (single high-water mark).** Simpler bookkeeping, but a slow/large watch would hold back the global boundary for fast watchers, and a new watch joining at an arbitrary `start_revision` does not fit one global mark. Rejected in favor of **per-watcher `delivered_rev`** — each watcher's boundary is independent, which is also what makes the §3 de-dup a purely local `>` test.
4. **Dispatch via a separate broker actor (not the shard actor).** A dedicated watch-broker would decouple Watch from the shard, but then "an event is applied" and "the broker learns of it" cross a thread boundary, **re-introducing the exact race §3 is designed to eliminate** (a live event could be produced between the broker's replay-finished and listen-started). Rejected: co-locating the registry with the single apply thread is what makes the seam race-free for free. (The *streaming* actors are separate, per-connection — that boundary is fine because they only consume already-ordered `send`s.)
5. **Interval-tree watcher index from day one.** The right asymptotic answer for many watchers, but premature for the MVP and orthogonal to correctness (it changes only *which* watchers a per-event scan considers, behind the same `range-in-range?` predicate). Deferred to a later perf task; the linear scan is correct and simple now (§4).

## Consequences

**Positive**
- The historical-replay foundation (`mvcc-watch-events`) is **proven over the real REV-CF event log** by `test/watch-replay-poc.scm` (19/19, twice back-to-back): single-key, prefix/range, from-mid-revision, NOPUT, NODELETE, and ErrCompacted all return exactly the right events in revision order.
- **Zero write-format change** this task: Watch is a pure additive read over the existing event log; ADR 0001's `mvcc-apply` and its Jepsen-inherited crash-consistency are untouched. The one source edit is the pure read `mvcc-watch-events`.
- The §3 seam is **race-free by construction**: co-locating the registry + `delivered_rev` marker + apply-dispatch on the shard's single green thread means "register unsynced" and "apply an event" are serialized, so the gap-free, exactly-once handoff needs **no locks**.
- One `range-in-range?` matcher and one `rev16` ordering serve Range **and** Watch, so the two can never disagree on what is in a range or in what order.

**Negative / trade-offs**
- Delivery does **1–2 extra KEY-CF point reads per event** (§2 (b): full KeyValue, and `prev_kv` when requested). Bounded each by a single prefix scan, but real amplification under high event rates — the `.13` enrichment option (§2 (a)) exists for when this bites.
- The MVP dispatch is a **per-event linear scan of all active watchers** (§4): `O(watchers)` per applied event. Fine for modest watcher counts; the interval-tree index (deferred) is required before very large fan-out.
- The catch-up loop's marker-only DISPATCH (§3 (i)) **re-scans REV-CF** for the `(snap_rev, current]` tail before promotion. Correct and simple, but redundant reads under a steady write stream during replay; buffer-and-merge (§3 (ii)) is the noted remedy.
- Watch is served **only by the leader** (it alone applies + holds client streams). A leadership change cancels in-flight watches on the old leader (its streaming actors tear down); the client reconnects and re-establishes from its last-seen revision — standard etcd client behavior, but it does mean a watch is not transparently HA across failover.
