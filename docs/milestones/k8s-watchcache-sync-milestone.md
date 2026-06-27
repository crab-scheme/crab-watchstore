# Milestone: stable watchCache sync — crab-watchstore backing a full k8s cluster

**Status:** open. Core capability proven; last-mile not closed.
**Tracking issue:** `cw-l5h`.
**Owner note:** this is focused-session work, not a 15-min loop iteration. The `/loop`
that produced the fixes below was stopped 2026-06-27 after ~23 iterations / ~18 AWS e2e
cycles confirmed the remaining gap is a single, well-characterized watch-protocol problem.

---

## What already works (do not re-litigate)

11 committed fixes took crab-watchstore from "boots and immediately wedges under a kube-
apiserver" to **a real apiserver reaching `readyz=OK` and registering a node** (once, not
stably). The `current: 1` stuck-watchCache saga that blocked everything for ~9 iterations is
fully solved. Highlights, all committed on `fix/cw-6m8-server-leaks`:

- raft append/match clamps (shard-main crash under WAN), WAN election profile
- `cs-diag` bounds-checked SourceMap accessors (cross-actor Diagnostic panic killed ~20 actors/run)
- O(W)→indexed watcher registry (`watch.scm`, key-range index; regression guard `watch-scale-bench.scm`)
- async Txn batching path (`grpc-kv.scm`)
- watch-created carries accurate commit rev; per-wid progress; non-blocking `do-progress`
- **periodic ProgressNotify push** (`watch-progress-all!` from the shard tick) — the breakthrough
  that solved `current: 1`; later made every-tick (`shard-actor.scm` tick branch)

etcd Watch+Lease conformance: `test/etcd-watch-lease-grpc.sh` 18/18.

## The exact remaining gap

Under **continuous write load**, a per-resource apiserver watchCache cannot *stably* reach
`synced`. Observed: store at rev ~205, the resource's cache stuck at ~203 (a ~2-rev lag);
`stats.go` logs `Too large resource version: 205, current: 203`; with `v=6`, the affected
informers are `resource.k8s.io/v1` (resourceslices/resourceclaims) on `sendInitialEvents=true`,
retrying on an **exact 3.00s timeout loop** — the watchCache never emits the
`k8s.io/initial-events-end` bookmark because it never marks synced. Result: apiservice/discovery
poststarthooks don't complete → no stable node-token → agents 401 → cluster doesn't form.

### The k8s contract being violated (consistent-list-via-watch, 1.31+)

1. Apiserver does a consistent **LIST** (etcd `Range`) → gets `header.revision = R` (global).
2. It opens a **watch from R** for that resource and waits for any frame (event or progress)
   with `rev >= R` **that is consistent** — i.e. it must have received every event `<= rev`
   for that resource before it will advance the cache to `rev`. Only then: mark synced, emit
   the initial-events-end bookmark.
3. If no consistent frame `>= R` arrives within ~3s, it times out and re-lists. Forever.

### Why crab-watchstore loses the race (hypothesis to confirm first)

`watch-progress-all!` pushes the **global current rev** to every synced watcher each tick.
For an **idle** resource (no events in range) that is correct and sufficient. The suspected
failure is on **active** resources: a progress at global rev N is only valid if the watcher
has delivered all of *its* events `<= N`. Today the push reports the global current regardless
of whether this watcher's matching events up to that rev were emitted by its worker yet — under
load the worker's emit lags the shard's apply, so the pushed progress can momentarily claim a
rev for which an event is still in flight. The apiserver then refuses to advance (it would skip
an event), and on re-list (`do-create`) the watcher is briefly unsynced and gets no push at all.

**This is a hypothesis.** It has not been disambiguated from a simpler LIST-rev/watch-rev
coordination bug. Step 0 below settles it before any code changes.

---

## Plan

### Step 0 — disambiguate (measure, don't guess). ~1 e2e cycle.

For ONE stuck resource (e.g. `resourceslices`), capture simultaneously:
- the `R` the apiserver LISTs at (apiserver `v=6`, the `Range` response header rev), and
- the exact rev sequence the watch stream delivers for that resource (instrument `emit-wr!`
  in `grpc-watch.scm` to log `wid`, frame rev, created/progress flag).

Two outcomes:
- **(A)** the watch *does* deliver a frame `>= R` but the apiserver still re-lists → it's a
  **consistency** rejection (active-resource progress claiming undelivered events) → fix = Step 1.
- **(B)** the watch *never* delivers `>= R` → it's a **coverage/coordination** bug (the push
  rev or LIST rev is wrong, or the worker is stuck mid-reestablish) → fix = Step 2.

### Step 1 — couple each watcher's progress rev to its own delivered_rev (if outcome A)

Make a watcher's reported progress rev = the global store rev up to which it has delivered
**all matching events**, exactly per etcd. Mechanically: advance a per-watcher `caught_up_rev`
inside `watch-on-apply!` for **every** write — matching watchers after delivering the event,
non-matching watchers immediately (no event owed) — then have `watch-progress-all!` /
`do-progress` report `caught_up_rev`, never the bare global current.

- `src/watch.scm`: add `caught_up_rev` to the watcher record; set it in `watch-on-apply!` /
  `watch-dispatch-live!`; `watch-progress-all!` reads it instead of `mvcc-current-rev`.
  **Cost watch:** advancing non-matching watchers per-write reintroduces O(W) per apply — the
  exact thing the key-range index removed. Keep it O(matching) by tracking a single global
  `applied_rev` and reporting `min(applied_rev, …)` per watcher; only watchers with a pending
  in-range event hold below `applied_rev`. Re-run `test/watch-scale-bench.scm` to prove flat.
- `src/server/grpc-watch.scm`: `do-progress` and the `cur-rev-ok` handler emit `caught_up_rev`.

### Step 2 — coordinate the LIST response rev (if outcome B, or as belt-and-suspenders)

Ensure the rev returned by a consistent `Range` is one the watch is guaranteed to satisfy
promptly: return the latest rev for which a progress has already gone out (the "stable" rev),
not the absolute newest write. `src/server/grpc-kv.scm` Range/List response header. **Correctness
caveat:** a consistent LIST must not return a rev *older* than a write the client already
observed — bound the stable rev below by any compaction floor and never below a previously
returned rev. This is the riskier change; prefer Step 1 if Step 0 says A.

### Step 3 — kill the re-establish gap

On re-list, `do-create` re-replays and the watcher is unsynced (gets no push) until
`watch-created`. Under churn this is most of the time. Make re-establish O(1) when the new
`start_rev` is at/above the watcher's existing `delivered_rev` (no full replay), and emit the
created-ack + first progress in the same frame so there's no unsynced window. `grpc-watch.scm`
`do-create` + `watch.scm` `watch-register!`/`watch-replay-to-current!`.

### Verification

- Unit: extend `test/etcd-watch-lease-grpc.sh` with a concurrent-writes-during-watch case that
  asserts a progress frame arrives at-or-above a just-listed rev within 1s.
- `test/watch-scale-bench.scm` must stay flat (Step 1 must not reintroduce O(W)).
- e2e: ONE clean run — fresh crab-watchstore db + fresh k3s data-dir (see deploy note) — assert
  5/5 nodes Ready and `readyz=ok` sustained for 60s.

---

## Deploy/e2e gotchas (cost real time across the 23 iterations — don't repeat)

- **Token extraction:** never `tail` the readyz-loop output into the token var — "READYZ OK try6"
  bled into the agent `--token` for several false 0/5s. Read the token in its own ssh call.
- **Data-dir drift:** repeated in-place `sed` on `k3s-server.service` left data-dir vs datastore
  mismatches (old certs against a fresh db) → false 0/5s. Always use a **fresh** data-dir name AND
  a **fresh** crab-watchstore `--db` together for each crab-watchstore e2e; reset cleanly.
- The standalone kube-apiserver (iter 2) synced fine; the bug only appears under **full k3s load**.
  Reproduce with k3s, not the standalone apiserver.
- Restore-to-embedded recipe (working): strip `--datastore-endpoint` + `--kube-apiserver-arg=v=6`,
  fresh `--data-dir`, restart server, re-fetch token, point agents at the same data-dir+token.
  Embedded demo is currently up at 5/5 on `k3s-emb3`.

## References

- Files: `src/watch.scm`, `src/server/grpc-watch.scm`, `src/server/grpc-kv.scm`,
  `src/server/shard-actor.scm`, `src/mvcc.scm`.
- k8s: storage/cacher consistent-list-via-watch; `WatchList`/`sendInitialEvents`;
  `k8s.io/initial-events-end` bookmark; apiserver storage `etcd3/watcher.go` + `cacher`.
