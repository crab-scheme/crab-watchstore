; test/lease-poc.scm — validates the LEASE->KEYS INDEX foundation that the Lease
; design (ADR 0003) builds on, over the REAL .6 write machinery (mvcc-apply +
; mvcc-put!/mvcc-delete-range!), the same way watch-replay-poc.scm validates the
; REV-CF event log.
;
; The lease subsystem (cw-u4a.16 ADR; .17 expiry/revoke; .18 keepalive) hangs the
; whole revoke path off ONE invariant: scanning the 0x03 || u64be(leaseId) prefix
; returns EXACTLY the keys currently attached to that lease.  When the leader-driven
; expiry proposes ("LEASE-REVOKE" id) through Raft, every replica's apply must
; enumerate that prefix and tombstone precisely those keys (and no stale ones) at
; one revision.  This POC asserts that invariant holds across the four ways a key
; enters/leaves a lease index, all through the validated .6 write path:
;
;   - attach (PUT k v L)        -> key appears under L's prefix
;   - reattach (PUT k v L2)      -> key LEAVES L1, appears under L2 (no stale L1 — .6
;                                  stale-removal)
;   - detach (PUT k v 0)         -> key removed from its lease prefix
;   - delete (DEL k)             -> tombstoned key removed from its lease prefix
;
; and the "revoke scan" itself: collecting all keys under 0x03||L1 yields exactly
; L1's current attached set — the iteration cw-u4a.17 performs on revoke.
;
; The only src/mvcc.scm change this task makes is the PURE READ helper
; mvcc-lease-keys (the revoke-scan); the lease WRITE path is untouched (that's .6).
; If this is green, the index foundation the Lease ADR (0003) specifies is sound
; over the real store.

(include "test/harness.scm")
(include "src/store-ctx.scm")
(include "src/mvcc.scm")
(include "test/mvcc-util.scm")

; ---- open a fresh store (unique per run; substrate has no system/rm -rf) ----
(define run-tag (number->string (current-jiffy)))
(define DB-PATH (string-append "/tmp/cws-lease-poc-" run-tag))
(define CTX (make-ctx (store-open DB-PATH #t) "default" #t))

; (current-jiffy) is process-relative, so back-to-back runs may reuse the same dir
; — empty the store before building any state so the assertions are deterministic.
(reset-ctx! CTX)

; ---- helpers ----
(define (b s) (string->utf8 s))
(define (put . parts) (mvcc-apply CTX (map b parts)))   ; ("PUT" "k" "v" ["lease"])
(define (del . parts) (mvcc-apply CTX (map b parts)))   ; ("DEL" "k" ["end"])

; The lease ids the test uses (decimal-ASCII when passed to mvcc-apply; integer
; when scanning, matching mvcc-apply's bytes->int parse of the 4th PUT arg).
(define L1 100)
(define L2 200)

; The revoke scan: the set of user-keys (as strings, SORTED for order-independent
; comparison) currently attached to lease `id`.  This is exactly what
; ("LEASE-REVOKE" id) will iterate in .17 — mvcc-lease-keys is the pure-read helper
; (the only src/mvcc.scm addition this task makes).  mvcc-lease-keys already returns
; on-disk (ascending-K) order; we sort defensively so the assertions are order-
; independent regardless of how the substrate iterates.
(define (lease-keys id)
  (list-sort string<? (map utf8->string (mvcc-lease-keys CTX id))))

; A key may only attach to a GRANTED lease (cw-u4a.17 put-to-dead-lease guard,
; etcd ErrLeaseNotFound) — grant L1/L2 up front.  A LEASE-GRANT does NOT bump the
; revision, so every revision assertion below is unchanged (rev 1 is the first PUT).
(put "LEASE-GRANT" "100" "60")    ; grant L1 (ttl 60), no rev bump
(put "LEASE-GRANT" "200" "60")    ; grant L2 (ttl 60), no rev bump

; ===========================================================================
(section "attach: 3 keys to lease L1 -> prefix scan returns exactly those 3")
; Each PUT k v L1 attaches k to L1.  (One mvcc-apply = one Raft entry = one rev.)
(put "PUT" "k1" "v1" "100")    ; rev 1, attach k1 -> L1
(put "PUT" "k2" "v2" "100")    ; rev 2, attach k2 -> L1
(put "PUT" "k3" "v3" "100")    ; rev 3, attach k3 -> L1
(check "current-rev advanced to 3" 3 (mvcc-current-rev CTX))
; The record carries the lease inline (the read returns it with no extra lookup).
(check "k1 record lease = 100" 100 (kv-rec-lease (mvcc-get-latest CTX (b "k1"))))
(check "k2 record lease = 100" 100 (kv-rec-lease (mvcc-get-latest CTX (b "k2"))))
(check "k3 record lease = 100" 100 (kv-rec-lease (mvcc-get-latest CTX (b "k3"))))
; THE INDEX: scanning 0x03||u64be(100) returns exactly {k1,k2,k3}.
(check "L1 index = {k1,k2,k3}" '("k1" "k2" "k3") (lease-keys L1))
; Raw scan count = the length-9 meta sentinel (granted above) + one empty-valued
; entry per attached key = 1 + 3 = 4.  mvcc-lease-keys skips the sentinel (= 3).
(check "L1 prefix scan count = meta + 3 keys = 4" 4 (length (kv-scan CTX (lease-prefix L1))))
; A lease with no keys scans empty.
(check "L2 index empty (no attaches yet)" '() (lease-keys L2))

; ===========================================================================
(section "reattach: PUT k2 v L2 -> k2 leaves L1, appears under L2 (no stale L1)")
; Re-PUTting k2 with a DIFFERENT lease must move it: .6's mvcc-put! drops the stale
; 0x03||L1||k2 entry and adds 0x03||L2||k2.  This is the load-bearing stale-removal
; the revoke path depends on — without it L1's revoke scan would still hit k2.
(put "PUT" "k2" "v2b" "200")   ; rev 4, reattach k2 -> L2
(check "current-rev advanced to 4" 4 (mvcc-current-rev CTX))
(check "k2 record lease now 200" 200 (kv-rec-lease (mvcc-get-latest CTX (b "k2"))))
; k2 is GONE from L1's index (proves stale-entry removal) ...
(check "L1 index = {k1,k3} (k2 removed, no stale entry)" '("k1" "k3") (lease-keys L1))
; ... and present under L2's index.
(check "L2 index = {k2}" '("k2") (lease-keys L2))
; Belt-and-braces: the explicit stale L1 entry for k2 is physically absent.
(check "stale 0x03||L1||k2 entry absent" #f
       (and (kv-get CTX (enc-lease L1 (b "k2"))) #t))

; ===========================================================================
(section "detach via lease=0: PUT k1 v 0 -> k1 leaves L1's index")
; A PUT with lease 0 (or omitted) detaches: mvcc-put! drops the prior lease entry
; and adds none.  etcd: writing a key without a lease (or with lease 0) clears any
; existing lease attachment.
(put "PUT" "k1" "v1b" "0")     ; rev 5, detach k1 (explicit lease 0)
(check "current-rev advanced to 5" 5 (mvcc-current-rev CTX))
(check "k1 record lease now 0" 0 (kv-rec-lease (mvcc-get-latest CTX (b "k1"))))
(check "L1 index = {k3} (k1 detached)" '("k3") (lease-keys L1))
(check "stale 0x03||L1||k1 entry absent" #f
       (and (kv-get CTX (enc-lease L1 (b "k1"))) #t))

; ===========================================================================
(section "delete a leased key: DEL k3 -> k3 leaves its lease index")
; DeleteRange tombstones k3 AND removes its 0x03||L1||k3 entry (mvcc-delete-range!
; drops the lease entry of each victim).  After this L1 has NO attached keys.
(check "DEL k3 -> rev 6, 1 deleted" (cons "DEL" (cons 6 1)) (del "DEL" "k3"))
(check "k3 now absent (tombstoned)" #f (mvcc-get-latest CTX (b "k3")))
(check "L1 index now EMPTY (all keys gone)" '() (lease-keys L1))
; L1 is still GRANTED (only its keys are gone), so its meta sentinel remains: the
; raw prefix scan is 1 (just the meta), mvcc-lease-keys is 0 (sentinel skipped).
(check "L1 prefix scan count = 1 (meta only, keys gone)" 1 (length (kv-scan CTX (lease-prefix L1))))
; L2 still holds k2 (untouched by k3's delete).
(check "L2 index still = {k2}" '("k2") (lease-keys L2))

; ===========================================================================
(section "THE REVOKE SCAN: 0x03||L2 yields exactly L2's current attached set")
; This is precisely what ("LEASE-REVOKE" L2) iterates in .17: the keys it will
; tombstone in one revision.  Build L2 up to a multi-key set first so the scan is
; exercised on >1 key (and proves it is L2's CURRENT set, not a stale superset).
(put "PUT" "m1" "mv1" "200")   ; rev 7, attach m1 -> L2  (L2 now {k2,m1})
(put "PUT" "m2" "mv2" "200")   ; rev 8, attach m2 -> L2  (L2 now {k2,m1,m2})
(check "L2 index = {k2,m1,m2}" '("k2" "m1" "m2") (lease-keys L2))
; The revoke iteration target = exactly this set; each will be tombstoned by .17.
(let ((victims (lease-keys L2)))
  (check "revoke scan of L2 = its 3 current keys" '("k2" "m1" "m2") victims)
  (check "every revoke victim is currently LIVE (a real key to tombstone)" #t
         (let loop ((vs victims))
           (cond ((null? vs) #t)
                 ((mvcc-get-latest CTX (b (car vs))) (loop (cdr vs)))
                 (else #f)))))
; L1 (fully drained earlier) revoke-scans to the empty set — a revoke is a no-op.
(check "revoke scan of drained L1 = {}" '() (lease-keys L1))
; A never-granted lease id revoke-scans empty too (no spurious keys).
(check "revoke scan of unknown lease 999 = {}" '() (lease-keys 999))

; ===========================================================================
(section "index isolation: L1 and L2 prefixes never bleed into each other")
; The u64be(leaseId) prefix groups each lease disjointly (ADR 0001 §8 / the
; encoding POC's 'lease 100 vs 101 disjoint' assertion), so a scan of one lease
; can never return another lease's keys — the revoke of L2 can't touch L1's keys.
(put "PUT" "shared" "sv" "100")   ; rev 9, attach to L1
(check "L1 index = {shared}" '("shared") (lease-keys L1))
(check "L2 index unchanged = {k2,m1,m2}" '("k2" "m1" "m2") (lease-keys L2))

(done!)
