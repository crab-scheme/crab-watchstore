; test/mvcc-compact.scm — unit tests for mvcc-compact (cw-u4a.8).
;
; Drives mvcc-compact (via mvcc-apply ("COMPACT" rev)) against a real RocksDB
; context built via mvcc-apply.  Fresh per-run temp dir ((current-jiffy)) prevents
; state bleed between runs.

(include "test/harness.scm")
(include "src/store-ctx.scm")
(include "src/mvcc.scm")
(include "test/mvcc-util.scm")

; ---- open a fresh store ----
(define run-tag (number->string (current-jiffy)))
(define DB-PATH (string-append "/tmp/cws-mvcc-compact-" run-tag))
(define CTX (make-ctx (store-open DB-PATH #t) "default" #t))

; Guarantee isolation: (current-jiffy) is process-relative, so back-to-back runs
; reuse the same dir — empty the store before building any state.
(reset-ctx! CTX)

; ---- helpers ----
(define (b s) (string->utf8 s))
(define (put . parts)  (mvcc-apply CTX (map b parts)))
(define (del . parts)  (mvcc-apply CTX (map b parts)))
(define (compact rev)  (mvcc-apply CTX (list (b "COMPACT") (b (number->string rev)))))

; COMPACT via mvcc-apply flips only the ErrCompacted gate (cw-vku); the
; physical GC is incremental.  Drive it to completion — as the shard drivers
; do one slice per tick — before asserting physical row counts, so the
; assertions also cover the incremental path converging to the old end-state.
(define (drain-gc!)
  (let loop () (if (mvcc-compact-gc-step! CTX) (loop))))

(define (latest K)        (mvcc-get-latest CTX (b K)))
(define (latest-at K rev) (mvcc-get-latest CTX (b K) rev))
(define (val-of r) (and r (utf8->string (kv-rec-value r))))

; count KEY-CF entries for a single user-key
(define (key-cf-count K)
  (kv-scan-count CTX (key-cf-prefix (b K))))

; count REV-CF entries in the entire namespace
(define (rev-cf-count)
  (kv-scan-count CTX (mvcc-byte NS-REV)))

; ===========================================================================
; Build state:
;   k: PUT v1 -> r1=1, PUT v2 -> r2=2, PUT v3 -> r3=3
;   e: PUT ve -> r_e=4  (live key; will survive compaction at e's rev)
;   d: PUT vd -> r4=5,  DEL  -> r5=6  (tombstone case)
; current-rev = 6, REV-CF has 6 events
; ===========================================================================

(put "PUT" "k" "v1")   ; r1=1
(put "PUT" "k" "v2")   ; r2=2
(put "PUT" "k" "v3")   ; r3=3
(put "PUT" "e" "ve")   ; r_e=4
(put "PUT" "d" "vd")   ; r4=5
(del "DEL" "d")        ; r5=6  (tombstone)

(check "current-rev = 6 before compact" 6 (mvcc-current-rev CTX))
(check "REV-CF has 6 events before compact" 6 (rev-cf-count))
(check "k has 3 KEY-CF versions before compact" 3 (key-cf-count "k"))
(check "e has 1 KEY-CF version before compact"  1 (key-cf-count "e"))
; d has PUT + tombstone = 2 KEY-CF versions
(check "d has 2 KEY-CF versions before compact" 2 (key-cf-count "d"))

; ===========================================================================
(section "COMPACT r2 — multi-version key: r1 GCed, r2 kept, r3 untouched")

(define compact-result (compact 2))
(check "COMPACT r2 returns ok" (cons 'ok 2) compact-result)
(drain-gc!)
(check "compact-rev = 2 after compact" 2 (mvcc-compact-rev CTX))

; current-rev must NOT have changed
(check "current-rev still 6 after compact" 6 (mvcc-current-rev CTX))

; k's r1 (mod_rev=1 <= compactRev=2) old version is physically gone;
; k's r2 (mod_rev=2 = compactRev, latest-≤-compactRev, non-tombstone) is kept;
; k's r3 (mod_rev=3 > compactRev=2) is always kept.
; => 2 KEY-CF versions remain for k
(check "k has 2 KEY-CF versions after COMPACT r2 (r1 GCed)" 2 (key-cf-count "k"))

; read at current rev => v3 (latest overall)
(check "k at current = v3" "v3" (val-of (latest "k")))

; read-at-rev r2 = 2 => v2 (latest-≤-r2 was kept)
(check "k at r2 = v2 (latest-≤-compactRev preserved)" "v2" (val-of (latest-at "k" 2)))

; read-at-rev below compactRev => ErrCompacted (mvcc-range gate)
(let ((r (mvcc-range CTX (b "k") #f (list (cons 'revision 1)))))
  (check "range at rev 1 < compact-rev => err-compacted" 'err-compacted (car r))
  (check "err-compacted carries compact-rev=2" 2 (cdr r)))

; ===========================================================================
(section "live key 'e' with only version <= compactRev — kept (non-tombstone)")

; e has mod_rev=4 > compactRev=2, so it was untouched in the COMPACT r2 pass.
; Let's do a fresh compact to a rev that covers e (c=4) to verify the keep rule.
; First add another key so we have something newer too.
(put "PUT" "z" "vz")   ; rev 7

(define compact-result-4 (compact 4))
(check "COMPACT r4 returns ok" (cons 'ok 4) compact-result-4)
(drain-gc!)
(check "compact-rev = 4" 4 (mvcc-compact-rev CTX))
(check "current-rev still 7 after second compact" 7 (mvcc-current-rev CTX))

; e: only version is mod_rev=4 = compactRev, non-tombstone => must be KEPT
(check "e has 1 KEY-CF version after COMPACT r4 (non-tombstone kept)" 1 (key-cf-count "e"))
(check "e is still readable at current" "ve" (val-of (latest "e")))

; ===========================================================================
(section "tombstone key 'd' — all versions removed after COMPACT r6")

; d: PUT(mod_rev=5) + TOMBSTONE(mod_rev=6). current compact-rev=4.
; COMPACT r6 => all of d's versions (5 and 6) have mod_rev <= 6.
; Latest-≤-6 is the tombstone at r6 => delete ALL (tombstone rule).
(define compact-result-6 (compact 6))
(check "COMPACT r6 returns ok" (cons 'ok 6) compact-result-6)
(drain-gc!)
(check "compact-rev = 6 after third compact" 6 (mvcc-compact-rev CTX))
(check "current-rev still 7 after third compact" 7 (mvcc-current-rev CTX))

; d must have ZERO KEY-CF versions (both the PUT and the tombstone deleted)
(check "d has 0 KEY-CF versions after COMPACT r6" 0 (key-cf-count "d"))

; d reads as absent at current
(check "d absent at current after full compaction" #f (latest "d"))

; ===========================================================================
(section "REV-CF: only events with rev > compactRev remain")

; After COMPACT r6: compact-rev=6, current-rev=7.
; Revisions 1–6 have been compacted (REV-CF events for those revs deleted).
; Only rev=7 (PUT z) should remain in REV-CF.
(check "REV-CF has 1 event remaining (rev 7 only)" 1 (rev-cf-count))

; ===========================================================================
(section "error cases: re-compact to <= current compact-rev => ErrCompacted")

(let ((r (compact 5)))
  (check "COMPACT 5 <= compact-rev=6 => err-compacted" 'err-compacted (car r))
  (check "err-compacted carries compact-rev=6" 6 (cdr r)))

(let ((r (compact 6)))
  (check "COMPACT 6 = compact-rev=6 => err-compacted" 'err-compacted (car r))
  (check "err-compacted carries compact-rev=6" 6 (cdr r)))

; ===========================================================================
(section "error cases: COMPACT to rev > current-rev => err-future-rev")

(let ((r (compact 999)))
  (check "COMPACT 999 > current-rev=7 => err-future-rev" 'err-future-rev (car r))
  (check "err-future-rev carries current-rev=7" 7 (cdr r)))

; ===========================================================================
(section "current-rev was never changed by any COMPACT")

; We asserted this after each compact above, but do one final check.
(check "current-rev is still 7 (unchanged by compactions)" 7 (mvcc-current-rev CTX))

; ===========================================================================
(done!)
