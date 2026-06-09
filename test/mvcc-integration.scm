; test/mvcc-integration.scm — cross-feature integration test for the MVCC layer (cw-u4a.9).
;
; PURPOSE: prove that mvcc-put!/mvcc-delete-range!/mvcc-range/mvcc-compact COMPOSE
; correctly when exercised through a single shared store in a realistic lifecycle
; sequence.  The per-feature tests cover isolated semantics; this test covers the
; joints between features that isolated tests cannot.
;
; MVCC SEMANTICS COVERAGE MAP (which test file owns each concern):
;
;   Feature                           Primary test            Integration proof here
;   ─────────────────────────────────────────────────────────────────────────────
;   revision bumping / create/mod/ver mvcc-apply.scm          v
;   version reset after delete        mvcc-apply.scm          v (recreate k=v3)
;   read-at-revision                  mvcc-apply.scm          v (read at compactRev)
;   range delete                      mvcc-apply.scm          cross-key del in range
;   lease index                       mvcc-apply.scm          -
;   point range                       mvcc-range.scm          -
;   half-open / prefix / all-keys     mvcc-range.scm          all-keys in sorted order
;   sort orders                       mvcc-range.scm          -
;   rev filters                       mvcc-range.scm          -
;   ErrCompacted (range gate)         mvcc-range.scm+compact  v (below/at/above)
;   multi-version GC                  mvcc-compact.scm        v (via full lifecycle)
;   tombstone reclaim                 mvcc-compact.scm        -
;   REV-CF event stream               mvcc-compact.scm        v (kinds/revs coherence)
;   current-rev invariant             mvcc-compact.scm        v
;   apply+range+compact composition   —                       <<< THIS FILE >>>
;
; ISOLATION: uses reset-ctx! from mvcc-util.scm so back-to-back runs without
; external cleanup are fully deterministic.

(include "test/harness.scm")
(include "src/store-ctx.scm")
(include "src/mvcc.scm")
(include "test/mvcc-util.scm")

; ---- open store (per-run tag; reset-ctx! guarantees clean state) ----
(define run-tag (number->string (current-jiffy)))
(define DB-PATH (string-append "/tmp/cws-mvcc-integ-" run-tag))
(define CTX (make-ctx (store-open DB-PATH #t) "default" #t))

(reset-ctx! CTX)

; ---- helpers ----
(define (b s) (string->utf8 s))
(define (put . parts) (mvcc-apply CTX (map b parts)))
(define (del . parts) (mvcc-apply CTX (map b parts)))
(define (compact rev)
  (mvcc-apply CTX (list (b "COMPACT") (b (number->string rev)))))

(define (latest K)         (mvcc-get-latest CTX (b K)))
(define (latest-at K rev)  (mvcc-get-latest CTX (b K) rev))
(define (val-of r)         (and r (utf8->string (kv-rec-value r))))

; Run mvcc-range from keyword-style pairs.
(define (range key range-end . opts-pairs)
  (let ((opts (let build ((ps opts-pairs) (acc '()))
                (if (null? ps)
                    (reverse acc)
                    (build (cddr ps) (cons (cons (car ps) (cadr ps)) acc))))))
    (mvcc-range CTX key range-end opts)))

(define (range-count r)  (car r))
(define (range-kvs   r)  (cdr r))
(define (kv-key item)    (utf8->string (car item)))
(define (kv-val item)    (utf8->string (kv-rec-value (cdr item))))
(define (kv-keys r)      (map kv-key (range-kvs r)))
(define (kv-vals r)      (map kv-val (range-kvs r)))

; REV-CF events for a whole-namespace scan.
(define (all-rev-events)
  (map (lambda (row) (event-decode (cdr row)))
       (kv-scan CTX (mvcc-byte NS-REV))))

; ===========================================================================
(section "lifecycle: put k=v1 (rev 1)")
; ===========================================================================
(check "put k=v1 -> (PUT . 1)" (cons "PUT" 1) (put "PUT" "k" "v1"))
(check "current-rev = 1" 1 (mvcc-current-rev CTX))
(let ((r (latest "k")))
  (check "k value = v1"   "v1" (val-of r))
  (check "k create_rev=1" 1    (kv-rec-create-rev r))
  (check "k mod_rev=1"    1    (kv-rec-mod-rev r))
  (check "k version=1"    1    (kv-rec-version r)))

; ===========================================================================
(section "lifecycle: update k=v2 (rev 2) — range sees v2, not v1")
; ===========================================================================
(check "update k=v2 -> (PUT . 2)" (cons "PUT" 2) (put "PUT" "k" "v2"))
(let ((r (latest "k")))
  (check "k value = v2"         "v2" (val-of r))
  (check "k create_rev still 1" 1    (kv-rec-create-rev r))
  (check "k mod_rev=2"          2    (kv-rec-mod-rev r))
  (check "k version=2"          2    (kv-rec-version r)))

; range sees v2 (not v1) — cross-feature: range + multi-version
(let ((res (range (b "k") #f)))
  (check "range k sees v2 after update" "v2" (kv-val (car (range-kvs res)))))

; ===========================================================================
(section "lifecycle: delete k (rev 3) — range omits k, get-latest #f")
; ===========================================================================
(check "del k -> (DEL 3 . 1)" (cons "DEL" (cons 3 1)) (del "DEL" "k"))
(check "current-rev = 3" 3 (mvcc-current-rev CTX))
(check "get-latest k = #f (tombstoned)" #f (latest "k"))

; range must not include the tombstoned key
(let ((res (range (b "k") #f)))
  (check "range k = 0 after delete" 0 (range-count res))
  (check "range k empty list"       '() (range-kvs res)))

; ===========================================================================
(section "lifecycle: recreate k=v3 (rev 4) — version resets, create_rev = 4")
; ===========================================================================
(check "recreate k=v3 -> (PUT . 4)" (cons "PUT" 4) (put "PUT" "k" "v3"))
(let ((r (latest "k")))
  (check "k value = v3"               "v3" (val-of r))
  (check "k version RESETS to 1"      1    (kv-rec-version r))
  (check "k create_rev = 4 (new life)" 4   (kv-rec-create-rev r))
  (check "k mod_rev=4"                4    (kv-rec-mod-rev r)))

; ===========================================================================
(section "add more keys (rev 5..9) — range-all in sorted order")
; ===========================================================================
(put "PUT" "alpha"  "va")   ; rev 5
(put "PUT" "beta"   "vb")   ; rev 6
(put "PUT" "gamma"  "vg")   ; rev 7
(put "PUT" "delta"  "vd")   ; rev 8
(put "PUT" "epsilon" "ve")  ; rev 9
(check "current-rev = 9" 9 (mvcc-current-rev CTX))

; all-keys ascending: alpha, beta, delta, epsilon, gamma, k
(let* ((zero (make-bytevector 1 0))
       (res  (range zero zero 'sort-order 'ascend 'sort-target 'key)))
  (check "all-keys count = 6" 6 (range-count res))
  (check "all-keys sorted ascending"
         '("alpha" "beta" "delta" "epsilon" "gamma" "k")
         (kv-keys res)))

; ===========================================================================
(section "compact at rev 5 (a middle revision)")
; ===========================================================================
; State at rev 5: alpha=va (just created). k was deleted at rev 3, recreated at rev 4.
; After compact(5):
;   - k: create_rev=4 > 5 — all versions above compactRev, untouched
;   - alpha, beta, gamma, delta, epsilon: various states above compactRev
; Actually k has 4 KEY-CF versions (rev1, rev2 tombstone=rev3, rev4).
; compact(5) GCs everything <= 5:
;   k: versions at rev1,rev2,rev3(tomb),rev4 are all <= 5 EXCEPT rev4 <= 5 is yes.
;   latest-<=5 for k: rev4 is live (non-tomb) -> keep rev4, GC rev1/rev2/rev3.
;   alpha: only version rev5 <= 5 -> non-tombstone -> keep (1 version retained).
;   beta through epsilon: all > rev5 -> untouched.
; REV-CF events 1..5 are deleted; events 6..9 remain.

(define compact-result (compact 5))
(check "compact(5) -> (ok . 5)" (cons 'ok 5) compact-result)
(check "compact-rev = 5" 5 (mvcc-compact-rev CTX))
(check "current-rev still 9 (compact does NOT bump)" 9 (mvcc-current-rev CTX))

; ===========================================================================
(section "after compact(5): read-at-rev BELOW compactRev => ErrCompacted")
; ===========================================================================
; rev 3 < 5 -> ErrCompacted
(let ((res (range (b "k") #f 'revision 3)))
  (check "read k @rev3 < compact(5) => err-compacted" 'err-compacted (car res))
  (check "err-compacted carries compact-rev=5" 5 (cdr res)))

; rev 1 < 5 -> ErrCompacted (all-keys scan below compact)
(let* ((zero (make-bytevector 1 0))
       (res  (range zero zero 'revision 1)))
  (check "all-keys @rev1 < compact(5) => err-compacted" 'err-compacted (car res)))

; ===========================================================================
(section "after compact(5): read-at-rev = compactRev => correct historical value")
; ===========================================================================
; At rev 5: k=v3 (create_rev=4, recreated at rev 4, mod_rev=4 <= 5), alpha=va.
; The latest-<=5 version of k is rev4 (live, kept by GC).
(let ((res (range (b "k") #f 'revision 5)))
  (check "read k @compactRev=5 => v3 (historical anchor)" "v3"
         (kv-val (car (range-kvs res)))))

; alpha at rev5 = "va" (its only version, kept as anchor)
(let ((res (range (b "alpha") #f 'revision 5)))
  (check "read alpha @compactRev=5 => va" "va"
         (kv-val (car (range-kvs res)))))

; ===========================================================================
(section "after compact(5): current range still correct")
; ===========================================================================
; All 6 keys are still live at current-rev=9.
(let* ((zero (make-bytevector 1 0))
       (res  (range zero zero 'sort-order 'ascend 'sort-target 'key)))
  (check "all-keys count = 6 after compact" 6 (range-count res))
  (check "all-keys correct after compact"
         '("alpha" "beta" "delta" "epsilon" "gamma" "k")
         (kv-keys res)))

; ===========================================================================
(section "after compact(5): REV-CF event stream — only events > compactRev, coherent")
; ===========================================================================
; REV-CF should have events at revisions 6,7,8,9 only (rev 1..5 GC'd).
; Events: rev6=beta PUT, rev7=gamma PUT, rev8=delta PUT, rev9=epsilon PUT.
(let* ((evts (all-rev-events))
       (revs (map ev-mod-rev evts))
       (kinds (map ev-kind evts)))
  (check "REV-CF has 4 events after compact(5)" 4 (length evts))
  ; All remaining revisions must be > compactRev=5
  (check "all remaining events > compactRev=5"
         #t
         (let verify ((rs revs))
           (if (null? rs) #t
               (and (> (car rs) 5) (verify (cdr rs))))))
  ; All remaining events are PUT kind (no deletes above rev 5)
  (check "all remaining events are PUT"
         #t
         (let verify ((ks kinds))
           (if (null? ks) #t
               (and (= (car ks) EV-PUT) (verify (cdr ks))))))
  ; Revisions are strictly ascending (Watch replay order preserved)
  (check "REV-CF revisions ascending 6 7 8 9" '(6 7 8 9) revs))

; ===========================================================================
(section "read-at-rev above compactRev still works correctly")
; ===========================================================================
; At rev 6 (just beta was added): alpha=va, beta=vb, k=v3 all visible.
(let* ((zero (make-bytevector 1 0))
       (res  (range zero zero 'revision 6 'sort-order 'ascend 'sort-target 'key)))
  (check "all-keys @rev6: count=3 (alpha beta k)" 3 (range-count res))
  (check "all-keys @rev6 keys" '("alpha" "beta" "k") (kv-keys res)))

; At rev 9 (all keys): same as current
(let* ((zero (make-bytevector 1 0))
       (res  (range zero zero 'revision 9 'sort-order 'ascend 'sort-target 'key)))
  (check "all-keys @rev9 = current" 6 (range-count res)))

; ===========================================================================
(section "ErrCompacted gate is sticky across range calls")
; ===========================================================================
; A second call below compactRev should also return err-compacted (not stale #f).
(let ((res1 (range (b "alpha") #f 'revision 2))
      (res2 (range (b "beta")  #f 'revision 4)))
  (check "alpha @rev2 < compact(5) => err-compacted" 'err-compacted (car res1))
  (check "beta @rev4 < compact(5) => err-compacted"  'err-compacted (car res2)))

; revision=0 (default = current) is NEVER compacted regardless of compact-rev
(let ((res (range (b "k") #f)))
  (check "default revision (0=current) never err-compacted" 1 (range-count res)))

; ===========================================================================
(done!)
