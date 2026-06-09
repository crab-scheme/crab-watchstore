; test/txn-append.scm — strict-serializable readiness proof at the mvcc/txn level.
;
; SCOPE: this file proves the Txn primitive (cw-u4a.10) correctly supports the
; read-modify-write idioms that strict-serializability rests on — specifically the
; jepsen-etcd `append` workload's list-append idiom — at the MVCC layer.  The CAS
; guard (mod_rev or version comparison) is the mechanism that prevents lost updates.
;
; NOTE: this is strict-serializable *readiness* at the mvcc/txn level.  The CAS
; primitive is proven correct here (single-writer, no concurrency).  Full Elle
; list-append strict-serializability under concurrent writers and network faults is
; validated by Jepsen in cw-u4a.35.
;
; Every Txn is driven through the full flat apply path:
;   txn-encode -> ("TXN" bytes) -> mvcc-apply -> txn-decode -> compare/branch/apply
; so the node-send-safe serialization is exercised on every assertion.
;
; ISOLATION: reset-ctx! (mvcc-util.scm) between tests.  Back-to-back runs are
; deterministic.

(include "test/harness.scm")
(include "src/store-ctx.scm")
(include "src/mvcc.scm")          ; includes src/txn.scm at its tail
(include "test/mvcc-util.scm")

; ---- open a fresh store ----
(define run-tag (number->string (current-jiffy)))
(define DB-PATH (string-append "/tmp/cws-txn-append-" run-tag))
(define CTX (make-ctx (store-open DB-PATH #t) "default" #t))
(reset-ctx! CTX)

; ---- helpers ----
(define (b s)   (string->utf8 s))
(define (i64 n) (u64->bytes n))

(define (latest K)  (mvcc-get-latest CTX (b K)))
(define (val-of r)  (and r (utf8->string (kv-rec-value r))))
(define (cur-rev)   (mvcc-current-rev CTX))

; Run a Txn struct through the full flat apply path.
(define (run-txn t)
  (mvcc-apply CTX (list (b "TXN") (txn-encode t))))

; Parse a "|"-separated list string into elements.
(define (parse-list s)
  (if (string=? s "")
      '()
      (let loop ((chars (string->list s)) (cur '()) (acc '()))
        (cond ((null? chars)
               (reverse (cons (list->string (reverse cur)) acc)))
              ((char=? (car chars) #\|)
               (loop (cdr chars) '() (cons (list->string (reverse cur)) acc)))
              (else
               (loop (cdr chars) (cons (car chars) cur) acc))))))

; Build the Elle list-append Txn: guard on MOD_REV(K)==observed-mod-rev (or
; VERSION(K)==0 when absent), success=[Put K new-value], failure=[Range K].
; Returns the constructed Txn struct.
(define (make-append-txn K old-val observed-mod-rev absent? new-element sep)
  (let* ((new-val  (if (string=? old-val "")
                       new-element
                       (string-append old-val sep new-element)))
         (compare  (if absent?
                       ; key absent -> guard VERSION==0 (create-if-not-exists)
                       (make-compare CMP-VERSION RES-EQUAL (b K) (i64 0))
                       ; key present -> guard MOD==observed-mod-rev
                       (make-compare CMP-MOD RES-EQUAL (b K) (i64 observed-mod-rev)))))
    (make-txn (list compare)
              (list (op-put (b K) (b new-val) 0))   ; success: write new list
              (list (op-range (b K) #f '())))))      ; failure: read current value

; ===========================================================================
(section "Elle list-append idiom: 20 sequential appends, no lost updates")
; The Elle `append` workload model: append element E to the list at key K.
; Idiom: read K's mod_rev, CAS-guarded Put with new list.  Drive 20 appends
; and assert the final value is exactly the 20 elements in order.
(reset-ctx! CTX)

(define APPEND-KEY "elist")
(define SEP "|")

; Perform one append of `element` to KEY (default APPEND-KEY).
; Retries on stale-guard failure (not expected in single-writer path).
(define (do-append-to K element)
  (let loop ()
    (let* ((rec     (latest K))
           (absent? (not rec))
           (old-val (if absent? "" (utf8->string (kv-rec-value rec))))
           (mod-rev (if absent? 0 (kv-rec-mod-rev rec)))
           (txn     (make-append-txn K old-val mod-rev absent? element SEP))
           (res     (run-txn txn)))
      (if (car res)
          (cons 'ok (cdr (car (cdr res))))   ; (ok . put-mod-rev)
          ; guard failed -> re-read and retry
          (loop)))))

(define (do-append element) (do-append-to APPEND-KEY element))

; Collect the mod_revs of all 20 successful appends (for REV-CF coherence check).
(define append-mod-revs '())

(let loop ((i 1))
  (if (<= i 20)
      (begin
        (let* ((elem (string-append "e" (number->string i)))
               (r    (do-append elem)))
          (set! append-mod-revs (append append-mod-revs (list (cdr r)))))
        (loop (+ i 1)))))

; Final value must be exactly "e1|e2|...|e20" — no lost updates, no dups, no reorder.
(define final-val (val-of (latest APPEND-KEY)))
(define final-elements (parse-list final-val))

(check "append: 20 elements in final list" 20 (length final-elements))
(check "append: first element is e1"  "e1"  (list-ref final-elements 0))
(check "append: last element is e20"  "e20" (list-ref final-elements 19))
(check "append: element 10 is e10"    "e10" (list-ref final-elements 9))
; verify full ordered sequence
(let loop ((i 1) (ok #t))
  (if (> i 20)
      (check "append: all 20 elements in order (no lost update, no dup, no reorder)" #t ok)
      (let ((expected (string-append "e" (number->string i)))
            (actual   (list-ref final-elements (- i 1))))
        (loop (+ i 1) (and ok (string=? expected actual))))))

; ===========================================================================
(section "REV-CF coherence: one PUT event per append, strictly ascending revisions")
; Each successful append must have produced exactly one PUT event in the REV-CF,
; and the mod_revs must be strictly ascending (one rev per Txn, no gaps).
(check "append: 20 mod_revs collected" 20 (length append-mod-revs))
; strictly ascending
(let loop ((revs append-mod-revs) (ok #t))
  (if (or (null? revs) (null? (cdr revs)))
      (check "append: mod_revs strictly ascending" #t ok)
      (loop (cdr revs) (and ok (< (car revs) (cadr revs))))))
; each mod_rev from the response matches what the record reports
(let* ((rec     (latest APPEND-KEY))
       (final-mod-rev (kv-rec-mod-rev rec)))
  (check "append: final record mod_rev = last append's mod_rev"
         (list-ref append-mod-revs 19) final-mod-rev))
; verify REV-CF has a PUT event for each of these revisions
(let ((rev-rows (kv-scan CTX (mvcc-byte NS-REV))))
  (let loop ((revs append-mod-revs) (ok #t))
    (if (null? revs)
        (check "append: every mod_rev has a PUT event in REV-CF" #t ok)
        (let* ((target-rev (car revs))
               (found (filter
                       (lambda (row)
                         (let* ((fk  (car row))
                                ; REV-CF key: 0x02 || u64be(main) || u64be(sub)
                                (main-rev (bytes->u64 fk 1))
                                (ev  (event-decode (cdr row))))
                           (and (= main-rev target-rev)
                                (= (ev-kind ev) EV-PUT))))
                       rev-rows)))
          (loop (cdr revs) (and ok (= 1 (length found))))))))

; ===========================================================================
(section "Conflict / lost-update prevention: stale guard MUST fail")
; Prove that the CAS guard prevents a lost update:
;   1. Observe mod_rev R of key "conflict-key" (after one initial write).
;   2. Advance the key past R by doing another append (mod_rev now R+something).
;   3. Apply the original (stale-R) Txn -> must FAIL (succeeded? = #f).
;   4. The list must NOT be corrupted (only the committed appends are present).
;   5. A retry with the FRESH mod_rev succeeds and appends cleanly.
(reset-ctx! CTX)

(define CK "conflict-key")

; Step 1: initial write — "A"
(do-append-to CK "A")

; Capture the stale observation: mod_rev R, value "A"
(define stale-rec (latest CK))
(define stale-mod-rev (kv-rec-mod-rev stale-rec))
(define stale-val "A")

; Step 2: advance past R — append "B" (mod_rev now stale-mod-rev + 1)
(do-append-to CK "B")
(define after-B-val (val-of (latest CK)))
(define after-B-mod-rev (kv-rec-mod-rev (latest CK)))
(check "conflict: after B, value is A|B" "A|B" after-B-val)
(check "conflict: after B, mod_rev advanced past stale" #t (> after-B-mod-rev stale-mod-rev))

; Step 3: now apply the stale Txn (guard MOD==stale-mod-rev, would write "A|C")
; This MUST fail because mod_rev has advanced to after-B-mod-rev.
(define stale-txn
  (make-txn (list (make-compare CMP-MOD RES-EQUAL (b CK) (i64 stale-mod-rev)))
            (list (op-put (b CK) (b "A|C") 0))    ; would clobber "B"
            (list (op-range (b CK) #f '()))))      ; failure: read back

(let ((res (run-txn stale-txn)))
  (check "conflict: stale guard FAILED (succeeded?=#f)" #f (car res))
  ; failure branch returns the range — verify it's "A|B" (not clobbered)
  (let* ((range-resp (car (cdr res)))
         (kv-item    (car (cdr range-resp)))
         (read-val   (utf8->string (kv-rec-value (cdr kv-item)))))
    (check "conflict: failure Range read = A|B (not clobbered)" "A|B" read-val)))

; Step 4: list is not corrupted
(check "conflict: key value still A|B (no lost update)" "A|B" (val-of (latest CK)))

; Step 5: retry with fresh mod_rev succeeds and appends "C" cleanly
(do-append-to CK "C")
(check "conflict: retry succeeded, value is A|B|C" "A|B|C" (val-of (latest CK)))
(check "conflict: C is the third element (no A|C overwrite)"
       "C" (list-ref (parse-list (val-of (latest CK))) 2))

; ===========================================================================
(section "Multi-key atomic transfer: both keys update together or not at all")
; Model: a=10, b=0.  Transfer amount=3 from a to b.
; Txn: compare VERSION(a) != 0 (key exists, guards against absent), plus we use
; a VALUE compare so the debit check is built into the guard.
; If amount can be satisfied (a has "10"), success=[Put a "7", Put b "3"].
; Failure branch=[Range a, Range b] (read current values back).
;
; Because VALUE compare operates on raw bytes (string representation here), we
; use CMP-VALUE RES-EQUAL for the exact debit amount guard.  This is sufficient
; to prove atomicity: either both writes happen under one main rev, or neither does.
(reset-ctx! CTX)

(define AK "acct-a")
(define BK "acct-b")

; Seed: put a=10 (plain PUT), b=0
(mvcc-apply CTX (list (b "PUT") (b AK) (b "10")))
(mvcc-apply CTX (list (b "PUT") (b BK) (b "0")))
(define pre-transfer-rev (cur-rev))

; Transfer Txn: guard = VALUE(a)=="10", success=[Put a "7", Put b "3"], failure=[Range a, Range b]
(define transfer-txn
  (make-txn (list (make-compare CMP-VALUE RES-EQUAL (b AK) (b "10")))
            (list (op-put (b AK) (b "7") 0)
                  (op-put (b BK) (b "3") 0))
            (list (op-range (b AK) #f '())
                  (op-range (b BK) #f '()))))

(let ((res (run-txn transfer-txn)))
  (check "transfer: succeeded?=#t (guard VALUE(a)==10 held)" #t (car res))
  (check "transfer: 2 responses (both Puts)" 2 (length (cdr res)))
  (check "transfer: resp0 = (put . new-rev)" 'put (car (list-ref (cdr res) 0)))
  (check "transfer: resp1 = (put . new-rev)" 'put (car (list-ref (cdr res) 1))))

; Both keys updated atomically
(check "transfer: a is now 7" "7" (val-of (latest AK)))
(check "transfer: b is now 3" "3" (val-of (latest BK)))

; Both share ONE main rev (atomic: one Txn = one revision)
(define transfer-rev (cur-rev))
(check "transfer: exactly one rev bump" (+ pre-transfer-rev 1) transfer-rev)
(check "transfer: a mod_rev = transfer rev" transfer-rev (kv-rec-mod-rev (latest AK)))
(check "transfer: b mod_rev = transfer rev (same rev as a)" transfer-rev (kv-rec-mod-rev (latest BK)))

; Guard-failing transfer: try to transfer 10 from a (now "7", guard expects "10").
; The Txn MUST fail and leave BOTH a and b unchanged (all-or-nothing).
(define pre-fail-rev (cur-rev))
(let ((res (run-txn transfer-txn)))
  (check "transfer-fail: succeeded?=#f (guard VALUE(a)==10 not held)" #f (car res))
  (check "transfer-fail: 2 failure responses (Range a, Range b)" 2 (length (cdr res)))
  ; failure ranges show current values
  (let* ((range-a (list-ref (cdr res) 0))
         (range-b (list-ref (cdr res) 1))
         (val-a   (utf8->string (kv-rec-value (cdr (car (cdr range-a))))))
         (val-b   (utf8->string (kv-rec-value (cdr (car (cdr range-b)))))))
    (check "transfer-fail: range-a reads 7" "7" val-a)
    (check "transfer-fail: range-b reads 3" "3" val-b)))

; All-or-nothing: neither key changed
(check "transfer-fail: a unchanged at 7" "7" (val-of (latest AK)))
(check "transfer-fail: b unchanged at 3" "3" (val-of (latest BK)))
; A pure-read failure branch does NOT bump the revision (resolves cw-u4a.40)
(check "transfer-fail: rev NOT bumped (read-only failure branch)" pre-fail-rev (cur-rev))

; ===========================================================================
(done!)
