; test/mvcc-txn.scm — unit tests for the etcd Txn (If/Then/Else) layer (cw-u4a.10).
;
; Drives the Txn path against a real RocksDB ctx.  Every Txn is run through the
; FULL apply seam: txn-encode -> ("TXN" bytes) -> mvcc-apply -> txn-decode ->
; compare/branch/apply, so the flat (node-send-safe) serialization is exercised on
; every assertion, not just the dedicated round-trip section.
;
; Covers (per cw-u4a.10):
;   - flat round-trip structural equality (compares + nested ops + nested Txn);
;   - the canonical CAS pattern (VALUE==v0 ? Put v1 : Range);
;   - every compare target (VERSION/CREATE/MOD/VALUE/LEASE) with EQUAL + an
;     ordering/NOT_EQUAL, incl. absent-key (VERSION==0 "create if not exists");
;   - multiple ops in one branch sharing one main rev with successive sub-revs;
;   - revision semantics: a writing Txn bumps once; a pure-read Txn does NOT bump
;     (resolves cw-u4a.40 for the Txn path);
;   - responses match the executed ops (count + content) and succeeded? correctness;
;   - a nested Txn as a success op (its result nested in the responses).
;
; ISOLATION: reset-ctx! (mvcc-util.scm) so back-to-back runs are deterministic.

(include "test/harness.scm")
(include "src/store-ctx.scm")
(include "src/mvcc.scm")          ; includes src/txn.scm at its tail
(include "test/mvcc-util.scm")

; ---- open a fresh store ----
(define run-tag (number->string (current-jiffy)))
(define DB-PATH (string-append "/tmp/cws-mvcc-txn-" run-tag))
(define CTX (make-ctx (store-open DB-PATH #t) "default" #t))
(reset-ctx! CTX)

; ---- helpers ----
(define (b s) (string->utf8 s))
(define (i64 n) (u64->bytes n))          ; integer target-value side (u64be)

; plain PUT through the existing apply path (to set up state)
(define (put . parts) (mvcc-apply CTX (map b parts)))

; latest record / value for a string key
(define (latest K)  (mvcc-get-latest CTX (b K)))
(define (val-of r)  (and r (utf8->string (kv-rec-value r))))
(define (cur-rev)   (mvcc-current-rev CTX))

; Run a Txn STRUCT through the full flat apply seam; returns (succeeded? . responses).
(define (run-txn t)
  (mvcc-apply CTX (list (b "TXN") (txn-encode t))))

; ---- shorthand compare builders ----
(define (cmp-version-eq K n)  (make-compare CMP-VERSION RES-EQUAL     (b K) (i64 n)))
(define (cmp-version-gt K n)  (make-compare CMP-VERSION RES-GREATER   (b K) (i64 n)))
(define (cmp-create-eq  K n)  (make-compare CMP-CREATE  RES-EQUAL     (b K) (i64 n)))
(define (cmp-mod-eq     K n)  (make-compare CMP-MOD     RES-EQUAL     (b K) (i64 n)))
(define (cmp-mod-lt     K n)  (make-compare CMP-MOD     RES-LESS      (b K) (i64 n)))
(define (cmp-lease-eq   K n)  (make-compare CMP-LEASE   RES-EQUAL     (b K) (i64 n)))
(define (cmp-lease-ne   K n)  (make-compare CMP-LEASE   RES-NOT-EQUAL (b K) (i64 n)))
(define (cmp-value-eq   K s)  (make-compare CMP-VALUE   RES-EQUAL     (b K) (b s)))
(define (cmp-value-ne   K s)  (make-compare CMP-VALUE   RES-NOT-EQUAL (b K) (b s)))

; ===========================================================================
(section "FLAT round-trip: encode -> decode is structurally identical")
; A deliberately non-trivial Txn: 2 compares (a VALUE + an integer target), a
; multi-op success branch incl. a Put-with-lease and a DeleteRange-with-range-end,
; a Range op carrying opts, AND a nested Txn; a non-empty failure branch too.
(define rt-nested
  (make-txn (list (cmp-version-eq "n" 0))
            (list (op-put (b "n") (b "created") 0))
            '()))
(define rt-txn
  (make-txn
   (list (cmp-value-eq "k" "v0")
         (cmp-mod-lt "k" 100))
   ; success ops
   (list (op-put (b "k") (b "v1") 7)
         (op-del (b "d") (b "e"))                 ; range-end set
         (op-del (b "single") #f)                 ; range-end #f (single key)
         (op-range (b "p") (b "q")
                   (list (cons 'limit 5) (cons 'count-only #f)
                         (cons 'sort-order 'ascend)))
         (op-txn rt-nested))
   ; failure ops
   (list (op-range (b "k") #f '()))))

(define rt-decoded (txn-decode (txn-encode rt-txn)))
(check "round-trip: struct equal? after encode/decode" #t (equal? rt-txn rt-decoded))
; spot-check a couple of nested pieces survived flattening
(check "round-trip: success op count = 5" 5 (length (txn-success rt-decoded)))
(check "round-trip: nested txn recovered"
       'txn (op-kind (list-ref (txn-success rt-decoded) 4)))
(check "round-trip: nested txn's inner put key = n"
       (b "n")
       (vector-ref (car (txn-success (vector-ref (list-ref (txn-success rt-decoded) 4) 1))) 1))
(check "round-trip: range op opts survived"
       (list (cons 'limit 5) (cons 'count-only #f) (cons 'sort-order 'ascend))
       (vector-ref (list-ref (txn-success rt-decoded) 3) 3))
(check "round-trip: del with #f range-end survived"
       #f (vector-ref (list-ref (txn-success rt-decoded) 2) 2))

; ===========================================================================
(section "CAS: VALUE==v0 ? [Put v1] : [Range] — the canonical etcd pattern")
(put "PUT" "cas" "v0")                              ; rev r
(define cas-rev-before (cur-rev))
(define cas-txn
  (make-txn (list (cmp-value-eq "cas" "v0"))
            (list (op-put (b "cas") (b "v1") 0))    ; success
            (list (op-range (b "cas") #f '()))))    ; failure

; first run: VALUE==v0 holds -> success branch puts v1, rev bumps
(let ((res (run-txn cas-txn)))
  (check "CAS#1 succeeded? = #t" #t (car res))
  (check "CAS#1 one response (the Put)" 1 (length (cdr res)))
  (check "CAS#1 response is (put . modrev)"
         (cons 'put (+ cas-rev-before 1)) (car (cdr res))))
(check "CAS#1 cas now v1" "v1" (val-of (latest "cas")))
(check "CAS#1 rev bumped by 1" (+ cas-rev-before 1) (cur-rev))

; second run: VALUE is now v1 (≠ v0) -> failure branch Ranges, NO write, NO bump
(define cas-rev-after1 (cur-rev))
(let ((res (run-txn cas-txn)))
  (check "CAS#2 succeeded? = #f" #f (car res))
  (check "CAS#2 one response (the Range)" 1 (length (cdr res)))
  (let ((range-resp (car (cdr res))))
    (check "CAS#2 failure Range count=1" 1 (car range-resp))
    (check "CAS#2 failure Range returns v1"
           "v1" (utf8->string (kv-rec-value (cdr (car (cdr range-resp))))))))
(check "CAS#2 cas unchanged (still v1)" "v1" (val-of (latest "cas")))
(check "CAS#2 rev NOT bumped (read-only branch)" cas-rev-after1 (cur-rev))

; ===========================================================================
(section "absent-key compare: VERSION==0 ⇒ 'create if not exists' idiom")
; key "fresh" does not exist -> VERSION(fresh)==0 holds -> Put runs.
(let ((res (run-txn (make-txn (list (cmp-version-eq "fresh" 0))
                              (list (op-put (b "fresh") (b "born") 0))
                              '()))))
  (check "create-if-absent succeeded? = #t" #t (car res)))
(check "create-if-absent wrote value" "born" (val-of (latest "fresh")))
(check "create-if-absent version=1" 1 (kv-rec-version (latest "fresh")))

; run the SAME guard again: now VERSION==1 (≠0) -> guard fails -> failure (empty)
(define fresh-rev-after (cur-rev))
(let ((res (run-txn (make-txn (list (cmp-version-eq "fresh" 0))
                              (list (op-put (b "fresh") (b "again") 0))
                              '()))))
  (check "create-if-absent#2 succeeded? = #f" #f (car res))
  (check "create-if-absent#2 empty failure -> no responses" '() (cdr res)))
(check "create-if-absent#2 value unchanged" "born" (val-of (latest "fresh")))
(check "create-if-absent#2 empty-branch did NOT bump rev" fresh-rev-after (cur-rev))

; ===========================================================================
(section "each compare target: VERSION / CREATE / MOD / VALUE / LEASE")
; set up "t" with known fields: put twice (version=2), and with a lease.
(put "PUT" "t" "tv1")                  ; create_rev = this, version 1
(define t-create (kv-rec-create-rev (latest "t")))
(put "PUT" "t" "tv2" "55")             ; version 2, lease 55, mod_rev = this
(define t-mod (kv-rec-mod-rev (latest "t")))

; helper: does a single-compare guard pass? (success=[Range], failure=[]) -> succeeded?
(define (guard-passes? cmp)
  (car (run-txn (make-txn (list cmp)
                          (list (op-range (b "t") #f '()))   ; read-only success (no bump)
                          '()))))

; VERSION == 2 (true), VERSION > 1 (true), VERSION == 1 (false)
(check "VERSION==2 true"  #t (guard-passes? (cmp-version-eq "t" 2)))
(check "VERSION>1 true"   #t (guard-passes? (cmp-version-gt "t" 1)))
(check "VERSION==1 false" #f (guard-passes? (cmp-version-eq "t" 1)))
; CREATE == t-create (true), CREATE == t-create+1 (false)
(check "CREATE==create true"  #t (guard-passes? (cmp-create-eq "t" t-create)))
(check "CREATE==create+1 false" #f (guard-passes? (cmp-create-eq "t" (+ t-create 1))))
; MOD == t-mod (true), MOD < t-mod+1 (true), MOD == t-mod-... use EQUAL false
(check "MOD==mod true"     #t (guard-passes? (cmp-mod-eq "t" t-mod)))
(check "MOD<mod+1 true"    #t (guard-passes? (cmp-mod-lt "t" (+ t-mod 1))))
(check "MOD==mod-1 false"  #f (guard-passes? (cmp-mod-eq "t" (- t-mod 1))))
; VALUE == tv2 (true), VALUE != tv1 (true), VALUE == tv1 (false)
(check "VALUE==tv2 true"   #t (guard-passes? (cmp-value-eq "t" "tv2")))
(check "VALUE!=tv1 true"   #t (guard-passes? (cmp-value-ne "t" "tv1")))
(check "VALUE==tv1 false"  #f (guard-passes? (cmp-value-eq "t" "tv1")))
; LEASE == 55 (true), LEASE != 0 (true), LEASE == 0 (false)
(check "LEASE==55 true"    #t (guard-passes? (cmp-lease-eq "t" 55)))
(check "LEASE!=0 true"     #t (guard-passes? (cmp-lease-ne "t" 0)))
(check "LEASE==0 false"    #f (guard-passes? (cmp-lease-eq "t" 0)))

; ===========================================================================
(section "multiple ops in one branch: success=[Put a, Put b, DeleteRange c]")
; pre-seed key "delc" so the DeleteRange has a victim (real effect).
(put "PUT" "delc" "tofill")
(define multi-rev-before (cur-rev))
(define multi-txn
  (make-txn '()                                     ; empty compares ⇒ success branch
            (list (op-put (b "ma") (b "AA") 0)
                  (op-put (b "mb") (b "BB") 0)
                  (op-del (b "delc") #f))
            '()))
(let ((res (run-txn multi-txn)))
  (check "multi succeeded? = #t (empty compares)" #t (car res))
  (check "multi: 3 responses" 3 (length (cdr res)))
  (check "multi: resp0 put ma" 'put (car (list-ref (cdr res) 0)))
  (check "multi: resp1 put mb" 'put (car (list-ref (cdr res) 1)))
  (check "multi: resp2 del-count = 1" (cons 'del-count 1) (list-ref (cdr res) 2)))
; ONE main rev bump for the whole Txn
(define multi-main (+ multi-rev-before 1))
(check "multi: current-rev bumped exactly +1" multi-main (cur-rev))
; a and b share the SAME main rev as their mod_rev (successive subs under one main)
(check "multi: ma mod_rev = new main" multi-main (kv-rec-mod-rev (latest "ma")))
(check "multi: mb mod_rev = new main" multi-main (kv-rec-mod-rev (latest "mb")))
(check "multi: ma value" "AA" (val-of (latest "ma")))
(check "multi: mb value" "BB" (val-of (latest "mb")))
(check "multi: delc deleted" #f (latest "delc"))

; sub-revisions are distinct within the one main rev (ma sub 0, mb sub 1, del sub 2).
; Verify via the REV-CF event log: three events all at this main, in op order.
(let* ((events (kv-scan CTX (mvcc-byte NS-REV)))
       (this-main (filter (lambda (kv) (= (ev-mod-rev (event-decode (cdr kv))) multi-main))
                          events))
       (kinds (map (lambda (kv) (ev-kind (event-decode (cdr kv)))) this-main)))
  (check "multi: 3 REV-CF events at this main" 3 (length this-main))
  (check "multi: events = PUT PUT DELETE in sub order"
         (list EV-PUT EV-PUT EV-DELETE) kinds))

; ===========================================================================
(section "revision semantics: pure-read Txn does NOT bump (cw-u4a.40)")
(put "PUT" "ro" "roval")
(define ro-rev-before (cur-rev))
; success branch is a single Range (no mutation) -> no bump
(let ((res (run-txn (make-txn (list (cmp-value-eq "ro" "roval"))
                              (list (op-range (b "ro") #f '()))
                              '()))))
  (check "pure-read succeeded? = #t" #t (car res))
  (check "pure-read response is the range (count=1)" 1 (car (car (cdr res)))))
(check "pure-read did NOT bump current-rev" ro-rev-before (cur-rev))

; a zero-EFFECT mutating branch also does not bump: DeleteRange matching no live key.
(let ((res (run-txn (make-txn '()
                              (list (op-del (b "no-such-key") #f))
                              '()))))
  (check "zero-effect del succeeded? = #t" #t (car res))
  (check "zero-effect del-count = 0" (cons 'del-count 0) (car (cdr res))))
(check "zero-effect del did NOT bump current-rev" ro-rev-before (cur-rev))

; control: a Txn that DOES write bumps exactly once.
(let ((res (run-txn (make-txn '() (list (op-put (b "wb") (b "x") 0)) '()))))
  (check "writing txn succeeded? = #t" #t (car res)))
(check "writing txn bumped current-rev +1" (+ ro-rev-before 1) (cur-rev))
(check "writing txn: wb mod_rev = new main" (+ ro-rev-before 1) (kv-rec-mod-rev (latest "wb")))

; ===========================================================================
(section "responses: success vs failure branch content")
(put "PUT" "rk" "rv")
; guard FALSE (VALUE==nope) -> failure branch runs: [Range rk] then [DeleteRange zzz (no effect)]
(let ((res (run-txn (make-txn (list (cmp-value-eq "rk" "nope"))
                              (list (op-put (b "rk") (b "should-not-write") 0))
                              (list (op-range (b "rk") #f '())
                                    (op-del (b "zzz") #f))))))
  (check "resp branch: succeeded? = #f" #f (car res))
  (check "resp branch: 2 failure responses" 2 (length (cdr res)))
  (check "resp branch: resp0 Range count=1" 1 (car (list-ref (cdr res) 0)))
  (check "resp branch: resp0 Range value=rv"
         "rv" (utf8->string (kv-rec-value (cdr (car (cdr (list-ref (cdr res) 0)))))))
  (check "resp branch: resp1 del-count=0" (cons 'del-count 0) (list-ref (cdr res) 1)))
(check "resp branch: rk NOT overwritten by success op" "rv" (val-of (latest "rk")))

; ===========================================================================
(section "nested Txn: a success op that is itself a Txn")
; outer success runs [Put outer, <nested Txn>]; the nested Txn's guard
; (VERSION(nest)==0) holds -> nested success Puts "nest".  Result of the nested
; Txn must appear, intact, as the outer response for that op.
(define nested-rev-before (cur-rev))
(define inner-txn
  (make-txn (list (cmp-version-eq "nest" 0))
            (list (op-put (b "nest") (b "deep") 0))
            (list (op-range (b "nest") #f '()))))
(define outer-txn
  (make-txn '()
            (list (op-put (b "outer") (b "shallow") 0)
                  (op-txn inner-txn))
            '()))
(let* ((res (run-txn outer-txn))
       (responses (cdr res)))
  (check "nested: outer succeeded? = #t" #t (car res))
  (check "nested: 2 outer responses" 2 (length responses))
  (check "nested: resp0 is outer Put" 'put (car (list-ref responses 0)))
  (let ((inner-res (list-ref responses 1)))      ; the nested Txn's (succeeded? . responses)
    (check "nested: resp1 is the inner Txn result, succeeded? = #t" #t (car inner-res))
    (check "nested: inner had 1 response (its Put)" 1 (length (cdr inner-res)))
    (check "nested: inner response is a put" 'put (car (car (cdr inner-res))))))
(check "nested: outer key written" "shallow" (val-of (latest "outer")))
(check "nested: inner key written" "deep" (val-of (latest "nest")))
; outer Put + inner Put share ONE main rev (nested continues the same sub sequence)
(define nested-main (+ nested-rev-before 1))
(check "nested: ONE rev bump for outer+inner" nested-main (cur-rev))
(check "nested: outer mod_rev = main" nested-main (kv-rec-mod-rev (latest "outer")))
(check "nested: nest  mod_rev = main (shared)" nested-main (kv-rec-mod-rev (latest "nest")))

; ===========================================================================
(done!)
