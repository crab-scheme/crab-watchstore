; src/txn.scm — etcd Txn (If/Then/Else) over the MVCC data model (cw-u4a.10).
;
; An etcd Txn is the atomic compare-and-swap at the heart of etcd's consistency
; story (it is what makes the strict-serializable Jepsen workloads pass):
;
;   Txn = { compares: [Compare...], success: [RequestOp...], failure: [RequestOp...] }
;
; Eval rule: if EVERY compare is true (empty compares ⇒ vacuously true) run the
; SUCCESS op list, else run the FAILURE op list.  The whole Txn is ONE Raft entry
; applied atomically: it bumps the global MAIN revision by exactly 1 — but ONLY if
; the executed branch performs ≥1 mutating op that has effect (ADR-0001 §2; this
; resolves cw-u4a.40 for the Txn path: a pure-read Txn, or one whose executed ops
; write nothing, does NOT advance the revision, matching real etcd).  Within the
; Txn each mutating op gets a successive SUB-revision under that single main rev.
;
; CRITICAL — FLAT serialization.  `node-send` (the Raft AppendEntries path) cannot
; carry nested Scheme lists; that is the exact bug that broke crab-cache's
; MULTI/EXEC.  So a Txn is flattened to a single self-describing, length-prefixed
; BYTEVECTOR (`txn-encode`) before it is proposed to Raft, and decoded back
; (`txn-decode`) on apply.  The byte idioms below are the same length-prefix /
; u64be-int idioms the KeyValue record encoder in mvcc.scm uses.
;
; Depends on: mvcc.scm (mvcc-get-latest / mvcc-put! / mvcc-delete-range! /
; mvcc-range / live-keys-in-range / mvcc-set-current-rev! / kv-rec-* accessors /
; range-end-unset?) and encoding.scm (subbv, u64->bytes, bytes->u64).
;
; INCLUDE THIS AFTER src/mvcc.scm.

; ===========================================================================
; In-memory Txn structures
; ===========================================================================
;
; A Txn struct is a 3-vector:  #( compares success-ops failure-ops )
;   compares     : list of compare 4-vectors
;   success-ops  : list of op vectors
;   failure-ops  : list of op vectors
;
; A compare is a 4-vector:  #( target result key target-value )
;   target  : one of CMP-VERSION / CMP-CREATE / CMP-MOD / CMP-VALUE / CMP-LEASE
;   result  : one of RES-EQUAL / RES-GREATER / RES-LESS / RES-NOT-EQUAL
;   key     : bytevector (the key whose current visible record is compared)
;   target-value : bytevector — for VALUE it is the raw bytes; for the integer
;                  targets (VERSION/CREATE/MOD/LEASE) it is u64be(n) (8 bytes).
;
; An op is a vector tagged by its head symbol:
;   ('put  key value lease)        lease = integer (0 = none)
;   ('del  key range-end)          range-end = bytevector | #f  (#f/empty ⇒ single key)
;   ('range key range-end opts)    opts = assoc list (symbol . value), as mvcc-range wants
;   ('txn  <txn-struct>)           a nested Txn (recursively evaluated/encoded)

; ---- compare targets / results (wire-stable small ints) ----
(define CMP-VERSION 0)
(define CMP-CREATE  1)   ; create_revision
(define CMP-MOD     2)   ; mod_revision
(define CMP-VALUE   3)
(define CMP-LEASE   4)

(define RES-EQUAL     0)
(define RES-GREATER   1)
(define RES-LESS      2)
(define RES-NOT-EQUAL 3)

; ---- op kind tags (wire-stable small ints) ----
(define OP-PUT   0)
(define OP-DEL   1)
(define OP-RANGE 2)
(define OP-TXN   3)

; ---- constructors (so call sites read clearly) ----
(define (make-txn compares success failure) (vector compares success failure))
(define (txn-compares t) (vector-ref t 0))
(define (txn-success  t) (vector-ref t 1))
(define (txn-failure  t) (vector-ref t 2))

(define (make-compare target result key target-value)
  (vector target result key target-value))
(define (cmp-target c) (vector-ref c 0))
(define (cmp-result c) (vector-ref c 1))
(define (cmp-key    c) (vector-ref c 2))
(define (cmp-tval   c) (vector-ref c 3))

(define (op-put   key value lease) (vector 'put key value lease))
(define (op-del   key range-end)   (vector 'del key range-end))
(define (op-range key range-end opts) (vector 'range key range-end opts))
(define (op-txn   txn-struct)      (vector 'txn txn-struct))
(define (op-kind  op) (vector-ref op 0))

; ===========================================================================
; FLAT serialization  (txn-encode / txn-decode)
; ===========================================================================
;
; Self-describing, length-prefixed, NO nested Scheme lists survive on the wire —
; this is exactly the form proposed to Raft and replicated.
;
;   txn   := u32be n_compares ‖ compare*  ‖ u32be n_success ‖ op* ‖ u32be n_failure ‖ op*
;   compare := u8 target ‖ u8 result ‖ u64be keylen ‖ key ‖ u64be tvlen ‖ target-value
;   op    := u8 kind ‖ body, where body by kind:
;     OP-PUT   : u64be keylen ‖ key ‖ u64be vallen ‖ value ‖ u64be lease
;     OP-DEL   : u64be keylen ‖ key ‖ <opt-bv range-end>
;     OP-RANGE : u64be keylen ‖ key ‖ <opt-bv range-end> ‖ u32be n_opts ‖ rangeopt*
;     OP-TXN   : u64be txnlen ‖ <recursively-encoded txn bytes>
;   <opt-bv X> := u8 present ‖ [ u64be len ‖ bytes ]   (present=0 ⇒ #f, no len/bytes)
;   rangeopt := u64be namelen ‖ name(utf8 symbol) ‖ u8 vkind ‖ vbody, where:
;     vkind 0 = int  : u64be n
;     vkind 1 = bool : u8 (0/1)
;     vkind 2 = sym  : u64be len ‖ utf8 symbol-name
;
; u32be is encoded as the low 4 bytes of a u64be(8) we then slice — we reuse
; u64->bytes/bytes->u64 (already in encoding.scm) and keep counts as full u64be
; for simplicity, since these are small and node-send carries the whole blob anyway.

; -- length-prefixed bytevector (u64be len ‖ bytes) --
(define (enc-lp bv)
  (bytevector-append (u64->bytes (bytevector-length bv)) bv))

; -- optional bytevector: u8 present ‖ [u64be len ‖ bytes] --
(define (enc-opt-bv bv)              ; bv is a bytevector OR #f
  (if bv
      (bytevector-append (mvcc-byte 1) (enc-lp bv))
      (mvcc-byte 0)))

; -- u32-as-count: we store counts as u64be for idiom-reuse --
(define (enc-count n) (u64->bytes n))

; ---- encode one compare ----
(define (compare-encode c)
  (bytevector-append
   (mvcc-byte (cmp-target c))
   (mvcc-byte (cmp-result c))
   (enc-lp (cmp-key c))
   (enc-lp (cmp-tval c))))

; ---- encode one range-opt (symbol . value) cell ----
(define (rangeopt-encode cell)
  (let* ((name (symbol->string (car cell)))
         (val  (cdr cell))
         (name-bv (string->utf8 name)))
    (bytevector-append
     (enc-lp name-bv)
     (cond
       ((and (number? val) (exact? val) (integer? val))
        (bytevector-append (mvcc-byte 0) (u64->bytes val)))
       ((boolean? val)
        (bytevector-append (mvcc-byte 1) (mvcc-byte (if val 1 0))))
       ((symbol? val)
        (bytevector-append (mvcc-byte 2) (enc-lp (string->utf8 (symbol->string val)))))
       (else (error "rangeopt-encode: unsupported opt value" cell))))))

; ---- encode one op ----
(define (op-encode op)
  (let ((kind (op-kind op)))
    (cond
      ((eq? kind 'put)
       (bytevector-append
        (mvcc-byte OP-PUT)
        (enc-lp (vector-ref op 1))                ; key
        (enc-lp (vector-ref op 2))                ; value
        (u64->bytes (vector-ref op 3))))          ; lease
      ((eq? kind 'del)
       (bytevector-append
        (mvcc-byte OP-DEL)
        (enc-lp (vector-ref op 1))                ; key
        (enc-opt-bv (vector-ref op 2))))          ; range-end (opt)
      ((eq? kind 'range)
       (let ((opts (vector-ref op 3)))
         (bytevector-append
          (mvcc-byte OP-RANGE)
          (enc-lp (vector-ref op 1))              ; key
          (enc-opt-bv (vector-ref op 2))          ; range-end (opt)
          (enc-count (length opts))
          (apply bytevector-append (map rangeopt-encode opts)))))
      ((eq? kind 'txn)
       (bytevector-append
        (mvcc-byte OP-TXN)
        (enc-lp (txn-encode (vector-ref op 1)))))  ; nested txn, recursively encoded
      (else (error "op-encode: unknown op kind" kind)))))

; ---- encode a list of ops ----
(define (ops-encode ops)
  (bytevector-append
   (enc-count (length ops))
   (apply bytevector-append (map op-encode ops))))

; ---- top-level: encode a whole Txn struct to one flat bytevector ----
(define (txn-encode t)
  (bytevector-append
   (enc-count (length (txn-compares t)))
   (apply bytevector-append (map compare-encode (txn-compares t)))
   (ops-encode (txn-success t))
   (ops-encode (txn-failure t))))

; ---------------------------------------------------------------------------
; Decoding — a tiny cursor (offset threaded explicitly; everything returns the
; decoded value plus the next offset, so the format stays purely positional).
; ---------------------------------------------------------------------------

; read u64be at off -> (values n next-off)
(define (dec-u64 b off) (values (bytes->u64 b off) (+ off 8)))

; read length-prefixed bytevector -> (values bv next-off)
(define (dec-lp b off)
  (let* ((len (bytes->u64 b off))
         (start (+ off 8))
         (end (+ start len)))
    (values (subbv b start end) end)))

; read optional bytevector -> (values (bv|#f) next-off)
(define (dec-opt-bv b off)
  (let ((present (bytevector-u8-ref b off)))
    (if (= present 0)
        (values #f (+ off 1))
        (call-with-values (lambda () (dec-lp b (+ off 1)))
          (lambda (bv next) (values bv next))))))

; read one compare -> (values compare next-off)
(define (dec-compare b off)
  (let ((target (bytevector-u8-ref b off))
        (result (bytevector-u8-ref b (+ off 1))))
    (call-with-values (lambda () (dec-lp b (+ off 2)))
      (lambda (key off2)
        (call-with-values (lambda () (dec-lp b off2))
          (lambda (tval off3)
            (values (make-compare target result key tval) off3)))))))

; read one range-opt cell -> (values (sym . value) next-off)
(define (dec-rangeopt b off)
  (call-with-values (lambda () (dec-lp b off))
    (lambda (name-bv off2)
      (let ((name (string->symbol (utf8->string name-bv)))
            (vkind (bytevector-u8-ref b off2)))
        (cond
          ((= vkind 0)   ; int
           (call-with-values (lambda () (dec-u64 b (+ off2 1)))
             (lambda (n next) (values (cons name n) next))))
          ((= vkind 1)   ; bool
           (values (cons name (= 1 (bytevector-u8-ref b (+ off2 1)))) (+ off2 2)))
          ((= vkind 2)   ; symbol
           (call-with-values (lambda () (dec-lp b (+ off2 1)))
             (lambda (sbv next) (values (cons name (string->symbol (utf8->string sbv))) next))))
          (else (error "dec-rangeopt: bad vkind" vkind)))))))

; read n range-opt cells -> (values opts-alist next-off)
(define (dec-rangeopts b off n)
  (let loop ((i 0) (o off) (acc '()))
    (if (= i n)
        (values (reverse acc) o)
        (call-with-values (lambda () (dec-rangeopt b o))
          (lambda (cell next) (loop (+ i 1) next (cons cell acc)))))))

; read one op -> (values op next-off)
(define (dec-op b off)
  (let ((kind (bytevector-u8-ref b off))
        (o1   (+ off 1)))
    (cond
      ((= kind OP-PUT)
       (call-with-values (lambda () (dec-lp b o1))
         (lambda (key o2)
           (call-with-values (lambda () (dec-lp b o2))
             (lambda (value o3)
               (call-with-values (lambda () (dec-u64 b o3))
                 (lambda (lease o4) (values (op-put key value lease) o4))))))))
      ((= kind OP-DEL)
       (call-with-values (lambda () (dec-lp b o1))
         (lambda (key o2)
           (call-with-values (lambda () (dec-opt-bv b o2))
             (lambda (rend o3) (values (op-del key rend) o3))))))
      ((= kind OP-RANGE)
       (call-with-values (lambda () (dec-lp b o1))
         (lambda (key o2)
           (call-with-values (lambda () (dec-opt-bv b o2))
             (lambda (rend o3)
               (let ((n-opts (bytes->u64 b o3)))
                 (call-with-values (lambda () (dec-rangeopts b (+ o3 8) n-opts))
                   (lambda (opts o4) (values (op-range key rend opts) o4)))))))))
      ((= kind OP-TXN)
       (call-with-values (lambda () (dec-lp b o1))
         (lambda (txn-bv o2) (values (op-txn (txn-decode txn-bv)) o2))))
      (else (error "dec-op: unknown op kind" kind)))))

; read n ops -> (values ops-list next-off)
(define (dec-ops b off n)
  (let loop ((i 0) (o off) (acc '()))
    (if (= i n)
        (values (reverse acc) o)
        (call-with-values (lambda () (dec-op b o))
          (lambda (op next) (loop (+ i 1) next (cons op acc)))))))

; read n compares -> (values compares-list next-off)
(define (dec-compares b off n)
  (let loop ((i 0) (o off) (acc '()))
    (if (= i n)
        (values (reverse acc) o)
        (call-with-values (lambda () (dec-compare b o))
          (lambda (c next) (loop (+ i 1) next (cons c acc)))))))

; ---- top-level: decode a flat bytevector back to a Txn struct ----
(define (txn-decode b)
  (let ((n-cmp (bytes->u64 b 0)))
    (call-with-values (lambda () (dec-compares b 8 n-cmp))
      (lambda (compares off1)
        (let ((n-succ (bytes->u64 b off1)))
          (call-with-values (lambda () (dec-ops b (+ off1 8) n-succ))
            (lambda (success off2)
              (let ((n-fail (bytes->u64 b off2)))
                (call-with-values (lambda () (dec-ops b (+ off2 8) n-fail))
                  (lambda (failure off3)
                    (make-txn compares success failure)))))))))))

; ===========================================================================
; Compare evaluation
; ===========================================================================
;
; A compare is evaluated against the key's CURRENT visible record
; (mvcc-get-latest).  ABSENT key (none / tombstoned) ⇒ version=0, create_rev=0,
; mod_rev=0, lease=0, value=empty (#u8()).  The compare is true iff
; (current-target-field  <result>  target-value).

(define EMPTY-BV (make-bytevector 0 0))

; Pull the current field value for a target from a record (or #f = absent).
(define (compare-current-field rec target)
  (if rec
      (cond
        ((= target CMP-VERSION) (kv-rec-version    rec))
        ((= target CMP-CREATE)  (kv-rec-create-rev rec))
        ((= target CMP-MOD)     (kv-rec-mod-rev    rec))
        ((= target CMP-LEASE)   (kv-rec-lease      rec))
        ((= target CMP-VALUE)   (kv-rec-value      rec))
        (else (error "compare-current-field: bad target" target)))
      ; absent key: etcd treats every field as its zero
      (if (= target CMP-VALUE) EMPTY-BV 0)))

; Decode the target-value side to compare against (int targets carry u64be).
(define (compare-target-value c)
  (let ((target (cmp-target c))
        (tval   (cmp-tval c)))
    (if (= target CMP-VALUE)
        tval                              ; raw bytes
        (bytes->u64 tval 0))))            ; u64be integer

; Apply the result relation.  For VALUE the operands are bytevectors (EQUAL via
; equal?, ordering via lexicographic bv<?); for the integer targets they are ints.
(define (compare-relation result cur tgt value?)
  (cond
    ((= result RES-EQUAL)
     (if value? (equal? cur tgt) (= cur tgt)))
    ((= result RES-NOT-EQUAL)
     (if value? (not (equal? cur tgt)) (not (= cur tgt))))
    ((= result RES-GREATER)
     (if value? (bv<? tgt cur) (> cur tgt)))    ; cur > tgt
    ((= result RES-LESS)
     (if value? (bv<? cur tgt) (< cur tgt)))    ; cur < tgt
    (else (error "compare-relation: bad result" result))))

(define (compare-eval ctx c)
  (let* ((target (cmp-target c))
         (rec    (mvcc-get-latest ctx (cmp-key c)))
         (cur    (compare-current-field rec target))
         (tgt    (compare-target-value c))
         (value? (= target CMP-VALUE)))
    (compare-relation (cmp-result c) cur tgt value?)))

; ALL compares true? (empty ⇒ #t)
(define (compares-all-true? ctx compares)
  (let loop ((cs compares))
    (cond ((null? cs) #t)
          ((compare-eval ctx (car cs)) (loop (cdr cs)))
          (else #f))))

; ===========================================================================
; Effect peek — does a chosen branch mutate (with effect)?  (resolves cw-u4a.40)
; ===========================================================================
;
; PURE (reads only): used BEFORE assigning the main rev to decide whether the Txn
; advances the revision.  Rule (ADR-0001 §2):
;   - a Put always has effect (always writes a version);
;   - a DeleteRange has effect iff ≥1 LIVE key falls in its range (a pure read);
;   - a Range never has effect;
;   - a nested Txn has effect iff ITS chosen branch has effect (recurse).
;
; Soundness vs the actual apply: a Put's effect is unconditional, so any branch
; containing a Put bumps regardless of ordering.  The only branch whose bump
; decision depends on store state is one with NO Put — i.e. DeleteRange/Range/
; nested-del only — and in such a branch no earlier op creates a key, so the
; liveness this peek reads is exactly the liveness the apply will act on.  Hence
; the peek's verdict equals the apply's actual effect.

(define (op-will-mutate? ctx op)
  (let ((kind (op-kind op)))
    (cond
      ((eq? kind 'put) #t)
      ((eq? kind 'del)
       (not (null? (live-keys-in-range ctx (vector-ref op 1) (vector-ref op 2)))))
      ((eq? kind 'range) #f)
      ((eq? kind 'txn) (txn-will-mutate? ctx (vector-ref op 1)))
      (else (error "op-will-mutate?: unknown op kind" kind)))))

(define (ops-will-mutate? ctx ops)
  (let loop ((os ops))
    (cond ((null? os) #f)
          ((op-will-mutate? ctx (car os)) #t)
          (else (loop (cdr os))))))

(define (txn-will-mutate? ctx t)
  (let ((branch (if (compares-all-true? ctx (txn-compares t))
                    (txn-success t)
                    (txn-failure t))))
    (ops-will-mutate? ctx branch)))

; ===========================================================================
; Txn evaluation + application
; ===========================================================================
;
; (txn-eval-apply ctx t main-rev start-sub)
;   -> (values result next-sub)
; where result = (succeeded? . responses):
;   succeeded?  : #t iff the SUCCESS branch ran (all compares true)
;   responses   : one response per executed op, in order:
;     Put         -> (cons 'put mod-rev)
;     DeleteRange -> (cons 'del-count n)
;     Range       -> the mvcc-range result: (count . kvlist) | (cons 'err-* ...)
;     nested Txn  -> that Txn's full (succeeded? . responses) result
; and next-sub is the sub-revision cursor after this (and any nested) Txn — so a
; nested Txn shares the SAME main rev and CONTINUES the sub-revision sequence.
;
; Sub-revision numbering (ADR-0001 §2, pinned here): the FIRST mutating op of the
; Txn writes at sub = start-sub (0 for a top-level Txn), and the cursor advances
; by exactly the number of sub-revisions a mutating op consumed:
;   - Put consumes 1 sub (writes one version);
;   - DeleteRange consumes one sub PER LIVE key deleted (mvcc-delete-range!
;     increments sub per victim), so it advances the cursor by its deleted count;
;   - Range/no-op consume 0 subs;
;   - a nested Txn consumes whatever its mutating ops consumed (threaded through).
; Non-mutating ops never burn a sub, so sub numbers are dense over actual writes.

(define (txn-eval-apply ctx t main-rev start-sub)
  (let* ((succeeded? (compares-all-true? ctx (txn-compares t)))
         (branch     (if succeeded? (txn-success t) (txn-failure t))))
    (let loop ((ops branch) (sub start-sub) (responses '()))
      (if (null? ops)
          (values (cons succeeded? (reverse responses)) sub)
          (let ((op (car ops)))
            (call-with-values
                (lambda () (op-eval-apply ctx op main-rev sub))
              (lambda (resp next-sub)
                (loop (cdr ops) next-sub (cons resp responses)))))))))

; Execute ONE op at (main-rev, sub) -> (values response next-sub).
(define (op-eval-apply ctx op main-rev sub)
  (let ((kind (op-kind op)))
    (cond
      ((eq? kind 'put)
       (let ((mr (mvcc-put! ctx (vector-ref op 1) (vector-ref op 2)
                            (vector-ref op 3) main-rev sub)))
         (values (cons 'put mr) (+ sub 1))))            ; Put consumes 1 sub
      ((eq? kind 'del)
       (let ((n (mvcc-delete-range! ctx (vector-ref op 1) (vector-ref op 2)
                                    main-rev sub)))
         (values (cons 'del-count n) (+ sub n))))       ; consumes 1 sub per victim
      ((eq? kind 'range)
       ; Range is a pure read at current rev — never consumes a sub.
       (values (mvcc-range ctx (vector-ref op 1) (vector-ref op 2) (vector-ref op 3))
               sub))
      ((eq? kind 'txn)
       ; Nested Txn shares this main rev and CONTINUES the sub sequence.
       (txn-eval-apply ctx (vector-ref op 1) main-rev sub))
      (else (error "op-eval-apply: unknown op kind" kind)))))

; ===========================================================================
; Top-level Txn apply (used by mvcc-apply's "TXN" case)
; ===========================================================================
;
; (txn-apply ctx txn-bytes prev-rev) -> (succeeded? . responses)
;
; 1. Decode the flat txn-bytes (node-send safe form) back to a struct.
; 2. PEEK (pure): will the executed branch mutate with effect?
; 3. main = prev-rev + 1 ONLY if it will mutate, else main = prev-rev (no bump).
; 4. Apply the branch's ops threading sub-revs from 0.
; 5. Persist current-rev ONLY if we bumped (zero-effect ⇒ revision unchanged,
;    resolving cw-u4a.40 for the Txn path).

(define (txn-apply ctx txn-bytes prev-rev)
  (let* ((t        (txn-decode txn-bytes))
         (mutates? (txn-will-mutate? ctx t))
         (main     (if mutates? (+ prev-rev 1) prev-rev)))
    (call-with-values
        (lambda () (txn-eval-apply ctx t main 0))
      (lambda (result _next-sub)
        (if mutates?
            (mvcc-set-current-rev! ctx main))
        result))))
