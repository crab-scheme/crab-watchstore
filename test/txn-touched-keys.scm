; test/txn-touched-keys.scm — cw-kp0 Phase 4 foundation: the distinct keys a Txn
; reads/writes (compare targets + put/del op keys of BOTH branches, deduped). Mapping
; these to shards (key-shard) gives the Txn's participant shards: all-on-one-shard ⇒
; commit atomically as today; spanning shards ⇒ the cross-shard 2PC coordinator.
; The function under test mirrors txn-touched-keys in src/server/grpc-kv.scm (keep in sync).
; Run from repo root:  crabscheme run test/txn-touched-keys.scm
(include "test/harness.scm")
(include "src/txn.scm")

; --- mirror of src/server/grpc-kv.scm:txn-touched-keys (pure; same logic) ---
(define (txn-touched-keys t)
  (let loop ((in (append (map cmp-key (txn-compares t))
                         (map (lambda (o) (vector-ref o 1)) (txn-success t))
                         (map (lambda (o) (vector-ref o 1)) (txn-failure t))))
             (out '()))
    (cond ((null? in) (reverse out))
          ((member (car in) out) (loop (cdr in) out))
          (else (loop (cdr in) (cons (car in) out))))))

; a Txn's participant shards (the algorithm the cross-shard coordinator will branch on)
(define (key-shard-mod k n)
  (if (<= n 1) 0
      (let ((len (bytevector-length k)))
        (let h ((i 0) (acc 2166136261))
          (if (= i len) (modulo acc n)
              (h (+ i 1) (modulo (* (bitwise-xor acc (bytevector-u8-ref k i)) 16777619)
                                 4294967296)))))))
(define (txn-participant-shards t n)
  (let loop ((ks (txn-touched-keys t)) (out '()))
    (cond ((null? ks) (reverse out))
          ((memv (key-shard-mod (car ks) n) out) (loop (cdr ks) out))
          (else (loop (cdr ks) (cons (key-shard-mod (car ks) n) out))))))

(define (b s) (string->utf8 s))
(define (cmp k) (make-compare 0 0 k (b "")))   ; target/result irrelevant for key extraction

(section "touched-keys: compares + both branches, deduped")
; compares on k1,k2 ; success puts k2 (dup) + k3 ; failure puts k1 (dup)
(define t
  (make-txn (list (cmp (b "k1")) (cmp (b "k2")))
            (list (op-put (b "k2") (b "v") 0) (op-put (b "k3") (b "v") 0))
            (list (op-put (b "k1") (b "v") 0))))
(define keys (map utf8->string (txn-touched-keys t)))
(display "  touched: ") (write keys) (newline)
(check "distinct keys across compares + success + failure" '("k1" "k2" "k3") keys)

(section "participant-shards: single-shard vs spanning")
; with N=1 every key is shard 0 -> single participant
(check "N=1 -> one participant shard {0}" '(0) (txn-participant-shards t 1))
; a single-key txn -> exactly one participant shard whatever N
(define t1 (make-txn (list (cmp (b "only"))) (list (op-put (b "only") (b "v") 0)) '()))
(check "single-key txn -> one participant shard" 1 (length (txn-participant-shards t1 3)))
; the 3-key txn across N=3 -> may span (>=1) participant shards; the coordinator branches
; on (> (length participants) 1). Assert it's a subset of {0,1,2}.
(define ps (txn-participant-shards t 3))
(check "3-key txn participants are a subset of the 3 groups"
       #t (and (>= (length ps) 1) (<= (length ps) 3)
               (let ok ((p ps)) (or (null? p) (and (memv (car p) '(0 1 2)) (ok (cdr p)))))))

(done!)
