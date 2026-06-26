; test/txn-split-by-shard.scm — cw-kp0 Phase 4: split an etcd Txn into per-participant
; sub-txns (its compares/success-ops/failure-ops filed under each key's shard). The
; coordinator PREPAREs each sub-txn at one global rev, then COMMITs all / ABORTs all.
; Mirrors txn-split-by-shard in src/server/grpc-kv.scm (keep in sync).
; Run from repo root:  crabscheme run test/txn-split-by-shard.scm
(include "test/harness.scm")
(include "src/txn.scm")

; --- mirror of src/server/grpc-kv.scm:txn-split-by-shard ---
(define (txn-split-by-shard t shard-of)
  (let ((parts (make-eqv-hashtable)))
    (define (add! key slot item)
      (let ((cur (or (hashtable-ref parts (shard-of key) #f) (vector '() '() '()))))
        (vector-set! cur slot (cons item (vector-ref cur slot)))
        (hashtable-set! parts (shard-of key) cur)))
    (for-each (lambda (c) (add! (cmp-key c)      0 c)) (txn-compares t))
    (for-each (lambda (o) (add! (vector-ref o 1) 1 o)) (txn-success  t))
    (for-each (lambda (o) (add! (vector-ref o 1) 2 o)) (txn-failure  t))
    (map (lambda (s)
           (let ((v (hashtable-ref parts s #f)))
             (cons s (make-txn (reverse (vector-ref v 0))
                               (reverse (vector-ref v 1))
                               (reverse (vector-ref v 2))))))
         (vector->list (hashtable-keys parts)))))

(define (b s) (string->utf8 s))
(define (cmp k) (make-compare 0 0 k (b "")))
(define (put k) (op-put k (b "v") 0))
; shard = last byte of key mod 2:  ka->1 kb->0 kc->1 kd->0
(define (shard-of k) (modulo (bytevector-u8-ref k (- (bytevector-length k) 1)) 2))
(define (ckeys t) (map (lambda (c) (utf8->string (cmp-key c))) (txn-compares t)))
(define (skeys t) (map (lambda (o) (utf8->string (vector-ref o 1))) (txn-success t)))
(define (fkeys t) (map (lambda (o) (utf8->string (vector-ref o 1))) (txn-failure t)))

(section "split a spanning Txn by participant shard, preserving per-shard op order")
; compares ka,kb ; success put ka,kc,kd ; failure put kb
(define t (make-txn (list (cmp (b "ka")) (cmp (b "kb")))
                    (list (put (b "ka")) (put (b "kc")) (put (b "kd")))
                    (list (put (b "kb")))))
(define split (txn-split-by-shard t shard-of))
(define (sub s) (cdr (assv s split)))
(check "two participant shards" 2 (length split))
(check "shard 1 compares = (ka)"        '("ka")     (ckeys (sub 1)))
(check "shard 1 success  = (ka kc)"     '("ka" "kc") (skeys (sub 1)))
(check "shard 1 failure  = ()"          '()         (fkeys (sub 1)))
(check "shard 0 compares = (kb)"        '("kb")     (ckeys (sub 0)))
(check "shard 0 success  = (kd)"        '("kd")     (skeys (sub 0)))
(check "shard 0 failure  = (kb)"        '("kb")     (fkeys (sub 0)))

(section "single-shard Txn -> one participant (coordinator takes the fast path)")
(define t1 (make-txn (list (cmp (b "kb"))) (list (put (b "kd"))) '()))  ; both shard 0
(check "one participant" 1 (length (txn-split-by-shard t1 shard-of)))

(done!)
