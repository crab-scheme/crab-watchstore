; test/store-smoke.scm — exercise the generic durable-KV substrate over a real
; RocksDB via the cs-store FFI.
;
; The store is opened at a fixed path under /tmp.  The test pre-deletes every
; key it will write at the start of each section so reruns are deterministic
; (no Scheme-level rm -rf needed).

(include "test/harness.scm")
(include "src/store-ctx.scm")

; ---- helpers ----

; Lexicographic bytevector < (no built-in bytevector<? in CrabScheme).
(define (bv<? a b)
  (let ((la (bytevector-length a))
        (lb (bytevector-length b)))
    (let loop ((i 0))
      (cond ((= i la) (< la lb))
            ((= i lb) #f)
            ((< (bytevector-u8-ref a i) (bytevector-u8-ref b i)) #t)
            ((> (bytevector-u8-ref a i) (bytevector-u8-ref b i)) #f)
            (else (loop (+ i 1)))))))

; Delete a list of keys from ctx, ignoring missing.
(define (del-keys! ctx keys)
  (for-each (lambda (k) (kv-del! ctx (->bv k))) keys))

; ---- open store (create-if-missing; path fixed, reuse across runs) ----

(define STORE-PATH "/tmp/cws-store-smoke")
(define CTX (make-ctx (store-open STORE-PATH #t) "default" #t))

; ---- §1 basic kv-put!/kv-get/kv-exists?/kv-del! ----

(section "kv put/get/exists/del")

; Pre-delete so a previous run's state doesn't pollute.
(del-keys! CTX '("k1" "k2" "k3"))

(kv-put! CTX (->bv "k1") (->bv "val1"))
(kv-put! CTX (->bv "k2") (->bv "val2"))
(kv-put! CTX (->bv "k3") (->bv "val3"))

(check "get k1"      (->bv "val1") (kv-get CTX (->bv "k1")))
(check "get k2"      (->bv "val2") (kv-get CTX (->bv "k2")))
(check "get k3"      (->bv "val3") (kv-get CTX (->bv "k3")))
(check "exists k1"   #t  (kv-exists? CTX (->bv "k1")))
(check "exists miss" #f  (kv-exists? CTX (->bv "no-such-key-xyz")))

(kv-del! CTX (->bv "k2"))
(check "get after del"    #f (kv-get    CTX (->bv "k2")))
(check "exists after del" #f (kv-exists? CTX (->bv "k2")))

; ---- §2 kv-scan prefix ----

(section "kv-scan prefix")

; Pre-delete the scan keys so the count is exact.
(del-keys! CTX '("pfx:a" "pfx:b" "pfx:c" "other:x"))

(kv-put! CTX (->bv "pfx:a") (->bv "va"))
(kv-put! CTX (->bv "pfx:b") (->bv "vb"))
(kv-put! CTX (->bv "pfx:c") (->bv "vc"))
(kv-put! CTX (->bv "other:x") (->bv "vx"))

(let ((results (kv-scan CTX (->bv "pfx:"))))
  (check "scan count"  3 (length results))
  (check "scan key 0"  (->bv "pfx:a") (car  (car results)))
  (check "scan val 0"  (->bv "va")    (cdr  (car results)))
  (check "scan key 1"  (->bv "pfx:b") (car  (cadr results)))
  (check "scan key 2"  (->bv "pfx:c") (car  (caddr results))))

(check "scan-count" 3 (kv-scan-count CTX (->bv "pfx:")))
(check "scan empty" '() (kv-scan CTX (->bv "zzz:")))

; ---- §3 group-commit flush ----

(section "group-commit ctx-flush!")

; dirty counter is positive from writes above.
(check "dirty before flush" #t (ctx-dirty? CTX))
(ctx-flush! CTX)
(check "dirty after flush"  #f (ctx-dirty? CTX))

; Values survive a flush.
(check "get k1 after flush" (->bv "val1") (kv-get CTX (->bv "k1")))
(check "get k3 after flush" (->bv "val3") (kv-get CTX (->bv "k3")))

; ---- §4 ctx-save-applied! / ctx-load-applied ----

(section "applied-index round-trip")

; Start from a known base by writing 0/0 explicitly.
(ctx-save-applied! CTX 0 0)
(check "load-applied 0/0" (cons 0 0) (ctx-load-applied CTX))

(ctx-save-applied! CTX 42 7)
(check "load-applied 42/7" (cons 42 7) (ctx-load-applied CTX))

(ctx-save-applied! CTX 100 3)
(check "load-applied 100/3" (cons 100 3) (ctx-load-applied CTX))

; ---- §5 byte-util round-trips ----

(section "u64->bytes / bytes->u64")

(check "u64 zero"    0           (bytes->u64 (u64->bytes 0) 0))
(check "u64 one"     1           (bytes->u64 (u64->bytes 1) 0))
(check "u64 max32"   4294967295  (bytes->u64 (u64->bytes 4294967295) 0))
(check "u64 big"     1000000007  (bytes->u64 (u64->bytes 1000000007) 0))

(section "s64->order-bytes / order-bytes->s64")

(check "s64 zero"  0    (order-bytes->s64 (s64->order-bytes 0) 0))
(check "s64 pos"   99   (order-bytes->s64 (s64->order-bytes 99) 0))
(check "s64 neg"  -1    (order-bytes->s64 (s64->order-bytes -1) 0))
(check "s64 min"  -99   (order-bytes->s64 (s64->order-bytes -99) 0))
; Order-preservation: -5 < 5 under unsigned byte compare.
(check "s64 order -5 < 5"
       #t
       (bv<? (s64->order-bytes -5) (s64->order-bytes 5)))
; -10 < -1 (bigger negative magnitude sorts lower).
(check "s64 order -10 < -1"
       #t
       (bv<? (s64->order-bytes -10) (s64->order-bytes -1)))

(section "subbv")

(let ((bv (string->utf8 "hello")))
  (check "subbv full"   (string->utf8 "hello") (subbv bv 0 5))
  (check "subbv prefix" (string->utf8 "hel")   (subbv bv 0 3))
  (check "subbv mid"    (string->utf8 "ell")   (subbv bv 1 4))
  (check "subbv empty"  (make-bytevector 0 0)  (subbv bv 2 2)))

(done!)
