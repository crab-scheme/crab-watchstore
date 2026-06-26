; test/txn-2pc-apply.scm — cw-kp0 Phase 4 cross-shard 2PC, PARTICIPANT (shard) side.
; The new mvcc-apply verbs that a participant shard runs through Raft for a Txn spanning
; groups: TXN-PREPARE (check THIS shard's guards; if all hold, STAGE its ops at the txn's
; global rev — not yet visible), TXN-COMMIT (apply the staged ops at that rev, make them
; visible), TXN-ABORT (drop the stage). The coordinator (grpc-kv, next increment) drives
; PREPARE-all → COMMIT-all / ABORT-all so a multi-key Txn commits atomically at one rev.
; Run from repo root:  crabscheme run test/txn-2pc-apply.scm
(include "test/harness.scm")
(include "src/store-ctx.scm")
(include "src/mvcc.scm")          ; tail-includes src/txn.scm (make-txn/op-put/compare/CMP-*)
(include "test/mvcc-util.scm")

(define run-tag (number->string (current-jiffy)))
(define CTX (make-ctx (store-open (string-append "/tmp/cws-2pc-" run-tag) #t) "default" #t))
(reset-ctx! CTX)

(define (b s) (string->utf8 s))
(define (latest K) (mvcc-get-latest CTX (b K)))
(define (val-of r) (and r (utf8->string (kv-rec-value r))))
(define (mod-of r) (and r (kv-rec-mod-rev r)))
(define (prepare txnid rev sub)
  (mvcc-apply CTX (list (b "TXN-PREPARE") (int->bytes txnid) (int->bytes rev) (txn-encode sub))))
(define (commit txnid) (mvcc-apply CTX (list (b "TXN-COMMIT") (int->bytes txnid))))
(define (abort  txnid) (mvcc-apply CTX (list (b "TXN-ABORT")  (int->bytes txnid))))

(section "prepare (guards pass) stages but does NOT make visible; commit applies at the rev")
(define p1 (prepare 100 5 (make-txn '() (list (op-put (b "k1") (b "v1") 0)) '())))
(check "prepared #t"                         (list "TXN-PREPARE" 100 #t) p1)
(check "staged put NOT visible pre-commit"   #f  (val-of (latest "k1")))
(check "commit reply"                        (list "TXN-COMMIT" 100) (commit 100))
(check "committed value now visible"         "v1" (val-of (latest "k1")))
(check "committed at the txn's GLOBAL rev 5"  5   (mod-of (latest "k1")))

(section "prepare with a FAILING guard does not stage")
; guard: k1 mod_revision = 999 (actually 5) -> fails -> nothing staged
(define p2 (prepare 101 6 (make-txn (list (make-compare CMP-MOD RES-EQUAL (b "k1") (u64->bytes 999)))
                                    (list (op-put (b "k2") (b "v2") 0)) '())))
(check "prepared #f (guard failed)"          (list "TXN-PREPARE" 101 #f) p2)
(check "k2 not staged/visible"               #f  (val-of (latest "k2")))
(commit 101)   ; committing an unprepared txnid is a no-op
(check "k2 still not visible after no-op commit" #f (val-of (latest "k2")))

(section "prepare (guards pass) then ABORT discards the stage")
(check "prepared #t" (list "TXN-PREPARE" 102 #t)
       (prepare 102 7 (make-txn '() (list (op-put (b "k3") (b "v3") 0)) '())))
(abort 102)
(check "aborted put not visible"             #f (val-of (latest "k3")))
(commit 102)   ; no-op (stage already dropped)
(check "still not visible after commit of aborted txn" #f (val-of (latest "k3")))

(section "ISOLATION: a second prepare overlapping an in-flight stage conflicts (votes #f)")
; txn 200 prepares + stages key kx (held until commit/abort)
(check "txn 200 prepared" (list "TXN-PREPARE" 200 #t)
       (prepare 200 8 (make-txn '() (list (op-put (b "kx") (b "a") 0)) '())))
; txn 201 touches the SAME key kx while 200 is in-flight -> conflict -> votes #f
(check "txn 201 on the same key CONFLICTS (votes #f)" (list "TXN-PREPARE" 201 #f)
       (prepare 201 9 (make-txn '() (list (op-put (b "kx") (b "b") 0)) '())))
(check "kx still not visible (neither committed)" #f (val-of (latest "kx")))
; commit 200 -> visible; its lock released
(commit 200)
(check "after 200 commits, kx = a" "a" (val-of (latest "kx")))
; a disjoint key never conflicts
(check "txn 202 on a disjoint key prepares" (list "TXN-PREPARE" 202 #t)
       (prepare 202 10 (make-txn '() (list (op-put (b "ky") (b "c") 0)) '())))
(commit 202)
(check "disjoint commit visible" "c" (val-of (latest "ky")))

(section "ISOLATION: read-write conflict (my write vs their read; rr does NOT conflict)")
; txn 300 READS kr (guard, version=0 since absent -> passes) and writes ka; stages
(check "txn 300 prepared (reads kr, writes ka)" (list "TXN-PREPARE" 300 #t)
       (prepare 300 11 (make-txn (list (make-compare CMP-VERSION RES-EQUAL (b "kr") (u64->bytes 0)))
                                 (list (op-put (b "ka") (b "x") 0)) '())))
; txn 301 WRITES kr while 300 holds a read on kr -> rw conflict -> votes #f
(check "txn 301 writing a key 300 READS conflicts" (list "TXN-PREPARE" 301 #f)
       (prepare 301 12 (make-txn '() (list (op-put (b "kr") (b "y") 0)) '())))
(abort 300)
; two pure-READ txns on the same key do NOT conflict (rr is safe)
(check "txn 302 read-only on kr prepares" (list "TXN-PREPARE" 302 #t)
       (prepare 302 13 (make-txn (list (make-compare CMP-VERSION RES-EQUAL (b "kr") (u64->bytes 0))) '() '())))
(check "txn 303 read-only on the SAME kr also prepares (rr ok)" (list "TXN-PREPARE" 303 #t)
       (prepare 303 14 (make-txn (list (make-compare CMP-VERSION RES-EQUAL (b "kr") (u64->bytes 0))) '() '())))
(abort 302) (abort 303)

(done!)
