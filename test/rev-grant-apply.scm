; test/rev-grant-apply.scm — cw-kp0 phase 2: the rev-authority's REV-GRANT apply
; command, driven directly against a real RocksDB ctx (NO Raft), mirroring
; test/mvcc-apply.scm. Proves the authority hands out monotonic, contiguous,
; non-overlapping revision blocks and that granting is INDEPENDENT of current-rev.
; Run from repo root:  crabscheme run test/rev-grant-apply.scm
(include "test/harness.scm")
(include "src/store-ctx.scm")
(include "src/mvcc.scm")
(include "src/auth.scm")
(include "test/mvcc-util.scm")

(define run-tag (number->string (current-jiffy)))
(define CTX (make-ctx (store-open (string-append "/tmp/cws-rev-grant-" run-tag) #t) "default" #t))
(reset-ctx! CTX)

(define (b s) (string->utf8 s))
(define (grant n) (mvcc-apply CTX (map b (list "REV-GRANT" (number->string n)))))
(define (put . parts) (mvcc-apply CTX (map b parts)))
(define (put-at rev k v) (mvcc-apply CTX (map b (list "PUT-AT" (number->string rev) k v))))
(define (latest k) (mvcc-get-latest CTX (b k)))

; ---- monotonic, contiguous, non-overlapping blocks --------------------------
(check "grant of 3 returns block lo=1"      (cons "REV-GRANT" 1) (grant 3))
(check "global-rev advanced to 3"           3 (mvcc-global-rev CTX))
(check "grant of 2 returns lo=4 (contiguous, no overlap)" (cons "REV-GRANT" 4) (grant 2))
(check "global-rev advanced to 5"           5 (mvcc-global-rev CTX))
(check "grant of 1 returns lo=6"            (cons "REV-GRANT" 6) (grant 1))
(check "global-rev = 6"                     6 (mvcc-global-rev CTX))

; ---- granting does NOT bump current-rev (a grant reserves, it doesn't commit) -
(check "current-rev still 0 after grants"   0 (mvcc-current-rev CTX))

; ---- current-rev and global-rev are independent counters --------------------
(check "a PUT commits at current-rev 1"     (cons "PUT" 1) (put "PUT" "k" "v"))
(check "current-rev = 1 after the PUT"      1 (mvcc-current-rev CTX))
(check "global-rev untouched by the PUT"    6 (mvcc-global-rev CTX))
(check "next grant continues from 6 -> lo=7" (cons "REV-GRANT" 7) (grant 1))

; ---- persistence: global-rev is read straight from the META key every time ---
(check "global-rev persisted in META (re-read)" 7 (mvcc-global-rev CTX))

; ---- PUT-AT: apply a write at an EXPLICIT embedded revision (2b.4) -----------
; In global-rev mode the leader proposes PUT-AT <granted-rev>; all replicas apply
; at that rev deterministically and current-rev tracks the granted global sequence.
(check "PUT-AT 100 acks at the embedded rev"   (cons "PUT" 100) (put-at 100 "gk" "gv"))
(check "current-rev jumped TO the embedded rev" 100 (mvcc-current-rev CTX))
(let ((r (latest "gk")))
  (check "PUT-AT key value"                "gv" (utf8->string (kv-rec-value r)))
  (check "PUT-AT mod_revision = embedded"   100 (kv-rec-mod-rev r)))
(check "later PUT-AT 105 advances current-rev" (cons "PUT" 105) (put-at 105 "gk" "gv2"))
(check "current-rev = 105"                      105 (mvcc-current-rev CTX))

(done!)
