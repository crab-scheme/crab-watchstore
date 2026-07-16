; test/qp-rev-grant-shard.scm — cw-6cq step 1: prove the rev-authority's grant
; path LIVE through the real QUEPAXA shard actor. Clone of rev-grant-shard.scm
; (raft) swapping shard-actor.scm/shard-main for quepaxa-shard.scm/qp-shard-main
; — same client-proposal path, same REV-GRANT/PUT-GLOBAL wire contract.
; Run from repo root:  crabscheme run test/qp-rev-grant-shard.scm
(include "test/harness.scm")

(make-table 'ws-shard-pid "set")
(make-table 'ws-shard-role "set")
(make-table 'ws-shard-leader "set")
(make-table 'ws-shard-commit "set")
(make-table 'ws-shard-applied "set")
(make-table 'ws-test "set")

(node-make "a")
(define run-tag (number->string (exact (round (* 1000000 (current-second))))))
(define db (string-append "/tmp/cws-qp-revgrant-" run-tag "-a-s0"))

; shard "0", voters '(a); rest tail matches shard-actor's positionally
; (slot 2 = hedge ticks here, not election-ticks; slot 7 = global-rev? = #t).
(spawn-source "(include \"src/server/quepaxa-shard.scm\")" 'qp-shard-main
              "0" '(a) 'a db #f 1 4 #f '() 0 '() #f #t)
(spawn-source "(include \"src/server/peer-poller.scm\")" 'peer-poller
              'a '("0") 150 '() 0)

(define (role) (table-lookup 'ws-shard-role "a:0"))
(define (spin pred who)
  (let loop ((i 0))
    (cond ((pred) #t) ((> i 400000000) (error (string-append "timeout: " who)))
          (else (loop (+ i 1))))))

(section "rev-authority bring-up (quepaxa, single-voter coordinator)")
(spin (lambda () (eq? (role) 'leader)) "coordinator")
(check "shard 0 coordinator up (rev-authority)" 'leader (role))

(section "REV-GRANT hands out monotonic blocks via the real shard + QuePaxa")
(define client-src "
  (define (b s) (string->utf8 s))
  (define (ask pid msg) (send pid msg) (raw-receive))
  (define (propose pid cmd) (ask pid (cons (self) cmd)))
  (define (client)
    (let ((o (table-lookup 'ws-shard-pid \"a:0\")))
      (table-insert! 'ws-test \"g1\" (propose o (list (b \"REV-GRANT\") (b \"3\"))))
      (table-insert! 'ws-test \"g2\" (propose o (list (b \"REV-GRANT\") (b \"2\"))))
      (table-insert! 'ws-test \"gh\" (ask o (list 'global-high (self))))
      (table-insert! 'ws-test \"p1\" (propose o (list (b \"PUT\") (b \"k\") (b \"v\"))))
      (table-insert! 'ws-test \"done\" #t)))")
(spawn-source client-src 'client)
(spin (lambda () (table-lookup 'ws-test "done")) "grants + put acked")

(check "grant of 3 -> block lo=1"                        (cons "REV-GRANT" 1) (table-lookup 'ws-test "g1"))
(check "grant of 2 -> block lo=4 (monotonic, no overlap)" (cons "REV-GRANT" 4) (table-lookup 'ws-test "g2"))
; the authority's own PUT draws the NEXT global rev (PUT-GLOBAL): after granting
; 5 (3+2), the next global rev is 6.
(check "a PUT commits at the next GLOBAL rev 6 (authority shares the global rev space)"
       (cons "PUT" 6) (table-lookup 'ws-test "p1"))
(check "global-high query returns granted high = 5 (after grant 3 + grant 2)"
       (list 'global-high-ok 5) (table-lookup 'ws-test "gh"))

(done!)
