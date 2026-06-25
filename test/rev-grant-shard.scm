; test/rev-grant-shard.scm — cw-kp0 phase 2b.2: prove the rev-authority's grant
; path LIVE through the real shard actor + Raft. A grant request rides the EXISTING
; client-proposal path: a client proposes ("REV-GRANT" N) to the authority shard
; (shard "0", global-rev? = #t), which routes it through Raft, applies 2a's REV-GRANT
; command, and replies ("REV-GRANT" . lo) via the standard pending/ack drain — no
; new authority-side code. Single-voter group (always leader) so no election wait.
; Run from repo root:  crabscheme run test/rev-grant-shard.scm
(include "test/harness.scm")

(make-table 'ws-shard-pid "set")
(make-table 'ws-shard-role "set")
(make-table 'ws-shard-leader "set")
(make-table 'ws-shard-commit "set")
(make-table 'ws-shard-applied "set")
(make-table 'ws-test "set")

(node-make "a")
(define run-tag (number->string (exact (round (* 1000000 (current-second))))))
(define db (string-append "/tmp/cws-revgrant-" run-tag "-a-s0"))

; shard "0", voters '(a); rest = apply-shards election-ticks leader-region region-map
; ser-max-lag learners leader-node global-rev?  -> global-rev? = #t (rest[7]).
(spawn-source "(include \"src/server/shard-actor.scm\")" 'shard-main
              "0" '(a) 'a db #f 1 4 #f '() 0 '() #f #t)
(spawn-source "(include \"src/server/peer-poller.scm\")" 'peer-poller
              'a '("0") 150 '() 0)

(define (role) (table-lookup 'ws-shard-role "a:0"))
(define (spin pred who)
  (let loop ((i 0))
    (cond ((pred) #t) ((> i 400000000) (error (string-append "timeout: " who)))
          (else (loop (+ i 1))))))

(section "rev-authority bring-up")
(spin (lambda () (eq? (role) 'leader)) "leader")
(check "shard 0 elected leader (rev-authority)" 'leader (role))

(section "REV-GRANT hands out monotonic blocks via the real shard + Raft")
(define client-src "
  (define (b s) (string->utf8 s))
  (define (ask pid msg) (send pid msg) (raw-receive))
  (define (propose pid cmd) (ask pid (cons (self) cmd)))
  (define (client)
    (let ((o (table-lookup 'ws-shard-pid \"a:0\")))
      (table-insert! 'ws-test \"g1\" (propose o (list (b \"REV-GRANT\") (b \"3\"))))
      (table-insert! 'ws-test \"g2\" (propose o (list (b \"REV-GRANT\") (b \"2\"))))
      (table-insert! 'ws-test \"p1\" (propose o (list (b \"PUT\") (b \"k\") (b \"v\"))))
      (table-insert! 'ws-test \"done\" #t)))")
(spawn-source client-src 'client)
(spin (lambda () (table-lookup 'ws-test "done")) "grants + put acked")

(check "grant of 3 -> block lo=1"                        (cons "REV-GRANT" 1) (table-lookup 'ws-test "g1"))
(check "grant of 2 -> block lo=4 (monotonic, no overlap)" (cons "REV-GRANT" 4) (table-lookup 'ws-test "g2"))
(check "a PUT commits at current-rev 1 (independent of global-rev)" (cons "PUT" 1) (table-lookup 'ws-test "p1"))

(done!)
