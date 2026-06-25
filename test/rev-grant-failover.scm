; test/rev-grant-failover.scm — cw-kp0 Phase 5: the rev-authority's granted-high is
; Raft-REPLICATED, so it survives leader loss. A 3-voter authority group (shard "0" on
; a,b,c): a grant proposed at the leader commits on a quorum and APPLIES on every voter
; (REV-GRANT advances the global-rev META key in each replica's committed state). We
; grant 10 revs, then query (global-high) on ALL THREE replicas — all return 10. So any
; voter can take over as leader with the counter intact: grants never reuse a revision
; across a failover. (Full kill-and-re-elect is the Jepsen membership/kill workload;
; this proves the underlying replication invariant.)
; Run from repo root:  crabscheme run test/rev-grant-failover.scm
(include "test/harness.scm")

(make-table 'ws-shard-pid "set")
(make-table 'ws-shard-role "set")
(make-table 'ws-shard-leader "set")
(make-table 'ws-shard-commit "set")
(make-table 'ws-shard-applied "set")
(make-table 'ws-test "set")

(for-each node-make (list "a" "b" "c"))
(node-link! "a" "b") (node-link! "a" "c") (node-link! "b" "c")

(define run-tag (number->string (exact (round (* 1000000 (current-second))))))
(define (db nd) (string-append "/tmp/cws-fo-" run-tag "-" (symbol->string nd) "-s0"))
; shard "0" replicated on all three (voters '(a b c)), global-rev? = #t.
(for-each
 (lambda (nd)
   (spawn-source "(include \"src/server/shard-actor.scm\")" 'shard-main
                 "0" '(a b c) nd (db nd) #f 1 4 #f '() 0 '() #f #t))
 '(a b c))
(for-each
 (lambda (nd)
   (spawn-source "(include \"src/server/peer-poller.scm\")" 'peer-poller nd '("0") 150 '() 0))
 '(a b c))

(define (role nd) (table-lookup 'ws-shard-role (string-append nd ":0")))
(define (spin pred who)
  (let loop ((i 0))
    (cond ((pred) #t) ((> i 400000000) (error (string-append "timeout: " who)))
          (else (loop (+ i 1))))))
(define (leader-node)
  (cond ((eq? (role "a") 'leader) "a") ((eq? (role "b") 'leader) "b")
        ((eq? (role "c") 'leader) "c") (else #f)))

(section "3-voter authority elects a leader")
(spin (lambda () (leader-node)) "leader election")
(define ldr (leader-node))
(display "  leader: ") (display ldr) (newline)
(check "a leader emerged" #t (and (member ldr '("a" "b" "c")) #t))
(table-insert! 'ws-test "ldr" ldr)

(section "grant 10 revs; granted-high replicated to every voter")
(define client-src "
  (define (b s) (string->utf8 s))
  (define (ask pid msg) (send pid msg) (raw-receive))
  (define (propose pid cmd) (ask pid (cons (self) cmd)))
  (define (client)
    (let ((o (table-lookup 'ws-shard-pid (string-append (table-lookup 'ws-test \"ldr\") \":0\"))))
      (propose o (list (b \"REV-GRANT\") (b \"10\")))   ; commits on quorum, applies on all
      ; let the AEs carry the committed entry to followers, then read each replica
      (define (gh nd) (ask (table-lookup 'ws-shard-pid (string-append nd \":0\")) (list 'global-high (self))))
      (let wait ((i 0))
        (let ((ga (gh \"a\")) (gb (gh \"b\")) (gc (gh \"c\")))
          (cond ((and (equal? ga (list 'global-high-ok 10))
                      (equal? gb (list 'global-high-ok 10))
                      (equal? gc (list 'global-high-ok 10)))
                 (table-insert! 'ws-test \"ga\" ga) (table-insert! 'ws-test \"gb\" gb)
                 (table-insert! 'ws-test \"gc\" gc))
                ((> i 2000) (table-insert! 'ws-test \"ga\" ga) (table-insert! 'ws-test \"gb\" gb)
                            (table-insert! 'ws-test \"gc\" gc))
                (else (sleep-ms 10) (wait (+ i 1))))))
      (table-insert! 'ws-test \"done\" #t)))")
(spawn-source client-src 'client)
(spin (lambda () (table-lookup 'ws-test "done")) "grant replicated")

(check "node a has granted-high 10" (list 'global-high-ok 10) (table-lookup 'ws-test "ga"))
(check "node b has granted-high 10" (list 'global-high-ok 10) (table-lookup 'ws-test "gb"))
(check "node c has granted-high 10" (list 'global-high-ok 10) (table-lookup 'ws-test "gc"))

(done!)
