; test/sim-cluster-smoke.scm — Phase 0 keystone: the ported pure Raft engine +
; durable-KV substrate + actor/transport layer wired into a working in-process
; multi-voter sim-cluster, exercised end-to-end with the STUB apply-fn.
;
; ONE process, the in-memory sim transport (node-link!): 3 logical nodes a/b/c,
; one shard "0" replicated on all three (a 3-voter Raft group). Proves leader
; election, cross-"node" replication over node-send, the commit->ack bridge, and
; that the stub apply-fn recorded the proposed commands (in order) on every node.
; This is the exact code that runs over real TCP; only the transport wiring
; differs (node-link! here vs node-listen/node-connect in node-cluster.scm).
;
; Adapted from crab-cache/test/sim-cluster-smoke.scm. The peer-pollers are real
; spawn-source actors (they ARE the tick clock + frame drainers); the test just
; spins on the shared ws-shard-* tables and drives proposals via a client actor's
; real PID reply path. No manual tick loop is needed — the spawned pollers pump
; ticks (tick-every idle iterations) and node-poll for each node.

(include "test/harness.scm")

(make-table 'ws-shard-pid "set")
(make-table 'ws-shard-role "set")
(make-table 'ws-shard-leader "set")
(make-table 'ws-shard-commit "set")
(make-table 'ws-shard-applied "set")
(make-table 'ws-test "set")

; ---- in-memory mesh: 3 logical nodes, fully linked ----
(for-each node-make (list "a" "b" "c"))
(node-link! "a" "b") (node-link! "a" "c") (node-link! "b" "c")

; ---- one shard "0" replica per node (voters = all three), relaxed mode ----
; Distinct RocksDB dir per node so the in-process replicas never collide; opened
; create-if-missing. A per-RUN unique suffix ((current-jiffy)) gives every run a
; FRESH store, so a prior run's persisted applied-index can't make a replica skip
; re-applying (persist-applied! restores applied from RocksDB on restart — exactly
; what we want in prod, but it would empty applied-cmds on a reused dir here).
(define run-tag (number->string (current-jiffy)))
(define (db-dir nd)
  (string-append "/tmp/cws-sim-" run-tag "-" (symbol->string nd) "-s0"))
(for-each
 (lambda (nd)
   (spawn-source "(include \"src/server/shard-actor.scm\")" 'shard-main
                 "0" '(a b c) nd (db-dir nd) #f))
 '(a b c))
; ---- one peer-poller per node (tick clock + sole inbound-frame drainer) ----
; 5-arg form: (node shard-keys tick-every dial-addrs target). In the sim the mesh
; is pre-linked, so there is nothing to dial and target=0 (heal! is a no-op).
(for-each
 (lambda (nd)
   (spawn-source "(include \"src/server/peer-poller.scm\")" 'peer-poller
                 nd '("0") 150 '() 0))
 '(a b c))

(define (role nd)    (table-lookup 'ws-shard-role (string-append nd ":0")))
(define (commit nd)  (let ((c (table-lookup 'ws-shard-commit  (string-append nd ":0")))) (if c c 0)))
(define (applied nd) (let ((a (table-lookup 'ws-shard-applied (string-append nd ":0")))) (if a a 0)))
(define (shard-pid nd) (table-lookup 'ws-shard-pid (string-append nd ":0")))

(define (spin pred who)
  (let loop ((i 0))
    (cond ((pred) #t)
          ((> i 400000000) (error (string-append "timeout: " who)))
          (else (loop (+ i 1))))))

(define (leader-node)
  (cond ((eq? (role "a") 'leader) "a")
        ((eq? (role "b") 'leader) "b")
        ((eq? (role "c") 'leader) "c")
        (else #f)))

(section "leader election")
(spin (lambda () (leader-node)) "leader election")
(define ldr (leader-node))
(display "  leader elected: ") (display ldr) (newline)
(check "a leader emerged" #t (and (member ldr '("a" "b" "c")) #t))

; ---- drive two write proposals at the leader via a client actor (real PID reply
;      path), then collect the per-node applied-cmds via the dump probe. The
;      client runs in its own actor so (self)/raw-receive give it a real mailbox.
(section "propose + commit + apply")
(table-insert! 'ws-test "ldr" ldr)
(define client-src "
  (define (b s) (string->utf8 s))
  (define (ask pid msg) (send pid msg) (raw-receive))
  (define (propose pid cmd) (ask pid (cons (self) cmd)))
  (define (dump pid)        (ask pid (list 'dump (self))))
  (define (client)
    (let ((o (table-lookup 'ws-shard-pid (string-append (table-lookup 'ws-test \"ldr\") \":0\"))))
      ; two client proposals — each routed through Raft; reply is 'applied once
      ; the entry commits + applies on a quorum.
      (table-insert! 'ws-test \"r1\" (propose o (list (b \"PUT\") (b \"city\") (b \"oslo\"))))
      (table-insert! 'ws-test \"r2\" (propose o (list (b \"PUT\") (b \"n\")    (b \"1\"))))
      ; a linearizable read probe (ReadIndex round) — exercises the read path
      (table-insert! 'ws-test \"rd\" (ask o (list 'read (self))))
      (table-insert! 'ws-test \"done\" #t)))")
(spawn-source client-src 'client)

(spin (lambda () (table-lookup 'ws-test "done")) "writes acked by quorum")
(check "write 1 acked"          'applied (table-lookup 'ws-test "r1"))
(check "write 2 acked"          'applied (table-lookup 'ws-test "r2"))
(check "read probe confirmed"   'read-ok (car (table-lookup 'ws-test "rd")))

; ---- every replica must converge: commit index catches up on all three nodes.
;      A 3-voter group with the leader's no-op barrier commits: barrier(1) +
;      PUT(2) + PUT(3) = applied/commit >= 3 once both writes replicate.
(section "replication convergence")
(spin (lambda () (and (>= (commit "a") 3) (>= (commit "b") 3) (>= (commit "c") 3)))
      "all replicas commit >= 3")
(display "  commit a/b/c  = ") (display (list (commit "a") (commit "b") (commit "c"))) (newline)
(spin (lambda () (and (>= (applied "a") 3) (>= (applied "b") 3) (>= (applied "c") 3)))
      "all replicas apply >= 3")
(display "  applied a/b/c = ") (display (list (applied "a") (applied "b") (applied "c"))) (newline)
(check "a commit advanced" #t (>= (commit "a") 3))
(check "b commit advanced" #t (>= (commit "b") 3))
(check "c commit advanced" #t (>= (commit "c") 3))
(check "a applied advanced" #t (>= (applied "a") 3))
(check "b applied advanced" #t (>= (applied "b") 3))
(check "c applied advanced" #t (>= (applied "c") 3))

; ---- the STUB apply-fn must have recorded the SAME two commands, in order, on
;      EVERY node (quorum + the leader). The no-op barrier is NOT recorded, so the
;      applied-cmds list is exactly the two client PUTs in propose order.
(section "stub apply-fn recorded commands on all nodes")
(define (dump-node nd)
  ; ask the replica actor for its applied-cmds via a per-node probe actor
  (table-insert! 'ws-test "dump-target" nd)
  (table-insert! 'ws-test "dump-done" #f)
  (spawn-source "
    (define (probe)
      (let ((pid (table-lookup 'ws-shard-pid
                   (string-append (table-lookup 'ws-test \"dump-target\") \":0\"))))
        (send pid (list 'dump (self)))
        (table-insert! 'ws-test \"dump-result\" (raw-receive))
        (table-insert! 'ws-test \"dump-done\" #t)))" 'probe)
  (spin (lambda () (table-lookup 'ws-test "dump-done")) (string-append "dump " nd))
  (table-lookup 'ws-test "dump-result"))

; decode an applied cmd (list of bytevectors) to a list of strings for assertion
(define (decode cmd) (map utf8->string cmd))
(define expected (list (list "PUT" "city" "oslo") (list "PUT" "n" "1")))

(for-each
 (lambda (nd)
   (let ((got (map decode (dump-node nd))))
     (display "  ") (display nd) (display " applied-cmds = ") (write got) (newline)
     (check (string-append nd " recorded both PUTs in order") expected got)))
 '("a" "b" "c"))

(done!)
