; test/sim-cluster-smoke.scm — Phase 0 keystone: the ported pure Raft engine +
; durable-KV substrate + actor/transport layer wired into a working in-process
; multi-voter sim-cluster, exercised end-to-end with the MVCC apply-fn (cw-u4a.6).
;
; ONE process, the in-memory sim transport (node-link!): 3 logical nodes a/b/c,
; one shard "0" replicated on all three (a 3-voter Raft group). Proves leader
; election, cross-"node" replication over node-send, the commit->ack bridge, and
; that the MVCC apply-fn wrote identical committed state (via the real
; mvcc-get-latest read path) on every node.
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
; create-if-missing. A per-RUN unique suffix gives every run a FRESH store, so a
; prior run's persisted applied-index can't make a replica skip re-applying
; (persist-applied! restores applied from RocksDB on restart — exactly what we want
; in prod, but it would skip the MVCC writes on a reused dir here, so the
; keys+revisions would not match this run's expectations).
; NOTE: use WALL-CLOCK microseconds, not (current-jiffy): jiffy is process-relative
; uptime, so separate `crabscheme run`s hit this line at ~the same tick -> the SAME
; tag -> dirs reused -> accumulated state (stale revisions) + slow store-open. That
; was the test-isolation half of bug cw-u4a.39's sim flakiness; current-second is
; microsecond wall-clock and is unique across runs.
(define run-tag (number->string (exact (round (* 1000000 (current-second))))))
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
; the MVCC apply-fn acks each PUT with its committed revision: ("PUT" . rev). The
; no-op become-leader barrier does NO MVCC write (no revision bump), so the first
; client PUT (city) commits at rev 1 and the second (n) at rev 2.
(check "write 1 acked at rev 1" (cons "PUT" 1) (table-lookup 'ws-test "r1"))
(check "write 2 acked at rev 2" (cons "PUT" 2) (table-lookup 'ws-test "r2"))
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

; ---- the MVCC apply-fn must have written the SAME committed state on EVERY node
;      (quorum + the leader). After both PUTs commit+apply, query each replica's
;      ctx through the `get` probe (the real mvcc-get-latest read path) and assert
;      that city="oslo"@rev1 and n="1"@rev2 are present, with the correct
;      create_rev/mod_rev/version, on all three voters. The no-op become-leader
;      barrier writes NO MVCC record (no revision bump), so city is rev 1 / n is
;      rev 2 — a distinct first-write per key, each version 1.
(section "MVCC state replicated on all nodes")

; ask replica `nd` for its MVCC view of key K via a per-node probe actor; returns
; (value-bytes create-rev mod-rev version) or #f.
(define (get-node nd k)
  (table-insert! 'ws-test "get-target" nd)
  (table-insert! 'ws-test "get-key" k)
  (table-insert! 'ws-test "get-done" #f)
  (spawn-source "
    (define (probe)
      (let ((pid (table-lookup 'ws-shard-pid
                   (string-append (table-lookup 'ws-test \"get-target\") \":0\"))))
        (send pid (list 'get (self) (string->utf8 (table-lookup 'ws-test \"get-key\"))))
        (table-insert! 'ws-test \"get-result\" (raw-receive))
        (table-insert! 'ws-test \"get-done\" #t)))" 'probe)
  (spin (lambda () (table-lookup 'ws-test "get-done")) (string-append "get " nd " " k))
  (table-lookup 'ws-test "get-result"))

; decode a (value-bytes create-rev mod-rev version) summary for assertion.
(define (summarize r)
  (and r (list (utf8->string (car r)) (cadr r) (caddr r) (cadddr r))))

; expected MVCC view: (value create-rev mod-rev version)
(define exp-city (list "oslo" 1 1 1))   ; city: first PUT at rev 1
(define exp-n    (list "1"    2 2 1))   ; n:    first PUT at rev 2

(for-each
 (lambda (nd)
   (let ((city (summarize (get-node nd "city")))
         (n    (summarize (get-node nd "n"))))
     (display "  ") (display nd) (display " city=") (write city)
     (display " n=") (write n) (newline)
     (check (string-append nd " city=oslo @rev1 v1") exp-city city)
     (check (string-append nd " n=1 @rev2 v1")       exp-n    n)))
 '("a" "b" "c"))

(done!)
