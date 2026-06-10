; test/membership-harness.scm — SHARED live-cluster test machinery for crab-watchstore
; dynamic-membership tests, factored out of membership-cluster.scm (cw-u4a.29) so the
; cw-u4a.31 safety/stress capstone (membership-load.scm) reuses the SAME load-bearing
; harness without duplication: the cooperative `spin`, the dedicated-thread `bringup`/
; `spawn-joiner` sim mesh, the exactly-once `commit-write!` (+ `linread!` indeterminate
; seal), `await-key` (real mvcc key-presence, not an applied-index proxy), `member-op!`,
; the client-actor source strings, and the small set/leader helpers.  Pure machinery —
; no (check)s, no (section)s; the including test owns those (via test/harness.scm).
; Behaviour is IDENTICAL to the inline helpers it replaced — membership-cluster.scm must
; still pass 36/0 verbatim after this extraction (the cw-u4a.31 no-regression gate).

(make-table 'ws-shard-pid "set")
(make-table 'ws-shard-role "set")
(make-table 'ws-shard-leader "set")
(make-table 'ws-shard-commit "set")
(make-table 'ws-shard-applied "set")
(make-table 'ws-test "set")

; ---- per-run unique RocksDB dir (wall-clock microseconds; see sim-cluster-smoke) ----
(define run-tag (number->string (exact (round (* 1000000 (current-second))))))
(define (db-dir nd)
  (string-append "/tmp/cws-mem-" run-tag "-" (symbol->string nd) "-s0"))

; ---- shared-table accessors (node name = string key "nd:0") ----
(define (role nd)    (table-lookup 'ws-shard-role (string-append nd ":0")))
(define (applied nd) (let ((a (table-lookup 'ws-shard-applied (string-append nd ":0")))) (if a a 0)))
(define (shard-pid nd) (table-lookup 'ws-shard-pid (string-append nd ":0")))

; COOPERATIVE spin (yield + periodic sleep-ms) — same rationale as lease-expiry.scm. The
; shard + poller actors run on DEDICATED threads, but the client probe actors (put/get/member)
; live on the shared GREEN pool, and a tight main-thread busy-loop both starves that pool AND
; adds CPU pressure that destabilises leadership (spurious CheckQuorum stepdowns -> 'tryagain
; writes -> flaky #f reads). Yielding every pass + sleeping periodically lets the pool drain
; and keeps the leader's heartbeat window honoured, so leadership stays stable across the test.
(define (spin pred who)
  (let loop ((i 0))
    (cond ((pred) #t)
          ((> i 8000000) (error (string-append "timeout: " who)))
          (else
           (yield)
           (if (= 0 (modulo i 200)) (sleep-ms 1))
           (loop (+ i 1))))))

; ---- small set helpers (equal-set? lives in raft.scm, not included here) ----
(define (subset? a b) (cond ((null? a) #t) ((memv (car a) b) (subset? (cdr a) b)) (else #f)))
(define (set= a b)    (and (subset? a b) (subset? b a)))
(define (in? x lst)   (and (memv x lst) #t))
(define (remove-str nm lst)
  (let loop ((l lst) (acc '()))
    (cond ((null? l) (reverse acc))
          ((string=? (car l) nm) (loop (cdr l) acc))
          (else (loop (cdr l) (cons (car l) acc))))))

(define (leader-among names)              ; names: list of node-name strings
  (let loop ((ns names))
    (cond ((null? ns) #f)
          ((eq? (role (car ns)) 'leader) (car ns))
          (else (loop (cdr ns))))))

; resolve the CURRENT leader among `cands`, waiting cooperatively until one exists. Used
; before every write / member op so a leadership shift (election, post-membership settle)
; never targets a stale node.
(define (cur-leader cands)
  (spin (lambda () (leader-among cands)) "await a leader")
  (leader-among cands))

; ---- cluster bring-up: N voters, fully linked sim mesh, shards (relaxed) + pollers ----
(define (link-all names)                  ; one sim transport per pair (node-link! is symmetric)
  (let outer ((ns names))
    (if (pair? ns)
        (begin
          (for-each (lambda (m) (node-link! (symbol->string (car ns)) (symbol->string m))) (cdr ns))
          (outer (cdr ns))))))

; Shards + pollers run on DEDICATED threads (as node-cluster.scm does in production): the
; poller is the Raft tick-clock AND sole inbound-frame drainer (green-threads INV-2/INV-3),
; so on the shared cooperative pool it would be starved by this test's main-thread busy-spin
; + the many concurrent nodes (3 sections worth), delaying AER round-trips past the tight
; CheckQuorum window and spuriously stepping the leader down.  Dedicated threads are OS-
; preempted, so heartbeats/AERs flow on time — the same protocol-timing reason node-cluster
; spawns them dedicated.  (Client probe actors below stay on the green pool — they're one-shot.)
(define (bringup names)                    ; names: list of node-name symbols (all voters)
  (for-each (lambda (n) (node-make (symbol->string n))) names)
  (link-all names)
  (for-each (lambda (n)
              (spawn-source-dedicated "(include \"src/server/shard-actor.scm\")" 'shard-main
                            "0" names n (db-dir n) #f))
            names)
  ; sim poller: pre-linked mesh, so nothing to dial (dial-addrs '(), target 0; heal! no-op)
  (for-each (lambda (n)
              (spawn-source-dedicated "(include \"src/server/peer-poller.scm\")" 'peer-poller
                            n '("0") 150 '() 0))
            names))

; bring up ONE joining node as a NON-VOTER (initial voters = the existing set), linked
; live into the running mesh — the actor-layer analogue of node-cluster.scm's --join.
(define (spawn-joiner jn existing)         ; jn: symbol; existing: list of existing voter symbols
  (node-make (symbol->string jn))
  (for-each (lambda (m) (node-link! (symbol->string jn) (symbol->string m))) existing)
  (spawn-source-dedicated "(include \"src/server/shard-actor.scm\")" 'shard-main
                "0" existing jn (db-dir jn) #f)
  (spawn-source-dedicated "(include \"src/server/peer-poller.scm\")" 'peer-poller
                jn '("0") 150 '() 0))

; ============================================================
; client-actor sources (real (self)/raw-receive mailbox; params + results via ws-test)
; ============================================================
(define put-client "
  (define (put)
    (let ((pid (table-lookup 'ws-shard-pid
                 (string-append (table-lookup 'ws-test \"put-target\") \":0\"))))
      (send pid (cons (self) (list (string->utf8 \"PUT\")
                                   (string->utf8 (table-lookup 'ws-test \"put-key\"))
                                   (string->utf8 (table-lookup 'ws-test \"put-val\")))))
      (table-insert! 'ws-test \"put-result\" (raw-receive))
      (table-insert! 'ws-test \"put-done\" #t)))")

(define get-client "
  (define (probe)
    (let ((pid (table-lookup 'ws-shard-pid
                 (string-append (table-lookup 'ws-test \"get-target\") \":0\"))))
      (send pid (list 'get (self) (string->utf8 (table-lookup 'ws-test \"get-key\"))))
      (table-insert! 'ws-test \"get-result\" (raw-receive))
      (table-insert! 'ws-test \"get-done\" #t)))")

; one member-* op, async-acked: the reply arrives only after the change commits.
(define mop-client "
  (define (mop)
    (let* ((tgt  (table-lookup 'ws-test \"mop-target\"))
           (pid  (table-lookup 'ws-shard-pid (string-append tgt \":0\")))
           (kind (table-lookup 'ws-test \"mop-kind\"))
           (nn   (table-lookup 'ws-test \"mop-node\"))
           (lrn  (table-lookup 'ws-test \"mop-learner\"))
           (msg  (cond ((eq? kind 'add)     (list 'member-add (self) nn lrn))
                       ((eq? kind 'remove)  (list 'member-remove (self) nn))
                       ((eq? kind 'promote) (list 'member-promote (self) nn))
                       (else                (list 'member-list (self))))))
      (send pid msg)
      (table-insert! 'ws-test \"mop-result\" (raw-receive))
      (table-insert! 'ws-test \"mop-done\" #t)))")

; two member-adds back-to-back, then collect both replies — exercises the in-flight
; refusal: #1 is proposed (pending), #2 is refused synchronously ('member-pending),
; #1's 'member-ok arrives later when it commits.
(define inflight-client "
  (define (inflight)
    (let ((pid (table-lookup 'ws-shard-pid
                 (string-append (table-lookup 'ws-test \"if-target\") \":0\"))))
      (send pid (list 'member-add (self) (table-lookup 'ws-test \"if-n1\") #f))
      (send pid (list 'member-add (self) (table-lookup 'ws-test \"if-n2\") #f))
      (let* ((r1 (raw-receive)) (r2 (raw-receive)))
        (table-insert! 'ws-test \"if-r1\" r1)
        (table-insert! 'ws-test \"if-r2\" r2)
        (table-insert! 'ws-test \"if-done\" #t))))")

; ---- Scheme-side drivers (set params, spawn the client, spin for its result) ----
(define (put! target k v)
  (table-insert! 'ws-test "put-done" #f)
  (table-insert! 'ws-test "put-target" target)
  (table-insert! 'ws-test "put-key" k)
  (table-insert! 'ws-test "put-val" v)
  (spawn-source put-client 'put)
  (spin (lambda () (table-lookup 'ws-test "put-done")) (string-append "put " k))
  (table-lookup 'ws-test "put-result"))

(define (get-node nd k)
  (table-insert! 'ws-test "get-done" #f)
  (table-insert! 'ws-test "get-target" nd)
  (table-insert! 'ws-test "get-key" k)
  (spawn-source get-client 'probe)
  (spin (lambda () (table-lookup 'ws-test "get-done")) (string-append "get " nd " " k))
  (table-lookup 'ws-test "get-result"))
(define (summarize r)                      ; (value-bytes create mod ver) -> (value create mod ver)
  (and r (list (utf8->string (car r)) (cadr r) (caddr r) (cadddr r))))

; linearizable read probe (ReadIndex) at the leader — used to SEAL the fate of an
; 'indeterminate write. A confirmed (read-ok . idx) proves the (possibly new) leader has
; committed an entry in its OWN term (its no-op become-leader barrier), so any
; replicated-but-uncommitted entry from the deposed leader is now decided in this leader's
; committed state — a point read of the key is then authoritative. Retries on 'tryagain
; (the round can't complete across a stepdown). Cooperative.
(define read-client "
  (define (rd)
    (let ((pid (table-lookup 'ws-shard-pid
                 (string-append (table-lookup 'ws-test \"rd-target\") \":0\"))))
      (send pid (list 'read (self)))
      (table-insert! 'ws-test \"rd-result\" (raw-receive))
      (table-insert! 'ws-test \"rd-done\" #t)))")
(define (linread! ldr)
  (let retry ((n 0))
    (table-insert! 'ws-test "rd-done" #f)
    (table-insert! 'ws-test "rd-target" ldr)
    (spawn-source read-client 'rd)
    (spin (lambda () (table-lookup 'ws-test "rd-done")) "linread confirm")
    (let ((r (table-lookup 'ws-test "rd-result")))
      (cond ((and (pair? r) (eq? (car r) 'read-ok)) r)
            ((> n 200000) (error "linread never confirmed"))
            (else (yield) (sleep-ms 1) (retry (+ n 1)))))))

; Commit a write at the CURRENT leader among `cands`, re-resolving as leadership shifts while
; the cluster settles (election, post-membership-change). Guarantees the write commits EXACTLY
; ONCE — essential because a PUT is non-idempotent (each apply bumps the global revision, so a
; double-apply would shift every later rev the test asserts). Replies:
;   ("PUT" . rev) -> committed; return it (rev is stable across re-elections — a new leader
;                    inserts only a no-op barrier, which bumps no revision).
;   'tryagain / no leader -> not the leader; re-resolve + retry (safe, nothing was proposed).
;   'indeterminate -> the leader stepped down mid-flight; the entry MAY have committed. DON'T
;                    blindly retry. Wait for a stable leader, seal its fate with a confirmed
;                    ReadIndex, then point-read the key: present => it committed exactly once
;                    (done); absent => it was discarded (now safe to retry). Cooperative.
(define (commit-write! cands k v)
  (let retry ((n 0))
    (let* ((ldr (leader-among cands))
           (r   (if ldr (put! ldr k v) 'tryagain)))
      (cond ((and (pair? r) (equal? (car r) "PUT")) r)
            ((> n 200000) (error (string-append "write never committed: " k)))
            ((or (not ldr) (eq? r 'tryagain)) (yield) (sleep-ms 1) (retry (+ n 1)))
            ((eq? r 'indeterminate)
             (let ((l2 (cur-leader cands)))
               (linread! l2)                          ; seal the in-flight entry's fate
               (let ((rec (get-node l2 k)))
                 (if rec (cons "PUT" (caddr rec))     ; present => committed once (mod-rev)
                     (begin (yield) (sleep-ms 1) (retry (+ n 1)))))))  ; absent => retry
            (else (error "unexpected write reply" k r))))))

; Wait until node `nd` actually shows key `k` (replicated + applied on THAT node), then
; return its decoded summary. Spins on the REAL key presence via the mvcc read path — not an
; applied-index proxy — so it can never report success before the value is truly there (the
; index-proxy false-positive was the source of the harness flakiness). Cooperative.
(define (await-key nd k who)
  (spin (lambda () (get-node nd k)) who)
  (summarize (get-node nd k)))

(define (member-op! target kind nn lrn)
  (table-insert! 'ws-test "mop-done" #f)
  (table-insert! 'ws-test "mop-target" target)
  (table-insert! 'ws-test "mop-kind" kind)
  (table-insert! 'ws-test "mop-node" nn)
  (table-insert! 'ws-test "mop-learner" lrn)
  (spawn-source mop-client 'mop)
  (spin (lambda () (table-lookup 'ws-test "mop-done"))
        (string-append "member-op " (symbol->string kind)))
  (table-lookup 'ws-test "mop-result"))
(define (member-list-of nd) (member-op! nd 'list #f #f))   ; -> (member-list voters learners)
