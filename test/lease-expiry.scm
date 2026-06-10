; test/lease-expiry.scm — cw-u4a.17 "Lease expiry (replicated revoke)", ADR 0003.
;
; Validates the LIVE lease machinery the design (ADR 0003 §1/§2/§4/§6) specifies and
; the index POC (lease-poc.scm) builds toward:
;
;   • LEASE-GRANT apply — writes the replicated lease-meta entry (the lease object),
;     auto-assigns an id from the replicated lease-id-seq, and does NOT bump the rev.
;   • LEASE-REVOKE apply — tombstones EVERY key attached to the lease (KEY-CF
;     tombstone + REV-CF DELETE event + index removal) AND drops the lease-meta entry,
;     all at ONE revision, bumping current-rev exactly once IFF ≥1 key was deleted.
;   • put-to-dead-lease guard — PUT attaching to a never-granted / already-revoked
;     lease errors ErrLeaseNotFound and writes nothing.
;   • THE CRUX (cluster) — the LEADER's tick scans deadlines and proposes
;     ("LEASE-REVOKE" id) through Raft, so every replica deletes the SAME keys at the
;     SAME revision (the linearizable replicated revoke), and the lease-meta is gone
;     on all three.
;
; TWO harnesses (mirrors watch-stream.scm):
;   (A) SINGLE-CTX UNIT — drives mvcc-apply directly on one durable ctx; fully
;       deterministic.  Covers grant / attach / revoke / zero-key revoke /
;       put-to-dead-lease + the single-revision + REV-CF-DELETE-events contract.
;   (B) CLUSTER INTEGRATION — a 3-voter in-process cluster (the reliable post-cw-u4a.39
;       harness): grant a SHORT-ttl lease on the leader via the mailbox, attach 2 keys
;       through the leader, then drive the cluster with cooperative spin until the
;       leader's tick fires the expiry (a real wall-clock short-ttl wait, NOT a forced
;       hook — the faithful path), and assert all three replicas tombstoned both keys
;       at the SAME mod_rev with the meta gone everywhere.
;
; Per-run WALL-CLOCK dir tag (NOT current-jiffy — see cw-u4a.39 / sim-cluster-smoke),
; so back-to-back runs get FRESH stores with no manual cleanup.

(include "test/harness.scm")

; ===========================================================================
; (A) SINGLE-CTX UNIT — mvcc-apply lease grant / revoke / guard, deterministic.
; ===========================================================================
(include "src/store-ctx.scm")
(include "src/mvcc.scm")
(include "test/mvcc-util.scm")

(define (b s) (string->utf8 s))
(define (ap . parts) (mvcc-apply UCTX (map b parts)))    ; ("PUT"/"DEL"/"LEASE-*" ...)
(define U-TAG (number->string (exact (round (* 1000000 (current-second))))))
(define U-DB (string-append "/tmp/cws-lease-expiry-unit-" U-TAG))
(define UCTX (make-ctx (store-open U-DB #t) "default" #t))
(reset-ctx! UCTX)

; ---------------------------------------------------------------------------
(section "UNIT: LEASE-GRANT writes the meta entry, no rev bump, id assigned")
(reset-ctx! UCTX)
; Explicit-id grant: ("LEASE-GRANT" id ttl).  Returns (cons "LEASE-GRANT" id).
(check "grant id=100 ttl=60 -> ('LEASE-GRANT . 100)" (cons "LEASE-GRANT" 100)
       (ap "LEASE-GRANT" "100" "60"))
(check "grant did NOT bump current-rev (still 0)" 0 (mvcc-current-rev UCTX))
(check "lease 100 now exists" #t (mvcc-lease-exists? UCTX 100))
(check "lease 100 granted-ttl = 60" 60 (mvcc-lease-meta-get UCTX 100))
(check "lease 100 has no attached keys yet" '() (mvcc-lease-keys UCTX 100))
; Duplicate grant of a live id is rejected.
(check "re-grant of live id 100 -> err-lease-exists" (cons 'err-lease-exists 100)
       (ap "LEASE-GRANT" "100" "99"))
(check "ttl unchanged after rejected re-grant (still 60)" 60 (mvcc-lease-meta-get UCTX 100))

; ---------------------------------------------------------------------------
(section "UNIT: auto-id grant (id=0) assigns from the replicated lease-id-seq")
; id=0 ⇒ allocate next = lease-id-seq + 1, persist the counter (no rev bump).
(check "auto-id grant #1 -> id 1" (cons "LEASE-GRANT" 1) (ap "LEASE-GRANT" "0" "30"))
(check "auto-id grant #2 -> id 2 (monotone)" (cons "LEASE-GRANT" 2) (ap "LEASE-GRANT" "0" "30"))
(check "lease-id-seq persisted = 2" 2 (mvcc-lease-id-seq UCTX))
(check "auto-id grants did NOT bump current-rev (still 0)" 0 (mvcc-current-rev UCTX))
(check "auto lease 1 exists" #t (mvcc-lease-exists? UCTX 1))
(check "auto lease 2 exists" #t (mvcc-lease-exists? UCTX 2))

; ---------------------------------------------------------------------------
(section "UNIT: attach 3 keys to lease 100, then LEASE-REVOKE deletes all 3 at ONE rev")
(reset-ctx! UCTX)
(check "re-grant lease 100 ttl 60" (cons "LEASE-GRANT" 100) (ap "LEASE-GRANT" "100" "60"))
; attach k1,k2,k3 to lease 100 (each a PUT lease=100; each bumps the rev once).
(check "PUT k1 v1 lease 100 -> rev 1" (cons "PUT" 1) (ap "PUT" "k1" "v1" "100"))
(check "PUT k2 v2 lease 100 -> rev 2" (cons "PUT" 2) (ap "PUT" "k2" "v2" "100"))
(check "PUT k3 v3 lease 100 -> rev 3" (cons "PUT" 3) (ap "PUT" "k3" "v3" "100"))
(check "current-rev = 3 after 3 attaches" 3 (mvcc-current-rev UCTX))
(check "lease 100 index = {k1,k2,k3}"
       '("k1" "k2" "k3")
       (list-sort string<? (map utf8->string (mvcc-lease-keys UCTX 100))))
(check "k1 live before revoke" #t (and (mvcc-get-latest UCTX (b "k1")) #t))
(check "k2 live before revoke" #t (and (mvcc-get-latest UCTX (b "k2")) #t))
(check "k3 live before revoke" #t (and (mvcc-get-latest UCTX (b "k3")) #t))

; snapshot REV-CF DELETE-event count before revoke, so we can assert +3.
(define (delete-event-count)
  (length (filter (lambda (kv) (= (ev-kind (event-decode (cdr kv))) EV-DELETE))
                  (kv-scan UCTX (mvcc-byte NS-REV)))))
(define dels-before (delete-event-count))

; THE REVOKE: tombstones all 3 keys + drops the meta, bumping the rev exactly once.
(check "LEASE-REVOKE 100 -> rev 4, 3 keys deleted" (cons "LEASE-REVOKE" (cons 4 3))
       (ap "LEASE-REVOKE" "100"))
(check "current-rev bumped EXACTLY once (3 -> 4)" 4 (mvcc-current-rev UCTX))
(check "k1 now tombstoned (absent)" #f (mvcc-get-latest UCTX (b "k1")))
(check "k2 now tombstoned (absent)" #f (mvcc-get-latest UCTX (b "k2")))
(check "k3 now tombstoned (absent)" #f (mvcc-get-latest UCTX (b "k3")))
(check "lease 100 META is GONE (lease no longer exists)" #f (mvcc-lease-exists? UCTX 100))
(check "lease 100 index now empty" '() (mvcc-lease-keys UCTX 100))
(check "REV-CF gained exactly 3 DELETE events" (+ dels-before 3) (delete-event-count))
; the 3 tombstones share the single revoke revision (4) as their mod_rev (sub 0,1,2).
(let* ((rows (kv-scan UCTX (mvcc-byte NS-REV)))
       (dels (filter (lambda (kv) (= (ev-kind (event-decode (cdr kv))) EV-DELETE)) rows))
       (revs (map (lambda (kv) (ev-mod-rev (event-decode (cdr kv)))) dels)))
  (check "all 3 revoke DELETE events are at mod_rev 4 (one revision)"
         '(4 4 4) revs))

; ---------------------------------------------------------------------------
(section "UNIT: zero-key revoke removes the meta but does NOT bump the rev")
(reset-ctx! UCTX)
(check "grant lease 7 ttl 60" (cons "LEASE-GRANT" 7) (ap "LEASE-GRANT" "7" "60"))
(check "PUT base v lease 0 -> rev 1 (advance the rev, unattached)" (cons "PUT" 1)
       (ap "PUT" "base" "v"))
(check "current-rev = 1" 1 (mvcc-current-rev UCTX))
(check "lease 7 exists, no keys" #t (and (mvcc-lease-exists? UCTX 7)
                                         (null? (mvcc-lease-keys UCTX 7))))
; revoke a lease with ZERO attached keys: deletes nothing, so NO rev bump (§6 /
; cw-u4a.40), only the (empty) meta entry is removed.  Result carries prev-rev + 0.
(check "LEASE-REVOKE 7 (no keys) -> prev-rev 1, 0 deleted" (cons "LEASE-REVOKE" (cons 1 0))
       (ap "LEASE-REVOKE" "7"))
(check "current-rev UNCHANGED (still 1, zero-effect revoke)" 1 (mvcc-current-rev UCTX))
(check "lease 7 META gone" #f (mvcc-lease-exists? UCTX 7))

; ---------------------------------------------------------------------------
(section "UNIT: put-to-dead-lease -> ErrLeaseNotFound, writes nothing")
(reset-ctx! UCTX)
(check "PUT a v (no lease) -> rev 1" (cons "PUT" 1) (ap "PUT" "a" "v"))
(check "current-rev = 1" 1 (mvcc-current-rev UCTX))
; attach to lease 999 which was NEVER granted -> ErrLeaseNotFound, no write.
(check "PUT dead v lease 999 -> err-lease-not-found 999" (cons 'err-lease-not-found 999)
       (ap "PUT" "dead" "v" "999"))
(check "current-rev UNCHANGED (still 1, rejected PUT)" 1 (mvcc-current-rev UCTX))
(check "key 'dead' was NOT written" #f (mvcc-get-latest UCTX (b "dead")))
(check "no stray lease-999 index entry" '() (mvcc-lease-keys UCTX 999))
; an already-revoked lease is also dead: grant 50, attach, revoke, then re-attach fails.
(check "grant lease 50 ttl 60" (cons "LEASE-GRANT" 50) (ap "LEASE-GRANT" "50" "60"))
(check "PUT p v lease 50 -> rev 2" (cons "PUT" 2) (ap "PUT" "p" "v" "50"))
(check "LEASE-REVOKE 50 -> rev 3, 1 deleted" (cons "LEASE-REVOKE" (cons 3 1))
       (ap "LEASE-REVOKE" "50"))
(check "PUT q v lease 50 (now revoked) -> err-lease-not-found 50" (cons 'err-lease-not-found 50)
       (ap "PUT" "q" "v" "50"))
(check "key 'q' was NOT written" #f (mvcc-get-latest UCTX (b "q")))
; a lease=0 PUT is unaffected by the guard (the common no-lease path).
(check "PUT z v lease 0 still works -> rev 4" (cons "PUT" 4) (ap "PUT" "z" "v" "0"))

; ---------------------------------------------------------------------------
(section "UNIT: failover re-derivation source — mvcc-all-lease-ids + TTLs")
; On leadership change a NEW leader re-derives its (leader-local) deadlines from the
; REPLICATED lease-meta entries: deadline = now + granted_ttl, a fresh full window
; (ADR 0003 §2).  The data it scans is exactly (mvcc-all-lease-ids) + the per-id TTL.
; Prove that scan returns precisely the LIVE leases (and their TTLs), and that a
; revoked lease drops out of it (so the new leader won't re-window a dead lease).
(reset-ctx! UCTX)
(check "grant lease 11 ttl 30" (cons "LEASE-GRANT" 11) (ap "LEASE-GRANT" "11" "30"))
(check "grant lease 22 ttl 45" (cons "LEASE-GRANT" 22) (ap "LEASE-GRANT" "22" "45"))
(check "grant lease 33 ttl 60" (cons "LEASE-GRANT" 33) (ap "LEASE-GRANT" "33" "60"))
(check "all-lease-ids = {11,22,33} (the re-derivation set)"
       '(11 22 33) (list-sort < (mvcc-all-lease-ids UCTX)))
; the TTLs the new leader would window from
(check "lease 11 ttl 30" 30 (mvcc-lease-meta-get UCTX 11))
(check "lease 22 ttl 45" 45 (mvcc-lease-meta-get UCTX 22))
(check "lease 33 ttl 60" 60 (mvcc-lease-meta-get UCTX 33))
; attach a key to 22 then revoke it: 22 leaves the re-derivation set (its meta is gone).
(check "PUT k v lease 22 -> rev 1" (cons "PUT" 1) (ap "PUT" "kk" "v" "22"))
(check "LEASE-REVOKE 22 -> rev 2, 1 deleted" (cons "LEASE-REVOKE" (cons 2 1))
       (ap "LEASE-REVOKE" "22"))
(check "all-lease-ids = {11,33} after revoking 22 (dead lease excluded)"
       '(11 33) (list-sort < (mvcc-all-lease-ids UCTX)))
(check "revoked lease 22 has no TTL (meta gone)" #f (mvcc-lease-meta-get UCTX 22))

; ===========================================================================
; (B) CLUSTER INTEGRATION — 3-voter in-process cluster (mirrors sim-cluster-smoke).
;     THE CRUX: leader-driven short-ttl expiry -> replicated revoke on all voters.
; ===========================================================================
(make-table 'ws-shard-pid "set")
(make-table 'ws-shard-role "set")
(make-table 'ws-shard-leader "set")
(make-table 'ws-shard-commit "set")
(make-table 'ws-shard-applied "set")
(make-table 'ws-test "set")

(for-each node-make (list "a" "b" "c"))
(node-link! "a" "b") (node-link! "a" "c") (node-link! "b" "c")

(define CL-TAG (number->string (exact (round (* 1000000 (current-second))))))
(define (db-dir nd)
  (string-append "/tmp/cws-lease-expiry-cl-" CL-TAG "-" (symbol->string nd) "-s0"))
(for-each
 (lambda (nd)
   (spawn-source "(include \"src/server/shard-actor.scm\")" 'shard-main
                 "0" '(a b c) nd (db-dir nd) #f))
 '(a b c))
(for-each
 (lambda (nd)
   (spawn-source "(include \"src/server/peer-poller.scm\")" 'peer-poller
                 nd '("0") 150 '() 0))
 '(a b c))

(define (role nd)      (table-lookup 'ws-shard-role (string-append nd ":0")))
(define (applied nd)   (let ((a (table-lookup 'ws-shard-applied (string-append nd ":0")))) (if a a 0)))
(define (shard-pid nd) (table-lookup 'ws-shard-pid (string-append nd ":0")))

; COOPERATIVE spin (yield + periodic sleep-ms) — same rationale as watch-stream.scm:
; the cluster has 3 shard + 3 poller + a client actor on the green pool, so a tight
; main-thread busy-loop starves the leader's poller of its heartbeat window.  We yield
; every pass and sleep-ms periodically so the pool always drains.  The cap is generous
; because the expiry path waits a real ~1s wall-clock TTL.
(define (spin pred who)
  (let loop ((i 0))
    (cond ((pred) #t)
          ((> i 12000000) (error (string-append "timeout: " who)))
          (else
           (yield)
           (if (= 0 (modulo i 200)) (sleep-ms 1))
           (loop (+ i 1))))))
(define (leader-node)
  (cond ((eq? (role "a") 'leader) "a")
        ((eq? (role "b") 'leader) "b")
        ((eq? (role "c") 'leader) "c")
        (else #f)))

(section "CLUSTER: leader election")
(spin (lambda () (leader-node)) "leader election")
(define LDR (leader-node))
(display "  leader elected: ") (display LDR) (newline)
(check "a leader emerged" #t (and (member LDR '("a" "b" "c")) #t))
(table-insert! 'ws-test "ldr" LDR)

; ---- a CLIENT actor that drives lease-grant / PUT / lease-probe at the cluster via
;      its real PID reply path (sim-cluster-smoke idiom).  Job protocol via ws-test:
;        ('grant TTL ID)   -> send (lease-grant (self) TTL ID) to the leader, stash the
;                             apply reply (cons "LEASE-GRANT" assigned-id) under "g-res"
;        ('put K V LEASE)  -> propose a PUT attaching to LEASE, stash ack under "p-res"
;        ('probe NODE ID K1 K2) -> send (lease-probe (self) ID (K1 K2)) to NODE's shard,
;                             stash reply under "probe-res"
(define client-src "
  (define (b s) (string->utf8 s))
  (define (ask pid msg) (send pid msg) (raw-receive))
  (define (pid-of nd) (table-lookup 'ws-shard-pid (string-append nd \":0\")))
  (define (client)
    (let ((ldr (table-lookup 'ws-test \"ldr\")))
      (let loop ()
        (let ((job (table-lookup 'ws-test \"job\")))
          (cond
            ((eq? job 'done) #t)
            ((and (pair? job) (eq? (car job) 'grant))
             (let ((r (ask (pid-of ldr) (list 'lease-grant (self) (cadr job) (caddr job)))))
               (table-insert! 'ws-test \"g-res\" r)
               (table-insert! 'ws-test \"job\" #f) (loop)))
            ((and (pair? job) (eq? (car job) 'put))
             (let ((r (ask (pid-of ldr)
                           (cons (self) (list (b \"PUT\") (b (cadr job)) (b (caddr job))
                                              (b (number->string (cadddr job))))))))
               (table-insert! 'ws-test \"p-res\" r)
               (table-insert! 'ws-test \"job\" #f) (loop)))
            ((and (pair? job) (eq? (car job) 'probe))
             (let* ((nd (cadr job)) (id (caddr job)) (k1 (cadddr job)) (k2 (car (cddddr job)))
                    (r (ask (pid-of nd) (list 'lease-probe (self) id (list (b k1) (b k2))))))
               (table-insert! 'ws-test \"probe-res\" r)
               (table-insert! 'ws-test \"job\" #f) (loop)))
            (else (yield) (loop)))))))")
(spawn-source client-src 'client)

; submit a job and await the named result slot.
(define (job! result-key job)
  (table-insert! 'ws-test result-key #f)
  (table-insert! 'ws-test "job" job)
  (spin (lambda () (table-lookup 'ws-test result-key)) (symbol->string (car job)))
  (table-lookup 'ws-test result-key))

(define (grant! ttl id)      (job! "g-res" (list 'grant ttl id)))
(define (put-leased! k v lease) (job! "p-res" (list 'put k v lease)))
(define (probe! nd id k1 k2) (job! "probe-res" (list 'probe nd id k1 k2)))

; ---------------------------------------------------------------------------
(section "CLUSTER: grant a short-ttl lease + attach 2 keys through the leader")
; grant with TTL=1s and an explicit id so the test can reference it.  LEASE-GRANT
; does NOT bump the rev, so the two attaches below are revs 1 and 2.
(define LID 4242)
(check "grant lease 4242 ttl 1 -> assigned id 4242" (cons "LEASE-GRANT" LID)
       (grant! 1 LID))
; attach 2 keys to the lease (each a replicated PUT).
(check "PUT le-k1 v1 lease 4242 -> rev 1" (cons "PUT" 1) (put-leased! "le-k1" "v1" LID))
(check "PUT le-k2 v2 lease 4242 -> rev 2" (cons "PUT" 2) (put-leased! "le-k2" "v2" LID))
; the keys + meta should already be on all replicas (probe the leader to confirm live).
(let ((pr (probe! LDR LID "le-k1" "le-k2")))
  (check "leader: lease 4242 exists before expiry" #t (car pr))
  (check "leader: both keys LIVE before expiry (not tombstones)"
         '(#f #f) (map cadr (caddr pr))))   ; tombstone? = #f for both

; ---------------------------------------------------------------------------
(section "CLUSTER: the leader's tick expires the lease -> replicated LEASE-REVOKE")
; The leader-local deadline (now+1s) is seeded on a leader tick; ~1s later a tick
; finds it expired and proposes ("LEASE-REVOKE" 4242) through Raft.  The committed
; entry applies on EVERY replica, tombstoning both keys + dropping the meta at one
; revision.  Spin (cooperatively) until all 3 replicas show the keys GONE + meta gone.
; helper: pull a key's (key tombstone? mod-rev) triple out of a probe reply's key-list.
(define (assoc-key pr kstr)
  (let ((target (string->utf8 kstr)))
    (let loop ((ks (caddr pr)))
      (cond ((null? ks) (list target 'absent 0))
            ((equal? (car (car ks)) target) (car ks))
            (else (loop (cdr ks)))))))
(define (revoked-everywhere?)
  (let loop ((nds '("a" "b" "c")))
    (cond ((null? nds) #t)
          (else
           (let ((pr (probe! (car nds) LID "le-k1" "le-k2")))
             (if (and (not (car pr))                              ; meta gone
                      (eq? (cadr (assoc-key pr "le-k1")) #t)      ; k1 tombstoned
                      (eq? (cadr (assoc-key pr "le-k2")) #t))     ; k2 tombstoned
                 (loop (cdr nds))
                 #f))))))

(spin revoked-everywhere? "lease expiry revoked on all 3 replicas")
(display "  lease expired + revoked on all replicas") (newline)

; THE LINEARIZABLE-REVOKE PROOF: gather each replica's probe and assert that on all
; three the meta is gone, both keys are tombstoned, AND the tombstone mod_rev is
; IDENTICAL across replicas (one Raft entry, one revision, every node).
(define pa (probe! "a" LID "le-k1" "le-k2"))
(define pb (probe! "b" LID "le-k1" "le-k2"))
(define pc (probe! "c" LID "le-k1" "le-k2"))

(check "meta GONE on a/b/c" '(#f #f #f) (list (car pa) (car pb) (car pc)))

; k1 tombstone revision on each replica
(define k1-ra (caddr (assoc-key pa "le-k1")))
(define k1-rb (caddr (assoc-key pb "le-k1")))
(define k1-rc (caddr (assoc-key pc "le-k1")))
(define k2-ra (caddr (assoc-key pa "le-k2")))
(define k2-rb (caddr (assoc-key pb "le-k2")))
(define k2-rc (caddr (assoc-key pc "le-k2")))
(display "  revoke rev — a:") (display (list k1-ra k2-ra))
(display " b:") (display (list k1-rb k2-rb))
(display " c:") (display (list k1-rc k2-rc)) (newline)

(check "le-k1 tombstoned on a/b/c" '(#t #t #t)
       (list (cadr (assoc-key pa "le-k1")) (cadr (assoc-key pb "le-k1")) (cadr (assoc-key pc "le-k1"))))
(check "le-k2 tombstoned on a/b/c" '(#t #t #t)
       (list (cadr (assoc-key pa "le-k2")) (cadr (assoc-key pb "le-k2")) (cadr (assoc-key pc "le-k2"))))
; SAME revision on all 3 replicas — the replicated-revoke linearizability proof.
(check "le-k1 deleted at the SAME revision on all 3 replicas" #t
       (and (= k1-ra k1-rb) (= k1-rb k1-rc) (> k1-ra 0)))
(check "le-k2 deleted at the SAME revision on all 3 replicas" #t
       (and (= k2-ra k2-rb) (= k2-rb k2-rc) (> k2-ra 0)))
; both keys share the one revoke revision (sub-revisions of a single main).
(check "both keys deleted at the same single revoke revision (a)" #t (= k1-ra k2-ra))

; ---------------------------------------------------------------------------
(section "CLUSTER: a put attaching to the now-revoked lease is rejected everywhere")
; The lease is gone on every replica, so a PUT attaching to it errors ErrLeaseNotFound
; (the apply guard, evaluated identically on each replica) and writes nothing.
(check "PUT orphan v lease 4242 (revoked) -> err-lease-not-found"
       (cons 'err-lease-not-found LID)
       (put-leased! "orphan" "v" LID))
(let ((pr (probe! LDR LID "orphan" "le-k1")))
  (check "orphan key was NOT written on the leader" 'absent (cadr (assoc-key pr "orphan"))))

; stop the client cleanly.
(table-insert! 'ws-test "job" 'done)

(done!)
