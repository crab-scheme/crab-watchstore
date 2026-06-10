; test/lease-keepalive.scm — cw-u4a.18 "Lease keepalive bidi stream + tests", ADR 0003.
;
; Validates the keepalive/TTL/LeaseLeases machinery (ADR 0003 §3/§5) and the
; leader-local deadline reset contract:
;
;   • keepalive PREVENTS expiry — repeated (lease-keepalive) resets the leader-local
;     deadline; stopping keepalive lets the lease expire normally.
;   • LeaseTimeToLive — returns granted-ttl + remaining (> 0 while live, -1 dead).
;   • LeaseLeases — lists all live lease ids; shrinks when a lease is revoked.
;   • Non-leader redirect — keepalive / lease-ttl / lease-leases to a follower
;     returns ('lease-not-leader . LEADER-SYMBOL).
;
; TWO harnesses:
;   (A) SINGLE-CTX UNIT — drives mvcc helpers directly; deterministic.  Covers
;       mvcc-lease-meta-get / mvcc-all-lease-ids / mvcc-lease-keys which the new
;       shard-actor handlers call.
;   (B) CLUSTER INTEGRATION — 3-voter in-process cluster (mirrors lease-expiry.scm).
;       THE CRUX: keepalive PREVENTS expiry (the key correctness proof):
;         1. Grant a 2s-ttl lease, attach a key.
;         2. Send keepalive at ~1s; assert the key STILL EXISTS at ~2.2s total.
;         3. STOP keepalives; assert the key IS revoked ~2s+slack later.
;       Also covers: LeaseTimeToLive, LeaseLeases, non-leader redirect.
;
; Per-run WALL-CLOCK dir tag (current-second, not current-jiffy) so back-to-back
; runs get FRESH stores with no manual cleanup.

(include "test/harness.scm")

; ===========================================================================
; (A) SINGLE-CTX UNIT — exercise the mvcc helpers the handlers call.
; ===========================================================================
(include "src/store-ctx.scm")
(include "src/mvcc.scm")
(include "test/mvcc-util.scm")

(define (b s) (string->utf8 s))
(define (ap . parts) (mvcc-apply UCTX (map b parts)))
(define U-TAG (number->string (exact (round (* 1000000 (current-second))))))
(define U-DB (string-append "/tmp/cws-lease-keepalive-unit-" U-TAG))
(define UCTX (make-ctx (store-open U-DB #t) "default" #t))
(reset-ctx! UCTX)

; ---------------------------------------------------------------------------
(section "UNIT: mvcc-lease-meta-get / mvcc-lease-exists? (keepalive/ttl reads)")
(reset-ctx! UCTX)
(check "absent lease has no meta" #f (mvcc-lease-meta-get UCTX 1))
(check "absent lease does not exist" #f (mvcc-lease-exists? UCTX 1))
(ap "LEASE-GRANT" "1" "30")
(check "granted ttl = 30" 30 (mvcc-lease-meta-get UCTX 1))
(check "lease 1 exists" #t (mvcc-lease-exists? UCTX 1))
(ap "LEASE-REVOKE" "1")
(check "revoked lease has no meta" #f (mvcc-lease-meta-get UCTX 1))
(check "revoked lease does not exist" #f (mvcc-lease-exists? UCTX 1))

; ---------------------------------------------------------------------------
(section "UNIT: mvcc-all-lease-ids (LeaseLeases source)")
(reset-ctx! UCTX)
(check "empty store -> no lease ids" '() (mvcc-all-lease-ids UCTX))
(ap "LEASE-GRANT" "10" "30")
(ap "LEASE-GRANT" "20" "60")
(ap "LEASE-GRANT" "30" "90")
(check "three grants -> ids 10, 20, 30"
       '(10 20 30)
       (list-sort < (mvcc-all-lease-ids UCTX)))
(ap "PUT" "k" "v" "20")
(ap "LEASE-REVOKE" "20")
(check "after revoking 20 -> ids 10, 30"
       '(10 30)
       (list-sort < (mvcc-all-lease-ids UCTX)))
(check "revoked 20 absent from list" #f (and (member 20 (mvcc-all-lease-ids UCTX)) #t))

; ---------------------------------------------------------------------------
(section "UNIT: mvcc-lease-keys (LeaseTimeToLive with-keys source)")
(reset-ctx! UCTX)
(ap "LEASE-GRANT" "5" "60")
(check "no keys attached yet" '() (mvcc-lease-keys UCTX 5))
(ap "PUT" "foo" "v1" "5")
(ap "PUT" "bar" "v2" "5")
(check "two keys attached"
       '("bar" "foo")
       (list-sort string<? (map utf8->string (mvcc-lease-keys UCTX 5))))
(ap "PUT" "bar" "v3" "0")
(check "after detach via lease=0, only foo remains"
       '("foo")
       (map utf8->string (mvcc-lease-keys UCTX 5)))

; ===========================================================================
; (B) CLUSTER INTEGRATION — 3-voter in-process cluster.
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
  (string-append "/tmp/cws-lease-keepalive-cl-" CL-TAG "-" (symbol->string nd) "-s0"))
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
(define (shard-pid nd) (table-lookup 'ws-shard-pid (string-append nd ":0")))

; COOPERATIVE spin (yield + periodic sleep-ms) — same rationale as lease-expiry.scm.
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
(define (follower-nodes ldr)
  (filter (lambda (nd) (not (string=? nd ldr))) '("a" "b" "c")))

(section "CLUSTER: leader election")
(spin (lambda () (leader-node)) "leader election")
(define LDR (leader-node))
(display "  leader elected: ") (display LDR) (newline)
(check "a leader emerged" #t (and (member LDR '("a" "b" "c")) #t))
(table-insert! 'ws-test "ldr" LDR)

; ---- CLIENT ACTOR ----
; Job protocol via ws-test "job" key; results stashed in named result keys.
; Supported job shapes:
;   ('grant TTL ID)                  -> lease-grant;             result in "g-res"
;   ('put K V LEASE)                 -> PUT;                     result in "p-res"
;   ('keepalive ID)                  -> lease-keepalive/leader;  result in "ka-res"
;   ('keepalive-foll ND ID)          -> lease-keepalive/ND;      result in "ka-foll-res"
;   ('ttl ID WITH-KEYS?)             -> lease-ttl/leader;        result in "ttl-res"
;   ('ttl-foll ND ID WITH-KEYS?)     -> lease-ttl/ND;            result in "ttl-foll-res"
;   ('leases)                        -> lease-leases/leader;     result in "ll-res"
;   ('leases-foll ND)                -> lease-leases/ND;         result in "ll-foll-res"
;   ('revoke ID)                     -> lease-revoke/leader;     result in "rv-res"
;   ('probe ND ID KEYS)              -> lease-probe/ND;          result in "probe-res"
(define client-src
"  (define (b s) (string->utf8 s))
  (define (ask pid msg) (send pid msg) (raw-receive))
  (define (pid-of nd) (table-lookup 'ws-shard-pid (string-append nd \":0\")))
  (define (client)
    (let loop ()
      (let ((ldr (table-lookup 'ws-test \"ldr\"))  ; re-read leader each iteration
            (job (table-lookup 'ws-test \"job\")))
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
            ((and (pair? job) (eq? (car job) 'keepalive))
             (let ((r (ask (pid-of ldr) (list 'lease-keepalive (self) (cadr job)))))
               (table-insert! 'ws-test \"ka-res\" r)
               (table-insert! 'ws-test \"job\" #f) (loop)))
            ((and (pair? job) (eq? (car job) 'keepalive-foll))
             (let ((r (ask (pid-of (cadr job)) (list 'lease-keepalive (self) (caddr job)))))
               (table-insert! 'ws-test \"ka-foll-res\" r)
               (table-insert! 'ws-test \"job\" #f) (loop)))
            ((and (pair? job) (eq? (car job) 'ttl))
             (let ((r (ask (pid-of ldr) (list 'lease-ttl (self) (cadr job) (caddr job)))))
               (table-insert! 'ws-test \"ttl-res\" r)
               (table-insert! 'ws-test \"job\" #f) (loop)))
            ((and (pair? job) (eq? (car job) 'ttl-foll))
             (let ((r (ask (pid-of (cadr job)) (list 'lease-ttl (self) (caddr job) (cadddr job)))))
               (table-insert! 'ws-test \"ttl-foll-res\" r)
               (table-insert! 'ws-test \"job\" #f) (loop)))
            ((and (pair? job) (eq? (car job) 'leases))
             (let ((r (ask (pid-of ldr) (list 'lease-leases (self)))))
               (table-insert! 'ws-test \"ll-res\" r)
               (table-insert! 'ws-test \"job\" #f) (loop)))
            ((and (pair? job) (eq? (car job) 'leases-foll))
             (let ((r (ask (pid-of (cadr job)) (list 'lease-leases (self)))))
               (table-insert! 'ws-test \"ll-foll-res\" r)
               (table-insert! 'ws-test \"job\" #f) (loop)))
            ((and (pair? job) (eq? (car job) 'revoke))
             (let ((r (ask (pid-of ldr) (list 'lease-revoke (self) (cadr job)))))
               (table-insert! 'ws-test \"rv-res\" r)
               (table-insert! 'ws-test \"job\" #f) (loop)))
            ((and (pair? job) (eq? (car job) 'probe))
             (let* ((nd (cadr job)) (id (caddr job)) (ks (cadddr job))
                    (r (ask (pid-of nd) (list 'lease-probe (self) id (map (lambda (k) (b k)) ks)))))
               (table-insert! 'ws-test \"probe-res\" r)
               (table-insert! 'ws-test \"job\" #f) (loop)))
            (else (yield) (loop))))))")
(spawn-source client-src 'client)

; ensure-leader!: re-detect the current leader and update ws-test "ldr" so the
; client actor's `let ((ldr ...))` picks up the right node on the next invocation.
; Called before each cluster section to absorb any transient leadership change
; (election + CheckQuorum can shift leadership during a test's idle periods).
(define (ensure-leader!)
  (spin (lambda () (leader-node)) "ensure-leader!")
  (set! LDR (leader-node))
  (table-insert! 'ws-test "ldr" LDR)
  LDR)

; job! submits a job, waits for the named result slot, returns it.
(define (job! result-key job)
  (table-insert! 'ws-test result-key #f)
  (table-insert! 'ws-test "job" job)
  (spin (lambda () (table-lookup 'ws-test result-key))
        (symbol->string (car job)))
  (table-lookup 'ws-test result-key))

(define (grant!  ttl id)        (job! "g-res"       (list 'grant ttl id)))
(define (put-leased! k v lid)   (job! "p-res"       (list 'put k v lid)))
(define (keepalive! id)         (job! "ka-res"      (list 'keepalive id)))
(define (keepalive-foll! nd id) (job! "ka-foll-res" (list 'keepalive-foll nd id)))
(define (ttl! id wk?)           (job! "ttl-res"     (list 'ttl id wk?)))
(define (ttl-foll! nd id wk?)   (job! "ttl-foll-res" (list 'ttl-foll nd id wk?)))
(define (leases!)               (job! "ll-res"      (list 'leases)))
(define (leases-foll! nd)       (job! "ll-foll-res" (list 'leases-foll nd)))
(define (revoke! id)            (job! "rv-res"      (list 'revoke id)))
(define (probe! nd id keys)     (job! "probe-res"   (list 'probe nd id keys)))

; assoc-key: pull the (key tombstone? mod-rev) triple for a key string from a probe reply.
(define (assoc-key pr kstr)
  (let ((target (string->utf8 kstr)))
    (let loop ((ks (caddr pr)))
      (cond ((null? ks) (list target 'absent 0))
            ((equal? (car (car ks)) target) (car ks))
            (else (loop (cdr ks)))))))

; ===========================================================================
; CLUSTER: non-leader redirect for keepalive / lease-ttl / lease-leases
; ===========================================================================
(section "CLUSTER: non-leader redirect for keepalive / lease-ttl / lease-leases")
; ensure we have a stable leader before this section
(ensure-leader!)
(define FOLL (car (follower-nodes LDR)))
(display "  leader: ") (display LDR) (display "  testing follower redirect on: ") (display FOLL) (newline)

; Grant a lease first so there is something to test against.
(define REDIR-LID 9001)
(check "grant lease 9001 ttl 30" (cons "LEASE-GRANT" REDIR-LID) (grant! 30 REDIR-LID))

; lease-keepalive to a follower -> ('lease-not-leader . LEADER-SYMBOL)
(let ((r (keepalive-foll! FOLL REDIR-LID)))
  (check "keepalive to follower -> lease-not-leader" 'lease-not-leader (car r))
  (check "keepalive redirect carries a valid leader node" #t
         (and (pair? r) (member (symbol->string (cdr r)) '("a" "b" "c")) #t)))

; lease-ttl to a follower -> ('lease-not-leader . LEADER-SYMBOL)
(let ((r (ttl-foll! FOLL REDIR-LID #f)))
  (check "lease-ttl to follower -> lease-not-leader" 'lease-not-leader (car r))
  (check "lease-ttl redirect carries a valid leader node" #t
         (and (pair? r) (member (symbol->string (cdr r)) '("a" "b" "c")) #t)))

; lease-leases to a follower -> ('lease-not-leader . LEADER-SYMBOL)
(let ((r (leases-foll! FOLL)))
  (check "lease-leases to follower -> lease-not-leader" 'lease-not-leader (car r))
  (check "lease-leases redirect carries a valid leader node" #t
         (and (pair? r) (member (symbol->string (cdr r)) '("a" "b" "c")) #t)))

; revoke the redirect-test lease
(revoke! REDIR-LID)

; ===========================================================================
; CLUSTER: LeaseLeases — 3 grants, list reflects live set, shrinks on revoke.
; ===========================================================================
(section "CLUSTER: LeaseLeases lists granted ids, shrinks on revoke")
(ensure-leader!)
(define LL-ID1 1001)
(define LL-ID2 1002)
(define LL-ID3 1003)
(grant! 60 LL-ID1)
(grant! 60 LL-ID2)
(grant! 60 LL-ID3)
(let ((r (leases!)))
  (check "lease-leases-ok tag" 'lease-leases-ok (car r))
  (let ((ids (cadr r)))
    (check "LL-ID1 1001 in list" #t (and (member LL-ID1 ids) #t))
    (check "LL-ID2 1002 in list" #t (and (member LL-ID2 ids) #t))
    (check "LL-ID3 1003 in list" #t (and (member LL-ID3 ids) #t))))
; revoke one and confirm it drops out
(put-leased! "ll-k" "v" LL-ID2)
(revoke! LL-ID2)
(let ((r (leases!)))
  (let ((ids (cadr r)))
    (check "LL-ID1 still present after revoking LL-ID2" #t (and (member LL-ID1 ids) #t))
    (check "LL-ID3 still present after revoking LL-ID2" #t (and (member LL-ID3 ids) #t))
    (check "LL-ID2 gone from list after revoke" #f (and (member LL-ID2 ids) #t))))
(revoke! LL-ID1)
(revoke! LL-ID3)

; ===========================================================================
; CLUSTER: LeaseTimeToLive — granted + remaining + attached keys.
; ===========================================================================
(section "CLUSTER: LeaseTimeToLive returns granted-ttl, remaining > 0, attached keys")
(ensure-leader!)
(define TTL-LID 7777)
(grant! 30 TTL-LID)
(put-leased! "ttl-k1" "v1" TTL-LID)
(put-leased! "ttl-k2" "v2" TTL-LID)

; with-keys? = #t
(let ((r (ttl! TTL-LID #t)))
  (check "lease-ttl-ok tag" 'lease-ttl-ok (car r))
  (let* ((gid  (cadr r))
         (gttl (caddr r))
         (rem  (cadddr r))
         (keys (car (cddddr r))))
    (check "id echoed" TTL-LID gid)
    (check "granted-ttl = 30" 30 gttl)
    (check "remaining in (0, 30]" #t (and (> rem 0) (<= rem 30)))
    (check "two attached keys returned" 2 (length keys))
    (check "ttl-k1 in keys" #t (and (member (string->utf8 "ttl-k1") keys) #t))
    (check "ttl-k2 in keys" #t (and (member (string->utf8 "ttl-k2") keys) #t))))

; with-keys? = #f (no keys returned)
(let ((r (ttl! TTL-LID #f)))
  (check "lease-ttl-ok no-keys tag" 'lease-ttl-ok (car r))
  (check "no keys when with-keys?=#f" '() (car (cddddr r))))

; after revoke -> remaining = -1 (dead marker)
(revoke! TTL-LID)
(let ((r (ttl! TTL-LID #f)))
  (check "lease-ttl-ok tag after revoke" 'lease-ttl-ok (car r))
  (check "remaining = -1 for dead lease" -1 (cadddr r)))

; ===========================================================================
; CLUSTER: keepalive PREVENTS expiry — the key correctness proof.
;
; 1. Grant a 2s-ttl lease, attach 1 key.
; 2. At ~1s (half TTL) send keepalive, which resets the deadline to now+2s.
; 3. At ~2.2s total elapsed assert the key STILL EXISTS on the leader.
;    (Without keepalive the original deadline would have fired at ~2s.)
; 4. STOP keepalives.  Wait ~2s+slack; the (reset) deadline expires, tick fires
;    LEASE-REVOKE through Raft.  Spin until all 3 replicas show the key gone.
; ===========================================================================
(section "CLUSTER: keepalive PREVENTS expiry while active; STOP -> key revoked")
(ensure-leader!)
(define KA-LID 5555)
(define KA-KEY "ka-key")
(check "grant lease 5555 ttl 2s" (cons "LEASE-GRANT" KA-LID) (grant! 2 KA-LID))
(let ((r (put-leased! KA-KEY "v" KA-LID)))
  (check "PUT ka-key with lease 5555 succeeded" #t
         (and (pair? r) (string=? (car r) "PUT") (> (cdr r) 0))))

; STEP 2 — wait ~1s (half TTL), then send keepalive.
(display "  [keepalive test] sleeping 1100ms before keepalive...") (newline)
(sleep-ms 1100)

(let ((r (keepalive! KA-LID)))
  (check "keepalive-ok tag" 'keepalive-ok (car r))
  (check "keepalive echoes lease id 5555" KA-LID (cadr r))
  (check "keepalive returns granted-ttl=2" 2 (caddr r)))

; STEP 3 — wait another ~1.1s (now ~2.2s elapsed total).  Without the keepalive
; the original 2s deadline would have fired; with it the deadline was reset to
; now+2s, so the lease should STILL be alive.
(display "  [keepalive test] sleeping 1100ms to verify key still alive...") (newline)
(sleep-ms 1100)

(let ((pr (probe! LDR KA-LID (list KA-KEY))))
  (check "after keepalive: lease meta still exists on leader" #t (car pr))
  (check "after keepalive: key still LIVE (not tombstoned)" #f
         (cadr (assoc-key pr KA-KEY))))

; STEP 4 — stop keepalives.  The deadline (reset by the last keepalive) will
; fire ~2s later.  Spin cooperatively until all 3 replicas see the key gone.
(display "  [keepalive test] stopping keepalives; waiting for expiry...") (newline)

(define (revoked-on-all-replicas?)
  (let loop ((nds '("a" "b" "c")))
    (cond ((null? nds) #t)
          (else
           (let ((pr (probe! (car nds) KA-LID (list KA-KEY))))
             (if (and (not (car pr))                          ; meta gone
                      (eq? (cadr (assoc-key pr KA-KEY)) #t)) ; key tombstoned
                 (loop (cdr nds))
                 #f))))))

(spin revoked-on-all-replicas? "keepalive-stop -> expiry on all 3 replicas")
(display "  lease expired after keepalive stop — confirmed on all replicas") (newline)

; Collect probes from all three and assert SAME tombstone revision on every replica
; (linearizable revoke: one Raft entry, one revision, every node).
(define pa (probe! "a" KA-LID (list KA-KEY)))
(define pb (probe! "b" KA-LID (list KA-KEY)))
(define pc (probe! "c" KA-LID (list KA-KEY)))

(check "meta GONE on a/b/c" '(#f #f #f) (list (car pa) (car pb) (car pc)))
(check "key tombstoned on a/b/c" '(#t #t #t)
       (list (cadr (assoc-key pa KA-KEY))
             (cadr (assoc-key pb KA-KEY))
             (cadr (assoc-key pc KA-KEY))))
(let ((ra (caddr (assoc-key pa KA-KEY)))
      (rb (caddr (assoc-key pb KA-KEY)))
      (rc (caddr (assoc-key pc KA-KEY))))
  (display "  revoke rev — a:") (display ra)
  (display " b:") (display rb)
  (display " c:") (display rc) (newline)
  (check "key deleted at SAME revision on all 3 replicas (linearizable revoke)" #t
         (and (= ra rb) (= rb rc) (> ra 0))))

; ===========================================================================
; CLUSTER: keepalive of a GONE (revoked) lease returns TTL=0.
; ===========================================================================
(section "CLUSTER: keepalive of a dead (revoked) lease returns TTL=0")
(ensure-leader!)
(define DEAD-LID 6666)
(grant! 60 DEAD-LID)
(revoke! DEAD-LID)
(let ((r (keepalive! DEAD-LID)))
  (check "keepalive-ok tag for dead lease" 'keepalive-ok (car r))
  (check "keepalive echoes id for dead lease" DEAD-LID (cadr r))
  (check "keepalive returns ttl=0 for dead lease (gone signal)" 0 (caddr r)))

; clean up client
(table-insert! 'ws-test "job" 'done)

(done!)
