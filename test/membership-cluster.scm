; test/membership-cluster.scm — cw-u4a.29: DYNAMIC MEMBERSHIP over the LIVE actor +
; transport layer. Binds the cw-u4a.28 joint-consensus engine (raft.scm, used as-is)
; to the shard-actor mailbox + cs-net node transport and proves config changes work
; on a running multi-voter cluster — not just in the pure engine simulator.
;   crabscheme run test/membership-cluster.scm
;
; In-process sim (mirrors sim-cluster-smoke): N logical nodes, the in-memory mesh
; (node-link!), one shard "0" per node, real spawn-source shard + peer-poller actors
; (the tick clock + frame drainers), cooperative spin on the shared ws-shard-* tables,
; a per-run wall-clock dir tag. This is the SAME code that runs over real TCP — only
; the transport wiring differs (node-link! here vs node-connect in node-cluster.scm,
; whose --join path brings a new node up as a non-voter that dials the existing mesh).
;
; Coverage (all over the live cluster, driven through the member-* mailbox protocol):
;   * add a voter   : 3 -> 4; the 4th node joins live, catches up the prior log by
;                     replication, becomes a voter on every node, and a NEW write commits
;                     under the 4-voter config and is visible on the new node.
;   * learner       : add L as a non-voting learner (single-phase), it catches up the
;                     log, the cluster keeps committing under the voter majority, then
;                     member-promote makes L a voter.
;   * remove a voter: 4 -> 3, removing the LEADER — it steps down after Cnew, a new
;                     leader emerges from the survivors, and the 3-voter cluster commits.
;   * member-list   : every node's config view reflects the changes.
;   * in-flight     : a second conf-change while one pends is refused ('member-pending).

(include "test/harness.scm")

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

; ============================================================
(section "add a voter: live 3-voter {a,b,c} -> 4 (join d)")
; ============================================================
(bringup '(a b c))
(define s1-cands '("a" "b" "c"))
(define s1-ldr (cur-leader s1-cands))
(display "  leader: ") (display s1-ldr) (newline)
; two committed writes form a prior log for the new node to catch up on. commit-write!
; re-resolves the leader + retries on a redirect, so a transient election never strands us.
(commit-write! s1-cands "city" "oslo")
(commit-write! s1-cands "n" "1")
(spin (lambda () (and (>= (applied "a") 3) (>= (applied "b") 3) (>= (applied "c") 3)))
      "prior writes applied on all 3")
(define pre-applied (applied s1-ldr))
(let ((ml (member-list-of s1-ldr)))
  (check "baseline voters = {a,b,c}" #t (set= (cadr ml) '(a b c)))
  (check "baseline has no learners"  #t (null? (caddr ml))))

; bring up the 4th node live (non-voter) and link it into the running mesh.
(spawn-joiner 'd '(a b c))
(spin (lambda () (shard-pid "d")) "d's shard published")

; in-flight test folded in: add d (real) + add e (refused while d's change pends).
(table-insert! 'ws-test "if-done" #f)
(table-insert! 'ws-test "if-target" (cur-leader s1-cands))
(table-insert! 'ws-test "if-n1" 'd)
(table-insert! 'ws-test "if-n2" 'e)
(spawn-source inflight-client 'inflight)
(spin (lambda () (table-lookup 'ws-test "if-done")) "member-add d settled + e refused")
(define if-r1 (table-lookup 'ws-test "if-r1"))
(define if-r2 (table-lookup 'ws-test "if-r2"))
(display "  in-flight replies: ") (write if-r1) (display " | ") (write if-r2) (newline)
(check "a second conf-change while one pends is refused"
       #t (or (eq? if-r1 'member-pending) (eq? if-r2 'member-pending)))
(check "the member-add itself acked 'member-ok"
       #t (or (and (pair? if-r1) (eq? (car if-r1) 'member-ok))
              (and (pair? if-r2) (eq? (car if-r2) 'member-ok))))

; d catches up the prior log by replication (its applied advances).
(spin (lambda () (>= (applied "d") pre-applied)) "d catches up the prior log")
(display "  applied a/b/c/d = ")
(display (list (applied "a") (applied "b") (applied "c") (applied "d"))) (newline)
(check "prior key city replicated on d" (list "oslo" 1 1 1) (await-key "d" "city" "city -> d"))
(check "prior key n replicated on d"    (list "1"    2 2 1) (await-key "d" "n"    "n -> d"))

; a NEW write commits under the 4-voter config and is visible on the new node.
(commit-write! '("a" "b" "c" "d") "z" "added")
(check "new write z=added visible on the new node d" (list "added" 3 3 1)
       (await-key "d" "z" "z -> d (4-voter commit)"))

; every node's config view now shows the 4-voter set.
(for-each
 (lambda (nd)
   (let ((ml (member-list-of nd)))
     (check (string-append nd " sees voters {a,b,c,d}") #t (set= (cadr ml) '(a b c d)))
     (check (string-append nd " sees no learners")      #t (null? (caddr ml)))))
 '("a" "b" "c" "d"))

; ============================================================
(section "leader-gating: member-add to a FOLLOWER redirects")
; ============================================================
; The three mutating member ops are leader-gated exactly like watch-register / lease-grant:
; a non-leader must REDIRECT (so .30's Cluster gRPC / a client re-targets the leader), not
; silently serve the change. Pick a definite follower (re-confirm its role to avoid a race
; with a leadership flip) and assert the redirect; gate-x is never actually added.
(let* ((cur (cur-leader '("a" "b" "c" "d")))
       (flw (car (remove-str cur '("a" "b" "c" "d")))))
  (spin (lambda () (eq? (role flw) 'follower)) "a follower to probe")
  (let ((reply (member-op! flw 'add 'gate-x #f)))
    (display "  leader=") (display cur) (display " follower=") (display flw)
    (display " reply: ") (write reply) (newline)
    (check "member-add to a follower is refused with a redirect"
           'member-not-leader (and (pair? reply) (car reply)))
    (check "the redirect names a live node (or #f while settling)"
           #t (or (not (cdr reply)) (in? (cdr reply) '(a b c d))))
    ; the redirect must NOT have mutated the config — still the 4-voter set, no learners.
    (let ((ml (member-list-of cur)))
      (check "config unchanged after the refused add" #t (set= (cadr ml) '(a b c d)))
      (check "no stray learner from the refused add"  #t (null? (caddr ml))))))

; ============================================================
(section "learner: non-voting member, then promoted to voter")
; ============================================================
(bringup '(la lb lc))
(define s2-cands '("la" "lb" "lc"))
(define s2-ldr (cur-leader s2-cands))
(display "  leader: ") (display s2-ldr) (newline)
(commit-write! s2-cands "k0" "v0")
(spin (lambda () (and (>= (applied "la") 2) (>= (applied "lb") 2) (>= (applied "lc") 2)))
      "k0 on all voters")
(define s2-pre (applied s2-ldr))

(spawn-joiner 'L '(la lb lc))
(spin (lambda () (shard-pid "L")) "L's shard published")

; add L as a LEARNER — voter set UNCHANGED, so this is a single-phase change.
(define add-L (member-op! (cur-leader s2-cands) 'add 'L #t))
(display "  add-learner reply: ") (write add-L) (newline)
(check "learner add acked 'member-ok" 'member-ok (car add-L))
(let ((ml (member-list-of s2-ldr)))
  (check "voter set unchanged {la,lb,lc}" #t (set= (cadr ml) '(la lb lc)))
  (check "L is a learner, NOT a voter"    #t (and (in? 'L (caddr ml)) (not (in? 'L (cadr ml))))))

; L catches up the log by replication (its match/applied advances as the leader streams it
; the prior entries — the "receives entries" half of the learner contract).
(spin (lambda () (>= (applied "L") s2-pre)) "learner L catches up the log")
(check "k0 replicated on learner L" (list "v0" 1 1 1) (await-key "L" "k0" "k0 -> L"))

; the cluster keeps committing under the VOTER majority (L does not count toward quorum —
; proven strictly in raft-membership.scm; here the live cluster commits with L present).
(commit-write! s2-cands "k1" "v1")
(check "k1 replicated on learner L" (list "v1" 2 2 1) (await-key "L" "k1" "k1 -> L"))

; PROMOTE L to a voter (voter set changes -> two-phase joint).
(define prom-L (member-op! (cur-leader s2-cands) 'promote 'L #f))
(display "  promote reply: ") (write prom-L) (newline)
(check "promote acked 'member-ok" 'member-ok (car prom-L))
(let ((ml (member-list-of s2-ldr)))
  (check "after promotion L IS a voter" #t (in? 'L (cadr ml)))
  (check "voters = {la,lb,lc,L}"        #t (set= (cadr ml) '(la lb lc L)))
  (check "learners now empty"           #t (null? (caddr ml))))

; a write still commits under the new 4-voter config, replicated to the now-voter L.
(commit-write! '("la" "lb" "lc" "L") "k2" "v2")
(check "k2 replicated on the (now voter) L" (list "v2" 3 3 1) (await-key "L" "k2" "k2 -> L"))

; ============================================================
(section "remove a voter: 4-voter {ra,rb,rc,rd} -> 3 (remove the LEADER)")
; ============================================================
(bringup '(ra rb rc rd))
(define s3-cands '("ra" "rb" "rc" "rd"))
(define s3-ldr0 (cur-leader s3-cands))
(display "  initial leader: ") (display s3-ldr0) (newline)
(commit-write! s3-cands "before" "x")
(spin (lambda () (and (>= (applied "ra") 2) (>= (applied "rb") 2)
                      (>= (applied "rc") 2) (>= (applied "rd") 2)))
      "before-write on all 4")

; remove the CURRENT leader (re-resolve so we remove whoever actually leads now): the engine
; steps it down only AFTER the Cnew commits (Ongaro §4.3), so the change still acks 'member-ok.
(define s3-rmv (cur-leader s3-cands))
(define rm (member-op! s3-rmv 'remove (string->symbol s3-rmv) #f))
(display "  removed leader ") (display s3-rmv) (display " -> reply: ") (write rm) (newline)
(check "remove acked 'member-ok" 'member-ok (car rm))
(check "removed node dropped from the voter set" #f (in? (string->symbol s3-rmv) (cadr rm)))

; a new leader emerges from the surviving 3 voters, which keep committing.
(define survivors (remove-str s3-rmv '("ra" "rb" "rc" "rd")))
(spin (lambda () (leader-among survivors)) "new leader among survivors")
(define s3-ldr1 (leader-among survivors))
(display "  new leader: ") (display s3-ldr1) (newline)
(check "a survivor leads"                 #t (and (member s3-ldr1 survivors) #t))
(check "the removed old leader does NOT lead" #f (eq? (role s3-rmv) 'leader))

; the surviving 3-voter cluster commits a fresh write (re-resolving its leader as needed).
(commit-write! survivors "after" "y")
(check "after=y committed without the removed leader" (list "y" 2 2 1)
       (await-key s3-ldr1 "after" "after -> survivor"))

(let ((ml (member-list-of s3-ldr1)))
  (check "survivor sees a 3-voter config"       3 (length (cadr ml)))
  (check "removed node absent from the voters"  #f (in? (string->symbol s3-rmv) (cadr ml))))

(done!)
