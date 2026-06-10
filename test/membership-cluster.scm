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

; cw-u4a.31: the live-cluster machinery (make-tables, spin, bringup, spawn-joiner,
; commit-write!, await-key, member-op!, client sources, set/leader helpers) is shared
; with membership-load.scm.  This file owns ONLY the .29 coverage sections below.
(include "test/membership-harness.scm")
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
