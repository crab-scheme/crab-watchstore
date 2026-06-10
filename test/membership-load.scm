; test/membership-load.scm — cw-u4a.31 Phase 7 safety/stress capstone: dynamic
; membership UNDER WRITE LOAD + DEEP learner catch-up + LIVE no-split-brain under churn,
; over the same LIVE actor/transport layer as membership-cluster.scm (cw-u4a.29).
;   crabscheme run test/membership-load.scm
;
; TEST-ONLY: it CALLS the .28 joint-consensus engine (src/raft.scm) + the .29 shard-actor
; mailbox (src/server/shard-actor.scm) verbatim — it changes NEITHER.  It reuses the .29
; harness (test/membership-harness.scm) wholesale: the cooperative `spin` (REQUIRED — a
; tight main-thread busy-loop starves the green client pool and triggers spurious
; CheckQuorum stepdowns), the dedicated-thread `bringup`/`spawn-joiner` sim mesh, the
; exactly-once `commit-write!` (+ its `linread!`/point-read seal of an 'indeterminate
; write — a PUT is non-idempotent, so a double-apply would shift every later revision),
; and `await-key` (waits on REAL mvcc key presence, not an applied-index proxy).
;
; Coverage (NEW vs the .29 single-change happy paths):
;   1. add + remove a voter UNDER WRITE LOAD: a stream of writes is interleaved AROUND
;      both config changes; EVERY acked write survives on the final config with the exact
;      value AND a strictly-increasing, contiguous revision (no loss / dup / reorder).
;   2. DEEP learner catch-up: a learner joins AFTER a 12-write backlog and must replay the
;      WHOLE log (applied reaches the leader's; first/middle/last keys present), then is
;      promoted to a voter.
;   3. LIVE no-split-brain under churn: while removing the LEADER (forced re-election),
;      AT MOST ONE leader is ever observed, and committed state is consistent (a confirmed
;      linearizable read + the same value before and after the change).

(include "test/harness.scm")
(include "test/membership-harness.scm")

; [1,hi] inclusive integer range (the dialect has no `iota`) — for the expected rev list.
(define (range-incl lo hi) (if (> lo hi) '() (cons lo (range-incl (+ lo 1) hi))))

; ---- async member op (so client writes can commit WHILE a change is in flight) ----
; member-op! (shared harness) is synchronous; these split the same mop-client protocol
; into start (fire) + await (collect the async 'member-ok ack), so the test can interleave
; commit-write! calls between them.  Same tables/sources as member-op! — no new mailbox.
(define (member-op-start! target kind nn lrn)
  (table-insert! 'ws-test "mop-done" #f)
  (table-insert! 'ws-test "mop-target" target)
  (table-insert! 'ws-test "mop-kind" kind)
  (table-insert! 'ws-test "mop-node" nn)
  (table-insert! 'ws-test "mop-learner" lrn)
  (spawn-source mop-client 'mop))
(define (member-op-await! who)
  (spin (lambda () (table-lookup 'ws-test "mop-done")) who)
  (table-lookup 'ws-test "mop-result"))

; ============================================================
(section "add then remove a voter UNDER WRITE LOAD: no committed write is lost")
; ============================================================
(bringup '(a b c))
(define s1-cands '("a" "b" "c"))
(display "  leader: ") (display (cur-leader s1-cands)) (newline)

; commit-write! is exactly-once; record each acked (key value rev) to assert no loss/dup.
(define load-log '())                              ; (key value rev), newest-first
(define (load-write! cands i)
  (let* ((k (string-append "load-" (number->string i)))
         (v (string-append "v" (number->string i)))
         (r (commit-write! cands k v)))
    (set! load-log (cons (list k v (cdr r)) load-log))))
(define (load-range! cands lo hi)
  (let loop ((i lo)) (if (<= i hi) (begin (load-write! cands i) (loop (+ i 1))))))

; first batch: a stream of K+1 = 9 committed writes on the 3-voter cluster.
(load-range! s1-cands 0 8)
(display "  committed load-0..load-8 on {a,b,c}") (newline)

; ADD d UNDER LOAD: bring d up, START the add async, commit MORE writes while the change
; is in flight (they commit under the joint quorum — a,b,c satisfy both Cold and Cnew),
; then await its commit.  "under load" on a cooperative pool = interleave, not parallel.
(spawn-joiner 'd '(a b c))
(spin (lambda () (shard-pid "d")) "d's shard published")
(member-op-start! (cur-leader s1-cands) 'add 'd #f)
(load-range! s1-cands 9 10)                        ; writes committing WHILE the add is in flight
(define add-reply (member-op-await! "member-add d settles"))
(display "  add d reply: ") (write add-reply) (newline)
(check "the member-add d acked 'member-ok" 'member-ok (car add-reply))
(load-range! '("a" "b" "c" "d") 11 12)             ; writes under the settled 4-voter config

; REMOVE a NON-LEADER voter UNDER LOAD (no leadership change, isolates the no-loss claim):
; same interleave — start async, commit writes mid-flight, await.
(define ldr4 (cur-leader '("a" "b" "c" "d")))
(define rm-target (car (remove-str ldr4 (list "a" "b" "c"))))   ; a non-leader original voter
(member-op-start! ldr4 'remove (string->symbol rm-target) #f)
(load-range! '("a" "b" "c" "d") 13 14)             ; writes committing WHILE the remove is in flight
(define rm-reply (member-op-await! "member-remove settles"))
(display "  removed non-leader ") (display rm-target) (display " -> reply: ") (write rm-reply) (newline)
(check "the member-remove acked 'member-ok" 'member-ok (car rm-reply))
(define final-cands (remove-str rm-target '("a" "b" "c" "d")))
(load-range! final-cands 15 16)                    ; writes under the final 3-voter config
(define M 16)

; ---- ASSERT: every acked write survived with the EXACT value AND revision, and the
;      revisions are strictly increasing + contiguous (membership entries bump no rev) —
;      i.e. no write was lost, duplicated, or reordered across either config change. ----
(define revs-in-order (map caddr (reverse load-log)))   ; revs in write order
(check "all load writes got STRICTLY-INCREASING CONTIGUOUS revs (exactly-once, no reorder)"
       (range-incl 1 (+ M 1)) revs-in-order)
(define read-node (cur-leader final-cands))
(let loop ((i 0))
  (if (<= i M)
      (let* ((k  (string-append "load-" (number->string i)))
             (ev (list (string-append "v" (number->string i)) (+ i 1) (+ i 1) 1))
             (got (await-key read-node k k)))
        (check (string-append "committed write " k " survives with correct value+rev") ev got)
        (loop (+ i 1)))))

; the final config is exactly the surviving 3 voters (removed gone, d present, no learners).
(let ((ml (member-list-of read-node)))
  (check "final config is a 3-voter set"     3  (length (cadr ml)))
  (check "the removed voter is gone"         #f (in? (string->symbol rm-target) (cadr ml)))
  (check "the added voter d is present"      #t (in? 'd (cadr ml)))
  (check "no learners in the final config"   #t (null? (caddr ml))))

; ============================================================
(section "deep learner catch-up: L replays a full pre-join backlog")
; ============================================================
(bringup '(e f g))
(define s2-cands '("e" "f" "g"))
(display "  leader: ") (display (cur-leader s2-cands)) (newline)

; a BACKLOG of N = 12 distinct writes committed BEFORE any learner exists.
(define N 12)
(define (back-write! i)
  (commit-write! s2-cands (string-append "back-" (number->string i))
                          (string-append "b" (number->string i))))
(let loop ((i 0)) (if (< i N) (begin (back-write! i) (loop (+ i 1)))))
(spin (lambda () (and (>= (applied "e") (+ N 1)) (>= (applied "f") (+ N 1)) (>= (applied "g") (+ N 1))))
      "backlog applied on all 3 voters")
(define s2-ldr (cur-leader s2-cands))
(define pre-applied (applied s2-ldr))
(display "  pre-join applied (leader) = ") (display pre-applied) (newline)

; bring L up as a non-voter, then add it as a LEARNER (voter set unchanged -> single-phase).
(spawn-joiner 'L '(e f g))
(spin (lambda () (shard-pid "L")) "L's shard published")
(define add-L (member-op! (cur-leader s2-cands) 'add 'L #t))
(check "learner add acked 'member-ok" 'member-ok (car add-L))

; L replays the WHOLE backlog: its applied index reaches the leader's pre-join index.
(spin (lambda () (>= (applied "L") pre-applied)) "L replays the full pre-join backlog")
(check "L's applied reached the leader's pre-join index" #t (>= (applied "L") pre-applied))
(display "  applied e/f/g/L = ")
(display (list (applied "e") (applied "f") (applied "g") (applied "L"))) (newline)

; spot-check FIRST / MIDDLE / LAST backlog keys present on L — proves it replayed the
; whole log, not just entries appended after it joined.  back-i committed at rev i+1.
(check "backlog FIRST  key back-0  replayed on L" (list "b0"  1  1  1) (await-key "L" "back-0"  "back-0 -> L"))
(check "backlog MIDDLE key back-6  replayed on L" (list "b6"  7  7  1) (await-key "L" "back-6"  "back-6 -> L"))
(check "backlog LAST   key back-11 replayed on L" (list "b11" 12 12 1) (await-key "L" "back-11" "back-11 -> L"))

; PROMOTE L -> voter (two-phase joint), then commit one more write visible on the now-voter L.
(define prom-L (member-op! (cur-leader s2-cands) 'promote 'L #f))
(check "promote L acked 'member-ok" 'member-ok (car prom-L))
(let ((ml (member-list-of s2-ldr)))
  (check "after promotion L IS a voter" #t (in? 'L (cadr ml)))
  (check "voters = {e,f,g,L}"           #t (set= (cadr ml) '(e f g L))))
(commit-write! '("e" "f" "g" "L") "back-12" "b12")
(check "post-promotion write visible on the now-voter L" (list "b12" 13 13 1)
       (await-key "L" "back-12" "back-12 -> L"))

; ============================================================
(section "live no-split-brain under churn: <=1 leader; committed state consistent")
; ============================================================
(bringup '(p q r s))
(define s3-cands '("p" "q" "r" "s"))
(display "  initial leader: ") (display (cur-leader s3-cands)) (newline)
(commit-write! s3-cands "anchor" "v0")
(spin (lambda () (and (>= (applied "p") 2) (>= (applied "q") 2)
                      (>= (applied "r") 2) (>= (applied "s") 2)))
      "anchor applied on all 4")
; SEAL the committed state BEFORE the change: a confirmed linearizable read + anchor value.
(define before-read   (linread! (cur-leader s3-cands)))
(define before-anchor (summarize (get-node (cur-leader s3-cands) "anchor")))
(check "linread confirmed BEFORE the change" 'read-ok (car before-read))

; CHURN: remove the LEADER (forces a stepdown + a fresh election among the survivors).
; While the change settles AND the survivors re-elect, densely sample every node's role
; and assert AT MOST ONE leader is ever observed (no two simultaneous leaders).  The strict
; joint-quorum split-brain impossibility is engine-proven in raft-membership.scm; this is
; the live realism check.
(define max-leaders 0)
(define (count-leaders names)
  (let loop ((ns names) (c 0))
    (cond ((null? ns) c)
          ((eq? (role (car ns)) 'leader) (loop (cdr ns) (+ c 1)))
          (else (loop (cdr ns) c)))))
(define (sample-leaders!)
  (let ((c (count-leaders s3-cands))) (if (> c max-leaders) (set! max-leaders c))))

(define rm-ldr (cur-leader s3-cands))
(member-op-start! rm-ldr 'remove (string->symbol rm-ldr) #f)
(spin (lambda () (sample-leaders!) (table-lookup 'ws-test "mop-done")) "remove-leader settles")
(define rm-reply3 (member-op-await! "remove-leader ack"))
(display "  removed leader ") (display rm-ldr) (display " -> reply: ") (write rm-reply3) (newline)
(check "remove-leader acked 'member-ok" 'member-ok (car rm-reply3))
(define survivors3 (remove-str rm-ldr s3-cands))
(spin (lambda () (sample-leaders!) (leader-among survivors3)) "new leader among survivors")
(check "AT MOST ONE leader at any sample across the membership change" #t (<= max-leaders 1))

; committed state is consistent across the churn (no divergence): same anchor value + a
; confirmed linearizable read on the NEW leader, with a monotonic read index.
(define new-ldr3    (leader-among survivors3))
(display "  new leader: ") (display new-ldr3) (newline)
(define after-read   (linread! new-ldr3))
(define after-anchor (summarize (get-node new-ldr3 "anchor")))
(check "linread confirmed AFTER the change"  'read-ok (car after-read))
(check "the committed anchor is UNCHANGED across the churn (no divergence)"
       before-anchor after-anchor)
(check "the new leader's read index is >= the pre-change index (monotonic)"
       #t (>= (cdr after-read) (cdr before-read)))
; the post-churn cluster is still live: a fresh write commits + is readable.
(commit-write! survivors3 "post" "v1")
(check "the cluster still commits after the churn" (list "v1" 2 2 1)
       (await-key new-ldr3 "post" "post -> survivor"))

(done!)
