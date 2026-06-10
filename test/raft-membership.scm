; raft-membership.scm — pure-engine tests for DYNAMIC MEMBERSHIP (joint consensus,
; Ongaro thesis §4.3) + learners, added to raft.scm (cw-u4a.28). Mirrors
; raft-cq-prevote.scm: it drives the PURE engine + the in-Scheme cluster simulator
; deterministically to quiescence — no actors, no transport, no clocks.
;   crabscheme run test/raft-membership.scm
;
; Coverage:
;   * add a voter   (3 -> 4): two-phase joint commit; all agree; new voter has the
;                             log; a proposal commits under the new majority.
;   * remove a voter(5 -> 4): the removed node (here the LEADER) leaves quorum and
;                             steps down after Cnew commits; survivors elect + commit.
;   * joint safety  : during the joint phase NEITHER the old nor the new config alone
;                     can elect a leader OR commit — only the joint (old AND new)
;                     majority can. Proven in BOTH directions (no split-brain).
;   * learner       : added (non-voting), replicated-to, ack does NOT count; then
;                     promoted to voter, after which its ack DOES count.
;   * in-flight     : a second conf-change is refused while one is pending.

(include "src/raft.scm")
(include "test/harness.scm")

(define (noop sm cmd) sm)
(define (cmd s) (list (string->utf8 s)))        ; a real (non-conf, non-noop) command

; ---- simulator accessors (config is read straight off the node assoc-list) ----
(define (voters-of c id)   (aget (cluster-get c id) 'voters))
(define (old-of c id)      (aget (cluster-get c id) 'voters-old))
(define (learners-of c id) (aget (cluster-get c id) 'learners))
(define (peers-of c id)    (aget (cluster-get c id) 'peers))
(define (role-of c id)     (raft-role (cluster-get c id)))
(define (commit-of c id)   (aget (cluster-get c id) 'commit))
(define (loglen-of c id)   (log-len (cluster-get c id)))
(define (sset= a b)        (equal-set? a b))     ; equal-set? is from raft.scm
(define (mem? x lst)       (and (memv x lst) #t))

; add a node that EXISTS (routable) but is NOT yet a voter: its config is the current
; voter set, which excludes it. It becomes a participant only via a conf-change.
(define (cluster-add-node c id voters)
  (cluster-set c id (make-raft id voters noop 0)))

; deliver ONE message and return (c' . resulting-(from to msg)-tuples), so a test can
; route selectively (e.g. withhold replies to freeze a partial state).
(define (deliver1 c from to msg)
  (let* ((res (raft-step (cluster-get c to) from msg))
         (c2  (cluster-set c to (car res))))
    (cons c2 (map (lambda (o) (list to (car o) (cdr o))) (cdr res)))))

; ============================================================
(section "add a voter: 3 -> 4 (two-phase joint)")
; ============================================================
(define av0 (cluster-add-node (cluster-make '(a b c) noop 0) 'd '(a b c)))
(define av1 (cluster-campaign av0 'a))
(check "a leads the initial {a,b,c}"  'leader (role-of av1 'a))
(check "d exists but is NOT a voter"  #f (is-voter? (cluster-get av1 'd) 'd))
; conf-change: add d. Voter set {a,b,c} -> {a,b,c,d} => enters joint, auto-leaves it.
(define av2 (cluster-propose-conf-change av1 'a '(a b c d) '()))
(check "a now config {a,b,c,d}"       #t (sset= (voters-of av2 'a) '(a b c d)))
(check "b now config {a,b,c,d}"       #t (sset= (voters-of av2 'b) '(a b c d)))
(check "c now config {a,b,c,d}"       #t (sset= (voters-of av2 'c) '(a b c d)))
(check "d now config {a,b,c,d}"       #t (sset= (voters-of av2 'd) '(a b c d)))
(check "transition done: a not joint" #f (old-of av2 'a))
(check "transition done: d not joint" #f (old-of av2 'd))
(check "no conf-change pending (a)"   #f (conf-change-pending? (cluster-get av2 'a)))
(check "d is now a voter"             #t (is-voter? (cluster-get av2 'd) 'd))
(check "d caught up the full log"     #t (= (loglen-of av2 'd) (loglen-of av2 'a)))
(check "a is still leader"            'leader (role-of av2 'a))
; a fresh proposal now commits under the 4-node majority (3 of 4) on the leader.
(define av-c0 (commit-of av2 'a))
(define av3 (cluster-propose av2 'a (cmd "x")))
(check "proposal commits on the leader" #t (> (commit-of av3 'a) av-c0))
; tick once to flush the new commit index; every replica converges.
(define av4 (cluster-tick av3 'a))
(check "d's commit converges to a's"  #t (= (commit-of av4 'd) (commit-of av4 'a)))

; ============================================================
(section "remove a voter: 5 -> 4 (the LEADER removes itself)")
; ============================================================
(define rv0 (cluster-make '(a b c d e) noop 0))
(define rv1 (cluster-campaign rv0 'a))
(check "a leads the 5-node group"     'leader (role-of rv1 'a))
; remove the leader a: voter set {a,b,c,d,e} -> {b,c,d,e}
(define rv2 (cluster-propose-conf-change rv1 'a '(b c d e) '()))
(check "a stepped down after Cnew"    'follower (role-of rv2 'a))
(check "a config {b,c,d,e}"           #t (sset= (voters-of rv2 'a) '(b c d e)))
(check "b config {b,c,d,e}"           #t (sset= (voters-of rv2 'b) '(b c d e)))
(check "a is no longer a voter"       #f (is-voter? (cluster-get rv2 'a) 'a))
(check "no conf-change pending (a)"   #f (conf-change-pending? (cluster-get rv2 'a)))
; the surviving four elect a new leader and commit WITHOUT a.
(define rv3 (cluster-campaign rv2 'b))
(check "b becomes the new leader"     'leader (role-of rv3 'b))
(check "removed a is NOT in b's peers" #f (mem? 'a (peers-of rv3 'b)))
(define rv-c0 (commit-of rv3 'b))
(define rv4 (cluster-propose rv3 'b (cmd "y")))
(check "new 4-node cluster commits w/o a" #t (> (commit-of rv4 'b) rv-c0))

; ============================================================
(section "joint safety: no split-brain across the transition")
; ============================================================
; 5 voters {a,b,c,d,e}; begin shrinking to {a,b}. Freeze the cluster MID-JOINT
; (every node has appended the joint entry, so it knows old={a,b,c,d,e}/new={a,b},
; but Cnew has NOT committed). Then prove neither side can act alone.
(define js0 (cluster-make '(a b c d e) noop 0))
(define js1 (cluster-campaign js0 'a))
(define js-jr (raft-propose-conf-change (cluster-get js1 'a) '(a b) '()))  ; (a' . AEs)
(define js2 (cluster-set js1 'a (car js-jr)))
; deliver the joint AE to each follower (they ADOPT the joint config) but WITHHOLD
; their replies, so the leader never commits / never appends Cnew — frozen in joint.
(define js3
  (let loop ((cc js2) (outs (cdr js-jr)))
    (if (null? outs) cc
        (let ((o (car outs)))
          (loop (car (deliver1 cc 'a (car o) (cdr o))) (cdr outs))))))
(check "a is mid-joint"               #t (sset= (old-of js3 'a) '(a b c d e)))
(check "c adopted the joint config"   #t (sset= (old-of js3 'c) '(a b c d e)))
(check "c new voters = {a,b}"         #t (sset= (voters-of js3 'c) '(a b)))

; (1) ELECTION: the old-majority {c,d,e} alone CANNOT elect (no new-majority).
(define el0 (car (raft-campaign (cluster-get js3 'c))))    ; c -> candidate (old voter)
(check "c is a candidate"             'candidate (raft-role el0))
(define el1 (car (on-rvr el0 'd (list 'rvr (raft-term el0) #t))))
(define el2 (car (on-rvr el1 'e (list 'rvr (raft-term el1) #t))))
(check "old-majority {c,d,e} canNOT elect" 'candidate (raft-role el2))
; add the new-side {a,b} votes -> both majorities -> NOW c can lead.
(define el3 (car (on-rvr el2 'a (list 'rvr (raft-term el2) #t))))
(define el4 (car (on-rvr el3 'b (list 'rvr (raft-term el3) #t))))
(check "old AND new majority elects c"      'leader (raft-role el4))

; (2) COMMIT: the new-majority {a,b} alone CANNOT commit the joint entry (no
;     old-majority). a is the leader; set who has acked via matchIndex.
(define cm-a   (cluster-get js3 'a))
(define cm-idx (log-len cm-a))                              ; the joint entry's index
(define cm-c0  (aget cm-a 'commit))
(define cm-ab  (aset cm-a 'match (aset (aget cm-a 'match) 'b cm-idx)))    ; {a,b} only
(check "new-majority {a,b} canNOT commit"  cm-c0 (aget (maybe-commit cm-ab) 'commit))
; add old-side acks (c,d) -> both majorities -> the joint entry commits.
(define cm-all (aset cm-ab 'match (aset (aset (aget cm-ab 'match) 'c cm-idx) 'd cm-idx)))
(check "old AND new majority commits"      #t (> (aget (maybe-commit cm-all) 'commit) cm-c0))

; ============================================================
(section "learner: non-counting member, then promoted to voter")
; ============================================================
(define ln0 (cluster-add-node (cluster-make '(a b c) noop 0) 'L '(a b c)))
(define ln1 (cluster-campaign ln0 'a))
; add L as a LEARNER — the voter set is UNCHANGED, so this is a single-phase change.
(define ln2 (cluster-propose-conf-change ln1 'a '(a b c) '(L)))
(check "a learners = {L}"             #t (sset= (learners-of ln2 'a) '(L)))
(check "voter set unchanged {a,b,c}"  #t (sset= (voters-of ln2 'a) '(a b c)))
(check "L is NOT a voter"             #f (is-voter? (cluster-get ln2 'L) 'L))
(check "leader replicates to L (peer)" #t (mem? 'L (peers-of ln2 'a)))
(check "L received entries (match>0)" #t (> (assq-def 'L (aget (cluster-get ln2 'a) 'match) 0) 0))
(check "L knows it is a learner"      #t (sset= (learners-of ln2 'L) '(L)))
; PROOF the learner's ack does NOT count: propose an entry; self + L "acked" is not a
; quorum (L is a learner); adding a real voter (b) makes it commit.
(define lead0 (car (raft-propose (cluster-get ln2 'a) (cmd "k"))))
(define k-idx (log-len lead0))
(define lead-L  (aset lead0 'match (aset (aget lead0 'match) 'L k-idx)))      ; only L
(check "learner ack alone does NOT commit"
       (aget lead0 'commit) (aget (maybe-commit lead-L) 'commit))
(define lead-Lb (aset lead-L 'match (aset (aget lead-L 'match) 'b k-idx)))    ; + voter b
(check "a real voter ack DOES commit"
       #t (> (aget (maybe-commit lead-Lb) 'commit) (aget lead0 'commit)))
; PROMOTE L to a voter (voter set changes {a,b,c} -> {a,b,c,L} => two-phase joint).
(define ln3 (cluster-propose-conf-change ln2 'a '(a b c L) '()))
(check "after promotion L IS a voter" #t (is-voter? (cluster-get ln3 'L) 'L))
(check "a voters = {a,b,c,L}"         #t (sset= (voters-of ln3 'a) '(a b c L)))
(check "a learners now empty"         #t (null? (learners-of ln3 'a)))
; PROOF the promoted member now counts: {a,b} (2 of 4) is short; {a,b,L} (3 of 4) commits.
(define lead2 (car (raft-propose (cluster-get ln3 'a) (cmd "m"))))
(define m-idx (log-len lead2))
(define m-ab  (aset lead2 'match (aset (aget lead2 'match) 'b m-idx)))        ; a,b only
(check "2 of 4 voters does NOT commit"
       (aget lead2 'commit) (aget (maybe-commit m-ab) 'commit))
(define m-abL (aset m-ab 'match (aset (aget m-ab 'match) 'L m-idx)))          ; + promoted L
(check "promoted member's ack now COUNTS (3 of 4)"
       #t (> (aget (maybe-commit m-abL) 'commit) (aget lead2 'commit)))

; ============================================================
(section "conf-change in flight: a second change is refused")
; ============================================================
(define if0 (cluster-make '(a b c) noop 0))
(define if1 (cluster-campaign if0 'a))
(define if-r1 (raft-propose-conf-change (cluster-get if1 'a) '(a b c d) '()))  ; #1, unsettled
(define if-a1 (car if-r1))
(check "change #1 appended a joint entry" #t (> (log-len if-a1) (log-len (cluster-get if1 'a))))
(check "a is mid-joint (pending)"         #t (and (conf-change-pending? if-a1) #t))
(define if-r2 (raft-propose-conf-change if-a1 '(a b c e) '()))                 ; #2 while pending
(check "change #2 refused: log unchanged" (log-len if-a1) (log-len (car if-r2)))
(check "change #2 refused: no outputs"    '() (cdr if-r2))
(check "change #2 refused: node untouched" #t (eq? (car if-r2) if-a1))

; ============================================================
(section "leader loss mid-joint-config: joint quorum re-elects + completes")
; ============================================================
; 5 voters {a,b,c,d,e} SHRINKING to {a,b,c}.  Get a leading, append the JOINT entry
; (Cold={a,b,c,d,e}, Cnew={a,b,c}) and deliver it to every follower so they ADOPT the
; joint config, but WITHHOLD their AERs so Cnew never commits — the whole cluster is
; frozen MID-JOINT.  Then LOSE the leader a (stop using it) and prove the joint quorum
;   (1) re-elects a new leader, needing an OLD *and* a NEW majority;
;   (2) lets that leader COMPLETE the transition to a stable Cnew; and
;   (3) leaves NO split-brain: the deposed a cannot commit a conflicting entry under
;       its now-stale term.
(define ll0 (cluster-make '(a b c d e) noop 0))
(define ll1 (cluster-campaign ll0 'a))
(check "a leads the initial {a,b,c,d,e}" 'leader (role-of ll1 'a))
; a appends the joint entry (Cold={a,b,c,d,e}, Cnew={a,b,c}) and broadcasts the AE.
(define ll-jr (raft-propose-conf-change (cluster-get ll1 'a) '(a b c) '()))  ; (a' . AEs)
(define ll2 (cluster-set ll1 'a (car ll-jr)))
; deliver the joint AE to every follower (each ADOPTS the joint config) but WITHHOLD
; their AERs, so a never commits Cnew / never appends the leave-joint entry — frozen.
(define ll3
  (let loop ((cc ll2) (outs (cdr ll-jr)))
    (if (null? outs) cc
        (let ((o (car outs)))
          (loop (car (deliver1 cc 'a (car o) (cdr o))) (cdr outs))))))
(check "a is mid-joint: old set retained {a,b,c,d,e}" #t (sset= (old-of ll3 'a) '(a b c d e)))
(check "a's incoming voter set = {a,b,c}"             #t (sset= (voters-of ll3 'a) '(a b c)))
(check "follower b adopted the joint config"          #t (sset= (old-of ll3 'b) '(a b c d e)))
(check "leaving node e is still a joint voter"        #t (is-voter? (cluster-get ll3 'e) 'e))

; ---- leader a is now LOST (gone). It holds the joint entry uncommitted. ----

; (1) ELECTION mid-joint: a candidate needs an OLD *and* a NEW majority. b campaigns;
;     a never votes (it's gone). new-side {b,c} is a majority of Cnew but NOT of Cold,
;     so b canNOT win on it; adding old-side {d,e} reaches both majorities -> b wins.
(define ll-b0 (car (raft-campaign (cluster-get ll3 'b))))
(check "b becomes a candidate (its term bumps)" 'candidate (raft-role ll-b0))
(define ll-b1 (car (on-rvr ll-b0 'c (list 'rvr (raft-term ll-b0) #t))))   ; + new-side c
(check "new-side {b,c} alone (a Cnew majority, NOT a Cold majority) does NOT elect"
       'candidate (raft-role ll-b1))
(define ll-b2 (car (on-rvr ll-b1 'd (list 'rvr (raft-term ll-b1) #t))))   ; + old-side d
(define ll-b3 (car (on-rvr ll-b2 'e (list 'rvr (raft-term ll-b2) #t))))   ; + old-side e
(check "old-side votes complete BOTH majorities -> b is elected mid-joint"
       'leader (raft-role ll-b3))
(check "the new leader b is itself in the incoming config {a,b,c}" #t (is-voter? ll-b3 'b))

; (2) COMPLETE the transition. b's own-term no-op barrier commits under the JOINT quorum,
;     which indirectly commits the joint entry beneath it (Raft 5.4.2); committing the
;     joint auto-appends the leave-joint Cnew, which then commits under the new SIMPLE
;     quorum -> a stable Cnew={a,b,c}, no pending change, b still leading.
(define n-noop (log-len ll-b3))                  ; index of b's no-op barrier
(check "b appended a no-op barrier in its OWN term" (raft-term ll-b3) (entry-term ll-b3 n-noop))
; a is gone: b(self)+c form the NEW majority {a,b,c}; b,c,d form the OLD majority {a,b,c,d,e}.
(define ll-m1 (aset ll-b3 'match (aset* (aget ll-b3 'match) (list 'c n-noop 'd n-noop))))
(define ll-c1 (maybe-commit ll-m1))
(check "b's own-term no-op commits -> joint entry commits beneath it (5.4.2)"
       #t (>= (aget ll-c1 'commit) n-noop))
(check "committing the joint auto-appended the leave-joint Cnew (b LEFT joint)"
       #f (aget ll-c1 'voters-old))
(check "b's voters are now Cnew {a,b,c}" #t (sset= (aget ll-c1 'voters) '(a b c)))
(define n-cnew (log-len ll-c1))                  ; the auto-appended leave-joint entry
(check "the leave-joint Cnew entry sits above the no-op" #t (> n-cnew n-noop))
; commit the Cnew under the new SIMPLE quorum {a,b,c} (maj 2): b(self)+c.
(define ll-m2 (aset ll-c1 'match (aset (aget ll-c1 'match) 'c n-cnew)))
(define ll-c2 (maybe-commit ll-m2))
(check "b commits Cnew -> the two-phase transition completes" #t (>= (aget ll-c2 'commit) n-cnew))
(check "b ends as LEADER of the new config" 'leader (raft-role ll-c2))
(check "b's config is a STABLE non-joint Cnew {a,b,c}"
       #t (and (sset= (aget ll-c2 'voters) '(a b c)) (not (aget ll-c2 'voters-old))))
(check "no conf-change pending after the transition completes" #f (conf-change-pending? ll-c2))

; (3) NO SPLIT-BRAIN. The deposed a still BELIEVES it leads the joint (its stale view).
;     It cannot commit a conflicting entry: with only its stale matchIndex it has no
;     fresh quorum, and the instant it contacts a peer on b's term the higher term
;     deposes it — a's term can never out-commit b's.
(define a-frozen (cluster-get ll3 'a))
(check "the deposed a still BELIEVES it leads (its stale joint view)" 'leader (raft-role a-frozen))
(check "a's term is now STALE — strictly behind the new leader b's"
       #t (< (raft-term a-frozen) (raft-term ll-c2)))
; a, isolated, proposes a NEW client entry: it would CONFLICT with b's no-op at the same index.
(define a-stale (car (raft-propose a-frozen (cmd "stale"))))
(check "a's conflicting entry lands at the SAME index b used for its no-op"
       n-noop (log-len a-stale))
; but a cannot commit it: its matchIndex holds no fresh acks (the freeze withheld them),
; so no joint quorum exists -> maybe-commit does NOT advance a's commit.
(check "a canNOT commit its conflicting entry (no fresh joint quorum under the stale term)"
       (aget a-stale 'commit) (aget (maybe-commit a-stale) 'commit))
; and the instant a contacts a peer that has moved to b's term, the higher term deposes it.
(define a-deposed (car (on-aer a-stale 'c (list 'aer (raft-term ll-c2) #f 0 0))))
(check "contacting a current-term peer deposes the stale leader a" 'follower (raft-role a-deposed))
(check "deposed a adopts the new term — it can never out-commit b"
       (raft-term ll-c2) (raft-term a-deposed))

(done!)
