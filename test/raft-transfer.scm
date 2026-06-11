; raft-transfer.scm — pure-engine tests for LEADERSHIP TRANSFER (cw-u4a.42,
; Ongaro thesis §3.10 / etcd MoveLeader).  Drives the in-Scheme cluster simulator
; deterministically to quiescence — no actors, transport, or clocks.
;   crabscheme run test/raft-transfer.scm
;
; Coverage:
;   * refusals     : transfer is refused (no message sent) when we don't lead, the
;                    target is ourselves, the target is not a voter, or the target
;                    is not fully caught up (it would lose the election).
;   * success      : 'timeout-now to a caught-up follower makes it campaign + win;
;                    the old leader steps down; the new leader still commits.
;   * on-timeout-now: a follower receiving it campaigns immediately (term bumps,
;                    request-votes emitted) — skipping its timeout + PreVote.

(include "src/raft.scm")
(include "test/harness.scm")

(define (noop sm cmd) sm)
(define (cmd s) (list (string->utf8 s)))
(define (role-of c id)   (raft-role (cluster-get c id)))
(define (commit-of c id) (aget (cluster-get c id) 'commit))
(define (loglen-of c id) (log-len (cluster-get c id)))
(define (match-of c id peer)
  (let ((m (assv peer (aget (cluster-get c id) 'match)))) (and m (cdr m))))

; Drive a transfer at leader `id` -> target.  Returns (cons c' verdict): on 'ok the
; cluster is settled (the 'timeout-now and the election it begets are delivered);
; on refusal the cluster is unchanged and verdict is the reason symbol.
(define (do-transfer c id target)
  (let ((r (raft-transfer-leadership (cluster-get c id) target)))
    (if (eq? (car r) 'ok)
        (cons (cluster-settle c (map (lambda (o) (list id (car o) (cdr o))) (cadr r))) 'ok)
        (cons c (cadr r)))))

; ============================================================
(section "setup: a leads {a,b,c}, all voters caught up")
; ============================================================
(define c0 (cluster-campaign (cluster-make '(a b c) noop 0) 'a))
(define c1 (cluster-tick (cluster-propose c0 'a (cmd "x")) 'a))
(check "a leads"      'leader (role-of c1 'a))
(check "b caught up"  #t (= (match-of c1 'a 'b) (loglen-of c1 'a)))
(check "c caught up"  #t (= (match-of c1 'a 'c) (loglen-of c1 'a)))

; ============================================================
(section "refusals (pure — no 'timeout-now emitted)")
; ============================================================
(check "to self        -> 'self"        'self        (cdr (do-transfer c1 'a 'a)))
(check "to a non-voter -> 'not-voter"   'not-voter   (cdr (do-transfer c1 'a 'z)))
(check "from a follower -> 'not-leader" 'not-leader  (cdr (do-transfer c1 'b 'a)))

; not-caught-up: grow a's log WITHOUT settling, so b's match lags a's last index.
(define c-lag (cluster-set c1 'a (car (raft-propose (cluster-get c1 'a) (cmd "y")))))
(check "a's last index now exceeds b's match"
       #t (> (loglen-of c-lag 'a) (match-of c-lag 'a 'b)))
(check "lagging target -> 'not-caught-up"
       'not-caught-up (cdr (do-transfer c-lag 'a 'b)))

; ============================================================
(section "transfer a -> b: b wins, a steps down, cluster still commits")
; ============================================================
(define a-term0 (raft-term (cluster-get c1 'a)))
(define t  (do-transfer c1 'a 'b))
(check "transfer accepted (caught-up voter)" 'ok (cdr t))
(define c2 (car t))
(check "b is now the leader"        'leader (role-of c2 'b))
(check "a is no longer leader"      #f (eq? (role-of c2 'a) 'leader))
(check "b's term exceeds a's old"   #t (> (raft-term (cluster-get c2 'b)) a-term0))
; the new leader commits a fresh write and the old leader converges under it.
(define c2-commit (commit-of c2 'b))
(define c3 (cluster-tick (cluster-propose c2 'b (cmd "z")) 'b))
(check "new leader b commits a write" #t (> (commit-of c3 'b) c2-commit))
(check "old leader a converges to b"  #t (= (commit-of c3 'a) (commit-of c3 'b)))

; ============================================================
(section "on-timeout-now: a follower campaigns immediately")
; ============================================================
(define fb (cluster-get c1 'b))
(check "b starts as a follower" 'follower (raft-role fb))
(define tn (on-timeout-now fb (list 'timeout-now (raft-term fb))))
(check "b became a candidate (term bumped)" #t (> (raft-term (car tn)) (raft-term fb)))
(check "b emitted request-votes"            #t (pair? (cdr tn)))
; a node that is already leader ignores 'timeout-now (no spurious re-election).
(define tn-leader (on-timeout-now (cluster-get c1 'a) (list 'timeout-now 99)))
(check "a leader ignores 'timeout-now" 'leader (raft-role (car tn-leader)))
(check "leader emits nothing on 'timeout-now" #t (null? (cdr tn-leader)))

(done!)
