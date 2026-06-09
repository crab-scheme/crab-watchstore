; raft-cq-prevote.scm — pure-engine tests for the CheckQuorum + PreVote additions
; to raft.scm (the read-side / churn fix for cc-idc). Run directly:
;   crabscheme run test/raft-cq-prevote.scm
(include "src/raft.scm")
(include "test/harness.scm")

(define (noop sm cmd) sm)
(define (tick-n st n win) (if (<= n 0) st (tick-n (raft-checkquorum st win) (- n 1) win)))

(section "CheckQuorum")
(define a0 (make-raft 'a '(a b c) noop 0))
(define aL (car (become-leader a0)))
(check "become-leader -> leader"      #t (raft-leader? aL))
(check "become-leader resets q-ticks" 0  (aget aL 'q-ticks))
(check "become-leader resets heard"   '() (aget aL 'heard))
; no contact for `window` ticks -> step down
(check "no-quorum -> follower"        'follower (raft-role (tick-n aL 4 4)))
; under the window -> still leader, q-ticks accrues
(check "within window stays leader"   #t (raft-leader? (tick-n aL 3 4)))
(check "within window q-ticks=3"      3  (aget (tick-n aL 3 4) 'q-ticks))
; heard a quorum peer (3-node majority=2; self + b) -> renew + stay leader
(define aHeard (aset aL 'heard '(b)))
(check "quorum heard stays leader"    #t (raft-leader? (tick-n aHeard 4 4)))
(check "renew resets q-ticks"         0  (aget (tick-n aHeard 4 4) 'q-ticks))
(check "renew resets heard"           '() (aget (tick-n aHeard 4 4) 'heard))
; on-aer (success OR rejection) records the peer for CheckQuorum
(check "on-aer success records heard" '(b)
       (memv 'b (aget (car (on-aer aL 'b (list 'aer (raft-term aL) #t 0))) 'heard)))
(check "on-aer reject records heard"  '(b)
       (memv 'b (aget (car (on-aer aL 'b (list 'aer (raft-term aL) #f 0))) 'heard)))
; solo (majority 1): self always counts -> never demotes
(define s0 (car (raft-campaign (make-raft 's '(s) noop 0))))
(check "solo is leader"               #t (raft-leader? s0))
(check "solo never demotes"           #t (raft-leader? (tick-n s0 20 4)))
; a non-leader is untouched
(define f0 (make-raft 'a '(a b c) noop 0))
(check "follower role untouched"      'follower (raft-role (raft-checkquorum f0 4)))
(check "follower q-ticks untouched"   0  (aget (raft-checkquorum f0 4) 'q-ticks))

(section "PreVote")
(define pv (raft-prevote f0))
(check "prevote does NOT bump term"   0 (raft-term (car pv)))
(check "prevote role pre-candidate"   'pre-candidate (raft-role (car pv)))
(check "prevote self-grant tallied"   '(a) (aget (car pv) 'pre-votes))
(check "prevote sends prv to peers"   2 (length (cdr pv)))
(check "prv message tag"              'prv (cadr (car (cdr pv))))
(check "prv carries next-term"        1 (caddr (car (cdr pv))))  ; (prv NEXT-TERM id idx term)

(section "ReadIndex round-ids")
; every AE the leader sends carries its rseq at index 7
(define ae-out (cdr (car (cdr (become-leader (aset a0 'rseq 7))))))
(check "AE has 8 fields"              8 (length ae-out))
(check "AE carries rseq at idx 7"     7 (list-ref ae-out 7))
; a follower echoes the AE's rseq in its AER (idx 4)
(define aer-out (cdr (car (cdr (on-ae (make-raft 'a '(a b c) noop 0)
                                      (list 'ae 1 'b 0 0 '() 0 9))))))
(check "AER has 5 fields"             5 (length aer-out))
(check "AER success at idx 2"         #t (list-ref aer-out 2))
(check "AER echoes rseq at idx 4"     9 (list-ref aer-out 4))
; a stale-term AE is rejected but still echoes rseq (so the old leader can match it)
(define rej (cdr (car (cdr (on-ae (aset (make-raft 'a '(a b c) noop 0) 'term 5)
                                  (list 'ae 1 'b 0 0 '() 0 3))))))
(check "reject AER echoes rseq"       3 (list-ref rej 4))
(check "reject AER not success"       #f (list-ref rej 2))

(done!)
