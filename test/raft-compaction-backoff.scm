; raft-compaction-backoff.scm — regression guard for bug cw-u4a.39.
;
; The bug: on-aer's rejection backoff clamped nextIndex to (max 1 (- nx 1)) — a
; floor of 1, correct only when base=0. After compaction advances base>0, a peer's
; `next` could back off to <= base; the next append-for then called
;   (entries-from st nx) = (list-tail log (- nx base 1))
; with a NEGATIVE index -> "error in list-tail: negative index" -> shard crash.
;
; The fix: floor the backoff at base+1 (the earliest index the in-memory log can
; serve; entries <= base are compacted into the snapshot). This test constructs a
; leader with base>0 and a peer whose next <= base and asserts: no crash, next
; clamps to base+1, the emitted AppendEntries is well-formed (prev=base,
; prev-term=base-term, entries = the whole in-memory log), the floor holds under
; repeated rejections, AND the base=0 common case is unchanged.
;
;   crabscheme run test/raft-compaction-backoff.scm
(include "src/raft.scm")
(include "test/harness.scm")

(define (dummy sm cmd) sm)
(define (cmd s) (cons 2 (list (string->utf8 s))))   ; a term-2 log entry (term . command)

(section "base=0 common case: backoff unchanged (floor = 1)")
(let* ((st (make-raft 'a '(a b c) dummy 0))
       (st (aset* st (list 'term 2 'role 'leader
                           'log (list (cmd "x") (cmd "y") (cmd "z"))   ; entries 1..3
                           'next (list (cons 'b 3) (cons 'c 4))
                           'match (list (cons 'b 0) (cons 'c 3))
                           'commit 3 'applied 3)))
       (res (on-aer st 'b (list 'aer 2 #f 0))))     ; b rejects
  (check "base=0: b next 3 -> 2 (normal -1)" 2 (cdr (assq 'b (aget (car res) 'next)))))

(section "base=5, peer next <= base: NO crash, clamp to base+1 (the bug scenario)")
(let* ((st (make-raft 'a '(a b c) dummy 0))
       ; base=5 (entries 1..5 compacted into the snapshot), in-memory log = entries 6,7,8
       (st (aset* st (list 'base 5 'base-term 1 'term 2 'role 'leader
                           'log (list (cmd "x") (cmd "y") (cmd "z"))   ; entries 6..8
                           'next (list (cons 'b 3) (cons 'c 9))        ; b next=3 <= base=5 (trigger)
                           'match (list (cons 'b 0) (cons 'c 8))
                           'commit 8 'applied 8 'rseq 0)))
       (res (on-aer st 'b (list 'aer 2 #f 0)))       ; PRE-FIX: this line crashes in append-for
       (st2 (car res))
       (out (cdr res))                                ; ((b . ae-msg))
       (ae  (cdr (car out))))
  (check "no crash; b next clamped to base+1 = 6" 6 (cdr (assq 'b (aget st2 'next))))
  (check "emitted AE prev-idx = base (5)"        5 (list-ref ae 3))
  (check "emitted AE prev-term = base-term (1)"  1 (list-ref ae 4))
  (check "emitted AE carries the whole in-mem log (3 entries)" 3 (length (list-ref ae 5)))
  ; a second rejection must NOT push next below base+1 (the floor holds)
  (let* ((res2 (on-aer st2 'b (list 'aer 2 #f 0)))
         (st3  (car res2)))
    (check "second rejection: b next stays at base+1 = 6" 6 (cdr (assq 'b (aget st3 'next))))))

(section "sanity: a healthy entries-from at base+1 returns the full in-mem log")
(let* ((st (make-raft 'a '(a b c) dummy 0))
       (st (aset* st (list 'base 5 'base-term 1 'log (list (cmd "x") (cmd "y") (cmd "z"))))))
  (check "entries-from base+1 (6) = 3 entries" 3 (length (entries-from st 6)))
  (check "entries-from len+1 (9) = 0 entries"  0 (length (entries-from st 9))))

(done!)
