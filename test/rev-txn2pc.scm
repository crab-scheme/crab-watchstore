; test/rev-txn2pc.scm — self-check for cw-kp0 Phase 4 core.
; Run from repo root:  crabscheme run test/rev-txn2pc.scm
(include "src/rev-allocator.scm")
(include "src/rev-watch-merge.scm")
(include "src/rev-txn2pc.scm")

(define fails 0)
(define (check name got want)
  (if (equal? got want)
      (begin (display "  ok   ") (display name) (newline))
      (begin (set! fails (+ fails 1))
             (display "  FAIL ") (display name)
             (display " got=") (write got) (display " want=") (write want) (newline))))

; ---- branch decision -------------------------------------------------------
(check "all compares true -> then" (txn-branch '(#t #t #t)) 'then)
(check "any compare false -> else" (txn-branch '(#t #f #t)) 'else)
(check "empty compares -> then (vacuous)" (txn-branch '()) 'then)

; ---- cross-shard txn atomicity via a multi-shard claim on one revision -------
; Txn at R=5 spans shards A,B,C. high=5. No single-shard batches in flight.
(let* ((high 5)
       (c0 (txn-claim 5 '(A B C))))
  (check "claim pins watermark at R-1 while any participant pending"
         (txn-watermark high (list c0) '()) 4)
  (let* ((c1 (claim-apply c0 'A))
         (c2 (claim-apply c1 'B)))
    (check "still pinned at 4 while C pending (no partial-txn visibility)"
           (txn-watermark high (list c2) '()) 4)
    (check "claim not yet resolved (C outstanding)" (claim-resolved? c2) #f)
    (let ((c3 (claim-apply c2 'C)))
      (check "claim resolved once all three applied" (claim-resolved? c3) #t)
      (check "watermark advances to high once txn fully committed"
             (txn-watermark high (list c3) '()) 5))))

; ---- a watcher must not release the txn's revision until atomically complete -
; Buffer the txn event at R=5; only release once the claim resolves.
(let* ((high 5)
       (c0 (txn-claim 5 '(A B)))
       ; pre-resolve: W=4, event@5 held
       (s1 (mc-step (mc-new) (list (cons 5 'txn)) high
                    ; express the open claim as an in-flight single-shard at lo=5
                    (list (shard-inflight (claim-rev c0)))))
       (rel1 (car s1)))
  (check "watcher holds txn event@5 while txn pending" (map car rel1) '())
  ; resolve the claim (all applied) -> W=high=5 -> event releases
  (let* ((s2 (mc-step (cdr s1) '() high '()))   ; no open claims now
         (rel2 (car s2)))
    (check "watcher releases txn event@5 only after atomic completion"
           (map car rel2) '(5))))

; ---- global compaction coordinator -----------------------------------------
(check "compact R=3 admissible when W=4 (R<=W)" (compact-admissible? 3 4) #t)
(check "compact R=6 REJECTED when W=4 (would discard pending history)"
       (compact-admissible? 6 4) #f)
(check "compact R=4 admissible at the watermark boundary" (compact-admissible? 4 4) #t)
(check "compaction complete only when EVERY shard acks"
       (compact-complete? '(A B) '(A B C)) #f)
(check "compaction complete when all shards ack"
       (compact-complete? '(A B C) '(A B C)) #t)

(newline)
(if (= fails 0)
    (begin (display "rev-txn2pc: ALL PASS — cross-shard txn is atomic at one ")
           (display "revision (no partial visibility) + safe global compaction") (newline))
    (begin (display "rev-txn2pc: ") (display fails) (display " FAILED") (newline) (exit 1)))
