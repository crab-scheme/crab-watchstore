; test/rev-lease.scm — self-check for cw-kp0 Phase 2 core (grant-lease + reap).
; Run from repo root:  crabscheme run test/rev-lease.scm
(include "src/rev-allocator.scm")
(include "src/rev-watch-merge.scm")
(include "src/rev-lease.scm")

(define fails 0)
(define (check name got want)
  (if (equal? got want)
      (begin (display "  ok   ") (display name) (newline))
      (begin (set! fails (+ fails 1))
             (display "  FAIL ") (display name)
             (display " got=") (write got) (display " want=") (write want) (newline))))

; ---- grant/commit happy path ------------------------------------------------
(let* ((a0 (auth-new))
       (gA (auth-grant a0 'A 3 0 10))   ; A: revs 1..3, expiry 10
       (a1 (cdr gA))
       (gB (auth-grant a1 'B 2 0 100))  ; B: revs 4..5, expiry 100
       (a2 (cdr gB)))
  (check "A block" (car gA) (cons 1 3))
  (check "B block (contiguous)" (car gB) (cons 4 5))
  (check "high after two grants" (auth-high a2) 5)
  (check "watermark gated at min live lo-1 (A@1)" (auth-watermark a2) 0)
  ; B commits; A still in-flight -> W still gated at 0 by A
  (let ((a3 (auth-commit a2 'B)))
    (check "after B commit, A still pins W=0" (auth-watermark a3) 0)
    ; A commits too -> no live leases -> W = high = 5
    (let ((a4 (auth-commit a3 'A)))
      (check "both committed -> W = high = 5" (auth-watermark a4) 5)
      (check "no leases left" (auth-leases a4) '()))))

; ---- failure path: a shard dies holding a grant; reap un-freezes the watermark
(let* ((a0 (auth-new))
       (gA (auth-grant a0 'A 3 0 10))    ; A: 1..3, expiry 10  (A will DIE)
       (a1 (cdr gA))
       (gB (auth-grant a1 'B 2 0 100))   ; B: 4..5, expiry 100
       (a2 (cdr gB))
       (a3 (auth-commit a2 'B)))         ; B commits (events 4,5 exist)
  ; A never commits. Before reap, A's lease freezes W at 0 forever:
  (check "pre-reap: dead A freezes W at 0" (auth-watermark a3) 0)
  ; reap at now=20 (> A's expiry 10): void A's block, drop its lease
  (let* ((r (auth-reap a3 20))
         (voids (car r))
         (a4 (cdr r)))
    (check "reap voids A's abandoned block [1,3]" voids (list (cons 1 3)))
    (check "reap drops the dead lease" (auth-leases a4) '())
    (check "post-reap: watermark recovers to high=5" (auth-watermark a4) 5)
    ; end-to-end with the Phase 3 merge: B's events 4,5 release; voided 1..3 are
    ; simply empty (no events) — watcher proceeds in order with no freeze.
    (let* ((mc (mc-step (mc-new) (list (cons 4 'b1) (cons 5 'b2))
                        (auth-high a4) (list)))   ; no live leases -> W=high=5
           (released (car mc)))
      (check "watcher releases 4,5 across the void; no stall"
             (map car released) '(4 5))
      (check "voided revs 1..3 carry no events (correctly skipped)"
             (filter (lambda (r) (<= r 3)) (map car released)) '()))))

; ---- reap is selective: a still-live lease is NOT reaped ---------------------
(let* ((a0 (auth-new))
       (a1 (cdr (auth-grant a0 'A 2 0 5)))    ; expiry 5
       (a2 (cdr (auth-grant a1 'B 2 0 100)))  ; expiry 100 (healthy)
       (r  (auth-reap a2 10)))                ; now=10: A expired, B alive
  (check "reap voids only the expired lease (A)" (car r) (list (cons 1 2)))
  (check "live lease B survives reap, pins W at its lo-1 (3-1=2)" (auth-watermark (cdr r)) 2))

(newline)
(if (= fails 0)
    (begin (display "rev-lease: ALL PASS — grant/commit + a dead shard's grant is ")
           (display "reaped and voided so the watermark never freezes") (newline))
    (begin (display "rev-lease: ") (display fails) (display " FAILED") (newline) (exit 1)))
