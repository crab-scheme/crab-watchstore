; test/rev-allocator.scm — self-check for the cw-kp0 global revision allocator +
; low-watermark keystone. Pure algorithm test (no Raft/IO). Run from repo root:
;   crabscheme run test/rev-allocator.scm
(include "src/rev-allocator.scm")

(define fails 0)
(define (check name got want)
  (if (equal? got want)
      (begin (display "  ok   ") (display name) (newline))
      (begin (set! fails (+ fails 1))
             (display "  FAIL ") (display name)
             (display " got=") (write got) (display " want=") (write want) (newline))))

; ---- 1. grants are monotonic, contiguous, non-overlapping ------------------
(let* ((g1 (rev-grant 0 3))      ; [1,4)  -> lo 1, high 3
       (g2 (rev-grant (cdr g1) 2))  ; [4,6)  -> lo 4, high 5
       (g3 (rev-grant (cdr g2) 1))) ; [6,7)  -> lo 6, high 6
  (check "grant1 lo" (car g1) 1)
  (check "grant1 high" (cdr g1) 3)
  (check "grant2 lo (contiguous, no overlap)" (car g2) 4)
  (check "grant3 lo" (car g3) 6)
  (check "grant3 high" (cdr g3) 6))
(check "empty grant does not advance high" (rev-grant 5 0) (cons 6 5))

; ---- 2. watermark: idle shards do NOT freeze it ----------------------------
; all idle => everything up to `high` is releasable
(check "all-idle watermark = high"
       (global-watermark 5 (list (shard-idle) (shard-idle))) 5)
; an idle shard whose last write was rev 2 must NOT pin W at 2 while high=5
(check "idle shard does not freeze watermark below high"
       (global-watermark 5 (list (shard-idle) (shard-idle) (shard-idle))) 5)
; one in-flight shard at lo=4 gates W at 3 regardless of high
(check "single in-flight gates at lo-1"
       (global-watermark 5 (list (shard-idle) (shard-inflight 4))) 3)
; two in-flight: the LOWEST lo binds
(check "two in-flight: lowest lo binds"
       (global-watermark 9 (list (shard-inflight 7) (shard-inflight 4) (shard-idle))) 3)

; ---- 3. the keystone: out-of-order shard commits still deliver in rev order -
; Scenario: A granted [1,4) (inflight@1), B granted [4,6) (inflight@4). B commits
; FIRST (events at rev 4,5) while A is still pending. A cross-shard prefix watcher
; must NOT release 4,5 yet (rev 1..3 from A are lower and not yet real). Then A
; commits (events 1,2,3); now everything releases in strict order 1..5.
(let* ((high 5)
       (states-both-inflight (list (shard-inflight 1) (shard-inflight 4)))
       (buf0 '())
       ; B commits first: buffer its events, compute watermark (A still inflight@1)
       (buf1 (wm-add-events buf0 (list (cons 4 'b1) (cons 5 'b2))))
       (w-after-b (global-watermark high states-both-inflight))
       (rel-b (wm-release buf1 w-after-b)))
  (check "watermark still 0 while A inflight@1 (B raced ahead)" w-after-b 0)
  (check "B's events 4,5 are BUFFERED, not released out of order" (car rel-b) '())
  ; A now commits; only B inflight? no — both done => all idle => W = high = 5
  (let* ((states-after-a (list (shard-idle) (shard-idle)))
         (buf2 (wm-add-events (cdr rel-b) (list (cons 1 'a1) (cons 2 'a2) (cons 3 'a3))))
         (w-final (global-watermark high states-after-a))
         (rel-final (wm-release buf2 w-final)))
    (check "watermark = high once all batches settled" w-final 5)
    (check "released strictly in revision order 1..5"
           (map car (car rel-final)) '(1 2 3 4 5))
    (check "released payloads in matching order"
           (map cdr (car rel-final)) '(a1 a2 a3 b1 b2))
    (check "nothing left buffered" (cdr rel-final) '())))

; ---- 4. partial release: watermark mid-stream releases only the safe prefix --
(let* ((buf (wm-add-events '() (list (cons 1 'x) (cons 2 'y) (cons 7 'z))))
       (rel (wm-release buf 2)))          ; W=2 => release 1,2; hold 7
  (check "partial release respects watermark (≤2 out, 7 held)"
         (list (map car (car rel)) (map car (cdr rel))) '((1 2) (7))))

(newline)
(if (= fails 0)
    (begin (display "rev-allocator: ALL PASS — globally-monotonic revisions + ")
           (display "in-order cross-shard delivery under out-of-order commits") (newline))
    (begin (display "rev-allocator: ") (display fails) (display " FAILED") (newline)
           (exit 1)))
