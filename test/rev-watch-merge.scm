; test/rev-watch-merge.scm — self-check for cw-kp0 Phase 3 watch-merge core.
; Run from repo root:  crabscheme run test/rev-watch-merge.scm
(include "src/rev-allocator.scm")
(include "src/rev-watch-merge.scm")

(define fails 0)
(define (check name got want)
  (if (equal? got want)
      (begin (display "  ok   ") (display name) (newline))
      (begin (set! fails (+ fails 1))
             (display "  FAIL ") (display name)
             (display " got=") (write got) (display " want=") (write want) (newline))))

; 3 shards A,B,C. Grants happen in order (authority serializes); commits arrive
; interleaved and OUT of wall-clock order. We drive mc-step round by round and
; concatenate everything released; it must come out in strict global revision
; order 1..N with nothing missing, and nothing released while a lower in-flight
; block is still pending.

; grants: A[1,4) B[4,6) C[6,9)  -> high=8
(define gA (rev-grant 0 3))      ; lo 1 high 3
(define gB (rev-grant (cdr gA) 2)) ; lo 4 high 5
(define gC (rev-grant (cdr gB) 3)) ; lo 6 high 8
(define HIGH (cdr gC))
(check "authority high after 3 grants" HIGH 8)

(define (evs lo . payloads)        ; build (rev . payload) events from lo upward
  (let loop ((r lo) (ps payloads) (out '()))
    (if (null? ps) (reverse out)
        (loop (+ r 1) (cdr ps) (cons (cons r (car ps)) out)))))

; Round 1: C commits first (events 6,7,8) while A and B still in-flight.
;   states: A inflight@1, B inflight@4, C idle  -> W = min(1-1,4-1)=0 -> release nothing
(define r1 (mc-step (mc-new) (evs 6 'c1 'c2 'c3) HIGH
                    (list (shard-inflight 1) (shard-inflight 4) (shard-idle))))
(check "round1 (C raced ahead): nothing released" (map car (car r1)) '())

; Round 2: B commits (events 4,5). A still in-flight@1.
;   states: A inflight@1, B idle, C idle -> W = 0 -> still nothing (A blocks)
(define r2 (mc-step (cdr r1) (evs 4 'b1 'b2) HIGH
                    (list (shard-inflight 1) (shard-idle) (shard-idle))))
(check "round2 (B commits, A still pending): nothing released" (map car (car r2)) '())

; Round 3: A commits (events 1,2,3). All idle -> W = high = 8 -> full drain in order.
(define r3 (mc-step (cdr r2) (evs 1 'a1 'a2 'a3) HIGH
                    (list (shard-idle) (shard-idle) (shard-idle))))
(check "round3 drains in strict global order 1..8"
       (map car (car r3)) '(1 2 3 4 5 6 7 8))
(check "round3 payloads follow revision order, not arrival order"
       (map cdr (car r3)) '(a1 a2 a3 b1 b2 c1 c2 c3))
(check "buffer empty after drain" (mc-buffer (cdr r3)) '())
(check "delivered-hi advanced to 8" (mc-delivered-hi (cdr r3)) 8)

; ---- a second wave proves monotonic continuation across mc reuse -------------
; new grants D[9,11) E[11,12); E commits before D.
(define gD (rev-grant HIGH 2))   ; lo 9  high 10
(define gE (rev-grant (cdr gD) 1)) ; lo 11 high 11
(define HIGH2 (cdr gE))
(define mc3 (cdr r3))
; E commits first; D in-flight@9 -> W = 9-1 = 8 -> nothing new (8 already delivered)
(define w1 (mc-step mc3 (evs 11 'e1) HIGH2 (list (shard-inflight 9) (shard-idle))))
(check "wave2: E early, D pending -> nothing released" (map car (car w1)) '())
; D commits; all idle -> W = 11 -> release 9,10,11 in order
(define w2 (mc-step (cdr w1) (evs 9 'd1 'd2) HIGH2 (list (shard-idle) (shard-idle))))
(check "wave2 drains 9,10,11 in order" (map car (car w2)) '(9 10 11))
(check "wave2 payloads in order" (map cdr (car w2)) '(d1 d2 e1))
(check "delivered-hi now 11" (mc-delivered-hi (cdr w2)) 11)

; ---- partial-watermark release within a single step ------------------------
; one shard idle (its events ready) + one in-flight gating partway
(define p1 (mc-step (mc-new)
                    (list (cons 1 'x) (cons 2 'y) (cons 5 'z))
                    5 (list (shard-idle) (shard-inflight 3))))  ; W = 3-1 = 2
(check "partial: only 1,2 released (5 held behind W=2)" (map car (car p1)) '(1 2))
(check "partial: 5 stays buffered" (map car (mc-buffer (cdr p1))) '(5))

(newline)
(if (= fails 0)
    (begin (display "rev-watch-merge: ALL PASS — cross-shard watch delivers in strict ")
           (display "global revision order under interleaved out-of-order commits") (newline))
    (begin (display "rev-watch-merge: ") (display fails) (display " FAILED") (newline) (exit 1)))
