; test/rev-lease-consumer.scm — cw-kp0 phase 2b.3 writer-side lease core.
; Run from repo root:  crabscheme run test/rev-lease-consumer.scm
(include "src/rev-lease-consumer.scm")

(define fails 0)
(define (check name got want)
  (if (equal? got want)
      (begin (display "  ok   ") (display name) (newline))
      (begin (set! fails (+ fails 1))
             (display "  FAIL ") (display name)
             (display " got=") (write got) (display " want=") (write want) (newline))))

; drain helper: take K revs, return the list handed out (#f for empty)
(define (drain blocks k)
  (let loop ((b blocks) (k k) (out '()))
    (if (= k 0) (reverse out)
        (let ((r (lease-take b))) (loop (cdr r) (- k 1) (cons (car r) out))))))

; ---- empty lease ------------------------------------------------------------
(check "empty lease remaining 0" (lease-remaining (lease-new)) 0)
(check "take from empty -> #f" (car (lease-take (lease-new))) #f)

; ---- a single block is consumed front-to-back, then empty --------------------
(let ((L (lease-add (lease-new) 1 3)))            ; block [1,4): revs 1,2,3
  (check "remaining = 3" (lease-remaining L) 3)
  (check "drains 1,2,3 then #f" (drain L 4) '(1 2 3 #f)))

; ---- NON-CONTIGUOUS blocks (other writers interleaved) consumed in order -----
; this writer granted [1,4) then [10,12); the gap 4..9 belongs to other writers.
(let* ((L (lease-add (lease-add (lease-new) 1 3) 10 2)))   ; [1,4) ++ [10,12)
  (check "remaining across two blocks = 5" (lease-remaining L) 5)
  (check "drains front-first across the gap: 1,2,3,10,11" (drain L 6) '(1 2 3 10 11 #f)))

; ---- refill-before-empty: the deadlock-free buffer ---------------------------
; low=2 watermark: refill is requested while >low revs remain, so a write never
; blocks waiting on the authority round-trip.
(let* ((L0 (lease-add (lease-new) 1 4)))           ; [1,5): 1,2,3,4  (remaining 4)
  (check "fresh block does not need refill (4 > low 2)" (lease-needs-refill? L0 2) #f)
  (let* ((r1 (lease-take L0)) (L1 (cdr r1))        ; took 1 -> remaining 3
         (r2 (lease-take L1)) (L2 (cdr r2)))       ; took 2 -> remaining 2
    (check "took 1,2" (list (car r1) (car r2)) '(1 2))
    (check "now needs refill (remaining 2 <= low 2)" (lease-needs-refill? L2 2) #t)
    ; refill enqueues the next granted block [20,23) WHILE 3,4 still serve
    (let ((L3 (lease-add L2 20 3)))                 ; [3,5) ++ [20,23)
      (check "post-refill no longer needs refill" (lease-needs-refill? L3 2) #f)
      (check "continues 3,4 then jumps to 20,21,22" (drain L3 6) '(3 4 20 21 22 #f)))))

; ---- propose-time rewrite: PUT -> PUT-AT <leased rev> -----------------------
(define (b s) (string->utf8 s))
(define (str x) (utf8->string x))
; a PUT with a non-empty lease becomes PUT-AT carrying the next rev
(let* ((L (lease-add (lease-new) 7 2))               ; lease [7,9): revs 7,8
       (r (global-rev-rewrite (list (b "PUT") (b "k") (b "v")) L))
       (cmd (car r)) (L2 (cdr r)))
  (check "PUT rewritten to PUT-AT"        "PUT-AT" (str (list-ref cmd 0)))
  (check "PUT-AT carries the leased rev 7" "7"     (str (list-ref cmd 1)))
  (check "key preserved"                  "k"      (str (list-ref cmd 2)))
  (check "value preserved"                "v"      (str (list-ref cmd 3)))
  (check "lease advanced (rev 7 consumed)" 1       (lease-remaining L2)))
; an empty lease yields #f -> caller must refill + retry (never propose without a rev)
(let ((r (global-rev-rewrite (list (b "PUT") (b "k") (b "v")) (lease-new))))
  (check "empty lease -> #f cmd (refill+retry)" #f (car r)))
; a non-PUT command passes through unchanged
(let* ((del (list (b "DEL") (b "k")))
       (r (global-rev-rewrite del (lease-add (lease-new) 7 2))))
  (check "non-PUT passes through unchanged" "DEL" (str (list-ref (car r) 0))))

(newline)
(if (= fails 0)
    (begin (display "rev-lease-consumer: ALL PASS — writer draws global revs from a ")
           (display "block buffer, refills before empty (no actor-loop block)") (newline))
    (begin (display "rev-lease-consumer: ") (display fails) (display " FAILED") (newline) (exit 1)))
