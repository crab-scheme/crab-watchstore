; rev-lease-consumer.scm — cw-kp0 phase 2b.3 core (raft.scm-free): the WRITER-side
; revision lease. The leader of a writer group draws one global revision per client
; write from a buffer of granted blocks, and refills the buffer (a fresh REV-GRANT
; from the authority) BEFORE it empties — so a write never blocks the actor loop
; waiting for a grant (the deadlock hazard). Builds on the authority side (2a/2b.2).
;
; Successive grants to ONE writer are NOT contiguous — other writers interleave
; grants from the same authority, so this writer's blocks have gaps between them
; (e.g. [1,4) then [10,13)). Hence a QUEUE of blocks, not a single [next,hi) range:
; consume from the front, enqueue refills at the back, skip exhausted front blocks.
;
; PURE / threaded-state (no Raft/IO): the live wiring (request a refill via the
; authority client-proposal path when lease-needs-refill?, stamp PUT-AT <rev> at
; propose) is the next increment.

; lease = list of blocks (cons lo hi)  [half-open: revs lo .. hi-1], FIFO, front first
(define (lease-new) '())

; total revisions still available across all queued blocks
(define (lease-remaining blocks)
  (let loop ((bs blocks) (n 0))
    (if (null? bs) n (loop (cdr bs) (+ n (max 0 (- (cdar bs) (caar bs))))))))

; (lease-take blocks) -> (cons rev blocks') ; rev = #f (and blocks' = lease-new) when empty.
; Drops fully-consumed front blocks, then hands out the front block's low rev.
(define (lease-take blocks)
  (cond ((null? blocks) (cons #f '()))
        ((>= (caar blocks) (cdar blocks)) (lease-take (cdr blocks)))   ; front exhausted: skip
        (else (let ((rev (caar blocks)))
                (cons rev (cons (cons (+ rev 1) (cdar blocks)) (cdr blocks)))))))

; enqueue a freshly-granted block [lo, lo+n) at the back of the buffer
(define (lease-add blocks lo n)
  (if (<= n 0) blocks (append blocks (list (cons lo (+ lo n))))))

; refill when the remaining buffer drops to `low` or below — request the next grant
; while there are still revs to serve, so the request/reply round overlaps real work.
(define (lease-needs-refill? blocks low) (<= (lease-remaining blocks) low))

; ---------------------------------------------------------------------------
; propose-time rewrite: the exact transform the writer-group leader applies in
; global-rev mode before proposing a client write — PUT becomes PUT-AT carrying the
; next leased global revision, so all replicas apply that rev deterministically
; (2b.4). Pure so the propose-path hook is a one-liner over proven logic.
; cmd = ("PUT" K V [lease]) as bytevectors. Returns (cons cmd' lease'):
;   cmd' = ("PUT-AT" revStr K V [lease])         when a rev was available
;   cmd' = #f (lease unchanged)                  when the lease is EMPTY — the caller
;          must refill (request a REV-GRANT) and retry; never propose without a rev.
; A non-PUT cmd passes through unchanged (DEL/TXN global-rev handling is a later phase).
; ---------------------------------------------------------------------------
(define (global-rev-rewrite cmd lease)
  (if (and (pair? cmd) (string=? (utf8->string (car cmd)) "PUT"))
      (let ((t (lease-take lease)))
        (if (car t)
            (cons (cons (string->utf8 "PUT-AT")
                        (cons (string->utf8 (number->string (car t))) (cdr cmd)))
                  (cdr t))
            (cons #f lease)))
      (cons cmd lease)))
