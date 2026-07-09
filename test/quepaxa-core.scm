; quepaxa-core.scm — deterministic tests for the QuePaxa engine (cw-e9x/cw-979/cw-pk0).
;   crabscheme run test/quepaxa-core.scm
; Covers: solo decide; 3-node coordinator 1-RTT fast path; agreement; hedged
; propose via coordinator; DEAD-coordinator liveness (the QuePaxa selling
; point: no election, randomized rounds still decide); contention + loser
; retry; bid-dedup exactly-once under dual (hedged+forwarded) proposal; gap
; fill; chaos soak (drops/reorder/dups) under a seeded PRNG.
(include "test/harness.scm")
(include "src/quepaxa.scm")

(define (test-apply sm cmd) (append sm (list cmd)))

(define (qcl-make ids opts)
  (map (lambda (id) (cons id (make-qp id ids test-apply '() opts))) ids))
(define (qcl-get c id) (cdr (assv id c)))
(define (qcl-set c id st)
  (cond ((null? c) (list (cons id st)))
        ((eqv? (caar c) id) (cons (cons id st) (cdr c)))
        (else (cons (car c) (qcl-set (cdr c) id st)))))

; deliver queue entries (from to msg) in order until quiescent.
; `blocked` = node ids that neither send nor receive (crashed).
(define (qsettle c queue blocked)
  (if (null? queue) c
      (let* ((m (car queue)) (from (car m)) (to (cadr m)) (msg (caddr m)))
        (if (or (memv from blocked) (memv to blocked))
            (qsettle c (cdr queue) blocked)
            (let* ((r (qp-step (qcl-get c to) from msg))
                   (c2 (qcl-set c to (car r)))
                   (more (map (lambda (o) (list to (car o) (cdr o))) (cdr r))))
              (qsettle c2 (append (cdr queue) more) blocked))))))

(define (qdrive c id action blocked)
  (let* ((r (action (qcl-get c id)))
         (c2 (qcl-set c id (car r))))
    (qsettle c2 (map (lambda (o) (list id (car o) (cdr o))) (cdr r)) blocked)))

(define (qtick-all c blocked)
  (let loop ((ids (map car c)) (c c) (q '()))
    (if (null? ids)
        (qsettle c q blocked)
        (if (memv (car ids) blocked)
            (loop (cdr ids) c q)
            (let ((r (qp-tick (qcl-get c (car ids)))))
              (loop (cdr ids)
                    (qcl-set c (car ids) (car r))
                    (append q (map (lambda (o) (list (car ids) (car o) (cdr o))) (cdr r)))))))))

; all live nodes: identical sm up to the shorter applied prefix + decided agree
(define (agree? c blocked)
  (let ((live (let loop ((l c) (acc '()))
                (cond ((null? l) (reverse acc))
                      ((memv (caar l) blocked) (loop (cdr l) acc))
                      (else (loop (cdr l) (cons (cdar l) acc)))))))
    (let loop ((l (cdr live)) (ok #t))
      (if (or (null? l) (not ok)) ok
          (let* ((a (qp-sm (car live))) (b (qp-sm (car l)))
                 (n (min (length a) (length b))))
            (loop (cdr l) (equal? (qp-take-n a n) (qp-take-n b n))))))))

; ============================================================
(section "solo node: propose decides + applies immediately")
(let* ((st (make-qp 1 '(1) test-apply '() '()))
       (r (qp-propose st 'a)))
  (check "applied=1" 1 (qp-applied (car r)))
  (check "sm" '(a) (qp-sm (car r)))
  (check "no outputs" '() (cdr r)))

; ============================================================
(section "3-node coordinator fast path is 1 RTT")
(let* ((c (qcl-make '(1 2 3) '()))
       (r (qp-propose (qcl-get c 1) 'x))        ; 1 is coord
       (c (qcl-set c 1 (car r)))
       (outs (cdr r)))
  (check "broadcasts esp to both peers" 2 (length outs))
  (check "not yet decided" 0 (qp-applied (qcl-get c 1)))
  ; deliver ONE esp to node 2, return its reply to 1: self + 2 = majority
  (let* ((o (assv 2 outs))
         (r2 (qp-step (qcl-get c 2) 1 (cdr o)))
         (c (qcl-set c 2 (car r2)))
         (reply (car (cdr r2)))
         (r3 (qp-step (qcl-get c 1) 2 (cdr reply)))
         (c (qcl-set c 1 (car r3))))
    (check "decided after ONE reply (1 RTT)" 1 (qp-applied (qcl-get c 1)))
    (check "value applied" '(x) (qp-sm (qcl-get c 1)))
    ; settle the rest: everyone converges
    (let ((c (qsettle c (append (map (lambda (o) (list 1 (car o) (cdr o))) (cdr r3))
                                (list (list 1 3 (cdr (assv 3 outs)))))
                      '())))
      (check "all applied" '(1 1 1) (map (lambda (e) (qp-applied (cdr e))) c))
      (check "agreement" #t (agree? c '())))))

; ============================================================
(section "hedged propose at a follower: forwarded to coordinator, applied everywhere")
(let* ((c (qcl-make '(1 2 3) '()))
       (c (qdrive c 2 (lambda (st) (qp-propose st 'y)) '())))
  (check "applied via coordinator" '(1 1 1) (map (lambda (e) (qp-applied (cdr e))) c))
  (check "value applied" '(y) (qp-sm (qcl-get c 3)))
  (check "agreement" #t (agree? c '())))

; ============================================================
(section "dead coordinator: hedge fires, randomized rounds decide (no election)")
(let* ((c (qcl-make '(1 2 3) '((hedge . 2))))
       (dead '(1))
       (c (qdrive c 2 (lambda (st) (qp-propose st 'z)) dead))  ; pfwd swallowed
       (c (qtick-all c dead))    ; hedge 2 -> 1
       (c (qtick-all c dead))    ; hedge fires: self-propose, randomized rounds
       (c (qtick-all c dead))    ; retransmits drive any remaining phases
       (c (qtick-all c dead)))
  (check "node2 applied without coordinator" #t (>= (qp-applied (qcl-get c 2)) 1))
  (check "node3 applied without coordinator" #t (>= (qp-applied (qcl-get c 3)) 1))
  (check "z survived" #t (equal? '(z) (qp-sm (qcl-get c 2))))
  (check "agreement" #t (agree? c dead)))

; ============================================================
(section "contention: two concurrent proposers, loser retries, both apply once")
(let* ((c (qcl-make '(1 2 3) '((hedge . 0))))   ; hedge 0: everyone self-proposes
       ; start both proposals WITHOUT settling (concurrent), then interleave
       (r1 (qp-propose (qcl-get c 1) 'a))
       (c (qcl-set c 1 (car r1)))
       (r2 (qp-propose (qcl-get c 2) 'b))
       (c (qcl-set c 2 (car r2)))
       (q (append (map (lambda (o) (list 1 (car o) (cdr o))) (cdr r1))
                  (map (lambda (o) (list 2 (car o) (cdr o))) (cdr r2))))
       (c (qsettle c q '()))
       ; ticks flush any retry slots
       (c (qtick-all c '())) (c (qtick-all c '())) (c (qtick-all c '())))
  (let ((sm (qp-sm (qcl-get c 3))))
    (check "both commands applied exactly once (node3)" #t
           (and (= 2 (length sm)) (memv 'a sm) (memv 'b sm) #t))
    (check "agreement" #t (agree? c '()))
    (check "all reached same applied" #t
           (= (qp-applied (qcl-get c 1)) (qp-applied (qcl-get c 2))
              (qp-applied (qcl-get c 3))))))

; ============================================================
(section "bid dedup: hedged copy AND forwarded copy both decide -> applied once")
(let* ((c (qcl-make '(1 2 3) '((hedge . 1))))
       ; propose at 2: pfwd emitted to coord 1 but NOT delivered yet
       (r (qp-propose (qcl-get c 2) 'w))
       (c (qcl-set c 2 (car r)))
       (pfwd-out (car (cdr r)))
       ; hedge fires first: 2 self-proposes and settles
       (c (qtick-all c '()))
       ; NOW deliver the stale pfwd: coordinator proposes the same bid again
       (c (qsettle c (list (list 2 1 (cdr pfwd-out))) '()))
       (c (qtick-all c '())) (c (qtick-all c '())))
  (check "w applied exactly once at node3" '(w) (qp-sm (qcl-get c 3)))
  (check "agreement" #t (agree? c '())))

; ============================================================
(section "gap fill: crashed proposer's stuck slot gets no-op'd, log advances")
(let* ((c (qcl-make '(1 2 3) '((hedge . 0))))
       ; slot 1 decided normally
       (c (qdrive c 1 (lambda (st) (qp-propose st 'p)) '()))
       ; node 2 starts slot 2 but its messages reach ONLY node 3's recorder,
       ; then node 2 "crashes" (blocked from here on)
       (r (qp-propose (qcl-get c 2) 'q))
       (c (qcl-set c 2 (car r)))
       (c (qsettle c (let loop ((o (cdr r)) (acc '()))
                       (cond ((null? o) (reverse acc))
                             ((eqv? 3 (caar o)) (loop (cdr o) (cons (list 2 3 (cdar o)) acc)))
                             (else (loop (cdr o) acc))))
                   '(2)))       ; 3 records slot 2; its reply back to 2 is dropped
       (dead '(2))
       ; node 1 proposes at slot 3 (its next-slot is 2... drive ticks so node 1
       ; learns slot 2 is taken via node 3? Simpler: propose from 3 whose
       ; next-slot advanced to 3 after seeing 2's esp)
       (c (qdrive c 3 (lambda (st) (qp-propose st 'r)) dead)))
  ; now: slot1 decided, slot2 stuck (proposer dead), slot3 decided at coord+3
  (let loop ((i 0) (c c))
    (if (< i 10)
        (loop (+ i 1) (qtick-all c dead))
        (begin
          (check "log advanced past the stuck slot (node1)" #t
                 (>= (qp-applied (qcl-get c 1)) 2)
          )
          (check "node1/node3 agree" #t (agree? c dead))
          ; q may or may not have survived (its proposer died mid-flight);
          ; p and r MUST both be applied
          (let ((sm (qp-sm (qcl-get c 3))))
            (check "p survived" #t (and (memv 'p sm) #t))
            (check "r survived" #t (and (memv 'r sm) #t)))))))

; ============================================================
(section "chaos soak: drops + reorder + dup, seeded; agreement + exactly-once")
(define (chaos-run seed)
  ; lossy delivery: each queued message 20% dropped, 10% duplicated, delivery
  ; order randomized; a tick round every 6 deliveries. After the chaos budget,
  ; ticks + clean settle to quiescence.
  (let loop ((c (qcl-make '(1 2 3) '((hedge . 2) (seed . 1))))
             (q (let* ((c0 (qcl-make '(1 2 3) '())) (dummy #f)) '()))
             (rng seed) (steps 0)
             (pending '((1 a1) (2 b1) (3 c1) (1 a2) (2 b2))))
    (cond
      ; inject proposals over time
      ((and (pair? pending) (= 0 (modulo steps 7)))
       (let* ((pr (car pending))
              (r (qp-propose-batch (qcl-get c (car pr)) (cdr pr)))
              (c2 (qcl-set c (car pr) (car r))))
         (loop c2 (append q (map (lambda (o) (list (car pr) (car o) (cdr o))) (cdr r)))
               rng (+ steps 1) (cdr pending))))
      ((> steps 400)   ; chaos budget exhausted: drain cleanly
       (let drain ((c c) (q q) (i 0))
         (let ((c (qsettle c q '())))
           (if (< i 12)
               (let tick ((ids '(1 2 3)) (c c) (tq '()))
                 (if (null? ids)
                     (drain c tq (+ i 1))
                     (let ((r (qp-tick (qcl-get c (car ids)))))
                       (tick (cdr ids) (qcl-set c (car ids) (car r))
                             (append tq (map (lambda (o) (list (car ids) (car o) (cdr o)))
                                             (cdr r)))))))
               c))))
      ((null? q)       ; quiescent mid-chaos: tick to regenerate traffic
       (let tick ((ids '(1 2 3)) (c c) (tq '()))
         (if (null? ids)
             (loop c tq rng (+ steps 1) pending)
             (let ((r (qp-tick (qcl-get c (car ids)))))
               (tick (cdr ids) (qcl-set c (car ids) (car r))
                     (append tq (map (lambda (o) (list (car ids) (car o) (cdr o))) (cdr r))))))))
      (else
       (let* ((rng (qp-rng-next rng))
              (idx (modulo rng (length q)))
              (m (list-ref q idx))
              (q2 (let cut ((l q) (i 0))
                    (if (= i idx) (cdr l) (cons (car l) (cut (cdr l) (+ i 1))))))
              (rng2 (qp-rng-next rng))
              (roll (modulo rng2 100)))
         (cond
           ((< roll 20) (loop c q2 rng2 (+ steps 1) pending))          ; drop
           (else
            (let* ((r (qp-step (qcl-get c (cadr m)) (car m) (caddr m)))
                   (c2 (qcl-set c (cadr m) (car r)))
                   (more (map (lambda (o) (list (cadr m) (car o) (cdr o))) (cdr r)))
                   (q3 (append q2 more))
                   (q3 (if (< roll 30) (append q3 (list m)) q3)))      ; dup
              (loop c2 q3 rng2 (+ steps 1) pending)))))))))

(define (chaos-check seed)
  (let* ((c (chaos-run seed))
         (sm (qp-sm (qcl-get c 1)))
         (want '(a1 b1 c1 a2 b2)))
    (check (string-append "seed " (number->string seed) ": agreement")
           #t (agree? c '()))
    (check (string-append "seed " (number->string seed) ": all 5 cmds exactly once")
           #t (and (= 5 (length sm))
                   (let loop ((w want))
                     (cond ((null? w) #t) ((memv (car w) sm) (loop (cdr w))) (else #f)))))
    (check (string-append "seed " (number->string seed) ": all nodes fully applied")
           '(5 5 5) (map (lambda (e) (length (qp-sm (cdr e)))) c))))

(chaos-check 7)
(chaos-check 101)
(chaos-check 20260709)

(done!)
