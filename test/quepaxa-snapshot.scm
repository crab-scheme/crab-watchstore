; quepaxa-snapshot.scm — Q4 compaction/snapshot + Q5 linearizable reads
; (cw-cec, cw-h4z).  crabscheme run test/quepaxa-snapshot.scm
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

; ============================================================
(section "compaction: prune below floor, serve fetch above it, snapo below it")
(let* ((c (qcl-make '(1 2 3) '()))
       (c (let loop ((i 0) (c c))
            (if (= i 6) c
                (loop (+ i 1) (qdrive c 1 (lambda (st) (qp-propose st i)) '())))))
       (n1 (qcl-get c 1)))
  (check "6 applied" 6 (qp-applied n1))
  (let ((n1c (qp-compact-to n1 4)))
    (check "base=4" 4 (qp-base n1c))
    (check "slot 5 still fetchable" #t
           (let ((r (qp-on-fetch n1c 2 '(fetch 5)))) (eq? 'decd (car (cdar (cdr r))))))
    (check "slot 3 answers snapo" #t
           (let ((r (qp-on-fetch n1c 2 '(fetch 3)))) (eq? 'snapo (car (cdar (cdr r))))))
    (check "esp below floor answers snapo" #t
           (let ((r (qp-on-esp n1c 2 (list 'esp 2 4 '(9 2 ()))))) (eq? 'snapo (car (cdar (cdr r))))))
    (check "no-op compact below base" 4 (qp-base (qp-compact-to n1c 2)))
    (check "no-op compact above applied" 4 (qp-base (qp-compact-to n1c 99)))))

; ============================================================
(section "lagging replica: snapo -> snap-need surfaced -> install -> converges")
(let* ((c (qcl-make '(1 2 3) '()))
       (dead '(3))
       ; 5 slots decided while node 3 is dark
       (c (let loop ((i 0) (c c))
            (if (= i 5) c
                (loop (+ i 1) (qdrive c 1 (lambda (st) (qp-propose st (* 10 i))) dead)))))
       ; 1 and 2 compact past what 3 has
       (c (qcl-set c 1 (qp-compact-to (qcl-get c 1) 5)))
       (c (qcl-set c 2 (qp-compact-to (qcl-get c 2) 5)))
       ; node 3 wakes up: another write happens with 3 live -> 3 sees slot-6
       ; traffic, gap-fetches slot 1, gets snapo
       (c (qdrive c 1 (lambda (st) (qp-propose st 'post)) '()))
       (c (qtick-all c '())) (c (qtick-all c '())) (c (qtick-all c '())))
  (let ((n3 (qcl-get c 3)))
    (check "snap-need surfaced" #t (pair? (qp-snap-need n3)))
    (check "peer base carried" 5 (cdr (qp-snap-need n3)))
    ; driver ships the store snapshot: adopt node 1's sm at base 5 + bids
    (let* ((n1 (qcl-get c 1))
           (sm5 (qp-take-n (qp-sm n1) 5))
           (n3i (qp-install-snapshot n3 5 sm5 '()))
           (c (qcl-set c 3 n3i))
           (c (qtick-all c '())) (c (qtick-all c '())) (c (qtick-all c '())))
      (check "node3 caught up past snapshot" (qp-applied (qcl-get c 1))
             (qp-applied (qcl-get c 3)))
      (check "node3 sm matches node1" (qp-sm (qcl-get c 1)) (qp-sm (qcl-get c 3))))))

; ============================================================
(section "linearizable reads: read-after-write ordering, done tags drain FIFO")
(let* ((c (qcl-make '(1 2 3) '()))
       (c (qdrive c 1 (lambda (st) (qp-propose st 'w1)) '()))
       (c (qdrive c 1 (lambda (st) (qp-read st 'r1)) '()))
       (c (qdrive c 1 (lambda (st) (qp-propose st 'w2)) '()))
       (c (qdrive c 1 (lambda (st) (qp-read st 'r2)) '())))
  (let* ((r (qp-take-reads (qcl-get c 1)))
         (tags (car r))
         (c (qcl-set c 1 (cdr r))))
    (check "both read tags done, FIFO" '(r1 r2) tags)
    (check "drained" '() (car (qp-take-reads (qcl-get c 1))))
    ; the read slot ordered after w1: when r1 completed, w1 was applied
    (check "writes applied" '(w1 w2) (qp-sm (qcl-get c 1)))
    (check "read slots consumed log positions" 4 (qp-applied (qcl-get c 1)))))

; ============================================================
(section "hedged read at a follower completes via coordinator")
(let* ((c (qcl-make '(1 2 3) '()))
       (c (qdrive c 2 (lambda (st) (qp-propose st 'v)) '()))
       (c (qdrive c 2 (lambda (st) (qp-read st 'fr)) '())))
  (let ((tags (car (qp-take-reads (qcl-get c 2)))))
    (check "follower read completed" '(fr) tags)
    (check "write visible at follower" '(v) (qp-sm (qcl-get c 2)))))

; ============================================================
(section "read at follower with DEAD coordinator still completes (hedge)")
(let* ((c (qcl-make '(1 2 3) '((hedge . 2))))
       (dead '(1))
       (c (qdrive c 2 (lambda (st) (qp-propose st 'dw)) dead))
       (c (qdrive c 2 (lambda (st) (qp-read st 'dr)) dead))
       (c (qtick-all c dead)) (c (qtick-all c dead))
       (c (qtick-all c dead)) (c (qtick-all c dead)))
  (let ((tags (car (qp-take-reads (qcl-get c 2)))))
    (check "read completed without coordinator" '(dr) tags)
    (check "write visible" '(dw) (qp-sm (qcl-get c 2)))))

; ============================================================
(section "restart: anti-entropy catch-up + boot-epoch bid uniqueness (WAN-soak bugs)")
(let* ((c (qcl-make '(1 2 3) '()))
       ; node 3 does some writes in boot 1 (bids (3 0 1), (3 0 2) with default boot 0)
       (c (qdrive c 3 (lambda (st) (qp-propose st 'w1)) '()))
       (c (qdrive c 3 (lambda (st) (qp-propose st 'w2)) '()))
       (p3 (qp-applied (qcl-get c 3)))
       (dead '(3))
       ; cluster moves on while 3 is dead
       (c (qdrive c 1 (lambda (st) (qp-propose st 'x1)) dead))
       (c (qdrive c 1 (lambda (st) (qp-propose st 'x2)) dead))
       ; 3 "restarts": fresh engine, applied restored from its store, boot BUMPED
       (sm3 (qp-take-n (qp-sm (qcl-get c 1)) p3))
       (n3 (qp-install-snapshot
            (make-qp 3 '(1 2 3) test-apply '() '((boot . 1))) p3 sm3 '()))
       (c (qcl-set c 3 n3)))
  (check "restarted node is behind" #t (< (qp-applied (qcl-get c 3))
                                          (qp-applied (qcl-get c 1))))
  ; idle anti-entropy probes fire after QP-AE-TICKS ticks with NO local gap signal
  (let loop ((i 0) (c c))
    (if (< i 20)
        (loop (+ i 1) (qtick-all c '()))
        (begin
          (check "restarted node caught up via anti-entropy fetch"
                 (qp-applied (qcl-get c 1)) (qp-applied (qcl-get c 3)))
          (check "histories agree" (qp-sm (qcl-get c 1)) (qp-sm (qcl-get c 3)))
          ; boot-epoch: 3's fresh seq would repeat pre-crash bids without the
          ; boot field — peers' dedup window must NOT skip the new write
          (let* ((c (qdrive c 3 (lambda (st) (qp-propose st 'after-restart)) '()))
                 (c (qtick-all c '())) (c (qtick-all c '())))
            (check "post-restart write applies everywhere (no bid collision)"
                   #t (and (memv 'after-restart (qp-sm (qcl-get c 1)))
                           (memv 'after-restart (qp-sm (qcl-get c 2)))
                           (memv 'after-restart (qp-sm (qcl-get c 3))) #t))
            (check "final agreement" (qp-sm (qcl-get c 1)) (qp-sm (qcl-get c 2))))))))

(done!)
