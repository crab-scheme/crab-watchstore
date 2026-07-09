; quepaxa-differential.scm — Q7 (cw-4q5): drive raft.scm and quepaxa.scm over
; the SAME op traces and assert the replicated-log abstraction they expose is
; identical: same applied command sequence on every node, exactly once,
; including across a leader/coordinator failure mid-stream.
;   crabscheme run test/quepaxa-differential.scm
(include "test/harness.scm")
(include "src/raft.scm")
(include "src/quepaxa.scm")

; shared apply-fn: append commands, skip raft's '() no-op barrier
(define (test-apply sm cmd) (if (null? cmd) sm (append sm (list cmd))))

; ---- qp cluster helpers ----
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

; raft's simulator delivers everything in-order; a blocked-node variant for
; the failure trace (drop anything to/from a blocked node).
(define (rsettle c queue blocked)
  (if (null? queue) c
      (let* ((m (car queue)) (from (car m)) (to (cadr m)) (msg (caddr m)))
        (if (or (memv from blocked) (memv to blocked))
            (rsettle c (cdr queue) blocked)
            (let* ((res (raft-step (cluster-get c to) from msg))
                   (c2 (cluster-set c to (car res)))
                   (more (map (lambda (o) (list to (car o) (cdr o))) (cdr res))))
              (rsettle c2 (append (cdr queue) more) blocked))))))
(define (rdrive c id action blocked)
  (let* ((res (action (cluster-get c id)))
         (c2 (cluster-set c id (car res))))
    (rsettle c2 (map (lambda (o) (list id (car o) (cdr o))) (cdr res)) blocked)))

; ============================================================
(section "happy path: identical applied sequence on every node, both engines")
(let* ((cmds '(a b c d e))
       ; raft: elect 1, propose all at the leader; final tick propagates the
       ; commit index to followers (raft applies at followers one AE late —
       ; quepaxa gossips decisions immediately, so tick to compare fairly)
       (rc (cluster-tick
            (let loop ((c (cluster-campaign (cluster-make '(1 2 3) test-apply '()) 1))
                       (l cmds))
              (if (null? l) c (loop (cluster-propose c 1 (car l)) (cdr l))))
            1))
       ; quepaxa: propose all at the coordinator (node 1)
       (qc (let loop ((c (qcl-make '(1 2 3) '())) (l cmds))
             (if (null? l) c (loop (qdrive c 1 (lambda (st) (qp-propose st (car l))) '()) (cdr l))))))
  (for-each
   (lambda (id)
     (check (string-append "node " (number->string id) ": raft sm = quepaxa sm = trace")
            (list cmds cmds)
            (list (raft-sm (cluster-get rc id)) (qp-sm (qcl-get qc id)))))
   '(1 2 3)))

; ============================================================
(section "batch propose parity")
(let* ((batch '(x y z))
       (rc (let ((c (cluster-campaign (cluster-make '(1 2 3) test-apply '()) 1)))
             (cluster-tick (rdrive c 1 (lambda (st) (raft-propose-batch st batch)) '()) 1)))
       (qc (qdrive (qcl-make '(1 2 3) '()) 1
                   (lambda (st) (qp-propose-batch st batch)) '())))
  (check "raft batch applied" batch (raft-sm (cluster-get rc 2)))
  (check "quepaxa batch applied" batch (qp-sm (qcl-get qc 2)))
  (check "equal across engines" (raft-sm (cluster-get rc 3)) (qp-sm (qcl-get qc 3))))

; ============================================================
(section "mid-stream coordinator/leader death: same surviving history")
; trace: a,b committed under node 1; node 1 dies; c,d committed via node 2.
; raft needs an explicit new election (its driver's timeout would fire it);
; quepaxa needs only its hedge ticks — no election exists.
(let* ((rc (let* ((c (cluster-campaign (cluster-make '(1 2 3) test-apply '()) 1))
                  (c (cluster-propose c 1 'a))
                  (c (cluster-propose c 1 'b))
                  (dead '(1))
                  (c (rdrive c 2 raft-campaign dead))
                  (c (rdrive c 2 (lambda (st) (raft-propose st 'c)) dead))
                  (c (rdrive c 2 (lambda (st) (raft-propose st 'd)) dead))
                  (c (rdrive c 2 raft-tick dead)))
             c))
       (qc (let* ((c (qcl-make '(1 2 3) '((hedge . 2))))
                  (c (qdrive c 1 (lambda (st) (qp-propose st 'a)) '()))
                  (c (qdrive c 1 (lambda (st) (qp-propose st 'b)) '()))
                  (dead '(1))
                  (c (qdrive c 2 (lambda (st) (qp-propose st 'c)) dead))
                  (c (qtick-all c dead)) (c (qtick-all c dead)) (c (qtick-all c dead))
                  (c (qdrive c 2 (lambda (st) (qp-propose st 'd)) dead))
                  (c (qtick-all c dead)) (c (qtick-all c dead)) (c (qtick-all c dead)))
             c)))
  (for-each
   (lambda (id)
     (check (string-append "survivor " (number->string id) ": identical history")
            (raft-sm (cluster-get rc id))
            (qp-sm (qcl-get qc id))))
   '(2 3))
  (check "history is the full trace" '(a b c d) (qp-sm (qcl-get qc 3))))

; ============================================================
(section "post-failover linearizable read agrees with raft's committed state")
(let* ((qc (let* ((c (qcl-make '(1 2 3) '((hedge . 1))))
                  (c (qdrive c 1 (lambda (st) (qp-propose st 'w)) '()))
                  (dead '(1))
                  (c (qdrive c 3 (lambda (st) (qp-read st 'rr)) dead))
                  (c (qtick-all c dead)) (c (qtick-all c dead)) (c (qtick-all c dead)))
             c)))
  (check "read completed after coordinator death" '(rr)
         (car (qp-take-reads (qcl-get qc 3))))
  (check "read observes all prior committed writes" '(w) (qp-sm (qcl-get qc 3))))

(done!)
