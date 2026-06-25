; test/rev-grant-multiwriter.scm — cw-kp0: two independent WRITER groups drawing
; global revisions from one AUTHORITY must get GLOBALLY UNIQUE, non-colliding revs
; (the core allocator safety invariant, live). Three groups on one node (single-voter
; => always leader): shard "0" = rev-authority, shards "1" and "2" = writers. Writes
; to BOTH writers; assert every committed revision is distinct (no two writes — on any
; group — ever share a global rev). This is the foundation Phase 3 (global header
; monotonicity + watch merge) builds on.
; Run from repo root:  crabscheme run test/rev-grant-multiwriter.scm
(include "test/harness.scm")

(make-table 'ws-shard-pid "set")
(make-table 'ws-shard-role "set")
(make-table 'ws-shard-leader "set")
(make-table 'ws-shard-commit "set")
(make-table 'ws-shard-applied "set")
(make-table 'ws-test "set")

(node-make "a")
(define run-tag (number->string (exact (round (* 1000000 (current-second))))))
(define (db sk) (string-append "/tmp/cws-mw-" run-tag "-a-s" sk))

(for-each
 (lambda (sk)
   (spawn-source "(include \"src/server/shard-actor.scm\")" 'shard-main
                 sk '(a) 'a (db sk) #f 1 4 #f '() 0 '() #f #t))   ; global-rev? = #t
 '("0" "1" "2"))
(spawn-source "(include \"src/server/peer-poller.scm\")" 'peer-poller
              'a '("0" "1" "2") 150 '() 0)

(define (role sk) (table-lookup 'ws-shard-role (string-append "a:" sk)))
(define (spin pred who)
  (let loop ((i 0))
    (cond ((pred) #t) ((> i 400000000) (error (string-append "timeout: " who)))
          (else (loop (+ i 1))))))

(section "authority + two writer groups elect leaders")
(spin (lambda () (and (eq? (role "0") 'leader) (eq? (role "1") 'leader) (eq? (role "2") 'leader)))
      "all leaders")
(check "all three groups led" #t #t)

(section "writes to BOTH writer groups get globally-unique revisions")
(define client-src "
  (define (b s) (string->utf8 s))
  (define (ask pid msg) (send pid msg) (raw-receive))
  ; a global-rev writer may bounce 'tryagain while its lease warms — retry (as the real client does)
  (define (propose pid cmd)
    (let loop () (let ((r (ask pid (cons (self) cmd)))) (if (eq? r 'tryagain) (begin (sleep-ms 5) (loop)) r))))
  (define (rev r) (and (pair? r) (cdr r)))   ; (\"PUT\" . rev) -> rev
  (define (client)
    (let ((w1 (table-lookup 'ws-shard-pid \"a:1\"))
          (w2 (table-lookup 'ws-shard-pid \"a:2\")))
      ; interleave writes across the two writer groups
      (table-insert! 'ws-test \"r1\" (rev (propose w1 (list (b \"PUT\") (b \"a\") (b \"1\")))))
      (table-insert! 'ws-test \"r2\" (rev (propose w2 (list (b \"PUT\") (b \"b\") (b \"2\")))))
      (table-insert! 'ws-test \"r3\" (rev (propose w1 (list (b \"PUT\") (b \"c\") (b \"3\")))))
      (table-insert! 'ws-test \"r4\" (rev (propose w2 (list (b \"PUT\") (b \"d\") (b \"4\")))))
      (table-insert! 'ws-test \"done\" #t)))")
(spawn-source client-src 'client)
(spin (lambda () (table-lookup 'ws-test "done")) "all writes acked")

(define revs (list (table-lookup 'ws-test "r1") (table-lookup 'ws-test "r2")
                   (table-lookup 'ws-test "r3") (table-lookup 'ws-test "r4")))
(define (all-distinct? xs)
  (cond ((null? xs) #t)
        ((member (car xs) (cdr xs)) #f)
        (else (all-distinct? (cdr xs)))))
(display "  assigned global revs: ") (write revs) (newline)
(check "every write got a revision (no #f)" #t (and (not (memv #f revs)) #t))
(check "all four global revisions are DISTINCT (no collision across writer groups)"
       #t (all-distinct? revs))
(check "each writer group's two revs are monotonic"
       #t (and (< (table-lookup 'ws-test "r1") (table-lookup 'ws-test "r3"))
               (< (table-lookup 'ws-test "r2") (table-lookup 'ws-test "r4"))))

(done!)
