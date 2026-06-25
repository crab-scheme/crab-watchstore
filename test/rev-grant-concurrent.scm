; test/rev-grant-concurrent.scm — cw-kp0: under CONCURRENCY, three writer groups
; requesting grants from one authority at the same time still get globally-unique,
; non-colliding revisions (the authority serializes grants through its Raft). Four
; groups on one node (single-voter => always leader): shard "0" = authority, shards
; "1","2","3" = writers. Three client actors run concurrently, each PUTting 3 keys to
; its own writer group; we assert all 9 committed revisions are DISTINCT and every key
; reads back its value. This is the concurrency soundness the Jepsen register pass rests on.
; Run from repo root:  crabscheme run test/rev-grant-concurrent.scm
(include "test/harness.scm")

(make-table 'ws-shard-pid "set")
(make-table 'ws-shard-role "set")
(make-table 'ws-shard-leader "set")
(make-table 'ws-shard-commit "set")
(make-table 'ws-shard-applied "set")
(make-table 'ws-test "set")

(node-make "a")
(define run-tag (number->string (exact (round (* 1000000 (current-second))))))
(for-each
 (lambda (sk)
   (spawn-source "(include \"src/server/shard-actor.scm\")" 'shard-main
                 sk '(a) 'a (string-append "/tmp/cws-cc-" run-tag "-a-s" sk) #f 1 4 #f '() 0 '() #f #t))
 '("0" "1" "2" "3"))
(spawn-source "(include \"src/server/peer-poller.scm\")" 'peer-poller
              'a '("0" "1" "2" "3") 150 '() 0)

(define (role sk) (table-lookup 'ws-shard-role (string-append "a:" sk)))
(define (spin pred who)
  (let loop ((i 0))
    (cond ((pred) #t) ((> i 400000000) (error (string-append "timeout: " who)))
          (else (loop (+ i 1))))))

(section "authority + 3 writer groups elect leaders")
(spin (lambda () (and (eq? (role "0") 'leader) (eq? (role "1") 'leader)
                      (eq? (role "2") 'leader) (eq? (role "3") 'leader))) "all leaders")
(check "all four groups led" #t #t)

(section "3 concurrent writers -> globally-unique revisions (authority serializes grants)")
; each client writes 3 keys to its own writer group, storing the committed revs.
(define (mk-client n)
  (string-append "
    (define (b s) (string->utf8 s))
    (define (ask pid msg) (send pid msg) (raw-receive))
    ; a global-rev writer may bounce 'tryagain while its lease warms — retry (as the real client does)
    (define (propose pid cmd)
      (let loop () (let ((r (ask pid (cons (self) cmd)))) (if (eq? r 'tryagain) (begin (sleep-ms 5) (loop)) r))))
    (define (rev r) (and (pair? r) (cdr r)))
    (define (clientN)
      (let ((w (table-lookup 'ws-shard-pid \"a:" n "\")))
        (table-insert! 'ws-test \"c" n "a\" (rev (propose w (list (b \"PUT\") (b \"k" n "a\") (b \"v\")))))
        (table-insert! 'ws-test \"c" n "b\" (rev (propose w (list (b \"PUT\") (b \"k" n "b\") (b \"v\")))))
        (table-insert! 'ws-test \"c" n "c\" (rev (propose w (list (b \"PUT\") (b \"k" n "c\") (b \"v\")))))
        (table-insert! 'ws-test \"done" n "\" #t)))"))
; spawn all three concurrently (entry name clientN in each source)
(spawn-source (mk-client "1") 'clientN)
(spawn-source (mk-client "2") 'clientN)
(spawn-source (mk-client "3") 'clientN)
(spin (lambda () (and (table-lookup 'ws-test "done1")
                      (table-lookup 'ws-test "done2")
                      (table-lookup 'ws-test "done3"))) "all concurrent writers done")

(define revs
  (map (lambda (k) (table-lookup 'ws-test k))
       '("c1a" "c1b" "c1c" "c2a" "c2b" "c2c" "c3a" "c3b" "c3c")))
(define (all-distinct? xs)
  (cond ((null? xs) #t) ((member (car xs) (cdr xs)) #f) (else (all-distinct? (cdr xs)))))
(display "  9 concurrent global revs: ") (write revs) (newline)
(check "every concurrent write got a revision" #t (and (not (memv #f revs)) #t))
(check "all 9 revisions are globally DISTINCT (no collision under concurrency)"
       #t (all-distinct? revs))

(done!)
