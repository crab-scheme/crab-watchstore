; test/rev-grant-multishard.scm — cw-kp0 phase 2b.3 MILESTONE: a client write to a
; WRITER group draws a GLOBAL revision from the AUTHORITY group and applies as PUT-AT.
; Two shard groups on one node (single-voter each => always leader): shard "0" = the
; rev-authority, shard "1" = a writer. A client PUT to shard 1 triggers gr-ensure! ->
; REV-GRANT to shard 0 -> lease -> PUT-AT <global rev> -> applied. Proves the live
; cross-group global-revision write path end-to-end.
; Run from repo root:  crabscheme run test/rev-grant-multishard.scm
(include "test/harness.scm")

(make-table 'ws-shard-pid "set")
(make-table 'ws-shard-role "set")
(make-table 'ws-shard-leader "set")
(make-table 'ws-shard-commit "set")
(make-table 'ws-shard-applied "set")
(make-table 'ws-test "set")

(node-make "a")
(define run-tag (number->string (exact (round (* 1000000 (current-second))))))
(define (db sk) (string-append "/tmp/cws-ms-" run-tag "-a-s" sk))

; two groups on node a, both global-rev? = #t (rest[7]); voters '(a) => always leader.
(for-each
 (lambda (sk)
   (spawn-source "(include \"src/server/shard-actor.scm\")" 'shard-main
                 sk '(a) 'a (db sk) #f 1 4 #f '() 0 '() #f #t))
 '("0" "1"))
(spawn-source "(include \"src/server/peer-poller.scm\")" 'peer-poller
              'a '("0" "1") 150 '() 0)

(define (role sk) (table-lookup 'ws-shard-role (string-append "a:" sk)))
(define (spin pred who)
  (let loop ((i 0))
    (cond ((pred) #t) ((> i 400000000) (error (string-append "timeout: " who)))
          (else (loop (+ i 1))))))

(section "both groups elect leaders (authority=0, writer=1)")
(spin (lambda () (and (eq? (role "0") 'leader) (eq? (role "1") 'leader))) "both leaders")
(check "authority group 0 leader" 'leader (role "0"))
(check "writer group 1 leader"    'leader (role "1"))

(section "client PUT to the writer group draws a GLOBAL rev from the authority")
(define client-src "
  (define (b s) (string->utf8 s))
  (define (ask pid msg) (send pid msg) (raw-receive))
  ; a global-rev writer may bounce 'tryagain while its lease warms — retry (as the real client does)
  (define (propose pid cmd)
    (let loop () (let ((r (ask pid (cons (self) cmd)))) (if (eq? r 'tryagain) (begin (sleep-ms 5) (loop)) r))))
  (define (client)
    (let ((w (table-lookup 'ws-shard-pid \"a:1\")))    ; the WRITER group's shard pid
      (table-insert! 'ws-test \"p1\" (propose w (list (b \"PUT\") (b \"k1\") (b \"v1\"))))
      (table-insert! 'ws-test \"p2\" (propose w (list (b \"PUT\") (b \"k2\") (b \"v2\"))))
      ; read the writes back: (get conn key) -> (value create-rev mod-rev version)
      (table-insert! 'ws-test \"r1\" (ask w (list 'get (self) (b \"k1\"))))
      (table-insert! 'ws-test \"r2\" (ask w (list 'get (self) (b \"k2\"))))
      (table-insert! 'ws-test \"done\" #t)))")
(spawn-source client-src 'client)
(spin (lambda () (table-lookup 'ws-test "done")) "writer writes acked")

; the writer's PUTs commit at GLOBAL revisions granted by the authority (1, then 2),
; applied as PUT-AT so every replica would agree. Reply shape matches PUT.
(check "writer PUT 1 commits at global rev 1" (cons "PUT" 1) (table-lookup 'ws-test "p1"))
(check "writer PUT 2 commits at global rev 2" (cons "PUT" 2) (table-lookup 'ws-test "p2"))

(section "the writes are READABLE at their global revisions (data persisted)")
(let ((r1 (table-lookup 'ws-test "r1")) (r2 (table-lookup 'ws-test "r2")))
  (check "k1 value reads back"             "v1" (and r1 (utf8->string (car r1))))
  (check "k1 mod_revision = global rev 1"   1   (and r1 (caddr r1)))
  (check "k2 value reads back"             "v2" (and r2 (utf8->string (car r2))))
  (check "k2 mod_revision = global rev 2"   2   (and r2 (caddr r2))))

(section "writer reports progress on tick -> authority watermark reflects applied rev 2")
; the writer applied up to global rev 2 and still holds leased revs, so it reports
; lowest-unapplied = 3 each tick -> authority's global-watermark settles at 2.
(define wm-src "
  (define (ask pid msg) (send pid msg) (raw-receive))
  (define (wm-checker)
    (let ((o (table-lookup 'ws-shard-pid \"a:0\")))
      (let loop ((i 0))
        (let ((r (ask o (list 'global-watermark (self)))))
          (cond ((and (pair? r) (eqv? (cadr r) 2)) (table-insert! 'ws-test \"wm\" 2))
                ((> i 300) (table-insert! 'ws-test \"wm\" (cadr r)))
                (else (sleep-ms 10) (loop (+ i 1))))))))")   ; yield so the poller ticks the writer
(spawn-source wm-src 'wm-checker)
(spin (lambda () (table-lookup 'ws-test "wm")) "watermark settles")
(check "authority global-watermark = writer's applied rev 2" 2 (table-lookup 'ws-test "wm"))

(done!)
