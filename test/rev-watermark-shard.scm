; test/rev-watermark-shard.scm — cw-kp0 Phase 3: the authority-side low-watermark
; aggregator, live. A single rev-authority (shard 0, global-rev?) grants revs (advancing
; granted-high), receives writer progress reports (lowest-unapplied global rev), and
; answers (global-watermark) = min over writers-with-unapplied of (lowest-unapplied - 1),
; else granted-high — an idle/caught-up writer never freezes the watermark.
; Run from repo root:  crabscheme run test/rev-watermark-shard.scm
(include "test/harness.scm")

(make-table 'ws-shard-pid "set")
(make-table 'ws-shard-role "set")
(make-table 'ws-shard-leader "set")
(make-table 'ws-shard-commit "set")
(make-table 'ws-shard-applied "set")
(make-table 'ws-test "set")

(node-make "a")
(define run-tag (number->string (exact (round (* 1000000 (current-second))))))
(spawn-source "(include \"src/server/shard-actor.scm\")" 'shard-main
              "0" '(a) 'a (string-append "/tmp/cws-wm-" run-tag "-a-s0") #f 1 4 #f '() 0 '() #f #t)
(spawn-source "(include \"src/server/peer-poller.scm\")" 'peer-poller 'a '("0") 150 '() 0)

(define (role) (table-lookup 'ws-shard-role "a:0"))
(define (spin pred who)
  (let loop ((i 0))
    (cond ((pred) #t) ((> i 400000000) (error (string-append "timeout: " who)))
          (else (loop (+ i 1))))))

(section "rev-authority up")
(spin (lambda () (eq? (role) 'leader)) "leader")
(check "authority leader" 'leader (role))

(section "low-watermark = min in-flight (lowest-unapplied-1), else granted-high")
(define client-src "
  (define (b s) (string->utf8 s))
  (define (ask pid msg) (send pid msg) (raw-receive))
  (define (propose pid cmd) (ask pid (cons (self) cmd)))
  (define (client)
    (let ((o (table-lookup 'ws-shard-pid \"a:0\")))
      (propose o (list (b \"REV-GRANT\") (b \"10\")))               ; granted-high = 10
      (table-insert! 'ws-test \"w0\" (ask o (list 'global-watermark (self))))   ; no writers -> 10
      (send o (list 'rev-progress 1 5))                            ; writer 1 unapplied from rev 5
      (table-insert! 'ws-test \"w1\" (ask o (list 'global-watermark (self))))   ; min(10, 5-1)=4
      (send o (list 'rev-progress 2 #f))                           ; writer 2 caught up
      (table-insert! 'ws-test \"w2\" (ask o (list 'global-watermark (self))))   ; still 4 (w1 constrains)
      (send o (list 'rev-progress 1 #f))                           ; writer 1 caught up
      (table-insert! 'ws-test \"w3\" (ask o (list 'global-watermark (self))))   ; all caught up -> 10
      (table-insert! 'ws-test \"done\" #t)))")
(spawn-source client-src 'client)
(spin (lambda () (table-lookup 'ws-test "done")) "watermark queries done")

(check "no writers reported -> watermark = granted-high 10" (list 'global-watermark-ok 10) (table-lookup 'ws-test "w0"))
(check "writer 1 unapplied@5 -> watermark = 4"              (list 'global-watermark-ok 4)  (table-lookup 'ws-test "w1"))
(check "idle writer 2 does NOT lower it -> still 4"          (list 'global-watermark-ok 4)  (table-lookup 'ws-test "w2"))
(check "all writers caught up -> watermark back to high 10" (list 'global-watermark-ok 10) (table-lookup 'ws-test "w3"))

(done!)
