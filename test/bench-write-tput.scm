; test/bench-write-tput.scm — cw-65x: local write-throughput benchmark.
;
; 3-voter in-process quepaxa cluster (sim transport, real RocksDB, real
; peer-pollers), C concurrent client actors spread across ALL THREE nodes
; (like real apiserver traffic — non-coordinator nodes pfwd to the coord,
; which is where the prod funnel lives). Each client runs sync
; propose->ack PUTs of ~256B for the measurement window; we report
; total acked / elapsed.
;
;   CWS_BT_CLIENTS=64 CWS_BT_SECS=10 CWS_BT_DURABLE=1 crabscheme run test/bench-write-tput.scm
(include "test/harness.scm")

(make-table 'ws-shard-pid "set")
(make-table 'ws-shard-role "set")
(make-table 'ws-shard-leader "set")
(make-table 'ws-shard-commit "set")
(make-table 'ws-shard-applied "set")
(make-table 'ws-test "set")

(define CLIENTS (let ((e (get-environment-variable "CWS_BT_CLIENTS")))
                  (if e (or (string->number e) 64) 64)))
(define SECS (let ((e (get-environment-variable "CWS_BT_SECS")))
               (if e (or (string->number e) 10) 10)))
(define DURABLE? (equal? "1" (get-environment-variable "CWS_BT_DURABLE")))

(for-each node-make (list "a" "b" "c"))
(node-link! "a" "b") (node-link! "a" "c") (node-link! "b" "c")

(define run-tag (number->string (exact (round (* 1000000 (current-second))))))
(define (db n) (string-append "/tmp/cws-bt-" run-tag "-" n "-s0"))

; same positional tail as node-cluster's spawn; coord = a, default hedge.
(for-each
 (lambda (n)
   (spawn-source "(include \"src/server/quepaxa-shard.scm\")" 'qp-shard-main
                 "0" '(a b c) (string->symbol n) (db n) DURABLE? 1 4 #f '() 0 '() #f #f)
   ; channel 1 = group 0's engine channel — legacy channel-less node-poll no
   ; longer drains ch-1 frames, so match prod's per-channel poller (cw-c8b).
   (spawn-source "(include \"src/server/peer-poller.scm\")" 'peer-poller
                 (string->symbol n) '("0") 150 '() 0 1))
 (list "a" "b" "c"))

(define (spin pred who)
  (let loop ((i 0))
    (cond ((pred) #t) ((> i 400000000) (error (string-append "timeout: " who)))
          (else (loop (+ i 1))))))
(section "bring-up")
(spin (lambda () (eq? (table-lookup 'ws-shard-role "a:0") 'leader)) "coordinator")
; clients on b/c look their shard pid up immediately — wait for ALL nodes' pids,
; not just the coordinator's election (cw-2au: the lookup raced b/c compile).
(for-each (lambda (n) (spin (lambda () (table-lookup 'ws-shard-pid (string-append n ":0"))) n))
          (list "a" "b" "c"))
(check "coordinator up" 'leader (table-lookup 'ws-shard-role "a:0"))

; ---- clients: CLIENTS actors, round-robin across nodes; each loops sync PUTs
; until the deadline table flag flips, then reports its acked count. ----
(section (string-append "measure " (number->string SECS) "s x "
                        (number->string CLIENTS) " clients"
                        (if DURABLE? " durable" " non-durable")))
(make-table 'ws-bt "set")
(define (client-src id node)
  (string-append "
  (define (b s) (string->utf8 s))
  (define VAL (make-bytevector 256 118))
  (define (run)
    (let ((o (table-lookup 'ws-shard-pid \"" node ":0\")))
      (let loop ((i 0))
        (if (table-lookup 'ws-bt \"stop\")
            (table-insert! 'ws-bt \"done-" id "\" i)
            (begin
              (send o (cons (self)
                            (list (b \"PUT\")
                                  (b (string-append \"c" id "-\" (number->string i)))
                                  VAL)))
              (raw-receive)
              (loop (+ i 1)))))))"))
(let loop ((i 0))
  (if (< i CLIENTS)
      (let ((node (list-ref (list "a" "b" "c") (modulo i 3))))
        (spawn-source (client-src (number->string i) node) 'run)
        (loop (+ i 1)))))

(define t0 (current-second))
(sleep-ms (* SECS 1000))
(table-insert! 'ws-bt "stop" #t)
; wait for all clients to report
(let wait ((i 0))
  (if (< i CLIENTS)
      (begin (spin (lambda () (table-lookup 'ws-bt (string-append "done-" (number->string i))))
                   "client drain")
             (wait (+ i 1)))))
(define elapsed (- (current-second) t0))
(define total
  (let sum ((i 0) (acc 0))
    (if (< i CLIENTS) (sum (+ i 1) (+ acc (table-lookup 'ws-bt (string-append "done-" (number->string i)))))
        acc)))
(define wps (exact (round (/ total elapsed))))
(display (string-append "RESULT total=" (number->string total)
                        " elapsed=" (number->string (exact (round (* 1000 elapsed))))
                        "ms wps=" (number->string wps)))
(newline)
(check "throughput measured (>0)" #t (> wps 0))
(done!)
