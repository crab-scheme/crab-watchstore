; test/g1-quepaxa-range.scm — cw-m9c (G1) on the QUEPAXA driver: Range/LIST is
; served by the range-worker pool off the shard mailbox, for BOTH consistency
; modes (serializable inline dispatch + linearizable via the read-slot drain),
; and every worker's read is FRESH (count + header revision advance per PUT).
;
; Mirrors test/g1-write-stall.scm (raft driver): single-voter quepaxa shard,
; put-during-LIST stall probe + per-worker freshness sweep. Smaller default N
; (2000) — the mechanism (off-mailbox dispatch) is engine-independent; the
; at-scale numbers are the raft probe's job.
;
; Run from repo root:  crabscheme run test/g1-quepaxa-range.scm
(include "test/harness.scm")

(make-table 'ws-shard-pid "set")
(make-table 'ws-shard-role "set")
(make-table 'ws-shard-leader "set")
(make-table 'ws-shard-commit "set")
(make-table 'ws-shard-applied "set")
(make-table 'ws-test "set")

(node-make "a")
(define run-tag (number->string (exact (round (* 1000000 (current-second))))))
(define db (string-append "/tmp/cws-g1-qp-range-" run-tag "-a-s0"))

(define N (let ((e (get-environment-variable "CWS_G1_N")))
            (if e (or (string->number e) 2000) 2000)))

; shard "0", single voter (coord = a, quorum of 1); same positional tail as
; node-cluster's spawn (rest slot 2 = hedge ticks, slot 7 = leader-node).
(spawn-source "(include \"src/server/quepaxa-shard.scm\")" 'qp-shard-main
              "0" '(a) 'a db #f 1 4 #f '() 0 '() #f #f)
(spawn-source "(include \"src/server/peer-poller.scm\")" 'peer-poller
              'a '("0") 150 '() 0)

(define (role) (table-lookup 'ws-shard-role "a:0"))
(define (spin pred who)
  (let loop ((i 0))
    (cond ((pred) #t) ((> i 400000000) (error (string-append "timeout: " who)))
          (else (loop (+ i 1))))))

(section "quepaxa shard bring-up")
(spin (lambda () (eq? (role) 'leader)) "coordinator")
(check "shard 0 coordinator up" 'leader (role))

; ---- seed N rows through the real client-proposal path ----
(section (string-append "seed " (number->string N) " rows"))
(define seed-src (string-append "
  (define (b s) (string->utf8 s))
  (define (ask pid msg) (send pid msg) (raw-receive))
  (define (propose pid cmd) (ask pid (cons (self) cmd)))
  (define (pad n)
    (let ((s (number->string n)))
      (string-append (make-string (- 8 (string-length s)) #\\0) s)))
  (define (seed)
    (let ((o (table-lookup 'ws-shard-pid \"a:0\")))
      (let loop ((i 0))
        (if (< i " (number->string N) ")
            (begin
              (propose o (list (b \"PUT\") (b (string-append \"k\" (pad i))) (b \"v\")))
              (loop (+ i 1)))
            (table-insert! 'ws-test \"seeded\" #t)))))"))
(spawn-source seed-src 'seed)
(spin (lambda () (table-lookup 'ws-test "seeded")) "seed rows")
(check "seeded" #t (table-lookup 'ws-test "seeded"))

; ---- put-during-LIST stall (serializable LIST: dispatched inline, so this is
; the path that used to run mvcc-range ON the quepaxa mailbox) ----
(section "put-during-LIST stall (quepaxa)")
(define zero-src "
  (define (list-listener)
    (let ((o (table-lookup 'ws-shard-pid \"a:0\"))
          (zero (make-bytevector 1 0)))
      (send o (list 'kv-range (self)
                    (list (cons 'key zero) (cons 'range-end zero)
                          (cons 'serializable #t))))
      (table-insert! 'ws-test \"list-fired\" #t)
      (let ((r (raw-receive)))
        (table-insert! 'ws-test \"list-total\" (cadddr (cdr r))))))")
(define race-src "
  (define (b s) (string->utf8 s))
  (define (ask pid msg) (send pid msg) (raw-receive))
  (define (propose pid cmd) (ask pid (cons (self) cmd)))
  (define (spin-fired)
    (if (table-lookup 'ws-test \"list-fired\") #t (spin-fired)))
  (define (racer)
    (spin-fired)
    (let ((o (table-lookup 'ws-shard-pid \"a:0\"))
          (t0 (current-jiffy)))
      (propose o (list (b \"PUT\") (b \"race-key\") (b \"v\")))
      (table-insert! 'ws-test \"stall-ms\"
                      (round (/ (- (current-jiffy) t0) (/ (jiffies-per-second) 1000))))))")
(spawn-source zero-src 'list-listener)
(spawn-source race-src 'racer)
(spin (lambda () (table-lookup 'ws-test "stall-ms")) "put-during-list")
(spin (lambda () (table-lookup 'ws-test "list-total")) "list reply")

(display "put-during-LIST stall: ") (display (table-lookup 'ws-test "stall-ms")) (display "ms") (newline)
(display "LIST total rows: ") (display (table-lookup 'ws-test "list-total")) (newline)

(check "quepaxa put-during-LIST stall < 50ms" #t
       (< (table-lookup 'ws-test "stall-ms") 50))

; ---- per-worker read freshness, BOTH consistency modes ----
; Interleave PUT/LIST 4 times (2x the 2-worker pool), alternating serializable
; and linearizable LISTs; assert count + kv-range-ok header rev advance every
; time. A single stale worker ctx fails this.
(section "per-worker read freshness (serializable + linearizable)")
(define fresh-src "
  (define (b s) (string->utf8 s))
  (define (ask pid msg) (send pid msg) (raw-receive))
  (define (propose pid cmd) (ask pid (cons (self) cmd)))
  (define (fresh)
    (let ((o (table-lookup 'ws-shard-pid \"a:0\"))
          (zero (make-bytevector 1 0)))
      (define (list-now ser?)
        (let ((r (ask o (list 'kv-range (self)
                              (append (list (cons 'key zero) (cons 'range-end zero))
                                      (if ser? (list (cons 'serializable #t)) '()))))))
          (cons (cadr r) (cadddr (cdr r)))))   ; (rev . total)
      (let loop ((i 0) (prev (list-now #t)) (ok #t))
        (if (= i 4)
            (table-insert! 'ws-test \"fresh-ok\" (if ok #t 'stale))
            (begin
              (propose o (list (b \"PUT\")
                               (b (string-append \"fresh-\" (number->string i))) (b \"v\")))
              (let ((cur (list-now (even? i))))
                (loop (+ i 1) cur
                      (and ok
                           (> (car cur) (car prev))
                           (= (cdr cur) (+ (cdr prev) 1))))))))))")
(spawn-source fresh-src 'fresh)
(spin (lambda () (table-lookup 'ws-test "fresh-ok")) "freshness sweep")
(check "quepaxa LIST count+rev advance after every PUT, both modes, full pool"
       #t (table-lookup 'ws-test "fresh-ok"))

(done!)
