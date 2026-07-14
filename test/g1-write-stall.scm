; test/g1-write-stall.scm — cw-m9c (G1) exit probe: put-during-LIST write stall.
;
; Seeds a real shard actor + RocksDB with N rows, fires a full-keyspace LIST
; (kv-range, all-keys sentinel), and — while that LIST is still in flight —
; proposes a PUT and times how long its ack takes to land. Before G1 the LIST
; ran INLINE on the shard's mailbox, so the PUT queued behind the whole
; scan+encode (the "LIST blocks lease" class); after G1 the LIST is served by
; a dedicated range-worker off the mailbox, so the PUT should ack in normal
; single-write time regardless of LIST size.
;
; Exit target (docs/multiregion-test-plan.md Phase 0 G1): <50ms write stall
; while a >=10k row (ideally 100k) LIST is in flight.
;
; Run from repo root:  crabscheme run test/g1-write-stall.scm [N]
(include "test/harness.scm")

(make-table 'ws-shard-pid "set")
(make-table 'ws-shard-role "set")
(make-table 'ws-shard-leader "set")
(make-table 'ws-shard-commit "set")
(make-table 'ws-shard-applied "set")
(make-table 'ws-test "set")

(node-make "a")
(define run-tag (number->string (exact (round (* 1000000 (current-second))))))
(define db (string-append "/tmp/cws-g1-stall-" run-tag "-a-s0"))

; N rows to seed — default 10k (CI-friendly); pass a larger N via CWS_G1_N for
; the 100k probe (command-line args here are the interpreter's own, not this
; script's — an env var is the simplest way to parameterize a `crabscheme run`).
(define N (let ((e (get-environment-variable "CWS_G1_N")))
            (if e (or (string->number e) 10000) 10000)))

; shard "0", single voter (always leader, no election wait); n-apply-workers=1.
(spawn-source "(include \"src/server/shard-actor.scm\")" 'shard-main
              "0" '(a) 'a db #f 1 4 #f '() 0 '() #f #f)
(spawn-source "(include \"src/server/peer-poller.scm\")" 'peer-poller
              'a '("0") 150 '() 0)

(define (role) (table-lookup 'ws-shard-role "a:0"))
(define (spin pred who)
  (let loop ((i 0))
    (cond ((pred) #t) ((> i 400000000) (error (string-append "timeout: " who)))
          (else (loop (+ i 1))))))

(section "shard bring-up")
(spin (lambda () (eq? (role) 'leader)) "leader")
(check "shard 0 elected leader" 'leader (role))

; ---- seed N rows sequentially through the real client-proposal path ----
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
(check (string-append "seeded " (number->string N) " rows") #t (table-lookup 'ws-test "seeded"))

; ---- baseline PUT latency (no LIST in flight) ----
(section "baseline PUT latency")
(define baseline-src "
  (define (b s) (string->utf8 s))
  (define (ask pid msg) (send pid msg) (raw-receive))
  (define (propose pid cmd) (ask pid (cons (self) cmd)))
  (define (baseline)
    (let* ((o (table-lookup 'ws-shard-pid \"a:0\"))
           (t0 (current-jiffy)))
      (propose o (list (b \"PUT\") (b \"baseline-key\") (b \"v\")))
      (table-insert! 'ws-test \"baseline-ms\"
                      (round (/ (- (current-jiffy) t0) (/ (jiffies-per-second) 1000))))))")
(spawn-source baseline-src 'baseline)
(spin (lambda () (table-lookup 'ws-test "baseline-ms")) "baseline put")

; ---- fire a full-keyspace LIST from a DEDICATED lister actor, then race a
; PUT from a SEPARATE actor while it's in flight. Two actors (not one, racing
; on its own mailbox) so the PUT's ack can never be shadowed by the LIST
; reply landing first in a shared mailbox — each actor's raw-receive only
; ever sees its own request's reply.
(section "put-during-LIST stall")
(define zero-src "
  (define (list-listener)
    (let ((o (table-lookup 'ws-shard-pid \"a:0\"))
          (zero (make-bytevector 1 0)))
      (send o (list 'kv-range (self) (list (cons 'key zero) (cons 'range-end zero))))
      ; signal the LIST is already enqueued on the shard's mailbox BEFORE the
      ; racer proposes its PUT, so the PUT can never sneak in ahead of it.
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

(display "baseline PUT: ")   (display (table-lookup 'ws-test "baseline-ms")) (display "ms") (newline)
(display "put-during-LIST stall: ") (display (table-lookup 'ws-test "stall-ms")) (display "ms") (newline)
(display "LIST total rows: ") (display (table-lookup 'ws-test "list-total")) (newline)

(check "put-during-LIST stall < 50ms (cw-m9c G1 exit target)"
       #t (< (table-lookup 'ws-test "stall-ms") 50))

; ---- read freshness across the WHOLE worker pool (judge blocker regression) ----
; Each range-worker owns its own ctx; a cached current-rev there goes stale the
; moment the shard applies a write. Interleave PUT/LIST 4 times (2x the 2-worker
; round-robin pool) and assert BOTH the row count and the kv-range-ok header
; revision advance on EVERY worker — a single stale worker fails this.
(section "per-worker read freshness")
(define fresh-src "
  (define (b s) (string->utf8 s))
  (define (ask pid msg) (send pid msg) (raw-receive))
  (define (propose pid cmd) (ask pid (cons (self) cmd)))
  (define (fresh)
    (let ((o (table-lookup 'ws-shard-pid \"a:0\"))
          (zero (make-bytevector 1 0)))
      (define (list-now)
        (let ((r (ask o (list 'kv-range (self)
                              (list (cons 'key zero) (cons 'range-end zero))))))
          (cons (cadr r) (cadddr (cdr r)))))   ; (rev . total)
      (let loop ((i 0) (prev (list-now)) (ok #t))
        (if (= i 4)
            (table-insert! 'ws-test \"fresh-ok\" (if ok #t 'stale))
            (begin
              (propose o (list (b \"PUT\")
                               (b (string-append \"fresh-\" (number->string i))) (b \"v\")))
              (let ((cur (list-now)))
                (loop (+ i 1) cur
                      (and ok
                           (> (car cur) (car prev))
                           (= (cdr cur) (+ (cdr prev) 1))))))))))")
(spawn-source fresh-src 'fresh)
(spin (lambda () (table-lookup 'ws-test "fresh-ok")) "freshness sweep")
(check "LIST count+rev advance after every PUT, across full worker pool"
       #t (table-lookup 'ws-test "fresh-ok"))

(done!)
