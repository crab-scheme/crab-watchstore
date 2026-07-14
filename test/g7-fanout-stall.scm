; test/g7-fanout-stall.scm — cw-04k exit probe: watch-event fanout stall.
;
; Before cw-04k, watch dispatch (watch-check-compaction! / watch-on-apply! matching
; + per-watcher deliver-fn send) ran INLINE on the shard actor's apply path, exactly
; like Range/LIST used to before cw-m9c. With many watchers on a busy prefix, a
; churn burst occupies the shard thread doing fanout work, delaying every
; concurrent PUT's consensus decide (CWS_PROF showed cons= jumping from ~10-30ms
; to 900-1700ms under real k8s-scale churn). After cw-04k, fanout runs on a
; dedicated watch-fanout worker fed by a bounded (pre,post] notify queue, so the
; shard only enqueues and returns to consensus work.
;
; This probe registers N watchers on a shared busy prefix, fires a burst of writes
; under that prefix (heavy fanout: every event matches every watcher), and
; CONCURRENTLY races PUTs against a disjoint key to measure write-path latency
; throughout the burst. It also drains every watcher's own mailbox and asserts
; each received every burst event exactly once, in ascending revision order (the
; etcd per-watcher ordering guarantee) — the correctness half of the fix, not just
; the performance half.
;
; Exit target: put p99 during the fanout burst stays <50ms (matches cw-m9c G1's
; bar). Run BOTH engines (CWS_G7_ENGINE=raft|quepaxa, default raft).
;
; Run from repo root:  crabscheme run test/g7-fanout-stall.scm
;   CWS_G7_WATCHERS=200 CWS_G7_EVENTS=300 CWS_G7_ENGINE=raft crabscheme run test/g7-fanout-stall.scm
(include "test/harness.scm")

(make-table 'ws-shard-pid "set")
(make-table 'ws-shard-role "set")
(make-table 'ws-shard-leader "set")
(make-table 'ws-shard-commit "set")
(make-table 'ws-shard-applied "set")
(make-table 'ws-test "set")

(node-make "a")
(define run-tag (number->string (exact (round (* 1000000 (current-second))))))
(define db (string-append "/tmp/cws-g7-fanout-" run-tag "-a-s0"))

(define ENGINE (let ((e (get-environment-variable "CWS_G7_ENGINE"))) (if e e "raft")))
(define NWATCH (let ((e (get-environment-variable "CWS_G7_WATCHERS")))
                 (if e (or (string->number e) 200) 200)))
(define NEVENTS (let ((e (get-environment-variable "CWS_G7_EVENTS")))
                  (if e (or (string->number e) 300) 300)))

(if (string=? ENGINE "quepaxa")
    (spawn-source "(include \"src/server/quepaxa-shard.scm\")" 'qp-shard-main
                  "0" '(a) 'a db #f 1 4 #f '() 0 '() #f #f)
    (spawn-source "(include \"src/server/shard-actor.scm\")" 'shard-main
                  "0" '(a) 'a db #f 1 4 #f '() 0 '() #f #f))
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

; ---- register N watchers on the shared busy prefix "churn/" ----
; Each watcher is a DEDICATED actor (own mailbox) so per-watcher delivery order is
; observed independently — a shared mailbox across watchers couldn't distinguish
; "watcher 7 got event 3 out of order" from "watcher 12's event landed first".
(section (string-append "register " (number->string NWATCH) " watchers on churn/"))
; Termination is by COUNT (mrevs reaching NEVENTS), not by the burst-actor's
; "burst-done" flag: burst-done only means the writes were PROPOSED, not that
; watch-fanout has finished DISPATCHING them to every watcher — under real
; backpressure (WATCH-FANOUT-MAX-INFLIGHT) delivery can trail proposal
; completion, so a done-flag-only stop condition would race and undercount.
(define watcher-src (string-append "
  (define (watcher n)
    (let* ((o (table-lookup 'ws-shard-pid \"a:0\"))
           (pfx (string->utf8 \"churn/\"))
           ; \"churn0\" is the standard prefix-range-end trick ('/' = 0x2F < '0' =
           ; 0x30, so every \"churn/...\" key sorts strictly below it) — avoids
           ; depending on mvcc.scm's prefix-range-end inside this spawned actor's
           ; own (separate) environment.
           (rend (string->utf8 \"churn0\"))
           (spec (list (cons 'key pfx) (cons 'range-end rend) (cons 'start-rev 0))))
      (send o (list 'watch-register (self) spec))
      (let ((created (raw-receive)))          ; ('watch-created id rev)
        (table-insert! 'ws-test (string-append \"w-created-\" (number->string n)) #t))
      ; drain WatchResponse frames until this watcher has NEVENTS mod_revs (from
      ; the wire kv-view: vector idx 2 = mod-rev), OR a generous idle-timeout
      ; ceiling trips (a real hang, not just fanout being behind).
      (let loop ((mrevs '()) (idle 0))
        (if (>= (length mrevs) " (number->string NEVENTS) ")
            (table-insert! 'ws-test (string-append \"w-mrevs-\" (number->string n))
                           (reverse mrevs))
            (if (> idle 20000)
                (table-insert! 'ws-test (string-append \"w-mrevs-\" (number->string n))
                               (reverse mrevs))     ; give up — checker will catch the shortfall
                (let ((m (raw-receive 50)))
                  (if (and (pair? m) (eq? (car m) 'watch-response))
                      (let* ((s (cadr m)) (evs (list-ref s 6))
                             (news (map (lambda (e) (vector-ref (cadr e) 2)) evs)))
                        (loop (append (reverse news) mrevs) 0))
                      (loop mrevs (+ idle 1)))))))))"))
(let reg ((i 0))
  (if (< i NWATCH)
      (begin (spawn-source watcher-src 'watcher i) (reg (+ i 1)))))
(spin (lambda ()
        (let loop ((i 0))
          (cond ((= i NWATCH) #t)
                ((not (table-lookup 'ws-test (string-append "w-created-" (number->string i)))) #f)
                (else (loop (+ i 1))))))
      "all watchers registered")
(check (string-append (number->string NWATCH) " watchers registered") #t #t)

; ---- concurrently: burst of churn/ writes (fanout load) + a racer measuring
; PUT latency against a DISJOINT key throughout the burst ----
(section (string-append "fanout burst: " (number->string NEVENTS)
                        " events x " (number->string NWATCH) " watchers"))
(define burst-src (string-append "
  (define (b s) (string->utf8 s))
  (define (ask pid msg) (send pid msg) (raw-receive))
  (define (propose pid cmd) (ask pid (cons (self) cmd)))
  (define (pad n)
    (let ((s (number->string n)))
      (string-append (make-string (- 6 (string-length s)) #\\0) s)))
  (define (burst)
    (let ((o (table-lookup 'ws-shard-pid \"a:0\")))
      (let loop ((i 0))
        (if (< i " (number->string NEVENTS) ")
            (begin
              (propose o (list (b \"PUT\") (b (string-append \"churn/\" (pad i))) (b \"v\")))
              (loop (+ i 1)))
            (table-insert! 'ws-test \"burst-done\" #t)))))"))
(define racer-src "
  (define (b s) (string->utf8 s))
  (define (ask pid msg) (send pid msg) (raw-receive))
  (define (propose pid cmd) (ask pid (cons (self) cmd)))
  (define (racer)
    (let ((o (table-lookup 'ws-shard-pid \"a:0\")))
      (let loop ((n 0) (ms '()))
        (if (table-lookup 'ws-test \"burst-done\")
            (begin (table-insert! 'ws-test \"race-lat-ms\" (reverse ms))
                   (table-insert! 'ws-test \"race-n\" n))
            (let ((t0 (current-jiffy)))
              (propose o (list (b \"PUT\") (b (string-append \"racer-key-\" (number->string n))) (b \"v\")))
              (loop (+ n 1)
                    (cons (round (/ (- (current-jiffy) t0) (/ (jiffies-per-second) 1000))) ms)))))))")
(spawn-source burst-src 'burst)
(spawn-source racer-src 'racer)
(spin (lambda () (table-lookup 'ws-test "burst-done")) "burst complete")
(spin (lambda () (table-lookup 'ws-test "race-lat-ms")) "racer latencies")

; ---- p50/p99 over the racer's PUT latencies during the burst ----
(define (sorted-list lst) (list-sort < lst))
(define (pct lst p)
  (let* ((v (list->vector (sorted-list lst))) (n (vector-length v)))
    (if (= n 0) 0 (vector-ref v (min (- n 1) (exact (floor (* p n))))))))
(define race-lat (table-lookup 'ws-test "race-lat-ms"))
(define p50 (pct race-lat 0.50))
(define p99 (pct race-lat 0.99))
(display "engine=") (display ENGINE)
(display " watchers=") (display NWATCH)
(display " events=") (display NEVENTS)
(display " racer-writes=") (display (table-lookup 'ws-test "race-n"))
(display " p50=") (display p50) (display "ms")
(display " p99=") (display p99) (display "ms")
(newline)

(check "put p99 during fanout burst < 50ms (cw-04k exit target)" #t (< p99 50))

; ---- per-watcher ordering: every watcher saw every churn/ event exactly once,
; strictly ascending (no gaps, no dups — the etcd per-watcher guarantee) ----
(section "per-watcher order/gap/dup check")
(spin (lambda ()
        (let loop ((i 0))
          (cond ((= i NWATCH) #t)
                ((not (table-lookup 'ws-test (string-append "w-mrevs-" (number->string i)))) #f)
                (else (loop (+ i 1))))))
      "all watchers drained")
(let check-w ((i 0) (all-ok #t) (bad-detail #f))
  (if (= i NWATCH)
      (check "every watcher: exactly-once, ascending, gap-free delivery" #t all-ok)
      (let* ((mrevs (table-lookup 'ws-test (string-append "w-mrevs-" (number->string i))))
             (n (length mrevs))
             (ascending? (let loop ((l mrevs))
                           (cond ((or (null? l) (null? (cdr l))) #t)
                                 ((< (car l) (cadr l)) (loop (cdr l)))
                                 (else #f))))
             (ok (and (= n NEVENTS) ascending?)))
        (check-w (+ i 1) (and all-ok ok)
                 (if (and (not ok) (not bad-detail))
                     (list i n ascending?) bad-detail)))))

(done!)
