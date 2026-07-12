; test/mvcc-compact-incremental.scm — incremental COMPACT GC (cw-vku).
;
; The replicated COMPACT apply used to run the whole window's physical GC
; synchronously on every replica's shard thread at the same log position
; (field: ~1s cluster-wide put stall every k8s 5-min compaction). It now only
; flips the ErrCompacted gate (mvcc-compact-begin!); the sweep runs one
; bounded slice per driver tick (mvcc-compact-gc-step!). These tests pin:
;   1. begin! flips the gate immediately (ErrCompacted below the floor) while
;      the physical rows are still present;
;   2. gc-step! converges to exactly the state the old synchronous
;      mvcc-compact produced (same live rows, old versions/events purged);
;   3. begin!'s error protocol matches mvcc-compact (err-compacted/future-rev);
;   4. step! is a cheap no-op when caught up.

(include "test/harness.scm")
(include "src/store-ctx.scm")
(include "src/mvcc.scm")
(include "test/mvcc-util.scm")

(define run-tag (number->string (current-jiffy)))
(define DB-PATH (string-append "/tmp/cws-mvcc-compact-inc-" run-tag))
(define CTX (make-ctx (store-open DB-PATH #t) "default" #t))
(reset-ctx! CTX)

(define (b s) (string->utf8 s))
(define (put k v) (mvcc-apply CTX (list (b "PUT") (b k) (b v))))
(define (rev-cf-rows) (length (kv-scan CTX (mvcc-byte NS-REV))))

(section "incremental COMPACT: gate flips first, GC converges via steps")

; 40 revisions on 10 keys => 30 dead versions + 40 events below a rev-35 floor
(let loop ((i 1))
  (when (<= i 40)
    (put (string-append "k" (number->string (modulo i 10))) (number->string i))
    (loop (+ i 1))))
(define pre-events (rev-cf-rows))
(check "seeded 40 events in REV-CF" 40 pre-events)

(define r1 (mvcc-compact-begin! CTX 35))
(check "begin! acks the full target rev" '(ok . 35) r1)
(check "gate is live immediately (compact-rev = 35)" 35 (mvcc-compact-rev CTX))
(check "physical GC deferred: REV-CF still full" 40 (rev-cf-rows))
(check "read below the floor errors immediately"
       'err-compacted
       (car (mvcc-range CTX (b "k1") #f (list (cons 'revision 10)))))

; drive the driver-tick seam until it reports caught-up
(let steps ((n 0))
  (if (mvcc-compact-gc-step! CTX)
      (if (> n 100) (error "gc-step! did not converge") (steps (+ n 1)))))
(check "gc cursor reached the floor" 35 (mvcc-compact-gc-rev CTX))
(check "events <= 35 purged, > 35 kept" 5 (rev-cf-rows))
(check "latest values survive GC"
       "40" (utf8->string (kv-rec-value (mvcc-get-latest CTX (b "k0")))))
(check "current-rev untouched by compaction" 40 (mvcc-current-rev CTX))
(check "caught-up step! is a no-op" #f (mvcc-compact-gc-step! CTX))

(section "incremental COMPACT: error protocol matches mvcc-compact")
(check "re-compacting at/below the floor -> err-compacted"
       '(err-compacted . 35) (mvcc-compact-begin! CTX 35))
(check "future rev -> err-future-rev"
       '(err-future-rev . 40) (mvcc-compact-begin! CTX 99))

(done!)
