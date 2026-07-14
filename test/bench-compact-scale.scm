; bench-compact-scale.scm — cw-8vb (G6): prove mvcc-compact (cw-xq9, commit 92efd88's
; window-driven GC) is O(revision window), NOT O(keyspace), at 100k live keys.
;
; Two things to show:
;   A. compaction duration scales with the WINDOW size (compactRev - prevCompactRev),
;      not with the number of live keys in the store — grow the keyspace across three
;      stages (10k -> 50k -> 100k live keys) and re-measure a FIXED-size window
;      compaction at each stage; if window-driven, duration stays flat across stages.
;   B. at a FIXED 100k-key keyspace, sweep the window size itself and confirm duration
;      grows roughly linearly with window, confirming the O(window) shape directly (not
;      just "not O(N)").
;   C. shard-thread block time for a single compaction call at 100k keys sized to a
;      5-minute k3s compaction cadence. k3s's default --etcd-compaction-interval is 5m.
;      No production write-rate measurement exists yet for 100k-pod steady state (the
;      only recorded number is ~10 rev/s from the *rung-1 100-pod create burst*,
;      docs/milestones/pod-ladder-results.md line 35 — a creation burst, not steady-state
;      status/lease churn at 100k). We use a documented, deliberately generous assumption
;      of 100 rev/s sustained fleet-wide churn at 100k-pod scale (10x the only measured
;      burst rate, to bias toward "worse than reality") -> a 5-min window is ~30000 revs.
;      mvcc-compact runs synchronously on the shard thread (see mvcc.scm comment above
;      mvcc-compact), so this call's wall time IS the shard block time.
;
; IMPORTANT measurement discipline: each timed compaction is preceded by an UNTIMED
; "catch-up" compact to the current revision, so the timed call always scans EXACTLY the
; freshly-churned W-revision window, never a backlog of older uncompacted history left
; over from keyspace-seeding or earlier rounds. Without this, mvcc-compact's (cur-compact,
; compactRev] window can straddle unrelated old revisions whose KEY-CF groups may carry
; leftover tombstones from prior rounds — a real RocksDB read-amplification effect, but a
; confound for isolating "does cost track window size", which is what this gate asks.
;
; Env overrides (small defaults so CI stays fast; full run uses the values below):
;   CWS_G6_N=100000        final live-key count (default 2000)
;   CWS_G6_WINDOW=1000      fixed window used for the keyspace-scaling check (default 200)
;   CWS_G6_CADENCE=30000    window size for the 5-min-cadence block-time check (default 3000)

(include "test/harness.scm")
(include "src/store-ctx.scm")
(include "src/mvcc.scm")
(include "test/mvcc-util.scm")

(define (env-num name default)
  (let ((v (get-environment-variable name)))
    (if v (string->number v) default)))

(define N        (env-num "CWS_G6_N" 2000))          ; final live-key count
(define WINDOW   (env-num "CWS_G6_WINDOW" 200))       ; fixed window for part A
(define CADENCE  (env-num "CWS_G6_CADENCE" 3000))     ; 5-min-cadence window for part C

(define run-tag (number->string (current-jiffy)))
(define CTX (make-ctx (store-open (string-append "/tmp/cws-bench-compact-" run-tag) #t) "default" #t))
(reset-ctx! CTX)
(define (b s) (string->utf8 s))

(define (put! k v) (mvcc-apply CTX (list (b "PUT") (b k) (b v))))
(define (compact-to! rev) (mvcc-apply CTX (list (b "COMPACT") (b (number->string rev)))))

(define (elapsed-since t0) (/ (- (current-jiffy) t0) (jiffies-per-second)))

; seed `count` NEW live keys (grows keyspace; no churn on existing keys)
(define (seed-new-keys! from count)
  (let loop ((i from))
    (when (< i (+ from count))
      (put! (string-append "seed" (number->string i)) "v")
      (loop (+ i 1)))))

; apply `count` churn PUTs to a rotating band of the ALREADY-seeded keys (creates a
; revision window without growing the keyspace)
(define (churn! keyspace-size count)
  (let loop ((i 0))
    (when (< i count)
      (put! (string-append "seed" (number->string (modulo i keyspace-size))) "v2")
      (loop (+ i 1)))))

; untimed: eliminate any backlog so the NEXT compaction call starts from a clean floor
(define (catch-up!)
  (let ((cur (mvcc-current-rev CTX)) (cr (mvcc-compact-rev CTX)))
    (when (> cur cr) (compact-to! cur))))

; the actual timed measurement: catch up (untimed), churn exactly `w` fresh writes,
; then time compacting exactly that w-sized window.
(define (measure-window! keyspace-size w)
  (catch-up!)
  (churn! keyspace-size w)
  (let* ((floor (+ (mvcc-compact-rev CTX) w))
         (t0 (current-jiffy))
         (r  (compact-to! floor))
         (dt (elapsed-since t0)))
    (cons dt r)))

(section "part A — fixed-size window compaction as keyspace grows 10k -> 50k -> 100k")

(define stage1 (min (quotient N 10) N))
(define stage2 (min (quotient (* N 5) 10) N))
(define stage3 N)

(seed-new-keys! 0 stage1)
(display "  keyspace=") (display stage1) (display " live keys seeded") (newline)
(let* ((m (measure-window! stage1 WINDOW)) (dt (car m)) (r (cdr m)))
  (display "  compact window=") (display WINDOW) (display " at keyspace=") (display stage1)
  (display " -> ") (display dt) (display " s  ") (display r) (newline)
  (check "stage1 compact ok" 'ok (car r)))

(seed-new-keys! stage1 (- stage2 stage1))
(display "  keyspace=") (display stage2) (display " live keys seeded") (newline)
(let* ((m (measure-window! stage2 WINDOW)) (dt (car m)) (r (cdr m)))
  (display "  compact window=") (display WINDOW) (display " at keyspace=") (display stage2)
  (display " -> ") (display dt) (display " s  ") (display r) (newline)
  (check "stage2 compact ok" 'ok (car r)))

(seed-new-keys! stage2 (- stage3 stage2))
(display "  keyspace=") (display stage3) (display " live keys seeded") (newline)
(let* ((m (measure-window! stage3 WINDOW)) (dt (car m)) (r (cdr m)))
  (display "  compact window=") (display WINDOW) (display " at keyspace=") (display stage3)
  (display " -> ") (display dt) (display " s  ") (display r) (newline)
  (check "stage3 compact ok" 'ok (car r)))

(section "part B — at fixed 100k keyspace, sweep window size (should grow ~linearly)")

(define sweep-windows (list (quotient WINDOW 4) WINDOW (* WINDOW 4) (* WINDOW 16)))
(for-each
  (lambda (w)
    (let* ((m (measure-window! stage3 w)) (dt (car m)) (r (cdr m)))
      (display "  window=") (display w) (display " -> ") (display dt) (display " s  ") (display r) (newline)
      (check "sweep compact ok" 'ok (car r))))
  sweep-windows)

(section "part C — shard-block time for one compaction at full keyspace, 5-min-cadence window")

(let* ((m (measure-window! stage3 CADENCE)) (dt (car m)) (r (cdr m)))
  (display "  keyspace=") (display stage3) (display "  cadence-window=") (display CADENCE)
  (display " -> ") (display dt) (display " s (single synchronous shard-thread call)  ") (display r) (newline)
  (check "cadence compact ok" 'ok (car r)))

(done!)
