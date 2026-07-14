; test/bench-range-per-row.scm — cw-71k (G2) per-row range-cost microbench.
;
; Seeds N keys, each with V versions (simulating kubelet status churn: many
; superseded versions per key), then times a full mvcc-range scan at current
; revision and reports ms/row = wall-ms / rows SCANNED (not just rows
; returned — mvcc-range walks every version of every key, so this matches
; the "per-row" cost the gate is about: key-cf-decode-user-key +
; kv-record-decode on the visible winner, tag/mod-rev peeks on the rest).
;
; Not a pass/fail test (no ALL PASS gate) — prints the measured ms/row for
; the report. Run from repo root:
;   crabscheme run test/bench-range-per-row.scm
; Override sizes with CWS_BENCH_KEYS / CWS_BENCH_VERSIONS env vars.

(include "src/store-ctx.scm")
(include "src/mvcc.scm")
(include "test/mvcc-util.scm")

(define run-tag (number->string (current-jiffy)))
(define DB-PATH (string-append "/tmp/cws-bench-range-" run-tag))
(define CTX (make-ctx (store-open DB-PATH #t) "default" #t))
(reset-ctx! CTX)

(define (b s) (string->utf8 s))
(define (put . parts) (mvcc-apply CTX (map b parts)))

(define NKEYS (let ((e (get-environment-variable "CWS_BENCH_KEYS")))
                (if e (or (string->number e) 2000) 2000)))
(define NVERS (let ((e (get-environment-variable "CWS_BENCH_VERSIONS")))
                (if e (or (string->number e) 5) 5)))

(define (pad n)
  (let ((s (number->string n)))
    (string-append (make-string (- 6 (string-length s)) #\0) s)))

; ~2KB filler, roughly pod-status-JSON scale (kubelet churn is the real
; motivating workload — see docs/multiregion-test-plan.md G2).
(define filler (make-string 2048 #\x))

(display "seeding ") (display NKEYS) (display " keys x ") (display NVERS)
(display " versions each...") (newline)

(let loop-v ((v 0))
  (if (< v NVERS)
      (begin
        (let loop-k ((k 0))
          (if (< k NKEYS)
              (begin
                (put "PUT" (string-append "/registry/pods/k" (pad k))
                     (string-append "v" (number->string v) filler))
                (loop-k (+ k 1)))))
        (loop-v (+ v 1)))))

(define total-rows (* NKEYS NVERS))
(display "seeded. total version-rows in KEY-CF = ") (display total-rows) (newline)

; ---- time a full ascending range over the whole prefix, current revision ----
(define lo (b "/registry/pods/"))
(define hi (b "/registry/pods0")) ; exclusive end, one past the '/' byte range

(define REPS 5)
(define t0 (current-jiffy))
(let loop ((i 0))
  (if (< i REPS)
      (begin
        (mvcc-range CTX lo hi '())
        (loop (+ i 1)))))
(define t1 (current-jiffy))

(define elapsed-ms (/ (* (- t1 t0) 1000.0) (jiffies-per-second)))
(define per-run-ms (/ elapsed-ms REPS))
(define ms-per-row (/ per-run-ms total-rows))

(display "REPS=") (display REPS)
(display " total-elapsed-ms=") (display elapsed-ms)
(display " ms/run=") (display per-run-ms)
(display " rows-scanned/run=") (display total-rows)
(display " ms/row=") (display ms-per-row)
(newline)
