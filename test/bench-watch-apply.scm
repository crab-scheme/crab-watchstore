; bench-watch-apply.scm — confirm whether watch-on-apply!'s full NS-REV scan is O(store)
; per apply. Register a watch, apply PUTs in batches, time each batch. If per-batch time
; GROWS with store size, the full scan is the bottleneck (cw-p86 root-cause check).
(include "test/harness.scm")
(include "src/store-ctx.scm")
(include "src/mvcc.scm")
(include "src/watch.scm")
(include "test/mvcc-util.scm")

(define run-tag (number->string (current-jiffy)))
(define CTX (make-ctx (store-open (string-append "/tmp/cws-bench-" run-tag) #t) "default" #t))
(reset-ctx! CTX)
(define (b s) (string->utf8 s))

; register a current/future watch on prefix "k" so watch-on-apply! has a synced watcher
(define REG (make-watch-registry))
(watch-register! REG CTX
  (list (cons 'key (b "k")) (cons 'range-end (b "l")) (cons 'start-rev 0)
        (cons 'prev-kv #f) (cons 'filters '()) (cons 'progress-notify #f))
  (lambda (wr) #f))   ; deliver-fn: drop (we only measure apply cost)

(define (apply-put! i)
  (let ((pre (mvcc-current-rev CTX)))
    (mvcc-apply CTX (list (b "PUT") (b (string-append "k" (number->string (modulo i 16)))) (b "v")))
    (watch-on-apply! REG CTX pre (mvcc-current-rev CTX))))

(define (time-block start n)
  (let ((t0 (current-jiffy)))
    (let loop ((i start)) (when (< i (+ start n)) (apply-put! i) (loop (+ i 1))))
    (/ (- (current-jiffy) t0) (jiffies-per-second))))

(section "per-100-apply time as the store grows (full-scan => grows; windowed => flat)")
(let loop ((blk 0))
  (when (< blk 8)
    (let ((secs (time-block (* blk 100) 100)))
      (display "  store ~") (display (* blk 100)) (display " revs: 100 applies took ")
      (display secs) (display " s") (newline))
    (loop (+ blk 1))))
(check "benchmark ran" #t #t)
(done!)
