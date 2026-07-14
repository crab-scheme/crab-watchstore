; watch-scale-bench.scm — regression guard for cw-l5h: watch-on-apply! must be ~O(matching
; watchers), NOT O(total watchers). Pre-fix it scanned every watcher per write (linear in W:
; W=2000 was ~20 ms/apply), which collapsed write throughput at k8s scale (thousands of
; informer watches) and wedged the apiserver watch-cache. The key-range index makes dispatch
; route each event only to watchers whose range covers it. This asserts near-flat scaling.
;   crabscheme run test/watch-scale-bench.scm
(include "test/harness.scm")
(include "src/store-ctx.scm")
(include "src/mvcc.scm")
(include "src/watch.scm")
(include "test/mvcc-util.scm")
(define (b s) (string->utf8 s))

(define (mk-reg ctx n)
  (let ((reg (make-watch-registry)))
    (let loop ((i 0))
      (when (< i n)
        (let ((p (string-append "p" (number->string i) "/")))
          (watch-register! reg ctx
            (list (cons 'key (b p))
                  (cons 'range-end (b (string-append "p" (number->string i) "0")))
                  (cons 'start-rev 0) (cons 'prev-kv #f)
                  (cons 'filters '()) (cons 'progress-notify #f))
            (lambda (wr) #f)))
        (loop (+ i 1))))
    reg))

(define (ms-per-apply W)
  (let ((ctx (make-ctx (store-open (string-append "/tmp/cws-wsb-" (number->string W) "-"
                                                  (number->string (current-jiffy))) #t) "default" #t)))
    (reset-ctx! ctx)
    (let ((reg (mk-reg ctx W)) (t0 (current-jiffy)))
      (let loop ((i 0))
        (when (< i 200)
          (let ((pre (mvcc-current-rev ctx)))
            (mvcc-apply ctx (list (b "PUT") (b (string-append "p3/x" (number->string i))) (b "v")))
            (watch-on-apply! reg ctx pre (mvcc-current-rev ctx)))
          (loop (+ i 1))))
      (/ (* 1000.0 (/ (- (current-jiffy) t0) (jiffies-per-second))) 200))))

(section "watch-on-apply! scales ~flat with watcher count (indexed, not O(W))")
(define t100  (ms-per-apply 100))
(define t2000 (ms-per-apply 2000))
(display "  W=100: ") (display t100) (display " ms/apply;  W=2000: ") (display t2000)
(display " ms/apply;  ratio=") (display (/ t2000 (max t100 0.0001))) (newline)
; O(W) would be ~20x; the index keeps it well under 6x (slack for hash/bucket overhead + noise).
(check "20x watcher growth stays sub-6x in apply time (not O(W))" #t (< (/ t2000 (max t100 0.0001)) 6.0))
(done!)
