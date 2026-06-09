; test/harness.scm — check infrastructure for crab-watchstore tests.

(define *checks* 0)
(define *fails* 0)

(define (check name expected actual)
  (set! *checks* (+ *checks* 1))
  (if (equal? expected actual)
      (begin (display "  ok   ") (display name) (newline))
      (begin (set! *fails* (+ *fails* 1))
             (display "  FAIL ") (display name)
             (display "  expected=") (write expected)
             (display "  got=") (write actual) (newline))))

(define (section name)
  (display "== ") (display name) (display " ==") (newline))

(define (done!)
  (newline)
  (display *checks*) (display " checks, ")
  (display *fails*) (display " failed") (newline)
  (if (> *fails* 0)
      (error "TESTS FAILED" *fails*)
      (begin (display "ALL PASS") (newline))))

; coerce strings/symbols/numbers to bytevector arguments
(define (->bv x)
  (cond ((bytevector? x) x)
        ((string? x) (string->utf8 x))
        ((symbol? x) (string->utf8 (symbol->string x)))
        ((number? x) (string->utf8 (number->string x)))
        (else (error "->bv: cannot coerce" x))))
