; src/node-watchstore.scm — entry point skeleton.
; Parses argv for an optional --port flag, prints a banner, and exits.
; No networking or etcd logic yet.

(define (parse-port args)
  (let loop ((rest args))
    (cond ((null? rest) 2379)
          ((and (pair? rest) (equal? (car rest) "--port") (pair? (cdr rest)))
           (string->number (cadr rest)))
          (else (loop (cdr rest))))))

(define port (parse-port (command-line)))

(display "crab-watchstore 0.0.1 — etcd-compatible store (skeleton)") (newline)
(display "  port: ") (display port) (newline)
