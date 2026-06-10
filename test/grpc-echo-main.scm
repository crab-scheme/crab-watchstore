; test/grpc-echo-main.scm — entry for the .23 streaming-transport smoke server.
; Spawns the echo handler actor (test/grpc-echo-server.scm) and starts the h2c
; gRPC server on 127.0.0.1:<--port>, then parks.  Run:
;   crabscheme run test/grpc-echo-main.scm -- --port 32123

(define (arg-after flag default)
  (let loop ((a (command-line)))
    (cond ((or (null? a) (null? (cdr a))) default)
          ((string=? (car a) flag) (cadr a))
          (else (loop (cdr a))))))

(define port (arg-after "--port" "32123"))
(define addr (string-append "127.0.0.1:" port))

; The handler runs on its own dedicated thread; it does no blocking work, but a
; dedicated actor keeps the smoke independent of the green pool.
(define handler (spawn-source-dedicated "(include \"test/grpc-echo-server.scm\")" 'echo-main))
(define sid (grpc-serve addr handler))

(display "echo gRPC serving on ") (display addr) (newline)

(let park () (yield) (park))
