; test/grpc-echo-server.scm — the echo handler ACTOR body (cw-u4a.23 smoke).
;
; Proves the .23 STREAMING transport primops in isolation (no etcd semantics):
;   /echo.Echo/Unary        : one request  -> one response                 (grpc-respond!)
;   /echo.Echo/ServerStream : one request  -> THREE responses + status     (grpc-stream-send! x3 + close)
;   /echo.Echo/BidiStream   : N requests    -> N "echo:" responses + status (bidi bridge)
;
; The bidi bridge contract: the handler actor receives ('*grpc-request* h) for the
; FIRST client message, ('*grpc-stream-msg* h bytes) for each subsequent one, and
; ('*grpc-stream-end* h) when the client half-closes.  Responses are queued with
; (grpc-stream-send! h bytes) and the stream ended with (grpc-stream-close! h status).
;
; This is the spawn-source body only (a separate entry runs grpc-serve), so the
; actor never re-spawns/​re-serves itself.

(define (echo-main)
  ; bytes ++ utf8(suffix)
  (define (bv-suffix a suffix)
    (let* ((s (string->utf8 suffix)) (n (bytevector-length a)) (m (bytevector-length s))
           (out (make-bytevector (+ n m) 0)))
      (bytevector-copy! out 0 a 0 n)
      (bytevector-copy! out n s 0 m)
      out))
  ; utf8("echo:") ++ bytes
  (define (echo-prefix bytes)
    (let* ((p (string->utf8 "echo:")) (n (bytevector-length p)) (m (bytevector-length bytes))
           (out (make-bytevector (+ n m) 0)))
      (bytevector-copy! out 0 p 0 n)
      (bytevector-copy! out n bytes 0 m)
      out))

  (let loop ()
    (let ((msg (raw-receive)))
      (cond
        ((not (pair? msg)) (loop))
        ((eq? (car msg) '*grpc-request*)
         (let* ((h (cadr msg)) (path (grpc-request-path h)) (bytes (grpc-request-bytes h)))
           (cond
             ((string=? path "/echo.Echo/Unary")
              (grpc-respond! h bytes))
             ((string=? path "/echo.Echo/ServerStream")
              (grpc-stream-send! h (bv-suffix bytes "-0"))
              (grpc-stream-send! h (bv-suffix bytes "-1"))
              (grpc-stream-send! h (bv-suffix bytes "-2"))
              (grpc-stream-close! h 0))
             ((string=? path "/echo.Echo/BidiStream")
              ; echo the first client message; keep the stream open for the rest
              (grpc-stream-send! h (echo-prefix bytes)))
             (else (grpc-respond-error! h 12 "unimplemented"))))
         (loop))
        ; subsequent bidi client message -> echo it (guard: stream may have closed)
        ((eq? (car msg) '*grpc-stream-msg*)
         (guard (e (#t #f)) (grpc-stream-send! (cadr msg) (echo-prefix (caddr msg))))
         (loop))
        ; client half-closed -> end the stream (no-op if already closed, e.g. ServerStream)
        ((eq? (car msg) '*grpc-stream-end*)
         (guard (e (#t #f)) (grpc-stream-close! (cadr msg) 0))
         (loop))
        (else (loop))))))
