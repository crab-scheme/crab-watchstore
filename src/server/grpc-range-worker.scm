; server/grpc-range-worker.scm — async unary Range/LIST responder pool (cw-vku).
;
; handle-range used to run its blocking shard round-trip AND the full
; RangeResponse encode ON the kv dispatcher: one big LIST (k8s relist at 11k
; pods = seconds of scan+encode on a 2-vCPU node) held the dispatcher's
; mailbox, so EVERY other RPC on this node's client port — including sub-ms
; async PUT acks — queued behind it (field cw-vku: 1.9-15s cluster-wide put
; stalls whenever apiservers relist, with consensus itself fast the whole
; time). The dispatcher now stops at decode+authz and hands off:
;   ('range-do H OPTS LIMIT ROUTE)   ROUTE = group index | 'all (scatter)
; Each pool worker does the shard ask(s) with ITS OWN (self) as reply-pid
; (only an actor's own self is globally routable — same lesson as
; grpc-kv.scm's xshard-watch-register!), merges/encodes, and responds
; straight on H (cross-actor respond is the router -> dispatcher pattern).
; Consistency is unchanged: the ask still goes through the shard's
; serializable/linearizable 'kv-range seam; only WHERE the caller blocks
; moved. Response codes mirror handle-range exactly.

(include "src/safe-send.scm")  ; cw-2au: send-to-dead-pid is a no-op
(define QRW-EMPTY (make-bytevector 0 0))
(define QRW-UNAVAILABLE 14)
(define QRW-OUT-OF-RANGE 11)
(define QRW-ERR-COMPACTED "etcdserver: mvcc: required revision has been compacted")

(define (qrw-bv<? a b)
  (let ((la (bytevector-length a)) (lb (bytevector-length b)))
    (let loop ((i 0))
      (cond ((and (= i la) (= i lb)) #f)
            ((= i la) #t)
            ((= i lb) #f)
            ((< (bytevector-u8-ref a i) (bytevector-u8-ref b i)) #t)
            ((> (bytevector-u8-ref a i) (bytevector-u8-ref b i)) #f)
            (else (loop (+ i 1)))))))

(define (qrw-take-n lst n)
  (let loop ((l lst) (k n) (acc '()))
    (if (or (null? l) (= k 0)) (reverse acc) (loop (cdr l) (- k 1) (cons (car l) acc)))))

(define (grpc-range-worker-main node-name shard-pid0 cluster-id member-id shard-groups)
  (define pid-cache (make-vector shard-groups #f))
  (define (group-pid i)
    (or (vector-ref pid-cache i)
        (let ((p (and (> (string-length node-name) 0)
                      (table-lookup 'ws-shard-pid
                                    (string-append node-name ":" (number->string i))))))
          (if p (vector-set! pid-cache i p))
          (or p shard-pid0))))

  ; requests that race in while we block on a shard reply (same buffering
  ; discipline as grpc-kv.scm's ask-shard-on).
  (define pending '())
  (define (ask pid msg)
    (send pid msg)
    (let wait ()
      (let ((r (raw-receive)))
        (if (and (pair? r) (eq? (car r) 'range-do))
            (begin (set! pending (append pending (list r))) (wait))
            r))))

  ; mirror of grpc-kv.scm's shard-range-all: scatter to every group, gather,
  ; merge by user-key, sum totals, max the rev, re-apply the limit post-merge.
  (define (range-all opts limit)
    (let loop ((i 0) (crev 0) (term 1) (total 0) (tuples '()) (compacted #f))
      (if (>= i shard-groups)
          (if compacted (list 'kv-range-ok (if (< crev 1) 1 crev) term 'compacted 0 '())
              (let* ((sorted (list-sort (lambda (a b) (qrw-bv<? (car a) (car b))) tuples))
                     (final (if (and (> limit 0) (> (length sorted) limit))
                                (qrw-take-n sorted limit) sorted)))
                (list 'kv-range-ok (if (< crev 1) 1 crev) term #f total final)))
          (let ((r (ask (group-pid i) (list 'kv-range (self) opts))))
            (if (and (pair? r) (eq? (car r) 'kv-range-ok))
                (loop (+ i 1) (max crev (list-ref r 1)) (list-ref r 2)
                      (+ total (list-ref r 4)) (append (list-ref r 5) tuples)
                      (or compacted (eq? (list-ref r 3) 'compacted)))
                (loop (+ i 1) crev term total tuples compacted))))))

  ; encode + respond on H — the same result mapping as the old inline handle-range.
  (define (respond! h res limit)
    (cond
      ((and (pair? res) (eq? (car res) 'kv-range-ok))
       (let ((cur-rev (let ((r (list-ref res 1))) (if (< r 1) 1 r)))
             (term    (list-ref res 2))
             (err     (list-ref res 3)) (total (list-ref res 4))
             (tuples  (list-ref res 5)))
         (if (eq? err 'compacted)
             (grpc-respond-error! h QRW-OUT-OF-RANGE QRW-ERR-COMPACTED)
             (let ((more (and (> limit 0) (> total (length tuples)))))
               (grpc-respond! h (etcd-pb-encode-range-resp cluster-id member-id cur-rev term
                                                           tuples more total))))))
      (else (grpc-respond-error! h QRW-UNAVAILABLE "etcdserver: not leader"))))

  (let loop ()
    (let ((m (if (pair? pending)
                 (let ((b (car pending))) (set! pending (cdr pending)) b)
                 (raw-receive))))
      (if (and (pair? m) (eq? (car m) 'range-do))
          (let ((h (list-ref m 1)) (opts (list-ref m 2))
                (limit (list-ref m 3)) (route (list-ref m 4)))
            ; a dead/cancelled call handle must not take the pool worker down —
            ; but never drop an error SILENTLY (an encode/protocol bug would
            ; otherwise leave the client hanging until deadline with no trace):
            ; log the condition, then best-effort UNAVAILABLE so the client
            ; fails fast instead of timing out.
            (guard (e (#t
                       (guard (e2 (#t #f))
                         (display (string-append
                                   "ERR grpc-range-worker t=" (number->string (current-second))
                                   " err=" (if (error-object? e)
                                               (error-object-message e)
                                               "non-error condition")
                                   " path=" (grpc-request-path h)))
                         (newline))
                       (guard (e3 (#t #f))
                         (grpc-respond-error! h QRW-UNAVAILABLE
                                              "etcdserver: range worker failure"))))
              (respond! h
                        (if (eq? route 'all)
                            (range-all opts limit)
                            (ask (group-pid route) (list 'kv-range (self) opts)))
                        limit))))
      (loop))))
