; server/grpc-router.scm — fan ONE registered gRPC handler PID across a POOL of
; identical grpc-kv-main worker actors (cw-6i0).
;
; WHY: the gRPC transport (cw-u4a.20) delivers one ('*grpc-request* H) per unary
; call to ONE registered PID.  A lone grpc-kv-main blocks a full Raft commit
; (~40-76ms) per write inside ask-shard, so throughput ~= 1/commit-latency and the
; shard's group-commit (PROPOSE-BATCH-CAP 64) never sees more than ONE in-flight
; proposal.  N workers each block their OWN ask-shard => N concurrent proposals =>
; group-commit coalesces them into one AE round, amortizing the commit cost across
; the batch.  Each worker keeps the simple blocking model (a tryagain/indeterminate
; ack is still just a gRPC error) — they only block CONCURRENTLY.
;
; ZERO Rust change: the call handle H is a process-global i64 slot (grpc.rs), so any
; worker can (grpc-request-bytes H) / (grpc-respond! H).  The router only reads the
; PATH + token header (both O(1) global lookups, NO protobuf decode) to pick a
; worker, so it is never the bottleneck.
;
; ROUTING:
;   - streaming paths (Watch / LeaseKeepAlive / Snapshot / Health-Watch): the worker
;     keeps per-stream state (stream-workers) across many mailbox messages, so EVERY
;     *grpc-stream-msg* / *grpc-stream-end* for a handle MUST reach the SAME worker.
;     Round-robin the OPENing *grpc-request*, record handle->worker, then route the
;     follow-ups by that map.
;   - auth: the token table is worker-LOCAL (leader-local ephemeral `simple` tokens),
;     so a token minted on one worker is invisible to the others.  Pin every /Auth/*
;     call AND every token-bearing request to worker 0 (auth OFF => never triggers;
;     auth ON => the pool degrades to a single worker, which is correct).
;   - everything else: round-robin across the pool.
;
; Spawn (node-cluster.scm):
;   (spawn-source-dedicated "(include \"src/server/grpc-router.scm\")" 'grpc-router-main
;                           shard-pid cluster-id member-id cluster-members my-name pool-size)
; Then (grpc-serve "host:port" <this-router-pid>) routes etcd calls through it.

(define GRPC-UNAVAILABLE-ROUTER 14)   ; gRPC UNAVAILABLE (client retries)

(define (grpc-router-main shard-pid cluster-id member-id cluster-members my-node-name pool-size . rest)
  ; cw-ivt: shard-groups (number of Raft groups) threaded to each worker so it can
  ; resolve the per-key shard pid. Default 1 (single group).
  (define shard-groups (if (and (pair? rest) (number? (car rest))) (car rest) 1))
  ; ---- spawn the worker pool: each worker is the FULL grpc-kv-main body ----
  ; Identical args to the pre-pool single-handler spawn (node-cluster.scm), so a
  ; worker behaves exactly as the old lone handler did.
  ; pool-size arrives via string->number (--grpc-workers), so coerce to an exact
  ; positive fixnum the vector/loop bounds can rely on.
  (define pool (if (and (fixnum? pool-size) (> pool-size 0)) pool-size 32))
  (define workers (make-vector pool #f))
  (define n (vector-length workers))
  (define (worker i) (vector-ref workers (modulo i n)))

  ; round-robin cursor for unary calls.
  (define rr 0)
  (define (next-worker!)
    (let ((w (worker rr))) (set! rr (+ rr 1)) w))

  ; handle -> worker pid, held only for the lifetime of a streaming call.
  (define stream-routes (make-eqv-hashtable))

  (define (string-prefix? p s)
    (and (>= (string-length s) (string-length p))
         (string=? (substring s 0 (string-length p)) p)))

  (define (streaming-path? path)
    (or (string=? path "/etcdserverpb.Watch/Watch")
        (string=? path "/etcdserverpb.Lease/LeaseKeepAlive")
        (string=? path "/etcdserverpb.Maintenance/Snapshot")
        (string=? path "/grpc.health.v1.Health/Watch")))

  ; pin auth-management + any token-bearing request to worker 0 (token-table is
  ; worker-local; see header).  Cheap: path string + one O(1) metadata lookup.
  (define (auth-pinned? path h)
    (or (string-prefix? "/etcdserverpb.Auth/" path)
        (let ((tok (grpc-request-metadata h "token")))
          (and tok (> (string-length tok) 0)))))

  ; ---- spawn the pool (an expression, so AFTER all internal defines) ----
  (let build ((i 0))
    (when (< i pool)
      (vector-set! workers i
                   (spawn-source-dedicated "(include \"src/server/grpc-kv.scm\")"
                                           'grpc-kv-main
                                           shard-pid cluster-id member-id
                                           cluster-members my-node-name shard-groups))
      (build (+ i 1))))

  (display "node ") (display my-node-name)
  (display ": grpc-kv worker pool = ") (display n) (display " workers") (newline)

  (let loop ()
    (let ((m (raw-receive)))
      (cond
        ((not (pair? m)) (loop))
        ; a fresh unary/stream-opening call: choose a worker, route it.
        ((eq? (car m) '*grpc-request*)
         (let* ((h    (cadr m))
                (path (grpc-request-path h))
                (w    (if (auth-pinned? path h) (worker 0) (next-worker!))))
           (when (streaming-path? path)
             (hashtable-set! stream-routes h w))
           (guard (e (#t (grpc-respond-error! h GRPC-UNAVAILABLE-ROUTER
                                              "router: worker unavailable")))
             (send w m)))
         (loop))
        ; a follow-up client-streamed message -> the worker that owns the stream.
        ((eq? (car m) '*grpc-stream-msg*)
         (let ((w (hashtable-ref stream-routes (cadr m) #f)))
           (when w (guard (e (#t #f)) (send w m))))
         (loop))
        ; client half-closed -> forward + forget the route.
        ((eq? (car m) '*grpc-stream-end*)
         (let ((w (hashtable-ref stream-routes (cadr m) #f)))
           (when w (guard (e (#t #f)) (send w m)))
           (hashtable-delete! stream-routes (cadr m)))
         (loop))
        (else (loop))))))
