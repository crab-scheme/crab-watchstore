; server/grpc-watch.scm — the etcd Watch (bidi) + Lease KeepAlive (bidi) gRPC
; STREAMING workers (cw-u4a.23).
;
; The KV handler actor (grpc-kv.scm) is UNARY: one ('*grpc-request* H) -> one
; grpc-respond!.  A bidi RPC can't be served inline there — a Watch stream is
; long-lived and the server pushes events asynchronously, so blocking the single
; KV dispatcher on it would stall every other call.  Instead, on a streaming
; path the dispatcher SPAWNS one of these per-stream worker actors (passing the
; call handle H), records H -> worker-pid, and forwards the gRPC stream events:
;
;   ('*grpc-stream-msg* H bytes)  --(dispatcher)-->  (client-msg bytes)
;   ('*grpc-stream-end* H)        --(dispatcher)-->  (client-end)
;
; The worker drives the response with the .23 streaming primops:
;   (grpc-stream-send! H response-bytes)   ; queue one response message
;   (grpc-stream-close! H [status [msg]])  ; end the stream with trailers
;
; ---------------------------------------------------------------------------
; Watch worker — the cw-u4a.14 watch-stream CONSUMER bound to a gRPC stream.
; ---------------------------------------------------------------------------
; It speaks the EXACT shard watch-registry mailbox protocol the .14 streaming
; actor uses (watch-stream.scm): it registers/cancels watchers at the leader's
; shard with ITSELF as the reply-pid, so every delivered WatchResponse lands on
; its own mailbox as a flat ('watch-response WR-SEXP) frame (the .14 wire bridge
; shape — records can't cross `send`).  It then encodes each WR-SEXP into a
; WatchResponse protobuf and grpc-stream-send!s it — multiplexing every watch_id
; on this connection over the one gRPC stream.  start_revision is adapted from
; the etcd-wire INCLUSIVE form to the internal EXCLUSIVE contract exactly as .14
; does (internal = wire - 1; 0 stays future-only), and an immediate empty
; created WatchResponse is synthesised on establish (etcd's create ack).
;
; ---------------------------------------------------------------------------
; LeaseKeepAlive worker — one shard round-trip per client keepalive.
; ---------------------------------------------------------------------------
; Each client LeaseKeepAliveRequest{ID} -> (lease-keepalive (self) ID) at the
; leader shard (cw-u4a.18, a leader-local deadline reset, NO Raft round) ->
; LeaseKeepAliveResponse{ID,TTL} streamed back.  TTL=0 signals the lease is gone.

(include "src/encoding.scm")
(include "src/proto.scm")

; ===========================================================================
; small helpers (self-contained; this file is its own spawn-source runtime)
; ===========================================================================

(define WG-EMPTY (make-bytevector 0 0))
(define WG-OK 0)
(define WG-UNAVAILABLE 14)

(define (wg-galist key alist default)
  (let ((c (assq key alist))) (if c (cdr c) default)))

(define (wg-make-header cluster-id member-id revision raft-term)
  (list (cons 'cluster_id cluster-id) (cons 'member_id member-id)
        (cons 'revision revision) (cons 'raft_term raft-term)))

; WatchCreateRequest.filters enum -> internal filter symbol (NOPUT=0, NODELETE=1)
(define (wg-filter->sym n) (if (= n 1) 'nodelete 'noput))

; a kv-view vector #(key create-rev mod-rev version lease value) -> KeyValue alist
(define (wg-kvv->keyvalue v)
  (list (cons 'key             (vector-ref v 0))
        (cons 'create_revision (vector-ref v 1))
        (cons 'mod_revision    (vector-ref v 2))
        (cons 'version         (vector-ref v 3))
        (cons 'lease           (vector-ref v 4))
        (cons 'value           (vector-ref v 5))))

; one wr-sexp event (TYPE KV PREV-KV) -> mvccpb.Event alist
(define (wg-event->event e)
  (let ((type (list-ref e 0)) (kv (list-ref e 1)) (prev (list-ref e 2)))
    (append
      (list (cons 'type (if (eq? type 'del) 1 0))
            (cons 'kv   (wg-kvv->keyvalue kv)))
      (if prev (list (cons 'prev_kv (wg-kvv->keyvalue prev))) '()))))

; a flat WatchResponse sexp (the .14 wire shape) -> WatchResponse alist.
;   (WID HEADER-REV CREATED? CANCELED? CANCEL-REASON COMPACT-REV ((TYPE KV PREV-KV)...))
(define (wg-wr->watchresponse s cluster-id member-id term)
  (let ((wid     (list-ref s 0)) (hrev    (list-ref s 1))
        (created (list-ref s 2)) (cancel  (list-ref s 3))
        (reason  (list-ref s 4)) (compact (list-ref s 5))
        (events  (list-ref s 6)))
    (append
      (list (cons 'header (wg-make-header cluster-id member-id hrev term))
            (cons 'watch_id wid)
            (cons 'created created)
            (cons 'canceled cancel)
            (cons 'compact_revision compact))
      (if (and (string? reason) (> (string-length reason) 0))
          (list (cons 'cancel_reason reason)) '())
      (if (pair? events)
          (list (cons 'events (map wg-event->event events))) '()))))

; ===========================================================================
; Watch worker — one per /etcdserverpb.Watch/Watch stream
; ===========================================================================
;   (spawn-source "(include \"src/server/grpc-watch.scm\")" 'grpc-watch-worker
;                 H SHARD-PID CLUSTER-ID MEMBER-ID)
; The first WatchRequest is read from the call slot via (grpc-request-bytes h).
(define (grpc-watch-worker h shard-pid cluster-id member-id)
  (define live-wids '())   ; watch_ids established on THIS stream (for teardown)

  (define (ask-shard msg) (send shard-pid msg) (raw-receive))
  ; (rev . term) read from the shard — called only when the mailbox is clean
  ; (worker startup / start of a create, before any watcher can deliver).
  (define (rev+term)
    (let ((r (ask-shard (list 'cur-rev (self)))))
      (if (and (pair? r) (eq? (car r) 'cur-rev-ok)) (cons (cadr r) (caddr r)) (cons 0 1))))
  (define cached-term (cdr (rev+term)))

  ; encode + push one wr-sexp frame; #f return = client hung up.
  (define (emit-wr! wr-sexp)
    (grpc-stream-send! h (pb-encode WatchResponse-schema
                                    (wg-wr->watchresponse wr-sexp cluster-id member-id cached-term))))

  ; ---- WatchCreate: wire->internal start-rev, register at the shard, ack ----
  (define (do-create create-req)
    (let* ((snap-rev (car (rev+term)))   ; current rev BEFORE register (mailbox clean)
           (key (wg-galist 'key create-req WG-EMPTY))
           (range-end (let ((re (wg-galist 'range_end create-req WG-EMPTY)))
                        (if (= (bytevector-length re) 0) #f re)))
           (start-rev (wg-galist 'start_revision create-req 0))
           (prev-kv? (wg-galist 'prev_kv create-req #f))
           (progress? (wg-galist 'progress_notify create-req #f))
           (client-wid (let ((w (wg-galist 'watch_id create-req 0))) (if (= w 0) #f w)))
           (filters (map wg-filter->sym (wg-galist 'filters create-req '())))
           (internal-start (if (> start-rev 0) (- start-rev 1) 0))
           (spec (append
                   (list (cons 'key key) (cons 'start-rev internal-start)
                         (cons 'prev-kv prev-kv?) (cons 'filters filters)
                         (cons 'progress-notify progress?))
                   (if range-end (list (cons 'range-end range-end)) '())
                   (if client-wid (list (cons 'watch-id client-wid)) '()))))
      (send shard-pid (list 'watch-register (self) spec))
      ; Await the register ack.  The shard replays historical events as
      ; watch-response frames BEFORE acking the watch-id — but etcd's wire
      ; contract (and clientv3) wants the CREATED response FIRST, then the
      ; historical events.  So BUFFER the replay frames and flush them AFTER the
      ; created ack (newest-first accumulation, reversed on flush to keep order).
      (let await ((buffered '()))
        (let ((r (raw-receive)))
          (cond
            ((not (pair? r)) (await buffered))
            ((eq? (car r) 'watch-response) (await (cons (cadr r) buffered)))
            ((eq? (car r) 'watch-created)
             (let ((wid (cdr r)))
               (set! live-wids (cons wid live-wids))
               ; etcd's immediate empty CREATED ack FIRST...
               (emit-wr! (list wid snap-rev #t #f "" 0 '()))
               ; ...then the buffered historical replay, in revision order.
               (for-each emit-wr! (reverse buffered))))
            ((eq? (car r) 'watch-compacted)
             ; start-rev below the floor: a canceled frame with compact_revision.
             (emit-wr! (list (if client-wid client-wid 0) 0 #f #t
                             "mvcc: required revision has been compacted" (cdr r) '())))
            ((eq? (car r) 'watch-not-leader)
             (grpc-stream-close! h WG-UNAVAILABLE "etcdserver: no leader"))
            (else (await buffered)))))))

  ; ---- WatchCancel: deregister at the shard (it emits the canceled frame) ----
  (define (do-cancel wid)
    (send shard-pid (list 'watch-cancel (self) wid))
    (let await ()
      (let ((r (raw-receive)))
        (cond
          ((not (pair? r)) (await))
          ((eq? (car r) 'watch-response) (emit-wr! (cadr r)) (await))
          ((eq? (car r) 'watch-canceled)
           (set! live-wids (filter (lambda (w) (not (eqv? w wid))) live-wids)))
          ((eq? (car r) 'watch-not-leader)
           (grpc-stream-close! h WG-UNAVAILABLE "etcdserver: no leader"))
          (else (await))))))

  ; progress_request (§6): not exercised by the etcdctl proof; a no-op here
  ; (the synced-watcher progress emit lives in the .14 streaming actor).
  (define (do-progress) #f)

  (define (handle-client-msg bytes)
    (let* ((req (pb-decode WatchRequest-schema bytes))
           (create   (wg-galist 'create_request req #f))
           (cancel   (wg-galist 'cancel_request req #f))
           (progress (wg-galist 'progress_request req #f)))
      (cond
        (create   (do-create create))
        (cancel   (do-cancel (wg-galist 'watch_id cancel 0)))
        (progress (do-progress))
        (else #f))))

  ; teardown: deregister every live watcher at the shard, end the stream, exit.
  ; We must stay alive until each cancel is ACKed: the shard's cancel path sends
  ; us the canceled WatchResponse + the watch-canceled ack (and possibly a final
  ; in-flight event), and `send` to an already-exited actor would CRASH the shard.
  ; So drain our mailbox until every wid is acknowledged, discarding the frames.
  (define (teardown)
    (if (null? live-wids)
        (grpc-stream-close! h WG-OK)
        (begin
          (for-each (lambda (wid) (send shard-pid (list 'watch-cancel (self) wid))) live-wids)
          (let drain ((remaining (length live-wids)))
            (if (<= remaining 0)
                (grpc-stream-close! h WG-OK)
                (let ((r (raw-receive)))
                  (if (and (pair? r)
                           (or (eq? (car r) 'watch-canceled) (eq? (car r) 'watch-not-leader)))
                      (drain (- remaining 1))
                      (drain remaining))))))))   ; discard watch-response/stray frames

  (handle-client-msg (grpc-request-bytes h))   ; the first WatchRequest
  (let loop ()
    (let ((m (raw-receive)))
      (cond
        ((not (pair? m)) (loop))
        ((eq? (car m) 'client-msg)     (handle-client-msg (cadr m)) (loop))
        ((eq? (car m) 'watch-response) (emit-wr! (cadr m)) (loop))
        ((eq? (car m) 'client-end)     (teardown))   ; falls out of the loop -> exit
        (else (loop))))))

; ===========================================================================
; LeaseKeepAlive worker — one per /etcdserverpb.Lease/LeaseKeepAlive stream
; ===========================================================================
;   (spawn-source "(include \"src/server/grpc-watch.scm\")" 'grpc-lease-keepalive-worker
;                 H SHARD-PID CLUSTER-ID MEMBER-ID)
(define (grpc-lease-keepalive-worker h shard-pid cluster-id member-id)
  (define (ask-shard msg) (send shard-pid msg) (raw-receive))
  ; A header (revision is cosmetic for keepalive); cached at startup so each
  ; keepalive is a single shard round-trip (no header-fetch interleaving).
  (define hdr
    (let ((r (ask-shard (list 'cur-rev (self)))))
      (if (and (pair? r) (eq? (car r) 'cur-rev-ok))
          (wg-make-header cluster-id member-id (cadr r) (caddr r))
          (wg-make-header cluster-id member-id 0 1))))

  (define (handle-keepalive bytes)
    (let* ((req (pb-decode LeaseKeepAliveRequest-schema bytes))
           (id  (wg-galist 'id req 0))
           (r   (ask-shard (list 'lease-keepalive (self) id)))
           ; (keepalive-ok id ttl) | (lease-not-leader . L); ttl=0 => lease gone
           (ttl (if (and (pair? r) (eq? (car r) 'keepalive-ok)) (caddr r) 0)))
      (grpc-stream-send! h (pb-encode LeaseKeepAliveResponse-schema
                                      (list (cons 'header hdr) (cons 'id id) (cons 'ttl ttl))))))

  (handle-keepalive (grpc-request-bytes h))   ; the first LeaseKeepAliveRequest
  (let loop ()
    (let ((m (raw-receive)))
      (cond
        ((not (pair? m)) (loop))
        ((eq? (car m) 'client-msg) (handle-keepalive (cadr m)) (loop))
        ((eq? (car m) 'client-end) (grpc-stream-close! h WG-OK))   ; exit
        (else (loop))))))
