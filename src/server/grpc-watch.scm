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
(define WG-INTERNAL 13)
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

; etcdserverpb.SnapshotResponse{header=1, remaining_bytes=2 uint64, blob=3 bytes}
; (cw-u4a.32).  Defined HERE because the snapshot stream worker (below) lives in this
; runtime; the nested ResponseHeader resolves via proto.scm's schema-ref-table.
(define SnapshotResponse-schema
  (list
    (list 1 'header          '(message ResponseHeader-schema-ref) 'optional)
    (list 2 'remaining_bytes 'uint64 'optional)
    (list 3 'blob            'bytes  'optional)))

; ===========================================================================
; Watch worker — one per /etcdserverpb.Watch/Watch stream
; ===========================================================================
;   (spawn-source "(include \"src/server/grpc-watch.scm\")" 'grpc-watch-worker
;                 H SHARD-PID CLUSTER-ID MEMBER-ID)
; The first WatchRequest is read from the call slot via (grpc-request-bytes h).
(define (grpc-watch-worker h shard-pid cluster-id member-id . rest)
  ; cw-kp0 Phase 3: cross-shard prefix/range watch. A prefix watch over keys that hash to
  ; different shards must register at EVERY shard and MERGE their event streams in global-
  ; revision order. n-shards=1 / node-name="" -> single-shard (the original path, untouched).
  (define n-shards  (if (pair? rest) (car rest) 1))
  (define node-name (if (and (pair? rest) (pair? (cdr rest))) (cadr rest) ""))
  (define (shard-pid-i i) (table-lookup 'ws-shard-pid (string-append node-name ":" (number->string i))))
  (define XSHARD-FLUSH-MS 200)
  (define xshard? #f)            ; this stream registered a watch across all shards
  (define xbuf '())             ; buffered (rev . wr-sexp) awaiting the safe-delivery gate
  (define xgot #f)              ; an event arrived since the last flush tick
  (define xidle 0)             ; consecutive idle flush ticks (no new events)
  (define XQUIESCENT-TICKS 4)   ; ~0.8s of no events -> writes paused; flush held tail in rev order
  (define (xbuf! wr) (set! xbuf (cons (cons (cadr wr) wr) xbuf)) (set! xgot #t))
  ; insertion sort a (rev . wr) list ascending by rev (small lists)
  (define (xsort lst)
    (let ins ((in lst) (out '()))
      (if (null? in) out
          (ins (cdr in)
               (let place ((o out))
                 (cond ((null? o) (list (car in)))
                       ((<= (caar in) (caar o)) (cons (car in) o))
                       (else (cons (car o) (place (cdr o))))))))))
  ; safe delivery point = MIN current-rev across all shards: every shard has applied (and
  ; thus already delivered to us) every event <= that rev, so flushing <= it is gap-free
  ; and in global-rev order. Buffers any watch frames that arrive during the cur-rev poll.
  (define (xmin-rev)
    (let loop ((i 0) (m #f))
      (if (>= i n-shards) (or m 0)
          (let ((p (shard-pid-i i)))
            (if (not p) (loop (+ i 1) m)
                (begin (send p (list 'cur-rev (self)))
                       (let await ()
                         (let ((r (raw-receive)))
                           (cond ((and (pair? r) (eq? (car r) 'cur-rev-ok))
                                  (loop (+ i 1) (if m (min m (cadr r)) (cadr r))))
                                 ((and (pair? r) (eq? (car r) 'watch-response)) (xbuf! (cadr r)) (await))
                                 (else (await)))))))))))
  (define (xflush!)
    (let ((w (xmin-rev)))
      (let split ((in (xsort xbuf)) (held '()))
        (cond ((null? in) (set! xbuf (reverse held)))
              ((<= (caar in) w) (emit-wr! (cdar in)) (split (cdr in) held))
              (else (split (cdr in) (cons (car in) held)))))))
  ; deliver the ENTIRE buffer in rev order — safe ONLY at quiescence (no events for
  ; XQUIESCENT-TICKS, so writes have stopped and every committed event has applied +
  ; arrived). Clears the min-cur-rev gate's idle-shard stall at the workload's drain.
  (define (xflush-all!)
    (for-each (lambda (e) (emit-wr! (cdr e))) (xsort xbuf))
    (set! xbuf '()))
  ; one flush tick: gate normally; if the stream has gone quiescent, flush everything.
  (define (xtick!)
    (if xgot (begin (set! xgot #f) (set! xidle 0)) (set! xidle (+ xidle 1)))
    (if (>= xidle XQUIESCENT-TICKS) (xflush-all!) (xflush!)))
  (define live-wids '())   ; watch_ids established on THIS stream (for teardown)
  (define xshard-regs '())  ; (shard-pid . wid) registered across shards, for teardown
  ; cw-5w8 root-cause #2: once ANY watch on this stream sets progress_notify=true
  ; (etcd's WithProgressNotify), the server owes PERIODIC progress notifications, not
  ; just on-demand RequestProgress replies. The kube-apiserver watch cache marks a
  ; static-resource informer "synced" off these periodic ticks; without them the
  ; KCM controllers' caches (deployments/replicasets/endpoints/...) never reach
  ; synced and the controllers never run. The main loop arms a receive timeout when
  ; this is set and emits a broadcast progress frame on each idle interval (the timer
  ; resets on every real message, so a busy stream — whose events already advance the
  ; revision — does not emit, matching etcd's "suppress when events recently flowed").
  (define any-progress? #f)
  ; ponytail: 5s matches the apiserver progressRequester period; env knob if a
  ; deployment needs it slower (CW_WATCH_PROGRESS_MS) — flat constant until then.
  (define PROGRESS-INTERVAL-MS 5000)

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
           ; wire start_revision -> internal EXCLUSIVE bound (deliver main > bound).
           ; rev>1 -> rev-1; rev=0 -> 0 (the future-only sentinel). rev=1 must NOT
           ; map to 0 (that IS the sentinel — the cw-24e.5 bug: `watch --rev 1`
           ; silently became future-only); it maps to -1, "replay everything".
           (internal-start (cond ((= start-rev 1) -1)
                                 ((> start-rev 1) (- start-rev 1))
                                 (else 0)))
           (spec (append
                   (list (cons 'key key) (cons 'start-rev internal-start)
                         (cons 'prev-kv prev-kv?) (cons 'filters filters)
                         (cons 'progress-notify progress?))
                   (if range-end (list (cons 'range-end range-end)) '())
                   (if client-wid (list (cons 'watch-id client-wid)) '()))))
      (if (and range-end (> n-shards 1))
          ; ---- cross-shard prefix/range watch: register at EVERY shard with one client
          ; wid, buffer all replay, then deliver merged in global-rev order via xflush! ----
          (let ((cw (or client-wid 1))
                ; cw-kp0: a FUTURE-ONLY watch (internal-start 0) would miss events applied
                ; on a shard between the snapshot and that shard's (sequential) registration.
                ; Floor the start-rev at snap-rev so each shard REPLAYS the registration
                ; window (events > snap-rev) — correct for a future-only watch (it wants
                ; everything after the watch point) and closes the cross-shard window gap.
                (xstart (if (= internal-start 0) snap-rev internal-start)))
            (set! xshard? #t)
            (if progress? (set! any-progress? #t))
            (set! live-wids (cons cw live-wids))
            (emit-wr! (list cw snap-rev #t #f "" 0 '()))   ; ONE created ack first
            (let rloop ((i 0))
              (when (< i n-shards)
                (let ((p (shard-pid-i i)))
                  (when p
                    (send p (list 'watch-register (self)
                                  (append (list (cons 'key key) (cons 'start-rev xstart)
                                                (cons 'prev-kv prev-kv?) (cons 'filters filters)
                                                (cons 'progress-notify progress?)
                                                (cons 'range-end range-end) (cons 'watch-id cw)))))
                    (let aw ()
                      (let ((r (raw-receive)))
                        (cond ((not (pair? r)) (aw))
                              ((eq? (car r) 'watch-response) (xbuf! (cadr r)) (aw))
                              ((eq? (car r) 'watch-created)
                               (set! xshard-regs (cons (cons p (cdr r)) xshard-regs)))
                              ((eq? (car r) 'watch-compacted) #t)
                              ((eq? (car r) 'watch-not-leader) #t)   ; nemesis re-establishes
                              (else (aw)))))))
                (rloop (+ i 1))))
            (xflush!))
          ; ---- single-shard (single-group, or a single-key watch) — original path ----
          (begin
            (send shard-pid (list 'watch-register (self) spec))
            (let await ((buffered '()))
              (let ((r (raw-receive)))
                (cond
                  ((not (pair? r)) (await buffered))
                  ((eq? (car r) 'watch-response) (await (cons (cadr r) buffered)))
                  ((eq? (car r) 'watch-created)
                   (let ((wid (cdr r)))
                     (set! live-wids (cons wid live-wids))
                     (if progress? (set! any-progress? #t))
                     (emit-wr! (list wid snap-rev #t #f "" 0 '()))
                     (for-each emit-wr! (reverse buffered))))
                  ((eq? (car r) 'watch-compacted)
                   (emit-wr! (list (if client-wid client-wid 0) 0 #f #t
                                   "mvcc: required revision has been compacted" (cdr r) '())))
                  ((eq? (car r) 'watch-not-leader)
                   (grpc-stream-close! h WG-UNAVAILABLE "etcdserver: no leader"))
                  (else (await buffered)))))))))

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

  ; progress_request (§6, cw-5w8): etcd RequestProgress. The kube-apiserver watch
  ; cache sends this to confirm a watch is caught up to its LIST revision; for an
  ; UNCHANGING resource (no events ever flow) it is the ONLY way the cache reaches
  ; "synced", so the old no-op left those informers hung forever (KCM controllers
  ; stuck on "Waiting for caches to sync"; the scheduler's high-traffic pod/node
  ; watches synced via events, masking it). Reply with a broadcast progress
  ; notification — a WatchResponse with watch_id = -1 and the current store
  ; revision, no events — exactly etcd's ProgressNotify (clientv3 advances every
  ; substream on this stream to that revision). Drain any interleaved watcher
  ; frames while awaiting the shard's cur-rev so delivery order is preserved.
  (define (do-progress)
    ; cw-kp0: a cross-shard watcher asking "am I caught up?" (the workload's drain) -> flush
    ; the held tail in rev order before answering, so the progress reply reflects everything.
    (if xshard? (xflush-all!))
    (send shard-pid (list 'cur-rev (self)))
    (let await ()
      (let ((r (raw-receive)))
        (cond
          ((not (pair? r)) (await))
          ((eq? (car r) 'watch-response) (emit-wr! (cadr r)) (await))
          ((eq? (car r) 'cur-rev-ok)
           (emit-wr! (list -1 (cadr r) #f #f "" 0 '())))
          ((eq? (car r) 'watch-not-leader)
           (grpc-stream-close! h WG-UNAVAILABLE "etcdserver: no leader"))
          (else (await))))))

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
    (if xshard?
        ; cross-shard: cancel the watch at each shard we registered with, drain their acks
        (begin
          (for-each (lambda (pw) (send (car pw) (list 'watch-cancel (self) (cdr pw)))) xshard-regs)
          (let drain ((remaining (length xshard-regs)))
            (if (<= remaining 0)
                (grpc-stream-close! h WG-OK)
                (let ((r (raw-receive)))
                  (if (and (pair? r)
                           (or (eq? (car r) 'watch-canceled) (eq? (car r) 'watch-not-leader)))
                      (drain (- remaining 1))
                      (drain remaining))))))
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
                          (drain remaining)))))))))   ; discard watch-response/stray frames

  (handle-client-msg (grpc-request-bytes h))   ; the first WatchRequest
  (let loop ()
    ; arm the idle timeout only on streams that requested progress_notify; others
    ; block forever (no spurious wakeups, no progress they never asked for).
    (let ((m (cond (xshard?      (raw-receive XSHARD-FLUSH-MS))   ; flush merged events periodically
                   (any-progress? (raw-receive PROGRESS-INTERVAL-MS))
                   (else (raw-receive)))))
      (cond
        ((eq? m '*timeout*) (if xshard? (xtick!) (do-progress)) (loop))   ; merge-flush or progress
        ((not (pair? m)) (loop))
        ((eq? (car m) 'client-msg)     (handle-client-msg (cadr m)) (loop))
        ((eq? (car m) 'watch-response) (if xshard? (xbuf! (cadr m)) (emit-wr! (cadr m))) (loop))
        ((eq? (car m) 'client-end)     (teardown))   ; falls out of the loop -> exit
        (else (loop))))))

; ===========================================================================
; LeaseKeepAlive worker — one per /etcdserverpb.Lease/LeaseKeepAlive stream
; ===========================================================================
;   (spawn-source "(include \"src/server/grpc-watch.scm\")" 'grpc-lease-keepalive-worker
;                 H SHARD-PID CLUSTER-ID MEMBER-ID)
(define (grpc-lease-keepalive-worker h shard-pid cluster-id member-id . _rest)
  (define (ask-shard msg) (send shard-pid msg) (raw-receive))
  ; A header (revision is cosmetic for keepalive); cached at startup so each
  ; keepalive is a single shard round-trip (no header-fetch interleaving).
  (define hdr
    (let ((r (ask-shard (list 'cur-rev (self)))))
      (if (and (pair? r) (eq? (car r) 'cur-rev-ok))
          (wg-make-header cluster-id member-id (cadr r) (caddr r))
          (wg-make-header cluster-id member-id 0 1))))

  ; Process one keepalive request.  Returns #t to keep the stream open, #f after it
  ; has CLOSED the stream (the caller must then stop the receive loop).
  (define (handle-keepalive bytes)
    (let* ((req (pb-decode LeaseKeepAliveRequest-schema bytes))
           (id  (wg-galist 'id req 0))
           (r   (ask-shard (list 'lease-keepalive (self) id))))
      (cond
        ; (keepalive-ok id ttl); ttl=0 => lease gone (etcd's TTL-zero signal).
        ((and (pair? r) (eq? (car r) 'keepalive-ok))
         (grpc-stream-send! h (pb-encode LeaseKeepAliveResponse-schema
                                         (list (cons 'header hdr) (cons 'id id)
                                               (cons 'ttl (caddr r)))))
         #t)
        ; cw-u4a.43: on a FOLLOWER the shard replies (lease-not-leader . L).  REDIRECT
        ; with UNAVAILABLE not-leader — do NOT stream ttl=0, which etcd reads as "the
        ; lease expired" and would make the client drop a still-live lease.  Mirrors the
        ; Watch worker's watch-not-leader close + the KV write path's not-leader status.
        ((and (pair? r) (eq? (car r) 'lease-not-leader))
         (grpc-stream-close! h WG-UNAVAILABLE "etcdserver: not leader")
         #f)
        ; Contract violation (the shard only ever replies keepalive-ok / lease-not-leader).
        (else
         (grpc-stream-close! h WG-INTERNAL "lease-keepalive: unexpected ack")
         #f))))

  ; first LeaseKeepAliveRequest; if it redirected (closed the stream), do not loop.
  (if (handle-keepalive (grpc-request-bytes h))
      (let loop ()
        (let ((m (raw-receive)))
          (cond
            ((not (pair? m)) (loop))
            ((eq? (car m) 'client-msg) (if (handle-keepalive (cadr m)) (loop)))
            ((eq? (car m) 'client-end) (grpc-stream-close! h WG-OK))   ; exit
            (else (loop)))))))

; ===========================================================================
; Snapshot worker — one per /etcdserverpb.Maintenance/Snapshot stream (cw-u4a.32)
; ===========================================================================
;   (spawn-source "(include \"src/server/grpc-watch.scm\")" 'grpc-snapshot-worker
;                 H SHARD-PID CLUSTER-ID MEMBER-ID)
;
; Maintenance/Snapshot is SERVER-STREAMING: one (empty) SnapshotRequest -> many
; SnapshotResponse chunks -> close.  We ask the shard for a CONSISTENT point-in-time
; logical snapshot (the shard is single-threaded, so the read sees no interleaved write),
; frame it as length-prefixed key->value records, append a trailing sha256 of those bytes
; (etcd's Snapshot appends one, and `etcdctl snapshot save` VERIFIES it), and stream it in
; chunks; the client receives io.EOF when we grpc-stream-close! with OK.
;
; LIMITATION (documented): this is a crab-watchstore-NATIVE logical snapshot over the
; RocksDB backend — NOT etcd's bbolt .db file.  `etcdctl snapshot save` DOWNLOADS +
; checksum-verifies it (proving the server-stream works end-to-end), but `etcdctl snapshot
; restore/status` (which parse bbolt) will NOT accept it — restore is crab-watchstore-native.

(define SNAP-CHUNK 32768)   ; 32 KiB logical chunks

; one snapshot record: u64be(len key) ‖ key ‖ u64be(len value) ‖ value
(define (snap-record k v)
  (bytevector-append (u64->bytes (bytevector-length k)) k
                     (u64->bytes (bytevector-length v)) v))

; Pad a bytevector up to the next multiple of 512 with zero bytes.  etcdctl `snapshot save`
; gates the download on hasChecksum(size): (size % 512) == sha256.Size(32) — a real bbolt db
; is 512-sector aligned, then a 32-byte trailing sha256 is appended.  Padding the logical
; body to a 512 multiple makes (padded-len + 32) % 512 == 32, so the save verifies + writes.
(define (snap-pad-512 bv)
  (let ((rem (modulo (bytevector-length bv) 512)))
    (if (= rem 0) bv (bytevector-append bv (make-bytevector (- 512 rem) 0)))))

(define (grpc-snapshot-worker h shard-pid cluster-id member-id)
  ; ask-shard tolerant of a racing client half-close: a server-streaming call's
  ; ('*grpc-stream-end* h) may arrive while we await the shard reply, so loop until
  ; we see one of OUR shard reply tags (discarding any client-end — we close the
  ; stream ourselves once the chunks are sent).
  (define (ask-shard msg)
    (send shard-pid msg)
    (let wait ()
      (let ((r (raw-receive)))
        (if (and (pair? r) (memq (car r) '(cur-rev-ok snapshot-ok))) r (wait)))))
  (define hdr
    (let ((r (ask-shard (list 'cur-rev (self)))))
      (if (and (pair? r) (eq? (car r) 'cur-rev-ok))
          (wg-make-header cluster-id member-id (cadr r) (caddr r))
          (wg-make-header cluster-id member-id 0 1))))
  ; pull the consistent logical snapshot: (snapshot-ok rev ((key . value) ...)).
  (let* ((snap (ask-shard (list 'snapshot (self))))
         (kvs  (if (and (pair? snap) (eq? (car snap) 'snapshot-ok)) (list-ref snap 2) '()))
         ; logical snapshot body (every length-prefixed key->value record, canonical order),
         ; padded to a 512-byte multiple so the etcdctl `snapshot save` size gate accepts it.
         (body (snap-pad-512 (apply bytevector-append
                                    (map (lambda (kv) (snap-record (car kv) (cdr kv))) kvs))))
         ; etcd's Maintenance/Snapshot appends a sha256 of the streamed bytes as the final
         ; chunk; `etcdctl snapshot save` writes db++sha256 and accepts it when (size % 512)
         ; == 32.  We append the REAL sha256(body) (our 32-bit content hash is HashKV's
         ; separate cross-member proof).
         (blob (bytevector-append body (hash-sha256 body)))
         (total (bytevector-length blob)))
    ; stream the blob in chunks; remaining_bytes = bytes left AFTER each chunk.
    (let stream ((off 0))
      (if (< off total)
          (let* ((end       (min total (+ off SNAP-CHUNK)))
                 (chunk      (subbv blob off end))
                 (remaining  (- total end)))
            (grpc-stream-send! h (pb-encode SnapshotResponse-schema
                                            (list (cons 'header hdr)
                                                  (cons 'remaining_bytes remaining)
                                                  (cons 'blob chunk))))
            (stream end))
          (grpc-stream-close! h WG-OK)))))

; ===========================================================================
; Health Watch worker — one per /grpc.health.v1.Health/Watch stream (cw-u4a.33)
; ===========================================================================
;   (spawn-source "(include \"src/server/grpc-watch.scm\")" 'grpc-health-watch-worker
;                 H SHARD-PID CLUSTER-ID MEMBER-ID)
;
; The standard gRPC Health Watch (server-streaming): the server pushes the serving status
; whenever it changes.  MINIMAL impl (DOCUMENTED): emit the CURRENT status ONCE, then HOLD the
; stream open until the client half-closes (emit-once-then-hold).  Re-emit-on-leadership-change
; would need a leadership-change subscription the shard does not expose (it only answers point
; reads), so we do not synthesise it — the unary Check is the live probe; Watch is the stream
; surface.  The HealthCheckResponse schema mirrors grpc-kv.scm's (defined locally here exactly
; like SnapshotResponse-schema above, since this is a separate spawn-source runtime).
(define HealthCheckResponse-schema (list (list 1 'status 'enum 'optional)))
(define HEALTH-SERVING     1)
(define HEALTH-NOT-SERVING 2)

(define (grpc-health-watch-worker h shard-pid cluster-id member-id . _rest)
  (define ended? #f)
  ; READINESS = has-leader AND initialized (applied >= commit), read from the shard's .32
  ; status seam (cw-u4a.33 appended the key-count, unused here).  A client half-close may race
  ; the reply (server-streaming auto-closes the client side after the one request); if it
  ; arrives mid-read we note it and close right after the single emit.
  (define (status-ready?)
    (send shard-pid (list 'status (self)))
    (let wait ()
      (let ((r (raw-receive)))
        (cond
          ((and (pair? r) (eq? (car r) 'status-ok))
           (and (list-ref r 6) (>= (list-ref r 4) (list-ref r 3))))
          ((and (pair? r) (eq? (car r) 'client-end)) (set! ended? #t) (wait))
          (else (wait))))))
  (grpc-stream-send! h (pb-encode HealthCheckResponse-schema
                                  (list (cons 'status (if (status-ready?)
                                                          HEALTH-SERVING HEALTH-NOT-SERVING)))))
  (if ended?
      (grpc-stream-close! h WG-OK)
      (let loop ()
        (let ((m (raw-receive)))
          (cond
            ((not (pair? m)) (loop))
            ((eq? (car m) 'client-end) (grpc-stream-close! h WG-OK))   ; falls out -> exit
            (else (loop)))))))
