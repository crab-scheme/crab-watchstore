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
  ; cw-kp0: shard pids arrive as spawn args (grpc-kv passes them, in shard-index order) AFTER
  ; shard-groups + node-name. Spawn-arg pids route from THIS worker (like the group-0 shard-pid
  ; arg the single-shard path uses); the ws-shard-pid table is NOT shared into this nested runtime.
  (define shard-pid-args (if (and (pair? rest) (pair? (cdr rest))) (cddr rest) '()))
  (define (shard-pid-i i)
    (if (< i (length shard-pid-args))
        (list-ref shard-pid-args i)
        (table-lookup 'ws-shard-pid (string-append node-name ":" (number->string i)))))
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
                       ; BOUNDED await: a bare raw-receive here hung the worker forever when a
                       ; shard didn't reply 'cur-rev-ok promptly (end-of-run load) -> the quiescent
                       ; flush was never reached -> the buffered TAIL was never delivered. Time the
                       ; poll out; a non-responding shard just doesn't lower the watermark this tick
                       ; (its events still ship in rev order via the next tick / the quiescent flush).
                       (let await ((spins 0))
                         (let ((r (raw-receive 20)))
                           (cond ((not r) (loop (+ i 1) m))   ; shard silent this tick -> skip it
                                 ((and (pair? r) (eq? (car r) 'cur-rev-ok))
                                  (loop (+ i 1) (if m (min m (cadr r)) (cadr r))))
                                 ((and (pair? r) (eq? (car r) 'watch-response)) (xbuf! (cadr r)) (await (+ spins 1)))
                                 ((> spins 40) (loop (+ i 1) m))   ; hard cap: never wedge the worker (<=0.8s/shard)
                                 (else (await (+ spins 1)))))))))))) ; cw-kp0 FIX: was missing one close — the worker never loaded (parse error), so watch delivery was dead
  ; BATCH a rev-ordered list of single-event wr-sexps into ONE WatchResponse and send it.
  ; cw-kp0 THROUGHPUT FIX: emit-wr! routes through the shared gRPC dispatcher; one send per
  ; event floods it under watch load and starves the write/ack path (write throughput
  ; collapse). etcd allows many events per WatchResponse, so coalesce per flush -> ~1 send
  ; instead of N. wr-sexp = (wid header-rev created? canceled? reason compact events).
  (define (emit-batch! wrs)
    (when (pair? wrs)
      (let ((wid (car (car wrs)))
            (hdr (let mx ((l wrs) (m 0)) (if (null? l) m (mx (cdr l) (max m (cadr (car l)))))))
            (evs (apply append (map (lambda (w) (list-ref w 6)) wrs))))
        (when (pair? evs) (emit-wr! (list wid hdr #f #f "" 0 evs))))))
  (define (xflush!)
    (let ((w (xmin-rev)))
      (let split ((in (xsort xbuf)) (deliver '()) (held '()))
        (cond ((null? in) (emit-batch! (reverse deliver)) (set! xbuf (reverse held)))
              ((<= (caar in) w) (split (cdr in) (cons (cdar in) deliver) held))
              (else (split (cdr in) deliver (cons (car in) held)))))))
  ; deliver the ENTIRE buffer in rev order — safe ONLY at quiescence (no events for
  ; XQUIESCENT-TICKS, so writes have stopped and every committed event has applied +
  ; arrived). Clears the min-cur-rev gate's idle-shard stall at the workload's drain.
  (define (xflush-all!)
    (emit-batch! (map cdr (xsort xbuf)))
    (set! xbuf '()))
  ; one flush tick: track quiescence and flush the buffer when writes pause. The active-
  ; phase min-rev gate (xflush!) is intentionally NOT run here — it polled every shard for
  ; cur-rev each tick, and that worker<->shard round-trip under load competes with the write
  ; path. Buffer during active writing; deliver in rev order at quiescence (and on the
  ; drain's progress probe). Delivery is still gap-free + ordered, just batched.
  ; cw-xq9 round 4: true once this stream has reached REAL quiescence (every shard caught
  ; up, nothing lagging) at least once -- NOT just "xflush-all! was called" (do-progress
  ; also calls xflush-all! unconditionally on every RequestProgress, which only drains
  ; whatever happens to be buffered AT THAT INSTANT and proves nothing about catch-up).
  ; Gates progress replies below (mirrors real etcd's progressIfSync, which sends nothing
  ; at all while any watcher on the stream is unsynced).
  (define xsynced-once? #f)
  (define (xtick!)
    (if xgot (begin (set! xgot #f) (set! xidle 0)) (set! xidle (+ xidle 1)))
    ; NO active-phase poll: xmin-rev's in-band shard round-trip both (a) starved delivery
    ; (under load every 'cur-rev missed its 20ms slot -> watermark 0 -> nothing shipped ->
    ; :observed-put-events 0) and (b) competed with the write path. Buffer during writes;
    ; deliver the whole rev-ordered batch at quiescence. Ordering is safe BECAUSE we only
    ; flush after XQUIESCENT-TICKS of silence (every shard has caught up; no lagging low-rev
    ; event can arrive after a higher one ships). One batched send, no poll.
    (if (>= xidle XQUIESCENT-TICKS) (begin (xflush-all!) (set! xsynced-once? #t))))
  (define live-wids '())   ; watch_ids established on THIS stream (for teardown)
  (define xcreated-pending #f)  ; cw-kp0: a cross-shard created-ack to emit from the loop (deferred)
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
  ; BOUNDED: a bare raw-receive here hung the worker at STARTUP when the shard was mid-batch
  ; and slow to answer 'cur-rev -> the worker never reached registration (no delivery,
  ; :observed-put-events 0, the client re-establishes -> cascade). The term is cosmetic for
  ; the header; fall back to (0 . 1) on timeout and establish immediately.
  (define (rev+term)
    (send shard-pid (list 'cur-rev (self)))
    (let await ((spins 0))
      (let ((r (raw-receive 100)))
        (cond ((and (pair? r) (eq? (car r) 'cur-rev-ok)) (cons (cadr r) (caddr r)))
              ((> spins 5) (cons 0 1))           ; shard busy -> default term, don't hang
              (else (await (+ spins 1)))))))
  (define init-rt (rev+term))
  (define cached-term (cdr init-rt))
  ; cw-l5h: best-known GLOBAL rev for progress frames, so do-progress never has to BLOCK on a
  ; shard cur-rev round-trip (which stalls under write saturation). Advanced by every emitted
  ; frame's header rev (events carry the live rev) and by async 'cur-rev-ok replies (main loop).
  (define last-rev (car init-rt))

  ; encode + push one wr-sexp frame; #f return = client hung up.
  ;
  ; cw-xq9: watch.scm's canceled-response ALWAYS carries header-rev=0 (documented
  ; there as "header-rev unknown to the registry here -> 0; .14 fills the live
  ; current-rev when it owns the stream") -- but nothing ever did that fill-in, so
  ; every ErrCompacted cancellation (watch-check-compaction! mid-stream, or
  ; watch-cancel!'s ack) reached the client with Header.Revision=0. Under k8s 1.36's
  ; ConsistentListFromCache (GA+locked), a reflector recovering from ErrCompacted
  ; relists from ITS OWN watch cache rather than fresh storage -- if that cache's
  ; tracked revision is itself behind (e.g. it hasn't seen a fresh progress/event in
  ; a while) and our cancellation gives it no real revision to jump to, there is no
  ; way to escape: List-from-stale-cache returns a stale RV, Watch resumes below the
  ; compaction floor, ErrCompacted again -- forever (matches the observed 100+
  ; repeated "required revision has been compacted" on one frozen resource while the
  ; store climbed far past it). Patch a REAL current revision onto any 0-header
  ; canceled/compacted frame via a bounded fresh cur-rev round-trip -- rare (only on
  ; cancel/compaction), never on the hot event-delivery path.
  (define (emit-wr! wr-sexp0)
    (let* ((canceled? (list-ref wr-sexp0 3))
           (wr-sexp (if (and canceled? (= (cadr wr-sexp0) 0))
                        (cons (car wr-sexp0) (cons (car (rev+term)) (cddr wr-sexp0)))
                        wr-sexp0))
           (hrev (cadr wr-sexp)))                ; cw-l5h: track the high-water global rev
      (if (> hrev last-rev) (set! last-rev hrev))
      (grpc-stream-send! h (pb-encode WatchResponse-schema
                                      (wg-wr->watchresponse wr-sexp cluster-id member-id cached-term)))))

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
                ; cw-kp0: a FUTURE-ONLY watch (internal-start 0) seeds each shard's
                ; delivered_rev to that shard's current-rev at registration -> it MISSES any
                ; write that committed between the snapshot and that shard's (sequential)
                ; registration. Replay the registration window instead: floor start-rev at
                ; snap-rev (events > snap-rev). On a FRESH store snap-rev=0, and start-rev=0
                ; is the future-only sentinel (no replay) -> it would miss the very first
                ; writes {1..5}; use -1 ("replay from rev 1") there so nothing is skipped. The
                ; replay is bounded (windowed scan, delivered_rev monotone) and was NOT the
                ; write-collapse cause (that was the writer mishandling timeouts).
                ; future-only (internal-start 0) => replay from rev 1 (-1) so NO write is
                ; skipped, even one that lands before the worker reads snap-rev (snap-rev
                ; floor skipped rev<=snap-rev -> early gaps like {2}). A re-establish sends
                ; last-rev+1 (>0) => resumes from there, not a full replay.
                ; future-only (internal-start <=0) => 0, the NO-REPLAY sentinel: the shard acks
                ; 'watch-created instantly so the worker reaches its delivery loop (a replay,
                ; e.g. -1, delayed that ack -> the worker hung in registration ->
                ; :observed-put-events 0). Trade: the brief sequential-registration window may
                ; miss the very first writes; the watcher opens before load so it's small.
                ; A re-establish (last-rev+1 > 0) resumes from there.
                )
            ; cw-kp0: grpc-kv (dispatch! -> xshard-watch-register!) registers this watch at EVERY
            ; shard with OUR pid as reply-pid — the nested worker's own N-shard 'watch-register
            ; never reached the shards (:observed-put-events 0 across 60+ runs; unobservable in the
            ; nested spawn-source). Here we only enter merge mode + ack. Replay/live events arrive
            ; on our mailbox as 'watch-response and ship in rev order via the quiescent flush (xtick!).
            (set! xshard? #t)
            (if progress? (set! any-progress? #t))
            (set! live-wids (cons cw live-wids))
            ; cw-kp0: grpc-kv (xshard-watch-register!) registers this watch at every shard with
            ; ITS OWN (self) as reply-pid and RELAYS each 'watch-response to us by wid — the only
            ; routable path (a nested worker's send never reached the shards; a spawn-return handle
            ; RAISES at the shard). We just enter merge mode + ack; events arrive on our mailbox.
            (set! xcreated-pending (list cw snap-rev #t #f "" 0 '())))
          ; ---- single-shard (single-group, or a single-key watch) — original path ----
          (begin
            (send shard-pid (list 'watch-register (self) spec))
            (let await ((buffered '()))
              (let ((r (raw-receive)))
                (cond
                  ((not (pair? r)) (await buffered))
                  ((eq? (car r) 'watch-response) (await (cons (cadr r) buffered)))
                  ((eq? (car r) 'watch-created)
                   ; cw-l5h: shape is now (watch-created WID CUR-REV). Use the shard's accurate
                   ; current-rev for the created-ack header, NOT the pre-register snap-rev — that
                   ; came from rev+term, which falls back to 0 when the shard mailbox is backed up
                   ; under write load, leaving the apiserver watch-cache stuck at "current: 1".
                   (let ((wid (cadr r))
                         (crev (caddr r)))
                     (set! live-wids (cons wid live-wids))
                     (if progress? (set! any-progress? #t))
                     (emit-wr! (list wid crev #t #f "" 0 '()))
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
  ; substream on this stream to that revision).
  ;
  ; cw-xq9: real etcd's RequestProgress contract is FRESH-AT-REQUEST — the returned
  ; revision reflects what the server has applied AT THE TIME IT RECEIVED the request,
  ; not some earlier cached value. k8s 1.36's consistent-reads-from-cache path
  ; (apiserver/pkg/storage/cacher) blocks on exactly this via waitUntilFreshAndBlock,
  ; comparing the watch cache's tracked resourceVersion against a target it needs
  ; to reach. The OLD implementation here answered IMMEDIATELY at the cached last-rev
  ; and only corrected it ASYNCHRONOUSLY afterward (via the main loop's 'cur-rev-ok
  ; branch) — for a watch whose own keys never change, last-rev never gets refreshed
  ; by an event either, so every RequestProgress reply could keep echoing the SAME
  ; stale revision the watch started with, forever, even while the store's real
  ; revision climbs from unrelated writes (matches apiserver stats.go's observed
  ; "current: 1" pin — [-]informer-sync never completes). 1.31 tolerated this because
  ; it only uses progress bookmarks for lenient "has-synced-eventually" gating, not a
  ; live freshness bound. Block (bounded, ~0.8s cap so a wedged shard can't hang the
  ; worker) for a REAL cur-rev round-trip before answering, buffering any interleaved
  ; watch-response frames so delivery order is preserved (mirrors rev+term / xmin-rev's
  ; existing bounded-await pattern).
  ; cw-xq9 round 3: real etcd's RequestProgress handler (server/etcdserver/api/v3rpc/
  ; watch.go's ProgressRequest case) calls watchStream.RequestProgressAll(), which
  ; sends watchableStore.progressAll() -> progressIfSync(watchers, InvalidWatchID) --
  ; i.e. etcd emits EXACTLY ONE WatchResponse per stream with watch_id = -1 ("Progress
  ; notifications can have WatchID -1 if they announce on behalf of multiple
  ; watchers" -- sendLoop's own comment). The etcd client fans that ONE broadcast
  ; frame out to every substream sharing the grpc stream itself; the server never
  ; addresses individual watch_ids for a progress reply. The old per-wid loop here
  ; sent one frame PER live watch_id instead -- on a stream multiplexing several
  ; watches (apiserver's etcd client coalesces compatible watchers onto shared grpc
  ; streams), only the ONE substream whose watch_id happened to match a given frame
  ; got its cache advanced; every other watcher on that same stream never saw a
  ; progress notify addressed to it and stayed frozen forever (matches "current: 2"
  ; -- exactly one bookmark ever consumed, then nothing). Always send a single
  ; broadcast (-1) frame, never per-wid.
  ; cw-xq9 round 4: real etcd's progressIfSync (server/storage/mvcc/watchable_store.go)
  ; sends NOTHING AT ALL -- not even a stale/best-effort frame -- if any watcher on the
  ; stream is unsynced ("for _, w := range watchers { if _, ok := s.synced.watchers[w];
  ; !ok { return false } }"). A progress bookmark at revision R is a promise that every
  ; event <= R has already been delivered; during a bootstrap catch-up window (a
  ; cross-shard watch's replay hasn't reached quiescence yet) we cannot honor that
  ; promise, and a bookmark that OVERTAKES undelivered events would let a k8s watch
  ; cache latch a revision below events it hasn't seen yet -- exactly the kind of
  ; regression k8s 1.36's ConsistentListFromCache treats as authoritative and can get
  ; permanently stuck on (observed live: caches frozen at rev 2 through a fresh-store
  ; bootstrap write burst). Single-shard watches are always synced by the time their
  ; created-ack is processed (watch-register! replays+promotes synchronously before
  ; acking, and this actor's mailbox is processed sequentially, so no progress_request
  ; can race in before that completes) -- the real risk is cross-shard (xshard?), whose
  ; replay/catch-up is asynchronous; gate on xsynced-once?.
  (define (stream-synced?) (or (not xshard?) xsynced-once?))
  (define (do-progress)
    (define (reply! rev)
      (if (> rev last-rev) (set! last-rev rev))
      (emit-wr! (list -1 last-rev #f #f "" 0 '())))
    ; cw-kp0: a cross-shard watcher asking "am I caught up?" (the workload's drain) -> flush
    ; the held tail in rev order before answering, so the progress reply reflects everything.
    (if xshard? (xflush-all!))
    (if (stream-synced?)
        (begin
          (send shard-pid (list 'cur-rev (self)))
          (let await ((spins 0) (buffered '()))
            (let ((r (raw-receive 20)))
              (cond
                ((and (pair? r) (eq? (car r) 'cur-rev-ok))
                 (for-each emit-wr! (reverse buffered))
                 (reply! (cadr r)))
                ((and (pair? r) (eq? (car r) 'watch-response))
                 (if xshard? (xbuf! (cadr r)) (await (+ spins 1) (cons (cadr r) buffered))))
                ; hard cap: never wedge the worker. cw-xq9 round 4: emit NOTHING here (not a
                ; stale-last-rev reply) -- a timed-out fresh-rev fetch means we genuinely don't
                ; know the current revision right now; a silent skip is spec-conformant (etcd
                ; itself sends nothing when it can't honor the progress promise) and a future
                ; periodic tick / RequestProgress will get a real answer instead of latching a
                ; possibly-stale one.
                ((> spins 40) (for-each emit-wr! (reverse buffered)))
                (else (await (+ spins 1) buffered)))))))) ; unsynced: no reply at all (matches etcd's progressIfSync == false)

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
    (when xcreated-pending (emit-wr! xcreated-pending) (set! xcreated-pending #f))   ; deferred created ack
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
        ; cw-xq9: 'cur-rev-ok is now consumed entirely inside do-progress's own bounded
        ; await loop (fresh-at-request semantics) — nothing else sends 'cur-rev without
        ; consuming its own reply (rev+term, xmin-rev), so this main-loop branch is dead;
        ; any stray reply just falls through to (else (loop)) and is discarded, which is safe.
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

(define (grpc-snapshot-worker h shard-pid cluster-id member-id . ignored)  ; extra spawn args (shard-groups, node-name) unused here
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
