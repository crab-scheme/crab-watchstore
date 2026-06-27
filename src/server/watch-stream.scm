; server/watch-stream.scm — the per-connection Watch STREAMING ACTOR (cw-u4a.14).
;
; ADR 0002 §4: the watch registry + apply-side dispatch live on the LEADER's shard
; actor (.13).  The per-connection streaming actor is the CONSUMER: a green actor
; owning ONE connection's watch state — a map watch_id -> watcher-info.  It
;   (1) accepts client requests (WatchCreate / WatchCancel / progress) from an
;       abstract INPUT (its mailbox),
;   (2) forwards register/cancel to the shard registry over the mailbox protocol
;       ((watch-register (self) spec) / (watch-cancel (self) wid)), passing ITSELF
;       as the reply-pid so delivered WatchResponses land on its own mailbox,
;   (3) receives (watch-response WR-SEXP) messages from the shard's deliver-fn and
;       forwards each to the connection's abstract OUTPUT sink — MULTIPLEXING all of
;       this conn's watch_ids over the one output (each WatchResponse already carries
;       its own watch_id),
;   (4) adapts the etcd-wire INCLUSIVE start_revision to the internal EXCLUSIVE
;       contract (ADR 0002 §1 note: internal = wire - 1), and
;   (5) drives progress_notify (§6): an empty WatchResponse advancing header.revision
;       for an idle SYNCED watcher, on a tick/request.
;
; The gRPC bidi transport (.23) WRAPS this actor: it decodes protobuf
; WatchCreate/WatchCancel/WatchProgress into the INPUT mailbox messages below, and
; encodes the OUTPUT-sink WatchResponses (the watch-response->sexp wire shape, see
; src/watch.scm) back onto the stream.  Both stay ABSTRACT here (mailbox in / sink
; out) so .23 only has to bind the two edges.
;
; ===========================================================================
; INPUT — mailbox message shapes (.23 produces these from the gRPC stream)
; ===========================================================================
;   (watch-create REPLY-PID CLIENT-WID WIRE-START-REV SPEC-ALIST)
;       Establish a watch.  CLIENT-WID is the client-chosen watch_id (or #f to let
;       the server assign one).  WIRE-START-REV is the etcd-wire INCLUSIVE
;       start_revision (0 => future-only).  SPEC-ALIST is the rest of the spec as a
;       sendable assoc list (key/range-end/filters/prev-kv).  Replies to REPLY-PID:
;         (watch-create-ok . SERVER-WID)        established (+ caught up)
;       | (watch-create-compacted . COMPACT-REV) start-rev below the floor (§5)
;       | (watch-create-not-leader . LEADER)     this shard isn't the leader (§4)
;   (watch-cancel-req REPLY-PID WID)
;       Cancel WID.  Replies (watch-cancel-ok . WID) | (watch-cancel-not-leader . L).
;   (progress-tick CURRENT-REV)
;       Drive progress_notify (§6): for each idle SYNCED progress_notify watcher,
;       emit an empty WatchResponse with header.revision = CURRENT-REV to the sink.
;       (.23 fires this off the gRPC progress timer / WatchProgressRequest, supplying
;       the shard's current-rev; the cluster test supplies the rev it proposed to.)
;   (watch-response WR-SEXP)
;       A delivered WatchResponse from the shard registry (created / events /
;       canceled / compacted).  Reconstructed + forwarded to the OUTPUT sink.
;   (watch-stream-stop)
;       Teardown: deregister every live watcher from the shard, then exit.
;
; ===========================================================================
; OUTPUT — the abstract sink (.23 swaps the emitter for a gRPC byte-writer)
; ===========================================================================
; The actor never holds a procedure for its sink (procedures can't cross the spawn
; boundary).  Instead the sink is described by two SENDABLE spawn args:
;   OUT-TABLE : a cs-table name (the actor appends each emitted wr-sexp here)
;   OUT-TAG   : a string key prefix scoping this conn's output in that table
; watch-stream-emit! appends the wr-sexp to the list cell (OUT-TABLE, OUT-TAG):
; the test reads it back; .23 overrides watch-stream-emit! to frame bytes onto the
; gRPC stream instead.  This is the ONE seam .23 replaces for output.

(include "src/watch.scm")   ; watch-response<->sexp bridge + record accessors

; ---- OUTPUT sink ----------------------------------------------------------
; Append one wire WatchResponse (a watch-response->sexp list) to the conn's output.
; Stored as a NEWEST-FIRST list in the cs-table cell (OUT-TABLE, OUT-TAG); the
; reader reverses for delivery order.  Multiplexed: every watch_id for this conn
; lands in the same cell, distinguished by the wr-sexp's car (watch_id).
(define (watch-stream-emit! out-table out-tag wr-sexp)
  (let ((cur (table-lookup out-table out-tag)))
    (table-insert! out-table out-tag (cons wr-sexp (if cur cur '())))))

; ---- per-conn watcher info (LOCAL to this actor, never crosses a boundary) ----
; A small mutable record per established watch_id.  synced? + progressed-rev support
; progress_notify (§6): a watch is "idle" for a progress tick when it has had no
; event since its last progress emit (progressed-rev tracks the last header.revision
; we progressed it to, so we never emit a redundant progress frame).
(define-record-type ws-info
  (make-ws-info watch-id synced? progress? progressed-rev)
  ws-info?
  (watch-id       wsi-watch-id)
  (synced?        wsi-synced?        set-wsi-synced?!)
  (progress?      wsi-progress?)
  (progressed-rev wsi-progressed-rev set-wsi-progressed-rev!))

; ===========================================================================
; watch-stream — the actor entry (spawned by .23 / the test, one per connection)
; ===========================================================================
;   (spawn-source "(include \"src/server/watch-stream.scm\")" 'watch-stream
;                 SHARD-PID OUT-TABLE OUT-TAG)
;   SHARD-PID : the leader shard replica's PID (the registry owner)
;   OUT-TABLE : cs-table name for this conn's output sink
;   OUT-TAG   : key prefix scoping this conn in OUT-TABLE
(define (watch-stream shard-pid out-table out-tag)
  ; watch_id -> ws-info for every watch established on THIS connection.
  (define watchers (make-eqv-hashtable))

  (define (emit! wr-sexp) (watch-stream-emit! out-table out-tag wr-sexp))

  ; ---- handle one delivered WatchResponse (forward to the sink, multiplexed) ----
  ; The wr-sexp already carries its watch_id, so forwarding it verbatim to the one
  ; output IS the multiplexing.  We also reconstruct it to update local watcher
  ; state: a created frame marks the watcher synced (it's caught up by the time the
  ; shard's register handoff returns); a canceled frame drops the watcher locally.
  ; events advance nothing local here — delivered_rev lives on the shard's watcher.
  (define (on-watch-response wr-sexp)
    (let* ((wr  (sexp->watch-response wr-sexp))
           (wid (wr-watch-id wr))
           (info (hashtable-ref watchers wid #f)))
      (cond
        ((wr-canceled? wr)
         (if info (hashtable-delete! watchers wid)))
        ((and info (pair? (wr-events wr)))
         ; a real event arrived -> this watch is no longer "idle" for progress; mark
         ; its progressed-rev current so the next progress tick is a fresh checkpoint.
         (set-wsi-progressed-rev! info (wr-header-rev wr))))
      (emit! wr-sexp)))

  ; ---- WatchCreate: wire->internal start-rev, register at the shard ----
  ; The shard's register handoff runs synchronously on ITS thread and delivers any
  ; historical replay to OUR mailbox (as (watch-response ...) frames) BEFORE it acks
  ; the watch-id here.  So by the time we get the id back, replay has been emitted —
  ; but those frames are queued behind THIS request's reply on our own mailbox; we
  ; drain them in the main loop after acking the client.  We synthesise the etcd
  ; immediate CREATED frame to the sink (etcd sends an empty created-response with
  ; header.revision = current) and record the watcher as synced.
  (define (do-watch-create reply-pid client-wid wire-start-rev spec-alist)
    ; wire (INCLUSIVE) -> internal (EXCLUSIVE): deliver revs > internal-start.  A
    ; wire start of 0 stays 0 (future-only sentinel); else internal = wire - 1.
    (let* ((internal-start (if (> wire-start-rev 0) (- wire-start-rev 1) 0))
           (spec (let ((base (cons (cons 'start-rev internal-start) spec-alist)))
                   (if client-wid (cons (cons 'watch-id client-wid) base) base))))
      (send shard-pid (list 'watch-register (self) spec))
      ; wait for the shard's ack to THIS register (watch-created / compacted /
      ; not-leader).  Replay (watch-response) frames may arrive FIRST on our mailbox;
      ; forward them as they come and keep waiting for the register ack.
      (let await ()
        (let ((r (raw-receive)))
          (cond
            ((not (pair? r)) (await))
            ((eq? (car r) 'watch-response) (on-watch-response (cadr r)) (await))
            ((eq? (car r) 'watch-created)
             (let* ((wid (cadr r))           ; cw-l5h: shape is now (watch-created WID CUR-REV)
                    (crev (caddr r))
                    (progress? (let ((c (assq 'progress-notify spec-alist)))
                                 (and c (cdr c)))))
               (hashtable-set! watchers wid (make-ws-info wid #t progress? 0))
               ; etcd's immediate empty CREATED ack on the stream — carry the shard's real rev.
               (emit! (watch-response->sexp
                       (make-watch-response wid crev '() #t #f #f 0)))
               (send reply-pid (cons 'watch-create-ok wid))))
            ((eq? (car r) 'watch-compacted)
             (send reply-pid (cons 'watch-create-compacted (cdr r))))
            ((eq? (car r) 'watch-not-leader)
             (send reply-pid (cons 'watch-create-not-leader (cdr r))))
            (else (await)))))))   ; ignore stray frames while awaiting the ack

  ; ---- WatchCancel: deregister at the shard + locally ----
  (define (do-watch-cancel reply-pid wid)
    (send shard-pid (list 'watch-cancel (self) wid))
    (let await ()
      (let ((r (raw-receive)))
        (cond
          ((not (pair? r)) (await))
          ; the shard's deliver-fn emits the canceled WatchResponse for wid BEFORE
          ; (or around) the cancel ack; forward any frames we see meanwhile.
          ((eq? (car r) 'watch-response) (on-watch-response (cadr r)) (await))
          ((eq? (car r) 'watch-canceled)
           (if (hashtable-ref watchers wid #f) (hashtable-delete! watchers wid))
           (send reply-pid (cons 'watch-cancel-ok (cdr r))))
          ((eq? (car r) 'watch-not-leader)
           (send reply-pid (cons 'watch-cancel-not-leader (cdr r))))
          (else (await))))))

  ; ---- progress_notify (§6): empty WatchResponse advancing header.revision ----
  ; For each idle SYNCED progress_notify watcher whose last progress point is BELOW
  ; current-rev, emit an empty (events=()) WatchResponse at current-rev and remember
  ; it (so we don't re-emit until something moves).  Driven by a (progress-tick rev)
  ; message — the .23 progress timer / the test supplies the live current-rev.
  (define (do-progress-tick current-rev)
    (for-each
     (lambda (info)
       (if (and (wsi-synced? info)
                (wsi-progress? info)
                (< (wsi-progressed-rev info) current-rev))
           (begin
             (emit! (watch-response->sexp
                     (make-watch-response (wsi-watch-id info) current-rev '()
                                          #f #f #f 0)))
             (set-wsi-progressed-rev! info current-rev))))
     (vector->list (hashtable-values watchers))))

  ; ---- teardown: deregister every live watcher, then exit the actor ----
  (define (do-stop)
    (for-each
     (lambda (info)
       (send shard-pid (list 'watch-cancel (self) (wsi-watch-id info))))
     (vector->list (hashtable-values watchers)))
    'stopped)

  ; ---- main loop: pure mailbox dispatch ----
  (let loop ()
    (let ((m (raw-receive)))
      (cond
        ((not (pair? m)) (loop))
        ((eq? (car m) 'watch-create)
         (do-watch-create (cadr m) (caddr m) (cadddr m) (list-ref m 4)) (loop))
        ((eq? (car m) 'watch-cancel-req)
         (do-watch-cancel (cadr m) (caddr m)) (loop))
        ((eq? (car m) 'watch-response)
         (on-watch-response (cadr m)) (loop))
        ((eq? (car m) 'progress-tick)
         (do-progress-tick (cadr m)) (loop))
        ((eq? (car m) 'watch-stream-stop)
         (do-stop))   ; falls out of the loop -> actor exits
        (else (loop))))))
