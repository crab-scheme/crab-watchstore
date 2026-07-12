; server/watch-fanout.scm — dedicated-thread Watch-event fanout worker (cw-04k).
;
; At 10k pods x 3 apiservers, watch-event fanout (registry matching + per-watcher
; delivery, watch-on-apply!/watch-check-compaction!) used to run INLINE on the shard
; actor's apply path, exactly like Range/LIST used to before cw-m9c. A churn burst
; occupied the shard's mailbox for 900-1700ms per decide (CWS_PROF: cons= jumped
; from ~10-30ms), stalling consensus/apply cluster-wide (dual-endpoint probe showed
; multiple nodes stalling at the same second, same magnitude — the shard actor
; simultaneously saturated by the same fanout workload).
;
; This worker OWNS the watch registry entirely (registration, cancel, live dispatch,
; compaction-floor advance, ProgressNotify) off the shard's mailbox. It opens its own
; ctx over the SAME shared RocksDB handle (the range-worker.scm pattern — MultiThreaded,
; concurrent reads never block the sequencer's writes).
;
; ORDERING (etcd guarantee: per-watcher events are gap-free, in revision order, exactly
; once): the shard actor is this worker's ONLY sender. A `send`/`raw-receive` pair
; between one sender and one receiver is FIFO, so every message this worker sees
; arrives in EXACTLY the order the shard actor emitted it — the same order the shard's
; own single-threaded apply loop used to process watch-on-apply!/watch-register!/
; watch-cancel!/watch-check-compaction! inline. Moving the registry here changes WHERE
; that single-threaded serialization happens, not the fact that it stays single-threaded
; and matches the shard's real apply order. That is why watch-register! (which reads
; current-rev to seed a future-only watcher, or replays from REV-CF) is safe here: by
; the time THIS worker dequeues a 'watch-register message, it has already processed
; every 'watch-apply the shard sent before forwarding that client's request — so its
; own ctx's current-rev/REV-CF already reflect exactly what the shard actor's ctx held
; at that same point (the synced-watcher guarantee: no event between a watcher's
; snapshot rev and its registration is missed, because that snapshot rev IS this
; worker's fresh current-rev, not a stale one).
;
; FRESHNESS: like range-worker, this ctx's cached current-rev (shard-ctx-crev) does NOT
; auto-update when the shard writes through ITS OWN ctx to the same handle. A
; 'watch-apply message carries the authoritative POST revision (already durably
; visible in RocksDB — the shard sends it only after mvcc-apply/flush-materializations!
; returns), so we seed the cache directly to POST (cheaper than a re-read and exactly
; as correct). Registration/progress/compaction have no such authoritative value handy,
; so those invalidate to -1 and re-read META-CURRENT-REV, same as range-worker.
;
; BOUNDED MEMORY / BACKPRESSURE: a 'watch-apply message carries only two small ints
; (pre post) — the worker re-reads the actual events from REV-CF itself, so mailbox
; growth under a slow worker is O(inflight-count), not O(event-volume). The shard still
; caps outstanding un-acked 'watch-apply messages (WATCH-FANOUT-MAX-INFLIGHT, enforced
; in shard-actor.scm/quepaxa-shard.scm's watch-notify-apply!) and blocks (draining other
; messages to `backlog`, same idiom as flush-materializations!) rather than growing the
; queue without bound.
;
; Protocol (from the owning shard actor only):
;   ('watch-apply PRE POST)            -> watch-on-apply! (pre,post], then ('watch-apply-ack)
;   ('watch-compact)                   -> watch-check-compaction! (post a COMPACT apply /
;                                          snapshot install)
;   ('watch-progress)                  -> watch-progress-all! (tick-driven ProgressNotify)
;   ('watch-register REPLY-PID SPEC)   -> watch-register!, replies straight to REPLY-PID
;   ('watch-cancel REPLY-PID WATCH-ID) -> watch-cancel!, replies straight to REPLY-PID
;
; Do NOT re-attempt pinned-revision chunking through mvcc-range (ca79c2c) — unrelated
; here, this worker never touches KEY-CF range scans, only REV-CF watch events.

(include "src/encoding.scm")
(include "src/store-ctx.scm")
(include "src/mvcc.scm")
(include "src/watch.scm")

(define (watch-fanout-main handle cf sync?)
  (let ((ctx (make-ctx handle cf sync?))
        (reg (make-watch-registry)))
    (define (invalidate!) (set-shard-ctx-crev! ctx -1))
    (let loop ()
      (let ((m (raw-receive)))
        (guard (e (#t #f))     ; a registry op must never crash this worker (cw-04k)
          (if (pair? m)
              (cond
                ((eq? (car m) 'watch-apply)
                 (let ((pre (cadr m)) (post (caddr m)))
                   (set-shard-ctx-crev! ctx post)   ; authoritative — already durable
                   (watch-on-apply! reg ctx pre post)
                   (send (cadddr m) (list 'watch-apply-ack))))
                ((eq? (car m) 'watch-compact)
                 (invalidate!)
                 (watch-check-compaction! reg ctx))
                ((eq? (car m) 'watch-progress)
                 (invalidate!)
                 (watch-progress-all! reg ctx))
                ((eq? (car m) 'watch-register)
                 (let ((reply-pid (cadr m)) (spec (caddr m)))
                   (invalidate!)
                   (let* ((deliver-fn
                           (lambda (wr)
                             (guard (e (#t #f))
                               (send reply-pid (list 'watch-response (watch-response->sexp wr))))))
                          (res (watch-register! reg ctx spec deliver-fn)))
                     (if (and (pair? res) (eq? (car res) 'compacted))
                         (send reply-pid (cons 'watch-compacted (cdr res)))
                         (send reply-pid (list 'watch-created res (mvcc-current-rev ctx)))))))
                ((eq? (car m) 'watch-cancel)
                 (let ((reply-pid (cadr m)) (wid (caddr m)))
                   (let ((ok (watch-cancel! reg wid)))
                     (send reply-pid (cons 'watch-canceled (if ok wid #f))))))
                (else #f))))
        (loop)))))
