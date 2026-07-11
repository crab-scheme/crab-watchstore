; server/range-worker.scm — dedicated-thread Range/LIST reader (cw-m9c, G1).
;
; The shard actor's mailbox is single-threaded: a big serializable LIST used
; to run mvcc-range INLINE there, so writes (and everything else) queued
; behind it for the full scan+encode duration (2.8s at 5k rows; ~55s
; projected at 100k). RocksDB itself was never the bottleneck — it's opened
; MultiThreaded (cs-store), so concurrent reads on the SAME handle proceed
; without blocking concurrent writes. This worker owns its own ctx over that
; SAME shared handle (exactly the apply-worker.scm pattern) and does the scan
; off the shard's mailbox entirely.
;
; Protocol (from the owning shard actor only):
;   ('kv-range-do CONN OPTS TERM) -> mvcc-range ctx + reply straight to CONN.
;     The shard has already resolved whatever consistency ordering the read
;     needs (serializable: none; linearizable: leader-gated / forwarded to
;     leader) before dispatching here — this worker only does the read-only
;     scan+encode, never anything that requires shard state.
;
; Do NOT re-attempt pinned-revision chunking here (ca79c2c revert:
; 3.2s->31.5s) — this is the same single non-chunked mvcc-range call the
; shard used to make inline, just off its mailbox.

(include "src/encoding.scm")
(include "src/store-ctx.scm")
(include "src/mvcc.scm")

(define (range-worker-main handle cf sync?)
  (let ((ctx (make-ctx handle cf sync?)))
    (define (range-reply opts term)
      (let* ((key (range-opt opts 'key (make-bytevector 0 0)))
             (rend (range-opt opts 'range-end #f))
             (res (mvcc-range ctx key rend opts)))
        (if (and (pair? res) (eq? (car res) 'err-compacted))
            (list 'kv-range-ok (mvcc-current-rev ctx) term 'compacted 0 '())
            (list 'kv-range-ok (mvcc-current-rev ctx) term #f
                  (car res)
                  (map (lambda (item)
                         (let ((uk (car item)) (rec (cdr item)))
                           (list uk (kv-rec-value rec)
                                 (kv-rec-create-rev rec) (kv-rec-mod-rev rec)
                                 (kv-rec-version rec) (kv-rec-lease rec))))
                       (cdr res))))))
    (let loop ()
      (let ((m (raw-receive)))
        (if (and (pair? m) (eq? (car m) 'kv-range-do))
            (let ((conn (cadr m)) (opts (caddr m)) (term (cadddr m)))
              (guard (e (#t #f)) (send conn (range-reply opts term)))))
        (loop)))))
