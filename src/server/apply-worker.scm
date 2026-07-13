; src/server/apply-worker.scm — parallel PUT materializer (cw-b5w.4, ADR 0005 B).
;
; One of N dedicated-thread workers owned by a shard actor (the sequencer).
; The sequencer STAMPS committed PUTs (assigns revisions, computes acks, bumps
; current-rev) and hash-routes the heavy half — prev-record lookup, record/event
; encode, RocksDB writes (mvcc-put!) — here.  Key-hash routing guarantees all
; versions of a user key land on ONE worker in revision order, so per-key
; create_rev/version chains stay exactly as the serial path computes them.
;
; Protocol (from the owning shard actor only):
;   ('apply-slice REPLY-PID ops)   ops = list of (K V lease main), rev-ascending
;     -> materialize each via mvcc-put! (explicit main; current-rev untouched —
;        the sequencer owns that), then reply ('apply-slice-done . count).
;
; The sequencer BARRIERS on all slice-done replies before persist-applied!,
; acks, reads, watch emission, or any serial (non-PUT) apply — so nothing
; observes a half-materialized batch (no watermark needed; ADR 0005 §B).
;
; ctx: own dirty counter over the SHARED process-global store handle; the
; sequencer's ctx-flush! fsyncs the DB WAL, covering writes from every worker.

(include "src/safe-send.scm")  ; cw-2au: send-to-dead-pid is a no-op
(include "src/encoding.scm")
(include "src/store-ctx.scm")
(include "src/mvcc.scm")

(define (apply-worker-main handle cf sync?)
  (let ((ctx (make-ctx handle cf sync?)))
    (let loop ()
      (let ((m (raw-receive)))
        (if (and (pair? m) (eq? (car m) 'apply-slice))
            (let ((reply (cadr m)) (ops (caddr m)))
              (for-each
               (lambda (o)
                 (mvcc-put! ctx (car o) (cadr o) (caddr o) (cadddr o) 0))
               ops)
              (send reply (cons 'apply-slice-done (length ops)))))
        (loop)))))
