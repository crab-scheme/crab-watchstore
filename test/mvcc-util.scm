; test/mvcc-util.scm — shared test helpers for the MVCC unit tests.
;
; reset-ctx! GUARANTEES a deterministically-empty store regardless of whether the
; per-run temp dir was reused.  This matters because the test dir tag is
; (current-jiffy) — PROCESS-RELATIVE uptime — so two separate `crabscheme run`
; invocations reach the run-tag line at ~the same jiffy count and compute the SAME
; small tag, reusing the same /tmp/cws-* dir and accumulating MVCC state across
; runs.  Pre-deleting every key (mirroring store-smoke.scm's "pre-delete at start"
; idiom, but whole-store) makes the tests pass on repeated back-to-back runs with
; NO external cleanup.
;
; Depends on: store-ctx.scm (kv-scan, kv-del!, ctx-flush!) and mvcc.scm
; (NS-META/NS-KEY/NS-REV/NS-LEASE, mvcc-byte).

; Delete every key under a single 1-byte namespace prefix.
(define (reset-ns! ctx tag)
  (for-each (lambda (row) (kv-del! ctx (car row)))
            (kv-scan ctx (mvcc-byte tag))))

; Empty the store: drop all four MVCC namespaces plus the raw raft-applied key
; (which carries no namespace tag, so a per-namespace scan never catches it), then
; flush so the deletes are durable before the test rebuilds state.
(define (reset-ctx! ctx)
  (reset-ns! ctx NS-META)    ; current-rev / compact-rev scalars
  (reset-ns! ctx NS-KEY)     ; key-ordered KeyValue records
  (reset-ns! ctx NS-REV)     ; revision-ordered events
  (reset-ns! ctx NS-LEASE)   ; lease -> keys index
  (kv-del! ctx RAFT-APPLIED-KEY)
  (ctx-flush! ctx))
