; rev-watch-merge.scm — cw-kp0 Phase 3 core: cross-shard prefix-watch merge gated
; by the global low-watermark. Builds on src/rev-allocator.scm (Phase 1, ADR 0006).
;
; A watcher whose key range spans multiple shard groups receives a separate event
; stream from each group, each in its own assigned-revision order but arriving in
; arbitrary wall-clock order relative to the others. etcd's contract: the watcher
; sees events in strict GLOBAL revision order with no missing event. This driver
; merges the per-shard streams and releases only the prefix that the low-watermark
; W proves is complete (no in-flight shard can still emit a revision <= W).
;
; PURE / threaded-state (no Raft/IO) — like the allocator core, the ordering logic
; is unit-testable in isolation before it is wired into grpc-watch.scm (the live
; wiring is the rest of Phase 3; this is its keystone). Depends on rev-allocator.scm
; being included first (wm-add-events / wm-release / global-watermark).

; merge context: (cons buffer delivered-hi)
;   buffer       : rev-sorted pending events (cons rev payload) not yet releasable
;   delivered-hi : highest revision already released (monotonicity guard; -1 at start)
(define (mc-new) (cons '() -1))
(define (mc-buffer mc) (car mc))
(define (mc-delivered-hi mc) (cdr mc))

; highest revision in a rev-sorted released list (its last element's rev), or #f if empty
(define (released-hi released)
  (cond ((null? released) #f)
        ((null? (cdr released)) (car (car released)))
        (else (released-hi (cdr released)))))

; (mc-step mc new-events high shard-states) -> (cons released mc')
;   new-events  : events newly arrived this step, from any shard(s), any order
;   high        : authority's current granted high (all-idle release ceiling)
;   shard-states: per-shard batch state (shard-idle / shard-inflight lo)
; Ingests new-events, recomputes W, releases every buffered event <= W in revision
; order. Asserts (defensively) that released revisions strictly exceed delivered-hi
; — a violation would mean an out-of-order release (the bug this module prevents).
(define (mc-step mc new-events high shard-states)
  (let* ((buf      (wm-add-events (mc-buffer mc) new-events))
         (w        (global-watermark high shard-states))
         (split    (wm-release buf w))
         (released (car split))
         (remaining (cdr split)))
    (if (and (pair? released) (<= (car (car released)) (mc-delivered-hi mc)))
        (error "rev-watch-merge: out-of-order release"))
    (cons released
          (cons remaining (if (pair? released) (released-hi released) (mc-delivered-hi mc))))))
