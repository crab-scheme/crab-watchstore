; rev-allocator.scm — synthetic global revision allocator + low-watermark for
; multi-Raft-group crab-watchstore (cw-kp0, ADR 0006).
;
; PROBLEM (ADR 0005 §"the semantic wall"): the multi-shard-group substrate already
; runs N independent Raft groups, but each mints its OWN revision counter — etcd's
; single totally-ordered revision domain is broken for any client touching >1 shard.
; This module is the keystone that restores it: a globally-monotonic revision
; sequence across the N groups, plus the low-watermark that lets a cross-shard
; prefix watch deliver events in strict revision order with no holes.
;
; This file is PURE (no Raft/IO/mutation): the authority counter, per-shard batch
; state, and watcher buffer are plain values threaded by the caller. Phase 2 wires
; these into shard-actor.scm (the authority is the rev-leader's replicated counter;
; the watermark gates watch emission). Keeping the algorithm pure makes the hard
; part — ordering correctness under out-of-order shard commits — unit-testable in
; isolation, before touching the consensus core. See test/rev-allocator.scm.
;
; ---------------------------------------------------------------------------
; The protocol (lease-batched, per-Raft-batch grants — no holes)
; ---------------------------------------------------------------------------
; - One authority holds `high` = the highest revision granted so far (starts 0).
; - A shard about to commit a group-commit batch of K writes asks the authority
;   for a contiguous block of exactly K revisions: (rev-grant high K) ->
;   block [lo, lo+K). Granting per-batch with size = batch length means a settled
;   batch uses ALL its revisions — there are NO intra-batch holes to reconcile.
; - Grants are serialized by the authority and strictly increasing, so blocks are
;   globally ordered and non-overlapping => the revision sequence is monotonic.
; - A shard assigns rev = lo, lo+1, ... to its writes in commit order. While its
;   batch is in-flight (granted but not yet applied+published) the shard is
;   "pending at lo": nothing it will emit is < lo, but lo..lo+K-1 are not yet real.
;
; ---------------------------------------------------------------------------
; The low-watermark (the correctness keystone)
; ---------------------------------------------------------------------------
; A cross-shard watcher must deliver events in strict revision order with no gaps.
; It can safely release an event at revision R only once NO shard can still produce
; an event <= R. That bound is the low-watermark W:
;   - a shard with an IN-FLIGHT batch starting at lo guarantees nothing < lo pending,
;     so it constrains W at lo-1;
;   - an IDLE shard (no in-flight batch) has nothing pending below the global high
;     at all — its next grant will be ABOVE high — so it does NOT pin W at its old
;     last write (the bug a naive min-of-last-rev would have: one idle shard would
;     freeze the watermark forever).
; => W = (min over in-flight shards of lo-1), or `high` when no shard is in-flight.

; (rev-grant high k) -> (cons lo new-high): block [lo, lo+k), new-high = lo+k-1.
(define (rev-grant high k)
  (if (<= k 0)
      (cons (+ high 1) high)                 ; empty grant: no revisions, high unchanged
      (let ((lo (+ high 1)))
        (cons lo (+ high k)))))

; shard batch state for the watermark. A shard is either:
;   (cons 'idle #f)        no in-flight batch
;   (cons 'inflight lo)    holds a granted batch whose lowest revision is lo
(define (shard-idle) (cons 'idle #f))
(define (shard-inflight lo) (cons 'inflight lo))
(define (shard-inflight? s) (eq? (car s) 'inflight))

; (global-watermark high shard-states) -> W, the highest revision safe to release.
; high = authority's current high (all-idle ceiling); shard-states = per-shard batch
; state. Pure min; an all-idle store releases everything (W = high).
(define (global-watermark high shard-states)
  (let loop ((ss shard-states) (w high) (saw-inflight #f))
    (if (null? ss)
        w
        (let ((s (car ss)))
          (if (shard-inflight? s)
              (let ((ceil (- (cdr s) 1)))
                (loop (cdr ss) (if saw-inflight (min w ceil) ceil) #t))
              (loop (cdr ss) w saw-inflight))))))

; ---------------------------------------------------------------------------
; Cross-shard watcher merge: buffer events from all shards, release in revision
; order everything at or below the current watermark. An "event" is (cons rev payload).
; Returns (cons released-in-order remaining-buffer); both kept sorted by rev.
; ---------------------------------------------------------------------------
(define (wm-insert buf ev)                    ; insert ev into rev-sorted buffer
  (cond ((null? buf) (list ev))
        ((< (car ev) (car (car buf))) (cons ev buf))
        (else (cons (car buf) (wm-insert (cdr buf) ev)))))

(define (wm-add-events buf evs)
  (if (null? evs) buf (wm-add-events (wm-insert buf (car evs)) (cdr evs))))

; (wm-release buf watermark) -> (cons released remaining): split the rev-sorted
; buffer at the watermark; `released` are <= watermark (in order), `remaining` > W.
(define (wm-release buf watermark)
  (let loop ((b buf) (out '()))
    (if (or (null? b) (> (car (car b)) watermark))
        (cons (reverse out) b)
        (loop (cdr b) (cons (car b) out)))))
