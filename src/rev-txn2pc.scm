; rev-txn2pc.scm — cw-kp0 Phase 4 core (raft.scm-free): cross-shard transaction
; atomicity + global compaction coordination. Builds on rev-allocator.scm (ADR 0006).
;
; KEY INSIGHT: a multi-key txn spanning shards needs to commit atomically at ONE
; revision R, and no reader/watcher may observe a partial txn. That is EXACTLY the
; low-watermark mechanism generalized: a cross-shard txn CLAIMS one revision R across
; all its participant shards at once; the claim resolves only when EVERY participant
; has applied; while it is pending it pins the watermark at R-1 (just like a single-
; shard in-flight batch at lo=R, but multi-owner). So the same watermark that orders
; cross-shard watches also gives cross-shard-txn atomicity for free — readers gated by
; W never see R until all participants committed.
;
; Compaction ("compact < R" globally) is the dual: R may only be compacted once it is
; fully resolved, i.e. R <= W; the coordinator broadcasts R and requires an ack from
; every shard. PURE / threaded-state (no Raft/IO); the live wiring is the gated remainder.

; --- cross-shard txn branch decision (etcd: all compares true => then, else else) ---
(define (txn-branch compare-results)
  (if (fold-left (lambda (a b) (and a b)) #t compare-results) 'then 'else))

; --- a cross-shard claim on revision R held by a set of participant shards ---
; claim = (cons R pending-shards). Resolves when pending becomes empty.
(define (txn-claim R shards) (cons R shards))
(define (claim-rev c) (car c))
(define (claim-pending c) (cdr c))
(define (claim-resolved? c) (null? (cdr c)))
; one participant applies its slice -> drop it from pending
(define (claim-apply c shard)
  (cons (car c) (filter (lambda (s) (not (equal? s shard))) (cdr c))))

; watermark contribution of a set of OPEN claims (unresolved cross-shard txns) plus
; the single-shard in-flight set. A claim at R pins W at R-1 while pending; resolved
; claims don't constrain. Mirrors rev-allocator's global-watermark, with claims as
; multi-owner in-flight blocks of size 1.
(define (txn-watermark high open-claims single-shard-states)
  (let ((claim-los (map claim-rev (filter (lambda (c) (not (claim-resolved? c))) open-claims)))
        (ss-w (global-watermark high single-shard-states)))
    (if (null? claim-los)
        ss-w
        (min ss-w (- (apply min claim-los) 1)))))

; --- global compaction coordinator ---
; compaction below R is admissible only once R is fully resolved (R <= W): you may
; never discard history a pending txn/batch could still reference.
(define (compact-admissible? R watermark) (<= R watermark))
; completion requires an ack from EVERY shard (no shard left holding old history).
(define (compact-complete? shard-acks shards)
  (fold-left (lambda (a s) (and a (and (memv s shard-acks) #t))) #t shards))
