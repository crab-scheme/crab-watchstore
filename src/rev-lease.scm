; rev-lease.scm — cw-kp0 Phase 2 core (raft.scm-free part): the revision-block
; LEASE + failure-reaping protocol. Builds on src/rev-allocator.scm (ADR 0006).
;
; Phase 1 grants per-batch blocks sized exactly to the batch, so the HAPPY path has
; no holes. Holes only arise on FAILURE: a shard is granted [lo,hi] then dies / loses
; leadership before committing, so those revisions are pending forever and would
; freeze the low-watermark for the whole store. This module is the failure handling:
; grants carry a TTL lease; an unreaped expired lease is reclaimed and its block
; VOIDED (recorded as empty revisions), which drops it from the in-flight set so the
; watermark advances past it. A cross-shard watcher simply finds no events at voided
; revisions (etcd does not require every revision to carry an event for a given key).
;
; PURE / threaded-state (no Raft/IO) — the authority counter + lease set are values
; the caller threads; `now` is supplied (no clock dep, keeps it deterministic + test-
; able). The live wiring (replicate the counter on a rev-leader, run reap on the Raft
; tick) is the gated, Jepsen-validated remainder of Phase 2.

; authority = (cons high leases); lease = (list shard lo hi expiry)
(define (auth-new) (cons 0 '()))
(define (auth-high a) (car a))
(define (auth-leases a) (cdr a))

; (auth-grant a shard n now ttl) -> (cons (cons lo hi) a')
; block is revs lo..hi inclusive (n of them); a lease records it until now+ttl.
(define (auth-grant a shard n now ttl)
  (if (<= n 0)
      (cons (cons (+ (auth-high a) 1) (auth-high a)) a)   ; empty grant, no lease
      (let* ((lo (+ (auth-high a) 1))
             (hi (+ (auth-high a) n))
             (lease (list shard lo hi (+ now ttl))))
        (cons (cons lo hi) (cons hi (cons lease (auth-leases a)))))))

; (auth-commit a shard) -> a' : shard used its block; drop its lease.
(define (auth-commit a shard)
  (cons (auth-high a)
        (filter (lambda (l) (not (equal? (car l) shard))) (auth-leases a))))

; (auth-reap a now) -> (cons voided-ranges a') : reclaim leases whose expiry <= now.
; voided-ranges = list of (cons lo hi) for the abandoned blocks (empty rev spans);
; a' drops those leases so they no longer pin the watermark.
(define (auth-reap a now)
  (let loop ((ls (auth-leases a)) (voids '()) (live '()))
    (if (null? ls)
        (cons (reverse voids) (cons (auth-high a) (reverse live)))
        (let ((l (car ls)))
          (if (<= (list-ref l 3) now)
              (loop (cdr ls) (cons (cons (cadr l) (caddr l)) voids) live)
              (loop (cdr ls) voids (cons l live)))))))

; (auth-watermark a) -> W : the low-watermark from the authority's live leases.
; Binding constraint = (min live-lease lo) - 1; with no live lease, everything
; granted is resolved => W = high. Equivalent to Phase 1's global-watermark, sourced
; from the lease set (the canonical in-flight record) instead of separate shard state.
(define (auth-watermark a)
  (let ((los (map cadr (auth-leases a))))
    (if (null? los) (auth-high a) (- (apply min los) 1))))
