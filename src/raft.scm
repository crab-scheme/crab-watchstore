; raft.scm — VENDORED verbatim from crabscheme lib/consensus/raft.scm
; (the pure-Scheme Raft engine; the Rust cs-consensus crate is not
; Scheme-callable, so the cache drives this — consensus itself in Scheme,
; fully §0-aligned). Pure transitions (node,input)->(node'.outputs);
; outputs are (peer-id . rpc). See that file for the full contract.
;
; Raft consensus engine — in CrabScheme.
;
; Per CONSTITUTION.md Article I (the code is Scheme; Rust is the machine),
; the consensus PROTOCOL is pure dispatch and lives here, not in a Rust crate.
; Only the transport (cs-net Channel::Consensus) and actors (cs-actor) are Rust
; primitives.
;
; Article II — this engine is PURE: every transition is
;   (node, input) -> (node' . outputs)
; with no clocks, sockets, or mutation. `outputs` is a list of (peer . message).
; A node value is an association list; messages are tagged lists:
;
;   (rv  term cand last-idx last-term)        ; RequestVote
;   (rvr term granted)                        ; RequestVote reply
;   (ae  term leader prev-idx prev-term entries leader-commit)  ; AppendEntries
;   (aer term success match-idx)              ; AppendEntries reply
;
; A log entry is (term . command). The state machine is a pure function
; (apply-fn state command) -> state'.
;
; The networked driver (spawn a loop that ticks on a timer + steps on
; raw-receive, sending outputs over cs-net) is a design-draft sketched at the
; bottom — it needs cluster send/recv primops not yet wired (same status as
; lib/beam/prelude.scm).

; ============================================================
; assoc-list node helpers (immutable, shadow-update)
; ============================================================

(define (aget al k) (cdr (assq k al)))
; Proper (non-growing) replace — bounds node state to O(fields) instead of
; letting a shadow-cons alist grow O(transitions) and turn lookups quadratic.
(define (aset al k v)
  (cond ((null? al) (list (cons k v)))
        ((eq? (caar al) k) (cons (cons k v) (cdr al)))
        (else (cons (car al) (aset (cdr al) k v)))))
; aset* takes a flat list (k v k v ...) — this dialect has no rest-args.
(define (aset* al kvs)
  (if (null? kvs) al (aset* (aset al (car kvs) (cadr kvs)) (cddr kvs))))

(define (others id ids)                          ; ids minus id  -> peers
  (cond ((null? ids) '())
        ((eqv? (car ids) id) (others id (cdr ids)))
        (else (cons (car ids) (others id (cdr ids))))))

(define (take-n lst n)                            ; first n elements
  (if (or (<= n 0) (null? lst)) '()
      (cons (car lst) (take-n (cdr lst) (- n 1)))))

(define (add-mem x lst) (if (memv x lst) lst (cons x lst)))  ; set-cons (eqv?)

; ============================================================
; node construction + accessors
; ============================================================

(define (make-raft id ids apply-fn sm0)
  (list (cons 'id id) (cons 'peers (others id ids)) (cons 'all ids)
        (cons 'role 'follower) (cons 'term 0) (cons 'voted-for #f)
        (cons 'log '()) (cons 'commit 0) (cons 'applied 0) (cons 'votes '())
        (cons 'next '()) (cons 'match '()) (cons 'apply apply-fn) (cons 'sm sm0)
        ; `base` = highest log index covered by the persisted snapshot (RocksDB
        ; state); the in-memory `log` list holds entries base+1.. only. `base` is
        ; 0 in a fresh node and after no restart, so all helpers below reduce to
        ; the original positional log. It advances on solo compaction and is
        ; restored from RocksDB on restart so committed entries are never
        ; re-applied (idempotent recovery/rejoin).
        (cons 'base 0) (cons 'base-term 0)
        ; CheckQuorum: `heard` = peers whose AER arrived this window (set by
        ; on-aer); `q-ticks` = ticks since the last quorum check (raft-checkquorum).
        ; PreVote: `pre-votes` = pre-vote grants tallied while a `pre-candidate`.
        ; ReadIndex: `rseq` = monotone read-sequence stamped on every AE and echoed
        ; in the AER, so the leader counts only confirmation acks that reply to a
        ; heartbeat sent AFTER a read was issued (Raft §6.4 freshness).
        (cons 'heard '()) (cons 'q-ticks 0) (cons 'pre-votes '()) (cons 'rseq 0)))

(define (raft-id st)      (aget st 'id))
(define (raft-role st)    (aget st 'role))
(define (raft-leader? st) (eq? (aget st 'role) 'leader))
(define (raft-term st)    (aget st 'term))
(define (raft-commit st)  (aget st 'commit))
(define (raft-sm st)      (aget st 'sm))

; ---- log helpers (1-based ABSOLUTE indices; `log` list holds base+1..) ----
(define (log-len st) (+ (aget st 'base) (length (aget st 'log))))
(define (entry-term st i)
  (let ((b (aget st 'base)))
    (cond ((<= i 0) 0)
          ((<= i b) (aget st 'base-term))                 ; at/below the snapshot base
          (else (car (list-ref (aget st 'log) (- i b 1)))))))
(define (last-log-term st) (entry-term st (log-len st)))
(define (entries-from st i)                               ; i in base+1..len+1
  (list-tail (aget st 'log) (- i (aget st 'base) 1)))

(define (majority st) (+ 1 (quotient (length (aget st 'all)) 2)))

; ============================================================
; leader replication helpers
; ============================================================

(define (append-for st peer)
  (let* ((nx (cdr (assq peer (aget st 'next))))
         (prev (- nx 1)))
    (list 'ae (aget st 'term) (aget st 'id) prev (entry-term st prev)
          (entries-from st nx) (aget st 'commit) (aget st 'rseq))))  ; +rseq (ReadIndex)

(define (broadcast-append st)
  (cons st (map (lambda (p) (cons p (append-for st p))) (aget st 'peers))))

(define (become-leader st)
  ; §5.4.2 / §6.4 no-op barrier: a fresh leader appends an empty entry in its OWN
  ; term and commits it, which advances its commit/applied past every prior-term
  ; committed entry (Leader Completeness) — so a ReadIndex read it serves reflects
  ; all committed writes, never stale state from before its election. The no-op's
  ; command is '() (a real command always has a name bv), recognised + skipped by
  ; apply-fn. It also survives node-send replication (unlike nested lists).
  (let* ((st (aset st 'log (append (aget st 'log) (list (cons (aget st 'term) '())))))
         (nx (+ 1 (log-len st)))
         (st (aset* st (list 'role 'leader
                             'next (map (lambda (p) (cons p nx)) (aget st 'peers))
                             'match (map (lambda (p) (cons p 0)) (aget st 'peers))
                             'q-ticks 0 'heard '()))))   ; fresh CheckQuorum lease
    (broadcast-append st)))

; ============================================================
; commit + apply
; ============================================================

(define (count-acks match peers n)
  (if (null? peers) 0
      (+ (if (>= (cdr (assq (car peers) match)) n) 1 0)
         (count-acks match (cdr peers) n))))

(define (apply-committed st)
  (let loop ((st st))
    (if (>= (aget st 'applied) (aget st 'commit)) st
        (let* ((i (+ 1 (aget st 'applied)))
               (cmd (cdr (list-ref (aget st 'log) (- i (aget st 'base) 1))))
               (sm2 ((aget st 'apply) (aget st 'sm) cmd)))
          (loop (aset* st (list 'applied i 'sm sm2)))))))

; Leader: advance commit to the highest index replicated on a quorum AND from
; the current term (Raft §5.4.2), then apply.
(define (maybe-commit st)
  (let loop ((n (log-len st)))
    (cond
      ((<= n (aget st 'commit)) st)
      ((and (= (entry-term st n) (aget st 'term))
            (>= (+ 1 (count-acks (aget st 'match) (aget st 'peers) n)) (majority st)))
       (apply-committed (aset st 'commit n)))
      (else (loop (- n 1))))))

; ============================================================
; public transitions: each returns (node' . outputs)
; ============================================================

(define (raft-campaign st)
  (let* ((term (+ 1 (aget st 'term)))
         (id (aget st 'id))
         (st (aset* st (list 'role 'candidate 'term term 'voted-for id 'votes (list id)))))
    (if (>= (length (aget st 'votes)) (majority st))
        (become-leader st)                       ; single-node: instant majority
        (cons st (map (lambda (p)
                        (cons p (list 'rv term id (log-len st) (last-log-term st))))
                      (aget st 'peers))))))

(define (raft-propose st command)
  (if (not (raft-leader? st))
      (cons st '())
      (broadcast-append
       (aset st 'log (append (aget st 'log) (list (cons (aget st 'term) command)))))))

(define (raft-tick st)
  (if (raft-leader? st) (broadcast-append st) (cons st '())))

; CheckQuorum (Ongaro thesis §6.2): a leader that has NOT been contacted by a
; quorum within an election-timeout `window` steps DOWN to follower. This makes an
; isolated/minority former leader stop believing it leads, so the cache's read
; fast-path (get-fast, gated on cc-shard-leader) stops serving stale values once
; the driver republishes the demotion. `heard` (peers whose AER arrived) is
; accumulated by on-aer; it is reset here every `window` ticks. Pure: a non-leader
; is unchanged, and solo (majority 1, self always counts) renews unconditionally.
(define (raft-checkquorum st window)
  (if (not (raft-leader? st)) st
      (let ((q (+ 1 (aget st 'q-ticks))))
        (if (< q window)
            (aset st 'q-ticks q)                                   ; window not up yet
            (if (>= (+ 1 (length (aget st 'heard))) (majority st))
                (aset* st (list 'q-ticks 0 'heard '()))            ; quorum seen -> renew
                (aset* st (list 'role 'follower 'voted-for #f      ; lost quorum -> step down
                                'q-ticks 0 'heard '())))))))

; PreVote (Ongaro thesis §9.6): before a real election, a timed-out follower sends
; a pre-vote (prv) WITHOUT bumping its term and becomes a `pre-candidate`. A peer
; grants only if it has itself seen no live leader (driver gate: elapsed >= its
; election timeout) and the pre-candidate's log is at least as up-to-date. Only on
; a pre-vote majority does the driver call raft-campaign (which bumps the term).
; This stops a partitioned or momentarily-slow node from disrupting a healthy
; leader via term inflation — the cure for spurious-election churn. Pure: returns
; (pre-candidate-node . prv-outputs).
(define (raft-prevote st)
  (let* ((id (aget st 'id))
         (st (aset* st (list 'role 'pre-candidate 'pre-votes (list id)))))
    (cons st (map (lambda (p)
                    (cons p (list 'prv (+ 1 (aget st 'term)) id
                                  (log-len st) (last-log-term st))))
                  (aget st 'peers)))))

(define (raft-step st from msg)
  (case (car msg)
    ((rv)  (on-rv st msg))
    ((rvr) (on-rvr st from msg))
    ((ae)  (on-ae st msg))
    ((aer) (on-aer st from msg))
    (else  (cons st '()))))

(define (on-rv st msg)
  (let* ((term (list-ref msg 1)) (cand (list-ref msg 2))
         (cidx (list-ref msg 3)) (cterm (list-ref msg 4))
         (st (if (> term (aget st 'term))
                 (aset* st (list 'term term 'role 'follower 'voted-for #f)) st))
         (up (or (> cterm (last-log-term st))
                 (and (= cterm (last-log-term st)) (>= cidx (log-len st)))))
         (grant (and (= term (aget st 'term))
                     (or (not (aget st 'voted-for)) (eqv? (aget st 'voted-for) cand))
                     up))
         (st (if grant (aset st 'voted-for cand) st)))
    (cons st (list (cons cand (list 'rvr (aget st 'term) grant))))))

(define (on-rvr st from msg)
  (let ((term (list-ref msg 1)) (granted (list-ref msg 2)))
    (cond
      ((> term (aget st 'term))
       (cons (aset* st (list 'term term 'role 'follower 'voted-for #f)) '()))
      ((and (eq? (aget st 'role) 'candidate) (= term (aget st 'term)) granted)
       (let* ((votes (if (memv from (aget st 'votes)) (aget st 'votes)
                         (cons from (aget st 'votes))))
              (st (aset st 'votes votes)))
         (if (>= (length votes) (majority st)) (become-leader st) (cons st '()))))
      (else (cons st '())))))

(define (on-ae st msg)
  (let ((term (list-ref msg 1)) (leader (list-ref msg 2))
        (pidx (list-ref msg 3)) (pterm (list-ref msg 4))
        (entries (list-ref msg 5)) (lc (list-ref msg 6))
        (rseq (list-ref msg 7)))                              ; ReadIndex round id to echo
    (if (< term (aget st 'term))
        (cons st (list (cons leader (list 'aer (aget st 'term) #f 0 rseq))))
        (let* ((st (aset* st (list 'term term 'role 'follower)))
               (ok (and (<= pidx (log-len st)) (= (entry-term st pidx) pterm))))
          (if (not ok)
              (cons st (list (cons leader (list 'aer (aget st 'term) #f 0 rseq))))
              (let* ((b (aget st 'base))
                     (kept (take-n (aget st 'log) (- pidx b)))   ; keep base+1..pidx
                     (newlog (append kept entries))
                     (midx (+ pidx (length entries)))
                     (st (aset st 'log newlog))
                     (st (if (> lc (aget st 'commit))
                             (apply-committed (aset st 'commit (min lc (+ b (length newlog)))))
                             st)))
                (cons st (list (cons leader (list 'aer (aget st 'term) #t midx rseq))))))))))

(define (on-aer st from msg)
  (let ((term (list-ref msg 1)) (succ (list-ref msg 2)) (midx (list-ref msg 3)))
    (cond
      ((> term (aget st 'term))
       (cons (aset* st (list 'term term 'role 'follower 'voted-for #f)) '()))
      ((not (and (raft-leader? st) (= term (aget st 'term)))) (cons st '()))
      ; Any AER (success OR rejection) proves this peer reached us this window —
      ; record it for CheckQuorum (raft-checkquorum counts `heard` + self).
      (succ
       (let ((st (aset* st (list 'match (aset (aget st 'match) from midx)
                                 'next (aset (aget st 'next) from (+ midx 1))
                                 'heard (add-mem from (aget st 'heard))))))
         (cons (maybe-commit st) '())))
      (else
       (let* ((nx (cdr (assq from (aget st 'next))))
              (st (aset* st (list 'next (aset (aget st 'next) from (max 1 (- nx 1)))
                                  'heard (add-mem from (aget st 'heard))))))
         (cons st (list (cons from (append-for st from)))))))))

; ============================================================
; deterministic in-Scheme cluster simulator (Article III: prove it)
; ============================================================
;
; A cluster is an alist (id . node). It routes outputs to quiescence with full
; control over delivery — no tokio, no sockets, no wall clock.

(define (cluster-make ids apply-fn sm0)
  (map (lambda (id) (cons id (make-raft id ids apply-fn sm0))) ids))

(define (cluster-get c id) (cdr (assq id c)))
(define (cluster-set c id st) (aset c id st))           ; proper replace (no growth)

; Deliver every queued (from to msg) — and the replies they beget — until none
; remain. Returns the settled cluster.
(define (cluster-settle c queue)
  (if (null? queue) c
      (let* ((m (car queue)) (from (car m)) (to (cadr m)) (msg (caddr m))
             (res (raft-step (cluster-get c to) from msg))
             (c2 (cluster-set c to (car res)))
             (more (map (lambda (o) (list to (car o) (cdr o))) (cdr res))))
        (cluster-settle c2 (append (cdr queue) more)))))

; Run an action (campaign / propose / tick) on one node, then settle.
(define (cluster-drive c id action)
  (let* ((res (action (cluster-get c id)))
         (c2 (cluster-set c id (car res)))
         (q (map (lambda (o) (list id (car o) (cdr o))) (cdr res))))
    (cluster-settle c2 q)))

(define (cluster-campaign c id) (cluster-drive c id raft-campaign))
(define (cluster-propose c id cmd) (cluster-drive c id (lambda (st) (raft-propose st cmd))))
(define (cluster-tick c id) (cluster-drive c id raft-tick))

; ============================================================
; networked driver — DESIGN-DRAFT (needs primops, not yet wired)
; ============================================================
;
; Once cs-runtime exposes the cluster send/recv primops (M02 tail) alongside the
; cs-actor primops (spawn/send/raw-receive/self), a node runs as an actor that
; pumps the SAME pure transitions:
;
;   (define (raft-actor st0 tick-ms)
;     (spawn
;       (lambda ()
;         (let loop ((st st0))
;           (let ((msg (raw-receive tick-ms)))           ; cluster message or timeout
;             (let ((res (if (eq? msg '*timeout*)
;                            (raft-tick st)
;                            (raft-step st (msg-from msg) (msg-body msg)))))
;               (for-each (lambda (o) (cluster-send (car o) (cdr o))) (cdr res))
;               (loop (car res))))))))
;
; `cluster-send` / the inbound framing ride cs-net's Channel::Consensus. Until
; those primops land this is illustrative only — the pure engine above is the
; part that is real and tested.
