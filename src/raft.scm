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

; `ids` is the INITIAL VOTER SET (back-compat: existing 4-arg callers pass the
; full member list as the voters; no learners). The voter set is DYNAMIC from here
; on (cw-u4a.28 joint consensus): `voters` is the current/incoming voter set,
; `voters-old` is #f when the config is SIMPLE and the OUTGOING voter set while a
; joint config is in flight, and `learners` are non-voting members that receive
; AppendEntries but never count for quorum/elections. `peers` (everyone the leader
; replicates to = voters ∪ voters-old ∪ learners, minus self) and `all` (= voters,
; kept so the legacy `majority` reader stays correct for a fixed/simple config) are
; DERIVED and recomputed by `recompute-config` whenever a ConfChange entry is
; appended or truncated. `base-voters`/`base-voters-old`/`base-learners` snapshot
; the config covered by `base` (the compacted prefix), so a truncation that drops
; every in-memory ConfChange reverts to the snapshot's config.
; `ids` = the genesis VOTER set. Optional trailing `learners` (cw-85j) = the
; genesis NON-VOTING members: they receive AppendEntries (so they are in `peers`)
; but never count for quorum/elections (NOT in `voters`/`all`). Omitting it is the
; original behaviour byte-for-byte (learners '(), peers = others id ids). A
; genesis learner lets a WAN deployment seat a local-majority voter set (e.g. 5
; voters with 3 in the leader region) while far-region members ride along as
; learners off the commit critical path.
(define (make-raft id ids apply-fn sm0 . rest)
  (let ((learners (if (pair? rest) (car rest) '())))
  (list (cons 'id id) (cons 'peers (others id (append ids learners))) (cons 'all ids)
        (cons 'voters ids) (cons 'voters-old #f) (cons 'learners learners)
        (cons 'base-voters ids) (cons 'base-voters-old #f) (cons 'base-learners learners)
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
        (cons 'heard '()) (cons 'q-ticks 0) (cons 'pre-votes '()) (cons 'rseq 0)
        ; snap-req: peers a leader must catch up with a STORE snapshot — they
        ; rejected AppendEntries at the compaction floor (base+1), so no log
        ; replay can reach them (cw-lkq.15). The hosting actor ships ws-snap
        ; and clears this via raft-clear-snap-req.
        (cons 'snap-req '()))))

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

(define (drop-at-most lst n)                              ; list-tail, short-list safe
  (if (or (<= n 0) (null? lst)) lst (drop-at-most (cdr lst) (- n 1))))

; ---- multi-voter log compaction + snapshot install (cw-lkq.15) ----
;
; raft-compact-to: drop in-memory entries <= floor (their effects are applied in
; the store, which IS the snapshot). Pure; no-op unless base < floor <= applied.
; Absolute indexing is preserved (base advances with the same offset helpers
; above), so pending/conf bookkeeping keyed by absolute index is unaffected.
(define (raft-compact-to st floor)
  (let ((b (aget st 'base)))
    (if (or (<= floor b) (> floor (aget st 'applied))) st
        (aset* st (list 'base floor
                        'base-term (entry-term st floor)
                        'log (list-tail (aget st 'log) (- floor b)))))))

; raft-install-snapshot: a follower adopts a leader's store snapshot at `base`
; (its log can no longer be reconciled below the leader's compaction floor).
; The caller must have already replaced the persistent store contents.
(define (raft-install-snapshot st base term)
  (aset* st (list 'base base 'base-term term 'log '()
                  'commit base 'applied base 'sm base 'role 'follower)))

(define (raft-snap-requests st) (aget st 'snap-req))
(define (raft-clear-snap-req st) (aset st 'snap-req '()))

; Legacy fixed-config majority (count threshold), still used by the actor
; (shard-actor) and PreVote/ReadIndex tallies, which only ever run a SIMPLE config
; where `all` = the voter set. Joint-aware decisions use the predicates below.
(define (majority st) (+ 1 (quotient (length (aget st 'all)) 2)))

; ============================================================
; dynamic membership: config representation + joint quorum (cw-u4a.28)
; ============================================================
;
; ConfChange log entries (Ongaro thesis §4.3). A log entry is (term . command);
; a ConfChange command is the tagged pair (cons 'conf SPEC) — distinguishable from
; the no-op barrier ('()) and from a real command (a list of bytevectors), and
; flat enough to survive node-send (symbols + nested lists round-trip through
; to_sendable_in/from_sendable). SPEC is one of:
;   (joint  OLD NEW LEARNERS)  ; enter joint consensus (Cold,new)
;   (simple NEW LEARNERS)      ; leave joint / single-phase change (Cnew)
(define (conf-cmd? cmd) (and (pair? cmd) (eq? (car cmd) 'conf)))
(define (conf-spec cmd) (cdr cmd))

; --- small set utilities (eqv? element identity; the dialect has no `filter`) ---
(define (subset? a b)
  (cond ((null? a) #t) ((memv (car a) b) (subset? (cdr a) b)) (else #f)))
(define (equal-set? a b) (and (subset? a b) (subset? b a)))
(define (count-in members ids)                ; how many of `members` are in `ids`
  (cond ((null? members) 0)
        ((memv (car members) ids) (+ 1 (count-in (cdr members) ids)))
        (else (count-in (cdr members) ids))))
(define (dedup lst)                            ; keep the LAST occurrence of each id
  (cond ((null? lst) '())
        ((memv (car lst) (cdr lst)) (dedup (cdr lst)))
        (else (cons (car lst) (dedup (cdr lst))))))
(define (union3 a b c) (dedup (append a (if b b '()) c)))
(define (assq-def k al default) (let ((e (assq k al))) (if e (cdr e) default)))

; A node is a voter iff it appears in the current OR outgoing voter set. Used to
; gate elections so a learner / removed node never campaigns.
(define (is-voter? st id)
  (and (or (memv id (aget st 'voters))
           (let ((old (aget st 'voters-old))) (and old (memv id old))))
       #t))

; --- joint quorum (Ongaro §4.3): a decision needs a STRICT majority of the NEW
;     voters AND, while joint, a strict majority of the OLD voters. Learners never
;     count (they are in neither set). 2*count > N is a strict majority of N. ---
(define (set-majority? voters granted) (> (* 2 (count-in voters granted)) (length voters)))
(define (votes-quorum? st granted)            ; election tally (granted = voter ids)
  (and (set-majority? (aget st 'voters) granted)
       (let ((old (aget st 'voters-old))) (or (not old) (set-majority? old granted)))))
; commit tally: a voter "acks" index n if it is self (the leader always holds it)
; or its matchIndex >= n. Every voter (minus self) is a peer, hence present in
; `match` after sync-progress, so count-acks never sees a missing key.
(define (voter-majority? st voters n)
  (> (* 2 (+ (if (memv (aget st 'id) voters) 1 0)
             (count-acks (aget st 'match) (others (aget st 'id) voters) n)))
     (length voters)))
(define (committed-by-quorum? st n)
  (and (voter-majority? st (aget st 'voters) n)
       (let ((old (aget st 'voters-old))) (or (not old) (voter-majority? st old n)))))

; --- config adoption: takes effect on APPEND, not commit (the §4.3 safety rule).
;     `recompute-config` rederives voters/voters-old/learners (and peers/all) from
;     the NEWEST ConfChange still in the in-memory log; if none remains (e.g. a
;     truncation discarded it, or all are compacted), it reverts to the base/
;     snapshot config. Called after every log mutation that can add/remove a
;     ConfChange (conf-change propose, on-ae append/truncate, Cnew auto-append). ---
(define (latest-conf-spec log)                ; newest (conf . spec) in the log, or #f
  (let loop ((l log) (found #f))
    (if (null? l) found
        (let ((cmd (cdr (car l))))
          (loop (cdr l) (if (conf-cmd? cmd) (conf-spec cmd) found))))))
(define (set-config st voters voters-old learners)
  (aset* st (list 'voters voters 'voters-old voters-old 'learners learners
                  'all voters
                  'peers (others (aget st 'id) (union3 voters voters-old learners)))))
(define (apply-conf-spec st spec)
  (case (car spec)
    ((joint)  (set-config st (caddr spec) (cadr spec) (cadddr spec)))  ; voters=NEW, old=OLD
    ((simple) (set-config st (cadr spec) #f (caddr spec)))             ; voters=NEW, old=#f
    (else st)))
(define (recompute-config st)
  (let ((spec (latest-conf-spec (aget st 'log))))
    (if spec (apply-conf-spec st spec)
        (set-config st (aget st 'base-voters) (aget st 'base-voters-old)
                    (aget st 'base-learners)))))

; A leader must hold next/match for EXACTLY the current peers: seed a freshly-added
; peer (new voter/learner) and drop a removed one. Preserves existing progress.
(define (sync-progress st)
  (if (not (raft-leader? st)) st
      (let ((nx (aget st 'next)) (mt (aget st 'match)) (peers (aget st 'peers))
            ; A NEWLY-added peer starts at next = base+1 (the snapshot floor),
            ; NOT log-len+1: a wiped rejoiner's first AppendEntries then ships
            ; the full tail in ONE round. Seeding at the log head made it walk
            ; next back ONE PER REJECT round-trip — under concurrent writes the
            ; log grows while next decrements, so catch-up could stall forever
            ; (cw-lkq.14; user-approved consensus-core change, 2026-06-12).
            ; A non-wiped joiner receives a redundant-but-correct prefix (the
            ; consistency check truncates/overlays identically — idempotent).
            (dflt (+ 1 (aget st 'base))))
        (aset* st (list
          'next  (map (lambda (p) (cons p (assq-def p nx dflt))) peers)
          'match (map (lambda (p) (cons p (assq-def p mt 0)))   peers))))))

; --- conf-change pacing: one in flight at a time. Pending iff we're mid-joint
;     (voters-old set) or any uncommitted entry above commit is a ConfChange. ---
(define (entry-cmd st i) (cdr (list-ref (aget st 'log) (- i (aget st 'base) 1))))
(define (uncommitted-conf? st)
  (let loop ((i (+ 1 (aget st 'commit))))
    (cond ((> i (log-len st)) #f)
          ((conf-cmd? (entry-cmd st i)) #t)
          (else (loop (+ 1 i))))))
(define (conf-change-pending? st)
  (or (aget st 'voters-old) (uncommitted-conf? st)))

; ============================================================
; leader replication helpers
; ============================================================

; Cap entries per AppendEntries (etcd's max-size-per-msg analogue): a freshly
; added member is seeded at next = base+1 (cw-lkq.14), so its first AE would
; otherwise carry the ENTIRE log — thousands of entries in one transport frame,
; which can fail outright and stall catch-up forever. Bounded batches pipeline:
; each AER advances next by the batch, the next heartbeat ships the next slice.
(define AE-MAX-ENTRIES 256)
(define (take-at-most lst n)
  (let loop ((l lst) (n n) (acc '()))
    (if (or (null? l) (= n 0)) (reverse acc)
        (loop (cdr l) (- n 1) (cons (car l) acc)))))
(define (append-for st peer)
  ; cw-bm5: clamp next to lastLogIndex+1. A peer's next must never exceed the leader's
  ; log end (a follower can't hold entries the leader lacks); if a next-advance bug let it,
  ; entry-term/entries-from ran off the end of the log and CRASHED shard-main under real
  ; control-plane load. Clamp to the caught-up value and send a contained heartbeat; a
  ; genuinely-behind follower self-corrects via AER reject. Never kill the store over it.
  (let* ((raw (cdr (assq peer (aget st 'next))))
         (nx  (min raw (+ 1 (log-len st))))
         (prev (- nx 1)))
    (list 'ae (aget st 'term) (aget st 'id) prev (entry-term st prev)
          (take-at-most (entries-from st nx) AE-MAX-ENTRIES)
          (aget st 'commit) (aget st 'rseq))))  ; +rseq (ReadIndex)

; Broadcast AppendEntries to every peer whose next is still serveable from the
; in-memory log. A peer with next <= base (compaction advanced past it while it
; lagged — cw-lkq.15) CANNOT be served entries (entries-from would take a
; negative list-tail): route it to snap-req for a store-snapshot catch-up
; instead of an AE. After installing, the follower acks (aer #t base), which
; advances its next past base and ordinary AEs resume.
(define (broadcast-append st)
  (let* ((b (aget st 'base))
         (nxt (aget st 'next)))
    (let part ((ps (aget st 'peers)) (fresh '()) (sr (aget st 'snap-req)))
      (if (pair? ps)
          (let ((p (car ps)))
            (if (> (cdr (assq p nxt)) b)
                (part (cdr ps) (cons p fresh) sr)
                (part (cdr ps) fresh (add-mem p sr))))
          (cons (aset st 'snap-req sr)
                (map (lambda (p) (cons p (append-for st p))) fresh))))))

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
               (cmd (cdr (list-ref (aget st 'log) (- i (aget st 'base) 1)))))
          ; ConfChange entries are engine-internal metadata (the config was already
          ; adopted on append) — advance `applied` past them WITHOUT calling apply-fn,
          ; so the state machine (e.g. the MVCC apply-fn) never sees config payloads.
          ; The no-op barrier ('()) is NOT a conf entry, so it still reaches apply-fn
          ; (which skips it), preserving the §5.4.2 behaviour.
          (if (conf-cmd? cmd)
              (loop (aset st 'applied i))
              (let ((sm2 ((aget st 'apply) (aget st 'sm) cmd)))
                (loop (aset* st (list 'applied i 'sm sm2)))))))))

; Leader: advance commit to the highest index replicated on a quorum AND from
; the current term (Raft §5.4.2), then apply. Quorum is config-aware
; (committed-by-quorum?): a JOINT config needs both an old- and a new-majority.
; After commit advances, react to any ConfChange that newly committed.
(define (maybe-commit st)
  (let ((c0 (aget st 'commit)))
    (let loop ((n (log-len st)))
      (cond
        ((<= n c0) st)
        ((and (= (entry-term st n) (aget st 'term))
              (committed-by-quorum? st n))
         (after-commit (apply-committed (aset st 'commit n)) c0))
        (else (loop (- n 1)))))))

; Two-phase membership transition driver, run after the leader's commit advances
; from `old-commit`. For each ConfChange that newly committed:
;   * a JOINT entry  -> auto-append the matching Cnew (leave-joint) entry, so the
;     transition proceeds to the new config. (The Cnew is replicated by the caller
;     — on-aer broadcasts when it sees the log grow.)
;   * a SIMPLE entry -> the transition is done; a LEADER no longer in the new voter
;     set steps down (Ongaro §4.3: a leader outside Cnew relinquishes leadership
;     only AFTER Cnew commits).
; Followers never drive transitions (they adopt every config by replication), so
; this is leader-only.
(define (after-commit st old-commit)
  (let ((c (aget st 'commit)))
    (let loop ((i (+ 1 old-commit)) (st st))
      (if (> i c) st
          (let ((cmd (entry-cmd st i)))
            (loop (+ 1 i)
                  (if (conf-cmd? cmd) (react-to-committed-conf st (conf-spec cmd) i) st)))))))

(define (react-to-committed-conf st spec idx)
  (cond
    ((not (raft-leader? st)) st)
    ((eq? (car spec) 'joint)
     (if (no-conf-after? st idx) (append-cnew st spec) st))
    ((eq? (car spec) 'simple)
     (if (memv (aget st 'id) (aget st 'voters)) st
         (aset* st (list 'role 'follower 'voted-for #f))))   ; removed leader steps down
    (else st)))

(define (no-conf-after? st idx)                ; is this joint still the log's tail conf?
  (let loop ((i (+ 1 idx)))
    (cond ((> i (log-len st)) #t)
          ((conf-cmd? (entry-cmd st i)) #f)
          (else (loop (+ 1 i))))))

; Append Cnew (leave-joint): voters = the joint's NEW set, with its learners. The
; config is adopted on this append (recompute-config) and progress re-seeded; the
; broadcast is left to the caller so maybe-commit stays (node)->(node').
(define (append-cnew st spec)
  (let ((new (caddr spec)) (lrn (cadddr spec)))
    (sync-progress
     (recompute-config
      (aset st 'log (append (aget st 'log)
                            (list (cons (aget st 'term) (cons 'conf (list 'simple new lrn))))))))))

; ============================================================
; public transitions: each returns (node' . outputs)
; ============================================================

(define (raft-campaign st)
  (if (not (is-voter? st (aget st 'id)))
      (cons st '())                              ; a learner / removed node never campaigns
      (let* ((term (+ 1 (aget st 'term)))
             (id (aget st 'id))
             (st (aset* st (list 'role 'candidate 'term term 'voted-for id 'votes (list id)))))
        (if (votes-quorum? st (aget st 'votes))
            (become-leader st)                   ; single-node: instant (joint) majority
            (cons st (map (lambda (p)
                            (cons p (list 'rv term id (log-len st) (last-log-term st))))
                          (aget st 'peers)))))))

; cw-u4a.42 — leadership transfer (Ongaro thesis §3.10).  A node that receives a
; 'timeout-now from the current leader campaigns IMMEDIATELY: it skips its
; election-timeout wait AND PreVote and calls raft-campaign directly.  This is safe
; because the election itself still follows every normal rule (new term, and on-rv's
; voted-for / log-up-to-date checks) — the only effect is a faster, directed
; re-election that the leader cooperatively yields to (so there is no term-inflation
; risk that PreVote otherwise guards against).  A node already leading, or not a
; voter, ignores it (raft-campaign self-guards the voter check too).
(define (on-timeout-now st msg)
  (if (raft-leader? st) (cons st '()) (raft-campaign st)))

; cw-u4a.42 — leader-side initiation of a leadership transfer.  Emits a 'timeout-now
; to TARGET iff we lead, TARGET is a voter other than ourselves, and TARGET is fully
; CAUGHT UP (its match index >= our last log index).  An out-of-date target would
; campaign but fail the log-up-to-date vote check, so we refuse rather than trigger a
; doomed, disruptive election (etcd would first replicate to catch it up; here the
; caller may retry once replication advances).  Pure: returns
;   (list 'ok (output ...))   — the 'timeout-now to send; OR
;   (list 'err REASON)        — REASON in {not-leader, self, not-voter, not-caught-up}.
(define (raft-transfer-leadership st target)
  (cond
    ((not (raft-leader? st))      (list 'err 'not-leader))
    ((eqv? target (aget st 'id))  (list 'err 'self))
    ((not (is-voter? st target))  (list 'err 'not-voter))
    ((let ((m (assv target (aget st 'match))))
       (or (not m) (< (cdr m) (log-len st))))
     (list 'err 'not-caught-up))
    (else
     (list 'ok (list (cons target (list 'timeout-now (aget st 'term))))))))

(define (raft-propose st command)
  (if (not (raft-leader? st))
      (cons st '())
      (broadcast-append
       (aset st 'log (append (aget st 'log) (list (cons (aget st 'term) command)))))))

; EXP19 (cw-t0n): batch propose. Append a whole list of commands to the log in ONE
; (append log entries) and broadcast ONCE — vs the drain calling raft-propose per
; entry, which was O(log-len) PER entry (O(batch*loglen) total) AND ran
; broadcast-append per entry while the caller discarded all but the last. The
; resulting log + AE are byte-identical to proposing the commands one-by-one and
; broadcasting after the last. Returns (st . broadcast-outputs) like raft-propose.
; Empty list => no-op (st, no outputs).
(define (raft-propose-batch st commands)
  (if (or (not (raft-leader? st)) (null? commands))
      (cons st '())
      (let ((term (aget st 'term)))
        (broadcast-append
         (aset st 'log (append (aget st 'log)
                               (map (lambda (c) (cons term c)) commands)))))))

; Leader-side membership change (cw-u4a.28). TARGET-VOTERS / TARGET-LEARNERS describe
; the desired final config. Refused (no-op) if we don't lead or a change is already
; in flight (one at a time). If the VOTER set is unchanged (a learner-only add/remove)
; the change is safe in one phase -> append a SIMPLE entry directly. Otherwise enter
; joint consensus -> append a JOINT (Cold,new) entry; when it commits the leader
; auto-appends Cnew (see after-commit). Config is adopted on append; progress is
; re-seeded for any new peer; the entry is broadcast immediately.
(define (raft-propose-conf-change st target-voters target-learners)
  (cond
    ((not (raft-leader? st)) (cons st '()))
    ((conf-change-pending? st) (cons st '()))
    ((equal-set? target-voters (aget st 'voters))
     (append-conf st (list 'simple target-voters target-learners)))
    (else
     (append-conf st (list 'joint (aget st 'voters) target-voters target-learners)))))

(define (append-conf st spec)
  (broadcast-append
   (sync-progress
    (recompute-config
     (aset st 'log (append (aget st 'log)
                           (list (cons (aget st 'term) (cons 'conf spec)))))))))

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
  (if (not (is-voter? st (aget st 'id)))
      (cons st '())                              ; a learner / removed node does not pre-vote
      (let* ((id (aget st 'id))
             (st (aset* st (list 'role 'pre-candidate 'pre-votes (list id)))))
        (cons st (map (lambda (p)
                        (cons p (list 'prv (+ 1 (aget st 'term)) id
                                      (log-len st) (last-log-term st))))
                      (aget st 'peers))))))

(define (raft-step st from msg)
  (case (car msg)
    ((rv)  (on-rv st msg))
    ((rvr) (on-rvr st from msg))
    ((ae)  (on-ae st msg))
    ((aer) (on-aer st from msg))
    ((timeout-now) (on-timeout-now st msg))   ; cw-u4a.42 leadership transfer
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
         ; Config-aware: a joint config needs an old- AND a new-majority of grants.
         (if (votes-quorum? st votes) (become-leader st) (cons st '()))))
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
                     ; A stale/in-flight AE may start BELOW our base (we compacted
                     ; past it, or just installed a snapshot): entries <= base are
                     ; already applied — skip them and splice from base, so `log`
                     ; keeps holding exactly base+1.. (cw-lkq.15).
                     (skip (if (< pidx b) (- b pidx) 0))
                     (entries (drop-at-most entries skip))
                     (pidx (+ pidx skip))
                     (kept (take-n (aget st 'log) (- pidx b)))   ; keep base+1..pidx
                     (newlog (append kept entries))
                     (midx (+ pidx (length entries)))
                     ; Adopt config on APPEND, not commit (§4.3): if these entries add
                     ; a ConfChange, or the truncation dropped one, recompute-config
                     ; switches us to the right config for all subsequent quorum votes.
                     (st (recompute-config (aset st 'log newlog)))
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
       ; cw-bm5 SOURCE fix: clamp the acked match to lastLogIndex. A follower cannot have
       ; replicated entries the leader lacks, so midx > log-len is a stale/racey AER; left
       ; unclamped it corrupts BOTH match (-> maybe-commit advances commit past the log) AND
       ; next (-> append-for indexes off the end -> shard crash). The iter-1 append-for clamp
       ; is the downstream guard; this keeps the stored match/next sane at the source.
       (let* ((m   (min midx (log-len st)))
              (st1 (aset* st (list 'match (aset (aget st 'match) from m)
                                   'next (aset (aget st 'next) from (+ m 1))
                                   'heard (add-mem from (aget st 'heard)))))
              (st2 (maybe-commit st1)))
         ; maybe-commit auto-appends Cnew when a joint config commits — replicate it
         ; now so the transition settles. (Stepping down on a committed Cnew shrinks
         ; the log? no — Cnew was already present, so the log only grows on the joint
         ; commit, while we are still leader.)
         (if (and (raft-leader? st2) (> (log-len st2) (log-len st1)))
             (broadcast-append st2)
             (cons st2 '()))))
      (else
       ; Back off nextIndex on rejection — floor is base+1, NOT 1: entries <= base
       ; are compacted into the snapshot, so entries-from/append-for cannot serve
       ; them (a negative list-tail index -> crash; bug cw-u4a.39). Clamp to base+1;
       ; the follower re-syncs from base+1 (all voters compact to the same base).
       (let ((nx (cdr (assq from (aget st 'next))))
             (b  (aget st 'base)))
         (if (and (> b 0) (= nx (+ b 1)))
             ; next was ALREADY at the floor, so the peer rejected an AE with
             ; prev = base — it lacks (or disagrees below) our compaction floor,
             ; and the log holds nothing earlier to walk back to: only a STORE
             ; snapshot can catch it up (cw-lkq.15). Mark it for the hosting
             ; actor (ws-snap); resending the same AE would reject forever.
             ; (A reject with next still BELOW the floor — a stale walk-back —
             ; clamps to base+1 and tries the floor AE first, as before.)
             (cons (aset* st (list 'snap-req (add-mem from (aget st 'snap-req))
                                   'heard (add-mem from (aget st 'heard))))
                   '())
             (let ((st (aset* st (list 'next (aset (aget st 'next) from
                                                   (max (+ b 1) (- nx 1)))
                                       'heard (add-mem from (aget st 'heard))))))
               (cons st (list (cons from (append-for st from)))))))))))

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
; Drive a membership change at leader `id` and settle. Because each phase's commit
; begets further AppendEntries (joint commit -> Cnew broadcast; Cnew replication),
; cluster-settle carries the WHOLE two-phase transition (and any new-peer log catch-up
; via reject/backoff) to quiescence in one call.
(define (cluster-propose-conf-change c id voters learners)
  (cluster-drive c id (lambda (st) (raft-propose-conf-change st voters learners))))

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
