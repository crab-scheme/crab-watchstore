; server/shard-actor.scm — the shard-replica actor body for crab-watchstore.
;
; PORTED from crab-cache/src/server/shard-actor.scm with the Redis command /
; read / txn / pubsub layers STRIPPED and the state-machine apply-fn STUBBED.
; The consensus machinery (Raft engine binding, group-commit ack gate, ReadIndex
; round management, PreVote/CheckQuorum, the stepdown cross-wire fix, the
; committed-batch ack/non-retryable split) is KEPT FAITHFUL — this is the
; Phase 0 keystone that proves the ported pure engine + durable-KV substrate +
; actor/transport layer compose into a working multi-voter sim-cluster.
;
; Loaded by spawn-source into its own runtime/thread:
;   (spawn-source "(include \"src/server/shard-actor.scm\")" 'shard-main
;                 SHARD-KEY VOTERS NODE-NAME DB-PATH SYNC?)
;   SHARD-KEY : string routing/table key ("0".."N-1")
;   VOTERS    : list of node-name symbols in this shard's Raft group
;   NODE-NAME : this replica's Raft id = this node's name (symbol)
;   DB-PATH   : this shard's own RocksDB directory
;   SYNC?     : #t = durable mode (group-commit fsync); #f = relaxed
;
; It is ENTIRELY MAILBOX-DRIVEN. Message shapes that arrive on the mailbox:
;   (conn-pid . cmd)        a client proposal (conn-pid is a PID); EVERY client
;                           proposal is a write routed through Raft (no Redis
;                           write/read classification in this Phase-0 stub).
;   (engine FROM RPC)       a Raft RPC, forwarded by the node's peer-poller
;   (tick)                  a heartbeat/election tick from the peer-poller
;   (read CONN)             a linearizable read probe (ReadIndex round); the
;                           stub answers with the current applied index so the
;                           ReadIndex machinery is still exercised.
; Raft OUTPUTS (AppendEntries/RequestVote to peers) are shipped by node name via
;   (node-send self peer (list 'ws-engine SHARD-KEY self rpc));
; the peer node's peer-poller delivers them to the right local replica's mailbox.
;   (The Raft RPC frame tag is renamed crab-cache `shard-engine` -> `ws-engine`.)
;
; raft.scm has no timers, so leadership is driven here: the leader heartbeats on
; every tick (raft-tick -> AE), and a follower that misses `timeout` ticks
; (staggered by voter index) campaigns. The commit->ack bridge is async:
; pending[log-index]=conn at propose; the reply is sent when that index
; commits+applies (which may be a later engine message once a quorum of AERs
; arrives).

(include "src/encoding.scm")
(include "src/store-ctx.scm")
(include "src/mvcc.scm")
(include "src/watch.scm")     ; Watch backend: registry + replay->live dispatch (cw-u4a.13)
(include "src/raft.scm")

(define (raft-applied st) (aget st 'applied))

(define (index-of x lst)
  (let loop ((i 0) (l lst))
    (cond ((null? l) 0) ((eqv? (car l) x) i) (else (loop (+ i 1) (cdr l))))))

(define (shard-main shard-key voters node-name db-path sync?)
  (let* ((handle  (store-open db-path #t))      ; create-if-missing
         (ctx     (make-ctx handle "default" sync?))
         (pending (make-eqv-hashtable))          ; log-index -> conn-pid
         (acc     '())                           ; apply markers, newest-first (apply order)
         (read-q  '())                           ; ReadIndex: reads awaiting a round, (conn . #f)
         (batch   '())                           ; ReadIndex: reads the open round is confirming
         (read-acks '())                         ; ReadIndex: peers acked (fresh) since round opened
         (round-open? #f)                        ; ReadIndex: a confirmation heartbeat is in flight
         (round-rseq 0)                          ; ReadIndex: the rseq this round's acks must echo (>=)
         (watch-reg (make-watch-registry))       ; Watch backend registry (cw-u4a.13); empty => apply hook is a no-op
         ; ---- Lease expiry (cw-u4a.17, ADR 0003 §2) — LEADER-LOCAL, never replicated.
         ; lease-deadlines : id -> deadline-second (wall-clock).  The leader seeds it
         ; (untracked live leases get now+ttl on the tick) and scans it for expiry.
         ; lease-revoking  : id -> #t for a revoke proposed-but-not-yet-applied, so the
         ; tick does NOT re-seed/re-propose it every tick until its LEASE-REVOKE
         ; commits (ADR §2 "remove id at propose time").  Both are EMPTY on a follower
         ; (only the leader touches them) and cleared on stepdown (fail-pending!), so a
         ; newly-elected leader re-derives a FRESH full window (§2 failover).  With no
         ; leases granted, every lease-tick step is a no-op (the meta scan is empty), so
         ; the sim-cluster / Watch paths are unperturbed.
         (lease-deadlines (make-eqv-hashtable))   ; id -> deadline-second (leader-local)
         (lease-revoking  (make-eqv-hashtable))   ; id -> #t  (revoke in flight)
         (solo    (null? (cdr voters)))          ; 1-voter group?
         ; Staggered election timeout, ROTATED by shard so leadership spreads:
         ; for shard S the voter at index S has the shortest timeout and tends to
         ; win it. Deterministic => no split votes, predictable failover.
         (timeout (+ 4 (* (modulo (- (index-of node-name voters)
                                     (let ((n (string->number shard-key))) (if n n 0)))
                                  (length voters))
                          3))))
    ; ---- MVCC state machine (cw-u4a.6, ADR 0001) ----
    ; Apply one committed Raft entry as one etcd Txn: mvcc-apply stamps the new
    ; revision, writes the KEY-CF record + REV-CF event (+ lease index) and bumps
    ; the current-rev META key — all via `ctx` (kv-put!/kv-del!), so they ride the
    ; SAME group-commit batch as persist-applied! below and land under one fsync.
    ; mvcc-apply's result (e.g. ("PUT" . rev) / ("DEL" rev . n)) is pushed onto `acc`
    ; as that waiter's client ack. The '() become-leader no-op barrier (§5.4.2) is a
    ; pure barrier: it does NO MVCC write and MUST NOT bump the revision — it only
    ; contributes an #f `acc` slot so drain!'s positional index alignment still holds.
    ; Watch backend hook (cw-u4a.13, ADR 0002 §3/§4): capture current-rev BEFORE and
    ; AFTER the MVCC write, then notify the watch registry of the (pre, post] window
    ; so SYNCED watchers get their live events.  watch-on-apply! has a no-op fast path
    ; (empty registry OR post==pre => returns immediately, no REV-CF read), so when no
    ; watcher is registered this is strictly additive and perturbs nothing — the
    ; sim-cluster / group-commit / apply path behaves exactly as before.  The register/
    ; cancel mailbox protocol, leader-gating, and streaming are .14, NOT here.
    (define (apply-fn sm cmd)
      (if (null? cmd)
          (set! acc (cons #f acc))                       ; no-op barrier: acc slot only, no rev bump
          (let ((pre (mvcc-current-rev ctx)))
            (set! acc (cons (mvcc-apply ctx cmd) acc))    ; MVCC write; client waiter gets the result
            (watch-on-apply! watch-reg ctx pre (mvcc-current-rev ctx))
            ; Watch backend (cw-u4a.14, ADR 0002 §5 mid-stream ErrCompacted): a
            ; COMPACT applied here GC's REV-CF events <= compact-rev, so any watcher
            ; still BELOW that floor (delivered_rev < compact-rev) can no longer be
            ; served its remaining history — cancel it with compact_revision set so
            ; the client re-establishes above the floor.  COMPACT does NOT bump
            ; current-rev, so watch-on-apply! above is already a no-op for it; this
            ; is the separate wiring point §5 calls for.  Gated on a NON-EMPTY
            ; registry so a watcher-free apply path (sim-cluster) is unperturbed:
            ; with no watchers this is a single hashtable-size check, no scan.
            (if (and (> (reg-count watch-reg) 0)
                     (pair? cmd) (string=? (cmd-op cmd) "COMPACT"))
                (watch-check-compaction! watch-reg ctx))))
      (+ sm 1))
    ; Persist the applied index (+ its term) into the SAME group-commit batch as
    ; the entry's mutations, so a restart restores base/applied/commit and never
    ; re-applies already-applied committed entries (idempotent recovery/rejoin).
    (define (persist-applied! st)
      (ctx-save-applied! ctx (raft-applied st) (entry-term st (raft-applied st))))
    ; ship engine outputs (target-node . rpc) to peers over the node transport.
    ; A send to a DOWN peer must not crash us — Raft is lossy-tolerant and
    ; recovers the entry on the next heartbeat/AE, so swallow transport errors.
    (define (emit! outs)
      (for-each
       (lambda (o)
         (guard (e (#t #f))
           (node-send (symbol->string node-name) (symbol->string (car o))
                      (list 'ws-engine shard-key node-name (cdr o)))))
       outs))
    ; match in-order applied replies to the indices that produced them
    (define (drain! old-applied)
      (let loop ((k 0) (rs (reverse acc)))
        (if (pair? rs)
            (let* ((idx (+ old-applied 1 k)) (conn (hashtable-ref pending idx #f)))
              (if conn (begin (send conn (car rs)) (hashtable-delete! pending idx)))
              (loop (+ k 1) (cdr rs)))))
      (set! acc '()))

    ; On losing leadership, every conn still in `pending` is a write this node
    ; proposed as leader but never drained — its uncommitted tail is about to be
    ; (or was just) truncated by the new leader's AppendEntries. Reply a
    ; non-retryable indeterminate marker so the client does NOT silently retry a
    ; non-idempotent write, and CLEAR `pending` so the following drain! can't
    ; cross-wire a stale conn to a newly-applied entry's reply (the cc-idc / H1
    ; false-ack). `pending` is populated only on the leader's propose branch, so a
    ; non-empty `pending` means we led — and on solo (no peers) no `engine` RPC
    ; ever arrives, so this never fires there and the single-node path is unchanged.
    (define (fail-pending!)
      ; Writes still pending here are UNCOMMITTED at stepdown — the committed ones
      ; were already drained + acked before raft-step. Their outcome is genuinely
      ; INDETERMINATE (the new leader may yet commit a replicated-but-uncommitted
      ; entry). So reply a NON-RETRYABLE marker — the client must NOT retry, or a
      ; non-idempotent write double-applies (cc-cri). 'indeterminate is the
      ; truthful outcome.
      (vector-for-each
       (lambda (conn) (send conn 'indeterminate))
       (hashtable-values pending))
      (hashtable-clear! pending)
      ; Reads are idempotent — 'tryagain so the client safely retries against the
      ; new leader (a ReadIndex round we can no longer confirm here).
      (for-each (lambda (e) (send (car e) 'tryagain)) batch)
      (for-each (lambda (e) (send (car e) 'tryagain)) read-q)
      (set! batch '()) (set! read-q '()) (set! read-acks '()) (set! round-open? #f)
      ; Drop the leader-local lease deadlines + in-flight revokes on stepdown — the
      ; replicated TTLs are safe in the meta entries, and the next leader re-derives a
      ; fresh full window (ADR 0003 §2).  Harmless to discard.
      (hashtable-clear! lease-deadlines)
      (hashtable-clear! lease-revoking))

    ; ---- ReadIndex: linearizable read (Raft §6.4) ----
    ; A read is served only after a quorum heartbeat round confirms we are STILL
    ; the leader as of a point AFTER the read was issued — otherwise a just-deposed
    ; leader could serve a stale value (cc-idc). `batch` snapshots the reads a
    ; round confirms when it opens, so the quorum we then collect (read-acks, reset
    ; here) strictly follows them; reads arriving mid-round wait in read-q for the
    ; next round. Solo skips all this (served inline — see the read branch). On lost
    ; leadership fail-pending! tryagains both. The stub answers a confirmed read
    ; with the current applied index.
    (define (start-read-round! st)
      (let ((st2 (aset st 'rseq (+ 1 (aget st 'rseq)))))
        (set! batch read-q) (set! read-q '()) (set! read-acks '()) (set! round-open? #t)
        (set! round-rseq (aget st2 'rseq))
        (emit! (cdr (broadcast-append st2)))         ; heartbeat tagged with the new rseq
        st2))
    (define (serve-batch! st)
      (for-each (lambda (e) (send (car e) (cons 'read-ok (raft-applied st)))) batch)
      (set! batch '()) (set! round-open? #f)
      (if (pair? read-q) (start-read-round! st) st)) ; reads that arrived mid-round -> next round
    ; an AER is a FRESH confirmation only if it is a success, at our current term,
    ; echoing rseq >= this round's rseq (a reply to the round's own heartbeat, not
    ; a stale in-flight/backlogged ack). Returns st (rseq-bumped if a new round
    ; opened for mid-round reads).
    (define (note-read-ack! from st ack-term ack-rseq success?)
      (if (and round-open? success? (= ack-term (raft-term st)) (>= ack-rseq round-rseq))
          (begin (set! read-acks (add-mem from read-acks))
                 (if (>= (+ 1 (length read-acks)) (majority st)) (serve-batch! st) st))
          st))

    ; ---- group-commit ack gate (durable mode) ----
    ;
    ; In durable mode a write's RocksDB ops land immediately (sync=#f) but the
    ; fsync is amortised: replies are buffered and `flush-base` remembers the
    ; `applied` value BEFORE the first deferred write, so one drain!(flush-base)
    ; later acks the whole batch in index order. CRITICAL: a waiter is NEVER acked
    ; until ctx-flush! (the single fsync) has returned for its write. Cap the batch
    ; so a non-stop write stream still bounds ack latency/memory.
    (define FLUSH-CAP 256)
    ; fsync the batch, then ack every buffered waiter from `base`.
    (define (flush-and-drain! base)
      (ctx-flush! ctx)
      (if base (drain! base) (set! acc '())))
    ; Decide what to do after applying entries (`old` = applied-before):
    ;   leader + durable + writes buffered -> defer (return earliest flush-base),
    ;                                or flush+ack now if the batch hit FLUSH-CAP;
    ;   else (follower, relaxed, or nothing written) -> ack now (drain inline; a
    ;        follower has no client waiters in `pending`, so this is a no-op that
    ;        just resets `acc`; its writes are still in RocksDB (sync=#f) and get
    ;        fsync'd on the next tick / AER path).
    ; Only the leader holds client waiters, so only it group-commits; this keeps
    ; the follower catch-up path identical. Returns the new flush-base (#f = none).
    (define (settle! leader? old flush-base)
      (cond
        ((and leader? (ctx-dirty? ctx))
         (let ((base (if flush-base flush-base old)))
           (if (>= (ctx-dirty-count ctx) FLUSH-CAP)
               (begin (flush-and-drain! base) #f)
               base)))
        ; Not deferring: follower / relaxed / nothing-written, OR a leader that just
        ; stepped down with a batch still deferred. If a batch WAS deferred
        ; (flush-base set), fsync + ack it at flush-base — NOT `old` (this step's
        ; applied-before), which would mis-index the buffered replies and strand the
        ; waiters (HOLE 2). Otherwise drain inline at `old`. Nothing deferred after,
        ; so return #f.
        (else
         (if flush-base (flush-and-drain! flush-base) (drain! old))
         #f)))
    ; solo log compaction (RocksDB is the snapshot); no-op for multi-voter.
    ; NEVER compact while acks are deferred: `pending` is keyed by absolute log
    ; index and compaction resets the log to 0, which would collide the next
    ; proposal's index with an undrained one. flush-base = #f means every applied
    ; entry's ack has been drained, so compaction is safe then.
    (define (compact st)
      (if (and solo (= (raft-applied st) (log-len st)))
          (aset* st (list 'base (raft-applied st)
                          'base-term (last-log-term st)
                          'log '()))
          st))
    (define (maybe-compact st flush-base)
      (if flush-base st (compact st)))
    ; node-qualified table keys ("node:shard") so the in-process sim (all replicas
    ; in one process) doesn't collide; in production each node has its own
    ; process-global table and the node prefix is simply constant.
    (define (qk) (string-append (symbol->string node-name) ":" shard-key))
    (define (publish! st leader)
      (table-insert! 'ws-shard-pid (qk) (self))
      (table-insert! 'ws-shard-role (qk) (raft-role st))
      (table-insert! 'ws-shard-leader (qk) leader)
      (table-insert! 'ws-shard-commit (qk) (raft-commit st))
      (table-insert! 'ws-shard-applied (qk) (raft-applied st)))

    ; ---- Lease expiry: leader-driven scan + replicated revoke (cw-u4a.17) ----
    ;
    ; Propose ONE internally-generated command (no client conn, so NO `pending`
    ; slot — nothing to ack) through the SAME path a client write takes: append to
    ; the log, AE to followers (emit!), solo-commit (maybe-commit), persist the
    ; applied index, and group-commit-settle.  Returns (st' . flush-base').  Used
    ; for ("LEASE-REVOKE" id) — explicit/keepalive/expiry all converge on this entry.
    (define (propose-internal! st cmd flush-base)
      (let ((old (raft-applied st)))
        (let* ((r (raft-propose st cmd)) (st1 (car r)))
          (emit! (cdr r))                 ; AE to followers (cluster) / none (solo)
          (let ((st2 (maybe-commit st1))) ; solo commits+applies now; cluster waits for AERs
            (if (> (raft-applied st2) old) (persist-applied! st2))
            (cons st2 (settle! #t old flush-base))))))   ; this runs only on the leader

    ; The leader's per-tick lease pass.  THREE steps, all leader-local, all a strict
    ; no-op when no leases are granted (the meta scan is empty):
    ;   1. RECONCILE: drop any `lease-revoking` id whose meta entry is gone (its
    ;      LEASE-REVOKE has applied) — self-heals the in-flight set.
    ;   2. SEED:  for every live lease (meta entry present) not already tracked AND
    ;      not revoke-in-flight, seed deadline = now + granted_ttl.  This seeds a
    ;      freshly-granted lease (its meta is now durable) AND, on a NEW leader whose
    ;      maps were cleared at stepdown/start, re-derives the FULL set as a fresh
    ;      window (ADR §2 failover).
    ;   3. EXPIRE: for every tracked id with deadline <= now, PROPOSE
    ;      ("LEASE-REVOKE" id) through Raft (propose-internal!), drop it from
    ;      lease-deadlines, and mark it revoke-in-flight (so it isn't re-proposed
    ;      before the entry commits).  The authoritative meta+keys removal happens on
    ;      apply, every replica, at one revision.
    ; Returns (st' . flush-base').
    (define (lease-tick! st flush-base)
      (let ((now  (current-second))
            (live (mvcc-all-lease-ids ctx)))
        ; (1) reconcile in-flight revokes whose meta is gone (applied)
        (vector-for-each
         (lambda (id)
           (if (not (mvcc-lease-exists? ctx id)) (hashtable-delete! lease-revoking id)))
         (hashtable-keys lease-revoking))
        ; (2) seed untracked live leases with a fresh full window
        (for-each
         (lambda (id)
           (if (and (not (hashtable-contains? lease-deadlines id))
                    (not (hashtable-contains? lease-revoking id)))
               (let ((ttl (mvcc-lease-meta-get ctx id)))
                 (if ttl (hashtable-set! lease-deadlines id (+ now ttl))))))
         live)
        ; (3) collect expired ids, then propose a revoke for each (threading st/fb)
        (let ((expired
               (let collect ((ks (vector->list (hashtable-keys lease-deadlines))) (out '()))
                 (cond ((null? ks) out)
                       ((<= (hashtable-ref lease-deadlines (car ks) 0) now)
                        (collect (cdr ks) (cons (car ks) out)))
                       (else (collect (cdr ks) out))))))
          (let loop ((ids expired) (st st) (fb flush-base))
            (if (null? ids)
                (cons st fb)
                (let ((id (car ids)))
                  (hashtable-delete! lease-deadlines id)   ; stop scanning it
                  (hashtable-set! lease-revoking id #t)    ; mark in flight
                  (let ((res (propose-internal!
                              st (list (string->utf8 "LEASE-REVOKE") (int->bytes id)) fb)))
                    (loop (cdr ids) (car res) (cdr res)))))))))

    (let* ((loaded (ctx-load-applied ctx))                 ; (idx . term) from RocksDB
           (p (car loaded)) (pt (cdr loaded))
           (st0 (make-raft node-name voters apply-fn 0))
           ; restart: RocksDB already reflects entries up to p, so start with the
           ; log compacted to base=p (applied=commit=p). The log replays only
           ; entries above p, so committed entries are never re-applied.
           (st0 (if (> p 0) (aset* st0 (list 'base p 'base-term pt 'applied p 'commit p)) st0))
           (stI (if solo (car (raft-campaign st0)) st0))   ; solo self-elects now
           (ldr0 (if solo node-name #f)))
      (publish! stI ldr0)
      ; `flush-base` (#f = no deferred acks) carries the group-commit ack gate:
      ; when set, durable writes are buffered awaiting their batch fsync. While
      ; deferred, poll the mailbox non-blocking — an empty mailbox flushes the
      ; batch (one fsync) and acks all waiters at once; a steady stream keeps
      ; batching until a tick or FLUSH-CAP. Relaxed mode never sets flush-base, so
      ; it always blocks and acks inline.
      (let loop ((st stI) (leader ldr0) (elapsed 0) (flush-base #f))
        (let ((m (if flush-base (raw-receive 0) (raw-receive))))
          (cond
            ;; mailbox empty while acks are pending -> flush the batch + ack now
            ((eq? m '*timeout*)
             (flush-and-drain! flush-base)
             (loop (maybe-compact st #f) leader elapsed #f))
            ((not (pair? m)) (loop st leader elapsed flush-base))

            ;; ---- Raft RPC from a peer ----
            ((eq? (car m) 'engine)
             (let ((from (cadr m)) (rpc (caddr m)))
               (cond
                 ;; ---- PreVote request -> grant iff OUR own election timer has
                 ;; expired (no live leader from our view), we don't lead, and the
                 ;; pre-candidate's log is at least as up-to-date as ours. Does NOT
                 ;; touch our term/role — the whole point of pre-vote.
                 ((eq? (car rpc) 'prv)
                  (let* ((cidx (list-ref rpc 3)) (clt (list-ref rpc 4))
                         (up (or (> clt (last-log-term st))
                                 (and (= clt (last-log-term st)) (>= cidx (log-len st)))))
                         (grant (and (not (raft-leader? st)) (>= elapsed timeout) up)))
                    (emit! (list (cons from (list 'prvr (raft-term st) grant))))
                    (loop st leader elapsed flush-base)))
                 ;; ---- PreVote reply -> tally; on a majority start the REAL
                 ;; election (raft-campaign bumps the term + sends RequestVote).
                 ((eq? (car rpc) 'prvr)
                  (if (and (eq? (raft-role st) 'pre-candidate) (list-ref rpc 2))
                      (let* ((pv (add-mem from (aget st 'pre-votes)))
                             (st2 (aset st 'pre-votes pv)))
                        (if (>= (length pv) (majority st))
                            (let* ((r (raft-campaign st2)) (st3 (car r))
                                   (ldr (if (raft-leader? st3) node-name #f)))
                              (emit! (cdr r)) (publish! st3 ldr) (loop st3 ldr 0 #f))
                            (loop st2 leader elapsed flush-base)))
                      (loop st leader elapsed flush-base)))
                 ;; ---- all other RPCs (rv / rvr / ae / aer): the normal Raft step
                 (else
                  (let* ((was-leader? (raft-leader? st))    ; role BEFORE this RPC steps us
                         ; A higher-term ae/rv/aer/rvr will depose us. BEFORE
                         ; raft-step truncates our uncommitted tail, ACK our
                         ; COMMITTED deferred batch (fsync + drain) while acc/pending
                         ; are still aligned — so a committed non-idempotent write is
                         ; reported acked and the client does NOT silently retry it
                         ; after stepdown (cc-cri). Committed = quorum-durable
                         ; (Leader Completeness), so acking is safe; only the
                         ; genuinely-uncommitted tail is then 'indeterminate'd by
                         ; fail-pending!. flush-base is consumed here, so pass #f on.
                         (depose? (and was-leader?
                                       (memq (car rpc) '(ae rv aer rvr))
                                       (> (list-ref rpc 1) (raft-term st))))
                         (fb (if (and depose? flush-base)
                                 (begin (flush-and-drain! flush-base) #f)
                                 flush-base))
                         (old (raft-applied st))
                         (r (raft-step st from rpc)) (st2 (car r)))
                    ; leader -> follower: 'indeterminate the remaining (now
                    ; uncommitted) in-flight proposals + clear pending, before any
                    ; drain! can cross-wire them (H1).
                    (if (and was-leader? (not (raft-leader? st2))) (fail-pending!))
                    ; record the new applied-index in the batch BEFORE the fsync
                    (if (> (raft-applied st2) old) (persist-applied! st2))
                    ; HOLE 1 fix: a FOLLOWER fsyncs its applied writes (one flush)
                    ; BEFORE emitting the AppendEntries reply. The AER success means
                    ; "durably stored", so the leader may commit+ack a client only
                    ; once a quorum has truly fsync'd. The leader itself keeps
                    ; deferring (group-commit) via settle! below. No-op on solo.
                    (if (and (not (raft-leader? st2)) (ctx-dirty? ctx)) (ctx-flush! ctx))
                    (emit! (cdr r))
                    ; defer (leader, has waiters) or ack inline (follower) the applied entries
                    (let ((nb (settle! (raft-leader? st2) old fb)))
                      ; ReadIndex: an AER may be a fresh confirmation ack;
                      ; note-read-ack! counts it (success + current term + rseq >=
                      ; round) and releases the batch on quorum. Returns st
                      ; (rseq-bumped if a round opened).
                      (let ((st3 (if (and (raft-leader? st2) (eq? (car rpc) 'aer))
                                     (note-read-ack! from st2 (list-ref rpc 1) (list-ref rpc 4) (list-ref rpc 2))
                                     st2)))
                        (let* ((ae? (eq? (car rpc) 'ae))
                               (ldr (cond ((raft-leader? st3) node-name) (ae? from) (else leader)))
                               (el  (if ae? 0 elapsed)))
                          (publish! st3 ldr)
                          (loop (maybe-compact st3 nb) ldr el nb)))))))))

            ;; ---- heartbeat / election tick ----
            ;; Bound durable-write ack latency to one tick: fsync any buffered
            ;; writes (leader batch AND a follower's applied-but-unsynced entries)
            ;; and ack any pending batch before doing Raft tick work.
            ;; flush-and-drain! is a no-op when nothing is dirty / deferred.
            ((eq? (car m) 'tick)
             (flush-and-drain! flush-base)
             (cond
               ((raft-leader? st)
                ; CheckQuorum: if we lost quorum contact this window, step down —
                ; abandon in-flight writes (fail-pending!) and republish with no
                ; leader. Otherwise heartbeat.
                (let ((cq (raft-checkquorum st timeout)))
                  (if (raft-leader? cq)
                      (let ((r (raft-tick cq)))
                        (emit! (cdr r))
                        ; Lease expiry (cw-u4a.17, ADR 0003 §2): RIDES this same tick.
                        ; Seed/re-derive deadlines + propose ("LEASE-REVOKE" id) for any
                        ; expired lease.  propose-internal! threads the state + emits the
                        ; revoke AEs (+ solo-commits) itself; returns (st' . flush-base').
                        ; A strict no-op when no leases are granted (empty meta scan), so
                        ; the heartbeat path is unchanged for the leaseless sim-cluster.
                        (let ((lr (lease-tick! (car r) #f)))
                          (loop (maybe-compact (car lr) (cdr lr)) node-name 0 (cdr lr))))
                      (begin (fail-pending!) (publish! cq #f)
                             (loop cq #f 0 #f)))))
               (solo (loop (maybe-compact st #f) leader elapsed #f))
               ((>= elapsed timeout)
                ; PreVote round (NO term bump): become pre-candidate + solicit
                ; pre-votes. The real election (raft-campaign) starts only on a
                ; pre-vote majority — handled in the engine branch on `prvr`.
                (let* ((r (raft-prevote st)) (st2 (car r)))
                  (emit! (cdr r))
                  (loop st2 leader 0 #f)))
               (else (loop st leader (+ elapsed 1) #f))))

            ;; ---- test-support: (get CONN K) -> this replica's MVCC view of K ----
            ;; Reads the REAL committed MVCC state on THIS node's ctx via the same
            ;; mvcc-get-latest read path the apply-fn uses, and replies a small
            ;; serializable summary: (value-bytes create-rev mod-rev version) for a
            ;; live key, or #f if absent/tombstoned. Lets a test assert that the MVCC
            ;; write path replicated identically across every voter. Not part of the
            ;; consensus protocol — a harness probe; .7 Range generalises this seam.
            ((eq? (car m) 'get)
             (let* ((conn (cadr m)) (k (caddr m))
                    (r (mvcc-get-latest ctx k)))
               (send conn
                     (if r
                         (list (kv-rec-value r) (kv-rec-create-rev r)
                               (kv-rec-mod-rev r) (kv-rec-version r))
                         #f)))
             (loop st leader elapsed flush-base))

            ;; ---- KV read seam: (kv-range CONN OPTS) -> RangeResponse data  (cw-u4a.22)
            ;; The etcd KV gRPC binding (.22) serves Range/point reads here, where the
            ;; ctx lives.  OPTS is a SENDABLE assoc-list carrying the mvcc-range request:
            ;;   key / range-end (bytevectors) + the range opts (revision/limit/
            ;;   count-only/keys-only/sort-order/sort-target/min-max revs) as the
            ;;   symbols mvcc-range already consumes.  LEADER-GATED like reads: a
            ;;   non-leader replies 'tryagain (the gRPC handler maps that to Unavailable
            ;;   so the client retries another endpoint — single-node is always leader,
            ;;   so this is the served path for THIS task).  We reply a fully SENDABLE
            ;;   summary the handler turns into protobuf:
            ;;     (kv-range-ok current-rev raft-term err-or-#f total
            ;;                  ((key-bytes value-bytes create-rev mod-rev version lease) ...))
            ;;   where err-or-#f is 'compacted (ErrCompacted -> gRPC OutOfRange) else #f,
            ;;   total is the pre-limit match count (etcd's `count`), and each kv is a
            ;;   flat list (kv-view records can't cross `send`).  count-only yields the
            ;;   total with an empty kv list; keys-only blanks the value bytes.
            ;;   A pure read over THIS node's committed ctx — not a Raft entry.
            ((eq? (car m) 'kv-range)
             (let ((conn (cadr m)) (opts (caddr m)))
               (if (not (raft-leader? st))
                   (send conn 'tryagain)
                   (let* ((key (range-opt opts 'key (make-bytevector 0 0)))
                          (rend (range-opt opts 'range-end #f))
                          (res (mvcc-range ctx key rend opts)))
                     (if (and (pair? res) (eq? (car res) 'err-compacted))
                         (send conn (list 'kv-range-ok (mvcc-current-rev ctx)
                                          (raft-term st) 'compacted 0 '()))
                         (send conn
                               (list 'kv-range-ok (mvcc-current-rev ctx) (raft-term st) #f
                                     (car res)
                                     (map (lambda (item)
                                            (let ((uk (car item)) (rec (cdr item)))
                                              (list uk (kv-rec-value rec)
                                                    (kv-rec-create-rev rec) (kv-rec-mod-rev rec)
                                                    (kv-rec-version rec) (kv-rec-lease rec))))
                                          (cdr res))))))) )
             (loop st leader elapsed flush-base))

            ;; ---- KV prev-kv seam: (kv-prev CONN KEY) -> the key's CURRENT live record
            ;; (cw-u4a.22).  etcd Put/DeleteRange prev_kv returns the value BEFORE the op;
            ;; the handler snapshots it through here just before proposing the write.
            ;; Reply: (kv-prev-ok (key-bytes value create-rev mod-rev version lease) | #f).
            ;; Single-node, single-writer, so this read-then-propose is atomic enough for
            ;; v1 (real etcd resolves prev_kv inside the apply txn; noted as a v1 gap).
            ((eq? (car m) 'kv-prev)
             (let* ((conn (cadr m)) (k (caddr m))
                    (rec (mvcc-get-latest ctx k)))
               (send conn
                     (list 'kv-prev-ok
                           (if rec
                               (list k (kv-rec-value rec) (kv-rec-create-rev rec)
                                     (kv-rec-mod-rev rec) (kv-rec-version rec) (kv-rec-lease rec))
                               #f))))
             (loop st leader elapsed flush-base))

            ;; ---- KV header seam: (cur-rev CONN) -> (cur-rev-ok current-rev raft-term)
            ;; (cw-u4a.22).  Every etcd response carries a ResponseHeader{revision,
            ;; raft_term, ...}.  After an async write ack (whose result shape — ("PUT" .
            ;; rev) etc. — is fixed by the existing apply contract and must not change),
            ;; the handler reads the store's CURRENT revision + the Raft term here to
            ;; fill the header.  A trivial leader-local read; never a Raft entry.
            ((eq? (car m) 'cur-rev)
             (let ((conn (cadr m)))
               (send conn (list 'cur-rev-ok (mvcc-current-rev ctx) (raft-term st))))
             (loop st leader elapsed flush-base))

            ;; ---- test-support: (lease-probe CONN ID KEYS) -> revoke proof on THIS
            ;; replica.  Reads THIS node's committed MVCC state and replies a small
            ;; serializable summary so a test can assert the LINEARIZABLE replicated
            ;; revoke (cw-u4a.17): the lease meta is gone + each attached key's NEWEST
            ;; version is a tombstone at the SAME mod_rev on every voter.  Reply shape:
            ;;   (lease-exists? current-rev ((KEY-BYTES TOMBSTONE? MOD-REV) ...))
            ;; where, per key, we read the NEWEST KEY-CF version directly (the first
            ;; INV-ordered row), so a tombstone's delete-revision is visible (the `get`
            ;; probe hides tombstones as #f).  A harness probe only, not consensus.
            ((eq? (car m) 'lease-probe)
             (let* ((conn (cadr m)) (id (caddr m)) (keys (cadddr m)))
               (send conn
                     (list (mvcc-lease-exists? ctx id)
                           (mvcc-current-rev ctx)
                           (map
                            (lambda (k)
                              (let ((rows (kv-scan ctx (key-cf-prefix k))))
                                (if (null? rows)
                                    (list k 'absent 0)        ; never written
                                    (let ((rec (kv-record-decode (cdar rows))))  ; newest version
                                      (list k (kv-rec-tombstone? rec) (kv-rec-mod-rev rec))))))
                            keys))))
             (loop st leader elapsed flush-base))

            ;; ---- Watch register: (watch-register REPLY-PID SPEC) ----  (cw-u4a.14)
            ;; The per-conn streaming actor (.14) asks the LEADER's shard registry to
            ;; establish a watcher.  LEADER-GATED exactly like reads: only the leader
            ;; applies + holds client streams (ADR 0002 §4), so a non-leader must
            ;; redirect rather than silently serve a watch — it replies
            ;; ('watch-not-leader . LEADER) so the streaming actor re-targets the new
            ;; leader (mirrors the read path's 'tryagain).
            ;;
            ;; SPEC is the sendable assoc-list watch-spec (keys: key/range-end/
            ;; start-rev/filters/prev-kv/watch-id — all bytevectors/ints/symbols, so
            ;; it crosses the actor boundary intact).  The registry's deliver-fn must
            ;; push a SENDABLE shape to REPLY-PID (a watch-response RECORD can't cross
            ;; a `send`), so we flatten it with watch-response->sexp and tag it
            ;; 'watch-response.  watch-register! runs the REGISTER->REPLAY->CATCH-UP->
            ;; PROMOTE handoff on THIS single thread (so the §3 seam stays race-free),
            ;; delivering any historical replay to REPLY-PID before we ack the id.
            ;; The reply to the REGISTER request itself is the assigned watch-id, or
            ;; ('watch-compacted . COMPACT-REV) if start-rev is below the floor (§5).
            ((eq? (car m) 'watch-register)
             (let ((reply-pid (cadr m)) (spec (caddr m)))
               (if (not (raft-leader? st))
                   (send reply-pid (cons 'watch-not-leader leader))
                   (let* ((deliver-fn
                           (lambda (wr)
                             (send reply-pid (list 'watch-response (watch-response->sexp wr)))))
                          (res (watch-register! watch-reg ctx spec deliver-fn)))
                     (if (and (pair? res) (eq? (car res) 'compacted))
                         (send reply-pid (cons 'watch-compacted (cdr res)))
                         (send reply-pid (cons 'watch-created res)))))   ; res = watch-id
               (loop st leader elapsed flush-base)))

            ;; ---- Watch cancel: (watch-cancel REPLY-PID WATCH-ID) ----  (cw-u4a.14)
            ;; Deregister the watcher + (via its deliver-fn) emit a canceled
            ;; WatchResponse to the stream, then ack the cancel to REPLY-PID.  Also
            ;; leader-gated: the registry lives only on the leader.  watch-cancel!
            ;; runs on this single thread so a cancel concurrent with an in-flight
            ;; dispatch is serialized (no use-after-cancel, ADR 0002 §6).
            ((eq? (car m) 'watch-cancel)
             (let ((reply-pid (cadr m)) (wid (caddr m)))
               (if (not (raft-leader? st))
                   (send reply-pid (cons 'watch-not-leader leader))
                   (let ((ok (watch-cancel! watch-reg wid)))
                     (send reply-pid (cons 'watch-canceled (if ok wid #f)))))
               (loop st leader elapsed flush-base)))

            ;; ---- Lease grant: (lease-grant REPLY-PID TTL ID) ----  (cw-u4a.17)
            ;; The client's LeaseGrant.  LEADER-GATED exactly like watch-register /
            ;; reads (ADR 0003 §1/§2: only the leader applies + owns the deadline map),
            ;; so a non-leader REDIRECTS — ('lease-not-leader . LEADER) — rather than
            ;; serving a grant no one would expire.  The leader PROPOSES
            ;; ("LEASE-GRANT" id ttl) through Raft (id=0 ⇒ apply auto-assigns from the
            ;; replicated lease-id-seq); the apply result (cons "LEASE-GRANT" assigned-id)
            ;; is drained back to REPLY-PID via `pending` (same async commit->ack bridge
            ;; as a client PUT), so the client learns the assigned id only after the
            ;; grant commits on a quorum.  The leader-local deadline is seeded by the
            ;; lease-tick (the meta entry is durable once applied), so failover re-derives
            ;; it identically — no special-casing here.
            ((eq? (car m) 'lease-grant)
             (let ((reply-pid (cadr m)) (ttl (caddr m)) (id (cadddr m)))
               (cond
                 ((not (raft-leader? st))
                  (send reply-pid (cons 'lease-not-leader leader))
                  (loop st leader elapsed flush-base))
                 (else
                  (let ((old (raft-applied st)) (idx (+ 1 (log-len st)))
                        (cmd (list (string->utf8 "LEASE-GRANT") (int->bytes id) (int->bytes ttl))))
                    (hashtable-set! pending idx reply-pid)
                    (let* ((r (raft-propose st cmd)) (st1 (car r)))
                      (emit! (cdr r))
                      (let ((st2 (maybe-commit st1)))
                        (if (> (raft-applied st2) old) (persist-applied! st2))
                        (let ((nb (settle! #t old flush-base)))
                          (loop (maybe-compact st2 nb) node-name elapsed nb)))))))))

            ;; ---- Lease revoke: (lease-revoke REPLY-PID ID) ----  (cw-u4a.17)
            ;; The client's explicit LeaseRevoke.  Takes the SAME replicated path as
            ;; expiry (ADR 0003 §2/§5): the leader PROPOSES ("LEASE-REVOKE" id), whose
            ;; apply tombstones all attached keys + drops the meta at one revision on
            ;; every replica.  Leader-gated (redirect on a non-leader).  The apply
            ;; result (cons "LEASE-REVOKE" (cons rev count)) is drained to REPLY-PID via
            ;; `pending`.  (Mark it revoke-in-flight so the concurrent tick scan doesn't
            ;; also propose it.)
            ((eq? (car m) 'lease-revoke)
             (let ((reply-pid (cadr m)) (id (caddr m)))
               (cond
                 ((not (raft-leader? st))
                  (send reply-pid (cons 'lease-not-leader leader))
                  (loop st leader elapsed flush-base))
                 (else
                  (let ((old (raft-applied st)) (idx (+ 1 (log-len st)))
                        (cmd (list (string->utf8 "LEASE-REVOKE") (int->bytes id))))
                    (hashtable-delete! lease-deadlines id)   ; stop the tick scanning it
                    (hashtable-set! lease-revoking id #t)    ; mark in flight (no double-propose)
                    (hashtable-set! pending idx reply-pid)
                    (let* ((r (raft-propose st cmd)) (st1 (car r)))
                      (emit! (cdr r))
                      (let ((st2 (maybe-commit st1)))
                        (if (> (raft-applied st2) old) (persist-applied! st2))
                        (let ((nb (settle! #t old flush-base)))
                          (loop (maybe-compact st2 nb) node-name elapsed nb)))))))))

            ;; ---- Lease keepalive: (lease-keepalive REPLY-PID ID) ----  (cw-u4a.18)
            ;; Leader-local deadline reset — NOT a Raft entry (ADR 0003 §3).  Only
            ;; the leader owns the `lease-deadlines` map; a non-leader replies the
            ;; standard redirect.  If the lease-meta exists: reset the deadline to
            ;; now + granted_ttl (a full fresh window) and reply (keepalive-ok id
            ;; granted_ttl).  If the lease no longer exists (expired/revoked): reply
            ;; (keepalive-ok id 0) — etcd's TTL-zero signal that the lease is gone.
            ;; NO Raft round, NO rev bump (§6).
            ((eq? (car m) 'lease-keepalive)
             (let ((reply-pid (cadr m)) (id (caddr m)))
               (if (not (raft-leader? st))
                   (send reply-pid (cons 'lease-not-leader leader))
                   (let ((ttl (mvcc-lease-meta-get ctx id)))
                     (if ttl
                         (begin
                           (hashtable-set! lease-deadlines id (+ (current-second) ttl))
                           (send reply-pid (list 'keepalive-ok id ttl)))
                         (send reply-pid (list 'keepalive-ok id 0)))))
               (loop st leader elapsed flush-base)))

            ;; ---- Lease TTL: (lease-ttl REPLY-PID ID WITH-KEYS?) ----  (cw-u4a.18)
            ;; Leader-gated pure read.  Replies (lease-ttl-ok id granted-ttl remaining keys):
            ;;   granted-ttl : the replicated TTL from lease-meta (mvcc-lease-meta-get).
            ;;   remaining   : ceil(deadline - now) from the leader-local map; -1 if the
            ;;                 lease is absent/expired (no meta OR no deadline entry).
            ;;   keys        : (mvcc-lease-keys ctx id) when with-keys? = #t, else '().
            ;; Non-leader redirects.
            ((eq? (car m) 'lease-ttl)
             (let ((reply-pid (cadr m)) (id (caddr m)) (with-keys? (cadddr m)))
               (if (not (raft-leader? st))
                   (send reply-pid (cons 'lease-not-leader leader))
                   (let ((ttl (mvcc-lease-meta-get ctx id)))
                     (let* ((deadline (hashtable-ref lease-deadlines id #f))
                            (now      (current-second))
                            (remaining (if (and ttl deadline)
                                           (max 0 (exact (ceiling (- deadline now))))
                                           -1))
                            (keys     (if with-keys? (mvcc-lease-keys ctx id) '())))
                       (send reply-pid (list 'lease-ttl-ok id (if ttl ttl 0) remaining keys)))))
               (loop st leader elapsed flush-base)))

            ;; ---- Lease list: (lease-leases REPLY-PID) ----  (cw-u4a.18)
            ;; Returns all live lease ids by scanning the NS-LEASE sentinel meta entries
            ;; (mvcc-all-lease-ids — already the failover re-derivation source).  Kept
            ;; leader-gated for consistency with keepalive/ttl (the ids are replicated so
            ;; any node could serve it, but a single authoritative path simplifies testing).
            ((eq? (car m) 'lease-leases)
             (let ((reply-pid (cadr m)))
               (if (not (raft-leader? st))
                   (send reply-pid (cons 'lease-not-leader leader))
                   (send reply-pid (list 'lease-leases-ok (mvcc-all-lease-ids ctx))))
               (loop st leader elapsed flush-base)))

            ;; ---- linearizable read probe: (read CONN) ----
            ;; Solo serves inline (a round would never complete); multi-voter
            ;; ReadIndex: enqueue + (if idle) open a confirmation round; serve-batch!
            ;; replies once a quorum AER confirms. A non-leader tryagains.
            ((eq? (car m) 'read)
             (let ((conn (cadr m)))
               (cond
                 ((not (raft-leader? st))
                  (send conn 'tryagain) (loop st leader elapsed flush-base))
                 (solo
                  (send conn (cons 'read-ok (raft-applied st)))
                  (loop st leader elapsed flush-base))
                 (else
                  (set! read-q (cons (cons conn #f) read-q))
                  (let ((st2 (if round-open? st (start-read-round! st))))
                    (loop st2 leader elapsed flush-base))))))

            ;; ---- local client command: (conn-pid . cmd) ----
            ;; EVERY client proposal is a write routed through Raft in this Phase-0
            ;; stub (no Redis write/read classification).
            (else
             (let* ((conn (car m)) (cmd (cdr m)))
               (cond
                 ((not (raft-leader? st))
                  (send conn 'tryagain) (loop st leader elapsed flush-base))
                 (else
                  (let ((old (raft-applied st)) (idx (+ 1 (log-len st))))
                    (hashtable-set! pending idx conn)
                    (let* ((r (raft-propose st cmd)) (st1 (car r)))
                      (emit! (cdr r))                 ; AE to followers (cluster) / none (solo)
                      (let ((st2 (maybe-commit st1)))  ; solo commits now; cluster waits for AERs
                        (if (> (raft-applied st2) old) (persist-applied! st2))
                        ; this branch only runs on the leader -> #t
                        (let ((nb (settle! #t old flush-base)))  ; defer ack (durable) or ack now
                          (loop (maybe-compact st2 nb) node-name elapsed nb)))))))))))))))
