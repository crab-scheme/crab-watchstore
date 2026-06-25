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
(include "src/auth.scm")      ; NS-AUTH storage + permission check (cw-u4a.25/.26): the
                              ; shard applies AUTH-* (mvcc-apply) + serves the auth read seams.
(include "src/watch.scm")     ; Watch backend: registry + replay->live dispatch (cw-u4a.13)
(include "src/raft.scm")
(include "src/rev-lease-consumer.scm")  ; cw-kp0: writer-side revision lease + PUT->PUT-AT rewrite (global-rev mode)

(define (raft-applied st) (aget st 'applied))

(define (index-of x lst)
  (let loop ((i 0) (l lst))
    (cond ((null? l) 0) ((eqv? (car l) x) i) (else (loop (+ i 1) (cdr l))))))

; Optional rest args (positional):
;   1 (cw-b5w.4)  apply-worker count. 1 (the default — every existing
;     caller/test) = the original fully-serial apply path, byte-for-byte.
;     >1 = ADR 0005 option B: sequencer stamping + N hash-routed workers
;     with a per-batch barrier (see flush-materializations!).
;   2 (cw-lkq.1)  election-ticks BASE (default 4): the election timeout in
;     ticks before the deterministic per-node stagger (+3 per rank). One tick
;     = the poller's tick-ms, so election-timeout-ms = tick-ms * (base+stagger)
;     — etcd's --election-timeout analogue. CheckQuorum + the PreVote grant
;     gate use the same value, so WAN profiles scale all three together.
;   3 (cw-lkq.2)  leader-region: preferred leader region string, or #f.
;   4 (cw-lkq.2)  region-map: ((node-name . region-or-#f) ...) for all members.
;   5 (cw-lkq.6)  serializable-max-lag: refuse serializable reads when
;     commit - applied exceeds this (0 = no freshness gate).
(define (shard-main shard-key voters node-name db-path sync? . rest)
  (let* ((n-apply-workers (if (and (pair? rest) (number? (car rest)) (> (car rest) 0))
                              (car rest) 1))
         (election-base (if (and (pair? rest) (pair? (cdr rest))
                                 (number? (cadr rest)) (> (cadr rest) 0))
                            (cadr rest) 4))
         (leader-region (if (and (pair? rest) (pair? (cdr rest)) (pair? (cddr rest))
                                 (string? (list-ref rest 2)))
                            (list-ref rest 2) #f))
         (region-map (if (and (pair? rest) (>= (length rest) 4) (list? (list-ref rest 3)))
                         (list-ref rest 3) '()))
         (ser-max-lag (if (and (>= (length rest) 5) (number? (list-ref rest 4)))
                          (list-ref rest 4) 0))
         ; 6 (cw-85j) genesis LEARNERS: non-voting members seated at bootstrap
         ; (a WAN local-majority config). Default '() = every existing caller /
         ; the original all-voters genesis.
         (genesis-learners (if (and (>= (length rest) 6) (list? (list-ref rest 5)))
                               (list-ref rest 5) '()))
         ; 7 (cw-e9a) leader-node: pin leadership to THIS named voter (not just a
         ; region). #f = no node pin (use leader-region / shard-rotated stagger).
         (leader-node (if (and (>= (length rest) 7) (symbol? (list-ref rest 6)))
                          (list-ref rest 6) #f))
         ; 8 (cw-kp0) global-rev?: when #t this group participates in the synthetic
         ; global revision allocator — shard "0" is the rev-authority (holds the
         ; replicated global-rev counter, ADR 0006), others request grants from it.
         ; Default #f = today's per-shard revision (single-group semantics intact);
         ; the rest of the wiring (grant request at propose, watermark gate) gates
         ; on this flag, so OFF is byte-identical to the current path.
         (global-rev? (if (>= (length rest) 8) (and (list-ref rest 7) #t) #f))
         (rev-authority? (and global-rev? (string=? shard-key "0")))
         ; cw-gx4: this group's cs-net channel — one Raft group → one channel so
         ; groups don't serialize on Messages. SHARD-CHANNELS = {1,3,4,5}
         ; (Consensus/Workflow/Bulk/Observability); Control=0 + Messages=2 are
         ; reserved. Groups beyond 4 share a channel (still N-way parallel up to 4).
         (my-group (let ((n (string->number shard-key))) (if n n 0)))
         (my-channel (vector-ref '#(1 3 4 5) (modulo my-group 4)))
         (handle  (store-open db-path #t))      ; create-if-missing
         (ctx     (make-ctx handle "default" sync?))
         (apply-workers
          (if (< n-apply-workers 2) #f
              (let spawn-w ((i 0) (acc '()))
                (if (= i n-apply-workers) (list->vector (reverse acc))
                    (spawn-w (+ i 1)
                             (cons (spawn-source-dedicated
                                    "(include \"src/server/apply-worker.scm\")"
                                    'apply-worker-main handle "default" sync?)
                                   acc))))))
         (pending (make-eqv-hashtable))          ; log-index -> conn-pid
         ; ---- dynamic membership (cw-u4a.29) — LEADER-LOCAL, additive. `member-reply-pid`
         ; is the pid of the caller awaiting the in-flight member-add/remove/promote (one at
         ; a time, gated by conf-change-pending?); it is acked only when the ConfChange fully
         ; settles (simple/learner committed, or the joint's auto-Cnew committed), or replied
         ; 'member-indeterminate when this leader steps down mid-change. `conf-skip` holds the
         ; ABSOLUTE log indices of ConfChange entries: apply-committed advances `applied` past
         ; them WITHOUT calling apply-fn, so they carry NO `acc` slot — drain! must skip those
         ; indices or the next client write's ack shifts onto the conf index (a cross-wire).
         ; Both are empty/idle on a follower and on the fixed-config path (no member-* ever),
         ; so this is strictly additive — drain! with an empty conf-skip is the original loop.
         (member-reply-pid #f)
         (conf-skip (make-eqv-hashtable))         ; absolute log-index -> #t for ConfChange entries
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
         ; cw-e9a: when leader-node is pinned, that voter gets the strictly-shortest
         ; timeout (election-base) and every other voter is staggered strictly above
         ; it (base + 3 + index*3, distinct per node => no split votes), so the pinned
         ; node wins the initial election; non-voters/absent pin falls through.
         (timeout (if (and leader-node (member leader-node voters))
                      (if (eqv? node-name leader-node)
                          election-base
                          (+ election-base 3 (* (index-of node-name voters) 3)))
                      (+ election-base (* (modulo (- (index-of node-name voters)
                                     (let ((n (string->number shard-key))) (if n n 0)))
                                  (length voters))
                          3)))))
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
    ; ---- parallel PUT materialization (cw-b5w.4, ADR 0005 option B) ----
    ; With n-apply-workers > 1, a committed plain PUT is split: the SEQUENCER
    ; (here) does the order-dependent half — lease guard, revision assignment,
    ; current-rev bump, the client ack — and queues the heavy half (prev-record
    ; lookup + record/event encode + RocksDB writes) for a key-hash-routed
    ; worker.  flush-materializations! dispatches the queues and BARRIERS on
    ; every worker before anything can observe the batch: persist-applied!
    ; (crash safety: the applied marker must never outrun the data), any serial
    ; (non-PUT) apply (DEL/TXN/COMPACT/LEASE/AUTH read live state), and watch
    ; emission (REV-CF events must exist, in revision order).  Reads/acks are
    ; per-iteration downstream of persist-applied!, so they never see a gap —
    ; this barrier is what makes ADR 0005's watermark unnecessary.
    (define mat-queues (if apply-workers (make-vector n-apply-workers '()) #f))
    (define mat-count 0)
    (define mat-pre #f)                    ; current-rev BEFORE the first queued PUT
    (define (kbucket K)                    ; stable per-key worker route (djb2 mod N)
      (let loop ((i 0) (h 5381))
        (if (= i (bytevector-length K)) (modulo h n-apply-workers)
            (loop (+ i 1) (modulo (+ (* h 33) (bytevector-u8-ref K i)) 16777216)))))
    ; Stamp one committed PUT (mirrors mvcc-apply's PUT branch minus mvcc-put!):
    ; same dead-lease guard (lease meta is stable during a parallel run — every
    ; LEASE-* apply is serial and flushes first), same no-write-no-bump on guard
    ; failure, same ("PUT" . rev) ack.
    (define (stamp-put! cmd)
      (let* ((K     (list-ref cmd 1))
             (V     (list-ref cmd 2))
             (lease (if (>= (length cmd) 4)
                        (let ((l (bytes->int (list-ref cmd 3)))) (if l l 0))
                        0))
             (main  (+ (mvcc-current-rev ctx) 1)))
        (if (and (not (= lease 0)) (not (mvcc-lease-exists? ctx lease)))
            (cons 'err-lease-not-found lease)
            (begin
              (if (not mat-pre) (set! mat-pre (mvcc-current-rev ctx)))
              (let ((w (kbucket K)))
                (vector-set! mat-queues w
                             (cons (list K V lease main) (vector-ref mat-queues w))))
              (set! mat-count (+ mat-count 1))
              (mvcc-set-current-rev! ctx main)
              (cons "PUT" main)))))
    ; Dispatch queued slices (rev-ascending per worker) + barrier on completion.
    ; Messages that arrive while waiting are stashed onto `backlog` in order.
    (define (flush-materializations!)
      (if (> mat-count 0)
          (let ((sent (let dispatch ((w 0) (n 0))
                        (if (= w n-apply-workers) n
                            (if (pair? (vector-ref mat-queues w))
                                (begin
                                  (send (vector-ref apply-workers w)
                                        (list 'apply-slice (self)
                                              (reverse (vector-ref mat-queues w))))
                                  (vector-set! mat-queues w '())
                                  (dispatch (+ w 1) (+ n 1)))
                                (dispatch (+ w 1) n))))))
            (let wait ((need sent))
              (if (> need 0)
                  (let ((r (raw-receive)))
                    (if (and (pair? r) (eq? (car r) 'apply-slice-done))
                        (wait (- need 1))
                        (begin (set! backlog (append backlog (list r)))
                               (wait need))))))
            ; the whole batch is durable-visible: emit its watch window at once
            (watch-on-apply! watch-reg ctx mat-pre (mvcc-current-rev ctx))
            (set! mat-count 0)
            (set! mat-pre #f))))
    (define (apply-fn sm cmd)
      (if (null? cmd)
          (set! acc (cons #f acc))                       ; no-op barrier: acc slot only, no rev bump
          (if (and apply-workers (string=? (cmd-op cmd) "PUT"))
              (set! acc (cons (stamp-put! cmd) acc))     ; parallel path: stamp now, materialize on flush
          (let ((pre (mvcc-current-rev ctx)))
            (flush-materializations!)                    ; serial ops read live state: batch must land first
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
                (watch-check-compaction! watch-reg ctx)))))
      (+ sm 1))
    ; Persist the applied index (+ its term) into the SAME group-commit batch as
    ; the entry's mutations, so a restart restores base/applied/commit and never
    ; re-applies already-applied committed entries (idempotent recovery/rejoin).
    (define (persist-applied! st)
      ; cw-b5w.4 barrier: the applied marker must never be persisted (let alone
      ; fsynced) before every materialized write of the entries it covers.
      (flush-materializations!)
      (ctx-save-applied! ctx (raft-applied st) (entry-term st (raft-applied st))))
    ; ship engine outputs (target-node . rpc) to peers over the node transport.
    ; A send to a DOWN peer must not crash us — Raft is lossy-tolerant and
    ; recovers the entry on the next heartbeat/AE, so swallow transport errors.
    ; cw-gx4: all of this group's inter-node frames route on MY-CHANNEL so
    ; independent groups drain in parallel (TO is already a node-name string).
    (define (gsend to msg) (node-send-ch (symbol->string node-name) to my-channel msg))
    (define (emit! outs)
      (for-each
       (lambda (o)
         (guard (e (#t #f))
           (gsend (symbol->string (car o))
                  (list 'ws-engine shard-key node-name (cdr o)))))
       outs))
    ; match in-order applied replies to the indices that produced them. `acc` holds one
    ; slot per applied NON-conf entry (apply-fn pushes it; the no-op barrier pushes #f); a
    ; ConfChange entry applies WITHOUT an `acc` slot, so its index must be SKIPPED here (no
    ; acc slot consumed) — otherwise the next client write's reply lands on the conf index
    ; and every following reply shifts by one (a cross-wire). With an empty conf-skip (the
    ; fixed-config path) this is the original positional loop, unchanged (cw-u4a.29).
    ; Deliver an ack to a pending waiter. A LOCAL waiter is a conn pid; a
    ; FORWARDED write's waiter (cw-lkq.13) is the stand-in (fwd ORIGIN . ID) —
    ; its ack rides 'ws-fwd-reply back to the origin member, whose shard relays
    ; to the real conn from ITS fwd-pending table.
    (define (ack-waiter! conn payload)
      (cond
        ; EXP5 (cw-juw) async write: conn = (async WORKER-PID HANDLE). The worker
        ; submitted without blocking; reply (put-done HANDLE payload) and it
        ; encodes + grpc-respond!s. Same pending/drain machinery, new sink.
        ((and (pair? conn) (eq? (car conn) 'async))
         (guard (e (#t #f)) (send (cadr conn) (list 'put-done (caddr conn) payload))))
        ((and (pair? conn) (eq? (car conn) 'fwd))
         (guard (e (#t #f))
           (gsend (symbol->string (cadr conn))
                      (list 'ws-fwd-reply shard-key node-name (cddr conn) payload))))
        (else (send conn payload))))
    (define (drain! old-applied)
      (let loop ((idx (+ old-applied 1)) (rs (reverse acc)))
        (if (pair? rs)
            (if (hashtable-contains? conf-skip idx)
                (loop (+ idx 1) rs)                       ; conf entry: no acc slot — skip index
                (let ((conn (hashtable-ref pending idx #f)))
                  (if conn (begin (ack-waiter! conn (car rs)) (hashtable-delete! pending idx)))
                  (loop (+ idx 1) (cdr rs))))))
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
       (lambda (conn) (ack-waiter! conn 'indeterminate))
       (hashtable-values pending))
      (hashtable-clear! pending)
      ; Reads are idempotent — 'tryagain so the client safely retries against the
      ; new leader (a ReadIndex round we can no longer confirm here).
      (for-each (lambda (e) (send (car e) 'tryagain)) batch)
      (for-each (lambda (e) (send (car e) 'tryagain)) read-q)
      (set! batch '()) (set! read-q '()) (set! read-acks '()) (set! round-open? #f)
      ; cw-u4a.29: drop the ConfChange skip-indices on stepdown — `pending` was just
      ; cleared above, so a later drain! has no client waiter to mis-route, and a fresh
      ; leader re-derives skip-indices as it proposes. (The in-flight member-* caller, if
      ; any, is acked at the stepdown CALL SITE: 'member-indeterminate on a deposing higher
      ; term / lost quorum, or 'member-ok when the change's own Cnew committed.)
      (hashtable-clear! conf-skip)
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
    ; GROUP COMMIT (cw-b5w.3): max client writes proposed per emit!/commit/settle
    ; round (the mailbox drain in the client-command branch), bounding the extra
    ; latency the last write in a batch can pick up under a flooded mailbox.
    (define PROPOSE-BATCH-CAP 64)
    ; --- TEMP write-path profiling (cw-jkz): batch size + round cadence ---
    (define prof-writes 0) (define prof-rounds 0) (define prof-t0 (current-second))
    ; EXP3 (cw-cex): leader emit(write)->commit round-trip. last-emit set when a
    ; write batch is emitted; on the AER that advances applied, accumulate the delta.
    (define last-emit #f) (define rtt-sum 0.0) (define rtt-n 0)
    (define (rtt-mark-emit!) (set! last-emit (current-second)))
    (define (rtt-on-commit!)
      (if last-emit
          (begin
            (set! rtt-sum (+ rtt-sum (- (current-second) last-emit)))
            (set! rtt-n (+ rtt-n 1)) (set! last-emit #f)
            (if (>= rtt-n 500)
                (begin
                  (display "RTT emit->commit avg-ms=") (display (* 1000.0 (/ rtt-sum rtt-n)))
                  (display " n=") (display rtt-n) (newline)
                  (set! rtt-sum 0.0) (set! rtt-n 0))))))
    (define (prof-tick! n)
      (set! prof-writes (+ prof-writes n)) (set! prof-rounds (+ prof-rounds 1))
      (if (>= prof-writes 500)
          (let ((dt (- (current-second) prof-t0)))
            (display "PROF w=") (display prof-writes)
            (display " rounds=") (display prof-rounds)
            (display " avgbatch=") (display (exact->inexact (/ prof-writes prof-rounds)))
            (display " w/s=") (display (/ prof-writes (if (> dt 0) dt 1)))
            (display " ms/round=") (display (* 1000.0 (/ dt prof-rounds)))
            (newline)
            (set! prof-writes 0) (set! prof-rounds 0) (set! prof-t0 (current-second)))))
    ; the non-write message the drain stopped on, replayed before the mailbox.
    (define backlog '())
    ; ---- follower-forwarded linearizable reads (cw-lkq.13) ----
    ; etcd serves LINEARIZABLE reads on ANY member by forwarding to the leader
    ; internally; clientv3 (kube-apiserver) round-robins endpoints and does NOT
    ; retry our 'not leader' redirect on the storage-cacher path. So a follower
    ; forwards the Range over the node mesh ('ws-fwd) to the leader, which
    ; serves it and ships the reply back ('ws-fwd-reply); the follower relays
    ; to the waiting local conn. fwd-pending maps request-id -> conn; entries
    ; for lost replies (leader died mid-forward) are dropped when the client
    ; retries (conn actors time out client-side).
    ; entries are (conn . expiry-tick); swept every tick so a forward to a
    ; dying/dead leader cannot wedge the asker forever (the member's gRPC actor
    ; serializes unary calls — one lost reply would hang the whole KV API).
    (define snap-accum #f)        ; in-flight inbound snapshot (BASE TERM ROWS) | #f
    (define fwd-pending (make-eqv-hashtable))
    (define fwd-seq 0)
    (define fwd-ticks 0)
    ; ---- cw-kp0 global-rev: writer-side revision lease (gated on global-rev? AND
    ; NOT rev-authority?). The leader draws a global rev per write from this buffer
    ; (global-rev-rewrite) and refills it from the authority before it empties
    ; (rev-refill-inflight = the block size of an outstanding REV-GRANT request, #f
    ; when none). All inert unless global-rev? — the default single-group path never
    ; touches these. The propose-branch hook that consumes them is the next increment.
    (define REV-BLOCK 64)                 ; revs per refill — amortizes the grant round-trip
    (define REV-LOW   8)                   ; refill when remaining drops to this
    (define rev-lease (lease-new))         ; FIFO buffer of granted (lo . hi) blocks
    (define rev-refill-inflight #f)        ; block size of an outstanding REV-GRANT, or #f
    ; authority-side low-watermark aggregator (rev-authority? only): writer-shard-num ->
    ; lowest global rev that writer has NOT yet applied (#f = caught up to its lease).
    ; W = min over writers-with-unapplied of (lowest-unapplied - 1), else granted-high —
    ; an idle/caught-up writer never freezes W below high (ADR 0006).
    (define rev-progress (make-eqv-hashtable))
    (define gr-writer? (and global-rev? (not rev-authority?)))   ; this group draws global revs
    ; the local rev-authority (shard "0") replica pid; a grant request to a follower
    ; replica forwards to the authority leader via the standard fwd-write path, and the
    ; reply returns here — so the writer just sends to its local shard-0 pid.
    (define (gr-authority-pid)
      (and gr-writer? (table-lookup 'ws-shard-pid (string-append (symbol->string node-name) ":0"))))
    ; async refill: request REV-BLOCK revs if low and none in flight (never blocks).
    (define (gr-maybe-refill!)
      (if (and gr-writer? (not rev-refill-inflight) (lease-needs-refill? rev-lease REV-LOW))
          (let ((ap (gr-authority-pid)))
            (if ap
                (begin (set! rev-refill-inflight REV-BLOCK)
                       (send ap (cons (self) (list (string->utf8 "REV-GRANT")
                                                   (string->utf8 (number->string REV-BLOCK))))))))))
    (define (gr-count-puts cmds)           ; PUTs each need one global rev
      (let loop ((cs cmds) (n 0))
        (if (null? cs) n
            (loop (cdr cs) (if (and (pair? (car cs)) (string=? (utf8->string (caar cs)) "PUT"))
                               (+ n 1) n)))))
    ; PUT -> PUT-AT <leased rev> for each cmd, drawing from rev-lease (caller guarantees
    ; lease covers all PUTs). Non-PUT passes through. Threads + persists the lease.
    (define (gr-rewrite-batch! cmds)
      (map (lambda (c) (let ((r (global-rev-rewrite c rev-lease)))
                         (set! rev-lease (cdr r)) (car r)))
           cmds))
    ; report this writer's low-watermark contribution to the authority (Phase 3): the
    ; lowest global rev it may still produce but has NOT applied. Conservatively
    ; current-rev+1 while it has unconsumed leased revs OR in-flight writes; else #f
    ; (caught up — does not constrain W). Sent on tick; no-op unless gr-writer?.
    (define (gr-report-progress!)
      (if gr-writer?
          (let ((ap (gr-authority-pid)))
            (if ap
                (let ((active? (or (> (lease-remaining rev-lease) 0)
                                   (> (hashtable-size pending) 0))))
                  (send ap (list 'rev-progress (string->number shard-key)
                                 (if active? (+ (mvcc-current-rev ctx) 1) #f))))))))
    ; authority-side: the current global low-watermark from the writer progress reports.
    ; W = min over writers-with-unapplied of (lowest-unapplied - 1), else granted-high.
    (define (gr-watermark-now)
      (let wmark ((ks (vector->list (hashtable-keys rev-progress))) (w (mvcc-global-rev ctx)))
        (if (null? ks) w
            (let ((lu (hashtable-ref rev-progress (car ks) #f)))
              (wmark (cdr ks) (if lu (min w (- lu 1)) w))))))
    ; ensure the lease holds >= `need` revs before a batch rewrite. Normally a no-op
    ; (refill-before-empty keeps it warm). Only when short does it request a grant and
    ; block for the reply, requeuing any other frame to `backlog` so nothing is lost.
    ; ponytail: blocks the loop only on a cold/burst miss; the warm lease makes it rare —
    ; a become-leader pre-seed would remove even that.
    (define (gr-ensure! need)
      (if (and gr-writer? (> need 0))
          (let wait ()
            (if (>= (lease-remaining rev-lease) need) #t
                (begin
                  (if (not rev-refill-inflight)
                      (let ((ap (gr-authority-pid)))
                        (if ap (begin (set! rev-refill-inflight REV-BLOCK)
                                      (send ap (cons (self) (list (string->utf8 "REV-GRANT")
                                                                  (string->utf8 (number->string (max REV-BLOCK need))))))))))
                  (let ((r (raw-receive)))
                    (if (and (pair? r) (string? (car r)) (string=? (car r) "REV-GRANT"))
                        (if rev-refill-inflight
                            (begin (set! rev-lease (lease-add rev-lease (cdr r) rev-refill-inflight))
                                   (set! rev-refill-inflight #f)))
                        (set! backlog (append backlog (list r)))))
                  (wait))))))
    (define FWD-EXPIRE-TICKS 25)              ; ~3s at the default 120ms tick
    ; ---- snapshot shipping, leader side (cw-lkq.15) ----
    ; The engine marks (snap-req) any peer that rejected AppendEntries at the
    ; compaction floor — log replay can no longer reach it. Ship the FULL store
    ; (raw rows, byte-level replica) + applied index/term over the mesh; the
    ; follower installs it and the next heartbeat's AE lands at its new base.
    ; Rate-limited per peer so a down/slow peer doesn't draw a snapshot per AER.
    (define snap-last (make-eqv-hashtable))
    (define SNAP-MIN-TICKS 50)
    ; Rows go in BOUNDED chunks (one giant frame both risks the transport and
    ; recurses message (de)serialization thousands of conses deep — a wiped
    ; rejoiner's install aborted on a literal stack overflow before chunking).
    ; The mesh is ordered per sender, so begin/rows.../end arrive in sequence.
    (define SNAP-CHUNK-ROWS 200)
    (define (snap-send! p payload)
      (guard (e (#t #f))
        (gsend (symbol->string p)
                   (list 'ws-snap shard-key node-name payload))))
    (define (ship-snaps! st)
      (let ((reqs (raft-snap-requests st)))
        (if (null? reqs) st
            (begin
              (for-each
               (lambda (p)
                 (let ((last (hashtable-ref snap-last p (- 0 SNAP-MIN-TICKS))))
                   (if (>= (- fwd-ticks last) SNAP-MIN-TICKS)
                       (begin
                         (hashtable-set! snap-last p fwd-ticks)
                         (snap-send! p (list 'begin (raft-applied st)
                                             (entry-term st (raft-applied st))))
                         (let chunk ((rows (kv-scan ctx (make-bytevector 0))))
                           (if (pair? rows)
                               (let split ((n SNAP-CHUNK-ROWS) (r rows) (acc '()))
                                 (if (or (= n 0) (null? r))
                                     (begin (snap-send! p (list 'rows (reverse acc)))
                                            (chunk r))
                                     (split (- n 1) (cdr r) (cons (car r) acc))))))
                         (snap-send! p (list 'end))))))
               reqs)
              (raft-clear-snap-req st)))))
    (define (fwd-sweep!)
      (set! fwd-ticks (+ fwd-ticks 1))
      (if (> (hashtable-size fwd-pending) 0)
          (vector-for-each
           (lambda (id)
             (let ((e (hashtable-ref fwd-pending id #f)))
               (if (and e (>= fwd-ticks (cdr e)))
                   (begin (hashtable-delete! fwd-pending id)
                          (guard (g (#t #f)) (send (car e) 'tryagain))))))
           (hashtable-keys fwd-pending))))
    ; the kv-range reply payload (sendable) served from THIS replica's ctx.
    (define (range-reply st opts)
      (let* ((key (range-opt opts 'key (make-bytevector 0 0)))
             (rend (range-opt opts 'range-end #f))
             (res (mvcc-range ctx key rend opts)))
        (if (and (pair? res) (eq? (car res) 'err-compacted))
            (list 'kv-range-ok (mvcc-current-rev ctx) (raft-term st) 'compacted 0 '())
            (list 'kv-range-ok (mvcc-current-rev ctx) (raft-term st) #f
                  (car res)
                  (map (lambda (item)
                         (let ((uk (car item)) (rec (cdr item)))
                           (list uk (kv-rec-value rec)
                                 (kv-rec-create-rev rec) (kv-rec-mod-rev rec)
                                 (kv-rec-version rec) (kv-rec-lease rec))))
                       (cdr res))))))
    ; ---- leader-region pinning (cw-lkq.2) ----
    ; A leader OUTSIDE leader-region tries, at most once per XFER-EVERY ticks,
    ; to hand leadership (TimeoutNow) to a CAUGHT-UP voter in the preferred
    ; region. raft-transfer-leadership refuses self/non-voter/not-caught-up, so
    ; a down or lagging preferred region is a harmless no-op (leadership stays
    ; where it is — availability over placement). No ping-pong: only an
    ; out-of-region leader initiates, and a preferred-region leader never does.
    (define XFER-EVERY 25)                  ; ticks between attempts (~3s at 120ms)
    (define xfer-ticks 0)
    (define (region-of n) (let ((hit (assv n region-map))) (and hit (cdr hit))))
    ; cw-e9a: leader-NODE pin takes precedence — a leader that is not the pinned
    ; node transfers straight to it (raft-transfer-leadership no-ops if the target
    ; is down / lagging / not a voter, so a missing pin keeps leadership put).
    ; When no node pin is set, fall back to the region pin (first caught-up voter
    ; in leader-region).
    (define (misplaced-leader? st)
      (and (raft-leader? st)
           (cond (leader-node   (not (eqv? node-name leader-node)))
                 (leader-region (not (equal? (region-of node-name) leader-region)))
                 (else #f))))
    (define (maybe-transfer-home! st)
      (if (misplaced-leader? st)
          (begin
            (set! xfer-ticks (+ xfer-ticks 1))
            (if (>= xfer-ticks XFER-EVERY)
                (begin
                  (set! xfer-ticks 0)
                  (if leader-node
                      ; node pin: transfer directly to the named voter
                      (let ((r (raft-transfer-leadership st leader-node)))
                        (if (eq? (car r) 'ok) (emit! (cadr r)) #f))
                      ; region pin: first caught-up voter in leader-region
                      (let try ((vs (aget st 'voters)))
                        (cond
                          ((null? vs) #f)        ; no caught-up preferred voter — stay
                          ((and (not (eqv? (car vs) node-name))
                                (equal? (region-of (car vs)) leader-region))
                           (let ((r (raft-transfer-leadership st (car vs))))
                             (if (eq? (car r) 'ok)
                                 (emit! (cadr r))
                                 (try (cdr vs)))))
                          (else (try (cdr vs)))))))))))
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
    ; Log compaction (RocksDB is the snapshot). NEVER compact while acks are
    ; deferred: `pending` is keyed by absolute log index — flush-base = #f means
    ; every applied entry's ack has been drained, so compaction is safe then.
    ;   solo:        compact fully once applied == log-len (original behavior).
    ;   multi-voter: keep the last COMPACT-KEEP applied entries for ordinary
    ;     catch-up and drop everything older (cw-lkq.15). Without this the log
    ;     grows O(total writes) and every propose's full-log copy makes the
    ;     cluster melt O(N²) under sustained load (the WAN kube-apiserver
    ;     collapse: GiBs of RSS, 200% CPU, tick starvation, election churn).
    ;     A peer that falls below the floor is caught up with a STORE snapshot
    ;     (ws-snap) instead of log replay — see ship-snaps! below.
    (define COMPACT-KEEP 1024)
    (define (compact st)
      (if solo
          (if (= (raft-applied st) (log-len st))
              (aset* st (list 'base (raft-applied st)
                              'base-term (last-log-term st)
                              'log '()))
              st)
          (raft-compact-to st (- (raft-applied st) COMPACT-KEEP))))
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

    ; ---- dynamic membership: propose a member change + settle its async ack (cw-u4a.29) ----
    ; propose-member! is LEADER-ONLY (callers gate on raft-leader? + (not conf-change-pending?)).
    ; It records the awaiting reply-pid + the new ConfChange entry's index (for drain! to skip),
    ; proposes through the SAME engine entry the pure tests use (raft-propose-conf-change), ships
    ; the resulting AppendEntries to the (now config-updated) peers via emit!, republishes, and
    ; returns st'. The config is adopted on APPEND (engine recompute-config), so st''s 'peers
    ; already includes the new member — emit! reaches it on this/the next heartbeat. The member
    ; ACK is async (settle-member!, from the AER path), so the caller hears the outcome only once
    ; the change commits on a quorum (mirrors the client-write commit->ack bridge).
    (define (propose-member! reply-pid target-voters target-learners st)
      (set! member-reply-pid reply-pid)
      (hashtable-set! conf-skip (+ 1 (log-len st)) #t)         ; the ConfChange entry's index
      (let* ((r (raft-propose-conf-change st target-voters target-learners))
             (st1 (car r)))
        (emit! (cdr r))
        (publish! st1 node-name)
        st1))
    ; Ack the in-flight member change once it has FULLY settled: conf-change-pending? is #f
    ; only after a simple/learner entry committed OR the joint's auto-Cnew committed (Ongaro
    ; §4.3 leave-joint). This also fires when a leader removed ITSELF and is stepping down on
    ; the committed Cnew — the removal succeeded, so it still acks 'member-ok. Reply the
    ; resulting config (voters + learners). A strict no-op when no change is in flight.
    (define (settle-member! st)
      (if (and member-reply-pid (not (conf-change-pending? st)))
          (begin
            (send member-reply-pid (list 'member-ok (aget st 'voters) (aget st 'learners)))
            (set! member-reply-pid #f))))

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
           (st0 (make-raft node-name voters apply-fn 0 genesis-learners))
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
      ; `backlog`: the (at most one per batch) NON-write message the group-commit
      ; drain (cw-b5w.3, client-command branch below) pulled off the mailbox while
      ; collecting a write batch. Consumed BEFORE the mailbox so its order w.r.t.
      ; later messages is preserved.
      (let loop ((st stI) (leader ldr0) (elapsed 0) (flush-base #f))
        (let ((m (cond ((pair? backlog)
                        (let ((b (car backlog))) (set! backlog (cdr backlog)) b))
                       (flush-base (raw-receive 0))
                       (else (raw-receive)))))
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
                         ; cw-lkq: grant a pre-vote when we have not heard from a
                         ; live leader for the BASE election timeout — NOT our own
                         ; per-node STAGGERED `timeout`. The stagger exists only to
                         ; pick who CAMPAIGNS first; using it as the grant threshold
                         ; deadlocks at >3 voters: a node resets `elapsed` to 0 the
                         ; instant it starts a pre-vote (tick branch), so no set of
                         ; majority voters is ever simultaneously past its staggered
                         ; timeout, and pre-vote never reaches a majority. The base
                         ; threshold is also correct Raft PreVote: a live, heart-
                         ; beating leader keeps elapsed < base on every voter (AE
                         ; resets it), so no disruptive pre-vote is ever granted.
                         (grant (and (not (raft-leader? st)) (>= elapsed election-base) up)))
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
                         (old-loglen (log-len st))          ; cw-u4a.29: detect an auto-appended Cnew
                         (r (raft-step st from rpc)) (st2 (ship-snaps! (car r))))
                    ; leader -> follower: 'indeterminate the remaining (now
                    ; uncommitted) in-flight proposals + clear pending, before any
                    ; drain! can cross-wire them (H1).
                    (if (and was-leader? (not (raft-leader? st2))) (fail-pending!))
                    ; cw-u4a.29 membership: on the joint-commit AER the engine auto-appends
                    ; the Cnew (leave-joint) entry — record its index so drain! skips it (it
                    ; carries no `acc` slot). Then settle the in-flight member-* ack: a
                    ; deposing higher term makes the outcome indeterminate; otherwise the
                    ; change is done exactly when conf-change-pending? clears (Cnew / simple
                    ; committed), which also covers a leader that removed ITSELF and is now
                    ; stepping down on that committed Cnew (it still acks 'member-ok).
                    (if (and member-reply-pid (> (log-len st2) old-loglen))
                        (hashtable-set! conf-skip (log-len st2) #t))
                    (cond
                      ((and member-reply-pid depose?)
                       (send member-reply-pid 'member-indeterminate)
                       (set! member-reply-pid #f))
                      (else (settle-member! st2)))
                    ; record the new applied-index in the batch BEFORE the fsync
                    (if (> (raft-applied st2) old) (persist-applied! st2))
                    (if (and (raft-leader? st2) (eq? (car rpc) 'aer) (> (raft-applied st2) old))
                        (rtt-on-commit!))
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
             (fwd-sweep!)                      ; expire wedged forwards (cw-lkq.13)
             (gr-report-progress!)             ; cw-kp0 Phase 3: report low-watermark to authority (no-op unless gr-writer?)
             (if (raft-leader? st) (gr-maybe-refill!))  ; cw-kp0: pre-seed/keep the lease warm on the leader so no client op blocks on a cold cross-group grant (Jepsen-timeout fix)
             ; cw-lkq.15: long-lived server actors accumulate cyclic garbage the
             ; Rc heap can't free — sweep the (thread-local) cycle registry
             ; periodically. No-op on builds without tracing-cycle-collector.
             (if (= 0 (modulo fwd-ticks 16)) (collect-garbage))
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
                        (maybe-transfer-home! (car r))
                        ; Lease expiry (cw-u4a.17, ADR 0003 §2): RIDES this same tick.
                        ; Seed/re-derive deadlines + propose ("LEASE-REVOKE" id) for any
                        ; expired lease.  propose-internal! threads the state + emits the
                        ; revoke AEs (+ solo-commits) itself; returns (st' . flush-base').
                        ; A strict no-op when no leases are granted (empty meta scan), so
                        ; the heartbeat path is unchanged for the leaseless sim-cluster.
                        (let ((lr (lease-tick! (car r) #f)))
                          (loop (maybe-compact (car lr) (cdr lr)) node-name 0 (cdr lr))))
                      (begin (fail-pending!)
                             ; cw-u4a.29: a leader that loses quorum and steps down can no
                             ; longer confirm an in-flight member change — its outcome is
                             ; indeterminate (a surviving quorum may yet commit it or not).
                             (if member-reply-pid
                                 (begin (send member-reply-pid 'member-indeterminate)
                                        (set! member-reply-pid #f)))
                             (publish! cq #f)
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
               ; cw-lkq.4: a SERIALIZABLE read (etcd r.serializable) is served from
               ; THIS replica's committed ctx with no leader gate / quorum round —
               ; possibly stale, locally consistent. Linearizable (the default)
               ; keeps the leader gate.
               (if (and (not (raft-leader? st))
                        (or (not (range-opt opts 'serializable #f))
                            ; cw-lkq.6 freshness gate: a serializable read on a
                            ; replica more than ser-max-lag entries behind the
                            ; leader's commit redirects instead of serving stale.
                            (and (> ser-max-lag 0)
                                 (> (- (raft-commit st) (raft-applied st)) ser-max-lag))))
                   ; cw-lkq.13: forward a linearizable read to the KNOWN leader
                   ; instead of erroring (etcd parity); no known leader -> the
                   ; original 'tryagain redirect.
                   (if (and leader (not (eqv? leader node-name)))
                       (begin
                         (set! fwd-seq (+ fwd-seq 1))
                         (hashtable-set! fwd-pending fwd-seq
                                         (cons conn (+ fwd-ticks FWD-EXPIRE-TICKS)))
                         (guard (e (#t (hashtable-delete! fwd-pending fwd-seq)
                                       (send conn 'tryagain)))
                           (gsend (symbol->string leader)
                                      (list 'ws-fwd shard-key node-name fwd-seq opts))))
                       (send conn 'tryagain))
                   (send conn (range-reply st opts))))
             (loop st leader elapsed flush-base))

            ;; ---- forwarded WRITE, leader side (cw-lkq.13) ----
            ;; Re-enter the client-command path with a (fwd ORIGIN . ID) stand-in
            ;; as the waiter: propose/batch/ack machinery is identical; the ack
            ;; (or 'indeterminate on stepdown) rides ws-fwd-reply home. A
            ;; non-leader bounces 'tryagain straight back.
            ((eq? (car m) 'fwd-write)
             (let ((origin (cadr m)) (id (caddr m)) (cmd (cadddr m)))
               (if (not (raft-leader? st))
                   (begin
                     (guard (e (#t #f))
                       (gsend (symbol->string origin)
                                  (list 'ws-fwd-reply shard-key node-name id 'tryagain)))
                     (loop st leader elapsed flush-base))
                   ; replay through the normal client-write path via backlog so
                   ; the group-commit drain handles it uniformly.
                   (begin
                     (set! backlog (append backlog
                                           (list (cons (cons 'fwd (cons origin id)) cmd))))
                     (loop st leader elapsed flush-base)))))

            ;; ---- forwarded linearizable read, leader side (cw-lkq.13) ----
            ;; (fwd-range ORIGIN-NODE ID OPTS): serve from OUR ctx iff we lead,
            ;; else bounce 'tryagain back; the reply rides 'ws-fwd-reply home.
            ((eq? (car m) 'fwd-range)
             (let ((origin (cadr m)) (id (caddr m)) (opts (cadddr m)))
               (let ((payload (if (raft-leader? st) (range-reply st opts) 'tryagain)))
                 (guard (e (#t #f))
                   (gsend (symbol->string origin)
                              (list 'ws-fwd-reply shard-key node-name id payload))))
               (loop st leader elapsed flush-base)))
            ;; ---- snapshot install, follower side (cw-lkq.15) ----
            ;; ('snap-install FROM ('begin BASE TERM) | ('rows CHUNK) | ('end)):
            ;; the leader compacted past our log position; adopt a byte-level
            ;; replica of its store at BASE (raw rows span every namespace incl.
            ;; MVCC history + REV-CF, so local watchers replay the jumped window
            ;; from the installed history). Chunks accumulate in snap-accum and
            ;; install on 'end. Stale/duplicate offers (base <= our applied) and
            ;; offers while WE lead are ignored; a fresh 'begin discards any
            ;; partial transfer (the leader re-ships whole).
            ((eq? (car m) 'snap-install)
             (let ((payload (caddr m)))
               (cond
                 ((eq? (car payload) 'begin)
                  (let ((sbase (cadr payload)) (sterm (caddr payload)))
                    (set! snap-accum
                          (if (or (raft-leader? st) (<= sbase (raft-applied st)))
                              #f                       ; stale offer — swallow its chunks
                              (list sbase sterm '())))
                    (loop st leader elapsed flush-base)))
                 ((eq? (car payload) 'rows)
                  (if snap-accum
                      (set-car! (cddr snap-accum)
                                (append (caddr snap-accum) (cadr payload))))
                  (loop st leader elapsed flush-base))
                 ((and (eq? (car payload) 'end) snap-accum)
                  (let ((sbase (car snap-accum)) (sterm (cadr snap-accum))
                        (rows (caddr snap-accum))
                        (pre (mvcc-current-rev ctx)))
                    (set! snap-accum #f)
                    (if flush-base (flush-and-drain! flush-base))
                    (flush-materializations!)
                    (for-each (lambda (kv) (kv-del! ctx (car kv)))
                              (kv-scan ctx (make-bytevector 0)))
                    (for-each (lambda (kv) (kv-put! ctx (car kv) (cdr kv))) rows)
                    ; EXP7: snapshot rows wrote META-CURRENT-REV directly (bypassing
                    ; mvcc-set-current-rev!), so the cached crev is stale — invalidate
                    ; so the next mvcc-current-rev re-reads the installed value.
                    (set-shard-ctx-crev! ctx -1)
                    (ctx-save-applied! ctx sbase sterm)
                    (ctx-flush! ctx)
                    (if (> (reg-count watch-reg) 0)
                        (begin
                          (watch-on-apply! watch-reg ctx pre (mvcc-current-rev ctx))
                          (watch-check-compaction! watch-reg ctx)))
                    (let ((st2 (raft-install-snapshot st sbase sterm)))
                      ; ack the installed position to the sender (an ordinary
                      ; AER success at base): its next advances past its
                      ; compaction floor and ordinary AEs resume — without this
                      ; the leader would keep filtering us out of broadcasts
                      ; (next <= base) and re-shipping snapshots forever.
                      (emit! (list (cons (cadr m)
                                         (list 'aer (raft-term st2) #t sbase 0))))
                      (publish! st2 (cadr m))
                      (loop st2 (cadr m) 0 #f))))
                 (else (loop st leader elapsed flush-base)))))

            ;; ---- forwarded read reply, origin side (cw-lkq.13) ----
            ((eq? (car m) 'fwd-reply)
             (let* ((id (caddr m)) (payload (cadddr m))
                    (e (hashtable-ref fwd-pending id #f)))
               (if e (begin (hashtable-delete! fwd-pending id)
                            (guard (g (#t #f)) (send (car e) payload))))
               (loop st leader elapsed flush-base)))

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

            ;; cw-kp0 global-rev: refill reply from the rev-authority. The grant rides the
            ;; standard client-proposal ack path, so it arrives as ("REV-GRANT" . lo);
            ;; enqueue the granted block [lo, lo+N) into the writer lease (N = the size of
            ;; the outstanding request). Inert unless global-rev?; the propose-branch hook
            ;; that requests refills + draws revs from rev-lease is the next increment.
            ((and global-rev? (pair? m) (string? (car m)) (string=? (car m) "REV-GRANT"))
             (if rev-refill-inflight
                 (begin (set! rev-lease (lease-add rev-lease (cdr m) rev-refill-inflight))
                        (set! rev-refill-inflight #f)))
             (loop st leader elapsed flush-base))

            ;; cw-kp0 Phase 3: the rev-authority's current granted global-rev high
            ;; (META-GLOBAL-REV). A building block for the global header.revision /
            ;; low-watermark coordinator. Harmless leader-local read (0 in default mode);
            ;; any shard may query the authority group.
            ((eq? (car m) 'global-high)
             (send (cadr m) (list 'global-high-ok (mvcc-global-rev ctx)))
             (loop st leader elapsed flush-base))

            ;; cw-kp0 Phase 3: a writer reports the lowest global rev it has NOT yet
            ;; applied (#f = caught up). The authority records it for the low-watermark.
            ;; (rev-progress writer-shard-num lowest-unapplied|#f)
            ((eq? (car m) 'rev-progress)
             (if rev-authority? (hashtable-set! rev-progress (cadr m) (caddr m)))
             (loop st leader elapsed flush-base))

            ;; cw-kp0 Phase 3: the global low-watermark — the highest rev safe to surface
            ;; as header.revision (everything <= W is applied on its owning group). W =
            ;; min over writers-with-unapplied of (lowest-unapplied - 1), else granted-high.
            ((eq? (car m) 'global-watermark)
             (send (cadr m) (list 'global-watermark-ok (gr-watermark-now)))
             (loop st leader elapsed flush-base))

            ;; cw-kp0 Phase 4: global compaction admissibility. A compaction below R is
            ;; safe only once R <= the low-watermark — never discard history a pending
            ;; op could still reference (ADR 0006). The coordinator asks here before
            ;; broadcasting "compact < R" to the groups. (compact-admissible R reply-pid)
            ((eq? (car m) 'compact-admissible)
             (send (caddr m) (list 'compact-admissible-ok (<= (cadr m) (gr-watermark-now))))
             (loop st leader elapsed flush-base))

            ;; ---- Auth read seams (cw-u4a.26) — pure reads over NS-AUTH on THIS
            ;; node's committed ctx (auth.scm).  The gRPC handler (grpc-kv) owns the
            ;; leader-local token table + identity resolution; it asks here for the
            ;; replicated auth STATE (the ctx lives in this actor).  Single-node = always
            ;; leader, so served inline like cur-rev (no leader gate needed for v1).
            ;;   (auth-state CONN)                         -> (auth-state-ok enabled? auth-rev)
            ;;   (auth-authorize CONN USER KEY REND REQ)   -> (auth-authorize-ok decision)
            ;;   (auth-lookup CONN NAME)                   -> (auth-lookup-ok exists? hash admin? auth-rev)
            ;; USER/NAME/KEY are bytevectors (or #f for no-identity USER); REQ is 'read|'write.
            ((eq? (car m) 'auth-state)
             (send (cadr m) (list 'auth-state-ok (auth-enabled? ctx) (auth-rev ctx)))
             (loop st leader elapsed flush-base))
            ((eq? (car m) 'auth-authorize)
             (let ((conn (cadr m)) (user (caddr m)) (key (cadddr m))
                   (rend (list-ref m 4)) (req (list-ref m 5)))
               (send conn (list 'auth-authorize-ok
                                (auth-authorize? ctx user key rend req))))
             (loop st leader elapsed flush-base))
            ((eq? (car m) 'auth-lookup)
             (let* ((conn (cadr m)) (name (caddr m))
                    (u (auth-get-user ctx name)))
               (send conn
                     (if u
                         (list 'auth-lookup-ok #t (auth-user-hash u)
                               (auth-has-root-role? (auth-user-roles u)) (auth-rev ctx))
                         (list 'auth-lookup-ok #f #f #f (auth-rev ctx)))))
             (loop st leader elapsed flush-base))

            ;; ---- Auth read seams (cw-u4a.27) — UserGet/UserList/RoleGet/RoleList.
            ;; Pure reads over NS-AUTH; admin-gated by the gRPC handler upstream.
            ;;   (auth-user-info CONN name)   -> (auth-user-info-ok #t roles) | (auth-user-info-ok #f #f)
            ;;   (auth-user-list CONN)        -> (auth-user-list-ok names)
            ;;   (auth-role-info CONN name)   -> (auth-role-info-ok #t perms) | (auth-role-info-ok #f #f)
            ;;   (auth-role-list CONN)        -> (auth-role-list-ok names)
            ((eq? (car m) 'auth-user-info)
             (let* ((conn (cadr m)) (name (caddr m))
                    (u (auth-get-user ctx name)))
               (send conn
                     (if u
                         (list 'auth-user-info-ok #t (auth-user-roles u))
                         (list 'auth-user-info-ok #f #f))))
             (loop st leader elapsed flush-base))
            ((eq? (car m) 'auth-user-list)
             (send (cadr m) (list 'auth-user-list-ok (auth-all-users ctx)))
             (loop st leader elapsed flush-base))
            ((eq? (car m) 'auth-role-info)
             (let* ((conn (cadr m)) (name (caddr m))
                    (perms (auth-get-role ctx name)))
               (send conn
                     (if perms
                         ; Convert #(ptype key rend) vectors to lists so they survive
                         ; the spawn-source cross-runtime boundary (vectors may not be
                         ; sendable; lists of bytevectors/ints always are).
                         (list 'auth-role-info-ok #t
                               (map (lambda (p)
                                      (list (vector-ref p 0) (vector-ref p 1) (vector-ref p 2)))
                                    perms))
                         (list 'auth-role-info-ok #f #f))))
             (loop st leader elapsed flush-base))
            ((eq? (car m) 'auth-role-list)
             (send (cadr m) (list 'auth-role-list-ok (auth-all-roles ctx)))
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
            ;; The per-conn streaming actor (.14) asks THIS member's shard registry to
            ;; establish a watcher.  Served on EVERY replica (cw-lkq.5, etcd-faithful:
            ;; any member serves watches): followers apply the same committed entries,
            ;; so watch-on-apply! fires here too and REV-CF replay is identical — a
            ;; follower watch delivers in THIS replica's revision order, exactly
            ;; once, possibly later than the leader's wall clock (like etcd).  The
            ;; original leader gate (pre-cw-lkq.5) forced a WAN hop per watch from
            ;; remote regions.
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
               (if #f                       ; cw-lkq.5: served on every replica
                   (send reply-pid (cons 'watch-not-leader leader))
                   (let* ((deliver-fn
                           (lambda (wr)
                             ; A registry's delivery must NEVER crash the registry:
                             ; the per-conn consumer (the gRPC stream worker) may have
                             ; exited, and `send` to a dead actor raises.  Guard it.
                             (guard (e (#t #f))
                               (send reply-pid (list 'watch-response (watch-response->sexp wr))))))
                          (res (watch-register! watch-reg ctx spec deliver-fn)))
                     (if (and (pair? res) (eq? (car res) 'compacted))
                         (send reply-pid (cons 'watch-compacted (cdr res)))
                         (send reply-pid (cons 'watch-created res)))))   ; res = watch-id
               (loop st leader elapsed flush-base)))

            ;; ---- Watch cancel: (watch-cancel REPLY-PID WATCH-ID) ----  (cw-u4a.14)
            ;; Deregister the watcher + (via its deliver-fn) emit a canceled
            ;; WatchResponse to the stream, then ack the cancel to REPLY-PID.
            ;; Replica-local like register (cw-lkq.5).  watch-cancel!
            ;; runs on this single thread so a cancel concurrent with an in-flight
            ;; dispatch is serialized (no use-after-cancel, ADR 0002 §6).
            ((eq? (car m) 'watch-cancel)
             (let ((reply-pid (cadr m)) (wid (caddr m)))
               (if #f                       ; cw-lkq.5: served on every replica
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

            ;; ---- Membership: add / remove / promote / list (cw-u4a.29) ----
            ;; Dynamic Raft membership bound to the live actor/transport layer. The three
            ;; mutating ops are LEADER-GATED exactly like watch-register / lease-grant — a
            ;; non-leader REDIRECTS, ('member-not-leader . LEADER), so the .30 Cluster gRPC /
            ;; a test re-targets the leader — and serialized: a second change while one is in
            ;; flight is refused ('member-pending), matching the engine's one-at-a-time rule
            ;; (conf-change-pending?). Each proposes through raft-propose-conf-change (the
            ;; SAME entry the pure raft-membership tests drive) and is acked ASYNCHRONOUSLY,
            ;; (list 'member-ok voters learners), only once the change COMMITS (settle-member!,
            ;; from the AER path) — the caller learns the outcome only after the new config is
            ;; durable on a quorum. The config is adopted on APPEND, so st's 'peers (hence
            ;; emit!) already targets the new member; in the sim the test node-link!s it first,
            ;; over real TCP the joiner node-connects (see node-cluster.scm --join).
            ;;   (member-add     REPLY-PID NODE-NAME AS-LEARNER?)  add a voter, or a learner
            ;;   (member-remove  REPLY-PID NODE-NAME)              drop a voter/learner
            ;;   (member-promote REPLY-PID NODE-NAME)              learner -> voter (two-phase)
            ;;   (member-list    REPLY-PID)                        read THIS node's config view
            ((eq? (car m) 'member-add)
             (let ((reply-pid (cadr m)) (nn (caddr m)) (as-learner? (cadddr m)))
               (cond
                 ((not (raft-leader? st))
                  (send reply-pid (cons 'member-not-leader leader))
                  (loop st leader elapsed flush-base))
                 ((conf-change-pending? st)
                  (send reply-pid 'member-pending)
                  (loop st leader elapsed flush-base))
                 (else
                  ; voter add changes the voter set (two-phase joint); learner add leaves the
                  ; voter set unchanged (single-phase simple) — the engine picks which.
                  (let ((st1 (propose-member! reply-pid
                              (if as-learner? (aget st 'voters) (add-mem nn (aget st 'voters)))
                              (if as-learner? (add-mem nn (aget st 'learners)) (aget st 'learners))
                              st)))
                    (loop st1 node-name elapsed flush-base))))))
            ((eq? (car m) 'member-remove)
             (let ((reply-pid (cadr m)) (nn (caddr m)))
               (cond
                 ((not (raft-leader? st))
                  (send reply-pid (cons 'member-not-leader leader))
                  (loop st leader elapsed flush-base))
                 ((conf-change-pending? st)
                  (send reply-pid 'member-pending)
                  (loop st leader elapsed flush-base))
                 (else
                  ; drop nn from both sets; if nn is the leader (self), the engine steps it
                  ; down only AFTER the Cnew commits (Ongaro §4.3) — settle-member! still acks.
                  (let ((st1 (propose-member! reply-pid
                              (others nn (aget st 'voters))
                              (others nn (aget st 'learners))
                              st)))
                    (loop st1 node-name elapsed flush-base))))))
            ((eq? (car m) 'member-promote)
             (let ((reply-pid (cadr m)) (nn (caddr m)))
               (cond
                 ((not (raft-leader? st))
                  (send reply-pid (cons 'member-not-leader leader))
                  (loop st leader elapsed flush-base))
                 ((conf-change-pending? st)
                  (send reply-pid 'member-pending)
                  (loop st leader elapsed flush-base))
                 (else
                  ; move nn from learners into voters — a voter-set change, so two-phase.
                  (let ((st1 (propose-member! reply-pid
                              (add-mem nn (aget st 'voters))
                              (others nn (aget st 'learners))
                              st)))
                    (loop st1 node-name elapsed flush-base))))))
            ;; member-list is a pure read of THIS node's replicated config (adopted on append),
            ;; so it is NOT leader-gated — a test can query every node to assert convergence.
            ((eq? (car m) 'member-list)
             (send (cadr m) (list 'member-list (aget st 'voters) (aget st 'learners)))
             (loop st leader elapsed flush-base))

            ;; ---- MoveLeader: leadership transfer (cw-u4a.42, etcd Maintenance/MoveLeader) ----
            ;;   (move-leader REPLY-PID TARGET-NAME)  transfer leadership to TARGET-NAME (symbol)
            ;; Leader-gated.  raft-transfer-leadership refuses (self/not-voter/not-caught-up)
            ;; WITHOUT sending; on 'ok we emit! the 'timeout-now and TARGET campaigns + wins,
            ;; deposing us through the normal higher-term election machinery (no special
            ;; stepdown here).  Replies: 'move-leader-ok | (move-leader-not-leader . LEADER) |
            ;; (move-leader-err . REASON).
            ((eq? (car m) 'move-leader)
             (let ((reply-pid (cadr m)) (target (caddr m)))
               (if (not (raft-leader? st))
                   (begin (send reply-pid (cons 'move-leader-not-leader leader))
                          (loop st leader elapsed flush-base))
                   (let ((r (raft-transfer-leadership st target)))
                     (cond
                       ((eq? (car r) 'ok)
                        (emit! (cadr r))                 ; 'timeout-now -> TARGET
                        (send reply-pid 'move-leader-ok)
                        (loop st leader elapsed flush-base))
                       (else
                        (send reply-pid (cons 'move-leader-err (cadr r)))
                        (loop st leader elapsed flush-base)))))))

            ;; ---- Maintenance read/flush seams (cw-u4a.32) ----
            ;; Status / Hash / HashKV / Snapshot are READS; Defragment is an advisory flush.
            ;; ALL un-gated (NOT leader-gated), exactly like member-list: etcdctl `endpoint
            ;; status` / `endpoint hashkv` query each endpoint DIRECTLY (Maintenance is
            ;; endpoint-local), and a cross-member hash comparison REQUIRES every replica to
            ;; answer from its OWN committed ctx — identical hashes across members prove the
            ;; replicas hold the identical committed keyspace (the consistency proof).  These
            ;; read THIS node's ctx via the pure mvcc-* helpers and NEVER propose a Raft entry,
            ;; so they touch neither the write/consensus path nor `pending`.  Replies are fully
            ;; SENDABLE (ints / a leader-name symbol / lists of bytevectors).
            ;;   (status   reply-pid)      -> (status-ok rev term commit applied db-size leader key-count)
            ;;   (hashkv   reply-pid rev)  -> (hashkv-ok hash compact-rev hash-rev)  (rev 0 = current)
            ;;   (snapshot reply-pid)      -> (snapshot-ok rev ((key . value) ...))
            ;;   (defrag   reply-pid)      -> (defrag-ok)
            ((eq? (car m) 'status)
             (let ((conn (cadr m))
                   (dig  (mvcc-digest-at ctx 0)))         ; (hash size count) at current rev
               ;; cw-u4a.33: append the live key COUNT (caddr dig) as a trailing field so the
               ;; /metrics endpoint (etcd_debugging_mvcc_keys_total) + the gRPC Health readiness
               ;; check read it from the SAME digest this seam already computes for db-size — zero
               ;; extra shard cost.  APPENDED, so handle-status (reads indices 1–6) is unaffected.
               (send conn (list 'status-ok (mvcc-current-rev ctx) (raft-term st)
                                (raft-commit st) (raft-applied st) (cadr dig) leader (caddr dig))))
             (loop st leader elapsed flush-base))
            ((eq? (car m) 'hashkv)
             (let* ((conn (cadr m)) (req-rev (caddr m))
                    (at   (if (= req-rev 0) (mvcc-current-rev ctx) req-rev))
                    (dig  (mvcc-digest-at ctx at)))
               (send conn (list 'hashkv-ok (car dig) (mvcc-compact-rev ctx) at)))
             (loop st leader elapsed flush-base))
            ;; A CONSISTENT point-in-time logical snapshot: because the shard is
            ;; single-threaded, this read sees no interleaved write — the kvs are the live
            ;; keyspace at one revision.  (Materialises the whole keyspace into one reply —
            ;; fine for v1; a chunked store-iter stream is a future refinement.)
            ((eq? (car m) 'snapshot)
             (let ((conn (cadr m)))
               (send conn (list 'snapshot-ok (mvcc-current-rev ctx)
                                (mvcc-snapshot-kvs ctx 0))))
             (loop st leader elapsed flush-base))
            ;; Defragment: advisory.  RocksDB has no bbolt-style page defragmentation, so the
            ;; closest honest analogue is flushing memtables to SSTs + fsyncing the WAL.
            ((eq? (car m) 'defrag)
             (let ((conn (cadr m)))
               (guard (e (#t #f)) (store-flush (shard-ctx-handle ctx)))
               (ctx-flush! ctx)
               (send conn (list 'defrag-ok)))
             (loop st leader elapsed flush-base))
            ;; Alarm list (cw-u4a.42): un-gated read of the REPLICATED NS-ALARM set, so any
            ;; member answers and `etcdctl alarm list` agrees cluster-wide. ACTIVATE/DEACTIVATE
            ;; are ALARM-SET/ALARM-DISARM Raft writes (the normal commit->apply path), not here.
            ;;   (alarm-list reply-pid) -> (alarm-list-ok ((memberID . alarmType) ...))
            ((eq? (car m) 'alarm-list)
             (send (cadr m) (list 'alarm-list-ok (mvcc-alarm-list ctx)))
             (loop st leader elapsed flush-base))

            ;; ---- linearizable read probe: (read CONN) ----
            ;; Solo serves inline (a round would never complete); multi-voter
            ;; ReadIndex: enqueue + (if idle) open a confirmation round; serve-batch!
            ;; replies once a quorum AER confirms. A non-leader tryagains.
            ((eq? (car m) 'read)
             (let ((conn (cadr m)))
               (cond
                 ((not (raft-leader? st))
                  ; cw-lkq.13: forward the WRITE to the known leader (etcd
                  ; serves writes via any member); unknown leader -> redirect.
                  ; A FWD STAND-IN landing here (leadership flipped between
                  ; backlog push and processing) bounces 'tryagain to origin.
                  (cond
                    ; EXP5 async write on a non-leader: bounce tryagain via the
                    ; async sink (client retries to the leader); no fwd relay.
                    ((and (pair? conn) (eq? (car conn) 'async))
                     (ack-waiter! conn 'tryagain))
                    ((and (pair? conn) (eq? (car conn) 'fwd))
                     (ack-waiter! conn 'tryagain))
                    ((and leader (not (eqv? leader node-name)))
                     (set! fwd-seq (+ fwd-seq 1))
                     (hashtable-set! fwd-pending fwd-seq
                                     (cons conn (+ fwd-ticks FWD-EXPIRE-TICKS)))
                     (guard (e (#t (hashtable-delete! fwd-pending fwd-seq)
                                   (send conn 'tryagain)))
                       (gsend (symbol->string leader)
                                  (list 'ws-fwd-write shard-key node-name fwd-seq cmd))))
                    (else (send conn 'tryagain)))
                  (loop st leader elapsed flush-base))
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
            ;;
            ;; GROUP COMMIT (cw-b5w.3): after this write, greedily DRAIN any further
            ;; client writes already queued in the mailbox (non-blocking) and propose
            ;; them too, then do ONE emit!/commit/settle round for the whole batch.
            ;; Safe without touching raft.scm: broadcast-append sends each peer its
            ;; full uncommitted tail from next[p], so the LAST propose's AEs carry
            ;; every batched entry.  Leadership cannot change mid-drain (no RPC is
            ;; processed inside the drain).  The drain stops at the first NON-write
            ;; message, which is stashed in `backlog` and processed next iteration
            ;; (before the mailbox), preserving its order w.r.t. later messages.
            ;; PROPOSE-BATCH-CAP bounds added latency under a flooded mailbox.
            (else
             (let* ((conn (car m)) (cmd (cdr m)))
               (cond
                 ((not (raft-leader? st))
                  ; cw-lkq.13: forward the WRITE to the known leader (etcd
                  ; serves writes via any member); unknown leader -> redirect.
                  ; A FWD STAND-IN landing here (leadership flipped between
                  ; backlog push and processing) bounces 'tryagain to origin.
                  (cond
                    ; A FWD STAND-IN on a non-leader (leadership flipped) → bounce,
                    ; never re-forward (no relay loops).
                    ((and (pair? conn) (eq? (car conn) 'fwd))
                     (ack-waiter! conn 'tryagain))
                    ; cw-ivt: FORWARD to the shard's known leader — for ASYNC conns too
                    ; (was: async bounced tryagain, so a write whose group is led on
                    ; another node never progressed). The fwd-reply acks the original
                    ; conn via ack-waiter! (async → put-done).
                    ((and leader (not (eqv? leader node-name)))
                     (set! fwd-seq (+ fwd-seq 1))
                     (hashtable-set! fwd-pending fwd-seq
                                     (cons conn (+ fwd-ticks FWD-EXPIRE-TICKS)))
                     (guard (e (#t (hashtable-delete! fwd-pending fwd-seq)
                                   (ack-waiter! conn 'tryagain)))
                       (gsend (symbol->string leader)
                                  (list 'ws-fwd-write shard-key node-name fwd-seq cmd))))
                    (else (ack-waiter! conn 'tryagain)))    ; no known leader → bounce (async-safe)
                  (loop st leader elapsed flush-base))
                 (else
                  ; EXP19 (cw-t0n): COLLECT the queued client writes first (no per-entry
                  ; propose), then append the whole batch in ONE raft-propose-batch +
                  ; ONE broadcast. The old drain proposed per entry — O(loglen) append
                  ; AND a discarded broadcast-append EACH — i.e. O(batch*loglen). Each
                  ; item is (conn . cmd); entry i (1-based) lands at index base-idx+i.
                  (let ((old (raft-applied st)))
                    (let collect ((acc (list (cons conn cmd))) (n 1))
                      (let ((nxt (if (< n PROPOSE-BATCH-CAP) (raw-receive 0) '*timeout*)))
                        (cond
                          ;; another queued client write -> add to the batch
                          ((and (pair? nxt) (not (symbol? (car nxt))))
                           (collect (cons nxt acc) (+ n 1)))
                          ;; mailbox empty / cap hit / non-write -> close + propose the batch
                          (else
                           (if (not (eq? nxt '*timeout*))
                               (set! backlog (append backlog (list nxt))))
                           (let ((batch (reverse acc)) (base-idx (log-len st)))
                             ; cw-kp0: in a global-rev writer group, secure enough leased
                             ; global revisions for this batch's PUTs, then rewrite each
                             ; PUT -> PUT-AT <rev> so all replicas apply the same rev. A
                             ; no-op (byte-identical to the default path) unless gr-writer?.
                             (gr-ensure! (if gr-writer? (gr-count-puts (map cdr batch)) 0))
                             ; register each waiter at its prospective log index
                             (let reg ((bs batch) (i 1))
                               (when (pair? bs)
                                 (hashtable-set! pending (+ base-idx i) (caar bs))
                                 (reg (cdr bs) (+ i 1))))
                             (prof-tick! n)
                             (let* ((r (raft-propose-batch
                                         st (if gr-writer? (gr-rewrite-batch! (map cdr batch)) (map cdr batch))))
                                    (st1 (car r)) (outs1 (cdr r)))
                               (emit! outs1)              ; ONE AE round for the whole batch
                               (rtt-mark-emit!)
                               (gr-maybe-refill!)         ; cw-kp0: keep the lease warm
                               (let ((st2 (maybe-commit st1))) ; solo commits now; cluster waits for AERs
                                 (if (> (raft-applied st2) old) (persist-applied! st2))
                                 ; this branch only runs on the leader -> #t
                                 (let ((nb (settle! #t old flush-base)))  ; defer ack (durable) or ack now
                                   (loop (maybe-compact st2 nb) node-name elapsed nb))))))))))))))))))))
