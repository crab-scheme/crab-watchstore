; server/quepaxa-shard.scm — the QuePaxa shard-replica actor body (Q6, cw-gl8).
;
; A drop-in alternative to shard-actor.scm's raft driver for a shard group,
; selected per group by node-cluster's --engine quepaxa. SAME mailbox protocol
; where the concept exists; the differences are what QuePaxa buys us:
;   * NO leader gate on writes/reads: ANY replica proposes (hedged to the
;     coordinator, self-proposing if the hedge fires) — the fwd-write/fwd-range
;     relay machinery does not exist here.
;   * NO elections/PreVote/CheckQuorum/timeout-now: the tick only drives
;     hedge countdowns, retransmission, gap fill, lease expiry.
;   * Linearizable reads = a no-op slot through consensus (qp-read), not
;     ReadIndex.
;   * The write ack bridge is BID-keyed, not log-index-keyed: pending maps
;     bid -> waiter conns; (qp-take-applied) aligns applied batches to `acc`.
;   * publish! reports the COORDINATOR as "leader" (metrics/back-compat only).
; Snapshot catch-up: the LAGGING node learns via 'snapo (qp-snap-need) and
; sends (snap-pull) to the up-to-date peer, which ships the ws-snap
; begin/rows/end chunks (same frames as the raft driver's leader push).
;
; Deliberately NOT supported on quepaxa groups (raft-only for now):
;   dynamic membership (Q11/cw-2w6), MoveLeader (static coordinator; Q10),
;   parallel apply workers.
; cw-6cq: --global-rev IS supported. Unlike raft (one leader draws the lease),
; EVERY replica of a writer group keeps its own rev-lease and rewrites PUT ->
; PUT-AT at the ORIGIN inside propose-client!, before consensus — so a hedge
; retry re-proposes the already-rewritten value (cw-rz9 dedup applies it once,
; never a second draw). See src/rev-allocator.scm / rev-lease-consumer.scm
; (pure, engine-agnostic, reused unchanged) and shard-actor.scm's gr-* helpers
; (same names here, ported almost verbatim; the only behavioral difference is
; WHERE the rewrite happens and that rev-progress keys on group+node, since
; more than one replica per group can hold an active lease).
; Lease grant/keepalive/ttl stay COORDINATOR-gated (one deadline owner), same
; redirect shape ('lease-not-leader . coord) the gRPC layer already handles.
; ponytail: durable mode fsyncs per transition batch (no cross-transition
; group-commit deferral yet); add the flush-base machinery if w/s needs it.

(include "src/safe-send.scm")  ; cw-2au: send-to-dead-pid is a no-op
(include "src/encoding.scm")
(include "src/store-ctx.scm")
(include "src/mvcc.scm")
(include "src/auth.scm")
(include "src/watch.scm")
(include "src/quepaxa.scm")
(include "src/rev-lease-consumer.scm")  ; cw-6cq: writer-side revision lease + PUT->PUT-AT rewrite (global-rev mode)

(define (qp-index-of x lst)
  (let loop ((i 0) (l lst))
    (cond ((null? l) 0) ((eqv? (car l) x) i) (else (loop (+ i 1) (cdr l))))))

; args mirror shard-main positionally; slot 2 of `rest` (raft's election-ticks)
; is reused as the HEDGE tick count; slot 7 (leader-node) pins the COORDINATOR.
(define (qp-shard-main shard-key voters node-name db-path sync? . rest)
  (let* ((hedge-ticks (if (and (pair? rest) (pair? (cdr rest))
                               (number? (cadr rest)) (> (cadr rest) 0))
                          (cadr rest) 3))
         (leader-node (if (and (>= (length rest) 7) (symbol? (list-ref rest 6)))
                          (list-ref rest 6) #f))
         ; cw-6cq: same positional slot as shard-actor.scm's global-rev? (rest[7]).
         (global-rev? (if (>= (length rest) 8) (and (list-ref rest 7) #t) #f))
         (rev-authority? (and global-rev? (string=? shard-key "0")))
         (gr-writer? (and global-rev? (not rev-authority?)))
         ; coordinator: the pinned node if it's a member, else rotated by shard
         ; index so groups spread their fast-path load (mirrors raft's stagger).
         (coord (if (and leader-node (memv leader-node voters))
                    leader-node
                    (list-ref voters (modulo (let ((n (string->number shard-key)))
                                               (if n n 0))
                                             (length voters)))))
         (my-group (let ((n (string->number shard-key))) (if n n 0)))
         (my-channel (vector-ref '#(1 3 4 5) (modulo my-group 4)))
         (handle (store-open db-path #t))
         (ctx (make-ctx handle "default" sync?))
         ; cw-65x: this is the sole applier ctx — enable the latest-version
         ; cache + the H6 write buffer (one store-put-many WriteBatch per
         ; command; needs a post-July-12 binary, deployed since cw-c8b).
         (ctx (begin (mvcc-enable-latest-cache! ctx) (kv-wbuf-enable! ctx) ctx))
         ; cw-m9c (G1): same dedicated-thread Range/LIST reader pool as the raft
         ; driver (shard-actor.scm) — big scans must not hold THIS mailbox
         ; either, or writes/consensus ticks stall behind them for the full
         ; scan+encode. Workers open their own ctx over the SAME shared handle.
         (range-workers (let spawn-r ((i 0) (acc '()))
                          (if (= i 2) (list->vector (reverse acc))
                              (spawn-r (+ i 1)
                                       (cons (spawn-source-dedicated
                                              "(include \"src/server/range-worker.scm\")"
                                              'range-worker-main handle "default" sync?)
                                             acc)))))
         ; cw-04k: same off-thread watch-fanout worker as the raft driver — see
         ; watch-fanout.scm and shard-actor.scm for the ordering/freshness/backpressure
         ; argument (identical here: this actor is the worker's sole sender, so FIFO
         ; preserves the registry's single-thread ordering guarantees off this mailbox).
         (watch-fanout (spawn-source-dedicated
                        "(include \"src/server/watch-fanout.scm\")"
                        'watch-fanout-main handle "default" sync?))
         (lease-deadlines (make-eqv-hashtable))
         (lease-revoking (make-eqv-hashtable)))

    ; ---- cw-6cq global-rev: writer-side revision lease, ported from
    ; shard-actor.scm (see rev-lease-consumer.scm/rev-allocator.scm for the
    ; pure logic). Unlike raft, EVERY replica of a writer group carries its
    ; own lease (no single leader), so rev-progress is keyed per (group.node)
    ; instead of per group.
    (define REV-BLOCK 256)
    (define REV-LOW   32)
    ; cw-ecu follow-up: an outstanding REV-GRANT request has no deadline — if the
    ; reply is lost (peer restart) or simply delayed behind a saturated authority
    ; mailbox under real concurrent load, rev-refill-inflight stays set FOREVER
    ; (gr-maybe-refill! refuses to re-request while it's set) and every write on
    ; this writer group bounces 'tryagain permanently, not transiently. Observed
    ; live: a 34s wedge under a 64-conn benchmark. REV-REFILL-STALE-TICKS bounds
    ; the wedge to a few seconds; a duplicate/late reply for an abandoned request
    ; is a no-op window (the flag it would clear no longer matches) and any
    ; revisions it granted are simply never drawn — a harmless gap, same as the
    ; snapshot-install lease reset already tolerates (rev-allocator.scm's
    ; watermark design does not assume every granted rev is eventually applied).
    (define REV-REFILL-STALE-TICKS 8)      ; ~2s at the default 250ms tick
    (define rev-lease (lease-new))
    (define rev-refill-inflight #f)
    (define rev-refill-since #f)           ; tick the outstanding request was sent
    (define rev-progress (make-eqv-hashtable))   ; authority-only: writer-key -> lowest-unapplied|#f
    (define (gr-writer-key) (string->symbol (string-append shard-key ":" (symbol->string node-name))))
    (define (gr-authority-pid)
      (and gr-writer? (table-lookup 'ws-shard-pid (string-append (symbol->string node-name) ":0"))))
    (define (gr-maybe-refill!)
      (if (and gr-writer? (lease-needs-refill? rev-lease REV-LOW)
               (or (not rev-refill-inflight)
                   (>= (- ticks rev-refill-since) REV-REFILL-STALE-TICKS)))
          (let ((ap (gr-authority-pid)))
            (if ap
                (begin (set! rev-refill-inflight REV-BLOCK)
                       (set! rev-refill-since ticks)
                       (send ap (cons (self) (list (string->utf8 "REV-GRANT")
                                                   (string->utf8 (number->string REV-BLOCK))))))))))
    (define (gr-count-puts cmds)
      (let loop ((cs cmds) (n 0))
        (if (null? cs) n
            (loop (cdr cs) (if (and (pair? (car cs)) (string=? (utf8->string (caar cs)) "PUT"))
                               (+ n 1) n)))))
    (define (gr-rewrite-batch! cmds)
      (map (lambda (c) (let ((r (global-rev-rewrite c rev-lease)))
                         (set! rev-lease (cdr r)) (car r)))
           cmds))
    (define (auth-rewrite-batch cmds)
      (map (lambda (c)
             (if (and (pair? c) (string=? (utf8->string (car c)) "PUT"))
                 (cons (string->utf8 "PUT-GLOBAL") (cdr c))
                 c))
           cmds))
    (define (gr-report-progress!)
      (if gr-writer?
          (let ((ap (gr-authority-pid)))
            (if ap
                (let ((active? (or (> (lease-remaining rev-lease) 0) (pair? pending-bids))))
                  (send ap (list 'rev-progress (gr-writer-key)
                                 (if active? (+ (mvcc-current-rev ctx) 1) #f))))))))
    (define (gr-watermark-now)
      (let wmark ((ks (vector->list (hashtable-keys rev-progress))) (w (mvcc-global-rev ctx)))
        (if (null? ks) w
            (let ((lu (hashtable-ref rev-progress (car ks) #f)))
              (wmark (cdr ks) (if lu (min w (- lu 1)) w))))))

    (define acc '())                         ; apply results, newest-first
    (define pending-bids '())                ; ((bid . (conn ...)) ...)
    (define snap-accum #f)                   ; inbound snapshot (BASE ROWS)
    (define snap-last (make-eqv-hashtable))  ; outbound rate limit, per peer
    (define snap-pull-last -1000)            ; inbound (our own pull) rate limit
    (define ticks 0)
    (define SNAP-MIN-TICKS 50)
    (define SNAP-CHUNK-ROWS 200)
    (define PROPOSE-BATCH-CAP 64)
    ; cw-65x H8: propose gating — while >= GATE local batches are in flight,
    ; newly arriving client cmds accumulate in `stash` instead of opening
    ; ever more tiny slots (measured: median batch n=2 at 256 clients, every
    ; slot paying full consensus ceremony). post! flushes the stash as ONE
    ; batch once in-flight drops below the gate. Pipelining is preserved
    ; (GATE > 1); batch size becomes arrival-rate x decide-latency.
    (define PROPOSE-GATE
      (let ((e (get-environment-variable "CWS_PROPOSE_GATE")))
        (if e (or (string->number e) 16) 16)))
    (define stash '())                       ; reversed ((conn . cmd) ...)
    (define stash-n 0)
    (define (inflight) (length pending-bids))
    (define (flush-stash! st)
      (if (and (pair? stash) (< (inflight) PROPOSE-GATE))
          (let* ((items (reverse stash)))
            (set! stash '()) (set! stash-n 0)
            (propose-client! st items))
          st))

    ; CWS_PROF=1: per-write hop profiling (cw-xq9). Logs one line per acked batch:
    ;   PROF <shard> n=<batch> wait=<submit->propose ms> cons=<propose->ack ms> flush=<last fsync ms>
    ; wait covers grpc-worker send -> shard dequeue+batch; cons covers the full
    ; consensus round (mesh hops + peer processing + apply + fsync-before-ack).
    (define prof? (equal? (get-environment-variable "CWS_PROF") "1"))
    (define prof-pending '())                ; ((bid tpropose tsubmit n) ...)
    (define prof-flush-ms 0)
    (define (prof-ms a b) (exact (round (* 1000 (- a b)))))
    ; cw-vku diagnosis: always-on slow-path spans (cheap when fast). Any shard
    ; action (engine step / GC / flush) that holds THIS mailbox >100ms logs one
    ; line WITH a wall-clock timestamp so stalls correlate across nodes.
    (define SLOW-MS
      (let ((e (get-environment-variable "CWS_SLOW_MS")))
        (if e (or (string->number e) 100) 100)))
    (define (slow! what ms t0)
      (if (>= ms SLOW-MS)
          (begin
            (display (string-append "SLOW " (qk) " t=" (number->string t0)
                                    " " what "=" (number->string ms)))
            (newline))))

    (define new-coord #f)                    ; set by a QP-COORD apply; post! adopts
    (define (apply-cmd! sm cmd)
      (if (and (pair? cmd) (not (bytevector? (car cmd))))
          (begin (display "BADCMD apply-cmd!: ") (write cmd) (newline)
                 (set! acc (cons #f acc))
                 (+ sm 1))
      (begin
      (if (null? cmd)
          (set! acc (cons #f acc))
          (if (and (pair? cmd) (string=? (cmd-op cmd) "QP-COORD"))
              ; replicated coordinator transfer: no MVCC write, no rev bump —
              ; every replica flips its coordinator at the same log position.
              (begin
                (set! new-coord (string->symbol (utf8->string (cadr cmd))))
                (set! acc (cons 'move-leader-ok acc)))
          (if (and (pair? cmd) (string=? (cmd-op cmd) "LEASE-KA"))
              ; replicated keepalive (leaderless leases): every replica bumps its
              ; local deadline clock at this log position; only the coordinator's
              ; lease-tick! ever acts on expiry, so per-node clock skew is benign.
              (let* ((id (bytes->int (cadr cmd)))
                     (ttl (mvcc-lease-meta-get ctx id)))
                (if ttl (hashtable-set! lease-deadlines id (+ (current-second) ttl)))
                (set! acc (cons (list 'keepalive-ok id (if ttl ttl 0)) acc)))
          (let ((pre (mvcc-current-rev ctx))
                (tap (current-second)))
            (let ((res (mvcc-apply ctx cmd)))
              (slow! (string-append "apply-" (cmd-op cmd)) (prof-ms (current-second) tap) tap)
              (set! acc (cons (if (string=? (cmd-op cmd) "TXN")
                                  (cons 'txnr (cons (mvcc-current-rev ctx) res))
                                  res)
                              acc)))
            (kv-wbuf-drain! ctx)
            (watch-notify-apply! pre (mvcc-current-rev ctx))
            (if (and (pair? cmd) (string=? (cmd-op cmd) "COMPACT"))
                (send watch-fanout (list 'watch-compact)))))))
      (+ sm 1))))

    (define (gsend to msg) (node-send-ch (symbol->string node-name) to my-channel msg))
    (define (emit! outs)
      (for-each
       (lambda (o)
         (guard (e (#t #f))
           (gsend (symbol->string (car o))
                  (list 'ws-engine shard-key node-name (cdr o)))))
       outs))

    (define (ack-waiter! conn payload)
      (cond
        ((and (pair? conn) (eq? (car conn) 'async))
         (guard (e (#t #f)) (send (cadr conn) (list 'put-done (caddr conn) payload))))
        ((not conn) #f)                      ; internal propose (lease expiry)
        (else (guard (e (#t #f)) (send conn payload)))))

    (define (qk) (string-append (symbol->string node-name) ":" shard-key))
    (define (publish! st)
      (table-insert! 'ws-shard-pid (qk) (self))
      (table-insert! 'ws-shard-role (qk) (if (qp-coord? st) 'leader 'follower))
      (table-insert! 'ws-shard-leader (qk) (qp-coord st))
      (table-insert! 'ws-shard-commit (qk) (qp-commit st))
      (table-insert! 'ws-shard-applied (qk) (qp-applied st)))

    ; drain the bid-keyed write acks: align applied batches to acc positionally.
    ; cw-rz9: the applied list is TAKEN in post! before the fsync (so the bid
    ; ring persists atomically with the effects) and parked in pending-done;
    ; acks still only fire here, after the flush.
    (define pending-done '())
    (define (drain-writes! st0)
      (let ((r (cons pending-done st0)))
        (set! pending-done '())
        (let ((done (car r)) (st (cdr r)))
          (if (pair? done)
              (let walk ((d done) (results (reverse acc)))
                (if (null? d)
                    (set! acc '())
                    (let* ((bid (caar d)) (n (cdar d))
                           (hit (assoc bid pending-bids)))
                      (let take ((k n) (rs results) (out '()))
                        (if (> k 0)
                            (take (- k 1) (cdr rs) (cons (car rs) out))
                            (begin
                              (if hit
                                  (let ackp ((cs (cdr hit)) (ps (reverse out)))
                                    (if (pair? cs)
                                        (begin (ack-waiter! (car cs) (car ps))
                                               (ackp (cdr cs) (cdr ps)))))
                                  #f)
                              (if hit (set! pending-bids
                                            (let del ((l pending-bids))
                                              (cond ((null? l) '())
                                                    ((equal? (caar l) bid) (cdr l))
                                                    (else (cons (car l) (del (cdr l))))))))
                              (if prof?
                                  (let ((pe (assoc bid prof-pending)))
                                    (if pe
                                        (let ((now (current-second)))
                                          (display (string-append
                                            "PROF " (qk)
                                            " n=" (number->string (cadddr pe))
                                            " wait=" (if (caddr pe)
                                                         (number->string (prof-ms (cadr pe) (caddr pe)))
                                                         "-")
                                            " cons=" (number->string (prof-ms now (cadr pe)))
                                            " flush=" (number->string prof-flush-ms)
                                            " t=" (number->string now)))
                                          (newline)
                                          (set! prof-pending
                                                (let del2 ((l prof-pending))
                                                  (cond ((null? l) '())
                                                        ((equal? (caar l) bid) (cdr l))
                                                        (else (cons (car l) (del2 (cdr l)))))))))))
                              (walk (cdr d) rs))))))))
          st)))

    ; cw-m9c (G1): dispatch a Range/LIST to the next range-worker in
    ; round-robin, off this mailbox entirely — the worker scans its own
    ; (per-request-refreshed) ctx over the shared handle and replies straight
    ; to CONN. Any consensus ordering (linearizable read slot) has ALREADY
    ; resolved by the time we dispatch. Term slot is 0, as range-reply's was.
    (define range-rr 0)
    (define (dispatch-range! conn opts)
      (set! range-rr (modulo (+ range-rr 1) (vector-length range-workers)))
      (send (vector-ref range-workers range-rr) (list 'kv-range-do conn opts 0)))

    ; cw-04k: notify watch-fanout of a committed (pre,post] window, off this mailbox
    ; (see shard-actor.scm's watch-notify-apply! for the full rationale — identical
    ; here). watch-backlog is this file's equivalent of the raft driver's `backlog`:
    ; this driver has no group-commit stash otherwise, so the (rare) blocking-ack
    ; path needs its own small requeue list, checked first by the main loop below.
    (define WATCH-FANOUT-MAX-INFLIGHT 512)
    (define watch-inflight 0)
    (define watch-backlog '())
    (define (watch-drain-one-ack!)
      (let ((t0 (current-second)))
        (let wait ()
          (let ((r (raw-receive)))
            (if (and (pair? r) (eq? (car r) 'watch-apply-ack))
                (set! watch-inflight (- watch-inflight 1))
                (begin (set! watch-backlog (append watch-backlog (list r))) (wait)))))
        (slow! "watch-wait" (prof-ms (current-second) t0) t0)))
    (define (watch-notify-apply! pre post)
      (if (> post pre)
          (begin
            (if (>= watch-inflight WATCH-FANOUT-MAX-INFLIGHT) (watch-drain-one-ack!))
            (send watch-fanout (list 'watch-apply pre post (self)))
            (set! watch-inflight (+ watch-inflight 1)))))

    ; drain completed linearizable reads: tag = ('read conn) | ('range conn opts)
    (define (drain-reads! st0)
      (let ((r (qp-take-reads st0)))
        (for-each
         (lambda (tag)
           (guard (e (#t #f))
             (case (car tag)
               ((read) (send (cadr tag) (cons 'read-ok (qp-applied (cdr r)))))
               ((range) (dispatch-range! (cadr tag) (caddr tag))))))
         (car r))
        (cdr r)))

    ; the lagging side pulls; the up-to-date side ships (same ws-snap frames)
    (define (maybe-snap-pull! st0)
      (let ((need (qp-snap-need st0)))
        (if (and need (>= (- ticks snap-pull-last) SNAP-MIN-TICKS))
            (begin
              (set! snap-pull-last ticks)
              (guard (e (#t #f))
                (gsend (symbol->string (car need))
                       (list 'ws-engine shard-key node-name (list 'snap-pull))))
              (qp-clear-snap-need st0))
            st0)))

    (define (ship-snapshot! to st)
      (let ((last (hashtable-ref snap-last to (- 0 SNAP-MIN-TICKS)))
            (send-payload
             (lambda (payload)
               (guard (e (#t #f))
                 (gsend (symbol->string to)
                        (list 'ws-snap shard-key node-name payload))))))
        (if (>= (- ticks last) SNAP-MIN-TICKS)
            (begin
              (hashtable-set! snap-last to ticks)
              (send-payload (list 'begin (qp-applied st) 0))
              (let chunk ((rows (kv-scan ctx (make-bytevector 0))))
                (if (pair? rows)
                    (let split ((n SNAP-CHUNK-ROWS) (r rows) (acc2 '()))
                      (if (or (= n 0) (null? r))
                          (begin (send-payload (list 'rows (reverse acc2)))
                                 (chunk r))
                          (split (- n 1) (cdr r) (cons (car r) acc2))))))
              (send-payload (list 'end))))))

    ; persist + fsync + drain acks/reads + snapshot pull + publish, after any
    ; engine transition. old-applied gates the persist/fsync work.
    ;
    ; ENGINE LOG COMPACTION (cw-dgp): the engine's per-slot state lives in
    ; alists; nothing ever pruned them in normal operation, so at k8s load
    ; every slot lookup walked an ever-growing list — CPU grew with uptime
    ; until the shard actor pinned a core (~35-45 min) and the node starved.
    ; Compact the engine to (applied - QP-LOG-KEEP) every QP-COMPACT-EVERY
    ; applied slots: laggards within the window use ranged fetch, deeper
    ; laggards take the ws-snap store-snapshot path (both already exist).
    (define QP-LOG-KEEP
      (let ((e (get-environment-variable "CWS_QP_LOG_KEEP")))
        (if e (or (string->number e) 512) 512)))
    (define QP-COMPACT-EVERY 64)
    (define next-compact QP-COMPACT-EVERY)
    (define (post! st old-applied)
      ; cw-rz9: take the batches this action applied and persist each
      ; origin's seq state BEFORE the flush below — same WriteBatch + fsync
      ; as their effects, so a crash can never separate "applied" from "in
      ; the dedup state". drain-writes! acks them after the flush.
      (let ((done (car (qp-take-applied st))))
        (if (pair? done)
            (let obs ((d done) (seen '()))
              (if (null? d)
                  (set! pending-done (append pending-done done))
                  (let* ((bid (caar d)) (ob (cons (car bid) (cadr bid))))
                    (if (member ob seen)
                        (obs (cdr d) seen)
                        (begin (persist-seq-state! st (car ob) (cdr ob))
                               (obs (cdr d) (cons ob seen)))))))))
      (if (> (qp-applied st) old-applied)
          (ctx-save-applied! ctx (qp-applied st) 0))
      (if new-coord
          (begin (set! st (qp-set-coord st new-coord)) (set! new-coord #f)))
      (if (ctx-dirty? ctx)
          (let ((t0 (current-second)))
            (ctx-flush! ctx)                     ; durable BEFORE any ack
            (set! prof-flush-ms (prof-ms (current-second) t0))
            (slow! "flush" prof-flush-ms t0)))
      (if (>= (qp-applied st) next-compact)
          (begin
            (set! next-compact (+ (qp-applied st) QP-COMPACT-EVERY))
            (if (> (- (qp-applied st) QP-LOG-KEEP) (qp-base st))
                (let ((t0 (current-second)))
                  (set! st (qp-compact-to st (- (qp-applied st) QP-LOG-KEEP)))
                  (slow! "compact" (prof-ms (current-second) t0) t0)))))
      (let* ((td (current-second))
             (st (drain-writes! st))
             (x (slow! "drainw" (prof-ms (current-second) td) td))
             (st (flush-stash! st))          ; cw-65x H8
             (st (drain-reads! st))
             (st (maybe-snap-pull! st)))
        (publish! st)
        st))

    ; run one engine action: (st -> (st' . outs)), then post!
    (define engine-what "?")                 ; cw-vku: label for slow! spans
    (define (engine! st action)
      (let* ((t0 (current-second))
             (old (qp-applied st))
             (r (action st))
             (t1 (current-second)))
        (slow! (string-append engine-what "-act") (prof-ms t1 t0) t0)
        (emit! (cdr r))
        (slow! "emit" (prof-ms (current-second) t1) t1)
        (let ((st2 (post! (car r) old)))
          (slow! engine-what (prof-ms (current-second) t0) t0)
          st2)))

    ; ---- coordinator lease expiry (same ADR 0003 §2 flow, coordinator-owned) ----
    (define (lease-tick! st)
      (let ((now (current-second))
            (live (mvcc-all-lease-ids ctx)))
        (vector-for-each
         (lambda (id)
           (if (not (mvcc-lease-exists? ctx id)) (hashtable-delete! lease-revoking id)))
         (hashtable-keys lease-revoking))
        (for-each
         (lambda (id)
           (if (and (not (hashtable-contains? lease-deadlines id))
                    (not (hashtable-contains? lease-revoking id)))
               (let ((ttl (mvcc-lease-meta-get ctx id)))
                 (if ttl (hashtable-set! lease-deadlines id (+ now ttl))))))
         live)
        (let ((expired
               (let collect ((ks (vector->list (hashtable-keys lease-deadlines))) (out '()))
                 (cond ((null? ks) out)
                       ((<= (hashtable-ref lease-deadlines (car ks) 0) now)
                        (collect (cdr ks) (cons (car ks) out)))
                       (else (collect (cdr ks) out))))))
          (let loop ((ids expired) (st st))
            (if (null? ids) st
                (let ((id (car ids)))
                  (hashtable-delete! lease-deadlines id)
                  (hashtable-set! lease-revoking id #t)
                  (loop (cdr ids)
                        (engine! st (lambda (s)
                                      (qp-propose-batch s
                                        (list (list (string->utf8 "LEASE-REVOKE")
                                                    (int->bytes id)))))))))))))

    ; propose a client batch: register waiters under the NEXT bid the engine
    ; will assign ((id . seq+1)) — qp-propose-batch allocates exactly one.
    (define (propose-client! st items)          ; items: ((conn . cmd) ...)
      (let* ((raw-cmds (map cdr items))
             (need (if gr-writer? (gr-count-puts raw-cmds) 0)))
        ; cw-6cq: bounce (never block) when this batch needs more global revs
        ; than the local lease currently holds — mirrors shard-actor.scm's
        ; liveness fix. The client retries once the async refill lands.
        (if (and gr-writer? (> need 0) (< (lease-remaining rev-lease) need))
            (begin
              (gr-maybe-refill!)
              (for-each (lambda (it) (ack-waiter! (car it) 'tryagain)) items)
              st)
            (let* ((bid (qp-next-bid st))
                   (cmds (cond (gr-writer? (gr-rewrite-batch! raw-cmds))
                               (rev-authority? (auth-rewrite-batch raw-cmds))
                               (else raw-cmds))))
              (set! pending-bids (cons (cons bid (map car items)) pending-bids))
              (if prof?
                  (let ((tsub (let scan ((l items) (best #f))
                                (if (null? l) best
                                    (let ((c (caar l)))
                                      (scan (cdr l)
                                            (if (and (pair? c) (eq? (car c) 'async)
                                                     (>= (length c) 4)
                                                     (or (not best) (< (cadddr c) best)))
                                                (cadddr c) best)))))))
                    (set! prof-pending
                          (cons (list bid (current-second) tsub (length items)) prof-pending))))
              (set! engine-what "propose")
              (let ((st2 (engine! st (lambda (s) (qp-propose-batch s cmds)))))
                (gr-maybe-refill!)
                st2)))))

    ; boot epoch: persisted, strictly increasing per restart (bids embed it so
    ; a restart's fresh seq can never collide with pre-crash bids).
    (define QP-BOOT-KEY (string->utf8 "_qp_boot"))   ; plain key, like _raft_applied
    (define (persist-boot! n)
      (kv-put! ctx QP-BOOT-KEY (u64->bytes n))
      (ctx-flush! ctx))
    (define boot-epoch
      (let* ((v (kv-get ctx QP-BOOT-KEY))
             (nu (+ 1 (if (and v (>= (bytevector-length v) 8)) (bytes->u64 v 0) 0))))
        (persist-boot! nu)
        nu))

    ; cw-rz9: persisted per-origin applied-seq state — exactly-once across
    ; restarts AND across window overflow. The engine dedups by
    ; (origin boot) -> (floor . sparse) applied-seq entries (complete: a
    ; hedged duplicate deciding arbitrarily many slots later is still
    ; caught — the old 256-bid ring overflowed under kill+partition and an
    ; empty reload re-applied batches after restart; both showed up as
    ; Elle duplicate-elements). Each applied batch overwrites its origin's
    ; one small `_qp_seq_<origin>_<boot>` key IN THE SAME WriteBatch/fsync
    ; as its effects (post! persists before ctx-flush!); the full set is
    ; reloaded at boot and after ws-snap install (the keys ride the store
    ; snapshot, so a rejoiner inherits the SENDER's state — the cc-cri
    ; lesson). Value: [u64 floor][u64 n][u64 sparse ...].
    (define QP-SEQ-PREFIX "_qp_seq_")
    (define (seq-key origin boot)
      (string->utf8 (string-append QP-SEQ-PREFIX (symbol->string origin)
                                   "_" (number->string boot))))
    (define (seq-state->bytes e)                     ; e = (floor . sparse)
      (let loop ((l (cdr e))
                 (acc (bytevector-append (u64->bytes (car e))
                                         (u64->bytes (length (cdr e))))))
        (if (null? l) acc
            (loop (cdr l) (bytevector-append acc (u64->bytes (car l)))))))
    (define (persist-seq-state! st origin boot)
      (let ((e (qp-seq-state st origin boot)))
        (if e (kv-put! ctx (seq-key origin boot) (seq-state->bytes e)))))
    (define (load-seq-window!)                       ; -> ((origin boot f . sparse) ...)
      (let ((pfx (string->utf8 QP-SEQ-PREFIX)) (plen (string-length QP-SEQ-PREFIX)))
        (let loop ((kvs (kv-scan ctx pfx)) (out '()))
          (if (null? kvs) out
              (let* ((k (utf8->string (caar kvs))) (v (cdar kvs))
                     ; origin_boot: boot is everything after the LAST "_"
                     (cut (let find ((i (- (string-length k) 1)))
                            (if (char=? (string-ref k i) #\_) i (find (- i 1)))))
                     (origin (string->symbol (substring k plen cut)))
                     (boot (string->number (substring k (+ cut 1) (string-length k))))
                     (f (bytes->u64 v 0)) (n (bytes->u64 v 8))
                     (sparse (let sp ((j 0) (acc '()))
                               (if (>= j n) (reverse acc)
                                   (sp (+ j 1)
                                       (cons (bytes->u64 v (+ 16 (* 8 j))) acc))))))
                (loop (cdr kvs) (cons (cons origin (cons boot (cons f sparse))) out)))))))
    (let* ((loaded (ctx-load-applied ctx))
           (p (car loaded))
           (st0 (make-qp node-name voters apply-cmd! 0
                         (list (cons 'coord coord) (cons 'hedge hedge-ticks)
                               (cons 'boot boot-epoch)
                               (cons 'seed (+ 1 (qp-index-of node-name voters))))))
           ; cw-rz9: reload the persisted per-origin dedup state
           (seqs0 (load-seq-window!))
           (st0 (if (> p 0) (qp-install-snapshot st0 p 0 seqs0) st0)))
      (publish! st0)
      (let loop ((st st0))
        (let ((m (cond ((pair? watch-backlog)
                        (let ((b (car watch-backlog))) (set! watch-backlog (cdr watch-backlog)) b))
                       (else (raw-receive)))))
          (cond
            ((not (pair? m)) (loop st))

            ;; ---- watch-fanout ack (cw-04k): normal-path drain — see shard-actor.scm.
            ((eq? (car m) 'watch-apply-ack)
             (set! watch-inflight (- watch-inflight 1))
             (loop st))

            ;; ---- engine RPC from a peer (incl. our snap-pull extension) ----
            ((eq? (car m) 'engine)
             (let ((from (cadr m)) (rpc (caddr m)))
               (cond
                 ((eq? (car rpc) 'snap-pull)
                  (ship-snapshot! from st) (loop st))
                 ; cw-65x: coordinator slot-coalescing — a pfwd storm (every
                 ; non-coordinator forwards its batches here) used to open one
                 ; consensus slot PER forwarded batch, so throughput was capped
                 ; at slots/s regardless of batch size. Scoop every further
                 ; pfwd already sitting in the mailbox and decide them all in
                 ; ONE ('multi ...) slot; other frames are re-enqueued to self
                 ; (same order-tolerance argument as the client-batch drain).
                 ((and (eq? (car rpc) 'pfwd) (qp-coord? st))
                  (let collect ((vals (list (cadr rpc))) (n 1))
                    (let ((nxt (if (< n PROPOSE-BATCH-CAP) (raw-receive 0) '*timeout*)))
                      (cond
                        ((and (pair? nxt) (eq? (car nxt) 'engine)
                              (pair? (caddr nxt)) (eq? (car (caddr nxt)) 'pfwd))
                         (collect (cons (cadr (caddr nxt)) vals) (+ n 1)))
                        (else
                         (if (not (eq? nxt '*timeout*)) (send (self) nxt))
                         (set! engine-what "pfwd-multi")
                         (loop (engine! st
                                 (lambda (s)
                                   (if (null? (cdr vals))
                                       (qp-start-slot s (car vals))
                                       (qp-start-slot s (cons 'multi (reverse vals))))))))))))
                 (else
                  (set! engine-what (string-append "peer-" (symbol->string (car rpc))))
                  (loop (engine! st (lambda (s) (qp-step s from rpc))))))))

            ;; ---- tick: hedge/retransmit/gap-fill + lease expiry + progress ----
            ((eq? (car m) 'tick)
             (set! ticks (+ ticks 1))
             (if (= 0 (modulo ticks 16))
                 (let ((t0 (current-second)))
                   (collect-garbage)
                   (slow! "gc" (prof-ms (current-second) t0) t0)))
             (send watch-fanout (list 'watch-progress))   ; cw-04k: off-thread
             ; cw-vku: incremental COMPACT GC — one bounded slice per tick (the
             ; apply only flips the gate; see mvcc-compact-gc-step!). Flushed by
             ; the engine!'s post! below (ctx goes dirty) before any ack.
             (mvcc-compact-gc-step! ctx)
             (gr-report-progress!)             ; cw-6cq: no-op unless gr-writer?
             (gr-maybe-refill!)                ; keep the lease warm
             (set! engine-what "tick")
             (let* ((st (engine! st qp-tick))
                    ; cw-2au scale cliff: lease-tick!'s mvcc-all-lease-ids is a FULL
                    ; lease-namespace scan in interpreted Scheme ON this shard thread —
                    ; at ~20k leases (15k+ pods) every-250ms scans starved the shard
                    ; (38s Txn/LeaseGrant stalls -> k3s deadline storms). Renewals bump
                    ; lease-deadlines at LEASE-KA apply time; the scan only seeds
                    ; unseen leases + cleans revoked ones, so 5s granularity is fine
                    ; (TTLs are 10s+; etcd revokes lazily too).
                    (st (if (and (qp-coord? st) (= 0 (modulo ticks 20))) (lease-tick! st) st)))
               (loop st)))

            ;; ---- linearizable read probe ----
            ((eq? (car m) 'read)
             (loop (engine! st (lambda (s) (qp-read s (list 'read (cadr m)))))))

            ;; ---- KV range: serializable local, linearizable via a read slot ----
            ((eq? (car m) 'kv-range)
             (let ((conn (cadr m)) (opts (caddr m)))
               (if (range-opt opts 'serializable #f)
                   (begin (dispatch-range! conn opts) (loop st))
                   (loop (engine! st (lambda (s) (qp-read s (list 'range conn opts))))))))

            ;; ---- replica-local reads (identical to the raft driver) ----
            ((eq? (car m) 'get)
             (let* ((conn (cadr m)) (k (caddr m)) (r (mvcc-get-latest ctx k)))
               (send conn (if r (list (kv-rec-value r) (kv-rec-create-rev r)
                                      (kv-rec-mod-rev r) (kv-rec-version r))
                              #f)))
             (loop st))
            ((eq? (car m) 'kv-prev)
             (let* ((conn (cadr m)) (k (caddr m)) (rec (mvcc-get-latest ctx k)))
               (send conn (list 'kv-prev-ok
                                (if rec
                                    (list k (kv-rec-value rec) (kv-rec-create-rev rec)
                                          (kv-rec-mod-rev rec) (kv-rec-version rec)
                                          (kv-rec-lease rec))
                                    #f))))
             (loop st))
            ((eq? (car m) 'cur-rev)
             (send (cadr m) (list 'cur-rev-ok (mvcc-current-rev ctx) 1))
             (loop st))
            ((eq? (car m) 'status)
             ; cw-xq9: O(1) incremental stats — the old mvcc-digest-at full-scan
             ; here (per Status AND per health probe) pinned the shard thread at
             ; 500-pod k8s scale and crash-looped k3s on lease-Txn deadlines.
             (let ((dig (mvcc-live-stats ctx)))
               ; term is a raft concept; report the constant 1 (etcdctl and the
               ; maintenance proof assert raftTerm > 0). QuePaxa has no terms.
               (send (cadr m) (list 'status-ok (mvcc-current-rev ctx) 1
                                    (qp-commit st) (qp-applied st) (car dig)
                                    (qp-coord st) (cadr dig))))
             (loop st))
            ((eq? (car m) 'hashkv)
             (let* ((req-rev (caddr m))
                    (at (if (= req-rev 0) (mvcc-current-rev ctx) req-rev))
                    (dig (mvcc-digest-at ctx at)))
               (send (cadr m) (list 'hashkv-ok (car dig) (mvcc-compact-rev ctx) at)))
             (loop st))
            ((eq? (car m) 'snapshot)
             (send (cadr m) (list 'snapshot-ok (mvcc-current-rev ctx)
                                  (mvcc-snapshot-kvs ctx 0)))
             (loop st))
            ((eq? (car m) 'defrag)
             (guard (e (#t #f)) (store-flush (shard-ctx-handle ctx)))
             (ctx-flush! ctx)
             (send (cadr m) (list 'defrag-ok))
             (loop st))
            ((eq? (car m) 'alarm-list)
             (send (cadr m) (list 'alarm-list-ok (mvcc-alarm-list ctx)))
             (loop st))
            ((eq? (car m) 'lease-probe)
             (let ((conn (cadr m)) (id (caddr m)) (keys (cadddr m)))
               (send conn
                     (list (mvcc-lease-exists? ctx id)
                           (mvcc-current-rev ctx)
                           (map (lambda (k)
                                  (let ((rows (kv-scan ctx (key-cf-prefix k))))
                                    (if (null? rows)
                                        (list k 'absent 0)
                                        (let ((rec (kv-record-decode (cdar rows))))
                                          (list k (kv-rec-tombstone? rec)
                                                (kv-rec-mod-rev rec))))))
                                keys))))
             (loop st))

            ;; ---- auth read seams (replica-local, same as raft driver) ----
            ((eq? (car m) 'auth-state)
             (send (cadr m) (list 'auth-state-ok (auth-enabled? ctx) (auth-rev ctx)))
             (loop st))
            ((eq? (car m) 'auth-authorize)
             (let ((conn (cadr m)) (user (caddr m)) (key (cadddr m))
                   (rend (list-ref m 4)) (req (list-ref m 5)))
               (send conn (list 'auth-authorize-ok (auth-authorize? ctx user key rend req))))
             (loop st))
            ((eq? (car m) 'auth-lookup)
             (let* ((conn (cadr m)) (name (caddr m)) (u (auth-get-user ctx name)))
               (send conn
                     (if u
                         (list 'auth-lookup-ok #t (auth-user-hash u)
                               (auth-has-root-role? (auth-user-roles u)) (auth-rev ctx))
                         (list 'auth-lookup-ok #f #f #f (auth-rev ctx)))))
             (loop st))
            ((eq? (car m) 'auth-user-info)
             (let* ((conn (cadr m)) (name (caddr m)) (u (auth-get-user ctx name)))
               (send conn (if u (list 'auth-user-info-ok #t (auth-user-roles u))
                              (list 'auth-user-info-ok #f #f))))
             (loop st))
            ((eq? (car m) 'auth-user-list)
             (send (cadr m) (list 'auth-user-list-ok (auth-all-users ctx)))
             (loop st))
            ((eq? (car m) 'auth-role-info)
             (let* ((conn (cadr m)) (name (caddr m)) (perms (auth-get-role ctx name)))
               (send conn
                     (if perms
                         (list 'auth-role-info-ok #t
                               (map (lambda (p) (list (vector-ref p 0) (vector-ref p 1)
                                                      (vector-ref p 2)))
                                    perms))
                         (list 'auth-role-info-ok #f #f))))
             (loop st))
            ((eq? (car m) 'auth-role-list)
             (send (cadr m) (list 'auth-role-list-ok (auth-all-roles ctx)))
             (loop st))

            ;; ---- watch register/cancel (replica-local; cw-04k: forwarded to
            ;; watch-fanout, which owns the whole registry now — see shard-actor.scm) ----
            ((eq? (car m) 'watch-register)
             (send watch-fanout (list 'watch-register (cadr m) (caddr m)))
             (loop st))
            ((eq? (car m) 'watch-cancel)
             (send watch-fanout (list 'watch-cancel (cadr m) (caddr m)))
             (loop st))

            ;; ---- leases: fully leaderless (any node serves; found on the k3s run —
            ;; real etcd serves Lease RPCs on ANY member, and a kube control plane
            ;; whose LeaseGrants bounce 4/5 of the time never converges).
            ;; grant/revoke were always replicated cmds — the coordinator gate was
            ;; unnecessary. keepalive replicates as LEASE-KA so EVERY node's deadline
            ;; clock advances; only the coordinator's lease-tick! enforces expiry.
            ((eq? (car m) 'lease-grant)
             (let ((reply-pid (cadr m)) (ttl (caddr m)) (id (cadddr m)))
               (loop (propose-client! st
                       (list (cons reply-pid
                                   (list (string->utf8 "LEASE-GRANT")
                                         (int->bytes id) (int->bytes ttl))))))))
            ((eq? (car m) 'lease-revoke)
             (let ((reply-pid (cadr m)) (id (caddr m)))
               (hashtable-delete! lease-deadlines id)
               (hashtable-set! lease-revoking id #t)
               (loop (propose-client! st
                       (list (cons reply-pid
                                   (list (string->utf8 "LEASE-REVOKE")
                                         (int->bytes id))))))))
            ((eq? (car m) 'lease-keepalive)
             (let ((reply-pid (cadr m)) (id (caddr m)))
               (if (mvcc-lease-meta-get ctx id)
                   (loop (propose-client! st
                           (list (cons reply-pid
                                       (list (string->utf8 "LEASE-KA")
                                             (int->bytes id))))))
                   (begin (send reply-pid (list 'keepalive-ok id 0))
                          (loop st)))))
            ((eq? (car m) 'lease-ttl)
             (let ((reply-pid (cadr m)) (id (caddr m)) (with-keys? (cadddr m)))
               (let ((ttl (mvcc-lease-meta-get ctx id)))
                 ; a TTL query can land before anything seeds this lease's local
                 ; deadline — seed the full window now (same value a KA would set).
                 (if (and ttl (not (hashtable-contains? lease-deadlines id))
                          (not (hashtable-contains? lease-revoking id)))
                     (hashtable-set! lease-deadlines id (+ (current-second) ttl)))
                 (let* ((deadline (hashtable-ref lease-deadlines id #f))
                        (now (current-second))
                        (remaining (if (and ttl deadline)
                                       (max 0 (exact (ceiling (- deadline now))))
                                       -1))
                        (keys (if with-keys? (mvcc-lease-keys ctx id) '())))
                   (send reply-pid (list 'lease-ttl-ok id (if ttl ttl 0) remaining keys))))
               (loop st)))
            ((eq? (car m) 'lease-leases)
             (let ((reply-pid (cadr m)))
               (send reply-pid (list 'lease-leases-ok (mvcc-all-lease-ids ctx)))
               (loop st)))

            ;; ---- cw-6cq global-rev: refill reply from the rev-authority. Rides the
            ;; standard client-proposal ack path as ("REV-GRANT" . lo) — a STRING car,
            ;; so this MUST be matched before the generic client-cmd fallthrough below.
            ((and global-rev? (pair? m) (string? (car m)) (string=? (car m) "REV-GRANT"))
             (if rev-refill-inflight
                 (begin (set! rev-lease (lease-add rev-lease (cdr m) rev-refill-inflight))
                        (set! rev-refill-inflight #f)))
             (loop st))
            ((eq? (car m) 'global-high)
             (send (cadr m) (list 'global-high-ok (mvcc-global-rev ctx)))
             (loop st))
            ((eq? (car m) 'rev-progress)
             (if rev-authority? (hashtable-set! rev-progress (cadr m) (caddr m)))
             (loop st))
            ((eq? (car m) 'global-watermark)
             (send (cadr m) (list 'global-watermark-ok (gr-watermark-now)))
             (loop st))
            ((eq? (car m) 'compact-admissible)
             (send (caddr m) (list 'compact-admissible-ok (<= (cadr m) (gr-watermark-now))))
             (loop st))

            ;; ---- unsupported on quepaxa groups (raft-only features) ----
            ((memq (car m) '(member-add member-remove member-promote))
             (send (cadr m) 'member-pending)     ; refused; Q11 (cw-2w6)
             (loop st))
            ((eq? (car m) 'member-list)
             (send (cadr m) (list 'member-list voters '()))
             (loop st))
            ;; ---- MoveLeader = replicated coordinator transfer ----
            ((eq? (car m) 'move-leader)
             (let ((reply-pid (cadr m)) (target (caddr m)))
               (cond
                 ((not (memv target voters))
                  (send reply-pid (cons 'move-leader-err 'not-voter))
                  (loop st))
                 ((eqv? target (qp-coord st))
                  (send reply-pid (if (eqv? target node-name)
                                      (cons 'move-leader-err 'self)
                                      'move-leader-ok))
                  (loop st))
                 (else
                  (loop (propose-client! st
                          (list (cons reply-pid
                                      (list (string->utf8 "QP-COORD")
                                            (string->utf8 (symbol->string target)))))))))))

            ;; ---- snapshot install (pulled; same ws-snap frames) ----
            ((eq? (car m) 'snap-install)
             (let ((payload (caddr m)))
               (cond
                 ((eq? (car payload) 'begin)
                  (set! snap-accum
                        (if (<= (cadr payload) (qp-applied st))
                            #f
                            (list (cadr payload) '())))
                  (loop st))
                 ((eq? (car payload) 'rows)
                  (if snap-accum
                      (set-car! (cdr snap-accum)
                                (append (cadr snap-accum) (cadr payload))))
                  (loop st))
                 ((and (eq? (car payload) 'end) snap-accum)
                  (let ((sbase (car snap-accum)) (rows (cadr snap-accum))
                        (pre (mvcc-current-rev ctx)))
                    (set! snap-accum #f)
                    ; cw-6cq: a snapshot install may supersede revs this replica's
                    ; own lease had not yet drawn on — drop it; gr-maybe-refill!
                    ; draws a fresh block on the next write/tick.
                    (if gr-writer? (begin (set! rev-lease (lease-new)) (set! rev-refill-inflight #f)))
                    (for-each (lambda (kv) (kv-del! ctx (car kv)))
                              (kv-scan ctx (make-bytevector 0)))
                    (for-each (lambda (kv) (kv-put! ctx (car kv) (cdr kv))) rows)
                    (set-shard-ctx-crev! ctx -1)
                    (mvcc-live-stats-invalidate! ctx) (mvcc-latest-cache-invalidate! ctx)   ; cw-xq9: bulk install bypassed mvcc-put!
                    (ctx-save-applied! ctx sbase 0)
                    (persist-boot! boot-epoch)   ; the wipe adopted the SENDER's counter
                    (ctx-flush! ctx)
                    (set! acc '())                 ; installed state supersedes
                    ; cw-04k: off-thread; order matters (apply window before the
                    ; compaction-floor advance) — FIFO to the single fanout worker
                    ; preserves it, same as shard-actor.scm's snapshot-install path.
                    (kv-wbuf-drain! ctx)
            (watch-notify-apply! pre (mvcc-current-rev ctx))
                    (send watch-fanout (list 'watch-compact))
                    ; cw-rz9: the installed rows carry the SENDER's per-origin
                    ; seq state — adopt it so a pre-snapshot batch can't
                    ; re-apply here.
                    (let ((st2 (qp-install-snapshot st sbase 0 (load-seq-window!))))
                      (publish! st2)
                      (loop st2))))
                 (else (loop st)))))

            ;; ---- local client command: (conn-pid . cmd), batched drain ----
            (else
             (let collect ((items (list (cons (car m) (cdr m)))) (n 1))
               (let ((nxt (if (< n PROPOSE-BATCH-CAP) (raw-receive 0) '*timeout*)))
                 (cond
                   ; cw-ecu: exclude string-car pairs — ("REV-GRANT" . lo) authority
                   ; replies have a STRING car, and scooping one here as a fake
                   ; (conn . cmd) client item would drop the grant and wedge
                   ; rev-refill-inflight forever. Re-enqueue it to self instead (else
                   ; branch below) so the dedicated handler in the main cond sees it.
                   ((and (pair? nxt) (not (symbol? (car nxt))) (not (string? (car nxt))))
                    (collect (cons nxt items) (+ n 1)))
                   (else
                    ; re-enqueue a non-write frame to SELF (mailbox order shifts
                    ; by one batch; every handler is order-tolerant)
                    (if (not (eq? nxt '*timeout*)) (send (self) nxt))
                    (if (>= (inflight) PROPOSE-GATE)
                        (begin                       ; H8: too many slots in flight — stash
                          (set! stash (append items stash))
                          (set! stash-n (+ stash-n (length items)))
                          (loop st))
                        (loop (propose-client! st (reverse items)))))))))))))))
