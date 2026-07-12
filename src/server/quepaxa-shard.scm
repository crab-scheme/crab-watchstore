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
;   global-rev multi-group allocation, parallel apply workers.
; Lease grant/keepalive/ttl stay COORDINATOR-gated (one deadline owner), same
; redirect shape ('lease-not-leader . coord) the gRPC layer already handles.
; ponytail: durable mode fsyncs per transition batch (no cross-transition
; group-commit deferral yet); add the flush-base machinery if w/s needs it.

(include "src/encoding.scm")
(include "src/store-ctx.scm")
(include "src/mvcc.scm")
(include "src/auth.scm")
(include "src/watch.scm")
(include "src/quepaxa.scm")

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
         (watch-reg (make-watch-registry))
         (lease-deadlines (make-eqv-hashtable))
         (lease-revoking (make-eqv-hashtable)))

    (define acc '())                         ; apply results, newest-first
    (define pending-bids '())                ; ((bid . (conn ...)) ...)
    (define snap-accum #f)                   ; inbound snapshot (BASE ROWS)
    (define snap-last (make-eqv-hashtable))  ; outbound rate limit, per peer
    (define snap-pull-last -1000)            ; inbound (our own pull) rate limit
    (define ticks 0)
    (define SNAP-MIN-TICKS 50)
    (define SNAP-CHUNK-ROWS 200)
    (define PROPOSE-BATCH-CAP 64)

    ; CWS_PROF=1: per-write hop profiling (cw-xq9). Logs one line per acked batch:
    ;   PROF <shard> n=<batch> wait=<submit->propose ms> cons=<propose->ack ms> flush=<last fsync ms>
    ; wait covers grpc-worker send -> shard dequeue+batch; cons covers the full
    ; consensus round (mesh hops + peer processing + apply + fsync-before-ack).
    (define prof? (equal? (get-environment-variable "CWS_PROF") "1"))
    (define prof-pending '())                ; ((bid tpropose tsubmit n) ...)
    (define prof-flush-ms 0)
    (define (prof-ms a b) (exact (round (* 1000 (- a b)))))

    (define new-coord #f)                    ; set by a QP-COORD apply; post! adopts
    (define (apply-cmd! sm cmd)
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
          (let ((pre (mvcc-current-rev ctx)))
            (let ((res (mvcc-apply ctx cmd)))
              (set! acc (cons (if (string=? (cmd-op cmd) "TXN")
                                  (cons 'txnr (cons (mvcc-current-rev ctx) res))
                                  res)
                              acc)))
            (watch-on-apply! watch-reg ctx pre (mvcc-current-rev ctx))
            (if (and (> (reg-count watch-reg) 0)
                     (pair? cmd) (string=? (cmd-op cmd) "COMPACT"))
                (watch-check-compaction! watch-reg ctx))))))
      (+ sm 1))

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

    ; drain the bid-keyed write acks: align applied batches to acc positionally
    (define (drain-writes! st0)
      (let ((r (qp-take-applied st0)))
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
                                            " flush=" (number->string prof-flush-ms)))
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
    (define QP-LOG-KEEP 512)
    (define QP-COMPACT-EVERY 64)
    (define next-compact QP-COMPACT-EVERY)
    (define (post! st old-applied)
      (if (> (qp-applied st) old-applied)
          (ctx-save-applied! ctx (qp-applied st) 0))
      (if new-coord
          (begin (set! st (qp-set-coord st new-coord)) (set! new-coord #f)))
      (if (ctx-dirty? ctx)
          (if prof?
              (let ((t0 (current-second)))
                (ctx-flush! ctx)
                (set! prof-flush-ms (prof-ms (current-second) t0)))
              (ctx-flush! ctx)))                 ; durable BEFORE any ack
      (if (>= (qp-applied st) next-compact)
          (begin
            (set! next-compact (+ (qp-applied st) QP-COMPACT-EVERY))
            (if (> (- (qp-applied st) QP-LOG-KEEP) (qp-base st))
                (set! st (qp-compact-to st (- (qp-applied st) QP-LOG-KEEP))))))
      (let* ((st (drain-writes! st))
             (st (drain-reads! st))
             (st (maybe-snap-pull! st)))
        (publish! st)
        st))

    ; run one engine action: (st -> (st' . outs)), then post!
    (define (engine! st action)
      (let* ((old (qp-applied st))
             (r (action st)))
        (emit! (cdr r))
        (post! (car r) old)))

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
      (let ((bid (qp-next-bid st)))
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
        (engine! st (lambda (s) (qp-propose-batch s (map cdr items))))))

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
    (let* ((loaded (ctx-load-applied ctx))
           (p (car loaded))
           (st0 (make-qp node-name voters apply-cmd! 0
                         (list (cons 'coord coord) (cons 'hedge hedge-ticks)
                               (cons 'boot boot-epoch)
                               (cons 'seed (+ 1 (qp-index-of node-name voters))))))
           (st0 (if (> p 0) (qp-install-snapshot st0 p 0 '()) st0)))
      (publish! st0)
      (let loop ((st st0))
        (let ((m (raw-receive)))
          (cond
            ((not (pair? m)) (loop st))

            ;; ---- engine RPC from a peer (incl. our snap-pull extension) ----
            ((eq? (car m) 'engine)
             (let ((from (cadr m)) (rpc (caddr m)))
               (if (eq? (car rpc) 'snap-pull)
                   (begin (ship-snapshot! from st) (loop st))
                   (loop (engine! st (lambda (s) (qp-step s from rpc)))))))

            ;; ---- tick: hedge/retransmit/gap-fill + lease expiry + progress ----
            ((eq? (car m) 'tick)
             (set! ticks (+ ticks 1))
             (if (= 0 (modulo ticks 16)) (collect-garbage))
             (if (> (reg-count watch-reg) 0)
                 (watch-progress-all! watch-reg ctx))
             (let* ((st (engine! st qp-tick))
                    (st (if (qp-coord? st) (lease-tick! st) st)))
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

            ;; ---- watch register/cancel (replica-local, identical) ----
            ((eq? (car m) 'watch-register)
             (let ((reply-pid (cadr m)) (spec (caddr m)))
               (let* ((deliver-fn
                       (lambda (wr)
                         (guard (e (#t #f))
                           (send reply-pid (list 'watch-response (watch-response->sexp wr))))))
                      (res (watch-register! watch-reg ctx spec deliver-fn)))
                 (if (and (pair? res) (eq? (car res) 'compacted))
                     (send reply-pid (cons 'watch-compacted (cdr res)))
                     (send reply-pid (list 'watch-created res (mvcc-current-rev ctx)))))
               (loop st)))
            ((eq? (car m) 'watch-cancel)
             (let ((reply-pid (cadr m)) (wid (caddr m)))
               (let ((ok (watch-cancel! watch-reg wid)))
                 (send reply-pid (cons 'watch-canceled (if ok wid #f))))
               (loop st)))

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
                    (for-each (lambda (kv) (kv-del! ctx (car kv)))
                              (kv-scan ctx (make-bytevector 0)))
                    (for-each (lambda (kv) (kv-put! ctx (car kv) (cdr kv))) rows)
                    (set-shard-ctx-crev! ctx -1)
                    (mvcc-live-stats-invalidate! ctx)   ; cw-xq9: bulk install bypassed mvcc-put!
                    (ctx-save-applied! ctx sbase 0)
                    (persist-boot! boot-epoch)   ; the wipe adopted the SENDER's counter
                    (ctx-flush! ctx)
                    (set! acc '())                 ; installed state supersedes
                    (if (> (reg-count watch-reg) 0)
                        (begin
                          (watch-on-apply! watch-reg ctx pre (mvcc-current-rev ctx))
                          (watch-check-compaction! watch-reg ctx)))
                    (let ((st2 (qp-install-snapshot st sbase 0 '())))
                      (publish! st2)
                      (loop st2))))
                 (else (loop st)))))

            ;; ---- local client command: (conn-pid . cmd), batched drain ----
            (else
             (let collect ((items (list (cons (car m) (cdr m)))) (n 1))
               (let ((nxt (if (< n PROPOSE-BATCH-CAP) (raw-receive 0) '*timeout*)))
                 (cond
                   ((and (pair? nxt) (not (symbol? (car nxt))))
                    (collect (cons nxt items) (+ n 1)))
                   (else
                    ; re-enqueue a non-write frame to SELF (mailbox order shifts
                    ; by one batch; every handler is order-tolerant)
                    (if (not (eq? nxt '*timeout*)) (send (self) nxt))
                    (loop (propose-client! st (reverse items))))))))))))))
