; server/peer-poller.scm — one per node. The ONLY actor that calls node-poll
; for this node: it drains inbound frames and fans each Raft RPC to the right
; LOCAL shard-replica's mailbox, and emits periodic ticks (heartbeat for a
; leader, election clock for a follower). Replicas never poll the network —
; they're mailbox-driven — so frames are never split between pollers.
;
; PORTED from crab-cache/src/server/peer-poller.scm. The Raft RPC frame tag is
; renamed `shard-engine` -> `ws-engine` (matching shard-actor's emit!), and the
; cross-node PUBLISH / broker routing is DROPPED (no pub/sub in watchstore).
; The inbound-frame drain + tag-demux + route-to-local-actor logic is intact.
;
;   (spawn-source "(include \"src/server/peer-poller.scm\")" 'peer-poller
;                 NODE-NAME SHARD-KEYS TICK-EVERY DIAL-ADDRS TARGET)
;   SHARD-KEYS : list of shard-key strings whose replicas live on this node
;   TICK-EVERY : emit a tick after this many idle poll iterations. NOTE: the idle
;                branch must stay (yield), NOT (sleep-ms). This loop is two things
;                at once: the Raft tick clock (ticks pace heartbeats/elections,
;                counted in idle iterations) AND this node's sole inbound-frame
;                drainer. Any real sleep slows tick emission + frame delivery,
;                stretching the propose->replicate window. (This actor is a
;                dedicated-thread spawn-source actor, so it's a protocol-timing
;                issue, not worker starvation.)

; dial-addrs : raft addresses of higher-named peers to (re)dial
; target     : expected peer count (= #nodes - 1); when we have fewer, a peer has
;              gone (or a restarted peer is coming back) so we re-dial, healing
;              the mesh. node-connect is keyed by peer name, so re-dialing a live
;              peer is a harmless replace.
; cw-gx4: CHANNEL (optional, in `rest`) — when given, this is a PER-GROUP poller
; that drains only its group's cs-net channel via node-poll-ch, so independent
; groups drain on separate dedicated threads in parallel instead of serializing
; one node-poll over Messages. When absent, it's the legacy single-node poller
; (node-poll over Messages, routes ALL groups). heal! runs on the "0"-owning
; poller only, to avoid N threads racing node-connect.
(define (peer-poller node-name shard-keys tick-every dial-addrs target . rest)
  (define channel (if (and (pair? rest) (number? (car rest))) (car rest) #f))
  (define heal-owner? (if channel (member "0" shard-keys) #t))
  ; cw-xq9: the per-group path BLOCKS (node-poll-ch-wait) up to wait-ms for
  ; inbound traffic instead of sleep-polling — mesh hop latency becomes
  ; delivery latency, not polling granularity. Safe: this actor is a
  ; spawn-source-dedicated thread. The legacy all-channel path keeps the
  ; non-blocking poll + adaptive sleep.
  (define (poll-msgs wait-ms)
    (if channel
        (node-poll-ch-wait (symbol->string node-name) channel wait-ms)
        (node-poll (symbol->string node-name))))
  (define (local-pid sk)
    (table-lookup 'ws-shard-pid (string-append (symbol->string node-name) ":" sk)))
  (define (heal!)
    (node-detect-disconnects (symbol->string node-name))   ; prune dead peers first
    (if (< (node-peer-count (symbol->string node-name)) target)
        (for-each (lambda (a) (guard (e (#t #f)) (node-connect (symbol->string node-name) a)))
                  dial-addrs)))
  ; route an inbound frame: a Raft RPC to its local shard replica.
  (define (route! frame)
    (cond
      ((not (pair? frame)) #f)
      ((eq? (car frame) 'ws-engine)
       (let ((pid (local-pid (cadr frame))))
         (if pid (send pid (list 'engine (caddr frame) (cadddr frame))))))
      ; forwarded linearizable reads (cw-lkq.13): request -> the local shard
      ; (leader side); reply -> the local shard (origin side relays to its conn).
      ((eq? (car frame) 'ws-fwd-write)
       (let ((pid (local-pid (cadr frame))))
         (if pid (send pid (list 'fwd-write (caddr frame) (cadddr frame) (car (cddddr frame)))))))
      ((eq? (car frame) 'ws-fwd)
       (let ((pid (local-pid (cadr frame))))
         (if pid (send pid (list 'fwd-range (caddr frame) (cadddr frame) (car (cddddr frame)))))))
      ((eq? (car frame) 'ws-fwd-reply)
       (let ((pid (local-pid (cadr frame))))
         (if pid (send pid (list 'fwd-reply (caddr frame) (cadddr frame) (car (cddddr frame)))))))
      ; store-snapshot catch-up (cw-lkq.15): leader -> below-floor follower
      ((eq? (car frame) 'ws-snap)
       (let ((pid (local-pid (cadr frame))))
         (if pid (send pid (list 'snap-install (caddr frame) (cadddr frame))))))))
  (define (tick-all!)
    (for-each (lambda (sk) (let ((p (local-pid sk))) (if p (send p (list 'tick)))))
              shard-keys))
  ; TICK PACING (cw-b5w.7): WALL-CLOCK, not iteration-count. The original
  ; "tick after TICK-EVERY idle iterations" implicitly assumed (yield) costs
  ; ~1ms; on current crabscheme yield is essentially free, so the tick clock
  ; ran ~100x fast (measured ~700-1000 ticks/s), the 4-tick election timeout
  ; became ~5ms, and idle clusters churned elections continuously (terms
  ; +2-4/s). TICK-EVERY is now the tick interval in MILLISECONDS (callers
  ; already pass 120). The drain/yield structure is unchanged — the loop still
  ; spins draining frames; only tick emission is time-gated.
  ; IDLE PARK (cw-lkq scale): the idle branch used to (yield) and immediately
  ; re-poll. On current crabscheme yield is ~free, so an idle node spun this
  ; dedicated thread at ~100% CPU (~2.5 cores/member measured). With a real
  ; k8s control plane (many small lease/watch ops) and a 9-member region×AZ
  ; topology that oversubscribed the host and stalled op handling. Tick pacing
  ; is already WALL-CLOCK (cw-b5w.7), so a short idle sleep does NOT change tick
  ; timing — it only adds <=1ms to inbound-frame drain latency, negligible vs
  ; the 20-150ms inter-region RTT. The sleep is taken ONLY when there is nothing
  ; to drain (null? msgs); under active Raft replication msgs is non-empty and
  ; the loop stays hot, so throughput is unaffected. (Dedicated-thread actor =>
  ; sleep-ms is a real thread sleep that releases the core.)
  ; HOP LATENCY (cw-xq9): sleep-polling makes the sleep granularity the mesh
  ; hop latency for low-concurrency traffic — a sequential writer (k8s
  ; bootstrap) hits a sleeping poller on EVERY consensus hop (measured 12.9ms
  ; consensus round on loopback at 5ms sleeps, CWS_PROF). Per-group pollers now
  ; BLOCK in node-poll-ch-wait until inbound traffic or the next tick deadline,
  ; so frames route immediately and ticks stay wall-clock-paced. The legacy
  ; all-channel path keeps the adaptive 1ms/5ms sleep.
  (let ((tick-secs (/ tick-every 1000.0)))
    (let loop ((last (current-second)) (busy 0))
      (let* ((wait-ms (if channel
                          (let ((remain (- (+ last tick-secs) (current-second))))
                            (if (> remain 0) (exact (ceiling (* 1000 remain))) 0))
                          0))
             (msgs (poll-msgs wait-ms)))
        (for-each route! msgs)
        (let ((now (current-second)))
          (cond
            ((>= (- now last) tick-secs) (tick-all!) (if heal-owner? (heal!))
             (loop now (if (pair? msgs) now busy)))
            ((and (not channel) (null? msgs))
             (sleep-ms (if (< (- now busy) 0.05) 1 5)) (loop last busy))
            (else (loop last (if (pair? msgs) now busy)))))))))
