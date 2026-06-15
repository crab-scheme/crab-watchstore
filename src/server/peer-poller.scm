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
(define (peer-poller node-name shard-keys tick-every dial-addrs target)
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
  (let ((tick-secs (/ tick-every 1000.0)))
    (let loop ((last (current-second)))
      (let ((msgs (node-poll (symbol->string node-name))))
        (for-each route! msgs)
        (let ((now (current-second)))
          (cond
            ((>= (- now last) tick-secs) (tick-all!) (heal!) (loop now))
            ((null? msgs) (sleep-ms 5) (loop last))
            (else (loop last))))))))
