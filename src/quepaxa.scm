; quepaxa.scm — QuePaxa consensus engine (Tennage et al., SOSP'23) in CrabScheme.
;
; Leaderless WAN consensus: normal case = 1-RTT coordinator fast path (Multi-
; Paxos-equivalent latency); fallback = a randomized asynchronous core with NO
; timeouts — hedging delays are a latency knob only and can never cost liveness.
; This replaces (per shard-group, opt-in) raft.scm's timeout-driven elections,
; which thrash under WAN jitter (the "tyranny of timeouts").
;
; Same purity contract as raft.scm: every transition is
;   (node, input) -> (node' . outputs)
; where outputs is a list of (peer . message). No clocks, sockets, or mutation.
; Randomness is an explicit LCG seed threaded through the node (deterministic
; replay in tests). Faithful to the reference implementation (dedis/quepaxa):
;
;   Recorder (per slot): the "interval summary register" {S,F,A,M}.
;     record(s,p): S=s  -> A=max(A,p)
;                  S<s  -> (if S+1<s: A=bot) ; M=A ; F=p ; A=p ; S=s
;     reply (S F M).  F = first proposal of step S; M = aggregate of step S-1.
;   Proposer (per slot): proposal P=(prio pid val), steps S=4,5,6,... in rounds
;     of 4 phases (S mod 4). Broadcast record(S,P) to ALL replicas (self incl.),
;     await a majority of replies:
;       any reply S'>S            -> adopt (S,P) <- (S',F') of max-S' reply
;       phase 0: all F'.prio = HI -> DECIDE F'.val (coordinator 1-RTT fast path)
;                else P <- max F'
;       phase 1: (spread)
;       phase 2: max M' = P       -> DECIDE P.val
;       phase 3: P <- max M'
;     then S <- S+1. Entering a randomized phase 0 (S>4, or S=4 when not the
;     coordinator) each replica is sent an independent random priority in
;     [1, HI-10]; only the coordinator's first attempt uses HI. INVARIANT: at
;     most one proposer per slot ever uses HI, or the fast path is unsafe.
;
; SMR layering (slots = log): a slot's value is (bid batch) where bid=(pid seq)
; and batch is a list of commands, or '() for a gap-filling no-op. `applied`
; advances over the contiguous decided prefix; bid-dedup makes application
; exactly-once even when a hedged copy and the coordinator's copy both decide
; (the cc-cri lesson: never re-apply a non-idempotent batch).
; Hedging (Q3): a non-coordinator holds its batch for `hedge` ticks while
; forwarding to the coordinator (pfwd); if the bid is not applied in time it
; proposes the batch itself. A proposer whose value LOSES a slot re-proposes at
; a fresh slot until its bid applies.
;
; Messages (node-send-safe: symbols/numbers/#f/nested lists/bytevectors):
;   (esp slot s P)             record request; P = (prio pid val)
;   (espr slot req-s S F M)    reply; F/M = proposal or #f (bot)
;   (decd slot val)            decision gossip / fetch reply
;   (fetch slot)               gap-fill request for a decided value
;   (pfwd val)                 forward a value to the coordinator
;
; ponytail: decided log is not yet pruned and lagging replicas catch up via
; decd/fetch only — compaction + ws-snap store-snapshot catch-up is Q4 (cw-cec).

; ---- helpers (qp- prefixed: raft.scm may be co-included) ----
(define (qp-nget st k) (cdr (assq k st)))
(define (qp-nset st k v)
  (cond ((null? st) (list (cons k v)))
        ((eq? (caar st) k) (cons (cons k v) (cdr st)))
        (else (cons (car st) (qp-nset (cdr st) k v)))))
(define (qp-nset* st kvs)
  (if (null? kvs) st (qp-nset* (qp-nset st (car kvs) (cadr kvs)) (cddr kvs))))
(define (qp-sget al k) (let ((e (assv k al))) (if e (cdr e) #f)))
(define (qp-sset al k v)
  (cond ((null? al) (list (cons k v)))
        ((eqv? (caar al) k) (cons (cons k v) (cdr al)))
        (else (cons (car al) (qp-sset (cdr al) k v)))))
(define (qp-sdel al k)
  (cond ((null? al) '())
        ((eqv? (caar al) k) (cdr al))
        (else (cons (car al) (qp-sdel (cdr al) k)))))
(define (qp-others id ids)
  (cond ((null? ids) '())
        ((eqv? (car ids) id) (qp-others id (cdr ids)))
        (else (cons (car ids) (qp-others id (cdr ids))))))
(define (qp-take-n lst n)
  (if (or (<= n 0) (null? lst)) '()
      (cons (car lst) (qp-take-n (cdr lst) (- n 1)))))

; total order over replica ids (numbers in tests, symbols/strings in prod)
(define (qp->str x)
  (cond ((string? x) x)
        ((symbol? x) (symbol->string x))
        ((number? x) (number->string x))
        (else "?")))
(define (qp-id>? a b)
  (if (and (number? a) (number? b)) (> a b) (string>? (qp->str a) (qp->str b))))

; ---- proposals: (prio pid val); bot = #f ----
(define (qp-p-prio p) (car p))
(define (qp-p-pid p) (cadr p))
(define (qp-p-val p) (caddr p))
(define (qp-p>? a b)
  (or (> (car a) (car b))
      (and (= (car a) (car b)) (qp-id>? (cadr a) (cadr b)))))
(define (qp-pmax a b)
  (cond ((not a) b) ((not b) a) ((qp-p>? b a) b) (else a)))
(define (qp-pmax-of replies sel)
  (let loop ((l replies) (m #f))
    (if (null? l) m (loop (cdr l) (qp-pmax m (sel (car l)))))))

; ---- deterministic PRNG (LCG), threaded through the node ----
(define (qp-rng-next s) (modulo (+ (* s 1103515245) 12345) 2147483648))
(define (qp-draw st k)                       ; -> ([1,k] . st')
  (let ((s (qp-rng-next (qp-nget st 'rng))))
    (cons (+ 1 (modulo s k)) (qp-nset st 'rng s))))

(define QP-GAP-FETCH 2)   ; gap ticks before asking peers for the decided value
(define QP-GAP-NOOP 6)    ; gap ticks before filling the slot with a no-op
; ponytail: bounded exactly-once window; retries land within ticks, and Q4's
; compaction floor will make anything older a snapshot catch-up anyway.
(define QP-BIDS-KEEP 256)

; ============================================================
; node construction + accessors
; ============================================================

; opts (optional alist): (coord . id) (hi . n) (hedge . ticks) (seed . n)
(define (make-qp id ids apply-fn sm0 . rest)
  (let* ((o (if (pair? rest) (car rest) '()))
         (getopt (lambda (k d) (let ((e (assq k o))) (if e (cdr e) d)))))
    (list (cons 'id id) (cons 'all ids) (cons 'peers (qp-others id ids))
          (cons 'maj (+ 1 (quotient (length ids) 2)))
          (cons 'hi (getopt 'hi 1000000))
          (cons 'coord (getopt 'coord (car ids)))
          (cons 'hedge (getopt 'hedge 3))
          ; boot epoch: MUST be strictly increasing across restarts of the same
          ; node (the driver persists it). It is part of every bid, so a
          ; restarted node's fresh seq counter can never collide with its
          ; pre-crash bids — a collision makes peers' exactly-once dedup window
          ; silently SKIP applying the new batch (found by the WAN kill soak).
          (cons 'boot (getopt 'boot 0))
          (cons 'apply apply-fn) (cons 'sm sm0)
          (cons 'rng (getopt 'seed 42))
          (cons 'rec '()) (cons 'props '()) (cons 'decided '())
          (cons 'applied 0) (cons 'bids '()) (cons 'holds '())
          (cons 'mine '()) (cons 'next-slot 1) (cons 'gapt 0) (cons 'seq 0)
          ; base = compaction floor: slots <= base are applied AND pruned; a
          ; peer asking below it can only be caught up by a STORE snapshot
          ; (ws-snap, driver's job — mirrors raft's base/snap-req contract).
          ; snap-need = (peer . peer-base) when WE are the lagging one.
          (cons 'base 0) (cons 'snap-need #f)
          ; linearizable reads (Q5): pending bid->tag, done tag list (FIFO).
          (cons 'reads '()) (cons 'rdone '())
          ; applied-batch log for the driver's write-ack bridge (Q6): (bid . ncmds)
          ; appended (newest first) the FIRST time a bid's batch applies.
          (cons 'adone '()))))

(define (qp-id st)      (qp-nget st 'id))
(define (qp-applied st) (qp-nget st 'applied))
(define (qp-commit st)  (qp-nget st 'applied))   ; contiguous decided = committed
(define (qp-sm st)      (qp-nget st 'sm))
(define (qp-coord st)   (qp-nget st 'coord))
(define (qp-coord? st)  (eqv? (qp-nget st 'id) (qp-nget st 'coord)))
; replicated coordinator reassignment (applied identically on every replica).
; Safe with the strict unanimous-F fast path even during the handoff window.
(define (qp-set-coord st id) (qp-nset st 'coord id))
(define (qp-decided-val st slot) (qp-sget (qp-nget st 'decided) slot))
(define (qp-base st)      (qp-nget st 'base))
(define (qp-snap-need st) (qp-nget st 'snap-need))
(define (qp-clear-snap-need st) (qp-nset st 'snap-need #f))

; ---- compaction + snapshot install (Q4, cw-cec) ----
; Prune decided slots <= floor (their effects live in the store, which IS the
; snapshot). No-op unless base < floor <= applied. NOTE: the applied-bid dedup
; window ('bids) is in-memory; a snapshot payload should carry it (the driver
; ships it with ws-snap) so a rejoiner cannot re-apply a pre-snapshot batch —
; same lesson as crab-cache's atomic applied-index (cc-cri).
(define (qp-compact-to st floor)
  (if (or (<= floor (qp-nget st 'base)) (> floor (qp-nget st 'applied)))
      st
      (qp-nset* st (list
        'base floor
        'decided (let loop ((l (qp-nget st 'decided)) (acc '()))
                   (cond ((null? l) (reverse acc))
                         ((<= (caar l) floor) (loop (cdr l) acc))
                         (else (loop (cdr l) (cons (car l) acc)))))))))

; adopt a store snapshot at `base` (the driver has already replaced the
; persistent store contents and passes the matching sm + applied-bid window).
(define (qp-install-snapshot st base sm bids)
  (let ((drop<= (lambda (al) (let loop ((l al) (acc '()))
                               (cond ((null? l) (reverse acc))
                                     ((<= (caar l) base) (loop (cdr l) acc))
                                     (else (loop (cdr l) (cons (car l) acc))))))))
    (qp-apply-prefix                       ; decided slots above base we already
     (qp-nset* st (list                    ; heard about apply immediately
       'base base 'applied base 'sm sm 'bids bids 'snap-need #f 'gapt 0
       'decided (drop<= (qp-nget st 'decided))
       'rec (drop<= (qp-nget st 'rec))
       'props (drop<= (qp-nget st 'props))
       'mine (drop<= (qp-nget st 'mine))
       'next-slot (max (qp-nget st 'next-slot) (+ base 1)))))))

; ============================================================
; recorder (interval summary register)
; ============================================================

; entry (S F A M) -> (entry' . (S F M) reply payload)
(define (qp-rec-record e s p)
  (let ((S (car e)) (F (cadr e)) (A (caddr e)) (M (cadddr e)))
    (cond
      ((= S s)
       (cons (list S F (qp-pmax A p) M) (list S F M)))
      ((< S s)
       (let ((M2 (if (< (+ S 1) s) #f A)))   ; jumped >1 step: prev-step aggregate unknown
         (cons (list s p p M2) (list s p M2))))
      (else (cons e (list S F M))))))

(define (qp-on-esp st from msg)
  (let* ((slot (list-ref msg 1)) (s (list-ref msg 2)) (p (list-ref msg 3))
         (dv (qp-sget (qp-nget st 'decided) slot)))
    (cond
      ; slot compacted: the decided value is gone — never re-run consensus for
      ; an applied slot; the asker needs a store snapshot.
      ((<= slot (qp-nget st 'base))
       (cons st (list (cons from (list 'snapo (qp-nget st 'base))))))
      (dv
        (cons st (list (cons from (list 'decd slot dv)))))
      (else
        (let* ((rec (qp-nget st 'rec))
               (e (let ((x (qp-sget rec slot))) (if x x (list 0 #f #f #f))))
               (r (qp-rec-record e s p))
               (st (qp-nset st 'rec (qp-sset rec slot (car r))))
               ; seeing traffic for slot k means k is taken: propose above it
               (st (if (>= slot (qp-nget st 'next-slot))
                       (qp-nset st 'next-slot (+ slot 1)) st)))
          (cons st (list (cons from (cons 'espr (cons slot (cons s (cdr r))))))))))))

; ============================================================
; proposer
; ============================================================

; (re)enter the current phase of `slot`: pick per-replica priorities (random on
; a randomized phase 0), store them for retransmission, broadcast, and process
; our OWN recorder inline (self counts toward the majority; a solo node decides
; right here with no outputs).
(define (qp-send-phase st slot)
  (let* ((prop (qp-sget (qp-nget st 'props) slot))
         (s (qp-nget prop 's)) (p (qp-nget prop 'p))
         (rand? (and (= 0 (modulo s 4)) (or (> s 4) (not (qp-coord? st)))))
         (r (let loop ((ids (qp-nget st 'all)) (st st) (acc '()))
              (if (null? ids) (cons st acc)
                  (if rand?
                      (let ((d (qp-draw st (- (qp-nget st 'hi) 10))))
                        (loop (cdr ids) (cdr d)
                              (cons (cons (car ids) (list (car d) (qp-p-pid p) (qp-p-val p)))
                                    acc)))
                      (loop (cdr ids) st (cons (cons (car ids) p) acc))))))
         (st (car r)) (pis (cdr r))
         (prop (qp-nset* prop (list 'pis pis 'got '())))
         (st (qp-nset st 'props (qp-sset (qp-nget st 'props) slot prop)))
         (self (qp-nget st 'id))
         (outs (map (lambda (pr) (cons pr (list 'esp slot s (cdr (assv pr pis)))))
                    (qp-nget st 'peers)))
         (r2 (qp-on-esp st self (list 'esp slot s (cdr (assv self pis)))))
         (st (car r2))
         (reply (cdr (car (cdr r2)))))
    (let ((r3 (if (eq? (car reply) 'espr) (qp-on-espr st self reply) (cons st '()))))
      (cons (car r3) (append outs (cdr r3))))))

(define (qp-on-espr st from msg)
  (let* ((slot (list-ref msg 1)) (reqs (list-ref msg 2))
         (prop (qp-sget (qp-nget st 'props) slot)))
    (if (or (not prop) (not (= reqs (qp-nget prop 's))))
        (cons st '())                                     ; stale phase / no attempt
        (let ((got (qp-nget prop 'got)))
          (if (assv from got)
              (cons st '())                               ; duplicate reply
              (let* ((got (cons (cons from (list (list-ref msg 3) (list-ref msg 4)
                                                 (list-ref msg 5)))
                               got))
                     (prop (qp-nset prop 'got got))
                     (st (qp-nset st 'props (qp-sset (qp-nget st 'props) slot prop))))
                (if (< (length got) (qp-nget st 'maj))
                    (cons st '())
                    (qp-process-majority st slot))))))))

; a majority of replies for the current step is in: decide, adopt, or advance.
(define (qp-process-majority st slot)
  (let* ((prop (qp-sget (qp-nget st 'props) slot))
         (s (qp-nget prop 's)) (p (qp-nget prop 'p))
         (replies (map cdr (qp-nget prop 'got)))          ; each (S F M)
         (all-same (let loop ((l replies))
                     (cond ((null? l) #t)
                           ((= (car (car l)) s) (loop (cdr l)))
                           (else #f)))))
    (if (not all-same)
        ; catch up: adopt (S,P) from the max-S' reply
        (let loop ((l replies) (bs s) (bp p))
          (if (pair? l)
              (let ((S (car (car l))) (F (cadr (car l))))
                (if (> S bs) (loop (cdr l) S (if F F bp)) (loop (cdr l) bs bp)))
              (qp-advance-to st slot bs bp)))
        (let ((ph (modulo s 4)))
          (cond
            ((= ph 0)
             ; Fast path — STRICTER than dedis/quepaxa's prio==HI check: require
             ; every F' to be the SAME hi-priority proposal. A recorder's F is
             ; write-once per step, so two overlapping majorities can never both
             ; be unanimous on different values — this keeps the fast path safe
             ; even if a coordinator handoff briefly leaves two HI proposers on
             ; one slot (the Go check is safe only under unique-HI-by-design).
             (let* ((hi (qp-nget st 'hi))
                    (f0 (cadr (car replies))))
               (if (and f0 (= (car f0) hi)
                        (let loop ((l (cdr replies)))
                          (cond ((null? l) #t)
                                ((equal? (cadr (car l)) f0) (loop (cdr l)))
                                (else #f))))
                   (qp-decide st slot (qp-p-val f0) #t)  ; fast path
                   (let ((bf (qp-pmax-of replies cadr)))
                     (qp-advance-to st slot (+ s 1) (if bf bf p))))))
            ((= ph 1) (qp-advance-to st slot (+ s 1) p))
            ((= ph 2)
             (let ((mm (qp-pmax-of replies caddr)))
               (if (and mm (equal? mm p))
                   (qp-decide st slot (qp-p-val p) #t)
                   (qp-advance-to st slot (+ s 1) p))))
            (else
             (let ((mm (qp-pmax-of replies caddr)))
               (qp-advance-to st slot (+ s 1) (if mm mm p)))))))))

(define (qp-advance-to st slot s p)
  (let* ((prop (qp-sget (qp-nget st 'props) slot))
         (prop (qp-nset* prop (list 's s 'p p 'got '())))
         (st (qp-nset st 'props (qp-sset (qp-nget st 'props) slot prop))))
    (qp-send-phase st slot)))

; ============================================================
; decide + apply (SMR)
; ============================================================

(define (qp-decide st slot val gossip?)
  (if (qp-sget (qp-nget st 'decided) slot)
      (cons st '())
      (let* ((st (qp-nset* st (list
                    'decided (qp-sset (qp-nget st 'decided) slot val)
                    'props (qp-sdel (qp-nget st 'props) slot)
                    'rec (qp-sdel (qp-nget st 'rec) slot)
                    'next-slot (max (qp-nget st 'next-slot) (+ slot 1))
                    'gapt 0)))
             (outs (if gossip?
                       (map (lambda (pr) (cons pr (list 'decd slot val)))
                            (qp-nget st 'peers))
                       '()))
             (r (qp-post-decide st slot val)))
        (cons (car r) (append outs (cdr r))))))

; after a decision lands: apply the contiguous prefix, and if OUR value lost
; this slot re-propose it at a fresh slot (unless its bid already applied).
(define (qp-post-decide st slot val)
  (let* ((mine (qp-nget st 'mine))
         (mv (qp-sget mine slot))
         (st (qp-nset st 'mine (qp-sdel mine slot)))
         (st (qp-apply-prefix st)))
    (if (and mv (not (equal? mv val)) (not (qp-val-applied? st mv)))
        (qp-start-slot st mv)
        (cons st '()))))

; cw-65x: a merged ('multi ...) value is applied iff every sub-bid applied
; (they always apply together, but check all for safety).
(define (qp-val-applied? st val)
  (if (eq? (car val) 'multi)
      (let loop ((vs (cdr val)))
        (cond ((null? vs) #t)
              ((qp-bid-applied? st (caar vs)) (loop (cdr vs)))
              (else #f)))
      (qp-bid-applied? st (car val))))

(define (qp-bid-applied? st bid)
  (let loop ((l (qp-nget st 'bids)))
    (cond ((null? l) #f) ((equal? (car l) bid) #t) (else (loop (cdr l))))))

(define (qp-hold-del holds bid)
  (cond ((null? holds) '())
        ((equal? (caar holds) bid) (cdr holds))
        (else (cons (car holds) (qp-hold-del (cdr holds) bid)))))

(define (qp-apply-prefix st)
  (let loop ((st st))
    (let* ((next (+ 1 (qp-nget st 'applied)))
           (val (qp-sget (qp-nget st 'decided) next)))
      (if (not val) st
          (loop (qp-nset (qp-apply-val st val) 'applied next))))))

(define (qp-apply-val st val)
  (cond
    ((null? val) st)                          ; no-op gap filler
    ; cw-65x: merged slot — a coordinator coalesced several forwarded batches
    ; into ONE consensus slot. Apply each sub-batch under its own bid so
    ; exactly-once dedup, hold clearing, read completion and per-bid acks all
    ; behave exactly as if each had won its own slot.
    ((eq? (car val) 'multi)
     (let loop ((vs (cdr val)) (st st))
       (if (null? vs) st (loop (cdr vs) (qp-apply-val st (car vs))))))
    (else (qp-apply-one st val))))

(define (qp-apply-one st val)
  (let ((bid (car val)) (cmds (cadr val)))
        (if (qp-bid-applied? st bid)
            st                                ; exactly-once: hedged duplicate
            (let* ((sm (let loop ((c cmds) (sm (qp-nget st 'sm)))
                         (if (null? c) sm
                             (loop (cdr c)
                                   (if (null? (car c)) sm
                                       ((qp-nget st 'apply) sm (car c)))))))
                   ; a completed read: move its tag to the done queue
                   (rd (assoc bid (qp-nget st 'reads))))
              (qp-nset* st (list
                'sm sm
                'bids (cons bid (qp-take-n (qp-nget st 'bids) (- QP-BIDS-KEEP 1)))
                'holds (qp-hold-del (qp-nget st 'holds) bid)
                'reads (if rd (qp-hold-del (qp-nget st 'reads) bid) (qp-nget st 'reads))
                'rdone (if rd (cons (cdr rd) (qp-nget st 'rdone)) (qp-nget st 'rdone))
                'adone (cons (cons bid (length cmds)) (qp-nget st 'adone))))))))

; ============================================================
; public API: propose / step / tick
; ============================================================

(define (qp-start-slot st val)
  (let* ((slot (qp-nget st 'next-slot))
         (st (qp-nset st 'next-slot (+ slot 1)))
         (st (if (null? val) st
                 (qp-nset st 'mine (qp-sset (qp-nget st 'mine) slot val))))
         (prop (list (cons 's 4)
                     (cons 'p (list (qp-nget st 'hi) (qp-nget st 'id) val))
                     (cons 'pis '()) (cons 'got '())))
         (st (qp-nset st 'props (qp-sset (qp-nget st 'props) slot prop))))
    (qp-send-phase st slot)))

(define (qp-start-noop-at st slot)            ; gap fill at an explicit slot
  (let* ((prop (list (cons 's 4)
                     (cons 'p (list (qp-nget st 'hi) (qp-nget st 'id) '()))
                     (cons 'pis '()) (cons 'got '())))
         (st (qp-nset st 'props (qp-sset (qp-nget st 'props) slot prop))))
    (qp-send-phase st slot)))

(define (qp-propose-batch st cmds)
  (if (null? cmds) (cons st '())
      (let* ((seq (+ 1 (qp-nget st 'seq)))
             (bid (list (qp-nget st 'id) (qp-nget st 'boot) seq))
             (val (list bid cmds))
             (st (qp-nset st 'seq seq)))
        (if (or (qp-coord? st) (<= (qp-nget st 'hedge) 0))
            (qp-start-slot st val)
            ; hedge: hold the batch, forward to the coordinator; the tick fires
            ; a self-propose if the bid hasn't applied within `hedge` ticks.
            (let ((st (qp-nset st 'holds
                        (cons (cons bid (cons val (qp-nget st 'hedge)))
                              (qp-nget st 'holds)))))
              (cons st (list (cons (qp-nget st 'coord) (list 'pfwd val)))))))))

(define (qp-propose st cmd) (qp-propose-batch st (list cmd)))

(define (qp-on-decd st from msg)
  (qp-decide st (list-ref msg 1) (list-ref msg 2) #f))

(define QP-FETCH-SPAN 32)   ; decided slots served per fetch (rejoin catch-up)
(define (qp-on-fetch st from msg)
  (let ((slot (list-ref msg 1)))
    (if (<= slot (qp-nget st 'base))
        (cons st (list (cons from (list 'snapo (qp-nget st 'base)))))
        ; reply a contiguous RANGE of decided slots from `slot` so a rejoining
        ; node catches up in one round instead of one-slot-per-tick.
        (let loop ((i slot) (outs '()))
          (let ((dv (and (< (- i slot) QP-FETCH-SPAN)
                         (qp-sget (qp-nget st 'decided) i))))
            (if dv
                (loop (+ i 1) (cons (cons from (list 'decd i dv)) outs))
                (cons st (reverse outs))))))))

; a peer told us the slot we need is below its compaction floor: surface the
; snapshot need to the driver (it ships/installs the store snapshot, then
; calls qp-install-snapshot).
(define (qp-on-snapo st from msg)
  (let ((pbase (list-ref msg 1)))
    (if (> pbase (qp-nget st 'applied))
        (cons (qp-nset st 'snap-need (cons from pbase)) '())
        (cons st '()))))

(define (qp-step st from msg)
  (case (car msg)
    ((esp)   (qp-on-esp st from msg))
    ((espr)  (qp-on-espr st from msg))
    ((decd)  (qp-on-decd st from msg))
    ((fetch) (qp-on-fetch st from msg))
    ((snapo) (qp-on-snapo st from msg))
    ((pfwd)  (qp-start-slot st (list-ref msg 1)))   ; coordinator adopts + retries
    (else    (cons st '()))))

; ---- linearizable reads (Q5, cw-h4z): a consistent read is an empty batch
;      through consensus (Meerkat-style read-through-log — no leader, so no
;      ReadIndex). When its bid applies, every write decided before it has
;      been applied; the driver drains qp-take-reads and serves at the then-
;      current store rev. ----
(define (qp-read st tag)
  (let* ((seq (+ 1 (qp-nget st 'seq)))
         (bid (list (qp-nget st 'id) (qp-nget st 'boot) seq))
         (val (list bid '()))
         (st (qp-nset st 'seq seq))
         (st (qp-nset st 'reads (cons (cons bid tag) (qp-nget st 'reads)))))
    (if (or (qp-coord? st) (<= (qp-nget st 'hedge) 0))
        (qp-start-slot st val)
        (let ((st (qp-nset st 'holds
                    (cons (cons bid (cons val (qp-nget st 'hedge)))
                          (qp-nget st 'holds)))))
          (cons st (list (cons (qp-nget st 'coord) (list 'pfwd val))))))))

; -> (done-tags-oldest-first . st')
(define (qp-take-reads st)
  (cons (reverse (qp-nget st 'rdone)) (qp-nset st 'rdone '())))

 ; -> (((bid . ncmds) ...) oldest-first . st') — batches applied since last take
(define (qp-take-applied st)
  (cons (reverse (qp-nget st 'adone)) (qp-nset st 'adone '())))

; the bid the NEXT qp-propose-batch/qp-read on this node will use
(define (qp-next-bid st)
  (list (qp-nget st 'id) (qp-nget st 'boot) (+ 1 (qp-nget st 'seq))))

; ---- tick: hedge countdowns, retransmission, gap fill. No timeouts: none of
;      this affects safety, and liveness needs only that ticks keep coming. ----

(define (qp-tick-holds st)
  (let loop ((hs (qp-nget st 'holds)) (kept '()) (st st) (outs '()))
    (if (null? hs)
        (cons (qp-nset st 'holds (reverse kept)) outs)
        (let* ((h (car hs)) (val (cadr h)) (t (cddr h)))
          (if (<= t 1)
              (let ((r (qp-start-slot st val)))       ; hedge fires: self-propose
                (loop (cdr hs) kept (car r) (append outs (cdr r))))
              (loop (cdr hs) (cons (cons (car h) (cons val (- t 1))) kept) st outs))))))

(define (qp-retransmits st)
  (let loop ((ps (qp-nget st 'props)) (outs '()))
    (if (null? ps) outs
        (let* ((slot (caar ps)) (prop (cdar ps))
               (s (qp-nget prop 's)) (pis (qp-nget prop 'pis))
               (got (qp-nget prop 'got)))
          (loop (cdr ps)
                (append outs
                        (let inner ((pr (qp-nget st 'peers)) (acc '()))
                          (cond ((null? pr) acc)
                                ((assv (car pr) got) (inner (cdr pr) acc))
                                (else
                                 (inner (cdr pr)
                                        (cons (cons (car pr)
                                                    (list 'esp slot s (cdr (assv (car pr) pis))))
                                              acc)))))))))))

(define QP-AE-TICKS 8)      ; idle anti-entropy probe period (rejoin catch-up)
(define (qp-tick-gap st)
  (let ((next (+ 1 (qp-nget st 'applied))))
    (if (and (< next (qp-nget st 'next-slot))
             (not (qp-sget (qp-nget st 'decided) next))
             (not (qp-sget (qp-nget st 'props) next)))
        (let* ((g (+ 1 (qp-nget st 'gapt)))
               (st (qp-nset st 'gapt g)))
          (cond
            ((= g QP-GAP-FETCH)
             (cons st (map (lambda (p) (cons p (list 'fetch next))) (qp-nget st 'peers))))
            ((>= g QP-GAP-NOOP)
             (qp-start-noop-at (qp-nset st 'gapt 0) next))
            (else (cons st '()))))
        ; no LOCAL evidence of a gap — but a restarted node (applied=P,
        ; next-slot=P+1) can be silently behind a cluster that moved on while
        ; it was dead. Probe peers periodically; they answer decd only if they
        ; actually decided our next slot (else silence). Found by the WAN kill
        ; soak: the restarted coordinator served stale reads forever.
        (let ((g (+ 1 (qp-nget st 'gapt))))
          (if (>= g QP-AE-TICKS)
              (cons (qp-nset st 'gapt 0)
                    (map (lambda (p) (cons p (list 'fetch next))) (qp-nget st 'peers)))
              (cons (qp-nset st 'gapt g) '()))))))

(define (qp-tick st)
  (let* ((r1 (qp-tick-holds st))
         (outs2 (qp-retransmits (car r1)))
         (r3 (qp-tick-gap (car r1))))
    (cons (car r3) (append (cdr r1) outs2 (cdr r3)))))
