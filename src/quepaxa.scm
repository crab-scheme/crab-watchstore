; quepaxa.scm — QuePaxa consensus engine (Tennage et al., SOSP'23) in CrabScheme.
;
; Leaderless WAN consensus: normal case = 1-RTT coordinator fast path (Multi-
; Paxos-equivalent latency); fallback = a randomized asynchronous core with NO
; timeouts — hedging delays are a latency knob only and can never cost liveness.
; This replaces (per shard-group, opt-in) raft.scm's timeout-driven elections,
; which thrash under WAN jitter (the "tyranny of timeouts").
;
; Transition contract (cw-97b): every transition is
;   (node, input) -> (node . outputs)
; where outputs is a list of (peer . message). The node is a MUTABLE record
; mutated in place — the returned node is the same object, kept in the return
; shape so drivers (which thread st linearly) are unchanged. This replaced the
; original persistent-alist state after profiling (cw-2au dig) showed ~95% of
; shard CPU under write load was qp-sset/qp-nset alist rebuilding, not the
; state machine. Slot maps (rec/props/decided/mine) are eqv-hashtables; the
; applied-bid dedup window is an equal-hashtable set + eviction ring. No
; clocks or sockets; randomness is an explicit LCG seed in the node
; (deterministic replay in tests). Faithful to dedis/quepaxa:
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

; ---- deterministic PRNG (LCG) held in the node ----
(define (qp-rng-next s) (modulo (+ (* s 1103515245) 12345) 2147483648))
(define (qp-draw! st k)                      ; -> [1,k], advances the node seed
  (let ((s (qp-rng-next (qpn-rng st))))
    (set-qpn-rng! st s)
    (+ 1 (modulo s k))))

(define QP-GAP-FETCH 2)   ; gap ticks before asking peers for the decided value
(define QP-GAP-NOOP 6)    ; gap ticks before filling the slot with a no-op

; ============================================================
; node construction + accessors
; ============================================================

(define-record-type qpn
  (fields (immutable id)
          (immutable all)
          (immutable peers)
          (immutable maj)
          (immutable hi)
          (immutable hedge)
          ; boot epoch: MUST be strictly increasing across restarts of the same
          ; node (the driver persists it). It is part of every bid, so a
          ; restarted node's fresh seq counter can never collide with its
          ; pre-crash bids — a collision makes peers' exactly-once dedup window
          ; silently SKIP applying the new batch (found by the WAN kill soak).
          (immutable boot)
          (immutable apply-fn)
          (mutable coord)
          (mutable sm)
          (mutable rng)
          (mutable rec)          ; eqv-ht slot -> recorder entry (S F A M)
          (mutable props)        ; eqv-ht slot -> qprop
          (mutable decided)      ; eqv-ht slot -> val
          (mutable mine)         ; eqv-ht slot -> our proposed val
          (mutable applied)
          (mutable next-slot)
          (mutable gapt)
          (mutable seq)
          ; base = compaction floor: slots <= base are applied AND pruned; a
          ; peer asking below it can only be caught up by a STORE snapshot
          ; (ws-snap, driver's job — mirrors raft's base/snap-req contract).
          ; snap-need = (peer . peer-base) when WE are the lagging one.
          (mutable base)
          (mutable snap-need)
          (mutable holds)        ; ((bid . (val . ticks)) ...)
          ; linearizable reads (Q5): pending bid->tag, done tag list (FIFO).
          (mutable reads)
          (mutable rdone)
          ; applied-batch log for the driver's write-ack bridge (Q6): (bid . ncmds)
          ; appended (newest first) the FIRST time a bid's batch applies.
          (mutable adone)
          ; exactly-once dedup (cw-rz9): per-(origin . boot) applied-seq state,
          ; entry = (F . sparse) where F is the contiguous floor (every seq
          ; <= F applied) and sparse lists applied seqs > F. COMPLETE — a
          ; hedged duplicate deciding arbitrarily many slots later is still
          ; deduped (the old 256-bid ring window overflowed under kill +
          ; partition: Elle duplicate-elements). Bounded: origins are the
          ; cluster nodes, seqs are per-origin monotone, and a hole below F
          ; only outlives its origin's death (new boot = fresh entry), so
          ; sparse is capped by that boot's in-flight count.
          (mutable seqs)))       ; equal-ht (origin . boot) -> (F . sparse)

; per-slot proposer attempt
(define-record-type qprop
  (fields (mutable s)            ; step
          (mutable p)            ; our proposal (prio pid val)
          (mutable pis)          ; per-replica priorities alist for this phase
          (mutable got)))        ; replies alist (from . (S F M))

; opts (optional alist): (coord . id) (hi . n) (hedge . ticks) (seed . n)
(define (make-qp id ids apply-fn sm0 . rest)
  (let* ((o (if (pair? rest) (car rest) '()))
         (getopt (lambda (k d) (let ((e (assq k o))) (if e (cdr e) d)))))
    (make-qpn id ids (qp-others id ids)
              (+ 1 (quotient (length ids) 2))
              (getopt 'hi 1000000)
              (getopt 'hedge 3)
              (getopt 'boot 0)
              apply-fn
              (getopt 'coord (car ids))
              sm0
              (getopt 'seed 42)
              (make-eqv-hashtable) (make-eqv-hashtable)
              (make-eqv-hashtable) (make-eqv-hashtable)
              0 1 0 0
              0 #f
              '() '() '() '()
              (make-hashtable equal-hash equal?))))

(define (qp-id st)      (qpn-id st))
(define (qp-applied st) (qpn-applied st))
(define (qp-commit st)  (qpn-applied st))   ; contiguous decided = committed
(define (qp-sm st)      (qpn-sm st))
(define (qp-coord st)   (qpn-coord st))
(define (qp-coord? st)  (eqv? (qpn-id st) (qpn-coord st)))
; replicated coordinator reassignment (applied identically on every replica).
; Safe with the strict unanimous-F fast path even during the handoff window.
(define (qp-set-coord st id) (set-qpn-coord! st id) st)
(define (qp-decided-val st slot) (hashtable-ref (qpn-decided st) slot #f))
(define (qp-base st)      (qpn-base st))
(define (qp-snap-need st) (qpn-snap-need st))
(define (qp-clear-snap-need st) (set-qpn-snap-need! st #f) st)

; ---- exactly-once dedup (cw-rz9): per-origin applied-seq tracking ----
(define (qp-del-num l n)
  (cond ((null? l) '())
        ((eqv? (car l) n) (cdr l))
        (else (cons (car l) (qp-del-num (cdr l) n)))))
(define (qp-bid-applied? st bid)
  (let ((e (hashtable-ref (qpn-seqs st) (cons (car bid) (cadr bid)) #f))
        (s (caddr bid)))
    (if e (or (<= s (car e)) (if (memv s (cdr e)) #t #f)) #f)))
(define (qp-bid-note! st bid)
  (let* ((ht (qpn-seqs st)) (k (cons (car bid) (cadr bid)))
         (e (let ((x (hashtable-ref ht k #f)))
              (if x x (let ((x (cons 0 '()))) (hashtable-set! ht k x) x))))
         (s (caddr bid)))
    (if (and (> s (car e)) (not (memv s (cdr e))))
        (begin
          (set-cdr! e (cons s (cdr e)))
          (let adv ()
            (let ((nx (+ 1 (car e))))
              (if (memv nx (cdr e))
                  (begin (set-car! e nx)
                         (set-cdr! e (qp-del-num (cdr e) nx))
                         (adv)))))))))
; driver persistence bridge: read/install one origin's state
(define (qp-seq-state st origin boot)        ; -> (F . sparse) or #f
  (hashtable-ref (qpn-seqs st) (cons origin boot) #f))
(define (qp-seq-install! st origin boot f sparse)
  (hashtable-set! (qpn-seqs st) (cons origin boot) (cons f sparse)))
(define (qp-seqs-reset! st entries)          ; entries = ((origin boot f . sparse) ...)
  (set-qpn-seqs! st (make-hashtable equal-hash equal?))
  (for-each (lambda (en)
              (qp-seq-install! st (car en) (cadr en) (caddr en) (cdddr en)))
            entries))

; drop hashtable keys <= floor
(define (qp-ht-prune! ht floor)
  (let loop ((ks (vector->list (hashtable-keys ht))))
    (if (pair? ks)
        (begin (if (<= (car ks) floor) (hashtable-delete! ht (car ks)))
               (loop (cdr ks))))))

; ---- compaction + snapshot install (Q4, cw-cec) ----
; Prune decided slots <= floor (their effects live in the store, which IS the
; snapshot). No-op unless base < floor <= applied. NOTE: the applied-bid dedup
; window is in-memory; a snapshot payload should carry it (the driver ships it
; with ws-snap) so a rejoiner cannot re-apply a pre-snapshot batch — same
; lesson as crab-cache's atomic applied-index (cc-cri).
(define (qp-compact-to st floor)
  (if (or (<= floor (qpn-base st)) (> floor (qpn-applied st)))
      st
      (begin
        (set-qpn-base! st floor)
        (qp-ht-prune! (qpn-decided st) floor)
        st)))

; adopt a store snapshot at `base` (the driver has already replaced the
; persistent store contents and passes the matching sm + per-origin
; applied-seq entries, each (origin boot f . sparse)).
(define (qp-install-snapshot st base sm seq-entries)
  (set-qpn-base! st base)
  (set-qpn-applied! st base)
  (set-qpn-sm! st sm)
  (set-qpn-snap-need! st #f)
  (set-qpn-gapt! st 0)
  (qp-seqs-reset! st seq-entries)
  (qp-ht-prune! (qpn-decided st) base)
  (qp-ht-prune! (qpn-rec st) base)
  (qp-ht-prune! (qpn-props st) base)
  (qp-ht-prune! (qpn-mine st) base)
  (set-qpn-next-slot! st (max (qpn-next-slot st) (+ base 1)))
  ; decided slots above base we already heard about apply immediately
  (qp-apply-prefix st))

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
         (dv (hashtable-ref (qpn-decided st) slot #f)))
    (cond
      ; slot compacted: the decided value is gone — never re-run consensus for
      ; an applied slot; the asker needs a store snapshot.
      ((<= slot (qpn-base st))
       (cons st (list (cons from (list 'snapo (qpn-base st))))))
      (dv
        (cons st (list (cons from (list 'decd slot dv)))))
      (else
        (let* ((rec (qpn-rec st))
               (e (let ((x (hashtable-ref rec slot #f))) (if x x (list 0 #f #f #f))))
               (r (qp-rec-record e s p)))
          (hashtable-set! rec slot (car r))
          ; seeing traffic for slot k means k is taken: propose above it
          (if (>= slot (qpn-next-slot st)) (set-qpn-next-slot! st (+ slot 1)))
          (cons st (list (cons from (cons 'espr (cons slot (cons s (cdr r))))))))))))

; ============================================================
; proposer
; ============================================================

; (re)enter the current phase of `slot`: pick per-replica priorities (random on
; a randomized phase 0), store them for retransmission, broadcast, and process
; our OWN recorder inline (self counts toward the majority; a solo node decides
; right here with no outputs).
(define (qp-send-phase st slot)
  (let* ((prop (hashtable-ref (qpn-props st) slot #f))
         (s (qprop-s prop)) (p (qprop-p prop))
         (rand? (and (= 0 (modulo s 4)) (or (> s 4) (not (qp-coord? st)))))
         (pis (map (lambda (id)
                     (cons id
                           (if rand?
                               (list (qp-draw! st (- (qpn-hi st) 10))
                                     (qp-p-pid p) (qp-p-val p))
                               p)))
                   (qpn-all st))))
    (set-qprop-pis! prop pis)
    (set-qprop-got! prop '())
    (let* ((self (qpn-id st))
           (outs (map (lambda (pr) (cons pr (list 'esp slot s (cdr (assv pr pis)))))
                      (qpn-peers st)))
           (r2 (qp-on-esp st self (list 'esp slot s (cdr (assv self pis)))))
           (reply (cdr (car (cdr r2)))))
      (let ((r3 (if (eq? (car reply) 'espr) (qp-on-espr st self reply) (cons st '()))))
        (cons st (append outs (cdr r3)))))))

(define (qp-on-espr st from msg)
  (let* ((slot (list-ref msg 1)) (reqs (list-ref msg 2))
         (prop (hashtable-ref (qpn-props st) slot #f)))
    (if (or (not prop) (not (= reqs (qprop-s prop))))
        (cons st '())                                     ; stale phase / no attempt
        (let ((got (qprop-got prop)))
          (if (assv from got)
              (cons st '())                               ; duplicate reply
              (let ((got (cons (cons from (list (list-ref msg 3) (list-ref msg 4)
                                                (list-ref msg 5)))
                               got)))
                (set-qprop-got! prop got)
                (if (< (length got) (qpn-maj st))
                    (cons st '())
                    (qp-process-majority st slot))))))))

; a majority of replies for the current step is in: decide, adopt, or advance.
(define (qp-process-majority st slot)
  (let* ((prop (hashtable-ref (qpn-props st) slot #f))
         (s (qprop-s prop)) (p (qprop-p prop))
         (replies (map cdr (qprop-got prop)))             ; each (S F M)
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
             (let* ((hi (qpn-hi st))
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
  (let ((prop (hashtable-ref (qpn-props st) slot #f)))
    (set-qprop-s! prop s)
    (set-qprop-p! prop p)
    (set-qprop-got! prop '())
    (qp-send-phase st slot)))

; ============================================================
; decide + apply (SMR)
; ============================================================

(define (qp-decide st slot val gossip?)
  (if (hashtable-ref (qpn-decided st) slot #f)
      (cons st '())
      (begin
        (hashtable-set! (qpn-decided st) slot val)
        (hashtable-delete! (qpn-props st) slot)
        (hashtable-delete! (qpn-rec st) slot)
        (set-qpn-next-slot! st (max (qpn-next-slot st) (+ slot 1)))
        (set-qpn-gapt! st 0)
        (let* ((outs (if gossip?
                         (map (lambda (pr) (cons pr (list 'decd slot val)))
                              (qpn-peers st))
                         '()))
               (r (qp-post-decide st slot val)))
          (cons st (append outs (cdr r)))))))

; after a decision lands: apply the contiguous prefix, and if OUR value lost
; this slot re-propose it at a fresh slot (unless its bid already applied).
(define (qp-post-decide st slot val)
  (let ((mv (hashtable-ref (qpn-mine st) slot #f)))
    (hashtable-delete! (qpn-mine st) slot)
    (qp-apply-prefix st)
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

(define (qp-hold-del holds bid)
  (cond ((null? holds) '())
        ((equal? (caar holds) bid) (cdr holds))
        (else (cons (car holds) (qp-hold-del (cdr holds) bid)))))

(define (qp-apply-prefix st)
  (let loop ()
    (let* ((next (+ 1 (qpn-applied st)))
           (val (hashtable-ref (qpn-decided st) next #f)))
      (if (not val) st
          (begin (qp-apply-val st val)
                 (set-qpn-applied! st next)
                 (loop))))))

(define (qp-apply-val st val)
  (cond
    ((null? val) st)                          ; no-op gap filler
    ; cw-65x: merged slot — a coordinator coalesced several forwarded batches
    ; into ONE consensus slot. Apply each sub-batch under its own bid so
    ; exactly-once dedup, hold clearing, read completion and per-bid acks all
    ; behave exactly as if each had won its own slot.
    ((eq? (car val) 'multi)
     (let loop ((vs (cdr val)))
       (if (null? vs) st (begin (qp-apply-val st (car vs)) (loop (cdr vs))))))
    (else (qp-apply-one st val))))

(define (qp-apply-one st val)
  ; cw-65x diagnostic guard: a malformed decided value must not kill the
  ; shard actor — log the datum (the bug evidence) and apply nothing.
  (if (not (and (pair? val) (pair? (cdr val)) (list? (cadr val))))
      (begin
        (display "BADVAL qp-apply-one: ") (write val) (newline)
        st)
      (qp-apply-one* st val)))

(define (qp-apply-one* st val)
  (let ((bid (car val)) (cmds (cadr val)))
    (if (qp-bid-applied? st bid)
        st                                    ; exactly-once: hedged duplicate
        (let ((sm2 (let loop ((c cmds) (acc (qpn-sm st)))
                     (if (null? c) acc
                         (loop (cdr c)
                               (if (null? (car c)) acc
                                   ((qpn-apply-fn st) acc (car c)))))))
              ; a completed read: move its tag to the done queue
              (rd (assoc bid (qpn-reads st))))
          (set-qpn-sm! st sm2)
          (qp-bid-note! st bid)
          (set-qpn-holds! st (qp-hold-del (qpn-holds st) bid))
          (if rd
              (begin
                (set-qpn-reads! st (qp-hold-del (qpn-reads st) bid))
                (set-qpn-rdone! st (cons (cdr rd) (qpn-rdone st)))))
          (set-qpn-adone! st (cons (cons bid (length cmds)) (qpn-adone st)))
          st))))

; ============================================================
; public API: propose / step / tick
; ============================================================

(define (qp-start-slot st val)
  (let ((slot (qpn-next-slot st)))
    (set-qpn-next-slot! st (+ slot 1))
    (if (not (null? val)) (hashtable-set! (qpn-mine st) slot val))
    (hashtable-set! (qpn-props st) slot
                    (make-qprop 4 (list (qpn-hi st) (qpn-id st) val) '() '()))
    (qp-send-phase st slot)))

(define (qp-start-noop-at st slot)            ; gap fill at an explicit slot
  (hashtable-set! (qpn-props st) slot
                  (make-qprop 4 (list (qpn-hi st) (qpn-id st) '()) '() '()))
  (qp-send-phase st slot))

(define (qp-propose-batch st cmds)
  (if (null? cmds) (cons st '())
      (let* ((seq (+ 1 (qpn-seq st)))
             (bid (list (qpn-id st) (qpn-boot st) seq))
             (val (list bid cmds)))
        (set-qpn-seq! st seq)
        (if (or (qp-coord? st) (<= (qpn-hedge st) 0))
            (qp-start-slot st val)
            ; hedge: hold the batch, forward to the coordinator; the tick fires
            ; a self-propose if the bid hasn't applied within `hedge` ticks.
            (begin
              (set-qpn-holds! st (cons (cons bid (cons val (qpn-hedge st)))
                                       (qpn-holds st)))
              (cons st (list (cons (qpn-coord st) (list 'pfwd val)))))))))

(define (qp-propose st cmd) (qp-propose-batch st (list cmd)))

(define (qp-on-decd st from msg)
  (qp-decide st (list-ref msg 1) (list-ref msg 2) #f))

(define QP-FETCH-SPAN 32)   ; decided slots served per fetch (rejoin catch-up)
(define (qp-on-fetch st from msg)
  (let ((slot (list-ref msg 1)))
    (if (<= slot (qpn-base st))
        (cons st (list (cons from (list 'snapo (qpn-base st)))))
        ; reply a contiguous RANGE of decided slots from `slot` so a rejoining
        ; node catches up in one round instead of one-slot-per-tick.
        (let loop ((i slot) (outs '()))
          (let ((dv (and (< (- i slot) QP-FETCH-SPAN)
                         (hashtable-ref (qpn-decided st) i #f))))
            (if dv
                (loop (+ i 1) (cons (cons from (list 'decd i dv)) outs))
                (cons st (reverse outs))))))))

; a peer told us the slot we need is below its compaction floor: surface the
; snapshot need to the driver (it ships/installs the store snapshot, then
; calls qp-install-snapshot).
(define (qp-on-snapo st from msg)
  (let ((pbase (list-ref msg 1)))
    (if (> pbase (qpn-applied st))
        (set-qpn-snap-need! st (cons from pbase)))
    (cons st '())))

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
  (let* ((seq (+ 1 (qpn-seq st)))
         (bid (list (qpn-id st) (qpn-boot st) seq))
         (val (list bid '())))
    (set-qpn-seq! st seq)
    (set-qpn-reads! st (cons (cons bid tag) (qpn-reads st)))
    (if (or (qp-coord? st) (<= (qpn-hedge st) 0))
        (qp-start-slot st val)
        (begin
          (set-qpn-holds! st (cons (cons bid (cons val (qpn-hedge st)))
                                   (qpn-holds st)))
          (cons st (list (cons (qpn-coord st) (list 'pfwd val))))))))

; -> (done-tags-oldest-first . st')
(define (qp-take-reads st)
  (let ((done (reverse (qpn-rdone st))))
    (set-qpn-rdone! st '())
    (cons done st)))

 ; -> (((bid . ncmds) ...) oldest-first . st') — batches applied since last take
(define (qp-take-applied st)
  (let ((done (reverse (qpn-adone st))))
    (set-qpn-adone! st '())
    (cons done st)))

; the bid the NEXT qp-propose-batch/qp-read on this node will use
(define (qp-next-bid st)
  (list (qpn-id st) (qpn-boot st) (+ 1 (qpn-seq st))))

; ---- tick: hedge countdowns, retransmission, gap fill. No timeouts: none of
;      this affects safety, and liveness needs only that ticks keep coming. ----

(define (qp-tick-holds st)
  ; partition first and install the kept list BEFORE firing hedges: a fired
  ; self-propose can decide+apply inline (solo/majority-of-one paths) and
  ; qp-apply-one* prunes holds — mutating the field we would otherwise clobber.
  (let loop ((hs (qpn-holds st)) (kept '()) (fired '()))
    (if (pair? hs)
        (let* ((h (car hs)) (val (cadr h)) (t (cddr h)))
          (if (<= t 1)
              (loop (cdr hs) kept (cons val fired))
              (loop (cdr hs) (cons (cons (car h) (cons val (- t 1))) kept) fired)))
        (begin
          (set-qpn-holds! st (reverse kept))
          (let fire ((fs (reverse fired)) (outs '()))
            (if (null? fs)
                (cons st outs)
                (let ((r (qp-start-slot st (car fs))))   ; hedge fires: self-propose
                  (fire (cdr fs) (append outs (cdr r))))))))))

(define (qp-retransmits st)
  (let ((ht (qpn-props st)))
    (let loop ((slots (vector->list (hashtable-keys ht))) (outs '()))
      (if (null? slots) outs
          (let* ((slot (car slots)) (prop (hashtable-ref ht slot #f))
                 (pis (qprop-pis prop)) (s (qprop-s prop))
                 (got (qprop-got prop)))
            (loop (cdr slots)
                  (append outs
                          (let inner ((pr (qpn-peers st)) (acc '()))
                            (cond ((null? pr) acc)
                                  ((assv (car pr) got) (inner (cdr pr) acc))
                                  (else
                                   (inner (cdr pr)
                                          (cons (cons (car pr)
                                                      (list 'esp slot s (cdr (assv (car pr) pis))))
                                                acc))))))))))))

(define QP-AE-TICKS 8)      ; idle anti-entropy probe period (rejoin catch-up)
(define (qp-tick-gap st)
  (let ((next (+ 1 (qpn-applied st))))
    (if (and (< next (qpn-next-slot st))
             (not (hashtable-ref (qpn-decided st) next #f))
             (not (hashtable-ref (qpn-props st) next #f)))
        (let ((g (+ 1 (qpn-gapt st))))
          (set-qpn-gapt! st g)
          (cond
            ((= g QP-GAP-FETCH)
             (cons st (map (lambda (p) (cons p (list 'fetch next))) (qpn-peers st))))
            ((>= g QP-GAP-NOOP)
             (set-qpn-gapt! st 0)
             (qp-start-noop-at st next))
            (else (cons st '()))))
        ; no LOCAL evidence of a gap — but a restarted node (applied=P,
        ; next-slot=P+1) can be silently behind a cluster that moved on while
        ; it was dead. Probe peers periodically; they answer decd only if they
        ; actually decided our next slot (else silence). Found by the WAN kill
        ; soak: the restarted coordinator served stale reads forever.
        (let ((g (+ 1 (qpn-gapt st))))
          (if (>= g QP-AE-TICKS)
              (begin (set-qpn-gapt! st 0)
                     (cons st (map (lambda (p) (cons p (list 'fetch next))) (qpn-peers st))))
              (begin (set-qpn-gapt! st g)
                     (cons st '())))))))

(define (qp-tick st)
  (let* ((r1 (qp-tick-holds st))
         (outs2 (qp-retransmits st))
         (r3 (qp-tick-gap st)))
    (cons st (append (cdr r1) outs2 (cdr r3)))))
