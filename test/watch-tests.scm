; test/watch-tests.scm — cw-u4a.15 Watch tests: gap-filling coverage
;
; COVERAGE AUDIT — cw-u4a.15 deliverables:
;
;   Deliverable             Covered by
;   ─────────────────────   ──────────────────────────────────────────────────────
;   replay (historical)     watch-replay-poc.scm (19), watch-backend.scm §replay,
;                             watch-stream.scm §CLUSTER:replay+live,
;                             this file §2 (deeper 9-rev cluster replay)
;   live events             watch-backend.scm §future-only + §seam,
;                             watch-stream.scm §CLUSTER:replay+live,
;                             this file §1 (prev_kv stream path, live UPDATE+DELETE)
;   prefix/range            watch-replay-poc.scm, watch-backend.scm §prefix/range,
;                             watch-stream.scm §CLUSTER:multiplex
;   multi-watch-id          watch-backend.scm §multi-watcher,
;                             watch-stream.scm §CLUSTER:multiplex
;   cancel                  watch-backend.scm §cancel,
;                             watch-stream.scm §CLUSTER:cancel
;   compaction boundary     watch-replay-poc.scm §ErrCompacted,
;                             watch-backend.scm §ErrCompacted,
;                             watch-stream.scm §UNIT:compaction-cancel,
;                             this file §3 (mid-stream ErrCompacted via apply-fn wiring)
;   prev_kv                 watch-backend.scm §prev_kv (registry level),
;                             this file §1 (prev_kv over the stream/wire-bridge path)
;
;   Fragmentation (gRPC max-message-size) is deferred to cw-u4a.23 per ADR §6.
;
; WHAT IS NEW HERE (genuine gaps, not duplicated from the above):
;   §1 prev_kv over the STREAM path  — watch-backend tested prev_kv at the registry
;      level.  This test drives the full wire bridge end-to-end: a watch with prev_kv
;      enabled is registered and then observes a CREATE, UPDATE, and DELETE, asserting
;      the WatchResponse events carry the correct previous KeyValue after the
;      watch-response->sexp / sexp->watch-response round-trip.
;   §2 Deeper historical replay over the cluster — watch-stream did a 4-rev replay.
;      This test builds a 9-revision history across several keys, starts a watch from
;      an OLD wire revision, and asserts the FULL ordered replay set arrives (in the
;      right order) before any live events.
;   §3 End-to-end mid-stream ErrCompacted via the apply-fn wiring — watch-stream's
;      compaction cancel was proven with a direct watch-check-compaction! unit call.
;      This test fires COMPACT through the same mvcc-apply + watch-check-compaction!
;      pipeline the shard apply-fn wires, confirming the COMPACT branch (shard-actor
;      lines 102-105) drives cancellation.  As sanctioned by ADR §5/watch-stream §B,
;      we force the watcher to lag via set-w-delivered-rev!/set-w-synced?! (because a
;      genuinely-lagging unsynced watcher can't be forced deterministically via a live
;      cluster, as catch-up promotes it synchronously at create-time), and assert
;      through the wire bridge that the canceled WatchResponse reaches the consumer.
;   §4 Watch on an absent-then-created key — future-only watch on a key that does not
;      exist yet.  Asserts the first delivered event is a PUT with prev_kv = #f.
;   §5 NODELETE/NOPUT filter over the wire-bridge path — watch-backend proved it at
;      registry level.  This confirms the filter survives the sexp round-trip.
;
; HARNESS NOTES:
;   • Wall-clock microsecond dir-tag: (exact (round (* 1000000 (current-second)))) per
;     sim-cluster-smoke / watch-stream idiom — NOT (current-jiffy).
;   • Single-ctx sections (§1, §3, §4, §5) use reset-ctx! so they are deterministic
;     and pass back-to-back with no manual cleanup.
;   • Cluster section (§2) uses the cooperative spin loop from watch-stream.scm
;     (yield every iteration, sleep-ms 1 every 200 iterations).
;   • shard-actor.scm and peer-poller.scm hardcode table names ws-shard-*.  The
;     cluster section creates tables with exactly those names.

(include "test/harness.scm")

; ===========================================================================
; SINGLE-CTX SECTION — §1, §3, §4, §5 only need the backend + wire bridge
; ===========================================================================
(include "src/store-ctx.scm")
(include "src/mvcc.scm")
(include "src/watch.scm")
(include "test/mvcc-util.scm")

(define (b s) (string->utf8 s))

(define UNIT-TAG (number->string (exact (round (* 1000000 (current-second))))))
(define UNIT-DB  (string-append "/tmp/cws-watch-tests-unit-" UNIT-TAG))
(define UCTX (make-ctx (store-open UNIT-DB #t) "default" #t))
(reset-ctx! UCTX)

; wire-bridge collector: record -> sexp -> record (same round-trip the shard uses)
(define (make-wire-box) (vector '()))
(define (wire-collect box)
  (lambda (wr)
    (let ((sexp (watch-response->sexp wr)))
      (vector-set! box 0 (cons (sexp->watch-response sexp) (vector-ref box 0))))))
(define (wbox-responses box) (reverse (vector-ref box 0)))
(define (wbox-events box)
  (apply append (map wr-events (wbox-responses box))))
(define (wbox-cancels box)
  (filter wr-canceled? (wbox-responses box)))

; apply a command and fire watch dispatch (+ compaction check on COMPACT)
(define (unit-apply! reg . parts)
  (let* ((pre  (mvcc-current-rev UCTX))
         (res  (mvcc-apply UCTX (map b parts)))
         (post (mvcc-current-rev UCTX)))
    (watch-on-apply! reg UCTX pre post)
    (if (and (> (reg-count reg) 0) (string=? (car parts) "COMPACT"))
        (watch-check-compaction! reg UCTX))
    res))

; ===========================================================================
; §1 — prev_kv survives the record<->sexp wire bridge (stream path)
;      watch-backend.scm proved prev_kv at the registry level.  Here we run the
;      watch-response->sexp / sexp->watch-response round-trip and assert the prev_kv
;      field is preserved (correct value + mod_rev) end-to-end.
; ===========================================================================
(section "§1: prev_kv survives the record<->sexp wire bridge (stream path)")
(reset-ctx! UCTX)
(define reg1 (make-watch-registry))
(define box1 (make-wire-box))
(define wid1 (watch-register! reg1 UCTX
                               (list (cons 'key (b "")) (cons 'range-end (bytevector 0))
                                     (cons 'start-rev 0) (cons 'prev-kv #t))
                               (wire-collect box1)))
(check "§1 registered ok" #t (and (integer? wid1) #t))

(unit-apply! reg1 "PUT" "pk" "first")   ; rev 1 — create
(unit-apply! reg1 "PUT" "pk" "second")  ; rev 2 — update
(unit-apply! reg1 "DEL" "pk")           ; rev 3 — delete

; helpers to find events by mod-rev
(define (find-ev-by-rev evs mr)
  (let loop ((evs evs))
    (cond ((null? evs) #f)
          ((= (kvv-mod-rev (we-kv (car evs))) mr) (car evs))
          (else (loop (cdr evs))))))

(let* ((evs (wbox-events box1))
       (create-ev (find-ev-by-rev evs 1))
       (update-ev (find-ev-by-rev evs 2))
       (delete-ev (find-ev-by-rev evs 3)))
  (check "§1 three events delivered (via wire)" 3 (length evs))
  (check "§1 create @1: type=put" 'put (we-type create-ev))
  (check "§1 create @1: prev_kv=#f (no prior version; survived wire bridge)" #f (we-prev-kv create-ev))
  (check "§1 update @2: type=put" 'put (we-type update-ev))
  (check "§1 update @2: prev_kv present (survived wire bridge)" #t
         (and (we-prev-kv update-ev) #t))
  (check "§1 update @2: prev_kv value=first" "first"
         (utf8->string (kvv-value (we-prev-kv update-ev))))
  (check "§1 update @2: prev_kv mod_rev=1" 1 (kvv-mod-rev (we-prev-kv update-ev)))
  (check "§1 delete @3: type=del" 'del (we-type delete-ev))
  (check "§1 delete @3: prev_kv present (survived wire bridge)" #t
         (and (we-prev-kv delete-ev) #t))
  (check "§1 delete @3: prev_kv value=second" "second"
         (utf8->string (kvv-value (we-prev-kv delete-ev))))
  (check "§1 delete @3: prev_kv mod_rev=2" 2 (kvv-mod-rev (we-prev-kv delete-ev))))

; ===========================================================================
; §4 — Watch on an absent-then-created key (future-only, no history)
;      The first event must be a PUT and its prev_kv must be #f (key did not
;      exist before), confirming the backend emits the right shape for the
;      "brand new key" case over the wire bridge.
; ===========================================================================
(section "§4: future-only watch on absent key — first event is PUT with prev_kv=#f")
(reset-ctx! UCTX)
(define reg4 (make-watch-registry))
(define box4 (make-wire-box))
(define wid4 (watch-register! reg4 UCTX
                               (list (cons 'key (b "brand-new")) (cons 'range-end #f)
                                     (cons 'start-rev 0) (cons 'prev-kv #t))
                               (wire-collect box4)))
; key does not exist yet — no event before creation
(check "§4 no events before key created" 0 (length (wbox-events box4)))

(unit-apply! reg4 "PUT" "brand-new" "hello")   ; rev 1

(let ((evs (wbox-events box4)))
  (check "§4 exactly one event" 1 (length evs))
  (let ((ev (car evs)))
    (check "§4 type=put" 'put (we-type ev))
    (check "§4 key=brand-new" "brand-new" (utf8->string (kvv-key (we-kv ev))))
    (check "§4 value=hello" "hello" (utf8->string (kvv-value (we-kv ev))))
    (check "§4 prev_kv=#f (key did not exist; survived wire bridge)" #f (we-prev-kv ev))))

; ===========================================================================
; §5 — NODELETE / NOPUT filter over the wire-bridge path
;      watch-backend.scm tested filters at the registry level.  Here we run
;      the sexp round-trip to confirm filter fields survive the bridge.
;      NOTE: two watchers share the same registry; hashtable iteration order is
;      unspecified, but each watcher's delivery is independent.  We check the
;      delivered event types and key-set (order-insensitive via list-length +
;      member) rather than positional order across the two watchers.
; ===========================================================================
(section "§5: NODELETE / NOPUT filters survive the wire bridge")
(reset-ctx! UCTX)
(define reg5 (make-watch-registry))
(define box-nd (make-wire-box))  ; NODELETE: PUTs only
(define box-np (make-wire-box))  ; NOPUT:    DELETEs only

(watch-register! reg5 UCTX
                 (list (cons 'key (b "")) (cons 'range-end (bytevector 0))
                       (cons 'start-rev 0) (cons 'filters '(nodelete)))
                 (wire-collect box-nd))
(watch-register! reg5 UCTX
                 (list (cons 'key (b "")) (cons 'range-end (bytevector 0))
                       (cons 'start-rev 0) (cons 'filters '(noput)))
                 (wire-collect box-np))

(unit-apply! reg5 "PUT" "ff" "v1")  ; rev 1 — PUT
(unit-apply! reg5 "DEL" "ff")       ; rev 2 — DEL
(unit-apply! reg5 "PUT" "gg" "v3")  ; rev 3 — PUT

; NODELETE: only PUTs (ff@1, gg@3) reach the wire bridge
(let ((evs-nd (wbox-events box-nd)))
  (check "§5 NODELETE (wire): 2 events" 2 (length evs-nd))
  (check "§5 NODELETE (wire): all type=put" '(put put) (map we-type evs-nd))
  (let ((keys-nd (map (lambda (ev) (utf8->string (kvv-key (we-kv ev)))) evs-nd)))
    (check "§5 NODELETE (wire): ff delivered" #t (and (member "ff" keys-nd) #t))
    (check "§5 NODELETE (wire): gg delivered" #t (and (member "gg" keys-nd) #t))))

; NOPUT: only DEL (ff@2) reaches the wire bridge
(let ((evs-np (wbox-events box-np)))
  (check "§5 NOPUT (wire): 1 event" 1 (length evs-np))
  (check "§5 NOPUT (wire): type=del" 'del (we-type (car evs-np)))
  (check "§5 NOPUT (wire): key=ff" "ff" (utf8->string (kvv-key (we-kv (car evs-np))))))

; ===========================================================================
; §3 — End-to-end mid-stream ErrCompacted via the apply-fn wiring
;      watch-stream.scm §UNIT proved this with a direct watch-check-compaction! call.
;      Here we fire COMPACT through the same apply-fn wiring the shard-actor runs
;      (mvcc-apply COMPACT then watch-check-compaction! — identical to the shard's
;      apply-fn COMPACT branch), confirming through the wire bridge that a lagging
;      watcher receives a canceled WatchResponse with compact_revision set.
;
;      The forced-lag via set-w-delivered-rev!/set-w-synced?! is the ADR-sanctioned
;      stand-in (see ADR §5 / watch-stream §B): a genuinely-lagging watcher can't be
;      forced on a live cluster because catch-up is synchronous at register-time.
; ===========================================================================
(section "§3: mid-stream ErrCompacted via apply-fn COMPACT wiring (wire bridge)")
(reset-ctx! UCTX)
(define reg3 (make-watch-registry))
(define box3 (make-wire-box))

; Build history revs 1-4
(mvcc-apply UCTX (list (b "PUT") (b "zz") (b "v1")))
(mvcc-apply UCTX (list (b "PUT") (b "zz") (b "v2")))
(mvcc-apply UCTX (list (b "PUT") (b "zz") (b "v3")))
(mvcc-apply UCTX (list (b "PUT") (b "zz") (b "v4")))
(check "§3 unit history at rev 4" 4 (mvcc-current-rev UCTX))

; Register future-only: delivered_rev=4, synced
(define wid3 (watch-register! reg3 UCTX
                               (list (cons 'key (b "")) (cons 'range-end (bytevector 0))
                                     (cons 'start-rev 0))
                               (wire-collect box3)))
(check "§3 watcher delivered_rev=4 at create" 4 (w-delivered-rev (reg-get reg3 wid3)))

; Force the watcher to lag (ADR §5 / watch-stream §B sanctioned stand-in)
(set-w-delivered-rev! (reg-get reg3 wid3) 3)
(set-w-synced?!       (reg-get reg3 wid3) #f)

; Apply rev 5
(mvcc-apply UCTX (list (b "PUT") (b "zz") (b "v5")))
(check "§3 current-rev=5 after v5" 5 (mvcc-current-rev UCTX))

; Apply COMPACT to rev 5 through the apply-fn wiring (same pipeline as shard-actor).
; cw-xq9 root-cause fix: watch-check-compaction! no longer CANCELS a registered
; (synced-by-construction) watcher below the floor — that mass-ErrCompacted every
; quiet k8s watch at each 5-min compaction and froze the apiserver caches. It now
; advances the de-dup floor to compact-1 and cancels nothing; live delivery continues.
(let ((r (mvcc-apply UCTX (list (b "COMPACT") (b "5")))))
  (check "§3 compact to 5 ok" (cons 'ok 5) r))
(let ((canceled (watch-check-compaction! reg3 UCTX)))
  (check "§3 watch-check-compaction! cancels NOTHING (cw-xq9)" '() canceled))
(check "§3 watcher still registered after compaction" 1 (reg-count reg3))
(check "§3 lagging delivered_rev advanced to compact-1"
       4 (w-delivered-rev (reg-get reg3 wid3)))
(check "§3 no canceled WatchResponse emitted" 0 (length (wbox-cancels box3)))

; the watcher still delivers live events past the compaction
(set-w-synced?! (reg-get reg3 wid3) #t)
(unit-apply! reg3 "PUT" "zz" "v6")
(check "§3 live event after compaction still delivered" 1 (length (wbox-events box3)))

; ===========================================================================
; CLUSTER SECTION — §2: deeper 9-rev historical replay
;
; The shard-actor.scm and peer-poller.scm hardcode global table names:
;   ws-shard-pid / ws-shard-role / ws-shard-leader / ws-shard-commit /
;   ws-shard-applied  — these MUST be the tables created here.
; We also create ws-test (for client/driver coordination) and
; wt-watch-out (for this test's watch-stream output sink, scoped away
; from the ws-watch-out table watch-stream.scm uses if run separately).
; ===========================================================================
(make-table 'ws-shard-pid    "set")
(make-table 'ws-shard-role   "set")
(make-table 'ws-shard-leader "set")
(make-table 'ws-shard-commit "set")
(make-table 'ws-shard-applied "set")
(make-table 'ws-test          "set")
(make-table 'wt-watch-out     "set")

(for-each node-make (list "x" "y" "z"))
(node-link! "x" "y") (node-link! "x" "z") (node-link! "y" "z")

(define CL-TAG (number->string (exact (round (* 1000000 (current-second))))))
(define (cl-db nd)
  (string-append "/tmp/cws-watch-tests-cl-" CL-TAG "-" nd "-s0"))

(for-each
 (lambda (nd)
   (spawn-source "(include \"src/server/shard-actor.scm\")" 'shard-main
                 "0" '(x y z) (string->symbol nd) (cl-db nd) #f))
 '("x" "y" "z"))
(for-each
 (lambda (nd)
   (spawn-source "(include \"src/server/peer-poller.scm\")" 'peer-poller
                 (string->symbol nd) '("0") 150 '() 0))
 '("x" "y" "z"))

; cooperative spin (yield every iteration, sleep-ms 1 every 200 — per watch-stream.scm)
(define (spin pred who)
  (let loop ((i 0))
    (cond ((pred) #t)
          ((> i 8000000) (error (string-append "timeout: " who)))
          (else
           (yield)
           (if (= 0 (modulo i 200)) (sleep-ms 1))
           (loop (+ i 1))))))

(define (cl-role nd) (table-lookup 'ws-shard-role (string-append nd ":0")))
(define (cl-pid  nd) (table-lookup 'ws-shard-pid  (string-append nd ":0")))

(define (leader-node)
  (cond ((eq? (cl-role "x") 'leader) "x")
        ((eq? (cl-role "y") 'leader) "y")
        ((eq? (cl-role "z") 'leader) "z")
        (else #f)))

(section "CLUSTER: leader election (§2 shared)")
(spin (lambda () (leader-node)) "leader election (watch-tests)")
(define LDR (leader-node))
(display "  leader: ") (display LDR) (newline)
(check "a leader emerged" #t (and (member LDR '("x" "y" "z")) #t))
(table-insert! 'ws-test "ldr" LDR)

; client actor: drives proposals at the leader via the real PID reply path
; (same idiom as watch-stream.scm's client actor, using ws-shard-pid + ws-test)
(spawn-source "
  (define (b s) (string->utf8 s))
  (define (ask pid msg) (send pid msg) (raw-receive))
  (define (propose-to ldr cmd)
    (ask (table-lookup 'ws-shard-pid (string-append ldr \":0\")) (cons (self) cmd)))
  (define (client)
    (let ((ldr (table-lookup 'ws-test \"ldr\")))
      (let loop ()
        (let ((job (table-lookup 'ws-test \"wt-job\")))
          (cond
            ((eq? job 'done) #t)
            ((and (pair? job) (eq? (car job) 'cmd))
             (let ((r (propose-to ldr (cadr job))))
               (table-insert! 'ws-test (caddr job) r)
               (table-insert! 'ws-test \"wt-job\" #f)
               (loop)))
            (else (yield) (loop)))))))"
             'client)

(define put-seq 0)
(define (put! k v)
  (set! put-seq (+ put-seq 1))
  (let ((ak (string-append "wt-ack" (number->string put-seq))))
    (table-insert! 'ws-test ak #f)
    (table-insert! 'ws-test "wt-job"
                   (list 'cmd (list (b "PUT") (b k) (b v)) ak))
    (spin (lambda () (table-lookup 'ws-test ak))
          (string-append "put " k))
    (table-lookup 'ws-test ak)))

(define (del! k)
  (set! put-seq (+ put-seq 1))
  (let ((ak (string-append "wt-ack" (number->string put-seq))))
    (table-insert! 'ws-test ak #f)
    (table-insert! 'ws-test "wt-job"
                   (list 'cmd (list (b "DEL") (b k)) ak))
    (spin (lambda () (table-lookup 'ws-test ak))
          (string-append "del " k))
    (table-lookup 'ws-test ak)))

; driver actor (relay create/cancel to a watch-stream conn using ws-test for coordination)
(spawn-source "
  (define (driver)
    (let loop ()
      (let ((cmd (table-lookup 'ws-test \"wt-drv\")))
        (cond
          ((eq? cmd 'stop) #t)
          ((pair? cmd)
           (table-insert! 'ws-test \"wt-drv\" #f)
           (let ((res-key (car cmd)) (conn (cadr cmd)) (kind (caddr cmd)) (rest (cdddr cmd)))
             (cond
               ((eq? kind 'create)
                (send conn (list 'watch-create (self) (car rest) (cadr rest) (caddr rest))))
               ((eq? kind 'cancel)
                (send conn (list 'watch-cancel-req (self) (car rest)))))
             (let ((r (raw-receive)))
               (table-insert! 'ws-test res-key r)))
           (loop))
          (else (yield) (loop))))))"
             'driver)

(define (drv! res-key conn . parts)
  (table-insert! 'ws-test res-key #f)
  (table-insert! 'ws-test "wt-drv" (cons res-key (cons conn parts)))
  (spin (lambda () (table-lookup 'ws-test res-key))
        (string-append "drv " res-key))
  (table-lookup 'ws-test res-key))

; spawn a watch-stream actor against the leader; sink into wt-watch-out
(define (spawn-conn out-tag)
  (spawn-source "(include \"src/server/watch-stream.scm\")" 'watch-stream
                (cl-pid LDR) 'wt-watch-out out-tag))

; read back a conn's wire WatchResponses oldest-first from wt-watch-out
(define (conn-responses out-tag)
  (let ((cur (table-lookup 'wt-watch-out out-tag)))
    (reverse (if cur cur '()))))

; flatten events to (wid type key-string mod-rev) tuples in delivery order
; Wire sexp: (WID HEADER CREATED? CANCELED? REASON COMPACT ((TYPE KV PREV-KV) ...))
; kv = #(key create-rev mod-rev version lease value)
(define (conn-event-tuples out-tag)
  (let loop ((rs (conn-responses out-tag)) (out '()))
    (if (null? rs)
        (reverse out)
        (let* ((wr (car rs)) (wid (list-ref wr 0)) (evs (list-ref wr 6)))
          (loop (cdr rs)
                (append
                 (reverse
                  (map (lambda (ev)
                         (let ((kv (cadr ev)))
                           (list wid (car ev) (utf8->string (vector-ref kv 0)) (vector-ref kv 2))))
                       evs))
                 out))))))

(define (conn-created out-tag)
  (filter (lambda (wr) (list-ref wr 2)) (conn-responses out-tag)))

; ===========================================================================
; §2 — Deeper historical replay over the cluster
;      Build 9 revisions across several keys, then watch from an old wire
;      start_revision and assert the full ordered replay arrives before live.
; ===========================================================================
(section "§2: deeper 9-rev cluster replay from old start_revision")

; build 9 revisions across /d/1, /d/2, /d/3, /d/4
(check "d/1 v1 -> rev 1"     (cons "PUT" 1) (put! "/d/1" "v1"))
(check "d/2 v2 -> rev 2"     (cons "PUT" 2) (put! "/d/2" "v2"))
(check "d/3 v3 -> rev 3"     (cons "PUT" 3) (put! "/d/3" "v3"))
(check "d/1 v4 -> rev 4"     (cons "PUT" 4) (put! "/d/1" "v4"))  ; update /d/1
(check "d/2 v5 -> rev 5"     (cons "PUT" 5) (put! "/d/2" "v5"))  ; update /d/2
(check "d/3 v6 -> rev 6"     (cons "PUT" 6) (put! "/d/3" "v6"))  ; update /d/3
(check "d/1 v7 -> rev 7"     (cons "PUT" 7) (put! "/d/1" "v7"))  ; update /d/1 again
(check "d/2 del -> rev 8"    (cons "DEL" (cons 8 1)) (del! "/d/2"))  ; ack shape ("DEL" rev . count)
(check "d/4 v9 -> rev 9"     (cons "PUT" 9) (put! "/d/4" "v9")) ; new key /d/4

; watch from wire start_revision=3 (inclusive, all keys)
; internal exclusive = wire-1 = 2 => replay mod_rev > 2 = revs 3..9 (7 events)
(define conn2 (spawn-conn "c-deep"))
(check "§2 create wid 500 wire-start=3 all-keys"
       (cons 'watch-create-ok 500)
       (drv! "r-deep" conn2 'create 500 3
             (list (cons 'key (b "")) (cons 'range-end (bytevector 0)))))

; replay is delivered synchronously during register, before the create ack,
; so by the time the ack lands the 7 historical frames are in the sink.
(check "§2 got a CREATED frame" 1 (length (conn-created "c-deep")))
(let ((tuples (conn-event-tuples "c-deep")))
  (check "§2 replay delivers 7 events (revs 3..9) before live"
         7 (length tuples))
  (check "§2 replay rev 3: put /d/3"  '(500 put "/d/3" 3) (list-ref tuples 0))
  (check "§2 replay rev 4: put /d/1"  '(500 put "/d/1" 4) (list-ref tuples 1))
  (check "§2 replay rev 5: put /d/2"  '(500 put "/d/2" 5) (list-ref tuples 2))
  (check "§2 replay rev 6: put /d/3"  '(500 put "/d/3" 6) (list-ref tuples 3))
  (check "§2 replay rev 7: put /d/1"  '(500 put "/d/1" 7) (list-ref tuples 4))
  (check "§2 replay rev 8: del /d/2"  '(500 del "/d/2" 8) (list-ref tuples 5))
  (check "§2 replay rev 9: put /d/4"  '(500 put "/d/4" 9) (list-ref tuples 6)))

; live event arrives AFTER the replay set — no gap, no duplicate
(check "d/4 live -> rev 10" (cons "PUT" 10) (put! "/d/4" "v10"))
(spin (lambda () (>= (length (conn-event-tuples "c-deep")) 8)) "§2 live event 10")
(let ((tuples (conn-event-tuples "c-deep")))
  (check "§2 total 8 events: 7 replay + 1 live, in order, no duplicate"
         8 (length tuples))
  (check "§2 live event 10 at position 7"
         '(500 put "/d/4" 10) (list-ref tuples 7)))

; ===========================================================================
; §6 — cw-i07 (G4): a pending do-create must not hold OTHER watch_ids'
;      already-flowing events hostage on the same stream.
;
;      grpc-watch.scm's do-create registers the new watch and then loops
;      awaiting its 'watch-created ack; any 'watch-response frame that
;      arrives on the worker's mailbox during that await used to be
;      unconditionally buffered and held until the new watch's created-ack
;      landed — even one that belonged to a DIFFERENT, already-established
;      watch_id on the same stream. Under a slow shard round-trip that
;      starved every other watcher on the connection of live events for the
;      whole registration window (the informer-pinning bug this gate
;      targets).
;
;      This exercises the FIXED arbitration rule directly: a wr-sexp whose
;      wid is already in live-wids ships immediately; only the pending new
;      watch's own replay (wid not yet established) waits for its ack. It
;      mirrors do-create's cond arm exactly (see cw-i07 in grpc-watch.scm)
;      against a scripted mailbox sequence, so the ordering guarantee is
;      pinned independent of real shard/network timing.
; ===========================================================================
(section "§6: do-create does not hold other watch_ids' events hostage (cw-i07)")

(define fair-live-wids '())
(define fair-out '())
(define (fair-emit! wr) (set! fair-out (cons wr fair-out)))

; mirrors grpc-watch.scm do-create's 'watch-response cond arm post-fix.
(define (fair-handle-response wr buffered)
  (let ((w (car wr)))
    (if (memv w fair-live-wids)
        (begin (fair-emit! wr) buffered)
        (cons wr buffered))))

; mirrors do-create's 'watch-created cond arm: establish, ack, flush replay.
(define (fair-handle-created wid buffered)
  (set! fair-live-wids (cons wid fair-live-wids))
  (fair-emit! (list wid 0 #t #f "" 0 '()))
  (for-each fair-emit! (reverse buffered)))

; wid 601 is already established (a prior create completed on this stream).
(fair-handle-created 601 '())

; wid 602's create is now in flight (its await loop is running). While it
; is pending: a LIVE event for the already-established wid 601 arrives,
; then wid 602's own replay event (buffered — no created-ack yet), then
; wid 602's created-ack (which flushes its buffered replay).
(let* ((b0 '())
       (b1 (fair-handle-response (list 601 5 #f #f "" 0 '((put #(k1) #f))) b0))
       (b2 (fair-handle-response (list 602 3 #f #f "" 0 '((put #(k2) #f))) b1)))
  (fair-handle-created 602 b2))

(let ((order (reverse fair-out)))
  (check "§6 4 frames emitted" 4 (length order))
  (check "§6 wid 601's created ack first"
         601 (car (list-ref order 0)))
  (check "§6 wid 601's live event ships WHILE 602's create is pending (not held hostage)"
         601 (car (list-ref order 1)))
  (check "§6 wid 602's created ack only after its own await resolves"
         602 (car (list-ref order 2)))
  (check "§6 wid 602's own buffered replay flushes after its created ack"
         602 (car (list-ref order 3)))
  (check "§6 no reordering within wid 601 (its one event is exactly its own)"
         '(put #(k1) #f) (car (list-ref (list-ref order 1) 6)))
  (check "§6 no reordering within wid 602 (its replay event is exactly its own)"
         '(put #(k2) #f) (car (list-ref (list-ref order 3) 6))))

(done!)
