; test/watch-stream.scm — the per-connection Watch STREAMING ACTOR (cw-u4a.14):
; wires the .13 watch backend (registry + dispatch) into a per-conn streaming actor
; + the shard-actor mailbox register/cancel protocol, so a client can create/cancel/
; multiplex watches and receive WatchResponses.  ADR 0002 §4 (registry on the
; leader's shard, per-conn streaming actors consume via mailbox), §5 (ErrCompacted
; incl. mid-stream lagging cancel), §6 (progress_notify, cancel on the single
; thread), and the wire-inclusive->internal-exclusive start_revision ±1.
;
; TWO harnesses:
;   (A) CLUSTER INTEGRATION (mirrors sim-cluster-smoke, now reliable after cw-u4a.39):
;       a 3-voter in-process cluster, settle to a leader, spawn watch-stream actors
;       against the leader's shard PID with cs-table output sinks, propose PUTs
;       through the leader, and assert the mock-conn output receives the correct
;       multiplexed WatchResponses (historical replay if start_rev>0 + live events),
;       in order, each once.  Covers: replay+live, multiplex, cancel, leader-gating,
;       progress_notify.
;   (B) SINGLE-CTX UNIT (mirrors watch-backend.scm) for the compaction-cancel WIRING:
;       a genuinely-lagging unsynced watcher can't be forced deterministically over a
;       live cluster (catch-up promotes it synchronously at create), so — as ADR/the
;       task sanction for the hard case — we prove the shard apply-fn's
;       watch-check-compaction! wiring on a single ctx by running the EXACT sequence
;       apply-fn runs for a COMPACT, through the streaming actor's sexp wire path.
;
; Per-RUN unique WALL-CLOCK dir tag (NOT current-jiffy — see cw-u4a.39 / sim-cluster
; -smoke), so back-to-back runs get FRESH stores with no manual cleanup.

(include "test/harness.scm")

; ===========================================================================
; (B) SINGLE-CTX UNIT first (no cluster needed) — runs the streaming actor's wire
;     path + the compaction-cancel wiring directly, deterministically.
; ===========================================================================
; We pull in the backend + the streaming-actor's wire bridge directly on a single
; ctx.  No spawn-source here: we drive watch-register!/watch-on-apply!/COMPACT exactly
; as the shard apply-fn does, with a deliver-fn that flattens to the SAME sexp the
; real shard sends, then reconstruct via sexp->watch-response (the streaming actor's
; receive path).  This isolates the §5 mid-stream wiring + the record<->sexp bridge.
(include "src/store-ctx.scm")
(include "src/mvcc.scm")
(include "src/watch.scm")
(include "test/mvcc-util.scm")

(define (b s) (string->utf8 s))
(define U-TAG (number->string (exact (round (* 1000000 (current-second))))))
(define U-DB (string-append "/tmp/cws-watch-stream-unit-" U-TAG))
(define UCTX (make-ctx (store-open U-DB #t) "default" #t))
(reset-ctx! UCTX)

; A mock conn sink: the deliver-fn flattens each WatchResponse to the wire sexp
; (exactly the shard's deliver-fn), and we accumulate the RECONSTRUCTED responses
; (sexp->watch-response, the streaming actor's receive path) newest-first.
(define (make-wire-collector box)
  (lambda (wr)
    (let ((sexp (watch-response->sexp wr)))            ; record -> wire (cross-actor)
      (vector-set! box 0 (cons (sexp->watch-response sexp) (vector-ref box 0))))))  ; wire -> record
(define (wbox) (vector '()))
(define (wresponses box) (reverse (vector-ref box 0)))
(define (wcancels box)
  (let loop ((rs (wresponses box)) (out '()))
    (cond ((null? rs) (reverse out))
          ((wr-canceled? (car rs)) (loop (cdr rs) (cons (car rs) out)))
          (else (loop (cdr rs) out)))))

; mirror the shard apply-fn's COMPACT path: apply the COMPACT then (registry non-
; empty) run watch-check-compaction! — the wiring under test.
(define (apply-compact! reg ctx rev)
  (let ((res (mvcc-apply ctx (list (b "COMPACT") (b (number->string rev))))))
    (if (> (reg-count reg) 0) (watch-check-compaction! reg ctx))
    res))

(section "UNIT: compaction-cancel wiring (apply-fn COMPACT -> watch-check-compaction!)")
(reset-ctx! UCTX)
(mvcc-apply UCTX (list (b "PUT") (b "z") (b "v1")))   ; rev 1
(mvcc-apply UCTX (list (b "PUT") (b "z") (b "v2")))   ; rev 2
(mvcc-apply UCTX (list (b "PUT") (b "z") (b "v3")))   ; rev 3
(mvcc-apply UCTX (list (b "PUT") (b "z") (b "v4")))   ; rev 4
(define ureg (make-watch-registry))
(define ubox (wbox))
; register future-only at current (4): delivered_rev=4, synced.
(define uwid (watch-register! ureg UCTX
                              (list (cons 'key (b "")) (cons 'range-end (bytevector 0))
                                    (cons 'start-rev 0))
                              (make-wire-collector ubox)))
(check "unit: watcher delivered_rev=4 at create" 4 (w-delivered-rev (reg-get ureg uwid)))
; force it to LAG below a future compaction floor (the deterministic stand-in for an
; unsynced watcher still replaying when a Compact lands — ADR §5 mid-stream).
(set-w-delivered-rev! (reg-get ureg uwid) 3)
(set-w-synced?! (reg-get ureg uwid) #f)
(mvcc-apply UCTX (list (b "PUT") (b "z") (b "v5")))   ; rev 5
; COMPACT to 5 through the apply-fn wiring: compact-rev (5) > delivered_rev (3).
(check "unit: compact to 5 ok" (cons 'ok 5) (apply-compact! ureg UCTX 5))
(check "unit: lagging watcher canceled out of registry" 0 (reg-count ureg))
(let ((cs (wcancels ubox)))
  (check "unit: a canceled WatchResponse reached the conn (via the wire bridge)" 1 (length cs))
  (check "unit: canceled response carries the watch_id" uwid (wr-watch-id (car cs)))
  (check "unit: cancel carries compact_revision 5" 5 (wr-compact-revision (car cs))))

(section "UNIT: COMPACT with NO watchers is a true no-op (sim-cluster unperturbed)")
(reset-ctx! UCTX)
(mvcc-apply UCTX (list (b "PUT") (b "z") (b "v1")))   ; rev 1
(mvcc-apply UCTX (list (b "PUT") (b "z") (b "v2")))   ; rev 2
(define ureg0 (make-watch-registry))
(check "unit: empty registry => apply-compact! still returns ok" (cons 'ok 1)
       (apply-compact! ureg0 UCTX 1))
(check "unit: empty registry untouched" 0 (reg-count ureg0))

(section "UNIT: record<->sexp wire bridge round-trips a full events WatchResponse")
; an events response with a 2-field event (kv vector + #f prev) survives flatten+rebuild.
(let* ((kv (make-kv-view (b "k") 1 2 2 0 (b "val")))
       (we (make-watch-event 'put kv #f))
       (wr (make-watch-response 7 9 (list we) #f #f #f 0))
       (rt (sexp->watch-response (watch-response->sexp wr))))
  (check "bridge: watch_id" 7 (wr-watch-id rt))
  (check "bridge: header-rev" 9 (wr-header-rev rt))
  (check "bridge: created? #f" #f (wr-created? rt))
  (check "bridge: one event" 1 (length (wr-events rt)))
  (check "bridge: event type put" 'put (we-type (car (wr-events rt))))
  (check "bridge: event kv value" "val" (utf8->string (kvv-value (we-kv (car (wr-events rt))))))
  (check "bridge: event prev-kv #f" #f (we-prev-kv (car (wr-events rt)))))

; ===========================================================================
; (A) CLUSTER INTEGRATION — 3-voter in-process cluster (mirrors sim-cluster-smoke)
; ===========================================================================
(make-table 'ws-shard-pid "set")
(make-table 'ws-shard-role "set")
(make-table 'ws-shard-leader "set")
(make-table 'ws-shard-commit "set")
(make-table 'ws-shard-applied "set")
(make-table 'ws-test "set")
; output sinks for the mock conns: each conn's watch-stream actor appends its wire
; WatchResponses here, keyed by a per-conn tag string.
(make-table 'ws-watch-out "set")

(for-each node-make (list "a" "b" "c"))
(node-link! "a" "b") (node-link! "a" "c") (node-link! "b" "c")

(define CL-TAG (number->string (exact (round (* 1000000 (current-second))))))
(define (db-dir nd)
  (string-append "/tmp/cws-watch-stream-cl-" CL-TAG "-" (symbol->string nd) "-s0"))
(for-each
 (lambda (nd)
   (spawn-source "(include \"src/server/shard-actor.scm\")" 'shard-main
                 "0" '(a b c) nd (db-dir nd) #f))
 '(a b c))
(for-each
 (lambda (nd)
   (spawn-source "(include \"src/server/peer-poller.scm\")" 'peer-poller
                 nd '("0") 150 '() 0))
 '(a b c))

(define (role nd)      (table-lookup 'ws-shard-role (string-append nd ":0")))
(define (applied nd)   (let ((a (table-lookup 'ws-shard-applied (string-append nd ":0")))) (if a a 0)))
(define (shard-pid nd) (table-lookup 'ws-shard-pid (string-append nd ":0")))
; COOPERATIVE spin (vs sim-cluster-smoke's tight busy-wait): this test spawns FOUR
; long-lived watch-stream conn actors + a driver on the green pool on top of the 3
; shard + 3 poller actors, so a tight main-thread busy-loop here can starve the
; leader's peer-poller of CPU long enough to MISS its heartbeat/CheckQuorum window
; -> spurious leadership churn -> the leader conn1 registered against steps down and
; later PUTs get 'tryagain (observed as flakiness on an unlucky schedule).  On the
; main (non-actor) thread (yield) is thread::yield_now() and (sleep-ms 1) is a real
; thread::sleep — both hand the core to a ready green worker, so the pollers always
; get to heartbeat.  We (yield) every iteration (cheap) and (sleep-ms 1) periodically
; as a hard guarantee the green pool drains.  Iteration cap is lower now that each
; pass cooperatively waits ~a scheduler quantum.
(define (spin pred who)
  (let loop ((i 0))
    (cond ((pred) #t)
          ((> i 8000000) (error (string-append "timeout: " who)))   ; ~40s of 1ms naps max
          (else
           (yield)
           (if (= 0 (modulo i 200)) (sleep-ms 1))
           (loop (+ i 1))))))
(define (leader-node)
  (cond ((eq? (role "a") 'leader) "a")
        ((eq? (role "b") 'leader) "b")
        ((eq? (role "c") 'leader) "c")
        (else #f)))
(define (follower-node ldr)
  (car (filter (lambda (nd) (not (string=? nd ldr))) '("a" "b" "c"))))

(section "CLUSTER: leader election")
(spin (lambda () (leader-node)) "leader election")
(define LDR (leader-node))
(display "  leader elected: ") (display LDR) (newline)
(check "a leader emerged" #t (and (member LDR '("a" "b" "c")) #t))
(table-insert! 'ws-test "ldr" LDR)

; ---- a CLIENT actor that drives proposals at the leader via its real PID reply
;      path (sim-cluster-smoke idiom); writes each ack into ws-test.
(define client-src "
  (define (b s) (string->utf8 s))
  (define (ask pid msg) (send pid msg) (raw-receive))
  (define (propose-to ldr cmd)
    (ask (table-lookup 'ws-shard-pid (string-append ldr \":0\")) (cons (self) cmd)))
  (define (client)
    (let ((ldr (table-lookup 'ws-test \"ldr\")))
      (let loop ((i 0))
        (let ((job (table-lookup 'ws-test \"job\")))
          (cond
            ((eq? job 'done) #t)
            ((and (pair? job) (eq? (car job) 'put))
             ; (put SEQ K V) -> propose, stash ack under (\"ack\" . SEQ), clear job
             (let ((r (propose-to ldr (list (b \"PUT\") (b (cadr (cdr job))) (b (cadddr job))))))
               (table-insert! 'ws-test (string-append \"ack\" (number->string (cadr job))) r)
               (table-insert! 'ws-test \"job\" #f)
               (loop i)))
            (else (yield) (loop i)))))))")
(spawn-source client-src 'client)

; drive one PUT through the leader and wait for the ack.
(define put-seq 0)
(define (put! k v)
  (set! put-seq (+ put-seq 1))
  (table-insert! 'ws-test (string-append "ack" (number->string put-seq)) #f)
  (table-insert! 'ws-test "job" (list 'put put-seq k v))
  (spin (lambda () (table-lookup 'ws-test (string-append "ack" (number->string put-seq))))
        (string-append "put " k))
  (table-lookup 'ws-test (string-append "ack" (number->string put-seq))))

; ---- spawn a watch-stream actor (a mock CONN) against the leader's shard, sinking
;      its wire WatchResponses into ws-watch-out under OUT-TAG.
(define (spawn-conn out-tag)
  (spawn-source "(include \"src/server/watch-stream.scm\")" 'watch-stream
                (shard-pid LDR) 'ws-watch-out out-tag))

; ---- a tiny DRIVER actor: relays a control request to a watch-stream conn actor
;      and stashes the conn's reply into ws-test under RES-KEY (so the main thread
;      can await it).  Crucially the watch-stream create/cancel reply-pid must be the
;      DRIVER's own PID — so the driver substitutes (self) into the message it
;      forwards (the main thread can't supply a usable reply PID; it isn't an actor).
;      drv-cmd shape from the main thread: (RES-KEY CONN-PID KIND . ARGS).
(define driver-src "
  (define (driver)
    (let loop ()
      (let ((cmd (table-lookup 'ws-test \"drv-cmd\")))
        (cond
          ((eq? cmd 'stop) #t)
          ((pair? cmd)
           (table-insert! 'ws-test \"drv-cmd\" #f)
           (let ((res-key (car cmd)) (conn (cadr cmd)) (kind (caddr cmd)) (rest (cdddr cmd)))
             ; kind = 'create -> (watch-create (self) CLIENT-WID WIRE-START SPEC-ALIST)
             ; kind = 'cancel -> (watch-cancel-req (self) WID)
             (cond
               ((eq? kind 'create)
                (send conn (list 'watch-create (self) (car rest) (cadr rest) (caddr rest))))
               ((eq? kind 'cancel)
                (send conn (list 'watch-cancel-req (self) (car rest)))))
             (let ((r (raw-receive)))
               (table-insert! 'ws-test res-key r)))
           (loop))
          (else (yield) (loop))))))")
(spawn-source driver-src 'driver)

; issue a create/cancel via the driver and await its relayed reply.
(define (drv! res-key conn . parts)
  (table-insert! 'ws-test res-key #f)
  (table-insert! 'ws-test "drv-cmd" (cons res-key (cons conn parts)))
  (spin (lambda () (table-lookup 'ws-test res-key)) (string-append "drv " res-key))
  (table-lookup 'ws-test res-key))

; read back a conn's wire WatchResponses (oldest-first) from the output sink.
(define (conn-responses out-tag)
  (let ((cur (table-lookup 'ws-watch-out out-tag)))
    (reverse (if cur cur '()))))
; flatten a conn's delivered events to (watch-id type key-string mod-rev) tuples,
; in delivery order, skipping created/canceled (event-less) frames.  Wire sexp shape:
;   (WID HEADER CREATED? CANCELED? REASON COMPACT ((TYPE KVVEC PREV) ...))
(define (conn-event-tuples out-tag)
  (let loop ((rs (conn-responses out-tag)) (out '()))
    (if (null? rs)
        (reverse out)
        (let* ((wr (car rs)) (wid (list-ref wr 0)) (evs (list-ref wr 6)))
          (loop (cdr rs)
                (append
                 (reverse
                  (map (lambda (ev)
                         (let ((kv (cadr ev)))   ; kv-view vector #(key cr mr ver lease val)
                           (list wid (car ev) (utf8->string (vector-ref kv 0)) (vector-ref kv 2))))
                       evs))
                 out))))))
(define (conn-created out-tag)
  (filter (lambda (wr) (list-ref wr 2)) (conn-responses out-tag)))
(define (conn-canceled out-tag)
  (filter (lambda (wr) (list-ref wr 3)) (conn-responses out-tag)))

; ---------------------------------------------------------------------------
(section "CLUSTER: build history, watch from start_rev>0 (replay) + live, in order")
; build revs 1..4 BEFORE the watch (the historical set).  We then watch from the
; etcd-wire start_revision = 2 (INCLUSIVE) — the normal client resume shape: a
; resuming client passes start_revision = lastSeen+1, here lastSeen = 1.  The wire
; ±1 adaptation (.14) maps wire 2 -> internal EXCLUSIVE 1 (deliver mod_rev > 1), so
; replay = revs 2,3,4.  (Wire 0 is the future-only sentinel; wire 1 = "from genesis"
; folds onto the backend's internal-0 future-only path — an inherent backend edge,
; so the resume case uses wire >= 2, which is what real clients send.)
(check "put k1=v1 -> rev 1" (cons "PUT" 1) (put! "k1" "v1"))
(check "put k2=v2 -> rev 2" (cons "PUT" 2) (put! "k2" "v2"))
(check "put k3=v3 -> rev 3" (cons "PUT" 3) (put! "k3" "v3"))
(check "put k4=v4 -> rev 4" (cons "PUT" 4) (put! "k4" "v4"))

(define conn1 (spawn-conn "c1"))
; WatchCreate, client-wid 100, WIRE start_rev=2 (inclusive) => internal exclusive 1
; => replay revs (1,4] = 2,3,4 (all keys), then live.  range = all keys.
(check "conn1 create wid 100 (replay from wire rev 2)"
       (cons 'watch-create-ok 100)
       (drv! "r-c1" conn1 'create 100 2
             (list (cons 'key (b "")) (cons 'range-end (bytevector 0)))))
; replay is delivered synchronously during register, before the create ack — so by
; the time the ack lands, the historical frames are already in the sink.
(check "conn1 got an immediate CREATED frame" 1 (length (conn-created "c1")))
(check "conn1 replay delivered revs 2,3,4 (all tagged wid 100), in order"
       (list '(100 put "k2" 2) '(100 put "k3" 3) '(100 put "k4" 4))
       (conn-event-tuples "c1"))

; now LIVE applies — conn1 must get exactly 5,6 next, each once, never re-deliver <=4.
(check "put k5=v5 -> rev 5" (cons "PUT" 5) (put! "k5" "v5"))
(check "put k1=v1b -> rev 6" (cons "PUT" 6) (put! "k1" "v1b"))
; spin until both live events have propagated to the sink (delivery is async via the
; shard's apply hook -> our mailbox -> sink).
(spin (lambda () (>= (length (conn-event-tuples "c1")) 5)) "conn1 live events delivered")
(check "conn1 total = replay(2,3,4) + live(5,6), in order, each once"
       (list '(100 put "k2" 2) '(100 put "k3" 3) '(100 put "k4" 4)
             '(100 put "k5" 5) '(100 put "k1" 6))
       (conn-event-tuples "c1"))

; ---------------------------------------------------------------------------
(section "CLUSTER: multiplex — two watch_ids, different ranges, one conn")
(define conn2 (spawn-conn "c2"))
; wid 200 watches [m, n)  (range-end "n") ; wid 201 watches single key "z".
; both future-only (wire start 0) so they pick up only the live burst below.
(check "conn2 create wid 200 range [m,n)"
       (cons 'watch-create-ok 200)
       (drv! "r-c2a" conn2 'create 200 0
             (list (cons 'key (b "m")) (cons 'range-end (b "n")))))
(check "conn2 create wid 201 single key z"
       (cons 'watch-create-ok 201)
       (drv! "r-c2b" conn2 'create 201 0
             (list (cons 'key (b "z")) (cons 'range-end #f))))
(check "conn2 has two CREATED frames" 2 (length (conn-created "c2")))
; one burst touching both ranges + neither.
(put! "m1" "v")    ; -> wid 200
(put! "mz" "v")    ; -> wid 200 (still < "n")
(put! "z"  "v")    ; -> wid 201
(put! "q"  "v")    ; -> neither
(spin (lambda () (>= (length (conn-event-tuples "c2")) 3)) "conn2 multiplexed events delivered")
; each watch_id gets ONLY its subset, tagged by watch_id; "q" reaches neither.
(let ((tuples (conn-event-tuples "c2")))
  (check "conn2 wid 200 got only [m,n) keys m1,mz"
         (list "m1" "mz")
         (map caddr (filter (lambda (t) (= (car t) 200)) tuples)))
  (check "conn2 wid 201 got only key z"
         (list "z")
         (map caddr (filter (lambda (t) (= (car t) 201)) tuples)))
  (check "conn2 total multiplexed events = 3 (q dropped)" 3 (length tuples)))

; ---------------------------------------------------------------------------
(section "CLUSTER: cancel — no further events after cancel; a canceled frame sent")
; cancel wid 200; subsequent [m,n) writes must NOT reach conn2 for wid 200.
(check "cancel wid 200" (cons 'watch-cancel-ok 200) (drv! "r-cxl" conn2 'cancel 200))
(spin (lambda () (>= (length (conn-canceled "c2")) 1)) "conn2 canceled frame arrived")
(check "conn2 got a canceled frame for wid 200" #t
       (and (memv 200 (map (lambda (wr) (list-ref wr 0)) (conn-canceled "c2"))) #t))
(define c2-events-before (length (conn-event-tuples "c2")))
(put! "m9" "v")    ; was in [m,n) — but wid 200 is canceled
(put! "z"  "v2")   ; -> wid 201 still live (proves the conn itself is fine)
(spin (lambda ()
        (> (length (filter (lambda (t) (= (car t) 201)) (conn-event-tuples "c2"))) 1))
      "conn2 wid 201 still delivering after wid 200 cancel")
(check "no NEW wid-200 events after cancel"
       '()
       (filter (lambda (t) (and (= (car t) 200)
                                (member (caddr t) (list "m9"))))
               (conn-event-tuples "c2")))

; ---------------------------------------------------------------------------
(section "CLUSTER: leader-gating — WatchCreate to a NON-leader is redirected")
(define FOLL (follower-node LDR))
(display "  follower: ") (display FOLL) (newline)
(define conn3
  (spawn-source "(include \"src/server/watch-stream.scm\")" 'watch-stream
                (shard-pid FOLL) 'ws-watch-out "c3"))
; create against the FOLLOWER's shard -> not served, redirected with the leader name.
(let ((r (drv! "r-c3" conn3 'create 300 0
               (list (cons 'key (b "")) (cons 'range-end (bytevector 0))))))
  (check "non-leader create redirected (not silently served)"
         'watch-create-not-leader (car r))
  (check "redirect names a leader (or #f if unknown)" #t
         (or (not (cdr r)) (and (member (symbol->string (cdr r)) '("a" "b" "c")) #t))))
(check "no watcher established on the follower's conn (no CREATED frame)"
       0 (length (conn-created "c3")))

; ---------------------------------------------------------------------------
(section "CLUSTER: progress_notify — idle synced watch gets an empty advancing frame")
(define conn4 (spawn-conn "c4"))
; future-only watch on a key that will NEVER be written, WITH progress-notify.
(check "conn4 create wid 400 (progress_notify, idle key)"
       (cons 'watch-create-ok 400)
       (drv! "r-c4" conn4 'create 400 0
             (list (cons 'key (b "idle-")) (cons 'range-end (b "idle."))
                   (cons 'progress-notify #t))))
; advance the cluster's current-rev with an UNRELATED write (out of the watch range),
; so the watch is idle but the store moved on.
(define rev-before-progress (cdr (put! "unrelated" "v")))
(check "an unrelated PUT advanced current-rev" #t (> rev-before-progress 0))
; drive a progress tick carrying the live current-rev (the .23 progress timer / test
; supplies it).  The idle synced progress watcher must get an empty frame at that rev.
(send conn4 (list 'progress-tick rev-before-progress))
(spin (lambda ()
        (let ((rs (conn-responses "c4")))
          ; look for an EMPTY (no events) NON-created NON-canceled frame at the rev.
          (and (pair? rs)
               (let scan ((rs rs))
                 (cond ((null? rs) #f)
                       ((let ((wr (car rs)))
                          (and (= (list-ref wr 0) 400)
                               (not (list-ref wr 2)) (not (list-ref wr 3))
                               (null? (list-ref wr 6))
                               (= (list-ref wr 1) rev-before-progress)))
                        #t)
                       (else (scan (cdr rs))))))))
      "conn4 progress frame")
(let* ((rs (conn-responses "c4"))
       (prog (let scan ((rs rs))
               (cond ((null? rs) #f)
                     ((let ((wr (car rs)))
                        (and (= (list-ref wr 0) 400)
                             (not (list-ref wr 2)) (not (list-ref wr 3))
                             (null? (list-ref wr 6)))) (car rs))
                     (else (scan (cdr rs)))))))
  (check "conn4 got a progress frame (empty events) for wid 400" #t (and prog #t))
  (check "conn4 progress frame header.revision advanced to current-rev"
         rev-before-progress (list-ref prog 1)))

(done!)
