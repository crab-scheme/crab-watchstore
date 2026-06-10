; test/watch-backend.scm — unit test for the Watch backend (cw-u4a.13): the watcher
; registry + the unsynced->synced replay->live handoff + live apply-dispatch, exactly
; per ADR 0002 §3.  STANDALONE backend logic — no streaming actor / gRPC (those are
; .14/.23): the consumer is a MOCK deliver-fn that appends WatchResponses to a list.
;
; Drives a single durable ctx over a real RocksDB.  Builds history with mvcc-apply
; (so the REV-CF events are exactly what production writes), registers watchers via
; watch-register!, simulates live applies by calling mvcc-apply then watch-on-apply!
; with the pre/post current-rev (the same hook the shard actor adds), and asserts
; the delivered events are EXACTLY the right set, in revision order, each once.
;
; A fresh per-run temp dir ((current-jiffy)) + reset-ctx! make this green twice
; back-to-back with no external cleanup (see test/mvcc-util.scm).

(include "test/harness.scm")
(include "src/store-ctx.scm")
(include "src/mvcc.scm")
(include "src/watch.scm")
(include "test/mvcc-util.scm")

; ---- open a fresh store ----
(define run-tag (number->string (current-jiffy)))
(define DB-PATH (string-append "/tmp/cws-watch-backend-" run-tag))
(define CTX (make-ctx (store-open DB-PATH #t) "default" #t))
(reset-ctx! CTX)

; ---- helpers ----
(define (b s) (string->utf8 s))

; Apply a command AND fire the live watch dispatch with the pre/post current-rev,
; exactly as the shard-actor hook does.  Returns mvcc-apply's result.
(define (apply! reg . parts)
  (let* ((pre (mvcc-current-rev CTX))
         (res (mvcc-apply CTX (map b parts)))
         (post (mvcc-current-rev CTX)))
    (watch-on-apply! reg CTX pre post)
    res))

; Apply WITHOUT firing watch dispatch — used to build pure history before any
; watcher is registered (replay must pick these up).
(define (hist . parts) (mvcc-apply CTX (map b parts)))

; ---- mock collector: a deliver-fn + readback over a captured list ----
; Returns a deliver-fn closure; the responses accumulate in `box` (a 1-vector).
(define (make-collector box)
  (lambda (wr) (vector-set! box 0 (cons wr (vector-ref box 0)))))
(define (new-box) (vector '()))
(define (responses box) (reverse (vector-ref box 0)))   ; oldest-first

; Flatten all WatchResponses' events into a flat list of (type key-string mod-rev)
; triples, in delivery order.  Skips created/canceled (event-less) frames.
(define (delivered-triples box)
  (let loop ((rs (responses box)) (out '()))
    (if (null? rs)
        (reverse out)
        (let ((evs (wr-events (car rs))))
          (loop (cdr rs)
                (append (reverse (map we->triple evs)) out))))))

(define (we->triple we)
  (let ((kv (we-kv we)))
    (list (we-type we) (utf8->string (kvv-key kv)) (kvv-mod-rev kv))))

; All canceled WatchResponses in `box`.
(define (cancels box)
  (let loop ((rs (responses box)) (out '()))
    (cond ((null? rs) (reverse out))
          ((wr-canceled? (car rs)) (loop (cdr rs) (cons (car rs) out)))
          (else (loop (cdr rs) out)))))

; ===========================================================================
(section "future-only watch (start_rev=0): only NEW events, in order, each once")
; Build some history FIRST; a future-only watch must NOT replay it.
(hist "PUT" "k1" "old1")   ; rev 1
(hist "PUT" "k2" "old2")   ; rev 2
(check "current-rev = 2 before register" 2 (mvcc-current-rev CTX))

(define reg1 (make-watch-registry))
(define box1 (new-box))
(define wid1 (watch-register! reg1 CTX
                              (list (cons 'key (b "")) (cons 'range-end (bytevector 0))
                                    (cons 'start-rev 0))
                              (make-collector box1)))
(check "future watcher got an id" #t (and (integer? wid1) #t))
(check "future watcher registered" 1 (reg-count reg1))
(check "future watcher synced immediately" #t (w-synced? (reg-get reg1 wid1)))
(check "future watcher delivered_rev = current (2)" 2 (w-delivered-rev (reg-get reg1 wid1)))
(check "no replay delivered to future-only watcher" '() (delivered-triples box1))

; Now live applies — the watcher gets ONLY these, in order.
(apply! reg1 "PUT" "k3" "v3")   ; rev 3
(apply! reg1 "PUT" "k1" "v1b")  ; rev 4 (update of pre-existing k1)
(apply! reg1 "DEL" "k2")        ; rev 5
(check "future watcher live events 3,4,5 in order"
       (list '(put "k3" 3) '(put "k1" 4) '(del "k2" 5))
       (delivered-triples box1))

; ===========================================================================
(section "historical replay: register at start_rev=k -> immediately get (k, current]")
(reset-ctx! CTX)
; Build revs 1..6.
(hist "PUT" "/a/1" "v1")   ; rev 1
(hist "PUT" "/a/2" "v2")   ; rev 2
(hist "PUT" "/b/1" "v3")   ; rev 3
(hist "PUT" "/a/1" "v1b")  ; rev 4 (update /a/1)
(hist "DEL" "/a/2")        ; rev 5 (tombstone /a/2)
(hist "PUT" "/c"   "v6")   ; rev 6
(check "history current-rev = 6" 6 (mvcc-current-rev CTX))

(define reg2 (make-watch-registry))
(define box2 (new-box))
; all-keys, start_rev=2 (exclusive) => replay events 3,4,5,6 in order.
(define wid2 (watch-register! reg2 CTX
                              (list (cons 'key (b "")) (cons 'range-end (bytevector 0))
                                    (cons 'start-rev 2))
                              (make-collector box2)))
(check "replay watcher synced after catch-up" #t (w-synced? (reg-get reg2 wid2)))
(check "replay watcher delivered_rev = current (6)" 6 (w-delivered-rev (reg-get reg2 wid2)))
(check "replay delivered events 3..6 in revision order"
       (list '(put "/b/1" 3) '(put "/a/1" 4) '(del "/a/2" 5) '(put "/c" 6))
       (delivered-triples box2))

; Assert the FULL KeyValue is correct on a replayed event (the /a/1 update @4).
(define (find-event box pred)
  (let loop ((rs (responses box)))
    (if (null? rs) #f
        (let scan ((evs (wr-events (car rs))))
          (cond ((null? evs) (loop (cdr rs)))
                ((pred (car evs)) (car evs))
                (else (scan (cdr evs))))))))
(let ((we (find-event box2 (lambda (we)
                             (and (eq? (we-type we) 'put)
                                  (equal? (kvv-key (we-kv we)) (b "/a/1")))))) )
  (check "replayed /a/1@4 value v1b" "v1b" (utf8->string (kvv-value (we-kv we))))
  (check "replayed /a/1@4 create_rev 1 (update keeps create)" 1 (kvv-create-rev (we-kv we)))
  (check "replayed /a/1@4 mod_rev 4" 4 (kvv-mod-rev (we-kv we)))
  (check "replayed /a/1@4 version 2" 2 (kvv-version (we-kv we))))

; ===========================================================================
(section "exactly-once across the replay->live seam (the CRUX, ADR §3)")
(reset-ctx! CTX)
; History revs 1..4 (C = current = 4 at register time).
(hist "PUT" "x" "v1")   ; rev 1
(hist "PUT" "x" "v2")   ; rev 2
(hist "PUT" "y" "v3")   ; rev 3
(hist "PUT" "x" "v4")   ; rev 4
(check "seam history current = 4" 4 (mvcc-current-rev CTX))

(define reg3 (make-watch-registry))
(define box3 (new-box))
; all-keys, start_rev=1 (exclusive): replay must deliver (1,4] = revs 2,3,4.
(define wid3 (watch-register! reg3 CTX
                              (list (cons 'key (b "")) (cons 'range-end (bytevector 0))
                                    (cons 'start-rev 1))
                              (make-collector box3)))
(check "seam replay delivered (1,4] = 2,3,4"
       (list '(put "x" 2) '(put "y" 3) '(put "x" 4))
       (delivered-triples box3))
(check "seam watcher delivered_rev = 4 after replay" 4 (w-delivered-rev (reg-get reg3 wid3)))

; Now live-apply C+1, C+2 — live must deliver EXACTLY 5,6 and NEVER re-deliver <=4.
(apply! reg3 "PUT" "x" "v5")   ; rev 5
(apply! reg3 "DEL" "y")        ; rev 6
(check "seam total delivered = (1,6] with NO duplicate"
       (list '(put "x" 2) '(put "y" 3) '(put "x" 4) '(put "x" 5) '(del "y" 6))
       (delivered-triples box3))
(check "seam watcher delivered_rev = 6 after live" 6 (w-delivered-rev (reg-get reg3 wid3)))

; ===========================================================================
(section "prefix / range / single-key scoping")
(reset-ctx! CTX)
(hist "PUT" "/a/1" "a1")   ; rev 1
(hist "PUT" "/a/2" "a2")   ; rev 2
(hist "PUT" "/b/1" "b1")   ; rev 3
(define reg4 (make-watch-registry))
; prefix watcher on /a/  (range-end = /a0), start_rev=0 -> future only.
(define boxA (new-box))
(define widA (watch-register! reg4 CTX
                              (list (cons 'key (b "/a/")) (cons 'range-end (b "/a0"))
                                    (cons 'start-rev 0))
                              (make-collector boxA)))
; single-key watcher on /b/1
(define boxB (new-box))
(define widB (watch-register! reg4 CTX
                              (list (cons 'key (b "/b/1")) (cons 'range-end #f)
                                    (cons 'start-rev 0))
                              (make-collector boxB)))
; live applies touching both ranges
(apply! reg4 "PUT" "/a/1" "a1b")   ; rev 4 -> prefix only
(apply! reg4 "PUT" "/b/1" "b1b")   ; rev 5 -> single-key only
(apply! reg4 "PUT" "/a/3" "a3")    ; rev 6 -> prefix only
(apply! reg4 "PUT" "/c"   "c1")    ; rev 7 -> NEITHER
(check "prefix /a/ watcher got only /a/ events (4,6)"
       (list '(put "/a/1" 4) '(put "/a/3" 6))
       (delivered-triples boxA))
(check "single-key /b/1 watcher got only its key (5)"
       (list '(put "/b/1" 5))
       (delivered-triples boxB))

; ===========================================================================
(section "NOPUT / NODELETE filters drop the filtered kind")
(reset-ctx! CTX)
(define reg5 (make-watch-registry))
; NOPUT watcher (deletes only)
(define boxNP (new-box))
(define widNP (watch-register! reg5 CTX
                               (list (cons 'key (b "")) (cons 'range-end (bytevector 0))
                                     (cons 'start-rev 0) (cons 'filters '(noput)))
                               (make-collector boxNP)))
; NODELETE watcher (puts only)
(define boxND (new-box))
(define widND (watch-register! reg5 CTX
                               (list (cons 'key (b "")) (cons 'range-end (bytevector 0))
                                     (cons 'start-rev 0) (cons 'filters '(nodelete)))
                               (make-collector boxND)))
(apply! reg5 "PUT" "f1" "v1")   ; rev 1
(apply! reg5 "DEL" "f1")        ; rev 2
(apply! reg5 "PUT" "f2" "v3")   ; rev 3
(check "NOPUT watcher got only the DELETE (2)"
       (list '(del "f1" 2))
       (delivered-triples boxNP))
(check "NODELETE watcher got only the PUTs (1,3)"
       (list '(put "f1" 1) '(put "f2" 3))
       (delivered-triples boxND))

; ===========================================================================
(section "prev_kv: update carries prior value; create carries #f prev")
(reset-ctx! CTX)
(define reg6 (make-watch-registry))
(define box6 (new-box))
(define wid6 (watch-register! reg6 CTX
                              (list (cons 'key (b "")) (cons 'range-end (bytevector 0))
                                    (cons 'start-rev 0) (cons 'prev-kv #t))
                              (make-collector box6)))
(apply! reg6 "PUT" "p" "first")    ; rev 1 -> create, prev #f
(apply! reg6 "PUT" "p" "second")   ; rev 2 -> update, prev = "first"
(apply! reg6 "DEL" "p")            ; rev 3 -> delete, prev = "second"
(let ((create-ev (find-event box6 (lambda (we) (= (kvv-mod-rev (we-kv we)) 1))))
      (update-ev (find-event box6 (lambda (we) (= (kvv-mod-rev (we-kv we)) 2))))
      (delete-ev (find-event box6 (lambda (we) (= (kvv-mod-rev (we-kv we)) 3)))))
  (check "create @1 prev_kv is #f" #f (we-prev-kv create-ev))
  (check "update @2 prev_kv present" #t (and (we-prev-kv update-ev) #t))
  (check "update @2 prev_kv value = first" "first"
         (utf8->string (kvv-value (we-prev-kv update-ev))))
  (check "update @2 prev_kv mod_rev = 1" 1 (kvv-mod-rev (we-prev-kv update-ev)))
  (check "delete @3 prev_kv value = second" "second"
         (utf8->string (kvv-value (we-prev-kv delete-ev)))))

; ===========================================================================
(section "cancel: after cancel no further delivery; a canceled response was sent")
(reset-ctx! CTX)
(define reg7 (make-watch-registry))
(define box7 (new-box))
(define wid7 (watch-register! reg7 CTX
                              (list (cons 'key (b "")) (cons 'range-end (bytevector 0))
                                    (cons 'start-rev 0))
                              (make-collector box7)))
(apply! reg7 "PUT" "c1" "v1")   ; rev 1 -> delivered
(check "before cancel: got event 1" (list '(put "c1" 1)) (delivered-triples box7))
(check "cancel returns #t" #t (watch-cancel! reg7 wid7))
(check "registry empty after cancel" 0 (reg-count reg7))
(check "canceled WatchResponse sent" 1 (length (cancels box7)))
(check "canceled response carries the watch_id" wid7 (wr-watch-id (car (cancels box7))))
; further applies deliver NOTHING (watcher is gone)
(apply! reg7 "PUT" "c2" "v2")   ; rev 2 -> nobody listening
(check "no further events after cancel" (list '(put "c1" 1)) (delivered-triples box7))

; ===========================================================================
(section "ErrCompacted: at creation (start_rev < compact) + mid-stream lagging cancel")
(reset-ctx! CTX)
(hist "PUT" "z" "v1")   ; rev 1
(hist "PUT" "z" "v2")   ; rev 2
(hist "PUT" "z" "v3")   ; rev 3
(hist "PUT" "z" "v4")   ; rev 4
; compact to rev 3 (GCs REV-CF <= 3, sets compact-rev = 3).
(check "compact to 3 ok" (cons 'ok 3) (mvcc-apply CTX (list (b "COMPACT") (b "3"))))
(check "compact-rev = 3" 3 (mvcc-compact-rev CTX))

(define reg8 (make-watch-registry))
(define box8 (new-box))
; (a) start_rev=1 < compact 3 => compacted result, NO watcher created.
(check "register at start_rev 1 (< compact 3) -> compacted 3"
       (cons 'compacted 3)
       (watch-register! reg8 CTX
                        (list (cons 'key (b "")) (cons 'range-end (bytevector 0))
                              (cons 'start-rev 1))
                        (make-collector box8)))
(check "no watcher created on compacted register" 0 (reg-count reg8))

; (b) mid-stream: register a future watcher (synced), then a NEW compaction passes
; its delivered_rev -> watch-check-compaction! cancels it.
; First put the watcher's delivered_rev BELOW a future compaction floor.  Register
; future-only at current (4): delivered_rev = 4, synced.  Then grow history and
; compact past 4 so the watcher lags.
(define box8b (new-box))
(define wid8b (watch-register! reg8 CTX
                               (list (cons 'key (b "")) (cons 'range-end (bytevector 0))
                                     (cons 'start-rev 0))
                               (make-collector box8b)))
(check "future watcher registered, delivered_rev=4" 4 (w-delivered-rev (reg-get reg8 wid8b)))
; Manually drop delivered_rev to simulate an unsynced watcher lagging below a new
; compaction floor (a clean unit-level check of the §5 mid-stream gate).
(set-w-delivered-rev! (reg-get reg8 wid8b) 3)
(set-w-synced?! (reg-get reg8 wid8b) #f)   ; treat as still-catching-up
; grow + compact to 5 so compact-rev (5) > the lagging watcher's delivered_rev (3).
(hist "PUT" "z" "v5")   ; rev 5
(check "compact to 5 ok" (cons 'ok 5) (mvcc-apply CTX (list (b "COMPACT") (b "5"))))
(check "compact-rev = 5" 5 (mvcc-compact-rev CTX))
(let ((canceled (watch-check-compaction! reg8 CTX)))
  (check "check-compaction canceled the lagging watcher" (list wid8b) canceled))
(check "lagging watcher removed from registry" 0 (reg-count reg8))
(let ((cs (cancels box8b)))
  (check "lagging watcher got a canceled response" 1 (length cs))
  (check "cancel carries compact_revision 5" 5 (wr-compact-revision (car cs))))

; ===========================================================================
(section "multi-watcher: two watchers, different ranges, each correct subset from one apply")
(reset-ctx! CTX)
(define reg9 (make-watch-registry))
(define boxL (new-box))   ; watches [k1, k3)  -> k1, k2
(define widL (watch-register! reg9 CTX
                              (list (cons 'key (b "k1")) (cons 'range-end (b "k3"))
                                    (cons 'start-rev 0))
                              (make-collector boxL)))
(define boxR (new-box))   ; watches single key k5
(define widR (watch-register! reg9 CTX
                              (list (cons 'key (b "k5")) (cons 'range-end #f)
                                    (cons 'start-rev 0))
                              (make-collector boxR)))
(check "two watchers registered" 2 (reg-count reg9))
; One burst of applies; each watcher gets only its subset.
(apply! reg9 "PUT" "k1" "v1")   ; rev 1 -> L
(apply! reg9 "PUT" "k2" "v2")   ; rev 2 -> L
(apply! reg9 "PUT" "k5" "v5")   ; rev 3 -> R
(apply! reg9 "PUT" "k9" "v9")   ; rev 4 -> neither
(check "left watcher [k1,k3) got k1,k2 (1,2)"
       (list '(put "k1" 1) '(put "k2" 2))
       (delivered-triples boxL))
(check "right watcher {k5} got only k5 (3)"
       (list '(put "k5" 3))
       (delivered-triples boxR))

(done!)
