; src/watch.scm — the Watch backend: revision event log + watcher registry
; (cw-u4a.13).  Implements ADR 0002 §3 (the gap-free unsynced->synced replay->live
; handoff), §2 (option b: keep the lean REV-CF event, RECONSTRUCT the full KeyValue
; + prev_kv at delivery via mvcc-get-latest), §4 (apply-side dispatch), and §5
; (ErrCompacted at creation + mid-stream).
;
; This is STANDALONE-testable backend logic: the per-connection streaming actor +
; gRPC live in .14/.23.  The CONSUMER is abstracted behind an opaque `deliver-fn`
; (a procedure (deliver-fn watch-response) -> unspec), so the backend is driven by a
; mock collector in test/watch-backend.scm and by a real "send to the conn mailbox"
; deliver-fn in .14.
;
; Depends (all already loaded by the includer, AFTER src/mvcc.scm) on:
;   mvcc-watch-events / mvcc-get-latest / mvcc-current-rev / mvcc-compact-rev,
;   ev-kind/ev-key/ev-value/ev-mod-rev, EV-PUT/EV-DELETE,
;   kv-rec-create-rev/kv-rec-mod-rev/kv-rec-version/kv-rec-lease/kv-rec-value,
;   range-in-range? / event-passes-filter?.
;
; NO change to the REV-CF write format; this is a pure additive read + bookkeeping.

; ===========================================================================
; WatchResponse — one frame to the client stream (ADR 0002 §1)
; ===========================================================================
;
; A record (not a bare list) so .14 and the tests introspect by accessor and never
; depend on positional layout.  `events` is a list of watch-event records (below),
; revision-ascending.  created? / canceled? are the etcd ack/teardown flags;
; cancel-reason + compact-revision accompany a cancel.
(define-record-type watch-response
  (make-watch-response watch-id header-rev events created? canceled? cancel-reason compact-revision)
  watch-response?
  (watch-id        wr-watch-id)
  (header-rev      wr-header-rev)
  (events          wr-events)
  (created?        wr-created?)
  (canceled?       wr-canceled?)
  (cancel-reason   wr-cancel-reason)
  (compact-revision wr-compact-revision))

; convenience constructors for the two shapes the backend emits
(define (events-response watch-id header-rev events)
  (make-watch-response watch-id header-rev events #f #f #f 0))
(define (canceled-response watch-id header-rev reason compact-rev)
  (make-watch-response watch-id header-rev '() #f #t reason compact-rev))

; ===========================================================================
; watch-event — one Event{type, kv, prev_kv?} inside a WatchResponse (§1/§2)
; ===========================================================================
;
; `type` is 'put | 'del.  `kv` and `prev-kv` are kv-view vectors (below) — `prev-kv`
; is #f when prev_kv wasn't requested OR the key had no prior version.
(define-record-type watch-event
  (make-watch-event type kv prev-kv)
  watch-event?
  (type    we-type)
  (kv      we-kv)
  (prev-kv we-prev-kv))

; ---- kv-view: the full etcd KeyValue carried by an Event ----
;   a vector #(key create-rev mod-rev version lease value), reconstructed from
;   KEY-CF via mvcc-get-latest (§2 option b).  For a DELETE the key is gone at its
;   own revision, so we synthesise the tombstone KeyValue (version=0, no value) —
;   the event itself supplies the delete fact.
(define (make-kv-view key create-rev mod-rev version lease value)
  (vector key create-rev mod-rev version lease value))
(define (kvv-key        v) (vector-ref v 0))
(define (kvv-create-rev v) (vector-ref v 1))
(define (kvv-mod-rev    v) (vector-ref v 2))
(define (kvv-version    v) (vector-ref v 3))
(define (kvv-lease      v) (vector-ref v 4))
(define (kvv-value      v) (vector-ref v 5))

; Build the full new-KeyValue for an event at (key, mod-rev) of the given kind.
;   PUT   -> mvcc-get-latest ctx key mod-rev gives the visible record at exactly
;            this rev (create_rev / version / lease / value).
;   DELETE-> that read is #f (tombstone); synthesise the tombstone KeyValue.
(define (reconstruct-kv ctx key kind mod-rev)
  (let ((r (mvcc-get-latest ctx key mod-rev)))
    (if r
        (make-kv-view key (kv-rec-create-rev r) (kv-rec-mod-rev r)
                      (kv-rec-version r) (kv-rec-lease r) (kv-rec-value r))
        ; DELETE (or no live version at this rev): tombstone KeyValue
        (make-kv-view key 0 mod-rev 0 0 (make-bytevector 0 0)))))

; Build prev_kv: the KeyValue visible IMMEDIATELY BEFORE this event's revision
; (mod-rev - 1).  #f if the key had no prior live version (e.g. a create) — exactly
; etcd's prev_kv semantics.
(define (reconstruct-prev-kv ctx key mod-rev)
  (if (<= mod-rev 1)
      #f
      (let ((r (mvcc-get-latest ctx key (- mod-rev 1))))
        (if r
            (make-kv-view key (kv-rec-create-rev r) (kv-rec-mod-rev r)
                          (kv-rec-version r) (kv-rec-lease r) (kv-rec-value r))
            #f))))

; ===========================================================================
; WatchResponse <-> sendable s-expr  (the cross-actor wire bridge, cw-u4a.14)
; ===========================================================================
;
; A watch-response is a RECORD; records can NOT cross a `send` actor boundary
; (only null/bool/char/num/string/symbol/pair/vector/bytevector/pid are sendable).
; So the shard registry's deliver-fn flattens each WatchResponse with
; watch-response->sexp before `send`ing it to the per-conn streaming actor (.14),
; which reconstructs it with sexp->watch-response on the other side.  A kv-view is
; already a vector of bytevectors+ints (sendable), so it crosses verbatim; a
; watch-event becomes the list (type kv prev-kv) with #f for an absent prev-kv.
;
; Wire shape:
;   (WATCH-ID HEADER-REV CREATED? CANCELED? CANCEL-REASON COMPACT-REV
;    ((TYPE KV PREV-KV) ...))
; This is the .23 framing surface too: gRPC encodes/decodes against THIS shape.
(define (watch-event->sexp we)
  (list (we-type we) (we-kv we) (we-prev-kv we)))
(define (sexp->watch-event s)
  (make-watch-event (list-ref s 0) (list-ref s 1) (list-ref s 2)))

(define (watch-response->sexp wr)
  (list (wr-watch-id wr) (wr-header-rev wr) (wr-created? wr) (wr-canceled? wr)
        (wr-cancel-reason wr) (wr-compact-revision wr)
        (map watch-event->sexp (wr-events wr))))
(define (sexp->watch-response s)
  (make-watch-response (list-ref s 0) (list-ref s 1)
                       (map sexp->watch-event (list-ref s 6))
                       (list-ref s 2) (list-ref s 3) (list-ref s 4) (list-ref s 5)))

; Turn one decoded REV-CF event (event-decode vector) into a watch-event, doing the
; §2 KEY-CF reconstruction.  prev-kv? toggles the (optional) second lookup.
(define (event->watch-event ctx ev prev-kv?)
  (let* ((kind    (ev-kind ev))
         (key     (ev-key ev))
         (mod-rev (ev-mod-rev ev))
         (type    (if (= kind EV-PUT) 'put 'del))
         (kv      (reconstruct-kv ctx key kind mod-rev))
         (prev    (if prev-kv? (reconstruct-prev-kv ctx key mod-rev) #f)))
    (make-watch-event type kv prev)))

; ===========================================================================
; Watcher — the unit a client creates (ADR 0002 §1)
; ===========================================================================
;
; Immutable identity/spec fields + the two MUTABLE bits the §3 handoff turns:
;   delivered-rev : the high-water mark — every event with rev <= delivered-rev has
;                   been delivered exactly once.  Both replay and live test `>` it.
;   synced?       : #f while catching up (unsynced), #t once delivered-rev reached
;                   current-rev at promotion.  Live dispatch only touches synced ones.
(define-record-type watcher
  (make-watcher-record watch-id key range-end start-rev filters prev-kv? deliver-fn
                       delivered-rev synced?)
  watcher?
  (watch-id      w-watch-id)
  (key           w-key)
  (range-end     w-range-end)
  (start-rev     w-start-rev)
  (filters       w-filters)
  (prev-kv?      w-prev-kv?)
  (deliver-fn    w-deliver-fn)
  (delivered-rev w-delivered-rev set-w-delivered-rev!)
  (synced?       w-synced?       set-w-synced?!))

; ---- watch-spec: the create request, an assoc list the caller passes in ----
; Supported keys (all optional except 'key):
;   key        the watched key / start of range (bytevector)         REQUIRED
;   range-end  range semantics (mvcc-range-identical); #f => single key
;   start-rev  first rev NOT yet seen; 0 => current/future-only (exclusive lower)
;   filters    list of 'noput / 'nodelete
;   prev-kv    #t => each event also carries prev_kv
;   watch-id   client-supplied id; if absent the registry assigns one
(define (spec-ref spec key default)
  (let ((cell (assq key spec))) (if cell (cdr cell) default)))

; ===========================================================================
; Registry — owns the active watchers (ADR 0002 §4: lives on the shard actor)
; ===========================================================================
;
; A mutable record: a watch_id -> watcher hashtable + a monotonic counter for
; server-assigned ids.  Single-threaded ownership (the shard actor) is what makes
; the §3 seam race-free; this code adds NO locks.
(define-record-type watch-registry
  (make-watch-registry-record table next-id)
  watch-registry?
  (table   reg-table)
  (next-id reg-next-id set-reg-next-id!))

(define (make-watch-registry)
  (make-watch-registry-record (make-eqv-hashtable) 0))

(define (reg-count reg) (hashtable-size (reg-table reg)))
(define (reg-get   reg id) (hashtable-ref (reg-table reg) id #f))

; allocate the next server-assigned watch_id (used iff the spec supplies none)
(define (reg-alloc-id! reg)
  (let ((id (+ 1 (reg-next-id reg))))
    (set-reg-next-id! reg id)
    id))

; the active watchers as a list (snapshot for iteration)
(define (reg-watchers reg)
  (vector->list (hashtable-values (reg-table reg))))

; ===========================================================================
; watch-register! — REGISTER -> REPLAY -> CATCH-UP -> PROMOTE (ADR 0002 §3)
; ===========================================================================
;
;   (watch-register! reg ctx spec deliver-fn)
;     -> watch_id                         on success (watcher established + caught up)
;      | (cons 'compacted compact-rev)    if 0 < start-rev < compact-rev (§5): the
;                                         historical events are gone; NO watcher is
;                                         created.
;
; The sequence, exactly per §3 (marker-only DISPATCH variant (i)):
;   1. ErrCompacted gate (§5 at-creation): a from-revision watch below the floor is
;      refused outright.
;   2. REGISTER-BEFORE-REPLAY: create the watcher UNSYNCED at delivered_rev =
;      start-rev and insert it into reg FIRST.  (On the single registry thread this
;      closes the seam: no apply can interleave between "registered" and "replaying".)
;   3. REPLAY-TO-SNAPSHOT + CATCH-UP LOOP: drive delivered_rev up to current-rev by
;      re-running mvcc-watch-events from the watcher's own delivered_rev each pass
;      (each pass strictly shrinks the gap; REV-CF is append-only => it terminates).
;   4. PROMOTE-AT-BOUNDARY: synced? := #t once delivered_rev == current-rev.
;
; start-rev = 0 is the FUTURE-ONLY sentinel: it does NOT replay.  delivered_rev is
; seeded to current-rev and the watcher goes synced immediately, so only SUBSEQUENT
; live events (mod_rev > current) reach it — each exactly once via watch-on-apply!.
; (start-rev > 0 is the exclusive lower bound that the replay query delivers above.)
(define (watch-register! reg ctx spec deliver-fn)
  (let ((key       (spec-ref spec 'key #f))
        (range-end (spec-ref spec 'range-end #f))
        (start-rev (spec-ref spec 'start-rev 0))
        (filters   (spec-ref spec 'filters '()))
        (prev-kv?  (spec-ref spec 'prev-kv #f))
        (compact   (mvcc-compact-rev ctx)))
    (cond
      ; (1) ErrCompacted at creation: 0 < start-rev < compact-rev -> refuse (§5).
      ((and (> start-rev 0) (< start-rev compact))
       (cons 'compacted compact))
      (else
       (let* ((id (let ((sid (spec-ref spec 'watch-id #f)))
                    (if sid sid (reg-alloc-id! reg))))
              (w  (make-watcher-record id key range-end start-rev filters prev-kv?
                                       deliver-fn
                                       start-rev   ; delivered-rev := start-rev
                                       #f)))        ; unsynced
         ; (2) REGISTER-BEFORE-REPLAY — into the registry FIRST.
         (hashtable-set! (reg-table reg) id w)
         ; (3)+(4) replay + catch-up to current, then promote.
         ; start-rev = 0 is the FUTURE-ONLY sentinel (§1/§3): NO historical replay —
         ; jump delivered_rev straight to current-rev and go synced.  Registration
         ; runs on the registry's single thread, so reading current-rev once and
         ; seeding delivered_rev to it is atomic w.r.t. apply (no event can slip in
         ; between this seed and the synced flip).  A subsequent live event with
         ; mod_rev > current is then delivered exactly once by watch-on-apply!.
         (if (= start-rev 0)
             (set-w-delivered-rev! w (mvcc-current-rev ctx))
             (watch-replay-to-current! ctx w))
         (set-w-synced?! w #t)
         id)))))

; REPLAY-TO-SNAPSHOT + CATCH-UP LOOP (§3) for a from-revision (start-rev > 0) watch.
; Drives w's delivered_rev up to current-rev, delivering each in-range/filter-passing
; event exactly once via the replay query.  Loops because current-rev may advance
; during a pass; each pass reads a fresh snapshot and replays (delivered_rev, snap].
; Terminates because delivered_rev is monotone and REV-CF is append-only (the gap
; strictly shrinks).  (Future-only start-rev=0 watches never enter here — see
; watch-register!.)
(define (watch-replay-to-current! ctx w)
  (let loop ()
    (let ((snap (mvcc-current-rev ctx)))
      (if (>= (w-delivered-rev w) snap)
          ; caught up to this snapshot — re-check current once more in case it moved
          ; AFTER we read snap but BEFORE this test; if still caught up, done.
          (if (>= (w-delivered-rev w) (mvcc-current-rev ctx))
              'caught-up
              (loop))
          (let ((evs (mvcc-watch-events ctx (w-delivered-rev w)
                                        (w-key w) (w-range-end w) (w-filters w))))
            ; evs is a (possibly empty) revision-ascending list of decoded events,
            ; OR (cons 'err-compacted r) — but the at-creation gate already ran and
            ; delivered_rev only rises, so a mid-replay compaction is handled by
            ; watch-check-compaction! (§5), not here.  Defensive: stop on that shape.
            (if (and (pair? evs) (eq? (car evs) 'err-compacted))
                'compacted-mid
                (begin
                  (watch-deliver-events! ctx w evs snap)
                  ; If the query returned nothing yet delivered_rev still < snap (a
                  ; gap of filtered/out-of-range revisions), advance to snap so the
                  ; loop terminates — those revisions hold no event for THIS watcher.
                  (if (< (w-delivered-rev w) snap)
                      (set-w-delivered-rev! w snap))
                  (loop))))))))

; Deliver a batch of decoded events to a watcher as ONE WatchResponse (header-rev =
; the snapshot/current at which it was produced), advancing delivered_rev to the last
; event's rev.  The `> delivered_rev` de-dup is enforced by callers (replay uses the
; exclusive query; live dispatch tests it per-event), so here we just reconstruct +
; emit and bump the marker.  An empty batch produces NO response (etcd never sends an
; empty events frame from replay).
(define (watch-deliver-events! ctx w evs header-rev)
  (if (pair? evs)
      (let ((wevs (map (lambda (ev) (event->watch-event ctx ev (w-prev-kv? w))) evs)))
        ((w-deliver-fn w) (events-response (w-watch-id w) header-rev wevs))
        ; advance the high-water mark to the highest delivered revision
        (set-w-delivered-rev! w (ev-mod-rev (list-ref evs (- (length evs) 1)))))))

; ===========================================================================
; watch-on-apply! — the LIVE dispatch off mvcc-apply (ADR 0002 §3 LIVE, §4)
; ===========================================================================
;
;   (watch-on-apply! reg ctx pre-rev post-rev)
;
; Called from the shard actor's apply path with current-rev captured BEFORE
; (pre-rev) and AFTER (post-rev) a committed command.  Reads the just-produced
; events in (pre-rev, post-rev] ONCE from REV-CF (a bounded scan — the same
; mvcc-watch-events query, all-keys/no-filter so one read serves every watcher),
; then for each SYNCED watcher whose range matches, whose filters pass, and where
; ev.mod_rev > delivered_rev (the exactly-once de-dup guard), delivers and advances
; that watcher's delivered_rev.  UNSYNCED watchers are SKIPPED — replay/catch-up in
; watch-register! owns them up to the promotion boundary (marker-only variant (i)).
;
; NO-OP FAST PATH: if the registry is empty OR no revision advanced, returns
; immediately without touching REV-CF — this is what keeps the shard-actor hook a
; true no-op when nobody is watching (so the sim-cluster apply path is unperturbed).
(define (watch-on-apply! reg ctx pre-rev post-rev)
  (if (and (> (reg-count reg) 0) (> post-rev pre-rev))
      (let ((all-events (mvcc-watch-events ctx pre-rev (make-bytevector 0 0)
                                           (bytevector 0) '())))
        ; all-events: every event in (pre-rev, post-rev], all keys, no filter,
        ; revision-ascending.  (mvcc-watch-events caps at current-rev = post-rev.)
        (if (and (pair? all-events) (eq? (car all-events) 'err-compacted))
            'compacted        ; can't happen for pre-rev that just advanced; defensive
            (for-each
             (lambda (w)
               (if (w-synced? w)
                   (watch-dispatch-live! ctx w all-events post-rev)))
             (reg-watchers reg)))))
  (if #f #f))   ; -> unspecified

; For one synced watcher, walk the applied events in order and deliver each that is
; in-range, passes its filters, and is strictly past its delivered_rev (the de-dup
; guard).  Each matching event is its OWN WatchResponse (header-rev = post-rev) so
; per-watcher delivered_rev advances precisely.  (etcd may coalesce a Txn's events
; into one frame; per-event frames are simpler and observably equivalent for the
; backend contract — order + exactly-once.)
(define (watch-dispatch-live! ctx w events header-rev)
  (for-each
   (lambda (ev)
     (let ((k    (ev-key ev))
           (kind (ev-kind ev))
           (mr   (ev-mod-rev ev)))
       (if (and (> mr (w-delivered-rev w))                 ; exactly-once de-dup
                (range-in-range? k (w-key w) (w-range-end w))
                (event-passes-filter? kind (w-filters w)))
           (begin
             ((w-deliver-fn w)
              (events-response (w-watch-id w) header-rev
                               (list (event->watch-event ctx ev (w-prev-kv? w)))))
             (set-w-delivered-rev! w mr)))))
   events))

; ===========================================================================
; watch-cancel! — deregister + a canceled WatchResponse (ADR 0002 §6)
; ===========================================================================
;
;   (watch-cancel! reg watch-id [reason [compact-rev]])  -> #t if it existed, else #f
;
; Removes the watcher and, via its deliver-fn, sends a canceled WatchResponse for
; that watch_id (header-rev unknown to the registry here -> 0; .14 fills the live
; current-rev when it owns the stream).  Server-initiated cancels (ErrCompacted §5,
; stream teardown) take this same path.  Runs on the registry's single thread, so a
; cancel concurrent with an in-flight dispatch is serialized — no use-after-cancel.
(define (watch-cancel! reg watch-id . opts)
  (let ((w (reg-get reg watch-id)))
    (if w
        (let ((reason   (if (pair? opts) (car opts) "watch canceled"))
              (compact  (if (and (pair? opts) (pair? (cdr opts))) (cadr opts) 0)))
          (hashtable-delete! (reg-table reg) watch-id)
          ((w-deliver-fn w) (canceled-response watch-id 0 reason compact))
          #t)
        #f)))

; ===========================================================================
; watch-check-compaction! — mid-stream ErrCompacted (ADR 0002 §5)
; ===========================================================================
;
;   (watch-check-compaction! reg ctx)  -> list of canceled watch_ids
;
; Call this from WHERE a Compact is applied (.8/.14 wiring — noted, NOT wired here).
; A compaction GC's REV-CF events <= compact-rev, so any watcher still BELOW that
; floor (delivered_rev < compact-rev) can no longer be served its remaining history;
; cancel it with compact_revision set so the client re-establishes above the floor.
; A synced watcher is by definition at delivered_rev == current-rev >= compact-rev,
; so it is never affected.
(define (watch-check-compaction! reg ctx)
  (let ((compact (mvcc-compact-rev ctx)))
    (if (= compact 0)
        '()
        (let loop ((ws (reg-watchers reg)) (canceled '()))
          (if (null? ws)
              (reverse canceled)
              (let ((w (car ws)))
                (if (< (w-delivered-rev w) compact)
                    (begin
                      (watch-cancel! reg (w-watch-id w)
                                     "mvcc: required revision has been compacted"
                                     compact)
                      (loop (cdr ws) (cons (w-watch-id w) canceled)))
                    (loop (cdr ws) canceled))))))))
