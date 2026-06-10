; test/watch-replay-poc.scm — validates the HISTORICAL-REPLAY half of the Watch
; design (ADR 0002) over the REAL REV-CF event log written by mvcc-apply (cw-u4a.6).
;
; A crab-watchstore Watch from start_revision <= current must, before it can stream
; live events, REPLAY the historical events in (start_revision, current] from the
; revision-ordered REV-CF index — filtered by the watcher's key/range and its
; NOPUT/NODELETE filters — IN STRICT REVISION ORDER, with the ErrCompacted gate
; when start_revision < compact-rev.  This is the foundation the live replay->live
; handoff (.13/.14/.15) is layered on; the gap-free live seam is implemented and
; tested THERE, not here (see ADR 0002 §3).
;
; This test BUILDS a known PUT/DELETE history across several keys + a prefix via
; mvcc-apply (so the events on disk are exactly what production writes), then
; implements the ADR's replay query (watch-replay below) and ASSERTS it returns
; EXACTLY the right events, in revision order, for:
;   - a single-key watch
;   - a prefix/range watch
;   - a from-revision-in-the-middle watch (only newer events)
;   - a NOPUT-filtered watch  (DELETE events only)
;   - a NODELETE-filtered watch (PUT events only)
;   - the ErrCompacted case (start_rev < compact-rev after a compaction)
;
; If this is green, the replay query the Watch backend (.13) builds on is sound
; over the real event log.  The query here is a PURE READ over REV-CF; the only
; src/mvcc.scm change this task makes is adding the matching pure read helper
; (mvcc-watch-events) — NO event WRITE-format change (enriching the event to carry
; the full KeyValue is a .13 decision, recorded in ADR 0002 §2).

(include "test/harness.scm")
(include "src/store-ctx.scm")
(include "src/mvcc.scm")
(include "test/mvcc-util.scm")

; ---- open a fresh store (unique per run; substrate has no system/rm -rf) ----
(define run-tag (number->string (current-jiffy)))
(define DB-PATH (string-append "/tmp/cws-watch-replay-" run-tag))
(define CTX (make-ctx (store-open DB-PATH #t) "default" #t))

; (current-jiffy) is process-relative, so back-to-back runs may reuse the same dir
; — empty the store before building any state so the assertions are deterministic.
(reset-ctx! CTX)

; ---- helpers ----
(define (b s) (string->utf8 s))
(define (put . parts) (mvcc-apply CTX (map b parts)))   ; ("PUT" "k" "v")
(define (del . parts) (mvcc-apply CTX (map b parts)))   ; ("DEL" "k" ["end"])

; A compact, human-readable projection of one replayed event for assertions:
;   (kind-symbol key-string mod-rev)  where kind-symbol is 'put | 'del
(define (ev->triple e)
  (list (if (= (ev-kind e) EV-PUT) 'put 'del)
        (utf8->string (ev-key e))
        (ev-mod-rev e)))

; Run the ADR's replay query and project each event to a triple, preserving order.
;   start-rev   exclusive lower bound (events with main-rev > start-rev)
;   key range-end : single-key (#f/empty end) or half-open [key,range-end)
;                   range-end = #u8(0) means "to end of keyspace" (prefix/all)
;   filters     : a list possibly containing 'noput and/or 'nodelete
; Returns either (cons 'err-compacted compact-rev) or a list of triples.
(define (replay start-rev key range-end filters)
  (let ((r (mvcc-watch-events CTX start-rev key range-end filters)))
    (if (and (pair? r) (eq? (car r) 'err-compacted))
        r
        (map ev->triple r))))

; ===========================================================================
; Build a known history.  Each mvcc-apply = one Raft entry = one main revision.
; ===========================================================================
;
;  rev 1  PUT  /a/1   v1
;  rev 2  PUT  /a/2   v2
;  rev 3  PUT  /b/1   v3
;  rev 4  PUT  /a/1   v1b   (update /a/1)
;  rev 5  DEL  /a/2          (tombstone /a/2)
;  rev 6  PUT  /c     v6
;  rev 7  DEL  [/a/, /a0)    (range delete the /a/ prefix -> deletes /a/1; /a/2
;                            already tombstoned -> only /a/1 is a live victim)
;
; "/a0" is the exclusive upper bound for the "/a/" prefix: "/a/" with its last
; byte ('/' = 0x2F) incremented to '0' (0x30), the standard etcd prefix range-end.

(put "PUT" "/a/1" "v1")     ; rev 1
(put "PUT" "/a/2" "v2")     ; rev 2
(put "PUT" "/b/1" "v3")     ; rev 3
(put "PUT" "/a/1" "v1b")    ; rev 4
(del "DEL" "/a/2")          ; rev 5
(put "PUT" "/c"   "v6")     ; rev 6
(del "DEL" "/a/" "/a0")     ; rev 7  (range delete of prefix "/a/")

(check "history advanced current-rev to 7" 7 (mvcc-current-rev CTX))

; ===========================================================================
(section "sanity: full REV-CF log is every event in strict revision order")
; A watch from start_revision 0 with an all-keys range replays the ENTIRE log.
; This is also the substrate the per-key/range/filter views are carved out of.
; Expected, in order:
;   1 put /a/1 ; 2 put /a/2 ; 3 put /b/1 ; 4 put /a/1 ; 5 del /a/2 ;
;   6 put /c   ; 7 del /a/1   (the range delete tombstoned the single live /a/1)
(check "all events (start 0, all keys)"
       (list '(put "/a/1" 1) '(put "/a/2" 2) '(put "/b/1" 3) '(put "/a/1" 4)
             '(del "/a/2" 5) '(put "/c" 6)   '(del "/a/1" 7))
       (replay 0 (b "") (bytevector 0) '()))      ; key="" end=#u8(0) => all keys

; ===========================================================================
(section "single-key watch: only that key's events, in revision order")
; Watch /a/1 from the beginning: its PUT@1, its update PUT@4, its range-delete@7.
(check "watch /a/1 (single key) from 0"
       (list '(put "/a/1" 1) '(put "/a/1" 4) '(del "/a/1" 7))
       (replay 0 (b "/a/1") #f '()))               ; range-end #f => single key

; A single-key watch on /c sees only its one PUT@6.
(check "watch /c (single key) from 0"
       (list '(put "/c" 6))
       (replay 0 (b "/c") #f '()))

; A single-key watch on a key that never existed yields no events.
(check "watch /nope (single key) from 0 -> no events"
       '()
       (replay 0 (b "/nope") #f '()))

; ===========================================================================
(section "prefix/range watch: every event whose key is in [/a/, /a0)")
; The "/a/" prefix covers /a/1 and /a/2.  In revision order that is:
;   1 put /a/1 ; 2 put /a/2 ; 4 put /a/1 ; 5 del /a/2 ; 7 del /a/1
(check "watch prefix /a/ from 0"
       (list '(put "/a/1" 1) '(put "/a/2" 2) '(put "/a/1" 4)
             '(del "/a/2" 5) '(del "/a/1" 7))
       (replay 0 (b "/a/") (b "/a0") '()))

; A half-open range [/a/2, /b/1) covers /a/2 only (/a/1 < /a/2 excluded; /b/1
; is the exclusive upper bound so excluded; /c is past it).
(check "watch range [/a/2,/b/1) from 0"
       (list '(put "/a/2" 2) '(del "/a/2" 5))
       (replay 0 (b "/a/2") (b "/b/1") '()))

; ===========================================================================
(section "from-revision-in-the-middle: only events with rev > start_revision")
; start_revision = 4 on the /a/ prefix => exclusive, so only events at rev 5,6,7
; in range are returned: del /a/2 @5 and del /a/1 @7 (rev 4's put is excluded).
(check "watch prefix /a/ from rev 4 (exclusive) -> only 5 and 7"
       (list '(del "/a/2" 5) '(del "/a/1" 7))
       (replay 4 (b "/a/") (b "/a0") '()))

; from rev 6 on all keys => only rev 7's del /a/1 (rev 6's put /c is excluded).
(check "watch all keys from rev 6 -> only rev 7"
       (list '(del "/a/1" 7))
       (replay 6 (b "") (bytevector 0) '()))

; from rev 7 (== current) on all keys => no historical events (nothing > 7);
; such a watch is immediately "caught up" and only ever sees live events.
(check "watch all keys from rev 7 (==current) -> no historical events"
       '()
       (replay 7 (b "") (bytevector 0) '()))

; ===========================================================================
(section "NOPUT filter: DELETE events only (PUTs suppressed)")
; The /a/ prefix history with NOPUT keeps only the two deletes: @5 and @7.
(check "watch prefix /a/ from 0, filter NOPUT -> deletes only"
       (list '(del "/a/2" 5) '(del "/a/1" 7))
       (replay 0 (b "/a/") (b "/a0") '(noput)))

; ===========================================================================
(section "NODELETE filter: PUT events only (DELETEs suppressed)")
; The /a/ prefix history with NODELETE keeps only the three puts: @1, @2, @4.
(check "watch prefix /a/ from 0, filter NODELETE -> puts only"
       (list '(put "/a/1" 1) '(put "/a/2" 2) '(put "/a/1" 4))
       (replay 0 (b "/a/") (b "/a0") '(nodelete)))

; Both filters together => no events (every event is a PUT or a DELETE).
(check "watch prefix /a/ from 0, filter NOPUT+NODELETE -> empty"
       '()
       (replay 0 (b "/a/") (b "/a0") '(noput nodelete)))

; ===========================================================================
(section "ErrCompacted: start_revision < compact-rev is rejected")
; Compact history up to rev 3.  After this, REV-CF events at rev <= 3 are GC'd and
; the compact-rev gate is set to 3.  A watch whose start_revision < 3 can no longer
; be served historically -> ErrCompacted (the client must re-establish).
(check "compact to rev 3 ok" (cons 'ok 3)
       (mvcc-apply CTX (list (b "COMPACT") (b "3"))))
(check "compact-rev now 3" 3 (mvcc-compact-rev CTX))

; start_revision 1 (< compact-rev 3) => ErrCompacted carrying the compact-rev.
(check "watch from rev 1 (< compact 3) -> err-compacted 3"
       (cons 'err-compacted 3)
       (replay 1 (b "/a/") (b "/a0") '()))

; start_revision exactly == compact-rev is allowed (etcd treats Watch start_rev
; as the first revision the client has NOT yet seen; compact-rev itself is still
; a valid floor).  Replay returns events with rev > 3 in the /a/ prefix: 4,5,7.
(check "watch from rev 3 (== compact) -> events 4,5,7 (no ErrCompacted)"
       (list '(put "/a/1" 4) '(del "/a/2" 5) '(del "/a/1" 7))
       (replay 3 (b "/a/") (b "/a0") '()))

; start_revision 0 means "current/future" and is ALWAYS valid regardless of
; compaction (no historical replay floor is crossed).
(check "watch from rev 0 (current/future) is never ErrCompacted"
       (list '(put "/a/1" 4) '(del "/a/2" 5) '(del "/a/1" 7))
       (replay 0 (b "/a/") (b "/a0") '()))

; Post-compaction the physical REV-CF log indeed lost events <= 3 (only 4..7 of
; the original 7 events remain on disk) — proves the replay reads the REAL,
; compacted event log, not a cache.
(check "REV-CF physically holds only events 4..7 after compaction"
       4 (length (kv-scan CTX (mvcc-byte NS-REV))))

(done!)
