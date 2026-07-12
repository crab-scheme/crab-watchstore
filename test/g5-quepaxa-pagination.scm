; test/g5-quepaxa-pagination.scm — cw-qyk (G5): LIST limit/continue conformance
; on the QUEPAXA driver. Same paging scenario as test/g5-list-pagination.scm
; (raft) — the paging mechanism (per-page mvcc-range at a pinned revision via
; the range-worker pool) is engine-independent, so this keeps the CI-friendly
; default N; the at-scale (100k) numbers are the raft probe's job (mirrors the
; g1-write-stall / g1-quepaxa-range split).
;
; etcd v3 pagination (what kube-apiserver's reflector relist depends on) is
; entirely CLIENT-driven: there is no server-side "continue" field on the wire
; RangeRequest. A client pages by:
;   1. First page: Range(key=start, range-end=end, limit=L, revision=0).
;      The response header carries the revision the read was served AT; the
;      client pins every subsequent page to that revision.
;   2. Next page: Range(key=lastReturnedKey+0x00, range-end=end, limit=L,
;      revision=pinnedRev). Appending a single 0x00 byte to the last key
;      gives its immediate lexicographic successor, so the next page starts
;      exactly where the last one stopped (no skip, no dup).
;   3. `more` (etcd: count > len(kvs) at that limit) tells the client whether
;      to keep paging; `count` is the FULL match count at that revision,
;      unaffected by limit — apiserver totals its list from these.
; A page must be served at the PINNED revision throughout, even though writes
; keep landing at higher revisions concurrently (this is the "consistent
; revision across pages under concurrent writes" requirement) — mvcc-range's
; existing 'revision opt already gives every page point-in-time visibility;
; this test is what proves paging on top of it holds together end to end.
;
; NOTE (ca79c2c): this deliberately pages via ORDINARY per-page mvcc-range
; calls (the same non-chunked path the range-worker already serves) — do NOT
; attempt to chunk a single pinned-revision scan through mvcc-range itself.
;
; Run from repo root:  crabscheme run test/g5-quepaxa-pagination.scm
;   CWS_G5_N=100000 CWS_G5_LIMIT=500 crabscheme run test/g5-quepaxa-pagination.scm
(include "test/harness.scm")
(include "src/encoding.scm")

(make-table 'ws-shard-pid "set")
(make-table 'ws-shard-role "set")
(make-table 'ws-shard-leader "set")
(make-table 'ws-shard-commit "set")
(make-table 'ws-shard-applied "set")
(make-table 'ws-test "set")

(node-make "a")
(define run-tag (number->string (exact (round (* 1000000 (current-second))))))
(define db (string-append "/tmp/cws-g5-qp-page-" run-tag "-a-s0"))

; N rows to seed — default 2000 (CI-friendly); CWS_G5_N=100000 for the exit
; probe. limit defaults to etcd/apiserver's 500.
(define N (let ((e (get-environment-variable "CWS_G5_N")))
            (if e (or (string->number e) 2000) 2000)))
(define LIMIT (let ((e (get-environment-variable "CWS_G5_LIMIT")))
                (if e (or (string->number e) 500) 500)))

; shard "0", single voter (always leader, no election wait); n-apply-workers=1.
(spawn-source "(include \"src/server/quepaxa-shard.scm\")" 'qp-shard-main
              "0" '(a) 'a db #f 1 4 #f '() 0 '() #f #f)
(spawn-source "(include \"src/server/peer-poller.scm\")" 'peer-poller
              'a '("0") 150 '() 0)

(define (role) (table-lookup 'ws-shard-role "a:0"))
; time-based spin: the 100k paging leg legitimately runs for minutes (the
; per-page mvcc-range scan is O(remaining range), see the perf note below),
; so an iteration-count budget is the wrong clock.
(define (spin pred who)
  (let ((deadline (+ (current-second) 3000)))
    (let loop ((next-beat (+ (current-second) 30)))
      (cond ((pred) #t)
            ((> (current-second) deadline)
             (error (string-append "timeout: " who)))
            ((> (current-second) next-beat)
             (display "  ...spin ") (display who)
             (display " pages-done=")
             (display (table-lookup 'ws-test "pages-done")) (newline)
             (loop (+ (current-second) 30)))
            (else (loop next-beat))))))

(section "shard bring-up")
(spin (lambda () (eq? (role) 'leader)) "leader")
(check "shard 0 elected leader" 'leader (role))

; ---- seed N rows, keys "k00000000".."k<N-1 zero-padded>" ----
(section (string-append "seed " (number->string N) " rows"))
(define seed-src (string-append "
  (define (b s) (string->utf8 s))
  (define (ask pid msg) (send pid msg) (raw-receive))
  (define (propose pid cmd) (ask pid (cons (self) cmd)))
  (define (pad n)
    (let ((s (number->string n)))
      (string-append (make-string (- 8 (string-length s)) #\\0) s)))
  (define (seed)
    (let ((o (table-lookup 'ws-shard-pid \"a:0\")))
      (let loop ((i 0))
        (if (< i " (number->string N) ")
            (begin
              (propose o (list (b \"PUT\") (b (string-append \"k\" (pad i))) (b \"v\")))
              (loop (+ i 1)))
            (table-insert! 'ws-test \"seeded\" #t)))))"))
(spawn-source seed-src 'seed)
(spin (lambda () (table-lookup 'ws-test "seeded")) "seed rows")
(check "seeded" #t (table-lookup 'ws-test "seeded"))

; ---- concurrent writer: once paging starts, keeps writing NEW keys under a
; disjoint prefix ("z-late-...") plus overwrites of already-seeded keys, so a
; pinned-revision page must see NEITHER: neither new "z-late" keys nor the
; post-pin overwritten values, if that write lands after the page's pin. ----
(section "page through limit=500, pinned revision, under concurrent writes")
; racer is CAPPED (default 5000 write-pairs, CWS_G5_RACER_CAP to change): an
; UNCAPPED racer at 100k grows the keyspace by millions of (invisible at the
; pinned rev, but still SCANNED) z-late rows while the pager works, so the
; final z-region page degenerates into an unbounded scan. The cap still spans
; many pages of real write/paging overlap, which is what the gate requires.
(define RACER-CAP (let ((e (get-environment-variable "CWS_G5_RACER_CAP")))
                    (if e (or (string->number e) 5000) 5000)))
(define racer-src (string-append "
  (define (b s) (string->utf8 s))
  (define (ask pid msg) (send pid msg) (raw-receive))
  (define (propose pid cmd) (ask pid (cons (self) cmd)))
  (define (spin-fired)
    (if (table-lookup 'ws-test \"paging-started\") #t (spin-fired)))
  (define (racer)
    (spin-fired)
    (let ((o (table-lookup 'ws-shard-pid \"a:0\")))
      (let loop ((i 0))
        (if (or (>= i " (number->string RACER-CAP) ")
                (table-lookup 'ws-test \"paging-done\"))
            (table-insert! 'ws-test \"racer-writes\" i)
            (begin
              (propose o (list (b \"PUT\") (b (string-append \"z-late-\" (number->string i))) (b \"v\")))
              (propose o (list (b \"PUT\") (b \"k00000000\") (b \"overwritten\")))
              (loop (+ i 1)))))))"))
(spawn-source racer-src 'racer)

(define pager-src (string-append "
  (define (b s) (string->utf8 s))
  (define (bv<? a bv)
    (let ((la (bytevector-length a)) (lb (bytevector-length bv)))
      (let loop ((i 0))
        (cond ((= i (min la lb)) (< la lb))
              ((< (bytevector-u8-ref a i) (bytevector-u8-ref bv i)) #t)
              ((> (bytevector-u8-ref a i) (bytevector-u8-ref bv i)) #f)
              (else (loop (+ i 1)))))))
  (define zero (make-bytevector 1 0))
  (define (bv-append1-zero bv)
    (let* ((n (bytevector-length bv)) (out (make-bytevector (+ n 1) 0)))
      (bytevector-copy! out 0 bv 0 n)
      out))
  (define (ask pid msg) (send pid msg) (raw-receive))
  (define (page o key rev)
    (ask o (list 'kv-range (self)
                 (list (cons 'key key) (cons 'range-end zero)
                       (cons 'limit " (number->string LIMIT) ")
                       (cons 'revision rev) (cons 'serializable #t)))))
  (define (pager)
    (let* ((o (table-lookup 'ws-shard-pid \"a:0\"))
           (t0 (current-jiffy))
           (r1 (page o zero 0))
           (pinned-rev (cadr r1))
           (k0-tuple (assoc (b \"k00000000\") (cadr (cddddr r1)))))
      (table-insert! 'ws-test \"first-page-k0-value\"
                     (if k0-tuple (cadr k0-tuple) #f))
      ; etcd's RangeResponse.count is scoped to THIS request's [key,range-end)
      ; — it shrinks every page as `key` advances, it is NOT a running total
      ; of the whole original range. Page 1's count (key=start-of-keyspace)
      ; is the one that must equal the full match set.
      (table-insert! 'ws-test \"first-page-count\" (cadddr (cdr r1)))
      (table-insert! 'ws-test \"page1-ms\"
        (round (/ (- (current-jiffy) t0) (/ (jiffies-per-second) 1000))))
      (table-insert! 'ws-test \"paging-started\" #t)
      (let loop ((r r1) (rev pinned-rev) (pages 1) (keys '()) (last-key #f))
        (table-insert! 'ws-test \"pages-done\" pages)
        (let* ((total (cadddr (cdr r)))
               (tuples (cadr (cddddr r)))
               (page-keys (map car tuples))
               (more (> total (length page-keys)))
               (new-last (if (null? page-keys) last-key
                             (list-ref page-keys (- (length page-keys) 1)))))
          (if (not more)
              ; NOTE: do NOT table-insert! the raw 100k-key list — the deep
              ; recursive sendable-copy SIGBUSes at that depth (found by this
              ; test's first 100k run). Compute the order/completeness/leak
              ; verdicts HERE and publish scalars only.
              (let* ((all-keys (append keys page-keys))
                     (asc (let loop2 ((l all-keys))
                            (cond ((or (null? l) (null? (cdr l))) #t)
                                  ((bv<? (car l) (cadr l)) (loop2 (cdr l)))
                                  (else #f))))
                     ; seeded keys start with #\\k (0x6b); the racer's late
                     ; keys with #\\z (0x7a) — any 0x7a key in a page is a
                     ; pinned-revision leak.
                     (leak (let loop3 ((l all-keys))
                             (cond ((null? l) #f)
                                   ((= (bytevector-u8-ref (car l) 0) 122) #t)
                                   (else (loop3 (cdr l)))))))
                (table-insert! 'ws-test \"pages\" pages)
                (table-insert! 'ws-test \"total-count\" total)
                (table-insert! 'ws-test \"pinned-rev\" pinned-rev)
                (table-insert! 'ws-test \"elapsed-ms\"
                  (round (/ (- (current-jiffy) t0) (/ (jiffies-per-second) 1000))))
                (table-insert! 'ws-test \"keys-count\" (length all-keys))
                (table-insert! 'ws-test \"ascending\" asc)
                (table-insert! 'ws-test \"z-late-leak\" leak)
                (table-insert! 'ws-test \"paging-done\" #t))
              (let* ((next-key (bv-append1-zero new-last))
                     (r2 (page o next-key rev)))
                (loop r2 rev (+ pages 1) (append keys page-keys) new-last)))))))"))
(spawn-source pager-src 'pager)
(spin (lambda () (table-lookup 'ws-test "paging-done")) "pager")
(spin (lambda () (table-lookup 'ws-test "racer-writes")) "racer stop")

; ---- post-race verifier: the racer overwrote k00000000 at revisions AFTER
; the pin. A fresh read AT the pinned revision must still see the pinned
; value "v"; a fresh read at revision 0 (current) must see "overwritten".
; This is the pinned-revision check that actually races — page 1's own
; k00000000 read happened before the racer started. ----
(define verify-src "
  (define (b s) (string->utf8 s))
  (define zero (make-bytevector 1 0))
  (define (ask pid msg) (send pid msg) (raw-receive))
  (define (get-at o rev)
    (let ((r (ask o (list 'kv-range (self)
                          (list (cons 'key (b \"k00000000\"))
                                (cons 'revision rev)
                                (cons 'serializable #t))))))
      (let ((tuples (cadr (cddddr r))))
        (if (null? tuples) #f (cadr (car tuples))))))
  (define (verify)
    (let ((o (table-lookup 'ws-shard-pid \"a:0\"))
          (rev (table-lookup 'ws-test \"pinned-rev\")))
      (table-insert! 'ws-test \"k0-at-pinned\" (get-at o rev))
      (table-insert! 'ws-test \"k0-at-current\" (get-at o 0))
      (table-insert! 'ws-test \"verified\" #t)))")
(spawn-source verify-src 'verify)
(spin (lambda () (table-lookup 'ws-test "verified")) "verifier")

(define keys-count (table-lookup 'ws-test "keys-count"))
(define total-count (table-lookup 'ws-test "total-count"))
(define first-page-count (table-lookup 'ws-test "first-page-count"))
(define pages (table-lookup 'ws-test "pages"))
(define elapsed-ms (table-lookup 'ws-test "elapsed-ms"))
(define racer-writes (table-lookup 'ws-test "racer-writes"))

(display "N=") (display N) (display " limit=") (display LIMIT)
(display " pages=") (display pages)
(display " total-count=") (display total-count)
(display " elapsed-ms=") (display elapsed-ms)
(display " page1-ms=") (display (table-lookup 'ws-test "page1-ms"))
(display " racer-writes-during-paging=") (display racer-writes)
(newline)

; -- completeness: exactly N keys, seeded k00000000..k<N-1> --
; (per-page `count` is scoped to [key,range-end) of THAT request, so it
; shrinks as key advances — only page 1's count covers the whole keyspace)
(check "page-1 count == N (etcd's full-range match count)" N first-page-count)
(check "last page total-count <= N (scoped to its own remaining range)" #t
       (<= total-count N))
(check "returned key count == N (no dup/skip across the full page sequence)"
       N keys-count)

; -- order: strictly ascending across the whole page sequence (verdict
; computed in the pager actor — see the SIGBUS note there) --
(check "returned keys strictly ascending, no dup, no skip across pages" #t
       (table-lookup 'ws-test "ascending"))

; -- pinned-revision consistency: NONE of the concurrent writer's z-late-*
; keys or the k00000000 overwrite (both landing at revisions AFTER the pin)
; leaked into any page. Every page was read at the SAME pinned revision. --
(check "no z-late-* keys leaked (writes after the pinned revision excluded)"
       #f (table-lookup 'ws-test "z-late-leak"))
(check "racer actually raced (post-pin writes happened during paging)" #t
       (> racer-writes 0))
(check "pinned-revision overwrite excluded (k00000000 stays at its pinned value)"
       (string->utf8 "v") (table-lookup 'ws-test "first-page-k0-value"))
(check "post-race read AT pinned revision still sees the pinned value"
       (string->utf8 "v") (table-lookup 'ws-test "k0-at-pinned"))
(check "post-race read at current revision sees the racer's overwrite"
       (string->utf8 "overwritten") (table-lookup 'ws-test "k0-at-current"))

; -- more/count contract: every page but the last reported more=#t, matching
; etcd (limit>0 AND total > returned-so-far); already enforced by the loop's
; own termination, so this just re-asserts the wiring didn't silently cap N
; below limit in the trivial (N<=limit) case, which would falsely pass above.
(check "pages > 1 when N > limit (pagination actually exercised)" #t
       (or (<= N LIMIT) (> pages 1)))
(check "single page when N <= limit" #t
       (or (> N LIMIT) (= pages 1)))

(done!)
