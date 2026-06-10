; test/mvcc-apply.scm — unit test for the MVCC apply path (cw-u4a.6), NO Raft.
;
; Drives mvcc-apply directly against a single durable ctx over a real RocksDB and
; asserts the full revision-stamping semantics of ADR 0001: revision bumps,
; create_rev/mod_rev/version, tombstones, read-at-revision, range delete, lease
; index, and the current-rev meta key.  A fresh per-run temp dir ((current-jiffy))
; means a prior run's persisted state can never bleed in.

(include "test/harness.scm")
(include "src/store-ctx.scm")
(include "src/mvcc.scm")
(include "test/mvcc-util.scm")

; ---- open a fresh store (unique per run; substrate has no system/rm -rf) ----
(define run-tag (number->string (current-jiffy)))
(define DB-PATH (string-append "/tmp/cws-mvcc-apply-" run-tag))
(define CTX (make-ctx (store-open DB-PATH #t) "default" #t))

; Guarantee isolation: (current-jiffy) is process-relative, so back-to-back runs
; reuse the same dir — empty the store before building any state.
(reset-ctx! CTX)

; ---- helpers ----
(define (b s) (string->utf8 s))
(define (put . parts) (mvcc-apply CTX (map b parts)))      ; ("PUT" "k" "v" ["lease"])
(define (del . parts) (mvcc-apply CTX (map b parts)))      ; ("DEL" "k" ["end"])

; latest record for K (or at-rev), decoded; #f if absent/tombstoned
(define (latest K)         (mvcc-get-latest CTX (b K)))
(define (latest-at K rev)  (mvcc-get-latest CTX (b K) rev))
(define (val-of r) (and r (utf8->string (kv-rec-value r))))

; ===========================================================================
(section "PUT k v -> rev 1, version 1, create_rev 1, mod_rev 1")
(check "apply returns rev 1" (cons "PUT" 1) (put "PUT" "k" "v"))
(check "current-rev = 1"     1 (mvcc-current-rev CTX))
(let ((r (latest "k")))
  (check "k value"      "v" (val-of r))
  (check "k create_rev" 1   (kv-rec-create-rev r))
  (check "k mod_rev"    1   (kv-rec-mod-rev r))
  (check "k version"    1   (kv-rec-version r))
  (check "k not tombstone" #f (kv-rec-tombstone? r)))

; ===========================================================================
(section "PUT k v2 -> rev 2, version 2, create_rev 1, mod_rev 2 (update keeps create)")
(check "apply returns rev 2" (cons "PUT" 2) (put "PUT" "k" "v2"))
(check "current-rev = 2"     2 (mvcc-current-rev CTX))
(let ((r (latest "k")))
  (check "k value now v2" "v2" (val-of r))
  (check "k create_rev kept 1" 1 (kv-rec-create-rev r))
  (check "k mod_rev 2"    2 (kv-rec-mod-rev r))
  (check "k version 2"    2 (kv-rec-version r)))

; ===========================================================================
(section "DEL k -> rev 3, tombstoned (get-latest #f)")
(check "apply returns DEL rev 3, 1 deleted" (cons "DEL" (cons 3 1)) (del "DEL" "k"))
(check "current-rev = 3" 3 (mvcc-current-rev CTX))
(check "k now absent (tombstoned)" #f (latest "k"))

; REV-CF holds PUT, PUT, DELETE in revision order (Watch order).
(let* ((events (kv-scan CTX (mvcc-byte NS-REV)))
       (kinds  (map (lambda (kv) (ev-kind (event-decode (cdr kv)))) events))
       (revs   (map (lambda (kv) (ev-mod-rev (event-decode (cdr kv)))) events)))
  (check "REV-CF kinds = PUT PUT DELETE"
         (list EV-PUT EV-PUT EV-DELETE) kinds)
  (check "REV-CF revs ascending 1 2 3" (list 1 2 3) revs))

; ===========================================================================
(section "PUT k v3 after delete -> rev 4, version RESETS to 1, create_rev 4")
(check "apply returns rev 4" (cons "PUT" 4) (put "PUT" "k" "v3"))
(let ((r (latest "k")))
  (check "k value v3"        "v3" (val-of r))
  (check "k version reset 1" 1  (kv-rec-version r))
  (check "k create_rev 4 (recreate)" 4 (kv-rec-create-rev r))
  (check "k mod_rev 4"       4  (kv-rec-mod-rev r)))

; ===========================================================================
(section "read-at-revision: historical reads")
; at rev 1 k was "v"; at rev 3 k was tombstoned (deleted)
(check "get-latest k @1 = v (historical)" "v" (val-of (latest-at "k" 1)))
(check "get-latest k @2 = v2"             "v2" (val-of (latest-at "k" 2)))
(check "get-latest k @3 = #f (deleted)"   #f   (latest-at "k" 3))
(check "get-latest k @4 = v3"             "v3" (val-of (latest-at "k" 4)))

; ===========================================================================
(section "range delete: DEL [a, c) deletes a,b not c")
(put "PUT" "a" "va")    ; rev 5
(put "PUT" "b" "vb")    ; rev 6
(put "PUT" "c" "vc")    ; rev 7
(check "current-rev = 7 after a,b,c" 7 (mvcc-current-rev CTX))
(check "a live before"  "va" (val-of (latest "a")))
(check "b live before"  "vb" (val-of (latest "b")))
(check "c live before"  "vc" (val-of (latest "c")))
(let ((res (del "DEL" "a" "c")))             ; rev 8, range [a,c)
  (check "DEL [a,c) deleted 2 (a,b)" (cons "DEL" (cons 8 2)) res))
(check "a deleted" #f (latest "a"))
(check "b deleted" #f (latest "b"))
(check "c survives range" "vc" (val-of (latest "c")))

; ===========================================================================
(section "lease: PUT lk v 100 -> LEASE index has 0x03||u64be(100)||lk")
; A key may only attach to a GRANTED lease (cw-u4a.17 put-to-dead-lease guard,
; etcd ErrLeaseNotFound).  Grant lease 100 first; a grant does NOT bump the rev.
(put "LEASE-GRANT" "100" "60")               ; grant lease 100 (ttl 60), no rev bump
(put "PUT" "lk" "lv" "100")                  ; rev 9, lease 100
(let* ((lkey (enc-lease 100 (b "lk")))
       (got  (kv-get CTX lkey)))
  (check "lease index entry present" #t (and got #t))
  (check "lease index value empty"   0  (bytevector-length got)))
; the record carries the lease inline
(check "lk record lease = 100" 100 (kv-rec-lease (latest "lk")))
; scanning the lease-100 prefix finds the length-9 meta sentinel (the lease object,
; cw-u4a.17) PLUS the single attached key lk = 2 rows.  mvcc-lease-keys skips the
; sentinel and returns just {lk}.
(let ((rows (kv-scan CTX (lease-prefix 100))))
  (check "lease-100 prefix scan finds meta + lk = 2 rows" 2 (length rows)))
(check "mvcc-lease-keys 100 = {lk} (sentinel skipped)"
       (list (b "lk")) (mvcc-lease-keys CTX 100))

; ===========================================================================
(section "META current-rev matches last bump")
(check "current-rev = 9 (last PUT lk)" 9 (mvcc-current-rev CTX))

(done!)
