; test/mvcc-edge.scm — byte-level robustness tests for the MVCC encoding (cw-u4a.9).
;
; Covers gaps not exercised by the per-feature tests (which use only plain ASCII
; keys).  Specifically:
;
;  §1  Keys whose byte values equal namespace tags (0x00..0x03) — the u64be(lenK)
;      length-prefix in KEY-CF keys must prevent these from colliding with META/REV
;      or LEASE namespace prefixes, and the range scan must handle them correctly.
;
;  §2  Keys that are prefixes of other keys ("a" vs "ab", #u8(1) vs #u8(1 1)) —
;      the length-prefix must keep them in disjoint groups, and both point reads
;      and range reads must return exactly the right members.
;
;  §3  Value edge cases: empty value (#u8()), a 4KB value, and a key with 50
;      versions followed by range (returns latest) and compact at version 25
;      (verifies older versions are GC'd but >= 25 are readable).
;
;  §4  Lease round-trip: key with a lease id — the returned record carries the id.
;
;  §5  Revision=0 semantics: reads current; revision=current-rev reads the same.
;
; ISOLATION: uses reset-ctx! from mvcc-util.scm so back-to-back runs are
; fully deterministic without external store cleanup.

(include "test/harness.scm")
(include "src/store-ctx.scm")
(include "src/mvcc.scm")
(include "test/mvcc-util.scm")

; ---- open store ----
(define run-tag (number->string (current-jiffy)))
(define DB-PATH (string-append "/tmp/cws-mvcc-edge-" run-tag))
(define CTX (make-ctx (store-open DB-PATH #t) "default" #t))

(reset-ctx! CTX)

; ---- helpers ----
(define (b s) (string->utf8 s))

; Apply a PUT cmd with a raw bytevector key and value (not through string->utf8).
(define (raw-put! K V)
  (let* ((prev-rev (mvcc-current-rev CTX))
         (main     (+ prev-rev 1)))
    (mvcc-put! CTX K V 0 main 0)
    (mvcc-set-current-rev! CTX main)
    main))

; Apply a DEL cmd with a raw bytevector key.
(define (raw-del! K)
  (let* ((prev-rev (mvcc-current-rev CTX))
         (main     (+ prev-rev 1)))
    (mvcc-delete-range! CTX K #f main 0)
    (mvcc-set-current-rev! CTX main)
    main))

; Convenience range over raw key/range-end bytevectors.
(define (range-bv key range-end . opts-pairs)
  (let ((opts (let build ((ps opts-pairs) (acc '()))
                (if (null? ps)
                    (reverse acc)
                    (build (cddr ps) (cons (cons (car ps) (cadr ps)) acc))))))
    (mvcc-range CTX key range-end opts)))

(define (range-count r) (car r))
(define (range-kvs r)   (cdr r))

; Make a bytevector from a list of byte values.
(define (bv . bytes)
  (let* ((n (length bytes))
         (v (make-bytevector n 0)))
    (let loop ((i 0) (bs bytes))
      (if (null? bs) v
          (begin (bytevector-u8-set! v i (car bs))
                 (loop (+ i 1) (cdr bs)))))))

; Build a bytevector of `n` repetitions of byte `b`.
(define (bv-repeat n byte)
  (make-bytevector n byte))

; Compute the prefix-range-end for a user key prefix (mirrors mvcc.scm helper).
; (needed to drive a prefix-range check without a string version)
; We just expose the internal helper directly — it's defined in mvcc.scm.
(define pfe prefix-range-end)

; Decode keys list from a range result (bytevectors).
(define (kv-keys-bv r) (map car (range-kvs r)))

; ===========================================================================
(section "§1 tag-byte keys: user keys whose content = namespace tag bytes")
; ===========================================================================
; Keys: #u8(0), #u8(1), #u8(2), #u8(3) — bytes equal to NS-META/NS-KEY/NS-REV/NS-LEASE.
; These must be stored and retrieved correctly; the KEY-CF prefix NS-KEY||u64be(1)||<byte>
; is at least 10 bytes, so NS-KEY(1-byte)||u64be(1)||0x00 can never overlap with
; a raw NS-META(1-byte) key which starts with 0x00.

(define tag0 (bv 0))       ; byte = NS-META tag
(define tag1 (bv 1))       ; byte = NS-KEY tag
(define tag2 (bv 2))       ; byte = NS-REV tag
(define tag3 (bv 3))       ; byte = NS-LEASE tag
(define val-t0 (b "val-tag0"))
(define val-t1 (b "val-tag1"))
(define val-t2 (b "val-tag2"))
(define val-t3 (b "val-tag3"))

(raw-put! tag0 val-t0)   ; rev 1
(raw-put! tag1 val-t1)   ; rev 2
(raw-put! tag2 val-t2)   ; rev 3
(raw-put! tag3 val-t3)   ; rev 4

; Point reads must succeed.
(let ((r0 (mvcc-get-latest CTX tag0))
      (r1 (mvcc-get-latest CTX tag1))
      (r2 (mvcc-get-latest CTX tag2))
      (r3 (mvcc-get-latest CTX tag3)))
  (check "get tag-byte key 0x00" val-t0 (and r0 (kv-rec-value r0)))
  (check "get tag-byte key 0x01" val-t1 (and r1 (kv-rec-value r1)))
  (check "get tag-byte key 0x02" val-t2 (and r2 (kv-rec-value r2)))
  (check "get tag-byte key 0x03" val-t3 (and r3 (kv-rec-value r3))))

; These user-key records must NOT bleed into other MVCC namespaces.
; META should still only have current-rev (and nothing from user-key tag0 etc.).
; A META prefix scan returns only META-keyed entries; current-rev is one of them.
; Because we haven't compacted, compact-rev may or may not be written.
; We only assert the tag0 user key does NOT appear as a raw META entry.
(let* ((meta-rows (kv-scan CTX (mvcc-byte NS-META)))
       ; Each META key is: 0x00 || string-name.  The tag0 user-key in KEY-CF is:
       ; 0x01 || u64be(1) || 0x00 || inv16 — totally different prefix.
       ; We check no META entry has a key that equals the raw tag0 user-key byte.
       (meta-keys (map car meta-rows))
       (raw-tag0  tag0)
       (collision? (let loop ((ks meta-keys))
                     (if (null? ks) #f
                         (or (equal? (car ks) raw-tag0) (loop (cdr ks)))))))
  (check "tag0 user-key does not collide with META namespace" #f collision?))

; Range scan over all-keys must find our 4 tag-byte keys.
(let* ((zero (make-bytevector 1 0))
       (res  (range-bv zero zero 'sort-order 'ascend 'sort-target 'key)))
  ; The store was reset; only tag0-tag3 written so far.
  (check "tag-byte keys: all-keys count = 4" 4 (range-count res))
  ; Keys must be tag0, tag1, tag2, tag3 sorted by bv<?
  ; tag0=#u8(0) < tag1=#u8(1) < tag2=#u8(2) < tag3=#u8(3)
  (let ((returned-keys (map car (range-kvs res))))
    (check "tag0 key correct"    tag0 (list-ref returned-keys 0))
    (check "tag1 key correct"    tag1 (list-ref returned-keys 1))
    (check "tag2 key correct"    tag2 (list-ref returned-keys 2))
    (check "tag3 key correct"    tag3 (list-ref returned-keys 3))))

; ===========================================================================
(section "§1b embedded-zero and all-0xFF keys")
; ===========================================================================
; Keys: #u8(107 0 107)="k\x00k" and #u8(255 255)="\xFF\xFF".
(define key-with-null  (bv 107 0 107))    ; 'k', NUL, 'k'
(define key-all-ff     (bv 255 255))
(define val-null       (b "val-null"))
(define val-ff         (b "val-ff"))

(reset-ctx! CTX)    ; fresh slate for this section

(raw-put! key-with-null val-null)   ; rev 1
(raw-put! key-all-ff    val-ff)     ; rev 2

(let ((rn (mvcc-get-latest CTX key-with-null))
      (rf (mvcc-get-latest CTX key-all-ff)))
  (check "get key with embedded NUL" val-null (and rn (kv-rec-value rn)))
  (check "get key with all-0xFF bytes" val-ff  (and rf (kv-rec-value rf))))

; Range over both should find exactly 2 keys — and return the DECODED user
; keys byte-exactly (exercises key-cf-decode-user-key's escaped-0x00 path,
; native bytevector-nul-unescape since cw-71k).
(let* ((zero (make-bytevector 1 0))
       (res  (range-bv zero zero 'sort-order 'ascend 'sort-target 'key)))
  (check "embedded-null + all-ff: count = 2" 2 (range-count res))
  (let ((returned-keys (map car (range-kvs res))))
    (check "embedded-NUL key decodes byte-exactly" key-with-null (list-ref returned-keys 0))
    (check "all-0xFF key decodes byte-exactly"     key-all-ff    (list-ref returned-keys 1))))

; Direct enc/decode round-trips through the escaped-0x00 path (cw-71k):
; leading, trailing, and consecutive NUL runs all collapse back exactly.
(check "decode round-trip: leading NUL"   (bv 0 65)     (key-cf-decode-user-key (enc-key (bv 0 65) 7 0)))
(check "decode round-trip: trailing NUL"  (bv 65 0)     (key-cf-decode-user-key (enc-key (bv 65 0) 7 0)))
(check "decode round-trip: NUL run"       (bv 0 0 0)    (key-cf-decode-user-key (enc-key (bv 0 0 0) 7 0)))
(check "decode round-trip: mixed"         (bv 107 0 107) (key-cf-decode-user-key (enc-key (bv 107 0 107) 7 0)))

; ===========================================================================
(section "§2 prefix-vs-superkey: length-prefix keeps groups disjoint")
; ===========================================================================
; Keys "a" and "ab" — without the u64be(lenK) length prefix they could collide.
; With it len("a")=1 and len("ab")=2 force all of "a"'s enc-keys before "ab"'s.
; Same test for binary: #u8(1) vs #u8(1 1).

(reset-ctx! CTX)

(define ka  (b "a"))
(define kab (b "ab"))
(define kb1 (bv 1))
(define kb2 (bv 1 1))

(raw-put! ka  (b "va"))    ; rev 1
(raw-put! kab (b "vab"))   ; rev 2
(raw-put! kb1 (b "v1"))    ; rev 3
(raw-put! kb2 (b "v11"))   ; rev 4

; Point reads return each key independently.
(check "get 'a'"       (b "va")  (and (mvcc-get-latest CTX ka)
                                      (kv-rec-value (mvcc-get-latest CTX ka))))
(check "get 'ab'"      (b "vab") (and (mvcc-get-latest CTX kab)
                                      (kv-rec-value (mvcc-get-latest CTX kab))))
(check "get #u8(1)"    (b "v1")  (and (mvcc-get-latest CTX kb1)
                                      (kv-rec-value (mvcc-get-latest CTX kb1))))
(check "get #u8(1 1)"  (b "v11") (and (mvcc-get-latest CTX kb2)
                                      (kv-rec-value (mvcc-get-latest CTX kb2))))

; Range: prefix scan on "a" returns both "a" AND "ab" — both have prefix "a".
; pfe("a") = "b" (increments 0x61 -> 0x62), so the scan range is ["a","b"), which
; includes "a" (0x61) and "ab" (0x61 0x62), both < "b" (0x62).
; This is CORRECT prefix-scan semantics: the length-prefix in KEY-CF only guarantees
; that the STORE groups (for point reads) are disjoint; the user-space range ["a","b")
; intentionally matches everything that starts with 'a'.  Disjoint-group proof is in
; the point reads and ['a','ab') tests below.
(let* ((end (pfe ka))
       (res (range-bv ka end 'sort-order 'ascend 'sort-target 'key)))
  (check "prefix 'a' scan returns 'a' AND 'ab' (correct prefix semantics)" 2 (range-count res))
  (check "prefix 'a' first = 'a'"  ka  (car (list-ref (range-kvs res) 0)))
  (check "prefix 'a' second = 'ab'" kab (car (list-ref (range-kvs res) 1))))

; Range: point read of "ab" returns only "ab" (not "a").
(let ((res (range-bv kab #f)))
  (check "point 'ab' returns only 'ab'" 1 (range-count res))
  (check "point 'ab' key = 'ab'" kab (car (car (range-kvs res)))))

; Range: half-open ["a", "ab") must return only "a" since "ab" is the range-end.
(let ((res (range-bv ka kab)))
  (check "['a','ab'): only 'a'" 1 (range-count res))
  (check "['a','ab'): key = 'a'" ka (car (car (range-kvs res)))))

; Range: all-keys must find all 4 without bleed.
(let* ((zero (make-bytevector 1 0))
       (res  (range-bv zero zero 'sort-order 'ascend 'sort-target 'key)))
  (check "all 4 prefix/superkey keys present" 4 (range-count res)))

; ===========================================================================
(section "§3a empty value (#u8())")
; ===========================================================================

(reset-ctx! CTX)

(define key-ev  (b "empty-val"))
(define empty-v (make-bytevector 0 0))
(raw-put! key-ev empty-v)   ; rev 1

(let ((r (mvcc-get-latest CTX key-ev)))
  (check "empty-value key is live"  #t     (and r (not (kv-rec-tombstone? r))))
  (check "empty-value length = 0"   0      (and r (bytevector-length (kv-rec-value r))))
  (check "empty-value content"      empty-v (and r (kv-rec-value r))))

; Range should find it.
(let ((res (range-bv key-ev #f)))
  (check "range finds empty-value key" 1 (range-count res)))

; ===========================================================================
(section "§3b 4KB value")
; ===========================================================================
; A value of exactly 4096 bytes, all set to 0xAB.

(reset-ctx! CTX)

(define key-big  (b "bigval"))
(define big-v    (make-bytevector 4096 #xAB))
(raw-put! key-big big-v)   ; rev 1

(let ((r (mvcc-get-latest CTX key-big)))
  (check "4KB value: key is live"          #t   (and r (not (kv-rec-tombstone? r))))
  (check "4KB value: length = 4096"        4096 (and r (bytevector-length (kv-rec-value r))))
  (check "4KB value: first byte = 0xAB"    #xAB (and r (bytevector-u8-ref (kv-rec-value r) 0)))
  (check "4KB value: last byte = 0xAB"     #xAB (and r (bytevector-u8-ref (kv-rec-value r) 4095))))

; ===========================================================================
(section "§3c 50 versions: range returns latest; compact at 25 GCs old, keeps >=25")
; ===========================================================================

(reset-ctx! CTX)

(define key-mv (b "multikey"))

; Write 50 versions; each value encodes the version number for easy verification.
(let loop ((i 1))
  (if (<= i 50)
      (begin
        (raw-put! key-mv (string->utf8 (string-append "v" (number->string i))))
        (loop (+ i 1)))))

(check "current-rev = 50 after 50 versions" 50 (mvcc-current-rev CTX))

; Range at current should return version 50 (the latest).
(let ((res (range-bv key-mv #f)))
  (check "range at current: count=1" 1 (range-count res))
  (check "range at current: value=v50" "v50"
         (utf8->string (kv-rec-value (cdr (car (range-kvs res)))))))

; Compact at version 25.  After compaction:
;   - Versions 1..24 for key-mv are GC'd (old, below the latest-<=25 anchor).
;   - Version 25 (latest-<=25, non-tombstone) is kept as the historical anchor.
;   - Versions 26..50 are untouched (above compactRev).
;   - Read at rev 25 => "v25" (anchor readable).
;   - Read at rev 1..24 => ErrCompacted (below compactRev=25).
;   - Read at rev 26..50 => correct historical values.
(define cresult (mvcc-compact CTX 25))
(check "compact(25) -> (ok . 25)" (cons 'ok 25) cresult)
(check "compact-rev = 25" 25 (mvcc-compact-rev CTX))
(check "current-rev still 50" 50 (mvcc-current-rev CTX))

; Read at compactRev=25 must succeed and return v25.
(let ((res (range-bv key-mv #f 'revision 25)))
  (check "read multikey @rev25 = v25 (anchor)" "v25"
         (utf8->string (kv-rec-value (cdr (car (range-kvs res)))))))

; Read below compactRev => ErrCompacted.
(let ((res (range-bv key-mv #f 'revision 1)))
  (check "read multikey @rev1 < compact(25) => err-compacted" 'err-compacted (car res)))
(let ((res (range-bv key-mv #f 'revision 24)))
  (check "read multikey @rev24 < compact(25) => err-compacted" 'err-compacted (car res)))

; Read above compactRev => correct historical values.
(let ((res (range-bv key-mv #f 'revision 30)))
  (check "read multikey @rev30 = v30" "v30"
         (utf8->string (kv-rec-value (cdr (car (range-kvs res)))))))

; Current range still correct: v50.
(let ((res (range-bv key-mv #f)))
  (check "range at current after compact: v50" "v50"
         (utf8->string (kv-rec-value (cdr (car (range-kvs res)))))))

; ===========================================================================
(section "§4 lease round-trip")
; ===========================================================================
; A key stored with a non-zero lease id must have that id in the returned record.

(reset-ctx! CTX)

(define key-leased (b "leased-key"))
(define lease-id   12345)

(let* ((main (+ (mvcc-current-rev CTX) 1)))
  (mvcc-put! CTX key-leased (b "leased-val") lease-id main 0)
  (mvcc-set-current-rev! CTX main))

(let ((r (mvcc-get-latest CTX key-leased)))
  (check "leased key is live"      #t        (and r (not (kv-rec-tombstone? r))))
  (check "leased key value"        (b "leased-val") (and r (kv-rec-value r)))
  (check "leased key lease id"     lease-id  (and r (kv-rec-lease r))))

; The lease index entry must be present.
(let* ((lkey (enc-lease lease-id key-leased))
       (got  (kv-get CTX lkey)))
  (check "lease index entry present" #t (and got #t))
  (check "lease index value empty"   0  (and got (bytevector-length got))))

; Range on the key must also expose the lease id in the record.
(let* ((res (range-bv key-leased #f))
       (rec (cdr (car (range-kvs res)))))
  (check "range returns lease id" lease-id (kv-rec-lease rec)))

; ===========================================================================
(section "§5 revision semantics: 0 = current; current-rev = same as current")
; ===========================================================================
; Build a small store: two keys.

(reset-ctx! CTX)

(raw-put! (b "r5a") (b "r5va"))   ; rev 1
(raw-put! (b "r5b") (b "r5vb"))   ; rev 2
(define cur (mvcc-current-rev CTX))
(check "current-rev = 2 for §5" 2 cur)

; revision=0 means "use current".
(let* ((zero (make-bytevector 1 0))
       (res0 (range-bv zero zero 'revision 0 'sort-order 'ascend 'sort-target 'key))
       (resd (range-bv zero zero             'sort-order 'ascend 'sort-target 'key)))
  (check "revision=0 count = 2"       2 (range-count res0))
  (check "default revision count = 2" 2 (range-count resd))
  ; Results must be identical (same key bytevectors in the same order).
  (check "revision=0 same as default"
         (kv-keys-bv res0)
         (kv-keys-bv resd)))

; revision=current-rev must return the same set.
(let* ((zero (make-bytevector 1 0))
       (res-cur (range-bv zero zero 'revision cur 'sort-order 'ascend 'sort-target 'key))
       (res-def (range-bv zero zero             'sort-order 'ascend 'sort-target 'key)))
  (check "revision=current-rev count = 2" 2 (range-count res-cur))
  (check "revision=current-rev same as default"
         (kv-keys-bv res-cur)
         (kv-keys-bv res-def)))

; ===========================================================================
(done!)
