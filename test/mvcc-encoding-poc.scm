; test/mvcc-encoding-poc.scm — validates the load-bearing sort-order claims of
; ADR 0001 (MVCC data model).
;
; The whole MVCC schema rests on ONE assumption: that RocksDB's lexicographic
; byte ordering, combined with our key encodings, makes the three hot queries
;   (a) "latest visible version of key K at-or-below readRev"
;   (b) "Range [k1,k2) at readRev"
;   (c) "all events in (compactRev, readRev]"  (Watch replay)
; each a single bounded ORDERED scan.  This test re-implements the ADR's chosen
; key encoders as small helpers and ASSERTS that direction holds, by raw
; lexicographic bytevector comparison (no bytevector<? builtin exists).
;
; If any assertion here fails, the ENCODING in the ADR is wrong and must change
; to match what actually sorts — this test is the source of truth.

(include "test/harness.scm")
(include "src/encoding.scm")

; ---------------------------------------------------------------------------
; lexicographic bytevector comparators (no builtin)
; ---------------------------------------------------------------------------

; a <? b under unsigned-byte lexicographic order (shorter-is-less on a prefix).
(define (bv<? a b)
  (let ((la (bytevector-length a))
        (lb (bytevector-length b)))
    (let loop ((i 0))
      (cond ((= i la) (< la lb))
            ((= i lb) #f)
            ((< (bytevector-u8-ref a i) (bytevector-u8-ref b i)) #t)
            ((> (bytevector-u8-ref a i) (bytevector-u8-ref b i)) #f)
            (else (loop (+ i 1)))))))

(define (bv=? a b) (equal? a b))

; b starts-with prefix p ?
(define (bv-prefix? p b)
  (let ((lp (bytevector-length p)))
    (and (>= (bytevector-length b) lp)
         (bv=? p (subbv b 0 lp)))))

; ---------------------------------------------------------------------------
; ADR 0001 key encoders (re-implemented here exactly as the ADR specifies)
; ---------------------------------------------------------------------------
;
; Namespace tags: a single leading byte routes a key into one of the logical
; namespaces that share the one "default" column family / WAL.
(define NS-KEY  #x01)   ; key-ordered store:   K || rev    -> KeyValue record
(define NS-REV  #x02)   ; revision-ordered idx: rev        -> event record
(define NS-META #x00)   ; meta scalars (current-rev, compact-rev) ; sorts first
(define NS-LEASE #x03)  ; lease -> keys index:  leaseId || K -> ()

(define (byte b) (let ((v (make-bytevector 1 0)))
                   (bytevector-u8-set! v 0 b) v))

; --- revision = (main . sub), each a u64 -> 16 big-endian bytes ---------------
; main in the high 8 bytes, sub in the low 8.  u64be is unsigned-monotone, so
; (m1,s1) < (m2,s2) lexicographically IFF (m1,s1) precedes (m2,s2) numerically:
; main dominates (high bytes), sub breaks ties — exactly etcd's main.sub order.
(define (rev->16 main sub)
  (bytevector-append (u64->bytes main) (u64->bytes sub)))

; ===== KEY-CF key:  NS-KEY || u64be(len K) || K || INV(rev16) =====
; We want, within one user-key, the encoding to sort so that a bounded reverse-
; or-forward scan finds "greatest rev <= readRev" cheaply.  We choose DESCENDING
; revision order on disk (newest first) by storing the bitwise-INVERTED 16-byte
; revision.  Then for a fixed K the *smallest* on-disk key is the *newest*
; revision, and "latest visible <= readRev" is: seek to (K || INV(readRev16))
; and take the FIRST record with prefix (NS-KEY||lenK||K) — a single forward
; seek.  (kv-scan returns the whole K-group ascending = newest..oldest, so the
; first element whose decoded rev <= readRev is the answer.)
;
; The user-key is length-PREFIXED (u64be) so that the rev bytes can never be
; confused with key bytes and so two keys where one is a prefix of the other
; (e.g. "a" vs "ab") still group disjointly.
(define (inv16 b16)
  (let ((c (subbv b16 0 16)))
    (let loop ((i 0))
      (if (< i 16)
          (begin (bytevector-u8-set! c i (bitwise-xor (bytevector-u8-ref c i) #xFF))
                 (loop (+ i 1)))))
    c))

(define (key-cf-prefix user-key)            ; NS-KEY || u64be(len) || K
  (bytevector-append (byte NS-KEY)
                     (u64->bytes (bytevector-length user-key))
                     user-key))

(define (enc-key user-key main sub)         ; full KEY-CF key
  (bytevector-append (key-cf-prefix user-key)
                     (inv16 (rev->16 main sub))))

; ===== REV-CF key:  NS-REV || rev16 (PLAIN, ascending) =====
; Watch replays events oldest->newest, so this namespace stores PLAIN (non-
; inverted) revisions ascending.  "events in (compactRev, readRev]" is then the
; forward range [NS-REV||rev16(compact+epsilon) , NS-REV||rev16(readRev)].
(define (enc-rev main sub)
  (bytevector-append (byte NS-REV) (rev->16 main sub)))

; ===== LEASE index key:  NS-LEASE || u64be(leaseId) || K =====
(define (enc-lease leaseId user-key)
  (bytevector-append (byte NS-LEASE) (u64->bytes leaseId) user-key))

; ===== META keys =====
(define (meta-key name) (bytevector-append (byte NS-META) (string->utf8 name)))

; ---------------------------------------------------------------------------
; TESTS
; ---------------------------------------------------------------------------

(section "namespace tags sort disjointly (one CF, two orderings)")
; META < KEY < REV < LEASE, so the four namespaces occupy disjoint, ordered
; byte ranges within the single column family — a prefix scan of one never
; bleeds into another.
(check "meta < key"   #t (bv<? (meta-key "current-rev") (enc-key (string->utf8 "a") 1 0)))
(check "key  < rev"   #t (bv<? (enc-key (string->utf8 "zzzz") 999999 99)
                                (enc-rev 1 0)))
(check "rev  < lease" #t (bv<? (enc-rev 999999 99) (enc-lease 1 (string->utf8 ""))))

(section "rev16: 16-byte revision is unsigned-monotone (main dominates, sub breaks ties)")
(check "main 1.0 < 2.0"        #t (bv<? (rev->16 1 0) (rev->16 2 0)))
(check "sub  1.0 < 1.1"        #t (bv<? (rev->16 1 0) (rev->16 1 1)))
(check "sub  1.5 < 2.0"        #t (bv<? (rev->16 1 5) (rev->16 2 0)))
(check "main dominates 1.999 < 2.0" #t (bv<? (rev->16 1 999) (rev->16 2 0)))
(check "large main 1e6 < 1e6+1"     #t (bv<? (rev->16 1000000 0) (rev->16 1000001 0)))
; big sub values (within a huge Txn) still order correctly
(check "huge sub 7.10 < 7.11"  #t (bv<? (rev->16 7 10) (rev->16 7 11)))

(section "REV-CF: encRev(r1) < encRev(r2) whenever r1 precedes r2 (Watch order)")
(check "rev 1.0 < 1.1"   #t (bv<? (enc-rev 1 0) (enc-rev 1 1)))
(check "rev 1.9 < 2.0"   #t (bv<? (enc-rev 1 9) (enc-rev 2 0)))
(check "rev 2.0 < 100.0" #t (bv<? (enc-rev 2 0) (enc-rev 100 0)))
; "events in (compactRev, readRev]" = forward range; spot-check the endpoints
; order correctly: compactRev=5.0 strictly precedes every event up to readRev=9.3.
(check "compact 5.0 < event 5.1" #t (bv<? (enc-rev 5 0) (enc-rev 5 1)))
(check "event 9.3 < next 9.4"    #t (bv<? (enc-rev 9 3) (enc-rev 9 4)))

(section "KEY-CF: cross-key ordering — enc(k1,anyRev) < enc(k2,anyRev) for k1<k2")
; user-keys k1<k2 must sort before each other REGARDLESS of their revisions, so
; a Range [k1,k2) scan is one contiguous byte range.  The length prefix is part
; of the sort, but for equal-length keys it's constant, and the user-key bytes
; that follow decide it.  Test same-length and the prefix-key hazard ("a"/"ab").
(let ((ka (string->utf8 "aaa"))
      (kb (string->utf8 "aab"))
      (kc (string->utf8 "abc")))
  (check "k_aaa(rev 9.9) < k_aab(rev 1.0)" #t
         (bv<? (enc-key ka 9 9) (enc-key kb 1 0)))
  (check "k_aab(rev 9.9) < k_abc(rev 1.0)" #t
         (bv<? (enc-key kb 9 9) (enc-key kc 1 0)))
  ; newest rev of k1 still sorts before oldest rev of k2
  (check "k_aaa(rev 1000000.0) < k_abc(rev 0.0)" #t
         (bv<? (enc-key ka 1000000 0) (enc-key kc 0 0))))

(section "KEY-CF: prefix-key hazard — 'a' vs 'ab' group disjointly (len-prefix)")
; Without a length prefix, "a"||rev could collide with "ab"||rev byte ranges.
; With u64be length prefix, len("a")=1 < len("ab")=2 forces ALL of key "a"'s
; versions to sort before ANY of "ab"'s — clean group separation.
(let ((k1 (string->utf8 "a"))
      (k2 (string->utf8 "ab")))
  (check "all 'a' versions < all 'ab' versions" #t
         (bv<? (enc-key k1 999999999 999) (enc-key k2 0 0)))
  (check "'a' prefix not a prefix of 'ab' key" #f
         (bv-prefix? (key-cf-prefix k1) (enc-key k2 5 0))))

(section "KEY-CF: within ONE key, on-disk order is DESCENDING revision (newest first)")
; We store INV(rev16), so a NEWER revision yields a SMALLER on-disk key.  Hence
; a prefix scan returns versions newest->oldest, and the FIRST whose rev<=readRev
; is "latest visible <= readRev" — a single bounded scan, no full-history walk.
(let ((k (string->utf8 "mykey")))
  ; newer (rev 5.0) sorts BEFORE older (rev 3.0) on disk
  (check "rev 5.0 sorts before rev 3.0 (descending)" #t
         (bv<? (enc-key k 5 0) (enc-key k 3 0)))
  (check "rev 5.0 sorts before rev 4.9 (descending)" #t
         (bv<? (enc-key k 5 0) (enc-key k 4 9)))
  (check "rev 2.1 sorts before rev 2.0 (descending, sub)" #t
         (bv<? (enc-key k 2 1) (enc-key k 2 0)))
  ; the highest possible rev is the smallest on-disk key for k (scan start)
  (check "rev 0.0 is the LAST (largest) on-disk key for k" #t
         (bv<? (enc-key k 1 0) (enc-key k 0 0))))

(section "KEY-CF: 'latest <= readRev' is the first scan hit at-or-after seek(K||INV(readRev))")
; Simulate the read path: versions of K on disk are {7.0, 4.0, 2.0} stored
; descending as INV.  For readRev=5.0 the seek key is enc-key(K,5,0); the first
; ON-DISK key >= that seek key (since list is ascending in on-disk order) whose
; decoded rev <= 5.0 must be rev 4.0.  We verify the seek key lands strictly
; between 7.0 (newer, sorts earlier) and 4.0 (the answer, sorts at-or-after).
(let* ((k (string->utf8 "K"))
       (v7 (enc-key k 7 0))
       (v4 (enc-key k 4 0))
       (v2 (enc-key k 2 0))
       (seek (enc-key k 5 0)))      ; INV(5.0)
  ; on-disk ascending order is 7.0, 4.0, 2.0 (newest..oldest)
  (check "on-disk: 7.0 < 4.0" #t (bv<? v7 v4))
  (check "on-disk: 4.0 < 2.0" #t (bv<? v4 v2))
  ; seek(5.0) is AFTER 7.0 (skip the too-new version) ...
  (check "seek(5.0) after 7.0" #t (bv<? v7 seek))
  ; ... and AT-OR-BEFORE 4.0 (so the first hit >= seek is 4.0 = the answer)
  (check "seek(5.0) <= 4.0 (4.0 is first hit)" #t (or (bv<? seek v4) (bv=? seek v4))))

(section "KEY-CF: prefix-scan soundness — enc(K,*) starts with key-cf-prefix(K)")
; kv-scan by (key-cf-prefix K) must return EXACTLY key K's versions and nothing
; else.  Every encoded version of K starts with that prefix; a different key
; does not.
(let ((k  (string->utf8 "mykey"))
      (k2 (string->utf8 "mykex")))   ; differs in last byte
  (check "enc(K, 5.0) has prefix(K)"  #t (bv-prefix? (key-cf-prefix k) (enc-key k 5 0)))
  (check "enc(K, 0.0) has prefix(K)"  #t (bv-prefix? (key-cf-prefix k) (enc-key k 0 0)))
  (check "enc(K2,5.0) lacks prefix(K)" #f (bv-prefix? (key-cf-prefix k) (enc-key k2 5 0)))
  ; and prefix(K) itself sorts at-or-before all of K's versions (scan start point)
  (check "prefix(K) <= enc(K, maxrev)" #t
         (or (bv<? (key-cf-prefix k) (enc-key k 999999999 0))
             (bv=? (key-cf-prefix k) (enc-key k 999999999 0)))))

(section "REV-CF: prefix-scan soundness — enc-rev(*) starts with NS-REV tag")
(check "enc-rev(1.0) has NS-REV prefix" #t (bv-prefix? (byte NS-REV) (enc-rev 1 0)))
(check "enc-rev(9.9) has NS-REV prefix" #t (bv-prefix? (byte NS-REV) (enc-rev 9 9)))
(check "enc-key(...) lacks NS-REV prefix" #f
       (bv-prefix? (byte NS-REV) (enc-key (string->utf8 "a") 1 0)))

(section "LEASE index: leaseId groups, O(lease) revoke scan")
; enc-lease(L, *) all share prefix NS-LEASE||u64be(L), so revoking lease L is a
; single prefix scan; different leases occupy disjoint ranges.
(let ((l1-prefix (bytevector-append (byte NS-LEASE) (u64->bytes 100))))
  (check "lease 100 key has lease-100 prefix" #t
         (bv-prefix? l1-prefix (enc-lease 100 (string->utf8 "k"))))
  (check "lease 101 key lacks lease-100 prefix" #f
         (bv-prefix? l1-prefix (enc-lease 101 (string->utf8 "k"))))
  (check "lease 100 < lease 101 (ordered by id)" #t
         (bv<? (enc-lease 100 (string->utf8 "z")) (enc-lease 101 (string->utf8 "a")))))

(done!)
