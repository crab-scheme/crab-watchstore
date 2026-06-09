; test/mvcc-range.scm — unit tests for mvcc-range (cw-u4a.7).
;
; Drives mvcc-range against a real RocksDB context built via mvcc-apply.
; Fresh per-run temp dir ((current-jiffy)) prevents state bleed between runs.

(include "test/harness.scm")
(include "src/store-ctx.scm")
(include "src/mvcc.scm")

; ---- open a fresh store ----
(define run-tag (number->string (current-jiffy)))
(define DB-PATH (string-append "/tmp/cws-mvcc-range-" run-tag))
(define CTX (make-ctx (store-open DB-PATH #t) "default" #t))

; ---- helpers ----
(define (b s) (string->utf8 s))
(define (put . parts)  (mvcc-apply CTX (map b parts)))
(define (del . parts)  (mvcc-apply CTX (map b parts)))

; Convenience: run mvcc-range and return the (count . kvlist) result.
(define (range key range-end . opts-pairs)
  (let ((opts (if (null? opts-pairs)
                  '()
                  (let build ((ps opts-pairs) (acc '()))
                    (if (null? ps)
                        (reverse acc)
                        (build (cddr ps) (cons (cons (car ps) (cadr ps)) acc)))))))
    (mvcc-range CTX key range-end opts)))

(define (range-count r)  (car r))
(define (range-kvs r)    (cdr r))
(define (kv-key  item)   (utf8->string (car item)))
(define (kv-val  item)   (utf8->string (kv-rec-value (cdr item))))
(define (kv-keys r)      (map kv-key (range-kvs r)))
(define (kv-vals r)      (map kv-val (range-kvs r)))

; ===========================================================================
; Build initial state
;   rev 1: a = "va"
;   rev 2: b = "vb"
;   rev 3: c = "vc"
;   rev 4: d = "vd"
;   rev 5: e = "ve"
; ===========================================================================
(put "PUT" "a" "va")   ; rev 1
(put "PUT" "b" "vb")   ; rev 2
(put "PUT" "c" "vc")   ; rev 3
(put "PUT" "d" "vd")   ; rev 4
(put "PUT" "e" "ve")   ; rev 5

; ===========================================================================
(section "point read: range-end #f")
(let ((r (range (b "a") #f)))
  (check "point a: count=1"   1  (range-count r))
  (check "point a: value=va"  '("va") (kv-vals r)))

(let ((r (range (b "c") #f)))
  (check "point c: count=1"   1  (range-count r))
  (check "point c: value=vc"  '("vc") (kv-vals r)))

(let ((r (range (b "z") #f)))
  (check "absent key: count=0"  0  (range-count r))
  (check "absent key: empty"    '() (kv-vals r)))

; ===========================================================================
(section "half-open range [a, c)")
(let ((r (range (b "a") (b "c"))))
  (check "[a,c) count=2"   2  (range-count r))
  (check "[a,c) keys=a,b"  '("a" "b") (kv-keys r))
  (check "[a,c) vals"      '("va" "vb") (kv-vals r)))

; ===========================================================================
(section "prefix scan via prefix-range-end")
; put "aa", "ab", "ac", "b" to test prefix isolation
(put "PUT" "aa" "vaa")   ; rev 6
(put "PUT" "ab" "vab")   ; rev 7
(put "PUT" "ac" "vac")   ; rev 8

(let* ((pfx (b "a"))
       (end (prefix-range-end pfx))
       (r   (range pfx end 'sort-order 'ascend 'sort-target 'key)))
  (check "prefix 'a': count=4 (a,aa,ab,ac)" 4 (range-count r))
  (check "prefix 'a': keys"
         '("a" "aa" "ab" "ac")
         (kv-keys r)))

; ===========================================================================
(section "all-keys convention: key=#u8(0) range-end=#u8(0)")
(let* ((zero (make-bytevector 1 0))
       (r    (range zero zero 'sort-order 'ascend 'sort-target 'key)))
  ; keys present at current-rev (8): a,aa,ab,ac,b,c,d,e
  (check "all-keys count=8"  8  (range-count r))
  (check "all-keys first=a"  "a" (kv-key (car (range-kvs r))))
  (check "all-keys last=e"   "e" (kv-key (car (reverse (range-kvs r))))))

; ===========================================================================
(section "to-eof range: range-end=#u8(0) with key='c'")
(let* ((zero (make-bytevector 1 0))
       (r    (range (b "c") zero 'sort-order 'ascend 'sort-target 'key)))
  ; keys >= c: c, d, e
  (check "to-eof from c: count=3"  3  (range-count r))
  (check "to-eof from c: keys"     '("c" "d" "e") (kv-keys r)))

; ===========================================================================
(section "multi-version: read-at-rev")
; put "mv" three times to get three versions
(put "PUT" "mv" "mv1")  ; rev 9  (create_rev=9, mod_rev=9, version=1)
(put "PUT" "mv" "mv2")  ; rev 10 (create_rev=9, mod_rev=10, version=2)
(put "PUT" "mv" "mv3")  ; rev 11 (create_rev=9, mod_rev=11, version=3)

(define rev-r1 9)
(define rev-r2 10)
(define rev-r3 11)

(let ((r (range (b "mv") #f)))
  (check "mv at current rev: count=1" 1 (range-count r))
  (check "mv at current rev: mv3"     "mv3" (kv-val (car (range-kvs r)))))

(let ((r (range (b "mv") #f 'revision rev-r2)))
  (check "mv at rev-r2: count=1" 1 (range-count r))
  (check "mv at rev-r2: mv2"     "mv2" (kv-val (car (range-kvs r)))))

(let ((r (range (b "mv") #f 'revision rev-r1)))
  (check "mv at rev-r1: count=1" 1 (range-count r))
  (check "mv at rev-r1: mv1"     "mv1" (kv-val (car (range-kvs r)))))

; ===========================================================================
(section "tombstone: delete b, live range excludes it")
(del "DEL" "b")  ; rev 12

; use [b, c) to isolate just key "b" (aa/ab/ac all sort before "b" under bv<?)
(let ((r (range (b "b") (b "c"))))
  (check "[b,c) after del b: count=0"   0  (range-count r))
  (check "[b,c) after del b: empty"     '() (kv-keys r)))

; read-at-rev BEFORE the delete (rev 11): b was live
(let ((r (range (b "b") (b "c") 'revision 11)))
  (check "[b,c) @rev11 (before del): count=1" 1 (range-count r))
  (check "[b,c) @rev11: key=b"                '("b") (kv-keys r)))

; ===========================================================================
(section "limit: N records, total count = full match count")
; all-keys at current-rev: a, aa, ab, ac, b(tombstone), c, d, e, mv + no b
; live: a, aa, ab, ac, c, d, e, mv = 8 keys
(let* ((zero (make-bytevector 1 0))
       (r    (range zero zero 'sort-order 'ascend 'sort-target 'key
                              'limit 3)))
  (check "limit 3: count = 8 (full)"   8  (range-count r))
  (check "limit 3: kvs length = 3"     3  (length (range-kvs r)))
  (check "limit 3: first 3 keys"       '("a" "aa" "ab") (kv-keys r)))

; ===========================================================================
(section "count-only: correct count, no records")
(let* ((zero (make-bytevector 1 0))
       (r    (range zero zero 'count-only #t)))
  (check "count-only: count=8"    8  (range-count r))
  (check "count-only: no kvs"     '() (range-kvs r)))

; ===========================================================================
(section "keys-only: keys present, values empty")
; use point read on "a" for simplicity
(let ((r (range (b "a") #f 'keys-only #t)))
  (check "keys-only: count=1" 1 (range-count r))
  (let ((item (car (range-kvs r))))
    (check "keys-only: key=a"       "a" (kv-key item))
    (check "keys-only: value empty"  0  (bytevector-length (kv-rec-value (cdr item))))))

; ===========================================================================
(section "sort: key descend")
(let* ((zero (make-bytevector 1 0))
       (r    (range zero zero 'sort-order 'descend 'sort-target 'key)))
  ; live keys ascending: a, aa, ab, ac, c, d, e, mv
  ; descending: mv, e, d, c, ac, ab, aa, a
  (check "sort key descend: first=mv" "mv" (kv-key (car (range-kvs r))))
  (check "sort key descend: last=a"   "a"  (kv-key (car (reverse (range-kvs r))))))

; ===========================================================================
(section "sort: mod_rev ascending")
; Build a fresh mini-db with known mod_revs: keys x,y,z put in order rev 13,14,15
(put "PUT" "x" "vx")   ; rev 13, mod_rev=13
(put "PUT" "y" "vy")   ; rev 14, mod_rev=14
(put "PUT" "z" "vz")   ; rev 15, mod_rev=15

(let ((r (range (b "x") (b "z\x01") 'sort-order 'ascend 'sort-target 'mod)))
  (check "sort mod ascend: count=3" 3 (range-count r))
  (let ((mod-revs (map (lambda (item) (kv-rec-mod-rev (cdr item))) (range-kvs r))))
    (check "sort mod ascend: order 13,14,15" '(13 14 15) mod-revs)))

; ===========================================================================
(section "sort: version ascending")
; mv has version=3 (put 3 times), x/y/z have version=1
; Range [mv, z+1) ascending by version: mv last (version 3), x/y/z first (version 1)
; -- just check the relative order of versions in a known set
(let ((r (range (b "mv") (b "z\x01") 'sort-order 'ascend 'sort-target 'version)))
  ; mv=version3, x=version1, y=version1, z=version1
  (check "sort version ascend: first has version 1"
         1 (kv-rec-version (cdr (car (range-kvs r))))))

; ===========================================================================
(section "sort: create_rev ascending")
; For this test: aa was created at rev 6, ac at rev 8, ab at rev 7
; range [aa, ad) sort by create_rev ascending => aa(6), ab(7), ac(8)
(let ((r (range (b "aa") (b "ad") 'sort-order 'ascend 'sort-target 'create)))
  (check "sort create ascend: count=3" 3 (range-count r))
  (let ((crs (map (lambda (item) (kv-rec-create-rev (cdr item))) (range-kvs r))))
    (check "sort create ascend: 6,7,8" '(6 7 8) crs))
  (check "sort create ascend: keys" '("aa" "ab" "ac") (kv-keys r)))

; ===========================================================================
(section "min/max mod-rev filter")
; keys x,y,z have mod_rev 13,14,15
; min-mod-rev=14 => y,z (drop x)
(let ((r (range (b "x") (b "z\x01") 'min-mod-rev 14)))
  (check "min-mod-rev 14: count=2" 2 (range-count r))
  (check "min-mod-rev 14: keys=y,z" '("y" "z") (kv-keys r)))

; max-mod-rev=14 => x,y (drop z)
(let ((r (range (b "x") (b "z\x01") 'max-mod-rev 14)))
  (check "max-mod-rev 14: count=2" 2 (range-count r))
  (check "max-mod-rev 14: keys=x,y" '("x" "y") (kv-keys r)))

; min=14 max=14 => only y
(let ((r (range (b "x") (b "z\x01") 'min-mod-rev 14 'max-mod-rev 14)))
  (check "min=max=14: count=1"   1  (range-count r))
  (check "min=max=14: key=y"     '("y") (kv-keys r)))

; ===========================================================================
(section "ErrCompacted: query below compact-rev returns error")
; Manually set compact-rev to simulate compaction (cw-u4a.8 does the full impl).
; compact-rev = 5 means reads at revision < 5 should return err-compacted.
(kv-put! CTX META-COMPACT-REV (u64->bytes 5))

(let ((r (range (b "a") #f 'revision 3)))
  (check "err-compacted at rev 3 (compact=5)"
         'err-compacted (car r))
  (check "err-compacted carries compact-rev=5"
         5 (cdr r)))

; revision = compact-rev itself (5) is NOT compacted (only < compact-rev is)
(let ((r (range (b "a") #f 'revision 5)))
  (check "revision=5 not compacted (>= compact-rev)"
         1 (range-count r)))

; revision 0 => current rev => no compacted error
(let ((r (range (b "a") #f)))
  (check "default revision (0 => current) no error"
         1 (range-count r)))

; reset compact-rev so remaining tests (if any) don't interfere
(kv-put! CTX META-COMPACT-REV (u64->bytes 0))

; ===========================================================================
(done!)
