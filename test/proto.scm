; test/proto.scm — round-trip + byte-for-byte WIRE-COMPAT proof for the pure-Scheme
; proto3 codec (cw-u4a.19).  No store, no Raft, no reset — pure functions only.
;
; Two proofs:
;   §A  ROUND-TRIP    — (pb-decode S (pb-encode S v)) recovers v for every etcd
;                       message, including empty/default, repeated, nested, large,
;                       negative, and the cyclic TxnRequest/TxnResponse schemas.
;   §B  WIRE-COMPAT   — pb-encode produces EXACTLY the bytes hand-computed from the
;                       proto3 spec (protoc/python-protobuf are NOT installed), and
;                       pb-decode of those exact real-format bytes yields the right
;                       alist.  This is the byte-level compatibility proof with etcd.

(include "test/harness.scm")
(include "src/encoding.scm")
(include "src/proto.scm")

; ---------------------------------------------------------------------------
; helpers
; ---------------------------------------------------------------------------

(define (b s) (string->utf8 s))

; bytevector -> list of u8 (so `check` prints a readable diff on mismatch)
(define (bv->list x)
  (let loop ((i 0) (acc '()))
    (if (= i (bytevector-length x)) (reverse acc)
        (loop (+ i 1) (cons (bytevector-u8-ref x i) acc)))))

; list of u8 -> bytevector
(define (u8s . xs)
  (let* ((n (length xs)) (out (make-bytevector n 0)))
    (let loop ((i 0) (l xs))
      (if (null? l) out
          (begin (bytevector-u8-set! out i (car l)) (loop (+ i 1) (cdr l)))))))

; is x an alist (message), as opposed to a plain list (repeated)?
(define (msg-alist? x) (and (pair? x) (pair? (car x)) (symbol? (caar x))))

; Directional, order-INSENSITIVE "round-trip match": every key in `a` must
; appear in `b` with a pb-equal? value (b MAY carry extra keys — those are the
; proto3 defaults that pb-decode fills for fields `a` omitted).  Repeated lists
; match elementwise with equal length (repeated fields aren't default-filled).
; The companion `canon idempotent` check (raw equal?) bounds b's extras to
; defaults only, so "a ⊆ b" + idempotency together pin the round-trip exactly.
(define (pb-equal? a b)
  (cond
    ((and (bytevector? a) (bytevector? b)) (equal? a b))
    ((null? a) #t)                       ; empty message: asserts no keys -> matches
    ((and (msg-alist? a) (msg-alist? b))
     (let loop ((ks a))
       (or (null? ks)
           (let* ((k (caar ks)) (cell (assq k b)))
             (and cell (pb-equal? (cdar ks) (cdr cell)) (loop (cdr ks)))))))
    ((and (list? a) (list? b))           ; repeated field
     (and (= (length a) (length b))
          (let loop ((x a) (y b))
            (or (null? x) (and (pb-equal? (car x) (car y)) (loop (cdr x) (cdr y)))))))
    (else (equal? a b))))

; canonical form = decode . encode (fills defaults, fixes key order)
(define (canon S v) (pb-decode S (pb-encode S v)))

; round-trip assertion: decode∘encode recovers the input (order-insensitive),
; AND the canonical form is a stable fixpoint under raw equal? (idempotent).
(define (check-roundtrip name S v)
  (check (string-append name " round-trip")     #t (pb-equal? v (canon S v)))
  (check (string-append name " canon idempotent") #t (equal? (canon S v) (canon S (canon S v)))))

; ===========================================================================
(section "Layer-1 primitives — varint / zigzag / tag / fixed")
; ===========================================================================

; varint encode (the LSB-group, MSB-continuation base-128 core)
(check "varint 0"     '(0)         (bv->list (uint->varint 0)))
(check "varint 1"     '(1)         (bv->list (uint->varint 1)))
(check "varint 127"   '(127)       (bv->list (uint->varint 127)))
(check "varint 128"   '(128 1)     (bv->list (uint->varint 128)))
(check "varint 150"   '(150 1)     (bv->list (uint->varint 150)))
(check "varint 300"   '(172 2)     (bv->list (uint->varint 300)))   ; 0xAC 0x02
(check "varint 16384" '(128 128 1) (bv->list (uint->varint 16384)))
(check "varint maxu64"
       '(255 255 255 255 255 255 255 255 255 1)
       (bv->list (uint->varint (- (expt 2 64) 1))))

; int64 two's-complement negatives -> 10-byte varint (proto3 rule, NOT zigzag)
(check "int64 -1 (2c)"
       '(255 255 255 255 255 255 255 255 255 1)
       (bv->list (sint-2c->varint -1)))
(check "int64 -2 (2c)"
       '(254 255 255 255 255 255 255 255 255 1)
       (bv->list (sint-2c->varint -2)))

; varint decode round-trip
(check "decode 300"   300   (call-with-values (lambda () (read-varint (u8s 172 2) 0))
                                              (lambda (v o) v)))
(check "decode -1"    -1    (varint->int64
                              (call-with-values (lambda () (read-varint (u8s 255 255 255 255 255 255 255 255 255 1) 0))
                                                (lambda (v o) v))))

; zigzag (sint*): small magnitudes -> small varints
(check "zigzag 0"   0 (zigzag-encode 0))
(check "zigzag -1"  1 (zigzag-encode -1))
(check "zigzag 1"   2 (zigzag-encode 1))
(check "zigzag -2"  3 (zigzag-encode -2))
(check "zigzag 2147483647" 4294967294 (zigzag-encode 2147483647))
(check "zigzag-decode 1"  -1 (zigzag-decode 1))
(check "zigzag-decode 2"   1 (zigzag-decode 2))
(check "zigzag roundtrip -2147483648" -2147483648 (zigzag-decode (zigzag-encode -2147483648)))

; field tag = (field_number << 3) | wire_type, varint-encoded
(check "tag f1 wt2 (LEN)"    '(10) (bv->list (encode-tag 1 WT-LEN)))     ; 0x0A
(check "tag f2 wt0 (VARINT)" '(16) (bv->list (encode-tag 2 WT-VARINT)))  ; 0x10
(check "tag f5 wt2 (LEN)"    '(42) (bv->list (encode-tag 5 WT-LEN)))     ; 0x2A
(check "tag f9 wt0"          '(72) (bv->list (encode-tag 9 WT-VARINT)))  ; 0x48
(check "tag f64 wt2"         '(130 4) (bv->list (encode-tag 64 WT-LEN))) ; range_end in Compare
(check "split-tag 10"  (cons 1 2) (split-tag 10))
(check "split-tag 72"  (cons 9 0) (split-tag 72))

; fixed32 / fixed64 little-endian
(check "fixed32 1 LE" '(1 0 0 0)         (bv->list (uint32->fixed32 1)))
(check "fixed64 1 LE" '(1 0 0 0 0 0 0 0) (bv->list (uint64->fixed64 1)))

; ===========================================================================
(section "§A round-trip — all etcd KV-service messages")
; ===========================================================================

; -- KeyValue with ALL fields populated --
(check-roundtrip "KeyValue full" KeyValue-schema
  (list (cons 'key (b "mykey")) (cons 'create_revision 10)
        (cons 'mod_revision 12) (cons 'version 3)
        (cons 'value (b "myval")) (cons 'lease 99)))

; -- KeyValue with default/zero fields OMITTED then refilled --
(check-roundtrip "KeyValue defaults" KeyValue-schema
  (list (cons 'key (b "k")) (cons 'value (b "v"))))   ; revisions/version/lease = 0

; -- KeyValue with a NEGATIVE revision (two's-complement 10-byte varint) --
(check-roundtrip "KeyValue neg rev" KeyValue-schema
  (list (cons 'create_revision -1) (cons 'mod_revision -2)))

; -- KeyValue with a LARGE value + large lease (near int64 max) --
(check-roundtrip "KeyValue large" KeyValue-schema
  (list (cons 'key (b "bigkey"))
        (cons 'value (make-bytevector 5000 65))      ; 5000 'A' bytes
        (cons 'lease (- (expt 2 63) 1))))

; -- empty KeyValue (everything default -> encodes to zero bytes) --
(check "empty KeyValue encodes to 0 bytes" 0 (bytevector-length (pb-encode KeyValue-schema '())))
(check-roundtrip "KeyValue empty" KeyValue-schema '())

; -- ResponseHeader --
(check-roundtrip "ResponseHeader" ResponseHeader-schema
  (list (cons 'cluster_id 7) (cons 'member_id 3) (cons 'revision 42) (cons 'raft_term 5)))

; -- RangeRequest (several fields incl. high field numbers + bools) --
(check-roundtrip "RangeRequest" RangeRequest-schema
  (list (cons 'key (b "a")) (cons 'range_end (b "z"))
        (cons 'limit 100) (cons 'revision 9)
        (cons 'serializable #t) (cons 'keys_only #t) (cons 'count_only #t)
        (cons 'min_mod_revision 1) (cons 'max_create_revision 50)))

; -- RangeResponse with REPEATED nested KeyValue + header --
(check-roundtrip "RangeResponse" RangeResponse-schema
  (list (cons 'header (list (cons 'revision 9)))
        (cons 'kvs (list (list (cons 'key (b "k1")) (cons 'value (b "v1")) (cons 'mod_revision 9))
                         (list (cons 'key (b "k2")) (cons 'value (b "v2")) (cons 'mod_revision 9))))
        (cons 'more #f) (cons 'count 2)))

; -- PutRequest / PutResponse --
(check-roundtrip "PutRequest" PutRequest-schema
  (list (cons 'key (b "k")) (cons 'value (b "v")) (cons 'lease 5) (cons 'prev_kv #t)))
(check-roundtrip "PutResponse" PutResponse-schema
  (list (cons 'header (list (cons 'revision 9)))
        (cons 'prev_kv (list (cons 'key (b "k")) (cons 'value (b "old")) (cons 'mod_revision 8)))))

; -- DeleteRangeRequest / DeleteRangeResponse (repeated prev_kvs) --
(check-roundtrip "DeleteRangeRequest" DeleteRangeRequest-schema
  (list (cons 'key (b "a")) (cons 'range_end (b "c")) (cons 'prev_kv #t)))
(check-roundtrip "DeleteRangeResponse" DeleteRangeResponse-schema
  (list (cons 'header (list (cons 'revision 9))) (cons 'deleted 2)
        (cons 'prev_kvs (list (list (cons 'key (b "a")) (cons 'value (b "va")))
                              (list (cons 'key (b "b")) (cons 'value (b "vb")))))))

; -- Compare (oneof target_union: only create_revision present; range_end high) --
(check-roundtrip "Compare" Compare-schema
  (list (cons 'result 0) (cons 'target 1) (cons 'key (b "k"))
        (cons 'create_revision 5) (cons 'range_end (b "k0"))))

; -- CompactionRequest / CompactionResponse --
(check-roundtrip "CompactionRequest" CompactionRequest-schema
  (list (cons 'revision 100) (cons 'physical #t)))
(check-roundtrip "CompactionResponse" CompactionResponse-schema
  (list (cons 'header (list (cons 'revision 100)))))

; -- TxnRequest with Compares + nested RequestOps (the CYCLIC schema path) --
(check-roundtrip "TxnRequest nested" TxnRequest-schema
  (list
    (cons 'compare (list (list (cons 'result 0) (cons 'target 1)
                               (cons 'key (b "k1")) (cons 'create_revision 5))))
    (cons 'success (list (list (cons 'request_put
                                     (list (cons 'key (b "k1")) (cons 'value (b "v1")))))))
    (cons 'failure (list (list (cons 'request_range
                                     (list (cons 'key (b "k1")))))))))

; -- TxnResponse with ResponseOps (recurses ResponseOp -> *Response -> header) --
(check-roundtrip "TxnResponse nested" TxnResponse-schema
  (list
    (cons 'header (list (cons 'cluster_id 7) (cons 'revision 9)))
    (cons 'succeeded #t)
    (cons 'responses (list (list (cons 'response_put
                                       (list (cons 'header (list (cons 'revision 9))))))
                           (list (cons 'response_range
                                       (list (cons 'header (list (cons 'revision 9)))
                                             (cons 'count 1))))))))

; -- RequestOp wrapping a nested TxnRequest (request_txn=4: 2-level recursion) --
(check-roundtrip "RequestOp -> nested Txn" RequestOp-schema
  (list (cons 'request_txn
              (list (cons 'success
                          (list (list (cons 'request_put
                                            (list (cons 'key (b "k")) (cons 'value (b "v")))))))))))

; ===========================================================================
(section "§B WIRE-COMPAT — pb-encode == hand-computed proto3 reference bytes")
; ===========================================================================
;
; Each reference byte sequence is hand-derived from the proto3 encoding spec.
; Tag byte = (field_number << 3) | wire_type.

; -- KeyValue{key="foo",create_revision=2,mod_revision=3,version=1,value="bar",lease=0} --
;   f1 key   : 0A 03 'f' 'o' 'o'      (66 6F 6F)
;   f2 crev  : 10 02
;   f3 mrev  : 18 03
;   f4 ver   : 20 01
;   f5 value : 2A 03 'b' 'a' 'r'      (62 61 72)
;   f6 lease : OMITTED (0 = proto3 default)
(check "KeyValue ref bytes"
       (u8s #x0A 3 #x66 #x6F #x6F  #x10 2  #x18 3  #x20 1  #x2A 3 #x62 #x61 #x72)
       (pb-encode KeyValue-schema
         (list (cons 'key (b "foo")) (cons 'create_revision 2) (cons 'mod_revision 3)
               (cons 'version 1) (cons 'value (b "bar")) (cons 'lease 0))))

; -- PutRequest{key="foo",value="bar"} --   f1: 0A 03 foo ; f2: 12 03 bar
(check "PutRequest ref bytes"
       (u8s #x0A 3 #x66 #x6F #x6F  #x12 3 #x62 #x61 #x72)
       (pb-encode PutRequest-schema
         (list (cons 'key (b "foo")) (cons 'value (b "bar")))))

; -- RangeRequest{key="a", limit=5} --   f1: 0A 01 'a'(61) ; f3 limit: 18 05
(check "RangeRequest ref bytes"
       (u8s #x0A 1 #x61  #x18 5)
       (pb-encode RangeRequest-schema
         (list (cons 'key (b "a")) (cons 'limit 5))))

; -- RangeRequest{key="a", range_end="b", count_only=#t} --
;   f1: 0A 01 61 ; f2: 12 01 62 ; f9 count_only(bool true): 48 01
(check "RangeRequest a/b/count_only ref bytes"
       (u8s #x0A 1 #x61  #x12 1 #x62  #x48 1)
       (pb-encode RangeRequest-schema
         (list (cons 'key (b "a")) (cons 'range_end (b "b")) (cons 'count_only #t))))

; -- varint edge: KeyValue{create_revision=300} -> f2 tag 0x10, varint 300 = AC 02
(check "varint-300 in message ref bytes"
       (u8s #x10 #xAC #x02)
       (pb-encode KeyValue-schema (list (cons 'create_revision 300))))

; -- negative int64: KeyValue{create_revision=-1} -> f2 tag 0x10, ten-byte varint
;    -1 two's-complement = FF FF FF FF FF FF FF FF FF 01
(check "neg-int64 in message ref bytes"
       (u8s #x10 #xFF #xFF #xFF #xFF #xFF #xFF #xFF #xFF #xFF #x01)
       (pb-encode KeyValue-schema (list (cons 'create_revision -1))))

; -- TxnRequest{compare:[{create_revision=5}]} : embedded-message + repeated --
;   f1 compare(LEN): 0A LEN <Compare bytes>
;   Compare{create_revision=5}: f5 crev tag 0x28, varint 5 -> 28 05  (len 2)
(check "TxnRequest embedded Compare ref bytes"
       (u8s #x0A 2 #x28 5)
       (pb-encode TxnRequest-schema
         (list (cons 'compare (list (list (cons 'create_revision 5)))))))

; ===========================================================================
(section "§B WIRE-COMPAT — pb-decode of real-format reference bytes")
; ===========================================================================

; decode the canonical KeyValue reference bytes -> the right alist (with lease
; defaulted back to 0).  Compared order-insensitively against the expected msg.
(let ((decoded (pb-decode KeyValue-schema
                 (u8s #x0A 3 #x66 #x6F #x6F  #x10 2  #x18 3  #x20 1  #x2A 3 #x62 #x61 #x72))))
  (check "decode KeyValue ref -> key"     (b "foo") (cdr (assq 'key decoded)))
  (check "decode KeyValue ref -> crev"    2         (cdr (assq 'create_revision decoded)))
  (check "decode KeyValue ref -> mrev"    3         (cdr (assq 'mod_revision decoded)))
  (check "decode KeyValue ref -> version" 1         (cdr (assq 'version decoded)))
  (check "decode KeyValue ref -> value"   (b "bar") (cdr (assq 'value decoded)))
  (check "decode KeyValue ref -> lease (default 0)" 0 (cdr (assq 'lease decoded))))

; decode the PutRequest reference bytes
(let ((decoded (pb-decode PutRequest-schema (u8s #x0A 3 #x66 #x6F #x6F  #x12 3 #x62 #x61 #x72))))
  (check "decode PutRequest ref -> key"   (b "foo") (cdr (assq 'key decoded)))
  (check "decode PutRequest ref -> value" (b "bar") (cdr (assq 'value decoded)))
  (check "decode PutRequest ref -> lease (default 0)" 0 (cdr (assq 'lease decoded))))

; decode the negative-int64 reference bytes -> -1
(check "decode neg-int64 ref -> -1"
       -1 (cdr (assq 'create_revision
                     (pb-decode KeyValue-schema
                       (u8s #x10 #xFF #xFF #xFF #xFF #xFF #xFF #xFF #xFF #xFF #x01)))))

; decode the varint-300 reference bytes -> 300
(check "decode varint-300 ref -> 300"
       300 (cdr (assq 'create_revision
                      (pb-decode KeyValue-schema (u8s #x10 #xAC #x02)))))

; ===========================================================================
(section "§B forward-compat — unknown fields skipped by wire type")
; ===========================================================================
;
; Inject fields NOT in KeyValue-schema and confirm the decoder skips them and
; still recovers the known fields (proto3 forward compatibility).
;   known f1 key="hi" : 0A 02 68 69
;   unknown f7 varint : 38 7F           (7<<3|0 = 0x38)
;   unknown f8 LEN    : 42 02 AA BB      (8<<3|2 = 0x42)
;   unknown f9 fixed32: 4D DE AD BE EF   (9<<3|5 = 0x4D)
;   unknown f10 fixed64:51 .. (8 bytes)  (10<<3|1 = 0x51)
;   known f4 version=2: 20 02
(let ((decoded (pb-decode KeyValue-schema
                 (u8s #x0A 2 #x68 #x69
                      #x38 #x7F
                      #x42 2 #xAA #xBB
                      #x4D #xDE #xAD #xBE #xEF
                      #x51 1 2 3 4 5 6 7 8
                      #x20 2))))
  (check "skip-unknown -> known key"     (b "hi") (cdr (assq 'key decoded)))
  (check "skip-unknown -> known version" 2        (cdr (assq 'version decoded)))
  (check "skip-unknown -> default crev"  0        (cdr (assq 'create_revision decoded))))

(done!)
