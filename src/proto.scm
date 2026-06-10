; src/proto.scm — pure-Scheme protocol-buffers (proto3) codec for crab-watchstore.
;
; This is the wire-format heart of the etcd-compatibility layer and the core
; CrabScheme stress test: a byte-for-byte proto3 encoder/decoder written entirely
; in Scheme over crabscheme's bytevector + arithmetic builtins.  No Rust/prost FFI;
; nothing in here touches the store.  gRPC framing (cw-u4a.20) and the service
; bindings (.22/.23/.27) layer ON TOP — they only add message schemas, never code.
;
; ============================================================================
; CRABSCHEME INTEGER MODEL (the load-bearing subtlety) — read this first.
; ----------------------------------------------------------------------------
; * `+ - * quotient remainder expt` and integer LITERALS are TRUE BIGNUMS:
;     (expt 2 64) => 18446744073709551616, (* MAXI64 2) => 18446744073709551614.
; * `arithmetic-shift`, `bitwise-and/ior/xor/not` operate on SIGNED i64 and
;   WRAP or ERROR outside [-(2^63), 2^63-1]:
;     (arithmetic-shift 1 64) => 0   (bitwise-and (2^64-1) 127) => ERROR.
; * `bytevector-u64-set!` requires a NON-NEGATIVE value in [0, 2^64-1].
;
; Therefore the varint core is built on ARITHMETIC (quotient/remainder/+/*),
; never on bit ops, so a uint64 up to 2^64-1 encodes/decodes without wrapping.
; Two's-complement of a negative int64/int32 is computed by bignum add (n + 2^64
; / n + 2^32), NOT by bitwise-not.  Zigzag uses (* n 2) for the unsigned shift.
; ============================================================================
;
; Depends on: src/encoding.scm (subbv).  (include "src/encoding.scm" first.)

; ---- constants ----

(define PB-TWO32 (expt 2 32))            ; 4294967296
(define PB-TWO64 (expt 2 64))            ; 18446744073709551616
(define PB-INT63 (expt 2 63))            ; sign threshold for int64 decode
(define PB-INT31 (expt 2 31))            ; sign threshold for int32 decode

; wire types
(define WT-VARINT 0)                     ; int*/uint*/sint*/bool/enum
(define WT-FIXED64 1)                    ; fixed64/sfixed64/double
(define WT-LEN 2)                        ; string/bytes/message/packed
(define WT-FIXED32 5)                    ; fixed32/sfixed32/float

; ============================================================================
; LAYER 1 — proto3 wire primitives
; ============================================================================

; ---- varint (base-128, LSB groups, MSB = continuation) ----
;
; Encodes a NON-NEGATIVE integer (0 .. 2^64-1, bignum-safe) to a bytevector.
; Each 7-bit group little-endian; high bit set on every byte but the last.

(define (uint->varint n)
  (when (< n 0) (error "uint->varint: negative" n))
  (let loop ((n n) (acc '()))
    (if (< n 128)
        ; last group: high bit clear
        (list->bytes (reverse (cons n acc)))
        ; more groups follow: low 7 bits with continuation bit (+128)
        (loop (quotient n 128)
              (cons (+ 128 (remainder n 128)) acc)))))

; int64/int32 per proto3: NEGATIVES are sign-extended to a full 64-bit two's
; complement and emitted as a 10-byte varint (int32 negatives ALSO take 10 bytes
; in real protobuf — the value is sign-extended to 64 bits before encoding).
; Positives encode as their natural unsigned value.
(define (sint-2c->varint n)
  (uint->varint (if (< n 0) (+ n PB-TWO64) n)))

; zigzag (sint32/sint64): (n << 1) XOR (n >> 63), mapping small-magnitude
; signed ints to small unsigned varints.  Done arithmetically so it never wraps:
;   n >= 0  ->  2n          n < 0  ->  -2n - 1
(define (zigzag-encode n)
  (if (>= n 0) (* n 2) (- (* (- n) 2) 1)))

(define (zigzag-decode u)
  ; inverse: even -> u/2 ; odd -> -(u+1)/2
  (if (even? u) (quotient u 2) (- (quotient (+ u 1) 2))))

; ---- fixed32 / fixed64 (little-endian) ----

(define (uint32->fixed32 n)
  (let ((b (make-bytevector 4 0)))
    (bytevector-u32-set! b 0 n (endianness little))
    b))

(define (uint64->fixed64 n)
  (let ((b (make-bytevector 8 0)))
    (bytevector-u64-set! b 0 n (endianness little))
    b))

; sfixed* accept negatives; convert via two's complement to the unsigned slot.
(define (sint32->fixed32 n) (uint32->fixed32 (if (< n 0) (+ n PB-TWO32) n)))
(define (sint64->fixed64 n) (uint64->fixed64 (if (< n 0) (+ n PB-TWO64) n)))

(define (float->fixed32 x)
  (let ((b (make-bytevector 4 0)))
    (bytevector-ieee-single-set! b 0 (exact->inexact x) (endianness little))
    b))

(define (double->fixed64 x)
  (let ((b (make-bytevector 8 0)))
    (bytevector-ieee-double-set! b 0 (exact->inexact x) (endianness little))
    b))

; ---- field tag: varint((field_number << 3) | wire_type) ----

(define (encode-tag field-number wire-type)
  ; field_number << 3 done as *8 (bignum-safe); |wire (0..5) is just +wire.
  (uint->varint (+ (* field-number 8) wire-type)))

; split a decoded tag integer back into (field-number . wire-type)
(define (split-tag tag)
  (cons (quotient tag 8) (remainder tag 8)))

; ---- length-delimited: varint(len) ‖ bytes ----

(define (encode-len-delim bv)
  (bytevector-append (uint->varint (bytevector-length bv)) bv))

; ============================================================================
; small byte helpers (a list of u8 -> bytevector; local to this module)
; ============================================================================

(define (list->bytes lst)
  (let* ((n (length lst)) (b (make-bytevector n 0)))
    (let loop ((i 0) (l lst))
      (if (null? l) b
          (begin (bytevector-u8-set! b i (car l)) (loop (+ i 1) (cdr l)))))))

; ============================================================================
; DECODER CURSOR — offset-threaded reads over a single bytevector.
;
; Each reader takes (bv offset) and RETURNS two values via `values`:
;   (the-decoded-value  next-offset)
; so callers thread the offset with call-with-values / let-values.  Unknown
; fields are skipped by wire type for proto3 forward compatibility.
; ============================================================================

; read a varint -> (value . next-offset) as two values
(define (read-varint bv offset)
  (let loop ((off offset) (shift 0) (acc 0))
    (let ((byte (bytevector-u8-ref bv off)))
      (if (< byte 128)
          (values (+ acc (* byte (expt 2 shift))) (+ off 1))
          (loop (+ off 1) (+ shift 7)
                (+ acc (* (- byte 128) (expt 2 shift))))))))

; interpret an already-read unsigned varint as a signed int64 (two's complement)
(define (varint->int64 u) (if (>= u PB-INT63) (- u PB-TWO64) u))
; ... and as a signed int32 (value was sign-extended to 64 bits on the wire, so
; first fold to int64, which is already the right signed value for int32 too).
(define (varint->int32 u) (varint->int64 u))

; read a fixed64 -> unsigned value
(define (read-fixed64 bv offset)
  (values (bytevector-u64-ref bv offset (endianness little)) (+ offset 8)))

; read a fixed32 -> unsigned value
(define (read-fixed32 bv offset)
  (values (bytevector-u32-ref bv offset (endianness little)) (+ offset 4)))

(define (read-double bv offset)
  (values (bytevector-ieee-double-ref bv offset (endianness little)) (+ offset 8)))

(define (read-float bv offset)
  (values (bytevector-ieee-single-ref bv offset (endianness little)) (+ offset 4)))

; read a length-delimited blob -> the inner bytevector
(define (read-len-delim bv offset)
  (call-with-values
    (lambda () (read-varint bv offset))
    (lambda (len after-len)
      (values (subbv bv after-len (+ after-len len)) (+ after-len len)))))

; read a field tag -> (field-number . wire-type) as ONE pair value + next-offset
(define (read-tag bv offset)
  (call-with-values
    (lambda () (read-varint bv offset))
    (lambda (tag after) (values (split-tag tag) after))))

; SKIP an unknown field's payload given its wire type; returns next-offset.
(define (skip-field bv offset wire-type)
  (cond
    ((= wire-type WT-VARINT)
     (call-with-values (lambda () (read-varint bv offset))
                       (lambda (_ after) after)))
    ((= wire-type WT-FIXED64) (+ offset 8))
    ((= wire-type WT-FIXED32) (+ offset 4))
    ((= wire-type WT-LEN)
     (call-with-values (lambda () (read-varint bv offset))
                       (lambda (len after) (+ after len))))
    (else (error "skip-field: bad wire type" wire-type))))

; ============================================================================
; LAYER 2 — schema-driven message codec
;
; A schema is a LIST of field specs:   (field-number name type label)
;   field-number : exact positive integer (etcd-canonical)
;   name         : symbol, the alist key for the Scheme message value
;   type         : one of
;                    int64 uint64 int32 uint32 bool enum
;                    sint64 sint32 fixed64 fixed32 sfixed64 sfixed32
;                    double float string bytes
;                    (message <schema>)
;   label        : 'optional (proto3 singular) or 'repeated
;
; A message VALUE is an alist ((name . value) ...).  Repeated fields hold a
; Scheme list.  bytes are bytevectors; string is a Scheme string; bool is #t/#f;
; enum is an integer.  Nested messages are themselves alists.
;
; proto3 "no default serialization": singular fields equal to their type's
; default (0 / 0.0 / #f / "" / #u8() / empty list) are OMITTED on encode and
; filled back in on decode.  oneof is modeled as plain optional fields whose
; presence the caller manages (encode emits whichever are non-default).
; ============================================================================

; ---- field-spec accessors ----
(define (fs-num spec)   (car spec))
(define (fs-name spec)  (cadr spec))
(define (fs-type spec)  (caddr spec))
(define (fs-label spec) (cadddr spec))

(define (fs-repeated? spec) (eq? (fs-label spec) 'repeated))
(define (fs-message? spec)  (and (pair? (fs-type spec)) (eq? (car (fs-type spec)) 'message)))

; Resolve a singular/repeated message field's sub-schema LAZILY.  The inner of
; (message X) is either a literal schema list or a *-ref symbol; symbols are
; dereferenced through schema-ref-table only at the recursion point, so the
; mutually-recursive etcd schemas (RequestOp <-> TxnRequest) never need an
; eager fixpoint and cannot diverge.
(define (fs-submessage-schema spec)
  (let ((inner (cadr (fs-type spec))))
    (if (symbol? inner) (deref-schema inner) inner)))

; scalar types that pack when repeated (varint/fixed wire forms)
(define (scalar-packable? type)
  (memq type '(int64 uint64 int32 uint32 bool enum sint64 sint32
               fixed64 fixed32 sfixed64 sfixed32 double float)))

; ---- proto3 default test (for singular omission) ----
(define (proto3-default? type v)
  (cond
    ((eq? type 'bool)   (eq? v #f))
    ((eq? type 'string) (and (string? v) (= (string-length v) 0)))
    ((eq? type 'bytes)  (and (bytevector? v) (= (bytevector-length v) 0)))
    ((and (pair? type) (eq? (car type) 'message)) (eq? v #f)) ; absent submessage
    ((memq type '(double float)) (and (number? v) (= v 0)))
    (else (and (number? v) (= v 0)))))                         ; numeric/enum 0

; ============================================================================
; ENCODE
; ============================================================================

; encode ONE scalar value to its raw payload bytevector (no tag).
(define (encode-scalar-payload type v)
  (cond
    ((memq type '(uint64 uint32)) (uint->varint v))
    ((memq type '(int64 int32))   (sint-2c->varint v))
    ((eq? type 'enum)             (sint-2c->varint v))  ; enums are int32 on wire
    ((eq? type 'bool)             (uint->varint (if v 1 0)))
    ((eq? type 'sint64)           (uint->varint (zigzag-encode v)))
    ((eq? type 'sint32)           (uint->varint (zigzag-encode v)))
    ((eq? type 'fixed64)          (uint64->fixed64 v))
    ((eq? type 'fixed32)          (uint32->fixed32 v))
    ((eq? type 'sfixed64)         (sint64->fixed64 v))
    ((eq? type 'sfixed32)         (sint32->fixed32 v))
    ((eq? type 'double)           (double->fixed64 v))
    ((eq? type 'float)            (float->fixed32 v))
    ((eq? type 'string)           (string->utf8 v))
    ((eq? type 'bytes)            v)
    (else (error "encode-scalar-payload: bad type" type))))

; wire type for a scalar
(define (scalar-wire-type type)
  (cond
    ((memq type '(fixed64 sfixed64 double)) WT-FIXED64)
    ((memq type '(fixed32 sfixed32 float))  WT-FIXED32)
    ((memq type '(string bytes))            WT-LEN)
    (else WT-VARINT)))                       ; all varint scalars + enum + bool

; emit a single TLV (tag ‖ payload) for one scalar.  string/bytes are
; length-delimited; everything else is the bare payload after the tag.
(define (encode-scalar-field num type v)
  (let ((wt (scalar-wire-type type))
        (payload (encode-scalar-payload type v)))
    (if (= wt WT-LEN)
        (bytevector-append (encode-tag num wt) (encode-len-delim payload))
        (bytevector-append (encode-tag num wt) payload))))

; emit a packed repeated scalar field: tag(LEN) ‖ len ‖ concat(payloads).
; (proto3 packs repeated scalars by default; an empty list emits nothing.)
(define (encode-packed-field num type vals)
  (if (null? vals)
      (make-bytevector 0 0)
      (let ((body (apply bytevector-append
                         (map (lambda (v) (encode-scalar-payload type v)) vals))))
        (bytevector-append (encode-tag num WT-LEN) (encode-len-delim body)))))

; emit a repeated message field: one LEN-delimited record per element.
(define (encode-repeated-message num schema vals)
  (apply bytevector-append
         (map (lambda (m)
                (bytevector-append (encode-tag num WT-LEN)
                                   (encode-len-delim (pb-encode schema m))))
              vals)))

; encode one field spec given the message alist; returns a (possibly empty) bv.
(define (encode-field spec msg)
  (let* ((num   (fs-num spec))
         (name  (fs-name spec))
         (type  (fs-type spec))
         (cell  (assq name msg))
         (v     (and cell (cdr cell))))
    (cond
      ; absent key -> nothing
      ((not cell) (make-bytevector 0 0))
      ; repeated
      ((fs-repeated? spec)
       (cond
         ((or (not v) (null? v)) (make-bytevector 0 0))
         ((fs-message? spec) (encode-repeated-message num (fs-submessage-schema spec) v))
         ((scalar-packable? type) (encode-packed-field num type v))
         ; repeated string/bytes: NOT packable -> one TLV each
         (else (apply bytevector-append
                      (map (lambda (e) (encode-scalar-field num type e)) v)))))
      ; singular message
      ((fs-message? spec)
       (if (or (not v) (proto3-default? type v))
           (make-bytevector 0 0)
           (bytevector-append (encode-tag num WT-LEN)
                              (encode-len-delim (pb-encode (fs-submessage-schema spec) v)))))
      ; singular scalar — OMIT proto3 defaults
      (else
       (if (proto3-default? type v)
           (make-bytevector 0 0)
           (encode-scalar-field num type v))))))

; pb-encode: schema + alist -> bytevector.  Fields emitted in ASCENDING
; field-number order (etcd/protoc canonical-ish; required for our byte refs).
(define (pb-encode schema msg)
  (let ((ordered (list-sort (lambda (a b) (< (fs-num a) (fs-num b))) schema)))
    (apply bytevector-append
           (map (lambda (spec) (encode-field spec msg)) ordered))))

; ============================================================================
; DECODE
; ============================================================================

; decode a scalar payload that has ALREADY been read off the wire.  For varint
; types `raw` is the unsigned varint integer; for fixed it is the unsigned slot;
; for LEN it is the inner bytevector.
(define (decode-scalar type raw)
  (cond
    ((memq type '(uint64 uint32)) raw)
    ((memq type '(int64))         (varint->int64 raw))
    ((memq type '(int32 enum))    (varint->int32 raw))
    ((eq? type 'bool)             (not (= raw 0)))
    ((eq? type 'sint64)           (zigzag-decode raw))
    ((eq? type 'sint32)           (zigzag-decode raw))
    ((eq? type 'fixed64)          raw)
    ((eq? type 'fixed32)          raw)
    ((eq? type 'sfixed64)         (if (>= raw PB-INT63) (- raw PB-TWO64) raw))
    ((eq? type 'sfixed32)         (if (>= raw PB-INT31) (- raw PB-TWO32) raw))
    ((eq? type 'string)           (utf8->string raw))
    ((eq? type 'bytes)            raw)
    (else (error "decode-scalar: bad type" type))))

; find the field spec whose number = num (or #f for an unknown field).
(define (schema-lookup schema num)
  (let loop ((s schema))
    (cond ((null? s) #f)
          ((= (fs-num (car s)) num) (car s))
          (else (loop (cdr s))))))

; read a single scalar value of `type` at offset -> (value . next-offset)
(define (read-scalar-value type bv offset)
  (let ((wt (scalar-wire-type type)))
    (cond
      ((= wt WT-FIXED64)
       (if (memq type '(double))
           (read-double bv offset)
           (call-with-values (lambda () (read-fixed64 bv offset))
                             (lambda (raw o) (values (decode-scalar type raw) o)))))
      ((= wt WT-FIXED32)
       (if (memq type '(float))
           (read-float bv offset)
           (call-with-values (lambda () (read-fixed32 bv offset))
                             (lambda (raw o) (values (decode-scalar type raw) o)))))
      ((= wt WT-LEN)
       (call-with-values (lambda () (read-len-delim bv offset))
                         (lambda (raw o) (values (decode-scalar type raw) o))))
      (else  ; varint
       (call-with-values (lambda () (read-varint bv offset))
                         (lambda (raw o) (values (decode-scalar type raw) o)))))))

; decode a PACKED scalar field body (an inner bytevector) into a list of values.
(define (decode-packed type body)
  (let ((n (bytevector-length body)))
    (let loop ((off 0) (acc '()))
      (if (>= off n)
          (reverse acc)
          (call-with-values
            (lambda () (read-scalar-value type body off))
            (lambda (v next) (loop next (cons v acc))))))))

; accumulate one decoded field into the in-progress alist `acc`.
; For repeated fields we PREPEND and reverse at the very end (in pb-decode).
(define (accumulate acc name value repeated?)
  (if repeated?
      (let ((cell (assq name acc)))
        (if cell
            ; prepend to the existing (reversed) list
            (cons (cons name (cons value (cdr cell)))
                  (filter (lambda (p) (not (eq? (car p) name))) acc))
            (cons (cons name (list value)) acc)))
      (cons (cons name value)
            (filter (lambda (p) (not (eq? (car p) name))) acc))))

; pb-decode: schema + bytevector -> alist, with proto3 defaults filled for
; absent fields, repeated accumulated (in encounter order), nested recursed,
; unknown tags skipped.
(define (pb-decode schema bv)
  (let ((n (bytevector-length bv)))
    (let loop ((off 0) (acc '()))
      (if (>= off n)
          (finalize-decode schema (reverse-repeated schema acc))
          (call-with-values
            (lambda () (read-tag bv off))
            (lambda (tag after-tag)
              (let* ((num (car tag)) (wt (cdr tag))
                     (spec (schema-lookup schema num)))
                (if (not spec)
                    ; unknown field: skip by wire type (forward compat)
                    (loop (skip-field bv after-tag wt) acc)
                    (let ((name (fs-name spec)) (type (fs-type spec)))
                      (cond
                        ; repeated message
                        ((and (fs-repeated? spec) (fs-message? spec))
                         (call-with-values
                           (lambda () (read-len-delim bv after-tag))
                           (lambda (inner next)
                             (loop next
                                   (accumulate acc name
                                               (pb-decode (fs-submessage-schema spec) inner)
                                               #t)))))
                        ; repeated scalar — may arrive PACKED (LEN) or as
                        ; individual TLVs (older/non-packed encoders).
                        ((fs-repeated? spec)
                         (if (= wt WT-LEN)
                             (call-with-values
                               (lambda () (read-len-delim bv after-tag))
                               (lambda (inner next)
                                 (loop next
                                       (fold-left (lambda (a v) (accumulate a name v #t))
                                                  acc (decode-packed type inner)))))
                             (call-with-values
                               (lambda () (read-scalar-value type bv after-tag))
                               (lambda (v next) (loop next (accumulate acc name v #t))))))
                        ; singular message
                        ((fs-message? spec)
                         (call-with-values
                           (lambda () (read-len-delim bv after-tag))
                           (lambda (inner next)
                             (loop next
                                   (accumulate acc name
                                               (pb-decode (fs-submessage-schema spec) inner)
                                               #f)))))
                        ; singular scalar
                        (else
                         (call-with-values
                           (lambda () (read-scalar-value type bv after-tag))
                           (lambda (v next)
                             (loop next (accumulate acc name v #f)))))))))))))))

; reverse every repeated field's accumulated list back into encounter order.
(define (reverse-repeated schema acc)
  (map (lambda (pair)
         (let ((spec (schema-lookup schema
                       (let ((s (find-spec-by-name schema (car pair)))) (and s (fs-num s))))))
           (if (and spec (fs-repeated? spec))
               (cons (car pair) (reverse (cdr pair)))
               pair)))
       acc))

(define (find-spec-by-name schema name)
  (let loop ((s schema))
    (cond ((null? s) #f)
          ((eq? (fs-name (car s)) name) (car s))
          (else (loop (cdr s))))))

; fill proto3 default values for every schema field absent from the alist, so
; pb-decode is a total inverse of pb-encode for default round-trips.
(define (finalize-decode schema acc)
  (let loop ((specs schema) (out acc))
    (if (null? specs)
        out
        (let* ((spec (car specs))
               (name (fs-name spec))
               (type (fs-type spec)))
          (if (assq name out)
              (loop (cdr specs) out)
              (loop (cdr specs)
                    (append out (list (cons name (field-default spec))))))))))

; the Scheme default value for a schema field.
(define (field-default spec)
  (let ((type (fs-type spec)))
    (cond
      ((fs-repeated? spec) '())
      ((fs-message? spec) #f)        ; absent submessage
      ((eq? type 'bool) #f)
      ((eq? type 'string) "")
      ((eq? type 'bytes) (make-bytevector 0 0))
      ((memq type '(double float)) 0)
      (else 0))))                     ; numeric/enum default 0

; ============================================================================
; etcd v3 KV-service message schemas (canonical field numbers).
; Verified against etcd's mvccpb.proto / kv.proto / rpc.proto field numbers.
; These are DATA: adding Watch/Lease/Auth (.23/.27) is a pure-data extension.
; ============================================================================

; mvccpb.KeyValue
(define KeyValue-schema
  '((1 key            bytes  optional)
    (2 create_revision int64 optional)
    (3 mod_revision    int64 optional)
    (4 version         int64 optional)
    (5 value           bytes optional)
    (6 lease           int64 optional)))

; etcdserverpb.ResponseHeader
(define ResponseHeader-schema
  '((1 cluster_id uint64 optional)
    (2 member_id  uint64 optional)
    (3 revision   int64  optional)
    (4 raft_term  uint64 optional)))

; etcdserverpb.RangeRequest
(define RangeRequest-schema
  '((1  key                bytes  optional)
    (2  range_end          bytes  optional)
    (3  limit              int64  optional)
    (4  revision           int64  optional)
    (5  sort_order         enum   optional)
    (6  sort_target        enum   optional)
    (7  serializable       bool   optional)
    (8  keys_only          bool   optional)
    (9  count_only         bool   optional)
    (10 min_mod_revision   int64  optional)
    (11 max_mod_revision   int64  optional)
    (12 min_create_revision int64 optional)
    (13 max_create_revision int64 optional)))

; etcdserverpb.RangeResponse
(define RangeResponse-schema
  (list
    (list 1 'header '(message ResponseHeader-schema-ref) 'optional)
    (list 2 'kvs    '(message KeyValue-schema-ref)       'repeated)
    (list 3 'more   'bool  'optional)
    (list 4 'count  'int64 'optional)))

; etcdserverpb.PutRequest
(define PutRequest-schema
  '((1 key          bytes optional)
    (2 value        bytes optional)
    (3 lease        int64 optional)
    (4 prev_kv      bool  optional)
    (5 ignore_value bool  optional)
    (6 ignore_lease bool  optional)))

; etcdserverpb.PutResponse
(define PutResponse-schema
  (list
    (list 1 'header  '(message ResponseHeader-schema-ref) 'optional)
    (list 2 'prev_kv '(message KeyValue-schema-ref)       'optional)))

; etcdserverpb.DeleteRangeRequest
(define DeleteRangeRequest-schema
  '((1 key       bytes optional)
    (2 range_end bytes optional)
    (3 prev_kv   bool  optional)))

; etcdserverpb.DeleteRangeResponse
(define DeleteRangeResponse-schema
  (list
    (list 1 'header   '(message ResponseHeader-schema-ref) 'optional)
    (list 2 'deleted  'int64 'optional)
    (list 3 'prev_kvs '(message KeyValue-schema-ref)       'repeated)))

; etcdserverpb.Compare — oneof target_union(version=4,create_revision=5,
; mod_revision=6,value=7,lease=8) modeled as plain optional fields; at most one
; carries a non-default value.  result=1(enum), target=2(enum), key=3, range_end=64.
(define Compare-schema
  '((1  result          enum  optional)
    (2  target          enum  optional)
    (3  key             bytes optional)
    (4  version         int64 optional)   ; oneof target_union
    (5  create_revision int64 optional)   ; oneof target_union
    (6  mod_revision    int64 optional)   ; oneof target_union
    (7  value           bytes optional)   ; oneof target_union
    (8  lease           int64 optional)   ; oneof target_union
    (64 range_end       bytes optional)))

; etcdserverpb.RequestOp — oneof request(request_range=1,request_put=2,
; request_delete_range=3,request_txn=4).  Each branch is an embedded message.
(define RequestOp-schema
  (list
    (list 1 'request_range        '(message RangeRequest-schema-ref)       'optional)
    (list 2 'request_put          '(message PutRequest-schema-ref)         'optional)
    (list 3 'request_delete_range '(message DeleteRangeRequest-schema-ref) 'optional)
    (list 4 'request_txn          '(message TxnRequest-schema-ref)         'optional)))

; etcdserverpb.ResponseOp — oneof response(response_range=1,response_put=2,
; response_delete_range=3,response_txn=4).
(define ResponseOp-schema
  (list
    (list 1 'response_range        '(message RangeResponse-schema-ref)       'optional)
    (list 2 'response_put          '(message PutResponse-schema-ref)         'optional)
    (list 3 'response_delete_range '(message DeleteRangeResponse-schema-ref) 'optional)
    (list 4 'response_txn          '(message TxnResponse-schema-ref)         'optional)))

; etcdserverpb.TxnRequest
(define TxnRequest-schema
  (list
    (list 1 'compare '(message Compare-schema-ref)    'repeated)
    (list 2 'success '(message RequestOp-schema-ref)  'repeated)
    (list 3 'failure '(message RequestOp-schema-ref)  'repeated)))

; etcdserverpb.TxnResponse
(define TxnResponse-schema
  (list
    (list 1 'header    '(message ResponseHeader-schema-ref) 'optional)
    (list 2 'succeeded 'bool  'optional)
    (list 3 'responses '(message ResponseOp-schema-ref)     'repeated)))

; etcdserverpb.CompactionRequest
(define CompactionRequest-schema
  '((1 revision int64 optional)
    (2 physical bool  optional)))

; etcdserverpb.CompactionResponse
(define CompactionResponse-schema
  (list
    (list 1 'header '(message ResponseHeader-schema-ref) 'optional)))

; ----------------------------------------------------------------------------
; Schema reference resolution (LAZY).
;
; Schemas reference one another by a *-ref symbol so they may be defined in any
; order and form cycles (RequestOp <-> TxnRequest).  deref-schema maps a ref
; symbol to its live schema list at the moment a nested field is encoded/decoded
; — no eager fixpoint, so cycles can never diverge.  Each entry is a THUNK so
; the binding is read at call time (after every define has run).
; ============================================================================

(define schema-ref-table
  (list
    (cons 'KeyValue-schema-ref            (lambda () KeyValue-schema))
    (cons 'ResponseHeader-schema-ref      (lambda () ResponseHeader-schema))
    (cons 'RangeRequest-schema-ref        (lambda () RangeRequest-schema))
    (cons 'RangeResponse-schema-ref       (lambda () RangeResponse-schema))
    (cons 'PutRequest-schema-ref          (lambda () PutRequest-schema))
    (cons 'PutResponse-schema-ref         (lambda () PutResponse-schema))
    (cons 'DeleteRangeRequest-schema-ref  (lambda () DeleteRangeRequest-schema))
    (cons 'DeleteRangeResponse-schema-ref (lambda () DeleteRangeResponse-schema))
    (cons 'Compare-schema-ref             (lambda () Compare-schema))
    (cons 'RequestOp-schema-ref           (lambda () RequestOp-schema))
    (cons 'ResponseOp-schema-ref          (lambda () ResponseOp-schema))
    (cons 'TxnRequest-schema-ref          (lambda () TxnRequest-schema))
    (cons 'TxnResponse-schema-ref         (lambda () TxnResponse-schema))
    (cons 'CompactionRequest-schema-ref   (lambda () CompactionRequest-schema))
    (cons 'CompactionResponse-schema-ref  (lambda () CompactionResponse-schema))))

(define (deref-schema ref-sym)
  (let ((cell (assq ref-sym schema-ref-table)))
    (if cell ((cdr cell)) (error "deref-schema: unknown schema ref" ref-sym))))
