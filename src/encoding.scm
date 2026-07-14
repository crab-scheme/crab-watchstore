; src/encoding.scm — generic byte utilities for crab-watchstore.
;
; Pure byte functions only: NO store access lives here.  Everything is a
; bytevector in/out so keys and values are binary-safe.
;
; These are the order-preserving / fixed-width integer encoders that underpin
; all on-disk key schemas.  MVCC key encoding (cw-u4a.5/.6) will build on top
; of these helpers.  Redis-type-specific key schemas (S:/H:/L:/E:/Z:/Zs:/T:
; prefixes, TTL/deadline encoding, per-type composite keys) are intentionally
; NOT included here.

; ---- bytevector slice ----

; cw-xq9: native bytevector-copy! instead of a byte-at-a-time interpreted loop.
; kv-record-decode subbv's the FULL VALUE of every row it touches — a 1000-pod
; k8s LIST copied ~3.5MB one interpreted byte at a time (~3ms/key, ~3s/LIST,
; all of it on the single shard thread).
(define (subbv b start end)
  (let ((out (make-bytevector (- end start) 0)))
    (bytevector-copy! out 0 b start end)   ; R7RS arg order: (dest at src start end)
    out))

; ---- u64 big-endian ----

(define (u64->bytes n)
  (let ((b (make-bytevector 8 0)))
    (bytevector-u64-set! b 0 n (endianness big))
    b))

(define (bytes->u64 b off)
  (bytevector-u64-ref b off (endianness big)))

; ---- s64 order-preserving big-endian ----
;
; Two's-complement big-endian sorts wrong (negatives have the high bit set, so
; they sort AFTER positives under unsigned byte compare).  Flipping just the
; sign bit (XOR byte 0 with 0x80) makes unsigned byte order == signed numeric
; order — no >i64 arithmetic.

(define (flip-top! b)
  (bytevector-u8-set! b 0 (bitwise-xor (bytevector-u8-ref b 0) #x80)))

(define (s64->order-bytes n)
  (let ((b (make-bytevector 8 0)))
    (bytevector-s64-set! b 0 n (endianness big))
    (flip-top! b)
    b))

(define (order-bytes->s64 b off)
  (let ((c (subbv b off (+ off 8))))
    (flip-top! c)
    (bytevector-s64-ref c 0 (endianness big))))

; ---- f64 order-preserving big-endian ----
;
; IEEE double -> 8 order-preserving bytes so RocksDB byte order == numeric
; order.  Standard total-order transform done at the byte level (no >i64 ints):
; positive -> set sign bit; negative -> invert all 8 bytes.

(define (invert-all! b)
  (let loop ((i 0))
    (if (< i (bytevector-length b))
        (begin (bytevector-u8-set! b i (bitwise-xor (bytevector-u8-ref b i) #xFF))
               (loop (+ i 1))))))

(define (f64->order-bytes x)
  (let ((b (make-bytevector 8 0)))
    (bytevector-ieee-double-set! b 0 (exact->inexact x) (endianness big))
    (if (>= (bytevector-u8-ref b 0) 128)   ; negative
        (invert-all! b)
        (bytevector-u8-set! b 0 (bitwise-ior (bytevector-u8-ref b 0) #x80)))
    b))

(define (order-bytes->f64 b off)
  (let ((c (subbv b off (+ off 8))))
    (if (>= (bytevector-u8-ref c 0) 128)   ; encoded high bit set => was positive
        (bytevector-u8-set! c 0 (bitwise-xor (bytevector-u8-ref c 0) #x80))
        (invert-all! c))                   ; else => was negative (invert back)
    (bytevector-ieee-double-ref c 0 (endianness big))))

; ---- integer string <-> bytevector ----
;
; Decimal ASCII encoding used by Raft-applied-index and counter helpers.
; bytes->int returns #f if the bytes are not a valid base-10 integer.

(define (bytes->int b)
  (let ((n (string->number (utf8->string b) 10)))
    (if (and n (integer? n) (exact? n)) n #f)))

(define (int->bytes n)
  (string->utf8 (number->string n)))
