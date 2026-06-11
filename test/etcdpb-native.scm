; test/etcdpb-native.scm — DIFFERENTIAL proof for the native etcdserverpb codec
; (cw-b5w.2): the crabscheme builtins etcd-pb-decode-put / etcd-pb-decode-range /
; etcd-pb-encode-put-resp / etcd-pb-encode-range-resp must agree with the
; pure-Scheme reference codec (src/proto.scm) on the SAME messages — decode
; field-for-field, encode byte-for-byte.  Requires a crabscheme built with the
; `grpc` feature (the same build the server itself requires).

(include "test/harness.scm")
(include "src/encoding.scm")
(include "src/proto.scm")

(define (b s) (string->utf8 s))
(define (galist k al d) (let ((hit (assq k al))) (if hit (cdr hit) d)))

; ---------------------------------------------------------------------------
; §A decode: native PutRequest/RangeRequest decode == pb-decode (reference)
; ---------------------------------------------------------------------------

(define put-msgs
  (list
    (pb-encode PutRequest-schema (list (cons 'key (b "k")) (cons 'value (b "v"))))
    (pb-encode PutRequest-schema (list (cons 'key (b "k2")) (cons 'value (b ""))
                                       (cons 'lease 123456789) (cons 'prev_kv #t)))
    (pb-encode PutRequest-schema (list (cons 'key (b "k3")) (cons 'value (make-bytevector 4096 120))
                                       (cons 'ignore_value #t) (cons 'ignore_lease #t)))
    (pb-encode PutRequest-schema '())))

(for-each
 (lambda (m)
   (let ((ref (pb-decode PutRequest-schema m))
         (nat (etcd-pb-decode-put m)))
     (check "put: key"          (galist 'key ref (b ""))      (list-ref nat 0))
     (check "put: value"        (galist 'value ref (b ""))    (list-ref nat 1))
     (check "put: lease"        (galist 'lease ref 0)         (list-ref nat 2))
     (check "put: prev_kv"      (galist 'prev_kv ref #f)      (list-ref nat 3))
     (check "put: ignore_value" (galist 'ignore_value ref #f) (list-ref nat 4))
     (check "put: ignore_lease" (galist 'ignore_lease ref #f) (list-ref nat 5))))
 put-msgs)

(define range-msgs
  (list
    (pb-encode RangeRequest-schema (list (cons 'key (b "a"))))
    (pb-encode RangeRequest-schema (list (cons 'key (b "a")) (cons 'range_end (b "z"))
                                         (cons 'limit 10) (cons 'revision 42)
                                         (cons 'sort_order 2) (cons 'sort_target 3)
                                         (cons 'serializable #t) (cons 'keys_only #t)
                                         (cons 'count_only #t)
                                         (cons 'min_mod_revision 1) (cons 'max_mod_revision 99)
                                         (cons 'min_create_revision 2) (cons 'max_create_revision 88)))
    (pb-encode RangeRequest-schema '())))

(for-each
 (lambda (m)
   (let ((ref (pb-decode RangeRequest-schema m))
         (nat (etcd-pb-decode-range m)))
     (check "range: key"        (galist 'key ref (b ""))       (list-ref nat 0))
     (check "range: range_end"  (galist 'range_end ref (b "")) (list-ref nat 1))
     (check "range: limit"      (galist 'limit ref 0)          (list-ref nat 2))
     (check "range: revision"   (galist 'revision ref 0)       (list-ref nat 3))
     (check "range: sort_order" (galist 'sort_order ref 0)     (list-ref nat 4))
     (check "range: sort_target" (galist 'sort_target ref 0)   (list-ref nat 5))
     (check "range: serializable" (galist 'serializable ref #f) (list-ref nat 6))
     (check "range: keys_only"  (galist 'keys_only ref #f)     (list-ref nat 7))
     (check "range: count_only" (galist 'count_only ref #f)    (list-ref nat 8))
     (check "range: min_mod"    (galist 'min_mod_revision ref 0)    (list-ref nat 9))
     (check "range: max_mod"    (galist 'max_mod_revision ref 0)    (list-ref nat 10))
     (check "range: min_create" (galist 'min_create_revision ref 0) (list-ref nat 11))
     (check "range: max_create" (galist 'max_create_revision ref 0) (list-ref nat 12))))
 range-msgs)

; ---------------------------------------------------------------------------
; §B encode: native response encode == pb-encode (reference), byte-for-byte
; ---------------------------------------------------------------------------

(define (mk-header cid mid rev term)
  (list (cons 'cluster_id cid) (cons 'member_id mid)
        (cons 'revision rev) (cons 'raft_term term)))

(define (tuple->kv t)
  (list (cons 'key (list-ref t 0)) (cons 'value (list-ref t 1))
        (cons 'create_revision (list-ref t 2)) (cons 'mod_revision (list-ref t 3))
        (cons 'version (list-ref t 4)) (cons 'lease (list-ref t 5))))

; PutResponse without prev_kv
(check "encode put-resp (no prev)"
       (pb-encode PutResponse-schema (list (cons 'header (mk-header 7 8 9 2))))
       (etcd-pb-encode-put-resp 7 8 9 2 #f))

; PutResponse with prev_kv (lease 0 + a zero-default field exercises omission)
(define prev-t (list (b "k") (b "old") 2 5 3 0))
(check "encode put-resp (prev)"
       (pb-encode PutResponse-schema (list (cons 'header (mk-header 7 8 9 2))
                                           (cons 'prev_kv (tuple->kv prev-t))))
       (etcd-pb-encode-put-resp 7 8 9 2 prev-t))

; RangeResponse: empty kvs, more=#f, count 0 (all defaults except header)
(check "encode range-resp (empty)"
       (pb-encode RangeResponse-schema (list (cons 'header (mk-header 1 2 3 4))
                                             (cons 'kvs '()) (cons 'more #f) (cons 'count 0)))
       (etcd-pb-encode-range-resp 1 2 3 4 '() #f 0))

; RangeResponse: 3 kvs incl. a 4KB value + an empty value (keys-only shape), more=#t
(define tuples
  (list (list (b "k1") (b "v1") 1 1 1 0)
        (list (b "k2") (make-bytevector 4096 121) 2 9 4 77)
        (list (b "k3") (b "") 3 3 1 0)))
(check "encode range-resp (kvs+more)"
       (pb-encode RangeResponse-schema (list (cons 'header (mk-header 495437907 876335078 6 2))
                                             (cons 'kvs (map tuple->kv tuples))
                                             (cons 'more #t) (cons 'count 12)))
       (etcd-pb-encode-range-resp 495437907 876335078 6 2 tuples #t 12))

; cross-check: reference DECODE of a native range encode recovers the kvs
(let* ((bytes (etcd-pb-encode-range-resp 1 2 3 4 tuples #f 3))
       (dec   (pb-decode RangeResponse-schema bytes))
       (kvs   (galist 'kvs dec '())))
  (check "native range-resp decodes (count)" 3 (galist 'count dec 0))
  (check "native range-resp decodes (n kvs)" 3 (length kvs))
  (check "native range-resp decodes (k2 value len)" 4096
         (bytevector-length (galist 'value (cadr kvs) (b "")))))

(done!)
