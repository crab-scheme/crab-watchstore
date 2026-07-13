; test/native-range-pb.scm — cw-2au native fused range differential test.
;
; store-range-latest-pb (cs-store) + etcd-pb-encode-range-resp-pb must produce
; BYTE-IDENTICAL RangeResponses to the interpreted mvcc-range -> tuple ->
; etcd-pb-encode-range-resp pipeline, across: prefix ranges, keys with embedded
; 0x00 bytes, multi-version keys, tombstones, limit/more/count, keys-only,
; count-only, pinned revisions, and range_end="\0".
;
;   crabscheme run test/native-range-pb.scm   (needs a binary with the builtin)
(include "test/harness.scm")
(include "src/encoding.scm")
(include "src/store-ctx.scm")
(include "src/mvcc.scm")

(define run-tag (number->string (exact (round (* 1000000 (current-second))))))
(define handle (store-open (string-append "/tmp/cws-nrpb-" run-tag) #t))
(define ctx (make-ctx handle "default" #f))

(define (b s) (string->utf8 s))
(define (put! k v) (mvcc-apply ctx (list (b "PUT") k v (b "0"))))
(define (del! k) (mvcc-apply ctx (list (b "DEL") k (make-bytevector 0 0))))

; ---- fixture: k8s-ish keys + edge cases ----
(put! (b "/registry/pods/ns1/a") (b "va1"))
(put! (b "/registry/pods/ns1/a") (b "va2"))          ; second version
(define REV-A2 (mvcc-current-rev ctx))
(put! (b "/registry/pods/ns1/b") (b "vb"))
(put! (b "/registry/pods/ns1/c") (b "vc"))
(put! (b "/registry/pods/ns2/d") (b "vd"))
(put! (b "/registry/svc/e") (b "ve"))
(del! (b "/registry/pods/ns1/b"))                     ; tombstone
(put! (b "/registry/pods/ns1/a") (b "va3"))          ; third version
; key with embedded NULs
(put! (bytevector-append (b "/registry/pods/nul/") (make-bytevector 2 0) (b "x"))
      (b "vnul"))
(put! (b "/registry/pods/ns1/empty") (make-bytevector 0 0))  ; empty value

(define CUR (mvcc-current-rev ctx))

(define (opts-base key rend extra)
  (append (list (cons 'key key) (cons 'range-end rend)) extra))

; interpreted pipeline -> full RangeResponse bytes
(define (resp-interp key rend extra)
  (let* ((opts (opts-base key rend extra))
         (res (mvcc-range ctx key rend opts)))
    (if (and (pair? res) (eq? (car res) 'err-compacted))
        'err-compacted
        (let* ((total (car res))
               (tuples (map (lambda (item)
                              (let ((uk (car item)) (rec (cdr item)))
                                (list uk (kv-rec-value rec)
                                      (kv-rec-create-rev rec) (kv-rec-mod-rev rec)
                                      (kv-rec-version rec) (kv-rec-lease rec))))
                            (cdr res)))
               (lim (let ((l (assq 'limit opts))) (if l (cdr l) 0)))
               (more (and (> lim 0) (> total (length tuples)))))
          (etcd-pb-encode-range-resp 7 9 CUR 3 tuples more total)))))

; native pipeline -> full RangeResponse bytes
(define (resp-native key rend extra)
  (let ((res (mvcc-range-pb ctx key rend (opts-base key rend extra))))
    (if (and (pair? res) (eq? (car res) 'err-compacted))
        'err-compacted
        (etcd-pb-encode-range-resp-pb 7 9 CUR 3 (caddr res) (cadr res) (car res)))))

(define (differential name key rend extra)
  (let ((i (resp-interp key rend extra))
        (n (resp-native key rend extra)))
    (check name #t (equal? i n))))

(section "differential: native vs interpreted RangeResponse bytes")
(differential "prefix LIST /registry/pods/ns1/" (b "/registry/pods/ns1/") (b "/registry/pods/ns10") '())
(differential "prefix LIST whole /registry/" (b "/registry/") (b "/registry0") '())
(differential "limit 2 (more + count)" (b "/registry/") (b "/registry0") '((limit . 2)))
(differential "limit 1000 (no more)" (b "/registry/") (b "/registry0") '((limit . 1000)))
(differential "keys-only" (b "/registry/") (b "/registry0") '((keys-only . #t)))
(differential "count-only" (b "/registry/") (b "/registry0") '((count-only . #t)))
(differential "pinned rev (pre-delete, pre-va3)" (b "/registry/") (b "/registry0")
              (list (cons 'revision REV-A2)))
(differential "range_end = nul (to end of keyspace)" (b "/registry/pods/") (make-bytevector 1 0) '())
(differential "empty range" (b "/zzz/") (b "/zzz0") '())
(differential "nul-key subtree" (b "/registry/pods/nul/") (b "/registry/pods/nul0") '())

(section "compaction floor")
(mvcc-apply ctx (list (b "COMPACT") (b (number->string REV-A2))))
(check "below-floor pinned rev errs both paths" #t
       (and (eq? 'err-compacted (resp-interp (b "/registry/") (b "/registry0")
                                             (list (cons 'revision 1))))
            (eq? 'err-compacted (resp-native (b "/registry/") (b "/registry0")
                                             (list (cons 'revision 1))))))
(differential "post-compaction latest LIST" (b "/registry/") (b "/registry0") '())

(done!)
