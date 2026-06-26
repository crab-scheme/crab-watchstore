; watch-decode.scm — does a PREFIX WatchCreateRequest (range_end set) round-trip through
; pb-encode/pb-decode the way the worker's handle-client-msg reads it? If create_request or
; range_end comes back wrong/empty, the worker never reaches/branches do-create -> SHWREG=0.
(include "test/harness.scm")
(include "src/encoding.scm")
(include "src/proto.scm")

(define WG-EMPTY (make-bytevector 0 0))
(define (wg-galist key alist default) (let ((c (assq key alist))) (if c (cdr c) default)))
(define (b s) (string->utf8 s))

(section "prefix WatchCreateRequest round-trips (the cross-shard watcher's request)")
(define req (list (cons 'create_request
                        (list (cons 'key (b "w/"))
                              (cons 'range_end (b "w0"))   ; prefix range end
                              (cons 'start_revision 1)
                              (cons 'progress_notify #f)))))
(define bytes (pb-encode WatchRequest-schema req))
(check "encoded to non-empty bytes" #t (> (bytevector-length bytes) 0))

(define decoded (pb-decode WatchRequest-schema bytes))
(define create (wg-galist 'create_request decoded #f))
(check "create_request present after decode" #t (and create #t))
(define re (wg-galist 'range_end create WG-EMPTY))
(define k  (wg-galist 'key create WG-EMPTY))
(define sr (wg-galist 'start_revision create 0))
(check "key round-trips"        "w/" (utf8->string k))
(check "range_end round-trips"  "w0" (utf8->string re))
(check "range_end NON-empty (=> xshard branch taken)" #t (> (bytevector-length re) 0))
(check "start_revision round-trips" 1 sr)

(section "single-KEY WatchCreateRequest (no range_end) for comparison (single-group path)")
(define req2 (list (cons 'create_request (list (cons 'key (b "k1")) (cons 'start_revision 0)))))
(define dec2 (pb-decode WatchRequest-schema (pb-encode WatchRequest-schema req2)))
(define c2 (wg-galist 'create_request dec2 #f))
(check "single-key create_request present" #t (and c2 #t))
(check "single-key range_end empty" 0 (bytevector-length (wg-galist 'range_end c2 WG-EMPTY)))

(done!)
