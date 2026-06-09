; test/smoke.scm — basic sanity checks: arithmetic + bytevector round-trip.

(include "test/harness.scm")

(section "arithmetic")
(check "add"      5  (+ 2 3))
(check "multiply" 42 (* 6 7))
(check "subtract" 0  (- 10 10))

(section "bytevector round-trip via ->bv")
(check "string->bv->string"
       "hello"
       (utf8->string (->bv "hello")))
(check "number->bv->string"
       "99"
       (utf8->string (->bv 99)))
(check "symbol->bv->string"
       "foo"
       (utf8->string (->bv 'foo)))

(done!)
