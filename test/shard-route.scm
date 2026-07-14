; test/shard-route.scm — cw-0v2 (G3): prefix-aware key->shard routing for the k8s
; keyspace (src/shard-route.scm — the REAL routing fns, included by grpc-kv.scm
; and grpc-watch.scm, not a mirror).
; Run from repo root:  crabscheme run test/shard-route.scm
(include "test/harness.scm")
(include "src/shard-route.scm")

(define (b s) (string->utf8 s))
(define (shard k n) (shard-route-hash (b k) n))
(define (single? k re n) (shard-route-single? (b k) (if re (b re) #f) n))

(section "routing span: /registry/ keys hash their resource segment")
(check "pods key -> segment span"    '(10 . 14) (shard-route-span (b "/registry/pods/ns1/web-0")))
(check "leases key -> segment span"  '(10 . 16) (shard-route-span (b "/registry/leases/kube-node-lease/n1")))
(check "unterminated segment spans to end" '(10 . 14) (shard-route-span (b "/registry/pods")))
(check "non-registry key -> whole key" '(0 . 6) (shard-route-span (b "foobar")))

(section "one resource = one shard; pods/leases/events spread at N=3")
(check "pods spread"   0 (shard "/registry/pods/ns1/web-0" 3))
(check "leases spread" 1 (shard "/registry/leases/kube-node-lease/n1" 3))
(check "events spread" 2 (shard "/registry/events/ns1/web-0.x" 3))
(check "same resource, any key -> same shard"
       (shard "/registry/pods/ns1/web-0" 3) (shard "/registry/pods/other-ns/db-12" 3))
(check "prefix key routes with its members"
       (shard "/registry/pods/ns1/web-0" 3) (shard "/registry/pods/" 3))
(check "N=1 -> always group 0" 0 (shard "/registry/leases/a/b" 1))

(section "whole-key FNV preserved for non-registry keys (matches old key-shard)")
; FNV-1a("ka")=0x9d5f2c04 -> mod 3 = 2, computed against the reference FNV.
(define (fnv-whole k n)
  (let ((kb (b k)))
    (let loop ((i 0) (h 2166136261))
      (if (= i (bytevector-length kb)) (modulo h n)
          (loop (+ i 1) (modulo (* (bitwise-xor h (bytevector-u8-ref kb i)) 16777619)
                                4294967296))))))
(check "ka" (fnv-whole "ka" 3) (shard "ka" 3))
(check "some/long/key" (fnv-whole "some/long/key" 5) (shard "some/long/key" 5))

(section "shard-route-single?: the k8s access pattern never spans shards")
(check "single key (no range-end)" #t (single? "/registry/pods/ns1/web-0" #f 3))
(check "per-resource prefix range" #t (single? "/registry/pods/" "/registry/pods0" 3))
(check "namespaced prefix range"   #t (single? "/registry/pods/ns1/" "/registry/pods/ns10" 3))
(check "leases prefix range"       #t (single? "/registry/leases/" "/registry/leases0" 3))
(check "N=1 always single"         #t (single? "a" "z" 1))
(check "cross-resource range spans"      #f (single? "/registry/a" "/registry/z" 3))
(check "whole-registry range spans"      #f (single? "/registry/" "/registry0" 3))
(check "non-registry prefix range spans" #f (single? "ka" "kz" 3))
(check "\\0 to-end sentinel spans"       #f (shard-route-single? (b "/registry/pods/") (make-bytevector 1 0) 3))
(check "unterminated segment is conservative (spans)"
       #f (single? "/registry/pods" "/registry/podt" 3))

(done!)
