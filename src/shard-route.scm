; src/shard-route.scm — cw-0v2 (G3): prefix-aware key->shard routing for the k8s keyspace.
;
; FNV-1a over the WHOLE key sprays one resource's keys across every group, so the
; k8s access pattern (per-prefix watch, prefix LIST, single-key CAS against a rev
; from that LIST) always spans shards and rides the cross-shard merge machinery —
; whose per-shard revisions are NOT comparable without --global-rev. Route by the
; RESOURCE SEGMENT instead: a key under "/registry/" hashes only its first path
; segment ("pods", "leases", "events", ...), so a resource's whole keyspace lives
; on ONE group (watch / LIST / CAS per resource are single-shard and rev-coherent)
; and distinct resources spread (pods/leases/events = 0/1/2 at N=3). Non-/registry
; keys keep whole-key FNV (max spread; their prefix ops use the existing
; scatter-gather / cross-shard-watch paths).
;
; Included by grpc-kv.scm AND grpc-watch.scm — both sides MUST agree on routing.

(define SHARD-ROUTE-REGISTRY (string->utf8 "/registry/"))

; (start . end) byte span of KEY that routing hashes: the first path segment
; after "/registry/" for registry keys (up to the next '/' or end-of-key),
; else the whole key.
(define (shard-route-span key)
  (let ((klen (bytevector-length key))
        (plen (bytevector-length SHARD-ROUTE-REGISTRY)))
    (if (not (and (>= klen plen)
                  (let pre ((i 0))
                    (or (= i plen)
                        (and (= (bytevector-u8-ref key i)
                                (bytevector-u8-ref SHARD-ROUTE-REGISTRY i))
                             (pre (+ i 1)))))))
        (cons 0 klen)
        (let seg ((i plen))
          (cond ((= i klen) (cons plen i))
                ((= (bytevector-u8-ref key i) 47) (cons plen i))   ; 47 = #\/
                (else (seg (+ i 1))))))))

; FNV-1a over KEY's routing span, mod N. N<=1 -> 0 (single-group).
(define (shard-route-hash key n)
  (if (<= n 1) 0
      (let* ((span (shard-route-span key)) (end (cdr span)))
        (let loop ((i (car span)) (h 2166136261))
          (if (= i end) (modulo h n)
              (loop (+ i 1)
                    (modulo (* (bitwise-xor h (bytevector-u8-ref key i)) 16777619)
                            4294967296)))))))

(define (shard-route-bv<=? a b)
  (let ((la (bytevector-length a)) (lb (bytevector-length b)))
    (let loop ((i 0))
      (cond ((= i la) #t)                    ; a = b, or a is a prefix of b
            ((= i lb) #f)
            ((< (bytevector-u8-ref a i) (bytevector-u8-ref b i)) #t)
            ((> (bytevector-u8-ref a i) (bytevector-u8-ref b i)) #f)
            (else (loop (+ i 1)))))))

; #t iff EVERY key in [KEY, REND) maps to the same group as KEY, so the op may
; route to that one shard. Claimed only for /registry/ keys whose resource
; segment is '/'-TERMINATED in KEY (e.g. key = "/registry/pods/..."): every
; bytestring >= KEY and < successor("/registry/pods/") starts with
; "/registry/pods/", so it hashes to the same segment. rend = #f is a single
; key (always one shard); etcd's "\0 = to end of keyspace" sentinel never is.
(define (shard-route-single? key rend n)
  (cond ((<= n 1) #t)
        ((not rend) #t)
        ((and (= (bytevector-length rend) 1) (= (bytevector-u8-ref rend 0) 0)) #f)
        (else
         (let* ((span (shard-route-span key))
                (s (car span)) (e (cdr span)))
           (and (> s 0)                            ; a /registry/ key
                (> e s)                            ; non-empty segment
                (< e (bytevector-length key))      ; segment '/'-terminated in KEY
                ; rend <= key[0..e] ++ ('/'+1): everything strictly below that
                ; bound still carries the "/registry/<segment>/" prefix.
                (let ((succ (make-bytevector (+ e 1) 0)))
                  (bytevector-copy! succ 0 key 0 e)
                  (bytevector-u8-set! succ e 48)   ; 48 = '/'+1
                  (shard-route-bv<=? rend succ)))))))
