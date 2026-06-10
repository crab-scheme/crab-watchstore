; src/auth.scm — Auth/RBAC storage encoding + the permission-check (ADR 0004).
;
; PURE helpers only — this is the .26 (enforcement) / .27 (gRPC Auth service)
; HOME, but this file implements NEITHER.  It provides exactly two things:
;
;   1. the on-disk encoding for the NS-AUTH namespace (users, roles, the
;      auth-enabled flag, the auth-revision counter) + their read/decode and the
;      thin storage primitives the .26 apply path calls; and
;   2. the load-bearing RANGE-CONTAINMENT permission check (auth-authorize? and
;      its pure sub-predicates) that decides allow/deny for one request.
;
; NOT here (by design): the gRPC metadata token table + Authenticate (.26), the
; per-request hook into grpc-kv + the deny->PermissionDenied error mapping (.26),
; password HASHING (the leader computes it via (crab crypto) crypto-password-hash
; and proposes the PHC string as bytes — see ADR §3 — so the hash replicates
; identically; this file only stores/reads the resulting bytes), and the Auth
; gRPC service handlers (.27).
;
; Depends on (must be included BEFORE this file, same order as the shard/grpc-kv
; prefix): encoding.scm (subbv, u64->bytes, bytes->u64), store-ctx.scm (kv-get,
; kv-put!, kv-scan), mvcc.scm (mvcc-byte, bv<?, prefix-range-end).

; ===========================================================================
; NS-AUTH — a FIFTH 1-byte namespace tag, disjoint from META/KEY/REV/LEASE.
; ===========================================================================
;
; Auth records ride the SAME single column family as the keyspace (so they share
; the one WAL -> one Raft group-commit fsync -> replicated + crash-consistent +
; survive leader change, exactly like every other write).  But they live under
; their OWN tag, NOT inside NS-KEY: a dedicated namespace means client Range
; (NS-KEY scan) and Watch (NS-REV scan) NEVER see auth records — password hashes
; are not enumerable — and auth mutations do NOT bump the keyspace `current-rev`
; or emit Watch events (they have a SEPARATE auth-revision, etcd's authRevision).
; See ADR 0004 §2 + Alternatives for why this beats a reserved prefix in NS-KEY.
;
;   NS-AUTH 0x04  auth namespace, sub-split by a second tag byte:
;     user record :  0x04 || 'U' || name        -> {hash, roles[]}   blob
;     role record :  0x04 || 'R' || name         -> {perms[]}         blob
;     meta scalar :  0x04 || 'M' || name         -> u8/u64 scalar
;        "enabled"   -> u8   (0/1)               the global auth-enabled flag
;        "auth-rev"  -> u64                       the auth-revision counter
;
; The sub-tag bytes are 'M'(0x4D) < 'R'(0x52) < 'U'(0x55), so meta/role/user
; occupy disjoint ordered sub-ranges: a kv-scan of 0x04||'U' lists every user
; (UserList, .27) and never bleeds into roles/meta, and vice-versa.

(define NS-AUTH       #x04)
(define AUTH-SUB-META #x4D)   ; 'M'
(define AUTH-SUB-ROLE #x52)   ; 'R'
(define AUTH-SUB-USER #x55)   ; 'U'

; etcd authpb.Permission.Type — wire-parity values for .27.
(define PERM-READ      0)
(define PERM-WRITE     1)
(define PERM-READWRITE 2)

; etcd's special full-access role + its all-keys range-end sentinel ("\0").
(define AUTH-ROOT-ROLE   (string->utf8 "root"))
(define AUTH-ALLKEYS-BYTE #x00)

; ---- key builders ----

(define (auth-user-key name)              ; 0x04 || 'U' || name
  (bytevector-append (mvcc-byte NS-AUTH) (mvcc-byte AUTH-SUB-USER) name))
(define (auth-role-key name)              ; 0x04 || 'R' || name
  (bytevector-append (mvcc-byte NS-AUTH) (mvcc-byte AUTH-SUB-ROLE) name))
(define (auth-meta-key name-bv)           ; 0x04 || 'M' || name
  (bytevector-append (mvcc-byte NS-AUTH) (mvcc-byte AUTH-SUB-META) name-bv))

(define (auth-user-prefix) (bytevector-append (mvcc-byte NS-AUTH) (mvcc-byte AUTH-SUB-USER)))
(define (auth-role-prefix) (bytevector-append (mvcc-byte NS-AUTH) (mvcc-byte AUTH-SUB-ROLE)))
(define AUTH-PREFIX-LEN 2)                 ; NS-AUTH(1) + sub-tag(1) before the name

(define AUTH-META-ENABLED (auth-meta-key (string->utf8 "enabled")))
(define AUTH-META-REV     (auth-meta-key (string->utf8 "auth-rev")))

; ---- length-prefixed field helper (same self-describing style as ADR 0001 §4) ----

(define (auth-lp b)                        ; u64be(len b) || b
  (bytevector-append (u64->bytes (bytevector-length b)) b))

; ===========================================================================
; User record value:  {hash, roles[]}   (name lives in the key, not the value)
;   u64be hash_len ; bytes hash            ; Argon2id PHC string bytes ("" = no pw)
;   u64be role_count
;   role_count times:  u64be name_len ; bytes role_name
; ===========================================================================

(define (auth-enc-user hash roles)         ; hash: bv ; roles: list of name bv
  (let loop ((rs roles)
             (acc (bytevector-append (auth-lp hash) (u64->bytes (length roles)))))
    (if (null? rs) acc
        (loop (cdr rs) (bytevector-append acc (auth-lp (car rs)))))))

(define (auth-dec-user b)                  ; -> #(hash roles-list)
  (let* ((hlen (bytes->u64 b 0))
         (hash (subbv b 8 (+ 8 hlen)))
         (off  (+ 8 hlen))
         (rc   (bytes->u64 b off)))
    (let loop ((i 0) (o (+ off 8)) (rs '()))
      (if (= i rc)
          (vector hash (reverse rs))
          (let* ((nl (bytes->u64 b o))
                 (nm (subbv b (+ o 8) (+ o 8 nl))))
            (loop (+ i 1) (+ o 8 nl) (cons nm rs)))))))

(define (auth-user-hash  u) (vector-ref u 0))
(define (auth-user-roles u) (vector-ref u 1))

; ===========================================================================
; Permission + Role record value:  {perms[]}   (role name lives in the key)
;   u64be perm_count
;   perm_count times:
;     u8    perm_type                       ; READ/WRITE/READWRITE
;     u64be key_len     ; bytes key
;     u64be rangeend_len; bytes range_end   ; "" (len 0) = single-key permission
; ===========================================================================

; A permission is a 3-vector #(perm-type key range-end).  range-end #f is stored
; as the empty bytevector (single-key); decode yields the empty bytevector, which
; the check treats as single-key (range-end-unset?, mirroring mvcc.scm).
(define (auth-make-perm ptype key rend)
  (vector ptype key (if rend rend (make-bytevector 0 0))))
(define (perm-type p) (vector-ref p 0))
(define (perm-key  p) (vector-ref p 1))
(define (perm-rend p) (vector-ref p 2))

(define (auth-enc-role perms)
  (let loop ((ps perms) (acc (u64->bytes (length perms))))
    (if (null? ps) acc
        (let ((p (car ps)))
          (loop (cdr ps)
                (bytevector-append acc
                                   (mvcc-byte (perm-type p))
                                   (auth-lp (perm-key p))
                                   (auth-lp (perm-rend p))))))))

(define (auth-dec-role b)                  ; -> list of #(ptype key rend)
  (let ((pc (bytes->u64 b 0)))
    (let loop ((i 0) (o 8) (ps '()))
      (if (= i pc) (reverse ps)
          (let* ((pt (bytevector-u8-ref b o))
                 (kl (bytes->u64 b (+ o 1)))
                 (k  (subbv b (+ o 9) (+ o 9 kl)))
                 (rl (bytes->u64 b (+ o 9 kl)))
                 (re (subbv b (+ o 17 kl) (+ o 17 kl rl))))
            (loop (+ i 1) (+ o 17 kl rl) (cons (vector pt k re) ps)))))))

; ===========================================================================
; Storage primitives — the thin write/read the .26 apply path calls.  These are
; the auth analog of mvcc-put!/mvcc-get-latest, NOT enforcement: they encode/
; decode + kv-put!/kv-get, riding the existing group-commit batch.  Every auth
; MUTATION command (.26) calls auth-bump-rev! in the same batch (etcd bumps
; authRevision on every auth change, for client cache invalidation).
; ===========================================================================

(define (auth-put-user! ctx name hash roles)
  (kv-put! ctx (auth-user-key name) (auth-enc-user hash roles)))
(define (auth-get-user ctx name)           ; -> #(hash roles) | #f
  (let ((b (kv-get ctx (auth-user-key name)))) (and b (auth-dec-user b))))

(define (auth-put-role! ctx name perms)
  (kv-put! ctx (auth-role-key name) (auth-enc-role perms)))
(define (auth-get-role ctx name)           ; -> list of perms | #f
  (let ((b (kv-get ctx (auth-role-key name)))) (and b (auth-dec-role b))))

; list every user / role name (UserList / RoleList, .27): scan the sub-prefix
; and strip the 2-byte NS-AUTH||sub-tag head off each full key.
(define (auth-names-under ctx prefix)
  (map (lambda (row) (subbv (car row) AUTH-PREFIX-LEN (bytevector-length (car row))))
       (kv-scan ctx prefix)))
(define (auth-all-users ctx) (auth-names-under ctx (auth-user-prefix)))
(define (auth-all-roles ctx) (auth-names-under ctx (auth-role-prefix)))

; ---- the auth-enabled flag + the auth-revision counter (NS-AUTH meta scalars) ----

(define (auth-enabled? ctx)
  (let ((b (kv-get ctx AUTH-META-ENABLED)))
    (and b (>= (bytevector-length b) 1) (= (bytevector-u8-ref b 0) 1))))
(define (auth-set-enabled! ctx on?)
  (kv-put! ctx AUTH-META-ENABLED (mvcc-byte (if on? 1 0))))

(define (auth-rev ctx)                      ; current auth-revision (0 if never set)
  (let ((b (kv-get ctx AUTH-META-REV)))
    (if (and b (>= (bytevector-length b) 8)) (bytes->u64 b 0) 0)))
(define (auth-bump-rev! ctx)                ; +1, persist, return new value
  (let ((n (+ 1 (auth-rev ctx))))
    (kv-put! ctx AUTH-META-REV (u64->bytes n))
    n))

; ===========================================================================
; THE LOAD-BEARING CHECK — range-containment authorization (ADR 0004 §5).
; ===========================================================================
;
; A request operates on the interval [key, range-end); a permission grants the
; interval [pk, pre).  The permission COVERS the request iff its interval
; CONTAINS the request's interval AND its perm-type satisfies the op type.
;
; We canonicalize each (key, range-end) into [lo . hi) where hi is either a
; bytevector exclusive upper bound or the symbol '+inf (the all-keys-from-key
; sentinel, range-end = "\0").  The single-key and prefix cases then fall out of
; one uniform containment test:
;
;   range-end #f / empty   -> single key  : lo=key, hi = key||0x00  (= exactly {key})
;   range-end = "\0"       -> to-eof      : lo=key, hi = '+inf
;       (key = "\0" too    -> all keys    : lo="\0", hi = '+inf)
;   otherwise              -> half-open   : lo=key, hi = range-end
;
; Containment:  P=[plo,phi) covers R=[rlo,rhi)  iff  plo <= rlo AND rhi <= phi
; (with '+inf greater than every bytevector) — exactly the ADR rule "pk <= k and
; (re <= pre OR pre is the all-keys sentinel)", the sentinel folded into hi.

(define (auth-bv<=? a b) (not (bv<? b a)))  ; a <= b

(define (auth-rangeend-eof? re)             ; re is the single 0x00 sentinel byte
  (and re (= (bytevector-length re) 1)
       (= (bytevector-u8-ref re 0) AUTH-ALLKEYS-BYTE)))

(define (auth-bv-succ b)                     ; b||0x00 — smallest key strictly > b
  (bytevector-append b (mvcc-byte AUTH-ALLKEYS-BYTE)))

(define (auth-interval key re)              ; -> (lo . hi) ; hi is bv | '+inf
  (cond
    ((or (not re) (= (bytevector-length re) 0)) (cons key (auth-bv-succ key))) ; single
    ((auth-rangeend-eof? re)                     (cons key '+inf))             ; to-eof/all
    (else                                        (cons key re))))             ; half-open

; rhi <= phi, with '+inf as the maximum on either side.
(define (auth-hi<=? rhi phi)
  (cond ((eq? phi '+inf) #t)                 ; perm reaches +inf: covers any hi
        ((eq? rhi '+inf) #f)                 ; req reaches +inf but perm doesn't
        (else (auth-bv<=? rhi phi))))

; Does permission interval pI contain request interval rI?
(define (auth-interval-covers? pI rI)
  (and (auth-bv<=? (car pI) (car rI))        ; plo <= rlo
       (auth-hi<=? (cdr rI) (cdr pI))))       ; rhi <= phi

; perm-type satisfies the required op-type ('read for Range/Watch, 'write for
; Put/DeleteRange).  READWRITE satisfies both.
(define (auth-perm-satisfies? ptype required)
  (cond ((= ptype PERM-READWRITE) #t)
        ((eq? required 'read)  (= ptype PERM-READ))
        ((eq? required 'write) (= ptype PERM-WRITE))
        (else #f)))

; one permission covers one request (key, re) for the required op-type.
(define (auth-perm-covers? perm key re required)
  (and (auth-perm-satisfies? (perm-type perm) required)
       (auth-interval-covers? (auth-interval (perm-key perm) (perm-rend perm))
                              (auth-interval key re))))

; UNION over a permission list: a request is allowed iff SOME SINGLE permission
; covers the whole [key, re) (etcd does NOT stitch multiple perms across one
; range request — a range needs one covering permission).
(define (auth-perms-allow? perms key re required)
  (let loop ((ps perms))
    (cond ((null? ps) #f)
          ((auth-perm-covers? (car ps) key re required) #t)
          (else (loop (cdr ps))))))

(define (auth-has-root-role? roles)         ; roles: list of name bv
  (let loop ((rs roles))
    (cond ((null? rs) #f)
          ((equal? (car rs) AUTH-ROOT-ROLE) #t)
          (else (loop (cdr rs))))))

; resolve a user's full permission set (union across all its roles).
(define (auth-user-perms ctx username)
  (let ((u (auth-get-user ctx username)))
    (if (not u) '()
        (let loop ((roles (auth-user-roles u)) (acc '()))
          (if (null? roles) acc
              (let ((rp (auth-get-role ctx (car roles))))
                (loop (cdr roles) (if rp (append acc rp) acc))))))))

; THE authorization decision (pure read; .26 maps #f -> PermissionDenied).
;   auth disabled               -> #t  (allow all)
;   username #f (auth enabled)   -> #f  (no identity; .26 maps to ErrUserEmpty /
;                                        ErrInvalidAuthToken)
;   user has the root role       -> #t  (full access)
;   else                          -> some role permission covers [key, re)
; required is 'read (Range/Watch) or 'write (Put/DeleteRange).
(define (auth-authorize? ctx username key re required)
  (cond
    ((not (auth-enabled? ctx)) #t)
    ((not username) #f)
    (else
     (let ((u (auth-get-user ctx username)))
       (cond
         ((not u) #f)
         ((auth-has-root-role? (auth-user-roles u)) #t)
         (else (auth-perms-allow? (auth-user-perms ctx username) key re required)))))))
