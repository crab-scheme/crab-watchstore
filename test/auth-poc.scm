; test/auth-poc.scm — validates the Auth/RBAC design (ADR 0004) over the REAL
; store machinery, the way lease-poc.scm validates the lease index: the
; load-bearing RANGE-CONTAINMENT permission check + the NS-AUTH storage
; round-trip, both through src/auth.scm + the real store-ctx/mvcc layer.
;
; Enforcement (.26: token table + Authenticate + the per-request hook in
; grpc-kv + the deny->PermissionDenied mapping) and the gRPC Auth service (.27)
; are NOT here — this is the design's foundation.  If this is green the ADR's
; check + encoding are sound over the real store.

(include "test/harness.scm")
(include "src/store-ctx.scm")
(include "src/mvcc.scm")
(include "src/auth.scm")
(include "test/mvcc-util.scm")
(import (crab crypto))                 ; Argon2id password hashing (ADR §3)

; ---- open a fresh store (unique per run; substrate has no system/rm -rf) ----
(define run-tag (number->string (current-jiffy)))
(define DB-PATH (string-append "/tmp/cws-auth-poc-" run-tag))
(define CTX (make-ctx (store-open DB-PATH #t) "default" #t))

; (current-jiffy) is process-relative, so back-to-back runs may reuse the dir —
; empty the store before building state.  reset-ctx! clears META/KEY/REV/LEASE +
; the raft key; NS-AUTH is this task's new namespace, so clear it explicitly too.
(reset-ctx! CTX)
(reset-ns! CTX NS-AUTH)
(ctx-flush! CTX)

(define (b s) (string->utf8 s))
(define ALLK (mvcc-byte AUTH-ALLKEYS-BYTE))     ; the "\0" all-keys sentinel byte

; ===========================================================================
(section "range-containment: READWRITE [a,c) — covers a,b ; denies c,d")
; The crux check.  A perm [a,c) is a half-open interval; a single-key request k
; is covered iff a <= k < c.
(define p-ac (auth-make-perm PERM-READWRITE (b "a") (b "c")))
(define perms-ac (list p-ac))
; single-key reads (op-type 'read) — range-end #f.
(check "READWRITE[a,c) allows GET a" #t (auth-perms-allow? perms-ac (b "a") #f 'read))
(check "READWRITE[a,c) allows GET b" #t (auth-perms-allow? perms-ac (b "b") #f 'read))
(check "READWRITE[a,c) DENIES GET c (c is the exclusive end)" #f
       (auth-perms-allow? perms-ac (b "c") #f 'read))
(check "READWRITE[a,c) DENIES GET d" #f (auth-perms-allow? perms-ac (b "d") #f 'read))
; single-key writes (op-type 'write) — READWRITE covers writes too.
(check "READWRITE[a,c) allows PUT a" #t (auth-perms-allow? perms-ac (b "a") #f 'write))
(check "READWRITE[a,c) allows PUT b" #t (auth-perms-allow? perms-ac (b "b") #f 'write))
(check "READWRITE[a,c) DENIES PUT c" #f (auth-perms-allow? perms-ac (b "c") #f 'write))
(check "READWRITE[a,c) DENIES PUT d" #f (auth-perms-allow? perms-ac (b "d") #f 'write))

; ===========================================================================
(section "perm-type: READ allows GET but DENIES PUT (write needs WRITE/READWRITE)")
(define perms-read (list (auth-make-perm PERM-READ (b "a") (b "c"))))
(check "READ[a,c) allows GET b"   #t (auth-perms-allow? perms-read (b "b") #f 'read))
(check "READ[a,c) DENIES PUT b"   #f (auth-perms-allow? perms-read (b "b") #f 'write))
; and the symmetric WRITE-only perm
(define perms-write (list (auth-make-perm PERM-WRITE (b "a") (b "c"))))
(check "WRITE[a,c) allows PUT b"  #t (auth-perms-allow? perms-write (b "b") #f 'write))
(check "WRITE[a,c) DENIES GET b"  #f (auth-perms-allow? perms-write (b "b") #f 'read))

; ===========================================================================
(section "prefix permission: \"foo\" prefix covers foo, foobar — not fop")
; etcd prefix perm = [foo, prefixEnd(foo)).  prefix-range-end (mvcc.scm) bumps the
; last byte: prefixEnd("foo") = "fop", so the perm interval is [foo, fop).
(define foo-end (prefix-range-end (b "foo")))
(check "prefixEnd(foo) = fop" "fop" (utf8->string foo-end))
(define perms-foo (list (auth-make-perm PERM-READWRITE (b "foo") foo-end)))
(check "foo-prefix covers GET foo"      #t (auth-perms-allow? perms-foo (b "foo")    #f 'read))
(check "foo-prefix covers GET foobar"   #t (auth-perms-allow? perms-foo (b "foobar") #f 'read))
(check "foo-prefix DENIES GET fop (== the exclusive end)" #f
       (auth-perms-allow? perms-foo (b "fop") #f 'read))
(check "foo-prefix DENIES GET fo (before the prefix)" #f
       (auth-perms-allow? perms-foo (b "fo") #f 'read))

; ===========================================================================
(section "all-keys permission ([\\0,\\0]) covers everything")
; etcd's full-keyspace perm: key="\0", range-end="\0" -> [\0, +inf).
(define perms-all (list (auth-make-perm PERM-READWRITE ALLK ALLK)))
(check "all-keys covers GET a"      #t (auth-perms-allow? perms-all (b "a")    #f 'read))
(check "all-keys covers GET zzz"    #t (auth-perms-allow? perms-all (b "zzz")  #f 'read))
(check "all-keys covers PUT \\xff"  #t (auth-perms-allow? perms-all (mvcc-byte #xFF) #f 'write))
(check "all-keys covers a whole RANGE [a,z)" #t
       (auth-perms-allow? perms-all (b "a") (b "z") 'read))
(check "all-keys covers the all-keys RANGE itself" #t
       (auth-perms-allow? perms-all ALLK ALLK 'read))

; ===========================================================================
(section "RANGE request: allowed only if a SINGLE perm covers the WHOLE range")
; perm [a,m).  A range request [k,re) is covered iff a <= k AND re <= m.
(define perms-am (list (auth-make-perm PERM-READWRITE (b "a") (b "m"))))
(check "[a,m) covers RANGE [a,c)"   #t (auth-perms-allow? perms-am (b "a") (b "c") 'read))
(check "[a,m) covers RANGE [a,m)"   #t (auth-perms-allow? perms-am (b "a") (b "m") 'read))
(check "[a,m) DENIES RANGE [a,z) (extends past m)" #f
       (auth-perms-allow? perms-am (b "a") (b "z") 'read))
(check "[a,m) DENIES RANGE [b,p) (extends past m)" #f
       (auth-perms-allow? perms-am (b "b") (b "p") 'read))
; union does NOT stitch: [a,m) + [m,z) still can't cover [a,z) with one request.
(define perms-split (list (auth-make-perm PERM-READWRITE (b "a") (b "m"))
                          (auth-make-perm PERM-READWRITE (b "m") (b "z"))))
(check "two abutting perms do NOT combine to cover RANGE [a,z)" #f
       (auth-perms-allow? perms-split (b "a") (b "z") 'read))
(check "but each perm covers its own half: [m,z) covers RANGE [m,p)" #t
       (auth-perms-allow? perms-split (b "m") (b "p") 'read))
; to-eof request: range-end "\0" means [k, +inf) — needs an all-keys / +inf perm.
(check "[a,m) DENIES to-eof RANGE [a,\\0)" #f
       (auth-perms-allow? perms-am (b "a") ALLK 'read))
(check "all-keys perm covers to-eof RANGE [a,\\0)" #t
       (auth-perms-allow? perms-all (b "a") ALLK 'read))

; ===========================================================================
(section "auth storage round-trip: User{name,hash,roles} under NS-AUTH")
; Hash a REAL password with Argon2id (ADR §3 password scheme) and store it — the
; bytes the leader replicates verbatim so every node verifies identically.
(define alice-phc (string->utf8 (crypto-password-hash "s3cret")))
(auth-put-user! CTX (b "alice") alice-phc (list (b "dev") (b "ops")))
(ctx-flush! CTX)
(define alice (auth-get-user CTX (b "alice")))
(check "alice user round-trips (decoded non-#f)" #t (and alice #t))
(check "alice hash bytes round-trip verbatim" alice-phc (auth-user-hash alice))
(check "alice roles round-trip = (dev ops)" '("dev" "ops")
       (map utf8->string (auth-user-roles alice)))
; the stored Argon2id PHC verifies the original password (de-risks .26 verify)
(check "stored hash verifies the password (Argon2id)" #t
       (crypto-password-verify "s3cret" (utf8->string (auth-user-hash alice))))
(check "stored hash REJECTS a wrong password" #f
       (crypto-password-verify "wrong" (utf8->string (auth-user-hash alice))))
; a no-password (cert-CN-only) user stores an empty hash and still round-trips.
(auth-put-user! CTX (b "certguy") (make-bytevector 0 0) (list (b "readers")))
(ctx-flush! CTX)
(check "no-password user: empty hash round-trips" 0
       (bytevector-length (auth-user-hash (auth-get-user CTX (b "certguy")))))

; ===========================================================================
(section "auth storage round-trip: Role{name,perms} under NS-AUTH")
(define dev-perms (list (auth-make-perm PERM-READWRITE (b "app/") (prefix-range-end (b "app/")))
                        (auth-make-perm PERM-READ      (b "cfg")  #f)))   ; single-key READ
(auth-put-role! CTX (b "dev") dev-perms)
(ctx-flush! CTX)
(define dev-back (auth-get-role CTX (b "dev")))
(check "dev role round-trips to 2 perms" 2 (length dev-back))
(check "dev perm 0 type = READWRITE" PERM-READWRITE (perm-type (car dev-back)))
(check "dev perm 0 key = app/"       "app/" (utf8->string (perm-key (car dev-back))))
(check "dev perm 0 range-end = app0 (prefixEnd)" (prefix-range-end (b "app/"))
       (perm-rend (car dev-back)))
(check "dev perm 1 type = READ"      PERM-READ (perm-type (cadr dev-back)))
(check "dev perm 1 key = cfg"        "cfg" (utf8->string (perm-key (cadr dev-back))))
(check "dev perm 1 range-end empty (single-key)" 0
       (bytevector-length (perm-rend (cadr dev-back))))
; UserList / RoleList scans (.27) see exactly what we stored, nothing else.
(check "UserList = {alice, certguy}" '("alice" "certguy")
       (list-sort string<? (map utf8->string (auth-all-users CTX))))
(check "RoleList = {dev}" '("dev") (map utf8->string (auth-all-roles CTX)))

; ===========================================================================
(section "disjointness: auth records do NOT collide with normal user keys")
; Write a NORMAL keyspace key named exactly like an auth sub-key would look, via
; the REAL .6 write path (mvcc-apply PUT), at the same byte name "alice".
(mvcc-apply CTX (list (b "PUT") (b "alice") (b "a-normal-value")))
(ctx-flush! CTX)
; The normal key lives in NS-KEY; the auth user lives in NS-AUTH — no collision.
(check "normal GET alice = its keyspace value" "a-normal-value"
       (utf8->string (kv-rec-value (mvcc-get-latest CTX (b "alice")))))
(check "auth user alice still = its auth roles (untouched by the PUT)" '("dev" "ops")
       (map utf8->string (auth-user-roles (auth-get-user CTX (b "alice")))))
; THE security property: a client Range over ALL keys returns ONLY the keyspace
; key, NEVER the auth records (no password-hash enumeration via Range/Watch).
(define all-range (mvcc-range CTX ALLK ALLK '()))
(check "mvcc-range all-keys count = 1 (only the normal key, no auth rows)" 1 (car all-range))
(check "mvcc-range all-keys returns key 'alice'" "alice"
       (utf8->string (caar (cdr all-range))))
; and the auth user prefix scan returns ONLY auth users, never the NS-KEY 'alice'.
(check "auth UserList still = {alice, certguy} (NS-KEY 'alice' not present)"
       '("alice" "certguy")
       (list-sort string<? (map utf8->string (auth-all-users CTX))))

; ===========================================================================
(section "auth-enabled flag + auth-revision counter (etcd authRevision)")
(check "auth disabled by default" #f (auth-enabled? CTX))
(check "auth-revision starts at 0" 0 (auth-rev CTX))
; each auth mutation bumps the auth-revision (NOT the keyspace current-rev).
(define keyspace-rev-before (mvcc-current-rev CTX))
(check "bump auth-rev -> 1" 1 (auth-bump-rev! CTX))
(check "bump auth-rev -> 2" 2 (auth-bump-rev! CTX))
(ctx-flush! CTX)
(check "auth-rev reads back 2" 2 (auth-rev CTX))
(check "auth mutations did NOT bump the keyspace current-rev" keyspace-rev-before
       (mvcc-current-rev CTX))
(auth-set-enabled! CTX #t)
(ctx-flush! CTX)
(check "auth now enabled" #t (auth-enabled? CTX))

; ===========================================================================
(section "auth-authorize?: the full identity->user->roles->perms decision")
; alice has roles (dev ops); role dev grants RW app/* + READ cfg.  (Role ops is
; absent -> contributes no perms.)  Auth is now ENABLED (above).
(check "alice may RW app/main (dev RW app/*)" #t
       (auth-authorize? CTX (b "alice") (b "app/main") #f 'write))
(check "alice may READ cfg (dev READ cfg)" #t
       (auth-authorize? CTX (b "alice") (b "cfg") #f 'read))
(check "alice may NOT WRITE cfg (dev only READs it)" #f
       (auth-authorize? CTX (b "alice") (b "cfg") #f 'write))
(check "alice may NOT touch secret/x (no covering perm)" #f
       (auth-authorize? CTX (b "alice") (b "secret/x") #f 'read))
; unknown / no identity -> deny (auth enabled).
(check "unknown user -> deny" #f (auth-authorize? CTX (b "nobody") (b "app/main") #f 'read))
(check "no identity (#f) -> deny" #f (auth-authorize? CTX #f (b "app/main") #f 'read))

; ===========================================================================
(section "root role + auth-disabled both short-circuit to allow-all")
; the root user (holds the root role) may do anything, anywhere.
(auth-put-role! CTX AUTH-ROOT-ROLE '())          ; root role needs no perms
(auth-put-user! CTX (b "root") (string->utf8 (crypto-password-hash "rootpw"))
                (list AUTH-ROOT-ROLE))
(ctx-flush! CTX)
(check "root may WRITE anywhere (secret/x)" #t
       (auth-authorize? CTX (b "root") (b "secret/x") #f 'write))
(check "root may READ the whole keyspace" #t
       (auth-authorize? CTX (b "root") ALLK ALLK 'read))
; auth-disabled: ANY request is allowed regardless of user/perms.
(auth-set-enabled! CTX #f)
(ctx-flush! CTX)
(check "auth disabled -> alice may WRITE secret/x" #t
       (auth-authorize? CTX (b "alice") (b "secret/x") #f 'write))
(check "auth disabled -> even no identity is allowed" #t
       (auth-authorize? CTX #f (b "anything") #f 'write))

(done!)
