; test/mvcc-apply.scm — unit test for the MVCC apply path (cw-u4a.6), NO Raft.
;
; Drives mvcc-apply directly against a single durable ctx over a real RocksDB and
; asserts the full revision-stamping semantics of ADR 0001: revision bumps,
; create_rev/mod_rev/version, tombstones, read-at-revision, range delete, lease
; index, and the current-rev meta key.  A fresh per-run temp dir ((current-jiffy))
; means a prior run's persisted state can never bleed in.

(include "test/harness.scm")
(include "src/store-ctx.scm")
(include "src/mvcc.scm")
(include "src/auth.scm")      ; NS-AUTH + the auth check — mvcc-apply's AUTH-* cases (cw-u4a.26)
(include "test/mvcc-util.scm")

; ---- open a fresh store (unique per run; substrate has no system/rm -rf) ----
(define run-tag (number->string (current-jiffy)))
(define DB-PATH (string-append "/tmp/cws-mvcc-apply-" run-tag))
(define CTX (make-ctx (store-open DB-PATH #t) "default" #t))

; Guarantee isolation: (current-jiffy) is process-relative, so back-to-back runs
; reuse the same dir — empty the store before building any state.
(reset-ctx! CTX)
(reset-ns! CTX NS-AUTH)       ; NS-AUTH is this task's namespace — clear it too

; ---- helpers ----
(define (b s) (string->utf8 s))
(define (put . parts) (mvcc-apply CTX (map b parts)))      ; ("PUT" "k" "v" ["lease"])
(define (del . parts) (mvcc-apply CTX (map b parts)))      ; ("DEL" "k" ["end"])

; latest record for K (or at-rev), decoded; #f if absent/tombstoned
(define (latest K)         (mvcc-get-latest CTX (b K)))
(define (latest-at K rev)  (mvcc-get-latest CTX (b K) rev))
(define (val-of r) (and r (utf8->string (kv-rec-value r))))

; ===========================================================================
(section "PUT k v -> rev 1, version 1, create_rev 1, mod_rev 1")
(check "apply returns rev 1" (cons "PUT" 1) (put "PUT" "k" "v"))
(check "current-rev = 1"     1 (mvcc-current-rev CTX))
(let ((r (latest "k")))
  (check "k value"      "v" (val-of r))
  (check "k create_rev" 1   (kv-rec-create-rev r))
  (check "k mod_rev"    1   (kv-rec-mod-rev r))
  (check "k version"    1   (kv-rec-version r))
  (check "k not tombstone" #f (kv-rec-tombstone? r)))

; ===========================================================================
(section "PUT k v2 -> rev 2, version 2, create_rev 1, mod_rev 2 (update keeps create)")
(check "apply returns rev 2" (cons "PUT" 2) (put "PUT" "k" "v2"))
(check "current-rev = 2"     2 (mvcc-current-rev CTX))
(let ((r (latest "k")))
  (check "k value now v2" "v2" (val-of r))
  (check "k create_rev kept 1" 1 (kv-rec-create-rev r))
  (check "k mod_rev 2"    2 (kv-rec-mod-rev r))
  (check "k version 2"    2 (kv-rec-version r)))

; ===========================================================================
(section "DEL k -> rev 3, tombstoned (get-latest #f)")
(check "apply returns DEL rev 3, 1 deleted" (cons "DEL" (cons 3 1)) (del "DEL" "k"))
(check "current-rev = 3" 3 (mvcc-current-rev CTX))
(check "k now absent (tombstoned)" #f (latest "k"))

; REV-CF holds PUT, PUT, DELETE in revision order (Watch order).
(let* ((events (kv-scan CTX (mvcc-byte NS-REV)))
       (kinds  (map (lambda (kv) (ev-kind (event-decode (cdr kv)))) events))
       (revs   (map (lambda (kv) (ev-mod-rev (event-decode (cdr kv)))) events)))
  (check "REV-CF kinds = PUT PUT DELETE"
         (list EV-PUT EV-PUT EV-DELETE) kinds)
  (check "REV-CF revs ascending 1 2 3" (list 1 2 3) revs))

; ===========================================================================
(section "PUT k v3 after delete -> rev 4, version RESETS to 1, create_rev 4")
(check "apply returns rev 4" (cons "PUT" 4) (put "PUT" "k" "v3"))
(let ((r (latest "k")))
  (check "k value v3"        "v3" (val-of r))
  (check "k version reset 1" 1  (kv-rec-version r))
  (check "k create_rev 4 (recreate)" 4 (kv-rec-create-rev r))
  (check "k mod_rev 4"       4  (kv-rec-mod-rev r)))

; ===========================================================================
(section "read-at-revision: historical reads")
; at rev 1 k was "v"; at rev 3 k was tombstoned (deleted)
(check "get-latest k @1 = v (historical)" "v" (val-of (latest-at "k" 1)))
(check "get-latest k @2 = v2"             "v2" (val-of (latest-at "k" 2)))
(check "get-latest k @3 = #f (deleted)"   #f   (latest-at "k" 3))
(check "get-latest k @4 = v3"             "v3" (val-of (latest-at "k" 4)))

; ===========================================================================
(section "range delete: DEL [a, c) deletes a,b not c")
(put "PUT" "a" "va")    ; rev 5
(put "PUT" "b" "vb")    ; rev 6
(put "PUT" "c" "vc")    ; rev 7
(check "current-rev = 7 after a,b,c" 7 (mvcc-current-rev CTX))
(check "a live before"  "va" (val-of (latest "a")))
(check "b live before"  "vb" (val-of (latest "b")))
(check "c live before"  "vc" (val-of (latest "c")))
(let ((res (del "DEL" "a" "c")))             ; rev 8, range [a,c)
  (check "DEL [a,c) deleted 2 (a,b)" (cons "DEL" (cons 8 2)) res))
(check "a deleted" #f (latest "a"))
(check "b deleted" #f (latest "b"))
(check "c survives range" "vc" (val-of (latest "c")))

; ===========================================================================
(section "lease: PUT lk v 100 -> LEASE index has 0x03||u64be(100)||lk")
; A key may only attach to a GRANTED lease (cw-u4a.17 put-to-dead-lease guard,
; etcd ErrLeaseNotFound).  Grant lease 100 first; a grant does NOT bump the rev.
(put "LEASE-GRANT" "100" "60")               ; grant lease 100 (ttl 60), no rev bump
(put "PUT" "lk" "lv" "100")                  ; rev 9, lease 100
(let* ((lkey (enc-lease 100 (b "lk")))
       (got  (kv-get CTX lkey)))
  (check "lease index entry present" #t (and got #t))
  (check "lease index value empty"   0  (bytevector-length got)))
; the record carries the lease inline
(check "lk record lease = 100" 100 (kv-rec-lease (latest "lk")))
; scanning the lease-100 prefix finds the length-9 meta sentinel (the lease object,
; cw-u4a.17) PLUS the single attached key lk = 2 rows.  mvcc-lease-keys skips the
; sentinel and returns just {lk}.
(let ((rows (kv-scan CTX (lease-prefix 100))))
  (check "lease-100 prefix scan finds meta + lk = 2 rows" 2 (length rows)))
(check "mvcc-lease-keys 100 = {lk} (sentinel skipped)"
       (list (b "lk")) (mvcc-lease-keys CTX 100))

; ===========================================================================
(section "META current-rev matches last bump")
(check "current-rev = 9 (last PUT lk)" 9 (mvcc-current-rev CTX))

; ===========================================================================
; AUTH-* apply path (cw-u4a.26, ADR 0004 §2) — NS-AUTH mutations, separate
; auth-revision, and the load-bearing authorize decision over the REAL store.
; ===========================================================================
(define (auth . parts) (mvcc-apply CTX (map ->bv parts)))   ; AUTH-* with bytevector args
(define root-hash (crypto-password-hash "rootpw"))          ; real Argon2id PHC (global builtin)
(define (user-add name hash) (mvcc-apply CTX (list (b "AUTH-USER-ADD") (b name) (string->utf8 hash))))

(section "auth starts disabled, auth-rev 0; keyspace current-rev untouched by auth")
(check "auth-enabled? #f initially" #f (auth-enabled? CTX))
(check "auth-rev 0 initially"        0 (auth-rev CTX))

(section "AUTH-ENABLE refused until a root user holds the root role (etcd guard)")
; auth-rev bumps +1 per SUCCESSFUL mutation; the rejected ENABLE does NOT bump.
(check "USER-ADD root -> AUTH-OK rev 1" (cons "AUTH-OK" 1) (user-add "root" root-hash))
(check "ENABLE w/o root role -> err-root-role-not-exist (no bump)"
       'err-root-role-not-exist (car (auth "AUTH-ENABLE")))
(check "ROLE-ADD root -> AUTH-OK rev 2" (cons "AUTH-OK" 2) (auth "AUTH-ROLE-ADD" "root"))
(check "GRANT-ROLE root root -> AUTH-OK rev 3" (cons "AUTH-OK" 3) (auth "AUTH-USER-GRANT-ROLE" "root" "root"))
(check "ENABLE now succeeds -> AUTH-OK rev 4" (cons "AUTH-OK" 4) (auth "AUTH-ENABLE"))
(check "auth-enabled? #t" #t (auth-enabled? CTX))

(section "stored root PHC verifies the password (Argon2id round-trip through NS-AUTH)")
(let ((u (auth-get-user CTX (b "root"))))
  (check "root pw verifies"  #t (crypto-password-verify "rootpw" (utf8->string (auth-user-hash u))))
  (check "wrong pw rejected" #f (crypto-password-verify "nope"   (utf8->string (auth-user-hash u)))))

(section "alice + dev role: readwrite app/ (prefix [app/, app0))")
(user-add "alice" (crypto-password-hash "alicepw"))
(auth "AUTH-ROLE-ADD" "dev")
(auth "AUTH-ROLE-GRANT-PERM" "dev" "2" "app/" "app0")   ; permType 2 = READWRITE
(auth "AUTH-USER-GRANT-ROLE" "alice" "dev")

(section "authorize: alice within app/ prefix; denied beyond; root override")
(check "alice WRITE app/x allowed"  #t (auth-authorize? CTX (b "alice") (b "app/x") #f 'write))
(check "alice READ  app/x allowed"  #t (auth-authorize? CTX (b "alice") (b "app/x") #f 'read))
(check "alice WRITE other/y DENIED" #f (auth-authorize? CTX (b "alice") (b "other/y") #f 'write))
(check "root WRITE anywhere allowed (root role)"
       #t (auth-authorize? CTX (b "root") (b "anywhere") #f 'write))
(check "unknown user DENIED" #f (auth-authorize? CTX (b "nobody") (b "app/x") #f 'read))

(section "duplicate / not-found guards")
(check "USER-ADD alice again -> err-user-exists"
       'err-user-exists (car (user-add "alice" root-hash)))
(check "ROLE-ADD dev again -> err-role-exists"
       'err-role-exists (car (auth "AUTH-ROLE-ADD" "dev")))
(check "GRANT-PERM on missing role -> err-role-not-found"
       'err-role-not-found (car (auth "AUTH-ROLE-GRANT-PERM" "ghost" "0" "k" "")))

(section "REVOKE-ROLE removes the grant; ROLE-DELETE strips the role from every user")
(auth "AUTH-USER-REVOKE-ROLE" "alice" "dev")
(check "alice WRITE app/x now DENIED (role revoked)"
       #f (auth-authorize? CTX (b "alice") (b "app/x") #f 'write))
(auth "AUTH-USER-GRANT-ROLE" "alice" "dev")              ; re-grant, then delete the role
(check "alice WRITE app/x allowed again" #t (auth-authorize? CTX (b "alice") (b "app/x") #f 'write))
(auth "AUTH-ROLE-DELETE" "dev")
(check "alice no longer holds dev after ROLE-DELETE"
       #f (let ((u (auth-get-user CTX (b "alice")))) (member (b "dev") (auth-user-roles u))))
(check "alice WRITE app/x DENIED after ROLE-DELETE"
       #f (auth-authorize? CTX (b "alice") (b "app/x") #f 'write))

(section "AUTH-DISABLE -> every request allowed again")
(auth "AUTH-DISABLE")
(check "auth-enabled? #f after disable" #f (auth-enabled? CTX))
(check "disabled -> even unknown user allowed" #t (auth-authorize? CTX (b "nobody") (b "x") #f 'write))

(section "auth mutations NEVER bumped the keyspace current-rev (still 9)")
(check "current-rev still 9 after all auth ops" 9 (mvcc-current-rev CTX))

(done!)
