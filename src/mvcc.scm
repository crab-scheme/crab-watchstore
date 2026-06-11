; src/mvcc.scm — the MVCC data-model layer for crab-watchstore (cw-u4a.6).
;
; Implements ADR 0001 (docs/adr/0001-mvcc-data-model.md) on top of the durable-KV
; substrate (src/store-ctx.scm).  All on-disk byte layouts here are the ones the
; POC test (test/mvcc-encoding-poc.scm) proves sort correctly — the encoders below
; are lifted from it verbatim so the sort order is identical.
;
; FOUR namespaces share the single column family, split by a 1-byte leading tag
; (META < KEY < REV < LEASE, so a prefix scan of one never bleeds into another):
;
;   NS-META  0x00  meta scalars        0x00 || name          -> u64  (current-rev / compact-rev)
;   NS-KEY   0x01  key-ordered store   0x01 || u64be(lenK) || K || INV(rev16) -> KeyValue record
;   NS-REV   0x02  revision-ordered    0x02 || rev16         -> event record
;   NS-LEASE 0x03  lease -> keys index 0x03 || u64be(leaseId) || K -> ()
;
; rev16 = u64be(main) || u64be(sub) (16 bytes).  INV(rev16) = bitwise complement,
; so a NEWER revision is a SMALLER on-disk key within a key group -> a forward
; kv-scan of the key prefix returns versions newest->oldest.
;
; Every write goes through the ctx (kv-put!/kv-del!), so it rides the existing
; group-commit batch + one-fsync atomicity (revision + records + applied-index land
; together).  NO direct store-put/store-get here.
;
; Depends on: encoding.scm (subbv, u64->bytes, bytes->u64), store-ctx.scm (kv-*).
;
; Public surface (see ADR §"Consumers" cw-u4a.6 row):
;   mvcc-current-rev ctx                       -> current main revision (0 default)
;   mvcc-get-latest  ctx K [at-rev]            -> KeyValue record (decoded) | #f
;   mvcc-put!        ctx K V lease main sub     -> writes KEY+REV(+LEASE), returns mod_rev
;   mvcc-delete-range! ctx K rangeEnd main sub  -> count deleted (writes tombstones+events)
;   mvcc-apply       ctx cmd                    -> result usable as the client ack
; plus the kv-record decode accessors (kv-rec-* / ev-* below) the later tasks reuse.

; ---------------------------------------------------------------------------
; namespace tags
; ---------------------------------------------------------------------------

(define NS-META  #x00)   ; meta scalars (current-rev, compact-rev) ; sorts first
(define NS-KEY   #x01)   ; key-ordered store:    K || INV(rev) -> KeyValue record
(define NS-REV   #x02)   ; revision-ordered idx: rev           -> event record
(define NS-LEASE #x03)   ; lease -> keys index:  leaseId || K  -> ()

(define (mvcc-byte b)
  (let ((v (make-bytevector 1 0))) (bytevector-u8-set! v 0 b) v))

; ---------------------------------------------------------------------------
; rev16 + INV  (lifted from the POC; the sort order it proves is load-bearing)
; ---------------------------------------------------------------------------

; rev16 = u64be(main) || u64be(sub).  main dominates (high 8 bytes), sub breaks
; ties — lexicographic order == etcd's numeric main.sub order.
(define (rev->16 main sub)
  (bytevector-append (u64->bytes main) (u64->bytes sub)))

; INV(rev16) = bitwise complement of each byte -> DESCENDING on-disk order
; (newest revision = smallest key) within a key group.
(define (inv16 b16)
  (let ((c (subbv b16 0 16)))
    (let loop ((i 0))
      (if (< i 16)
          (begin (bytevector-u8-set! c i (bitwise-xor (bytevector-u8-ref c i) #xFF))
                 (loop (+ i 1)))))
    c))

; Largest u64 (2^64-1).  Used as the `sub` in a latest-<=-readRev seek key so the
; seek lands at/before the largest-sub record at a given main (cw-u4a.38).
(define MAX-U64 (- (expt 2 64) 1))

; ---------------------------------------------------------------------------
; KEY-CF key:  NS-KEY || u64be(lenK) || K || INV(rev16)
; ---------------------------------------------------------------------------

(define (key-cf-prefix K)               ; NS-KEY || u64be(lenK) || K  (scan prefix)
  (bytevector-append (mvcc-byte NS-KEY)
                     (u64->bytes (bytevector-length K))
                     K))

(define (enc-key K main sub)            ; full KEY-CF key
  (bytevector-append (key-cf-prefix K)
                     (inv16 (rev->16 main sub))))

; ---------------------------------------------------------------------------
; REV-CF key:  NS-REV || rev16  (PLAIN, ascending — Watch replays oldest->newest)
; ---------------------------------------------------------------------------

(define (enc-rev main sub)
  (bytevector-append (mvcc-byte NS-REV) (rev->16 main sub)))

; ---------------------------------------------------------------------------
; LEASE index key:  NS-LEASE || u64be(leaseId) || K
; ---------------------------------------------------------------------------

(define (enc-lease leaseId K)
  (bytevector-append (mvcc-byte NS-LEASE) (u64->bytes leaseId) K))

(define (lease-prefix leaseId)          ; NS-LEASE || u64be(leaseId)  (revoke scan)
  (bytevector-append (mvcc-byte NS-LEASE) (u64->bytes leaseId)))

; PURE READ (no write-path change; the LEASE write path is cw-u4a.6, validated).
; Enumerate the user-keys currently attached to a lease — exactly the set the
; revoke path (cw-u4a.17 "LEASE-REVOKE") will iterate and tombstone.  The lease
; index entry is 0x03 || u64be(leaseId) || K, so the user-key K is everything
; after the 1-byte tag + 8-byte leaseId (offset 9).  Returns a list of user-key
; bytevectors in on-disk (ascending-K) order; '() if the lease has no keys.
;
; SKIP the length-9 lease-META sentinel (0x03 || u64be(id), empty K — the lease
; OBJECT, ADR 0003 §1) which sorts FIRST in the same prefix group: a real attached
; key K is non-empty, so a length-9 row is the meta entry, never an index entry.
(define (mvcc-lease-keys ctx leaseId)
  (let ((pfx-len 9))   ; NS-LEASE(1) + u64be(leaseId)(8)
    (let loop ((rows (kv-scan ctx (lease-prefix leaseId))) (out '()))
      (if (null? rows)
          (reverse out)
          (let ((fk (caar rows)))          ; full index key 0x03||u64be(id)||K
            (if (> (bytevector-length fk) pfx-len)        ; non-empty K => real index entry
                (loop (cdr rows) (cons (subbv fk pfx-len (bytevector-length fk)) out))
                (loop (cdr rows) out)))))))                ; length-9 meta sentinel: skip

; ---------------------------------------------------------------------------
; lease META entry (cw-u4a.17, ADR 0003 §1) — the lease OBJECT, replicated.
; ---------------------------------------------------------------------------
;
; The granted TTL must survive a leader change, so it lives in the durable,
; replicated store under NS-LEASE, distinguished from the lease->keys index
; entries by an EMPTY sentinel key (K = ""):
;
;   lease-meta key   :  0x03 || u64be(id)            (length exactly 9, K empty)
;   lease-meta value :  u64be(granted_ttl_seconds)
;
; The sentinel 0x03||u64be(id) is a byte-prefix of every real index entry
; 0x03||u64be(id)||K (K non-empty), so it sorts FIRST within the lease's group.
; "Is lease id live?" == this meta entry exists.  mvcc-lease-keys above already
; skips it (it returns only non-empty K), since the meta key's tail past offset 9
; is empty and is never produced by a real attach.

(define (lease-meta-key id)             ; 0x03 || u64be(id) || <empty>  (length 9)
  (lease-prefix id))                    ; identical bytes to the scan prefix

(define (mvcc-lease-meta-get ctx id)    ; granted-ttl | #f if no such lease
  (let ((b (kv-get ctx (lease-meta-key id))))
    (if (and b (>= (bytevector-length b) 8)) (bytes->u64 b 0) #f)))

(define (mvcc-lease-exists? ctx id)
  (and (mvcc-lease-meta-get ctx id) #t))

; the replicated auto-id counter (NS-META scalar, alongside current-rev), only
; ever incremented on grant (never reused) so ids are unique across the cluster's
; life even across leader changes.  META-LEASE-ID-SEQ itself is defined in the META
; keys section below (after meta-key), since this lease section runs before it.
(define (mvcc-lease-id-seq ctx)
  (let ((b (kv-get ctx META-LEASE-ID-SEQ)))
    (if (and b (>= (bytevector-length b) 8)) (bytes->u64 b 0) 0)))

; All live lease ids (decode the id out of each length-9 sentinel key under
; NS-LEASE).  The leader's failover re-derivation scans this; LeaseLeases (.18)
; will reuse it.  A real index entry is length > 9 (has a non-empty K), so the
; length-9 guard isolates the meta sentinels.
(define (mvcc-all-lease-ids ctx)
  (let loop ((rows (kv-scan ctx (mvcc-byte NS-LEASE))) (out '()))
    (if (null? rows)
        (reverse out)
        (let ((fk (caar rows)))
          (if (= (bytevector-length fk) 9)               ; 0x03 || u64be(id), K empty
              (loop (cdr rows) (cons (bytes->u64 fk 1) out))
              (loop (cdr rows) out))))))

; ---------------------------------------------------------------------------
; META keys
; ---------------------------------------------------------------------------

(define (meta-key name) (bytevector-append (mvcc-byte NS-META) (string->utf8 name)))
(define META-CURRENT-REV (meta-key "current-rev"))
(define META-COMPACT-REV (meta-key "compact-rev"))
(define META-LEASE-ID-SEQ (meta-key "lease-id-seq"))   ; lease auto-id counter (cw-u4a.17)

; ---------------------------------------------------------------------------
; KeyValue record (self-describing, length-prefixed; ADR §4)
;   u8    tag          ; 0 = VALUE (live), 1 = TOMBSTONE
;   u64be create_revision
;   u64be mod_revision
;   u64be version
;   u64be lease
;   u64be value_len
;   bytes value
; ---------------------------------------------------------------------------

(define REC-VALUE     0)
(define REC-TOMBSTONE 1)

(define (kv-record-encode tag create-rev mod-rev version lease value)
  (bytevector-append
   (mvcc-byte tag)
   (u64->bytes create-rev)
   (u64->bytes mod-rev)
   (u64->bytes version)
   (u64->bytes lease)
   (u64->bytes (bytevector-length value))
   value))

; decode -> a vector #(tag create-rev mod-rev version lease value) for cheap access
(define (kv-record-decode b)
  (let* ((tag     (bytevector-u8-ref b 0))
         (cr      (bytes->u64 b 1))
         (mr      (bytes->u64 b 9))
         (ver     (bytes->u64 b 17))
         (lease   (bytes->u64 b 25))
         (vlen    (bytes->u64 b 33))
         (value   (subbv b 41 (+ 41 vlen))))
    (vector tag cr mr ver lease value)))

; KeyValue accessors (the read API + later tasks use these)
(define (kv-rec-tombstone? r) (= (vector-ref r 0) REC-TOMBSTONE))
(define (kv-rec-create-rev r) (vector-ref r 1))
(define (kv-rec-mod-rev    r) (vector-ref r 2))
(define (kv-rec-version    r) (vector-ref r 3))
(define (kv-rec-lease      r) (vector-ref r 4))
(define (kv-rec-value      r) (vector-ref r 5))

; ---------------------------------------------------------------------------
; REV-CF event record (ADR §5)
;   u8    kind         ; 0 = PUT, 1 = DELETE
;   u64be key_len
;   bytes key
;   u64be value_len
;   bytes value        ; new value on PUT; empty on DELETE
;   u64be mod_revision ; = rev16.main of this event
; ---------------------------------------------------------------------------

(define EV-PUT    0)
(define EV-DELETE 1)

(define (event-encode kind key value mod-rev)
  (bytevector-append
   (mvcc-byte kind)
   (u64->bytes (bytevector-length key))
   key
   (u64->bytes (bytevector-length value))
   value
   (u64->bytes mod-rev)))

(define (event-decode b)
  (let* ((kind  (bytevector-u8-ref b 0))
         (klen  (bytes->u64 b 1))
         (key   (subbv b 9 (+ 9 klen)))
         (voff  (+ 9 klen))
         (vlen  (bytes->u64 b voff))
         (value (subbv b (+ voff 8) (+ voff 8 vlen)))
         (mroff (+ voff 8 vlen))
         (mr    (bytes->u64 b mroff)))
    (vector kind key value mr)))

(define (ev-kind  e) (vector-ref e 0))
(define (ev-key   e) (vector-ref e 1))
(define (ev-value e) (vector-ref e 2))
(define (ev-mod-rev e) (vector-ref e 3))

; ---------------------------------------------------------------------------
; revision management (META; ADR §2)
; ---------------------------------------------------------------------------

; current main revision; 0 if never written.
(define (mvcc-current-rev ctx)
  (let ((b (kv-get ctx META-CURRENT-REV)))
    (if (and b (>= (bytevector-length b) 8)) (bytes->u64 b 0) 0)))

(define (mvcc-set-current-rev! ctx main)
  (kv-put! ctx META-CURRENT-REV (u64->bytes main)))

; compact revision (read floor); 0 if never compacted.  (Written by .8.)
(define (mvcc-compact-rev ctx)
  (let ((b (kv-get ctx META-COMPACT-REV)))
    (if (and b (>= (bytevector-length b) 8)) (bytes->u64 b 0) 0)))

; ---------------------------------------------------------------------------
; point read: latest visible version of K (optionally at-or-below at-rev)
; ---------------------------------------------------------------------------
;
; kv-scan the key prefix; results are newest->oldest (INV).  Take the first record
; (or, with at-rev, the first whose mod_revision <= at-rev).  Return #f if absent
; or that newest-visible record is a tombstone.  This is the apply-time read for
; create_rev/version AND the seed for .7 Range / point reads.

(define (mvcc-get-latest ctx K . at)
  ; cw-u4a.38: a single O(log n) RocksDB seek instead of materialising K's whole
  ; version group with kv-scan and walking it.  KEY-CF sorts a key's versions
  ; newest-first via INV(rev16), so the latest version with mod_rev <= at-rev is the
  ; FIRST on-disk key >= prefix || INV(rev16(at-rev, MAX-sub)).  Maxing the sub
  ; (INV -> 0x00..) makes the target sort at/before the largest-sub record at at-rev,
  ; so the first hit is exactly that version — or, if none exists at at-rev, the
  ; next-older one (records with mod_rev > at-rev sort BEFORE the target and are
  ; skipped by the seek).  A current read (no at-rev) seeks to the prefix itself =
  ; the newest version.  A tombstone as the first visible version => key absent.
  (let* ((at-rev  (if (pair? at) (car at) #f))
         (prefix  (key-cf-prefix K))
         (seekkey (if at-rev
                      (bytevector-append prefix (inv16 (rev->16 at-rev MAX-U64)))
                      prefix))
         (hit     (kv-seek ctx seekkey prefix)))
    (if (not hit)
        #f
        (let ((r (kv-record-decode (cdr hit))))
          (if (kv-rec-tombstone? r) #f r)))))

; ---------------------------------------------------------------------------
; mvcc-put!  (ADR §3/§4/§5/§8)
; ---------------------------------------------------------------------------
;
; Write one PUT at revision main.sub:
;   - create_rev/version: if a live record exists -> create_rev kept, version+1;
;     else (absent or tombstoned) -> create_rev = this rev, version = 1.
;   - mod_rev = this rev.
;   - KEY-CF record (kv-put!), REV-CF PUT event (kv-put!), LEASE index maintenance
;     (add 0x03||lease||K when lease<>0; remove a stale prior lease-index entry when
;     the key previously had a different/zero lease).

(define (mvcc-put! ctx K V lease main sub)
  (let* ((prev (mvcc-get-latest ctx K))          ; newest visible (live) record, or #f
         (create-rev (if prev (kv-rec-create-rev prev) main))
         (version    (if prev (+ 1 (kv-rec-version prev)) 1))
         (prev-lease (if prev (kv-rec-lease prev) 0)))
    ; KEY-CF: the new live version
    (kv-put! ctx (enc-key K main sub)
             (kv-record-encode REC-VALUE create-rev main version lease V))
    ; REV-CF: a PUT event keyed by this op's own main.sub
    (kv-put! ctx (enc-rev main sub) (event-encode EV-PUT K V main))
    ; LEASE index: drop the stale entry if the lease changed, add the new one.
    (if (and (not (= prev-lease 0)) (not (= prev-lease lease)))
        (kv-del! ctx (enc-lease prev-lease K)))
    (if (not (= lease 0))
        (kv-put! ctx (enc-lease lease K) (make-bytevector 0 0)))
    main))

; ---------------------------------------------------------------------------
; mvcc-delete-range!  (ADR §6/§5/§8)
; ---------------------------------------------------------------------------
;
; For each LIVE key in [K, rangeEnd) (rangeEnd #f/empty => single key K) write a
; TOMBSTONE KEY-CF record at this rev, a REV-CF DELETE event, and remove its
; LEASE-index entry.  Sub-revision increments per deleted key (intra-Txn order).
; Returns the count deleted.

; Is `rangeEnd` an "unset" range (single-key delete)?  #f or the empty bytevector.
(define (range-end-unset? rangeEnd)
  (or (not rangeEnd) (= (bytevector-length rangeEnd) 0)))

; All currently-LIVE user keys in [K, rangeEnd), ascending — discovered by scanning
; the whole NS-KEY namespace and keeping the newest-visible (non-tombstone) version
; per user-key whose key falls in range.  (mvcc-get-latest does the per-key
; newest/tombstone resolution.)  A thin O(range) scan that .7 will refine.
(define (live-keys-in-range ctx K rangeEnd)
  ; collect distinct user-keys present in NS-KEY (any version), then filter to range
  ; + liveness.  We decode the user-key out of each NS-KEY composite key:
  ;   0x01 || u64be(lenK) || K || INV(rev16)
  (let ((rows (kv-scan ctx (mvcc-byte NS-KEY))))
    (let loop ((rs rows) (seen '()) (out '()))
      (if (null? rs)
          (reverse out)
          (let* ((fk   (caar rs))
                 (lenK (bytes->u64 fk 1))
                 (uk   (subbv fk 9 (+ 9 lenK))))
            (cond
              ((member uk seen) (loop (cdr rs) seen out))   ; already handled this key
              ((not (in-range? uk K rangeEnd)) (loop (cdr rs) (cons uk seen) out))
              ((mvcc-get-latest ctx uk)
               (loop (cdr rs) (cons uk seen) (cons uk out))) ; live -> include
              (else (loop (cdr rs) (cons uk seen) out))))))) ; tombstoned -> skip
  )

; uk in [K, rangeEnd) ?  Single-key (unset rangeEnd) => uk == K exactly.
(define (in-range? uk K rangeEnd)
  (if (range-end-unset? rangeEnd)
      (equal? uk K)
      (and (not (bv<? uk K)) (bv<? uk rangeEnd))))   ; K <= uk < rangeEnd

; lexicographic bytevector < (no builtin; same as the POC/store-smoke helper)
(define (bv<? a b)
  (let ((la (bytevector-length a)) (lb (bytevector-length b)))
    (let loop ((i 0))
      (cond ((= i la) (< la lb))
            ((= i lb) #f)
            ((< (bytevector-u8-ref a i) (bytevector-u8-ref b i)) #t)
            ((> (bytevector-u8-ref a i) (bytevector-u8-ref b i)) #f)
            (else (loop (+ i 1)))))))

(define (mvcc-delete-range! ctx K rangeEnd main sub)
  (let ((victims (live-keys-in-range ctx K rangeEnd)))
    (let loop ((vs victims) (s sub) (n 0))
      (if (null? vs)
          n
          (let* ((uk   (car vs))
                 (prev (mvcc-get-latest ctx uk))
                 (lease (if prev (kv-rec-lease prev) 0)))
            ; KEY-CF: a tombstone version (create_rev=0, version=0, no value)
            (kv-put! ctx (enc-key uk main s)
                     (kv-record-encode REC-TOMBSTONE 0 main 0 0 (make-bytevector 0 0)))
            ; REV-CF: a DELETE event
            (kv-put! ctx (enc-rev main s) (event-encode EV-DELETE uk (make-bytevector 0 0) main))
            ; LEASE index: drop the key's lease entry if it had one
            (if (and prev (not (= lease 0)))
                (kv-del! ctx (enc-lease lease uk)))
            (loop (cdr vs) (+ s 1) (+ n 1)))))))

; ---------------------------------------------------------------------------
; mvcc-range  (cw-u4a.7 — Range query API, ADR §3 read paths (a)/(b)/(c))
; ---------------------------------------------------------------------------
;
; NOTE: mvcc-range is a pure read over the ctx.  Linearizable gating (ReadIndex
; protocol) is the actor's existing job; the gRPC binding (.22) routes Range
; through that gate before calling here.
;
; (mvcc-range ctx key range-end opts) -> '(count . kvlist)
;                                      | (cons 'err-compacted compact-rev)
;
; opts is an assoc list; use (range-opt opts key default) to read with defaults.
;
; Supported opts keys (symbols):
;   revision       - read at this main rev (default = current-rev; 0 = current)
;   limit          - max keys returned (default = 0 = unlimited)
;   count-only     - #t => return count, no kvlist
;   keys-only      - #t => kvlist has keys but zero-length values
;   sort-order     - 'none | 'ascend | 'descend  (default 'none, natural scan order)
;   sort-target    - 'key | 'version | 'create | 'mod | 'value  (default 'key)
;   min-create-rev - min create_rev filter (0 = unset)
;   max-create-rev - max create_rev filter (0 = unset)
;   min-mod-rev    - min mod_rev filter (0 = unset)
;   max-mod-rev    - max mod_rev filter (0 = unset)
;
; A KeyValue result item is a pair (user-key-bv . record-vector), where
; record-vector is from kv-record-decode.  Callers use kv-rec-* accessors.

; ---- opts helper ----

(define (range-opt opts key default)
  (let ((cell (assq key opts)))
    (if cell (cdr cell) default)))

; ---- range-end semantics ----
;
;   #f or empty bv      => single-key point read (range-end-unset? already covers this)
;   #u8(0)              => "to end of keyspace" (all keys >= key)
;   key=#u8(0) + end=#u8(0) => ALL keys (etcd all-keys convention)
;
; The "all-keys" case is the intersection: key=zero-byte AND range-end=zero-byte.
; We canonicalise below so in-range? only needs to know whether we're in "half-open
; [key, range-end)" mode or the special full-range modes.

; Is range-end the sentinel meaning "to end of keyspace"?
; #u8(0) is a single zero byte, which has bytevector-length 1 and first byte 0.
(define (range-end-to-eof? range-end)
  (and range-end
       (= (bytevector-length range-end) 1)
       (= (bytevector-u8-ref range-end 0) 0)))

; uk in [key, range-end)?  Handles all-keys, to-eof, half-open, and single-key modes.
; uk is a plain user-key bytevector.
(define (range-in-range? uk key range-end)
  (cond
    ; single-key point read
    ((range-end-unset? range-end) (equal? uk key))
    ; all-keys: key=#u8(0) AND range-end=#u8(0)
    ((and (range-end-to-eof? range-end)
          (= (bytevector-length key) 1)
          (= (bytevector-u8-ref key 0) 0))
     #t)
    ; to-eof: range-end=#u8(0), key is anything
    ((range-end-to-eof? range-end) (not (bv<? uk key)))  ; uk >= key
    ; normal half-open [key, range-end)
    (else (and (not (bv<? uk key)) (bv<? uk range-end)))))

; prefix-range-end: increment the last non-0xFF byte to produce the exclusive
; upper bound for a prefix scan.  Returns #f if all bytes are 0xFF (overflow =>
; treat as to-eof, but that can't happen for any real key).
(define (prefix-range-end prefix)
  (let* ((n   (bytevector-length prefix))
         (out (subbv prefix 0 n)))
    (let loop ((i (- n 1)))
      (cond
        ((< i 0) #f)   ; all 0xFF — return #f (caller treats as to-eof)
        ((< (bytevector-u8-ref out i) #xFF)
         (bytevector-u8-set! out i (+ (bytevector-u8-ref out i) 1))
         ; zero out bytes after i
         (let clr ((j (+ i 1)))
           (if (< j n) (begin (bytevector-u8-set! out j 0) (clr (+ j 1)))))
         out)
        (else (loop (- i 1)))))))

; ---- decode user-key from a KEY-CF composite key ----
;   composite: 0x01 || u64be(lenK) || K || INV(rev16)
;   user-key lives at bytes [9, 9+lenK)
(define (key-cf-decode-user-key fk)
  (let ((lenK (bytes->u64 fk 1)))
    (subbv fk 9 (+ 9 lenK))))

; ---- decode the INV(rev16) back to the main revision ----
;   the inv16 bytes are at offset 9+lenK; un-invert then read the main u64be.
(define (key-cf-decode-main-rev fk)
  (let* ((lenK   (bytes->u64 fk 1))
         (inv-bv (subbv fk (+ 9 lenK) (+ 9 lenK 16)))
         (plain  (inv16 inv-bv)))      ; inv16 is its own inverse (XOR 0xFF)
    (bytes->u64 plain 0)))             ; high 8 bytes = main

; ---- in-memory sort of result pairs ((uk . rec) ...) ----

; Extract the sort key for a given sort-target from a (uk . rec) item.
(define (range-sort-key item target)
  (let ((uk  (car item))
        (rec (cdr item)))
    (cond
      ((eq? target 'key)     uk)
      ((eq? target 'version) (kv-rec-version    rec))
      ((eq? target 'create)  (kv-rec-create-rev rec))
      ((eq? target 'mod)     (kv-rec-mod-rev    rec))
      ((eq? target 'value)   (kv-rec-value      rec))
      (else uk))))

; compare two sort keys (either bytevectors or integers)
(define (sort-key<? a b)
  (if (bytevector? a)
      (bv<? a b)
      (< a b)))

; insertion sort (small result sets, avoids any stdlib sort dependency)
(define (isort lst less?)
  (define (insert x sorted)
    (cond ((null? sorted) (list x))
          ((less? x (car sorted)) (cons x sorted))
          (else (cons (car sorted) (insert x (cdr sorted))))))
  (let loop ((in lst) (out '()))
    (if (null? in) out
        (loop (cdr in) (insert (car in) out)))))

(define (range-sort items order target)
  (if (eq? order 'none)
      items
      (let* ((key-fn (lambda (item) (range-sort-key item target)))
             (asc?   (lambda (a b) (sort-key<? (key-fn a) (key-fn b))))
             (sorted (isort items asc?)))
        (if (eq? order 'descend) (reverse sorted) sorted))))

; ---- the main Range implementation ----

(define (mvcc-range ctx key range-end opts)
  ; -- read opts --
  (let* ((req-rev    (range-opt opts 'revision      0))
         (limit      (range-opt opts 'limit         0))
         (count-only (range-opt opts 'count-only    #f))
         (keys-only  (range-opt opts 'keys-only     #f))
         (sort-order (range-opt opts 'sort-order    'none))
         (sort-target (range-opt opts 'sort-target  'key))
         (min-cr     (range-opt opts 'min-create-rev 0))
         (max-cr     (range-opt opts 'max-create-rev 0))
         (min-mr     (range-opt opts 'min-mod-rev    0))
         (max-mr     (range-opt opts 'max-mod-rev    0))
         ; resolve the effective read revision
         (cur-rev    (mvcc-current-rev ctx))
         (at-rev     (if (= req-rev 0) cur-rev req-rev))
         ; compact-rev check
         (compact-rev (mvcc-compact-rev ctx)))
    ; ErrCompacted: if a non-zero explicit revision is below compact-rev
    (if (and (not (= req-rev 0)) (< req-rev compact-rev))
        (cons 'err-compacted compact-rev)
        ; -- scan NS-KEY namespace forward, group by user-key, pick visible version --
        (let ((rows (kv-scan ctx (mvcc-byte NS-KEY))))
          ; Iterate all KEY-CF rows in on-disk order.  Rows are ordered by
          ; user-key (ascending) then by INV(rev) (newest-first within key).
          ; We group consecutive rows sharing the same user-key.
          (let collect ((rs rows) (cur-uk #f) (cur-group '()) (results '()))
            (define (flush-group uk group)
              ; group is the accumulated (fk . vbv) pairs for uk, in scan order
              ; (newest→oldest).  Pick the visible version at at-rev.
              (if (null? group)
                  results
                  (let loop ((g group))
                    (if (null? g)
                        results                        ; no visible version
                        (let* ((row (car g))
                               (rec (kv-record-decode (cdr row)))
                               (mr  (kv-rec-mod-rev rec)))
                          (cond
                            ; skip versions newer than read revision
                            ((> mr at-rev) (loop (cdr g)))
                            ; first visible version is tombstone -> absent
                            ((kv-rec-tombstone? rec) results)
                            ; live -> add to results if in-range and passes rev filters
                            (else
                             (let ((cr (kv-rec-create-rev rec)))
                               (if (and (range-in-range? uk key range-end)
                                        (or (= min-cr 0) (>= cr min-cr))
                                        (or (= max-cr 0) (<= cr max-cr))
                                        (or (= min-mr 0) (>= mr min-mr))
                                        (or (= max-mr 0) (<= mr max-mr)))
                                   (cons (cons uk rec) results)
                                   results)))))))))
            (if (null? rs)
                ; no more rows: flush the final group then finalise
                (let* ((final-results (if cur-uk (flush-group cur-uk (reverse cur-group)) results))
                       (matched       (reverse final-results))
                       ; sort
                       (sorted        (range-sort matched sort-order sort-target))
                       ; total count BEFORE limit (etcd more/count semantics)
                       (total         (length sorted))
                       ; apply limit
                       (limited       (if (or (= limit 0) (>= limit total))
                                          sorted
                                          (let take ((lst sorted) (n limit) (acc '()))
                                            (if (or (null? lst) (= n 0))
                                                (reverse acc)
                                                (take (cdr lst) (- n 1) (cons (car lst) acc))))))
                       ; project
                       (projected     (if count-only
                                          '()
                                          (if keys-only
                                              ; blank the value in the record vector
                                              (map (lambda (item)
                                                     (let ((rec (cdr item)))
                                                       (cons (car item)
                                                             (vector
                                                              (vector-ref rec 0)  ; tag
                                                              (vector-ref rec 1)  ; create-rev
                                                              (vector-ref rec 2)  ; mod-rev
                                                              (vector-ref rec 3)  ; version
                                                              (vector-ref rec 4)  ; lease
                                                              (make-bytevector 0 0))))) ; blank value
                                                   limited)
                                              limited))))
                  (cons total projected))
                ; more rows: decode user-key and accumulate group
                (let* ((row (car rs))
                       (fk  (car row))
                       (uk  (key-cf-decode-user-key fk)))
                  (cond
                    ; same key as current group — accumulate
                    ((and cur-uk (equal? uk cur-uk))
                     (collect (cdr rs) cur-uk (cons row cur-group) results))
                    ; new key — flush previous group first, start new group
                    (else
                     (let ((new-results (if cur-uk (flush-group cur-uk (reverse cur-group)) results)))
                       (collect (cdr rs) uk (list row) new-results)))))))))))

; ---------------------------------------------------------------------------
; mvcc-digest-at / mvcc-snapshot-kvs  (cw-u4a.32 — Maintenance Status/Hash/HashKV/Snapshot)
; ---------------------------------------------------------------------------
;
; PURE reads over the VISIBLE keyspace at `at-rev` (0 => current), reusing mvcc-range's
; visible-version resolution so they fold over EXACTLY the live latest-≤-rev KeyValue of
; every key, in mvcc-range's canonical (NS-KEY on-disk) order.  Because every replica
; holds the byte-identical NS-KEY layout for the same committed state, the fold order —
; hence the hash — is IDENTICAL on every replica.  That cross-member determinism is the
; whole point of HashKV (a corruption/divergence check), so the exact algorithm need NOT
; match etcd's — only being deterministic across our replicas matters.

; FNV-1a folded into 32 bits, kept < 2^32 every step to dodge crabscheme's signed-i64
; bitwise wrap (same trick as grpc-kv's member-name->id / node-cluster's stable-id; the
; `*` is a true bignum so the 16777619 multiply never overflows before the modulo).
(define (mvcc-fnv1a-byte h b)
  (modulo (* (bitwise-xor h b) 16777619) 4294967296))
(define (mvcc-fnv1a-bytes h bv)
  (let ((n (bytevector-length bv)))
    (let loop ((i 0) (h h))
      (if (= i n) h (loop (+ i 1) (mvcc-fnv1a-byte h (bytevector-u8-ref bv i)))))))

; All live (user-key . record) at at-rev in canonical order (mvcc-range all-keys: key
; and range-end both = #u8(0)).  '() on an err-compacted result (an explicit at-rev below
; the compact floor) — Status/HashKV at rev 0 resolve to current-rev and never trip it.
(define (mvcc-live-kvs ctx at-rev)
  (let ((res (mvcc-range ctx (mvcc-byte 0) (mvcc-byte 0) (list (cons 'revision at-rev)))))
    (if (and (pair? res) (integer? (car res))) (cdr res) '())))

; (hash32 total-bytes count) over the live keyspace at at-rev.  The hash folds, per key
; in canonical order, key-bytes ‖ u64be(mod_rev) ‖ value-bytes; total-bytes sums
; keylen+valuelen — the LOGICAL db size Status reports (RocksDB exposes no page size).
(define (mvcc-digest-at ctx at-rev)
  (let loop ((kvs (mvcc-live-kvs ctx at-rev)) (h 2166136261) (sz 0) (n 0))
    (if (null? kvs)
        (list h sz n)
        (let* ((uk  (caar kvs))
               (rec (cdar kvs))
               (val (kv-rec-value rec))
               (h1  (mvcc-fnv1a-bytes h uk))
               (h2  (mvcc-fnv1a-bytes h1 (u64->bytes (kv-rec-mod-rev rec))))
               (h3  (mvcc-fnv1a-bytes h2 val)))
          (loop (cdr kvs) h3 (+ sz (bytevector-length uk) (bytevector-length val)) (+ n 1))))))

; Live (key . value) pairs at at-rev, canonical order — the LOGICAL snapshot payload.
(define (mvcc-snapshot-kvs ctx at-rev)
  (map (lambda (item) (cons (car item) (kv-rec-value (cdr item))))
       (mvcc-live-kvs ctx at-rev)))

; ---------------------------------------------------------------------------
; mvcc-watch-events  (cw-u4a.12 — Watch historical-replay query, ADR 0002 §3)
; ---------------------------------------------------------------------------
;
; A PURE READ over the REV-CF event log: returns every event in the revision
; range (start-rev, current] whose key falls in the watcher's [key, range-end)
; and that passes the watcher's NOPUT/NODELETE filters, IN STRICT REVISION ORDER.
; This is the historical half of a Watch (ADR 0002): replay before going live.
; The live replay->live handoff is built on this by .13/.14 (not here).
;
;   (mvcc-watch-events ctx start-rev key range-end filters)
;        -> a list of decoded event vectors (from event-decode), revision-ascending
;         | (cons 'err-compacted compact-rev)   when 0 < start-rev < compact-rev
;
;   start-rev   EXCLUSIVE lower bound (etcd start_revision is the first rev the
;               client has NOT seen; events with main-rev > start-rev replay).
;               start-rev = 0 means "current/future-only": no historical floor is
;               crossed, so it is NEVER ErrCompacted.
;   key/range-end : SAME semantics as mvcc-range (range-in-range?):
;               - #f / empty end      => single key (key == event-key exactly)
;               - #u8(0) end          => to-end-of-keyspace (and key=#u8(0) => all)
;               - otherwise           => half-open [key, range-end)
;   filters     : a list possibly containing 'noput and/or 'nodelete (symbols);
;               'noput drops PUT events, 'nodelete drops DELETE events.
;
; REV-CF keys are NS-REV || u64be(main) || u64be(sub), PLAIN ascending, so a scan
; of the NS-REV namespace already yields events in exactly Watch's stream order
; (oldest->newest, intra-Txn sub order preserved).  We read the main rev out of
; the on-disk key (byte 1) for the (start-rev, current] window test, then decode
; the event for key/filter matching.  A future perf pass can replace the
; whole-namespace scan with a bounded seek to NS-REV || rev16(start-rev+epsilon)
; (a thin kv-seek the layout already supports — ADR 0001 §3 (a)); the result set
; is identical, so this stays the source of truth for the replay contract.

(define (event-passes-filter? kind filters)
  (cond
    ((and (= kind EV-PUT)    (memq 'noput    filters)) #f)
    ((and (= kind EV-DELETE) (memq 'nodelete filters)) #f)
    (else #t)))

(define (mvcc-watch-events ctx start-rev key range-end filters)
  (let ((compact-rev (mvcc-compact-rev ctx))
        (cur-rev     (mvcc-current-rev ctx)))
    ; ErrCompacted: a from-revision watch whose floor is below compact-rev can no
    ; longer be served historically.  start-rev = 0 (current/future) never trips it.
    (if (and (> start-rev 0) (< start-rev compact-rev))
        (cons 'err-compacted compact-rev)
        (let ((rows (kv-scan ctx (mvcc-byte NS-REV))))   ; ascending = revision order
          (let loop ((rs rows) (out '()))
            (if (null? rs)
                (reverse out)
                (let* ((row  (car rs))
                       (fk   (car row))
                       ; fk = NS-REV(1) || u64be(main) || u64be(sub); main at byte 1
                       (main (bytes->u64 fk 1)))
                  (if (and (> main start-rev) (<= main cur-rev))
                      (let* ((ev   (event-decode (cdr row)))
                             (k    (ev-key ev))
                             (kind (ev-kind ev)))
                        (if (and (range-in-range? k key range-end)
                                 (event-passes-filter? kind filters))
                            (loop (cdr rs) (cons ev out))
                            (loop (cdr rs) out)))
                      (loop (cdr rs) out)))))))))

; ---------------------------------------------------------------------------
; mvcc-compact  (cw-u4a.8 — etcd-style MVCC history GC)
; ---------------------------------------------------------------------------
;
; (mvcc-compact ctx compactRev) -> (cons 'ok compactRev)
;                                | (cons 'err-compacted currentCompactRev)
;                                | (cons 'err-future-rev currentRev)
;
; Implements etcd compaction semantics exactly (ADR §compact):
;   1. Validate compactRev against current compact-rev and current-rev.
;   2. Persist the new compact-rev META key (activates ErrCompacted gate in
;      mvcc-range for any read at revision < compactRev).
;   3. KEY-CF GC: for each user-key group, keep the single latest-≤-compactRev
;      version ONLY IF it is a live (non-tombstone) record.  Delete all older
;      versions with mod_rev ≤ compactRev, and delete the latest-≤-compactRev
;      version too if it is a tombstone (a key deleted ≤ compactRev is fully gone).
;      Versions with mod_rev > compactRev are never touched.
;   4. REV-CF GC: delete every event entry with rev ≤ compactRev.
;   5. Does NOT bump current-rev (compaction is NOT a revision-creating operation).
;
; All deletes go through kv-del! so they batch with the WAL group-commit.
; Compaction is synchronous; incremental/background compaction is a future option.

(define (mvcc-compact ctx compactRev)
  (let ((cur-compact (mvcc-compact-rev ctx))
        (cur-rev     (mvcc-current-rev ctx)))
    (cond
      ; ErrCompacted: already compacted to >= compactRev
      ((<= compactRev cur-compact)
       (cons 'err-compacted cur-compact))
      ; future-rev: compactRev is beyond what has been written
      ((> compactRev cur-rev)
       (cons 'err-future-rev cur-rev))
      (else
       ; Step 2: persist the new compact-rev (activates mvcc-range's ErrCompacted gate)
       (kv-put! ctx META-COMPACT-REV (u64->bytes compactRev))
       ; Step 3: KEY-CF GC — scan NS-KEY grouped by user-key (newest-first per key)
       ; For each key, find the latest-≤-compactRev version and decide keep/delete.
       (let ((rows (kv-scan ctx (mvcc-byte NS-KEY))))
         ; Accumulate rows per user-key.  Scan order: ascending user-key, then
         ; newest-first within key (INV encoding).  So we process groups naturally.
         (let gc-loop ((rs rows) (cur-uk #f) (cur-group '()))
           (define (gc-flush-group group)
             ; group = list of (fullkey . value-bv) in newest-first scan order.
             ; Collect all versions with mod_rev <= compactRev.  Because we process
             ; the group newest-first and cons each matching row, the resulting
             ; at-or-below list ends up OLDEST-FIRST (the first-processed newest
             ; version is at the tail).  We reverse it to get newest-first so that
             ; the HEAD is the "latest-≤-compactRev" candidate.
             (let split ((g group) (at-or-below '()))
               (if (null? g)
                   ; Done splitting.
                   (if (null? at-or-below)
                       ; All versions are above compactRev — nothing to GC.
                       (values)
                       ; at-or-below is currently oldest-first (due to cons during
                       ; newest-first traversal); reverse to get newest-first.
                       (let* ((newest-first (reverse at-or-below))
                              (latest-row   (car newest-first))   ; highest mod_rev ≤ compactRev
                              (latest-rec   (kv-record-decode (cdr latest-row)))
                              (is-tomb      (kv-rec-tombstone? latest-rec))
                              ; Tombstone at/before compactRev: the key was deleted and
                              ; no live value needs to anchor reads at compactRev -> delete ALL.
                              ; Non-tombstone: keep latest (anchors reads at compactRev),
                              ; delete all older versions in at-or-below.
                              (to-delete    (if is-tomb
                                               newest-first          ; delete tombstone + older
                                               (cdr newest-first)))) ; keep latest, delete older
                         (for-each (lambda (row) (kv-del! ctx (car row))) to-delete)))
                   ; Partition this row by its mod_rev vs compactRev.
                   ; Versions with mod_rev > compactRev are skipped (left in place).
                   (let* ((row (car g))
                          (rec (kv-record-decode (cdr row)))
                          (mr  (kv-rec-mod-rev rec)))
                     (if (<= mr compactRev)
                         (split (cdr g) (cons row at-or-below))   ; accumulate GC candidate
                         (split (cdr g) at-or-below))))))         ; skip (above compactRev)
           (if (null? rs)
               ; Flush the last group
               (if cur-uk (gc-flush-group (reverse cur-group)) (values))
               ; Accumulate row into the current group, flushing on key change
               (let* ((row  (car rs))
                      (fk   (car row))
                      (uk   (key-cf-decode-user-key fk)))
                 (cond
                   ((and cur-uk (equal? uk cur-uk))
                    ; Same key — accumulate (rows arrive newest-first for this key)
                    (gc-loop (cdr rs) cur-uk (cons row cur-group)))
                   (else
                    ; New key — flush the previous group first
                    (if cur-uk (gc-flush-group (reverse cur-group)) (values))
                    (gc-loop (cdr rs) uk (list row))))))))
       ; Step 4: REV-CF GC — delete every event with rev <= compactRev.
       ; REV-CF keys are NS-REV || rev16 (plain ascending), so we scan the whole
       ; NS-REV namespace and delete entries whose embedded rev <= compactRev.
       (let ((rev-rows (kv-scan ctx (mvcc-byte NS-REV))))
         (for-each
          (lambda (row)
            (let* ((fk  (car row))
                   ; fk = 0x02 || u64be(main) || u64be(sub); main at byte 1
                   (rev (bytes->u64 fk 1)))
              (if (<= rev compactRev)
                  (kv-del! ctx fk))))
          rev-rows))
       ; Step 5: compaction does NOT bump current-rev
       (cons 'ok compactRev)))))

; ---------------------------------------------------------------------------
; mvcc-lease-grant!  (cw-u4a.17, ADR 0003 §1)
; ---------------------------------------------------------------------------
;
; Create the lease OBJECT: write the replicated lease-meta entry (granted TTL).
; A grant is NOT a keyspace mutation — it writes a side-namespace scalar, never a
; NS-KEY version — so it does NOT bump current-rev (§6).  Caller (mvcc-apply's
; "LEASE-GRANT" case) does not advance the revision either.
;
;   id = 0  ⇒ auto-assign next = (lease-id-seq) + 1, persist the counter (also a
;             non-bumping meta write), and use `next`.
;   id ≠ 0  ⇒ use the client-chosen id (etcd allows this).
;
; A duplicate grant (id already live) is rejected — the lease object already
; exists; re-granting would silently re-window/overwrite its TTL.
;
; Returns:  the assigned id (a positive integer)
;        |  (cons 'err-lease-exists id)   if id≠0 and a lease with that id is live.

(define (mvcc-lease-grant! ctx id ttl)
  (let ((eff-id (if (= id 0) (+ 1 (mvcc-lease-id-seq ctx)) id)))
    (cond
      ((mvcc-lease-exists? ctx eff-id) (cons 'err-lease-exists eff-id))
      (else
       ; bump the replicated id-seq counter only when we auto-assigned (so the next
       ; auto-id is unique forever); a client-chosen id never touches the counter.
       (if (= id 0) (kv-put! ctx META-LEASE-ID-SEQ (u64->bytes eff-id)))
       ; the lease-meta entry: 0x03 || u64be(id) -> u64be(granted_ttl)
       (kv-put! ctx (lease-meta-key eff-id) (u64->bytes ttl))
       eff-id))))

; ---------------------------------------------------------------------------
; mvcc-lease-revoke!  (cw-u4a.17, ADR 0003 §2)
; ---------------------------------------------------------------------------
;
; The LINEARIZABLE replicated revoke: enumerate the lease's currently-attached
; keys (mvcc-lease-keys) and TOMBSTONE each — writing a KEY-CF tombstone version
; AND a REV-CF DELETE event AND removing its 0x03||id||K index entry, i.e. the
; EXACT per-key delete body mvcc-delete-range! performs (so Watch sees a DELETE
; and reads after `main` see the key gone) — under successive sub-revisions
; main.0, main.1, ….  Then delete the lease-meta entry (the lease no longer
; exists).  Returns the count of keys deleted.
;
; This deletes EXACTLY the same keys at the same revision on every replica because
; it is one committed Raft entry applied identically everywhere (§2).  The caller
; (mvcc-apply's "LEASE-REVOKE" case) bumps current-rev once IFF count > 0 (a
; zero-key revoke is not a keyspace effect — §6 / cw-u4a.40), and only removes the
; (already-empty) meta entry.

(define (mvcc-lease-revoke! ctx id main)
  ; snapshot the attached keys BEFORE mutating (the per-key delete removes index
  ; entries as it goes; iterating a live scan would race that removal).
  (let ((victims (mvcc-lease-keys ctx id)))
    (let loop ((vs victims) (s 0) (n 0))
      (if (null? vs)
          (begin
            ; the lease itself is gone: drop the meta entry (so it can't be
            ; re-revoked and "does lease id exist?" is now #f on every replica).
            (kv-del! ctx (lease-meta-key id))
            n)
          (let* ((uk    (car vs))
                 (prev  (mvcc-get-latest ctx uk))
                 (lease (if prev (kv-rec-lease prev) 0)))
            ; KEY-CF: a tombstone version (create_rev=0, version=0, no value)
            (kv-put! ctx (enc-key uk main s)
                     (kv-record-encode REC-TOMBSTONE 0 main 0 0 (make-bytevector 0 0)))
            ; REV-CF: a DELETE event keyed by this op's own main.sub
            (kv-put! ctx (enc-rev main s) (event-encode EV-DELETE uk (make-bytevector 0 0) main))
            ; LEASE index: drop the key's lease entry (mirrors mvcc-delete-range!)
            (if (and prev (not (= lease 0)))
                (kv-del! ctx (enc-lease lease uk)))
            (loop (cdr vs) (+ s 1) (+ n 1)))))))

; ---------------------------------------------------------------------------
; AUTH-* apply support (cw-u4a.26 — NS-AUTH command vocabulary, ADR 0004 §2)
; ---------------------------------------------------------------------------
;
; The NS-AUTH storage primitives + the pure permission check live in src/auth.scm
; (cw-u4a.25); these two helpers are the only extra machinery mvcc-apply's AUTH-*
; cases need.  They reference auth.scm's perm-key/perm-rend accessors at CALL time,
; so a consumer that includes mvcc.scm but never drives an AUTH-* command (e.g. the
; grpc-kv handler, which proposes auth commands to the shard rather than applying
; them) does not need auth.scm loaded.  The shard actor + the mvcc-apply unit test —
; the real appliers — DO include src/auth.scm.

; remove every bytevector equal? to X from a list (REVOKE-ROLE / ROLE-DELETE strip).
(define (auth-remove-bv x lst)
  (let loop ((l lst) (acc '()))
    (cond ((null? l) (reverse acc))
          ((equal? (car l) x) (loop (cdr l) acc))
          (else (loop (cdr l) (cons (car l) acc))))))

; does permission P cover the SAME (key, range-end) as the command's (KEY, REND)?
; REND is the raw command bytevector (empty => single-key); normalise empty<->#f so
; an empty range-end matches a #f-stored single-key permission and vice versa.
(define (auth-perm-range=? p key rend)
  (and (equal? (perm-key p) key)
       (let ((pr (perm-rend p)))
         (equal? (if (= (bytevector-length pr) 0) #f pr)
                 (if (= (bytevector-length rend) 0) #f rend)))))

; drop any permission on (KEY, REND) from PERMS (REVOKE-PERM, and re-GRANT replace).
(define (auth-perms-drop-range perms key rend)
  (let loop ((l perms) (acc '()))
    (cond ((null? l) (reverse acc))
          ((auth-perm-range=? (car l) key rend) (loop (cdr l) acc))
          (else (loop (cdr l) (cons (car l) acc))))))

; ---------------------------------------------------------------------------
; mvcc-apply  (ADR §2 — one Raft entry = one Txn = one main revision)
; ---------------------------------------------------------------------------
;
; Parse a committed FLAT command (list of bytevectors, e.g. ("PUT" "city" "oslo"))
; and dispatch.  Bump current-rev (new main = prev+1, sub starts at 0) and persist
; the META current-rev in the SAME batch.  Supported commands:
;   ("PUT" K V)            put, no lease
;   ("PUT" K V leaseId)    put, attach lease (leaseId is a decimal-ASCII bytevector)
;   ("DEL" K)              delete single key
;   ("DEL" K rangeEnd)     delete range [K, rangeEnd)
;   ("COMPACT" rev)        compact history to rev (does NOT bump current-rev)
;   ("LEASE-GRANT" id ttl) create lease object (does NOT bump; cw-u4a.17)
;   ("LEASE-REVOKE" id)    revoke: tombstone all attached keys + drop meta, one
;                          revision IFF ≥1 key deleted (cw-u4a.17)
; Returns:
;   ("PUT" . newRev)                       ; the revision the put committed at
;   (cons 'err-lease-not-found leaseId)    ; PUT attached to a dead/unknown lease (no write)
;   ("DEL" newRev . deleted)               ; revision + count deleted
;   (cons 'ok compactRev)                  ; compaction succeeded
;   (cons 'err-compacted currentCompact)   ; already compacted to >= rev
;   (cons 'err-future-rev currentRev)      ; rev is beyond current-rev
;   (cons "LEASE-GRANT" id)                ; lease granted, assigned id
;   (cons 'err-lease-exists id)            ; grant of an already-live id
;   (cons "LEASE-REVOKE" (cons newRev n))  ; revoke: revision (or prev-rev if n=0) + count
; — a small s-expr usable as the client ack.

(define (cmd-op cmd) (utf8->string (car cmd)))

(define (mvcc-apply ctx cmd)
  (let* ((prev-rev (mvcc-current-rev ctx))
         (main     (+ prev-rev 1))
         (op       (cmd-op cmd)))
    (cond
      ((string=? op "PUT")
       (let* ((K     (list-ref cmd 1))
              (V     (list-ref cmd 2))
              (lease (if (>= (length cmd) 4)
                         (let ((l (bytes->int (list-ref cmd 3)))) (if l l 0))
                         0)))
         ; Put-to-dead-lease guard (ADR 0003 §4, etcd ErrLeaseNotFound): a PUT that
         ; attaches to a lease id (lease≠0) whose meta entry does NOT exist (never
         ; granted, or already revoked/expired) must FAIL and write nothing — so a
         ; key is never silently orphaned to a dead lease (it would leak, never
         ; revoked).  A single replicated kv-exists? on the apply path; every replica
         ; sees the same meta state, so the decision is identical everywhere.  The
         ; no-lease (lease=0) path is unchanged.
         (if (and (not (= lease 0)) (not (mvcc-lease-exists? ctx lease)))
             (cons 'err-lease-not-found lease)            ; no write, no rev bump
             (begin
               (mvcc-put! ctx K V lease main 0)
               (mvcc-set-current-rev! ctx main)
               (cons "PUT" main)))))
      ((string=? op "DEL")
       ; cw-u4a.40 / ADR-0001 §2: a DeleteRange removing ZERO live keys has no
       ; keyspace effect, so it must NOT advance current-rev (etcd parity) — exactly
       ; like a zero-key LEASE-REVOKE above.  Bump (and report) main iff ≥1 key was
       ; deleted; otherwise the revision is unchanged and the ack carries prev-rev.
       (let* ((K        (list-ref cmd 1))
              (rangeEnd (if (>= (length cmd) 3) (list-ref cmd 2) #f))
              (deleted  (mvcc-delete-range! ctx K rangeEnd main 0)))
         (if (> deleted 0)
             (begin (mvcc-set-current-rev! ctx main)
                    (cons "DEL" (cons main deleted)))
             (cons "DEL" (cons prev-rev 0)))))   ; no effect => no bump
      ((string=? op "COMPACT")
       ; COMPACT does NOT bump current-rev — call mvcc-compact directly.
       ; The rev argument is a decimal-ASCII bytevector (same convention as leaseId).
       (let ((rev (let ((l (bytes->int (list-ref cmd 1)))) (if l l 0))))
         (mvcc-compact ctx rev)))
      ((string=? op "LEASE-GRANT")
       ; ("LEASE-GRANT" id ttl): create the lease object.  A grant writes only the
       ; replicated lease-meta scalar (+ the auto-id counter) — NOT a keyspace
       ; version — so it does NOT bump current-rev (§6).  id/ttl are decimal-ASCII
       ; bytevectors (same convention as leaseId).  Returns ("LEASE-GRANT" . id)
       ; with the assigned id, or (cons 'err-lease-exists id) on a duplicate.
       (let* ((id  (let ((l (bytes->int (list-ref cmd 1)))) (if l l 0)))
              (ttl (let ((l (bytes->int (list-ref cmd 2)))) (if l l 0)))
              (res (mvcc-lease-grant! ctx id ttl)))
         (if (pair? res) res (cons "LEASE-GRANT" res))))
      ((string=? op "LEASE-REVOKE")
       ; ("LEASE-REVOKE" id): the replicated revoke — tombstone all attached keys +
       ; drop the meta entry.  Bumps current-rev exactly ONCE iff ≥1 key was deleted
       ; (a real keyspace effect); a zero-key revoke removes only the (empty) meta
       ; entry and does NOT advance the revision (§6 / cw-u4a.40).  Returns
       ; ("LEASE-REVOKE" newRev . count) — newRev is the bumped rev, or prev-rev when
       ; nothing was deleted (no bump).
       (let* ((id (let ((l (bytes->int (list-ref cmd 1)))) (if l l 0)))
              (n  (mvcc-lease-revoke! ctx id main)))
         (if (> n 0)
             (begin (mvcc-set-current-rev! ctx main)
                    (cons "LEASE-REVOKE" (cons main n)))
             (cons "LEASE-REVOKE" (cons prev-rev 0)))))   ; no effect => no bump
      ((string=? op "TXN")
       ; An etcd Txn: a single FLAT bytevector (node-send safe) carrying the whole
       ; compare/success/failure op tree.  txn-apply decodes it, peeks whether the
       ; executed branch mutates with effect, bumps main = prev+1 ONLY if so (a
       ; pure-read / zero-effect Txn does NOT advance the revision — cw-u4a.40),
       ; threads sub-revisions, and persists current-rev iff it bumped.
       ; Returns (succeeded? . responses).  Defined in src/txn.scm.
       (txn-apply ctx (list-ref cmd 1) prev-rev))

      ;; ===== Auth / RBAC mutations (cw-u4a.26, ADR 0004 §2) =====
      ;; Each writes NS-AUTH (via auth-put-*/auth-set-enabled! + auth.scm key
      ;; builders) and bumps the SEPARATE auth-revision (auth-bump-rev!) — NEVER
      ;; the keyspace current-rev (auth is not a keyspace effect, exactly like
      ;; LEASE-GRANT).  One committed Raft entry applied identically on every
      ;; replica, so auth state is replicated + survives leader change for free.
      ;; Success ack: (cons "AUTH-OK" new-auth-rev); failures: (cons 'err-* detail)
      ;; which grpc-kv maps to the etcd-faithful gRPC status (.27 error table).
      ((string=? op "AUTH-ENABLE")
       ; etcd guard: a user named "root" holding the "root" role must exist.
       (let ((ru (auth-get-user ctx AUTH-ROOT-ROLE)))   ; "root" user (name == role bytes)
         (cond
           ((not ru) (cons 'err-root-user-not-exist 0))
           ((not (auth-has-root-role? (auth-user-roles ru))) (cons 'err-root-role-not-exist 0))
           (else (auth-set-enabled! ctx #t) (cons "AUTH-OK" (auth-bump-rev! ctx))))))
      ((string=? op "AUTH-DISABLE")
       (auth-set-enabled! ctx #f)
       (cons "AUTH-OK" (auth-bump-rev! ctx)))
      ((string=? op "AUTH-USER-ADD")             ; ("AUTH-USER-ADD" name hash)
       (let ((name (list-ref cmd 1)) (hash (list-ref cmd 2)))
         (if (auth-get-user ctx name)
             (cons 'err-user-exists name)
             (begin (auth-put-user! ctx name hash '())
                    (cons "AUTH-OK" (auth-bump-rev! ctx))))))
      ((string=? op "AUTH-USER-DELETE")          ; ("AUTH-USER-DELETE" name)
       (let ((name (list-ref cmd 1)))
         (if (not (auth-get-user ctx name))
             (cons 'err-user-not-found name)
             (begin (kv-del! ctx (auth-user-key name))
                    (cons "AUTH-OK" (auth-bump-rev! ctx))))))
      ((string=? op "AUTH-USER-CHPASS")          ; ("AUTH-USER-CHPASS" name hash)
       (let ((name (list-ref cmd 1)) (hash (list-ref cmd 2)))
         (let ((u (auth-get-user ctx name)))
           (if (not u)
               (cons 'err-user-not-found name)
               (begin (auth-put-user! ctx name hash (auth-user-roles u))
                      (cons "AUTH-OK" (auth-bump-rev! ctx)))))))
      ((string=? op "AUTH-USER-GRANT-ROLE")      ; ("AUTH-USER-GRANT-ROLE" name role)
       (let ((name (list-ref cmd 1)) (role (list-ref cmd 2)))
         (let ((u (auth-get-user ctx name)))
           (if (not u)
               (cons 'err-user-not-found name)
               (let ((roles (auth-user-roles u)))
                 (auth-put-user! ctx name (auth-user-hash u)
                                 (if (member role roles) roles (append roles (list role))))
                 (cons "AUTH-OK" (auth-bump-rev! ctx)))))))
      ((string=? op "AUTH-USER-REVOKE-ROLE")     ; ("AUTH-USER-REVOKE-ROLE" name role)
       (let ((name (list-ref cmd 1)) (role (list-ref cmd 2)))
         (let ((u (auth-get-user ctx name)))
           (if (not u)
               (cons 'err-user-not-found name)
               (begin (auth-put-user! ctx name (auth-user-hash u)
                                      (auth-remove-bv role (auth-user-roles u)))
                      (cons "AUTH-OK" (auth-bump-rev! ctx)))))))
      ((string=? op "AUTH-ROLE-ADD")             ; ("AUTH-ROLE-ADD" name)
       (let ((name (list-ref cmd 1)))
         (if (auth-get-role ctx name)
             (cons 'err-role-exists name)
             (begin (auth-put-role! ctx name '())
                    (cons "AUTH-OK" (auth-bump-rev! ctx))))))
      ((string=? op "AUTH-ROLE-DELETE")          ; ("AUTH-ROLE-DELETE" name)
       (let ((role (list-ref cmd 1)))
         (if (not (auth-get-role ctx role))
             (cons 'err-role-not-found role)
             (begin
               ; etcd semantics: strip the deleted role from EVERY user.
               (for-each
                (lambda (uname)
                  (let ((u (auth-get-user ctx uname)))
                    (if u (auth-put-user! ctx uname (auth-user-hash u)
                                          (auth-remove-bv role (auth-user-roles u))))))
                (auth-all-users ctx))
               (kv-del! ctx (auth-role-key role))
               (cons "AUTH-OK" (auth-bump-rev! ctx))))))
      ((string=? op "AUTH-ROLE-GRANT-PERM")      ; ("AUTH-ROLE-GRANT-PERM" role ptype key rend)
       (let ((role  (list-ref cmd 1))
             (ptype (bytes->int (list-ref cmd 2)))
             (key   (list-ref cmd 3))
             (rend  (list-ref cmd 4)))
         ; auth-get-role returns #f ONLY for an absent role ('() = exists, no perms).
         (let ((perms (auth-get-role ctx role)))
           (if (not perms)
               (cons 'err-role-not-found role)
               (let* ((np (auth-make-perm (if ptype ptype 0) key
                                          (if (= (bytevector-length rend) 0) #f rend)))
                      ; replace any existing perm on the same (key,rend), then append.
                      (newperms (append (auth-perms-drop-range perms key rend) (list np))))
                 (auth-put-role! ctx role newperms)
                 (cons "AUTH-OK" (auth-bump-rev! ctx)))))))
      ((string=? op "AUTH-ROLE-REVOKE-PERM")     ; ("AUTH-ROLE-REVOKE-PERM" role key rend)
       (let ((role (list-ref cmd 1)) (key (list-ref cmd 2)) (rend (list-ref cmd 3)))
         (let ((perms (auth-get-role ctx role)))
           (if (not perms)
               (cons 'err-role-not-found role)
               (begin (auth-put-role! ctx role (auth-perms-drop-range perms key rend))
                      (cons "AUTH-OK" (auth-bump-rev! ctx)))))))

      (else
       (error "mvcc-apply: unknown command" op)))))

; ---------------------------------------------------------------------------
; etcd Txn (If/Then/Else) — cw-u4a.10
; ---------------------------------------------------------------------------
;
; The Txn layer (compares + success/failure RequestOp lists, flat node-send
; serialization, atomic compare-then-apply at this MVCC seam) lives in src/txn.scm
; and is included HERE — after every mvcc helper it builds on (mvcc-get-latest /
; mvcc-put! / mvcc-delete-range! / mvcc-range / live-keys-in-range / kv-rec-* /
; bv<? / mvcc-set-current-rev! / mvcc-byte).  mvcc-apply's "TXN" case dispatches to
; txn-apply, defined there.  Including it from mvcc.scm means every consumer of the
; apply-fn (shard-actor + the unit tests) gets the Txn path with zero extra wiring.

(include "src/txn.scm")
