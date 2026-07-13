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
;   NS-KEY   0x01  key-ordered store   0x01 || esc(K) || TERM(0x00 0x00) || INV(rev16) -> KeyValue record
;            (cw-zf7 re-key: esc()/TERM byte-order-preserve K, so on-disk order is
;            user-key ascending — was u64be(lenK)||K, which sorted length-major.)
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
; NS-AUTH = 0x04 (src/auth.scm)
(define NS-ALARM #x05)   ; alarm set (cw-u4a.42): u64be(memberID) || atype -> ()

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
; cw-65x H5 NOTE: the native (bytevector-not) variant needs a binary with the
; 566e51b builtins; the validated July-12 fleet binary predates them and an
; undefined global fails at LOAD, so ship the interpreted loop until cw-c8b
; (binary regression) is resolved.
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
; KEY-CF key:  NS-KEY || esc(K) || TERM(0x00 0x00) || INV(rev16)      (cw-zf7)
; ---------------------------------------------------------------------------
;
; cw-zf7: the ORIGINAL layout was NS-KEY || u64be(lenK) || K || INV(rev16).
; That length prefix makes on-disk order LENGTH-MAJOR (all 3-byte keys sort
; before all 4-byte keys regardless of content), so a user-key RANGE like
; "p/v1." can't be expressed as a row-bound scan — mvcc-range's general path
; had to kv-scan the ENTIRE NS-KEY namespace (every key's every version) for
; any non-point read.  Measured: an etcd LIST at 2000 keys took 11.7s and a
; kube control plane pointed at it collapsed.
;
; The fix is a memcmp/order-preserving escape instead of a length prefix, so
; the ON-DISK byte order is exactly user-key ascending (then newest-first via
; INV(rev) within a key's version group), and a user-key range [K1,K2) is a
; plain row-bound kv-scan-range.  Escape: each literal 0x00 byte in K becomes
; 0x00 0xFF; the escaped key is TERMINATED by 0x00 0x00.  Since an escaped
; null is always followed by 0xFF and the terminator is always followed by
; 0x00, and 0x00 < 0xFF, the terminator byte-pair can never be confused with
; an escaped null mid-key, and byte-lexicographic order over the escaped form
; equals byte-lexicographic order over the raw key (etcd keys may contain any
; byte, including 0x00, hence the escape rather than assuming printable keys).
;
; ponytail: NO data migration — pre-1.0 throwaway stores, fresh DBs only. A
; DB written under the old length-prefixed layout is NOT readable under this
; one (and vice versa); there is no on-disk version tag to detect/migrate.
; ---------------------------------------------------------------------------

; escape K: each 0x00 byte -> 0x00 0xFF.  Preallocate worst case (2x) and slice
; back to the actual length instead of an O(n^2) incremental bytevector-append.
; cw-65x H5 NOTE: native (bytevector-nul-escape) awaits a post-cw-c8b binary
; (same reason as inv16 above).
(define (key-cf-escape K)
  (let* ((n (bytevector-length K))
         (buf (make-bytevector (* 2 n) 0)))
    (let loop ((i 0) (j 0))
      (if (= i n)
          (subbv buf 0 j)
          (let ((byte (bytevector-u8-ref K i)))
            (if (= byte 0)
                (begin (bytevector-u8-set! buf j 0)
                       (bytevector-u8-set! buf (+ j 1) #xFF)
                       (loop (+ i 1) (+ j 2)))
                (begin (bytevector-u8-set! buf j byte)
                       (loop (+ i 1) (+ j 1)))))))))

; un-escape (0x00 followed by anything, always 0xFF inside an escaped run,
; collapses back to a single 0x00) is now done natively — see
; bytevector-nul-unescape at key-cf-decode-user-key (cw-71k, G2).

(define (key-cf-prefix K)               ; NS-KEY || esc(K) || TERM  (scan prefix; sorts ascending)
  (bytevector-append (mvcc-byte NS-KEY)
                     (key-cf-escape K)
                     (make-bytevector 2 0)))     ; #u8(0 0) terminator

(define (enc-key K main sub)            ; full KEY-CF key
  (bytevector-append (key-cf-prefix K)
                     (inv16 (rev->16 main sub))))

; find the byte offset of the 0x00 0x00 terminator in a full/prefix KEY-CF key,
; starting the scan at `start` (byte 1, right after the NS-KEY tag).  A 0x00
; byte NOT followed by another 0x00 is an escaped null (0x00 0xFF) — skip 2 and
; keep scanning; a 0x00 followed by 0x00 IS the terminator.
(define (key-cf-find-term fk start)
  (let loop ((i start))
    (if (= (bytevector-u8-ref fk i) 0)
        (if (= (bytevector-u8-ref fk (+ i 1)) 0)
            i
            (loop (+ i 2)))
        (loop (+ i 1)))))

; prefix-range-end: increment the last non-0xFF byte to produce the exclusive
; upper bound for a prefix scan.  Returns #f if all bytes are 0xFF (overflow =>
; treat as to-eof).  (Forward-declared here; also used below by range bounds —
; moved up from its original spot further down in this file so key-cf-row-bounds
; can call it.)
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

; row bounds [start, end) over KEY-CF for a mvcc-range request (K, range-end) —
; cw-zf7: this is what makes a bounded user-key range a bounded ROW scan instead
; of the whole-namespace kv-scan the old length-major layout was stuck with.
;   range-end unset (single key)     -> [prefix(K), prefix(K)+eps)
;   range-end = to-eof sentinel      -> [prefix(K)-or-NS-start, end-of-NS-KEY)
;   otherwise (half-open range)      -> [prefix(K), prefix(range-end))
; NS-KEY's next namespace is NS-REV, so "end of NS-KEY" = (mvcc-byte (+ NS-KEY 1)).
(define (key-cf-row-bounds key range-end)
  (let ((ns-end (mvcc-byte (+ NS-KEY 1))))
    (cond
      ((range-end-unset? range-end)
       (let ((p (key-cf-prefix key)))
         (cons p (or (prefix-range-end p) ns-end))))
      ((and (range-end-to-eof? range-end)
            (= (bytevector-length key) 1)
            (= (bytevector-u8-ref key 0) 0))
       (cons (mvcc-byte NS-KEY) ns-end))                          ; all-keys
      ((range-end-to-eof? range-end)
       (cons (key-cf-prefix key) ns-end))                         ; to-eof
      (else
       (cons (key-cf-prefix key) (key-cf-prefix range-end))))))   ; half-open [K, rangeEnd)

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
; Alarms (cw-u4a.42) — NOSPACE/CORRUPT, Raft-REPLICATED so every member agrees and
; they survive restart (etcd-faithful; replaces the old leader-local in-memory set).
; KEY: NS-ALARM || u64be(memberID) || atype-byte.  VALUE: empty (presence = active).
; ALARM-SET / ALARM-DISARM apply identically on every replica via the committed Raft
; command and write ONLY this namespace — they do NOT bump current-rev (an alarm is
; not a keyspace revision), exactly like the AUTH / LEASE-GRANT meta writes.
; ---------------------------------------------------------------------------
(define (alarm-key mid atype)
  (bytevector-append (mvcc-byte NS-ALARM) (u64->bytes mid) (mvcc-byte atype)))
(define (mvcc-alarm-set! ctx mid atype)
  (kv-put! ctx (alarm-key mid atype) (make-bytevector 0 0)))
(define (mvcc-alarm-disarm! ctx mid atype)
  (kv-del! ctx (alarm-key mid atype)))
; all active alarms as a list of (memberID . alarmType), scanning NS-ALARM.
(define (mvcc-alarm-list ctx)
  (let loop ((rows (kv-scan ctx (mvcc-byte NS-ALARM))) (out '()))
    (if (null? rows)
        (reverse out)
        (let ((k (caar rows)))      ; NS-ALARM(1) || u64be(mid)(8) || atype(1) = 10 bytes
          (if (= (bytevector-length k) 10)
              (loop (cdr rows) (cons (cons (bytes->u64 k 1) (bytevector-u8-ref k 9)) out))
              (loop (cdr rows) out))))))

; ---------------------------------------------------------------------------
; META keys
; ---------------------------------------------------------------------------

(define (meta-key name) (bytevector-append (mvcc-byte NS-META) (string->utf8 name)))
(define META-CURRENT-REV (meta-key "current-rev"))
(define META-COMPACT-REV (meta-key "compact-rev"))
(define META-LEASE-ID-SEQ (meta-key "lease-id-seq"))   ; lease auto-id counter (cw-u4a.17)
(define META-GLOBAL-REV   (meta-key "global-rev"))     ; cw-kp0: rev-authority's granted high (separate from current-rev)

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

; cw-71k (G2): raw peeks on the ENCODED record bytes.  mvcc-range/compaction
; inspect EVERY version row of a key but only the visible winner needs the
; full decode — kv-record-decode subbv's the whole value (multi-KB pod
; objects), so decoding skipped versions dominated per-row cost at real
; version depth.  A skipped version now costs two native refs instead.
(define (kv-raw-tombstone? b) (= (bytevector-u8-ref b 0) REC-TOMBSTONE))
(define (kv-raw-mod-rev    b) (bytes->u64 b 9))

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
; EXP7 (cw-u90): cache current-rev in the ctx. The shard is the SOLE writer of
; current-rev and (with serial apply, the committed default) reads + writes are
; serialized through one actor mailbox, so the in-memory value can never go stale
; vs RocksDB. This removes one RocksDB point-read per applied PUT (mvcc-current-rev
; is read per stamp), the hottest apply-path read after EXP6 batched the commits.
; Lazy-init from RocksDB (-1 sentinel) so a restarted shard picks up the persisted
; value on its first read.
(define (mvcc-current-rev ctx)
  (let ((c (shard-ctx-crev ctx)))
    (if (>= c 0) c
        (let* ((b (kv-get ctx META-CURRENT-REV))
               (v (if (and b (>= (bytevector-length b) 8)) (bytes->u64 b 0) 0)))
          (set-shard-ctx-crev! ctx v)
          v))))

(define (mvcc-set-current-rev! ctx main)
  (set-shard-ctx-crev! ctx main)
  (kv-put! ctx META-CURRENT-REV (u64->bytes main)))

; cw-kp0 phase 2: the global revision authority's counter (highest revision GRANTED
; across all shard groups). Separate from current-rev — granting does not commit a
; write, it reserves revisions. Only the designated authority shard maintains it.
(define (mvcc-global-rev ctx)
  (let ((b (kv-get ctx META-GLOBAL-REV)))
    (if (and b (>= (bytevector-length b) 8)) (bytes->u64 b 0) 0)))
(define (mvcc-set-global-rev! ctx v)
  (kv-put! ctx META-GLOBAL-REV (u64->bytes v)))

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

; cw-65x: latest-version cache helpers. Keys are utf8 strings (guarded: a
; non-UTF-8 user key just bypasses the cache — k8s registry keys are text).
(define (latest-cache-key K)
  (guard (e (#t #f)) (utf8->string K)))
(define (latest-cache-evict! ctx K)
  (let ((c (shard-ctx-latest-cache ctx)))
    (if c (let ((sk (latest-cache-key K)))
            (if sk (hashtable-delete! c sk))))))
(define (mvcc-enable-latest-cache! ctx)
  (set-shard-ctx-latest-cache! ctx (make-hashtable string-hash string=?)))
(define (mvcc-latest-cache-invalidate! ctx)
  (if (shard-ctx-latest-cache ctx) (mvcc-enable-latest-cache! ctx)))

(define (mvcc-put! ctx K V lease main sub)
  (let* ((cache (shard-ctx-latest-cache ctx))
         (ck (and cache (latest-cache-key K)))
         (hit (and ck (hashtable-ref cache ck #f)))
         (prev (if hit #f (mvcc-get-latest ctx K)))  ; newest visible (live) record, or #f
         (create-rev (cond (hit (vector-ref hit 0)) (prev (kv-rec-create-rev prev)) (else main)))
         (version    (cond (hit (+ 1 (vector-ref hit 1))) (prev (+ 1 (kv-rec-version prev))) (else 1)))
         (prev-lease (cond (hit (vector-ref hit 2)) (prev (kv-rec-lease prev)) (else 0)))
         (prev-vlen  (cond (hit (vector-ref hit 3)) (prev (bytevector-length (kv-rec-value prev))) (else #f))))
    (if ck (hashtable-set! cache ck (vector create-rev version lease (bytevector-length V))))
    ; cw-xq9: incremental live stats — replace = value-size delta; create = key+value +1
    (if prev-vlen
        (mvcc-live-stats-add! ctx (- (bytevector-length V) prev-vlen) 0)
        (mvcc-live-stats-add! ctx (+ (bytevector-length K) (bytevector-length V)) 1))
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

; ---------------------------------------------------------------------------
; NATIVE fused range (cw-2au LIST-scan wall): store-range-latest-pb walks the
; KEY-CF in Rust and returns count/more plus the CONCATENATED field-2-tagged
; `repeated KeyValue` protobuf bytes of a RangeResponse — no per-row Scheme
; tuples, no cross-actor row copies. Needs a post-cw-c8b binary (undefined
; globals fail at LOAD only when EVALUATED, so this whole path is gated at
; runtime on CWS_NATIVE_RANGE=1; do not set that env on the July-12 binary).
; Semantics mirror mvcc-range's latest-<=at-rev path (tombstone = absent,
; count ignores limit). Caller must have checked eligibility:
; multi-key range, sort none/key-ascending, no create/mod-rev filters.
; ---------------------------------------------------------------------------
(define (mvcc-range-pb ctx key range-end opts)
  ; -> (err-compacted . floor) | (count more pb-bytes)
  (let* ((req-rev     (range-opt opts 'revision 0))
         (cur-rev     (mvcc-current-rev ctx))
         (at-rev      (if (= req-rev 0) cur-rev req-rev))
         (compact-rev (mvcc-compact-rev ctx)))
    (if (and (not (= req-rev 0)) (< req-rev compact-rev))
        (cons 'err-compacted compact-rev)
        (let* ((start (bytevector-append (mvcc-byte NS-KEY) (key-cf-escape key)))
               ; etcd: range_end = #vu8(0) means "from key to end of keyspace"
               (end (if (and (= (bytevector-length range-end) 1)
                             (= (bytevector-u8-ref range-end 0) 0))
                        (mvcc-byte (+ NS-KEY 1))
                        (bytevector-append (mvcc-byte NS-KEY) (key-cf-escape range-end)))))
          (store-range-latest-pb (shard-ctx-handle ctx) (shard-ctx-cf ctx)
                                 start end at-rev
                                 (range-opt opts 'limit 0)
                                 (range-opt opts 'keys-only #f)
                                 (range-opt opts 'count-only #f))))))

; All currently-LIVE user keys in [K, rangeEnd), ascending — discovered by scanning
; the whole NS-KEY namespace and keeping the newest-visible (non-tombstone) version
; per user-key whose key falls in range.  (mvcc-get-latest does the per-key
; newest/tombstone resolution.)  A thin O(range) scan that .7 will refine.
(define (live-keys-in-range ctx K rangeEnd)
  ; EXP9 (cw-aka): SINGLE-KEY fast path, symmetric to EXP8's read path. A single-key
  ; DELETE (rangeEnd unset) targets exactly K — resolve it with one mvcc-get-latest
  ; point seek (O(log n)) instead of kv-scanning the WHOLE NS-KEY namespace
  ; (O(total keys)). A live K => (list K); a tombstoned/absent K => '(). This is
  ; etcd's common single-key Delete.
  (if (range-end-unset? rangeEnd)
      (if (mvcc-get-latest ctx K) (list K) '())
  ; cw-zf7: true ranges — the KEY-CF re-key (see the block comment above enc-key)
  ; sorts rows user-key-ascending, so [K, rangeEnd) is a bounded kv-scan-range,
  ; not a whole-namespace scan.  We decode the user-key out of each NS-KEY
  ; composite key: 0x01 || esc(K) || TERM(0x00 0x00) || INV(rev16).
  ; EXP9 (cw-aka): O(n) consecutive-grouping instead of the old O(n^2) `(member uk
  ; seen)` list dedup. KEY-CF sorts by (K, INV-rev), so ALL versions of a
  ; user-key are CONSECUTIVE — track the previous uk to skip its older versions
  ; without a membership scan. The old quadratic dedup over ~150k keys (a range
  ; DeleteRange, e.g. check perf cleanup) was ~n^2 ops and timed out; this is linear.
  (let* ((bounds (key-cf-row-bounds K rangeEnd))
         (rows   (kv-scan-range ctx (car bounds) (cdr bounds))))
    (let loop ((rs rows) (prev-uk #f) (out '()))
      (if (null? rs)
          (reverse out)
          (let* ((fk   (caar rs))
                 (uk   (key-cf-decode-user-key fk)))
            (cond
              ((and prev-uk (equal? uk prev-uk)) (loop (cdr rs) prev-uk out)) ; older version of same key
              ((not (in-range? uk K rangeEnd)) (loop (cdr rs) uk out))
              ; EXP9: the FIRST row of a key group is its NEWEST version (KEY-CF sorts
              ; versions newest-first via INV-rev), so decode it inline instead of a
              ; redundant mvcc-get-latest point seek per key (~n extra seeks on a bulk
              ; range delete). Live => include; tombstone => key already absent, skip.
              ((not (kv-raw-tombstone? (cdar rs)))   ; cw-71k: tag peek, no full decode
               (loop (cdr rs) uk (cons uk out)))                              ; live -> include
              (else (loop (cdr rs) uk out)))))))                              ; tombstoned -> skip
  ))

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
            ; cw-xq9: incremental live stats — a live key leaves the keyspace
            (if prev
                (mvcc-live-stats-add!
                 ctx (- (+ (bytevector-length uk)
                           (bytevector-length (kv-rec-value prev)))) -1))
            ; KEY-CF: a tombstone version (create_rev=0, version=0, no value)
            (kv-put! ctx (enc-key uk main s)
                     (kv-record-encode REC-TOMBSTONE 0 main 0 0 (make-bytevector 0 0)))
            (latest-cache-evict! ctx uk)  ; cw-65x: tombstone kills the live entry
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

; (prefix-range-end is defined earlier, alongside key-cf-row-bounds, since
;  key-cf-row-bounds itself calls it.)

; ---- decode user-key from a KEY-CF composite key ----
;   composite: 0x01 || esc(K) || TERM(0x00 0x00) || INV(rev16)
;   the escaped-key run is bytes [1, term); un-escape to recover K.
;
; cw-71k (G2): this is called once per SCANNED ROW in mvcc-range/DeleteRange
; (every version in a key's group, not just the winner) — profiled at
; ~0.55ms/row with key-cf-find-term (interpreted byte loop) + subbv (slice
; alloc) + key-cf-unescape (interpreted byte loop) as three separate passes.
; bytevector-nul-unescape fuses find-term + unescape into ONE native pass,
; same playbook as cw-xq9's subbv fix.
(define (key-cf-decode-user-key fk)
  (bytevector-nul-unescape fk 1))

; ---- decode the INV(rev16) back to the main revision ----
;   the inv16 bytes are the 16 bytes right after the TERM(0x00 0x00) pair.
(define (key-cf-decode-main-rev fk)
  (let* ((term    (key-cf-find-term fk 1))
         (inv-off (+ term 2))
         (inv-bv  (subbv fk inv-off (+ inv-off 16)))
         (plain   (inv16 inv-bv)))    ; inv16 is its own inverse (XOR 0xFF)
    (bytes->u64 plain 0)))            ; high 8 bytes = main

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

; merge sort (apiserver LISTs run 500+ items; insertion sort was O(n^2) in the
; interpreter's hot read path)
; cw-m9c (G1): `merge` must be TAIL-recursive — a naive `(cons (car a) (merge
; (cdr a) b))` recurses to depth O(n) (the top-level merge of two N/2 lists
; unwinds N/2 stack frames), which blew the stack on a dedicated actor thread
; at ~5-6k rows (a full-keyspace LIST always sorts, even for sort-order
; 'none — see the comment below). Accumulate + reverse instead: O(1) added
; stack depth regardless of N.
(define (isort lst less?)
  (define (merge a b)
    (let loop ((a a) (b b) (acc '()))
      (cond ((null? a) (append-reverse acc b))
            ((null? b) (append-reverse acc a))
            ((less? (car b) (car a)) (loop a (cdr b) (cons (car b) acc)))
            (else (loop (cdr a) b (cons (car a) acc))))))
  (define (split lst)
    (let loop ((slow lst) (fast lst) (acc '()))
      (if (or (null? fast) (null? (cdr fast)))
          (cons (reverse acc) slow)
          (loop (cdr slow) (cddr fast) (cons (car slow) acc)))))
  (if (or (null? lst) (null? (cdr lst)))
      lst
      (let ((halves (split lst)))
        (merge (isort (car halves) less?) (isort (cdr halves) less?)))))

; etcd's Range contract: results are KEY-ASCENDING even with sort NONE.  Before
; cw-zf7's re-key the on-disk KEY-CF layout was length-prefixed (raw scan order
; length-major, not key order — k8s apiserver pagination/digest checks broke on
; this, found on the k3s run), so this re-sort was load-bearing for correctness.
; The re-key makes raw scan order ALREADY key-ascending, but this stays: it's
; O(n log n) on the now-small per-request RESULT set (not the whole namespace),
; and keeping one code path for 'none is simpler than proving every mvcc-range
; caller can rely on scan order (in-flight compaction/GC ordering, etc).
(define (range-sort items order target)
  (if (eq? order 'none)
      (isort items (lambda (a b) (bv<? (car a) (car b))))
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
        ; EXP8 (cw-709): SINGLE-KEY fast path. etcd's most common read is a point
        ; GET (range-end unset). This uses mvcc-get-latest (one O(log n) seek),
        ; identical visibility semantics (newest version <= at-rev, tombstone =>
        ; absent) — cheaper than even the bounded group scan below. Only taken
        ; when NO create/mod-rev filters apply (those need the group scan);
        ; otherwise fall through to the general path (cw-zf7: now row-bounded,
        ; see key-cf-row-bounds — no longer a whole-namespace scan either way).
        (if (and (range-end-unset? range-end)
                 (= min-cr 0) (= max-cr 0) (= min-mr 0) (= max-mr 0))
            (let ((rec (if (= req-rev 0)
                           (mvcc-get-latest ctx key)
                           (mvcc-get-latest ctx key at-rev))))
              (if (not rec)
                  (cons 0 '())
                  (cons 1 (if count-only
                              '()
                              (list (cons key
                                          (if keys-only
                                              (vector (vector-ref rec 0)   ; tag
                                                      (vector-ref rec 1)   ; create-rev
                                                      (vector-ref rec 2)   ; mod-rev
                                                      (vector-ref rec 3)   ; version
                                                      (vector-ref rec 4)   ; lease
                                                      (make-bytevector 0 0)) ; blank value
                                              rec)))))))
        ; -- scan the [key,range-end) row bounds forward, group by user-key, pick
        ; visible version.  cw-zf7: key-cf-row-bounds turns the request's
        ; (key, range-end) into a KEY-CF row range, so this is a BOUNDED
        ; kv-scan-range over exactly the requested keys' version groups, not the
        ; whole NS-KEY namespace (the pre-re-key general path had no choice but
        ; to scan everything, since a length-major layout can't express a
        ; user-key range as row bounds).
        (let* ((bounds (key-cf-row-bounds key range-end))
               (rows   (kv-scan-range ctx (car bounds) (cdr bounds))))
          ; Iterate all KEY-CF rows in on-disk order.  Rows are ordered by
          ; user-key (ascending) then by INV(rev) (newest-first within key).
          ; We group consecutive rows sharing the same user-key.
          (let collect ((rs rows) (cur-uk #f) (cur-group '()) (results '()))
            (define (flush-group uk group)
              ; group is the accumulated (fk . vbv) pairs for uk, in scan order
              ; (newest→oldest).  Pick the visible version at at-rev.
              (if (null? group)
                  results
                  ; cw-71k (G2): peek tag/mod-rev off the raw bytes and decode
                  ; ONLY the visible winner — kv-record-decode copies the whole
                  ; value, and at real version depth (kubelet status churn)
                  ; most rows here are skipped, not returned.
                  (let loop ((g group))
                    (if (null? g)
                        results                        ; no visible version
                        (let* ((row (car g))
                               (vbv (cdr row))
                               (mr  (kv-raw-mod-rev vbv)))
                          (cond
                            ; skip versions newer than read revision
                            ((> mr at-rev) (loop (cdr g)))
                            ; first visible version is tombstone -> absent
                            ((kv-raw-tombstone? vbv) results)
                            ; live -> add to results if in-range and passes rev filters
                            (else
                             (let* ((rec (kv-record-decode vbv))
                                    (cr  (kv-rec-create-rev rec)))
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
                       (collect (cdr rs) uk (list row) new-results))))))))))))

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

; ---- cw-xq9: incremental live-keyspace stats (see store-ctx.scm field docs) ----
; Status and the health probes used to call mvcc-digest-at (a full-keyspace
; byte-at-a-time fold) on EVERY call, on the single shard thread — at 500-pod
; k8s scale each call pinned the shard for seconds; lease Txns queued behind it
; blew their 5s deadlines and k3s crash-looped. These counters make Status O(1):
; seeded lazily by ONE scan, then kept exact by mvcc-put!/mvcc-delete-range!.
; Bulk paths that bypass those (snapshot install, test reset) must call
; mvcc-live-stats-invalidate! to force a reseed.
(define (mvcc-live-stats-invalidate! ctx)
  (set-shard-ctx-live-bytes! ctx -1)
  (set-shard-ctx-live-count! ctx -1))
(define (mvcc-live-stats-add! ctx dbytes dcount)
  (if (>= (shard-ctx-live-bytes ctx) 0)
      (begin
        (set-shard-ctx-live-bytes! ctx (+ (shard-ctx-live-bytes ctx) dbytes))
        (set-shard-ctx-live-count! ctx (+ (shard-ctx-live-count ctx) dcount)))))
; (bytes count) — O(1) once seeded; one full scan on first call / after invalidate.
(define (mvcc-live-stats ctx)
  (if (< (shard-ctx-live-bytes ctx) 0)
      (let loop ((kvs (mvcc-live-kvs ctx 0)) (sz 0) (n 0))
        (if (null? kvs)
            (begin (set-shard-ctx-live-bytes! ctx sz)
                   (set-shard-ctx-live-count! ctx n))
            (loop (cdr kvs)
                  (+ sz (bytevector-length (caar kvs))
                        (bytevector-length (kv-rec-value (cdar kvs))))
                  (+ n 1)))))
  (list (shard-ctx-live-bytes ctx) (shard-ctx-live-count ctx)))

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
        ; cw-kp0 PERF: scan ONLY the (start-rev, cur-rev] revision WINDOW, not the whole
        ; NS-REV namespace. watch-on-apply! runs this per apply; a full scan made it
        ; O(store-size) per write -> O(N^2) under watch load -> writes time out. The NS-REV
        ; key is NS-REV || u64be(main) || u64be(sub); [rev16(start+1,0), rev16(cur+1,0)) is
        ; exactly main in (start-rev, cur-rev], ascending = revision order.
        (let ((rows (kv-scan-range ctx
                      (bytevector-append (mvcc-byte NS-REV) (rev->16 (+ start-rev 1) 0))
                      (bytevector-append (mvcc-byte NS-REV) (rev->16 (+ cur-rev 1) 0)))))
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
;
; cw-8vb (G6): a single window-driven pass (cw-xq9) is O(window) in REV-CF events, but at
; k3s's 5-min compaction cadence "window" can mean tens of thousands of revisions backed up
; behind a stale compact floor (e.g. after a burst of key creation, or a compactor that
; fell behind) — bench/test/bench-compact-scale.scm measured a single un-sliced pass over a
; ~29k-revision window taking minutes on the shard thread at 100k live keys, dominated by
; materializing one large sorted `keys` list plus its per-key KEY-CF scans in one shot.
; MVCC-COMPACT-SLICE bounds each internal pass to a fixed number of revisions so the
; working set (win-rows / keys) never grows past one slice's worth, regardless of how large
; the requested compactRev window is. External contract (ErrCompacted gate flips to the
; FULL target compactRev immediately, single (cons 'ok compactRev) result) is unchanged —
; only the GC work is chunked.
(define MVCC-COMPACT-SLICE 2000)

; GC exactly the (from-rev, to-rev] sub-window — the cw-xq9 window-driven pass, unchanged,
; just parameterized so mvcc-compact can call it repeatedly over bounded slices.
(define (mvcc-compact-slice! ctx from-rev to-rev)
  ; Only a key touched in (from-rev, to-rev] can have NEW garbage: every earlier
  ; compaction left each key holding at most one live version at-or-below its floor. One
  ; BOUNDED REV-CF window scan yields both the events to purge (step 4) and the
  ; changed-key set whose KEY-CF groups need GC (step 3, one bounded per-key scan each).
  (let* ((win-rows (kv-scan-range ctx (enc-rev (+ from-rev 1) 0)
                                      (enc-rev (+ to-rev 1) 0)))
         (keys (list-sort bv<? (map (lambda (row) (ev-key (event-decode (cdr row))))
                                    win-rows))))
    ; Step 3: KEY-CF GC per CHANGED key — same keep/delete rule as before:
    ; keep the single latest-≤-to-rev version iff it is live; delete
    ; every older ≤-to-rev version (and the latest too if tombstone).
    (let gc-keys ((ks keys) (prev #f))
      (if (pair? ks)
          (let ((uk (car ks)))
            (if (and prev (equal? uk prev))
                (gc-keys (cdr ks) prev)          ; dedup consecutive (sorted)
                (let* ((bounds (key-cf-row-bounds uk #f))
                       ; group rows arrive newest-first (INV-rev encoding)
                       (group (kv-scan-range ctx (car bounds) (cdr bounds))))
                  (let split ((g group) (below '()))
                    (if (null? g)
                        (let ((nf (reverse below)))          ; newest-first ≤ to-rev
                          (if (pair? nf)
                              (let* ((to-delete (if (kv-raw-tombstone? (cdar nf))  ; cw-71k: tag peek
                                                    nf          ; deleted key: drop all
                                                    (cdr nf)))) ; keep live latest
                                (for-each (lambda (row) (kv-del! ctx (car row)))
                                          to-delete))))
                        (let ((mr (kv-raw-mod-rev (cdar g))))   ; cw-71k: mod-rev peek
                          (if (<= mr to-rev)
                              (split (cdr g) (cons (car g) below))
                              (split (cdr g) below)))))
                  (gc-keys (cdr ks) uk))))))
    ; Step 4: REV-CF GC — the window rows ARE exactly the events to purge
    ; (events ≤ from-rev were deleted by prior compactions/slices).
    (for-each (lambda (row) (kv-del! ctx (car row))) win-rows)))

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
       ; cw-xq9 + cw-8vb: WINDOW-DRIVEN GC, sliced to bound per-pass working set — see
       ; MVCC-COMPACT-SLICE above.
       (let slice ((from cur-compact))
         (when (< from compactRev)
           (let ((to (min compactRev (+ from MVCC-COMPACT-SLICE))))
             (mvcc-compact-slice! ctx from to)
             (slice to))))
       ; Step 5: compaction does NOT bump current-rev
       (cons 'ok compactRev)))))

; ---------------------------------------------------------------------------
; INCREMENTAL compaction (cw-vku) — for the shard drivers' replicated COMPACT.
;
; mvcc-compact runs the whole window's GC synchronously; applied on the shard
; thread of EVERY replica at the same log position, a 5-min k8s window at 11k
; pods held every shard mailbox ~1s simultaneously (field: cluster-wide put
; stalls every ~302s, timestamp-aligned apply-COMPACT on all 5 nodes).
;
; mvcc-compact-begin! flips ONLY the ErrCompacted gate (the externally visible
; state transition — deterministic and identical on every replica) and leaves
; the physical GC to mvcc-compact-gc-step!, which the driver calls once per
; tick: one small slice per call, so the shard mailbox is never held for more
; than one slice. GC progress is tracked in META-COMPACT-GC ("GC done through
; rev X"); it rides the store, so restarts/snapshots resume where they left
; off. Physical deletes are invisible to reads (the gate already answers
; ErrCompacted below the floor; latest-version reads never see pruned rows),
; so per-replica GC timing may differ — only the gate is replicated state.
; ---------------------------------------------------------------------------
(define META-COMPACT-GC (meta-key "compact-gc"))
(define MVCC-COMPACT-STEP-SLICE 256)   ; revs GC'd per tick (bounds per-tick stall)

(define (mvcc-compact-gc-rev ctx)      ; floor GC has completed through
  (let ((b (kv-get ctx META-COMPACT-GC)))
    (if b (bytes->u64 b 0) 0)))

; gate-only compact: same result protocol as mvcc-compact, no GC work.
(define (mvcc-compact-begin! ctx compactRev)
  (let ((cur-compact (mvcc-compact-rev ctx))
        (cur-rev     (mvcc-current-rev ctx)))
    (cond
      ((<= compactRev cur-compact) (cons 'err-compacted cur-compact))
      ((> compactRev cur-rev)      (cons 'err-future-rev cur-rev))
      (else
       ; seed the GC cursor at the OLD floor exactly once (a pending cursor
       ; from an earlier begin! is already <= cur-compact — keep it).
       (if (not (kv-get ctx META-COMPACT-GC))
           (kv-put! ctx META-COMPACT-GC (u64->bytes cur-compact)))
       (kv-put! ctx META-COMPACT-REV (u64->bytes compactRev))
       (cons 'ok compactRev)))))

; one bounded GC slice; -> #t if more work remains, #f when caught up.
(define (mvcc-compact-gc-step! ctx)
  (let ((done (mvcc-compact-gc-rev ctx))
        (floor (mvcc-compact-rev ctx)))
    (if (>= done floor)
        #f
        (let ((to (min floor (+ done MVCC-COMPACT-STEP-SLICE))))
          (mvcc-compact-slice! ctx done to)
          (kv-put! ctx META-COMPACT-GC (u64->bytes to))
          (< to floor)))))

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
            (latest-cache-evict! ctx uk)  ; cw-65x: tombstone kills the live entry
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

; cw-kp0 Phase 4 cross-shard 2PC: per-shard prepared-txn stage, txnid -> (rev . ops).
; Built deterministically by applying TXN-PREPARE on every replica (it rides the Raft
; log), drained by TXN-COMMIT/ABORT. ponytail: lives outside ctx so a snapshot taken
; between PREPARE and COMMIT won't carry it — acceptable for the short prepare→commit
; window (the coordinator re-drives on a leader change); move into ctx if snapshots
; ever land mid-txn in practice.
(define txn-stage (make-eqv-hashtable))

; cw-kp0 Phase 4 ISOLATION: a prepared txn holds its keys until commit/abort. A new
; PREPARE whose keys overlap ANY other in-flight staged txn's written keys conflicts
; (votes no) — without this, two concurrent txns on the same key both pass their guards
; against the pre-commit state and both commit => lost update (Elle :G0/:G-single-item).
; No-wait (conflict => abort+retry, never block) so no deadlock. The stage is small
; (only in-flight prepared txns), so a linear scan is fine.
; stage entry = #(rev rkeys wkeys ops): rkeys = guard/read keys, wkeys = written keys,
; ops = the ops to apply on commit. Conflict (serializability) is rw/wr/ww — a shared key
; where at least one side WRITES it; two pure reads (rr) of the same key do NOT conflict.
(define (any-key-member? ks pool)
  (cond ((null? ks) #f) ((member (car ks) pool) #t) (else (any-key-member? (cdr ks) pool))))
(define (txn-stage-conflict? txnid my-rkeys my-wkeys)
  (let loop ((ids (vector->list (hashtable-keys txn-stage))))
    (cond ((null? ids) #f)
          ((= (car ids) txnid) (loop (cdr ids)))
          (else (let ((stg (hashtable-ref txn-stage (car ids) #f)))
                  (if (and stg
                           (let ((o-rkeys (vector-ref stg 1)) (o-wkeys (vector-ref stg 2)))
                             (or (any-key-member? my-wkeys o-rkeys)   ; my write vs their read
                                 (any-key-member? my-wkeys o-wkeys)   ; ww
                                 (any-key-member? my-rkeys o-wkeys)))) ; my read vs their write
                      #t (loop (cdr ids))))))))

(define (mvcc-apply ctx cmd)
  (let* ((prev-rev (mvcc-current-rev ctx))
         (main     (+ prev-rev 1))
         (op       (cmd-op cmd)))
    (cond
      ((string=? op "REV-GRANT")
       ; cw-kp0 phase 2: the rev-authority hands out a contiguous block of N global
       ; revisions. Advances the SEPARATE global-rev counter (NOT current-rev — a
       ; grant reserves revisions, it does not commit a write) and returns the
       ; block's low revision; hi = lo+N-1. Per-batch grants sized to the batch are
       ; hole-free (ADR 0006). Only the authority shard ever applies this command.
       (let* ((n  (let ((x (bytes->int (list-ref cmd 1)))) (if (and x (> x 0)) x 1)))
              (g  (mvcc-global-rev ctx))
              (lo (+ g 1)))
         (mvcc-set-global-rev! ctx (+ g n))
         (cons "REV-GRANT" lo)))
      ((string=? op "PUT-AT")
       ; cw-kp0 phase 2b.4: apply a PUT at an EXPLICIT revision carried in the entry
       ; (the global rev the leader granted at propose time, ADR 0006). All replicas
       ; apply at the same embedded rev => deterministic. current-rev is set TO that
       ; rev (not prev+1). In global-rev mode every write is a PUT-AT, so current-rev
       ; tracks the granted global sequence. Reply shape matches PUT ("PUT" . rev).
       ; ("PUT-AT" revStr K V [leaseStr])
       (let* ((at    (bytes->int (list-ref cmd 1)))
              (K     (list-ref cmd 2))
              (V     (list-ref cmd 3))
              (lease (if (>= (length cmd) 5)
                         (let ((l (bytes->int (list-ref cmd 4)))) (if l l 0))
                         0)))
         (if (and (not (= lease 0)) (not (mvcc-lease-exists? ctx lease)))
             (cons 'err-lease-not-found lease)
             (begin
               (mvcc-put! ctx K V lease at 0)
               (mvcc-set-current-rev! ctx at)
               (cons "PUT" at)))))
      ((string=? op "PUT-GLOBAL")
       ; cw-kp0: the AUTHORITY's own data PUT. Draw the next GLOBAL rev from the authority's
       ; counter at APPLY time (deterministic — every replica has the same mvcc-global-rev),
       ; so shard-0 keys share the ONE global rev space and never collide with a writer's
       ; granted global rev. The authority is the sole advancer of this counter (grants +
       ; its own writes interleave monotonically). ("PUT-GLOBAL" K V [leaseStr])
       (let* ((at    (+ (mvcc-global-rev ctx) 1))
              (K     (list-ref cmd 1))
              (V     (list-ref cmd 2))
              (lease (if (>= (length cmd) 4)
                         (let ((l (bytes->int (list-ref cmd 3)))) (if l l 0))
                         0)))
         (if (and (not (= lease 0)) (not (mvcc-lease-exists? ctx lease)))
             (cons 'err-lease-not-found lease)
             (begin
               (mvcc-set-global-rev! ctx at)
               (mvcc-put! ctx K V lease at 0)
               (mvcc-set-current-rev! ctx at)
               (cons "PUT" at)))))
      ((string=? op "TXN-PREPARE")
       ; cw-kp0 Phase 4 cross-shard 2PC, participant side: check THIS shard's guards and,
       ; if all hold, STAGE this shard's ops under txnid at the txn's global rev (not yet
       ; visible). Deterministic on every replica. Reply (list "TXN-PREPARE" txnid bool).
       ; ("TXN-PREPARE" txnidBytes revBytes subtxnBytes) — subtxn = (make-txn guards ops ()).
       (let* ((txnid  (bytes->int (list-ref cmd 1)))
              (rev    (bytes->int (list-ref cmd 2)))
              (sub    (txn-decode (list-ref cmd 3)))
              (rkeys  (map cmp-key (txn-compares sub)))
              (wkeys  (append (map (lambda (o) (vector-ref o 1)) (txn-success sub))
                              (map (lambda (o) (vector-ref o 1)) (txn-failure sub))))
              ; vote yes only if no in-flight txn conflicts (rw/wr/ww) AND my guards hold
              (ok     (and (not (txn-stage-conflict? txnid rkeys wkeys))
                           (compares-all-true? ctx (txn-compares sub)))))
         (if ok (hashtable-set! txn-stage txnid (vector rev rkeys wkeys (txn-success sub))))
         (list "TXN-PREPARE" txnid ok)))
      ((string=? op "TXN-COMMIT")
       ; apply the staged ops at the txn's global rev (deterministic), make them visible,
       ; drop the stage. Idempotent: a missing stage (already committed/never prepared
       ; here) is a no-op. ("TXN-COMMIT" txnidBytes)
       (let* ((txnid (bytes->int (list-ref cmd 1)))
              (stg   (hashtable-ref txn-stage txnid #f)))
         (if stg
             (let ((rev (vector-ref stg 0)))
               (let loop ((ops (vector-ref stg 3)) (sub 0))
                 (when (pair? ops)
                   (op-eval-apply ctx (car ops) rev sub)
                   (loop (cdr ops) (+ sub 1))))
               (mvcc-set-current-rev! ctx rev)
               (hashtable-delete! txn-stage txnid)))
         (list "TXN-COMMIT" txnid)))
      ((string=? op "TXN-ABORT")
       ; drop the stage (guards failed on some participant, or coordinator gave up).
       ; Idempotent. ("TXN-ABORT" txnidBytes)
       (let ((txnid (bytes->int (list-ref cmd 1))))
         (hashtable-delete! txn-stage txnid)
         (list "TXN-ABORT" txnid)))
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
       ; COMPACT does NOT bump current-rev.  cw-vku: flip ONLY the ErrCompacted
       ; gate here — the physical GC is incremental (mvcc-compact-gc-step!, one
       ; slice per driver tick), so a big window never holds every replica's
       ; shard mailbox for the full sweep at the same log position.
       ; The rev argument is a decimal-ASCII bytevector (same convention as leaseId).
       (let ((rev (let ((l (bytes->int (list-ref cmd 1)))) (if l l 0))))
         (mvcc-compact-begin! ctx rev)))
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

      ;; ===== Alarms (cw-u4a.42) — Raft-replicated NOSPACE/CORRUPT =====
      ;; ("ALARM-SET" memberID alarmType) / ("ALARM-DISARM" memberID alarmType): args are
      ;; decimal-ASCII bytevectors (the leaseId convention).  Writes NS-ALARM on EVERY
      ;; replica via the committed command; NO current-rev bump (alarms aren't keyspace
      ;; revisions).  Ack ("ALARM-OK" . active?) — usable as the client ack.
      ((string=? op "ALARM-SET")
       (mvcc-alarm-set! ctx (bytes->int (list-ref cmd 1)) (bytes->int (list-ref cmd 2)))
       (cons "ALARM-OK" #t))
      ((string=? op "ALARM-DISARM")
       (mvcc-alarm-disarm! ctx (bytes->int (list-ref cmd 1)) (bytes->int (list-ref cmd 2)))
       (cons "ALARM-OK" #f))

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
