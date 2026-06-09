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

; ---------------------------------------------------------------------------
; META keys
; ---------------------------------------------------------------------------

(define (meta-key name) (bytevector-append (mvcc-byte NS-META) (string->utf8 name)))
(define META-CURRENT-REV (meta-key "current-rev"))
(define META-COMPACT-REV (meta-key "compact-rev"))

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
  (let ((at-rev (if (pair? at) (car at) #f)))
    (let loop ((rows (kv-scan ctx (key-cf-prefix K))))
      (if (null? rows)
          #f
          (let ((r (kv-record-decode (cdar rows))))
            (cond
              ; skip versions newer than the requested read revision
              ((and at-rev (> (kv-rec-mod-rev r) at-rev)) (loop (cdr rows)))
              ; first visible version is a tombstone -> key absent at this rev
              ((kv-rec-tombstone? r) #f)
              (else r)))))))

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
; Returns:
;   ("PUT" . newRev)            ; the revision the put committed at
;   ("DEL" newRev . deleted)    ; revision + count deleted
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
         (mvcc-put! ctx K V lease main 0)
         (mvcc-set-current-rev! ctx main)
         (cons "PUT" main)))
      ((string=? op "DEL")
       (let* ((K        (list-ref cmd 1))
              (rangeEnd (if (>= (length cmd) 3) (list-ref cmd 2) #f))
              (deleted  (mvcc-delete-range! ctx K rangeEnd main 0)))
         (mvcc-set-current-rev! ctx main)
         (cons "DEL" (cons main deleted))))
      (else
       (error "mvcc-apply: unknown command" op)))))
