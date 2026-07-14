; src/store-ctx.scm — generic durable-KV substrate for crab-watchstore.
;
; This is the ONLY layer that touches the store-* host procedures (cs-store
; RocksDB FFI).  Everything above it sees logical bytevector keys/values and
; the helpers below — no raw store handle or CF name ever leaks out.
;
; Extracted from crab-cache/src/store-ctx.scm lines ~1–118:
;   KEPT:  make-ctx, kv-get, kv-put!, kv-del!, kv-exists?,
;          kv-scan, kv-scan-count,
;          ctx-dirty?, ctx-dirty-count, ctx-flush!,
;          ctx-save-applied!, ctx-load-applied.
;   OMITTED: dir-key/dir-val, key-entry/key-type, dir-set!, purge-key!,
;            ctype-touch!, purge-if-empty!, type-guard, and all Redis
;            data-type helpers.  Those will be replaced by MVCC in cw-u4a.5/.6.
;
; Depends on: encoding.scm (subbv, u64->bytes, bytes->u64).

(include "src/encoding.scm")

; ---- shard context record ----
;
; Wraps the open store handle, the column family name, and the group-commit
; dirty counter.  `sync` = #t enables durable mode: writes go to the WAL with
; sync=#f (no per-write fsync) and ctx-flush! issues ONE fsync per batch.
; sync = #f = relaxed mode: never fsyncs (dirty counter is still tracked so
; callers can query it if needed).

(define-record-type shard-ctx
  (fields (immutable handle)
          (immutable cf)
          (immutable sync)
          (mutable   dirty)         ; # of writes buffered since last fsync
          (mutable   crev)          ; EXP7: cached current-rev (-1 = not yet read)
          ; cw-xq9: INCREMENTAL live-keyspace accounting (logical bytes = sum of
          ; user-key+value lengths of live keys; count = # live keys). Status and
          ; the health probes used to FULL-SCAN + byte-fold the keyspace on every
          ; call (mvcc-digest-at) on the single shard thread — at 500-pod k8s
          ; scale each call pinned the shard for seconds, lease Txns behind it
          ; blew their 5s deadlines, and k3s crash-looped. Maintained by
          ; mvcc-put!/mvcc-delete-range!; seeded (-1 = unseeded) by
          ; mvcc-live-stats-seed! at shard boot / snapshot install / reset.
          (mutable   live-bytes)
          (mutable   live-count)
          ; cw-65x: OPTIONAL latest-version cache for the APPLY path only
          ; (#f = disabled). key (utf8 string of user key) -> vector
          ; #(create-rev version lease val-len). Kills the per-PUT KEY-CF
          ; iterator seek (mvcc-get-latest) that dominated the shard thread.
          ; Only the applier ctx enables it (single-threaded writes), so it
          ; is trivially coherent; snapshot-install/reset must invalidate.
          (mutable   latest-cache)
          ; cw-65x: per-command write buffer (#f disabled; else a list of
          ; (K . V) newest-first). kv-put! appends; ANY read/delete/flush on
          ; this ctx drains it first via ONE native store-put-many WriteBatch.
          ; Applier ctx only; kv-wbuf-drain! MUST run before other actors can
          ; observe the command (apply-cmd! drains before watch notify/acks).
          (mutable   wbuf)))

; (make-ctx handle [cf-name] [sync?])
(define (make-ctx handle . opts)
  (make-shard-ctx handle
                  (if (and (pair? opts) (car opts)) (car opts) "default")
                  (and (pair? opts) (pair? (cdr opts)) (cadr opts))
                  0
                  -1
                  -1
                  -1
                  #f
                  #f))

; ---- raw KV ops ----
;
; Writes go to RocksDB immediately with sync=#f so read-your-writes is
; preserved in the same command.  In durable mode the per-write fsync is
; deferred: dirty is bumped and ctx-flush! issues ONE fsync for the batch.

(define (ctx-mark-dirty! ctx)
  (set-shard-ctx-dirty! ctx (+ (shard-ctx-dirty ctx) 1)))

(define (kv-get ctx k)
  (kv-wbuf-drain! ctx)
  (store-get (shard-ctx-handle ctx) (shard-ctx-cf ctx) k))

(define (kv-wbuf-enable! ctx) (set-shard-ctx-wbuf! ctx '()))
(define (kv-wbuf-drain! ctx)
  (let ((b (shard-ctx-wbuf ctx)))
    (if (and b (pair? b))
        (begin
          (store-put-many (shard-ctx-handle ctx) (shard-ctx-cf ctx) (reverse b) #f)
          (set-shard-ctx-wbuf! ctx '())))))

(define (kv-put! ctx k v)
  (let ((b (shard-ctx-wbuf ctx)))
    (if b
        (set-shard-ctx-wbuf! ctx (cons (cons k v) b))
        (store-put (shard-ctx-handle ctx) (shard-ctx-cf ctx) k v #f)))
  (ctx-mark-dirty! ctx))

(define (kv-del! ctx k)
  (kv-wbuf-drain! ctx)
  (store-delete (shard-ctx-handle ctx) (shard-ctx-cf ctx) k #f)
  (ctx-mark-dirty! ctx))

(define (kv-exists? ctx k)
  (and (kv-get ctx k) #t))

; ---- group-commit flush ----

(define (ctx-dirty? ctx)
  (and (shard-ctx-sync ctx) (> (shard-ctx-dirty ctx) 0)))

(define (ctx-dirty-count ctx)
  (shard-ctx-dirty ctx))

; Issue ONE WAL fsync covering every write accumulated since the last flush,
; then reset the dirty counter.  No-op when nothing is buffered or in relaxed
; mode (the counter is reset either way).
(define (ctx-flush! ctx)
  (kv-wbuf-drain! ctx)
  (if (ctx-dirty? ctx)
      (store-flush-wal (shard-ctx-handle ctx) #t))
  (set-shard-ctx-dirty! ctx 0))

; ---- Raft applied-index persistence ----
;
; Persists the highest applied Raft index+term under a reserved key "_raft_applied"
; in the same group-commit batch as the entry's mutations — one fsync makes
; (mutation, applied-index) durable together.  On restart the shard restores
; base/applied/commit from it so the log replays only entries ABOVE the snapshot.

(define RAFT-APPLIED-KEY (string->utf8 "_raft_applied"))

(define (ctx-save-applied! ctx idx term)
  (store-put (shard-ctx-handle ctx) (shard-ctx-cf ctx) RAFT-APPLIED-KEY
             (bytevector-append (u64->bytes idx) (u64->bytes term)) #f)
  (ctx-mark-dirty! ctx))

(define (ctx-load-applied ctx)     ; -> (idx . term); (0 . 0) if none
  (let ((b (kv-get ctx RAFT-APPLIED-KEY)))
    (if (and b (>= (bytevector-length b) 16))
        (cons (bytes->u64 b 0) (bytes->u64 b 8))
        (cons 0 0))))

; ---- prefix scan ----
;
; Returns a list of (fullkey . value) bytevector pairs, over a stable snapshot.

(define (kv-scan ctx prefix)
  (kv-wbuf-drain! ctx)
  (let ((it (store-iter (shard-ctx-handle ctx) (shard-ctx-cf ctx) prefix)))
    (let loop ((acc '()))
      (let ((nx (store-iter-next it)))
        (if nx
            (loop (cons nx acc))
            (begin (store-iter-close it) (reverse acc)))))))

; half-open range scan [start, end) — bounds the scan to exactly the needed rows
; (e.g. a watch revision window) instead of a full-namespace prefix scan.
(define (kv-scan-range ctx start end)
  (kv-wbuf-drain! ctx)
  (let ((it (store-iter-range (shard-ctx-handle ctx) (shard-ctx-cf ctx) start end)))
    (let loop ((acc '()))
      (let ((nx (store-iter-next it)))
        (if nx
            (loop (cons nx acc))
            (begin (store-iter-close it) (reverse acc)))))))

(define (kv-scan-count ctx prefix)
  (kv-wbuf-drain! ctx)
  (let ((it (store-iter (shard-ctx-handle ctx) (shard-ctx-cf ctx) prefix)))
    (let loop ((n 0))
      (if (store-iter-next it)
          (loop (+ n 1))
          (begin (store-iter-close it) n)))))

; ---- point seek (cw-u4a.38) ----
;
; One RocksDB Seek to the first key >= `seekkey` that still starts with `prefix`
; (the bounding group); returns that single (fullkey . value) | #f.  O(log n) — it
; does NOT materialise the whole prefix range like kv-scan, so it is the right
; primitive for a "latest version <= readRev" MVCC point read.
(define (kv-seek ctx seekkey prefix)
  (store-seek (shard-ctx-handle ctx) (shard-ctx-cf ctx) seekkey prefix))
