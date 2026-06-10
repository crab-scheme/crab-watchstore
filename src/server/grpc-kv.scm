; server/grpc-kv.scm — the etcd v3 KV gRPC service binding (cw-u4a.22).
;
; THIS is the keystone that makes crab-watchstore speak etcd.  It is the handler
; ACTOR the gRPC transport (cw-u4a.20) drives: the transport delivers one
; ('*grpc-request* H) mailbox message per unary call; this actor reads the method
; PATH + de-framed request BYTES, decodes the protobuf with the pure-Scheme codec
; (cw-u4a.19, src/proto.scm), dispatches to the shard, re-encodes the response, and
; frames it back with grpc-respond! / grpc-respond-error!.
;
;   gRPC framing + trailers  : Rust (cs-web h2c, cw-u4a.20)
;   protobuf encode/decode   : Scheme (src/proto.scm, cw-u4a.19)
;   method dispatch + etcd<->internal translation + leader redirects : HERE
;
; ---------------------------------------------------------------------------
; How the handler reaches the shard (reads vs writes)
; ---------------------------------------------------------------------------
; The handler runs in its OWN spawn-source runtime/thread, so it CANNOT touch the
; shard's ctx (the RocksDB handle lives in the shard actor).  Everything goes
; through the shard's mailbox, with THIS actor's own PID ((self)) as the reply-pid:
;
;   WRITES (Put/DeleteRange/Txn/Compact) ride the EXISTING async commit->ack bridge:
;     (send shard (cons (self) INTERNAL-CMD)) ; shard proposes through Raft, replies
;     (raw-receive)                           ; on commit+apply via `pending` drain.
;   The internal command vocabulary is the shard apply-fn's flat form (bytevectors):
;     ("PUT" key value lease) / ("DEL" key range-end) / ("TXN" txn-bytes) /
;     ("COMPACT" rev).  The ack is ("PUT" . rev) / ("DEL" rev . n) /
;     (succeeded? . responses) / (cons 'ok rev) | (cons 'err-* ...), or 'tryagain
;     (not leader) / 'indeterminate (stepdown).
;
;   READS (Range) + prev_kv snapshots + the ResponseHeader revision use the KV read
;   seams cw-u4a.22 added to the shard actor (mirroring its (get ...) probe):
;     (kv-range CONN OPTS)  -> (kv-range-ok cur-rev term err total ((k v cr mr ver lease)...))
;     (kv-prev  CONN KEY)   -> (kv-prev-ok  (k v cr mr ver lease) | #f)
;     (cur-rev  CONN)       -> (cur-rev-ok  cur-rev term)
;   all leader-gated (single-node = always leader, so they always serve here).
;
; ResponseHeader on every response: revision = the store's CURRENT revision after the
; op; raft_term = the shard's Raft term; cluster_id / member_id derived deterministically
; from the node config (stable nonzero — etcdctl only needs them present + consistent).
;
; ---------------------------------------------------------------------------
; Spawn
; ---------------------------------------------------------------------------
;   (spawn-source "(include \"src/server/grpc-kv.scm\")" 'grpc-kv-main
;                 SHARD-PID CLUSTER-ID MEMBER-ID)
;   SHARD-PID  : the leader shard replica's PID (PID symbol, sendable)
;   CLUSTER-ID : u64 cluster id for ResponseHeader (any stable nonzero)
;   MEMBER-ID  : u64 member id for ResponseHeader (any stable nonzero)
; Then  (grpc-serve "host:port" <this-pid>)  routes etcd KV calls to it.

; Includes, in dependency order (mirrors shard-actor.scm's prefix):
;   encoding  -> u64->bytes / subbv / int->bytes
;   store-ctx -> ctx accessors (DEFINE-only; no store opened in this actor)
;   mvcc      -> mvcc-byte + kv-rec-* + range-opt, AND tail-includes src/txn.scm
;                (the internal Txn struct ctors make-compare/make-txn/op-* + txn-encode)
;   proto     -> pb-encode/pb-decode + every etcd KV message schema (cw-u4a.19)
; The handler NEVER opens a store / evaluates a Txn locally; it only uses the codec,
; the Txn STRUCT builders + txn-encode, and the kv-rec-*/range-opt helpers — so the
; store-touching bodies these files also define are simply never called here.
(include "src/encoding.scm")
(include "src/store-ctx.scm")
(include "src/mvcc.scm")
(include "src/proto.scm")

; ===========================================================================
; small helpers
; ===========================================================================

(define EMPTY (make-bytevector 0 0))

(define (galist key alist default)
  (let ((c (assq key alist))) (if c (cdr c) default)))

; etcd enum constants (kv.proto / rpc.proto)
;   RangeRequest.SortOrder : NONE=0 ASCEND=1 DESCEND=2
;   RangeRequest.SortTarget: KEY=0 VERSION=1 CREATE=2 MOD=3 VALUE=4
;   Compare.CompareResult  : EQUAL=0 GREATER=1 LESS=2 NOT_EQUAL=3
;   Compare.CompareTarget  : VERSION=0 CREATE=1 MOD=2 VALUE=3 LEASE=4
(define (etcd-sort-order->sym n)
  (cond ((= n 1) 'ascend) ((= n 2) 'descend) (else 'none)))
(define (etcd-sort-target->sym n)
  (cond ((= n 1) 'version) ((= n 2) 'create) ((= n 3) 'mod) ((= n 4) 'value) (else 'key)))

; gRPC status codes used here
(define GRPC-OK            0)
(define GRPC-OUT-OF-RANGE 11)   ; ErrCompacted surfaces here (etcd convention)
(define GRPC-UNIMPLEMENTED 12)
(define GRPC-UNAVAILABLE   14)   ; not-leader: client retries another endpoint
(define GRPC-INTERNAL      13)

; etcd's ErrCompacted gRPC message (what a real client matches on)
(define ETCD-ERR-COMPACTED "etcdserver: mvcc: required revision has been compacted")

; ===========================================================================
; Maintenance/Status + Cluster/MemberList — MINIMAL stub schemas (etcdctl connect).
; These live at module top level (NOT in proto.scm's shared KV schema set) because
; they are connection-plumbing for the etcdctl test, not the KV deliverable; full
; Maintenance/Cluster are cw-u4a.30/.32.  Field numbers from etcd's rpc.proto.
; ===========================================================================
;
; etcdserverpb.StatusResponse{header=1, version=2(string), dbSize=3, leader=4(u64),
;   raftIndex=5, raftTerm=6, raftAppliedIndex=7, ...} — header+version+raft scalars
;   are enough for etcdctl endpoint health / `endpoint status`.
(define StatusResponse-schema
  (list
    (list 1 'header     '(message ResponseHeader-schema-ref) 'optional)
    (list 2 'version    'string 'optional)
    (list 3 'dbSize     'int64  'optional)
    (list 4 'leader     'uint64 'optional)
    (list 5 'raftIndex  'uint64 'optional)
    (list 6 'raftTerm   'uint64 'optional)))

; etcdserverpb.Member{ID=1, name=2, peerURLs=3(rep string), clientURLs=4(rep string)}.
(define Member-schema
  (list
    (list 1 'ID         'uint64 'optional)
    (list 2 'name       'string 'optional)
    (list 3 'peerURLs   'string 'repeated)
    (list 4 'clientURLs 'string 'repeated)))

; etcdserverpb.MemberListResponse{header=1, members=2(repeated Member)}.
(define MemberListResponse-schema
  (list
    (list 1 'header  '(message ResponseHeader-schema-ref) 'optional)
    (list 2 'members '(message Member-schema-ref)         'repeated)))

; Register Member-schema-ref so the nested-message codec can resolve the repeated
; Member field.  proto.scm's deref-schema reads schema-ref-table at call time, so
; appending here (once, at include time) is additive and order-independent.
(set! schema-ref-table
      (cons (cons 'Member-schema-ref (lambda () Member-schema)) schema-ref-table))

; ===========================================================================
; ResponseHeader  (cluster-id / member-id are spawn args; rev/term per-op)
; ===========================================================================

(define (make-header cluster-id member-id revision raft-term)
  (list (cons 'cluster_id cluster-id)
        (cons 'member_id  member-id)
        (cons 'revision   revision)
        (cons 'raft_term  raft-term)))

; ===========================================================================
; A flat KV tuple (k v cr mr ver lease) from a read seam -> a KeyValue alist.
; keys-only is honoured upstream (mvcc-range blanks the value), so we just map.
; ===========================================================================

(define (tuple->keyvalue t)
  (list (cons 'key             (list-ref t 0))
        (cons 'value           (list-ref t 1))
        (cons 'create_revision (list-ref t 2))
        (cons 'mod_revision    (list-ref t 3))
        (cons 'version         (list-ref t 4))
        (cons 'lease           (list-ref t 5))))

; ===========================================================================
; PURE etcd <-> internal translation (no actor state — unit-testable).
; ===========================================================================

; The SCALAR mvcc-range options (revision/limit/count-only/keys-only/sort/min-max)
; from a RangeRequest alist — every value is an int / bool / symbol, so these are
; exactly what txn.scm's op-range encoder (rangeopt-encode) can serialize.  key and
; range_end are deliberately EXCLUDED (they travel as op-range's own vector slots,
; never inside the opts alist).
(define (range-request->scalar-opts rr)
  (list (cons 'revision       (galist 'revision rr 0))
        (cons 'limit          (galist 'limit rr 0))
        (cons 'count-only     (galist 'count_only rr #f))
        (cons 'keys-only      (galist 'keys_only rr #f))
        (cons 'sort-order     (etcd-sort-order->sym  (galist 'sort_order rr 0)))
        (cons 'sort-target    (etcd-sort-target->sym (galist 'sort_target rr 0)))
        (cons 'min-create-rev (galist 'min_create_revision rr 0))
        (cons 'max-create-rev (galist 'max_create_revision rr 0))
        (cons 'min-mod-rev    (galist 'min_mod_revision rr 0))
        (cons 'max-mod-rev    (galist 'max_mod_revision rr 0))))

; The FULL mvcc-range request the kv-range READ SEAM consumes: the scalar opts PLUS
; key + range-end (the shard's seam reads key/range-end out of this alist via
; range-opt and hands them to mvcc-range as its own arguments).  Used only for the
; top-level KV/Range read path — NOT for a Txn's nested op-range (see above).
(define (range-request->opts rr)
  (cons (cons 'key (galist 'key rr EMPTY))
        (cons (cons 'range-end (let ((re (galist 'range_end rr EMPTY)))
                                 (if (= (bytevector-length re) 0) #f re)))
              (range-request->scalar-opts rr))))

; etcd Compare (alist) -> internal compare 4-vector #(target result key tval):
;   target  : VERSION/CREATE/MOD/VALUE/LEASE enum -> CMP-* small int (SAME order)
;   result  : EQUAL/GREATER/LESS/NOT_EQUAL enum   -> RES-* small int (SAME order)
;   key     : the compare key bytevector
;   tval    : VALUE -> the raw `value` bytes ; int targets -> u64be(the oneof field)
; The oneof target_union field carrying the value is chosen by `target`.  The internal
; CMP-*/RES-* constants (src/txn.scm) are defined with the SAME integer values as the
; etcd enums, so the enum int passes straight through.
(define (etcd-compare->internal c)
  (let* ((result (galist 'result c 0))
         (target (galist 'target c 0))
         (key    (galist 'key c EMPTY)))
    (cond
      ((= target CMP-VALUE)
       (make-compare CMP-VALUE result key (galist 'value c EMPTY)))
      ((= target CMP-VERSION)
       (make-compare CMP-VERSION result key (u64->bytes (galist 'version c 0))))
      ((= target CMP-CREATE)
       (make-compare CMP-CREATE result key (u64->bytes (galist 'create_revision c 0))))
      ((= target CMP-MOD)
       (make-compare CMP-MOD result key (u64->bytes (galist 'mod_revision c 0))))
      ((= target CMP-LEASE)
       (make-compare CMP-LEASE result key (u64->bytes (galist 'lease c 0))))
      (else (make-compare target result key EMPTY)))))

; etcd RequestOp (alist; a oneof of request_range/put/delete_range/txn) -> internal op.
; Exactly ONE branch is non-default; pb-decode fills the others as #f (absent message).
(define (etcd-requestop->internal op)
  (let ((rng (galist 'request_range op #f))
        (put (galist 'request_put op #f))
        (del (galist 'request_delete_range op #f))
        (txn (galist 'request_txn op #f)))
    (cond
      (put (op-put (galist 'key put EMPTY) (galist 'value put EMPTY) (galist 'lease put 0)))
      (del (op-del (galist 'key del EMPTY)
                   (let ((re (galist 'range_end del EMPTY)))
                     (if (= (bytevector-length re) 0) #f re))))
      (rng (op-range (galist 'key rng EMPTY)
                     (let ((re (galist 'range_end rng EMPTY)))
                       (if (= (bytevector-length re) 0) #f re))
                     (range-request->scalar-opts rng)))
      (txn (op-txn (etcd-txn->internal txn)))
      ; an empty/unknown RequestOp: a no-effect single-key Range over "" (etcd rejects
      ; empty ops; we stay defensive so a malformed op can't crash the Txn).
      (else (op-range EMPTY #f '())))))

; etcd TxnRequest (alist) -> internal txn struct #(compares success failure).
(define (etcd-txn->internal tr)
  (make-txn
   (map etcd-compare->internal  (galist 'compare tr '()))
   (map etcd-requestop->internal (galist 'success tr '()))
   (map etcd-requestop->internal (galist 'failure tr '()))))

; internal op-response -> etcd ResponseOp (alist), the inverse of the request side.
; Internal response shapes (from txn-eval-apply / op-eval-apply, src/txn.scm):
;   (cons 'put mod-rev)          -> response_put : PutResponse{header}
;   (cons 'del-count n)          -> response_delete_range : DeleteRangeResponse{header,deleted}
;   (count . kvlist)             -> response_range : RangeResponse{header,kvs,count}
;       where kvlist items are (uk . record-vector) from mvcc-range
;   (succeeded? . responses)     -> response_txn : a nested TxnResponse
; `hdr` is the shared ResponseHeader (one revision for the whole Txn).
(define (internal-response->etcd r hdr)
  (cond
    ((and (pair? r) (eq? (car r) 'put))
     (list (cons 'response_put (list (cons 'header hdr)))))
    ((and (pair? r) (eq? (car r) 'del-count))
     (list (cons 'response_delete_range
                 (list (cons 'header hdr) (cons 'deleted (cdr r))))))
    ; a Range op response: (count . kvlist) where car is an integer count.
    ((and (pair? r) (integer? (car r)))
     (list (cons 'response_range
                 (list (cons 'header hdr)
                       (cons 'kvs (map (lambda (item)
                                         (let ((uk (car item)) (rec (cdr item)))
                                           (list (cons 'key uk)
                                                 (cons 'value (kv-rec-value rec))
                                                 (cons 'create_revision (kv-rec-create-rev rec))
                                                 (cons 'mod_revision (kv-rec-mod-rev rec))
                                                 (cons 'version (kv-rec-version rec))
                                                 (cons 'lease (kv-rec-lease rec)))))
                                       (cdr r)))
                       (cons 'count (car r))))))
    ; a nested Txn response: (succeeded? . responses).
    ((and (pair? r) (boolean? (car r)))
     (list (cons 'response_txn (internal-txn-result->etcd r hdr))))
    (else (list))))   ; unknown -> empty ResponseOp

; internal (succeeded? . responses) -> etcd TxnResponse alist (header shared).
(define (internal-txn-result->etcd result hdr)
  (list (cons 'header    hdr)
        (cons 'succeeded (car result))
        (cons 'responses (map (lambda (r) (internal-response->etcd r hdr)) (cdr result)))))

; ===========================================================================
; the actor
; ===========================================================================

(define (grpc-kv-main shard-pid cluster-id member-id)

  ; ---- streaming state (cw-u4a.23) ----
  ; Active bidi streams (Watch / LeaseKeepAlive): call-handle -> worker-pid.
  ; The transport delivers a stream's client messages to THIS actor tagged with
  ; the handle; we route them to the per-stream worker (server/grpc-watch.scm).
  (define stream-workers (make-eqv-hashtable))
  ; FIFO buffer of dispatcher messages (*grpc-*) that arrived while a UNARY
  ; handler was mid `ask-shard` (its raw-receive must return the SHARD reply, not
  ; a concurrent stream/request message).  The main loop drains these first.
  (define grpc-pending '())

  ; ---- shard round-trips (this actor's PID is the reply-pid) ----
  ; raw-receive here must yield the shard's reply; any dispatcher traffic that
  ; races in (*grpc-request* / *grpc-stream-msg* / *grpc-stream-end*) is buffered
  ; for the main loop so we never mis-parse it as a shard ack.
  (define (ask-shard msg)
    (send shard-pid msg)
    (let wait ()
      (let ((r (raw-receive)))
        (if (and (pair? r)
                 (memq (car r) '(*grpc-request* *grpc-stream-msg* *grpc-stream-end*)))
            (begin (set! grpc-pending (append grpc-pending (list r))) (wait))
            r))))

  ; next mailbox message for the main loop: buffered dispatcher traffic first
  ; (FIFO), else a fresh receive.
  (define (next-message)
    (if (pair? grpc-pending)
        (let ((m (car grpc-pending))) (set! grpc-pending (cdr grpc-pending)) m)
        (raw-receive)))

  ; current (revision . term) for a ResponseHeader, read from the shard.
  (define (shard-header)
    (let ((r (ask-shard (list 'cur-rev (self)))))
      ; (cur-rev-ok rev term) ; on a non-leader the seam still replies via leader gate
      (if (and (pair? r) (eq? (car r) 'cur-rev-ok))
          (make-header cluster-id member-id (cadr r) (caddr r))
          (make-header cluster-id member-id 0 0))))

  ; header built from an explicit rev (+ a term fetched cheaply).
  (define (header-at rev)
    (let ((r (ask-shard (list 'cur-rev (self)))))
      (make-header cluster-id member-id rev
                   (if (and (pair? r) (eq? (car r) 'cur-rev-ok)) (caddr r) 1))))

  ; snapshot a key's current live record BEFORE a write (for prev_kv).
  (define (shard-prev key)
    (let ((r (ask-shard (list 'kv-prev (self) key))))
      (if (and (pair? r) (eq? (car r) 'kv-prev-ok)) (cadr r) #f)))   ; tuple | #f

  ; run a Range against the shard's ctx (leader-gated).  Returns the raw seam reply:
  ;   (kv-range-ok cur-rev term err total tuples) | 'tryagain | 'indeterminate
  (define (shard-range opts) (ask-shard (list 'kv-range (self) opts)))

  ; propose an internal write command; returns the shard ack.
  (define (shard-write cmd) (ask-shard (cons (self) cmd)))

  ; ===========================================================================
  ; method handlers — each returns (cons 'ok response-bytes)
  ;                              or (cons 'err (cons status "message"))
  ; ===========================================================================

  ; ---- KV/Range ----
  ; Decode RangeRequest, route the read through the shard's ReadIndex/leader gate,
  ; build RangeResponse{header, kvs, more, count}.  ErrCompacted -> OutOfRange(11).
  (define (handle-range bytes)
    (let* ((rr   (pb-decode RangeRequest-schema bytes))
           (opts (range-request->opts rr))
           (res  (shard-range opts)))
      (cond
        ((not (pair? res)) (cons 'err (cons GRPC-UNAVAILABLE "etcdserver: not leader")))
        ((eq? (car res) 'kv-range-ok)
         (let ((cur-rev (list-ref res 1)) (term (list-ref res 2))
               (err     (list-ref res 3)) (total (list-ref res 4))
               (tuples  (list-ref res 5)))
           (if (eq? err 'compacted)
               (cons 'err (cons GRPC-OUT-OF-RANGE ETCD-ERR-COMPACTED))
               (let* ((limit (galist 'limit rr 0))
                      ; etcd `more` = a limit was applied AND more keys exist past it.
                      (more  (and (> limit 0) (> total (length tuples))))
                      (resp  (list (cons 'header (make-header cluster-id member-id cur-rev term))
                                   (cons 'kvs   (map tuple->keyvalue tuples))
                                   (cons 'more  more)
                                   (cons 'count total))))
                 (cons 'ok (pb-encode RangeResponse-schema resp))))))
        (else (cons 'err (cons GRPC-UNAVAILABLE "etcdserver: not leader"))))))

  ; ---- KV/Put ----
  ; prev_kv snapshot (if requested) -> propose ("PUT" key value lease) -> PutResponse.
  ; lease is decimal-ASCII bytes (apply-fn convention).  A put to a dead lease comes
  ; back (cons 'err-lease-not-found id) -> a gRPC error (FailedPrecondition 9, etcd's).
  (define (handle-put bytes)
    (let* ((pr    (pb-decode PutRequest-schema bytes))
           (key   (galist 'key pr EMPTY))
           (value (galist 'value pr EMPTY))
           (lease (galist 'lease pr 0))
           (want-prev (galist 'prev_kv pr #f))
           (prev  (and want-prev (shard-prev key)))
           (ack   (shard-write (list (string->utf8 "PUT") key value (int->bytes lease)))))
      (cond
        ((and (pair? ack) (string? (car ack)) (string=? (car ack) "PUT"))
         (let* ((rev  (cdr ack))
                (resp (append (list (cons 'header (header-at rev)))
                              (if prev (list (cons 'prev_kv (tuple->keyvalue prev))) '()))))
           (cons 'ok (pb-encode PutResponse-schema resp))))
        ((and (pair? ack) (eq? (car ack) 'err-lease-not-found))
         (cons 'err (cons 9 (string-append "etcdserver: requested lease not found"))))
        ((eq? ack 'tryagain)      (cons 'err (cons GRPC-UNAVAILABLE "etcdserver: not leader")))
        ((eq? ack 'indeterminate) (cons 'err (cons GRPC-UNAVAILABLE "etcdserver: leader changed")))
        (else (cons 'err (cons GRPC-INTERNAL (string-append "put: unexpected ack")))))))

  ; ---- KV/DeleteRange ----
  ; prev_kvs snapshot (if requested) -> propose ("DEL" key range-end) -> DeleteRangeResponse.
  (define (handle-delete-range bytes)
    (let* ((dr    (pb-decode DeleteRangeRequest-schema bytes))
           (key   (galist 'key dr EMPTY))
           (rend  (let ((re (galist 'range_end dr EMPTY)))
                    (if (= (bytevector-length re) 0) #f re)))
           (want-prev (galist 'prev_kv dr #f))
           ; snapshot the to-be-deleted live KVs via a Range over the same span.
           (prev-kvs (if want-prev
                         (let ((rres (shard-range (list (cons 'key key)
                                                        (cons 'range-end rend)))))
                           (if (and (pair? rres) (eq? (car rres) 'kv-range-ok))
                               (map tuple->keyvalue (list-ref rres 5))
                               '()))
                         '()))
           (cmd   (if rend
                      (list (string->utf8 "DEL") key rend)
                      (list (string->utf8 "DEL") key)))
           (ack   (shard-write cmd)))
      (cond
        ((and (pair? ack) (string? (car ack)) (string=? (car ack) "DEL"))
         (let* ((rev (cadr ack)) (deleted (cddr ack))
                (resp (append (list (cons 'header (header-at rev))
                                    (cons 'deleted deleted))
                              (if (and want-prev (pair? prev-kvs))
                                  (list (cons 'prev_kvs prev-kvs)) '()))))
           (cons 'ok (pb-encode DeleteRangeResponse-schema resp))))
        ((eq? ack 'tryagain)      (cons 'err (cons GRPC-UNAVAILABLE "etcdserver: not leader")))
        ((eq? ack 'indeterminate) (cons 'err (cons GRPC-UNAVAILABLE "etcdserver: leader changed")))
        (else (cons 'err (cons GRPC-INTERNAL "delete-range: unexpected ack"))))))

  ; ---- KV/Compact ----
  ; propose ("COMPACT" rev) -> CompactionResponse{header}.  ErrCompacted (already
  ; compacted past rev) / future-rev -> OutOfRange(11) with etcd's message.
  (define (handle-compact bytes)
    (let* ((cr  (pb-decode CompactionRequest-schema bytes))
           (rev (galist 'revision cr 0))
           (ack (shard-write (list (string->utf8 "COMPACT") (int->bytes rev)))))
      (cond
        ((and (pair? ack) (eq? (car ack) 'ok))
         (cons 'ok (pb-encode CompactionResponse-schema
                              (list (cons 'header (shard-header))))))
        ((and (pair? ack) (eq? (car ack) 'err-compacted))
         (cons 'err (cons GRPC-OUT-OF-RANGE ETCD-ERR-COMPACTED)))
        ((and (pair? ack) (eq? (car ack) 'err-future-rev))
         (cons 'err (cons GRPC-OUT-OF-RANGE
                          "etcdserver: mvcc: required revision is a future revision")))
        ((eq? ack 'tryagain)      (cons 'err (cons GRPC-UNAVAILABLE "etcdserver: not leader")))
        ((eq? ack 'indeterminate) (cons 'err (cons GRPC-UNAVAILABLE "etcdserver: leader changed")))
        (else (cons 'err (cons GRPC-INTERNAL "compact: unexpected ack"))))))

  ; ---- KV/Txn (translators are top-level; see etcd-txn->internal et al.) ----
  (define (handle-txn bytes)
    (let* ((tr    (pb-decode TxnRequest-schema bytes))
           (itxn  (etcd-txn->internal tr))
           (flat  (txn-encode itxn))
           (ack   (shard-write (list (string->utf8 "TXN") flat))))
      (cond
        ((and (pair? ack) (boolean? (car ack)))    ; (succeeded? . responses)
         ; the Txn bumped current-rev iff a branch mutated; either way the ResponseHeader
         ; carries the store's CURRENT revision after apply (etcd semantics).
         (let ((hdr (shard-header)))
           (cons 'ok (pb-encode TxnResponse-schema (internal-txn-result->etcd ack hdr)))))
        ((eq? ack 'tryagain)      (cons 'err (cons GRPC-UNAVAILABLE "etcdserver: not leader")))
        ((eq? ack 'indeterminate) (cons 'err (cons GRPC-UNAVAILABLE "etcdserver: leader changed")))
        (else (cons 'err (cons GRPC-INTERNAL "txn: unexpected ack"))))))

  ; ---- Lease/LeaseGrant (UNARY, cw-u4a.17) ----
  ; Propose ("LEASE-GRANT" id ttl) through the shard's async commit->ack bridge;
  ; ack (cons "LEASE-GRANT" assigned-id).  id=0 => apply auto-assigns.
  (define (handle-lease-grant bytes)
    (let* ((req (pb-decode LeaseGrantRequest-schema bytes))
           (ttl (galist 'ttl req 0))
           (id  (galist 'id req 0))
           (ack (ask-shard (list 'lease-grant (self) ttl id))))
      (cond
        ((and (pair? ack) (string? (car ack)) (string=? (car ack) "LEASE-GRANT"))
         (cons 'ok (pb-encode LeaseGrantResponse-schema
                              (list (cons 'header (shard-header))
                                    (cons 'id (cdr ack)) (cons 'ttl ttl)))))
        ((and (pair? ack) (eq? (car ack) 'lease-not-leader))
         (cons 'err (cons GRPC-UNAVAILABLE "etcdserver: not leader")))
        (else (cons 'err (cons GRPC-INTERNAL "lease-grant: unexpected ack"))))))

  ; ---- Lease/LeaseRevoke (UNARY, cw-u4a.17) ----
  ; Propose ("LEASE-REVOKE" id); ack (cons "LEASE-REVOKE" (cons rev count)).
  (define (handle-lease-revoke bytes)
    (let* ((req (pb-decode LeaseRevokeRequest-schema bytes))
           (id  (galist 'id req 0))
           (ack (ask-shard (list 'lease-revoke (self) id))))
      (cond
        ((and (pair? ack) (string? (car ack)) (string=? (car ack) "LEASE-REVOKE"))
         (cons 'ok (pb-encode LeaseRevokeResponse-schema
                              (list (cons 'header (shard-header))))))
        ((and (pair? ack) (eq? (car ack) 'lease-not-leader))
         (cons 'err (cons GRPC-UNAVAILABLE "etcdserver: not leader")))
        (else (cons 'err (cons GRPC-INTERNAL "lease-revoke: unexpected ack"))))))

  ; ---- Lease/LeaseTimeToLive (UNARY, cw-u4a.18) ----
  ; Leader-gated read; ack (lease-ttl-ok id granted-ttl remaining keys).  etcd's
  ; TTL field carries the REMAINING ttl (-1 if the lease is gone), grantedTTL the
  ; original; keys are the attached keys when requested.
  (define (handle-lease-ttl bytes)
    (let* ((req (pb-decode LeaseTimeToLiveRequest-schema bytes))
           (id  (galist 'id req 0))
           (want-keys (galist 'keys req #f))
           (ack (ask-shard (list 'lease-ttl (self) id want-keys))))
      (cond
        ((and (pair? ack) (eq? (car ack) 'lease-ttl-ok))
         (let ((granted (list-ref ack 2)) (remaining (list-ref ack 3)) (keys (list-ref ack 4)))
           (cons 'ok (pb-encode LeaseTimeToLiveResponse-schema
                                (append
                                  (list (cons 'header (shard-header))
                                        (cons 'id id)
                                        (cons 'ttl remaining)
                                        (cons 'granted_ttl granted))
                                  (if (pair? keys) (list (cons 'keys keys)) '()))))))
        ((and (pair? ack) (eq? (car ack) 'lease-not-leader))
         (cons 'err (cons GRPC-UNAVAILABLE "etcdserver: not leader")))
        (else (cons 'err (cons GRPC-INTERNAL "lease-ttl: unexpected ack"))))))

  ; ---- Lease/LeaseLeases (UNARY, cw-u4a.18) ----
  ; ack (lease-leases-ok (id ...)) -> LeaseLeasesResponse{leases:[{ID}...]}.
  (define (handle-lease-leases bytes)
    (let ((ack (ask-shard (list 'lease-leases (self)))))
      (cond
        ((and (pair? ack) (eq? (car ack) 'lease-leases-ok))
         (cons 'ok (pb-encode LeaseLeasesResponse-schema
                              (list (cons 'header (shard-header))
                                    (cons 'leases (map (lambda (id) (list (cons 'id id)))
                                                       (cadr ack)))))))
        ((and (pair? ack) (eq? (car ack) 'lease-not-leader))
         (cons 'err (cons GRPC-UNAVAILABLE "etcdserver: not leader")))
        (else (cons 'err (cons GRPC-INTERNAL "lease-leases: unexpected ack"))))))

  ; ===========================================================================
  ; dispatch one request handle
  ; ===========================================================================
  ; Streaming paths (Watch / LeaseKeepAlive are bidi) spawn a per-stream worker
  ; (server/grpc-watch.scm) and register it in stream-workers; everything else is
  ; served UNARY inline.
  ; The worker reads its FIRST client message itself via (grpc-request-bytes h)
  ; (the call slot lives until the worker closes the stream).  We pass only h +
  ; sendable scalars/pid — a bytevector spawn ARG would render as a #vu8(...)
  ; literal the spawn-source bootstrap reader rejects.
  (define (start-stream-worker! h entry)
    (let ((wpid (spawn-source "(include \"src/server/grpc-watch.scm\")" entry
                              h shard-pid cluster-id member-id)))
      (hashtable-set! stream-workers h wpid)))

  (define (dispatch! h)
    (let ((path (grpc-request-path h)))
      (cond
        ((string=? path "/etcdserverpb.Watch/Watch")
         (start-stream-worker! h 'grpc-watch-worker))
        ((string=? path "/etcdserverpb.Lease/LeaseKeepAlive")
         (start-stream-worker! h 'grpc-lease-keepalive-worker))
        (else (dispatch-unary! h path)))))

  (define (dispatch-unary! h path)
    (let* ((bytes (grpc-request-bytes h))
           (res   (guard (e (#t (cons 'err (cons GRPC-INTERNAL
                                                 (string-append "handler error")))))
                    (cond
                      ((string=? path "/etcdserverpb.KV/Range")       (handle-range bytes))
                      ((string=? path "/etcdserverpb.KV/Put")         (handle-put bytes))
                      ((string=? path "/etcdserverpb.KV/DeleteRange") (handle-delete-range bytes))
                      ((string=? path "/etcdserverpb.KV/Txn")         (handle-txn bytes))
                      ((string=? path "/etcdserverpb.KV/Compact")     (handle-compact bytes))
                      ; Lease UNARY RPCs (cw-u4a.17/.18); KeepAlive is bidi (above).
                      ((string=? path "/etcdserverpb.Lease/LeaseGrant")      (handle-lease-grant bytes))
                      ((string=? path "/etcdserverpb.Lease/LeaseRevoke")     (handle-lease-revoke bytes))
                      ((string=? path "/etcdserverpb.Lease/LeaseTimeToLive") (handle-lease-ttl bytes))
                      ((string=? path "/etcdserverpb.Lease/LeaseLeases")     (handle-lease-leases bytes))
                      ; minimal Status/MemberList STUBS so etcdctl's balancer connects
                      ; (full Maintenance/Cluster are cw-u4a.30/.32).
                      ((string=? path "/etcdserverpb.Maintenance/Status") (handle-status bytes))
                      ((string=? path "/etcdserverpb.Cluster/MemberList") (handle-member-list bytes))
                      (else (cons 'unimplemented path))))))
      (cond
        ((and (pair? res) (eq? (car res) 'ok))
         (grpc-respond! h (cdr res)))
        ((and (pair? res) (eq? (car res) 'err))
         (grpc-respond-error! h (cadr res) (cddr res)))
        ((and (pair? res) (eq? (car res) 'unimplemented))
         (grpc-respond-error! h GRPC-UNIMPLEMENTED
                              (string-append "unimplemented method: " (cdr res))))
        (else (grpc-respond-error! h GRPC-INTERNAL "handler produced no response")))))

  ; ---- Maintenance/Status + Cluster/MemberList stubs (schemas are at module top) ----
  ; Just enough for etcdctl's balancer to connect + `endpoint status` to print.
  (define (handle-status bytes)
    (let ((hdr (shard-header)))
      (cons 'ok (pb-encode StatusResponse-schema
                           (list (cons 'header hdr)
                                 (cons 'version "3.6.0")
                                 (cons 'dbSize 0)
                                 (cons 'leader member-id)
                                 (cons 'raftIndex (galist 'revision hdr 0))
                                 (cons 'raftTerm (galist 'raft_term hdr 0)))))))
  (define (handle-member-list bytes)
    (cons 'ok (pb-encode MemberListResponse-schema
                         (list (cons 'header (shard-header))
                               (cons 'members
                                     (list (list (cons 'ID member-id)
                                                 (cons 'name "crab-watchstore-0")
                                                 (cons 'clientURLs '())
                                                 (cons 'peerURLs '()))))))))

  ; ===========================================================================
  ; main loop: pure mailbox dispatch (unary inline + stream routing)
  ; ===========================================================================
  (let loop ()
    (let ((m (next-message)))
      (cond
        ((not (pair? m)) (loop))
        ((eq? (car m) '*grpc-request*)
         (dispatch! (cadr m)) (loop))
        ; a subsequent client-streamed message -> forward to the stream worker.
        ; A dead worker (it crashed / already exited) must NOT take down the
        ; dispatcher: guard the send and drop the stale route.
        ((eq? (car m) '*grpc-stream-msg*)
         (let ((w (hashtable-ref stream-workers (cadr m) #f)))
           (when w
             (guard (e (#t (hashtable-delete! stream-workers (cadr m))))
               (send w (list 'client-msg (caddr m))))))
         (loop))
        ; client half-closed -> tell the worker to tear down + drop the route
        ((eq? (car m) '*grpc-stream-end*)
         (let ((w (hashtable-ref stream-workers (cadr m) #f)))
           (when w
             (guard (e (#t #f)) (send w (list 'client-end)))
             (hashtable-delete! stream-workers (cadr m))))
         (loop))
        (else (loop))))))
