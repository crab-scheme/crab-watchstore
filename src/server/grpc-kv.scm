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
(define GRPC-INVALID-ARGUMENT  3)   ; ErrUserEmpty / ErrAuthFailed (etcd Auth, .26)
(define GRPC-PERMISSION-DENIED 7)   ; ErrPermissionDenied (the authz hook deny)
(define GRPC-FAILED-PRECONDITION 9) ; auth admin errors (user/role exists/not-found, root)
(define GRPC-OUT-OF-RANGE 11)   ; ErrCompacted surfaces here (etcd convention)
(define GRPC-UNIMPLEMENTED 12)
(define GRPC-UNAVAILABLE   14)   ; not-leader: client retries another endpoint
(define GRPC-INTERNAL      13)
(define GRPC-UNAUTHENTICATED 16) ; ErrInvalidAuthToken (present-but-bad token)

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
;   raftIndex=5, raftTerm=6, raftAppliedIndex=7, errors=8(rep string), dbSizeInUse=9}.
;   cw-u4a.32 fills the full set with REAL values so `etcdctl endpoint status` is honest.
(define StatusResponse-schema
  (list
    (list 1 'header           '(message ResponseHeader-schema-ref) 'optional)
    (list 2 'version          'string 'optional)
    (list 3 'dbSize           'int64  'optional)
    (list 4 'leader           'uint64 'optional)
    (list 5 'raftIndex        'uint64 'optional)
    (list 6 'raftTerm         'uint64 'optional)
    (list 7 'raftAppliedIndex 'uint64 'optional)
    (list 8 'errors           'string 'repeated)
    (list 9 'dbSizeInUse      'int64  'optional)))

; ---- Maintenance service request/response messages (cw-u4a.32; etcd rpc.proto fields) ----
; Hash / HashKV are keyspace-content checksums; a cross-member equality of the uint32 hash
; is a real corruption/consistency proof.  HashKVRequest.revision=0 => current rev.
(define HashRequest-schema '())
(define HashResponse-schema
  (list
    (list 1 'header '(message ResponseHeader-schema-ref) 'optional)
    (list 2 'hash   'uint32 'optional)))
(define HashKVRequest-schema
  '((1 revision int64 optional)))
(define HashKVResponse-schema
  (list
    (list 1 'header           '(message ResponseHeader-schema-ref) 'optional)
    (list 2 'hash             'uint32 'optional)
    (list 3 'compact_revision 'int64  'optional)
    (list 4 'hashRevision     'int64  'optional)))
; Defragment — empty request; header-only response.
(define DefragmentRequest-schema  '())
(define DefragmentResponse-schema
  (list (list 1 'header '(message ResponseHeader-schema-ref) 'optional)))
; Alarm — action(GET=0/ACTIVATE=1/DEACTIVATE=2), memberID, alarm(NONE=0/NOSPACE=1/CORRUPT=2).
; AlarmMember is the only NESTED schema here (registered below as AlarmMember-schema-ref).
(define AlarmMember-schema
  (list
    (list 1 'memberID 'uint64 'optional)
    (list 2 'alarm    'enum   'optional)))
(define AlarmRequest-schema
  (list
    (list 1 'action   'enum   'optional)
    (list 2 'memberID 'uint64 'optional)
    (list 3 'alarm    'enum   'optional)))
(define AlarmResponse-schema
  (list
    (list 1 'header '(message ResponseHeader-schema-ref) 'optional)
    (list 2 'alarms '(message AlarmMember-schema-ref)    'repeated)))
; MoveLeader{targetID} -> {header}.
(define MoveLeaderRequest-schema
  '((1 targetID uint64 optional)))
(define MoveLeaderResponse-schema
  (list (list 1 'header '(message ResponseHeader-schema-ref) 'optional)))

; ---- grpc.health.v1.Health (cw-u4a.33) — the STANDARD gRPC Health Checking Protocol ----
; (what grpc_health_probe / k8s liveness probes call; etcd serves it too.)  This is its OWN
; service (grpc.health.v1.Health), NOT etcdserverpb — but it rides the same client-port gRPC
; transport (any :path routes to this handler), so it is pure new Scheme, no Rust change.
;   HealthCheckRequest{service=1 string}
;   HealthCheckResponse{status=1 enum: UNKNOWN=0 SERVING=1 NOT_SERVING=2 SERVICE_UNKNOWN=3}
(define HealthCheckRequest-schema
  (list (list 1 'service 'string 'optional)))
(define HealthCheckResponse-schema
  (list (list 1 'status 'enum 'optional)))
(define HEALTH-SERVING     1)
(define HEALTH-NOT-SERVING 2)

; etcdserverpb.Member{ID=1, name=2, peerURLs=3(rep string), clientURLs=4(rep string),
;   isLearner=5(bool)}.  isLearner (cw-u4a.30) distinguishes a non-voting learner.
(define Member-schema
  (list
    (list 1 'ID         'uint64 'optional)
    (list 2 'name       'string 'optional)
    (list 3 'peerURLs   'string 'repeated)
    (list 4 'clientURLs 'string 'repeated)
    (list 5 'isLearner  'bool   'optional)))

; etcdserverpb.MemberListResponse{header=1, members=2(repeated Member)}.
(define MemberListResponse-schema
  (list
    (list 1 'header  '(message ResponseHeader-schema-ref) 'optional)
    (list 2 'members '(message Member-schema-ref)         'repeated)))

; ---- Cluster service request/response messages (cw-u4a.30; etcd rpc.proto fields) ----
; MemberAddResponse nests Member (member=2) + repeated Member (members=3); every other
; mutation/list response carries the full member list.  Member is the only NESTED schema
; here, so Member-schema-ref (registered below) is the only ref the codec must resolve —
; the *Response/*Request schemas are always TOP-LEVEL encode/decode targets.
(define MemberAddRequest-schema
  (list
    (list 1 'peerURLs  'string 'repeated)
    (list 2 'isLearner 'bool   'optional)))
(define MemberAddResponse-schema
  (list
    (list 1 'header  '(message ResponseHeader-schema-ref) 'optional)
    (list 2 'member  '(message Member-schema-ref)         'optional)
    (list 3 'members '(message Member-schema-ref)         'repeated)))
(define MemberRemoveRequest-schema
  (list (list 1 'ID 'uint64 'optional)))
(define MemberRemoveResponse-schema
  (list
    (list 1 'header  '(message ResponseHeader-schema-ref) 'optional)
    (list 2 'members '(message Member-schema-ref)         'repeated)))
(define MemberUpdateRequest-schema
  (list
    (list 1 'ID       'uint64 'optional)
    (list 2 'peerURLs 'string 'repeated)))
(define MemberUpdateResponse-schema
  (list
    (list 1 'header  '(message ResponseHeader-schema-ref) 'optional)
    (list 2 'members '(message Member-schema-ref)         'repeated)))
(define MemberPromoteRequest-schema
  (list (list 1 'ID 'uint64 'optional)))
(define MemberPromoteResponse-schema
  (list
    (list 1 'header  '(message ResponseHeader-schema-ref) 'optional)
    (list 2 'members '(message Member-schema-ref)         'repeated)))
; MemberListRequest{linearizable=1 bool} — parse-tolerant; the field is ignored (we
; always serve THIS node's replicated config view).
(define MemberListRequest-schema
  (list (list 1 'linearizable 'bool 'optional)))

; Register Member-schema-ref so the nested-message codec can resolve the repeated
; Member field.  proto.scm's deref-schema reads schema-ref-table at call time, so
; appending here (once, at include time) is additive and order-independent.
(set! schema-ref-table
      (cons (cons 'Member-schema-ref (lambda () Member-schema)) schema-ref-table))

; Register AlarmMember-schema-ref so the repeated `alarms` field (AlarmResponse) resolves
; (cw-u4a.32; same lazy-thunk append pattern as Member-schema-ref above).
(set! schema-ref-table
      (cons (cons 'AlarmMember-schema-ref (lambda () AlarmMember-schema)) schema-ref-table))

; ===========================================================================
; uint64 member-ID <-> node-name bijection (cw-u4a.30) — deterministic, stateless.
; ===========================================================================
; name->id is an FNV-1a fold of the node-name string into a positive integer in
; [1, 1e9] (kept well under 2^32 to dodge the i64-wrap in bitwise/shift, cw-u4a.41;
; etcdctl only displays it as hex and round-trips it).  It is IDENTICAL to
; node-cluster.scm's stable-id, so a node's own member-id spawn arg equals
; (member-name->id its-own-name) — the local member's MemberList ID matches the
; ResponseHeader member_id.  Collisions across ~5-20 short names cannot occur (the
; cluster test asserts the IDs are distinct).
(define (member-name->id name-str)
  (let loop ((i 0) (h 2166136261))
    (if (= i (string-length name-str))
        (+ 1 (modulo h 1000000000))
        (loop (+ i 1)
              (modulo (* (bitwise-xor h (char->integer (string-ref name-str i))) 16777619)
                      4294967296)))))

; id -> node-name SYMBOL, scanning a candidate name set (voters U learners U the
; cluster-spec names) for the one whose hash == id.  #f if none matches (a Remove/
; Promote/Update for an unknown ID -> a "member not found" gRPC error).
(define (member-id->name id candidates)
  (let loop ((cs candidates))
    (cond ((null? cs) #f)
          ((= (member-name->id (symbol->string (car cs))) id) (car cs))
          (else (loop (cdr cs))))))

; Take the node-name from a peerURL HOST (convention: "http://NAME:port" -> NAME;
; also tolerates "NAME:port" / "NAME").  etcd's MemberAddRequest carries no name, and
; in our model the node-name IS the cluster identity, so MemberAdd derives it here.
; -> NAME symbol, or #f for an empty/host-less URL.
(define (peer-url->name url)
  (let* ((rest (let scan ((i 0))                      ; strip a "scheme://" prefix
                 (cond ((> (+ i 3) (string-length url)) url)
                       ((and (char=? (string-ref url i) #\:)
                             (char=? (string-ref url (+ i 1)) #\/)
                             (char=? (string-ref url (+ i 2)) #\/))
                        (substring url (+ i 3) (string-length url)))
                       (else (scan (+ i 1))))))
         (end  (let scan ((i 0))                       ; up to the first ":port" / "/path"
                 (cond ((= i (string-length rest)) i)
                       ((or (char=? (string-ref rest i) #\:)
                            (char=? (string-ref rest i) #\/)) i)
                       (else (scan (+ i 1))))))
         (host (substring rest 0 end)))
    (if (= (string-length host) 0) #f (string->symbol host))))

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

(define (grpc-kv-main shard-pid cluster-id member-id cluster-members)
  ; cluster-members (cw-u4a.30): the static --cluster spec as a list of
  ; (name-string peerurl-string), e.g. (("a" "http://127.0.0.1:7001") ...).  Used by
  ; the Cluster service to report peerURLs in MemberList and as an extra id->name
  ; candidate source.  Runtime-added nodes (not in the spec) report an empty peerURL.

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
  ; Cluster service helpers (cw-u4a.30) — node-name <-> etcd Member, over the .29
  ; membership mailbox.  cluster-members supplies the static peerURLs.
  ; ===========================================================================

  ; node-name string -> its static peerURL (from the --cluster spec), or "" if the
  ; node was added at runtime (not in the spec; addresses are not modeled past .29).
  (define (name->peer-url name-str)
    (let ((c (assoc name-str cluster-members)))   ; string keys -> assoc/equal?
      (if c (cadr c) "")))

  ; the static cluster-spec node names (symbols) — extra id->name candidates.
  (define spec-names (map (lambda (e) (string->symbol (car e))) cluster-members))

  ; a name SYMBOL -> its peerURLs list ((url) or ()) for a Member message.
  (define (peer-urls-of name-sym)
    (let ((u (name->peer-url (symbol->string name-sym))))
      (if (> (string-length u) 0) (list u) '())))

  ; build one etcd Member alist for a node-name SYMBOL.  peer-urls is an explicit
  ; list of url strings (the requested URLs for a just-added node, else the spec's).
  (define (member-alist name-sym learner? peer-urls)
    (list (cons 'ID         (member-name->id (symbol->string name-sym)))
          (cons 'name       (symbol->string name-sym))
          (cons 'peerURLs   peer-urls)
          (cons 'clientURLs '())
          (cons 'isLearner  learner?)))

  ; voter + learner symbol lists -> the etcd `members` list (voters first, isLearner
  ; #f; learners next, isLearner #t), peerURLs from the static spec.
  (define (members-of voters learners)
    (append (map (lambda (n) (member-alist n #f (peer-urls-of n))) voters)
            (map (lambda (n) (member-alist n #t (peer-urls-of n))) learners)))

  ; read THIS node's replicated config (un-gated; (member-list voters learners)) and
  ; render it as the etcd `members` list.  Used by MemberList + MemberUpdate.
  (define (current-members)
    (let ((r (ask-shard (list 'member-list (self)))))
      (if (and (pair? r) (eq? (car r) 'member-list))
          (members-of (cadr r) (caddr r))
          '())))

  ; resolve an etcd member ID -> node-name SYMBOL, scanning voters U learners U the
  ; cluster-spec names.  #f if no candidate hashes to ID.
  (define (resolve-id->name id)
    (let* ((r (ask-shard (list 'member-list (self))))
           (cands (if (and (pair? r) (eq? (car r) 'member-list))
                      (append (cadr r) (caddr r) spec-names)
                      spec-names)))
      (member-id->name id cands)))

  ; Map a .29 member-* async outcome to (cons 'ok bytes) | (cons 'err (status . msg)).
  ; The mutating ops ack ASYNCHRONOUSLY only once the ConfChange COMMITS:
  ;   (member-ok VOTERS LEARNERS)  -> success; `build` renders the post-commit config.
  ;   (member-not-leader . LEADER) -> UNAVAILABLE(14), naming the leader (client retargets).
  ;   member-pending               -> FAILED_PRECONDITION(9), a change is already in flight.
  ;   member-indeterminate         -> UNAVAILABLE(14), leader stepped down mid-change; retry.
  ; (member-ok is a PROPER list -> cadr/caddr; member-not-leader is a DOTTED pair -> cdr.)
  (define (member-outcome->resp r build)
    (cond
      ((and (pair? r) (eq? (car r) 'member-ok))
       (cons 'ok (build (cadr r) (caddr r))))
      ((and (pair? r) (eq? (car r) 'member-not-leader))
       (cons 'err (cons GRPC-UNAVAILABLE
                        (if (cdr r)
                            (string-append "etcdserver: not leader, leader is "
                                           (symbol->string (cdr r)))
                            "etcdserver: not leader"))))
      ((eq? r 'member-pending)
       (cons 'err (cons GRPC-FAILED-PRECONDITION
                        "etcdserver: cluster reconfiguration in progress")))
      ((eq? r 'member-indeterminate)
       (cons 'err (cons GRPC-UNAVAILABLE "etcdserver: leader changed, retry")))
      (else (cons 'err (cons GRPC-INTERNAL "member: unexpected ack")))))

  ; ===========================================================================
  ; Auth enforcement (cw-u4a.26, ADR 0004 §4/§5) — the leader-local token table +
  ; identity resolution + the per-request authorization hook.
  ; ===========================================================================
  ; The replicated auth STATE (users/roles/enabled/auth-rev) lives in the shard's
  ; NS-AUTH; THIS actor owns only the ephemeral token table — etcd's `simple` token:
  ; leader-local, dropped on restart, the client just re-Authenticates.

  ; token-string -> (username-string . issue-auth-rev).  string=? keyed so a token
  ; read from request metadata (a fresh allocation) matches the one minted here.
  (define token-table (make-hashtable string-hash string=?))

  ; Resolve THIS request's identity (ADR §4 precedence: token header wins, else the
  ; verified client-cert CN, else none).  cur-auth-rev gates token staleness: an auth
  ; mutation bumps auth-rev, so a token issued at an older rev is rejected (re-auth).
  ;   -> (cons 'user name-str) | 'bad-token | 'none
  (define (resolve-identity h cur-auth-rev)
    (let ((tok (grpc-request-metadata h "token")))
      (if (and tok (> (string-length tok) 0))
          (let ((entry (hashtable-ref token-table tok #f)))
            (if (and entry (>= (cdr entry) cur-auth-rev))
                (cons 'user (car entry))
                'bad-token))                          ; unknown / stale token -> re-auth
          (let ((peer (grpc-request-peer-identity h)))
            (if peer (cons 'user peer) 'none)))))     ; cert-CN, else unauthenticated

  ; Resolve identity against the CURRENT replicated auth state.  Returns one of:
  ;   'disabled                  auth is OFF  -> the caller allows everything
  ;   (cons 'user name-str)      authenticated as name
  ;   (cons 'err (status . msg)) deny (etcd-faithful: empty name vs bad token)
  (define (auth-identify h)
    (let ((s (ask-shard (list 'auth-state (self)))))
      (if (or (not (pair? s)) (not (eq? (car s) 'auth-state-ok)) (not (cadr s)))
          'disabled                                   ; auth off (or seam down) -> allow
          (let ((id (resolve-identity h (caddr s))))
            (cond
              ((eq? id 'bad-token)
               (cons 'err (cons GRPC-UNAUTHENTICATED "etcdserver: invalid auth token")))
              ((eq? id 'none)
               (cons 'err (cons GRPC-INVALID-ARGUMENT "etcdserver: user name is empty")))
              (else id))))))

  ; Authorize one (key, range-end) for `required` ('read|'write) as user NAME, via
  ; the shard's pure auth-authorize? over NS-AUTH.  -> #f (allow) | (status . msg).
  (define (authorize-key name key rend required)
    (let ((d (ask-shard (list 'auth-authorize (self) (string->utf8 name) key rend required))))
      (if (and (pair? d) (eq? (car d) 'auth-authorize-ok) (cadr d))
          #f
          (cons GRPC-PERMISSION-DENIED "etcdserver: permission denied"))))

  ; The single-op KV guard: -> #f (allow, proceed) | (status . msg) (deny -> gRPC error).
  (define (authz-deny? h key rend required)
    (let ((id (auth-identify h)))
      (cond
        ((eq? id 'disabled) #f)
        ((eq? (car id) 'err) (cdr id))
        (else (authorize-key (cdr id) key rend required)))))

  ; Txn guard: each compare key -> READ, each inner op by its type (put/delete-range
  ; -> WRITE, range -> READ).  A nested request_txn is left to its own evaluation.
  (define (txn-authz-deny? h tr)
    (let ((id (auth-identify h)))
      (cond
        ((eq? id 'disabled) #f)
        ((eq? (car id) 'err) (cdr id))
        (else
         (let ((name (cdr id)))
           (define (rend-of m) (let ((re (galist 'range_end m EMPTY)))
                                 (if (= (bytevector-length re) 0) #f re)))
           (define (deny-cmps cs)
             (let loop ((cs cs))
               (if (null? cs) #f
                   (or (authorize-key name (galist 'key (car cs) EMPTY) #f 'read)
                       (loop (cdr cs))))))
           (define (deny-ops os)
             (let loop ((os os))
               (if (null? os) #f
                   (let* ((op (car os))
                          (put (galist 'request_put op #f))
                          (del (galist 'request_delete_range op #f))
                          (rng (galist 'request_range op #f))
                          (d (cond
                               (put (authorize-key name (galist 'key put EMPTY) #f 'write))
                               (del (authorize-key name (galist 'key del EMPTY) (rend-of del) 'write))
                               (rng (authorize-key name (galist 'key rng EMPTY) (rend-of rng) 'read))
                               (else #f))))
                     (or d (loop (cdr os)))))))
           (or (deny-cmps (galist 'compare tr '()))
               (deny-ops  (galist 'success tr '()))
               (deny-ops  (galist 'failure tr '()))))))))

  ; Admin (root-role) guard for the Auth MANAGEMENT RPCs.  Bootstrap (auth disabled)
  ; is allowed so the first root user/role can be created; once enabled, the request
  ; must present a root-role identity (etcd requires admin for user/role mutations).
  ;   -> #f (allow) | (status . msg).
  (define (admin-deny? h)
    (let ((id (auth-identify h)))
      (cond
        ((eq? id 'disabled) #f)
        ((eq? (car id) 'err) (cdr id))
        (else
         (let ((lk (ask-shard (list 'auth-lookup (self) (string->utf8 (cdr id))))))
           (if (and (pair? lk) (eq? (car lk) 'auth-lookup-ok) (list-ref lk 3)) ; admin? flag
               #f
               (cons GRPC-PERMISSION-DENIED "etcdserver: permission denied")))))))

  ; ===========================================================================
  ; method handlers — each returns (cons 'ok response-bytes)
  ;                              or (cons 'err (cons status "message"))
  ; ===========================================================================

  ; ---- KV/Range ----
  ; Decode RangeRequest, route the read through the shard's ReadIndex/leader gate,
  ; build RangeResponse{header, kvs, more, count}.  ErrCompacted -> OutOfRange(11).
  (define (handle-range h bytes)
    (let* ((rr   (pb-decode RangeRequest-schema bytes))
           (opts (range-request->opts rr))
           (deny (authz-deny? h (galist 'key opts EMPTY) (galist 'range-end opts #f) 'read)))
      (if deny (cons 'err deny)
      (let ((res (shard-range opts)))
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
        (else (cons 'err (cons GRPC-UNAVAILABLE "etcdserver: not leader")))))))) ; +let +if (authz)

  ; ---- KV/Put ----
  ; prev_kv snapshot (if requested) -> propose ("PUT" key value lease) -> PutResponse.
  ; lease is decimal-ASCII bytes (apply-fn convention).  A put to a dead lease comes
  ; back (cons 'err-lease-not-found id) -> a gRPC error (FailedPrecondition 9, etcd's).
  (define (handle-put h bytes)
    (let* ((pr    (pb-decode PutRequest-schema bytes))
           (key   (galist 'key pr EMPTY))
           (value (galist 'value pr EMPTY))
           (lease (galist 'lease pr 0))
           (want-prev (galist 'prev_kv pr #f))
           (deny  (authz-deny? h key #f 'write)))
      (if deny (cons 'err deny)
      (let* ((prev  (and want-prev (shard-prev key)))
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
        (else (cons 'err (cons GRPC-INTERNAL (string-append "put: unexpected ack")))))))))

  ; ---- KV/DeleteRange ----
  ; prev_kvs snapshot (if requested) -> propose ("DEL" key range-end) -> DeleteRangeResponse.
  (define (handle-delete-range h bytes)
    (let* ((dr    (pb-decode DeleteRangeRequest-schema bytes))
           (key   (galist 'key dr EMPTY))
           (rend  (let ((re (galist 'range_end dr EMPTY)))
                    (if (= (bytevector-length re) 0) #f re)))
           (deny  (authz-deny? h key rend 'write)))
      (if deny (cons 'err deny)
      (let* ((want-prev (galist 'prev_kv dr #f))
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
        (else (cons 'err (cons GRPC-INTERNAL "delete-range: unexpected ack")))))))) ; +let* +if (authz)

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
  (define (handle-txn h bytes)
    (let* ((tr   (pb-decode TxnRequest-schema bytes))
           (deny (txn-authz-deny? h tr)))   ; compares=READ, inner put/del=WRITE, range=READ
      (if deny (cons 'err deny)
      (let* ((itxn  (etcd-txn->internal tr))
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
        (else (cons 'err (cons GRPC-INTERNAL "txn: unexpected ack"))))))))

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
  ; etcd Auth service (cw-u4a.26 SUBSET) — enforcement + the MINIMUM RPCs that
  ; drive the etcdctl auth flow.  Each mutation PROPOSES an AUTH-* command through
  ; the shard (mvcc-apply writes NS-AUTH + bumps auth-rev), then maps the apply
  ; ack to an etcd-faithful gRPC status.  Authenticate runs the password verify +
  ; mints the leader-local token.  (UserDelete/ChangePassword/Get/List, RoleDelete/
  ; Get/List/RevokePermission, UserRevokeRole = cw-u4a.27.)
  ; ===========================================================================

  ; Map an AUTH-* apply ack -> (cons 'ok resp-bytes) | (cons 'err (status . msg)).
  ; SCHEMA is the (header-only) success response message for this RPC.
  (define (auth-write-ack->resp ack schema)
    (cond
      ((and (pair? ack) (string? (car ack)) (string=? (car ack) "AUTH-OK"))
       (cons 'ok (pb-encode schema (list (cons 'header (shard-header))))))
      ((and (pair? ack) (eq? (car ack) 'err-root-user-not-exist))
       (cons 'err (cons GRPC-FAILED-PRECONDITION "etcdserver: root user does not exist")))
      ((and (pair? ack) (eq? (car ack) 'err-root-role-not-exist))
       (cons 'err (cons GRPC-FAILED-PRECONDITION "etcdserver: root user does not have root role")))
      ((and (pair? ack) (eq? (car ack) 'err-user-exists))
       (cons 'err (cons GRPC-FAILED-PRECONDITION "etcdserver: user name already exists")))
      ((and (pair? ack) (eq? (car ack) 'err-user-not-found))
       (cons 'err (cons GRPC-FAILED-PRECONDITION "etcdserver: user name not found")))
      ((and (pair? ack) (eq? (car ack) 'err-role-exists))
       (cons 'err (cons GRPC-FAILED-PRECONDITION "etcdserver: role name already exists")))
      ((and (pair? ack) (eq? (car ack) 'err-role-not-found))
       (cons 'err (cons GRPC-FAILED-PRECONDITION "etcdserver: role name not found")))
      ((eq? ack 'tryagain)      (cons 'err (cons GRPC-UNAVAILABLE "etcdserver: not leader")))
      ((eq? ack 'indeterminate) (cons 'err (cons GRPC-UNAVAILABLE "etcdserver: leader changed")))
      (else (cons 'err (cons GRPC-INTERNAL "auth: unexpected ack")))))

  ; ---- Auth/AuthEnable / AuthDisable ----  admin-gated (bootstrap allowed while off).
  (define (handle-auth-enable h bytes)
    (let ((deny (admin-deny? h)))
      (if deny (cons 'err deny)
          (auth-write-ack->resp (shard-write (list (string->utf8 "AUTH-ENABLE")))
                                AuthEnableResponse-schema))))
  (define (handle-auth-disable h bytes)
    (let ((deny (admin-deny? h)))
      (if deny (cons 'err deny)
          (auth-write-ack->resp (shard-write (list (string->utf8 "AUTH-DISABLE")))
                                AuthDisableResponse-schema))))

  ; ---- Auth/Authenticate ----  verify the Argon2id PHC on the leader, mint a token.
  ; NO auth gate (this is how a client GETS a token).  ErrAuthFailed on bad user/pw.
  (define (handle-authenticate h bytes)
    (let* ((req  (pb-decode AuthenticateRequest-schema bytes))
           (name (galist 'name req ""))
           (pw   (galist 'password req ""))
           (lk   (ask-shard (list 'auth-lookup (self) (string->utf8 name)))))
      (if (and (pair? lk) (eq? (car lk) 'auth-lookup-ok) (cadr lk)         ; user exists?
               (crypto-password-verify pw (utf8->string (caddr lk))))      ; PHC verifies?
          (let ((token (crypto-random-token 24))
                (auth-rev (list-ref lk 4)))
            (hashtable-set! token-table token (cons name auth-rev))
            (cons 'ok (pb-encode AuthenticateResponse-schema
                                 (list (cons 'header (shard-header)) (cons 'token token)))))
          (cons 'err (cons GRPC-INVALID-ARGUMENT
                           "etcdserver: authentication failed, invalid user ID or password")))))

  ; ---- Auth/UserAdd ----  the leader hashes (Argon2id) + proposes the PHC bytes (ADR §3).
  (define (handle-user-add h bytes)
    (let ((deny (admin-deny? h)))
      (if deny (cons 'err deny)
          (let* ((req  (pb-decode AuthUserAddRequest-schema bytes))
                 (name (galist 'name req ""))
                 (pw   (galist 'password req ""))
                 ; empty password => no-password user (cert-CN only): store an empty hash.
                 (hash (if (> (string-length pw) 0)
                           (string->utf8 (crypto-password-hash pw))
                           EMPTY)))
            (auth-write-ack->resp
             (shard-write (list (string->utf8 "AUTH-USER-ADD") (string->utf8 name) hash))
             AuthUserAddResponse-schema)))))

  ; ---- Auth/RoleAdd ----
  (define (handle-role-add h bytes)
    (let ((deny (admin-deny? h)))
      (if deny (cons 'err deny)
          (let* ((req  (pb-decode AuthRoleAddRequest-schema bytes))
                 (name (galist 'name req "")))
            (auth-write-ack->resp
             (shard-write (list (string->utf8 "AUTH-ROLE-ADD") (string->utf8 name)))
             AuthRoleAddResponse-schema)))))

  ; ---- Auth/RoleGrantPermission ----  perm = authpb.Permission{permType,key,range_end}.
  (define (handle-role-grant-perm h bytes)
    (let ((deny (admin-deny? h)))
      (if deny (cons 'err deny)
          (let* ((req   (pb-decode AuthRoleGrantPermissionRequest-schema bytes))
                 (role  (galist 'name req ""))
                 (perm  (galist 'perm req '()))
                 (ptype (galist 'permType perm 0))           ; READ=0/WRITE=1/READWRITE=2
                 (key   (galist 'key perm EMPTY))
                 (rend  (galist 'range_end perm EMPTY)))
            (auth-write-ack->resp
             (shard-write (list (string->utf8 "AUTH-ROLE-GRANT-PERM")
                                (string->utf8 role) (int->bytes ptype) key rend))
             AuthRoleGrantPermissionResponse-schema)))))

  ; ---- Auth/UserGrantRole ----
  (define (handle-user-grant-role h bytes)
    (let ((deny (admin-deny? h)))
      (if deny (cons 'err deny)
          (let* ((req  (pb-decode AuthUserGrantRoleRequest-schema bytes))
                 (user (galist 'user req ""))
                 (role (galist 'role req "")))
            (auth-write-ack->resp
             (shard-write (list (string->utf8 "AUTH-USER-GRANT-ROLE")
                                (string->utf8 user) (string->utf8 role)))
             AuthUserGrantRoleResponse-schema)))))

  ; ===========================================================================
  ; Auth management RPCs (cw-u4a.27) — the remaining 9 Auth service methods.
  ; Mutations admin-gated + propose via shard-write (mirroring .26).
  ; Reads (UserGet/UserList/RoleGet/RoleList) admin-gated + served from shard seams.
  ; ===========================================================================

  ; ---- Auth/UserDelete ----
  (define (handle-user-delete h bytes)
    (let ((deny (admin-deny? h)))
      (if deny (cons 'err deny)
          (let* ((req  (pb-decode AuthUserDeleteRequest-schema bytes))
                 (name (galist 'name req "")))
            (auth-write-ack->resp
             (shard-write (list (string->utf8 "AUTH-USER-DELETE") (string->utf8 name)))
             AuthUserDeleteResponse-schema)))))

  ; ---- Auth/UserChangePassword ---- hash on the leader (Argon2id) + propose AUTH-USER-CHPASS.
  (define (handle-user-change-password h bytes)
    (let ((deny (admin-deny? h)))
      (if deny (cons 'err deny)
          (let* ((req  (pb-decode AuthUserChangePasswordRequest-schema bytes))
                 (name (galist 'name req ""))
                 (pw   (galist 'password req ""))
                 (hash (if (> (string-length pw) 0)
                           (string->utf8 (crypto-password-hash pw))
                           EMPTY)))
            (auth-write-ack->resp
             (shard-write (list (string->utf8 "AUTH-USER-CHPASS") (string->utf8 name) hash))
             AuthUserChangePasswordResponse-schema)))))

  ; ---- Auth/UserGet ---- admin-gated read; returns repeated role-name strings.
  ; AuthUserGetResponse{header=1, roles=2(repeated string)} — no nested User message.
  (define (handle-user-get h bytes)
    (let ((deny (admin-deny? h)))
      (if deny (cons 'err deny)
          (let* ((req  (pb-decode AuthUserGetRequest-schema bytes))
                 (name (galist 'name req ""))
                 (r    (ask-shard (list 'auth-user-info (self) (string->utf8 name)))))
            (if (and (pair? r) (eq? (car r) 'auth-user-info-ok) (cadr r))
                (let ((roles (map utf8->string (caddr r))))
                  (cons 'ok (pb-encode AuthUserGetResponse-schema
                                       (list (cons 'header (shard-header))
                                             (cons 'roles roles)))))
                (cons 'err (cons GRPC-FAILED-PRECONDITION "etcdserver: user name not found")))))))

  ; ---- Auth/UserList ---- admin-gated read; returns list of user names.
  (define (handle-user-list h bytes)
    (let ((deny (admin-deny? h)))
      (if deny (cons 'err deny)
          (let ((r (ask-shard (list 'auth-user-list (self)))))
            (if (and (pair? r) (eq? (car r) 'auth-user-list-ok))
                (cons 'ok (pb-encode AuthUserListResponse-schema
                                     (list (cons 'header (shard-header))
                                           (cons 'users (map utf8->string (cadr r))))))
                (cons 'err (cons GRPC-INTERNAL "user-list: unexpected ack")))))))

  ; ---- Auth/RoleDelete ----
  (define (handle-role-delete h bytes)
    (let ((deny (admin-deny? h)))
      (if deny (cons 'err deny)
          (let* ((req  (pb-decode AuthRoleDeleteRequest-schema bytes))
                 (role (galist 'role req "")))
            (auth-write-ack->resp
             (shard-write (list (string->utf8 "AUTH-ROLE-DELETE") (string->utf8 role)))
             AuthRoleDeleteResponse-schema)))))

  ; ---- Auth/RoleGet ---- admin-gated read; returns repeated Permission entries.
  ; AuthRoleGetResponse{header=1, perm=2(repeated Permission)} — no nested Role message.
  ; perm is (ptype key rend) list (shard seam converts #(ptype key rend) -> list for
  ; cross-runtime sendability).
  (define (handle-role-get h bytes)
    (let ((deny (admin-deny? h)))
      (if deny (cons 'err deny)
          (let* ((req  (pb-decode AuthRoleGetRequest-schema bytes))
                 (name (galist 'role req ""))
                 (r    (ask-shard (list 'auth-role-info (self) (string->utf8 name)))))
            (if (and (pair? r) (eq? (car r) 'auth-role-info-ok) (cadr r))
                (let ((perms (caddr r)))
                  (cons 'ok (pb-encode AuthRoleGetResponse-schema
                                       (list (cons 'header (shard-header))
                                             (cons 'perm
                                                   (map (lambda (p)
                                                          (list (cons 'permType (car p))
                                                                (cons 'key      (cadr p))
                                                                (cons 'range_end (caddr p))))
                                                        perms))))))
                (cons 'err (cons GRPC-FAILED-PRECONDITION "etcdserver: role name not found")))))))

  ; ---- Auth/RoleList ---- admin-gated read; returns list of role names.
  (define (handle-role-list h bytes)
    (let ((deny (admin-deny? h)))
      (if deny (cons 'err deny)
          (let ((r (ask-shard (list 'auth-role-list (self)))))
            (if (and (pair? r) (eq? (car r) 'auth-role-list-ok))
                (cons 'ok (pb-encode AuthRoleListResponse-schema
                                     (list (cons 'header (shard-header))
                                           (cons 'roles (map utf8->string (cadr r))))))
                (cons 'err (cons GRPC-INTERNAL "role-list: unexpected ack")))))))

  ; ---- Auth/RoleRevokePermission ----
  (define (handle-role-revoke-perm h bytes)
    (let ((deny (admin-deny? h)))
      (if deny (cons 'err deny)
          (let* ((req  (pb-decode AuthRoleRevokePermissionRequest-schema bytes))
                 (role (galist 'role req ""))
                 (key  (galist 'key req EMPTY))
                 (rend (galist 'range_end req EMPTY)))
            (auth-write-ack->resp
             (shard-write (list (string->utf8 "AUTH-ROLE-REVOKE-PERM")
                                (string->utf8 role) key rend))
             AuthRoleRevokePermissionResponse-schema)))))

  ; ---- Auth/UserRevokeRole ----
  (define (handle-user-revoke-role h bytes)
    (let ((deny (admin-deny? h)))
      (if deny (cons 'err deny)
          (let* ((req  (pb-decode AuthUserRevokeRoleRequest-schema bytes))
                 (name (galist 'name req ""))
                 (role (galist 'role req "")))
            (auth-write-ack->resp
             (shard-write (list (string->utf8 "AUTH-USER-REVOKE-ROLE")
                                (string->utf8 name) (string->utf8 role)))
             AuthUserRevokeRoleResponse-schema)))))

  ; Watch authz (READ): peek the first WatchRequest's create_request range + authorize,
  ; so an unauthorised Watch is rejected before a stream worker is spawned.
  ;   -> #f (allow) | (status . msg).
  (define (watch-authz-deny? h)
    (let ((wr (guard (e (#t #f)) (pb-decode WatchRequest-schema (grpc-request-bytes h)))))
      (let ((cr (and wr (galist 'create_request wr #f))))
        (if (not cr) #f                         ; cancel/progress -> the worker handles it
            (authz-deny? h (galist 'key cr EMPTY)
                         (let ((re (galist 'range_end cr EMPTY)))
                           (if (= (bytevector-length re) 0) #f re))
                         'read)))))

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
    (let ((path (grpc-request-path h))
          (peer (grpc-request-peer-identity h)))
      ; cw-u4a.21: surface the verified mTLS client identity (or #f over h2c /
      ; a TLS connection with no client cert).  This is the hook etcd Auth (.26)
      ; maps to a user; here we only LOG it so the mTLS proof can assert the
      ; server saw the client's SAN/CN.
      (when peer
        (display "grpc-kv: mTLS peer ") (display peer)
        (display " -> ") (display path) (newline))
      (cond
        ((string=? path "/etcdserverpb.Watch/Watch")
         ; Watch is a READ: enforce the per-request authz hook before spawning the
         ; stream worker (.26).  On deny, close the call with the gRPC error directly.
         (let ((deny (watch-authz-deny? h)))
           (if deny
               (grpc-respond-error! h (car deny) (cdr deny))
               (start-stream-worker! h 'grpc-watch-worker))))
        ((string=? path "/etcdserverpb.Lease/LeaseKeepAlive")
         (start-stream-worker! h 'grpc-lease-keepalive-worker))
        ; Maintenance/Snapshot (cw-u4a.32) is SERVER-STREAMING: one request -> many
        ; response chunks -> close.  Spawn the snapshot stream worker exactly like Watch.
        ((string=? path "/etcdserverpb.Maintenance/Snapshot")
         (start-stream-worker! h 'grpc-snapshot-worker))
        ; grpc.health.v1.Health/Watch (cw-u4a.33) is SERVER-STREAMING: emit the current
        ; serving status, then hold the stream until the client half-closes (the standard
        ; gRPC Health Watch).  Spawn the health-watch stream worker exactly like Snapshot.
        ((string=? path "/grpc.health.v1.Health/Watch")
         (start-stream-worker! h 'grpc-health-watch-worker))
        (else (dispatch-unary! h path)))))

  (define (dispatch-unary! h path)
    (let* ((bytes (grpc-request-bytes h))
           (res   (guard (e (#t (cons 'err (cons GRPC-INTERNAL
                                                 (string-append "handler error")))))
                    (cond
                      ; KV ops carry the per-request authz hook (.26): h flows in so the
                      ; handler resolves identity -> auth-authorize? -> deny mapping.
                      ((string=? path "/etcdserverpb.KV/Range")       (handle-range h bytes))
                      ((string=? path "/etcdserverpb.KV/Put")         (handle-put h bytes))
                      ((string=? path "/etcdserverpb.KV/DeleteRange") (handle-delete-range h bytes))
                      ((string=? path "/etcdserverpb.KV/Txn")         (handle-txn h bytes))
                      ((string=? path "/etcdserverpb.KV/Compact")     (handle-compact bytes))
                      ; Auth service (cw-u4a.26 subset): enforcement + the minimum RPCs.
                      ((string=? path "/etcdserverpb.Auth/AuthEnable")          (handle-auth-enable h bytes))
                      ((string=? path "/etcdserverpb.Auth/AuthDisable")         (handle-auth-disable h bytes))
                      ((string=? path "/etcdserverpb.Auth/Authenticate")        (handle-authenticate h bytes))
                      ((string=? path "/etcdserverpb.Auth/UserAdd")             (handle-user-add h bytes))
                      ((string=? path "/etcdserverpb.Auth/RoleAdd")             (handle-role-add h bytes))
                      ((string=? path "/etcdserverpb.Auth/RoleGrantPermission") (handle-role-grant-perm h bytes))
                      ((string=? path "/etcdserverpb.Auth/UserGrantRole")       (handle-user-grant-role h bytes))
                      ; Auth management RPCs (cw-u4a.27): mutating + read.
                      ((string=? path "/etcdserverpb.Auth/UserDelete")           (handle-user-delete h bytes))
                      ((string=? path "/etcdserverpb.Auth/UserChangePassword")   (handle-user-change-password h bytes))
                      ((string=? path "/etcdserverpb.Auth/UserGet")              (handle-user-get h bytes))
                      ((string=? path "/etcdserverpb.Auth/UserList")             (handle-user-list h bytes))
                      ((string=? path "/etcdserverpb.Auth/RoleDelete")           (handle-role-delete h bytes))
                      ((string=? path "/etcdserverpb.Auth/RoleGet")              (handle-role-get h bytes))
                      ((string=? path "/etcdserverpb.Auth/RoleList")             (handle-role-list h bytes))
                      ((string=? path "/etcdserverpb.Auth/RoleRevokePermission") (handle-role-revoke-perm h bytes))
                      ((string=? path "/etcdserverpb.Auth/UserRevokeRole")       (handle-user-revoke-role h bytes))
                      ; Lease UNARY RPCs (cw-u4a.17/.18); KeepAlive is bidi (above).
                      ((string=? path "/etcdserverpb.Lease/LeaseGrant")      (handle-lease-grant bytes))
                      ((string=? path "/etcdserverpb.Lease/LeaseRevoke")     (handle-lease-revoke bytes))
                      ((string=? path "/etcdserverpb.Lease/LeaseTimeToLive") (handle-lease-ttl bytes))
                      ((string=? path "/etcdserverpb.Lease/LeaseLeases")     (handle-lease-leases bytes))
                      ; Cluster service (cw-u4a.30): real replicated membership over the
                      ; .29 mailbox.  MemberList reports THIS node's config; the four
                      ; mutating ops drive raft-propose-conf-change + block on the commit.
                      ((string=? path "/etcdserverpb.Cluster/MemberList")    (handle-member-list bytes))
                      ((string=? path "/etcdserverpb.Cluster/MemberAdd")     (handle-member-add bytes))
                      ((string=? path "/etcdserverpb.Cluster/MemberRemove")  (handle-member-remove bytes))
                      ((string=? path "/etcdserverpb.Cluster/MemberUpdate")  (handle-member-update bytes))
                      ((string=? path "/etcdserverpb.Cluster/MemberPromote") (handle-member-promote bytes))
                      ; Maintenance service (cw-u4a.32): Status/Hash/HashKV/Defragment/
                      ; Alarm/MoveLeader unary; Snapshot is server-streaming (in dispatch!).
                      ((string=? path "/etcdserverpb.Maintenance/Status")     (handle-status bytes))
                      ((string=? path "/etcdserverpb.Maintenance/Hash")       (handle-hash bytes))
                      ((string=? path "/etcdserverpb.Maintenance/HashKV")     (handle-hashkv bytes))
                      ((string=? path "/etcdserverpb.Maintenance/Defragment") (handle-defragment bytes))
                      ((string=? path "/etcdserverpb.Maintenance/Alarm")      (handle-alarm bytes))
                      ((string=? path "/etcdserverpb.Maintenance/MoveLeader") (handle-move-leader bytes))
                      ; grpc.health.v1.Health/Check (cw-u4a.33): standard gRPC health probe.
                      ((string=? path "/grpc.health.v1.Health/Check")         (handle-health-check h bytes))
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

  ; ===========================================================================
  ; Maintenance service handlers (cw-u4a.32) — Status/Hash/HashKV/Defragment/Alarm/
  ; MoveLeader (UNARY).  Snapshot is server-STREAMING (dispatch! -> grpc-snapshot-worker,
  ; server/grpc-watch.scm), like Watch.  Each handler returns the (cons 'ok bytes) /
  ; (cons 'err (status . msg)) protocol.  All read seams are un-gated, so these serve on
  ; ANY node (which is required for the cross-member hashkv consistency check).
  ; ===========================================================================

  ; ask the shard's .32 status seam -> (status-ok rev term commit applied db-size leader)
  (define (shard-status) (ask-shard (list 'status (self))))

  ; Alarm action enums (Raft-REPLICATED alarm set: cw-u4a.42).  The active set lives in
  ; NS-ALARM (mvcc.scm), written by ALARM-SET/ALARM-DISARM through Raft and read via the
  ; shard's un-gated alarm-list seam — see handle-alarm below.
  (define ALARM-ACTIVATE 1)
  (define ALARM-DEACTIVATE 2)

  ; ---- Maintenance/Status (UPGRADE of the .22 stub) ----
  ; Real raft scalars (raftIndex=commit, raftAppliedIndex=applied, raftTerm) + a LOGICAL
  ; db size (sum of keylen+valuelen over the live keyspace) from the shard's status seam,
  ; so `etcdctl endpoint status` shows REAL data per endpoint.  leader = the member-id of
  ; the Raft leader (name->id), 0 if this node currently sees no leader.  dbSize ==
  ; dbSizeInUse (no bbolt free-page notion).  Served on any node (un-gated seam).
  (define (handle-status bytes)
    (let ((r (shard-status)))
      (if (and (pair? r) (eq? (car r) 'status-ok))
          (let* ((rev    (list-ref r 1)) (term    (list-ref r 2))
                 (commit (list-ref r 3)) (applied (list-ref r 4))
                 (dbsize (list-ref r 5)) (ldr     (list-ref r 6))
                 (leader-id (if ldr (member-name->id (symbol->string ldr)) 0)))
            (cons 'ok (pb-encode StatusResponse-schema
                                 (list (cons 'header (make-header cluster-id member-id rev term))
                                       (cons 'version "3.6.0")
                                       (cons 'dbSize dbsize)
                                       (cons 'leader leader-id)
                                       (cons 'raftIndex commit)
                                       (cons 'raftTerm term)
                                       (cons 'raftAppliedIndex applied)
                                       (cons 'dbSizeInUse dbsize)))))
          (cons 'err (cons GRPC-INTERNAL "status: unexpected ack")))))

  ; ---- Maintenance/HashKV + Hash ----  deterministic 32-bit keyspace checksum (mvcc-digest-at).
  ; Un-gated: a cross-member `etcdctl endpoint hashkv` reaches every replica, each answering
  ; from its OWN committed ctx — identical hashes prove the replicas hold the identical
  ; committed keyspace.  Hash = HashKV at the current rev.
  (define (handle-hashkv bytes)
    (let* ((req (pb-decode HashKVRequest-schema bytes))
           (rev (galist 'revision req 0))
           (r   (ask-shard (list 'hashkv (self) rev))))
      (if (and (pair? r) (eq? (car r) 'hashkv-ok))
          (cons 'ok (pb-encode HashKVResponse-schema
                               (list (cons 'header (shard-header))
                                     (cons 'hash (list-ref r 1))
                                     (cons 'compact_revision (list-ref r 2))
                                     (cons 'hashRevision (list-ref r 3)))))
          (cons 'err (cons GRPC-INTERNAL "hashkv: unexpected ack")))))
  (define (handle-hash bytes)
    (let ((r (ask-shard (list 'hashkv (self) 0))))   ; Hash == HashKV at current rev
      (if (and (pair? r) (eq? (car r) 'hashkv-ok))
          (cons 'ok (pb-encode HashResponse-schema
                               (list (cons 'header (shard-header))
                                     (cons 'hash (list-ref r 1)))))
          (cons 'err (cons GRPC-INTERNAL "hash: unexpected ack")))))

  ; ---- Maintenance/Defragment ----  advisory RocksDB flush (memtables->SSTs + WAL fsync);
  ; RocksDB has NO bbolt-style page defragmentation, so this is the closest honest analogue.
  (define (handle-defragment bytes)
    (let ((r (ask-shard (list 'defrag (self)))))
      (if (and (pair? r) (eq? (car r) 'defrag-ok))
          (cons 'ok (pb-encode DefragmentResponse-schema
                               (list (cons 'header (shard-header)))))
          (cons 'err (cons GRPC-INTERNAL "defragment: unexpected ack")))))

  ; ---- Maintenance/Alarm ----  Raft-replicated alarm set (cw-u4a.42).  ACTIVATE (alarm >
  ; NONE) and DEACTIVATE propose ALARM-SET / ALARM-DISARM through Raft (leader-gated) so the
  ; alarm state is agreed by every member + survives restart; GET (action 0, or unknown)
  ; reads the replicated set from THIS node (un-gated).  AlarmResponse.alarms is always the
  ; full active set, read back via the shard's alarm-list seam.
  (define (alarm-members)            ; replicated NS-ALARM set -> AlarmMember alists
    (let ((r (ask-shard (list 'alarm-list (self)))))
      (if (and (pair? r) (eq? (car r) 'alarm-list-ok))
          (map (lambda (a) (list (cons 'memberID (car a)) (cons 'alarm (cdr a)))) (cadr r))
          '())))
  (define (alarm-resp)
    (cons 'ok (pb-encode AlarmResponse-schema
                         (list (cons 'header (shard-header))
                               (cons 'alarms (alarm-members))))))
  (define (alarm-write cmd)          ; propose ALARM-SET/DISARM; map the ack
    (let ((ack (shard-write cmd)))
      (cond
        ((and (pair? ack) (string? (car ack)) (string=? (car ack) "ALARM-OK")) (alarm-resp))
        ((eq? ack 'tryagain)      (cons 'err (cons GRPC-UNAVAILABLE "etcdserver: not leader")))
        ((eq? ack 'indeterminate) (cons 'err (cons GRPC-UNAVAILABLE "etcdserver: leader changed")))
        (else (cons 'err (cons GRPC-INTERNAL "alarm: unexpected ack"))))))
  (define (handle-alarm bytes)
    (let* ((req    (pb-decode AlarmRequest-schema bytes))
           (action (galist 'action req 0))
           (mid    (galist 'memberID req 0))
           (atype  (galist 'alarm req 0)))
      (cond
        ((and (= action ALARM-ACTIVATE) (> atype 0))
         (alarm-write (list (string->utf8 "ALARM-SET")    (int->bytes mid) (int->bytes atype))))
        ((= action ALARM-DEACTIVATE)
         (alarm-write (list (string->utf8 "ALARM-DISARM") (int->bytes mid) (int->bytes atype))))
        (else (alarm-resp)))))       ; GET: list the replicated active set

  ; ---- Maintenance/MoveLeader ----  real leadership transfer (cw-u4a.42) via the raft.scm
  ; TimeoutNow primitive.  Resolve targetID -> node-name, then ask THIS node's shard to
  ; transfer: the leader sends 'timeout-now to the (caught-up) target, which campaigns + wins.
  ; Must be invoked on the leader (a follower returns UNAVAILABLE not-leader so the client
  ; retargets).  targetID == the current leader is a trivial no-op success; a non-voter or a
  ; not-caught-up target is FailedPrecondition "bad leader transferee" (etcd's error).
  (define (handle-move-leader bytes)
    (let* ((req    (pb-decode MoveLeaderRequest-schema bytes))
           (target (galist 'targetID req 0))
           (tname  (resolve-id->name target)))
      (if (not tname)
          (cons 'err (cons 9 "etcdserver: bad leader transferee"))   ; unknown member ID
          (let ((ack (ask-shard (list 'move-leader (self) tname))))
            (cond
              ((eq? ack 'move-leader-ok)
               (cons 'ok (pb-encode MoveLeaderResponse-schema
                                    (list (cons 'header (shard-header))))))
              ; target IS the current leader -> already there (etcd treats this as success).
              ((and (pair? ack) (eq? (car ack) 'move-leader-err) (eq? (cdr ack) 'self))
               (cons 'ok (pb-encode MoveLeaderResponse-schema
                                    (list (cons 'header (shard-header))))))
              ((and (pair? ack) (eq? (car ack) 'move-leader-not-leader))
               (cons 'err (cons GRPC-UNAVAILABLE "etcdserver: not leader")))
              ; not-voter / not-caught-up -> FailedPrecondition, etcd's "bad leader transferee".
              ((and (pair? ack) (eq? (car ack) 'move-leader-err))
               (cons 'err (cons 9 "etcdserver: bad leader transferee")))
              (else (cons 'err (cons GRPC-INTERNAL "move-leader: unexpected ack"))))))))

  ; ===========================================================================
  ; grpc.health.v1.Health/Check (cw-u4a.33) — standard gRPC health protocol (UNARY)
  ; ===========================================================================
  ; SERVING iff this node is READY: it has a leader AND is initialized (applied >= commit,
  ; i.e. caught up to the committed log).  Otherwise NOT_SERVING.  etcd reports SERVING when
  ; the server can serve.  The request's `service` field is ignored — like etcd, we report
  ; overall server health (the readiness seam is endpoint-local, un-gated, served on any node).
  (define (node-serving?)
    (let ((r (shard-status)))   ; (status-ok rev term commit applied db-size leader key-count)
      (and (pair? r) (eq? (car r) 'status-ok)
           (list-ref r 6)                          ; has a leader (non-#f)
           (>= (list-ref r 4) (list-ref r 3)))))   ; applied >= commit (initialized)
  (define (handle-health-check h bytes)
    (cons 'ok (pb-encode HealthCheckResponse-schema
                         (list (cons 'status (if (node-serving?)
                                                 HEALTH-SERVING HEALTH-NOT-SERVING))))))

  ; ===========================================================================
  ; Cluster service handlers (cw-u4a.30)
  ; ===========================================================================

  ; ---- Cluster/MemberList ---- REAL replicated config (replaces the .22 stub).
  ; Reads THIS node's voters/learners (un-gated) and renders them as etcd Members.
  ; MemberListRequest.linearizable is ignored (parse-tolerant; config is local).
  (define (handle-member-list bytes)
    (cons 'ok (pb-encode MemberListResponse-schema
                         (list (cons 'header  (shard-header))
                               (cons 'members (current-members))))))

  ; ---- Cluster/MemberAdd ---- derive NODE-NAME from peerURLs[0]'s host, propose
  ; (member-add (self) name isLearner?), BLOCK on the async commit (etcd MemberAdd
  ; blocks until the conf change commits; the peer-poller keeps ticking the shard so
  ; it makes progress).  Response member = the new member (with the REQUESTED peerURLs);
  ; members = the full post-commit list.
  (define (handle-member-add bytes)
    (let* ((req       (pb-decode MemberAddRequest-schema bytes))
           (peer-urls (galist 'peerURLs req '()))
           (learner?  (galist 'isLearner req #f))
           (name      (peer-url->name (if (pair? peer-urls) (car peer-urls) ""))))
      (if (not name)
          (cons 'err (cons GRPC-INVALID-ARGUMENT "etcdserver: peerURL is required"))
          (member-outcome->resp
            (ask-shard (list 'member-add (self) name learner?))
            (lambda (voters learners)
              (pb-encode MemberAddResponse-schema
                         (list (cons 'header  (shard-header))
                               (cons 'member  (member-alist name learner? peer-urls))
                               (cons 'members (members-of voters learners)))))))))

  ; ---- Cluster/MemberRemove ---- ID -> name, propose (member-remove (self) name),
  ; block on commit; response = the full post-commit member list.
  (define (handle-member-remove bytes)
    (let* ((req  (pb-decode MemberRemoveRequest-schema bytes))
           (id   (galist 'ID req 0))
           (name (resolve-id->name id)))
      (if (not name)
          (cons 'err (cons GRPC-FAILED-PRECONDITION "etcdserver: member not found"))
          (member-outcome->resp
            (ask-shard (list 'member-remove (self) name))
            (lambda (voters learners)
              (pb-encode MemberRemoveResponse-schema
                         (list (cons 'header  (shard-header))
                               (cons 'members (members-of voters learners)))))))))

  ; ---- Cluster/MemberPromote ---- ID -> name, propose (member-promote (self) name)
  ; (learner -> voter, two-phase), block on commit; response = the full member list.
  (define (handle-member-promote bytes)
    (let* ((req  (pb-decode MemberPromoteRequest-schema bytes))
           (id   (galist 'ID req 0))
           (name (resolve-id->name id)))
      (if (not name)
          (cons 'err (cons GRPC-FAILED-PRECONDITION "etcdserver: member not found"))
          (member-outcome->resp
            (ask-shard (list 'member-promote (self) name))
            (lambda (voters learners)
              (pb-encode MemberPromoteResponse-schema
                         (list (cons 'header  (shard-header))
                               (cons 'members (members-of voters learners)))))))))

  ; ---- Cluster/MemberUpdate ---- node addresses are STATIC (from the --cluster spec),
  ; so a peerURL change is a no-op in the consensus layer (runtime addr-change is not a
  ; .29 capability).  We validate the ID resolves, then return the CURRENT member list
  ; WITHOUT fabricating a ConfChange.
  (define (handle-member-update bytes)
    (let* ((req  (pb-decode MemberUpdateRequest-schema bytes))
           (id   (galist 'ID req 0))
           (name (resolve-id->name id)))
      (if (not name)
          (cons 'err (cons GRPC-FAILED-PRECONDITION "etcdserver: member not found"))
          (cons 'ok (pb-encode MemberUpdateResponse-schema
                               (list (cons 'header  (shard-header))
                                     (cons 'members (current-members))))))))

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
