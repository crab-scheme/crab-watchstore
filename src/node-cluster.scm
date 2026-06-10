; node-cluster.scm — a crab-watchstore CLUSTER node (Phase 0 keystone). One
; process per node. The CONSENSUS SUBSTRATE is the deliverable here: there is NO
; client/RESP listener yet (cw-u4a.5/.6 add the etcd KV API on top).
;
;   crabscheme run src/node-cluster.scm -- \
;       --node a --db /tmp/cws-a \
;       --cluster a:127.0.0.1:7001:6001,b:127.0.0.1:7002:6002,c:127.0.0.1:7003:6003
;
; PORTED from crab-cache/src/node-cluster.scm. KEPT: the --cluster parse, the
; static voter set (= all node names), the node-make/listen/connect full-mesh
; bring-up, the spawn-source-dedicated shard replica + peer-poller. STRIPPED: the
; slotmap, the RESP client listener + conn actors, the pub/sub broker, the
; cc-config / MOVED routing, multi-shard fan-out (a single shard "0" is enough
; for Phase 0.4). The clientport in the --cluster spec is parsed but unused (it
; keeps the spec format identical to crab-cache for forward compatibility).
;
; Every node replicates the single shard "0", so it is an R-voter Raft group
; (R = #nodes). The node:
;   1. node-make + node-listen on its raft addr; dials higher-named peers (one
;      TCP connection per pair) and waits for the full mesh;
;   2. spawns the shard-replica (voters = all node names);
;   3. spawns the peer-poller (drains node-poll -> local replica; ticks).

(include "src/encoding.scm")

(define (arg-after flag default)
  (let loop ((a (command-line)))
    (cond ((or (null? a) (null? (cdr a))) default)
          ((string=? (car a) flag) (cadr a))
          (else (loop (cdr a))))))

(define (split-on s ch)
  (let loop ((i 0) (start 0) (acc '()))
    (cond ((= i (string-length s)) (reverse (cons (substring s start i) acc)))
          ((char=? (string-ref s i) ch) (loop (+ i 1) (+ i 1) (cons (substring s start i) acc)))
          (else (loop (+ i 1) start acc)))))

(define me      (string->symbol (arg-after "--node" "a")))
(define dbbase  (arg-after "--db" "/tmp/cws-node"))
(define durable (string=? (arg-after "--durable" "no") "yes"))
(define cluster-spec (arg-after "--cluster" "a:127.0.0.1:7001:6001"))

; ---- optional TLS / mutual-TLS for the etcd gRPC CLIENT port (cw-u4a.21) ----
; When --tls-cert is supplied the client port is served over TLS instead of
; cleartext h2c.  --tls-ca + --tls-require-client-cert (default "yes") turn on
; MUTUAL TLS: a client must present a certificate chaining to the CA, else it is
; rejected at the TLS layer.  The verified peer identity (SAN/CN) is then exposed
; to the KV handler via (grpc-request-peer-identity h) — the etcd-Auth hook (.26).
; Raft inter-node transport is unaffected (this is the client port only).
(define tls-cert (arg-after "--tls-cert" #f))
(define tls-key  (arg-after "--tls-key"  #f))
(define tls-ca   (arg-after "--tls-ca"   #f))
(define tls-require-client
  (not (string=? (arg-after "--tls-require-client-cert" "yes") "no")))

; parse "name:host:raftport:clientport,..." -> list of (name host raftport clientport)
(define nodes
  (map (lambda (e)
         (let ((p (split-on e #\:)))
           (list (string->symbol (car p)) (cadr p)
                 (string->number (caddr p)) (string->number (cadddr p)))))
       (split-on cluster-spec #\,)))

(define (node-field nm i)
  (let loop ((ns nodes)) (cond ((null? ns) #f) ((eqv? (caar ns) nm) (list-ref (car ns) i)) (else (loop (cdr ns))))))
(define (raft-addr nm) (string-append (node-field nm 1) ":" (number->string (node-field nm 2))))

(define all-names (map car nodes))

(make-table 'ws-shard-pid "set")
(make-table 'ws-shard-role "set")
(make-table 'ws-shard-leader "set")
(make-table 'ws-shard-commit "set")
(make-table 'ws-shard-applied "set")

; ---- bring up the inter-node mesh ----
(node-make (symbol->string me))
(node-listen (symbol->string me) (raft-addr me))

; dial only higher-named peers (one connection per pair); retry until up.
(define (sym>? a b) (string>? (symbol->string a) (symbol->string b)))
(define dial-peers (let loop ((ns all-names) (acc '()))
                     (cond ((null? ns) (reverse acc))
                           ((sym>? (car ns) me) (loop (cdr ns) (cons (car ns) acc)))
                           (else (loop (cdr ns) acc)))))
(define (try-connect addr) (guard (e (#t #f)) (node-connect (symbol->string me) addr) #t))
; On a fresh start the dial-higher-named handshakes form the mesh; on a RESTART,
; peers re-dial us via their peer-poller heal, so we just wait for the full peer
; count (be patient — the healing peer may be a tick or two away).
(let mesh ((tries 0))
  (for-each (lambda (nm) (try-connect (raft-addr nm))) dial-peers)
  (cond ((>= (node-peer-count (symbol->string me)) (- (length nodes) 1)) #t)
        ((> tries 200000000) (error "cluster: mesh did not form"))
        (else (mesh (+ tries 1)))))
(display "node ") (display me) (display ": mesh up (")
(display (node-peer-count (symbol->string me))) (display " peers)") (newline)

; ---- single shard "0": one replica, voters = all nodes ----
; Dedicated thread — blocking RocksDB fsync must not freeze a shared green
; worker (green-threads INV-2).
(spawn-source-dedicated "(include \"src/server/shard-actor.scm\")" 'shard-main
              "0" all-names me (string-append dbbase "-shard0") durable)

(define dial-addrs (map raft-addr dial-peers))
; Dedicated thread — the poller is the Raft tick-clock AND sole network drainer;
; cooperative parking on a shared worker would slow the protocol (green-threads
; INV-3).
(spawn-source-dedicated "(include \"src/server/peer-poller.scm\")" 'peer-poller
              me '("0") 120 dial-addrs (- (length nodes) 1))

; wait until this node has elected/learned a leader for shard 0, so the substrate
; is ready before we report up.
(define (qk) (string-append (symbol->string me) ":0"))
(let spin ()
  (if (table-lookup 'ws-shard-leader (qk)) #t (spin)))

(display "node ") (display me) (display ": shard 0 ready (leader=")
(display (table-lookup 'ws-shard-leader (qk))) (display ", role=")
(display (table-lookup 'ws-shard-role (qk))) (display ")") (newline)

; ---- etcd v3 KV gRPC service (cw-u4a.22) ----
; The clientport in the --cluster spec (long parsed-but-unused) is THIS node's etcd
; gRPC client endpoint.  Spawn the KV handler actor (src/server/grpc-kv.scm) against
; this node's shard-0 replica, then start the h2c gRPC server (cw-u4a.20) bound to
; host:clientport routing every etcd KV call (Range/Put/DeleteRange/Txn/Compact +
; minimal Status/MemberList stubs) to it.  A SINGLE-NODE cluster (1 voter) is always
; leader, so reads + writes serve here with no leader-forwarding (that is cw-u4a.24).
;
;   ResponseHeader cluster_id / member_id: deterministic, stable, nonzero values
;   derived from the node config (a hash of the cluster spec / the node name) — etcdctl
;   only needs them present + consistent across responses, not etcd-bit-exact.
;
; Dedicated thread: the handler does a blocking (raw-receive) between shard round-trips
; (it asks the shard for reads / prev-kv / current-rev and awaits the reply), so keep
; it off the shared green pool — same rationale as the shard + poller (green-threads
; INV-2/3).  grpc-serve registers the handler PID; we look up the shard PID (already
; published to ws-shard-pid by now) and pass it in.
(define (stable-id s)
  ; a small deterministic nonzero u32-ish id from a string (FNV-1a-style fold).
  (let loop ((i 0) (h 2166136261))
    (if (= i (string-length s))
        (+ 1 (modulo h 1000000000))             ; keep nonzero, bounded
        (loop (+ i 1)
              (modulo (* (bitwise-xor h (char->integer (string-ref s i))) 16777619)
                      4294967296)))))
(define cluster-id (stable-id cluster-spec))
(define member-id  (stable-id (symbol->string me)))
(define client-host (node-field me 1))
(define client-port (node-field me 3))
(define client-addr (string-append client-host ":" (number->string client-port)))
(define shard-pid   (table-lookup 'ws-shard-pid (qk)))

(define grpc-handler
  (spawn-source-dedicated "(include \"src/server/grpc-kv.scm\")" 'grpc-kv-main
                          shard-pid cluster-id member-id))
; TLS path (cw-u4a.21) iff --tls-cert was given; otherwise the original h2c
; server.  grpc-serve-tls reuses the SAME handler actor — TLS only wraps the IO.
(define grpc-sid
  (if tls-cert
      (grpc-serve-tls client-addr grpc-handler tls-cert tls-key tls-ca tls-require-client)
      (grpc-serve client-addr grpc-handler)))

; Banner: keep the exact "etcd KV gRPC serving on" substring for the h2c path
; (the existing etcd-kv-grpc.sh proof waits on it); add a "(mTLS)"/"(TLS)" tag
; only when TLS is on.
(display "node ") (display me) (display ": etcd KV gRPC ")
(when tls-cert
  (display "(") (display (if tls-require-client "mTLS" "TLS")) (display ") "))
(display "serving on ")
(display client-addr) (display " (cluster-id=") (display cluster-id)
(display " member-id=") (display member-id) (display ")") (newline)

; The consensus substrate + etcd KV API are up. Park so the node process stays alive
; serving Raft RPCs to peers and gRPC calls to clients.
(let park () (yield) (park))
