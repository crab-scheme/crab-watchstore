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

(define (cli-after flag default)
  (let loop ((a (command-line)))
    (cond ((or (null? a) (null? (cdr a))) default)
          ((string=? (car a) flag) (cadr a))
          (else (loop (cdr a))))))

(define (split-on s ch)
  (let loop ((i 0) (start 0) (acc '()))
    (cond ((= i (string-length s)) (reverse (cons (substring s start i) acc)))
          ((char=? (string-ref s i) ch) (loop (+ i 1) (+ i 1) (cons (substring s start i) acc)))
          (else (loop (+ i 1) start acc)))))

; ---- config file (cw-24e.1) ----
; `--config FILE` loads defaults for every flag from a plain `key value` file
; (one option per line; the key is the flag name without the leading "--";
; blank lines and `#` comments ignored).  CLI flags OVERRIDE config values, so
; `crab-watchstore --config a.conf --durable yes` works as expected.  Example:
;     node     a
;     db       /var/lib/crab-watchstore/a
;     cluster  a:10.0.0.1:7001:2379,b:10.0.0.2:7001:2379,c:10.0.0.3:7001:2379
;     durable  yes
(define config-alist
  (let ((path (cli-after "--config" #f)))
    (if (not path) '()
        (let ((p (open-input-file path)))
          (let loop ((acc '()))
            (let ((line (read-line p)))
              (if (eof-object? line)
                  (begin (close-port p) (reverse acc))
                  (let* ((trimmed (string-trim line))
                         (skip? (or (= 0 (string-length trimmed))
                                    (char=? (string-ref trimmed 0) #\#)))
                         (sp (and (not skip?)
                                  (let find ((i 0))
                                    (cond ((= i (string-length trimmed)) #f)
                                          ((or (char=? (string-ref trimmed i) #\space)
                                               (char=? (string-ref trimmed i) #\tab)) i)
                                          (else (find (+ i 1))))))))
                    (if (not sp)
                        (loop acc)
                        (loop (cons (cons (substring trimmed 0 sp)
                                          (string-trim (substring trimmed sp (string-length trimmed))))
                                    acc)))))))))))

; option lookup: CLI flag wins, then the config file, then the default.
(define (arg-after flag default)
  (let ((cli (cli-after flag #f)))
    (or cli
        (let ((hit (assoc (substring flag 2 (string-length flag)) config-alist)))
          (if hit (cdr hit) default)))))

(define me      (string->symbol (arg-after "--node" "a")))
(define dbbase  (arg-after "--db" "/tmp/cws-node"))
(define durable (string=? (arg-after "--durable" "no") "yes"))
(define cluster-spec (arg-after "--cluster" "a:127.0.0.1:7001:6001"))

; ---- dynamic-membership join (cw-u4a.29) ----
; `--join yes` brings this node up as a NON-VOTER that dials every existing member, then
; waits to be added to the group (MemberAdd -> learner/voter) and catches up by Raft log
; replication.  The --cluster spec must list ALL members INCLUDING this node (so the joiner
; knows every raft addr to dial).  Without --join the node is a fresh-cluster voter — the
; original Phase-0 behavior, byte-for-byte unchanged.  Join sequence:
;   1. start an existing cluster normally (no --join);
;   2. start the new node with --join yes + the full --cluster spec (it listens, dials all
;      existing members, brings up shard+poller+gRPC as a non-voter);
;   3. on the leader, MemberAdd the new node (as voter, or as learner then MemberPromote);
;   4. the engine replicates the prior log to it (catch-up), the two-phase change commits,
;      and it becomes a full voter — all over the live transport.
(define join? (string=? (arg-after "--join" "no") "yes"))


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
; parse "name:host:raftport:clientport[:region]" -> (name host raft client region)
; — the optional 5th field is the member's region (cw-lkq.7); #f when absent.
(define nodes
  (map (lambda (e)
         (let ((p (split-on e #\:)))
           (list (string->symbol (car p)) (cadr p)
                 (string->number (caddr p)) (string->number (cadddr p))
                 (if (>= (length p) 5) (list-ref p 4) #f))))
       (split-on cluster-spec #\,)))

(define (node-field nm i)
  (let loop ((ns nodes)) (cond ((null? ns) #f) ((eqv? (caar ns) nm) (list-ref (car ns) i)) (else (loop (cdr ns))))))
(define (raft-addr nm) (string-append (node-field nm 1) ":" (number->string (node-field nm 2))))

; ---- member locality (cw-lkq.7) ----
; "region[/zone]" from --locality (or the `locality` config key), falling back
; to this member's optional 5th cluster-spec field. Exposed in /metrics
; (crabwatchstore_member_locality) and consumed by leader-region pinning (A2).
(define locality
  (let ((l (arg-after "--locality" #f)))
    (or l (node-field me 4) "")))

(define all-names (map car nodes))
; everyone but me — the members this node dials when joining a live cluster (cw-u4a.29).
(define existing-members
  (let loop ((ns all-names) (acc '()))
    (cond ((null? ns) (reverse acc))
          ((eqv? (car ns) me) (loop (cdr ns) acc))
          (else (loop (cdr ns) (cons (car ns) acc))))))

(make-table 'ws-shard-pid "set")
(make-table 'ws-shard-role "set")
(make-table 'ws-shard-leader "set")
(make-table 'ws-shard-commit "set")
(make-table 'ws-shard-applied "set")

; ---- bring up the inter-node mesh ----
(node-make (symbol->string me))
(node-listen (symbol->string me) (raft-addr me))

; dial only higher-named peers (one connection per pair); retry until up.  A JOINING node
; instead dials EVERY existing member: it has the complete addr map and must establish the
; links itself (the existing nodes don't know its address yet, cw-u4a.29).  Either way each
; pair ends up with one full-duplex connection, so node-send works in both directions.
(define (sym>? a b) (string>? (symbol->string a) (symbol->string b)))
(define higher-named (let loop ((ns all-names) (acc '()))
                       (cond ((null? ns) (reverse acc))
                             ((sym>? (car ns) me) (loop (cdr ns) (cons (car ns) acc)))
                             (else (loop (cdr ns) acc)))))
(define dial-peers (if join? existing-members higher-named))
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
; A joiner starts as a NON-VOTER follower: its initial voter set is the EXISTING members (it
; is added later via MemberAdd, then promoted), so it never campaigns before being admitted.
; A fresh node is a voter in the full set.  Either way the live config arrives by replication
; (adopted on append), overriding this bootstrap set.
; ---- genesis learners (cw-85j) — local-majority WAN config ----
; --learners name,name : these members boot as NON-VOTING (receive AppendEntries,
; never count for quorum/elections). Passed IDENTICALLY to every member (the spec
; + flag are shared), so all nodes derive the SAME genesis voters/learners split.
; Use to seat a local-majority voter set, e.g. 9 members with ap-south as learners
; => 5 voters (us-east 3 + eu-west 2), majority 3 = the leader's 3 local us-east
; voters, so a write commits on the LAN instead of crossing the 80ms WAN.
(define learner-names
  (let ((s (arg-after "--learners" "")))
    (if (string=? s "") '() (map string->symbol (split-on s #\,)))))
(define (drop-learners ns)
  (let loop ((ns ns) (acc '()))
    (cond ((null? ns) (reverse acc))
          ((memv (car ns) learner-names) (loop (cdr ns) acc))
          (else (loop (cdr ns) (cons (car ns) acc))))))
; A joiner is admitted via MemberAdd (no genesis learners); a fresh node seats the
; voter-only set + the learner subset.
(define shard-voters   (if join? existing-members (drop-learners all-names)))
(define shard-learners (if join? '() learner-names))
; --shards N (cw-b5w.4, ADR 0005 B): apply-worker count for parallel PUT
; materialization. DEFAULT 1 (serial): on darwin-arm64 the measured ladder
; REGRESSED with 4 workers (load=l 2707 -> 1603 w/s) — the per-batch barrier
; round-trips + the SingleThreaded-RocksDB mutex outweigh the parallelized VM
; work. The machinery is correct (all suites green at N=4) and stays available
; for retuning (bigger batches / Linux / multi-DB); see docs/adr/0005 + cw-b5w.5.
(define apply-shards
  (let ((n (string->number (arg-after "--shards" "1")))) (if (and n (> n 0)) n 1)))
; cw-ivt: --shard-groups N = number of INDEPENDENT Raft groups (keys 0..N-1), each a
; full replica group on every node. Writes to different keys land on different groups
; → different leaders (election stagger rotates by shard idx) → parallel consensus =
; scale-out. DEFAULT 1 (today's single-group behavior, byte-identical). Distinct from
; --shards (apply-worker count WITHIN a group).
(define shard-groups
  (let ((n (string->number (arg-after "--shard-groups" "1")))) (if (and n (> n 0)) n 1)))
(define shard-key-list
  (let loop ((i 0) (acc '())) (if (= i shard-groups) (reverse acc)
                                  (loop (+ i 1) (cons (number->string i) acc)))))
; ---- Raft timing knobs (cw-lkq.1, etcd analogues for WAN deployments) ----
; --tick-ms        heartbeat interval / Raft clock (etcd --heartbeat-interval);
;                  the poller paces ticks by wall clock (cw-b5w.7). Default 120.
; --election-ticks election timeout BASE in ticks before the per-node stagger
;                  (etcd --election-timeout ~= tick-ms * election-ticks). Default 4.
; WAN profile (100-300ms RTT): tick-ms 250, election-ticks 8 — heartbeats
; tolerate an RTT without eating the election window.
(define tick-ms
  (let ((n (string->number (arg-after "--tick-ms" "120")))) (if (and n (> n 0)) n 120)))
(define election-ticks
  (let ((n (string->number (arg-after "--election-ticks" "4")))) (if (and n (> n 0)) n 4)))
; ---- leader-region pinning (cw-lkq.2) ----
; --leader-region REGION: a leader OUTSIDE this region transfers leadership
; (TimeoutNow, cw-u4a.42) to a caught-up voter IN it — rate-limited, no-op when
; no preferred voter is caught up (e.g. the region is down). Region per member
; comes from the cluster spec's 5th field / --locality (cw-lkq.7).
(define leader-region (arg-after "--leader-region" #f))
(define region-map (map (lambda (n) (cons (car n) (list-ref n 4))) nodes))
; --serializable-max-lag N (cw-lkq.6): refuse SERIALIZABLE reads when this
; replica is more than N entries behind the leader's commit (commit - applied
; > N) — the freshness gate for learner/follower read replicas. 0 = no gate
; (serve whatever is applied; etcd's default serializable behavior).
(define serializable-max-lag
  (let ((n (string->number (arg-after "--serializable-max-lag" "0")))) (if (and n (>= n 0)) n 0)))
; cw-ivt: spawn one shard-main per group (key "0".."N-1"), each its own DB path.
; N=1 is byte-identical to the old single "0" spawn (dbbase-shard0).
(for-each
 (lambda (sk)
   (spawn-source-dedicated "(include \"src/server/shard-actor.scm\")" 'shard-main
                 sk shard-voters me (string-append dbbase "-shard" sk) durable apply-shards
                 election-ticks leader-region region-map serializable-max-lag shard-learners))
 shard-key-list)

(define dial-addrs (map raft-addr dial-peers))
; Dedicated thread — the poller is the Raft tick-clock AND sole network drainer;
; cooperative parking on a shared worker would slow the protocol (green-threads
; INV-3). cw-ivt: the poller already routes inbound frames by shard-key to the right
; local replica, so it drives ALL groups — pass the full shard-key list.
(spawn-source-dedicated "(include \"src/server/peer-poller.scm\")" 'peer-poller
              me shard-key-list tick-ms dial-addrs (- (length nodes) 1))

; wait until this node has elected/learned a leader for shard 0, so the substrate is ready
; before we report up.  A JOINER is not in consensus yet (no leader until MemberAdd lands +
; it catches up), so it does NOT wait — it parks and starts serving once promoted+elected.
(define (qk) (string-append (symbol->string me) ":0"))
(if join?
    (begin
      ; cw-24e.4: the joiner skips the LEADER wait, but it MUST still wait for
      ; the shard actor to publish its pid — the gRPC service below captures
      ; shard-pid once at spawn, and the shard's (include ...) compile takes
      ; long enough that a joiner raced past it with shard-pid = #f, making
      ; EVERY gRPC call raise ("handler error") for the life of the process.
      (let spin ()
        (if (table-lookup 'ws-shard-pid (qk)) #t (begin (sleep-ms 20) (spin))))
      (display "node ") (display me)
      (display ": joined mesh as a non-voter — awaiting MemberAdd (catch-up + promote)") (newline))
    (begin
      ; sleep-poll, do NOT busy-spin: this main-thread wait runs WHILE the shard
      ; actor + peer-poller are trying to elect a leader. A tight (spin) here pins
      ; the main thread at 100% and, on a CPU-starved host, starves the very
      ; threads running the election — so the election never completes and the
      ; wait spins forever (seen with a 9-member cluster on a loaded node). A
      ; 20ms poll frees the core; startup latency cost is at most one poll.
      ; cw-ivt: wait for + report EVERY shard group's leader (was hardcoded shard 0).
      (for-each
       (lambda (sk)
         (let ((qks (string-append (symbol->string me) ":" sk)))
           (let spin () (if (table-lookup 'ws-shard-leader qks) #t (begin (sleep-ms 20) (spin))))
           (display "node ") (display me) (display ": shard ") (display sk)
           (display " ready (leader=") (display (table-lookup 'ws-shard-leader qks))
           (display ", role=") (display (table-lookup 'ws-shard-role qks)) (display ")") (newline)))
       shard-key-list)))

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
; grpc-serve / the metrics listener bind via Rust SocketAddr parse — NUMERIC IPs
; only.  A --cluster row may carry a HOSTNAME (compose service names, cw-24e.2):
; peers dial it through DNS fine, but we cannot BIND to it — so a non-numeric
; host binds 0.0.0.0:<port> (all interfaces) instead.  Numeric specs (every
; existing test: 127.0.0.1) keep the original exact-address bind.
(define (numeric-host? s)
  (let loop ((i 0))
    (or (= i (string-length s))
        (let ((c (string-ref s i)))
          (and (or (char-numeric? c) (char=? c #\.) (char=? c #\:))
               (loop (+ i 1)))))))
(define bind-host (if (numeric-host? client-host) client-host "0.0.0.0"))
(define client-addr (string-append bind-host ":" (number->string client-port)))
(define shard-pid   (table-lookup 'ws-shard-pid (qk)))

; ---- endpoint metrics/health/version HTTP port (cw-u4a.33) ----
; etcd serves /metrics, /health and /version over plain HTTP on a SEPARATE listener
; (--listen-metrics-urls); the client port here is Rust-owned h2c gRPC (hardcoded
; application/grpc), which cannot serve plain HTTP, so the HTTP surface gets its own port.
; Default = client-port + 10000 (well clear of the raft/client ranges); override with
; --metrics-port.  It is endpoint-LOCAL (not part of cluster identity), so it stays OUT of the
; --cluster spec — the 4-field name:host:raftport:clientport parse is unchanged.
(define metrics-port
  (string->number (arg-after "--metrics-port" (number->string (+ client-port 10000)))))

; name->peerURL table for the Cluster gRPC service (cw-u4a.30): each node's raft addr
; is its peer URL.  MemberList reports these (best-effort); runtime-added nodes that
; aren't in the spec report an empty peerURL.  A list of (name-string peerurl-string)
; — sendable as a quoted datum (lists of strings serialize like dial-addrs above).
(define cluster-members
  (map (lambda (e)
         (list (symbol->string (car e))
               (string-append "http://" (cadr e) ":" (number->string (caddr e)))))
       nodes))

; cw-6i0: a POOL of grpc-kv-main workers behind a thin round-robin router, so
; concurrent requests issue concurrent shard proposals (the shard's group-commit
; then coalesces them) instead of serializing one Raft commit per write through a
; lone handler.  --grpc-workers tunes the pool size (default 32).
(define grpc-workers
  (string->number (arg-after "--grpc-workers" "32")))
(define grpc-handler
  (spawn-source-dedicated "(include \"src/server/grpc-router.scm\")" 'grpc-router-main
                          shard-pid cluster-id member-id cluster-members
                          (symbol->string me) grpc-workers shard-groups))
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

; ---- endpoint metrics/health/version HTTP server (cw-u4a.33) ----
; A dedicated HTTP/1.1 listener on metrics-port, etcd's --listen-metrics-urls faithful.  The
; gRPC Health service (grpc.health.v1.Health/Check + /Watch) rides the client-port gRPC server
; above (new handlers, no Rust change); /health, /version and /metrics ride THIS listener.
; Dedicated thread: tcp-accept blocks (no cooperative hook) and each request does a blocking
; shard round-trip — both would freeze the shared green pool (green-threads INV-2/3), same as
; the shard / poller / grpc-kv handler.  Reads this node's shard-0 replica via the un-gated .32
; status seam; a bind failure is contained inside the actor (the node keeps serving gRPC).
(spawn-source-dedicated "(include \"src/server/metrics-http.scm\")" 'metrics-http-main
                        shard-pid (symbol->string me) bind-host metrics-port locality)

; The consensus substrate + etcd KV API + metrics endpoint are up. Park so the node process
; stays alive serving Raft RPCs to peers and gRPC calls to clients. The main thread does NO
; work (shard/poller/grpc/metrics are all their own actor threads), so it must SLEEP, not
; (yield)-spin: on current crabscheme yield is ~free, so the old (yield) loop pinned this
; thread at ~100% CPU (~1 core/member at idle, on top of the peer-poller spin). A 1s sleep
; keeps the process alive with negligible CPU and starves nothing (the green pool runs on its
; own worker threads).
(let park () (sleep-ms 1000) (park))
