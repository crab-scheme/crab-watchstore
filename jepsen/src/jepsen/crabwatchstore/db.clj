(ns jepsen.crabwatchstore.db
  "Install / start / stop / kill / pause a crab-watchstore CLUSTER node on each
   Jepsen DB node.

   crab-watchstore is interpreted, so a node is `crabscheme run src/node-cluster.scm`
   rather than a compiled service. We do NOT build it here: the crabscheme binary
   (built with --features stdlib-store,grpc — the gRPC transport is REQUIRED here,
   unlike crab-cache's RESP build) and the crab-watchstore `src/` tree are expected
   under `/opt/crabwatchstore` on every node — baked into the Docker node image (or
   run `bin/sync-nodes.sh` to push to bare hosts). setup! just starts the process;
   kill!/pause! drive the nemesis; teardown! wipes data + logs but leaves the binary
   in place.

   Differences from crab-cache's db.clj:
     * NO --shards: crab-watchstore is a SINGLE Raft group (one shard \"0\"), so the
       flag does not exist.
     * The 4th --cluster field (clientport) is THIS node's etcd v3 gRPC client port
       (h2c), not a RESP port — the jetcd client connects there. crab-cache ignored
       it (RESP listened on a fixed 6379); crab-watchstore actually binds gRPC to it."
  (:require [clojure.string :as str]
            [clojure.tools.logging :refer [info]]
            [jepsen [control :as c]
                    [db :as db]]
            [jepsen.control.util :as cu]
            [jepsen.crabwatchstore.client :as client]))

(def dir          "/opt/crabwatchstore")
(def binary       (str dir "/crabscheme"))
(def logfile      (str dir "/node.log"))
(def pidfile      (str dir "/node.pid"))
(def data-dir     (str dir "/data"))
(def raft-port    7000)
(def client-port  client/client-port)  ; 2379 — the etcd v3 gRPC port the jetcd client dials
(def proc-pattern "node-cluster.scm")   ; matches the running process cmdline

(defn node-ip
  "Resolve a Jepsen node name to its IPv4 address (via the control node's resolver —
   compose DNS gives every container the same view). REQUIRED for the host field:
   crab-watchstore's `grpc-serve` binds a numeric SocketAddr and does NOT resolve
   hostnames (unlike Raft's `node-listen`, which accepts a name), so a name-as-host
   spec makes the gRPC server fail to bind (`invalid socket address syntax`) and the
   client port never opens. Raft dials/binds by IP just fine."
  [node]
  (.getHostAddress (java.net.InetAddress/getByName (name node))))

(defn cluster-spec
  "The --cluster argument: name:host:raftport:clientport,... for every node.
   The NAME field stays the Jepsen node name (the crab-watchstore node identity);
   the HOST field is the resolved IP (see node-ip — grpc-serve needs a numeric addr).
   The clientport is the per-node etcd gRPC endpoint the jetcd client dials."
  [test]
  (->> (:nodes test)
       (map (fn [n] (str (name n) ":" (node-ip n) ":" raft-port ":" client-port)))
       (str/join ",")))

(defn start-node!
  "Launch the node-cluster process under start-stop-daemon. chdir to `dir` so the
   script's relative (include \"src/...\") forms resolve."
  [test node]
  ;; cw-kp0: when --shard-groups > 1, launch N independent Raft groups so the
  ;; checkers exercise the global-revision allocator across groups. Default 1
  ;; preserves today's single-group launch + semantics exactly (flag omitted).
  (let [sg    (:shard-groups test)
        extra (if (and sg (> sg 1)) ["--shard-groups" (str sg)] [])]
    (apply cu/start-daemon!
      {:logfile logfile
       :pidfile pidfile
       :chdir   dir}
      binary
      "run" "src/node-cluster.scm" "--"
      "--node"    (name node)
      "--durable" (if (:durable test) "yes" "no")
      "--db"      (str data-dir "/cw")
      "--cluster" (cluster-spec test)
      extra)))

(defn signal!
  "Send a signal to the node process by cmdline match. Tolerates 'no process'."
  [sig]
  (c/su (try (c/exec :pkill sig :-f proc-pattern)
             (catch Exception _ :not-running))))

(defn db
  "crab-watchstore cluster DB."
  []
  (reify
    db/DB
    (setup! [this test node]
      (when-not (cu/exists? binary)
        (throw (ex-info (str "crabscheme binary not found at " binary
                             " — bake the node image (docker/) or run jepsen/bin/sync-nodes.sh first")
                        {:node node :expected binary})))
      ;; Defensive: kill any stale crabscheme from a prior run whose teardown
      ;; didn't fire. Raft busy/cooperative-polls, so orphaned processes peg the
      ;; CPU and skew results. Then clear RocksDB MVCC state and start fresh.
      (signal! :-9)
      (Thread/sleep 500)
      (c/su (c/exec :rm :-rf data-dir)
            (c/exec :rm :-f logfile pidfile)
            (c/exec :mkdir :-p data-dir))
      (info node "starting crab-watchstore")
      (start-node! test node)
      ;; Give the mesh + single-group leader election time to settle before
      ;; clients connect: the node only binds its etcd gRPC client port AFTER a
      ;; leader is known (node-cluster.scm spins on ws-shard-leader, then logs
      ;; "etcd KV gRPC serving on ...").
      (Thread/sleep 8000))

    (teardown! [this test node]
      (info node "stopping crab-watchstore")
      (signal! :-9)
      (c/su (c/exec :rm :-rf data-dir)
            (c/exec :rm :-f logfile pidfile)))

    db/LogFiles
    (log-files [this test node] [logfile])

    ;; --- nemesis hooks ---
    db/Kill
    (start! [this test node]
      ;; Restart WITHOUT wiping data — this exercises RocksDB crash recovery
      ;; + Raft rejoin, exactly what we want to verify.
      (when-not (cu/daemon-running? pidfile)
        (start-node! test node))
      :started)
    (kill! [this test node]
      (signal! :-9)
      :killed)

    db/Pause
    (pause!  [this test node] (signal! :-STOP) :paused)
    (resume! [this test node] (signal! :-CONT) :resumed)))
