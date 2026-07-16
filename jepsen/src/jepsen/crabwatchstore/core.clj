(ns jepsen.crabwatchstore.core
  "Jepsen test entry point for crab-watchstore — an etcd v3-compatible, Raft-
   replicated KV store written in CrabScheme, driven over the etcd gRPC API with
   the official jetcd client.

   Run from inside the jepsen/ directory on a Jepsen control node:

     lein run test --workload register --nemesis none \\
                   --nodes n1,n2,n3,n4,n5 --concurrency 10 --time-limit 30

     lein run serve            # browse results at http://localhost:8080

   See README.md for the Docker node setup.

   Differences from crab-cache's core.clj:
     * NO --shards option (crab-watchstore is a single Raft group).
     * NO counter workload (etcd has no atomic INCR; the cas / append-via-Txn
       workloads are the analogue).
     * The client speaks etcd gRPC (jetcd), not RESP."
  (:require [clojure.string :as str]
            [jepsen [checker :as checker]
                    [cli :as cli]
                    [generator :as gen]
                    [tests :as tests]]
            [jepsen.nemesis :as nemesis]
            [jepsen.nemesis.combined :as nc]
            [jepsen.os :as os]
            [jepsen.crabwatchstore [db :as db]
                                   [register :as register]
                                   [append :as append]
                                   [cas :as cas]
                                   [watch :as watch]
                                   [lease :as lease]
                                   [membership :as membership]]))

(def workloads
  "Workload name -> constructor."
  {:register register/workload
   :append   append/workload
   :cas      cas/workload
   :watch    watch/workload
   :lease    lease/workload})

;; clock excluded: shared-kernel containers can't skew one node's clock in isolation.
;; membership = the reconfiguration nemesis (cw-u4a.35) — remove a voter (5->4) then
;; re-add it (4->5) via the Cluster gRPC service, repeatedly, under load.
(def all-faults [:partition :kill :pause :membership])

(defn parse-faults
  [s]
  (cond (= s "none") #{}
        (= s "all")  (set all-faults)
        :else        (set (map keyword (str/split s #",")))))

(def cli-opts
  "Options on top of jepsen.cli/single-test-cmd's built-ins (which already provide
   --nodes, --concurrency, --time-limit, --test-count, --username, etc.)."
  [["-w" "--workload NAME" "Workload: register | cas | append | watch | lease"
    :default  :register
    :parse-fn keyword
    :validate [workloads (cli/one-of workloads)]]

   [nil "--nemesis FAULTS" "Faults: comma-separated subset of partition,kill,pause,membership (or 'none' / 'all')"
    :default  #{:partition}
    :parse-fn parse-faults
    :validate [(fn [fs] (every? (set all-faults) fs))
               (str "must be a subset of " (mapv name all-faults))]]

   [nil "--[no-]durable" "fsync every write (RocksDB durable mode)"
    :default true]

   [nil "--shard-groups N" "Independent Raft groups (multi-shard, cw-kp0). 1 = single group (today's default + semantics). >1 validates the global-revision allocator: the register/append/watch checkers assert etcd's cross-shard revision invariants hold across groups."
    :default  1
    :parse-fn #(Long/parseLong %)
    :validate [pos? "must be positive"]]

   [nil "--engine NAME" "Consensus engine: raft (default) | quepaxa (cw-97b validation)"
    :default  nil
    :parse-fn keyword
    :validate [#{:raft :quepaxa} "must be raft or quepaxa"]]

   [nil "--rate HZ" "Approx requests/sec/thread"
    :default  50
    :parse-fn #(Double/parseDouble %)
    :validate [pos? "must be positive"]]

   [nil "--register-group N" "register/cas: worker threads per key (per-key concurrency; --concurrency must be a multiple)"
    :default  2
    :parse-fn #(Long/parseLong %)
    :validate [pos? "must be positive"]]

   [nil "--register-ops N" "register/cas: ops per key — keep small so Knossos linearizability terminates"
    :default  100
    :parse-fn #(Long/parseLong %)
    :validate [pos? "must be positive"]]

   [nil "--append-keys N" "append: number of independent list keys"
    :default  8
    :parse-fn #(Long/parseLong %)
    :validate [pos? "must be positive"]]

   [nil "--append-txn-len N" "append: max micro-ops per transaction"
    :default  4
    :parse-fn #(Long/parseLong %)
    :validate [pos? "must be positive"]]])

(defn crabwatchstore-test
  "Builds a Jepsen test map from parsed CLI opts."
  [opts]
  (let [workload ((workloads (:workload opts)) opts)
        database (db/db)
        faults   (:nemesis opts)
        nopts    {:db        database
                  :nodes     (:nodes opts)
                  :faults    faults
                  ;; Isolate a MINORITY (one node, or a <quorum subset) so a majority
                  ;; keeps serving — this yields :ok ops + a real linearizability proof
                  ;; AND the split-brain probe (the isolated side must not serve stale),
                  ;; rather than the :majority / :majorities-ring partitions which just
                  ;; make the (correctly CP) cluster unavailable → an all-:info history.
                  :partition {:targets [:one :minority]}
                  :interval  10}
        nemesis  (if (empty? faults)
                   ;; No faults: plain noop nemesis (skip all package setup!).
                   {:nemesis nemesis/noop, :generator nil, :final-generator nil, :perf #{}}
                   ;; Compose partition + db (kill/pause/start) + membership. The
                   ;; membership package only fires when :membership is in faults
                   ;; (else nil generator), so it's safe to always include — same
                   ;; pattern as the built-ins. Deliberately excludes file-corruption
                   ;; and clock packages (per-node clock — impossible in shared-kernel
                   ;; containers); both would fail setup! here and we don't use them.
                   (nc/compose-packages [(nc/partition-package nopts)
                                         (nc/db-package nopts)
                                         (membership/package nopts)]))]
    (merge tests/noop-test
           opts
           {:name      (str "crabwatchstore " (name (:workload opts))
                            (when (:durable opts) " durable")
                            " {" (str/join "," (map name (sort (:nemesis opts)))) "}")
            :os        os/noop
            :db        database
            :client    (:client workload)
            :nemesis   (:nemesis nemesis)
            :checker   (checker/compose
                         {:perf       (checker/perf {:nemeses (:perf nemesis)})
                          :clock      (checker/clock-plot)
                          :stats      (checker/stats)
                          :exceptions (checker/unhandled-exceptions)
                          :workload   (:checker workload)})
            :generator (gen/phases
                         (->> (:generator workload)
                              (gen/stagger (/ 1 (:rate opts)))
                              (gen/nemesis (:generator nemesis))
                              (gen/time-limit (:time-limit opts)))
                         (gen/log "Healing cluster")
                         (gen/nemesis (:final-generator nemesis))
                         (gen/log "Final reads")
                         (gen/clients (:final-generator workload)))})))

(defn -main
  [& args]
  (cli/run! (merge (cli/single-test-cmd {:test-fn  crabwatchstore-test
                                         :opt-spec cli-opts})
                   (cli/serve-cmd))
            args))
