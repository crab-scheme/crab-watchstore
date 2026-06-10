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
                                   [lease :as lease]]))

(def workloads
  "Workload name -> constructor."
  {:register register/workload
   :append   append/workload
   :cas      cas/workload
   :watch    watch/workload     ; stub — cw-u4a.35
   :lease    lease/workload})   ; stub — cw-u4a.35

;; clock excluded: shared-kernel containers can't skew one node's clock in isolation.
(def all-faults [:partition :kill :pause])

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

   [nil "--nemesis FAULTS" "Faults: comma-separated subset of partition,kill,pause (or 'none' / 'all')"
    :default  #{:partition}
    :parse-fn parse-faults
    :validate [(fn [fs] (every? (set all-faults) fs))
               (str "must be a subset of " (mapv name all-faults))]]

   [nil "--[no-]durable" "fsync every write (RocksDB durable mode)"
    :default true]

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
                  :partition {:targets [:one :majority :majorities-ring]}
                  :interval  10}
        nemesis  (if (empty? faults)
                   ;; No faults: plain noop nemesis (skip all package setup!).
                   {:nemesis nemesis/noop, :generator nil, :final-generator nil, :perf #{}}
                   ;; Compose ONLY partition + db (kill/pause/start). Deliberately
                   ;; excludes file-corruption-package and clock-package (per-node
                   ;; clock — impossible in shared-kernel containers); both would
                   ;; fail setup! here and we don't use them.
                   (nc/compose-packages [(nc/partition-package nopts)
                                         (nc/db-package nopts)]))]
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
