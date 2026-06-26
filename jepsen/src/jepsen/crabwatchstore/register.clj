(ns jepsen.crabwatchstore.register
  "Linearizable register workload: many independent keys, each a read/write
   register, checked by Knossos for linearizability.

   read  = etcd Range (KV.get) → first KeyValue's value (nil if absent)
   write = etcd Put (KV.put)

   A read/write register catches stale reads, lost acked writes, and split-brain
   (a read on one side returning a value the other side has already overwritten).
   The stronger `cas` workload (see cas.clj) adds compare-and-set via etcd Txn for
   a tighter Knossos linearization."
  (:require [jepsen [checker :as checker]
                    [client :as client]
                    [generator :as gen]
                    [independent :as independent]]
            [jepsen.crabwatchstore.client :as cw]
            [knossos.model :as model]))

(defn- ->long [s] (when s (Long/parseLong (str s))))

(def op-read  (fn [_ _] {:type :invoke, :f :read,  :value nil}))
(def op-write (fn [_ _] {:type :invoke, :f :write, :value (rand-int 5)}))

(defrecord Client [conn kv]
  client/Client
  (open! [this test n]
    ;; Connect to ALL node endpoints (round-robin + retry finds the leader),
    ;; regardless of which node jepsen handed us — crab-watchstore does not proxy
    ;; writes, so a single-endpoint client pinned to a follower would never apply.
    (let [c (cw/connect (:nodes test))]
      (assoc this :conn c :kv (.getKVClient c))))

  (setup! [this test])

  (invoke! [this test op]
    (let [[k v] (:value op)
          rk    (str "reg:" k)]
      ;; A client-side timeout is an INDETERMINATE outcome (the op may or may not have
      ;; applied), so model it as :info — the truthful jepsen classification — rather
      ;; than letting it propagate as an unhandled exception (which the strict
      ;; UnhandledExceptions checker fails the run on). Common during the multi-group
      ;; startup window (3 Raft groups x N nodes electing + leases warming).
      (try
        (case (:f op)
          :read  (assoc op :type :ok
                        :value (independent/tuple k (->long (cw/kv-get (:kv this) rk))))
          :write (do (cw/kv-put (:kv this) rk (str v))
                     (assoc op :type :ok)))
        (catch java.util.concurrent.TimeoutException _
          (assoc op :type :info, :error :timeout))
        (catch java.util.concurrent.ExecutionException e
          (if (instance? java.util.concurrent.TimeoutException (.getCause e))
            (assoc op :type :info, :error :timeout)
            (throw e))))))

  (teardown! [this test])
  (close! [this test] (.close ^io.etcd.jetcd.Client (:conn this))))

(defn workload
  "Register workload. opts may carry :register-group (per-key concurrency) and
   :register-ops (ops per key)."
  [opts]
  (let [group (:register-group opts 2)   ; worker threads PER KEY (per-key concurrency)
        ops   (:register-ops   opts 100)] ; ops per key
    {:client    (->Client nil nil)
     :checker   (independent/checker
                  (checker/linearizable {:model     (model/register)
                                         :algorithm :linear}))
     :generator (independent/concurrent-generator
                  group
                  (range)
                  (fn [_k]
                    (->> (gen/mix [op-read op-write])
                         (gen/limit ops))))}))
