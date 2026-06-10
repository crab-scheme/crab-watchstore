(ns jepsen.crabwatchstore.cas
  "Linearizable cas-register workload — the strong register test. Each key is a
   register supporting read (Range), write (Put), and compare-and-set (an etcd Txn
   `If value == old Then put new`, one Raft entry). `cas` is what gives Knossos
   enough constraints to pin down a linearization, so this is a far stronger
   consistency probe than the plain read/write register.

   read  = KV.get → value
   write = KV.put
   cas   = KV.txn().If(Cmp value EQUAL old).Then(Op.put new).commit() → isSucceeded"
  (:require [jepsen [client :as client]
                    [checker :as checker]
                    [generator :as gen]
                    [independent :as independent]]
            [jepsen.crabwatchstore.client :as cw]
            [knossos.model :as model]))

(defn- ->long [s] (when s (Long/parseLong (str s))))

(def op-read  (fn [_ _] {:type :invoke, :f :read,  :value nil}))
(def op-write (fn [_ _] {:type :invoke, :f :write, :value (rand-int 5)}))
(def op-cas   (fn [_ _] {:type :invoke, :f :cas,   :value [(rand-int 5) (rand-int 5)]}))

(defrecord Client [conn kv]
  client/Client
  (open! [this test n]
    (let [c (cw/connect (:nodes test))]
      (assoc this :conn c :kv (.getKVClient c))))
  (setup! [this test])

  (invoke! [this test op]
    (let [[k v] (:value op)
          rk    (str "cas:" k)]
      (case (:f op)
        :read  (assoc op :type :ok
                      :value (independent/tuple
                               k (->long (cw/kv-get (:kv this) rk))))
        :write (do (cw/kv-put (:kv this) rk (str v))
                   (assoc op :type :ok))
        :cas   (let [[old new] v
                     ok? (cw/kv-cas (:kv this) rk (str old) (str new))]
                 ;; Txn isSucceeded=false ⇒ current != old ⇒ no write happened ⇒
                 ;; a DEFINITE failure (:fail), not :info.
                 (assoc op :type (if ok? :ok :fail))))))

  (teardown! [this test])
  (close! [this test] (.close ^io.etcd.jetcd.Client (:conn this))))

(defn workload
  [opts]
  (let [group (:register-group opts 2)
        ops   (:register-ops   opts 100)]
    {:client    (->Client nil nil)
     :checker   (independent/checker
                  (checker/linearizable {:model     (model/cas-register)
                                         :algorithm :linear}))
     :generator (independent/concurrent-generator
                  group
                  (range)
                  (fn [_k]
                    (->> (gen/mix [op-read op-write op-cas])
                         (gen/limit ops))))}))
