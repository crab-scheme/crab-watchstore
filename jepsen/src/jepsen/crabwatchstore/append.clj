(ns jepsen.crabwatchstore.append
  "The gold-standard workload, ported from jepsen-io/redis: list-append with Elle.

   Each jepsen transaction is a list of micro-ops — `[:append k v]` or `[:r k]` —
   that must commit atomically. crab-cache used Redis MULTI/EXEC; the etcd analogue
   is a single Txn. etcd has no native list type and Txn ops cannot read-modify-write
   within themselves, so we do an OPTIMISTIC read-modify-write:

     1. Pre-read every touched key (value + mod_revision).
     2. Replay the mops in order over working copies (so `:r` sees this txn's own
        earlier appends — read-your-writes — and returns a snapshot list).
     3. Commit ONE etcd Txn: If every touched key is still at its read mod_revision,
        Then put the appended keys' new values. The guards make the whole thing an
        atomic snapshot at commit time (mod_revision is monotonic, so a guard holding
        ⇒ the key never changed between read and commit ⇒ all reads are valid as of
        commit) — a genuine serializable transaction, not a stub.
     4. isSucceeded=false ⇒ a concurrent writer moved some key ⇒ re-read and retry
        (bounded). UNAVAILABLE/not-leader is handled one layer down by the client.

   Elle then looks for cycles in the read/write/append dependency graph, detecting
   anomalies up to STRICT SERIALIZABILITY — the same workload Kyle Kingsbury ran
   against Redis-Raft, so crab-watchstore's results are directly comparable."
  (:require [clojure.string :as str]
            [jepsen [client :as client]]
            [jepsen.tests.cycle.append :as append]
            [jepsen.crabwatchstore.client :as cw]))

(def max-conflict-retries 100)

(defn- lrk [k] (str "app:" k))

(defn- parse-list [s]
  (if (or (nil? s) (str/blank? s)) [] (mapv #(Long/parseLong %) (str/split s #","))))

(defn- attempt
  "One optimistic attempt at the transaction `mops`. Returns
   {:ok? bool, :mops filled-mops}. :ok? false ⇒ a guard failed (retry)."
  [kv mops]
  (let [touched (distinct (map second mops))
        ;; pre-read each touched key -> {k {:value :mod-revision}}
        reads   (into {} (map (fn [k]
                                (let [r (cw/kv-get-rev kv (lrk k))]
                                  [k {:list (parse-list (:value r))
                                      :mod  (:mod-revision r)}]))
                              touched))
        ;; replay mops in order over working copies; collect filled reads
        [filled work] (reduce
                        (fn [[acc work] [f k v :as mop]]
                          (case f
                            :append [(conj acc mop) (update work k (fnil conj []) v)]
                            :r      [(conj acc [:r k (get work k)]) work]))
                        [[] (into {} (map (fn [k] [k (get-in reads [k :list])]) touched))]
                        mops)
        appended (set (keep (fn [[f k _]] (when (= f :append) k)) mops))
        guards   (mapv (fn [k] [(lrk k) (get-in reads [k :mod])]) touched)
        puts     (mapv (fn [k] [(lrk k) (str/join "," (get work k))]) appended)]
    ;; A read-only transaction (no puts) still commits the guarded Txn (empty Then),
    ;; which succeeds iff the guards hold — i.e. a consistent snapshot.
    {:ok?  (cw/kv-txn-guarded kv guards puts)
     :mops filled}))

(defrecord Client [conn kv]
  client/Client
  (open! [this test n]
    (let [c (cw/connect (:nodes test))]
      (assoc this :conn c :kv (.getKVClient c))))
  (setup! [this test])

  (invoke! [this test op]
    (let [mops (:value op)]
      (loop [tries 0]
        (let [r (attempt (:kv this) mops)]
          (cond
            (:ok? r)                       (assoc op :type :ok :value (:mops r))
            (< tries max-conflict-retries) (recur (inc tries))
            ;; A guard fail is a DEFINITE no-op (nothing was put), so on exhaustion
            ;; the transaction simply did not apply: :fail, not :info.
            :else                          (assoc op :type :fail :error :too-many-conflicts))))))

  (teardown! [this test])
  (close! [this test] (.close ^io.etcd.jetcd.Client (:conn this))))

(defn workload
  "Elle list-append workload. opts may carry :append-keys / :append-txn-len."
  [opts]
  (assoc (append/test {:key-count          (:append-keys opts 8)
                       :min-txn-length     1
                       :max-txn-length     (:append-txn-len opts 4)
                       :max-writes-per-key 32
                       :consistency-models [:strict-serializable]})
         :client (->Client nil nil)))
