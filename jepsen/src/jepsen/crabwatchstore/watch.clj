(ns jepsen.crabwatchstore.watch
  "Watch-consistency workload (cw-u4a.35) — verifies the etcd Watch contract under
   nemesis: every committed write is delivered to the watcher EXACTLY ONCE, in strict
   revision order, with NO gaps and NO dups.

   Shape
   -----
   * N writer workers PUT to keys under the prefix \"w/\" (many keys, with overwrites,
     so the same key produces many distinct-revision events). Each :ok write records
     the revision it committed at (PutResponse header revision).
   * ONE self-healing watcher (the first worker to open!) watches the \"w/\" prefix from
     revision 1 and appends every event to a shared, epoch-tagged log.
   * A final :drain op (run after the cluster heals) waits for the watcher to catch up
     to the highest committed write, then snapshots the event log into the history.

   The watcher is LEADER-GATED on this store (a follower closes the stream with
   UNAVAILABLE \"no leader\"), and the leader moves under :kill/:partition/:membership.
   So the watcher RE-ESTABLISHES on error, resuming WithRevision(last-seen+1) — which is
   exactly how an etcd client survives a follower-route or a failover. Each
   (re)establishment bumps an `epoch`; etcd may legitimately re-deliver the boundary
   revision across a resume, so the checker enforces exactly-once / order WITHIN an
   epoch and de-dups ACROSS epochs (the etcd resume contract), while completeness is
   checked over the whole de-duped stream.

   Checker (a real consensus probe, not a smoke):
     * dups-within-epoch   — a revision delivered twice in ONE continuous stream  => BUG
     * out-of-order        — a stream segment whose revisions are not increasing   => BUG
     * missing-deliveries  — a committed (:ok) write never delivered to the watcher => BUG
     * value-mismatches    — a delivered event whose value != the write at that rev => BUG
   `:valid?` is the conjunction. (Observed revisions with no matching :ok write are
   reported informationally — they are almost always :info writes that DID commit, not
   a violation.)"
  (:require [clojure.set :as set]
            [clojure.tools.logging :refer [info warn]]
            [jepsen [checker :as checker]
                    [client :as client]
                    [generator :as gen]]
            [jepsen.crabwatchstore.client :as cw])
  (:import (io.etcd.jetcd ByteSequence Watch Watch$Listener Watch$Watcher KeyValue)
           (io.etcd.jetcd.options WatchOption)
           (io.etcd.jetcd.watch WatchEvent WatchEvent$EventType WatchResponse)
           (java.nio.charset StandardCharsets)
           (java.util.concurrent.atomic AtomicBoolean)))

(def prefix "w/")
(def n-keys 16)
(def drain-wait-ms 20000)   ; max wait for the watcher to catch up to the last write
;; Generous backoff so a no-leader window (cold start / failover) doesn't pile up
;; watch-stream (re)establishments on the leader — one re-establish in flight at a time.
(def reestablish-backoff-ms 1000)

(defn- event->map
  "A jetcd WatchEvent -> {:rev :key :value :type :epoch}."
  [^WatchEvent e epoch]
  (let [^KeyValue kv (.getKeyValue e)
        t            (.getEventType e)]
    {:rev   (.getModRevision kv)
     :key   (.toString (.getKey kv) StandardCharsets/UTF_8)
     :value (.toString (.getValue kv) StandardCharsets/UTF_8)
     :type  (cond (= t WatchEvent$EventType/PUT)    :put
                  (= t WatchEvent$EventType/DELETE) :delete
                  :else                             :other)
     :epoch epoch}))

(defn start-watch!
  "(Re)establish the prefix watch from `resume-rev`. Appends events to `events`,
   advances `last-rev`, and on error bumps `epoch` and re-establishes from
   last-seen+1 — self-healing across follower-routes and leader failovers."
  [^Watch wc {:keys [events last-rev epoch watcher establishing?] :as st} resume-rev]
  (let [opt      (-> (WatchOption/builder)
                     (.isPrefix true)
                     (.withRevision (max 1 resume-rev))
                     (.build))
        ep       @epoch
        listener (reify Watch$Listener
                   (onNext [_ resp]
                     (doseq [^WatchEvent e (.getEvents ^WatchResponse resp)]
                       (let [m (event->map e ep)]
                         (swap! events conj m)
                         (swap! last-rev max (:rev m)))))
                   (onError [_ t]
                     ;; Follower UNAVAILABLE / leader gone / stream reset: re-establish
                     ;; once (guarded), from the next unseen revision, on a side thread
                     ;; (never block the gRPC callback thread).
                     (when (.compareAndSet ^AtomicBoolean establishing? false true)
                       (future
                         (try
                           (warn "watch: stream error (" (some-> t .getMessage)
                                 ") — re-establishing from rev" (inc @last-rev))
                           (Thread/sleep (long reestablish-backoff-ms))
                           (swap! epoch inc)
                           (when-let [^Watch$Watcher w @watcher] (try (.close w) (catch Throwable _ nil)))
                           (start-watch! wc st (inc @last-rev))
                           (finally
                             (.set ^AtomicBoolean establishing? false))))))
                   (onCompleted [_] nil))
        w        (.watch wc (cw/->bs prefix) opt listener)]
    (reset! watcher w)
    w))

(defrecord WatchClient [conn watch-c kv state]
  client/Client
  (open! [this test n]
    (let [c  (cw/connect (:nodes test))
          wc (.getWatchClient c)
          kv (.getKVClient c)]
      ;; Exactly one worker starts the shared watcher (CAS on :started).
      (when (compare-and-set! (:started state) false true)
        (info n "watch: establishing shared prefix watcher from rev 1")
        (start-watch! wc state 1))
      (assoc this :conn c :watch-c wc :kv kv)))

  (setup! [this test])

  (invoke! [this test op]
    (case (:f op)
      :write
      (let [[k v] (:value op)
            rev   (cw/kv-put-rev (:kv this) (str prefix k) (str v))]
        (swap! (:max-write-rev state) max rev)
        (assoc op :type :ok :value [k v rev]))

      :drain
      ;; Wait (bounded) for the watcher to catch up to the highest committed write,
      ;; then snapshot the shared event log into the history for the checker.
      (let [deadline (+ (System/currentTimeMillis) drain-wait-ms)]
        (loop []
          (when (and (< (System/currentTimeMillis) deadline)
                     (< @(:last-rev state) @(:max-write-rev state)))
            (Thread/sleep 200)
            (recur)))
        (assoc op :type :ok
               :value {:events    @(:events state)
                       :last-rev  @(:last-rev state)
                       :max-write @(:max-write-rev state)
                       :epochs    (inc @(:epoch state))}))))

  (teardown! [this test])
  (close! [this test]
    (when-let [^Watch$Watcher w @(:watcher state)] (try (.close w) (catch Throwable _ nil)))
    (.close ^io.etcd.jetcd.Client (:conn this))))

(defn- new-state []
  {:events        (atom [])
   :last-rev      (atom 0)
   :max-write-rev (atom 0)
   :epoch         (atom 0)
   :started       (atom false)
   :watcher       (atom nil)
   :establishing? (AtomicBoolean. false)})

(defn checker
  "Verify the etcd watch contract over the history + the drained event log."
  []
  (reify checker/Checker
    (check [_ test history _opts]
      (let [ok-writes    (->> history
                              (filter #(and (= :ok (:type %)) (= :write (:f %))))
                              (map :value))                 ; ([k v rev] ...)
            write-by-rev (into {} (map (fn [[k v r]] [r {:k k :v (str v)}]) ok-writes))
            committed    (set (keys write-by-rev))
            drained      (->> history
                              (filter #(and (= :ok (:type %)) (= :drain (:f %))))
                              (map :value))
            events       (vec (mapcat :events drained))
            puts         (filter #(= :put (:type %)) events)
            by-epoch     (group-by :epoch puts)

            dups-within-epoch
            (->> by-epoch
                 (mapcat (fn [[ep evs]]
                           (->> (frequencies (map :rev evs))
                                (filter (fn [[_ c]] (> c 1)))
                                (map (fn [[r c]] {:epoch ep :rev r :count c}))))))

            out-of-order
            (->> by-epoch
                 (keep (fn [[ep evs]]
                         (let [revs (map :rev evs)]
                           (when-not (apply <= 0 revs)
                             {:epoch ep :revs (vec revs)})))))

            observed-by-rev (into {} (map (fn [e] [(:rev e) e]) puts))  ; de-dup across epochs
            observed        (set (keys observed-by-rev))
            missing         (sort (set/difference committed observed))
            value-mismatch  (->> committed
                                 (keep (fn [r]
                                         (let [w (write-by-rev r)
                                               o (observed-by-rev r)]
                                           (when (and o (not= (:v w) (:value o)))
                                             {:rev r :written (:v w) :observed (:value o)}))))
                                 vec)
            extra           (set/difference observed committed)
            valid?          (and (empty? dups-within-epoch)
                                 (empty? out-of-order)
                                 (empty? missing)
                                 (empty? value-mismatch))]
        {:valid?                  valid?
         :committed-writes        (count ok-writes)
         :observed-put-events     (count puts)
         :distinct-observed-revs  (count observed)
         :watch-epochs            (count by-epoch)
         :dups-within-epoch       (vec dups-within-epoch)
         :out-of-order            (vec out-of-order)
         :missing-deliveries      (vec missing)
         :value-mismatches        value-mismatch
         :uncommitted-observed    (count extra)}))))   ; informational: :info writes that committed

(def op-write
  (let [ctr (atom 0)]
    (fn [_ _] {:type :invoke, :f :write, :value [(rand-int n-keys) (swap! ctr inc)]})))

(defn workload
  "Watch-consistency workload."
  [_opts]
  (let [state (new-state)]
    {:client          (->WatchClient nil nil nil state)
     :checker         (checker)
     :generator       op-write
     :final-generator (gen/once {:type :invoke, :f :drain})}))
