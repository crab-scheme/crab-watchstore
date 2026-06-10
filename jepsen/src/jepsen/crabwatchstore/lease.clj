(ns jepsen.crabwatchstore.lease
  "Lease workload (cw-u4a.35) — verifies crab-watchstore's leader-driven lease expiry
   under nemesis, the etcd Lease contract:

     * a key attached to a lease that is CONTINUOUSLY kept alive SURVIVES (it must NOT
       be deleted — premature expiry would be a consensus/timer bug);
     * a key attached to a lease that is NOT kept alive is DELETED after the TTL lapses
       (expiry is proposed through Raft on the leader, so it holds across a failover).

   Two op types, each a self-contained lifecycle (so the checker needs no linearization):

     :keep    grant(TTL=3s); put key WITH the lease; keepAliveOnce ~3x over the TTL
              window; GET -> :present? . A premature delete here = BUG.
     :expire  grant(TTL=2s); put key WITH the lease; do NOT keep alive; wait well past
              the TTL; GET -> :present? . A surviving key here = expiry didn't fire.

   Fault tolerance vs. real bug: an op whose grant / keepAlive / read was interrupted by
   the active nemesis (couldn't reach a leader) is INCONCLUSIVE -> returned as :info and
   excluded from the verdict — only :clean ops (every step succeeded) are asserted, so a
   partition that merely delays progress can't masquerade as a violation. The SAFETY
   direction we hunt is a kept-alive key vanishing; the expiry direction is the
   functional/liveness check."
  (:require [clojure.tools.logging :refer [info warn]]
            [jepsen [checker :as checker]
                    [client :as client]
                    [generator :as gen]]
            [jepsen.crabwatchstore.client :as cw])
  (:import (io.etcd.jetcd Client KV Lease ByteSequence)
           (io.etcd.jetcd.lease LeaseGrantResponse LeaseKeepAliveResponse)
           (io.etcd.jetcd.options PutOption)
           (java.util.concurrent TimeUnit)))

(def key-prefix "lease/")

;; ---- lease primitives (leader-gated) ----
;; The Lease service is leader-gated like KV, BUT on this store a grant/keepalive that
;; lands on a FOLLOWER surfaces as gRPC INTERNAL "lease-grant: unexpected ack" (the
;; non-leader lease path returns a malformed ack rather than UNAVAILABLE "not leader"
;; — a multi-node lease-path quirk; see docs/jepsen-validation.md). That INTERNAL is
;; NOT in the client's UNAVAILABLE retry set, so a grant would fail the moment the
;; round-robin client hit a follower. A failed grant grants NOTHING (definitely did not
;; apply), so it is safe to retry until it lands on the leader — this lease-local retry
;; treats the lease INTERNAL/"unexpected ack"/"not leader" acks as transient too.

(defn- lease-retryable?
  [^Throwable t]
  (let [m (str (.getMessage t))]
    (boolean (or (cw/retryable? t)
                 (re-find #"(?i)unexpected ack|not leader|leader changed|INTERNAL" m)))))

(defn with-lease-retry
  "Like cw/with-retry but also retries the leader-gated lease INTERNAL acks
   (wall-clock bounded, so a fault can't stall the drain)."
  [f]
  (cw/retry-until lease-retryable? f))

(defn grant!
  "Grant a lease with `ttl` seconds; returns the lease id."
  [^Lease lease ttl]
  (with-lease-retry
    (fn []
      (let [^LeaseGrantResponse r (.get (.grant lease (long ttl))
                                        cw/call-timeout-secs TimeUnit/SECONDS)]
        (.getID r)))))

(defn put-with-lease!
  "Put k=v attached to lease `id`."
  [^KV kv ^String k ^String v id]
  (cw/with-retry
    (fn []
      (let [opt (-> (PutOption/builder) (.withLeaseId (long id)) (.build))]
        (.get (.put kv (cw/->bs k) (cw/->bs v) opt)
              cw/call-timeout-secs TimeUnit/SECONDS)
        :ok))))

(defn keep-alive-once!
  "One keepalive round-trip; returns the refreshed TTL (>0 alive, <=0 gone)."
  [^Lease lease id]
  (with-lease-retry
    (fn []
      (let [^LeaseKeepAliveResponse r (.get (.keepAliveOnce lease (long id))
                                            cw/call-timeout-secs TimeUnit/SECONDS)]
        (.getTTL r)))))

(defrecord LeaseClient [conn kv lease]
  client/Client
  (open! [this test n]
    (let [c (cw/connect (:nodes test))]
      (assoc this :conn c :kv (.getKVClient c) :lease (.getLeaseClient c))))

  (setup! [this test])

  (invoke! [this test op]
    (let [^KV kv       (:kv this)
          ^Lease lease (:lease this)
          tag          (str (:f op) "-" (rand-int 1000000))
          k            (str key-prefix tag)]
      (case (:f op)
        ;; --------- kept-alive key MUST survive ---------
        :keep
        (try
          (let [ttl     6
                id      (grant! lease ttl)
                _       (put-with-lease! kv k "alive" id)
                ;; keep alive: renew every ~1.2s, 3 times. The gap (plus keepalive
                ;; latency) stays well under the 6s TTL, so the lease never lapses.
                renewed (loop [i 0, n 0]
                          (if (>= i 3)
                            n
                            (let [t (keep-alive-once! lease id)]
                              (Thread/sleep 1200)
                              (recur (inc i) (if (pos? t) (inc n) n)))))
                present? (some? (cw/kv-get kv k))]
            ;; clean iff every keepalive refreshed a live lease (so the key was
            ;; under continuous keepalive the whole window).
            (assoc op :type (if (= renewed 3) :ok :info)
                   :value {:phase :keep :present? present? :renewed renewed
                           :clean (= renewed 3)}))
          (catch Throwable e
            (assoc op :type :info :value {:phase :keep :error (str (.getMessage e))})))

        ;; --------- un-renewed key MUST expire ---------
        :expire
        (try
          (let [ttl 3
                id  (grant! lease ttl)
                _   (put-with-lease! kv k "ephemeral" id)
                ;; no keepalive: POLL the key until it is reaped, up to 6x the TTL.
                ;; Returning the moment it disappears (fast path) AND tolerating a slow
                ;; lease-tick makes :present?=true a genuine 6x-TTL leak, not a
                ;; wait-too-short artifact.
                present? (loop [waited 0]
                           (Thread/sleep 1000)
                           (let [p (some? (cw/kv-get kv k))]
                             (cond (not p)            false                ; reaped — contract holds
                                   (>= waited (* 6 ttl)) true              ; survived 6x TTL — leak
                                   :else              (recur (inc waited)))))]
            (assoc op :type :ok
                   :value {:phase :expire :present? present? :clean true :ttl ttl :lease id}))
          (catch Throwable e
            (assoc op :type :info :value {:phase :expire :error (str (.getMessage e))})))) ))

  (teardown! [this test])
  (close! [this test] (.close ^io.etcd.jetcd.Client (:conn this))))

(defn checker
  "Assert the lease contract over the clean ops."
  []
  (reify checker/Checker
    (check [_ test history _opts]
      (let [oks   (filter #(= :ok (:type %)) history)
            keeps (filter #(= :keep   (:f %)) oks)
            exps  (filter #(= :expire (:f %)) oks)
            ;; THE bug: a continuously kept-alive key got deleted.
            premature (->> keeps
                           (filter #(and (get-in % [:value :clean])
                                         (not (get-in % [:value :present?]))))
                           (mapv #(select-keys (:value %) [:present? :renewed]))
                           )
            ;; expiry didn't fire: an un-renewed key survived well past its TTL.
            leaked    (->> exps
                           (filter #(and (get-in % [:value :clean])
                                         (get-in % [:value :present?])))
                           (mapv #(select-keys (:value %) [:present? :ttl])))
            keeps-clean (count (filter #(get-in % [:value :clean]) keeps))
            exps-clean  (count (filter #(get-in % [:value :clean]) exps))
            valid? (and (empty? premature) (empty? leaked))]
        {:valid?               valid?
         :keep-ops             (count keeps)
         :keep-clean           keeps-clean
         :expire-ops           (count exps)
         :expire-clean         exps-clean
         :premature-expiry     premature          ; non-empty => BUG (kept key vanished)
         :failed-expiry        leaked}))))         ; non-empty => BUG (expiry didn't fire)

(def op-keep   (fn [_ _] {:type :invoke, :f :keep}))
(def op-expire (fn [_ _] {:type :invoke, :f :expire}))

(defn workload
  "Lease lifecycle workload (a property checker, not full linearizability)."
  [_opts]
  {:client    (->LeaseClient nil nil nil)
   :checker   (checker)
   :generator (gen/mix [op-keep op-expire])})
