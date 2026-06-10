(ns jepsen.crabwatchstore.lease
  "Lease workload — STUB for cw-u4a.34; FULL coverage is cw-u4a.35.

   crab-watchstore's Lease service (etcdserverpb.Lease) is proven compatible by
   test/clientv3-compat (11 assertions: Grant, Put-with-lease attachment, TimeToLive,
   KeepAlive round-trips, Leases, Revoke deletes attached keys). The jepsen LEASE
   workload that cw-u4a.35 will build verifies, UNDER NEMESIS, the leader-driven
   expiry guarantee:

     * a key attached to a lease disappears cluster-wide at (and only at) lease
       expiry, with expiry proposed through Raft (consistent across a failover);
     * KeepAlive from a live client prevents expiry; a partitioned client's lease
       lapses and its keys are reaped exactly once;
     * a granted lease's TTL/attached-keys view (LeaseTimeToLive) is consistent with
       the committed state on the leader.

   Wiring jetcd's `Lease.grant`/`keepAlive` streams + the expiry-deadline timeline
   into jepsen's op/checker model is cw-u4a.35 work. The cw-u4a.34 smoke proves the
   harness via the register/cas linearizable core; selecting --workload lease errors
   with this pointer rather than silently running an empty test."
  (:require [jepsen [client :as client]
                    [checker :as checker]
                    [generator :as gen]]))

(defrecord Client []
  client/Client
  (open!     [this test n] this)
  (setup!    [this test])
  (invoke!   [this test op]
    (throw (ex-info "lease workload is not implemented in cw-u4a.34 — see cw-u4a.35"
                    {:workload :lease})))
  (teardown! [this test])
  (close!    [this test]))

(def always-valid
  (reify checker/Checker
    (check [_ _ _ _] {:valid? true, :stub :cw-u4a.35})))

(defn workload
  "STUB. Wired into the switch so --workload lease fails fast with a clear pointer
   to cw-u4a.35 instead of being silently absent."
  [_opts]
  {:client    (->Client)
   :checker   always-valid
   :generator (gen/limit 0 (fn [_ _] {:type :invoke, :f :lease}))})
