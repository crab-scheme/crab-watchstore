(ns jepsen.crabwatchstore.watch
  "Watch-consistency workload — STUB for cw-u4a.34; FULL coverage is cw-u4a.35.

   crab-watchstore's Watch service (etcdserverpb.Watch, bidi streaming) is proven
   compatible by test/clientv3-compat (12 assertions: CREATED ack, ordered
   PUT/DELETE events, prefix watch, historical replay with WithRev). The jepsen
   WATCH workload that cw-u4a.35 will build on top of jetcd's `Watch.Watcher`
   (callback/stream) verifies, UNDER NEMESIS, that:

     * every committed mutation is delivered to every live watcher, exactly once,
       in strictly increasing revision order (no gaps, no reordering);
     * a watch established WithRev replays history then transitions to live events
       without a gap across a leader failover;
     * a watcher on a partitioned-away node either stalls or is cancelled — it never
       observes a divergent (split-brain) event stream.

   Integrating jetcd's asynchronous Watch.Listener into jepsen's synchronous op
   model (latch-per-event, watcher lifecycle across open!/close!, revision-anchored
   event assertions) is the substance of cw-u4a.35 and is intentionally NOT done
   here — the cw-u4a.34 smoke proves the harness via the register/cas linearizable
   core. Selecting --workload watch errors with this pointer rather than silently
   running an empty test."
  (:require [jepsen [client :as client]
                    [checker :as checker]
                    [generator :as gen]]))

(defrecord Client []
  client/Client
  (open!     [this test n] this)
  (setup!    [this test])
  (invoke!   [this test op]
    (throw (ex-info "watch workload is not implemented in cw-u4a.34 — see cw-u4a.35"
                    {:workload :watch})))
  (teardown! [this test])
  (close!    [this test]))

(def always-valid
  "Trivial checker — an empty/stub history is vacuously valid. The real watch
   consistency checker is cw-u4a.35."
  (reify checker/Checker
    (check [_ _ _ _] {:valid? true, :stub :cw-u4a.35})))

(defn workload
  "STUB. Wired into the switch so --workload watch fails fast with a clear pointer
   to cw-u4a.35 instead of being silently absent."
  [_opts]
  {:client    (->Client)
   :checker   always-valid
   :generator (gen/limit 0 (fn [_ _] {:type :invoke, :f :watch}))})
