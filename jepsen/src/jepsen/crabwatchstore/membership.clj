(ns jepsen.crabwatchstore.membership
  "Membership-change nemesis (cw-u4a.35) — the unique deliverable unlocked by
   crab-watchstore's DYNAMIC membership (joint-consensus Raft + the etcd Cluster
   gRPC service, cw-u4a.28-.31).

   Mid-run it reconfigures the LIVE 5-node cluster through the etcd Cluster gRPC
   service (jetcd ClusterClient): it removes a *voter* (5 -> 4) and then re-adds the
   same, still-running node (4 -> 5), repeating. Throughout, the cluster MUST keep
   serving and stay linearizable — that is the property under test (run it under a
   linearizable workload: --workload register/cas --nemesis membership).

   The real-process re-add path
   ----------------------------
   * MemberRemove(id): drop a voter from the config. The removed node's PROCESS keeps
     running and stays wired into the cs-net mesh; the leader just stops replicating to
     it. (This is one ConfChange entry in the SAME Raft log as the KV writes, so the
     reconfiguration is linearized with the data — no split brain.)
   * MemberAdd([\"http://<name>:2380\"]): re-add by NAME. crab-watchstore derives the
     member identity from the peerURL *host* (peer-url->name, grpc-kv.scm) and node
     addresses are STATIC (from --cluster), so the host need only be the node NAME —
     the still-live mesh connection carries the catch-up AppendEntries. The leader
     re-admits <name>, replicates the prior log to it, the joint change commits, and the
     node is a full voter again. This is exactly the etcdctl
     `member add d --peer-urls=http://d:2380` path proven by test/etcd-cluster-grpc.sh,
     applied to a live, previously-removed node.

   We target a NON-leader voter (discovered via Maintenance/Status' leader id) so the
   reconfiguration is isolated from leader failover (the :kill nemesis already exercises
   failover). If the leader can't be determined we fall back to a rotating victim and
   still tolerate the leader-stepdown edge cases (a retried remove that now sees the
   member gone, or a retried add that now sees it already present, both count as success).

   Composes as a jepsen nemesis PACKAGE (:remove-member / :add-member fs), wired into
   core.clj's compose-packages alongside the partition + db (kill/pause) packages — so
   `--nemesis membership` and `--nemesis partition,kill,membership` both work."
  (:require [clojure.set :as set]
            [clojure.tools.logging :refer [info warn]]
            [jepsen [generator :as gen]
                    [nemesis :as nemesis]]
            [jepsen.crabwatchstore.client :as cw])
  (:import (io.etcd.jetcd Client Cluster Maintenance)
           (io.etcd.jetcd.cluster Member MemberListResponse)
           (io.etcd.jetcd.maintenance StatusResponse)
           (java.net URI)
           (java.util.concurrent TimeUnit)))

(def reconfig-timeout-secs
  "MemberAdd/MemberRemove BLOCK on the server until the ConfChange commits (the
   peer-poller keeps ticking so it makes progress); allow generous headroom."
  15)

(def min-voters-before-remove
  "Only remove when the cluster is at full strength (5 voters), so we always go
   5 -> 4 and the alternating add brings it back to 5 — never 4 -> 3."
  5)

;; ---- jetcd Cluster / Maintenance helpers -----------------------------------

(defn- cluster-client ^Cluster [^Client c] (.getClusterClient c))
(defn- maint-client   ^Maintenance [^Client c] (.getMaintenanceClient c))

(defn- member-retryable?
  "Transient for a membership op: UNAVAILABLE/not-leader (re-aim at the leader) OR
   'reconfiguration in progress' (a prior ConfChange — incl. the paired remove — is
   still settling; back off and retry rather than fast-fail)."
  [^Throwable t]
  (boolean (or (cw/retryable? t)
               (re-find #"(?i)reconfiguration in progress|in progress|leader changed"
                        (str (.getMessage t))))))

(defn with-member-retry
  "Like cw/with-retry but also rides out an in-flight ConfChange (wall-clock bounded)."
  [f]
  (cw/retry-until member-retryable? f))

(defn list-members
  "Current config as a vector of {:id long :name str :learner? bool}, via any
   reachable endpoint (round-robin + bounded retry on transient UNAVAILABLE)."
  [^Client c]
  (cw/with-retry
    (fn []
      (let [^MemberListResponse resp (.get (.listMember (cluster-client c))
                                           reconfig-timeout-secs TimeUnit/SECONDS)]
        (mapv (fn [^Member m]
                {:id (.getId m) :name (.getName m) :learner? (.isLearner m)})
              (.getMembers resp))))))

(defn leader-id
  "Best-effort current leader member-id, or nil. Maintenance/Status reports the
   leader id directly (StatusResponse.getLeader); we probe endpoints until one
   answers. Pinning quirks / a node mid-election just yield nil → caller falls back."
  [^Client c nodes]
  (some (fn [n]
          (try
            (let [ep                (str "http://" (name n) ":" cw/client-port)
                  ^StatusResponse s (.get (.statusMember (maint-client c) ep)
                                          5 TimeUnit/SECONDS)
                  l                 (.getLeader s)]
              (when (pos? l) l))
            (catch Throwable _ nil)))
        nodes))

(defn pick-victim
  "Choose a voter to remove: prefer a NON-leader voter, rotate by `idx` for variety.
   Returns {:id :name} or nil when the cluster is not at full strength (don't shrink
   below the safe set)."
  [members leader-id idx]
  (let [voters (filterv (complement :learner?) members)]
    (when (>= (count voters) min-voters-before-remove)
      (let [cands (or (seq (filterv #(not= (:id %) leader-id) voters)) voters)]
        (nth (vec cands) (mod idx (count cands)))))))

(defn remove-member!
  "MemberRemove(id), retrying transient UNAVAILABLE. If a retry (after a leader
   stepped down) now reports the member gone, that IS success."
  [^Client c id]
  (with-member-retry
    (fn []
      (try
        (.get (.removeMember (cluster-client c) id) reconfig-timeout-secs TimeUnit/SECONDS)
        :removed
        (catch Throwable e
          (if (re-find #"(?i)member not found|not.*a.*member|no such member"
                       (str (.getMessage e)))
            :removed
            (throw e)))))))

(defn add-member!
  "MemberAdd by NAME (peerURL host = node name), as a voter. Retries transient
   UNAVAILABLE; an 'already exists' from a retry of a committed add IS success."
  [^Client c name-str]
  (let [peers ^java.util.List [(URI. (str "http://" name-str ":2380"))]]
    (with-member-retry
      (fn []
        (try
          (.get (.addMember (cluster-client c) peers false)
                reconfig-timeout-secs TimeUnit/SECONDS)
          :added
          (catch Throwable e
            (if (re-find #"(?i)already exists|already part|exists as"
                         (str (.getMessage e)))
              :added
              (throw e))))))))

;; ---- the nemesis ------------------------------------------------------------

(defn nemesis
  "A reconfiguration nemesis. CONFIG-DERIVED (not state-tracked): each op reads the
   live MemberList and acts on the delta vs the full node set, so a remove that
   commits-then-throws (timeout / leader stepdown after Cnew) can never strand the
   cluster below 5 voters — the next :add-member re-adds whatever node is missing.
   This self-heals and guarantees the cluster returns to full strength (otherwise a
   reduced cluster with leader churn blocks the workload's ops and hangs the run)."
  []
  (let [conn (atom nil)
        idx  (atom 0)]
    (reify
      nemesis/Reflection
      (fs [_] #{:remove-member :add-member})

      nemesis/Nemesis
      (setup! [this test]
        (reset! conn (cw/connect (:nodes test)))
        this)

      (invoke! [this test op]
        (let [^Client c @conn
              full       (set (map name (:nodes test)))]
          (case (:f op)
            :remove-member
            (try
              (let [members (list-members c)
                    voters  (filterv (complement :learner?) members)
                    lid     (leader-id c (:nodes test))
                    victim  (pick-victim members lid @idx)]
                (cond
                  (< (count voters) (count full))      ; already reduced — let :add heal
                  (assoc op :type :info :value :already-reduced)
                  (nil? victim)
                  (assoc op :type :info :value :no-removable-voter)
                  :else
                  (do (info "membership: removing voter" (:name victim)
                            "(id" (:id victim) ") leader-id" lid)
                      (swap! idx inc)
                      (remove-member! c (:id victim))
                      (assoc op :type :info :value {:removed (:name victim) :id (:id victim)}))))
              (catch Throwable e
                (warn e "membership: remove failed")
                (assoc op :type :info :value {:error (str (.getMessage e))})))

            :add-member
            (try
              (let [members (list-members c)
                    names   (set (map :name members))
                    missing (sort (set/difference full names))]
                (if (empty? missing)
                  (assoc op :type :info :value :complete)
                  (do (info "membership: re-adding voter(s)" (vec missing))
                      (doseq [m missing] (add-member! c m))
                      (assoc op :type :info :value {:added (vec missing)}))))
              (catch Throwable e
                (warn e "membership: add failed")
                (assoc op :type :info :value {:add-error (str (.getMessage e))}))))))

      (teardown! [this test]
        (when-let [^Client c @conn]
          (try (.close c) (catch Throwable _ nil)))
        nil))))

;; ---- package (composes into core.clj's compose-packages) --------------------

(def remove-op (fn [_ _] {:type :info, :f :remove-member}))
(def add-op    (fn [_ _] {:type :info, :f :add-member}))

(defn package
  "Nemesis package. The generator only fires when :membership is in (:faults opts),
   so it is safe to ALWAYS include in compose-packages (an unrequested package just
   contributes a nil generator + an idle nemesis, exactly like the built-ins)."
  [opts]
  (let [needed? (contains? (set (:faults opts)) :membership)
        gen     (->> (gen/flip-flop remove-op (gen/repeat add-op))
                     (gen/stagger (:interval opts 10)))]
    {:generator       (when needed? gen)
     ;; Heal: re-add any still-removed node so the cluster ends at 5 voters before
     ;; the final reads. Emit ONCE (a bare fn would loop forever in the final phase).
     :final-generator (when needed? (gen/once add-op))
     :nemesis         (nemesis)
     :perf            #{{:name  "membership"
                         :start #{:remove-member}
                         :stop  #{:add-member}
                         :color "#A0C8E9"}}}))
