(ns jepsen.crabwatchstore.client
  "jetcd-based etcd v3 gRPC client with leader-finding across all node endpoints.

   crab-watchstore does NOT proxy writes: a *follower* rejects every KV mutation
   (and leader-gated Range) with gRPC `UNAVAILABLE(14)` `etcdserver: not leader`
   (see src/server/grpc-kv.scm). It does not forward to the leader the way real
   etcd does. So — exactly as etcd's own clientv3 does — we hand the jetcd client
   ALL node endpoints and let it round-robin + retry until a call lands on the
   current leader (cw-u4a.34 verified this multi-node path end-to-end; every prior
   gRPC proof, incl. the clientv3 capstone, was single-node). When the leader dies
   and a survivor is elected (the .35 failover path), in-flight clients simply keep
   retrying and re-aim at the new leader.

   Transport: the store serves cleartext HTTP/2 (h2c), NOT TLS. jetcd selects
   plaintext from the `http://` URI scheme (an `https://` scheme would start a TLS
   handshake the h2c server can't answer). `round_robin` is set explicitly because
   jetcd/gRPC default to `pick_first`, which would pin every call to one endpoint —
   if that endpoint were a follower, every op would `UNAVAILABLE` forever.

   A transient `UNAVAILABLE` (not-leader, or a node we couldn't reach) means the op
   was REJECTED before it entered the Raft log — never applied — so it is safe to
   retry against another endpoint (bounded). Anything else (a client-side timeout, a
   definite error) propagates: the jepsen client turns it into an `:info` (unknown)
   op, which is correct — only definitely-not-applied rejections are retried here.
   A clean cas-failure is NOT an exception (it's `isSucceeded=false`), so it returns
   normally as a definite `:fail`."
  (:import (io.etcd.jetcd Client ClientBuilder ByteSequence KV KeyValue Txn)
           (io.etcd.jetcd.kv GetResponse TxnResponse)
           (io.etcd.jetcd.op Cmp Cmp$Op CmpTarget Op)
           (io.etcd.jetcd.options PutOption)
           (io.grpc StatusRuntimeException Status$Code)
           (java.net URI)
           (java.nio.charset StandardCharsets)
           (java.util.concurrent CompletableFuture TimeUnit)))

(def client-port
  "Each node's etcd v3 gRPC client port (the 4th field of the --cluster spec)."
  2379)

(def call-timeout-secs
  "Per-RPC deadline applied when dereffing jetcd's CompletableFutures."
  10)

(def max-retries
  "Bounded retries for transient UNAVAILABLE/not-leader (round-robin finds the
   leader within a few hops on a 5-node cluster; this is generous headroom)."
  200)

;; ---- byte <-> string (etcd keys/values are arbitrary bytes; we use UTF-8) ----

(defn ->bs ^ByteSequence [^String s]
  (ByteSequence/from s StandardCharsets/UTF_8))

(defn bs->str [^ByteSequence bs]
  (when bs (.toString bs StandardCharsets/UTF_8)))

;; ---- connection ----

(defn endpoints
  "http://<node>:<client-port> URI per node — `http` ⇒ h2c cleartext (no TLS)."
  [nodes]
  (mapv (fn [n] (URI. (str "http://" (name n) ":" client-port))) nodes))

(defn connect
  "A jetcd Client aimed at ALL node endpoints with round-robin balancing."
  ^Client [nodes]
  (let [^ClientBuilder b      (Client/builder)
        ^"[Ljava.net.URI;" us (into-array URI (endpoints nodes))]
    (.endpoints b us)
    ;; round_robin (not gRPC's default pick_first): each new RPC advances to the
    ;; next endpoint, so a follower's UNAVAILABLE(not-leader) on one attempt is
    ;; followed by a different endpoint on the next — combined with with-retry's
    ;; re-issue loop this reliably lands on the leader. (Tuning retryMaxAttempts /
    ;; waitForReady here was tried and REGRESSED — it fails RPCs before the picker
    ;; cycles to the leader; jetcd's defaults + with-retry are what work.)
    (.loadBalancerPolicy b "round_robin")
    (.build b)))

;; ---- error classification ----

(defn- causes
  "The exception's cause chain as a seq (self first)."
  [^Throwable t]
  (take-while some? (iterate (fn [^Throwable e] (.getCause e)) t)))

(defn retryable?
  "True iff the throwable is a transient UNAVAILABLE / not-leader / leader-changed
   rejection — i.e. the op definitely did not apply, so retrying is safe."
  [^Throwable t]
  (boolean
    (some (fn [^Throwable e]
            (or (and (instance? StatusRuntimeException e)
                     (= Status$Code/UNAVAILABLE
                        (.getCode (.getStatus ^StatusRuntimeException e))))
                (let [m (str (.getMessage e))]
                  (re-find #"(?i)not leader|leader changed|UNAVAILABLE" m))))
          (causes t))))

(defn deref-future
  "Block on a jetcd CompletableFuture with a deadline."
  [^CompletableFuture fut]
  (.get fut call-timeout-secs TimeUnit/SECONDS))

(defn with-retry
  "Run thunk f; on a transient UNAVAILABLE/not-leader, back off briefly and retry
   (bounded). On exhaustion or any non-retryable throwable, the throwable
   propagates (→ jepsen :info)."
  [f]
  (loop [tries 0]
    (let [res (try {:ok (f)}
                   (catch Throwable e
                     (if (and (< tries max-retries) (retryable? e))
                       {:retry e}
                       (throw e))))]
      (if (contains? res :ok)
        (:ok res)
        (do (Thread/sleep 50) (recur (inc tries)))))))

;; ---- KV ops (the jepsen-op primitives the workloads build on) ----

(defn kv-get
  "Linearizable read of key `k`. Returns the value string, or nil if absent."
  [^KV kv ^String k]
  (with-retry
    (fn []
      (let [^GetResponse resp (deref-future (.get kv (->bs k)))
            kvs  (.getKvs resp)]
        (when (seq kvs)
          (bs->str (.getValue ^KeyValue (first kvs))))))))

(defn kv-put
  "Write `v` to key `k`."
  [^KV kv ^String k ^String v]
  (with-retry
    (fn []
      (deref-future (.put kv (->bs k) (->bs v)))
      :ok)))

(defn kv-cas
  "Atomic compare-and-set via an etcd Txn: if the current value of `k` equals
   `old`, put `new`. Returns true if the swap committed, false if the compare
   failed (a definite no-op). One Raft entry."
  [^KV kv ^String k ^String old ^String new]
  (with-retry
    (fn []
      (let [cmp  (Cmp. (->bs k) Cmp$Op/EQUAL (CmpTarget/value (->bs old)))
            op   (Op/put (->bs k) (->bs new) PutOption/DEFAULT)
            ^Txn txn  (-> (.txn kv)
                          (.If (into-array Cmp [cmp]))
                          (.Then (into-array Op [op])))
            ^TxnResponse resp (deref-future (.commit txn))]
        (.isSucceeded resp)))))

;; ---- transaction primitives for the Elle append workload ----

(defn kv-get-rev
  "Read key `k`, returning {:present? bool, :value str-or-nil, :mod-revision long}.
   An absent key reports mod_revision 0, which an etcd Txn Mod-compare also sees as
   0 — so the same guard works for both 'unchanged since read' and 'still absent'.
   Retries transient UNAVAILABLE (a read is always safe to retry)."
  [^KV kv ^String k]
  (with-retry
    (fn []
      (let [^GetResponse resp (deref-future (.get kv (->bs k)))
            kvs  (.getKvs resp)]
        (if (seq kvs)
          (let [^KeyValue c (first kvs)]
            {:present? true
             :value        (bs->str (.getValue c))
             :mod-revision (.getModRevision c)})
          {:present? false :value nil :mod-revision 0})))))

(defn kv-txn-guarded
  "Atomic multi-key transaction: If EVERY key's current mod_revision equals its
   guard, Then apply the puts. `guards` is a seq of [key mod-rev]; `puts` a seq of
   [key value-string]. Returns isSucceeded (false ⇒ some guard failed ⇒ a concurrent
   writer moved a key ⇒ definite no-op, retry the optimistic read). One Raft entry.
   Retries transient UNAVAILABLE (a rejected Txn never applied)."
  [^KV kv guards puts]
  (with-retry
    (fn []
      (let [cmps (into-array Cmp
                   (mapv (fn [[k rev]]
                           (Cmp. (->bs k) Cmp$Op/EQUAL (CmpTarget/modRevision rev)))
                         guards))
            ops  (into-array Op
                   (mapv (fn [[k v]]
                           (Op/put (->bs k) (->bs v) PutOption/DEFAULT))
                         puts))
            ^Txn txn  (-> (.txn kv) (.If cmps) (.Then ops))
            ^TxnResponse resp (deref-future (.commit txn))]
        (.isSucceeded resp)))))
