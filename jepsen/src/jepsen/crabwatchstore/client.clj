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
  "Per-RPC deadline applied when dereffing jetcd's CompletableFutures. Kept modest so
   an attempt that lands on a partitioned-away / dead endpoint returns quickly and the
   round-robin picker advances to another endpoint instead of blocking the full window.
   Raised from 6 to accommodate the slower-but-correct write acks under an active cross-shard
   Watch stream (the store commits them; they just need headroom past the watch-stream tax)."
  20)

(def max-retries
  "Bounded retries for transient UNAVAILABLE/not-leader (round-robin finds the
   leader within a few hops on a 5-node cluster; this is generous headroom)."
  200)

(def retry-deadline-ms
  "WALL-CLOCK cap on a single op's leader-finding retries. Under NO fault an op
   succeeds in well under a second; under a sustained partition/kill an op on the
   wrong side would otherwise retry ~forever (200 hops x a multi-second call each),
   stalling the jepsen phase drain. After this budget the op gives up and propagates
   -> jepsen :info (indeterminate), which is the truthful outcome and lets the run
   terminate. This is what makes the fault workloads (which cw-u4a.34 never ran) drain."
  20000)

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

(defn retry-until
  "Run thunk f; while `again?` says the throwable is transient AND the wall-clock
   deadline has not passed, back off briefly and retry. Otherwise the throwable
   propagates (→ jepsen :info). The deadline (not just a hop count) is what bounds an
   op stuck on the wrong side of a partition so the jepsen phase can drain."
  [again? f]
  (let [deadline (+ (System/currentTimeMillis) retry-deadline-ms)]
    (loop []
      (let [res (try {:ok (f)}
                     (catch Throwable e
                       (if (and (< (System/currentTimeMillis) deadline) (again? e))
                         {:retry e}
                         (throw e))))]
        (if (contains? res :ok)
          (:ok res)
          (do (Thread/sleep 50) (recur)))))))

(defn with-retry
  "Run thunk f; on a transient UNAVAILABLE/not-leader, back off briefly and retry
   until the wall-clock budget elapses. On a non-retryable throwable or budget
   exhaustion, the throwable propagates (→ jepsen :info)."
  [f]
  (retry-until retryable? f))

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

(defn kv-put-rev
  "Write `v` to key `k`, returning the store revision the put committed at
   (PutResponse header revision). The watch workload uses this to correlate each
   committed write with the revision the watcher must deliver it at."
  [^KV kv ^String k ^String v]
  (with-retry
    (fn []
      (let [resp (deref-future (.put kv (->bs k) (->bs v)))]
        (.getRevision (.getHeader ^io.etcd.jetcd.kv.PutResponse resp))))))

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
