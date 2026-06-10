(defproject jepsen.crabwatchstore "0.1.0-SNAPSHOT"
  :description "Jepsen tests for crab-watchstore (an etcd v3-compatible, Raft-replicated KV store written in CrabScheme), driven over the etcd gRPC API with the official jetcd client."
  :url "https://github.com/crab-scheme/crab-watchstore"
  :license {:name "MIT"}
  :main jepsen.crabwatchstore.core
  :jvm-opts ["-Xmx10g"      ; 5-node history + per-key Knossos analysis + save-2! serialization
             "-Djava.awt.headless=true"
             ;; Knossos / Elle want a deep stack on big histories.
             "-server"
             ;; Netty (jetcd's gRPC transport) wants these on modern JDKs to use
             ;; direct buffers + reflective accessors without warnings/failures.
             "-Dio.netty.tryReflectionSetAccessible=true"
             "--add-opens" "java.base/java.nio=ALL-UNNAMED"
             "--add-opens" "java.base/jdk.internal.misc=ALL-UNNAMED"]
  :dependencies [[org.clojure/clojure "1.11.4"]
                 [jepsen "0.3.11"]
                 ;; Official etcd v3 Java client (gRPC). Replaces carmine: the
                 ;; store speaks the etcd v3 gRPC API, not RESP.
                 [io.etcd/jetcd-core "0.8.3"]]
  :repl-options {:init-ns jepsen.crabwatchstore.core})
