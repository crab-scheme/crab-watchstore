// cwsloadgen — a shard-aware load harness for crab-watchstore (cw-ivt / cw-sva).
//
// Why: etcdctl `check perf` is rate-limited + unreliable on a busy host, and stock
// clients are NOT shard-aware, so most writes forward to a remote shard leader (the
// B3 flood). This harness drives a fixed-concurrency closed-loop PUT load over
// PERSISTENT clientv3 connections and, in -mode=sharded, routes each key directly to
// its shard-LEADER node — eliminating cluster-internal forwarding — to test whether
// multi-Raft-group sharding actually scales out.
//
// Routing: a key's group = FNV-1a-32(key) mod shards (IDENTICAL to the server's
// grpc-kv key-shard). The election stagger makes group S led by node (S mod nodes),
// so the leader endpoint = endpoints[(group) mod len(endpoints)]. -mode=rr round-robins
// (the forwarding baseline); -mode=one pins all to endpoints[0] (single-ingest baseline).
package main

import (
	"context"
	"flag"
	"fmt"
	"hash/fnv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

func keyGroup(key string, shards int) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() % uint32(shards))
}

func main() {
	eps := flag.String("endpoints", "127.0.0.1:2379", "comma-separated endpoints (ordered by node)")
	shards := flag.Int("shards", 1, "shard-group count (must match --shard-groups)")
	mode := flag.String("mode", "sharded", "sharded | rr | one")
	dur := flag.Duration("dur", 20*time.Second, "load duration")
	conc := flag.Int("conc", 64, "concurrent writers")
	valsize := flag.Int("valsize", 256, "value bytes")
	flag.Parse()

	endpoints := strings.Split(*eps, ",")
	// one persistent client per endpoint (so we can pin a key to its leader node).
	clients := make([]*clientv3.Client, len(endpoints))
	for i, e := range endpoints {
		c, err := clientv3.New(clientv3.Config{Endpoints: []string{e}, DialTimeout: 5 * time.Second})
		if err != nil {
			fmt.Printf("FATAL dial %s: %v\n", e, err)
			return
		}
		defer c.Close()
		clients[i] = c
	}
	val := strings.Repeat("x", *valsize)
	var ok, fail uint64
	deadline := time.Now().Add(*dur)
	var wg sync.WaitGroup
	for g := 0; g < *conc; g++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			i := 0
			for time.Now().Before(deadline) {
				key := fmt.Sprintf("k%d_%d", gid, i)
				i++
				var ci int
				switch *mode {
				case "sharded":
					ci = keyGroup(key, *shards) % len(endpoints)
				case "one":
					ci = 0
				default: // rr
					ci = i % len(endpoints)
				}
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				_, err := clients[ci].Put(ctx, key, val)
				cancel()
				if err != nil {
					atomic.AddUint64(&fail, 1)
				} else {
					atomic.AddUint64(&ok, 1)
				}
			}
		}(g)
	}
	wg.Wait()
	secs := dur.Seconds()
	fmt.Printf("mode=%s shards=%d conc=%d dur=%s -> ok=%d fail=%d  THROUGHPUT=%.0f writes/s (fail-rate=%.1f%%)\n",
		*mode, *shards, *conc, dur.String(), ok, fail, float64(ok)/secs, 100*float64(fail)/float64(ok+fail+1))
}
