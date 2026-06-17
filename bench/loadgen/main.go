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
	op := flag.String("op", "put", "put | get")
	serializable := flag.Bool("serializable", false, "get: serializable (stale-OK, any replica) vs linearizable (ReadIndex)")
	follower := flag.Bool("follower", false, "get: route to a follower node (multi-region local read) instead of the leader")
	seedKeys := flag.Int("seedkeys", 200, "get: keys per worker to seed before the timed read loop")
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
	// route a key to an endpoint: leader = endpoints[group % n]; a follower for
	// the same group (multi-region local read) = endpoints[(group+1) % n].
	route := func(key string, i int) int {
		switch *mode {
		case "one":
			return 0
		case "rr":
			return i % len(endpoints)
		default: // sharded
			ci := keyGroup(key, *shards) % len(endpoints)
			if *op == "get" && *follower {
				ci = (ci + 1) % len(endpoints)
			}
			return ci
		}
	}

	// get mode: seed each worker's keyspace once (linearizable puts to the
	// leader) so the timed read loop hits keys that exist.
	if *op == "get" {
		var swg sync.WaitGroup
		for g := 0; g < *conc; g++ {
			swg.Add(1)
			go func(gid int) {
				defer swg.Done()
				for i := 0; i < *seedKeys; i++ {
					key := fmt.Sprintf("k%d_%d", gid, i)
					ci := keyGroup(key, *shards) % len(endpoints)
					if *mode == "one" {
						ci = 0
					}
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					clients[ci].Put(ctx, key, val)
					cancel()
				}
			}(g)
		}
		swg.Wait()
	}

	var ok, fail uint64
	deadline := time.Now().Add(*dur)
	var wg sync.WaitGroup
	for g := 0; g < *conc; g++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			i := 0
			for time.Now().Before(deadline) {
				var key string
				if *op == "get" {
					key = fmt.Sprintf("k%d_%d", gid, i%*seedKeys)
				} else {
					key = fmt.Sprintf("k%d_%d", gid, i)
				}
				ci := route(key, i)
				i++
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				var err error
				if *op == "get" {
					var opts []clientv3.OpOption
					if *serializable {
						opts = append(opts, clientv3.WithSerializable())
					}
					_, err = clients[ci].Get(ctx, key, opts...)
				} else {
					_, err = clients[ci].Put(ctx, key, val)
				}
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
	unit := "writes/s"
	tag := ""
	if *op == "get" {
		unit = "reads/s"
		if *serializable {
			tag = " serializable"
		} else {
			tag = " linearizable"
		}
		if *follower {
			tag += " follower-routed"
		}
	}
	fmt.Printf("op=%s%s mode=%s shards=%d conc=%d dur=%s -> ok=%d fail=%d  THROUGHPUT=%.0f %s (fail-rate=%.1f%%)\n",
		*op, tag, *mode, *shards, *conc, dur.String(), ok, fail, float64(ok)/secs, unit, 100*float64(fail)/float64(ok+fail+1))
}
