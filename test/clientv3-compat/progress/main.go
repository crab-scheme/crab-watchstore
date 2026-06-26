// cw-5w8 #2: prove PERIODIC progress_notify emission.
//
// Create a watch WithProgressNotify (etcd's progress_notify=true create flag),
// send NO RequestProgress, keep the key idle, and assert a server-driven
// progress notification (IsProgressNotify, no events, current revision) arrives
// within ~2 idle intervals. This is the mechanism the kube-apiserver watch cache
// uses to mark a static-resource informer "synced"; without it KCM controllers
// hang on "Waiting for caches to sync" (cw-5w8 root cause #2).
//
// Usage: go run ./progress <endpoint>   (default 127.0.0.1:2379)
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

func main() {
	addr := "127.0.0.1:2379"
	if len(os.Args) > 1 {
		addr = os.Args[1]
	}
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{addr},
		DialTimeout: 8 * time.Second,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "FATAL: clientv3.New(%s): %v\n", addr, err)
		os.Exit(2)
	}
	defer cli.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	// Advance the store revision past 0 (k8s always has data; clientv3's
	// IsProgressNotify requires Header.Revision != 0). Then watch an UNRELATED
	// idle key WithProgressNotify and never send RequestProgress.
	if _, err := cli.Put(ctx, "cw-5w8-seed", "v"); err != nil {
		fmt.Printf("FAIL: seed put: %v\n", err)
		os.Exit(1)
	}
	wch := cli.Watch(ctx, "cw-5w8-idle-key", clientv3.WithProgressNotify())

	deadline := time.After(15 * time.Second) // > 2x the 5s server interval
	for {
		select {
		case <-deadline:
			fmt.Println("FAIL: no periodic progress notification within 15s idle")
			os.Exit(1)
		case resp, ok := <-wch:
			if !ok {
				fmt.Println("FAIL: watch channel closed before a progress notification")
				os.Exit(1)
			}
			if resp.Err() != nil {
				fmt.Printf("FAIL: watch error: %v\n", resp.Err())
				os.Exit(1)
			}
			if resp.IsProgressNotify() {
				fmt.Printf("ok   periodic progress notify: rev=%d events=%d\n",
					resp.Header.Revision, len(resp.Events))
				fmt.Println("PASS")
				return
			}
			// Created ack / any stray frame: keep waiting for the periodic tick.
		}
	}
}
