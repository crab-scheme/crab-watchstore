// test/health-probe/main.go — a tiny standard gRPC Health Checking Protocol client
// for crab-watchstore (cw-u4a.33).  Used by test/health-metrics.sh because grpcurl is
// not on PATH; reuses the same grpc-go (v1.59.0) the .24 clientv3-compat proof uses, so
// it builds offline from the module cache.
//
// Usage:  go run . <host:port> [service]
// Prints the serving status NAME (SERVING / NOT_SERVING / ...) on stdout; exits 0 iff SERVING.
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: health-probe <host:port> [service]")
		os.Exit(2)
	}
	target := os.Args[1]
	service := ""
	if len(os.Args) >= 3 {
		service = os.Args[2]
	}

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()

	// Plaintext h2c (the cluster's default client port is cleartext gRPC). WithBlock so the
	// dial waits for the connection (up to the ctx deadline) before the Check RPC.
	conn, err := grpc.DialContext(ctx, target,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock())
	if err != nil {
		fmt.Fprintf(os.Stderr, "dial %s: %v\n", target, err)
		os.Exit(1)
	}
	defer conn.Close()

	resp, err := healthpb.NewHealthClient(conn).Check(ctx, &healthpb.HealthCheckRequest{Service: service})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Check: %v\n", err)
		os.Exit(1)
	}

	fmt.Println(resp.Status.String())
	if resp.Status == healthpb.HealthCheckResponse_SERVING {
		os.Exit(0)
	}
	os.Exit(1)
}
