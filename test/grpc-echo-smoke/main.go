// A tiny REAL gRPC (grpc-go) client driving the crabscheme h2c streaming
// transport (cw-u4a.23) against the Scheme echo server (test/grpc-echo-main.scm):
//
//	ServerStream : one request  -> three responses + status trailer
//	BidiStream   : client sends N, server echoes each + status trailer
//
// Uses a raw []byte codec so the smoke is protobuf-free — it proves the FRAMING
// + TRAILERS + bidi message interleaving, not any message schema.
package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// rawCodec passes []byte payloads straight through.
type rawCodec struct{}

func (rawCodec) Marshal(v interface{}) ([]byte, error) { return v.([]byte), nil }
func (rawCodec) Unmarshal(data []byte, v interface{}) error {
	*(v.(*[]byte)) = data
	return nil
}
func (rawCodec) Name() string { return "raw" }

func main() {
	addr := "127.0.0.1:32123"
	if len(os.Args) > 1 {
		addr = os.Args[1]
	}
	cc, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.ForceCodec(rawCodec{})))
	if err != nil {
		panic(err)
	}
	defer cc.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// ---- SERVER-STREAM: one request -> N responses + status ----
	fmt.Println("== ServerStream /echo.Echo/ServerStream: send \"hi\" ==")
	ss, err := cc.NewStream(ctx, &grpc.StreamDesc{ServerStreams: true}, "/echo.Echo/ServerStream")
	if err != nil {
		panic(err)
	}
	if err := ss.SendMsg([]byte("hi")); err != nil {
		panic(err)
	}
	_ = ss.CloseSend()
	n := 0
	for {
		var out []byte
		if err := ss.RecvMsg(&out); err != nil {
			if err == io.EOF {
				break
			}
			panic(fmt.Sprintf("ServerStream recv: %v", err))
		}
		n++
		fmt.Printf("  <- %s\n", out)
	}
	fmt.Printf("  ServerStream: received %d responses + clean status trailer (EOF)\n\n", n)

	// ---- BIDI: client streams N, server echoes each ----
	fmt.Println("== BidiStream /echo.Echo/BidiStream: send m0, m1, m2 (interleaved) ==")
	bs, err := cc.NewStream(ctx, &grpc.StreamDesc{ClientStreams: true, ServerStreams: true}, "/echo.Echo/BidiStream")
	if err != nil {
		panic(err)
	}
	for i := 0; i < 3; i++ {
		req := []byte(fmt.Sprintf("m%d", i))
		if err := bs.SendMsg(req); err != nil {
			panic(err)
		}
		var out []byte
		if err := bs.RecvMsg(&out); err != nil {
			panic(fmt.Sprintf("BidiStream recv: %v", err))
		}
		fmt.Printf("  -> %-3s  <- %s\n", req, out)
	}
	_ = bs.CloseSend()
	for {
		var out []byte
		if err := bs.RecvMsg(&out); err != nil {
			if err == io.EOF {
				break
			}
			panic(fmt.Sprintf("BidiStream drain: %v", err))
		}
		fmt.Printf("  <- %s\n", out)
	}
	fmt.Println("  BidiStream: clean status trailer (EOF) after half-close")
	fmt.Println("\nSTREAMING SMOKE OK")
}
