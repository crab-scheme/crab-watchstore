#!/usr/bin/env bash
# test/grpc-echo-smoke.sh — STEP-1 streaming-transport smoke (cw-u4a.23).
#
# Starts the Scheme echo server (test/grpc-echo-main.scm) on h2c and drives it
# with a REAL grpc-go client (test/grpc-echo-smoke/) over the new .23 streaming
# primops: a SERVER-STREAM method (1 req -> 3 resp + status) and a BIDI method
# (client sends N, server echoes each + status).
#
# Usage:  bash test/grpc-echo-smoke.sh
# Env:    CRABSCHEME = crabscheme binary (default below).

set -uo pipefail
BIN="${CRABSCHEME:-/Users/ztaylor/repos/workspaces/crabscheme/target/release/crabscheme}"
HERE="$(cd "$(dirname "$0")/.." && pwd)"
cd "$HERE"

TAG="$(date +%s%N)"
PORT="$(( 33000 + (TAG % 1000) ))"
LOG="/tmp/cws-echo-${TAG}.log"

"$BIN" run test/grpc-echo-main.scm -- --port "$PORT" > "$LOG" 2>&1 &
SRV=$!
cleanup() { kill "$SRV" 2>/dev/null; wait "$SRV" 2>/dev/null; }
trap cleanup EXIT

up=0
for _ in $(seq 1 60); do
  grep -q "echo gRPC serving on" "$LOG" 2>/dev/null && { up=1; break; }
  kill -0 "$SRV" 2>/dev/null || break
  sleep 0.25
done
if [ "$up" != 1 ]; then echo "FATAL: echo server did not come up:"; cat "$LOG"; exit 1; fi
echo "server: $(grep 'echo gRPC serving on' "$LOG")"
echo

# Build (offline, from the module cache) + run the grpc-go client.
( cd test/grpc-echo-smoke && GOFLAGS=-mod=mod GOPROXY=off GOSUMDB=off GOTOOLCHAIN=local \
    go run . "127.0.0.1:${PORT}" )
rc=$?
echo
if [ "$rc" = 0 ]; then echo "GRPC-GO STREAMING SMOKE: PASS"; else echo "GRPC-GO STREAMING SMOKE: FAIL (rc=$rc)"; fi
exit "$rc"
