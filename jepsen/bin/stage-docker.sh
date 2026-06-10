#!/usr/bin/env bash
# stage-docker.sh — refresh the Docker node build context (docker/crabscheme +
# docker/src) before `docker compose ... up --build`.
#
# The node image bakes:
#   docker/crabscheme  — a LINUX arm64 crabscheme binary built with
#                        --features stdlib-store,grpc  (the gRPC transport is
#                        REQUIRED for crab-watchstore; the crab-cache build lacks it)
#   docker/src         — a copy of crab-watchstore's src/ tree
#
# The macOS release binary will NOT run in the Linux node container; build a Linux
# arm64 binary in a container (Apple Silicon runs linux/arm64 natively):
#
#   docker run --rm --platform linux/arm64 \
#     -v <crabscheme-repo>:/src -v cws-target:/build/target \
#     -e CARGO_TARGET_DIR=/build/target \
#     -e CARGO_PROFILE_RELEASE_LTO=false -e CARGO_PROFILE_RELEASE_CODEGEN_UNITS=16 \
#     -v "$PWD/docker":/out -w /src rust:1.95-bookworm bash -c '
#       apt-get update -qq && apt-get install -y --no-install-recommends \
#         clang cmake libclang-dev build-essential pkg-config libssl-dev >/dev/null &&
#       cargo build --release -p cs-cli --features stdlib-store,grpc &&
#       cp /build/target/release/crabscheme /out/crabscheme'
#
# Then: docker compose -f docker/docker-compose.yml up -d --build
set -euo pipefail
HERE="$(cd "$(dirname "$0")/.." && pwd)"        # jepsen/
CWS_SRC="$(cd "$HERE/.." && pwd)/src"           # crab-watchstore/src
DOCKER_DIR="$HERE/docker"

[ -d "$CWS_SRC" ] || { echo "error: $CWS_SRC not found" >&2; exit 1; }

echo ">> staging src -> docker/src"
rm -rf "$DOCKER_DIR/src"
cp -R "$CWS_SRC" "$DOCKER_DIR/src"

if [ -f "$DOCKER_DIR/crabscheme" ]; then
  echo ">> docker/crabscheme present:"
  file "$DOCKER_DIR/crabscheme" || true
  case "$(file -b "$DOCKER_DIR/crabscheme" 2>/dev/null)" in
    *ELF*aarch64*) echo "   OK: ELF aarch64 (Linux) — will run in the node container" ;;
    *) echo "   WARNING: not an ELF aarch64 binary — the Linux node container will fail to exec it" >&2 ;;
  esac
else
  echo "!! docker/crabscheme MISSING — build a Linux arm64 binary (see header) before up --build" >&2
fi
echo "done."
