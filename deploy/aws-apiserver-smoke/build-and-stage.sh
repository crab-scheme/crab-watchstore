#!/usr/bin/env bash
# build-and-stage.sh — build the arm64-Linux crabscheme binary and stage it + the
# crab-watchstore src tree to S3, so the EC2 (Graviton/arm64) nodes can pull and run
# it natively. Mirrors jepsen/bin/stage-docker.sh's proven build recipe.
#
#   AWS_PROFILE=stigen-io-tasks/sandbox/AdministratorAccess ./build-and-stage.sh
#
# Env overrides:
#   CRABSCHEME_REPO   path to the crabscheme repo (default: ../../../crabscheme)
#   REGION            bucket region (default: us-east-2)
#   RUST_IMAGE        builder image (default: rust:1.95-bookworm)
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
CWS_ROOT="$(cd "$HERE/../.." && pwd)"                       # crab-watchstore/
CRABSCHEME_REPO="${CRABSCHEME_REPO:-$(cd "$CWS_ROOT/../crabscheme" && pwd)}"
REGION="${REGION:-us-east-2}"
RUST_IMAGE="${RUST_IMAGE:-rust:1.95-bookworm}"
: "${AWS_PROFILE:?set AWS_PROFILE to the sandbox SSO profile}"

ACCOUNT="$(aws sts get-caller-identity --query Account --output text)"
BUCKET="cws-smoke-${ACCOUNT}-${REGION}"
STAGE="$HERE/.stage"
rm -rf "$STAGE"; mkdir -p "$STAGE"

echo ">> building arm64-linux crabscheme (features stdlib-store,grpc) from $CRABSCHEME_REPO"
[ -d "$CRABSCHEME_REPO" ] || { echo "error: crabscheme repo not at $CRABSCHEME_REPO (set CRABSCHEME_REPO)" >&2; exit 1; }
docker run --rm --platform linux/arm64 \
  -v "$CRABSCHEME_REPO":/src \
  -v cws-target:/build/target \
  -e CARGO_TARGET_DIR=/build/target \
  -e CARGO_PROFILE_RELEASE_LTO=false -e CARGO_PROFILE_RELEASE_CODEGEN_UNITS=16 \
  -v "$STAGE":/out -w /src "$RUST_IMAGE" bash -c '
    apt-get update -qq && apt-get install -y --no-install-recommends \
      clang cmake libclang-dev build-essential pkg-config libssl-dev >/dev/null &&
    cargo build --release -p cs-cli --features stdlib-store,grpc &&
    cp /build/target/release/crabscheme /out/crabscheme'

file "$STAGE/crabscheme" | grep -q aarch64 || { echo "error: built binary is not aarch64 ELF" >&2; exit 1; }

echo ">> packaging src tree"
cp -R "$CWS_ROOT/src" "$STAGE/src"
( cd "$STAGE" && tar czf crabwatchstore.tgz crabscheme src )

echo ">> ensuring bucket s3://$BUCKET"
aws s3api head-bucket --bucket "$BUCKET" 2>/dev/null || \
  aws s3api create-bucket --bucket "$BUCKET" --region "$REGION" \
    --create-bucket-configuration "LocationConstraint=$REGION" >/dev/null

echo ">> uploading artifact"
aws s3 cp "$STAGE/crabwatchstore.tgz" "s3://$BUCKET/crabwatchstore.tgz"

cat <<EOF

staged:  s3://$BUCKET/crabwatchstore.tgz
account: $ACCOUNT   region(bucket): $REGION

Next:
  cd terraform && terraform init && \\
    terraform apply -var "allowed_cidr=\$(curl -s https://checkip.amazonaws.com)/32"
EOF
