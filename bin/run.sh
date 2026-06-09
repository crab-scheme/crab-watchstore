#!/usr/bin/env bash
set -euo pipefail
BIN="${CRABSCHEME:-/Users/ztaylor/repos/workspaces/crabscheme/target/release/crabscheme}"
exec "$BIN" run src/node-watchstore.scm -- "$@"
