#!/usr/bin/env bash
set -euo pipefail
BIN="${CRABSCHEME:-/Users/ztaylor/repos/workspaces/crabscheme/target/release/crabscheme}"
FAILED=0

for f in test/*.scm; do
  [ "$(basename "$f")" = "harness.scm" ] && continue
  echo "--- $f ---"
  if output=$("$BIN" run "$f" 2>&1); then
    echo "$output"
    if ! echo "$output" | grep -q "ALL PASS"; then
      echo "FAIL: $f did not print ALL PASS"
      FAILED=$((FAILED + 1))
    fi
  else
    echo "$output"
    echo "FAIL: $f exited non-zero"
    FAILED=$((FAILED + 1))
  fi
done

if [ "$FAILED" -gt 0 ]; then
  echo ""
  echo "$FAILED test file(s) FAILED"
  exit 1
fi
echo ""
echo "All test files passed."
