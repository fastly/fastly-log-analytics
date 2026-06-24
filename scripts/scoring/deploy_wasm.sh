#!/usr/bin/env bash
# Build + deploy the matrix-less session-scorer Wasm.
#
# The trained transition matrix is NOT embedded in the Wasm — it is served from
# the `scoring_matrix` KV Store at runtime (see compute/scorer/src/matrix.rs and
# backend/scoring/matrix.py::serialize_kv, which the backend pushes via the
# Fastly API). This script only builds + activates the matrix-less Wasm; pushing
# the trained matrix to KV is a separate backend step.
#
# Pipeline:
#   1. Sanity-check that a trained matrix.json exists for this tenant — a
#      precondition guard ("did you train first?"), NOT an embed; the matrix is
#      pushed to KV separately, not by this script.
#   2. Copy the scorer sources into an isolated temp workspace (build outputs
#      excluded) so the build never mutates the working tree and concurrent
#      invocations can't clobber each other's build outputs.
#   3. `fastly compute build` to produce pkg/session-scorer.tar.gz.
#   4. `fastly compute deploy --service-id <sid>` to activate.
#   5. The temp workspace is removed on exit; the working tree is never touched.
#
# Required:
#   --service-id   target Compute service id (e.g. eHDt37QGSEfihZOuXJOREe)
#   --token        Fastly API token (or set FASTLY_API_TOKEN env var)
#
# Optional:
#   --matrix       path to trained matrix.json (defaults to compute/scorer/matrix.json)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
SCORER_DIR="$ROOT/compute/scorer"

# Put rustup's cargo/rustc shims ahead of Homebrew's rust on PATH so the
# rust-toolchain.toml pin in compute/scorer/ actually takes effect.
# Homebrew's /opt/homebrew/bin/rustc is a fixed-version symlink and
# ignores rust-toolchain.toml; that bit us once when Fastly CLI v13
# refused to build with Rust 1.95 even though we'd pinned 1.90.
# Harmless when rustup isn't installed — non-existent dirs on PATH are
# silently skipped.
if [ -d "$HOME/.cargo/bin" ]; then
  export PATH="$HOME/.cargo/bin:$PATH"
fi

SERVICE_ID=""
TOKEN="${FASTLY_API_TOKEN:-}"
MATRIX_PATH="$SCORER_DIR/matrix.json"

while [ $# -gt 0 ]; do
  case "$1" in
    --service-id) SERVICE_ID="$2"; shift 2 ;;
    --token)      TOKEN="$2"; shift 2 ;;
    --matrix)     MATRIX_PATH="$2"; shift 2 ;;
    -h|--help)
      grep -E '^# ' "$0" | sed 's/^# //'
      exit 0
      ;;
    *)
      echo "unknown arg: $1" >&2
      exit 1
      ;;
  esac
done

if [ -z "$SERVICE_ID" ]; then
  echo "error: --service-id is required" >&2
  exit 2
fi
if [ -z "$TOKEN" ]; then
  echo "error: --token or FASTLY_API_TOKEN is required" >&2
  exit 2
fi
# Hand the token to the fastly CLI via the environment, NEVER as a --token
# argv flag: process arguments are world-readable (ps aux, /proc/<pid>/cmdline)
# for the lifetime of the build/deploy, leaking a token that can edit/activate
# Compute services on a shared build host or CI runner. The CLI reads
# FASTLY_API_TOKEN from the environment.
export FASTLY_API_TOKEN="$TOKEN"
if [ ! -f "$MATRIX_PATH" ]; then
  echo "error: matrix not found at $MATRIX_PATH" >&2
  echo "       run: ./scripts/scoring/train.py --in <traces.jsonl> --out $MATRIX_PATH" >&2
  exit 2
fi

# Precondition guard — refuse to run if no trained matrix exists yet (the
# default placeholder has vocab_size=0; a real trained matrix has > 0). The
# matrix is pushed to KV separately by the backend, not embedded by this script;
# this is just a "did you train first?" check so we don't stand up scoring infra
# for a tenant with no model.
VOCAB=$(python3 -c "import json; print(json.load(open('$MATRIX_PATH')).get('vocab_size', 0))")
if [ "$VOCAB" -eq 0 ]; then
  echo "error: matrix at $MATRIX_PATH is empty (vocab_size=0)" >&2
  echo "       deploy refused. Train against a real fixture first." >&2
  exit 3
fi
VERSION=$(python3 -c "import json; print(json.load(open('$MATRIX_PATH')).get('version', '?'))")
echo "[deploy_wasm] trained matrix present: version=$VERSION vocab_size=$VOCAB (served via KV, not embedded)"

# Build in an isolated temp workspace so the build never mutates the working
# tree and concurrent invocations don't clobber each other's build outputs.
# The temp copy excludes build outputs (target/, pkg/) so it stays small.
TMP_WORKSPACE="$(mktemp -d)"
cleanup() {
  rm -rf "$TMP_WORKSPACE"
}
trap cleanup EXIT INT TERM

tar -C "$SCORER_DIR" --exclude=target --exclude=pkg --exclude=.DS_Store -cf - . \
  | tar -C "$TMP_WORKSPACE" -xf -

cd "$TMP_WORKSPACE"
echo "[deploy_wasm] fastly compute build (isolated workspace)"
# --auto-yes auto-approves the fastly.toml post_build (wasm-opt) prompt so the
# build doesn't abort non-interactively. wasm-opt is gated on being installed;
# if it isn't on PATH here the post_build no-ops and ships the un-optimized Wasm.
fastly compute build --auto-yes 2>&1 | tail -3

echo "[deploy_wasm] fastly compute deploy --service-id $SERVICE_ID"
fastly compute deploy --service-id "$SERVICE_ID" --accept-defaults 2>&1 | tail -10

echo "[deploy_wasm] done."
