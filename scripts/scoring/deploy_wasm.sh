#!/usr/bin/env bash
# Build + deploy the session-scorer Wasm with a trained matrix embedded.
#
# Pipeline:
#   1. Validate trained matrix.json exists (run scripts/scoring/train.py first).
#   2. Copy trained matrix.json over matrix.default.json (the include_bytes!
#      target). The original default is preserved in git so this is a
#      working-tree change only.
#   3. `fastly compute build` to produce pkg/session-scorer.tar.gz.
#   4. `fastly compute deploy --service-id <sid>` to activate.
#   5. Restore matrix.default.json from git so subsequent fresh builds get
#      the empty placeholder back.
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
if [ ! -f "$MATRIX_PATH" ]; then
  echo "error: matrix not found at $MATRIX_PATH" >&2
  echo "       run: ./scripts/scoring/train.py --in <traces.jsonl> --out $MATRIX_PATH" >&2
  exit 2
fi

# Sanity check the matrix — refuse to deploy an empty matrix as if it were
# trained. The default placeholder has vocab_size=0; a real matrix has > 0.
VOCAB=$(python3 -c "import json; print(json.load(open('$MATRIX_PATH')).get('vocab_size', 0))")
if [ "$VOCAB" -eq 0 ]; then
  echo "error: matrix at $MATRIX_PATH is empty (vocab_size=0)" >&2
  echo "       deploy refused. Train against a real fixture first." >&2
  exit 3
fi
VERSION=$(python3 -c "import json; print(json.load(open('$MATRIX_PATH')).get('version', '?'))")
echo "[deploy_wasm] embedding matrix version=$VERSION vocab_size=$VOCAB"

# Stash the default in working memory (git tracks it, so we restore from
# git at the end) and copy the trained matrix on top of the include_bytes!
# target.
cp "$MATRIX_PATH" "$SCORER_DIR/matrix.default.json"

# Make sure we restore the default no matter what — including on Ctrl+C or
# build failure. Otherwise a successful prior deploy would leave the
# customer matrix sitting in the workspace, vulnerable to accidental commit.
cleanup() {
  echo "[deploy_wasm] restoring matrix.default.json from git"
  git -C "$ROOT" checkout -- compute/scorer/matrix.default.json
}
trap cleanup EXIT INT TERM

cd "$SCORER_DIR"
echo "[deploy_wasm] fastly compute build"
fastly compute build --token "$TOKEN" 2>&1 | tail -3

echo "[deploy_wasm] fastly compute deploy --service-id $SERVICE_ID"
fastly compute deploy --token "$TOKEN" --service-id "$SERVICE_ID" --accept-defaults 2>&1 | tail -10

echo "[deploy_wasm] done."
