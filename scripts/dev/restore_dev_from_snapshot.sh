#!/usr/bin/env bash
#
# restore_dev_from_snapshot.sh — DEVELOPMENT SCRIPT.
#
# Roll the local dev tree back to a snapshot captured by
# scripts/dev/snapshot_prod_to_dev.sh.
#
# Usage:
#   scripts/dev/restore_dev_from_snapshot.sh [<snapshot-dir>]

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO_ROOT"

# shellcheck source=scripts/lib/gce_ssh.sh
source "$REPO_ROOT/scripts/lib/gce_ssh.sh"
init_dev_colors

SNAP_PATH="${1:-}"
if [ -z "$SNAP_PATH" ]; then
  for candidate in "$HOME/snapshots"/pre-v2.0-cutover-*; do
    [ -d "$candidate" ] || continue
    if [ -z "$SNAP_PATH" ] || [ "$candidate" -nt "$SNAP_PATH" ]; then
      SNAP_PATH="$candidate"
    fi
  done
  if [ -z "$SNAP_PATH" ]; then
    fail "no snapshot dir provided and no candidates under ~/snapshots/pre-v2.0-cutover-*"
    echo "      usage: $0 <snapshot-dir>" >&2
    exit 1
  fi
  info "no path given; using newest snapshot: ${C_BOLD}$SNAP_PATH${C_RESET}"
fi
SNAP_PATH="${SNAP_PATH%/}"

[ -d "$SNAP_PATH" ] || { fail "snapshot dir does not exist: $SNAP_PATH"; exit 1; }
[ -f "$SNAP_PATH/prod-snapshot.tar.gz" ] || { fail "missing prod-snapshot.tar.gz under $SNAP_PATH"; exit 1; }
[ -f "$SNAP_PATH/manifest.txt" ] || { fail "missing manifest.txt under $SNAP_PATH"; exit 1; }

section "♻️   Restore dev from snapshot"
info "snapshot:  ${C_BOLD}$SNAP_PATH${C_RESET}"
info "manifest:"
sed 's/^/      /' "$SNAP_PATH/manifest.txt"

EXPECTED_SHA="$(grep '^sha256:' "$SNAP_PATH/manifest.txt" | awk '{print $2}')"
if [ -z "$EXPECTED_SHA" ]; then
  warn "manifest has no sha256 line — skipping checksum verify"
else
  step "verifying sha256 of prod-snapshot.tar.gz"
  ACTUAL_SHA="$(shasum -a 256 "$SNAP_PATH/prod-snapshot.tar.gz" | awk '{print $1}')"
  if [ "$ACTUAL_SHA" != "$EXPECTED_SHA" ]; then
    fail "checksum mismatch — snapshot may be corrupted"
    echo "      expected: $EXPECTED_SHA" >&2
    echo "      actual:   $ACTUAL_SHA" >&2
    exit 1
  fi
  ok "checksum verified"
fi

require_local_backend_stopped "$REPO_ROOT" "the restore is atomic"

echo
echo "  ${C_BOLD}About to wipe local data/, cache/, configs/${C_RESET}"
echo "  and re-untar the snapshot at: $SNAP_PATH/prod-snapshot.tar.gz"
echo
printf "  proceed? (y/N) "
read -r reply
case "$reply" in
  y|Y|yes|YES) : ;;
  *)           fail "aborted"; exit 1 ;;
esac

step "wiping local data/ cache/ configs/"
rm -rf "$REPO_ROOT/data" "$REPO_ROOT/cache" "$REPO_ROOT/configs"

step "extracting snapshot into repo root"
tar -xzf "$SNAP_PATH/prod-snapshot.tar.gz" -C "$REPO_ROOT"
ok "extracted: $(ls -d data cache configs 2>/dev/null | tr '\n' ' ')"

mkdir -p "$REPO_ROOT/configs"
touch "$REPO_ROOT/configs/.gitkeep"

step "re-scrubbing configs via sync-from-remote.sh --prune-only"
echo
"$REPO_ROOT/scripts/dev/sync-from-remote.sh" --prune-only

section "✅  Restore complete"
echo "  ${C_BOLD}Restart the dev backend + frontend:${C_RESET}"
echo "    ./run.sh                       # backend :18002, frontend :13002"
echo "  ${C_BOLD}Snapshot is unchanged:${C_RESET} $SNAP_PATH"
echo
