#!/usr/bin/env bash
#
# restore_dev_from_snapshot.sh — DEVELOPMENT SCRIPT.
#
# Roll the local dev tree back to a snapshot captured by
# scripts/dev/snapshot_prod_to_dev.sh. Use when the dev sync produces
# a broken state and you want to start from the known-good captured tree
# instead of re-syncing from the live prod VM.
#
# Steps:
#   1. Verify the snapshot directory exists + has prod-snapshot.tar.gz + manifest.txt.
#   2. Verify sha256 of the tarball matches the manifest (corruption check).
#   3. Refuse to run if a local backend is using the data tree.
#   4. Wipe local data/, cache/, configs/ (keeps the tracked configs/.gitkeep marker).
#   5. Untar the snapshot into the repo root (recreates data/cache/configs).
#   6. Re-apply the dev-sandbox scrub on configs/*.json (per
#      dev-sandbox-scrub memory: clear FOS+Fastly creds, disable
#      provisioning crons, clear cdn_url, null provisioning.temp_admin_key_id).
#      We call out to scripts/dev/sync-from-remote.sh in --prune-only mode
#      for the config scrub step since that's where the scrub lives.
#
# Usage:
#   scripts/dev/restore_dev_from_snapshot.sh <snapshot-dir>
#   scripts/dev/restore_dev_from_snapshot.sh ~/snapshots/pre-v2.0-cutover-20260610T015000Z
#
# If <snapshot-dir> is omitted, uses the newest snapshot under ~/snapshots/.
#
# Inverse of snapshot_prod_to_dev.sh. Never touches the snapshot itself —
# you can restore multiple times from the same snapshot to retry a failed
# upgrade-path test.

set -euo pipefail

if [ -t 1 ] && [ -z "${NO_COLOR:-}" ]; then
  C_RESET=$'\033[0m'; C_BOLD=$'\033[1m'; C_DIM=$'\033[2m'
  C_GREEN=$'\033[32m'; C_YELLOW=$'\033[33m'; C_BLUE=$'\033[34m'; C_CYAN=$'\033[36m'; C_RED=$'\033[31m'
else
  C_RESET=""; C_BOLD=""; C_DIM=""; C_GREEN=""; C_YELLOW=""; C_BLUE=""; C_CYAN=""; C_RED=""
fi

section() { echo; echo "${C_BOLD}${1}${C_RESET}"; echo "${C_DIM}$(printf '─%.0s' $(seq 1 60))${C_RESET}"; }
ok()   { echo "  ${C_GREEN}✓${C_RESET} ${*}"; }
warn() { echo "  ${C_YELLOW}⚠${C_RESET} ${*}"; }
fail() { echo "  ${C_RED}✗${C_RESET} ${*}" >&2; }
info() { echo "  ${C_CYAN}ℹ${C_RESET} ${*}"; }
step() { echo "  ${C_BLUE}→${C_RESET} ${*}"; }

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO_ROOT"

# ── Resolve snapshot dir ────────────────────────────────────────────────────
SNAP_PATH="${1:-}"
if [ -z "$SNAP_PATH" ]; then
  # Default to newest snapshot in ~/snapshots/.
  SNAP_PATH="$(ls -1dt "$HOME/snapshots"/pre-v2.0-cutover-* 2>/dev/null | head -1 || true)"
  if [ -z "$SNAP_PATH" ]; then
    fail "no snapshot dir provided and no candidates under ~/snapshots/pre-v2.0-cutover-*"
    echo "      usage: $0 <snapshot-dir>" >&2
    exit 1
  fi
  info "no path given; using newest snapshot: ${C_BOLD}$SNAP_PATH${C_RESET}"
fi
SNAP_PATH="${SNAP_PATH%/}"

if [ ! -d "$SNAP_PATH" ]; then
  fail "snapshot dir does not exist: $SNAP_PATH"
  exit 1
fi
if [ ! -f "$SNAP_PATH/prod-snapshot.tar.gz" ]; then
  fail "missing prod-snapshot.tar.gz under $SNAP_PATH"
  exit 1
fi
if [ ! -f "$SNAP_PATH/manifest.txt" ]; then
  fail "missing manifest.txt under $SNAP_PATH"
  exit 1
fi

section "♻️   Restore dev from snapshot"
info "snapshot:  ${C_BOLD}$SNAP_PATH${C_RESET}"
info "manifest:"
sed 's/^/      /' "$SNAP_PATH/manifest.txt"

# ── Verify checksum ──────────────────────────────────────────────────────────
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

# ── Refuse if backend is running ────────────────────────────────────────────
RUNNING_PIDS="$(pgrep -f "$REPO_ROOT.*uvicorn" 2>/dev/null || true)"
if [ -n "$RUNNING_PIDS" ]; then
  fail "a local backend is running from this project (PIDs: $RUNNING_PIDS)"
  echo "      stop it first so the restore is atomic:" >&2
  echo "        ./run.sh --kill" >&2
  exit 1
fi

# ── Confirm ─────────────────────────────────────────────────────────────────
echo
echo "  ${C_BOLD}About to wipe local data/, cache/, configs/${C_RESET}"
echo "  and re-untar the snapshot at:"
echo "    $SNAP_PATH/prod-snapshot.tar.gz"
echo
printf "  proceed? (y/N) "
read -r reply
case "$reply" in
  y|Y|yes|YES) : ;;
  *)           fail "aborted"; exit 1 ;;
esac

# ── Wipe local data/cache/configs ───────────────────────────────────────────
step "wiping local data/ cache/ configs/"

rm -rf "$REPO_ROOT/data" "$REPO_ROOT/cache" "$REPO_ROOT/configs"

# ── Untar snapshot ──────────────────────────────────────────────────────────
step "extracting snapshot into repo root"
tar -xzf "$SNAP_PATH/prod-snapshot.tar.gz" -C "$REPO_ROOT"
ok "extracted: $(ls -d data cache configs 2>/dev/null | tr '\n' ' ')"

# Keep the tracked configs/.gitkeep marker present so `git status` stays clean.
mkdir -p "$REPO_ROOT/configs"
touch "$REPO_ROOT/configs/.gitkeep"

# ── Re-scrub configs ────────────────────────────────────────────────────────
# Delegates to sync-from-remote.sh --prune-only which runs the scrub (clears
# FOS+Fastly creds, disables provisioning crons, clears cdn_url) without
# touching prod or doing a wipe.
step "re-scrubbing configs via sync-from-remote.sh --prune-only"
echo
"$REPO_ROOT/scripts/dev/sync-from-remote.sh" --prune-only

# ── Done ────────────────────────────────────────────────────────────────────
section "✅  Restore complete"
echo "  ${C_BOLD}Restart the dev backend + frontend:${C_RESET}"
echo "    ./run.sh                       # backend :18002, frontend :13002"
echo
echo "  ${C_BOLD}Snapshot is unchanged — you can restore again from the same path:${C_RESET}"
echo "    $SNAP_PATH"
echo
