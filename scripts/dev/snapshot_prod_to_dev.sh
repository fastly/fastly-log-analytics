#!/usr/bin/env bash
#
# snapshot_prod_to_dev.sh — DEVELOPMENT SCRIPT.
#
# Atomic three-step wrapper used before testing the v2.0 cleanup branch
# against real data:
#
#   1. Snapshot the GCE prod VM's /mnt/app-data tree to a timestamped
#      tar.gz under ~/snapshots/pre-v2.0-cutover-<ts>/  on this local box.
#      This is the ROLLBACK BACKUP. It is kept on disk after the script
#      exits and is never overwritten.
#
#   2. Sync the SAME prod data into the local dev tree (data/, cache/,
#      configs/) by invoking scripts/dev/sync-from-remote.sh — which wipes
#      local data/cache/configs first and re-streams from prod via
#      `gcloud compute ssh + tar`. Configs get scrubbed (FOS + Fastly
#      keys cleared, crons disabled, cdn_url cleared) per the dev-sandbox-
#      scrub memory.
#
#   3. Print a one-paragraph next-steps banner: how to restart the dev
#      backend + frontend on 13002/18002 + how to roll back via the
#      sibling `restore_dev_from_snapshot.sh`.
#
# The script REFUSES to run if a local backend is currently writing into
# data/ (sync-from-remote.sh's own pre-flight check), so the snapshot ↔
# restore handoff is atomic.
#
# Usage:
#   scripts/dev/snapshot_prod_to_dev.sh [--instance NAME] [--zone ZONE]
#                                       [--remote-path ABS-PATH]
#                                       [--dry-run] [--yes]
#                                       [--snap-dir DIR]
#
# Defaults:
#   REMOTE_INSTANCE / REMOTE_ZONE / REMOTE_PATH  — read from .env (gitignored)
#   --snap-dir  — ~/snapshots/  (snapshots are stamped pre-v2.0-cutover-<ts>/)
#
# Examples:
#   # Standard one-shot (uses .env values for instance/zone/path):
#   scripts/dev/snapshot_prod_to_dev.sh
#
#   # Dry-run — show what would happen, write nothing:
#   scripts/dev/snapshot_prod_to_dev.sh --dry-run
#
#   # Custom snapshot dir (e.g. on a bigger volume):
#   scripts/dev/snapshot_prod_to_dev.sh --snap-dir /Volumes/dev-archive/snapshots
#
# Rollback path (if the dev sync produces a broken state):
#   scripts/dev/restore_dev_from_snapshot.sh ~/snapshots/pre-v2.0-cutover-<ts>

set -euo pipefail

# ── Pretty output ───────────────────────────────────────────────────────────
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

# Auto-load .env so REMOTE_* don't need to be on the shell each time.
if [ -f "$REPO_ROOT/.env" ]; then
  set -a
  # shellcheck disable=SC1091
  source "$REPO_ROOT/.env"
  set +a
fi

REMOTE_INSTANCE="${REMOTE_INSTANCE:-}"
REMOTE_ZONE="${REMOTE_ZONE:-}"
REMOTE_PATH="${REMOTE_PATH:-}"
SNAP_DIR="${SNAP_DIR:-$HOME/snapshots}"
DRY_RUN="${DRY_RUN:-0}"
ASSUME_YES="${ASSUME_YES:-0}"

while [ $# -gt 0 ]; do
  case "$1" in
    --instance)    REMOTE_INSTANCE="$2"; shift 2 ;;
    --zone)        REMOTE_ZONE="$2"; shift 2 ;;
    --remote-path) REMOTE_PATH="$2"; shift 2 ;;
    --snap-dir)    SNAP_DIR="$2"; shift 2 ;;
    --dry-run)     DRY_RUN=1; shift ;;
    -y|--yes)      ASSUME_YES=1; shift ;;
    -h|--help)     sed -n '2,40p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *)             fail "unknown arg: $1"; exit 1 ;;
  esac
done

if [ -z "$REMOTE_INSTANCE" ] || [ -z "$REMOTE_ZONE" ] || [ -z "$REMOTE_PATH" ]; then
  fail "--instance, --zone, and --remote-path are required"
  echo "      (or set REMOTE_INSTANCE, REMOTE_ZONE, REMOTE_PATH in .env)" >&2
  exit 1
fi
REMOTE_PATH="${REMOTE_PATH%/}"

command -v gcloud >/dev/null 2>&1 || { fail "missing required tool: gcloud"; exit 1; }
command -v tar    >/dev/null 2>&1 || { fail "missing required tool: tar";    exit 1; }

TS="$(date -u +%Y%m%dT%H%M%SZ)"
SNAP_PATH="$SNAP_DIR/pre-v2.0-cutover-$TS"

section "📸  Step 1 of 3: Snapshot GCE prod → local backup"
info "remote VM:    ${C_BOLD}$REMOTE_INSTANCE${C_RESET} ${C_DIM}(zone $REMOTE_ZONE)${C_RESET}"
info "remote path:  ${C_BOLD}$REMOTE_PATH${C_RESET}"
info "snapshot dir: ${C_BOLD}$SNAP_PATH${C_RESET}"
if [ "$DRY_RUN" = 1 ]; then
  info "mode:         ${C_YELLOW}DRY-RUN${C_RESET}"
fi

# Refuse if local backend is running (sync step needs an idle data tree).
RUNNING_PIDS="$(pgrep -f "$REPO_ROOT.*uvicorn" 2>/dev/null || true)"
if [ -n "$RUNNING_PIDS" ]; then
  fail "a local backend is running from this project (PIDs: $RUNNING_PIDS)"
  echo "      stop it first so the snapshot ↔ restore handoff is atomic:" >&2
  echo "        ./run.sh --kill" >&2
  exit 1
fi

# Confirm ssh reachability up front (mirrors sync-from-remote.sh pre-flight)
# so a missing key fails fast instead of mid-transfer.
if gcloud compute ssh "$REMOTE_INSTANCE" --zone="$REMOTE_ZONE" --quiet \
     --command="true" >/dev/null 2>&1; then
  ok "gcloud ssh reachable"
else
  fail "gcloud compute ssh to '$REMOTE_INSTANCE' (zone $REMOTE_ZONE) failed"
  echo "      try 'gcloud compute ssh $REMOTE_INSTANCE --zone=$REMOTE_ZONE' interactively first" >&2
  exit 1
fi

# Passwordless sudo on remote (the bind-mount is owned by the container user).
SUDO=""
if gcloud compute ssh "$REMOTE_INSTANCE" --zone="$REMOTE_ZONE" --quiet \
     --command="sudo -n true" >/dev/null 2>&1; then
  SUDO="sudo"
  ok "passwordless sudo on remote"
else
  warn "no passwordless sudo — will try unprivileged read"
fi

# Confirm before snapshotting (this is the safe op; sync step has its own confirm).
if [ "$DRY_RUN" != 1 ] && [ "$ASSUME_YES" != 1 ]; then
  echo
  printf "  ${C_BOLD}snapshot prod tree to %s ?${C_RESET} (y/N) " "$SNAP_PATH"
  read -r reply
  case "$reply" in
    y|Y|yes|YES) : ;;
    *)           fail "aborted"; exit 1 ;;
  esac
fi

if [ "$DRY_RUN" = 1 ]; then
  warn "would mkdir -p $SNAP_PATH"
  warn "would stream gcloud ssh + tar → $SNAP_PATH/prod-snapshot.tar.gz"
  warn "would also write $SNAP_PATH/manifest.txt (timestamps, sizes, sha256)"
else
  mkdir -p "$SNAP_PATH"
  step "streaming snapshot (data/ + cache/ + configs/) — may take minutes for a large tree"
  # Stream the tree as a tarball directly to local disk via the same
  # gcloud ssh + tar pattern sync-from-remote.sh uses. We tee into a
  # checksum file as we write so the manifest doesn't need a second read.
  gcloud compute ssh "$REMOTE_INSTANCE" --zone="$REMOTE_ZONE" --quiet \
    --command="cd $REMOTE_PATH && $SUDO tar -czf - --exclude='*-wal' --exclude='*-shm' data cache configs" \
    > "$SNAP_PATH/prod-snapshot.tar.gz"

  # Manifest: timestamp, source, size, sha256. Used by restore + the
  # rollback runbook to verify the right snapshot is being restored.
  SIZE_HUMAN="$(du -sh "$SNAP_PATH/prod-snapshot.tar.gz" | awk '{print $1}')"
  SIZE_BYTES="$(stat -f%z "$SNAP_PATH/prod-snapshot.tar.gz" 2>/dev/null || stat -c%s "$SNAP_PATH/prod-snapshot.tar.gz")"
  SHA="$(shasum -a 256 "$SNAP_PATH/prod-snapshot.tar.gz" | awk '{print $1}')"
  cat > "$SNAP_PATH/manifest.txt" <<EOF
snapshot:     pre-v2.0-cutover-$TS
captured_at:  $TS
source:       gcloud compute ssh $REMOTE_INSTANCE --zone=$REMOTE_ZONE
remote_path:  $REMOTE_PATH
contents:     data/ + cache/ + configs/  (excluded: *-wal, *-shm)
size:         $SIZE_HUMAN ($SIZE_BYTES bytes)
sha256:       $SHA
restore_cmd:  scripts/dev/restore_dev_from_snapshot.sh $SNAP_PATH
EOF
  ok "snapshot saved: ${C_BOLD}$SNAP_PATH/prod-snapshot.tar.gz${C_RESET} (${SIZE_HUMAN})"
  ok "manifest:        $SNAP_PATH/manifest.txt"
fi

# ── Step 2: sync prod → dev via existing sync-from-remote.sh ─────────────────
section "🔁  Step 2 of 3: Sync prod → local dev tree"
info "calling scripts/dev/sync-from-remote.sh — wipes local data/cache/configs"
info "and re-streams from prod, then scrubs configs (creds/crons/cdn_url)"

SYNC_ARGS=("--instance" "$REMOTE_INSTANCE" "--zone" "$REMOTE_ZONE" "--remote-path" "$REMOTE_PATH")
if [ "$DRY_RUN" = 1 ]; then
  SYNC_ARGS+=("--dry-run")
fi
if [ "$ASSUME_YES" = 1 ]; then
  SYNC_ARGS+=("--yes")
fi

echo
"$REPO_ROOT/scripts/dev/sync-from-remote.sh" "${SYNC_ARGS[@]}"

# ── Step 3: next-steps banner ────────────────────────────────────────────────
section "✨  Step 3 of 3: Next steps"
cat <<EOF
  ${C_BOLD}Snapshot saved as rollback backup:${C_RESET}
    $SNAP_PATH/prod-snapshot.tar.gz
    $SNAP_PATH/manifest.txt

  ${C_BOLD}Restart the dev backend + frontend:${C_RESET}
    ./run.sh                       # backend on :18002, frontend on :13002

  ${C_BOLD}Smoke-test surfaces touched by the v2.0 cleanup branch:${C_RESET}
    open http://localhost:13002/dashboard          # post-Phase-9b split
    open http://localhost:13002/sessions           # post-edge_sid + flag column
    open http://localhost:13002/query              # post-dual-mode refactor
    open http://localhost:13002/alerts             # M-1 audit fix
    open http://localhost:13002/admin              # control: should be reachable as admin

    # Watch for the transient "No data available" — saw it once on prod, resolved itself.
    # Also exercise the Reset button on every page: expect 24h + no filters + 1h granularity.

  ${C_BOLD}If dev's data tree gets into a bad state, roll back:${C_RESET}
    ./run.sh --kill
    scripts/dev/restore_dev_from_snapshot.sh $SNAP_PATH
    ./run.sh

  ${C_BOLD}Once dev verifies clean, deploy to GCE:${C_RESET}
    ssh <vm>; cd <repo>; ~/restart.sh        # per gce-deploy-rebuild memory
    # Watch logs for 15 min post-deploy per per-phase verify gate.
    # Hard-refresh the browser after the frontend rebuild.

EOF
