#!/usr/bin/env bash
#
# snapshot_prod_to_dev.sh — DEVELOPMENT SCRIPT.
#
# Atomic three-step wrapper used before testing against real data:
#   1. Snapshot the GCE prod VM's /mnt/app-data tree to a timestamped
#      tar.gz under ~/snapshots/pre-v2.0-cutover-<ts>/ on this local box.
#   2. Sync the SAME prod data into the local dev tree (data/, cache/,
#      configs/) by invoking scripts/dev/sync-from-remote.sh.
#   3. Print next-steps banner (restart dev backend + frontend or roll back).
#
# Usage:
#   scripts/dev/snapshot_prod_to_dev.sh [--instance NAME] [--zone ZONE]
#                                       [--remote-path ABS-PATH]
#                                       [--dry-run] [--yes]
#                                       [--snap-dir DIR]

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO_ROOT"

# shellcheck source=scripts/lib/gce_ssh.sh
source "$REPO_ROOT/scripts/lib/gce_ssh.sh"
init_dev_colors
load_repo_dotenv "$REPO_ROOT"

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
    -h|--help)     sed -n '2,18p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
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
[ "$DRY_RUN" = 1 ] && info "mode:         ${C_YELLOW}DRY-RUN${C_RESET}"

require_local_backend_stopped "$REPO_ROOT" "the snapshot ↔ restore handoff is atomic"

if gce_ssh_run "$REMOTE_INSTANCE" "$REMOTE_ZONE" --quiet -- true >/dev/null 2>&1; then
  ok "gcloud ssh reachable"
else
  fail "ssh to '$REMOTE_INSTANCE' (zone $REMOTE_ZONE) failed"
  exit 1
fi

SUDO_ARGV=()
if gce_ssh_run "$REMOTE_INSTANCE" "$REMOTE_ZONE" --quiet -- sudo -n true >/dev/null 2>&1; then
  SUDO_ARGV=(sudo)
  ok "passwordless sudo on remote"
else
  warn "no passwordless sudo — will try unprivileged read"
fi

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
  # shellcheck disable=SC2016
  gce_ssh_script "$REMOTE_INSTANCE" "$REMOTE_ZONE" --quiet -- \
    'cd "$1" && shift && exec "$@"' \
    "$REMOTE_PATH" ${SUDO_ARGV[@]+"${SUDO_ARGV[@]}"} \
    tar -czf - --exclude='*-wal' --exclude='*-shm' data cache configs \
    > "$SNAP_PATH/prod-snapshot.tar.gz"

  SIZE_HUMAN="$(du -sh "$SNAP_PATH/prod-snapshot.tar.gz" | awk '{print $1}')"
  SIZE_BYTES="$(stat -f%z "$SNAP_PATH/prod-snapshot.tar.gz" 2>/dev/null || stat -c%s "$SNAP_PATH/prod-snapshot.tar.gz")"
  SHA="$(shasum -a 256 "$SNAP_PATH/prod-snapshot.tar.gz" | awk '{print $1}')"
  cat > "$SNAP_PATH/manifest.txt" <<EOF
snapshot:     pre-v2.0-cutover-$TS
captured_at:  $TS
remote:       $REMOTE_INSTANCE ($REMOTE_ZONE)
remote_path:  $REMOTE_PATH
contents:     data/ + cache/ + configs/  (excluded: *-wal, *-shm)
size:         $SIZE_HUMAN ($SIZE_BYTES bytes)
sha256:       $SHA
restore_cmd:  scripts/dev/restore_dev_from_snapshot.sh $SNAP_PATH
EOF
  ok "snapshot saved: ${C_BOLD}$SNAP_PATH/prod-snapshot.tar.gz${C_RESET} (${SIZE_HUMAN})"
  ok "manifest:        $SNAP_PATH/manifest.txt"
fi

section "🔁  Step 2 of 3: Sync prod → local dev tree"
info "calling scripts/dev/sync-from-remote.sh — wipes local data/cache/configs and scrubs configs"

SYNC_ARGS=("--instance" "$REMOTE_INSTANCE" "--zone" "$REMOTE_ZONE" "--remote-path" "$REMOTE_PATH")
[ "$DRY_RUN" = 1 ] && SYNC_ARGS+=("--dry-run")
[ "$ASSUME_YES" = 1 ] && SYNC_ARGS+=("--yes")

echo
"$REPO_ROOT/scripts/dev/sync-from-remote.sh" "${SYNC_ARGS[@]}"

section "✨  Step 3 of 3: Next steps"
cat <<EOF
  ${C_BOLD}Snapshot saved as rollback backup:${C_RESET}
    $SNAP_PATH/prod-snapshot.tar.gz
    $SNAP_PATH/manifest.txt

  ${C_BOLD}Restart the dev backend + frontend:${C_RESET}
    ./run.sh                       # backend on :18002, frontend on :13002

  ${C_BOLD}If dev's data tree gets into a bad state, roll back:${C_RESET}
    ./run.sh --kill
    scripts/dev/restore_dev_from_snapshot.sh $SNAP_PATH
    ./run.sh
EOF
