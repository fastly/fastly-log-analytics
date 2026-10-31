#!/usr/bin/env bash
#
# snapshot_prod_to_dev.sh — DEVELOPMENT SCRIPT (plus consolidated dev/wasm subcommands).
#
# Modes:
#   default (no mode flag)       : Snapshot GCE prod VM -> local backup + sync-from-remote.sh
#   --restore [<snapshot-dir>]   : Roll local dev tree back to a snapshot
#   --start-dev                  : Start Colima, local Docker stacks, and remote port-forwards
#   --deploy-wasm --service-id … : Build + deploy the matrix-less session-scorer Wasm
#
# Usage:
#   scripts/dev/snapshot_prod_to_dev.sh [--instance NAME] [--zone ZONE]
#                                       [--remote-path ABS-PATH]
#                                       [--dry-run] [--yes]
#                                       [--snap-dir DIR]
#   scripts/dev/snapshot_prod_to_dev.sh --restore [<snapshot-dir>]
#   scripts/dev/snapshot_prod_to_dev.sh --start-dev
#   scripts/dev/snapshot_prod_to_dev.sh --deploy-wasm --service-id <id> [--token <tok>] [--matrix <path>]

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO_ROOT"

# shellcheck source=scripts/lib/gce_ssh.sh
source "$REPO_ROOT/scripts/lib/gce_ssh.sh"
init_dev_colors

run_restore_snapshot() {
  local SNAP_PATH="${1:-}" EXPECTED_SHA ACTUAL_SHA reply
  if [ -z "$SNAP_PATH" ]; then
    for candidate in "$HOME/snapshots"/pre-v2.0-cutover-*; do
      [ -d "$candidate" ] || continue
      if [ -z "$SNAP_PATH" ] || [ "$candidate" -nt "$SNAP_PATH" ]; then
        SNAP_PATH="$candidate"
      fi
    done
    if [ -z "$SNAP_PATH" ]; then
      fail "no snapshot dir provided and no candidates under ~/snapshots/pre-v2.0-cutover-*"
      echo "      usage: $0 --restore <snapshot-dir>" >&2
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
}

run_start_dev_environment() {
  local BOLD="\033[1m" GREEN="\033[32m" YELLOW="\033[33m" RED="\033[31m" CYAN="\033[36m" RESET="\033[0m"
  echo -e "${BOLD}${CYAN}🚀 STARTING DEV ENVIRONMENT & PORT-FORWARDS${RESET}"
  echo "=========================================="

  load_repo_dotenv "$REPO_ROOT"
  load_persisted_admin_mtls_env || { echo -e "${RED}❌ Failed to load persisted admin-mTLS settings${RESET}" >&2; exit 1; }

  local LOCAL_STANDARD_ADMIN_MTLS="${LOCAL_STANDARD_ADMIN_MTLS:-0}"
  local GCE_ADMIN_MTLS="${GCE_ADMIN_MTLS:-0}"
  local REMOTE_HS_ADMIN_MTLS="${REMOTE_HS_ADMIN_MTLS:-0}"
  local LOCAL_STD_ORIGIN REMOTE_STD_ORIGIN REMOTE_HS_ORIGIN
  LOCAL_STD_ORIGIN="$(python3 -c 'from scripts.lib.environments import admin_origin; print(admin_origin("local-standard") or "")' 2>/dev/null || true)"
  REMOTE_STD_ORIGIN="$(python3 -c 'from scripts.lib.environments import admin_origin; print(admin_origin("remote-standard") or "")' 2>/dev/null || true)"
  REMOTE_HS_ORIGIN="$(python3 -c 'from scripts.lib.environments import admin_origin; print(admin_origin("remote-high-scale") or "")' 2>/dev/null || true)"

  local LOCAL_STD_COMPOSE=(-f docker-compose.yml)
  [ "$LOCAL_STANDARD_ADMIN_MTLS" = "1" ] && LOCAL_STD_COMPOSE+=(-f docker-compose.admin-mtls.yml)

  local REMOTE_STANDARD_FORWARD_CMD="${REMOTE_STANDARD_FORWARD_CMD:-}"
  local REMOTE_HIGH_SCALE_FORWARD_CMD="${REMOTE_HIGH_SCALE_FORWARD_CMD:-}"

  launch_k8s_forwarders() {
      local log
      log="$(mktemp "${TMPDIR:-/tmp}/fla-port-forward.XXXXXX")"
      nohup python3 -m scripts.lib.port_forward --namespace "${K8S_NAMESPACE}" \
          svc/frontend-svc:3002:3000 svc/backend-svc:8002:8000 >"${log}" 2>&1 &
      echo -e "${CYAN}ℹ️ port-forward supervisor pid $! (log: ${log})${RESET}"
  }

  echo -e "\n${BOLD}1. Checking Colima status...${RESET}"
  if ! colima status &>/dev/null; then
      echo -e "${YELLOW}⚠️ Colima is not running. Starting Colima (4 vCPUs, 8GB RAM)...${RESET}"
      colima start --cpu 4 --memory 8
      echo -e "${GREEN}✅ Colima started successfully!${RESET}"
  else
      echo -e "${GREEN}✅ Colima is already running.${RESET}"
  fi

  echo -e "\n${BOLD}2. Checking Local Standard Docker stack...${RESET}"
  if ! docker compose "${LOCAL_STD_COMPOSE[@]}" ps --format json | grep -q "running"; then
      echo -e "${YELLOW}⚠️ Local Standard containers are not running. Starting standard stack...${RESET}"
      docker compose "${LOCAL_STD_COMPOSE[@]}" up -d
      echo -e "${GREEN}✅ Local Standard stack started.${RESET}"
  elif [ "${LOCAL_STANDARD_ADMIN_MTLS}" = "1" ] && ! docker compose "${LOCAL_STD_COMPOSE[@]}" ps admin-gateway --format json 2>/dev/null | grep -q "running"; then
      echo -e "${YELLOW}⚠️ Local Admin mTLS gateway is not running. Starting admin-gateway...${RESET}"
      docker compose "${LOCAL_STD_COMPOSE[@]}" up -d admin-gateway
      echo -e "${GREEN}✅ Local Admin mTLS gateway started.${RESET}"
  else
      echo -e "${GREEN}✅ Local Standard stack is already running.${RESET}"
  fi

  echo -e "\n${BOLD}3. Checking Remote Standard Connection...${RESET}"
  if [ -n "${REMOTE_STANDARD_FORWARD_CMD}" ]; then
      if ! lsof -i :3001 &>/dev/null && ! lsof -i :8001 &>/dev/null; then
          echo -e "${YELLOW}⚠️ Remote Standard ports 3001/8001 are not bound. Launching forwarder...${RESET}"
          launch_forwarders "${REMOTE_STANDARD_FORWARD_CMD}"
          sleep 3
          if lsof -i :3001 &>/dev/null || lsof -i :8001 &>/dev/null; then
              echo -e "${GREEN}✅ Remote Standard connection established.${RESET}"
          else
              echo -e "${RED}❌ Failed to establish Remote Standard connection.${RESET}"
          fi
      else
          echo -e "${GREEN}✅ Remote Standard connection (3001/8001) is already active.${RESET}"
      fi
  elif [ "${GCE_ADMIN_MTLS}" = "1" ] && [ -n "${REMOTE_STD_ORIGIN}" ]; then
      echo -e "${GREEN}✅ Remote Standard configured via Admin mTLS gateway (${REMOTE_STD_ORIGIN}).${RESET}"
  else
      echo -e "${YELLOW}⚠️ REMOTE_STANDARD_FORWARD_CMD is unset; skipping remote Standard forwarding.${RESET}"
  fi

  echo -e "\n${BOLD}4. Checking Remote High-Scale Connection...${RESET}"
  if [ -n "${REMOTE_HIGH_SCALE_FORWARD_CMD}" ] || [ -n "${K8S_NAMESPACE:-}" ]; then
      if ! lsof -i :3002 &>/dev/null || ! lsof -i :8002 &>/dev/null; then
          echo -e "${YELLOW}⚠️ Remote High-Scale ports 3002/8002 are not bound. Launching forwarder...${RESET}"
          if [ -n "${REMOTE_HIGH_SCALE_FORWARD_CMD}" ]; then
              launch_forwarders "${REMOTE_HIGH_SCALE_FORWARD_CMD}"
          else
              launch_k8s_forwarders
          fi
          if wait_for_ports 30 3002 8002; then
              echo -e "${GREEN}✅ Remote High-Scale connection established.${RESET}"
          else
              echo -e "${RED}❌ Failed to establish Remote High-Scale connection.${RESET}"
          fi
      else
          echo -e "${GREEN}✅ Remote High-Scale connection (3002/8002) is already active.${RESET}"
      fi
  elif [ "${REMOTE_HS_ADMIN_MTLS}" = "1" ] && [ -n "${REMOTE_HS_ORIGIN}" ]; then
      echo -e "${GREEN}✅ Remote High-Scale configured via Admin mTLS gateway (${REMOTE_HS_ORIGIN}).${RESET}"
  else
      echo -e "${YELLOW}⚠️ REMOTE_HIGH_SCALE_FORWARD_CMD and K8S_NAMESPACE are unset; skipping remote High-Scale forwarding.${RESET}"
  fi

  echo -e "\n${BOLD}5. Performing Environment Health Audit...${RESET}"
  python3 scripts/dev/audit_environments.py || true

  echo -e "\n${BOLD}${GREEN}🎉 DEV ENVIRONMENT SETUP PROCESS COMPLETE!${RESET}"
  echo "=========================================="
}

run_deploy_wasm() {
  local SCORER_DIR="$REPO_ROOT/compute/scorer" SERVICE_ID="" TOKEN="${FASTLY_API_TOKEN:-}" MATRIX_PATH="$REPO_ROOT/compute/scorer/matrix.json" VOCAB VERSION TMP_WORKSPACE
  [ -d "$HOME/.cargo/bin" ] && export PATH="$HOME/.cargo/bin:$PATH"
  while [ $# -gt 0 ]; do
    case "$1" in
      --service-id) [ $# -ge 2 ] || { echo "error: --service-id requires an argument" >&2; exit 2; }; SERVICE_ID="$2"; shift 2 ;;
      --token)      [ $# -ge 2 ] || { echo "error: --token requires an argument" >&2; exit 2; }; TOKEN="$2"; shift 2 ;;
      --matrix)     [ $# -ge 2 ] || { echo "error: --matrix requires an argument" >&2; exit 2; }; MATRIX_PATH="$2"; shift 2 ;;
      -h|--help)    sed -n '2,18p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
      *)            echo "unknown arg: $1" >&2; exit 1 ;;
    esac
  done
  [ -n "$SERVICE_ID" ] || { echo "error: --service-id is required" >&2; exit 2; }
  [ -n "$TOKEN" ] || { echo "error: --token or FASTLY_API_TOKEN is required" >&2; exit 2; }
  export FASTLY_API_TOKEN="$TOKEN"
  if [ ! -f "$MATRIX_PATH" ]; then
    echo "error: matrix not found at $MATRIX_PATH" >&2
    echo "       run POST /api/session-scoring/retrain (or backend.scoring.build_matrix) to produce $MATRIX_PATH" >&2
    exit 2
  fi
  VOCAB=$(python3 -c 'import json, sys; print(json.load(open(sys.argv[1])).get("vocab_size", 0))' "$MATRIX_PATH")
  if [ "$VOCAB" -eq 0 ]; then
    echo "error: matrix at $MATRIX_PATH is empty (vocab_size=0)" >&2
    echo "       deploy refused. Train against a real fixture first." >&2
    exit 3
  fi
  VERSION=$(python3 -c 'import json, sys; print(json.load(open(sys.argv[1])).get("version", "?"))' "$MATRIX_PATH")
  echo "[deploy_wasm] trained matrix present: version=$VERSION vocab_size=$VOCAB (served via KV, not embedded)"
  TMP_WORKSPACE="$(mktemp -d)"
  trap 'rm -rf "$TMP_WORKSPACE"' EXIT INT TERM
  tar -C "$SCORER_DIR" --exclude=target --exclude=pkg --exclude=.DS_Store -cf - . | tar -C "$TMP_WORKSPACE" -xf -
  cd "$TMP_WORKSPACE"
  echo "[deploy_wasm] fastly compute build (isolated workspace)"
  fastly compute build --auto-yes 2>&1 | tail -3
  echo "[deploy_wasm] fastly compute deploy --service-id $SERVICE_ID"
  fastly compute deploy --service-id "$SERVICE_ID" --accept-defaults 2>&1 | tail -10
  echo "[deploy_wasm] done."
}

case "${1:-}" in
  --restore)     shift; run_restore_snapshot "$@"; exit 0 ;;
  --start-dev)   shift; run_start_dev_environment "$@"; exit 0 ;;
  --deploy-wasm) shift; run_deploy_wasm "$@"; exit 0 ;;
esac

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
restore_cmd:  scripts/dev/snapshot_prod_to_dev.sh --restore $SNAP_PATH
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
    scripts/dev/snapshot_prod_to_dev.sh --restore $SNAP_PATH
    ./run.sh
EOF
