#!/usr/bin/env bash
#
# scripts/dev/sync-from-remote.sh — DEVELOPMENT SCRIPT.
#
# Reset the local tree to a baseline that mirrors the remote GCE
# deployment's site data, then disable every cloud-touching cron and
# credential so the local backend can serve the synced data without
# writing back or pulling more from FOS.
#
# Usage:
#   scripts/dev/sync-from-remote.sh [--instance NAME] [--zone ZONE]
#                                   [--remote-path ABS-PATH]
#                                   [--dry-run] [--yes]
#                                   [--keep data,cache,configs]
#
# Required values (flag or env):
#   REMOTE_INSTANCE   GCE instance name           (--instance)
#   REMOTE_ZONE       GCE zone                     (--zone)
#   REMOTE_PATH       absolute path on the VM      (--remote-path)
#                     holding data/ cache/ configs/

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
# shellcheck source=scripts/lib/gce_ssh.sh
source "$REPO_ROOT/scripts/lib/gce_ssh.sh"
init_dev_colors
load_repo_dotenv "$REPO_ROOT"

REMOTE_INSTANCE="${REMOTE_INSTANCE:-}"
REMOTE_ZONE="${REMOTE_ZONE:-}"
REMOTE_PATH="${REMOTE_PATH:-}"
DRY_RUN="${DRY_RUN:-0}"
ASSUME_YES="${ASSUME_YES:-0}"
KEEP="${KEEP:-}"
PRUNE_ONLY="${PRUNE_ONLY:-0}"

usage() {
  sed -n '2,20p' "$0" | sed 's/^# \{0,1\}//'
  exit "${1:-0}"
}

while [ $# -gt 0 ]; do
  case "$1" in
    --instance)    REMOTE_INSTANCE="$2"; shift 2 ;;
    --zone)        REMOTE_ZONE="$2"; shift 2 ;;
    --remote-path) REMOTE_PATH="$2"; shift 2 ;;
    --keep)        KEEP="$2"; shift 2 ;;
    --dry-run)     DRY_RUN=1; shift ;;
    --prune-only)  PRUNE_ONLY=1; shift ;;
    -y|--yes)      ASSUME_YES=1; shift ;;
    -h|--help)     usage 0 ;;
    *)             echo "[!] unknown arg: $1" >&2; usage 1 ;;
  esac
done

if [ "$PRUNE_ONLY" != 1 ]; then
  if [ -z "$REMOTE_INSTANCE" ] || [ -z "$REMOTE_ZONE" ] || [ -z "$REMOTE_PATH" ]; then
    fail "--instance, --zone, and --remote-path are required"
    echo "    (or set REMOTE_INSTANCE, REMOTE_ZONE, REMOTE_PATH — your .env" >&2
    echo "     is auto-sourced, and .env is gitignored)." >&2
    usage 1
  fi
fi

REMOTE_PATH="${REMOTE_PATH%/}"
cd "$REPO_ROOT"

ALL_CATEGORIES="data cache configs"
KEEP_PADDED=",$KEEP,"
is_skipped() { case "$KEEP_PADDED" in *",$1,"*) return 0 ;; *) return 1 ;; esac; }

ACTIVE_CATEGORIES=""
for c in $ALL_CATEGORIES; do
  if ! is_skipped "$c"; then
    ACTIVE_CATEGORIES="$ACTIVE_CATEGORIES $c"
  fi
done
ACTIVE_CATEGORIES="${ACTIVE_CATEGORIES# }"
if [ -z "$ACTIVE_CATEGORIES" ]; then
  fail "--keep excluded every category — nothing to do."
  exit 1
fi

if [ "$PRUNE_ONLY" = 1 ]; then
  section "${I_BROOM} prune-only mode"
  info "scanning local tree against configs/*.json — no wipe, no sync (configs are still scrubbed)."
  ALREADY_PRUNED_ONLY=1
  ACTIVE_CATEGORIES="data cache configs"
  KEEP_PADDED=","
fi

if [ "$PRUNE_ONLY" != 1 ]; then
  need() { command -v "$1" >/dev/null 2>&1 || { fail "missing required tool: $1"; exit 1; }; }
  need gcloud
  need tar
  need python3

  section "${I_GEAR}  pre-flight"
  info "repo:         ${C_BOLD}$REPO_ROOT${C_RESET}"
  info "remote:       ${C_BOLD}${REMOTE_INSTANCE}${C_RESET} ${C_DIM}(${REMOTE_ZONE})${C_RESET}"
  info "remote path:  ${C_BOLD}$REMOTE_PATH${C_RESET}"
  info "categories:   ${C_BOLD}${ACTIVE_CATEGORIES}${C_RESET}"
  [ "$DRY_RUN" = 1 ] && info "mode:         ${C_YELLOW}DRY-RUN${C_RESET} (no writes)"

  require_local_backend_stopped "$REPO_ROOT" "the sync is atomic"

  if gce_ssh_run "$REMOTE_INSTANCE" "$REMOTE_ZONE" --quiet -- true >/dev/null 2>&1; then
    ok "ssh reachable"
  else
    fail "ssh to '$REMOTE_INSTANCE' (zone $REMOTE_ZONE) failed"
    exit 1
  fi

  SUDO=""
  SUDO_ARGV=()
  read -r -a ACTIVE_ARR <<< "$ACTIVE_CATEGORIES"
  if gce_ssh_run "$REMOTE_INSTANCE" "$REMOTE_ZONE" --quiet -- sudo -n true >/dev/null 2>&1; then
    SUDO="sudo"
    SUDO_ARGV=(sudo)
    ok "passwordless sudo available on remote"
  else
    warn "no passwordless sudo on remote — will try unprivileged reads"
  fi

  # shellcheck disable=SC2016
  MISSING="$(gce_ssh_script "$REMOTE_INSTANCE" "$REMOTE_ZONE" --quiet -- \
      'p=$1; shift; for d in "$@"; do [ -d "$p/$d" ] || echo "$d"; done' \
      "$REMOTE_PATH" "${ACTIVE_ARR[@]}" 2>/dev/null || true)"
  if [ -n "$MISSING" ]; then
    fail "expected $REMOTE_PATH/{${ACTIVE_CATEGORIES}} on the remote, missing: $MISSING"
    exit 1
  fi
  ok "remote layout OK"

  echo
  step "${C_BOLD}local${C_RESET}    →    ${C_BOLD}remote${C_RESET}"
  REMOTE_BYTES_TOTAL=0
  # shellcheck disable=SC2016
  REMOTE_BYTES_RAW="$(gce_ssh_script "$REMOTE_INSTANCE" "$REMOTE_ZONE" --quiet -- \
      'cd "$1" && s=$2 && shift 2 && for d in "$@"; do $s du -sb --exclude="*-wal" --exclude="*-shm" --exclude="*.duckdb.wal" "$d" 2>/dev/null; done' \
      "$REMOTE_PATH" "$SUDO" "${ACTIVE_ARR[@]}" 2>/dev/null || true)"
  for c in $ACTIVE_CATEGORIES; do
    LSIZE="(absent)"
    [ -d "$c" ] && LSIZE="$(du -sh "$c" 2>/dev/null | awk '{print $1}')"
    RBYTES="$(echo "$REMOTE_BYTES_RAW" | awk -v p="$c" '$2==p {print $1; exit}')"
    if [[ "$RBYTES" =~ ^[0-9]+$ ]]; then
      REMOTE_BYTES_TOTAL=$((REMOTE_BYTES_TOTAL + RBYTES))
      RHUMAN="$(fmt_bytes "$RBYTES")"
    else
      RHUMAN="(missing)"
    fi
    printf "    %-10s %8s   ${I_ARROW}   %s\n" "${C_BOLD}$c/${C_RESET}" "$LSIZE" "$RHUMAN"
  done

  if [ "$DRY_RUN" != 1 ] && [ "$ASSUME_YES" != 1 ]; then
    echo
    echo "${I_WARN}  About to ${C_BOLD}wipe${C_RESET} local ${C_BOLD}${ACTIVE_CATEGORIES}${C_RESET} then mirror from remote."
    read -r -p "    Proceed? [y/N] " CONFIRM
    case "$CONFIRM" in
      y|Y|yes|YES) ;;
      *) fail "aborted"; exit 1 ;;
    esac
  fi

  section "${I_BROOM} wipe local"
  for c in $ACTIVE_CATEGORIES; do
    if [ "$c" = "configs" ]; then
      if [ "$DRY_RUN" = 1 ]; then
        step "${C_DIM}[dry-run]${C_RESET} would wipe configs/* (keep .gitkeep)"
      else
        mkdir -p configs
        find configs -mindepth 1 -maxdepth 1 ! -name .gitkeep -exec rm -rf {} +
        ok "wiped ${C_BOLD}configs/${C_RESET} ${C_DIM}(kept .gitkeep)${C_RESET}"
      fi
    elif [ "$DRY_RUN" = 1 ]; then
      step "${C_DIM}[dry-run]${C_RESET} would wipe $c/"
    else
      rm -rf "${c:?}/"
      mkdir -p "$c"
      ok "wiped ${C_BOLD}$c/${C_RESET}"
    fi
  done

  section "${I_DOWN} sync from remote ${C_DIM}(tar | tar over gcloud ssh)${C_RESET}"
  if [ "$DRY_RUN" = 1 ]; then
    step "${C_DIM}[dry-run]${C_RESET} would stream ${ACTIVE_CATEGORIES} from $REMOTE_PATH"
    step "${C_DIM}[dry-run]${C_RESET} estimated transfer: ~$(fmt_bytes "$REMOTE_BYTES_TOTAL")"
    echo
    echo "${I_SPARK} ${C_BOLD}dry-run complete${C_RESET} ${C_DIM}— configs/ not scrubbed in dry-run mode${C_RESET}"
    exit 0
  else
    step "expected ~$(fmt_bytes "$REMOTE_BYTES_TOTAL") on the wire"
    gce_ssh_run "$REMOTE_INSTANCE" "$REMOTE_ZONE" --quiet -- \
      ${SUDO_ARGV[@]+"${SUDO_ARGV[@]}"} tar -C "$REMOTE_PATH" \
      --exclude='*-wal' --exclude='*-shm' --exclude='*.duckdb.wal' \
      -cf - "${ACTIVE_ARR[@]}" \
      | stream_tar_with_progress "$REMOTE_BYTES_TOTAL" "$REPO_ROOT"
    ok "sync complete"
  fi
fi

# ── Scrub configs & Prune stray service artefacts ───────────────────────────
if ! is_skipped configs && [ -d configs ]; then
  section "${I_SOAP} scrub configs ${C_DIM}(strip FOS creds + disable crons)${C_RESET}"
  scrub_dev_configs "$REPO_ROOT/configs"
fi

section "${I_BROOM} prune non-configured service artefacts"
prune_unconfigured_service_artefacts

section "${I_SPARK} done"
for c in $ACTIVE_CATEGORIES; do
  if [ -d "$c" ]; then
    SIZE="$(du -sh "$c" 2>/dev/null | awk '{print $1}')"
    printf "  ${I_OK} ${C_BOLD}%-10s${C_RESET} %s\n" "$c/" "$SIZE"
  fi
done
echo
if [ "$PRUNE_ONLY" = 1 ]; then
  echo "  ${I_INFO} Local tree pruned to configured services only."
else
  echo "  ${I_INFO} Local tree is a sanitised mirror of ${C_BOLD}${REMOTE_INSTANCE}:${REMOTE_PATH}${C_RESET}."
  echo "  ${C_DIM}FOS credentials emptied + every provisioning cron disabled —${C_RESET}"
  echo "  ${C_DIM}safe to start the local backend without touching remote ingestion.${C_RESET}"
fi
echo
