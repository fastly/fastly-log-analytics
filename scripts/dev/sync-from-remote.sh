#!/usr/bin/env bash
#
# scripts/dev/sync-from-remote.sh — DEVELOPMENT SCRIPT.
#
# Reset the local tree to a baseline that mirrors the remote GCE
# deployment's site data, then disable every cloud-touching cron and
# credential so the local backend can serve the synced data without
# writing back or pulling more from FOS.
#
# This is a developer-only tool. It is not used by production, by the
# Docker image, by CI, or by the app at runtime. The sole purpose is to
# get a local working copy of the live data so you can iterate on perf
# work or new features against realistic volumes without touching the
# remote ingestion pipeline.
#
# Re-run any time to return to that baseline.
#
# What it does:
#   1. Refuses to run while a local backend is using the data tree.
#   2. Wipes local data/, cache/, configs/ (preserves configs/ssh_known_hosts).
#   3. Streams data/, cache/, configs/ from REMOTE_PATH on the GCE instance
#      via `gcloud compute ssh ... -- tar -c | tar -x` (one SSH session).
#   4. Scrubs each configs/*.json:
#        - empties FOS + Fastly API credentials,
#        - disables every provisioning.cron_*.enabled and metadata_sync,
#        - clears cdn_url at the top level and under provisioning,
#        - nulls provisioning.temp_admin_key_id.
#
# Usage:
#   scripts/dev/sync-from-remote.sh [--instance NAME] [--zone ZONE]
#                                   [--remote-path ABS-PATH]
#                                   [--dry-run] [--yes]
#                                   [--keep data,cache,configs]
#
# Required values (flag or env). Put them in your gitignored .env so a bare
# invocation just works:
#   REMOTE_INSTANCE   GCE instance name           (--instance)
#   REMOTE_ZONE       GCE zone                     (--zone)
#   REMOTE_PATH       absolute path on the VM      (--remote-path)
#                     holding data/ cache/ configs/

set -euo pipefail

# ── Pretty output (icons + colour) ───────────────────────────────────────────
# Honor NO_COLOR (https://no-color.org/) and disable when not a TTY.
if [ -t 1 ] && [ -z "${NO_COLOR:-}" ]; then
  C_RESET=$'\033[0m'
  C_BOLD=$'\033[1m'
  C_DIM=$'\033[2m'
  C_RED=$'\033[31m'
  C_GREEN=$'\033[32m'
  C_YELLOW=$'\033[33m'
  C_BLUE=$'\033[34m'
  C_CYAN=$'\033[36m'
  C_MAGENTA=$'\033[35m'
else
  C_RESET=""; C_BOLD=""; C_DIM=""
  C_RED=""; C_GREEN=""; C_YELLOW=""; C_BLUE=""; C_CYAN=""; C_MAGENTA=""
fi

I_OK="${C_GREEN}✓${C_RESET}"
I_FAIL="${C_RED}✗${C_RESET}"
I_WARN="${C_YELLOW}⚠${C_RESET}"
I_INFO="${C_CYAN}ℹ${C_RESET}"
I_BULLET="${C_DIM}•${C_RESET}"
I_ARROW="${C_BLUE}→${C_RESET}"
I_GEAR="${C_CYAN}⚙${C_RESET}"
I_BROOM="${C_YELLOW}🧹${C_RESET}"
I_DOWN="${C_BLUE}⬇${C_RESET}"
I_SOAP="${C_MAGENTA}🧼${C_RESET}"
I_SPARK="${C_GREEN}✨${C_RESET}"

section() { echo; echo "${C_BOLD}${1}${C_RESET}"; echo "${C_DIM}$(printf '─%.0s' $(seq 1 60))${C_RESET}"; }
ok()      { echo "  ${I_OK} ${*}"; }
warn()    { echo "  ${I_WARN} ${*}"; }
fail()    { echo "  ${I_FAIL} ${*}" >&2; }
info()    { echo "  ${I_INFO} ${*}"; }
step()    { echo "  ${I_ARROW} ${*}"; }
bullet()  { echo "    ${I_BULLET} ${*}"; }

# ── Defaults / arg parsing ───────────────────────────────────────────────────
# Auto-load .env so REMOTE_* don't need to be on every shell invocation.
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
if [ -f "$REPO_ROOT/.env" ]; then
  set -a
  # shellcheck disable=SC1091
  source "$REPO_ROOT/.env"
  set +a
fi

REMOTE_INSTANCE="${REMOTE_INSTANCE:-}"
REMOTE_ZONE="${REMOTE_ZONE:-}"
REMOTE_PATH="${REMOTE_PATH:-}"
DRY_RUN="${DRY_RUN:-0}"
ASSUME_YES="${ASSUME_YES:-0}"
KEEP="${KEEP:-}"
PRUNE_ONLY="${PRUNE_ONLY:-0}"

usage() {
  sed -n '2,40p' "$0" | sed 's/^# \{0,1\}//'
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
# Plain string instead of associative array — macOS ships bash 3.2, which
# doesn't support `declare -A`. `is_skipped` does a substring check on the
# comma-padded KEEP list ("," + KEEP + ",") so partial matches don't trip it.
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

# ── Prune-only short-circuit ────────────────────────────────────────────────
# Skip pre-flight, wipe, sync, scrub. Just prune the local tree using the
# existing configs/*.json as the source of truth for "valid services".
if [ "$PRUNE_ONLY" = 1 ]; then
  section "${I_BROOM} prune-only mode"
  info "scanning local tree against configs/*.json — no wipe, no sync, no scrub."
  ALREADY_PRUNED_ONLY=1
  # Jump straight to the prune block via the existing path: define the
  # variables it expects and run the same logic.
  ACTIVE_CATEGORIES="data cache configs"
  KEEP_PADDED=","
fi

# ── Pre-flight ───────────────────────────────────────────────────────────────
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
if [ "$DRY_RUN" = 1 ]; then
  info "mode:         ${C_YELLOW}DRY-RUN${C_RESET} (no writes)"
fi

# Refuse to run if a local backend is writing into data/ — wiping mid-write
# leaves on-disk SQLite + parquet state inconsistent.
RUNNING_PIDS="$(pgrep -f "$REPO_ROOT.*uvicorn" 2>/dev/null || true)"
if [ -n "$RUNNING_PIDS" ]; then
  fail "a local backend is running from this project (PIDs: $RUNNING_PIDS)"
  echo "      stop it first so the sync is atomic:" >&2
  echo "        ./run.sh --kill" >&2
  exit 1
fi

# Verify gcloud SSH connectivity + remote layout before touching anything local.
if gcloud compute ssh "$REMOTE_INSTANCE" --zone="$REMOTE_ZONE" \
     --command="true" --quiet >/dev/null 2>&1; then
  ok "ssh reachable"
else
  fail "gcloud compute ssh to '$REMOTE_INSTANCE' (zone $REMOTE_ZONE) failed"
  echo "      confirm 'gcloud compute ssh $REMOTE_INSTANCE --zone=$REMOTE_ZONE'" >&2
  echo "      works interactively first." >&2
  exit 1
fi

# Remote files under $REMOTE_PATH (the Docker bind-mount) are owned by the
# container user, not by the SSH user — tar would hit "Permission denied"
# without sudo. Test for passwordless sudo up front so we either succeed
# or fail fast (instead of hanging on a password prompt mid-transfer).
SUDO=""
if gcloud compute ssh "$REMOTE_INSTANCE" --zone="$REMOTE_ZONE" --quiet \
     --command="sudo -n true" >/dev/null 2>&1; then
  SUDO="sudo"
  ok "passwordless sudo available on remote"
else
  warn "no passwordless sudo on remote — will try unprivileged reads"
  warn "(if you hit 'Permission denied' below, either grant NOPASSWD sudo"
  warn " to your SSH user or chmod -R a+rX $REMOTE_PATH on the VM)"
fi

MISSING="$(gcloud compute ssh "$REMOTE_INSTANCE" --zone="$REMOTE_ZONE" --quiet \
    --command="for d in ${ACTIVE_CATEGORIES}; do [ -d \"$REMOTE_PATH/\$d\" ] || echo \$d; done" \
    2>/dev/null || true)"
if [ -n "$MISSING" ]; then
  fail "expected $REMOTE_PATH/{${ACTIVE_CATEGORIES}} on the remote, missing: $MISSING"
  exit 1
fi
ok "remote layout OK"

# Show local + remote sizes so the user can sanity-check before nuking.
# Also capture the raw byte total of the remote tree for the progress bar.
echo
step "${C_BOLD}local${C_RESET}    →    ${C_BOLD}remote${C_RESET}"

REMOTE_BYTES_TOTAL=0
REMOTE_BYTES_RAW="$(gcloud compute ssh "$REMOTE_INSTANCE" --zone="$REMOTE_ZONE" --quiet \
    --command="cd $REMOTE_PATH && for d in ${ACTIVE_CATEGORIES}; do $SUDO du -sb --exclude='*-wal' --exclude='*-shm' --exclude='*.duckdb.wal' \"\$d\" 2>/dev/null; done" \
    2>/dev/null || true)"
# Parse remote sizes (du -sb output: bytes\tpath)
fmt_bytes() {
  python3 -c "
b = float($1)
for unit in ('B','KB','MB','GB'):
  if b < 1024 or unit == 'GB':
    print(f'{b:6.1f} {unit}')
    break
  b /= 1024
"
}
for c in $ACTIVE_CATEGORIES; do
  LSIZE="(absent)"
  if [ -d "$c" ]; then
    LSIZE="$(du -sh "$c" 2>/dev/null | awk '{print $1}')"
  fi
  RBYTES="$(echo "$REMOTE_BYTES_RAW" | awk -v p="$c" '$2==p {print $1; exit}')"
  if [ -n "$RBYTES" ]; then
    REMOTE_BYTES_TOTAL=$((REMOTE_BYTES_TOTAL + RBYTES))
    RHUMAN="$(fmt_bytes "$RBYTES")"
  else
    RHUMAN="(missing)"
  fi
  printf "    %-10s %8s   ${I_ARROW}   %s\n" "${C_BOLD}$c/${C_RESET}" "$LSIZE" "$RHUMAN"
done

# ── Confirm ─────────────────────────────────────────────────────────────────
if [ "$DRY_RUN" != 1 ] && [ "$ASSUME_YES" != 1 ]; then
  echo
  echo "${I_WARN}  About to ${C_BOLD}wipe${C_RESET} local ${C_BOLD}${ACTIVE_CATEGORIES}${C_RESET} then mirror from remote."
  read -r -p "    Proceed? [y/N] " CONFIRM
  case "$CONFIRM" in
    y|Y|yes|YES) ;;
    *) fail "aborted"; exit 1 ;;
  esac
fi

# ── Wipe ────────────────────────────────────────────────────────────────────
section "${I_BROOM} wipe local"
for c in $ACTIVE_CATEGORIES; do
  if [ "$c" = "configs" ]; then
    if [ "$DRY_RUN" = 1 ]; then
      step "${C_DIM}[dry-run]${C_RESET} would wipe configs/* (keep ssh_known_hosts)"
    else
      mkdir -p configs
      # configs/ssh_known_hosts is source-controlled — preserve it.
      find configs -mindepth 1 -maxdepth 1 ! -name ssh_known_hosts -exec rm -rf {} +
      ok "wiped ${C_BOLD}configs/${C_RESET} ${C_DIM}(kept ssh_known_hosts)${C_RESET}"
    fi
  else
    if [ "$DRY_RUN" = 1 ]; then
      step "${C_DIM}[dry-run]${C_RESET} would wipe $c/"
    else
      rm -rf "${c:?}/"
      mkdir -p "$c"
      ok "wiped ${C_BOLD}$c/${C_RESET}"
    fi
  fi
done

# ── Sync ────────────────────────────────────────────────────────────────────
section "${I_DOWN} sync from remote ${C_DIM}(tar | tar over gcloud ssh)${C_RESET}"
if [ "$DRY_RUN" = 1 ]; then
  step "${C_DIM}[dry-run]${C_RESET} would stream ${ACTIVE_CATEGORIES} from $REMOTE_PATH"
  step "${C_DIM}[dry-run]${C_RESET} estimated transfer: ~$(fmt_bytes "$REMOTE_BYTES_TOTAL")"
else
  # Exclude ephemeral SQLite WAL/SHM sidecars — they're tied to the writer
  # process on the remote and will be recreated by ours.
  # Exclude configs/ssh_known_hosts so we don't trample the committed file.
  REMOTE_TAR_CMD="$SUDO tar -C \"$REMOTE_PATH\" \
    --exclude='*-wal' --exclude='*-shm' --exclude='*.duckdb.wal' \
    --exclude='configs/ssh_known_hosts' \
    -cf - ${ACTIVE_CATEGORIES}"

  step "expected ~$(fmt_bytes "$REMOTE_BYTES_TOTAL") on the wire"

  # Live progress: a tiny python middleman copies stdin→stdout while
  # tracking bytes and elapsed time, rendering a status line to stderr.
  # Width is computed from the terminal so the line fits in one row —
  # \033[K wipes any tail from a longer previous render, and the bar
  # auto-shrinks below ~80 cols so we never wrap (which would leave
  # the next \r writing one row below, producing a scrolling effect).
  gcloud compute ssh "$REMOTE_INSTANCE" --zone="$REMOTE_ZONE" --quiet \
    --command="$REMOTE_TAR_CMD" \
    | TOTAL_BYTES="$REMOTE_BYTES_TOTAL" python3 -c '
import os, sys, time, shutil

total = int(os.environ.get("TOTAL_BYTES", 0))
out = sys.stdout.buffer
err = sys.stderr
read = 0
start = time.time()
last = start
last_read = 0
# Cache the terminal width once — re-checking every render shows up on
# strace and is pointless mid-transfer (resize is rare).
try:
    cols = shutil.get_terminal_size((80, 24)).columns
except Exception:
    cols = 80

def fmt_mb(b):
    return f"{b/1048576:6.1f} MB"

def render(now, final=False):
    elapsed = max(now - start, 1e-6)
    overall = read / elapsed
    recent = (read - last_read) / max(now - last, 1e-6) if not final else overall

    # Compose the right-hand status (text after the bar). The bar then
    # uses whatever cells are left, with a min of 6 and a max of 24.
    if total > 0:
        pct = read / total * 100
        rhs = f" {fmt_mb(read)}/{fmt_mb(total)} {pct:5.1f}% {recent/1048576:5.1f} MB/s"
    else:
        rhs = f" {fmt_mb(read)} {recent/1048576:5.1f} MB/s"

    # Visible cells used by the fixed bits: 2 leading spaces, "⬇ ", "[", "]"
    fixed = len("  ⬇ [] ") + len(rhs)
    bar_w = max(6, min(24, cols - fixed - 2))

    if total > 0:
        filled = min(bar_w, int(bar_w * read / total))
        bar = "█" * filled + "░" * (bar_w - filled)
    else:
        win = 4 if bar_w >= 8 else 2
        pos = int((now - start) * 8) % max(1, bar_w - win)
        bar = ("░" * pos) + ("█" * win) + ("░" * (bar_w - win - pos))

    # \r returns cursor to col 0; \033[K wipes anything we wrote earlier
    # that the new (shorter) line would otherwise leave behind.
    err.write(f"\r\033[K  \033[34m⬇\033[0m \033[2m[\033[0m{bar}\033[2m]\033[0m{rhs}")
    err.flush()

while True:
    chunk = sys.stdin.buffer.read(262144)
    if not chunk:
        break
    out.write(chunk)
    read += len(chunk)
    now = time.time()
    if now - last >= 0.1:
        render(now)
        last = now
        last_read = read

render(time.time(), final=True)
err.write("\n")
' \
    | tar -C "$REPO_ROOT" -xf -

  ok "sync complete"
fi

if [ "$DRY_RUN" = 1 ]; then
  echo
  echo "${I_SPARK} ${C_BOLD}dry-run complete${C_RESET} ${C_DIM}— configs/ not scrubbed in dry-run mode${C_RESET}"
  exit 0
fi
fi  # end pre-flight/wipe/sync block (skipped under --prune-only)

# ── Scrub configs ───────────────────────────────────────────────────────────
if [ "$PRUNE_ONLY" != 1 ] && ! is_skipped configs && [ -d configs ]; then
  section "${I_SOAP} scrub configs ${C_DIM}(strip FOS creds + disable crons)${C_RESET}"
  python3 - "$REPO_ROOT/configs" <<'PY'
"""Render every configs/*.json safe for local dev:

  - empty FOS + Fastly + NGWAF credentials,
  - disable every provisioning.cron_*.enabled and provisioning.metadata_sync.enabled,
  - clear cdn_url at top level and under provisioning,
  - null-out provisioning.temp_admin_key_id.

Cron toggles must stay under ``provisioning.*`` — the backend only reads
that path, not the top level."""
import json, os, sys

cfg_dir = sys.argv[1]
CRED_FIELDS = ("fos_access_key_id", "fos_secret_access_key",
               "cdn_secret", "fastly_api_key", "ngwaf_workspace_id")
PROV_CRED_FIELDS = ("fos_key_id",)
CLEAR_URL_FIELDS = ("cdn_url",)

scrubbed = 0
for fname in sorted(os.listdir(cfg_dir)):
    if not fname.endswith(".json"):
        continue
    path = os.path.join(cfg_dir, fname)
    try:
        with open(path) as f:
            cfg = json.load(f)
    except Exception as e:
        print(f"  skip {fname}: {e}")
        continue
    if not isinstance(cfg, dict):
        continue

    changes = []
    for k in CRED_FIELDS:
        if k in cfg and cfg[k] not in ("", None):
            cfg[k] = ""
            changes.append(k)
    for k in CLEAR_URL_FIELDS:
        if k in cfg and cfg[k] not in ("", None):
            cfg[k] = ""
            changes.append(k)

    prov = cfg.get("provisioning")
    if isinstance(prov, dict):
        for k, v in list(prov.items()):
            if (k.startswith("cron_") or k == "metadata_sync") and isinstance(v, dict):
                if v.get("enabled") is True:
                    v["enabled"] = False
                    changes.append(f"provisioning.{k}.enabled")
        for k in PROV_CRED_FIELDS:
            if k in prov and prov[k] not in ("", None):
                prov[k] = ""
                changes.append(f"provisioning.{k}")
        for k in CLEAR_URL_FIELDS:
            if k in prov and prov[k] not in ("", None):
                prov[k] = ""
                changes.append(f"provisioning.{k}")
        if prov.get("temp_admin_key_id") is not None:
            prov["temp_admin_key_id"] = None
            changes.append("provisioning.temp_admin_key_id")

    with open(path, "w") as f:
        json.dump(cfg, f, indent=2)
        f.write("\n")
    scrubbed += 1
    if changes:
        # Dim list of changed keys so the filename pops.
        print(f"  \033[32m✓\033[0m \033[1m{fname}\033[0m  \033[2m{', '.join(changes)}\033[0m")
    else:
        print(f"  \033[32m✓\033[0m \033[1m{fname}\033[0m  \033[2m(already clean)\033[0m")

print(f"  \033[2m→ {scrubbed} config file(s) scrubbed\033[0m")
PY
fi

# ── Prune non-configured service artefacts ──────────────────────────────────
# The remote accumulates leftovers from e2e tests, security fixtures, and
# old provisioning runs: data/services/<weirdname>.{duckdb,metadata.db},
# cache/{attacker,e2e,fake}-bucket/, configs/*.json.bak.*, ...
# After the sync mirrors them down we prune anything not associated with a
# real configured service (one with a configs/<sid>.json file).
section "${I_BROOM} prune non-configured service artefacts"

# Discover valid service IDs from configs/*.json (excluding backups).
VALID_SIDS=""
for f in configs/*.json; do
  [ -f "$f" ] || continue
  case "$f" in *.bak.*) continue ;; esac
  sid="$(basename "$f" .json)"
  VALID_SIDS="$VALID_SIDS $sid"
done
VALID_SIDS="${VALID_SIDS# }"

if [ -z "$VALID_SIDS" ]; then
  warn "no configured services found in configs/*.json — skipping prune"
else
  info "configured services: ${C_BOLD}${VALID_SIDS}${C_RESET}"

  # Lowercase variants for matching cache/fos-<lower-sid>-logs/.
  VALID_LOWER=""
  for v in $VALID_SIDS; do
    VALID_LOWER="$VALID_LOWER $(echo "$v" | tr 'A-Z' 'a-z')"
  done

  in_list() {
    needle="$1"; shift
    for x in "$@"; do [ "$x" = "$needle" ] && return 0; done
    return 1
  }

  removed_data=0; removed_cache=0; removed_configs=0

  # configs/ — drop *.bak.* backups.
  if ! is_skipped configs; then
    for f in configs/*.bak.* configs/*.json.bak.*; do
      [ -e "$f" ] || continue
      rm -f "$f"
      removed_configs=$((removed_configs + 1))
    done
    [ $removed_configs -gt 0 ] && ok "configs/  removed ${C_BOLD}${removed_configs}${C_RESET} backup file(s)"
  fi

  # data/services/ — drop anything whose <base> isn't a configured SID.
  # File basenames look like: <sid>.duckdb, <sid>.metadata.db, <sid>.metadata.db-wal, ...
  # The SID is everything before the first dot.
  if ! is_skipped data && [ -d data/services ]; then
    for f in data/services/*; do
      [ -e "$f" ] || continue
      base="$(basename "$f")"
      sid="${base%%.*}"
      if ! in_list "$sid" $VALID_SIDS; then
        rm -rf "$f"
        removed_data=$((removed_data + 1))
      fi
    done
    [ $removed_data -gt 0 ] && ok "data/services/  removed ${C_BOLD}${removed_data}${C_RESET} stray file(s)"
  fi

  # cache/ — keep top-level globals + per-service fos-<lowercase-sid>-logs/.
  if ! is_skipped cache && [ -d cache ]; then
    for f in cache/*; do
      [ -e "$f" ] || continue
      base="$(basename "$f")"
      case "$base" in
        # Global cache artefacts — keep.
        pop_locations.json|manifest_metadata_cache.json|iceberg_catalog.db|snapshot_files_cache.json|top_values.json)
          continue ;;
      esac
      keep=0
      # cache/fos-<lowercase-sid>-logs/ → check stripped sid against VALID_LOWER.
      case "$base" in
        fos-*-logs)
          sid_lc="${base#fos-}"; sid_lc="${sid_lc%-logs}"
          if in_list "$sid_lc" $VALID_LOWER; then keep=1; fi
          ;;
      esac
      if [ $keep -eq 0 ]; then
        rm -rf "$f"
        removed_cache=$((removed_cache + 1))
      fi
    done
    [ $removed_cache -gt 0 ] && ok "cache/  removed ${C_BOLD}${removed_cache}${C_RESET} stray entry(ies)"
  fi

  if [ $removed_data -eq 0 ] && [ $removed_cache -eq 0 ] && [ $removed_configs -eq 0 ]; then
    info "nothing to prune"
  fi
fi

# ── Summary ─────────────────────────────────────────────────────────────────
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
