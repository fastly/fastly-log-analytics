#!/usr/bin/env bash
# Shared `gcloud compute ssh` and dev-workflow helpers. Source it; do not execute it.
#
# The remote command is always built from an argv, each element shell-quoted
# with `printf %q`, so operator-supplied values (paths, names) can never be
# re-parsed as shell syntax on the VM. Bash 3.2 compatible (macOS /bin/bash).
#
#   gce_ssh_run    INSTANCE ZONE [--flag ...] -- CMD [ARG ...]
#   gce_ssh_script INSTANCE ZONE [--flag ...] -- 'SCRIPT using $1 $2 ...' [ARG ...]

_gce_ssh_quote() {
  local out="" arg
  for arg in "$@"; do
    out="${out}$(printf '%q' "$arg") "
  done
  printf '%s' "${out% }"
}

gce_ssh_run() {
  if [ "$#" -lt 2 ]; then
    echo "gce_ssh: usage: INSTANCE ZONE [--flag ...] -- CMD [ARG ...]" >&2
    return 2
  fi
  local instance="$1" zone="$2"
  shift 2
  local re='^[A-Za-z0-9][A-Za-z0-9._-]*$'
  if ! [[ "$instance" =~ $re ]]; then
    echo "gce_ssh: refusing instance name with unexpected characters" >&2
    return 2
  fi
  if ! [[ "$zone" =~ $re ]]; then
    echo "gce_ssh: refusing zone with unexpected characters" >&2
    return 2
  fi
  local -a flags=()
  while [ "$#" -gt 0 ] && [ "$1" != "--" ]; do
    case "$1" in
      --*) flags+=("$1") ;;
      *)
        echo "gce_ssh: unexpected argument before '--': $1" >&2
        return 2
        ;;
    esac
    shift
  done
  if [ "$#" -lt 2 ]; then
    echo "gce_ssh: missing '-- CMD'" >&2
    return 2
  fi
  shift
  local remote
  remote="$(_gce_ssh_quote "$@")"
  gcloud compute ssh "$instance" --zone="$zone" ${flags[@]+"${flags[@]}"} --command="$remote"
}

gce_ssh_script() {
  if [ "$#" -lt 2 ]; then
    echo "gce_ssh: usage: INSTANCE ZONE [--flag ...] -- SCRIPT [ARG ...]" >&2
    return 2
  fi
  local instance="$1" zone="$2"
  shift 2
  local -a flags=()
  while [ "$#" -gt 0 ] && [ "$1" != "--" ]; do
    flags+=("$1")
    shift
  done
  if [ "$#" -lt 2 ]; then
    echo "gce_ssh: missing '-- SCRIPT'" >&2
    return 2
  fi
  shift
  local script="$1"
  shift
  gce_ssh_run "$instance" "$zone" ${flags[@]+"${flags[@]}"} -- bash -c "$script" _ "$@"
}

# ── Shared dev CLI output & environment helpers ──────────────────────────────

init_dev_colors() {
  if [ -t 1 ] && [ -z "${NO_COLOR:-}" ]; then
    C_RESET=$'\033[0m'; C_BOLD=$'\033[1m'; C_DIM=$'\033[2m'
    C_RED=$'\033[31m'; C_GREEN=$'\033[32m'; C_YELLOW=$'\033[33m'
    C_BLUE=$'\033[34m'; C_CYAN=$'\033[36m'; C_MAGENTA=$'\033[35m'
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
}

section() { echo; echo "${C_BOLD:-}${1}${C_RESET:-}"; echo "${C_DIM:-}$(printf '─%.0s' $(seq 1 60))${C_RESET:-}"; }
ok()      { echo "  ${I_OK:-✓} ${*}"; }
warn()    { echo "  ${I_WARN:-⚠} ${*}"; }
fail()    { echo "  ${I_FAIL:-✗} ${*}" >&2; }
err()     { fail "$@"; }
die()     { fail "$@"; exit 1; }
info()    { echo "  ${I_INFO:-ℹ} ${*}"; }
step()    { echo "  ${I_ARROW:-→} ${*}"; }
bullet()  { echo "    ${I_BULLET:-•} ${*}"; }

load_repo_dotenv() {
  local repo_root="${1:-.}"
  if [ -f "$repo_root/.env" ]; then
    set -a
    # shellcheck disable=SC1091
    source "$repo_root/.env"
    set +a
  fi
}

load_persisted_admin_mtls_env() {
  local env_file="${1:-${FLA_ADMIN_DEPLOY_ENV:-$HOME/.config/fastly-log-analytics/admin/deploy.env}}"
  if [ -e "$env_file" ] || [ -L "$env_file" ]; then
    local fla_persisted fla_key fla_value
    fla_persisted=$(python3 - "$env_file" <<'PY' | base64
import sys
from scripts.lib.environments import load_persisted_deploy_env
for key, value in load_persisted_deploy_env(sys.argv[1]).items():
    sys.stdout.write(f"{key}\0{value}\0")
PY
    ) || return 1
    while IFS= read -r -d '' fla_key && IFS= read -r -d '' fla_value; do
      if [ -n "${!fla_key+x}" ]; then
        continue
      fi
      printf -v "$fla_key" '%s' "$fla_value"
      export "${fla_key?}"
    done < <(printf '%s' "$fla_persisted" | base64 -d)
  fi
}

require_local_backend_stopped() {
  local repo_root="${1:-.}"
  local reason="${2:-the operation is atomic}"
  local running_pids
  running_pids="$(pgrep -f "$repo_root.*uvicorn" 2>/dev/null || true)"
  if [ -n "$running_pids" ]; then
    fail "a local backend is running from this project (PIDs: $running_pids)"
    echo "      stop it first so $reason:" >&2
    echo "        ./run.sh --kill" >&2
    exit 1
  fi
}

fmt_bytes() {
  python3 -c "
b = float(${1:-0})
for unit in ('B','KB','MB','GB'):
  if b < 1024 or unit == 'GB':
    print(f'{b:6.1f} {unit}')
    break
  b /= 1024
"
}

stream_tar_with_progress() {
  local total_bytes="${1:-0}"
  local dest_dir="${2:-.}"
  TOTAL_BYTES="$total_bytes" python3 -c '
import os, sys, time, shutil
total = int(os.environ.get("TOTAL_BYTES", 0))
out, err = sys.stdout.buffer, sys.stderr
read = last_read = 0
start = last = time.time()
try:
    cols = shutil.get_terminal_size((80, 24)).columns
except Exception:
    cols = 80
def fmt_mb(b): return f"{b/1048576:6.1f} MB"
def render(now, final=False):
    elapsed = max(now - start, 1e-6)
    overall = read / elapsed
    recent = (read - last_read) / max(now - last, 1e-6) if not final else overall
    rhs = (f" {fmt_mb(read)}/{fmt_mb(total)} {read/total*100:5.1f}% {recent/1048576:5.1f} MB/s"
           if total > 0 else f" {fmt_mb(read)} {recent/1048576:5.1f} MB/s")
    bar_w = max(6, min(24, cols - len("  ⬇ [] ") - len(rhs) - 2))
    if total > 0:
        filled = min(bar_w, int(bar_w * read / total))
        bar = "█" * filled + "░" * (bar_w - filled)
    else:
        win = 4 if bar_w >= 8 else 2
        pos = int((now - start) * 8) % max(1, bar_w - win)
        bar = ("░" * pos) + ("█" * win) + ("░" * (bar_w - win - pos))
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
        last, last_read = now, read
render(time.time(), final=True)
err.write("\n")
' | tar -C "$dest_dir" -xf -
}

scrub_dev_configs() {
  local configs_dir="$1"
  python3 - "$configs_dir" <<'PY'
import json, os, sys
cfg_dir = sys.argv[1]
CRED_FIELDS = ("fos_access_key_id", "fos_secret_access_key", "cdn_secret", "fastly_api_key", "ngwaf_workspace_id")
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
    for k in CRED_FIELDS + CLEAR_URL_FIELDS:
        if k in cfg and cfg[k] not in ("", None):
            cfg[k] = ""
            changes.append(k)
    prov = cfg.get("provisioning")
    if isinstance(prov, dict):
        for k, v in list(prov.items()):
            if k.startswith("cron_") and isinstance(v, dict) and v.get("enabled") is True:
                v["enabled"] = False
                changes.append(f"provisioning.{k}.enabled")
        for k in PROV_CRED_FIELDS + CLEAR_URL_FIELDS:
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
    summary = ", ".join(changes) if changes else "(already clean)"
    print(f"  \033[32m✓\033[0m \033[1m{fname}\033[0m  \033[2m{summary}\033[0m")
print(f"  \033[2m→ {scrubbed} config file(s) scrubbed\033[0m")
PY
}

prune_unconfigured_service_artefacts() {
  local valid_sids="" valid_lower="" f sid base sid_lc keep removed_data=0 removed_cache=0 removed_configs=0
  for f in configs/*.json; do
    [ -f "$f" ] || continue
    case "$f" in *.bak.*) continue ;; esac
    sid="$(basename "$f" .json)"
    valid_sids="$valid_sids $sid"
  done
  valid_sids="${valid_sids# }"
  if [ -z "$valid_sids" ]; then
    warn "no configured services found in configs/*.json — skipping prune"
    return 0
  fi
  info "configured services: ${C_BOLD}${valid_sids}${C_RESET}"
  for v in $valid_sids; do
    valid_lower="$valid_lower $(echo "$v" | tr 'A-Z' 'a-z')"
  done
  _in_list() { local n="$1"; shift; for x in "$@"; do [ "$x" = "$n" ] && return 0; done; return 1; }
  if ! is_skipped configs; then
    for f in configs/*.bak.* configs/*.json.bak.*; do
      [ -e "$f" ] || continue
      rm -f "$f"
      removed_configs=$((removed_configs + 1))
    done
    [ $removed_configs -gt 0 ] && ok "configs/  removed ${C_BOLD}${removed_configs}${C_RESET} backup file(s)"
  fi
  if ! is_skipped data && [ -d data/services ]; then
    for f in data/services/*; do
      [ -e "$f" ] || continue
      base="$(basename "$f")"; sid="${base%%.*}"
      if ! _in_list "$sid" $valid_sids; then
        rm -rf "$f"; removed_data=$((removed_data + 1))
      fi
    done
    [ $removed_data -gt 0 ] && ok "data/services/  removed ${C_BOLD}${removed_data}${C_RESET} stray file(s)"
  fi
  if ! is_skipped cache && [ -d cache ]; then
    for f in cache/*; do
      [ -e "$f" ] || continue
      base="$(basename "$f")"
      case "$base" in
        pop_locations.json|manifest_metadata_cache.json|iceberg_catalog.db|snapshot_files_cache.json|top_values.json) continue ;;
      esac
      keep=0
      case "$base" in
        fos-*-logs)
          sid_lc="${base#fos-}"; sid_lc="${sid_lc%-logs}"
          if _in_list "$sid_lc" $valid_lower; then keep=1; fi
          ;;
      esac
      if [ $keep -eq 0 ]; then
        rm -rf "$f"; removed_cache=$((removed_cache + 1))
      fi
    done
    [ $removed_cache -gt 0 ] && ok "cache/  removed ${C_BOLD}${removed_cache}${C_RESET} stray entry(ies)"
  fi
  if [ $removed_data -eq 0 ] && [ $removed_cache -eq 0 ] && [ $removed_configs -eq 0 ]; then
    info "nothing to prune"
  fi
}

launch_forwarders() {
  local -a words cmd=()
  local w
  read -r -a words <<< "$1"
  for w in "${words[@]}"; do
    if [ "$w" = "&" ]; then
      [ "${#cmd[@]}" -gt 0 ] && "${cmd[@]}" &
      cmd=()
    else
      cmd+=("$w")
    fi
  done
  [ "${#cmd[@]}" -gt 0 ] && "${cmd[@]}" &
}

wait_for_ports() {
  local deadline=$((SECONDS + $1)) port
  shift
  while [ "${SECONDS}" -lt "${deadline}" ]; do
    for port in "$@"; do
      lsof -i :"${port}" &>/dev/null || continue 2
    done
    return 0
  done
  return 1
}
