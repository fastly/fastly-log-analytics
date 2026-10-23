#!/usr/bin/env bash
# Shared logging, .env loading, and local backend preflight helpers for scripts/dev/*.sh.

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
err()  { fail "$@"; }
die()  { fail "$@"; exit 1; }
info() { echo "  ${C_CYAN}ℹ${C_RESET} ${*}"; }
step() { echo "  ${C_BLUE}→${C_RESET} ${*}"; }

load_repo_dotenv() {
  local repo_root="${1:-$REPO_ROOT}"
  if [ -f "$repo_root/.env" ]; then
    set -a
    # shellcheck disable=SC1091
    source "$repo_root/.env"
    set +a
  fi
}

require_local_backend_stopped() {
  local repo_root="${1:-$REPO_ROOT}"
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
