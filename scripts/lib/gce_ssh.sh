#!/usr/bin/env bash
# Shared `gcloud compute ssh` helper. Source it; do not execute it.
#
# The remote command is always built from an argv, each element shell-quoted
# with `printf %q`, so operator-supplied values (paths, names) can never be
# re-parsed as shell syntax on the VM. No eval, no hand-built command strings.
# Bash 3.2 compatible (macOS /bin/bash).
#
#   gce_ssh_run    INSTANCE ZONE [--flag ...] -- CMD [ARG ...]
#   gce_ssh_script INSTANCE ZONE [--flag ...] -- 'SCRIPT using $1 $2 ...' [ARG ...]
#
# gce_ssh_run executes CMD with ARGs. gce_ssh_script runs SCRIPT under
# `bash -c` on the VM with ARGs bound to $1, $2, ... (SCRIPT is a literal,
# never interpolated with caller data). Flags before `--` are passed to gcloud
# (e.g. --quiet). stdout/stderr/exit status pass through unchanged. The remote
# login shell must understand bash-style `printf %q` output (true for GCE
# Debian/Ubuntu images).

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
