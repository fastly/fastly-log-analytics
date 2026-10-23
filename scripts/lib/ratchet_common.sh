#!/usr/bin/env bash
# Shared ratchet/allowlist verification helpers for scripts/check_no_*.sh guards.

# check_ratchet_counts <counts_output> <baseline_table> <error_suffix> <remediation_hint> <ok_message>
check_ratchet_counts() {
  local counts="$1"
  local baseline="$2"
  local error_suffix="$3"
  local remediation_hint="$4"
  local ok_message="$5"

  local fail=0
  while read -r count file; do
    [ -z "${file:-}" ] && continue
    local allowed
    allowed=$(printf '%s\n' "$baseline" | awk -v f="$file" '$1 == f { print $2 }')
    allowed=${allowed:-0}
    if [ "$count" -gt "$allowed" ]; then
      echo "ERROR: $file has $count $error_suffix" | sed "s/__ALLOWED__/$allowed/g" >&2
      echo "  $remediation_hint" >&2
      fail=1
    fi
  done <<< "$counts"

  if [ "$fail" -ne 0 ]; then
    exit 1
  fi

  echo "$ok_message"
}

# check_allowlist_offenders <offenders_output> <error_header> <remediation_hint> <ok_message> <allowlist_items...>
check_allowlist_offenders() {
  local offenders="$1"
  local error_header="$2"
  local remediation_hint="$3"
  local ok_message="$4"
  shift 4
  local allowlist=("$@")

  local violations=""
  while IFS= read -r f; do
    [ -z "$f" ] && continue
    local allowed=false
    for a in "${allowlist[@]}"; do
      if [ "$f" = "$a" ]; then
        allowed=true
        break
      fi
    done
    if [ "$allowed" = false ]; then
      violations+="$f"$'\n'
    fi
  done <<< "$offenders"

  if [ -n "$violations" ]; then
    echo "$error_header" >&2
    echo "$violations" >&2
    echo "$remediation_hint" >&2
    exit 1
  fi

  echo "$ok_message"
}
