#!/usr/bin/env bash
# Shared ratchet, AST, regex, and count-gate helpers for scripts/check_*.sh guards.

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

# run_regex_ratchet <pattern> <baseline_table> <error_suffix> <remediation_hint> <ok_message> <pathspecs...>
run_regex_ratchet() {
  local pattern="$1"
  local baseline="$2"
  local error_suffix="$3"
  local remediation_hint="$4"
  local ok_message="$5"
  shift 5
  local counts
  counts=$(git grep -nE "$pattern" -- "$@" 2>/dev/null \
    | grep -vE '^[^:]+:[0-9]+:[[:space:]]*#' | cut -d: -f1 | sort | uniq -c || true)
  check_ratchet_counts "$counts" "$baseline" "$error_suffix" "$remediation_hint" "$ok_message"
}

# run_regex_ban <pattern> <error_header> <remediation_hint> <ok_message> <pathspecs...>
run_regex_ban() {
  local pattern="$1"
  local error_header="$2"
  local remediation_hint="$3"
  local ok_message="$4"
  shift 4
  local matches
  matches=$(git grep -nE "$pattern" -- "$@" 2>/dev/null || true)
  if [ -n "$matches" ]; then
    echo "$error_header" >&2
    echo "$matches" >&2
    echo "$remediation_hint" >&2
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

# run_ast_ratchet <repo_root> <baseline_table> <exempt_csv> <error_suffix> <remediation_hint> <ok_message>
run_ast_ratchet() {
  local repo_root="$1"
  local baseline="$2"
  local exempt_csv="$3"
  local error_suffix="$4"
  local remediation_hint="$5"
  local ok_message="$6"

  local counts
  counts=$(python3 -I - "$repo_root" "$exempt_csv" <<'PY'
from __future__ import annotations

import ast
import os
import subprocess
import sys
from collections import Counter

SLEEP_NAMES = {"sleep", "_retry_sleep"}
EXEMPT = {p.strip() for p in sys.argv[2].split(",") if p.strip()}


def _walk_same_scope(node: ast.AST):
    for child in ast.iter_child_nodes(node):
        if isinstance(child, ast.FunctionDef | ast.AsyncFunctionDef | ast.Lambda | ast.ClassDef):
            continue
        yield child
        yield from _walk_same_scope(child)


def _is_sleep(node: ast.AST) -> bool:
    if not isinstance(node, ast.Call):
        return False
    fn = node.func
    name = fn.attr if isinstance(fn, ast.Attribute) else getattr(fn, "id", "")
    return name in SLEEP_NAMES


def retry_loops(source: str) -> int:
    tree = ast.parse(source)
    n = 0
    for loop in ast.walk(tree):
        if not isinstance(loop, ast.For | ast.AsyncFor | ast.While):
            continue
        body = [
            c
            for stmt in loop.body
            if not isinstance(stmt, ast.FunctionDef | ast.AsyncFunctionDef | ast.ClassDef)
            for c in (stmt, *_walk_same_scope(stmt))
        ]
        if any(isinstance(c, ast.ExceptHandler) for c in body) and any(_is_sleep(c) for c in body):
            n += 1
    return n


def main() -> int:
    os.chdir(sys.argv[1])
    files = subprocess.run(
        ["git", "ls-files", "backend/*.py"], capture_output=True, text=True, check=True
    ).stdout.split()
    counts: Counter[str] = Counter()
    for path in files:
        if path in EXEMPT:
            continue
        try:
            with open(path, encoding="utf-8") as fh:
                n = retry_loops(fh.read())
        except (OSError, SyntaxError):
            continue
        if n:
            counts[path] = n
    for path, n in sorted(counts.items()):
        print(n, path)
    return 0


if __name__ == "__main__":
    sys.exit(main())
PY
)
  check_ratchet_counts "$counts" "$baseline" "$error_suffix" "$remediation_hint" "$ok_message"
}

# run_count_floor <floor> <marker>
run_count_floor() {
  local floor="$1"
  local marker="${2:-security_regression}"
  local count
  count=$(uv run pytest -m "$marker" --collect-only 2>/dev/null \
    | grep -E "tests? collected" \
    | tail -1 \
    | sed -E 's|^([0-9]+)/.*|\1|')

  if [[ -z "$count" || ! "$count" =~ ^[0-9]+$ ]]; then
    echo "ERROR: could not parse $marker test count" >&2
    exit 2
  fi

  echo "$marker tests: $count (floor: $floor)"
  if (( count < floor )); then
    echo "FAIL: count dropped below floor — a verified security fix lost test coverage." >&2
    echo "If the drop is intentional (e.g., a fix became structurally impossible to regress)," >&2
    echo "lower the FLOOR in this script in the same PR and explain in the commit message." >&2
    exit 1
  fi
  echo "OK"
}

# _eslint_count_errors <warn_flag> <paths...>
_eslint_count_errors() {
  local json warn="$1"
  shift
  json="$(npx --no-install eslint "$@" -f json 2>/dev/null || true)"
  if [[ -z "$json" ]]; then
    echo "ERROR: eslint produced no JSON output (is the frontend dependency tree installed?)" >&2
    exit 2
  fi
  local n
  n="$(printf '%s' "$json" | COUNT_WARNINGS="$warn" node -e \
    'let s="";process.stdin.on("data",d=>s+=d).on("end",()=>{try{const a=JSON.parse(s);console.log(a.reduce((n,f)=>n+(f.errorCount||0)+(process.env.COUNT_WARNINGS==="1"?(f.warningCount||0):0),0))}catch(e){process.exit(3)}})')"
  if [[ -z "$n" || ! "$n" =~ ^[0-9]+$ ]]; then
    echo "ERROR: could not parse eslint error count" >&2
    exit 2
  fi
  echo "$n"
}

# check_eslint_scope <label> <ceiling> <warn_flag> <paths...>
check_eslint_scope() {
  local label="$1" ceiling="$2" warn="$3"
  shift 3
  local count
  count="$(_eslint_count_errors "$warn" "$@")"
  echo "eslint errors$([[ "$warn" == 1 ]] && echo "+warnings") ($label: $*): $count (ceiling: $ceiling)"
  if (( count > ceiling )); then
    echo "FAIL: eslint error count rose above the ceiling — new lint violation(s) introduced." >&2
    echo "Fix the new violation(s). To see them: cd frontend && npx eslint $*" >&2
    echo "(Do NOT raise the ceiling to pass. Lower it when you remove violations.)" >&2
    exit 1
  fi
  if (( count < ceiling )); then
    echo "NOTE: count ($count) is below the ceiling ($ceiling) — ratchet it down to $count in this PR." >&2
  fi
}
