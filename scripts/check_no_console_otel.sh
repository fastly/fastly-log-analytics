#!/usr/bin/env bash
# SRE-10 / ADR-08 §4-§5: fail if any tracked deploy/env file activates the
# OTEL_EXPORTER=console spam mode.
#
# The canonical 2026-06-10 incident: OTEL_EXPORTER defaulted to `console`, so
# every metric tick wrote a ~50-line JSON blob to backend stdout (~1 MB/min of
# unconsumable noise) for weeks before anyone noticed. The exporter default is
# now `none` in code, but a contributor could still hardcode `console` into a
# compose file / Dockerfile / committed env. ADR-08 §5 lists "a grep CI step
# would catch OTEL_EXPORTER=console in deployed env" as a success criterion;
# this is that step. (The real prod .env lives on the VM, outside the repo, so
# this can only guard *tracked* files — but those are the ones a PR can break.)
set -euo pipefail
cd "$(dirname "$0")/.."

# Active (uncommented) assignment, env `=` or yaml `:` form, optional quote.
PATTERN='^[^#]*OTEL_EXPORTER[[:space:]]*[=:][[:space:]]*"?'"'"'?console'

# Scope to actual deploy/env files — compose, Dockerfiles, committed env. NOT
# CI workflows (their step names legitimately mention the string we forbid) and
# NOT docs/this-script (which describe the rule). Enumerated globs rather than a
# blanket *.yml so .github/ prose can't false-positive.
matches=$(git grep -nE "$PATTERN" -- \
  'docker-compose*.yml' 'docker-compose*.yaml' '*.env' '.env.*' 'Dockerfile*' '**/Dockerfile*' \
  ':(exclude).github/**' 2>/dev/null || true)

if [ -n "$matches" ]; then
  echo "ERROR: OTEL_EXPORTER=console found in a tracked deploy file (ADR-08 §4-§5):" >&2
  echo "$matches" >&2
  echo "Set OTEL_EXPORTER=none (or leave it unset) for any deployed environment." >&2
  exit 1
fi

echo "OK: no OTEL_EXPORTER=console in tracked deploy files."
