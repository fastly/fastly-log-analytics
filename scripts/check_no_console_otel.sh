#!/usr/bin/env bash
# SRE-10 / ADR-08 §4-§5: fail if any tracked deploy/env file activates OTEL_EXPORTER=console.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR/.."
# shellcheck source=scripts/lib/ratchet_common.sh
source "$SCRIPT_DIR/lib/ratchet_common.sh"

PATTERN='^[^#]*OTEL_EXPORTER[[:space:]]*[=:][[:space:]]*"?'"'"'?console'

run_regex_ban \
  "$PATTERN" \
  "ERROR: OTEL_EXPORTER=console found in a tracked deploy file (ADR-08 §4-§5):" \
  "Set OTEL_EXPORTER=none (or leave it unset) for any deployed environment." \
  "OK: no OTEL_EXPORTER=console in tracked deploy files." \
  'docker-compose*.yml' 'docker-compose*.yaml' '*.env' '.env.*' 'Dockerfile*' '**/Dockerfile*' \
  ':(exclude).github/**'
