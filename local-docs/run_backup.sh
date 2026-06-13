#!/bin/bash
# Wrapper that sets the prod-specific env vars + invokes the generic
# backup script. Lives in local-docs/ (untracked) per the project's
# infra-stays-local convention — the public repo's script
# (scripts/backup_service_configs.sh) takes everything via env vars
# precisely so the prod values don't end up committed.
#
# Schedule (laptop cron): see `crontab -l` — runs weekly. If the laptop
# is off when cron fires, the next run catches up; the GCS bucket's
# 30-day lifecycle keeps a month of history regardless of missed runs.
#
# To run manually:    bash local-docs/run_backup.sh
# To dry-run:         bash local-docs/run_backup.sh --dry-run

set -euo pipefail

# Ensure PATH has gcloud + the script's deps when cron invokes us with a
# stripped environment. Mirrors what an interactive shell would have.
export PATH="$HOME/Downloads/google-cloud-sdk/bin:/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin"

export BACKUP_BUCKET="gs://fastly-log-analytics-backups"
export BACKUP_INSTANCE="fastly-log-analysis"
export BACKUP_ZONE="us-central1-a"
export BACKUP_CONFIGS_DIR="/mnt/app-data/configs"

REPO_ROOT="/Users/drew.michael/Projects/fastly-log-analytics"
LOG_FILE="$HOME/backup_service_configs.log"

# Rotate the log when it crosses ~1 MB so it never grows unboundedly.
if [ -f "$LOG_FILE" ] && [ "$(wc -c < "$LOG_FILE" | tr -d ' ')" -gt 1048576 ]; then
  mv "$LOG_FILE" "$LOG_FILE.old"
fi

echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] starting backup" >> "$LOG_FILE"
if bash "$REPO_ROOT/scripts/backup_service_configs.sh" "$@" >> "$LOG_FILE" 2>&1; then
  echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] done" >> "$LOG_FILE"
else
  rc=$?
  echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] FAILED rc=$rc" >> "$LOG_FILE"
  exit $rc
fi
