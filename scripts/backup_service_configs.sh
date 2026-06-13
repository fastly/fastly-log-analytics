#!/bin/bash
# Backup per-service config JSON from a production VM to GCS.
#
# Runs FROM THE OPERATOR'S WORKSTATION (not from inside the VM). Uses the
# operator's local gcloud auth for both the SSH leg (gcloud compute ssh)
# and the GCS upload leg (gcloud storage cp). The production VM is not
# required to have a service account attached.
#
# What gets backed up
# -------------------
# Per ADR-13 §2.1, service config JSON on the VM is the ONE piece of
# VM-disk state that's NOT recoverable from FOS. Iceberg data files /
# manifests / metadata.json all live in the FOS bucket (durable);
# metadata.db, the DuckDB cache, and the iceberg catalog SQLite are all
# rebuildable from FOS on a fresh VM. Service configs are not — they
# contain FOS credentials, CDN secrets, log-field config, and ingest
# schedules. Losing them means re-provisioning each service from operator
# memory.
#
# Each run creates one timestamped tarball at:
#   $BACKUP_BUCKET/configs/<YYYY-MM-DD>/configs.tar.gz
#
# Configuration (no defaults — set these via env, .env file, or wrapper script)
# ----------------------------------------------------------------------------
#   BACKUP_BUCKET             GCS bucket URI, e.g. gs://my-backups
#   BACKUP_INSTANCE           GCE instance name
#   BACKUP_ZONE               GCE zone, e.g. us-central1-a
#   BACKUP_CONFIGS_DIR        Absolute path on the VM (e.g. /mnt/app-data/configs)
#
# Why no defaults: the values are infrastructure-specific (per project's
# convention that specific instance / bucket names stay in local-only
# config, not the public repo). Operator supplies them via:
#   - local cron job's env block, OR
#   - a sourced wrapper script outside the repo, OR
#   - inline export before invocation
#
# Bucket lifecycle (configure once on bucket creation):
#   gcloud storage buckets update $BACKUP_BUCKET --lifecycle-file=...
# Recommended: 30-day delete for service-config tarballs (small files,
# cheap to retain a month's worth).
#
# Usage
# -----
#   BACKUP_BUCKET=gs://... BACKUP_INSTANCE=... BACKUP_ZONE=... \
#     BACKUP_CONFIGS_DIR=/mnt/app-data/configs \
#     scripts/backup_service_configs.sh           # backup with today's date
#
#   ... scripts/backup_service_configs.sh --dry-run  # show what would happen
#
# Automation options (NONE wired up by default — pick one):
#   - **Local cron** (simplest): add to operator's crontab with env vars in
#     the line:
#       0 9 * * 1 BACKUP_BUCKET=... BACKUP_INSTANCE=... ... \
#         /path/to/scripts/backup_service_configs.sh >> ~/backup.log 2>&1
#     Runs Mondays 9am local. Operator's gcloud auth must remain valid.
#   - **GH Actions** (requires workload-identity-federation to GCP +
#     gcloud-compute-ssh IAM role): ~30 min setup, removes operator-
#     laptop dependency.
#   - **VM-side cron** (requires VM SA attachment + IAM grant): stop VM,
#     attach an SA with storage.objectAdmin on the bucket, restart. Then
#     a VM-side cron `gsutil cp` works.

set -euo pipefail

# Required configuration — fail fast with a useful message if unset.
: "${BACKUP_BUCKET:?Set BACKUP_BUCKET to a GCS bucket URI, e.g. gs://my-backups}"
: "${BACKUP_INSTANCE:?Set BACKUP_INSTANCE to the GCE instance name}"
: "${BACKUP_ZONE:?Set BACKUP_ZONE to the GCE zone, e.g. us-central1-a}"
: "${BACKUP_CONFIGS_DIR:?Set BACKUP_CONFIGS_DIR to the absolute on-VM path of the configs directory}"

DATE=$(date -u +%Y-%m-%d)
TMP_TAR="$(mktemp -t fla-configs-XXXXXX.tar.gz)"
GCS_PATH="${BACKUP_BUCKET}/configs/${DATE}/configs.tar.gz"

DRY_RUN=0
if [ "${1:-}" = "--dry-run" ]; then
  DRY_RUN=1
fi

cleanup() {
  rm -f "${TMP_TAR}"
}
trap cleanup EXIT

# 1. Tar the configs/ dir on the VM (sudo because the JSON files are
#    typically root-owned 0600 — they contain FOS credentials) and stream
#    the tarball back. tar-over-SSH is faster + atomic vs scp on a dir.
echo "[backup] streaming ${BACKUP_INSTANCE}:${BACKUP_CONFIGS_DIR}/ -> ${TMP_TAR}"
gcloud compute ssh "${BACKUP_INSTANCE}" --zone="${BACKUP_ZONE}" --command="\
  set -e; \
  cd \$(dirname ${BACKUP_CONFIGS_DIR}); \
  sudo tar czf - \$(basename ${BACKUP_CONFIGS_DIR})" \
  > "${TMP_TAR}"

SIZE=$(wc -c < "${TMP_TAR}" | tr -d ' ')
if [ "${SIZE}" -lt 256 ]; then
  echo "[backup] ERROR: tarball is only ${SIZE} bytes — refusing to upload." >&2
  echo "[backup] (Empty configs/ would shadow a real backup. Check SSH + sudo on the VM.)" >&2
  exit 1
fi
echo "[backup] tarball ready: ${SIZE} bytes"

# 2. Upload to GCS. Single-object PUT for files this small (KBs–low MB).
if [ "${DRY_RUN}" -eq 1 ]; then
  echo "[backup] DRY RUN: would upload to ${GCS_PATH}"
  exit 0
fi

echo "[backup] uploading -> ${GCS_PATH}"
gcloud storage cp "${TMP_TAR}" "${GCS_PATH}"

# 3. Verify the object landed (and is non-empty).
LISTED=$(gcloud storage ls -l "${GCS_PATH}" 2>&1 | head -1)
echo "[backup] verified: ${LISTED}"
echo "[backup] done."
