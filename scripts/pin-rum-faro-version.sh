#!/usr/bin/env bash
# pin-rum-faro-version.sh — pin a service's self-hosted Faro Web SDK version.
#
# Why this exists
# ----------------
# Before self-hosting, the RUM tracker loaded the Faro Web SDK from jsDelivr
# with a floating `@^1` range, which today resolves to 1.19.0. Once a service
# is on the self-hosted bundle (see backend/provision/rum_assets.py), leaving
# faro_version unset means the admin RUM page's upgrade flow (Task 8) is the
# only thing that ever sets a version — a service left un-pinned never gets a
# bundle uploaded at all. This script sets an explicit pin so a service keeps
# running the version it's known-good on, replacing that old floating range
# with an explicit, reviewable version string.
#
# Defaults to 2.9.0 (npm dist-tags.latest as of this task) — an operator
# decision made deliberately, not a "safe" fallback to 1.19.0. Pass an
# explicit version as the second argument to pin to something else.
#
# Scope: this script edits ONE service's config JSON in configs/ (gitignored,
# lives outside the repo's tracked tree, contains FOS credentials). It does
# not touch the backend's own contract — faro_version=None still means
# "not pinned" there; this is operator tooling, not a hidden backend default.
#
# Usage
# -----
#   scripts/pin-rum-faro-version.sh <service_id> [version]
#
#   scripts/pin-rum-faro-version.sh svc_abc123           # pins to 2.9.0
#   scripts/pin-rum-faro-version.sh svc_abc123 2.9.0     # same, explicit
#   scripts/pin-rum-faro-version.sh svc_abc123 1.19.0    # pin to a different version
#
# Idempotent: re-running with the same (or no) version argument against an
# already-pinned config is a no-op. Only cfg["rum"]["faro_version"] is
# touched — sibling keys under cfg["rum"] (notably faro_content_hash and
# faro_fos_etag_md5, which the reconcile cron owns) and every other top-level
# key are preserved byte-for-byte. Writes atomically (temp file + mv in the
# same directory) so a failure never leaves a truncated config.
#
# Requires: jq

set -euo pipefail

DEFAULT_VERSION="2.9.0"
# Same shape the backend enforces (_assert_faro_version_safe in
# backend/core/fastly/rum_provisioning.py) — a plain X.Y.Z version string,
# nothing else, since this interpolates into an FOS object path and VCL.
VERSION_RE='^[0-9]+\.[0-9]+\.[0-9]+$'

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CONFIGS_DIR="${REPO_ROOT}/configs"

usage() {
  echo "Usage: $(basename "$0") <service_id> [version]" >&2
  echo "  service_id   Fastly logging service ID (config file: configs/<service_id>.json)" >&2
  echo "  version      X.Y.Z version string; defaults to ${DEFAULT_VERSION}" >&2
}

if ! command -v jq >/dev/null 2>&1; then
  echo "ERROR: jq is required but not found on PATH." >&2
  exit 1
fi

if [[ $# -lt 1 || $# -gt 2 ]]; then
  usage
  exit 1
fi

SERVICE_ID="$1"
VERSION="${2:-$DEFAULT_VERSION}"

if [[ ! "$VERSION" =~ $VERSION_RE ]]; then
  echo "ERROR: version '${VERSION}' is not a plain X.Y.Z string (e.g. 2.9.0) — refusing to write." >&2
  exit 1
fi

CONFIG_FILE="${CONFIGS_DIR}/${SERVICE_ID}.json"

# Never create a config from nothing — a missing file means the service was
# never provisioned through this repo's config system, and a script-created
# stub would be missing FOS credentials and everything else load_config()
# expects.
if [[ ! -f "$CONFIG_FILE" ]]; then
  echo "ERROR: no config found at ${CONFIG_FILE} — refusing to create one." >&2
  echo "       Provision the service first; this script only pins an existing config." >&2
  exit 1
fi

if ! jq empty "$CONFIG_FILE" >/dev/null 2>&1; then
  echo "ERROR: ${CONFIG_FILE} is not valid JSON — refusing to modify it." >&2
  exit 1
fi

CURRENT_VERSION="$(jq -r '.rum.faro_version // empty' "$CONFIG_FILE")"

if [[ "$CURRENT_VERSION" == "$VERSION" ]]; then
  echo "[pin-rum-faro-version] ${SERVICE_ID} already pinned to ${VERSION} — no change."
  exit 0
fi

TMP_FILE="$(mktemp "${CONFIGS_DIR}/.${SERVICE_ID}.json.XXXXXX")"
cleanup() {
  rm -f "$TMP_FILE"
}
trap cleanup EXIT

# jq auto-vivifies .rum as an object if it's absent, and this path assignment
# touches nothing else — sibling keys (including cron-owned faro_content_hash
# / faro_fos_etag_md5) and every other top-level key pass through untouched.
jq --arg version "$VERSION" '.rum.faro_version = $version' "$CONFIG_FILE" > "$TMP_FILE"

# Atomic: mv within the same directory is a single rename on the same
# filesystem, so a crash mid-write never leaves CONFIG_FILE truncated.
mv -f "$TMP_FILE" "$CONFIG_FILE"
trap - EXIT

if [[ -n "$CURRENT_VERSION" ]]; then
  echo "[pin-rum-faro-version] ${SERVICE_ID}: faro_version ${CURRENT_VERSION} -> ${VERSION}"
else
  echo "[pin-rum-faro-version] ${SERVICE_ID}: faro_version pinned to ${VERSION} (was unset)"
fi
echo "[pin-rum-faro-version] Note: this only updates the pin. Run the RUM reconcile cron"
echo "                       (or the admin RUM page's upgrade flow) to actually fetch and"
echo "                       upload the ${VERSION} bundle to FOS if it isn't already there."
