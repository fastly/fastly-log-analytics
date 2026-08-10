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
if [[ $# -eq 2 ]]; then
  # An explicit second argument — even an empty string — must not be
  # silently treated as "omitted". Let it fall through to the format check
  # below like any other invalid value, so `pin-rum-faro-version.sh svc ""`
  # errors instead of quietly pinning DEFAULT_VERSION.
  VERSION="$2"
else
  VERSION="$DEFAULT_VERSION"
fi

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

# Structurally-invalid input (e.g. "rum" being a non-object, like a string
# or array) makes jq exit non-zero with its own message on stderr; capture
# that and re-report it as a clean ERROR: line rather than letting a raw
# "jq: error (...)" reach the operator directly. Nothing has been written
# yet at this point, so a failure here needs no cleanup beyond exiting.
if ! CURRENT_VERSION="$(jq -r '.rum.faro_version // empty' "$CONFIG_FILE" 2>&1)"; then
  echo "ERROR: failed to read rum.faro_version from ${CONFIG_FILE}: ${CURRENT_VERSION}" >&2
  exit 1
fi

if [[ "$CURRENT_VERSION" == "$VERSION" ]]; then
  echo "[pin-rum-faro-version] ${SERVICE_ID} already pinned to ${VERSION} — no change."
  exit 0
fi

# Preserve the original file's permission bits. These are credential files
# (FOS access/secret keys) — mktemp always creates its file at mode 0600,
# which could silently tighten (or otherwise diverge from) whatever mode the
# operator/deployment actually has set on CONFIG_FILE.
ORIGINAL_MODE="$(stat -c '%a' "$CONFIG_FILE" 2>/dev/null || stat -f '%Lp' "$CONFIG_FILE")"

TMP_FILE="$(mktemp "${CONFIGS_DIR}/.${SERVICE_ID}.json.XXXXXX")"
cleanup() {
  rm -f "$TMP_FILE"
}
trap cleanup EXIT

# jq auto-vivifies .rum as an object if it's absent, and this path assignment
# touches nothing else — sibling keys (including cron-owned faro_content_hash
# / faro_fos_etag_md5) and every other top-level key pass through untouched.
# Structurally-invalid input (e.g. "rum" being a non-object) makes jq exit
# non-zero with its own message on stderr; capture that and re-report it as
# a clean ERROR: line instead of letting a raw "jq: error (...)" reach the
# operator directly. The EXIT trap above still fires on this path, so a
# failure here still leaves no stray temp file and CONFIG_FILE untouched.
if ! JQ_ERR="$(jq --arg version "$VERSION" '.rum.faro_version = $version' "$CONFIG_FILE" 2>&1 1>"$TMP_FILE")"; then
  echo "ERROR: failed to set rum.faro_version in ${CONFIG_FILE}: ${JQ_ERR}" >&2
  exit 1
fi

chmod "$ORIGINAL_MODE" "$TMP_FILE"

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
