#!/usr/bin/env bash
# check_eslint_count.sh
#
# ESLint error-count CEILING gate for the frontend. Asserts the eslint error
# count over the production source dirs is monotonically <= a ceiling, so a
# refactor can't silently introduce new `as any`, rules-of-hooks, or other
# lint violations. ESLint is otherwise gated NOWHERE (CI runs only the Python
# import-linter; `make lint` is ruff), which is why these accumulated unchecked.
#
# This mirrors check_security_regression_count.sh, inverted: that is a FLOOR
# that must not drop (audited-fix coverage); this is a CEILING that must not
# rise (lint debt). RATCHET IT DOWN: when a PR removes violations, lower
# CEILING to the new actual in the same PR (same spirit as the coverage
# --cov-fail-under ratchet). Never raise it to "fix" a regression — fix the
# new violation instead.
#
# Run locally: bash scripts/check_eslint_count.sh
# Run in CI:   same; exits 1 if count > ceiling.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT/frontend"

# Ceiling = the error count at the time this gate was introduced (2026-06-19),
# scoped to the production source dirs below (generated types/, __tests__/,
# e2e/, and build output are out of scope — build output is ignored in
# eslint.config.mjs). Lower this number whenever a PR drives the count down.
# Lineage (ratchet down as `as any` / violations are removed): 940 -> 936 -> 932 -> 901 -> 895 -> 889 -> 875 -> 873 -> 866 -> 837 -> 835.
# The big drops came from typing openapi-fetch responses instead of `any` — UX-17
# (network/page.tsx), the SRE observability pass (health/scoring fields), and the
# UX type-drift remediation (sections request bodies, usage/dashboard/alerts reads,
# AdminPrefetchLinks). 895 -> 889: LatencyHeatmap column dedup collapsed six
# duplicated `(info: any)` cells into shared column constants. 889 -> 875: shielding
# audit display fixes typed shielding_analysis (ShieldingAnalysis/ShieldingRow) end
# to end, removing `as any` casts in network/page.tsx + ShieldingMap. 875 -> 873:
# the typed adjustShieldingRows helper (min-requests threshold) replaced ad-hoc
# row handling without new `any`. 873 -> 866: the share-dashboard usage/sort work
# typed ShareStatus.invites/sessions/audit_logs (Invite/ShareSession/AuditLog)
# instead of `any[]`, net-removing casts across the three share panels. 866 ->
# 837: Phase-4 Track D insights cleanup — typed InsightsClient availability/data
# reads against InsightsAvailabilityResponse/InsightsResponse (dropped `as any`),
# GeoJSON.Feature[] in ImpossibleDistanceModal, and escaped pre-existing JSX
# entities across the InsightHelpModal section files. 837 ->
# 835: MetadataStorageCard adopted the typed /api/admin/metadata-storage path
# (dropped both `as any` casts on the client.GET call). 835 -> 832: the
# FieldSearchDialog pinned-selected rework replaced its index key and both
# `v.value as any` casts (FieldTopEntry.value is `unknown` on the wire) with
# `String(v.value)` / `as string | number`. 824 -> 820: the RUM Faro
# self-hosting work typed RumClient's filter payload as FiltersPayload and its
# worst-pages/errors/live-events row shapes, swapped two array-index React keys
# for composite keys off the backend's dedup fields, moved two
# useWizardState effects to render-time derived state, and unified a mismatched
# optional-chain inside one _state.ts useMemo. 820 -> 819: resolved unescaped
# single quotes in ServicesTable / TeardownDialog and suppressed state-in-effect / purity
# warnings across multiple workspace files. 819 -> 817: net drop from the RUM
# branch's frontend work (measured on a clean worktree at the branch tip).
# Drive toward zero.
CEILING=817

# Scope: the user-facing source where the crash-class (rules-of-hooks) and the
# FE<->BE type-drift (no-explicit-any) live. Keep in sync with the `make
# lint-frontend` target.
SCOPE=(app components hooks lib stores)

# ESLint exits non-zero when it reports errors; capture the JSON anyway.
ESLINT_JSON="$(npx --no-install eslint "${SCOPE[@]}" -f json 2>/dev/null || true)"

if [[ -z "$ESLINT_JSON" ]]; then
    echo "ERROR: eslint produced no JSON output (is the frontend dependency tree installed?)" >&2
    exit 2
fi

COUNT="$(printf '%s' "$ESLINT_JSON" | node -e \
    'let s="";process.stdin.on("data",d=>s+=d).on("end",()=>{try{const a=JSON.parse(s);console.log(a.reduce((n,f)=>n+(f.errorCount||0),0))}catch(e){process.exit(3)}})')"

if [[ -z "$COUNT" || ! "$COUNT" =~ ^[0-9]+$ ]]; then
    echo "ERROR: could not parse eslint error count" >&2
    exit 2
fi

echo "eslint errors (${SCOPE[*]}): $COUNT (ceiling: $CEILING)"

if (( COUNT > CEILING )); then
    echo "FAIL: eslint error count rose above the ceiling — new lint violation(s) introduced." >&2
    echo "Fix the new violation(s). To see them: cd frontend && npx eslint ${SCOPE[*]}" >&2
    echo "(Do NOT raise CEILING to pass. Lower it when you remove violations.)" >&2
    exit 1
fi

if (( COUNT < CEILING )); then
    echo "NOTE: count ($COUNT) is below the ceiling ($CEILING) — ratchet CEILING down to $COUNT in this PR." >&2
fi

echo "OK"
