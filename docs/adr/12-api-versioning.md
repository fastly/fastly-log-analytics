# ADR-12 — API Versioning Doctrine

**Status:** Accepted (2026-06-10)
**Decided by:** v2.0 cleanup retrospective (2026-06-10)

## 1. Context & Motivation

The HTTP surface is `/api/*` with no `/api/v1/` prefix. Semantic versioning lives on the app object (`app.version='2.0.0b2'` in [backend/main.py](../../backend/main.py)). The typed frontend client is openapi-typescript-generated from FastAPI's OpenAPI schema, with the pre-commit `regen-openapi` hook gating drift.

The system is internally self-consistent but has no written rule for what counts as breaking. The 2026-06-10 sessions surfaced two related signals: (a) `/api/sync-status?skip_fos=true` was used as a soft-versioning mechanism (query-param "give me less"), and it failed because the underlying middleware 403'd analysts entirely regardless of the param; (b) the chosen fix was a sibling endpoint `/api/log-extents` rather than a versioned variant of `/api/sync-status`. That decision is the doctrine, but it lives in a session note rather than an ADR.

Composite endpoints (`POST /api/scoring/dashboard` collapsing per-card endpoints) accumulated similarly without a stated policy on whether the legacy per-card endpoints get deprecated. Today both ship; both must be kept in sync; no rule says when to retire the older.

This ADR codifies: what counts as a breaking change, the preferred evolution patterns, the analyst-vs-admin surface distinction, and what we explicitly do NOT do (URL versioning).

## 2. Decision

### 2.1 No URL versioning

The `/api/*` namespace is flat. We will not introduce `/api/v1/*` or `/api/v2/*`. Rationale:

- Single client (the frontend in this repo). Coordinated deploys; no third-party integrators to support across versions.
- openapi-typescript regen catches drift at PR time; the typed client is the contract, not the URL.
- URL versioning is a coordination tax that real cross-version migrations rarely benefit from at this scale. Sibling endpoints + Pydantic field aliasing cover everything we've actually needed.

If/when external integrators become real consumers, replace this rule with a real versioning scheme. Until then, this is the honest answer.

### 2.2 What counts as a breaking change

A change is **breaking** if any of these is true:

| Breaking | Not breaking |
|---|---|
| Removing a response field (any kind) | Adding an optional response field |
| Renaming a response field without `serialization_alias` covering the old name | Adding a new `Optional[...] = None` field |
| Changing a response field's type (`int → str`, `str → list[str]`) | Widening a numeric type (`int → float` if all old values still parse) |
| Removing an endpoint | Adding an endpoint |
| Removing a query/body parameter that the client passes | Adding a new optional query/body parameter |
| Making an optional parameter required | Making a required parameter optional with a default |
| Changing an endpoint's HTTP method | Adding a new method to an existing path |
| Changing an endpoint's success status code | Adding a new error status code with documented semantics |
| Changing analyst-visibility of an endpoint to admin-only (and vice versa is a security regression) | Adding new analyst-safe sibling for an admin-only endpoint |
| Changing an `enum` field's set by REMOVING members | Adding new enum members (clients should treat unknown as "other") |

The pre-commit `regen-openapi` hook catches schema drift mechanically; the categorization above is what reviewer enforces at PR time.

### 2.3 Preferred patterns when the API needs to evolve

#### Pattern A — Pure addition (most cases)
Add a field, add an endpoint, add a parameter with a default. openapi-typescript regen flows the type through to the frontend. The pre-commit hook ensures the generated client and the backend models can't diverge.

#### Pattern B — Sibling endpoint (when the projection is different)
The canonical example: `/api/log-extents` is a strict subset of `/api/sync-status` with the admin-only fields removed. Use this when:

- An existing endpoint can't be reduced in scope without breaking other callers
- A new client (analyst, public API) needs a different projection
- The endpoint's middleware behavior (e.g., admin-only gate) is the blocker, not the response shape

Naming convention: descriptive (`/api/log-extents`), not version-suffixed (`/api/sync-status-lite`).

#### Pattern C — Composite endpoint (when the client needs fewer round-trips)
The `/api/scoring/dashboard` collapse of multiple per-card endpoints is the canonical example. Use this for admin dashboards where N parallel queries are wasteful; keep the per-card endpoints live until you can prove no caller still uses them.

#### Pattern D — Deprecate-then-remove (only when D-day matters)
If a field/endpoint genuinely must be removed:

1. PR 1: mark deprecated in the docstring + add a `Deprecation: true` header to the response (FastAPI middleware or per-endpoint dependency). Frontend reviews to confirm nothing depends on it.
2. PR 2 (≥ 1 release later): remove. CHANGELOG.md entry under the version, marked as breaking.

This is rare. Sibling endpoint (B) is almost always cheaper than full deprecation.

### 2.4 Analyst-safe vs admin-only surfaces

The most subtle versioning rule we have. Encoded in middleware via `_ANALYST_BLOCKED_SUBPATHS` ([backend/utils/remote_access.py](../../backend/utils/remote_access.py)). Endpoints under that list are admin-only; the rest are analyst-accessible (subject to the per-endpoint service-scope check).

**Rules:**

- New endpoints default to analyst-safe (i.e., do nothing — they're accessible by default if not added to the blocklist).
- An endpoint is admin-only if it exposes any of: cron task internals, full sync status, FOS/CDN config, audit log full contents, secret material (hashes, key fingerprints).
- An endpoint becomes admin-only by adding its path prefix to `_ANALYST_BLOCKED_SUBPATHS` with a comment explaining why (the existing list is the precedent).
- An analyst-safe sibling for an admin-only endpoint (Pattern B above) is the answer when both surfaces need the data.
- **Never** change an endpoint's analyst-vs-admin classification without a security review note and a CHANGELOG entry. Promoting admin-only → analyst-safe is a potential information-disclosure event.

### 2.5 The contract for the typed client

- `frontend/openapi.json` and `frontend/types/api.generated.ts` are committed to git. They are the snapshot of the API at HEAD.
- The pre-commit `regen-openapi` hook fires on every `backend/*.py` or `scripts/generate_openapi.py` change and regenerates both files. Drift = failed commit. Re-stage and re-commit.
- `frontend/openapi.json` is excluded from end-of-file-fixer (the generator strips trailing newlines; the fixer added them; commit 2026-06-10 broke the cycle).
- Clients NOT in this repo (curl scripts, integrations) consume `openapi.json` directly; they get versioned via git history.

## 3. Out of Scope

- **GraphQL, gRPC, or non-REST surfaces.** FastAPI REST only.
- **Client library generation for other languages.** openapi-typescript covers TS; if Python/Go/Java clients become needed, they'll consume `openapi.json` via standard tooling.
- **Schema evolution at the log-field layer.** Covered by [ADR-10](10-schema-evolution.md); the openapi schema is downstream of that.
- **Error code taxonomy.** Error shapes are part of the response model contract; the categorization (what is a 4xx vs 5xx) lives in [ADR-09](09-error-handling.md) §2.3.
- **Rate limiting / quota policy.** Not part of the API contract; orthogonal concern.
- **Caddy / reverse-proxy URL rewriting.** Caddy routes; versioning is upstream of it.
- **Observability instrumentation per endpoint.** [ADR-08](08-observability.md) covers it.

## 4. Failure Modes & Recovery

| Scenario | Behavior |
|---|---|
| Backend model field renamed; `regen-openapi` updates the generated client; frontend code still references the old name | TypeScript build fails at the next pre-commit (`typecheck-frontend` hook) or in CI. Recovery: rename frontend usages, re-commit. |
| Endpoint removed without deprecation warning | Frontend build fails when openapi types update. Recovery: revert OR ship the frontend swap in the same PR. |
| Composite endpoint shipped while old per-card endpoints removed | Same as above — caught by typecheck. |
| Admin endpoint accidentally accessible to analysts | Security regression. Caught by `tests/routers/test_rbac_audit_fixes.py` + `tests/routers/test_cross_tenant_scope.py`. Add to `_ANALYST_BLOCKED_SUBPATHS` if missing. |
| Analyst endpoint accidentally locked to admin only | Frontend stops working for analysts. Recovery: review `_ANALYST_BLOCKED_SUBPATHS` and undo the inclusion. |
| External integrator depending on undocumented behavior | Out of contract. Document the behavior or revert if it's a security/privacy regression. |
| `openapi.json` has a trailing newline mismatch with what `gen:types` produces | Pre-commit loop. Already resolved: openapi.json excluded from end-of-file-fixer. |
| Major version bump (1.x → 2.0) | `app.version` bumped; this ADR amended if doctrine changes; CHANGELOG documents breaking changes. No URL change. |

## 5. Verification

This ADR succeeds if:

- Drift between backend models and `frontend/types/api.generated.ts` is caught at pre-commit, not in production.
- A new analyst-visible feature ships with the sibling-endpoint pattern by default rather than ad-hoc query-param hacks.
- No PR titled "introduce /api/v1/" lands without amending this ADR.
- A breaking change (per §2.2) is either avoided (use sibling pattern) or shipped with a CHANGELOG entry explicitly tagged as breaking.

It fails if undocumented breaking changes ship, if the analyst/admin classification drifts ad-hoc, or if URL versioning shows up without an explicit ADR amendment.

## 6. Rollback

This ADR documents existing patterns; rolling it back means deleting the doc.

A specific decision that turns out wrong (e.g., we DO need URL versioning) requires amending §2.1 with the new rule, the trigger (which external integrator made it necessary), and a migration plan for existing clients. Don't quietly add `/api/v1/`.
