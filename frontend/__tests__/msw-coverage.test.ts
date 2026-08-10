/**
 * MSW-vs-OpenAPI coverage gate.
 *
 * MSW is configured with ``onUnhandledRequest: 'error'`` (vitest.setup.ts),
 * which makes any unhandled fetch in a vitest test fail loudly. The
 * audit finding this test pins: that guarantee is INERT for any page
 * that has no test, because the unhandled request is never made. A
 * page can ship referencing 3 new endpoints, lose two of them, and
 * the MSW safety net doesn't trip because the page has no integration
 * coverage.
 *
 * This test closes the gap by going the other direction: load
 * ``frontend/openapi.json`` (regenerated from the FastAPI router on
 * every commit by the regen-openapi pre-commit hook), normalise the
 * path templates to MSW's ``:param`` shape, and assert each path is
 * covered by an MSW handler OR is explicitly listed in ALLOWED_GAPS
 * with a reason.
 *
 * When this fails, fix the test by EITHER adding a handler in
 * frontend/tests/msw/handlers.ts (preferred — the new endpoint is now
 * mockable from every test) OR by adding the path to ALLOWED_GAPS
 * with a short reason (acceptable when the endpoint is admin-only and
 * exercised only via E2E, or when it's truly server-side).
 */
import * as fs from 'node:fs'
import * as path from 'node:path'
import { describe, expect, it } from 'vitest'

import { handlers } from '../tests/msw/handlers'

// Paths that intentionally have NO MSW handler.
//
// Baseline established 2026-06-16: every (path × method) in this set
// existed in openapi.json at the time the gate was added but had no
// MSW handler — they're covered by Playwright E2E, the admin-only
// triage surface, or are streaming endpoints (SSE) that MSW doesn't
// model well. The gate's value is that any NEW endpoint added after
// the baseline date fails this test unless someone either (a) adds a
// default handler in handlers.ts, or (b) explicitly opts it into this
// set with a reason.
//
// Burn-down: convert entries to real handlers whenever a vitest test
// touches a page that calls one — that's the moment when the handler
// is genuinely useful and the entry stops being a placeholder.
const ALLOWED_GAPS = new Set<string>([
  // (Heavy analytics POST endpoints converted to real bland-shape
  // handlers in tests/msw/handlers.ts per audit R-9c. Per-test overrides
  // via server.use() still take precedence for tests needing richer
  // data — the burn-down hasn't changed those test bodies, just removed
  // the "no default at all" gap that made the onUnhandledRequest='error'
  // guard surprise unrelated components.)

  // Service-management mutations — admin-only flows covered by
  // Playwright E2E (frontend/e2e/*.spec.ts).
  'POST /api/services/:service_id/cron-settings',
  'DELETE /api/services/:service_id/time-range',
  'POST /api/services/:service_id/ngwaf-sync',
  'PATCH /api/services/:service_id/custom-fields/:field_name',
  'DELETE /api/cron-runs/:log_id',

  // SSE streams — MSW's HTTP intercept does not model SSE well; tests
  // that exercise these set up their own custom EventSource shims.
  'GET /api/cron-runs/:run_id/stream',
  'GET /api/log-extents/stream',
  'GET /api/admin/events/stream',

  // Usage / admin GETs — admin pages have per-test override handlers.
  'GET /api/usage/operations',
  'GET /api/usage/bandwidth',
  'GET /api/usage/log-activity',
  'GET /api/usage/rum-breakdown',
  'GET /api/admin/compaction-stats',
  'GET /api/admin/metric-history',
  'GET /api/admin/iceberg-tree',
  'GET /api/admin/usage-log/export',
  'GET /api/admin/slow-queries/count',
  'GET /api/admin/slow-queries',
  'GET /api/download-folder',
  'GET /api/download',
  'GET /api/download-all',

  // Admin mutation endpoints — Playwright E2E coverage.
  'GET /api/admin/vcl-health',
  'POST /api/provision/reconcile',
  'POST /api/admin/optimize-now',
  'POST /api/admin/local-compact-now',
  'POST /api/admin/backfill-window',
  'POST /api/admin/backfill-bundle-rollups',
  'POST /api/admin/usage-logging',
  'POST /api/provision/teardown',
  'PATCH /api/provision/services/:service_id/ngwaf-workspace',

  // Session-scoring sub-endpoints — the page hits the two composite
  // endpoints (scoring/analytics, scoring/config) which ARE in
  // handlers.ts; these individual sub-endpoints are only called from
  // component-level tests that supply their own handlers.
  'POST /api/services/:service_id/scoring/enable',
  'POST /api/services/:service_id/scoring/disable',
  'GET /api/services/:service_id/scoring/status',
  'GET /api/services/:service_id/scoring/labels',
  'POST /api/services/:service_id/scoring/labels',
  'PATCH /api/services/:service_id/scoring/labels/:label_id',
  'DELETE /api/services/:service_id/scoring/labels/:label_id',
  'GET /api/services/:service_id/scoring/top-flagged',
  'GET /api/services/:service_id/scoring/score-distribution',
  'GET /api/services/:service_id/scoring/latency-timeseries',
  'GET /api/services/:service_id/scoring/compliance-breakdown',
  // scoring/health now has a default handler in handlers.ts (the
  // ScoringHealthCard / ScorerFailOpenBreakdownCard fetch it directly, not
  // via the analytics composite), so it is no longer an allowed gap.
  'GET /api/services/:service_id/scoring/evaluation',
  'GET /api/services/:service_id/scoring/curves',
  'GET /api/services/:service_id/scoring/threshold-preview',
  'POST /api/services/:service_id/scoring/retrain',
  'GET /api/services/:service_id/scoring/sessions/:sid/events',
  'GET /api/services/:service_id/scoring/enforce-threshold',
  'PUT /api/services/:service_id/scoring/enforce-threshold',
  'GET /api/services/:service_id/scoring/exclude-regex',
  'PUT /api/services/:service_id/scoring/exclude-regex',
  'POST /api/services/:service_id/scoring/exclude-regex/validate',
  'GET /api/services/:service_id/scoring/enforce-status-code',
  'PUT /api/services/:service_id/scoring/enforce-status-code',
  // l2-enforce GET has a default handler in handlers.ts (L2EnforcementCard
  // fetches it directly); the PUT is a mutation, tested via per-test server.use.
  'PUT /api/services/:service_id/scoring/l2-enforce',
  'GET /api/services/:service_id/scoring/matrix-versions',
  'POST /api/services/:service_id/scoring/matrix-versions/:version/restore',
  'POST /api/services/:service_id/scoring/rotate-key',
  'GET /api/services/:service_id/scoring/audit',
  'GET /api/services/:service_id/scoring/threshold',
  'PUT /api/services/:service_id/scoring/threshold',
  'GET /api/services/:service_id/scoring/evaluation/per-reason',
  'GET /api/services/:service_id/scoring/dashboard',

  // Real User Monitoring (RUM) analytics & live-events are covered by
  // Playwright end-to-end tests or are polling endpoints.
  'GET /api/services/:service_id/rum/analytics',
  'GET /api/services/:service_id/rum/live-events',

  // CMCD admin — SSE-streamed enable/disable + status. Admin-only flows
  // covered by Playwright E2E; same pattern as scoring admin endpoints.
  'GET /api/services/:service_id/cmcd/status',
  'POST /api/services/:service_id/cmcd/enable',
  'POST /api/services/:service_id/cmcd/disable',

  // RUM admin — SSE-streamed enable/disable + status. Admin-only provisioning
  // flows covered by Playwright E2E; same pattern as scoring/cmcd endpoints.
  // Health/vitals/errors are Phase 3 stubs; beacon-health validates setup
  // (checks if beacons are arriving), exercised via admin tests.
  'GET /api/services/:service_id/rum/status',
  'POST /api/services/:service_id/rum/enable',
  'POST /api/services/:service_id/rum/disable',
  'GET /api/services/:service_id/rum/beacon-health',
  'GET /api/services/:service_id/rum/health',
  'GET /api/services/:service_id/rum/vitals',
  'GET /api/services/:service_id/rum/errors',
  // rum/versions has a real default handler (RumVersionPicker consumes it
  // directly). rum/upgrade is the not-yet-built admin "upgrade pinned
  // version" card — SSE-streamed, same pattern as rum/enable / rum/disable.
  'POST /api/services/:service_id/rum/upgrade',

  // Control Room — Phase 0 stubs. Tab GET returns canned data, mutation
  // endpoints are 501 placeholders, SSE is a heartbeat. Covered by
  // backend tests + Playwright E2E (e2e/control-room.spec.ts).
  'GET /api/services/:service_id/control-room/:tab',
  'POST /api/services/:service_id/control-room/mitigations',
  'POST /api/services/:service_id/control-room/rules',
  'POST /api/services/:service_id/control-room/allowlist',
  'POST /api/services/:service_id/control-room/big-red-button',
  'POST /api/services/:service_id/control-room/cost-governor',
  'GET /api/services/:service_id/control-room/wizard/state',
  'POST /api/services/:service_id/control-room/wizard/step',
  'GET /api/services/:service_id/realtime-stream',
  'GET /api/services/:service_id/log-field-audit',
  'POST /api/services/:service_id/control-room/correlate',

  // Debug + analyst-share — analyst tests use per-spec handlers,
  // debug/state is admin-only triage.
  'GET /api/debug/state',
  'POST /api/share/logout',
  'GET /api/share/heartbeat',
  'POST /api/share/claim/:token',
  'POST /api/admin/share/invites/:invite_id/revoke',
  'POST /api/admin/share/invites/:invite_id/claim-token',
  'POST /api/admin/share/backup/export',
  'POST /api/admin/share/backup/import',
  'POST /api/admin/share/gdpr/erase',
  'PATCH /api/admin/share/settings',
])

/** Normalise OpenAPI path templates (``{service_id}``) to MSW's ``:param`` shape. */
function openapiToMsw(p: string): string {
  return p.replace(/\{([^}]+)\}/g, ':$1')
}

/** Pull the handler's METHOD and path out of an MSW RequestHandler. */
function describeHandler(h: any): { method: string; pathname: string } | null {
  const info = h?.info
  if (!info) return null
  // RestHandler.info shape: { method: 'GET' | 'POST' | ..., path: string, ... }
  const method = String(info.method ?? '').toUpperCase()
  const raw = info.path
  if (!method || typeof raw !== 'string') return null
  // Handlers register against the absolute URL (``${API_BASE}/api/...``)
  // OR a relative path. Strip the origin if present.
  try {
    const u = new URL(raw)
    return { method, pathname: u.pathname }
  } catch {
    return { method, pathname: raw }
  }
}

describe('MSW handler coverage vs openapi.json', () => {
  it('every documented (path × method) has an MSW handler or an ALLOWED_GAPS entry', () => {
    const openapiPath = path.resolve(__dirname, '..', 'openapi.json')
    if (!fs.existsSync(openapiPath)) {
      throw new Error(
        `openapi.json not found at ${openapiPath}. Run 'npm run gen:types' (or commit, ` +
          `which triggers the regen-openapi pre-commit hook).`,
      )
    }
    const spec = JSON.parse(fs.readFileSync(openapiPath, 'utf-8')) as {
      paths: Record<string, Record<string, unknown>>
    }

    const covered = new Set<string>()
    for (const h of handlers) {
      const d = describeHandler(h)
      if (d) covered.add(`${d.method} ${d.pathname}`)
    }

    const documented: string[] = []
    for (const [p, methods] of Object.entries(spec.paths)) {
      for (const m of Object.keys(methods)) {
        const method = m.toUpperCase()
        // FastAPI exposes HEAD/OPTIONS implicitly; skip those — MSW
        // doesn't need to mock OPTIONS for CORS preflight in jsdom.
        if (method === 'HEAD' || method === 'OPTIONS') continue
        documented.push(`${method} ${openapiToMsw(p)}`)
      }
    }

    const missing = documented.filter(
      (entry) => !covered.has(entry) && !ALLOWED_GAPS.has(entry),
    )

    expect(missing, missing.length > 0 ? buildMissingMessage(missing) : 'all paths covered').toEqual([])
  })

  it('ALLOWED_GAPS does not list any path that DOES have a handler', () => {
    // Defense-in-depth: an ALLOWED_GAPS entry that's no longer needed
    // (because the handler exists) should be removed, not silently
    // shadowed.
    const covered = new Set<string>()
    for (const h of handlers) {
      const d = describeHandler(h)
      if (d) covered.add(`${d.method} ${d.pathname}`)
    }
    const stale = [...ALLOWED_GAPS].filter((entry) => covered.has(entry))
    expect(stale, stale.length > 0 ? `Remove these ALLOWED_GAPS entries: ${stale.join(', ')}` : '').toEqual([])
  })
})

function buildMissingMessage(missing: string[]): string {
  return [
    'OpenAPI documents these endpoints with no MSW handler and no ALLOWED_GAPS entry:',
    ...missing.map((e) => `  - ${e}`),
    '',
    'Fix by EITHER:',
    '  1. Add a handler to frontend/tests/msw/handlers.ts (preferred), OR',
    '  2. Add the entry to ALLOWED_GAPS in this test with a short reason.',
  ].join('\n')
}
