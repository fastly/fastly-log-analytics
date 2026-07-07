/**
 * MSW request handlers for vitest.
 *
 * Why MSW (vs per-test ``vi.mock('@/lib/api')``): MSW intercepts at the
 * fetch boundary, so the openapi-fetch client + its middleware (the
 * ``x-service-id`` header injection, the ``onResponse`` error-throwing
 * shim) all run for real. ``vi.mock`` short-circuits at the module
 * boundary and silently bypasses the middleware — which is exactly the
 * code a few of our bugs have lived in.
 *
 * The base URL must match what [lib/api.ts](../../lib/api.ts) computes
 * under jsdom: in jsdom, ``typeof window !== 'undefined'`` is true and
 * the helper returns ``${window.location.protocol}//127.0.0.1:8000``.
 * jsdom's default protocol is ``http:``.
 *
 * --- Extending these handlers (R-2) ---
 *
 * Defaults below are deliberately bland — empty arrays, zero counts,
 * stubbed timestamps. They exist so that flipping
 * ``server.listen({ onUnhandledRequest: 'error' })`` doesn't blow up
 * every test that doesn't care about a particular endpoint. When a
 * test needs richer data, override via ``server.use(http.get(...))``
 * at the top of the test — that takes precedence over the default
 * for the duration of that test (resetHandlers fires in afterEach).
 *
 * If you add a new backend route, prefer adding a default here AND
 * a per-test override where the shape matters. Skipping the default
 * means any unrelated component that happens to call your new route
 * (e.g. via a shared layout effect) will fail in a confusing way.
 */

import { http, HttpResponse, type JsonBodyType } from 'msw'

const API_BASE = 'http://127.0.0.1:8000'

const ok = (body: JsonBodyType = {}) => () => HttpResponse.json(body)
const okArray = () => () => HttpResponse.json([])
const noContent = () => () => new HttpResponse(null, { status: 204 })

/**
 * Default handlers used by ``server.listen()``. Override per-test with
 * ``server.use(http.get(...))`` rather than redefining the default set.
 */
export const handlers = [
  // ── Bootstrap + service catalog ────────────────────────────────────
  http.get(`${API_BASE}/api/bootstrap`, () =>
    HttpResponse.json({
      services: [
        { service_id: 'svc-default', name: 'Default Service', access_level: 'read_write' },
      ],
      active_service_id: 'svc-default',
    }),
  ),

  http.get(`${API_BASE}/api/services`, () =>
    HttpResponse.json({
      services: [
        { service_id: 'svc-default', name: 'Default Service', access_level: 'read_write' },
      ],
    }),
  ),

  http.get(`${API_BASE}/api/health`, ok({ status: 'ok' })),
  http.get(`${API_BASE}/api/schema`, ok({ tables: [], custom_fields: [] })),

  // ── Log-fields catalog (gates every analytics page) ───────────────
  http.get(`${API_BASE}/api/log-fields/catalog`, () =>
    HttpResponse.json({ fields: [], custom_fields: [], version: 'test' }),
  ),
  http.get(`${API_BASE}/api/log-extents`, () =>
    HttpResponse.json({ ts_min: null, ts_max: null, count: 0 }),
  ),
  http.get(`${API_BASE}/api/insight-availability`, () =>
    HttpResponse.json({ available: false, ts_min: null, ts_max: null }),
  ),
  http.get(`${API_BASE}/api/insight-availability/:service_id`, () =>
    HttpResponse.json({ available: false, ts_min: null, ts_max: null }),
  ),

  // ── Per-service introspection ─────────────────────────────────────
  http.get(`${API_BASE}/api/services/:service_id/credentials`, () =>
    HttpResponse.json({ credentials: [] }),
  ),
  http.get(`${API_BASE}/api/services/:service_id/logging-settings`, () =>
    HttpResponse.json({ logging_settings: { sources: [] } }),
  ),
  http.get(`${API_BASE}/api/services/:service_id/custom-fields`, () =>
    HttpResponse.json({ fields: [] }),
  ),
  http.post(`${API_BASE}/api/services/:service_id/custom-fields/import`, () =>
    HttpResponse.json({ imported: [], skipped: [] }),
  ),
  http.post(`${API_BASE}/api/services/:service_id/custom-fields/validate-vcl`, () =>
    HttpResponse.json({ valid: true, errors: [] }),
  ),
  http.put(`${API_BASE}/api/services/:service_id/custom-fields/:field_name`, () =>
    HttpResponse.json({ ok: true }),
  ),
  http.delete(`${API_BASE}/api/services/:service_id/custom-fields/:field_name`, noContent()),
  http.post(`${API_BASE}/api/services/:service_id/generate-viewer-key`, () =>
    HttpResponse.json({ passcode: 'TEST-0000-0000', expires_at: '2030-01-01T00:00:00Z' }),
  ),

  // ── Session scoring composite endpoints ────────────────────────────
  // The admin/session-scoring page fetches these two composite endpoints
  // on mount. Without these defaults the page would either spin forever
  // on the loading skeleton (queries stay pending) or trip the
  // onUnhandledRequest='error' guard. Empty-shape responses mirror what
  // a brand-new service returns before any scoring runs have completed
  // — every consumer treats the shape as nullable already.
  http.get(`${API_BASE}/api/services/:service_id/scoring/analytics`, () =>
    HttpResponse.json({
      health: null,
      top_flagged: { rows: [] },
      score_distribution: { buckets: [] },
      compliance_breakdown: { categories: [] },
      evaluation_per_reason: { rows: [] },
      evaluation: null,
    }),
  ),
  http.get(`${API_BASE}/api/services/:service_id/scoring/config`, () =>
    HttpResponse.json({
      status: { enabled: false },
      threshold: { value: 50 },
      exclude_regex: { pattern: '' },
      enforce_status_code: { code: 403 },
      matrix_versions: { versions: [] },
    }),
  ),
  // ScoringHealthCard / ScorerFailOpenBreakdownCard fetch this directly
  // (NOT via the analytics composite), so it needs its own default — without
  // it the page trips the onUnhandledRequest='error' guard. Empty-shape
  // response mirrors a service with no scoring runs yet.
  http.get(`${API_BASE}/api/services/:service_id/scoring/health`, () =>
    HttpResponse.json({
      since_hours: 24,
      total_edge_rows: 0,
      scored_rows: 0,
      fire_rate_pct: 0,
      distinct_sids: 0,
      avg_score: 0,
      p50_score: 0,
      p95_score: 0,
      max_score: 0,
      scorer_errors: 0,
      top_reasons: [],
      l2_enforce: null,
    }),
  ),
  // L2EnforcementCard fetches this directly. Default mirrors a freshly-enabled
  // service that has NOT opted L2 into enforcement (observe-only, not ready).
  http.get(`${API_BASE}/api/services/:service_id/scoring/l2-enforce`, () =>
    HttpResponse.json({
      available: true,
      enabled: false,
      l2_enabled_at: null,
      days_since_optin: null,
      ramp_progress: 0,
      fully_ramped: false,
      warmup_days_remaining: null,
      scoring_enabled_at: null,
      deployment_age_days: 0,
      ready: false,
      ramp_days: 3,
      readiness_days: 7,
    }),
  ),

  // ── Dashboard / analytics aggregates (default: empty results) ─────
  http.post(`${API_BASE}/api/dashboard/aggregates`, () =>
    HttpResponse.json({ rows: [], columns: [], total: 0 }),
  ),
  http.post(`${API_BASE}/api/insights`, () =>
    HttpResponse.json({ insights: [], next_cursor: null }),
  ),
  http.post(`${API_BASE}/api/insights/cache-collapse-detail`, () =>
    HttpResponse.json({
      url: '',
      timeline: [],
      recent_misses: [],
      breakdown: { hits: 0, misses: 0, passes: 0, other: 0 },
      baseline_hit_rate: 0,
      window_hit_rate: 0,
      baseline_pass_rate: 0,
      window_pass_rate: 0,
    }),
  ),
  http.post(`${API_BASE}/api/security/aggregates`, () =>
    HttpResponse.json({ rows: [], columns: [], total: 0 }),
  ),
  http.post(`${API_BASE}/api/security/top-bots`, () =>
    HttpResponse.json({ rows: [], total: 0 }),
  ),
  http.post(`${API_BASE}/api/performance/aggregates`, () =>
    HttpResponse.json({ rows: [], columns: [], total: 0 }),
  ),
  http.post(`${API_BASE}/api/network-health`, () =>
    HttpResponse.json({ rows: [], total: 0 }),
  ),
  http.post(`${API_BASE}/api/usage/bandwidth`, () =>
    HttpResponse.json({ rows: [], total_bytes: 0 }),
  ),
  http.post(`${API_BASE}/api/usage/log-activity`, () =>
    HttpResponse.json({ rows: [], total: 0 }),
  ),
  http.post(`${API_BASE}/api/usage/operations`, () =>
    HttpResponse.json({ rows: [], total: 0 }),
  ),
  http.get(`${API_BASE}/api/usage/prefill`, () =>
    HttpResponse.json({ rows: [], total: 0 }),
  ),
  http.get(`${API_BASE}/api/usage/prefill/rates`, () =>
    HttpResponse.json({ rows: [], total: 0 }),
  ),
  http.get(`${API_BASE}/api/usage/current-storage`, () =>
    HttpResponse.json({ bytes: 0, files: 0 }),
  ),

  // ── Sessions ──────────────────────────────────────────────────────
  http.get(`${API_BASE}/api/sessions`, () =>
    HttpResponse.json({ rows: [], total: 0 }),
  ),
  http.get(`${API_BASE}/api/sessions/detail`, () =>
    HttpResponse.json({ session_id: null, events: [] }),
  ),

  // ── Alerts ────────────────────────────────────────────────────────
  http.get(`${API_BASE}/api/alerts/`, () => HttpResponse.json({ alerts: [] })),
  http.get(`${API_BASE}/api/alerts/:service_id`, () => HttpResponse.json({ alerts: [] })),
  http.post(`${API_BASE}/api/alerts/`, () =>
    HttpResponse.json({ alert_id: 'alert-test', ok: true }),
  ),
  http.post(`${API_BASE}/api/alerts/preview`, () =>
    HttpResponse.json({ rows: [], total: 0 }),
  ),
  http.delete(`${API_BASE}/api/alerts/:alert_id`, noContent()),
  http.patch(`${API_BASE}/api/alerts/:alert_id/enabled`, ok({ ok: true })),

  // ── Saved views ───────────────────────────────────────────────────
  http.get(`${API_BASE}/api/views/`, () => HttpResponse.json({ views: [] })),
  http.get(`${API_BASE}/api/views/:service_id`, () => HttpResponse.json({ views: [] })),
  http.get(`${API_BASE}/api/views/:view_id`, () =>
    HttpResponse.json({ view_id: 'view-test', name: 'Test View', state: {} }),
  ),
  http.post(`${API_BASE}/api/views/`, () =>
    HttpResponse.json({ view_id: 'view-test', ok: true }),
  ),
  http.delete(`${API_BASE}/api/views/:view_id`, noContent()),

  // ── Provisioning ──────────────────────────────────────────────────
  http.post(`${API_BASE}/api/provision/validate`, () =>
    HttpResponse.json({ valid: true, errors: [] }),
  ),
  http.get(`${API_BASE}/api/provision/check-config`, () =>
    HttpResponse.json({ exists: false, service_id: null }),
  ),
  http.get(`${API_BASE}/api/provision/check-domain`, () =>
    HttpResponse.json({ available: true }),
  ),
  http.post(`${API_BASE}/api/provision/check-fos`, () =>
    HttpResponse.json({ ok: true, bucket: 'test-bucket' }),
  ),
  http.post(`${API_BASE}/api/provision/ingest`, () =>
    HttpResponse.json({ ok: true, ingested: 0 }),
  ),
  http.post(`${API_BASE}/api/provision/lake-info`, () =>
    HttpResponse.json({ ok: true, table: 'logs' }),
  ),
  http.post(`${API_BASE}/api/provision/execute`, () =>
    HttpResponse.json({ status: 'completed', service_id: 'svc-default' }),
  ),
  http.get(`${API_BASE}/api/provision/services`, () =>
    HttpResponse.json({ services: [] }),
  ),
  http.get(`${API_BASE}/api/provision/ngwaf-workspaces`, () =>
    HttpResponse.json({ workspaces: [] }),
  ),
  http.post(`${API_BASE}/api/provision/services/:service_id/ngwaf-workspace`, () =>
    HttpResponse.json({ ok: true }),
  ),
  http.post(`${API_BASE}/api/provision/terraform/export`, () =>
    HttpResponse.json({ tf: '# generated' }),
  ),
  http.post(`${API_BASE}/api/provision/terraform/preview`, () =>
    HttpResponse.json({ tf: '# preview' }),
  ),

  // ── Admin ─────────────────────────────────────────────────────────
  http.get(`${API_BASE}/api/admin/bot-sources`, () =>
    // BotSourcesPanel reads data.rdns.{total,pending}; without rdns
    // present the optional-chain-less reads crash the panel under tests.
    HttpResponse.json({ sources: [], rdns: { total: 0, pending: 0 } }),
  ),
  http.post(`${API_BASE}/api/admin/bot-sources/:source_id/refresh`, ok({ ok: true })),
  http.post(`${API_BASE}/api/admin/bot-sources/rdns/backfill`, ok({ enqueued: 0 })),
  http.post(`${API_BASE}/api/admin/bot-sources/rdns/enrich`, ok({ enriched: 0 })),
  http.get(`${API_BASE}/api/admin/system-jobs`, () => HttpResponse.json({ jobs: [] })),
  http.get(`${API_BASE}/api/admin/usage-logging`, () =>
    // GlobalSettings reads .enabled AND .retention_days.
    HttpResponse.json({ enabled: false, retention_days: 30 }),
  ),
  http.patch(`${API_BASE}/api/admin/usage-logging`, ok({ ok: true })),
  http.post(`${API_BASE}/api/admin/commit-iceberg`, ok({ ok: true })),
  http.post(`${API_BASE}/api/admin/ingest-logs`, ok({ ok: true, ingested: 0 })),
  http.get(`${API_BASE}/api/admin/iceberg-info`, () =>
    HttpResponse.json({ snapshots: [], current_snapshot_id: null }),
  ),
  http.get(`${API_BASE}/api/admin/iceberg-calendar`, () =>
    HttpResponse.json({ days: [] }),
  ),
  http.get(`${API_BASE}/api/admin/ingested-files`, () =>
    HttpResponse.json({ files: [], total: 0 }),
  ),
  http.post(`${API_BASE}/api/admin/metadata-cleanup`, ok({ ok: true, removed: 0 })),
  http.get(`${API_BASE}/api/admin/pop-locations`, () =>
    HttpResponse.json({ pops: [] }),
  ),
  http.post(`${API_BASE}/api/admin/pop-locations/refresh`, ok({ ok: true })),
  http.get(`${API_BASE}/api/admin/queries`, () => HttpResponse.json({ queries: [] })),
  http.get(`${API_BASE}/api/admin/raw-tree`, () =>
    HttpResponse.json({ tree: [], total: 0 }),
  ),
  http.get(`${API_BASE}/api/audit-logs`, () => HttpResponse.json({ logs: [] })),
  http.get(`${API_BASE}/api/cron-runs`, () => HttpResponse.json({ runs: [] })),
  http.get(`${API_BASE}/api/cron-schedule`, () =>
    HttpResponse.json({ schedule: [], next_run_at: null }),
  ),

  // ── AppLayout always-on calls (every page render hits these) ─────
  // sync-status: header lag indicator (useSyncStatus) — every admin
  // page fetches this on mount, so a missing default crashes the
  // suite under onUnhandledRequest: 'error'.
  http.get(`${API_BASE}/api/sync-status`, () =>
    HttpResponse.json({
      latest_log_at: null,
      local_rows: 0,
      fos_rows: null,
      last_sync_at: null,
    }),
  ),
  // share/banner: lean 80B response — useShareStatusBanner polls
  // every 15s when sharing is enabled, but mounts on every page load.
  http.get(`${API_BASE}/api/admin/share/banner`, () =>
    HttpResponse.json({ sharing_active: false, public_url: null }),
  ),
  // share/heartbeat: analyst-side keep-alive ping, mounted by AppLayout
  // when in analyst mode. Returns 204.
  http.post(`${API_BASE}/api/share/heartbeat`, noContent()),

  // ── Service-scoped CRUD (admin pages that don't vi.mock) ──────────
  http.get(`${API_BASE}/api/services/:service_id/log-fields`, () =>
    HttpResponse.json({ fields: [] }),
  ),
  http.post(`${API_BASE}/api/services/:service_id/log-fields`, ok({ ok: true })),
  http.patch(`${API_BASE}/api/services/:service_id/credentials`, ok({ ok: true })),
  http.get(`${API_BASE}/api/services/:service_id/lake-info`, () =>
    HttpResponse.json({ ok: true, table: 'logs' }),
  ),
  http.post(`${API_BASE}/api/services/:service_id/logging-settings/update`, ok({ ok: true })),
  // Bare custom-fields CREATE (the per-field PUT/DELETE already have
  // defaults above; the bare-resource POST was missing).
  http.post(`${API_BASE}/api/services/:service_id/custom-fields`, () =>
    HttpResponse.json({ ok: true, name: 'test-field' }),
  ),
  http.get(`${API_BASE}/api/services/:service_id/custom-fields/export`, () =>
    new HttpResponse('name,type,vcl\n', { headers: { 'content-type': 'text/csv' } }),
  ),

  // ── Analytics POSTs that page-level tests routinely override ──────
  http.post(`${API_BASE}/api/dashboard/bundle`, () =>
    HttpResponse.json({ rows: [], total: 0 }),
  ),
  http.post(`${API_BASE}/api/dashboard/field-values`, () =>
    HttpResponse.json({ values: [] }),
  ),
  http.post(`${API_BASE}/api/origin/aggregates`, () =>
    HttpResponse.json({ rows: [], columns: [], total: 0 }),
  ),
  http.post(`${API_BASE}/api/query`, () =>
    HttpResponse.json({ rows: [], columns: [] }),
  ),
  http.get(`${API_BASE}/api/presets`, okArray()),

  // ── Admin-page endpoint defaults ──────────────────────────────────
  http.get(`${API_BASE}/api/admin/health-snapshot`, ok({ ok: true })),
  http.get(`${API_BASE}/api/admin/metadata-storage`, () =>
    HttpResponse.json({ bytes: 0, tables: [] }),
  ),
  http.patch(`${API_BASE}/api/admin/metadata-retention`, ok({ ok: true })),
  http.get(`${API_BASE}/api/admin/log-accounting`, () =>
    HttpResponse.json({ entries: [] }),
  ),
  http.get(`${API_BASE}/api/admin/metric-history/batch`, () =>
    HttpResponse.json({ series: [] }),
  ),
  http.post(`${API_BASE}/api/admin/rebuild-local-view`, ok({ ok: true })),
  http.get(`${API_BASE}/api/admin/usage-log`, () =>
    HttpResponse.json({ rows: [] }),
  ),
  http.delete(`${API_BASE}/api/admin/usage-log`, noContent()),
  http.get(`${API_BASE}/api/admin/queries/summary`, () =>
    HttpResponse.json({ total: 0, active: 0 }),
  ),
  http.get(`${API_BASE}/api/admin/queries/:qid`, () =>
    HttpResponse.json({ query_id: 'q-test', status: 'completed' }),
  ),
  http.post(`${API_BASE}/api/admin/queries/:qid/cancel`, ok({ ok: true })),
  http.get(`${API_BASE}/api/admin/app-config/query-monitor`, () =>
    HttpResponse.json({ config: {} }),
  ),
  http.get(`${API_BASE}/api/debug/recent-sqlite`, () =>
    HttpResponse.json({ rows: [] }),
  ),
  http.post(`${API_BASE}/api/debug/clear-sqlite`, ok({ cleared: 0 })),
  http.delete(`${API_BASE}/api/cron-runs`, noContent()),

  // ── Share-admin family ────────────────────────────────────────────
  http.get(`${API_BASE}/api/admin/share/status`, () =>
    HttpResponse.json({ active: false, sessions: [] }),
  ),
  http.get(`${API_BASE}/api/admin/share/live`, () =>
    HttpResponse.json({ sessions: [] }),
  ),
  http.get(`${API_BASE}/api/admin/share/audit-logs`, () =>
    HttpResponse.json({ logs: [] }),
  ),
  http.post(`${API_BASE}/api/admin/share/start`, ok({ ok: true })),
  http.post(`${API_BASE}/api/admin/share/stop`, ok({ ok: true })),
  http.post(`${API_BASE}/api/admin/share/panic`, ok({ ok: true })),
  http.get(`${API_BASE}/api/admin/share/wordphrase`, () =>
    HttpResponse.json({ phrase: 'test-phrase' }),
  ),
  http.post(`${API_BASE}/api/admin/share/invites`, () =>
    HttpResponse.json({ invite_id: 'inv-test', passcode: 'TEST' }),
  ),
  http.delete(`${API_BASE}/api/admin/share/invites/:invite_id`, noContent()),
  http.patch(`${API_BASE}/api/admin/share/invites/:invite_id/passcode`, ok({ ok: true })),
  http.patch(`${API_BASE}/api/admin/share/invites/:invite_id/services`, ok({ ok: true })),
  http.patch(`${API_BASE}/api/admin/share/invites/:invite_id/pii`, ok({ ok: true })),
  http.patch(`${API_BASE}/api/admin/share/invites/:invite_id/sharing`, ok({ ok: true })),
  http.post(`${API_BASE}/api/admin/share/sessions/:session_id/boot`, ok({ ok: true })),

  // ── Share-login (analyst auth) ────────────────────────────────────
  // Default auth-config: passcode-only (the OAuth feature is off unless a test
  // overrides this). ShareLoginForm fetches this on mount to decide what to render.
  http.get('/api/share/auth-config', () =>
    HttpResponse.json({ passcode_enabled: true, providers: [] }),
  ),
  http.get('/api/admin/share/oauth-providers', () =>
    HttpResponse.json({ providers: [] }),
  ),
  // OAuth handshake routes are top-level browser navigations (302), never
  // fetched by the typed client — stub them so the coverage guard is satisfied.
  http.get('/api/share/oauth/authorize', () =>
    new HttpResponse(null, { status: 302, headers: { Location: '/idp-stub' } }),
  ),
  http.get('/api/share/oauth/callback', () =>
    new HttpResponse(null, { status: 302, headers: { Location: '/dashboard' } }),
  ),
  http.get('/api/share/tos', () =>
    HttpResponse.json({ version: 'v1', text: 'Test TOS' }),
  ),
  http.post('/api/share/login', () =>
    HttpResponse.json({ ok: true, must_acknowledge: false }),
  ),
  http.post('/api/share/acknowledge', () => HttpResponse.json({ ok: true })),
  http.post(`${API_BASE}/api/login`, () => HttpResponse.json({ ok: true })),
  http.get(`${API_BASE}/api/auth`, () =>
    HttpResponse.json({ authenticated: true, role: 'admin' }),
  ),

  // ── High-traffic analytics POSTs (audit follow-up R-9c) ──────────
  // Bland empty-shape defaults. Page tests that need richer data
  // override per-test via server.use(http.post(...)); these defaults
  // are here so an unrelated component effect calling one of these
  // endpoints doesn't trip the onUnhandledRequest='error' guard.
  http.post(`${API_BASE}/api/dashboard/raw/csv`, () =>
    new HttpResponse('', { status: 200, headers: { 'Content-Type': 'text/csv' } }),
  ),
  http.post(`${API_BASE}/api/sessions`, () =>
    HttpResponse.json({ sessions: [], total: 0 }),
  ),
  http.post(`${API_BASE}/api/sessions/detail`, () =>
    HttpResponse.json({ events: [], detail: null }),
  ),
  http.post(`${API_BASE}/api/network-quality`, () =>
    HttpResponse.json({ buckets: [], pop_breakdown: [] }),
  ),
  http.post(`${API_BASE}/api/origin/summary`, () =>
    HttpResponse.json({ stages: [], totals: {} }),
  ),
  http.post(`${API_BASE}/api/origin/timeseries`, () =>
    HttpResponse.json({ series: [], has_data: false }),
  ),
  http.post(`${API_BASE}/api/origin/status-codes`, () =>
    HttpResponse.json({ rows: [] }),
  ),
  http.post(`${API_BASE}/api/origin/slow-urls`, () =>
    HttpResponse.json({ rows: [] }),
  ),
  http.post(`${API_BASE}/api/origin/path-breakdown`, () =>
    HttpResponse.json({ rows: [] }),
  ),
  http.post(`${API_BASE}/api/origin/pop-latency`, () =>
    HttpResponse.json({ rows: [] }),
  ),
  http.post(`${API_BASE}/api/origin/ip-health`, () =>
    HttpResponse.json({ rows: [] }),
  ),
  http.post(`${API_BASE}/api/origin/shielding-analysis`, () =>
    HttpResponse.json({ shielding: [], origin: [] }),
  ),
  http.post(`${API_BASE}/api/performance/origin-ts`, () =>
    HttpResponse.json({ series: [], has_data: false }),
  ),

  // ── Client telemetry collectors ───────────────────────────────────
  // Mounted globally by the SPA: web-vitals reports on pagehide /
  // navigation; ux-events fires on column-reorder and similar SPA
  // interactions. Both return ``{ok: true}`` server-side. Bland
  // handlers here so a test that happens to trigger a page-hide or a
  // DataTable reorder doesn't trip the onUnhandledRequest='error'
  // guard.
  http.post(`${API_BASE}/api/web-vitals`, ok({ ok: true })),
  http.post(`${API_BASE}/api/ux-events`, ok({ ok: true })),

]
