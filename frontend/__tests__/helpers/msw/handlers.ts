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

import type { paths } from '@/types/api.generated'

const API_BASE = 'http://127.0.0.1:8000'

const ok = (body: JsonBodyType = {}) => () => HttpResponse.json(body)

type EnvelopeKeys =
  | '_debug_queries'
  | '_debug_calls'
  | '_debug_sqlite'
  | '_debug_postgres'
  | '_is_cached'
  | '_section_timings'
type StripEnvelope<T> = T extends readonly unknown[] ? T : T extends object ? { [K in keyof T as K extends EnvelopeKeys ? never : K]: T[K] } : T
type OkBody<P extends keyof paths, M extends keyof paths[P]> = paths[P][M] extends {
  responses: { 200: { content: { 'application/json': infer T } } }
}
  ? StripEnvelope<T>
  : paths[P][M] extends { responses: { 202: { content: { 'application/json': infer T } } } }
    ? StripEnvelope<T>
    : never

/**
 * `ok()` with the body checked against the OpenAPI response schema, so a
 * handler whose payload drifts from the backend contract fails `tsc`.
 * `path` is the spec path (`{id}` style), not the MSW `:id` form. The
 * response-envelope debug fields are optional in practice and omitted.
 */
const okFor =
  <P extends keyof paths, M extends keyof paths[P]>(_path: P, _method: M, body: OkBody<P, M>) =>
  () =>
    HttpResponse.json(body as JsonBodyType)
const noContent = () => () => new HttpResponse(null, { status: 204 })

export const CMCD_FIXTURE = {
  available: true,
  has_data: true,
  overview: {
    active_sessions: 1234,
    peak_viewers: 321,
    avg_session_duration: 95,
    rebuffer_rate: 4.2,
    avg_bitrate: 3200,
    avg_buffer_length: 14500,
    median_throughput: 28000,
  },
  sessions_ts: [{ bucket: '2026-01-01T00:00:00Z', concurrent_sessions: 40, rebuffer_session_pct: 3.1, new_sessions: 12 }],
  buffer_health_ts: [{ bucket: '2026-01-01T00:00:00Z', p50_buffer: 12000, p95_buffer: 26000, starvation_rate: 2.5 }],
  bitrate_ts: [{ bucket: '2026-01-01T00:00:00Z', avg_bitrate: 3000, utilization_ratio: 0.5 }],
  throughput_ts: [{ bucket: '2026-01-01T00:00:00Z', p50: 28000, p95: 45000, p99: 49000 }],
  startup_ts: [{ bucket: '2026-01-01T00:00:00Z', startup_ratio: 8.5 }],
  top_content: [{ content_id: 'movie-001', session_count: 77, rebuffer_rate: 5, avg_bitrate: 3100, avg_buffer_length: 13000 }],
  rebuffer_by_country: [{ country: 'US', rebuffer_rate: 4, session_count: 600, median_throughput: 30000 }],
  rebuffer_by_asn: [{ asn: 7922, label: 'AS7922 Comcast', rebuffer_rate: 6, session_count: 200, avg_bitrate: 2900, avg_buffer_length: 12000 }],
  object_type_dist: [{ object_type: 'v', request_count: 900 }],
  streaming_format_dist: [{ streaming_format: 'h', session_count: 700 }],
  session_duration_dist: [{ duration_bucket: '1-5 min', session_count: 300 }],
}

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

  http.get(`${API_BASE}/api/health`, okFor('/api/health', 'get', { status: 'ok', version: 'test' })),
  http.get(`${API_BASE}/api/query/schema/:service_id`, () =>
    HttpResponse.json({ source: 'svc-default', table_name: 'logs', schema: [], columns: [] }),
  ),
  http.get(`${API_BASE}/api/admin/sync-status`, () =>
    HttpResponse.json({ configured: false, access_level: 'read_write', state: 'idle' }),
  ),
  http.get(`${API_BASE}/api/admin/health`, () =>
    HttpResponse.json({ status: 'ok', load: [0, 0, 0], recent_cron_failures: [] }),
  ),
  http.get(`${API_BASE}/api/admin/system-metrics`, () =>
    HttpResponse.json({ load: [0, 0, 0], vcpus: 1, in_flight_runs: [], compaction: {} }),
  ),
  http.post(`${API_BASE}/api/admin/commit/:service_id`, () =>
    HttpResponse.json({ type: 'done', message: 'ok' }),
  ),
  http.get(`${API_BASE}/api/admin/log-accounting/:service_id`, () =>
    HttpResponse.json({
      configured: true,
      hours: 24,
      buckets: [],
      totals: { requests: 0, log_lines: 0, log_bytes: 0, ingested_rows: 0, gap: 0, gap_pct: 0 },
      sustained_loss: null,
    }),
  ),
  http.get(`${API_BASE}/api/admin/high-scale/retention-recommendation`, () =>
    HttpResponse.json({
      service_id: 'svc-default',
      recommended_hot_days: 1,
      recommended_warm_days: 30,
      reasoning: 'default',
      volume_total_bytes: 1_000_000_000,
      volume_total_source: 'request',
      explicit_retention_days: null,
      effective_retention_days: 30,
    }),
  ),
  http.delete(`${API_BASE}/api/admin/slow-queries`, () =>
    HttpResponse.json({ deleted: 0, service_id: 'svc-default' }),
  ),
  http.post(`${API_BASE}/api/high-scale/services/:service_id/request-facts`, () =>
    HttpResponse.json({
      rows: [],
      metadata: {
        status: 'complete',
        exact: true,
        coverage: 1,
        freshness_lag_seconds: 0,
        watermark: {
          service_id: 'svc-default',
          domain: 'request',
          owner_epoch: 1,
          coverage_start: null,
          coverage_end: null,
          last_accepted_cursor: null,
          last_archived_event_id: null,
          last_visible_event_id: null,
          exact: true,
        },
        approximation_error: null,
        error: null,
      },
      next_cursor: null,
    }),
  ),
  http.post(`${API_BASE}/api/high-scale/services/:service_id/rum-facts/:domain`, () =>
    HttpResponse.json({
      rows: [],
      metadata: {
        status: 'complete',
        exact: true,
        coverage: 1,
        freshness_lag_seconds: 0,
        watermark: {
          service_id: 'svc-default',
          domain: 'rum_vitals',
          owner_epoch: 1,
          coverage_start: null,
          coverage_end: null,
          last_accepted_cursor: null,
          last_archived_event_id: null,
          last_visible_event_id: null,
          exact: true,
        },
        approximation_error: null,
        error: null,
      },
      next_cursor: null,
    }),
  ),
  http.post(`${API_BASE}/api/high-scale/services/:service_id/cmcd-facts`, () =>
    HttpResponse.json({
      rows: [],
      metadata: {
        status: 'complete',
        exact: true,
        coverage: 1,
        freshness_lag_seconds: 0,
        watermark: {
          service_id: 'svc-default',
          domain: 'cmcd',
          owner_epoch: 1,
          coverage_start: null,
          coverage_end: null,
          last_accepted_cursor: null,
          last_archived_event_id: null,
          last_visible_event_id: null,
          exact: true,
        },
        approximation_error: null,
        error: null,
      },
      next_cursor: null,
    }),
  ),
  http.post(`${API_BASE}/api/high-scale/services/:service_id/aggregates`, () =>
    HttpResponse.json({
      service_id: 'svc-default',
      domain: 'request',
      dimension: 'url',
      request_count: 0,
      top_values: [],
      coverage: 1,
      freshness_lag_seconds: 0,
      exact: true,
      approximation_error: null,
    }),
  ),
  http.post(`${API_BASE}/api/high-scale/services/:service_id/queries`, () =>
    HttpResponse.json({
      service_id: 'svc-default',
      domain: 'request',
      plan: {
        tier: 'recent_facts',
        sources: ['clickhouse_facts'],
        queued: false,
        bounded: false,
        max_rows: 500,
        reason: 'recent raw facts',
      },
      rows: [],
      next_cursor: null,
      metadata: {
        status: 'complete',
        exact: true,
        coverage: 1,
        freshness_lag_seconds: 0,
        watermark: {
          service_id: 'svc-default',
          domain: 'request',
          owner_epoch: 1,
          coverage_start: null,
          coverage_end: null,
          last_accepted_cursor: null,
          last_archived_event_id: null,
          last_visible_event_id: null,
          exact: true,
        },
        approximation_error: null,
        error: null,
      },
      job_id: null,
    }),
  ),
  http.get(`${API_BASE}/api/high-scale/services/:service_id/queries/:job_id`, () =>
    HttpResponse.json({
      job_id: 'job-default',
      state: 'completed',
      rows: [],
      row_count: 0,
      output_bytes: 0,
      truncated: false,
      error: null,
      created_at: '2026-01-01T00:00:00Z',
      expires_at: '2026-01-01T00:15:00Z',
      terminal_at: '2026-01-01T00:00:01Z',
    }),
  ),
  http.post(`${API_BASE}/api/high-scale/services/:service_id/queries/:job_id/cancel`, () =>
    HttpResponse.json({ job_id: 'job-default', cancelled: true }),
  ),
  http.post(`${API_BASE}/api/high-scale/services/:service_id/exports`, () =>
    HttpResponse.json({ export_id: 'export-default', service_id: 'svc-default', status: 'queued' }),
  ),
  http.get(`${API_BASE}/api/high-scale/services/:service_id/exports/:export_id`, () =>
    HttpResponse.json({ export_id: 'export-default', service_id: 'svc-default', status: 'queued' }),
  ),
  http.post(`${API_BASE}/api/high-scale/services/:service_id/exports/:export_id/cancel`, () =>
    HttpResponse.json({ export_id: 'export-default', service_id: 'svc-default', status: 'cancelled', cancelled: true }),
  ),
  http.get(`${API_BASE}/api/admin/clickhouse/status`, ({ request }) =>
    HttpResponse.json({
      service_id: new URL(request.url).searchParams.get('service_id') ?? 'svc-default',
      enabled: false,
      health: 'disabled',
      schema_version: null,
      publication_counts: null,
      oldest_pending_age_seconds: null,
      last_published_at: null,
      active_generation: null,
      expired_dataset_count: null,
    }),
  ),
  http.post(`${API_BASE}/api/admin/clickhouse/replay`, () =>
    HttpResponse.json(
      { detail: { error: 'clickhouse_disabled', message: 'ClickHouse prototype disabled' } },
      { status: 409 },
    ),
  ),
  http.get(`${API_BASE}/api/schema`, okFor('/api/schema', 'get', { schema: [], table_name: 'logs' })),

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
  // RumVersionPicker (ProvisionWizard StorageStep) fetches this directly.
  // Default mirrors a fresh, never-provisioned service: nothing pinned yet.
  // Per-test overrides via server.use() cover the 503/empty-list branches.
  http.get(`${API_BASE}/api/services/:service_id/rum/versions`, () =>
    HttpResponse.json({
      available: [],
      current: null,
      latest: null,
      update_available: false,
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
  http.post(`${API_BASE}/api/security/proxies`, () =>
    HttpResponse.json({
      active_proxies_count: 0,
      tunnel_requests_count: 0,
      distance_mismatches_count: 0,
      traffic_quality: [],
      suspicious_isps: [],
      active_clients: [],
    }),
  ),
  http.post(`${API_BASE}/api/security/bundle`, () =>
    HttpResponse.json({ tls_fingerprints: [], bots: [], ngwaf_configured: false }),
  ),
  http.get(`${API_BASE}/api/security/bundle`, () =>
    HttpResponse.json({ tls_fingerprints: [], bots: [], ngwaf_configured: false }),
  ),
  http.post(`${API_BASE}/api/performance/aggregates`, () =>
    HttpResponse.json({ rows: [], columns: [], total: 0 }),
  ),
  http.post(`${API_BASE}/api/network-health`, () =>
    HttpResponse.json({ rows: [], total: 0 }),
  ),
  http.post(`${API_BASE}/api/network/bundle`, () =>
    HttpResponse.json({ available: false, health: {}, quality: {}, pop_health: [], transport: {} }),
  ),
  http.get(`${API_BASE}/api/network/bundle`, () =>
    HttpResponse.json({ available: false, health: {}, quality: {}, pop_health: [], transport: {} }),
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
    HttpResponse.json({
      live_bytes: 0, live_files: 0, deleted_bytes: 0, quarantine_bytes: 0,
      total_billed_bytes: 0, total_billed_gb_hours: 0, total_files: 0, total_bytes: 0,
      start: '', end: '',
    }),
  ),

  // ── Sessions ──────────────────────────────────────────────────────
  http.get(`${API_BASE}/api/sessions`, () =>
    HttpResponse.json({ rows: [], total: 0 }),
  ),
  http.get(`${API_BASE}/api/sessions/detail`, () =>
    HttpResponse.json({ session_id: null, events: [] }),
  ),

  // ── Alerts ────────────────────────────────────────────────────────
  http.get(`${API_BASE}/api/alerts/`, () => HttpResponse.json({ data: [] })),
  http.get(`${API_BASE}/api/alerts/:service_id`, () => HttpResponse.json({ data: [] })),
  http.post(`${API_BASE}/api/alerts/`, () =>
    HttpResponse.json({ data: { id: 'alert-test', status: 'saved' } }),
  ),
  http.put(`${API_BASE}/api/alerts/:alert_id`, ({ params }) =>
    HttpResponse.json({ data: { id: String(params.alert_id), status: 'updated' } }),
  ),
  http.post(`${API_BASE}/api/alerts/evaluate`, () =>
    HttpResponse.json({ data: { service_id: 'svc-default', evaluated: 0, triggered: 0, results: [] } }),
  ),
  http.post(`${API_BASE}/api/alerts/preview`, () =>
    HttpResponse.json({ data: { series: [], threshold: 0, operator: '>', metric: '5xx_rate', window_min: 5 } }),
  ),
  http.post(`${API_BASE}/api/alerts/:alert_id/test`, ({ params }) =>
    HttpResponse.json({
      data: {
        alert_id: String(params.alert_id),
        service_id: 'svc-default',
        status: 'validated',
        targets_tested: 1,
        channel_type: 'webhook',
      },
    }),
  ),
  http.post(`${API_BASE}/api/alerts/:service_id/:alert_id/test`, ({ params }) =>
    HttpResponse.json({
      data: {
        alert_id: String(params.alert_id),
        service_id: String(params.service_id),
        status: 'validated',
        targets_tested: 1,
        channel_type: 'webhook',
      },
    }),
  ),
  http.delete(`${API_BASE}/api/alerts/:alert_id`, () =>
    HttpResponse.json({ data: { id: 'alert-test', status: 'deleted' } }),
  ),
  http.patch(`${API_BASE}/api/alerts/:alert_id/enabled`, ({ params }) =>
    HttpResponse.json({ data: { id: String(params.alert_id), status: 'success' } }),
  ),

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
    HttpResponse.json([]),
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
  http.post(`${API_BASE}/api/admin/bot-sources/:source_id/refresh`, okFor('/api/admin/bot-sources/{source_id}/refresh', 'post', { ok: true, source: {} })),
  http.post(`${API_BASE}/api/admin/bot-sources/refresh`, okFor('/api/admin/bot-sources/refresh', 'post', { ok: true, sources: [], updated_count: 0, failed_count: 0 })),
  http.post(`${API_BASE}/api/admin/bots/refresh`, okFor('/api/admin/bots/refresh', 'post', { ok: true, sources: [], updated_count: 0, failed_count: 0 })),
  // The two rDNS routes below are called by BotSourcesPanel but are absent from
  // the OpenAPI spec (and the backend), so they cannot be spec-typed.
  http.post(`${API_BASE}/api/admin/bot-sources/rdns/backfill`, ok({ enqueued: 0 })),
  http.post(`${API_BASE}/api/admin/bot-sources/rdns/enrich`, ok({ enriched: 0 })),
  http.post(`${API_BASE}/api/admin/rdns/enrich`, okFor('/api/admin/rdns/enrich', 'post', { ok: true, summary: { resolved: 0, errors: 0, discovered: 0, batch_limit: 0 } })),
  http.get(`${API_BASE}/api/admin/rdns/stats`, okFor('/api/admin/rdns/stats', 'get', { ok: true, stats: {} })),
  http.get(`${API_BASE}/api/admin/system-jobs`, () => HttpResponse.json({ jobs: [] })),
  http.get(`${API_BASE}/api/admin/usage-logging`, () =>
    // GlobalSettings reads .enabled AND .retention_days.
    HttpResponse.json({ enabled: false, retention_days: 30 }),
  ),
  http.patch(`${API_BASE}/api/admin/usage-logging`, okFor('/api/admin/usage-logging', 'patch', { ok: true })),
  http.post(`${API_BASE}/api/admin/commit-iceberg`, okFor('/api/admin/commit-iceberg', 'post', { ok: true })),
  http.post(`${API_BASE}/api/admin/ingest-logs`, okFor('/api/admin/ingest-logs', 'post', { ok: true })),
  http.get(`${API_BASE}/api/admin/iceberg-info`, () =>
    HttpResponse.json({ snapshots: [], current_snapshot_id: null }),
  ),
  http.get(`${API_BASE}/api/admin/iceberg-calendar`, () =>
    HttpResponse.json({ days: [] }),
  ),
  http.get(`${API_BASE}/api/admin/ingested-files`, () =>
    HttpResponse.json({ files: [], total: 0 }),
  ),
  http.post(`${API_BASE}/api/admin/metadata-cleanup`, okFor('/api/admin/metadata-cleanup', 'post', { ok: true, removed: 0 })),
  http.get(`${API_BASE}/api/admin/pop-locations`, () =>
    HttpResponse.json({ pops: [] }),
  ),
  http.post(`${API_BASE}/api/admin/pop-locations/refresh`, okFor('/api/admin/pop-locations/refresh', 'post', { pops: [] })),
  http.get(`${API_BASE}/api/admin/queries`, () => HttpResponse.json({ queries: [] })),
  http.get(`${API_BASE}/api/admin/raw-tree`, () =>
    HttpResponse.json({ tree: [], total: 0 }),
  ),
  http.get(`${API_BASE}/api/audit-logs`, () => HttpResponse.json({ logs: [] })),
  http.get(`${API_BASE}/api/cron-runs`, () => HttpResponse.json({ runs: [] })),
  http.get(`${API_BASE}/api/cron-schedule`, () =>
    HttpResponse.json({ schedule: [], next_run_at: null }),
  ),
  http.get(`${API_BASE}/api/admin/celery/status`, () =>
    HttpResponse.json({ workers: [], schedule: [], queues: [] }),
  ),
  http.get(
    `${API_BASE}/api/admin/celery/ledger`,
    okFor('/api/admin/celery/ledger', 'get', { items: [], total: 0 }),
  ),
  http.post(
    `${API_BASE}/api/admin/celery/ledger/retry`,
    okFor('/api/admin/celery/ledger/retry', 'post', { ok: true, affected: 0, dispatched: 0 }),
  ),
  http.post(
    `${API_BASE}/api/admin/celery/ledger/purge`,
    okFor('/api/admin/celery/ledger/purge', 'post', { ok: true, affected: 0, dispatched: 0 }),
  ),
  http.post(`${API_BASE}/api/admin/duckdb/recycle`, okFor('/api/admin/duckdb/recycle', 'post', { ok: true, status: 'skipped' })),
  http.get(`${API_BASE}/api/admin/duckdb/status`, okFor('/api/admin/duckdb/status', 'get', { ok: true, memory: {}, barrier: { is_active: false, active_paths: [] }, pools: {}, retired_pools: [] })),
  http.post(
    `${API_BASE}/api/admin/optimize/:service_id`,
    okFor('/api/admin/optimize/{service_id}', 'post', {
      files_rewritten: 0,
      files_added: 0,
      eligible_partitions: 0,
      partition_errors: [],
      error: null,
    }),
  ),
  http.post(`${API_BASE}/api/admin/ngwaf/sync/:service_id`, okFor('/api/admin/ngwaf/sync/{service_id}', 'post', { ok: true, service_id: 'svc-default', workspace_id: 'ws-default', stats: {} })),
  http.post(`${API_BASE}/api/admin/rum/ledger/sweep/:service_id`, okFor('/api/admin/rum/ledger/sweep/{service_id}', 'post', { ok: true, run_id: 1 })),
  http.get(`${API_BASE}/api/admin/ngwaf/status`, okFor('/api/admin/ngwaf/status', 'get', { ok: true })),

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
  http.post(`${API_BASE}/api/services/:service_id/log-fields`, okFor('/api/services/{service_id}/log-fields', 'post', { ok: true })),
  http.patch(`${API_BASE}/api/services/:service_id/credentials`, okFor('/api/services/{service_id}/credentials', 'patch', { ok: true })),
  http.get(`${API_BASE}/api/services/:service_id/lake-info`, () =>
    HttpResponse.json({ ok: true, table: 'logs' }),
  ),
  http.post(`${API_BASE}/api/services/:service_id/logging-settings/update`, okFor('/api/services/{service_id}/logging-settings/update', 'post', { ok: true })),
  // Bare custom-fields CREATE (the per-field PUT/DELETE already have
  // defaults above; the bare-resource POST was missing).
  http.post(`${API_BASE}/api/services/:service_id/custom-fields`, () =>
    HttpResponse.json({ ok: true, name: 'test-field' }),
  ),
  http.get(`${API_BASE}/api/services/:service_id/custom-fields/export`, () =>
    new HttpResponse('name,type,vcl\n', { headers: { 'content-type': 'text/csv' } }),
  ),

  // ── Control Room ─────────────────────────────────────────────────
  http.get(`${API_BASE}/api/services/:service_id/realtime-seed`, () =>
    HttpResponse.json({ ticks: [] }),
  ),
  http.get(`${API_BASE}/api/services/:service_id/control-room/baseline`, () =>
    HttpResponse.json({ baselines: {} }),
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
  http.get(`${API_BASE}/api/presets`, okFor('/api/presets', 'get', [])),

  // ── Admin-page endpoint defaults ──────────────────────────────────
  http.get(`${API_BASE}/api/admin/health-snapshot`, okFor('/api/admin/health-snapshot', 'get', { in_flight_runs: [], compaction: {}, pool_wait: [], recent_cron_failures: [], config_lock_degraded: false })),
  http.get(`${API_BASE}/api/admin/debug-settings`, () =>
    HttpResponse.json({
      query_debug_visibility: 'disabled',
      api_call_debug_visibility: 'disabled',
    }),
  ),
  http.patch(`${API_BASE}/api/admin/debug-settings`, async ({ request }) => {
    const body = (await request.json()) as { query_debug_visibility?: string; api_call_debug_visibility?: string }
    return HttpResponse.json({
      query_debug_visibility: body.query_debug_visibility ?? 'disabled',
      api_call_debug_visibility: body.api_call_debug_visibility ?? 'disabled',
    })
  }),
  http.get(`${API_BASE}/api/admin/metadata-storage`, () =>
    HttpResponse.json({ bytes: 0, tables: [] }),
  ),
  http.patch(`${API_BASE}/api/admin/metadata-retention`, okFor('/api/admin/metadata-retention', 'patch', { ok: true })),
  http.get(`${API_BASE}/api/admin/log-accounting`, () =>
    HttpResponse.json({ entries: [] }),
  ),
  http.get(`${API_BASE}/api/admin/metric-history/batch`, () =>
    HttpResponse.json({ series: [] }),
  ),
  http.post(`${API_BASE}/api/admin/rebuild-local-view`, okFor('/api/admin/rebuild-local-view', 'post', { ok: true })),
  // Real handler is an SSE stream (EventSourceResponse); components under
  // test mock useSSE directly (see DeleteDataDialog.test.tsx), so this only
  // needs to exist for the MSW-coverage-vs-openapi.json gate.
  http.post(`${API_BASE}/api/admin/reset-logs`, okFor('/api/admin/reset-logs', 'post', { type: 'done', message: 'ok' })),
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
  http.post(`${API_BASE}/api/admin/queries/:qid/cancel`, okFor('/api/admin/queries/{qid}/cancel', 'post', { state: 'cancelled', query_id: 0 })),
  http.get(`${API_BASE}/api/admin/app-config/query-monitor`, () =>
    HttpResponse.json({ config: {} }),
  ),
  http.get(`${API_BASE}/api/debug/recent-postgres`, () =>
    HttpResponse.json({ rows: [] }),
  ),
  http.post(`${API_BASE}/api/debug/clear-postgres`, okFor('/api/debug/clear-postgres', 'post', { ok: true, buffer_size: 0, buffer_cap: 0, dropped: 0 })),
  http.get(`${API_BASE}/api/debug/recent-sqlite`, () =>
    HttpResponse.json({ rows: [] }),
  ),
  http.post(`${API_BASE}/api/debug/clear-sqlite`, okFor('/api/debug/clear-sqlite', 'post', { ok: true, buffer_size: 0, buffer_cap: 0, dropped: 0 })),
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
  http.post(`${API_BASE}/api/admin/share/start`, okFor('/api/admin/share/start', 'post', {})),
  http.post(`${API_BASE}/api/admin/share/stop`, okFor('/api/admin/share/stop', 'post', { ok: true })),
  http.post(`${API_BASE}/api/admin/share/panic`, okFor('/api/admin/share/panic', 'post', { ok: true })),
  http.get(`${API_BASE}/api/admin/share/wordphrase`, () =>
    HttpResponse.json({ phrase: 'test-phrase' }),
  ),
  http.get(`${API_BASE}/api/admin/share/invites`, () =>
    HttpResponse.json({ invites: [] }),
  ),
  http.post(`${API_BASE}/api/admin/share/invites`, () =>
    HttpResponse.json({ invite_id: 'inv-test', passcode: 'TEST' }),
  ),
  http.delete(`${API_BASE}/api/admin/share/invites/:invite_id`, noContent()),
  http.post(`${API_BASE}/api/admin/share/invites/:invite_id/revoke`, okFor('/api/admin/share/invites/{invite_id}/revoke', 'post', { ok: true })),
  http.post(`${API_BASE}/api/admin/share/invites/:invite_id/reactivate`, () =>
    HttpResponse.json({ invite_id: 'inv-test', email: 'test@example.com', service_ids: [] }),
  ),
  http.post(`${API_BASE}/api/admin/share/invites/:invite_id/regenerate-passcode`, () =>
    HttpResponse.json({ invite_id: 'inv-test', passcode: 'NEW-PASSCODE' }),
  ),
  http.patch(`${API_BASE}/api/admin/share/invites/:invite_id/passcode`, okFor('/api/admin/share/invites/{invite_id}/passcode', 'patch', { ok: true })),
  http.patch(`${API_BASE}/api/admin/share/invites/:invite_id/services`, okFor('/api/admin/share/invites/{invite_id}/services', 'patch', { ok: true })),
  http.patch(`${API_BASE}/api/admin/share/invites/:invite_id/pii`, okFor('/api/admin/share/invites/{invite_id}/pii', 'patch', { ok: true })),
  http.patch(`${API_BASE}/api/admin/share/invites/:invite_id/sharing`, okFor('/api/admin/share/invites/{invite_id}/sharing', 'patch', { ok: true })),
  http.post(`${API_BASE}/api/admin/share/sessions/:session_id/boot`, okFor('/api/admin/share/sessions/{session_id}/boot', 'post', { ok: true })),
  http.post(`${API_BASE}/api/admin/share/sessions/:session_id/revoke`, okFor('/api/admin/share/sessions/{session_id}/revoke', 'post', { ok: true })),
  http.get(`${API_BASE}/api/admin/share/settings`, () =>
    HttpResponse.json({ settings: {} }),
  ),
  http.patch(`${API_BASE}/api/admin/share/settings`, () =>
    HttpResponse.json({ settings: {} }),
  ),
  http.get(`${API_BASE}/api/admin/share/tos`, () =>
    HttpResponse.json({ version: 1, text: 'Test TOS', published_at: null }),
  ),
  http.post(`${API_BASE}/api/admin/share/tos`, () =>
    HttpResponse.json({ version: 2, text: 'Updated TOS', published_at: null }),
  ),
  http.put(`${API_BASE}/api/admin/share/tos`, () =>
    HttpResponse.json({ version: 2, text: 'Updated TOS', published_at: null }),
  ),
  http.post(`${API_BASE}/api/admin/share/purge`, okFor('/api/admin/share/purge', 'post', { ok: true })),

  // ── Quarantine ───────────────────────────────────────────────────
  http.get(`${API_BASE}/api/admin/quarantine`, () =>
    HttpResponse.json({ files: [], total: 0, summary: { total_files: 0, total_corrupt_rows: 0 } }),
  ),
  http.get(`${API_BASE}/api/admin/quarantine/summary`, () =>
    HttpResponse.json({ total_files: 0, total_corrupt_rows: 0 }),
  ),
  http.get(`${API_BASE}/api/admin/quarantine/export`, () =>
    new HttpResponse('', { headers: { 'Content-Type': 'application/x-ndjson' } }),
  ),
  http.get(`${API_BASE}/api/admin/quarantine/:quarantine_id/download`, () =>
    new HttpResponse('', { headers: { 'Content-Type': 'application/x-ndjson' } }),
  ),
  http.get(`${API_BASE}/api/admin/quarantine/download/:item_id`, () =>
    new HttpResponse(new Uint8Array(), { headers: { 'Content-Type': 'application/octet-stream' } }),
  ),
  http.post(`${API_BASE}/api/admin/quarantine/purge`, okFor('/api/admin/quarantine/purge', 'post', { purged: 0 })),

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
  http.post(`${API_BASE}/api/value/summary`, () =>
    HttpResponse.json({}),
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
  http.post(`${API_BASE}/api/origin/drilldown`, () =>
    HttpResponse.json({ has_data: false, total: 0, dimension: '', rows: [] }),
  ),
  http.post(`${API_BASE}/api/performance/origin-ts`, () =>
    HttpResponse.json({ series: [], has_data: false }),
  ),

  // ── CMCD / Streaming analytics ────────────────────────────────────
  http.post(`${API_BASE}/api/cmcd/aggregates`, () =>
    HttpResponse.json({
      available: false,
      has_data: false,
      overview: null,
      buffer_health_ts: [],
      bitrate_ts: [],
      throughput_ts: [],
      top_content: [],
      rebuffer_by_country: [],
      rebuffer_by_asn: [],
      object_type_dist: [],
      streaming_format_dist: [],
      startup_ts: [],
    }),
  ),

  // ── Client telemetry collectors ───────────────────────────────────
  // Mounted globally by the SPA: web-vitals reports on pagehide /
  // navigation; ux-events fires on column-reorder and similar SPA
  // interactions. Both return ``{ok: true}`` server-side. Bland
  // handlers here so a test that happens to trigger a page-hide or a
  // DataTable reorder doesn't trip the onUnhandledRequest='error'
  // guard.
  http.post(`${API_BASE}/api/web-vitals`, okFor('/api/web-vitals', 'post', { ok: true })),
  http.post(`${API_BASE}/api/ux-events`, okFor('/api/ux-events', 'post', { ok: true })),

]
