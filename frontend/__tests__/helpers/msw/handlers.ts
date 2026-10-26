/**
 * MSW request handlers for vitest + backend contract test runner helpers.
 */

import { spawn, type ChildProcess } from 'node:child_process'
import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { http, HttpResponse, type JsonBodyType } from 'msw'
import { setupServer } from 'msw/node'
import type { paths } from '@/types/api.generated'

const API_BASE = 'http://127.0.0.1:8000'
const url = (p: string) => (p.startsWith('http') ? p : `${API_BASE}${p}`)
const ok = (body: JsonBodyType = {}) => () => HttpResponse.json(body)
const noContent = () => () => new HttpResponse(null, { status: 204 })
const get = (p: string, body: JsonBodyType = {}) => http.get(url(p), ok(body))
const post = (p: string, body: JsonBodyType = {}) => http.post(url(p), ok(body))
const put = (p: string, body: JsonBodyType = {}) => http.put(url(p), ok(body))
const patch = (p: string, body: JsonBodyType = {}) => http.patch(url(p), ok(body))
const del = (p: string, body?: JsonBodyType) => http.delete(url(p), body === undefined ? noContent() : ok(body))

type EnvelopeKeys = '_debug_queries' | '_debug_calls' | '_debug_sqlite' | '_debug_postgres' | '_is_cached' | '_section_timings'
type StripEnvelope<T> = T extends readonly unknown[]
  ? T
  : T extends object
    ? { [K in keyof T as K extends EnvelopeKeys ? never : K]: T[K] }
    : T
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
 */
const okFor =
  <P extends keyof paths, M extends keyof paths[P]>(_path: P, _method: M, body: OkBody<P, M>) =>
  () =>
    HttpResponse.json(body as JsonBodyType)

const hsMeta = (domain: string) => ({
  status: 'complete',
  exact: true,
  coverage: 1,
  freshness_lag_seconds: 0,
  watermark: {
    service_id: 'svc-default',
    domain,
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
})

const defaultServiceList = [{ service_id: 'svc-default', name: 'Default Service', access_level: 'read_write' }]

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

export const handlers = [
  // ── Bootstrap + service catalog ────────────────────────────────────
  get('/api/bootstrap', { services: defaultServiceList, active_service_id: 'svc-default' }),
  get('/api/services', { services: defaultServiceList }),
  http.get(url('/api/health'), okFor('/api/health', 'get', { status: 'ok', version: 'test' })),
  get('/api/query/schema/:service_id', { source: 'svc-default', table_name: 'logs', schema: [], columns: [] }),
  get('/api/admin/sync-status', { configured: false, access_level: 'read_write', state: 'idle' }),
  get('/api/admin/health', { status: 'ok', load: [0, 0, 0], recent_cron_failures: [] }),
  get('/api/admin/system-metrics', { load: [0, 0, 0], vcpus: 1, in_flight_runs: [], compaction: {} }),
  post('/api/admin/commit/:service_id', { type: 'done', message: 'ok' }),
  get('/api/admin/log-accounting/:service_id', {
    configured: true,
    hours: 24,
    buckets: [],
    totals: { requests: 0, log_lines: 0, log_bytes: 0, ingested_rows: 0, gap: 0, gap_pct: 0 },
    sustained_loss: null,
  }),
  get('/api/admin/high-scale/retention-recommendation', {
    service_id: 'svc-default',
    recommended_hot_days: 1,
    recommended_warm_days: 30,
    reasoning: 'default',
    volume_total_bytes: 1_000_000_000,
    volume_total_source: 'request',
    explicit_retention_days: null,
    effective_retention_days: 30,
  }),
  del('/api/admin/slow-queries', { deleted: 0, service_id: 'svc-default' }),
  post('/api/high-scale/services/:service_id/request-facts', { rows: [], metadata: hsMeta('request'), next_cursor: null }),
  post('/api/high-scale/services/:service_id/rum-facts/:domain', { rows: [], metadata: hsMeta('rum_vitals'), next_cursor: null }),
  post('/api/high-scale/services/:service_id/cmcd-facts', { rows: [], metadata: hsMeta('cmcd'), next_cursor: null }),
  post('/api/high-scale/services/:service_id/aggregates', {
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
  post('/api/high-scale/services/:service_id/queries', {
    service_id: 'svc-default',
    domain: 'request',
    plan: { tier: 'recent_facts', sources: ['clickhouse_facts'], queued: false, bounded: false, max_rows: 500, reason: 'recent raw facts' },
    rows: [],
    next_cursor: null,
    metadata: hsMeta('request'),
    job_id: null,
  }),
  get('/api/high-scale/services/:service_id/queries/:job_id', {
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
  post('/api/high-scale/services/:service_id/queries/:job_id/cancel', { job_id: 'job-default', cancelled: true }),
  post('/api/high-scale/services/:service_id/exports', { export_id: 'export-default', service_id: 'svc-default', status: 'queued' }),
  get('/api/high-scale/services/:service_id/exports/:export_id', { export_id: 'export-default', service_id: 'svc-default', status: 'queued' }),
  post('/api/high-scale/services/:service_id/exports/:export_id/cancel', { export_id: 'export-default', service_id: 'svc-default', status: 'cancelled', cancelled: true }),
  http.get(url('/api/admin/clickhouse/status'), ({ request }) =>
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
  http.post(url('/api/admin/clickhouse/replay'), () =>
    HttpResponse.json({ detail: { error: 'clickhouse_disabled', message: 'ClickHouse prototype disabled' } }, { status: 409 }),
  ),
  http.get(url('/api/schema'), okFor('/api/schema', 'get', { schema: [], table_name: 'logs' })),

  // ── Log-fields catalog + per-service introspection ─────────────────
  get('/api/log-fields/catalog', { fields: [], custom_fields: [], version: 'test' }),
  get('/api/log-extents', { ts_min: null, ts_max: null, count: 0 }),
  get('/api/insight-availability', { available: false, ts_min: null, ts_max: null }),
  get('/api/insight-availability/:service_id', { available: false, ts_min: null, ts_max: null }),
  get('/api/services/:service_id/credentials', { credentials: [] }),
  get('/api/services/:service_id/logging-settings', { logging_settings: { sources: [] } }),
  get('/api/services/:service_id/custom-fields', { fields: [] }),
  post('/api/services/:service_id/custom-fields/import', { imported: [], skipped: [] }),
  post('/api/services/:service_id/custom-fields/validate-vcl', { valid: true, errors: [] }),
  put('/api/services/:service_id/custom-fields/:field_name', { ok: true }),
  del('/api/services/:service_id/custom-fields/:field_name'),
  post('/api/services/:service_id/generate-viewer-key', { passcode: 'TEST-0000-0000', expires_at: '2030-01-01T00:00:00Z' }),

  // ── Session scoring + RUM versions ─────────────────────────────────
  get('/api/services/:service_id/scoring/analytics', {
    health: null,
    top_flagged: { rows: [] },
    score_distribution: { buckets: [] },
    compliance_breakdown: { categories: [] },
    evaluation_per_reason: { rows: [] },
    evaluation: null,
  }),
  get('/api/services/:service_id/scoring/config', {
    status: { enabled: false },
    threshold: { value: 50 },
    exclude_regex: { pattern: '' },
    enforce_status_code: { code: 403 },
    matrix_versions: { versions: [] },
  }),
  get('/api/services/:service_id/scoring/health', {
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
  get('/api/services/:service_id/scoring/l2-enforce', {
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
  get('/api/services/:service_id/rum/versions', { available: [], current: null, latest: null, update_available: false }),

  // ── Dashboard / analytics aggregates ───────────────────────────────
  post('/api/dashboard/aggregates', { rows: [], columns: [], total: 0 }),
  post('/api/insights', { insights: [], next_cursor: null }),
  post('/api/insights/cache-collapse-detail', {
    url: '',
    timeline: [],
    recent_misses: [],
    breakdown: { hits: 0, misses: 0, passes: 0, other: 0 },
    baseline_hit_rate: 0,
    window_hit_rate: 0,
    baseline_pass_rate: 0,
    window_pass_rate: 0,
  }),
  post('/api/security/aggregates', { rows: [], columns: [], total: 0 }),
  post('/api/security/top-bots', { rows: [], total: 0 }),
  post('/api/security/proxies', {
    active_proxies_count: 0,
    tunnel_requests_count: 0,
    distance_mismatches_count: 0,
    traffic_quality: [],
    suspicious_isps: [],
    active_clients: [],
  }),
  post('/api/security/bundle', { tls_fingerprints: [], bots: [], ngwaf_configured: false }),
  get('/api/security/bundle', { tls_fingerprints: [], bots: [], ngwaf_configured: false }),
  post('/api/performance/aggregates', { rows: [], columns: [], total: 0 }),
  post('/api/network-health', { rows: [], total: 0 }),
  post('/api/network/bundle', { available: false, health: {}, quality: {}, pop_health: [], transport: {} }),
  get('/api/network/bundle', { available: false, health: {}, quality: {}, pop_health: [], transport: {} }),
  post('/api/usage/bandwidth', { rows: [], total_bytes: 0 }),
  post('/api/usage/log-activity', { rows: [], total: 0 }),
  post('/api/usage/operations', { rows: [], total: 0 }),
  get('/api/usage/prefill', { rows: [], total: 0 }),
  get('/api/usage/prefill/rates', { rows: [], total: 0 }),
  get('/api/usage/current-storage', {
    live_bytes: 0,
    live_files: 0,
    deleted_bytes: 0,
    quarantine_bytes: 0,
    total_billed_bytes: 0,
    total_billed_gb_hours: 0,
    total_files: 0,
    total_bytes: 0,
    start: '',
    end: '',
  }),

  // ── Sessions, Alerts, Saved views, Provisioning ────────────────────
  get('/api/sessions', { rows: [], total: 0 }),
  get('/api/sessions/detail', { session_id: null, events: [] }),
  get('/api/alerts/', { data: [] }),
  get('/api/alerts/:service_id', { data: [] }),
  post('/api/alerts/', { data: { id: 'alert-test', status: 'saved' } }),
  http.put(url('/api/alerts/:alert_id'), ({ params }) => HttpResponse.json({ data: { id: String(params.alert_id), status: 'updated' } })),
  post('/api/alerts/evaluate', { data: { service_id: 'svc-default', evaluated: 0, triggered: 0, results: [] } }),
  post('/api/alerts/preview', { data: { series: [], threshold: 0, operator: '>', metric: '5xx_rate', window_min: 5 } }),
  http.post(url('/api/alerts/:alert_id/test'), ({ params }) =>
    HttpResponse.json({ data: { alert_id: String(params.alert_id), service_id: 'svc-default', status: 'validated', targets_tested: 1, channel_type: 'webhook' } }),
  ),
  http.post(url('/api/alerts/:service_id/:alert_id/test'), ({ params }) =>
    HttpResponse.json({ data: { alert_id: String(params.alert_id), service_id: String(params.service_id), status: 'validated', targets_tested: 1, channel_type: 'webhook' } }),
  ),
  del('/api/alerts/:alert_id', { data: { id: 'alert-test', status: 'deleted' } }),
  http.patch(url('/api/alerts/:alert_id/enabled'), ({ params }) => HttpResponse.json({ data: { id: String(params.alert_id), status: 'success' } })),
  get('/api/views/', { views: [] }),
  get('/api/views/:service_id', { views: [] }),
  get('/api/views/:view_id', { view_id: 'view-test', name: 'Test View', state: {} }),
  post('/api/views/', { view_id: 'view-test', ok: true }),
  del('/api/views/:view_id'),
  post('/api/provision/validate', { valid: true, errors: [] }),
  get('/api/provision/check-config', { exists: false, service_id: null }),
  get('/api/provision/check-domain', { available: true }),
  post('/api/provision/check-fos', { ok: true, bucket: 'test-bucket' }),
  post('/api/provision/ingest', { ok: true, ingested: 0 }),
  post('/api/provision/lake-info', { ok: true, table: 'logs' }),
  post('/api/provision/execute', { status: 'completed', service_id: 'svc-default' }),
  get('/api/provision/services', []),
  get('/api/provision/ngwaf-workspaces', { workspaces: [] }),
  post('/api/provision/services/:service_id/ngwaf-workspace', { ok: true }),
  post('/api/provision/terraform/export', { tf: '# generated' }),
  post('/api/provision/terraform/preview', { tf: '# preview' }),

  // ── Admin & OpenAPI-checked routes ─────────────────────────────────
  get('/api/admin/bot-sources', { sources: [], rdns: { total: 0, pending: 0 } }),
  http.post(url('/api/admin/bot-sources/:source_id/refresh'), okFor('/api/admin/bot-sources/{source_id}/refresh', 'post', { ok: true, source: {} })),
  http.post(url('/api/admin/bot-sources/refresh'), okFor('/api/admin/bot-sources/refresh', 'post', { ok: true, sources: [], updated_count: 0, failed_count: 0 })),
  http.post(url('/api/admin/bots/refresh'), okFor('/api/admin/bots/refresh', 'post', { ok: true, sources: [], updated_count: 0, failed_count: 0 })),
  post('/api/admin/bot-sources/rdns/backfill', { enqueued: 0 }),
  post('/api/admin/bot-sources/rdns/enrich', { enriched: 0 }),
  http.post(url('/api/admin/rdns/enrich'), okFor('/api/admin/rdns/enrich', 'post', { ok: true, summary: { resolved: 0, errors: 0, discovered: 0, batch_limit: 0 } })),
  http.get(url('/api/admin/rdns/stats'), okFor('/api/admin/rdns/stats', 'get', { ok: true, stats: {} })),
  get('/api/admin/system-jobs', { jobs: [] }),
  get('/api/admin/usage-logging', { enabled: false, retention_days: 30 }),
  http.patch(url('/api/admin/usage-logging'), okFor('/api/admin/usage-logging', 'patch', { ok: true })),
  http.post(url('/api/admin/commit-iceberg'), okFor('/api/admin/commit-iceberg', 'post', { ok: true })),
  http.post(url('/api/admin/ingest-logs'), okFor('/api/admin/ingest-logs', 'post', { ok: true })),
  get('/api/admin/iceberg-info', { snapshots: [], current_snapshot_id: null }),
  get('/api/admin/iceberg-calendar', { days: [] }),
  get('/api/admin/ingested-files', { files: [], total: 0 }),
  http.post(url('/api/admin/metadata-cleanup'), okFor('/api/admin/metadata-cleanup', 'post', { ok: true, removed: 0 })),
  get('/api/admin/pop-locations', { pops: [] }),
  http.post(url('/api/admin/pop-locations/refresh'), okFor('/api/admin/pop-locations/refresh', 'post', { pops: [] })),
  get('/api/admin/queries', { queries: [] }),
  get('/api/admin/raw-tree', { tree: [], total: 0 }),
  get('/api/audit-logs', { logs: [] }),
  get('/api/cron-runs', { runs: [] }),
  get('/api/cron-schedule', { schedule: [], next_run_at: null }),
  get('/api/admin/celery/status', { workers: [], schedule: [], queues: [] }),
  http.get(url('/api/admin/celery/ledger'), okFor('/api/admin/celery/ledger', 'get', { items: [], total: 0 })),
  http.post(url('/api/admin/celery/ledger/retry'), okFor('/api/admin/celery/ledger/retry', 'post', { ok: true, affected: 0, dispatched: 0 })),
  http.post(url('/api/admin/celery/ledger/purge'), okFor('/api/admin/celery/ledger/purge', 'post', { ok: true, affected: 0, dispatched: 0 })),
  http.post(url('/api/admin/duckdb/recycle'), okFor('/api/admin/duckdb/recycle', 'post', { ok: true, status: 'skipped' })),
  http.get(url('/api/admin/duckdb/status'), okFor('/api/admin/duckdb/status', 'get', { ok: true, memory: {}, barrier: { is_active: false, active_paths: [] }, pools: {}, retired_pools: [] })),
  http.post(url('/api/admin/optimize/:service_id'), okFor('/api/admin/optimize/{service_id}', 'post', { files_rewritten: 0, files_added: 0, eligible_partitions: 0, partition_errors: [], error: null })),
  http.post(url('/api/admin/ngwaf/sync/:service_id'), okFor('/api/admin/ngwaf/sync/{service_id}', 'post', { ok: true, service_id: 'svc-default', workspace_id: 'ws-default', stats: {} })),
  http.post(url('/api/admin/rum/ledger/sweep/:service_id'), okFor('/api/admin/rum/ledger/sweep/{service_id}', 'post', { ok: true, run_id: 1 })),
  http.get(url('/api/admin/ngwaf/status'), okFor('/api/admin/ngwaf/status', 'get', { ok: true })),
  get('/api/sync-status', { latest_log_at: null, local_rows: 0, fos_rows: null, last_sync_at: null }),
  get('/api/admin/share/banner', { sharing_active: false, public_url: null }),
  http.post(url('/api/share/heartbeat'), noContent()),
  get('/api/services/:service_id/log-fields', { fields: [] }),
  http.post(url('/api/services/:service_id/log-fields'), okFor('/api/services/{service_id}/log-fields', 'post', { ok: true })),
  http.patch(url('/api/services/:service_id/credentials'), okFor('/api/services/{service_id}/credentials', 'patch', { ok: true })),
  get('/api/services/:service_id/lake-info', { ok: true, table: 'logs' }),
  http.post(url('/api/services/:service_id/logging-settings/update'), okFor('/api/services/{service_id}/logging-settings/update', 'post', { ok: true })),
  post('/api/services/:service_id/custom-fields', { ok: true, name: 'test-field' }),
  http.get(url('/api/services/:service_id/custom-fields/export'), () => new HttpResponse('name,type,vcl\n', { headers: { 'content-type': 'text/csv' } })),
  get('/api/services/:service_id/realtime-seed', { ticks: [] }),
  get('/api/services/:service_id/control-room/baseline', { baselines: {} }),
  post('/api/dashboard/bundle', { rows: [], total: 0 }),
  post('/api/dashboard/field-values', { values: [] }),
  post('/api/origin/aggregates', { rows: [], columns: [], total: 0 }),
  post('/api/query', { rows: [], columns: [] }),
  http.get(url('/api/presets'), okFor('/api/presets', 'get', [])),
  http.get(url('/api/admin/health-snapshot'), okFor('/api/admin/health-snapshot', 'get', { in_flight_runs: [], compaction: {}, pool_wait: [], recent_cron_failures: [], config_lock_degraded: false })),
  get('/api/admin/debug-settings', { query_debug_visibility: 'disabled', api_call_debug_visibility: 'disabled' }),
  http.patch(url('/api/admin/debug-settings'), async ({ request }) => {
    const b = (await request.json()) as { query_debug_visibility?: string; api_call_debug_visibility?: string }
    return HttpResponse.json({ query_debug_visibility: b.query_debug_visibility ?? 'disabled', api_call_debug_visibility: b.api_call_debug_visibility ?? 'disabled' })
  }),
  get('/api/admin/metadata-storage', { bytes: 0, tables: [] }),
  http.patch(url('/api/admin/metadata-retention'), okFor('/api/admin/metadata-retention', 'patch', { ok: true })),
  get('/api/admin/log-accounting', { entries: [] }),
  get('/api/admin/metric-history/batch', { series: [] }),
  http.post(url('/api/admin/rebuild-local-view'), okFor('/api/admin/rebuild-local-view', 'post', { ok: true })),
  http.post(url('/api/admin/reset-logs'), okFor('/api/admin/reset-logs', 'post', { type: 'done', message: 'ok' })),
  get('/api/admin/usage-log', { rows: [] }),
  del('/api/admin/usage-log'),
  get('/api/admin/queries/summary', { total: 0, active: 0 }),
  get('/api/admin/queries/:qid', { query_id: 'q-test', status: 'completed' }),
  http.post(url('/api/admin/queries/:qid/cancel'), okFor('/api/admin/queries/{qid}/cancel', 'post', { state: 'cancelled', query_id: 0 })),
  get('/api/admin/app-config/query-monitor', { config: {} }),
  get('/api/debug/recent-postgres', { rows: [] }),
  http.post(url('/api/debug/clear-postgres'), okFor('/api/debug/clear-postgres', 'post', { ok: true, buffer_size: 0, buffer_cap: 0, dropped: 0 })),
  get('/api/debug/recent-sqlite', { rows: [] }),
  http.post(url('/api/debug/clear-sqlite'), okFor('/api/debug/clear-sqlite', 'post', { ok: true, buffer_size: 0, buffer_cap: 0, dropped: 0 })),
  del('/api/cron-runs'),

  // ── Share-admin + Quarantine + Auth ────────────────────────────────
  get('/api/admin/share/status', { active: false, sessions: [] }),
  get('/api/admin/share/live', { sessions: [] }),
  get('/api/admin/share/audit-logs', { logs: [] }),
  http.post(url('/api/admin/share/start'), okFor('/api/admin/share/start', 'post', {})),
  http.post(url('/api/admin/share/stop'), okFor('/api/admin/share/stop', 'post', { ok: true })),
  http.post(url('/api/admin/share/panic'), okFor('/api/admin/share/panic', 'post', { ok: true })),
  get('/api/admin/share/wordphrase', { phrase: 'test-phrase' }),
  get('/api/admin/share/invites', { invites: [] }),
  post('/api/admin/share/invites', { invite_id: 'inv-test', passcode: 'TEST' }),
  del('/api/admin/share/invites/:invite_id'),
  http.post(url('/api/admin/share/invites/:invite_id/revoke'), okFor('/api/admin/share/invites/{invite_id}/revoke', 'post', { ok: true })),
  post('/api/admin/share/invites/:invite_id/reactivate', { invite_id: 'inv-test', email: 'test@example.com', service_ids: [] }),
  post('/api/admin/share/invites/:invite_id/regenerate-passcode', { invite_id: 'inv-test', passcode: 'NEW-PASSCODE' }),
  http.patch(url('/api/admin/share/invites/:invite_id/passcode'), okFor('/api/admin/share/invites/{invite_id}/passcode', 'patch', { ok: true })),
  http.patch(url('/api/admin/share/invites/:invite_id/services'), okFor('/api/admin/share/invites/{invite_id}/services', 'patch', { ok: true })),
  http.patch(url('/api/admin/share/invites/:invite_id/pii'), okFor('/api/admin/share/invites/{invite_id}/pii', 'patch', { ok: true })),
  http.patch(url('/api/admin/share/invites/:invite_id/sharing'), okFor('/api/admin/share/invites/{invite_id}/sharing', 'patch', { ok: true })),
  http.post(url('/api/admin/share/sessions/:session_id/boot'), okFor('/api/admin/share/sessions/{session_id}/boot', 'post', { ok: true })),
  http.post(url('/api/admin/share/sessions/:session_id/revoke'), okFor('/api/admin/share/sessions/{session_id}/revoke', 'post', { ok: true })),
  get('/api/admin/share/settings', { settings: {} }),
  patch('/api/admin/share/settings', { settings: {} }),
  get('/api/admin/share/tos', { version: 1, text: 'Test TOS', published_at: null }),
  post('/api/admin/share/tos', { version: 2, text: 'Updated TOS', published_at: null }),
  put('/api/admin/share/tos', { version: 2, text: 'Updated TOS', published_at: null }),
  http.post(url('/api/admin/share/purge'), okFor('/api/admin/share/purge', 'post', { ok: true })),
  get('/api/admin/quarantine', { files: [], total: 0, summary: { total_files: 0, total_corrupt_rows: 0 } }),
  get('/api/admin/quarantine/summary', { total_files: 0, total_corrupt_rows: 0 }),
  http.get(url('/api/admin/quarantine/export'), () => new HttpResponse('', { headers: { 'Content-Type': 'application/x-ndjson' } })),
  http.get(url('/api/admin/quarantine/:quarantine_id/download'), () => new HttpResponse('', { headers: { 'Content-Type': 'application/x-ndjson' } })),
  http.get(url('/api/admin/quarantine/download/:item_id'), () => new HttpResponse(new Uint8Array(), { headers: { 'Content-Type': 'application/octet-stream' } })),
  http.post(url('/api/admin/quarantine/purge'), okFor('/api/admin/quarantine/purge', 'post', { purged: 0 })),
  http.get('/api/share/auth-config', ok({ passcode_enabled: true, providers: [] })),
  http.get('/api/admin/share/oauth-providers', ok({ providers: [] })),
  http.get('/api/share/oauth/authorize', () => new HttpResponse(null, { status: 302, headers: { Location: '/idp-stub' } })),
  http.get('/api/share/oauth/callback', () => new HttpResponse(null, { status: 302, headers: { Location: '/dashboard' } })),
  http.get('/api/share/tos', ok({ version: 'v1', text: 'Test TOS' })),
  http.post('/api/share/login', ok({ ok: true, must_acknowledge: false })),
  http.post('/api/share/acknowledge', ok({ ok: true })),
  post('/api/login', { ok: true }),
  get('/api/auth', { authenticated: true, role: 'admin' }),

  // ── High-traffic analytics POSTs & Telemetry ───────────────────────
  http.post(url('/api/dashboard/raw/csv'), () => new HttpResponse('', { status: 200, headers: { 'Content-Type': 'text/csv' } })),
  post('/api/sessions', { sessions: [], total: 0 }),
  post('/api/sessions/detail', { events: [], detail: null }),
  post('/api/network-quality', { buckets: [], pop_breakdown: [] }),
  post('/api/value/summary', {}),
  post('/api/origin/summary', { stages: [], totals: {} }),
  post('/api/origin/timeseries', { series: [], has_data: false }),
  post('/api/origin/status-codes', { rows: [] }),
  post('/api/origin/slow-urls', { rows: [] }),
  post('/api/origin/path-breakdown', { rows: [] }),
  post('/api/origin/pop-latency', { rows: [] }),
  post('/api/origin/ip-health', { rows: [] }),
  post('/api/origin/shielding-analysis', { shielding: [], origin: [] }),
  post('/api/origin/drilldown', { has_data: false, total: 0, dimension: '', rows: [] }),
  post('/api/performance/origin-ts', { series: [], has_data: false }),
  post('/api/cmcd/aggregates', {
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
  http.post(url('/api/web-vitals'), okFor('/api/web-vitals', 'post', { ok: true })),
  http.post(url('/api/ux-events'), okFor('/api/ux-events', 'post', { ok: true })),
]

export const server = setupServer(...handlers)

// ── Backend contract/E2E test runner helpers ──────────────────────────

const PORT = 13003
const HOST = '127.0.0.1'
export const CONTRACT_API_BASE = `http://${HOST}:${PORT}`
const HOST_ENV_KEYS = ['PATH', 'HOME', 'USER', 'LOGNAME', 'LANG', 'LC_ALL', 'LC_CTYPE', 'TZ', 'SYSTEMROOT', 'WINDIR', 'COMSPEC', 'PATHEXT'] as const

interface MockOAuth {
  backendPort: number
  frontendPort: number
  registryPath: string
}

export function backendTestEnvironment(sandboxDir: string, oauth?: MockOAuth): NodeJS.ProcessEnv {
  const env: NodeJS.ProcessEnv = { NODE_ENV: 'test' }
  for (const key of HOST_ENV_KEYS) {
    if (process.env[key] !== undefined) env[key] = process.env[key]
  }
  const metaDsn =
    process.env.TEST_METADATA_DSN ||
    (process.env.METADATA_DSN && !process.env.METADATA_DSN.includes('example.invalid')
      ? process.env.METADATA_DSN
      : 'postgresql://fla:fla_test_password@localhost:5432/ducklake_test')
  const duckCatalog =
    process.env.TEST_DUCKLAKE_CATALOG ||
    (process.env.DUCKLAKE_CATALOG && !process.env.DUCKLAKE_CATALOG.includes('example.invalid') ? process.env.DUCKLAKE_CATALOG : metaDsn)

  Object.assign(env, {
    UV_NO_ENV_FILE: '1',
    UV_NO_CONFIG: '1',
    PYTHON_DOTENV_DISABLED: '1',
    DEBUG_RESPONSES: 'true',
    FASTLY_MOCK_MODE: '1',
    FLA_DEV_NO_CRONS: '1',
    FLA_DEV_LOCAL_CRONS: '0',
    DEPLOYMENT_MODE: 'standard',
    SSE_BACKPLANE: 'local',
    OTEL_EXPORTER: 'none',
    DUCKDB_POOL_MAX_SIZE: '16',
    CONTRACT_CONFIGS_DIR: join(sandboxDir, 'configs'),
    CONTRACT_DATA_DIR: join(sandboxDir, 'data'),
    METADATA_DSN: metaDsn,
    DUCKLAKE_CATALOG: duckCatalog,
  })
  if (oauth) {
    Object.assign(env, {
      OAUTH_MOCK_IDP: '1',
      OAUTH_MOCK_IDP_ISSUER: `http://127.0.0.1:${oauth.backendPort}/mock-idp`,
      OAUTH_PROVIDERS_CONFIG_PATH: oauth.registryPath,
      OAUTH_FLOW_STATE_SECRET: 'e2e-oauth-flow-state-secret-0123456789',
      OAUTH_GOOGLE_CLIENT_ID: 'e2e-mock-client-id',
      OAUTH_GOOGLE_CLIENT_SECRET: 'e2e-mock-client-secret',
      OAUTH_REDIRECT_BASE: `http://127.0.0.1:${oauth.frontendPort}`,
    })
  }
  return env
}

let proc: ChildProcess | null = null
let sandbox: string | null = null

async function poll(healthUrl: string, timeoutMs = 30_000): Promise<void> {
  const deadline = Date.now() + timeoutMs
  let lastErr: unknown = null
  while (Date.now() < deadline) {
    if (proc && proc.exitCode !== null) {
      throw new Error(`Backend process exited early with code ${proc.exitCode}`)
    }
    try {
      const r = await fetch(healthUrl)
      if (r.ok) return
    } catch (e) {
      lastErr = e
    }
    await new Promise((r) => setTimeout(r, 250))
  }
  throw new Error(`Backend never returned 200 from ${healthUrl}: ${String(lastErr)}`)
}

export async function startBackend(): Promise<void> {
  if (proc) return
  const repoRoot = join(__dirname, '..', '..', '..', '..')
  sandbox = mkdtempSync(join(tmpdir(), 'fla-contract-'))
  proc = spawn('uv', ['run', '--project', repoRoot, 'python', 'scripts/run_contract_backend.py', '--host', HOST, '--port', String(PORT)], {
    cwd: repoRoot,
    env: backendTestEnvironment(sandbox),
    stdio: ['ignore', 'pipe', 'pipe'],
  })
  proc.stdout?.on('data', (chunk) => process.stdout.write(`[backend] ${chunk}`))
  proc.stderr?.on('data', (chunk) => process.stderr.write(`[backend] ${chunk}`))
  await poll(`${CONTRACT_API_BASE}/api/health`, 120_000)
}

export async function stopBackend(): Promise<void> {
  if (!proc) return
  const p = proc
  proc = null
  p.kill('SIGTERM')
  await new Promise<void>((resolve) => {
    const t = setTimeout(() => (p.kill('SIGKILL'), resolve()), 5_000)
    p.once('exit', () => (clearTimeout(t), resolve()))
  })
  if (sandbox) {
    try {
      rmSync(sandbox, { recursive: true, force: true })
    } catch {
      /* Sandbox cleanup is best-effort. */
    }
    sandbox = null
  }
}
