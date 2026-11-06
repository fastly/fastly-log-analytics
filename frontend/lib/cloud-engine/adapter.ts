import {
  getStoreStats,
  runInWorker,
  searchFieldSummariesInWorker,
  searchFieldValuesInWorker,
  setDashboardCacheIdentity,
} from "./dashboard/client";
import type { DashboardFilter } from "./dashboard/filters";
import { parseVcpuMsHeader } from "./dashboard/sources";
import type { CardField } from "./dashboard/summaries";
import { parquetColumns, sampleStats } from "./schema";
import {
  deleteLogHistory,
  getLoggedTraffic,
  getLogsStatus,
  getOperationalLogging,
  getQuarantine,
  patchOperationalLogging,
  purgeQuarantine,
  reconcileStorage,
  startBackfill,
  sweepRetention,
} from "./telemetry/admin";
import { queryOpsLogs } from "./telemetry/client";
import { OPS_KINDS, type OpsKind } from "./telemetry/kinds";
import { evaluateCapacity } from "./usage/capacity";
import { estimateMonthlyUsage } from "./usage/estimate";
import { fetchEstimatePrefill } from "./usage/estimatePrefill";
import { LIST_RATES } from "./usage/rates";
import { bucketUsage } from "./usage/series";
import {
  buildSevenLineCostTable,
  deriveFosUsageFromCronRuns,
  fetchUsageSettings,
  fetchUsageSummary,
  reconcileSnapshot,
  type CronRunRow,
} from "./usage/summary";
import { useDebugStore, useFilterStore } from "@/stores/filterStore";

export { parseVcpuMsHeader };

type BucketInterval = "1m" | "5m" | "1h" | "1d";

let _runtimeCloudMode = false;

export const IS_CLOUD_MODE: boolean =
  process.env.NEXT_PUBLIC_DEPLOYMENT_MODE === "cloud" ||
  process.env.NEXT_PUBLIC_STATIC_EXPORT === "1" ||
  process.env.NEXT_PUBLIC_RUNTIME_MODE === "cloud";

export function isCloudMode(): boolean {
  return IS_CLOUD_MODE || _runtimeCloudMode;
}

export function setCloudModeRuntime(enabled: boolean): void {
  _runtimeCloudMode = enabled;
}

export interface CloudQueryTelemetryPayload {
  files_listed?: number;
  files_fetched?: number;
  files_cached?: number;
  cdn_hits?: number;
  cdn_misses?: number;
  compute_requests?: number;
  vcpu_ms?: number | null;
  bytes_scanned?: number;
  rows_scanned?: number;
  duration_ms?: number;
  surface?: string;
  outcome?: string;
  sql?: string;
  engine?: string;
  endpoint?: string;
}

export async function postCloudQueryTelemetry(
  payload: CloudQueryTelemetryPayload,
): Promise<void> {
  try {
    const debugState = useDebugStore.getState();
    if (debugState.enabled && payload.sql) {
      debugState.addQueries([
        {
          engine: payload.engine ?? "DuckDB-Wasm",
          sql: payload.sql,
          duration_ms: payload.duration_ms ?? 0,
          vcpu_ms: payload.vcpu_ms ?? null,
          rows_scanned: payload.rows_scanned,
          bytes_scanned: payload.bytes_scanned,
          endpoint: payload.endpoint ?? "/api/dashboard/bundle",
          timestamp: new Date().toISOString(),
        },
      ]);
    }
    if (typeof fetch !== "function") return;
    await fetch("/api/telemetry/query", {
      method: "POST",
      headers: {
        "content-type": "application/json",
      },
      body: JSON.stringify({
        files_listed: payload.files_listed ?? 0,
        files_fetched: payload.files_fetched ?? 0,
        files_cached: payload.files_cached ?? 0,
        cdn_hits: payload.cdn_hits ?? 0,
        cdn_misses: payload.cdn_misses ?? 0,
        compute_requests: payload.compute_requests ?? 0,
        vcpu_ms: payload.vcpu_ms ?? 0,
        bytes_scanned: payload.bytes_scanned ?? 0,
        rows_scanned: payload.rows_scanned ?? 0,
        duration_ms: payload.duration_ms ?? 0,
        surface: payload.surface ?? "dashboard",
        outcome: payload.outcome ?? "ok",
      }),
      keepalive: true,
    });
  } catch {
    /* telemetry is best-effort */
  }
}

function jsonResponse(
  body: unknown,
  request: Request,
  debugQuery?: {
    sql: string;
    duration_ms: number;
    engine?: string;
    vcpu_ms?: number | null;
    rows?: number;
  },
  status = 200,
): Response {
  const wantsDebug =
    request.headers.get("x-debug-responses") === "1" ||
    request.headers.get("X-Debug-Responses") === "1" ||
    request.headers.get("X-Debug-SQL") === "1";
  let payload = body;
  if (
    wantsDebug &&
    payload &&
    typeof payload === "object" &&
    !Array.isArray(payload) &&
    debugQuery
  ) {
    payload = {
      ...(payload as Record<string, unknown>),
      _debug_queries: [
        {
          label: debugQuery.engine ?? "DuckDB-Wasm",
          sql: debugQuery.sql,
          time_ms: debugQuery.duration_ms,
          duration_ms: debugQuery.duration_ms,
          rows: debugQuery.rows ?? 0,
          engine: debugQuery.engine ?? "DuckDB-Wasm",
          vcpu_ms: debugQuery.vcpu_ms ?? null,
        },
      ],
    };
  }
  return new Response(JSON.stringify(payload), {
    status,
    headers: { "content-type": "application/json" },
  });
}

function toBucketInterval(
  raw: string | undefined,
  fromMs: number,
  toMs: number,
): BucketInterval {
  if (raw === "1m" || raw === "5m" || raw === "1h" || raw === "1d") {
    return raw;
  }
  const spanHours = (toMs - fromMs) / 3_600_000;
  if (spanHours <= 2) return "1m";
  if (spanHours <= 12) return "5m";
  if (spanHours <= 72) return "1h";
  return "1d";
}

function parseFiltersPayload(
  rawFilters: Record<string, { mode?: string; values?: string[] }> | undefined,
  customFilters: Record<string, { mode?: string; values?: string[] }> | undefined,
): DashboardFilter[] {
  const out: DashboardFilter[] = [];
  const pushAll = (
    source: Record<string, { mode?: string; values?: string[] }> | undefined,
  ) => {
    if (!source || typeof source !== "object") return;
    for (const [rawKey, entry] of Object.entries(source)) {
      const field = rawKey.replace(/_\d+$/, "");
      const vals = Array.isArray(entry?.values) ? entry.values.map(String) : [];
      const mode = entry?.mode === "exclude" ? "exclude" : "include";
      for (const value of vals) {
        if (value) out.push({ field, value, mode });
      }
    }
  };
  pushAll(rawFilters);
  pushAll(customFilters);
  return out;
}

const SAVED_VIEWS_STORAGE_KEY = "flp-saved-views-v1";

function readSavedViews(): Array<Record<string, unknown>> {
  if (typeof localStorage === "undefined") return [];
  try {
    const raw = localStorage.getItem(SAVED_VIEWS_STORAGE_KEY);
    if (!raw) return [];
    const parsed = JSON.parse(raw);
    return Array.isArray(parsed) ? parsed : [];
  } catch {
    return [];
  }
}

function writeSavedViews(views: Array<Record<string, unknown>>): void {
  if (typeof localStorage === "undefined") return;
  try {
    localStorage.setItem(SAVED_VIEWS_STORAGE_KEY, JSON.stringify(views));
  } catch {
    /* ignore quota errors */
  }
}

export async function handleCloudAnalyticalRequest(
  request: Request,
): Promise<Response | null> {
  const url = new URL(request.url, "http://localhost");
  const { pathname, searchParams } = url;
  const method = (request.method || "GET").toUpperCase();

  // Fold per-file Compute telemetry into the query beacon
  if (pathname === "/api/file" || pathname === "/api/files") {
    try {
      request.headers.set("x-flp-fold-telemetry", "1");
    } catch {
      /* immutable headers in synthetic test requests */
    }
    return null;
  }

  // Silence high-scale-only browser beacons when running on Cloud Compute
  if (pathname === "/api/web-vitals" || pathname === "/api/ux-events") {
    return new Response(null, { status: 204 });
  }

  // Saved Views localStorage bridge
  if (pathname === "/api/views" && method === "GET") {
    return jsonResponse({ views: readSavedViews() }, request);
  }
  if (pathname === "/api/views" && method === "POST") {
    const body = (await request.clone().json().catch(() => ({}))) as Record<
      string,
      unknown
    >;
    const views = readSavedViews();
    const created = {
      id:
        typeof crypto?.randomUUID === "function"
          ? crypto.randomUUID()
          : `view-${Date.now()}`,
      created_at: new Date().toISOString(),
      ...body,
    };
    views.push(created);
    writeSavedViews(views);
    return jsonResponse(created, request);
  }
  if (pathname.startsWith("/api/views/") && (method === "PUT" || method === "DELETE")) {
    const viewId = decodeURIComponent(pathname.slice("/api/views/".length));
    const views = readSavedViews();
    if (method === "DELETE") {
      writeSavedViews(views.filter((v) => String(v.id) !== viewId));
      return jsonResponse({ status: "deleted" }, request);
    }
    const body = (await request.clone().json().catch(() => ({}))) as Record<
      string,
      unknown
    >;
    const next = views.map((v) =>
      String(v.id) === viewId ? { ...v, ...body } : v,
    );
    writeSavedViews(next);
    const updated = next.find((v) => String(v.id) === viewId) ?? {
      id: viewId,
      ...body,
    };
    return jsonResponse(updated, request);
  }

  // Page 4: Main Dashboard (/api/dashboard/bundle, /api/dashboard/aggregates, /api/dashboard/field-values)
  if (
    (pathname === "/api/dashboard/bundle" ||
      pathname === "/api/dashboard/aggregates" ||
      pathname === "/api/aggregates" ||
      pathname === "/api/timeseries") &&
    method === "POST"
  ) {
    const startedAt =
      typeof performance !== "undefined" ? performance.now() : Date.now();
    const body = (await request.clone().json().catch(() => ({}))) as {
      start_time?: string;
      end_time?: string;
      interval?: string;
      metric?: string;
      filters?: Record<string, { mode?: string; values?: string[] }>;
      custom_filters?: Record<string, { mode?: string; values?: string[] }>;
      fields?: string[];
    };
    const now = Date.now();
    const fromMs = body.start_time ? Date.parse(body.start_time) : now - 86_400_000;
    const toMs = body.end_time ? Date.parse(body.end_time) : now;
    const interval = toBucketInterval(body.interval, fromMs, toMs);
    const filters = parseFiltersPayload(body.filters, body.custom_filters);

    const result = await runInWorker(
      {
        from: new Date(fromMs).toISOString(),
        to: new Date(toMs).toISOString(),
        interval,
        metric: body.metric ?? "requests",
        filters,
        customFields: body.fields,
      },
      request.signal,
    );

    const durationMs = Math.max(
      1,
      Math.round(
        (typeof performance !== "undefined" ? performance.now() : Date.now()) -
          startedAt,
      ),
    );
    const engineName = result.stats.fetched > 0 ? "DuckDB-Wasm" : "Rollups";
    const vcpuMs: number | null = null;

    useFilterStore.getState().setLastFetchMeta({
      vcpuMs,
      engine: engineName,
      cachedFiles: result.stats.cached,
      fetchedFiles: result.stats.fetched,
    });

    const sqlText =
      result.stats.fetched > 0
        ? `SELECT time_bucket('${interval}', timestamp), ... FROM read_parquet(${result.stats.fetched} window files)`
        : `ROLLUP_FAST_PATH(${result.stats.cached} summary files)`;

    void postCloudQueryTelemetry({
      files_listed: result.stats.listed,
      files_fetched: result.stats.fetched,
      files_cached: result.stats.cached,
      cdn_hits: result.stats.cdnHits,
      vcpu_ms: vcpuMs,
      bytes_scanned: result.stats.bytes,
      rows_scanned: result.totalRequests,
      duration_ms: durationMs,
      surface: "dashboard",
      outcome: "ok",
      sql: sqlText,
      engine: engineName,
      endpoint: pathname,
    });

    const totalReqs = result.totalRequests;
    let totalBytes = 0;
    let total4xx = 0;
    let total5xx = 0;
    let totalHits = 0;
    let totalCacheRows = 0;
    for (const b of result.buckets) {
      totalBytes += b.bytes;
      total4xx += b.s4xx;
      total5xx += b.s5xx;
      totalHits += b.hits;
      totalCacheRows += b.cacheRows;
    }

    const aggregates = {
      interval,
      metric: body.metric ?? "requests",
      time_series: result.buckets.map((pt) => ({
        time: new Date(pt.t).toISOString(),
        value:
          body.metric === "bandwidth"
            ? pt.bytes
            : body.metric === "error_rate"
              ? pt.requests > 0
                ? ((pt.s4xx + pt.s5xx) / pt.requests) * 100
                : 0
              : pt.requests,
        category: null,
      })),
    };

    if (pathname === "/api/dashboard/aggregates" || pathname === "/api/timeseries") {
      return jsonResponse(aggregates, request, {
        sql: sqlText,
        duration_ms: durationMs,
        engine: engineName,
        vcpu_ms: vcpuMs,
        rows: totalReqs,
      });
    }

    const fields: Record<
      string,
      {
        field: string;
        rows: Array<{ value: string; count: number; pct: number }>;
        total_count: number;
      }
    > = {};
    for (const [fieldKey, card] of Object.entries(result.cards ?? {})) {
      const topRows = card.top ?? [];
      fields[fieldKey] = {
        field: fieldKey,
        rows: topRows.map((r) => ({
          value: String(r.value),
          count: r.count,
          pct: totalReqs > 0 ? (r.count / totalReqs) * 100 : 0,
        })),
        total_count: card.total ?? topRows.length,
      };
    }

    return jsonResponse(
      {
        summary: {
          total_requests: totalReqs,
          error_rate: totalReqs > 0 ? ((total4xx + total5xx) / totalReqs) * 100 : 0,
          error_5xx_rate: totalReqs > 0 ? (total5xx / totalReqs) * 100 : 0,
          error_4xx_rate: totalReqs > 0 ? (total4xx / totalReqs) * 100 : 0,
          avg_latency_ms: 0,
          p95_latency_ms: 0,
          p99_latency_ms: 0,
          cache_hit_ratio: totalCacheRows > 0 ? (totalHits / totalCacheRows) * 100 : 0,
          total_bandwidth_bytes: totalBytes,
          unique_visitors: result.cards?.client_ip?.distinct ?? 0,
        },
        aggregates,
        fields,
      },
      request,
      {
        sql: sqlText,
        duration_ms: durationMs,
        engine: engineName,
        vcpu_ms: vcpuMs,
        rows: totalReqs,
      },
    );
  }

  if (
    (pathname === "/api/dashboard/field-values" ||
      pathname === "/api/field-values") &&
    (method === "POST" || method === "GET")
  ) {
    const body =
      method === "POST"
        ? ((await request.clone().json().catch(() => ({}))) as {
            field?: string;
            fields?: string[];
            search?: string;
            start_time?: string;
            end_time?: string;
            limit?: number;
          })
        : {
            field: searchParams.get("field") ?? undefined,
            search: searchParams.get("search") ?? undefined,
            start_time: searchParams.get("start_time") ?? undefined,
            end_time: searchParams.get("end_time") ?? undefined,
            limit: Number(searchParams.get("limit") ?? 50),
          };
    const now = Date.now();
    const fromIso = body.start_time ?? new Date(now - 86_400_000).toISOString();
    const toIso = body.end_time ?? new Date(now).toISOString();
    if (Array.isArray(body.fields) && body.fields.length > 0) {
      const summaries = await searchFieldSummariesInWorker(
        fromIso,
        toIso,
        body.fields[0] as CardField,
        body.search ?? "",
        [],
        {
          limit: body.limit ?? 20,
          signal: request.signal,
        },
      );
      return jsonResponse({ summaries }, request);
    }
    const fieldName = (body.field ?? "url") as CardField;
    const items = await searchFieldValuesInWorker(
      fromIso,
      toIso,
      fieldName,
      body.search ?? "",
      [],
      {
        limit: body.limit ?? 50,
        signal: request.signal,
      },
    );
    return jsonResponse(
      {
        field: fieldName,
        values: items.map((i) => ({ value: i.value, count: i.count })),
      },
      request,
    );
  }

  // Page 5: Usage & Cost (/api/usage/*)
  if (pathname.startsWith("/api/usage/")) {
    const now = Date.now();
    const days = Number(searchParams.get("days") ?? 30) || 30;
    const fromMs = searchParams.get("start_time")
      ? Date.parse(searchParams.get("start_time")!)
      : now - days * 86_400_000;
    const toMs = searchParams.get("end_time")
      ? Date.parse(searchParams.get("end_time")!)
      : now;

    if (pathname === "/api/usage/prefill/rates") {
      const settings = await fetchUsageSettings().catch(() => LIST_RATES);
      return jsonResponse(settings, request);
    }
    if (pathname === "/api/usage/prefill") {
      const prefill = await fetchEstimatePrefill(
        { user: "default", role: "admin" },
        undefined,
        request.signal,
      ).catch(() => null);
      return jsonResponse(prefill ?? { rates: LIST_RATES }, request);
    }

    const usageRes = await fetchUsageSummary({
      from: fromMs,
      to: toMs,
      signal: request.signal,
    });
    const { input, summary } = usageRes;
    const points = bucketUsage(input.rows, {
      from: fromMs,
      to: toMs,
      by: days <= 2 ? "hour" : "day",
    });
    const costLines = buildSevenLineCostTable({
      hours: days * 24,
      settings: input.settings,
      billedGbHours: summary.storage.billedGbMonths * 720,
      classA: summary.totals.classA,
      classB: summary.totals.classB,
      cdnHits: summary.totals.cdnHit,
      loggedCdnBytes: summary.bytesRead,
      loggedComputeRequests: summary.computeRequests,
      loggedComputeVcpuMs: summary.computeVcpuMs,
      loggedComputeSegments: summary.computeSegments,
      billing: null,
    });

    if (pathname === "/api/usage/current-storage") {
      return jsonResponse(
        {
          total_billed_gb_hours: summary.storage.billedGbMonths * 720,
          live_bytes: summary.storage.totalBytes,
          deleted_bytes: 0,
          regular_log_bytes:
            summary.storage.datasets.find((d) => d.dataset === "pq_request")
              ?.bytes ?? 0,
          rum_bytes:
            summary.storage.datasets.find((d) => d.dataset === "pq_rum")
              ?.bytes ?? 0,
          compute_requests: summary.computeRequests,
          compute_vcpu_ms: summary.computeVcpuMs,
          compute_billed_vcpu_ms: summary.computeBilledVcpuMs ?? 0,
          kv_ops: summary.kvOps,
          cache_lookups: summary.cacheLookups,
          cache_replaces: summary.cacheReplaces,
          purges: summary.purges,
          incurred: costLines,
          reconciliation: summary.storage,
          seven_line_table: costLines.lines,
        },
        request,
      );
    }
    if (pathname === "/api/usage/operations") {
      return jsonResponse(
        {
          total_class_a: summary.totals.classA,
          total_class_b: summary.totals.classB,
          data: points.map((p) => ({
            date: new Date(p.t).toISOString(),
            class_a: p.classA,
            class_b: p.classB,
            kv_class_a: p.kvClassA,
            kv_class_b: p.kvClassB,
            kv_usd: p.kvUsd,
          })),
        },
        request,
      );
    }
    if (pathname === "/api/usage/bandwidth") {
      return jsonResponse(
        {
          total_bytes: summary.bytesRead,
          data: points.map((p) => ({
            time: new Date(p.t).toISOString(),
            bandwidth_bytes: p.bytesOut,
          })),
        },
        request,
      );
    }
    if (pathname === "/api/usage/log-activity") {
      return jsonResponse(
        {
          data: points.map((p) => ({
            time: new Date(p.t).toISOString(),
            row_count: p.ingestedRows,
            api_requests: p.requests,
          })),
        },
        request,
      );
    }
    if (pathname === "/api/usage/rum-breakdown") {
      return jsonResponse(
        {
          total_events: 0,
          total_bytes:
            summary.storage.datasets.find((d) => d.dataset === "pq_rum")
              ?.bytes ?? 0,
          data: [],
        },
        request,
      );
    }
  }

  // Page 6 & 7: Operational Logs, Schema & Usage Log
  if (pathname === "/api/cron-runs" && method === "DELETE") {
    const res = await deleteLogHistory("cron_runs");
    return jsonResponse(res, request);
  }
  if (pathname === "/api/cron-runs" && method === "GET") {
    const now = Date.now();
    const res = await queryOpsLogs(
      "cron_runs",
      {
        from: new Date(now - 7 * 86_400_000).toISOString(),
        to: new Date(now).toISOString(),
      },
      { signal: request.signal },
    );
    return jsonResponse(
      {
        entries: res.rows,
        total: res.rows.length,
        status: res.status,
        stats: res.stats,
      },
      request,
    );
  }
  if (pathname === "/api/cron-schedule" && method === "GET") {
    return jsonResponse(
      {
        tasks: [
          { name: "ingest", interval_s: 120, enabled: true },
          { name: "rum_ingest", interval_s: 120, enabled: true },
          { name: "ops_flush", interval_s: 120, enabled: true },
          { name: "rollup", interval_s: 120, enabled: true },
          { name: "retention", interval_s: 3600, enabled: true },
          { name: "reconcile", interval_s: 3600, enabled: true },
        ],
      },
      request,
    );
  }
  if (pathname === "/api/audit-logs" && method === "GET") {
    const now = Date.now();
    const res = await queryOpsLogs(
      "admin_audit",
      {
        from: new Date(now - 30 * 86_400_000).toISOString(),
        to: new Date(now).toISOString(),
      },
      { signal: request.signal },
    );
    return jsonResponse(
      { entries: res.rows, total: res.rows.length },
      request,
    );
  }
  if (pathname === "/api/admin/ingested-files" && method === "GET") {
    const now = Date.now();
    const res = await queryOpsLogs(
      "ingest_files",
      {
        from: new Date(now - 7 * 86_400_000).toISOString(),
        to: new Date(now).toISOString(),
      },
      { signal: request.signal },
    );
    return jsonResponse({ files: res.rows, total: res.rows.length }, request);
  }
  if (pathname === "/api/admin/metadata-storage" && method === "GET") {
    const [status, logging, quarantine, cacheStats] = await Promise.all([
      getLogsStatus().catch(() => null),
      getOperationalLogging().catch(() => null),
      getQuarantine().catch(() => null),
      getStoreStats().catch(() => null),
    ]);
    return jsonResponse(
      {
        status,
        logging,
        quarantine,
        browser_cache: cacheStats,
        kinds: OPS_KINDS,
      },
      request,
    );
  }
  if (pathname === "/api/admin/metadata-cleanup" && method === "POST") {
    const body = (await request.clone().json().catch(() => ({}))) as {
      action?: string;
      kind?: OpsKind;
      target?: string;
      record?: string;
    };
    if (body.action === "sweep_retention") {
      return jsonResponse(await sweepRetention(), request);
    }
    if (body.action === "reconcile_storage") {
      return jsonResponse(await reconcileStorage(), request);
    }
    if (body.action === "purge_quarantine") {
      const target =
        body.target && body.record
          ? { target: body.target, record: body.record }
          : ({ target: "all" } as const);
      return jsonResponse(await purgeQuarantine(target), request);
    }
    if (body.action === "backfill") {
      return jsonResponse(await startBackfill(), request);
    }
    if (body.kind) {
      return jsonResponse(await deleteLogHistory(body.kind), request);
    }
    return jsonResponse({ status: "ok" }, request);
  }
  if (pathname === "/api/admin/metadata-retention" && method === "PATCH") {
    const body = (await request.clone().json().catch(() => ({}))) as Record<
      string,
      boolean
    >;
    return jsonResponse(await patchOperationalLogging(body), request);
  }
  if (pathname === "/api/admin/usage-log" && method === "GET") {
    const now = Date.now();
    const [fosRes, cronRes, traffic] = await Promise.all([
      queryOpsLogs(
        "fos_usage",
        {
          from: new Date(now - 7 * 86_400_000).toISOString(),
          to: new Date(now).toISOString(),
        },
        { signal: request.signal },
      ),
      queryOpsLogs(
        "cron_runs",
        {
          from: new Date(now - 7 * 86_400_000).toISOString(),
          to: new Date(now).toISOString(),
        },
        { signal: request.signal },
      ),
      getLoggedTraffic(request.signal).catch(() => null),
    ]);
    const rows =
      fosRes.rows.length > 0
        ? fosRes.rows
        : deriveFosUsageFromCronRuns(cronRes.rows as unknown as CronRunRow[]);
    return jsonResponse(
      {
        entries: rows,
        total: rows.length,
        traffic,
      },
      request,
    );
  }
  if (pathname === "/api/schema" && method === "GET") {
    const catRes = await fetch("/api/log-fields/catalog", {
      signal: request.signal,
    }).catch(() => null);
    const catalog = catRes?.ok
      ? ((await catRes.json().catch(() => ({ fields: [] }))) as {
          fields?: Array<{
            name?: string;
            id?: string;
            arrow_type?: string;
            type?: string;
            group?: string;
            description?: string;
          }>;
        })
      : { fields: [] };
    const cols = parquetColumns(
      (catalog.fields ?? []).map((f) => ({
        name: f.name ?? f.id ?? "unknown",
        arrow_type: f.arrow_type ?? f.type ?? "Utf8",
        group: f.group ?? "core",
        description: f.description ?? "",
      })),
    );
    return jsonResponse({ columns: cols, sampleStats: null }, request);
  }

  return null;
}

const TRANSIENT_JOB_ERRORS = new Set([
  "service_locked",
  "job_leased",
  "generation_conflict",
  "state_propagating",
  "state_write_failed",
  "state_unavailable",
]);

async function stepJobToCompletion(
  jobId: string,
  controller: ReadableStreamDefaultController<Uint8Array>,
  encoder: TextEncoder,
  signal?: AbortSignal,
): Promise<void> {
  const emit = (payload: Record<string, unknown>) => {
    controller.enqueue(encoder.encode(`data: ${JSON.stringify(payload)}\n\n`));
  };

  let transientRetries = 0;
  while (!signal?.aborted) {
    const res = await fetch(
      `/api/admin/jobs/${encodeURIComponent(jobId)}/step`,
      {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: "{}",
        credentials: "include",
        signal,
      },
    );

    if (!res.ok) {
      const errBody = (await res.json().catch(() => ({}))) as {
        error?: string;
        detail?: string;
      };
      const code = errBody.error ?? errBody.detail ?? "";
      if (TRANSIENT_JOB_ERRORS.has(code) && transientRetries < 12) {
        transientRetries += 1;
        await new Promise((r) => setTimeout(r, 350));
        continue;
      }
      emit({
        type: "error",
        status: "failed",
        error: code || `Job step failed (${res.status})`,
        message: code || `Job step failed (${res.status})`,
      });
      return;
    }

    transientRetries = 0;
    const data = (await res.json()) as {
      job?: {
        id: string;
        status: string;
        step?: string;
        completed_steps?: string[];
        error?: string | null;
        retry_at?: number | null;
      };
    };
    const job = data.job;
    if (!job) {
      emit({
        type: "error",
        status: "failed",
        error: "Missing job payload from step endpoint",
      });
      return;
    }

    emit({
      type: "status",
      status: job.status,
      step: job.step ?? "running",
      completed_steps: job.completed_steps ?? [],
      message: job.step ? `Step: ${job.step}` : "Running...",
    });

    if (job.status === "complete") {
      emit({ type: "result", data: job });
      emit({ type: "done", status: "complete", job });
      return;
    }
    if (job.status === "failed" || job.status === "error") {
      emit({
        type: "error",
        status: "failed",
        error: job.error ?? "Job failed",
        message: job.error ?? "Job failed",
      });
      return;
    }

    const waitMs = job.retry_at
      ? Math.max(350, job.retry_at * 1000 - Date.now())
      : 350;
    await new Promise((r) => setTimeout(r, Math.min(waitMs, 2_000)));
  }
}

export async function cloudStreamFetch(
  input: RequestInfo | URL,
  init?: RequestInit,
): Promise<Response> {
  const rawUrl =
    typeof input === "string"
      ? input
      : input instanceof URL
        ? input.toString()
        : input.url;
  const url = new URL(rawUrl, "http://localhost");
  const { pathname } = url;
  const method = (init?.method ?? "GET").toUpperCase();

  // Pass through standard GET SSE streams (e.g. /api/ingest/events)
  if (method === "GET") {
    return fetch(input, init);
  }

  const encoder = new TextEncoder();
  const signal = init?.signal ?? undefined;

  const stream = new ReadableStream<Uint8Array>({
    async start(controller) {
      const emit = (payload: Record<string, unknown>) => {
        controller.enqueue(
          encoder.encode(`data: ${JSON.stringify(payload)}\n\n`),
        );
      };
      try {
        if (pathname === "/api/provision/ingest") {
          await fetch("/api/cron-runs/trigger", {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: "{}",
            credentials: "include",
            signal,
          });
          emit({ type: "status", status: "complete", message: "Triggered ingest tick" });
          emit({ type: "done", status: "complete" });
          controller.close();
          return;
        }

        const initRes = await fetch(input, {
          ...init,
          credentials: "include",
        });
        if (!initRes.ok) {
          const errJson = (await initRes.json().catch(() => ({}))) as {
            detail?: string;
            error?: string;
          };
          emit({
            type: "error",
            status: "failed",
            error:
              errJson.detail ??
              errJson.error ??
              `Request failed (${initRes.status})`,
          });
          controller.close();
          return;
        }

        const initPayload = (await initRes.json().catch(() => ({}))) as {
          job?: { id: string; status?: string };
        };
        if (initPayload.job?.id) {
          await stepJobToCompletion(
            initPayload.job.id,
            controller,
            encoder,
            signal,
          );
        } else {
          emit({ type: "result", data: initPayload });
          emit({ type: "done", status: "complete", data: initPayload });
        }
      } catch (err) {
        if (!signal?.aborted) {
          emit({
            type: "error",
            status: "failed",
            error: err instanceof Error ? err.message : String(err),
          });
        }
      } finally {
        try {
          controller.close();
        } catch {
          /* already closed */
        }
      }
    },
  });

  return new Response(stream, {
    status: 200,
    headers: { "content-type": "text/event-stream" },
  });
}

export {
  evaluateCapacity,
  estimateMonthlyUsage,
  parquetColumns,
  reconcileSnapshot,
  sampleStats,
  setDashboardCacheIdentity,
};
