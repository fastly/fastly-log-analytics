import { z } from "zod";
import {
  runInWorker,
  searchFieldSummariesInWorker,
  searchFieldValuesInWorker,
  setDashboardCacheIdentity,
} from "./dashboard/client";

z.config({ jitless: true });
import { assetsScanColumns, handleAssetsRequest, type AssetsQueryParams } from "./assets";
import { handleCmcdRequest, type CmcdQueryParams } from "./cmcd";
import type { DashboardFilter } from "./dashboard/filters";
import type { MinuteBucket } from "./dashboard/engine";
import { parseVcpuMsHeader } from "./dashboard/sources";
import { customCards, type CardField, type CustomField } from "./dashboard/summaries";
import {
  handleInsightAvailability,
  type CacheCollapseDetailRequest,
  type InsightsRequest,
} from "./insights";
import { insightScanColumns, queryCloudInsights, queryCloudCacheCollapse } from "./insight-scans";
import { CLOUD_SCORING_SCAN_ROUTES, cloudScoringRead } from "./scoring-reads";
import { handleNetworkRequest, networkScanColumns, type NetworkQueryParams } from "./network";
import { handleOriginRequest, originScanColumns } from "./origin";
import { handlePerformanceRequest, performanceScanColumns } from "./performance";
import {
  executeCloudQuery,
  getQueryPresets,
  getQuerySchema,
  type QueryRequest,
} from "./query";
import { handleRumRequest, UnsupportedRumFilterError, type RumQueryParams } from "./rum";
import { cloudRumAnalytics, cloudRumLiveEvents, cloudRumUsage } from "./rum-wire";
import { cloudLogFieldsCatalog, cloudSchema } from "./log-fields";
import { parquetColumns, sampleStats } from "./schema";
import { isNativeCloudRoute } from "./native-routes";
import { handleSavedViews } from "./saved-views";
import { cloudServiceStatus } from "./service-bridges";
import { cloudRumMutation } from "./rum-management";
import { cloudLogAccounting, cloudQuarantine } from "./admin-reads";
import { cloudAdminWire } from "./admin-wire";
import { beginCloudLoading, type LoadingStage } from "./loading-progress";
import { bindCloudReadContext, CloudBudgetError, cloudReadContext, ensureCloudRange } from "./range-loader";
import { captureCloudQueryContext, measureCloudQuery, publishCloudWorkerTelemetry, type CloudQueryContext } from "./readiness";
import { compileFiltersToSql, FIELD_DUCKDB_TYPES, getDuckDBEngine, type DuckDBReadContext } from "./dashboard/duckdb";
import type { components } from "@/types/api.generated";
import { handleSecurityRequest } from "./security";
import {
  handleSessionsDetail,
  handleSessionsList,
  openSessionToken,
  type SessionDetailRequest,
  type SessionsListRequest,
} from "./sessions";
import {
  deleteLogHistory,
  purgeQuarantine,
  reconcileStorage,
  startBackfill,
  sweepRetention,
} from "./telemetry/admin";
import { queryOpsLogs } from "./telemetry/client";
import type { OpsKind } from "./telemetry/kinds";
import { evaluateCapacity } from "./usage/capacity";
import { estimateMonthlyUsage } from "./usage/estimate";
import { fetchEstimatePrefill } from "./usage/estimatePrefill";
import { LIST_RATES } from "./usage/rates";
import { bucketUsage } from "./usage/series";
import {
  buildSevenLineCostTable,
  fetchUsageSettings,
  fetchUsageSummary,
  reconcileSnapshot,
} from "./usage/summary";
import { handleValueRequest, valueScanColumns, type ValueQueryParams } from "./value";
import {
  useDebugStore,
  useFilterStore,
  useSessionRoleStore,
} from "@/stores/filterStore";

export { parseVcpuMsHeader };
export { IS_CLOUD_MODE, isCloudMode, setCloudModeRuntime } from "./mode";

type BucketInterval = "1s" | "1m" | "5m" | "1h" | "1d";

const DASHBOARD_PATH = /^\/api\/(?:dashboard\/(?:bundle|aggregates)|aggregates|timeseries)$/;
const ROLLUP_CHART_METRICS = new Map([
  ["requests", "COUNT(*)"],
  ["5xx", "COUNT(*) FILTER (WHERE status >= 500 AND status < 600)"],
  ["4xx", "COUNT(*) FILTER (WHERE status >= 400 AND status < 500)"],
  ["hit_rate", "ROUND(100.0 * COUNT(*) FILTER (WHERE cache ILIKE 'HIT%') / NULLIF(COUNT(*), 0), 2)"],
  ["bandwidth", "SUM(COALESCE(resp_bytes, 0))"],
  ["error_rate", "100.0 * COUNT(*) FILTER (WHERE status >= 400 AND status < 600) / NULLIF(COUNT(*), 0)"],
]);
const RAW_CHART_METRICS = new Map([
  ["p50_latency", "QUANTILE_CONT(elapsed, 0.5) / 1000.0"],
  ["p95_latency", "QUANTILE_CONT(elapsed, 0.95) / 1000.0"],
  ["p99_latency", "QUANTILE_CONT(elapsed, 0.99) / 1000.0"],
  ["ttfb", "QUANTILE_CONT(COALESCE(ottfb / 1000.0, ttfb * 1000.0), 0.95)"],
  ["ttfb_ms", "MEDIAN(ttfb) * 1000.0"],
  ["req_size", "MEDIAN(CASE WHEN req_bytes IS NULL AND req_header_bytes IS NULL THEN NULL ELSE COALESCE(req_bytes, 0) + COALESCE(req_header_bytes, 0) END)"],
  ["throughput", "SUM(CASE WHEN cache ILIKE 'HIT%' THEN COALESCE(resp_bytes, 0) ELSE 0 END)"],
]);

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
  serviceId: string | null = null,
  origin?: CloudQueryContext,
): Promise<void> {
  publishCloudWorkerTelemetry(payload, serviceId, origin);
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
  const aliases: Record<string, BucketInterval> = {
    "1 second": "1s",
    "1 minute": "1m",
    "5 minutes": "5m",
    "1 hour": "1h",
    "1 day": "1d",
  };
  if (raw && aliases[raw]) return aliases[raw];
  if (raw === "1s" || raw === "1m" || raw === "5m" || raw === "1h" || raw === "1d") {
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

function unsupportedCloudEndpoint(request: Request): Response {
  return jsonResponse({
    detail: { error: "cloud_endpoint_unsupported", method: request.method,
      path: new URL(request.url).pathname },
  }, request, undefined, 501);
}

async function withLoading<T>(stage: LoadingStage, work: () => Promise<T>): Promise<T> {
  const loading = beginCloudLoading(stage);
  let success = false;
  try {
    const result = await work();
    success = true;
    return result;
  } finally {
    loading.finish(success);
  }
}

// Every Cloud scan needs the extents, catalog and services lookups, and a page
// load runs many scans. Share one answer while it keeps being used within a short
// window, so a long page load reads it once, but never past a hard cap; writes clear it.
const METADATA_TTL_MS = 15_000;
const METADATA_MAX_AGE_MS = 60_000;
type MetadataSnapshot = { status: number; contentType: string | null; body: string };
const metadataCache = new Map<string, { at: number; usedAt: number; value: Promise<MetadataSnapshot> }>();

export function resetCloudMetadataCache(): void {
  metadataCache.clear();
}

async function fetchCloudMetadata(path: string, signal: AbortSignal): Promise<Response> {
  signal.throwIfAborted();
  const now = Date.now();
  let entry = metadataCache.get(path);
  if (entry && now - entry.usedAt <= METADATA_TTL_MS && now - entry.at < METADATA_MAX_AGE_MS) {
    entry.usedAt = now;
  } else {
    const value = fetch(path).then(async (response) => ({
      status: response.status, contentType: response.headers.get("content-type"), body: await response.text(),
    }));
    const created = { at: now, usedAt: now, value };
    entry = created;
    metadataCache.set(path, created);
    const evict = () => { if (metadataCache.get(path) === created) metadataCache.delete(path); };
    value.then((snapshot) => { if (snapshot.status < 200 || snapshot.status > 299) evict(); }, evict);
  }
  let onAbort!: () => void;
  const aborted = new Promise<never>((_, reject) => {
    onAbort = () => reject(signal.reason);
    signal.addEventListener("abort", onAbort, { once: true });
  });
  try {
    const snapshot = await Promise.race([entry.value, aborted]);
    return new Response(snapshot.body, {
      status: snapshot.status,
      headers: snapshot.contentType ? { "content-type": snapshot.contentType } : undefined,
    });
  } finally {
    signal.removeEventListener("abort", onAbort);
  }
}

async function loadCloudCatalog(request: Request) {
  const response = await fetchCloudMetadata("/api/log-fields/catalog", request.signal);
  if (!response.ok) return response;
  const catalog = await response.json() as components["schemas"]["LogFieldsCatalogResponse"];
  const serviceId = new URL(request.url).searchParams.get("service_id") ??
    request.headers.get("x-service-id");
  if (!serviceId) return cloudLogFieldsCatalog(catalog);
  const services = await fetchCloudMetadata("/api/services", request.signal);
  if (!services.ok) return services;
  const payload = await services.json() as {
    services?: Array<{ service_id: string; log_fields?: { custom_fields?: Array<Record<string, unknown>> } }>;
  };
  const service = payload.services?.find((item) => item.service_id === serviceId);
  if (!service) return jsonResponse({ detail: { error: "service_not_found" } }, request, undefined, 404);
  return cloudLogFieldsCatalog(catalog, service.log_fields);
}

let analyticalQueue = Promise.resolve();
/** Needed-column bytes a speculative Insights prefetch may load. */
const PREFETCH_PROJECTION_BYTES = 32 * 1024 * 1024;

function isLocalScan(path: string, method: string): boolean {
  if (!["GET", "POST"].includes(method)) return false;
  const scoring = path.match(/^\/api\/services\/[^/]+\/scoring\/(.+)$/);
  if (scoring) return method === "GET" && (CLOUD_SCORING_SCAN_ROUTES.has(scoring[1]) || /^sessions\/[^/]+\/events$/.test(scoring[1]));
  if (path === "/api/insight-availability") return method === "GET";
  return /^\/api\/(?:origin\/(?:aggregates|summary|timeseries|slow-urls|status-codes|path-breakdown|pop-latency|ip-health|shielding-analysis|drilldown)|performance(?:\/aggregates)?|security\/(?:aggregates|bundle|top-bots|proxies(?:\/export)?|threat-intel)|network(?:\/(?:bundle|pop-health)|-health|-quality)|value\/summary|assets\/aggregates|cmcd\/aggregates|sessions(?:\/detail)?|query|insights(?:\/cache-collapse-detail)?|usage\/rum-breakdown|rum\/(?:analytics|live-events|beacon-health))$/.test(path) ||
    /^\/api\/services\/[^/]+\/rum\/(?:analytics|live-events|beacon-health)$/.test(path);
}

async function prepareCloudScan(request: Request): Promise<Request> {
  const engine = getDuckDBEngine();
  const generation = await engine.beginRequest();
  generation.assertCurrent();
  request.signal.throwIfAborted();
  const url = new URL(request.url);
  const body = request.method === "POST"
    ? await request.clone().json() as Record<string, unknown> : {};
  generation.assertCurrent();
  request.signal.throwIfAborted();
  const params = { ...Object.fromEntries(url.searchParams), ...body };
  const token = typeof params.session_token === "string" ? openSessionToken(params.session_token) : null;
  let end = String(params.end_time ?? params.end ?? params.clamp_end ?? token?.e ?? "");
  if (!end) {
    const response = await withLoading("range", () =>
      fetchCloudMetadata("/api/log-extents", request.signal));
    if (!response.ok) throw new Error(`Cloud range discovery failed (${response.status})`);
    const extents = await response.json() as { latest_log_at?: string | null };
    generation.assertCurrent();
    request.signal.throwIfAborted();
    end = extents.latest_log_at ?? new Date().toISOString();
  }
  const scoring = /\/scoring\//.test(url.pathname);
  const historyHours = url.pathname.startsWith("/api/insights")
    ? Number(params.window_size_hrs ?? 24) + Number(params.baseline_hours ?? 168)
    : scoring ? Number(params.since_hours ?? (url.pathname.includes("/sessions/") ? Number(params.since_days ?? 30) * 24 : 24)) : 24;
  const start = String(params.start_time ?? params.start ?? token?.s ??
    new Date(Date.parse(end) - historyHours * 3_600_000).toISOString());
  if (!Number.isFinite(Date.parse(start)) || !Number.isFinite(Date.parse(end)) ||
      Date.parse(start) > Date.parse(end) || !token && Date.parse(start) === Date.parse(end)) {
    throw new Error("Invalid Cloud query time range");
  }
  const dataset = params.dataset;
  const rum = /\/rum\//.test(url.pathname) || url.pathname === "/api/usage/rum-breakdown" ||
    dataset === "client_vitals" || dataset === "client_errors";
  const scanStart = url.pathname.startsWith("/api/insights")
    ? new Date(Math.min(Date.parse(start), Date.parse(end) - historyHours * 3_600_000)).toISOString() : start;
  const serviceId = url.pathname.match(/^\/api\/services\/([^/]+)\//)?.[1] ??
    params.service_id ?? request.headers.get("x-service-id") ?? token?.svc ?? undefined;
  let customFields: CustomField[] = [];
  if (!rum && serviceId) {
    const catalogUrl = new URL("/api/log-fields/catalog", request.url);
    catalogUrl.searchParams.set("service_id", decodeURIComponent(String(serviceId)));
    const catalog = await loadCloudCatalog(new Request(catalogUrl, request));
    generation.assertCurrent();
    request.signal.throwIfAborted();
    if (catalog instanceof Response) throw new Error(`Cloud field discovery failed (${catalog.status})`);
    customFields = (catalog.fields ?? []).filter((field) => field.is_custom).map((field) => ({
      id: field.id,
      type: /BOOL/i.test(String(field.sql_type)) ? "boolean"
        : /INT|FLOAT|DOUBLE|DECIMAL|REAL/i.test(String(field.sql_type)) ? "number" : "string",
    }));
  }
  // Detail and arbitrary SQL may explicitly include their upper endpoint.
  const scanEnd = token || url.pathname === "/api/query" ? new Date(Date.parse(end) + 1).toISOString() : end;
  const insights = url.pathname === "/api/insights" || url.pathname === "/api/insight-availability" ||
    url.pathname === "/api/insights/cache-collapse-detail";
  const filters = url.pathname === "/api/insights"
    ? parseFiltersPayload(params.filters as Parameters<typeof parseFiltersPayload>[0], undefined) : [];
  const scanColumns = /^\/api\/network(?:\/(?:bundle|pop-health)|-health|-quality)$/.test(url.pathname)
    ? networkScanColumns(params.filters)
    : url.pathname.startsWith("/api/origin/") ? originScanColumns(url.pathname, params)
    : url.pathname === "/api/value/summary" ? valueScanColumns(params)
    : url.pathname === "/api/assets/aggregates" ? assetsScanColumns(params.filters)
    : /^\/api\/performance(?:\/aggregates)?$/.test(url.pathname) ? performanceScanColumns(params.filters) : null;
  const read = await ensureCloudRange(scanStart, scanEnd, request.signal, rum ? ["rum"] : ["request"], {
    serviceId: serviceId ? decodeURIComponent(String(serviceId)) : undefined, customFields,
    ...insights ? { projection: {
      columns: insightScanColumns(filters),
      ...request.headers.get("x-flp-cloud-prefetch") === "1" ? { maxBytes: PREFETCH_PROJECTION_BYTES } : {},
    } } : scanColumns ? { projection: { columns: scanColumns } } : {},
  });
  try {
    generation.assertCurrent();
    request.signal.throwIfAborted();
    if (request.method === "POST") {
      return bindCloudReadContext(new Request(request, { body: JSON.stringify({ ...params,
        start_time: params.start_time ?? start, end_time: params.end_time ?? end,
        ...(serviceId ? { service_id: decodeURIComponent(String(serviceId)) } : {}),
        ...(url.pathname.startsWith("/api/insights") ? { clamp_end: end } : {}),
      }) }), read);
    }
    if (!url.searchParams.has("start_time")) url.searchParams.set("start_time", start);
    if (!url.searchParams.has("end_time")) url.searchParams.set("end_time", end);
    if (serviceId && !url.searchParams.has("service_id")) url.searchParams.set("service_id", decodeURIComponent(String(serviceId)));
    return bindCloudReadContext(new Request(url, request), read);
  } catch (error) {
    read.release?.();
    throw error;
  }
}

export async function handleCloudAnalyticalRequest(
  request: Request,
): Promise<Response | null> {
  const url = new URL(request.url);
  const serviceId = url.searchParams.get("service_id") ?? request.headers.get("x-service-id");
  const origin = captureCloudQueryContext();
  const handle = () => handleCloudRequest(request, origin).catch((error: unknown) => {
    if (!(error instanceof CloudBudgetError)) throw error;
    return Response.json({ detail: error.message }, { status: 413 });
  });
  // A speculative prefetch is not part of the page's selected result; its budget miss is expected.
  if (request.headers.get("x-flp-cloud-prefetch") === "1") return handle();
  return measureCloudQuery(`api.${url.pathname}`, handle, undefined, serviceId);
}

async function handleCloudRequest(
  request: Request,
  origin?: CloudQueryContext,
): Promise<Response | null> {
  const url = new URL(request.url);
  const path = url.pathname;
  if (!["GET", "HEAD"].includes(request.method) && !isLocalScan(path, request.method)) resetCloudMetadataCache();
  if (request.method === "POST" && /^\/api\/services\/[^/]+\/rum\/(?:enable|disable|upgrade|settings)$/.test(path)) {
    return cloudRumMutation(request, (jobId, signal) => {
      const encoder = new TextEncoder();
      return new Response(new ReadableStream<Uint8Array>({
        async start(controller) {
          try {
            await stepJobToCompletion(jobId, controller, encoder, signal);
            controller.close();
          } catch (error) {
            controller.error(error);
          }
        },
      }), { headers: { "content-type": "text/event-stream" } });
    });
  }
  if (/^\/api\/(?:cron-runs|cron-schedule|audit-logs|admin\/(?:usage-log(?:\/export)?|raw-tree|storage-tree|ingested-files|quarantine\/purge))$/.test(path)) {
    const administrative = await withLoading("listing", () => cloudAdminWire(request));
    if (administrative) return administrative;
  }
  if (request.method === "GET" && path === "/api/admin/log-accounting") return cloudLogAccounting(request);
  if (request.method === "GET" && ["/api/admin/quarantine", "/api/admin/quarantine/summary"].includes(path)) {
    return withLoading("listing", () => cloudQuarantine(request));
  }
  if (/^\/api\/services\/[^/]+\/scoring\//.test(path) && !isLocalScan(path, request.method)) {
    return (await cloudScoringRead(request)) ?? unsupportedCloudEndpoint(request);
  }
  if (/^\/api\/services\/[^/]+\/(?:rum|cmcd)\/(?:status|versions)$/.test(path)) {
    const response = await withLoading("session", () => cloudServiceStatus(request));
    return response ?? unsupportedCloudEndpoint(request);
  }
  if (/^\/api\/(?:origin|performance|security|network|value|assets|cmcd|rum)(?:\/|-)/.test(path) &&
      !isLocalScan(path, request.method) && !isNativeCloudRoute(request.method, path)) {
    return unsupportedCloudEndpoint(request);
  }
  if (/^\/api\/admin\/(?:health-snapshot|metric-history(?:\/batch)?|bot-sources|system-jobs|pop-locations)$/.test(path)) {
    return unsupportedCloudEndpoint(request);
  }
  if (path.startsWith("/api/views")) return (await handleSavedViews(request)) ?? unsupportedCloudEndpoint(request);
  if (request.method === "GET" && ["/api/log-fields/catalog", "/api/schema"].includes(path)) {
    const catalog = await withLoading("listing", () => loadCloudCatalog(request));
    if (catalog instanceof Response) return catalog;
    return jsonResponse(path === "/api/schema" ? cloudSchema(catalog) : catalog, request);
  }
  const stage = path === "/api/bootstrap" || path === "/api/auth/session" || path === "/api/services"
    ? "session" : path === "/api/log-extents" ? "range" : path === "/api/files" ? "listing" : null;
  if (stage && path !== "/api/bootstrap" && isNativeCloudRoute(request.method, path)) {
    // The server answers extents for its one active service, so the page's
    // own read can share the lookup its scans make.
    if (path === "/api/log-extents" && !url.search) {
      return withLoading(stage, () => fetchCloudMetadata(path, request.signal));
    }
    return withLoading(stage, () => fetch(request));
  }
  if ((request.method !== "GET" && (
    /^\/api\/services\/[^/]+\/(?:rum|cmcd)\//.test(path) && !isLocalScan(path, request.method) ||
    /^\/api\/services\/[^/]+\/scoring\//.test(path) &&
      !/\/(?:analytics|evaluation|threshold-preview|latency-timeseries|score-distribution|top-flagged|compliance-breakdown|audit|curves|health|events)$/.test(path)
  )) || /^\/api\/services\/[^/]+\/(?:rum|cmcd)\/(?:enable|disable|upgrade|settings)$/.test(path)) {
    return unsupportedCloudEndpoint(request);
  }

  const work = async () => {
    let localScan = isLocalScan(path, request.method);
    if (request.method === "POST" && DASHBOARD_PATH.test(path)) {
      const body = await request.clone().json() as {
        chart_metric?: string; metric?: string; chart_interval?: string; interval?: string;
      };
      const chartInterval = body.chart_interval ?? body.interval;
      localScan ||= RAW_CHART_METRICS.has(body.chart_metric ?? body.metric ?? "requests") ||
        chartInterval === "1 second" || chartInterval === "1s";
    }
    if (localScan) {
      const previous = analyticalQueue;
      let release!: () => void;
      analyticalQueue = new Promise<void>((resolve) => { release = resolve; });
      await previous;
      const loading = beginCloudLoading("files");
      let success = false;
      let read: DuckDBReadContext | undefined;
      try {
        request.signal.throwIfAborted();
        const prepared = await prepareCloudScan(request);
        read = cloudReadContext(prepared);
        const response = /\/scoring\//.test(path)
          ? await cloudScoringRead(prepared) : await dispatchCloudRequest(prepared, origin, cloudReadContext(prepared));
        cloudReadContext(prepared).assertCurrent();
        prepared.signal.throwIfAborted();
        success = Boolean(response?.ok);
        return response ?? unsupportedCloudEndpoint(request);
      } finally {
        read?.release?.();
        loading.finish(success);
        release();
      }
    }
    const response = await dispatchCloudRequest(request, origin);
    if (response) return response;
    return !path.startsWith("/api/") || isNativeCloudRoute(request.method, path)
      ? null : unsupportedCloudEndpoint(request);
  };
  return stage ? withLoading(stage, work) : work();
}

async function dispatchCloudRequest(request: Request, origin?: CloudQueryContext, read?: DuckDBReadContext): Promise<Response | null> {
  const executeSql = (sql: string, params: readonly unknown[] = [], signal?: AbortSignal) =>
    (read ?? cloudReadContext(request)).executeSql<Record<string, unknown>>(sql, params, signal);
  const getPresentColumns = (keys?: readonly string[], signal?: AbortSignal) => {
    signal?.throwIfAborted();
    return (read ?? cloudReadContext(request)).getPresentColumns(keys);
  };
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

  // Synthesize /api/bootstrap from Cloud Compute's /api/auth/session and /api/services
  if (pathname === "/api/bootstrap" && method === "GET") {
    const sessionRes = await fetch("/api/auth/session", {
      credentials: "include",
      signal: request.signal,
    });
    if (!sessionRes.ok && sessionRes.status !== 401) return sessionRes;
    if (!sessionRes || !sessionRes.ok) {
      return jsonResponse(
        {
          runtime_mode: "cloud",
          user: null,
          services: [],
          active_service_id: null,
          settings: {
            needs_login: true,
            is_remote_analyst: false,
            initializing: false,
          },
          debug_state: {
            query_debug_visibility: "disabled",
            api_call_debug_visibility: "disabled",
          },
        },
        request,
      );
    }
    const sessionData = (await sessionRes.json().catch(() => ({}))) as {
      user?: { id?: string; display_name?: string; role?: string } | null;
      query_debug_visibility?: string;
      api_call_debug_visibility?: string;
    };
    const user = sessionData.user ?? null;
    if (!user) {
      return jsonResponse(
        {
          runtime_mode: "cloud",
          user: null,
          services: [],
          active_service_id: null,
          settings: {
            needs_login: true,
            is_remote_analyst: false,
            initializing: false,
          },
          debug_state: {
            query_debug_visibility: "disabled",
            api_call_debug_visibility: "disabled",
          },
        },
        request,
      );
    }
    const servicesRes = await fetchCloudMetadata("/api/services", request.signal);
    if (!servicesRes.ok) return servicesRes;
    const servicesJson = servicesRes?.ok
      ? ((await servicesRes.json().catch(() => ({ services: [] }))) as {
          services?: Array<{
            service_id: string;
            service_name?: string;
            name?: string;
            rum_enabled?: boolean;
            cmcd?: { enabled?: boolean };
            log_fields?: { selected_fields?: string[] };
          }>;
        })
      : { services: [] };
    const rawServices = servicesJson.services ?? [];
    const isRemoteAnalyst =
      user.role === "analyst" || user.role === "analyst-no-pii";
    const mappedServices = rawServices.map((s) => ({
      ...s,
      service_id: s.service_id,
      name: s.name || s.service_name || s.service_id,
      cmcd_enabled: Boolean(s.cmcd?.enabled),
      rum_enabled: Boolean(s.rum_enabled),
    }));
    const services = mappedServices;
    const requestedService = searchParams.get("service_id") ?? request.headers.get("x-service-id");
    const activeServiceId = services.find((service) => service.service_id === requestedService)?.service_id ??
      services[0]?.service_id ?? null;
    const activeLogFieldIds = rawServices.find((service) => service.service_id === activeServiceId)?.log_fields?.selected_fields ?? [];
    return jsonResponse(
      {
        runtime_mode: "cloud",
        user,
        services,
        active_service_id: activeServiceId,
        active_log_field_ids: activeLogFieldIds,
        settings: {
          needs_login: false,
          is_remote_analyst: isRemoteAnalyst,
          mask_ips: user.role === "analyst-no-pii",
          initializing: false,
        },
        debug_state: {
          query_debug_visibility:
            sessionData.query_debug_visibility ?? "disabled",
          api_call_debug_visibility:
            sessionData.api_call_debug_visibility ?? "disabled",
        },
      },
      request,
    );
  }

  if (pathname === "/api/admin/debug-settings" && method === "GET") {
    const res = await fetch("/api/auth/session", {
      credentials: "include",
      signal: request.signal,
    }).catch(() => null);
    const session = res?.ok
      ? ((await res.json().catch(() => null)) as Record<string, unknown> | null)
      : null;
    return jsonResponse(
      {
        query_debug_visibility:
          typeof session?.query_debug_visibility === "string"
            ? session.query_debug_visibility
            : "disabled",
        api_call_debug_visibility:
          typeof session?.api_call_debug_visibility === "string"
            ? session.api_call_debug_visibility
            : "disabled",
      },
      request,
    );
  }
  if (pathname === "/api/admin/debug-settings" && method === "PATCH") {
    const body = (await request.clone().json().catch(() => ({}))) as Record<
      string,
      unknown
    >;
    const res = await fetch("/api/admin/usage-logging", {
      method: "PATCH",
      headers: { "content-type": "application/json" },
      credentials: "include",
      body: JSON.stringify(body),
      signal: request.signal,
    });
    if (!res.ok) return res;
    const updated = res.ok
      ? ((await res.json().catch(() => ({}))) as Record<string, unknown>)
      : {};
    return jsonResponse(
      {
        query_debug_visibility:
          (body.query_debug_visibility as string | undefined) ?? "disabled",
        api_call_debug_visibility:
          (body.api_call_debug_visibility as string | undefined) ?? "disabled",
        ...updated,
      },
      request,
    );
  }
  if (pathname === "/api/admin/ingest-logs" && method === "POST") {
    const startTime = searchParams.get("start_time");
    const endTime = searchParams.get("end_time");
    if (startTime || endTime) {
      await startBackfill();
    } else {
      const triggered = await fetch("/api/cron-runs/trigger", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: "{}",
        credentials: "include",
        signal: request.signal,
      });
      if (!triggered.ok) return triggered;
    }
    return jsonResponse({ run_id: "cloud-tick", status: "started" }, request);
  }
  if (pathname === "/api/admin/share/banner" && method === "GET") {
    return jsonResponse({ sharing_active: false, public_url: null }, request);
  }
  if (pathname === "/api/admin/share/oauth-providers" && method === "GET") {
    return jsonResponse({ providers: [] }, request);
  }
  if (
    (pathname === "/api/admin/share/status" ||
      pathname === "/api/admin/share/live") &&
    method === "GET"
  ) {
    const [invitesRes, usersRes, servicesRes] = await Promise.all([
      fetch("/api/admin/invites", {
        credentials: "include",
        signal: request.signal,
      }).catch(() => null),
      fetch("/api/admin/users", {
        credentials: "include",
        signal: request.signal,
      }).catch(() => null),
      fetch("/api/services", {
        credentials: "include",
        signal: request.signal,
      }).catch(() => null),
    ]);
    const invitesJson = invitesRes?.ok
      ? ((await invitesRes.json().catch(() => ({ invites: [] }))) as {
          invites?: Array<{
            id: string;
            display_name?: string;
            role?: string;
            created_at?: number | string;
            expires_at?: number | string;
            accepted_at?: number | string | null;
            revoked?: boolean;
          }>;
        })
      : { invites: [] };
    const usersJson = usersRes?.ok
      ? ((await usersRes.json().catch(() => ({ users: [] }))) as {
          users?: Array<{
            id: string;
            display_name?: string;
            role?: string;
            created_at?: number | string;
            revoked?: boolean;
            passkeys?: Array<{ id: string; created_at?: number | string }>;
          }>;
        })
      : { users: [] };
    const servicesJson = servicesRes?.ok
      ? ((await servicesRes.json().catch(() => ({ services: [] }))) as {
          services?: Array<{ service_id: string; name?: string; service_name?: string }>;
        })
      : { services: [] };
    const svcList = (servicesJson.services ?? []).map((s) => ({
      service_id: s.service_id,
      name: s.name || s.service_name || s.service_id,
    }));
    const allServiceIds = svcList.map((s) => s.service_id);
    const toIso = (v: number | string | null | undefined): string | null => {
      if (v == null) return null;
      if (typeof v === "number") {
        return new Date(v > 1e12 ? v : v * 1000).toISOString();
      }
      return String(v);
    };
    const invites = (invitesJson.invites ?? [])
      .filter((i) => !i.revoked)
      .map((i) => ({
        id: i.id,
        name: i.display_name || i.id,
        email: i.role || "analyst",
        auth_method: "passkey",
        created_at: toIso(i.created_at) ?? new Date().toISOString(),
        expires_at: toIso(i.expires_at),
        last_login_at: toIso(i.accepted_at),
        service_ids: allServiceIds,
        pii_policy: { mask_ips: i.role === "analyst-no-pii" },
        allow_concurrent_sessions: true,
      }));
    const sessions = (usersJson.users ?? [])
      .filter((u) => !u.revoked)
      .map((u) => ({
        session_id: u.id,
        invite_id: u.id,
        name: u.display_name || u.id,
        email: u.role || "analyst",
        auth_method: `Passkey (${u.passkeys?.length ?? 0})`,
        ip_address: u.role || "user",
        last_active_time: toIso(u.created_at) ?? new Date().toISOString(),
      }));
    const originUrl =
      typeof window !== "undefined" ? window.location.origin : null;
    return jsonResponse(
      {
        sharing_active: true,
        public_url: originUrl,
        active_session_count: sessions.length,
        max_concurrent_sessions: 100,
        invites,
        sessions,
        services: svcList,
        rate_limits: { lockouts: [], failures: [] },
      },
      request,
    );
  }
  if (pathname === "/api/admin/share/invites" && method === "POST") {
    const body = (await request.clone().json().catch(() => ({}))) as {
      name?: string;
      role?: string;
      pii_policy?: { mask_ips?: boolean };
    };
    const role =
      body.role === "admin" ||
      body.role === "analyst" ||
      body.role === "analyst-no-pii"
        ? body.role
        : body.pii_policy?.mask_ips
          ? "analyst-no-pii"
          : "analyst";
    const res = await fetch("/api/admin/invites", {
      method: "POST",
      headers: { "content-type": "application/json" },
      credentials: "include",
      body: JSON.stringify({
        display_name: body.name || "Analyst",
        role,
      }),
      signal: request.signal,
    });
    const data = await res.json().catch(() => ({}));
    return jsonResponse(data, request, undefined, res.status);
  }
  const revokeInviteMatch = pathname.match(
    /^\/api\/admin\/share\/invites\/([^/]+)$/,
  );
  if (revokeInviteMatch && method === "DELETE") {
    const inviteId = decodeURIComponent(revokeInviteMatch[1]);
    const res = await fetch(
      `/api/admin/invites/${encodeURIComponent(inviteId)}/revoke`,
      {
        method: "POST",
        headers: { "content-type": "application/json" },
        credentials: "include",
        body: "{}",
        signal: request.signal,
      },
    );
    const data = await res.json().catch(() => ({ ok: res.ok }));
    return jsonResponse(data, request, undefined, res.status);
  }
  const bootSessionMatch = pathname.match(
    /^\/api\/admin\/share\/sessions\/([^/]+)\/boot$/,
  );
  if (bootSessionMatch && method === "POST") {
    const userId = decodeURIComponent(bootSessionMatch[1]);
    const res = await fetch(
      `/api/admin/users/${encodeURIComponent(userId)}/revoke`,
      {
        method: "POST",
        headers: { "content-type": "application/json" },
        credentials: "include",
        body: "{}",
        signal: request.signal,
      },
    );
    const data = await res.json().catch(() => ({ ok: res.ok }));
    return jsonResponse(data, request, undefined, res.status);
  }
  if (pathname === "/api/admin/share/audit-logs" && method === "GET") {
    const until = url.searchParams.get("until");
    const since = url.searchParams.get("since");
    const to = until ? Date.parse(until) : Date.now();
    const from = since ? Date.parse(since) : to - 30 * 86_400_000;
    const limit = Number(url.searchParams.get("limit") ?? 200);
    if (!Number.isInteger(limit) || limit < 1 || limit > 2000) {
      return jsonResponse(
        { detail: { error: "invalid_audit_limit" } }, request, undefined, 422,
      );
    }
    if (!Number.isFinite(from) || !Number.isFinite(to) || from > to) {
      return jsonResponse(
        { detail: { error: "invalid_time_range" } }, request, undefined, 400,
      );
    }
    const eventType = url.searchParams.get("event_type");
    const email = url.searchParams.get("email");
    const res = await queryOpsLogs(
      "admin_audit",
      {
        from: new Date(from).toISOString(),
        to: new Date(to + 1).toISOString(),
      },
      { signal: request.signal },
    );
    const audit_logs = res.rows
      .filter((r) => r.timestamp >= from && r.timestamp <= to)
      .map((r, idx) => ({
        id: idx + 1,
        timestamp: new Date(r.timestamp).toISOString(),
        event_type: String(r.action ?? r.event_type ?? "ADMIN_ACTION"),
        email: String(r.actor ?? r.user_id ?? "admin"),
        ip_address: String(r.ip ?? r.target ?? "-"),
        details: String(r.target ?? r.details ?? ""),
      }))
      .filter((r) =>
        (!eventType || r.event_type === eventType) &&
        (!email || r.email.includes(email)),
      )
      .slice(0, limit);
    return jsonResponse({ audit_logs }, request);
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
      chart_interval?: string;
      chart_metric?: string;
      filters?: Record<string, { mode?: string; values?: string[] }>;
      custom_filters?: Record<string, { mode?: string; values?: string[] }>;
      fields?: string[];
    };
    const now = Date.now();
    const fromMs = body.start_time ? Date.parse(body.start_time) : now - 86_400_000;
    const toMs = body.end_time ? Date.parse(body.end_time) : now;
    const interval = toBucketInterval(body.chart_interval ?? body.interval, fromMs, toMs);
    const metric = body.chart_metric ?? body.metric ?? "requests";
    if (!ROLLUP_CHART_METRICS.has(metric) && !RAW_CHART_METRICS.has(metric)) {
      return jsonResponse({ detail: { error: "unsupported_chart_metric" } }, request, undefined, 400);
    }
    const filters = parseFiltersPayload(body.filters, body.custom_filters);

    const result = await runInWorker(
      {
        from: new Date(fromMs).toISOString(),
        to: new Date(toMs).toISOString(),
        filters,
        customFields: customCards((body.fields ?? []).map((id) => ({
          id, label: id, type: "string" as const,
        }))),
      },
      request.signal,
    );

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

    const bucketMs = { "1s": 1000, "1m": 60_000, "5m": 300_000, "1h": 3_600_000, "1d": 86_400_000 }[interval];
    const series = new Map<number, MinuteBucket>();
    for (const point of result.buckets) {
      const time = Math.floor(point.t / bucketMs) * bucketMs;
      const bucket = series.get(time);
      if (!bucket) {
        series.set(time, { ...point, t: time });
      } else {
        bucket.requests += point.requests;
        bucket.bytes += point.bytes;
        bucket.s4xx += point.s4xx;
        bucket.s5xx += point.s5xx;
        bucket.hits += point.hits;
        bucket.cacheRows += point.cacheRows;
      }
    }

    const rawMetric = RAW_CHART_METRICS.get(metric) ??
      (interval === "1s" ? ROLLUP_CHART_METRICS.get(metric) : undefined);
    const columns = rawMetric && filters.length > 0
      ? await executeSql("DESCRIBE requests", [], request.signal) : [];
    const filterTypes = new Map(columns.map((column) => [
      String(column.column_name),
      /BOOL/i.test(String(column.column_type)) ? "BOOLEAN" as const
        : /INT|FLOAT|DOUBLE|DECIMAL|REAL/i.test(String(column.column_type)) ? "DOUBLE" as const
          : "VARCHAR" as const,
    ] as const));
    const whereClause = compileFiltersToSql(filters, false, [], filterTypes);
    const rawSql = `
      SELECT FLOOR(ts_ms / ${bucketMs}) * ${bucketMs} AS bucket_ms,
        ${rawMetric}${metric === "throughput" ? ` / ${bucketMs / 1000}` : ""} AS value
      FROM requests
      WHERE ts_ms >= ? AND ts_ms < ? AND (${whereClause})
      GROUP BY 1 ORDER BY 1
    `;
    const rawSeries = rawMetric
      ? await executeSql(rawSql, [fromMs, toMs], request.signal) : null;
    const durationMs = Math.max(1, Math.round(
      (typeof performance !== "undefined" ? performance.now() : Date.now()) - startedAt,
    ));
    const engineName = rawMetric || result.stats.fetched > 0 ? "DuckDB-Wasm" : "Rollups";
    const vcpuMs: number | null = null;
    const sqlText = rawMetric ? rawSql : result.stats.fetched > 0
      ? `SELECT time_bucket('${interval}', timestamp), ... FROM read_parquet(${result.stats.fetched} window files)`
      : `ROLLUP_FAST_PATH(${result.stats.cached} summary files)`;

    useFilterStore.getState().setLastFetchMeta({
      vcpuMs,
      engine: engineName,
      cachedFiles: result.stats.cached,
      fetchedFiles: result.stats.fetched,
    });
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
    }, searchParams.get("service_id") ?? request.headers.get("x-service-id"), origin);
    const aggregates = {
      data: Object.fromEntries(Object.entries(result.cards).map(([field, card]) =>
        [field, { top: card.top, total: card.total }],
      )),
      map_data: (result.cards.country?.all ?? result.cards.country?.top ?? [])
        .map((row) => ({ country: String(row.value), count: row.count })),
      total_rows: totalReqs,
      total_rows_total: result.stats.rows,
      where_clause: whereClause,
      interval: { "1s": "1 second", "1m": "1 minute", "5m": "5 minutes", "1h": "1 hour", "1d": "1 day" }[interval],
      metric,
      time_series: rawSeries ? rawSeries.filter((row) => row.value !== null).map((row) => ({
        time: new Date(Number(row.bucket_ms)).toISOString(),
        value: Number(row.value),
        category: null,
      })) : [...series.values()].sort((a, b) => a.t - b.t).map((pt) => {
        let value = pt.requests;
        if (metric === "bandwidth") value = pt.bytes;
        else if (metric === "5xx") value = pt.s5xx;
        else if (metric === "4xx") value = pt.s4xx;
        else if (metric === "hit_rate") value = pt.requests > 0
          ? Math.round(pt.hits / pt.requests * 10_000) / 100 : 0;
        else if (metric === "error_rate") value = pt.requests > 0
          ? ((pt.s4xx + pt.s5xx) / pt.requests) * 100 : 0;
        return { time: new Date(pt.t).toISOString(), value, category: null };
      }),
    };

    if (pathname !== "/api/dashboard/bundle") {
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
            filters?: Record<string, { mode?: string; values?: string[] }>;
            custom_filters?: Record<string, { mode?: string; values?: string[] }>;
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
    const filters = parseFiltersPayload(body.filters, body.custom_filters);
    const serviceId = searchParams.get("service_id") ?? request.headers.get("x-service-id") ?? undefined;
    const fieldName = (body.fields?.[0] ?? body.field ?? "url") as CardField;
    const customIds = new Set([fieldName, ...filters.map((filter) => filter.field)]
      .filter((id) => !Object.hasOwn(FIELD_DUCKDB_TYPES, id)));
    let customFields: CustomField[] = [];
    if (customIds.size) {
      const catalog = await loadCloudCatalog(request);
      if (catalog instanceof Response) return catalog;
      customFields = (catalog.fields ?? [])
        .filter((field) => field.is_custom && customIds.has(field.id))
        .map((field) => ({
          id: field.id,
          type: /BOOL/i.test(String(field.sql_type)) ? "boolean"
            : /INT|FLOAT|DOUBLE|DECIMAL|REAL/i.test(String(field.sql_type)) ? "number" : "string",
        }));
      if ([...customIds].some((id) => !customFields.some((field) => field.id === id))) {
        return jsonResponse({ detail: "Unknown field in Cloud field search" }, request, undefined, 400);
      }
    }
    if (Array.isArray(body.fields) && body.fields.length > 0) {
      const summaries = await searchFieldSummariesInWorker(
        fromIso,
        toIso,
        body.fields[0] as CardField,
        body.search ?? "",
        filters,
        {
          limit: body.limit ?? 20,
          signal: request.signal,
          serviceId,
          customFields,
        },
      );
      return jsonResponse({ summaries }, request);
    }
    const items = await searchFieldValuesInWorker(
      fromIso,
      toIso,
      fieldName,
      body.search ?? "",
      filters,
      {
        limit: body.limit ?? 50,
        signal: request.signal,
        serviceId,
        customFields,
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
  if (pathname === "/api/usage/rum-breakdown" && method === "GET") {
    const settings = await fetchUsageSettings();
    return jsonResponse(await cloudRumUsage({
      start_time: searchParams.get("start_time")!,
      end_time: searchParams.get("end_time")!,
      signal: request.signal,
      engine: read,
    }, settings.class_a_rate_per_1k), request);
  }
  if (pathname.startsWith("/api/usage/")) {
    const now = Date.now();
    const days = Number(searchParams.get("days") ?? 30) || 30;
    const fromMs = searchParams.get("start_time") || searchParams.get("start")
      ? Date.parse((searchParams.get("start_time") ?? searchParams.get("start"))!)
      : now - days * 86_400_000;
    const toMs = searchParams.get("end_time") || searchParams.get("end")
      ? Date.parse((searchParams.get("end_time") ?? searchParams.get("end"))!)
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
    const requestedBucket = searchParams.get("by");
    const points = bucketUsage(input.rows, {
      from: fromMs,
      to: toMs,
      by: requestedBucket === "hour" || requestedBucket === "day"
        ? requestedBucket
        : toMs - fromMs <= 2 * 86_400_000 ? "hour" : "day",
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
  }

  // Page 6 & 7: Operational Logs, Schema & Usage Log
  if (pathname === "/api/cron-runs" && method === "DELETE") {
    const res = await deleteLogHistory("cron_runs");
    return jsonResponse(res, request);
  }
  if (pathname === "/api/admin/metadata-storage" && method === "GET") {
    return jsonResponse({ supported: false, reason: "cloud_postgres_storage_unavailable" }, request);
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
    return jsonResponse({ detail: { error: "unsupported_cleanup_action" } }, request, undefined, 400);
  }
  if (pathname === "/api/admin/metadata-retention" && method === "PATCH") {
    return unsupportedCloudEndpoint(request);
  }
  // Page 8: Origin Analytics (/api/origin/*)
  if (pathname.startsWith("/api/origin/")) {
    return handleOriginRequest(request, pathname, url, read);
  }

  // Page 9: Performance Analytics (/api/performance, /api/performance/aggregates)
  if (
    pathname === "/api/performance/aggregates" ||
    pathname === "/api/performance" ||
    pathname.startsWith("/api/performance/")
  ) {
    return handlePerformanceRequest(request, pathname, url, read);
  }

  // Page 10: Security Analytics (/api/security/*)
  if (pathname.startsWith("/api/security/")) {
    return handleSecurityRequest(request, pathname, url, read);
  }

  const parseRequestParams = async (): Promise<Record<string, unknown>> => {
    const out: Record<string, unknown> = {};
    for (const [k, v] of searchParams.entries()) {
      out[k] = v;
    }
    if (method === "POST" || method === "PUT" || method === "PATCH") {
      const body = (await request.clone().json().catch(() => ({}))) as Record<
        string,
        unknown
      >;
      if (body && typeof body === "object") {
        Object.assign(out, body);
      }
    }
    const headerSid = request.headers.get("x-service-id");
    if (headerSid && !out.service_id) {
      out.service_id = headerSid;
    }
    return out;
  };

  const resolveMaskIpsAndRole = (
    params: Record<string, unknown>,
  ): {
    maskIps: boolean;
    role: "admin" | "analyst" | "analyst-no-pii";
  } => {
    const headerRole = request.headers.get("x-session-role");
    const isNoPii =
      Boolean(params.mask_ips) ||
      request.headers.get("x-mask-ips") === "1" ||
      headerRole === "analyst-no-pii" ||
      useSessionRoleStore.getState().isRemoteAnalyst;
    return {
      maskIps: isNoPii,
      role: isNoPii
        ? "analyst-no-pii"
        : headerRole === "analyst"
          ? "analyst"
          : "admin",
    };
  };

  // Page 11: Network Analytics (/api/network/bundle, /api/network-health, /api/network-quality, /api/network/pop-health)
  if (
    pathname === "/api/network/bundle" ||
    pathname === "/api/network-health" ||
    pathname === "/api/network-quality" ||
    pathname === "/api/network/pop-health" ||
    pathname.startsWith("/api/network/")
  ) {
    const params = await parseRequestParams();
    const data = await handleNetworkRequest(pathname, {
      ...params,
      signal: request.signal,
      engine: read,
    } as NetworkQueryParams);
    return jsonResponse(data, request);
  }

  // Page 12: Fastly Value (/api/value/summary, /api/value/*)
  if (pathname === "/api/value/summary" || pathname.startsWith("/api/value/")) {
    const params = await parseRequestParams();
    const data = await handleValueRequest(pathname, {
      ...params,
      signal: request.signal,
      engine: read,
    } as ValueQueryParams);
    return jsonResponse(data, request);
  }

  // Page 12: Assets Shield (/api/assets/aggregates, /api/assets/*)
  if (
    pathname === "/api/assets/aggregates" ||
    pathname.startsWith("/api/assets/")
  ) {
    const params = await parseRequestParams();
    const data = await handleAssetsRequest(pathname, {
      ...params,
      signal: request.signal,
      engine: read,
    } as AssetsQueryParams);
    return jsonResponse(data, request);
  }

  // Page 13: Real User Monitoring (/api/services/{id}/rum/*, /api/rum/analytics, /api/rum/live-events, /api/rum/beacon-health)
  const rumServiceMatch = pathname.match(
    /^\/api\/services\/([^/]+)\/rum\/(.+)$/,
  );
  if (
    rumServiceMatch ||
    pathname === "/api/rum/analytics" ||
    pathname === "/api/rum/live-events" ||
    pathname === "/api/rum/beacon-health"
  ) {
    const params = await parseRequestParams();
    const serviceId = rumServiceMatch
      ? decodeURIComponent(rumServiceMatch[1])
      : String(params.service_id ?? "default");
    const rumParams = {
      ...params,
      service_id: serviceId,
      signal: request.signal,
      engine: read,
    } as RumQueryParams;
    try {
      const data = pathname.endsWith("/analytics") ? await cloudRumAnalytics(rumParams)
        : pathname.endsWith("/live-events") ? await cloudRumLiveEvents(rumParams)
        : await handleRumRequest(pathname, rumParams);
      return jsonResponse(data, request);
    } catch (error) {
      if (!(error instanceof UnsupportedRumFilterError)) throw error;
      return jsonResponse({
        detail: { error: "rum_filters_unsupported", message: error.message },
      }, request, undefined, 400);
    }
  }

  // Page 14: CMCD Streaming Analytics (/api/cmcd/aggregates, /api/cmcd/*, /api/services/{id}/cmcd/*)
  const cmcdServiceMatch = pathname.match(
    /^\/api\/services\/([^/]+)\/cmcd\/(.+)$/,
  );
  if (
    pathname === "/api/cmcd/aggregates" ||
    pathname.startsWith("/api/cmcd/") ||
    cmcdServiceMatch
  ) {
    const params = await parseRequestParams();
    const serviceId = cmcdServiceMatch
      ? decodeURIComponent(cmcdServiceMatch[1])
      : String(params.service_id ?? "default");
    const data = await handleCmcdRequest(pathname, {
      ...params,
      service_id: serviceId,
      signal: request.signal,
      engine: read,
    } as CmcdQueryParams);
    return jsonResponse(data, request);
  }

  // Page 15: Sessions & Session Scoring (/api/sessions, /api/sessions/detail, /api/services/{id}/scoring/*)
  if (pathname === "/api/sessions" && method === "POST") {
    const params = await parseRequestParams();
    const { maskIps, role } = resolveMaskIpsAndRole(params);
    const cols = await getPresentColumns(undefined, request.signal);
    const res = await handleSessionsList(
      params as unknown as SessionsListRequest,
      {
        serviceId: String(params.service_id ?? "default"),
        maskIps,
        role,
        availableColumns: cols && cols.size > 0 ? cols : undefined,
        customColumnTypes: read?.getCustomColumnTypes(),
        runSql: (sql) => executeSql(sql, [], request.signal),
      },
    );
    return jsonResponse(res.body, request, undefined, res.status);
  }

  if (pathname === "/api/sessions/detail" && method === "POST") {
    const params = await parseRequestParams();
    const { maskIps, role } = resolveMaskIpsAndRole(params);
    const res = await handleSessionsDetail(
      params as unknown as SessionDetailRequest,
      {
        serviceId: String(params.service_id ?? "default"),
        maskIps,
        role,
        runSql: (sql) => executeSql(sql, [], request.signal),
      },
    );
    return jsonResponse(res.body, request, undefined, res.status);
  }

  // Page 15: SQL Query Console (/api/presets, /api/query/schema*, /api/query)
  if (pathname === "/api/presets" && method === "GET") {
    const serviceId =
      searchParams.get("service_id") ??
      request.headers.get("x-service-id") ??
      undefined;
    return jsonResponse(getQueryPresets(serviceId), request);
  }

  if (
    (pathname === "/api/query/schema" ||
      pathname.startsWith("/api/query/schema/")) &&
    method === "GET"
  ) {
    const params = await parseRequestParams();
    const { maskIps } = resolveMaskIpsAndRole(params);
    const serviceId = pathname.startsWith("/api/query/schema/")
      ? decodeURIComponent(pathname.slice("/api/query/schema/".length))
      : String(params.service_id ?? "logs");
    return jsonResponse(getQuerySchema(serviceId, maskIps), request);
  }

  if (pathname === "/api/query" && method === "POST") {
    const params = await parseRequestParams();
    const { maskIps, role } = resolveMaskIpsAndRole(params);
    const res = await executeCloudQuery(params as unknown as QueryRequest, {
      serviceId: String(params.service_id ?? "default"),
      maskIps,
      role,
      allowedTables: ["logs", "requests", "client_vitals", "client_errors"],
      tableName: params.dataset === "client_vitals" || params.dataset === "client_errors"
        ? params.dataset : "requests",
      runSql: (sql) => executeSql(sql, [], request.signal),
    });
    return jsonResponse(res.body, request, undefined, res.status);
  }

  // Page 15: Insights (/api/insight-availability, /api/insights, /api/insights/cache-collapse-detail)
  if (pathname === "/api/insight-availability" && method === "GET") {
    const serviceId =
      searchParams.get("service_id") ??
      request.headers.get("x-service-id") ??
      undefined;
    const cols = await getPresentColumns(undefined, request.signal);
    return jsonResponse(handleInsightAvailability(serviceId, cols), request);
  }

  if (pathname === "/api/insights" && (method === "POST" || method === "GET")) {
    const params = await parseRequestParams();
    const { maskIps } = resolveMaskIpsAndRole(params);
    const data = await queryCloudInsights(params as unknown as InsightsRequest, {
      signal: request.signal,
      maskIps,
      filters: parseFiltersPayload(params.filters as Parameters<typeof parseFiltersPayload>[0], undefined),
      engine: read,
    });
    return jsonResponse(data, request);
  }

  if (pathname === "/api/insights/cache-collapse-detail" && method === "POST") {
    const params = await parseRequestParams();
    const { maskIps } = resolveMaskIpsAndRole(params);
    const data = await queryCloudCacheCollapse(
      params as unknown as CacheCollapseDetailRequest,
      { maskIps, signal: request.signal, engine: read },
    );
    return jsonResponse(data, request);
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
  const method = (init?.method ?? (input instanceof Request ? input.method : "GET")).toUpperCase();
  const localRequest = () => input instanceof Request
    ? new Request(input, init) : new Request(url, { ...init, method });

  if (method === "GET" && pathname !== "/api/cron-runs/cloud-tick/stream") {
    if (isNativeCloudRoute(method, pathname)) return fetch(input, init);
    return (await handleCloudAnalyticalRequest(localRequest())) ??
      unsupportedCloudEndpoint(localRequest());
  }
  if (!isNativeCloudRoute(method, pathname) &&
      !["/api/cron-runs/cloud-tick/stream", "/api/admin/ingest-logs", "/api/provision/ingest"].includes(pathname)) {
    return (await handleCloudAnalyticalRequest(localRequest())) ??
      unsupportedCloudEndpoint(localRequest());
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
        if (
          pathname === "/api/provision/ingest" ||
          pathname === "/api/cron-runs/cloud-tick/stream"
        ) {
          if (pathname === "/api/provision/ingest") {
            const triggered = await fetch("/api/cron-runs/trigger", {
              method: "POST",
              headers: { "content-type": "application/json" },
              body: "{}",
              credentials: "include",
              signal,
            });
            if (!triggered.ok) throw new Error(`Ingest trigger failed (${triggered.status})`);
          }
          emit({ type: "status", status: "complete", message: "Triggered ingest tick" });
          emit({ type: "done", status: "complete" });
          controller.close();
          return;
        }

        const initRes = isNativeCloudRoute(method, pathname)
          ? await fetch(input, { ...init, credentials: "include" })
          : (await handleCloudAnalyticalRequest(localRequest())) ??
            unsupportedCloudEndpoint(localRequest());
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
