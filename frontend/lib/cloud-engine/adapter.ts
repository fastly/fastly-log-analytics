import { z } from "zod";
import {
  executeSql,
  getPresentColumns,
  getStoreStats,
  runInWorker,
  searchFieldSummariesInWorker,
  searchFieldValuesInWorker,
  setDashboardCacheIdentity,
} from "./dashboard/client";

z.config({ jitless: true });
import { handleAssetsRequest, type AssetsQueryParams } from "./assets";
import { handleCmcdRequest, type CmcdQueryParams } from "./cmcd";
import type { DashboardFilter } from "./dashboard/filters";
import { parseVcpuMsHeader } from "./dashboard/sources";
import type { CardField } from "./dashboard/summaries";
import {
  handleCacheCollapseDetail,
  handleInsightAvailability,
  handleInsights,
  type CacheCollapseDetailRequest,
  type InsightsRequest,
} from "./insights";
import { handleNetworkRequest, type NetworkQueryParams } from "./network";
import { handleOriginRequest } from "./origin";
import { handlePerformanceRequest } from "./performance";
import {
  executeCloudQuery,
  getQueryPresets,
  getQuerySchema,
  type QueryRequest,
} from "./query";
import { handleRumRequest, type RumQueryParams } from "./rum";
import { parquetColumns, sampleStats } from "./schema";
import { handleSecurityRequest } from "./security";
import {
  handleSessionScoringRoute,
  handleSessionsDetail,
  handleSessionsList,
  type SessionDetailRequest,
  type SessionsListRequest,
} from "./sessions";
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
import { handleValueRequest, type ValueQueryParams } from "./value";
import {
  useDebugStore,
  useFilterStore,
  useSessionRoleStore,
} from "@/stores/filterStore";

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

  // Synthesize /api/bootstrap from Cloud Compute's /api/auth/session and /api/services
  if (pathname === "/api/bootstrap" && method === "GET") {
    const sessionRes = await fetch("/api/auth/session", {
      credentials: "include",
      signal: request.signal,
    }).catch(() => null);
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
    const servicesRes = await fetch("/api/services", {
      credentials: "include",
      signal: request.signal,
    }).catch(() => null);
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
    const services =
      mappedServices.length > 0
        ? mappedServices
        : isRemoteAnalyst
          ? [
              {
                service_id: "default",
                name: "Default Service",
                cmcd_enabled: false,
                rum_enabled: false,
              },
            ]
          : [];
    const activeServiceId = services[0]?.service_id ?? null;
    const activeLogFieldIds = rawServices[0]?.log_fields?.selected_fields ?? [];
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

  // High-scale-only admin overview endpoints bridged in Cloud mode
  if (pathname === "/api/admin/health-snapshot" && method === "GET") {
    return jsonResponse(
      {
        vcpus: 1,
        load: null,
        memory: null,
        data_mount: null,
        root_disk: null,
        scheduler_last_tick_age_s: null,
        recent_cron_failures: [],
        config_backup: null,
        in_flight_runs: [],
      },
      request,
    );
  }
  if (pathname === "/api/admin/metric-history/batch" && method === "GET") {
    return jsonResponse({ series: {} }, request);
  }
  if (pathname === "/api/admin/log-accounting" && method === "GET") {
    return jsonResponse(
      {
        totals: { gap_pct: 0, total_lines: 0, total_lost_lines: 0 },
        buckets: [],
        sustained_loss: null,
      },
      request,
    );
  }
  if (pathname === "/api/admin/quarantine/summary" && method === "GET") {
    return jsonResponse({ total_items: 0, by_category: {} }, request);
  }
  if (pathname === "/api/admin/quarantine" && method === "GET") {
    return jsonResponse({ items: [], total: 0 }, request);
  }
  if (pathname === "/api/admin/bot-sources" && method === "GET") {
    return jsonResponse(
      {
        sources: [],
        rdns: { total: 0, pending: 0, last_enrichment_at: null },
      },
      request,
    );
  }
  if (pathname === "/api/admin/system-jobs" && method === "GET") {
    return jsonResponse({ jobs: [] }, request);
  }
  if (pathname === "/api/admin/pop-locations" && method === "GET") {
    return jsonResponse({ pops: [] }, request);
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
    }).catch(() => null);
    const updated = res?.ok
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
      await startBackfill().catch(() => null);
    } else {
      await fetch("/api/cron-runs/trigger", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: "{}",
        credentials: "include",
        signal: request.signal,
      }).catch(() => null);
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
    const now = Date.now();
    const res = await queryOpsLogs(
      "admin_audit",
      {
        from: new Date(now - 30 * 86_400_000).toISOString(),
        to: new Date(now).toISOString(),
      },
      { signal: request.signal },
    ).catch(() => ({ rows: [] }));
    const audit_logs = (res.rows ?? []).map(
      (r: Record<string, unknown>, idx: number) => ({
        id: idx + 1,
        timestamp: String(
          r.timestamp ?? r.created_at ?? new Date().toISOString(),
        ),
        event_type: String(r.action ?? r.event_type ?? "ADMIN_ACTION"),
        email: String(r.actor ?? r.user_id ?? "admin"),
        ip_address: String(r.ip ?? r.target ?? "-"),
        details: String(r.target ?? r.details ?? ""),
      }),
    );
    return jsonResponse({ audit_logs }, request);
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

  // Page 8: Origin Analytics (/api/origin/*)
  if (pathname.startsWith("/api/origin/")) {
    return handleOriginRequest(request, pathname, url);
  }

  // Page 9: Performance Analytics (/api/performance, /api/performance/aggregates)
  if (
    pathname === "/api/performance/aggregates" ||
    pathname === "/api/performance" ||
    pathname.startsWith("/api/performance/")
  ) {
    return handlePerformanceRequest(request, pathname, url);
  }

  // Page 10: Security Analytics (/api/security/*)
  if (pathname.startsWith("/api/security/")) {
    return handleSecurityRequest(request, pathname, url);
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
    } as NetworkQueryParams);
    return jsonResponse(data, request);
  }

  // Page 12: Fastly Value (/api/value/summary, /api/value/*)
  if (pathname === "/api/value/summary" || pathname.startsWith("/api/value/")) {
    const params = await parseRequestParams();
    const data = await handleValueRequest(pathname, {
      ...params,
      signal: request.signal,
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
    const data = await handleRumRequest(pathname, {
      ...params,
      service_id: serviceId,
      signal: request.signal,
    } as RumQueryParams);
    return jsonResponse(data, request);
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
    } as CmcdQueryParams);
    return jsonResponse(data, request);
  }

  // Page 15: Sessions & Session Scoring (/api/sessions, /api/sessions/detail, /api/services/{id}/scoring/*)
  if (pathname === "/api/sessions" && method === "POST") {
    const params = await parseRequestParams();
    const { maskIps, role } = resolveMaskIpsAndRole(params);
    const cols = await getPresentColumns(undefined, request.signal).catch(
      () => undefined,
    );
    const res = await handleSessionsList(
      params as unknown as SessionsListRequest,
      {
        serviceId: String(params.service_id ?? "default"),
        maskIps,
        role,
        availableColumns: cols && cols.size > 0 ? cols : undefined,
        runSql: (sql) => executeSql(sql, [], request.signal).catch(() => []),
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
        runSql: (sql) => executeSql(sql, [], request.signal).catch(() => []),
      },
    );
    return jsonResponse(res.body, request, undefined, res.status);
  }

  const scoringServiceMatch = pathname.match(
    /^\/api\/services\/([^/]+)\/scoring\/(.+)$/,
  );
  if (scoringServiceMatch) {
    const serviceId = decodeURIComponent(scoringServiceMatch[1]);
    const subPath = scoringServiceMatch[2];
    const params = await parseRequestParams();
    const { maskIps, role } = resolveMaskIpsAndRole(params);
    const cols = await getPresentColumns(undefined, request.signal).catch(
      () => undefined,
    );
    const res = await handleSessionScoringRoute(
      method,
      serviceId,
      subPath,
      params,
      {
        serviceId,
        maskIps,
        role,
        availableColumns: cols && cols.size > 0 ? cols : undefined,
        runSql: (sql) => executeSql(sql, [], request.signal).catch(() => []),
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
      runSql: (sql) => executeSql(sql, [], request.signal).catch(() => []),
    });
    return jsonResponse(res.body, request, undefined, res.status);
  }

  // Page 15: Insights (/api/insight-availability, /api/insights, /api/insights/cache-collapse-detail)
  if (pathname === "/api/insight-availability" && method === "GET") {
    const serviceId =
      searchParams.get("service_id") ??
      request.headers.get("x-service-id") ??
      undefined;
    const cols = await getPresentColumns(undefined, request.signal).catch(
      () => undefined,
    );
    return jsonResponse(
      handleInsightAvailability(
        serviceId,
        cols && cols.size > 0 ? cols : undefined,
      ),
      request,
    );
  }

  if (pathname === "/api/insights" && (method === "POST" || method === "GET")) {
    const params = await parseRequestParams();
    const { maskIps, role } = resolveMaskIpsAndRole(params);
    const cols = await getPresentColumns(undefined, request.signal).catch(
      () => undefined,
    );
    const data = await handleInsights(params as unknown as InsightsRequest, {
      serviceId: String(params.service_id ?? "default"),
      maskIps,
      role,
      availableColumns: cols && cols.size > 0 ? cols : undefined,
    });
    return jsonResponse(data, request);
  }

  if (pathname === "/api/insights/cache-collapse-detail" && method === "POST") {
    const params = await parseRequestParams();
    const { maskIps, role } = resolveMaskIpsAndRole(params);
    const data = await handleCacheCollapseDetail(
      params as unknown as CacheCollapseDetailRequest,
      { maskIps, role },
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
  const method = (init?.method ?? "GET").toUpperCase();

  // Pass through standard GET SSE streams (e.g. /api/ingest/events), except synthetic cloud-tick stream
  if (method === "GET" && pathname !== "/api/cron-runs/cloud-tick/stream") {
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
        if (
          pathname === "/api/provision/ingest" ||
          pathname === "/api/cron-runs/cloud-tick/stream"
        ) {
          if (pathname === "/api/provision/ingest") {
            await fetch("/api/cron-runs/trigger", {
              method: "POST",
              headers: { "content-type": "application/json" },
              body: "{}",
              credentials: "include",
              signal,
            });
          }
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
