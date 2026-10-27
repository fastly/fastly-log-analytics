import createClient from "openapi-fetch";
import type { paths, components } from "@/types/api.generated";
import type { StripIndex, WireParity } from "@/types/api";
import { useDebugStore, useServiceStore, useSessionRoleStore } from "@/stores/filterStore";
import { parseExtentMs, showReadOnlyToast, showBusyToast, showToast, isUserActive } from "@/lib/utils";

export * from "./apiSsrProxy";
export * from "./apiQueryKeys";
export * from "./apiLogFields";

let pageLoadId: string | null = null;
const makeUuid = () =>
  typeof crypto?.randomUUID === "function" ? crypto.randomUUID() : `${Date.now()}-${Math.random().toString(36).slice(2)}`;

export function getPageLoadId(): string | null {
  if (typeof window === "undefined") return null;
  if (!pageLoadId) pageLoadId = makeUuid();
  return pageLoadId;
}

export function rotatePageLoadId(): string | null {
  if (typeof window === "undefined") return null;
  pageLoadId = makeUuid();
  return pageLoadId;
}

export function extractApiError(error: unknown): string {
  if (!error) return "Unknown error";
  if (typeof error === "string") return error;
  if (error instanceof Error && error.message) return error.message;
  const e = error as { detail?: unknown; message?: unknown; error?: unknown; errors?: unknown };
  if (e.detail) {
    if (typeof e.detail === "string") return e.detail;
    if (Array.isArray(e.detail)) {
      return (e.detail as Array<{ loc?: unknown[]; msg?: unknown }>)
        .map((err) => {
          const field = Array.isArray(err.loc) && err.loc.length > 0 ? String(err.loc[err.loc.length - 1]) : "Field";
          const msg = typeof err.msg === "string" ? err.msg : "Invalid value";
          return `${field}: ${msg}`;
        })
        .join(", ");
    }
    if (typeof e.detail === "object" && e.detail !== null) {
      const d = e.detail as { errors?: unknown; messages?: unknown; message?: unknown; error?: unknown };
      if (Array.isArray(d.errors)) return d.errors.map(String).join(", ");
      if (Array.isArray(d.messages) && d.messages.length > 0) return d.messages.map(String).join(", ");
      if (typeof d.message === "string" && d.message) return d.message;
      if (d.error) return String(d.error);
    }
    return JSON.stringify(e.detail);
  }
  if (typeof e.message === "string" && e.message) return e.message;
  if (e.error) return String(e.error);
  if (Array.isArray(e.errors)) return e.errors.map(String).join(", ");
  try {
    return JSON.stringify(error);
  } catch {
    return "Unknown error";
  }
}

export async function unwrapClient<T>(
  promise: Promise<{ data?: T; error?: unknown; response?: Response }>,
  fallbackMessage?: string,
): Promise<T> {
  const { data, error, response } = await promise;
  if (error || (response && !response.ok)) {
    const status = response?.status ?? 500;
    const statusText = fallbackMessage || response?.statusText || `HTTP ${status}`;
    const msg = error ? extractApiError(error) : statusText;
    const err = new Error(msg && msg !== "Unknown error" ? msg : statusText) as Error & {
      status?: number;
      response?: Response;
      detail?: unknown;
    };
    err.status = status;
    if (response) err.response = response;
    err.detail = error;
    throw err;
  }
  return data as T;
}

export function getApiBase() {
  if (process.env.NEXT_PUBLIC_API_URL) return process.env.NEXT_PUBLIC_API_URL;
  const port = process.env.NEXT_PUBLIC_BACKEND_PORT || "8000";
  if (typeof window !== "undefined") return "";
  return process.env.API_PROXY_URL || `http://127.0.0.1:${port}`;
}

export const client = createClient<paths>({ baseUrl: getApiBase(), fetch: (req) => globalThis.fetch(req) });

function stampClientContextHeaders(headers: Headers): string | null {
  const currentPageLoadId = getPageLoadId();
  if (currentPageLoadId && !headers.has("X-Page-Load-ID")) headers.set("X-Page-Load-ID", currentPageLoadId);
  try {
    const debug = useDebugStore.getState();
    if ((debug.enabled || debug.apiCallsEnabled) && !headers.has("x-debug-responses")) {
      headers.set("x-debug-responses", "1");
    }
  } catch {
    /* SSR pre-hydration */
  }
  let sid: string | null = null;
  try {
    sid = useServiceStore.getState().activeServiceId;
    if (!sid && typeof window !== "undefined") sid = new URLSearchParams(window.location.search).get("service");
    if (sid && !headers.has("x-service-id")) headers.set("x-service-id", sid);
  } catch {
    /* SSR pre-hydration */
  }
  return sid;
}

export function adminFetch(input: string | URL | Request, init: RequestInit = {}): Promise<Response> {
  const headers = new Headers(init.headers ?? {});
  stampClientContextHeaders(headers);
  return fetch(input, { ...init, headers });
}

type JsonOf<P extends keyof paths, M extends "get" | "post" = "get"> = paths[P] extends {
  [K in M]: { responses: { 200: { content: { "application/json": infer T } } } };
}
  ? T
  : never;

type ParityArgs<T, W> = [W] extends [T]
  ? []
  : W extends readonly (infer WE)[]
    ? T extends readonly (infer TE)[]
      ? ParityArgs<TE, WE>
      : [wire_parity: "local type is not an array"]
    : keyof StripIndex<W> extends never
      ? [T] extends [W]
        ? []
        : [wire_parity: "local type is not assignable to the free-dict wire type"]
      : WireParity<T, W> extends true
        ? []
        : [wire_parity: WireParity<T, W>];

type AdminJsonResult<T> =
  | { ok: true; status: number; data: T }
  | { ok: false; status: number; statusText: string; data: null; errorText: string };

export async function adminJsonResult<P extends keyof paths, T = JsonOf<P>, M extends "get" | "post" = "get">(
  _spec: P,
  url: string,
  init: RequestInit = {},
  ..._parity: ParityArgs<T, JsonOf<P, M>>
): Promise<AdminJsonResult<T>> {
  const r = await adminFetch(url, init);
  if (!r.ok) {
    return {
      ok: false,
      status: r.status,
      statusText: r.statusText,
      data: null,
      errorText: await r.text().catch(() => ""),
    };
  }
  return { ok: true, status: r.status, data: (await r.json()) as T };
}

export async function adminJson<P extends keyof paths, T = JsonOf<P>, M extends "get" | "post" = "get">(
  spec: P,
  url: string,
  init: RequestInit = {},
  errorMessage: (status: number) => string = (status) => `status ${status}`,
  ...parity: ParityArgs<T, JsonOf<P, M>>
): Promise<T> {
  const res = await adminJsonResult<P, T, M>(spec, url, init, ...parity);
  if (!res.ok) throw new Error(errorMessage(res.status));
  return res.data;
}

const SERVICELESS_PATH_PREFIXES = [
  "/api/bootstrap",
  "/api/share",
  "/api/admin/share",
  "/api/auth",
  "/api/login",
  "/api/health",
  "/api/debug",
  "/api/admin/debug-settings",
  "/api/admin/usage-logging",
  "/api/provision",
  "/api/log-fields/catalog",
  "/api/admin/health-snapshot",
  "/api/admin/system-metrics",
  "/api/admin/health",
  "/api/admin/system-jobs",
  "/api/admin/queries/summary",
  "/api/admin/celery/status",
  "/api/admin/celery/ledger",
  "/api/admin/metric-history",
  "/api/admin/bot-sources",
  "/rum/versions",
];

const SESSION_DEAD_403_ERRORS = new Set(["fingerprint_mismatch", "ip_not_whitelisted", "unauthenticated"]);

function redirectToShareLoginIfSessionDead(): void {
  if (typeof window === "undefined") return;
  if (window.location.pathname.startsWith("/share-login")) return;
  try {
    if (!useSessionRoleStore.getState().isRemoteAnalyst) return;
  } catch {
    /* store unavailable */
  }
  const returnTo = `${window.location.pathname}${window.location.search}`;
  window.location.replace(`/share-login?return=${encodeURIComponent(returnTo)}`);
}

client.use({
  async onRequest({ request }) {
    const activeServiceId = stampClientContextHeaders(request.headers);
    request.headers.set("X-User-Active", isUserActive() ? "1" : "0");
    if (activeServiceId) return request;
    const url = String(request.url);
    if (SERVICELESS_PATH_PREFIXES.some((p) => url.includes(p)) || url.split("?")[0].endsWith("/api/services")) {
      return request;
    }
    const err = new Error("No active service — request aborted") as Error & { code: string };
    err.code = "NO_SERVICE";
    throw err;
  },
  async onResponse({ request, response }) {
    if (response.status === 503 && String(request.url).includes("/api/admin/clickhouse/status")) {
      const payload = await response.clone().json().catch(() => null);
      if (payload && typeof payload === "object" && "health" in payload) {
        return new Response(JSON.stringify(payload), { status: 200, headers: response.headers });
      }
    }
    if (!response.ok) {
      const error = await response.json().catch(() => ({ message: "An unknown error occurred" }));
      const msg = extractApiError(error);
      const errorCode =
        (error as { detail?: { error?: string }; error?: string })?.detail?.error ??
        (error as { error?: string })?.error;
      const isDead403 =
        response.status === 403 && typeof errorCode === "string" && SESSION_DEAD_403_ERRORS.has(errorCode);
      if (response.status === 401 || isDead403) redirectToShareLoginIfSessionDead();
      if (response.status === 403 && msg === "read_only") showReadOnlyToast();
      const isBusy503 = response.status === 503 && (error as { detail?: { busy?: boolean } })?.detail?.busy === true;
      if (isBusy503) showBusyToast();
      const method = request.method?.toUpperCase();
      const isMutation = method === "PUT" || method === "PATCH" || method === "DELETE";
      const alreadyHandled =
        (response.status === 403 && msg === "read_only") || isBusy503 || response.status === 401 || isDead403;
      if (isMutation && !alreadyHandled) showToast(msg || "Request failed", "error");
      const err = new Error(msg) as Error & { status?: number; response?: { status: number }; busy?: boolean };
      err.status = response.status;
      err.response = { status: response.status };
      if (isBusy503) err.busy = true;
      throw err;
    }
    return response;
  },
});

export type CustomField = components["schemas"]["CustomField"];
export type CustomFieldUpdate = components["schemas"]["CustomFieldUpdate"];
export type VclLintResult = components["schemas"]["VclLintResponse"];
export type VclLintRequest = components["schemas"]["VclLintRequest"];

export const customFieldsApi = {
  listCustomFields: (service_id: string) =>
    unwrapClient(
      client.GET("/api/services/{service_id}/custom-fields", { params: { path: { service_id } } }),
      "Failed to list custom fields",
    ),
  createCustomField: (service_id: string, field: Omit<CustomField, "created_at" | "updated_at">) =>
    unwrapClient(
      client.POST("/api/services/{service_id}/custom-fields", { params: { path: { service_id } }, body: field }),
      "Failed to create custom field",
    ),
  updateCustomField: (service_id: string, field_name: string, updates: CustomFieldUpdate) =>
    unwrapClient(
      client.PATCH("/api/services/{service_id}/custom-fields/{field_name}", {
        params: { path: { service_id, field_name } },
        body: updates,
      }),
      "Failed to update custom field",
    ),
  deleteCustomField: (service_id: string, field_name: string) =>
    unwrapClient(
      client.DELETE("/api/services/{service_id}/custom-fields/{field_name}", {
        params: { path: { service_id, field_name } },
      }),
      "Failed to delete custom field",
    ),
  validateCustomVcl: (service_id: string, body: VclLintRequest) =>
    unwrapClient(
      client.POST("/api/services/{service_id}/custom-fields/validate-vcl", { params: { path: { service_id } }, body }),
      "Failed to validate VCL",
    ),
  exportCustomFields: async (service_id: string) => {
    const response = await adminFetch(`${getApiBase()}/api/services/${service_id}/custom-fields/export`, {
      headers: { "x-service-id": service_id },
    });
    if (!response.ok) throw new Error("Failed to export custom fields");
    return response.blob();
  },
  importCustomFields: (service_id: string, fields: components["schemas"]["CustomFieldsImportBody"]["custom_fields"]) =>
    unwrapClient(
      client.POST("/api/services/{service_id}/custom-fields/import", {
        params: { path: { service_id } },
        body: { custom_fields: fields },
      }),
      "Failed to import custom fields",
    ),
};

// ── Stale View Retry ──────────────────────────────────────────────────────────

class StaleDashboardViewError extends Error {
  constructor() {
    super("dashboard aggregates response is internally inconsistent (likely stale-view symptom during a commit tick)");
    this.name = "StaleDashboardViewError";
  }
}

export function isStaleDashboardViewError(err: unknown): err is StaleDashboardViewError {
  return err instanceof StaleDashboardViewError || (err instanceof Error && err.name === "StaleDashboardViewError");
}

interface AggregatesWindow {
  startTime?: string | null;
  endTime?: string | null;
}

function isStaleDashboardAggregates(d: unknown, window?: AggregatesWindow, hasFilters = false): boolean {
  if (!d || typeof d !== "object" || hasFilters) return false;
  const r = d as Record<string, unknown>;
  if (!r.latest_log_at) return false;
  if (window?.startTime && window?.endTime) {
    const winStart = Date.parse(window.startTime);
    const winEnd = Date.parse(window.endTime);
    const dataLatest = parseExtentMs(r.latest_log_at, "latest");
    if (Number.isFinite(winStart) && Number.isFinite(winEnd) && dataLatest !== null) {
      const dataEarliest = parseExtentMs(r.earliest_log_at, "earliest") ?? dataLatest;
      if (!(dataEarliest <= winEnd && dataLatest >= winStart)) return false;
    }
  }
  const timeSeries = (r.time_series ?? r.timeseries) as unknown[] | undefined;
  if (Array.isArray(timeSeries) && timeSeries.length > 0) return false;
  const fields = r.data as Record<string, { total?: number; top?: unknown[] }> | undefined;
  if (!fields) return true;
  return Object.values(fields).every((f) => !f || ((f.total ?? 0) === 0 && (!f.top || f.top.length === 0)));
}

export function throwIfStaleAggregates<T>(data: T, window?: AggregatesWindow, hasFilters = false): T {
  if (isStaleDashboardAggregates(data, window, hasFilters)) throw new StaleDashboardViewError();
  return data;
}

export const STALE_VIEW_POLL_MS = 5000;
export const STALE_VIEW_RETRY_OPTIONS = {
  retry: (failureCount: number, error: Error) => isStaleDashboardViewError(error) && failureCount < 2,
  retryDelay: (attempt: number) => 400 * (attempt + 1),
  refetchInterval: (query: { state: { error: unknown; data?: unknown } }) =>
    isStaleDashboardViewError(query.state.error) && query.state.data == null ? STALE_VIEW_POLL_MS : false,
} as const;
