import type { IncomingMessage } from "node:http";
import createClient from "openapi-fetch";
import type { paths, components } from "@/types/api.generated";
import type { FiltersPayload, LogFieldsConfig, StripIndex, WireParity } from "@/types/api";
import { useDebugStore, useServiceStore, useSessionRoleStore } from "@/stores/filterStore";
import { DEBUG_RESPONSES_COOKIE, parseExtentMs, showReadOnlyToast, showBusyToast, showToast, isUserActive } from "@/lib/utils";

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
  try { return JSON.stringify(error); } catch { return "Unknown error"; }
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
    if ((debug.enabled || debug.apiCallsEnabled) && !headers.has("x-debug-responses")) headers.set("x-debug-responses", "1");
  } catch { /* SSR pre-hydration */ }
  let sid: string | null = null;
  try {
    sid = useServiceStore.getState().activeServiceId;
    if (!sid && typeof window !== "undefined") sid = new URLSearchParams(window.location.search).get("service");
    if (sid && !headers.has("x-service-id")) headers.set("x-service-id", sid);
  } catch { /* SSR pre-hydration */ }
  return sid;
}

export function adminFetch(input: string | URL | Request, init: RequestInit = {}): Promise<Response> {
  const headers = new Headers(init.headers ?? {});
  stampClientContextHeaders(headers);
  return fetch(input, { ...init, headers });
}

type JsonOf<P extends keyof paths, M extends "get" | "post" = "get"> = paths[P] extends {
  [K in M]: { responses: { 200: { content: { "application/json": infer T } } } }
} ? T : never;

type ParityArgs<T, W> = [W] extends [T]
  ? []
  : W extends readonly (infer WE)[]
    ? T extends readonly (infer TE)[]
      ? ParityArgs<TE, WE>
      : [wire_parity: "local type is not an array"]
    : keyof StripIndex<W> extends never
      ? [T] extends [W] ? [] : [wire_parity: "local type is not assignable to the free-dict wire type"]
      : WireParity<T, W> extends true ? [] : [wire_parity: WireParity<T, W>];

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
  if (!r.ok) return { ok: false, status: r.status, statusText: r.statusText, data: null, errorText: await r.text().catch(() => "") };
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
  "/api/bootstrap", "/api/share", "/api/admin/share", "/api/auth", "/api/login", "/api/health",
  "/api/debug", "/api/admin/debug-settings", "/api/admin/usage-logging", "/api/provision",
  "/api/log-fields/catalog", "/api/admin/health-snapshot", "/api/admin/system-metrics",
  "/api/admin/health", "/api/admin/system-jobs", "/api/admin/queries/summary",
  "/api/admin/celery/status", "/api/admin/celery/ledger", "/api/admin/metric-history",
  "/api/admin/bot-sources", "/rum/versions",
];

const SESSION_DEAD_403_ERRORS = new Set(["fingerprint_mismatch", "ip_not_whitelisted", "unauthenticated"]);

function redirectToShareLoginIfSessionDead(): void {
  if (typeof window === "undefined") return;
  if (window.location.pathname.startsWith("/share-login")) return;
  try {
    if (!useSessionRoleStore.getState().isRemoteAnalyst) return;
  } catch { /* store unavailable */ }
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
      const isDead403 = response.status === 403 && typeof errorCode === "string" && SESSION_DEAD_403_ERRORS.has(errorCode);
      if (response.status === 401 || isDead403) redirectToShareLoginIfSessionDead();
      if (response.status === 403 && msg === "read_only") showReadOnlyToast();
      const isBusy503 = response.status === 503 && (error as { detail?: { busy?: boolean } })?.detail?.busy === true;
      if (isBusy503) showBusyToast();
      const method = request.method?.toUpperCase();
      const isMutation = method === "PUT" || method === "PATCH" || method === "DELETE";
      const alreadyHandled = (response.status === 403 && msg === "read_only") || isBusy503 || response.status === 401 || isDead403;
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
    unwrapClient(client.GET("/api/services/{service_id}/custom-fields", { params: { path: { service_id } } }), "Failed to list custom fields"),
  createCustomField: (service_id: string, field: Omit<CustomField, "created_at" | "updated_at">) =>
    unwrapClient(client.POST("/api/services/{service_id}/custom-fields", { params: { path: { service_id } }, body: field }), "Failed to create custom field"),
  updateCustomField: (service_id: string, field_name: string, updates: CustomFieldUpdate) =>
    unwrapClient(client.PATCH("/api/services/{service_id}/custom-fields/{field_name}", { params: { path: { service_id, field_name } }, body: updates }), "Failed to update custom field"),
  deleteCustomField: (service_id: string, field_name: string) =>
    unwrapClient(client.DELETE("/api/services/{service_id}/custom-fields/{field_name}", { params: { path: { service_id, field_name } } }), "Failed to delete custom field"),
  validateCustomVcl: (service_id: string, body: VclLintRequest) =>
    unwrapClient(client.POST("/api/services/{service_id}/custom-fields/validate-vcl", { params: { path: { service_id } }, body }), "Failed to validate VCL"),
  exportCustomFields: async (service_id: string) => {
    const response = await adminFetch(`${getApiBase()}/api/services/${service_id}/custom-fields/export`, { headers: { "x-service-id": service_id } });
    if (!response.ok) throw new Error("Failed to export custom fields");
    return response.blob();
  },
  importCustomFields: (service_id: string, fields: components["schemas"]["CustomFieldsImportBody"]["custom_fields"]) =>
    unwrapClient(client.POST("/api/services/{service_id}/custom-fields/import", { params: { path: { service_id } }, body: { custom_fields: fields } }), "Failed to import custom fields"),
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

interface AggregatesWindow { startTime?: string | null; endTime?: string | null }

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

// ── Server-Side Upstream Proxy & SSR Transport ────────────────────────────────

const HOP_BY_HOP_HEADERS = new Set([
  "connection", "keep-alive", "proxy-authenticate", "proxy-authorization",
  "te", "trailers", "transfer-encoding", "upgrade",
]);
const IDEMPOTENT_METHODS = new Set(["GET", "HEAD", "OPTIONS"]);
const SSR_TIMEOUT_MS = 5000;

function getNodeRequestFn(protocol: string) {
  const mod = process.getBuiltinModule(protocol === "https:" ? "node:https" : "node:http") as
    | typeof import("node:http")
    | undefined;
  if (!mod?.request) throw new Error("Node HTTP module unavailable in browser runtime");
  return mod.request;
}

function apiProxyTarget(): string {
  return (process.env.API_PROXY_URL || process.env.NEXT_PUBLIC_API_PROXY_URL || "http://caddy:80").replace(/\/+$/, "");
}

function isBenignDisconnectError(err: unknown): boolean {
  if (!err || typeof err !== "object") return false;
  const code = (err as NodeJS.ErrnoException).code;
  const message = (err as Error).message || "";
  return (
    code === "ECONNRESET" || code === "EPIPE" || code === "ERR_STREAM_PREMATURE_CLOSE" ||
    code === "ABORT_ERR" || message.includes("aborted") || message.includes("socket hang up")
  );
}

function buildUpstreamHeaders(request: Request, targetUrl: URL, bodyBuffer?: Buffer): Record<string, string> {
  const out: Record<string, string> = {};
  request.headers.forEach((value, key) => {
    const lower = key.toLowerCase();
    if (!HOP_BY_HOP_HEADERS.has(lower) && lower !== "host" && lower !== "content-length") out[key] = value;
  });
  out["host"] = targetUrl.host;
  out["connection"] = "close";
  if (bodyBuffer && bodyBuffer.byteLength > 0) out["content-length"] = String(bodyBuffer.byteLength);
  return out;
}

function sendUpstreamOnce(
  targetUrl: URL,
  method: string,
  headers: Record<string, string>,
  bodyBuffer: Buffer | undefined,
  signal: AbortSignal | undefined,
): Promise<Response> {
  return new Promise((resolve, reject) => {
    if (signal?.aborted) { resolve(new Response(null, { status: 499 })); return; }
    const lib = getNodeRequestFn(targetUrl.protocol);
    let settledHeaders = false;
    const upstreamReq = lib(
      {
        hostname: targetUrl.hostname,
        port: targetUrl.port || (targetUrl.protocol === "https:" ? 443 : 80),
        path: `${targetUrl.pathname}${targetUrl.search}`,
        method,
        headers,
        agent: false,
      },
      (upstreamRes: IncomingMessage) => {
        settledHeaders = true;
        const status = upstreamRes.statusCode || 502;
        const resHeaders = new Headers();
        for (const [rawKey, rawVal] of Object.entries(upstreamRes.headers)) {
          if (rawVal === undefined) continue;
          const lower = rawKey.toLowerCase();
          if (HOP_BY_HOP_HEADERS.has(lower)) continue;
          if (lower === "set-cookie") {
            for (const c of Array.isArray(rawVal) ? rawVal : [String(rawVal)]) resHeaders.append("set-cookie", c);
            continue;
          }
          resHeaders.set(rawKey, Array.isArray(rawVal) ? rawVal.join(", ") : String(rawVal));
        }
        if (method === "HEAD" || status === 204 || status === 205 || status === 304) {
          upstreamRes.resume();
          resolve(new Response(null, { status, statusText: upstreamRes.statusMessage, headers: resHeaders }));
          return;
        }
        let streamClosed = false;
        const closeCleanly = (controller: ReadableStreamDefaultController<Uint8Array>) => {
          if (streamClosed) return;
          streamClosed = true;
          try { controller.close(); } catch { /* already closed */ }
        };
        const stream = new ReadableStream<Uint8Array>({
          start(controller) {
            upstreamRes.on("data", (chunk: Buffer) => {
              if (streamClosed) return;
              try { controller.enqueue(new Uint8Array(chunk)); } catch { streamClosed = true; upstreamReq.destroy(); }
            });
            upstreamRes.on("end", () => closeCleanly(controller));
            upstreamRes.on("close", () => closeCleanly(controller));
            upstreamRes.on("error", (err) => {
              if (isBenignDisconnectError(err) || signal?.aborted) { closeCleanly(controller); return; }
              if (!streamClosed) {
                streamClosed = true;
                try { controller.error(err); } catch { /* ignore */ }
              }
            });
          },
          cancel() {
            streamClosed = true;
            upstreamRes.destroy();
            upstreamReq.destroy();
          },
        });
        resolve(new Response(stream, { status, statusText: upstreamRes.statusMessage, headers: resHeaders }));
      },
    );
    const onAbort = () => {
      upstreamReq.destroy();
      if (!settledHeaders) { settledHeaders = true; resolve(new Response(null, { status: 499 })); }
    };
    signal?.addEventListener("abort", onAbort, { once: true });
    upstreamReq.on("close", () => signal?.removeEventListener("abort", onAbort));
    upstreamReq.on("error", (err) => {
      if (settledHeaders) return;
      settledHeaders = true;
      if (signal?.aborted) { resolve(new Response(null, { status: 499 })); return; }
      reject(err);
    });
    if (bodyBuffer && bodyBuffer.byteLength > 0) upstreamReq.write(bodyBuffer);
    upstreamReq.end();
  });
}

export async function proxyUpstreamRequest(request: Request): Promise<Response> {
  const reqUrl = new URL(request.url);
  const targetUrl = new URL(`${apiProxyTarget()}${reqUrl.pathname}${reqUrl.search}`);
  const method = (request.method || "GET").toUpperCase();
  let bodyBuffer: Buffer | undefined;
  if (method !== "GET" && method !== "HEAD") {
    const ab = await request.arrayBuffer();
    if (ab.byteLength > 0) bodyBuffer = Buffer.from(ab);
  }
  const headers = buildUpstreamHeaders(request, targetUrl, bodyBuffer);
  try {
    return await sendUpstreamOnce(targetUrl, method, headers, bodyBuffer, request.signal);
  } catch (firstErr) {
    const code = (firstErr as NodeJS.ErrnoException | undefined)?.code;
    if (IDEMPOTENT_METHODS.has(method) && !request.signal?.aborted && (code === "ECONNRESET" || code === "EPIPE")) {
      try {
        return await sendUpstreamOnce(targetUrl, method, headers, bodyBuffer, request.signal);
      } catch (retryErr) {
        if (isBenignDisconnectError(retryErr) && request.signal?.aborted) return new Response(null, { status: 499 });
        return Response.json({ detail: "Upstream service unavailable" }, { status: 502 });
      }
    }
    if (isBenignDisconnectError(firstErr) && request.signal?.aborted) return new Response(null, { status: 499 });
    return Response.json({ detail: "Upstream service unavailable" }, { status: 502 });
  }
}

function isLoopbackHost(host: string): boolean {
  const bare = host.startsWith("[") ? host.slice(1, host.indexOf("]")) : host.split(":", 1)[0];
  const lc = bare.toLowerCase();
  return lc === "localhost" || lc === "127.0.0.1" || lc === "::1";
}

function timingSafeStringEqual(a: string, b: string): boolean {
  const enc = new TextEncoder();
  const ba = enc.encode(a);
  const bb = enc.encode(b);
  if (ba.byteLength !== bb.byteLength) return false;
  let diff = 0;
  for (let i = 0; i < ba.byteLength; i++) diff |= ba[i] ^ bb[i];
  return diff === 0;
}

export function classifySsrRequest(headerBag: Pick<Headers, "get">): {
  admin: boolean;
  gatewayAuthenticated: boolean;
  refusal: string | null;
} {
  const proxiedByCaddy = headerBag.get("x-proxied-by-caddy");
  const inboundHost = headerBag.get("host");
  const gatewayToken = headerBag.get("x-admin-gateway-token");
  const gatewaySecret = process.env.ADMIN_GATEWAY_SECRET || "";
  const gatewayAuthenticated =
    !proxiedByCaddy && gatewaySecret.length >= 32 && timingSafeStringEqual(gatewayToken || "", gatewaySecret);
  let refusal: string | null = null;
  if (gatewayToken !== null && !gatewayAuthenticated) {
    refusal = "invalid admin gateway credential";
  } else if (gatewayAuthenticated && !inboundHost) {
    refusal = "admin gateway request without Host";
  } else if (
    gatewayAuthenticated &&
    (headerBag.get("sec-fetch-site") === "cross-site" ||
      (headerBag.get("origin") !== null &&
        headerBag.get("origin")?.toLowerCase() !== `https://${inboundHost?.toLowerCase()}`))
  ) {
    refusal = "cross-origin admin gateway request";
  } else if (process.env.ADMIN_GATEWAY_REQUIRED === "1" && !gatewayAuthenticated && !proxiedByCaddy) {
    refusal = "authenticated admin gateway required";
  } else if (!gatewayAuthenticated && !proxiedByCaddy && (!inboundHost || !isLoopbackHost(inboundHost))) {
    refusal = `no X-Proxied-By-Caddy marker and non-loopback/absent Host=${inboundHost ?? "<none>"}`;
  }
  return { admin: !refusal && !proxiedByCaddy, gatewayAuthenticated, refusal };
}

export interface SSRUpstreamResponse { statusCode: number; body: string }

export function parseSsrJson<T = unknown>(resp: SSRUpstreamResponse | null, logPrefix: string): T | null {
  if (!resp) return null;
  if (resp.statusCode < 200 || resp.statusCode >= 300) {
    console.warn(`[${logPrefix}] upstream returned ${resp.statusCode}; falling back to client fetch`);
    return null;
  }
  try { return JSON.parse(resp.body) as T; } catch {
    console.warn(`[${logPrefix}] malformed upstream body; falling back to client fetch`);
    return null;
  }
}

export interface SSRGetOpts {
  path: string;
  logPrefix: string;
  extraHeaders?: Record<string, string>;
  method?: "GET" | "POST";
  body?: unknown;
  timeoutMs?: number;
}

type SsrHeadersProvider = {
  cookies: () => Promise<{ toString(): string; get(name: string): { value: string } | undefined }> | { toString(): string; get(name: string): { value: string } | undefined };
  headers: () => Promise<Pick<Headers, "get">> | Pick<Headers, "get">;
};

let _ssrHeadersProvider: SsrHeadersProvider | null = null;
export function registerSsrHeaders(provider: SsrHeadersProvider): void { _ssrHeadersProvider = provider; }

async function resolveSsrHeaders(): Promise<SsrHeadersProvider> {
  if (_ssrHeadersProvider) return _ssrHeadersProvider;
  const pkg = ["next", "headers"].join("/");
  return (await import(/* webpackIgnore: true */ pkg)) as SsrHeadersProvider;
}

export async function ssrUpstreamGet({
  path,
  logPrefix,
  extraHeaders,
  method = "GET",
  body,
  timeoutMs = SSR_TIMEOUT_MS,
}: SSRGetOpts): Promise<SSRUpstreamResponse | null> {
  const base = process.env.API_PROXY_URL;
  if (!base) return null;
  try {
    const nh = await resolveSsrHeaders();
    const [cookieJar, headerBag] = await Promise.all([nh.cookies(), nh.headers()]);
    const cookieHeader = cookieJar.toString();
    const proxiedByCaddy = headerBag.get("x-proxied-by-caddy");
    const inboundHost = headerBag.get("host");
    const gatewaySecret = process.env.ADMIN_GATEWAY_SECRET || "";
    const { gatewayAuthenticated, refusal } = classifySsrRequest(headerBag);
    if (refusal) {
      console.warn(`[${logPrefix}] refusal: ${refusal} — falling back to client fetch`);
      return null;
    }
    const debugResponsesRequested = cookieJar.get(DEBUG_RESPONSES_COOKIE)?.value === "1";
    const upstreamHeaders: Record<string, string> = { Accept: "application/json" };
    if (cookieHeader) upstreamHeaders.Cookie = cookieHeader;
    const ua = headerBag.get("user-agent");
    if (ua) upstreamHeaders["user-agent"] = ua;
    const platform = headerBag.get("sec-ch-ua-platform");
    if (platform) upstreamHeaders["sec-ch-ua-platform"] = platform;
    if (extraHeaders) {
      const trustHeaders = new Set(["host", "x-remote-analyst", "x-admin-gateway-token"]);
      for (const [key, value] of Object.entries(extraHeaders)) {
        if (!trustHeaders.has(key.toLowerCase())) upstreamHeaders[key] = value;
      }
    }
    if (gatewayAuthenticated) {
      upstreamHeaders["X-Admin-Gateway-Token"] = gatewaySecret;
      if (inboundHost) upstreamHeaders.Host = inboundHost;
    }
    if (proxiedByCaddy) {
      upstreamHeaders["X-Remote-Analyst"] = "1";
      if (inboundHost) upstreamHeaders.Host = inboundHost;
    }
    if (debugResponsesRequested && !proxiedByCaddy) upstreamHeaders["x-debug-responses"] = "1";
    let serialisedBody: string | undefined;
    if (method === "POST" && body !== undefined) {
      serialisedBody = JSON.stringify(body);
      upstreamHeaders["Content-Type"] = "application/json";
      upstreamHeaders["Content-Length"] = String(Buffer.byteLength(serialisedBody));
    }
    upstreamHeaders["Connection"] = "close";
    try {
      return await rawRequest(`${base}${path}`, upstreamHeaders, timeoutMs, method, serialisedBody);
    } catch (firstErr) {
      const code = (firstErr as NodeJS.ErrnoException | undefined)?.code;
      if (code === "ECONNRESET" || code === "EPIPE") {
        return await rawRequest(`${base}${path}`, upstreamHeaders, timeoutMs, method, serialisedBody);
      }
      throw firstErr;
    }
  } catch (err) {
    console.warn(`[${logPrefix}] fetch failed; falling back to client fetch:`, err);
    return null;
  }
}

function rawRequest(
  urlStr: string,
  reqHeaders: Record<string, string>,
  timeoutMs: number,
  method: "GET" | "POST" = "GET",
  body?: string,
): Promise<SSRUpstreamResponse> {
  return new Promise((resolve, reject) => {
    const url = new URL(urlStr);
    const lib = getNodeRequestFn(url.protocol);
    const req = lib(
      {
        hostname: url.hostname,
        port: url.port || (url.protocol === "https:" ? 443 : 80),
        path: `${url.pathname}${url.search}`,
        method,
        headers: reqHeaders,
        timeout: timeoutMs,
        agent: false,
      },
      (res) => {
        const chunks: Buffer[] = [];
        res.on("data", (c: Buffer) => chunks.push(c));
        res.on("end", () => resolve({ statusCode: res.statusCode ?? 0, body: Buffer.concat(chunks).toString("utf8") }));
      },
    );
    req.on("error", reject);
    req.on("timeout", () => req.destroy(new Error(`SSR upstream timeout after ${timeoutMs}ms`)));
    if (method === "POST" && body !== undefined) req.write(body);
    req.end();
  });
}

// ── React Query Key Factory & Cache Seeding ───────────────────────────────────

export const queryKeys = {
  bootstrap: () => ['bootstrap'] as const,

  shareBanner: () => ['share-banner'] as const,

  services: () => ['services'] as const,

  systemJobs: () => ['system-jobs'] as const,

  views: (serviceId: string | null | undefined) => ['views', serviceId] as const,

  logExtents: (serviceId: string | null | undefined) => ['log-extents', serviceId] as const,
  logExtentsAll: () => ['log-extents'] as const,

  scoringLabels: (serviceId: string | null | undefined) => ['scoring-labels', serviceId] as const,
  scoringLabelsAll: () => ['scoring-labels'] as const,

  rumStatus: (serviceId: string | null | undefined) => ['rum-status', serviceId] as const,
  rumVersions: (serviceId: string | null | undefined) => ['rum-versions', serviceId] as const,

  syncStatus: (serviceId: string | null | undefined) => ['sync-status', serviceId] as const,
  syncStatusAll: () => ['sync-status'] as const,

  lastSync: (serviceId: string | null | undefined) => ['last-sync', serviceId] as const,
  lastSyncAll: () => ['last-sync'] as const,

  logFieldsCatalog: (serviceId?: string) => (serviceId ? ['log-fields-catalog', serviceId] as const : ['log-fields-catalog'] as const),

  admin: {
    shareStatus: () => ['admin', 'share', 'status'] as const,
    shareLive: () => ['admin', 'share', 'live'] as const,
    metricHistoryBatch: (window: string) => ['admin', 'metric-history-batch', window] as const,
    celeryStatus: () => ['admin', 'celery-status'] as const,
    celeryLedger: (statusFilter: string, serviceId: string | null | undefined, offset: number) =>
      ['admin', 'celery-ledger', statusFilter, serviceId ?? null, offset] as const,
    celeryLedgerAll: () => ['admin', 'celery-ledger'] as const,
    healthSnapshot: () => ['admin', 'health-snapshot'] as const,
    metadataStorage: () => ['admin', 'metadata-storage'] as const,
    debugSettings: () => ['admin', 'debug-settings'] as const,
    status: () => ['admin', 'status'] as const,

    overviewAll: () => ['admin', 'overview'] as const,
    overviewQueriesSummary: () => ['admin', 'overview', 'queries-summary'] as const,
    overviewLogAccounting: (serviceId: string | null | undefined) => ['admin', 'overview', 'log-accounting', serviceId] as const,
    overviewSlowQueriesCount: (serviceId: string | null | undefined) =>
      ['admin', 'overview', 'slow-queries-count', serviceId] as const,
    overviewCeleryStatus: () => ['admin', 'overview', 'celery-status'] as const,

    queryMonitorConfig: () => ['admin', 'query-monitor', 'config'] as const,
    queryMonitorSnapshot: (includeCompleted: boolean) => ['admin', 'query-monitor', 'snapshot', includeCompleted] as const,
    queryMonitorSnapshotAll: () => ['admin', 'query-monitor', 'snapshot'] as const,
    queryMonitorSlowHistory: (mode: string, thresholdMs: number) =>
      ['admin', 'query-monitor', 'slow-history', mode, thresholdMs] as const,
    queryMonitorDetail: (queryId: number | null) => ['admin', 'query-monitor', 'detail', queryId] as const,

    quarantineSummary: (serviceId: string | null | undefined) => ['admin', 'quarantine', 'summary', serviceId] as const,
    quarantineList: (serviceId: string | null | undefined, errorCategory: string, offset: number) =>
      ['admin', 'quarantine', 'list', serviceId, errorCategory, offset] as const,
    quarantineAll: () => ['admin', 'quarantine'] as const,

    cronLogs: (serviceId: string | null | undefined, taskFilter: string, statusFilter: string) =>
      ['admin', 'cron-logs', serviceId, taskFilter, statusFilter] as const,
    cronLogsFor: (serviceId: string | null | undefined) => ['admin', 'cron-logs', serviceId] as const,
    cronLogsAll: () => ['admin', 'cron-logs'] as const,
    cronLogsRecent: (serviceId: string | null | undefined) => ['admin', 'cron-logs-recent', serviceId] as const,
    cronLogsRecentAll: () => ['admin', 'cron-logs-recent'] as const,
    cronSchedule: (serviceId: string | null | undefined) => ['admin', 'cron-schedule', serviceId] as const,

    auditLogs: (serviceId: string | null | undefined, eventFilter: string) =>
      ['admin', 'audit-logs', serviceId, eventFilter] as const,
    auditLogsFor: (serviceId: string | null | undefined) => ['admin', 'audit-logs', serviceId] as const,
    ingestedFiles: (serviceId: string | null | undefined) => ['admin', 'ingested-files', serviceId] as const,
    schema: (serviceId: string | null | undefined) => ['admin', 'schema', serviceId] as const,
    schemaAll: () => ['admin', 'schema'] as const,

    iceberg: (serviceId: string | null | undefined) => ['admin', 'iceberg', serviceId] as const,
    icebergAll: () => ['admin', 'iceberg'] as const,
    icebergCalendar: (serviceId: string | null | undefined) => ['admin', 'iceberg', 'calendar', serviceId] as const,
    icebergInfo: (serviceId: string | null | undefined) => ['admin', 'iceberg-info', serviceId] as const,
    icebergTree: (serviceId: string | null | undefined) => ['admin', 'iceberg-tree', serviceId] as const,
    rawTree: (serviceId: string | null | undefined) => ['admin', 'raw-tree', serviceId] as const,
    fileTree: (type: 'iceberg' | 'raw', serviceId: string | null | undefined, prefix: string) =>
      ['admin', type === 'iceberg' ? 'iceberg-tree' : 'raw-tree', serviceId, prefix] as const,
    fileTreeFor: (type: 'iceberg' | 'raw', serviceId: string | null | undefined) =>
      ['admin', type === 'iceberg' ? 'iceberg-tree' : 'raw-tree', serviceId] as const,
    clickhouseStatus: (serviceId: string | null | undefined) =>
      ['admin', 'clickhouse', 'status', serviceId] as const,
  },

  filtered: (
    domain: string,
    endpoint: string,
    serviceId: string | null,
    startTime: string,
    endTime: string,
    filters: FiltersPayload,
    ...extras: unknown[]
  ) => [domain, endpoint, serviceId, startTime, endTime, filters, ...extras] as const,
}

interface BootstrapCacheSeedSource {
  active_service_id?: string | null
  views?: unknown
  log_fields_catalog?: unknown
  sync_status?: unknown
  log_extents?: unknown
  schema?: unknown
  table_name?: unknown
  cron_runs_first_page?: unknown
  last_sync?: unknown
  scoring_labels?: unknown
  services?: unknown
  share_banner?: {
    sharing_active?: boolean | null
    public_url?: string | null
  } | null
}

export function seedBootstrapDependentCaches(
  queryClient: { setQueryData: (queryKey: readonly unknown[], updater: unknown) => unknown },
  data: BootstrapCacheSeedSource | null | undefined,
): void {
  if (!data) return
  if (data.active_service_id) {
    const sid = data.active_service_id
    if (Array.isArray(data.views)) {
      queryClient.setQueryData(queryKeys.views(sid), data.views)
    }
    if (data.log_fields_catalog) {
      queryClient.setQueryData(queryKeys.logFieldsCatalog(sid), data.log_fields_catalog)
    }
    queryClient.setQueryData(queryKeys.syncStatus(sid), data.sync_status ?? null)
    if (data.log_extents) {
      queryClient.setQueryData(queryKeys.logExtents(sid), data.log_extents)
    }
    const seededSchemaList = data.schema
    const seededTableName = data.table_name
    if (
      Array.isArray(seededSchemaList) &&
      seededSchemaList.length > 0 &&
      typeof seededTableName === 'string'
    ) {
      queryClient.setQueryData(queryKeys.admin.schema(sid), {
        schema: seededSchemaList,
        table_name: seededTableName,
      })
    }
    if (data.cron_runs_first_page) {
      queryClient.setQueryData(queryKeys.admin.cronLogsRecent(sid), data.cron_runs_first_page)
    }
    queryClient.setQueryData(queryKeys.lastSync(sid), data.last_sync ?? null)
    if (data.scoring_labels) {
      queryClient.setQueryData(queryKeys.scoringLabels(sid), data.scoring_labels)
    }
  }
  if (Array.isArray(data.services)) {
    queryClient.setQueryData(queryKeys.services(), {
      services: data.services,
      _section_timings: [],
    })
  }
  const shareBanner = data.share_banner
  if (shareBanner && typeof shareBanner === 'object') {
    queryClient.setQueryData(queryKeys.shareBanner(), {
      sharing_active: !!shareBanner.sharing_active,
      public_url: typeof shareBanner.public_url === 'string' ? shareBanner.public_url : null,
    })
  }
}

// ── Log Field Config Operations ───────────────────────────────────────────────

type LogFieldsCatalogLike =
  | components["schemas"]["LogFieldsCatalogResponse"]
  | null
  | undefined;

function applyGroupDependencies<T extends string | null>(
  groups: Set<T>,
  catalog: LogFieldsCatalogLike,
): void {
  const catalogGroups = catalog?.groups || [];
  let changed = true;
  while (changed) {
    changed = false;
    catalogGroups.forEach((g) => {
      if (
        g.id != null &&
        groups.has(g.id as T) &&
        g.requires &&
        !groups.has(g.requires as T)
      ) {
        groups.add(g.requires as T);
        changed = true;
      }
    });
  }
}

export function resolveEnabledGroupsWithDeps(
  groups: string[] | undefined,
  catalog: LogFieldsCatalogLike,
): Set<string | null> {
  const enabledGroups = new Set<string | null>([null, ...(groups || [])]);
  applyGroupDependencies(enabledGroups, catalog);
  return enabledGroups;
}

export function toggleLogFieldGroup(
  prev: LogFieldsConfig,
  catalog: LogFieldsCatalogLike,
  groupId: string,
  checked: boolean,
): LogFieldsConfig {
  const nextGroups = new Set<string>(prev.groups || []);
  if (checked) {
    nextGroups.add(groupId);
    applyGroupDependencies(nextGroups, catalog);
  } else {
    nextGroups.delete(groupId);
  }
  return { ...prev, groups: Array.from(nextGroups) };
}

export function toggleLogFieldOverride(
  prev: LogFieldsConfig,
  fieldId: string,
  checked: boolean,
  defaultEnabledByGroup: boolean,
): LogFieldsConfig {
  const overrides = { ...(prev.field_overrides || {}) };
  if (checked === defaultEnabledByGroup) {
    delete overrides[fieldId];
  } else {
    overrides[fieldId] = checked;
  }
  return { ...prev, field_overrides: overrides };
}

export function updateLogFieldLimit(
  prev: LogFieldsConfig,
  fieldId: string,
  limit?: number,
): LogFieldsConfig {
  const field_limits = { ...(prev.field_limits || {}) };
  if (limit === undefined) {
    delete field_limits[fieldId];
  } else {
    field_limits[fieldId] = limit;
  }
  return { ...prev, field_limits };
}

export function isLogFieldPresetActive(
  config: LogFieldsConfig | undefined,
  groups: string[],
): boolean {
  if (!groups.length) return false;
  const currentGroups = new Set(config?.groups || []);
  return groups.every((g) => currentGroups.has(g));
}

export function toggleLogFieldPreset(
  prev: LogFieldsConfig,
  catalog: LogFieldsCatalogLike,
  presetGroups: string[],
  isPresetActiveFn?: (groups: string[]) => boolean,
): LogFieldsConfig {
  const currentGroups = new Set<string>(prev.groups || []);
  const allActive = presetGroups.every((g) => currentGroups.has(g));
  const nextGroups = new Set<string>(prev.groups || []);
  const checkActive =
    isPresetActiveFn ?? ((groups: string[]) => isLogFieldPresetActive(prev, groups));

  if (allActive) {
    const otherActivePresetsGroups = new Set<string>();
    if (catalog?.presets) {
      Object.entries(catalog.presets).forEach(([_key, preset]) => {
        if (
          preset.groups.length !== presetGroups.length ||
          !preset.groups.every((g: string) => presetGroups.includes(g))
        ) {
          if (checkActive(preset.groups)) {
            preset.groups.forEach((g: string) => otherActivePresetsGroups.add(g));
          }
        }
      });
    }

    presetGroups.forEach((g) => {
      if (!otherActivePresetsGroups.has(g)) {
        nextGroups.delete(g);
        catalog?.groups?.forEach((cg) => {
          if (
            cg.id != null &&
            cg.requires === g &&
            !otherActivePresetsGroups.has(cg.id)
          ) {
            nextGroups.delete(cg.id);
          }
        });
      }
    });
  } else {
    presetGroups.forEach((g) => nextGroups.add(g));
    applyGroupDependencies(nextGroups, catalog);
  }

  return { ...prev, groups: Array.from(nextGroups) };
}

export function estimateLogFieldBytesPerLine(
  config: LogFieldsConfig | undefined,
  catalog: LogFieldsCatalogLike,
): number {
  if (!catalog?.fields) return 0;
  let total = 0;
  const enabledGroups = new Set(config?.groups || []);
  const overrides = config?.field_overrides || {};
  for (const field of catalog.fields) {
    const inGroup = field.group === null || enabledGroups.has(field.group);
    const override = overrides[field.id];
    if (override === true) {
      total += field.typical_bytes || 0;
      continue;
    }
    if (override === false) continue;
    if (inGroup) total += field.typical_bytes || 0;
  }
  return total;
}

export function evaluateLogFieldPresetSummary(
  config: LogFieldsConfig | undefined,
  catalog: LogFieldsCatalogLike,
): {
  enabledGroupsSet: Set<string>;
  bestPresetName: string | null;
  disabledCount: number;
} {
  const enabledGroupsSet = new Set(config?.groups || []);
  const overrides = config?.field_overrides || {};
  const hasOverrides = Object.keys(overrides).length > 0;
  let bestPresetName: string | null = null;

  if (catalog?.presets && !hasOverrides) {
    for (const [key, preset] of Object.entries(catalog.presets)) {
      const presetGroups = preset.groups;
      if (
        presetGroups.length === enabledGroupsSet.size &&
        presetGroups.every((g) => enabledGroupsSet.has(g))
      ) {
        bestPresetName = preset.label || key;
        break;
      }
    }
  }

  const disabledCount = (catalog?.groups || []).filter(
    (g) => !(g.locked || (g.id != null && enabledGroupsSet.has(g.id))),
  ).length;

  return { enabledGroupsSet, bestPresetName, disabledCount };
}
