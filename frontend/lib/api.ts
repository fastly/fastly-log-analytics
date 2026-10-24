import createClient from "openapi-fetch";
import type { paths } from "@/types/api.generated";
import type { StripIndex, WireParity } from "@/types/api";
import { useDebugStore } from "@/stores/debugStore";
import { useServiceStore } from "@/stores/serviceStore";
import { useSessionRoleStore } from "@/stores/sessionRoleStore";
import { showReadOnlyToast, showBusyToast, showToast } from "@/lib/toast";
import { isUserActive } from "@/lib/userActivity";
import { extractApiError, getPageLoadId, rotatePageLoadId } from "@/lib/api-client";

export { getPageLoadId, rotatePageLoadId, extractApiError };

export function getApiBase() {
  if (process.env.NEXT_PUBLIC_API_URL) return process.env.NEXT_PUBLIC_API_URL;

  const port = process.env.NEXT_PUBLIC_BACKEND_PORT || "8000";
  if (typeof window !== "undefined") {
    // Same-origin everywhere in the browser. A previous version of this
    // function shortcut admin SSH-tunnel pages (loaded from localhost /
    // 127.0.0.1) directly at `http://127.0.0.1:${port}` to skip the
    // Caddy hop. That shortcut became incompatible with three later
    // additions that all assume same-origin requests:
    //   - proxy.ts emits a `connect-src 'self'` CSP that blocks cross-
    //     origin fetches (per-request nonce commit).
    //   - CORS preflight on the cross-origin path needs an OPTIONS-
    //     exemption carve-out in the auth middleware PLUS CORSMiddleware
    //     to attach ACAO; either layer regressing puts us back here.
    // Routing through Caddy via relative URLs makes all three a no-op:
    // same-origin → no CSP violation, Caddy injects X-Proxied-By-Caddy,
    // no CORS preflight needed. The cost is one extra in-VM hop per
    // call, dominated by the actual backend SQL.
    return "";
  }

  return process.env.API_PROXY_URL || `http://127.0.0.1:${port}`;
}

export const client = createClient<paths>({
  baseUrl: getApiBase(),
});

/**
 * Shared client-context header stamper used by both `adminFetch` and the
 * `openapi-fetch` request middleware so `X-Page-Load-ID`,
 * `x-service-id`, and `x-debug-responses` are injected in a single place.
 */
function stampClientContextHeaders(headers: Headers): string | null {
  const currentPageLoadId = getPageLoadId();
  if (currentPageLoadId && !headers.has("X-Page-Load-ID")) {
    headers.set("X-Page-Load-ID", currentPageLoadId);
  }
  try {
    const debug = useDebugStore.getState();
    if ((debug.enabled || debug.apiCallsEnabled) && !headers.has("x-debug-responses")) {
      headers.set("x-debug-responses", "1");
    }
  } catch { /* SSR pre-hydration — fall through without the header */ }
  let sid: string | null = null;
  try {
    sid = useServiceStore.getState().activeServiceId;
    if (!sid && typeof window !== "undefined") {
      sid = new URLSearchParams(window.location.search).get("service");
    }
    if (sid && !headers.has("x-service-id")) {
      headers.set("x-service-id", sid);
    }
  } catch { /* SSR pre-hydration — fall through without the header */ }
  return sid;
}

// Plain ``fetch`` wrapper for the handful of callers that don't go
// through the openapi-fetch client (typically because the response
// type isn't in the OpenAPI spec or the caller wants raw Response).
//
// Same semantics as ``fetch``: returns Response, the caller is
// responsible for ``.json()`` / status checks.
export function adminFetch(
  input: string | URL | Request,
  init: RequestInit = {},
): Promise<Response> {
  const headers = new Headers(init.headers ?? {});
  stampClientContextHeaders(headers);
  return fetch(input, { ...init, headers });
}

type JsonOf<P extends keyof paths, M extends "get" | "post" = "get"> = paths[P] extends {
  [K in M]: { responses: { 200: { content: { "application/json": infer T } } } }
}
  ? T
  : never

// A caller may pin a deliberate narrowing ``T`` of the wire type (required
// fields, literal unions). The trailing guard argument is then required and
// only type-checks while ``T`` keeps WireParity with the generated schema, so a
// backend rename/removal still fails ``tsc`` at the call site.
// Arrays compare element-wise; a free-dict wire shape (no declared keys) only
// requires the local shape to be assignable to it.
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
        : [wire_parity: WireParity<T, W>]

type AdminJsonResult<T> =
  | { ok: true; status: number; data: T }
  | { ok: false; status: number; statusText: string; data: null; errorText: string }

// Typed JSON request over ``adminFetch`` that never throws on a non-2xx
// status — for callers that branch on ``status`` (403 -> unauthorized) or fall
// back to a default. ``spec`` is the OpenAPI path template the concrete ``url``
// instantiates; it only exists to derive the response type from the generated
// schema, so a backend model change fails ``tsc`` here instead of hiding
// behind a hand-written cast.
export async function adminJsonResult<
  P extends keyof paths,
  T = JsonOf<P>,
  M extends "get" | "post" = "get",
>(_spec: P, url: string, init: RequestInit = {}, ..._parity: ParityArgs<T, JsonOf<P, M>>): Promise<AdminJsonResult<T>> {
  const r = await adminFetch(url, init)
  if (!r.ok) return { ok: false, status: r.status, statusText: r.statusText, data: null, errorText: await r.text().catch(() => "") }
  return { ok: true, status: r.status, data: (await r.json()) as T }
}

// Typed JSON request over ``adminFetch``. Throws on a non-2xx status.
export async function adminJson<
  P extends keyof paths,
  T = JsonOf<P>,
  M extends "get" | "post" = "get",
>(
  spec: P,
  url: string,
  init: RequestInit = {},
  errorMessage: (status: number) => string = (status) => `status ${status}`,
  ...parity: ParityArgs<T, JsonOf<P, M>>
): Promise<T> {
  const res = await adminJsonResult<P, T, M>(spec, url, init, ...parity)
  if (!res.ok) throw new Error(errorMessage(res.status))
  return res.data
}

// Paths that don't need an active service (bootstrap before service is
// known, share/login flows, share-analyst chrome). Anything else is
// treated as a service-scoped call, and we abort on the FE if no
// service is set rather than letting the backend return a 400 that
// TanStack would log as an error.
const SERVICELESS_PATH_PREFIXES = [
  "/api/bootstrap",
  "/api/share",        // share-login, tos, heartbeat, etc.
  "/api/admin/share",  // global live-share admin endpoints (status, live, banner, invites, sessions, audit-logs)
  "/api/auth",
  "/api/login",
  "/api/health",
  "/api/debug",
  "/api/admin/debug-settings", // Global setting editable before a service exists
  "/api/admin/usage-logging",  // Global setting editable before a service exists
  "/api/provision",    // fresh-install flow: no active service exists yet —
                       // the wizard (validate/check-config/check-fos/…) MUST
                       // be exempt or step 1 aborts with "No active service".
  // The log-fields catalog is GLOBAL (service_id optional; falls through to
  // the default catalog when absent). The provision wizard's "Log Fields"
  // step fetches it with NO service param — on a fresh install there is no
  // active service yet, so without this exemption the request aborts with
  // NO_SERVICE and step 6 renders zero groups/fields (nothing to select).
  // This is the ONLY route under /api/log-fields/; service-scoped log-field
  // routes live under /api/services/{id}/log-fields, so the prefix is safe.
  "/api/log-fields/catalog",
  // Global admin observability — host/process metrics with NO service
  // dimension (backend handlers take no service param). They're meaningful
  // on a fresh install before any service exists; without these the System
  // Health card hangs forever on its NO_SERVICE loading state. Both prefixes
  // are specific enough not to shadow the service-scoped /api/admin/* routes
  // (log-accounting, slow-queries/count).
  "/api/admin/health-snapshot",
  "/api/admin/system-metrics",
  "/api/admin/health",
  "/api/admin/system-jobs",
  "/api/admin/queries/summary",
  "/api/admin/celery/status",
  "/api/admin/celery/ledger",
  "/api/admin/metric-history",
  // Bot intelligence sources + rDNS cache stats are GLOBAL — the backend
  // handlers (get_all_sources_meta / rdns stats) take no service param, so
  // they're meaningful on a fresh install before any service exists. Without
  // this the admin "Bot Intelligence Sources" panel hangs on "Loading…"
  // forever (its query aborts with NO_SERVICE and the panel only checks
  // !data). The prefix also covers the /{source_id}/refresh subpath.
  "/api/admin/bot-sources",
  // The provisioning wizard's Faro version picker (StorageStep) queries
  // this for the Fastly service id chosen a few steps earlier — before
  // /api/provision/execute has ever run, so there is no service config on
  // disk yet and (on a fresh install with zero services) no active service
  // at all. Match on the route suffix rather than a /api/services prefix so
  // every OTHER service-scoped route still aborts without one.
  "/rum/versions",
];

// 403 error codes that indicate the session itself is dead (not just
// "you can't access this resource"). For these, the user must re-login;
// for others (admin_only, read_only, service_not_authorized), the
// session is still valid and the redirect would be a UX dead-end.
const SESSION_DEAD_403_ERRORS = new Set([
  "fingerprint_mismatch",
  "ip_not_whitelisted",
  "unauthenticated",
]);

// E-3 (audit): when the analyst session is dead, kick the user back to
// /share-login with a return param so they land back on the page they
// were on after re-auth. Without this, every session-expired 401 just
// surfaces as a generic error in a toast / inline alert and the user
// has to manually navigate to /share-login.
function redirectToShareLoginIfSessionDead(): void {
  if (typeof window === "undefined") return;
  const path = window.location.pathname;
  // Already on (or transitioning to) the auth screen — don't loop.
  if (path.startsWith("/share-login")) return;
  // /share-login is the ANALYST recovery flow. An admin (loopback / SSH
  // tunnel) has no analyst credentials and can't use it — bouncing them
  // there on an admin-only 401 (e.g. a provision call missing its Fastly
  // token) ejects the operator mid-task to a useless sign-in screen. Only
  // remote analysts get redirected; for admins the 401 surfaces as a normal
  // error instead. is_remote_analyst is mirrored from bootstrap and stays
  // true for an analyst's whole session, so genuine mid-session expiry still
  // redirects. (Defaults false before bootstrap resolves — safe: an
  // anonymous remote visitor's needs_login redirect is handled in AppLayout.)
  try {
    if (!useSessionRoleStore.getState().isRemoteAnalyst) return;
  } catch { /* store unavailable (SSR/test) — fall through to legacy behavior */ }
  // Preserve the path + query so share-login can bounce them back.
  const returnTo = `${window.location.pathname}${window.location.search}`;
  const target = `/share-login?return=${encodeURIComponent(returnTo)}`;
  // Use replace so the dead-session page doesn't sit in history.
  window.location.replace(target);
}

// Middleware to inject activeServiceId and handle errors
client.use({
  async onRequest({ request }) {
    const activeServiceId = stampClientContextHeaders(request.headers);

    // Idle-timeout activity signal for the analyst session. "0" tells the
    // backend this request is NOT genuine user activity (no mouse/keyboard/
    // scroll within the active window) so it must not reset the 2h idle clock
    // — otherwise automated react-query refetches on a foreground tab keep an
    // idle session alive forever. Harmless on the admin branch (it never
    // touches a session). Set before the activeServiceId early-returns below.
    request.headers.set("X-User-Active", isUserActive() ? "1" : "0");

    if (activeServiceId) {
      return request;
    }
    // No active service at request time. Most paths will 400 — short-
    // circuit instead of making the round-trip. Race we're guarding:
    // useServiceQuery's `enabled: !!activeServiceId` was true when the
    // query mounted, but the store transitioned to null between mount
    // and fetch (page nav, bootstrap re-hydration). Result: queryFn
    // already running, middleware sees null, request goes out without
    // x-service-id, backend's get_source raises 400.
    const url = String(request.url);
    if (SERVICELESS_PATH_PREFIXES.some(p => url.includes(p))) {
      return request;
    }
    // The service-discovery LIST endpoint (GET /api/services) is how the
    // admin finds services before any is active — on a fresh install there
    // IS no service yet, so aborting it surfaces a bogus "Couldn't load
    // services / No active service" banner on the one screen meant to get
    // you started. Exempt it, but match the path EXACTLY (strip the query,
    // endsWith) so service-scoped subpaths (/api/services/{id}/…) still
    // abort. Avoid `new URL()` here: the public deploy's empty base makes
    // request.url relative and URL() would throw (see sse-hook pitfall).
    if (url.split("?")[0].endsWith("/api/services")) {
      return request;
    }
    // Path needs a service we don't have. Throw a sentinel error that
    // TanStack surfaces as a normal query error; the next render with a
    // non-null activeServiceId will refire naturally via queryKey change.
    // Tag with code='NO_SERVICE' so AnalyticsCard renders the loading
    // state (not a red "Something went wrong") and QueryProvider skips
    // retries (re-throwing the same sentinel is pure waste).
    const err = new Error("No active service — request aborted") as Error & { code: string };
    err.code = "NO_SERVICE";
    throw err;
  },
  async onResponse({ request, response }) {
    if (
      response.status === 503 &&
      String(request.url).includes("/api/admin/clickhouse/status")
    ) {
      const payload = await response.clone().json().catch(() => null);
      if (payload && typeof payload === "object" && "health" in payload) {
        return new Response(JSON.stringify(payload), {
          status: 200,
          headers: response.headers,
        });
      }
    }
    if (!response.ok) {
      const error = await response.json().catch(() => ({ message: "An unknown error occurred" }));
      const msg = extractApiError(error);

      // E-3 (audit): redirect to /share-login when the session is dead.
      // Two triggers: 401 (cookie expired or absent) is always a session
      // problem, and select 403s (fingerprint_mismatch, ip_not_whitelisted)
      // mean the backend just invalidated the session mid-flight. Other
      // 403s (admin_only, read_only, service_not_authorized) leave the
      // session intact and stay as inline / toast errors so we don't yank
      // the user off the page they're on.
      const errorCode =
        (error as { detail?: { error?: string }; error?: string })?.detail?.error ??
        (error as { error?: string })?.error;
      if (response.status === 401) {
        redirectToShareLoginIfSessionDead();
      } else if (response.status === 403) {
        if (typeof errorCode === "string" && SESSION_DEAD_403_ERRORS.has(errorCode)) {
          redirectToShareLoginIfSessionDead();
        }
      }

      // N-6 / M-1: any analyst-blocked mutation (Save View, Alerts modal,
      // etc.) used to fail silently — the modal would stay open with no
      // toast, no banner. Surface a global toast for the specific
      // ``403 read_only`` error so every analyst-visible write path gets
      // useful feedback without per-modal plumbing.
      if (response.status === 403 && msg === 'read_only') {
        showReadOnlyToast();
      }

      // E-5 (audit): 503 with detail.busy = true means the DuckDB pool is
      // saturated (DBBusyError / _PoolBusy). Looked identical to a generic
      // 500 in the UI — same error toast, same lack of retry signal. Tag
      // the error so QueryProvider can grant a third retry, and fire a
      // dedicated "Server busy, retrying…" toast so the analyst knows the
      // app is doing something instead of staring at a broken chart.
      const isBusy503 =
        response.status === 503 &&
        (error as { detail?: { busy?: boolean } })?.detail?.busy === true;
      if (isBusy503) {
        showBusyToast();
      }

      // U-4 (audit): mutations (PUT/PATCH/DELETE) used to fail silently — the
      // call sites typically wrap the request in try/catch and either swallow
      // the rejection or just console.error it, so the modal would close with
      // no UI signal that the delete/save/revoke didn't take. Fire a generic
      // error toast for any unhandled mutation failure. Cases already covered
      // above (session-dead 401/403 redirect, 403 read_only, 503 busy) skip
      // this so we don't double-toast. POST is excluded because the codebase
      // uses POST for analytics queries (/api/insights, /api/dashboard/
      // aggregates, etc.); component-level error handling on those queries
      // already surfaces inline errors, and toasting every analytics 5xx
      // would be noise. POST mutations (save alert, ingest-logs) opt in
      // explicitly at the call site.
      const method = request.method?.toUpperCase();
      const isMutation = method === 'PUT' || method === 'PATCH' || method === 'DELETE';
      const alreadyHandled =
        (response.status === 403 && msg === 'read_only') ||
        isBusy503 ||
        response.status === 401 ||
        (response.status === 403 && typeof errorCode === "string" && SESSION_DEAD_403_ERRORS.has(errorCode));
      if (isMutation && !alreadyHandled) {
        showToast(msg || 'Request failed', 'error');
      }

      const err = new Error(msg) as Error & {
        status?: number;
        response?: { status: number };
        busy?: boolean;
      };
      err.status = response.status;
      err.response = { status: response.status };
      if (isBusy503) err.busy = true;
      throw err;
    }
    return response;
  },
});
