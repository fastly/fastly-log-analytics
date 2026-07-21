import createClient from "openapi-fetch";
import type { paths } from "@/types/api.generated";
import { useAdminTokenStore } from "@/stores/adminTokenStore";
import { useDebugStore } from "@/stores/debugStore";
import { useServiceStore } from "@/stores/serviceStore";
import { useSessionRoleStore } from "@/stores/sessionRoleStore";
import { showReadOnlyToast, showBusyToast, showToast } from "@/lib/toast";
import { isUserActive } from "@/lib/userActivity";

export function extractApiError(error: unknown): string {
  if (!error) return 'Unknown error'
  if (typeof error === 'string') return error
  if (error instanceof Error && error.message) return error.message
  const e = error as Record<string, any>
  if (e.detail) {
    if (typeof e.detail === 'string') return e.detail
    if (Array.isArray(e.detail))
      return e.detail.map((err: any) => `${err.loc?.at(-1) ?? 'Field'}: ${err.msg}`).join(', ')
    // Legacy multi-message validation: {errors: ["msg1", "msg2"]}
    if (Array.isArray(e.detail.errors)) return e.detail.errors.join(', ')
    // Canonical ``validation_failed`` helper output: {error: code, messages: [...]}
    // Prefer the human-readable messages array over the machine code when
    // both are present — the messages are what the operator can act on.
    if (Array.isArray(e.detail.messages) && e.detail.messages.length > 0) {
      return e.detail.messages.join(', ')
    }
    // Unified envelope shape from backend.utils.router_utils.make_error:
    // {error: code, message: human-readable, error_id?: ...}. Prefer the
    // human-readable message; fall back to the machine code so callers
    // without a message still surface something actionable.
    if (typeof e.detail.message === 'string' && e.detail.message) return e.detail.message
    if (e.detail.error) return String(e.detail.error)
    return JSON.stringify(e.detail)
  }
  if (typeof e.message === 'string' && e.message) return e.message
  if (e.error) return String(e.error)
  if (Array.isArray(e.errors)) return e.errors.join(', ')
  try { return JSON.stringify(error) } catch { return 'Unknown error' }
}

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
    //   - The admin-token gate (ADMIN_SHARED_SECRET) returns 401 on
    //     direct-to-backend requests; only Caddy-fronted paths carry
    //     the token-bearing context the SPA needs.
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

// Plain ``fetch`` wrapper for the handful of callers that don't go
// through the openapi-fetch client (typically because the response
// type isn't in the OpenAPI spec or the caller wants raw Response).
// Injects ``X-Admin-Token`` from the same Zustand store the openapi-
// fetch middleware reads from, so callers like OperationsOverview
// and admin/queries that hand-roll their fetch don't have to
// duplicate the header-injection logic and don't 401 on the admin
// branch when the shared-secret gate is on.
//
// Same semantics as ``fetch``: returns Response, the caller is
// responsible for ``.json()`` / status checks.
export function adminFetch(
  input: string | URL | Request,
  init: RequestInit = {},
): Promise<Response> {
  const headers = new Headers(init.headers ?? {})
  try {
    const token = useAdminTokenStore.getState().token
    if (token && !headers.has("X-Admin-Token")) {
      headers.set("X-Admin-Token", token)
    }
  } catch { /* SSR pre-hydration — fall through without the header */ }
  // Service-scoped admin endpoints resolve the service from the x-service-id
  // header (same as the openapi-fetch `client` middleware below). adminFetch
  // historically only stamped the admin token, so service-scoped admin calls
  // (log-accounting, slow-queries/count) silently resolved to the backend's
  // DEFAULT service — which is why the ingest-gap card never changed when you
  // switched services. Mirror the client middleware and stamp the active
  // service. Global admin endpoints (live-query registry, host metrics) ignore
  // it; path-scoped /api/services/{id}/* calls already carry the id.
  try {
    const sid = useServiceStore.getState().activeServiceId
    if (sid && !headers.has("x-service-id")) {
      headers.set("x-service-id", sid)
    }
  } catch { /* SSR pre-hydration — fall through without the header */ }
  return fetch(input, { ...init, headers })
}

// Paths that don't need an active service (bootstrap before service is
// known, share/login flows, share-analyst chrome). Anything else is
// treated as a service-scoped call, and we abort on the FE if no
// service is set rather than letting the backend return a 400 that
// TanStack would log as an error.
const SERVICELESS_PATH_PREFIXES = [
  "/api/bootstrap",
  "/api/share",        // share-login, share-status, etc.
  "/api/auth",
  "/api/login",
  "/api/health",
  "/api/debug",
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
  "/api/admin/metric-history",
  // Bot intelligence sources + rDNS cache stats are GLOBAL — the backend
  // handlers (get_all_sources_meta / rdns stats) take no service param, so
  // they're meaningful on a fresh install before any service exists. Without
  // this the admin "Bot Intelligence Sources" panel hangs on "Loading…"
  // forever (its query aborts with NO_SERVICE and the panel only checks
  // !data). The prefix also covers the /{source_id}/refresh subpath.
  "/api/admin/bot-sources",
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

// Phase Q: 401s carrying these codes are the admin shared-secret gate
// rejecting the request — not an analyst session dying. Skip the
// /share-login redirect (admin doesn't use that flow) and clear the
// cached token so useBootstrap's next refetch picks up a fresh one.
const ADMIN_TOKEN_401_ERRORS = new Set([
  "admin_token_required",
  "admin_token_invalid",
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
    // The ``_debug_queries`` / ``_debug_calls`` envelope is ~40 KB on
    // /api/insights cold loads and ~5-15 KB on every other admin call.
    // Opt in via this header only when the admin Debug Panel toggle is
    // on, so the default admin payload stays slim. Backend strips the
    // envelope when the header is absent.
    try {
      const debug = useDebugStore.getState();
      if (debug.enabled || debug.apiCallsEnabled) {
        request.headers.set("x-debug-responses", "1");
      }
    } catch { /* zustand may not be hydrated yet on SSR — leave header off */ }

    // Phase Q: when the backend has ADMIN_SHARED_SECRET configured it
    // ships the token down in /api/bootstrap's settings.admin_token and
    // useBootstrap mirrors it into this store. We inject it here so every
    // outbound admin request carries the second-factor header without
    // each caller wiring it up. Analyst (Fastly-fronted) bootstraps
    // return null, the store stays empty, and this no-ops on the analyst
    // branch — so the Fastly path is unaffected.
    try {
      const adminToken = useAdminTokenStore.getState().token;
      if (adminToken) {
        request.headers.set("X-Admin-Token", adminToken);
      }
    } catch { /* SSR pre-hydration — interceptor will fire on the next request */ }

    // Idle-timeout activity signal for the analyst session. "0" tells the
    // backend this request is NOT genuine user activity (no mouse/keyboard/
    // scroll within the active window) so it must not reset the 2h idle clock
    // — otherwise automated react-query refetches on a foreground tab keep an
    // idle session alive forever. Harmless on the admin branch (it never
    // touches a session). Set before the activeServiceId early-returns below.
    request.headers.set("X-User-Active", isUserActive() ? "1" : "0");

    const { activeServiceId } = useServiceStore.getState();
    if (activeServiceId) {
      request.headers.set("x-service-id", activeServiceId);
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
        // Admin shared-secret 401s are NOT a dead analyst session — clear
        // the stale token so the next bootstrap refetch reseeds it, then
        // leave the error to surface as a normal toast instead of
        // bouncing the admin to /share-login (which they don't use).
        if (typeof errorCode === "string" && ADMIN_TOKEN_401_ERRORS.has(errorCode)) {
          try { useAdminTokenStore.getState().setToken(null); } catch { /* SSR */ }
        } else {
          redirectToShareLoginIfSessionDead();
        }
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
        (response.status === 403 &&
          SESSION_DEAD_403_ERRORS.has(
            (error as { detail?: { error?: string }; error?: string })?.detail?.error ??
              (error as { error?: string })?.error ??
              ''
          ));
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
