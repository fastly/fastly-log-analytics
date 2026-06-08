import createClient from "openapi-fetch";
import type { paths } from "@/types/api.generated";
import { useServiceStore } from "@/stores/serviceStore";

export function extractApiError(error: unknown): string {
  if (!error) return 'Unknown error'
  if (typeof error === 'string') return error
  if (error instanceof Error && error.message) return error.message
  const e = error as Record<string, any>
  if (e.detail) {
    if (typeof e.detail === 'string') return e.detail
    if (Array.isArray(e.detail))
      return e.detail.map((err: any) => `${err.loc?.at(-1) ?? 'Field'}: ${err.msg}`).join(', ')
    if (Array.isArray(e.detail.errors)) return e.detail.errors.join(', ')
    if (e.detail.error) return String(e.detail.error)
    return JSON.stringify(e.detail)
  }
  if (e.error) return String(e.error)
  if (Array.isArray(e.errors)) return e.errors.join(', ')
  try { return JSON.stringify(error) } catch { return 'Unknown error' }
}

export function getApiBase() {
  if (process.env.NEXT_PUBLIC_API_URL) return process.env.NEXT_PUBLIC_API_URL;

  const port = process.env.NEXT_PUBLIC_BACKEND_PORT || "8000";
  if (typeof window !== "undefined") {
    const host = window.location.hostname;
    // Admin SSH tunnel: page loaded from localhost/127.0.0.1 means the user
    // is forwarding both :3000 (frontend) AND :8000 (backend) via SSH. Hit
    // backend directly to skip the Next.js rewrite proxy hop on every API
    // call. No CSP in this path (Caddy isn't in the chain).
    if (host === "localhost" || host === "127.0.0.1" || host === "[::1]") {
      return `${window.location.protocol}//127.0.0.1:${port}`;
    }
    // Public deploy: relative URLs go through Fastly → Caddy → backend.
    return "";
  }

  return process.env.API_PROXY_URL || `http://127.0.0.1:${port}`;
}

export const client = createClient<paths>({
  baseUrl: getApiBase(),
});

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
];

// Middleware to inject activeServiceId and handle errors
client.use({
  async onRequest({ request }) {
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
    // Path needs a service we don't have. Throw a sentinel error that
    // TanStack surfaces as a normal query error; the next render with a
    // non-null activeServiceId will refire naturally via queryKey change.
    throw new Error("No active service — request aborted");
  },
  async onResponse({ response }) {
    if (!response.ok) {
      const error = await response.json().catch(() => ({ message: "An unknown error occurred" }));
      const msg = extractApiError(error);
      throw new Error(msg);
    }
    return response;
  },
});
