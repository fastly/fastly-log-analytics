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

// Middleware to inject activeServiceId and handle errors
client.use({
  async onRequest({ request }) {
    const { activeServiceId } = useServiceStore.getState();
    if (activeServiceId) {
      request.headers.set("x-service-id", activeServiceId);
    }
    return request;
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
