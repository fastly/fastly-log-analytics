import { dashboardCacheScope } from "./dashboard/sources";

type ServicesSnapshot = { status: number; contentType: string | null; body: string };
type SharedRead = { value: Promise<ServicesSnapshot>; controller: AbortController; waiters: number };
const inFlightServices = new Map<string, SharedRead>();

// A page asks for RUM status and versions together; both read the same services
// list, so concurrent callers share one request. Nothing is kept once it settles.
async function readServices(signal: AbortSignal): Promise<Response> {
  signal.throwIfAborted();
  const key = dashboardCacheScope();
  let shared = inFlightServices.get(key);
  if (!shared) {
    const controller = new AbortController();
    const value = fetch("/api/services", { signal: controller.signal }).then(async (response) => ({
      status: response.status, contentType: response.headers.get("content-type"), body: await response.text(),
    }));
    const created: SharedRead = { value, controller, waiters: 0 };
    shared = created;
    inFlightServices.set(key, created);
    const settle = () => { if (inFlightServices.get(key) === created) inFlightServices.delete(key); };
    value.then(settle, settle);
  }
  const entry = shared;
  entry.waiters++;
  let onAbort!: () => void;
  const aborted = new Promise<never>((_, reject) => {
    onAbort = () => {
      reject(signal.reason);
      // The last caller to give up cancels the read; earlier ones leave it running for the rest.
      if (--entry.waiters === 0) {
        if (inFlightServices.get(key) === entry) inFlightServices.delete(key);
        entry.controller.abort(signal.reason);
      }
    };
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
    if (!signal.aborted) entry.waiters--;
  }
}

export async function cloudServiceStatus(request: Request): Promise<Response | null> {
  const path = new URL(request.url).pathname;
  const match = path.match(/^\/api\/services\/([^/]+)\/(rum|cmcd)\/(status|versions)$/);
  if (!match || request.method !== "GET") return null;
  const response = await readServices(request.signal);
  if (!response.ok) return response;
  const payload = await response.json() as { services?: Array<{
    service_id: string;
    rum?: Record<string, unknown>;
    cmcd?: Record<string, unknown>;
    scoring?: Record<string, unknown>;
    rum_enabled?: boolean;
    faro_version?: string;
  }> };
  const service = payload.services?.find((item) => item.service_id === decodeURIComponent(match[1]));
  if (!service) return Response.json({ detail: { error: "service_not_found" } }, { status: 404 });
  if (match[3] === "versions") {
    if (match[2] !== "rum") return null;
    const versions = await fetch("/api/rum/versions", { signal: request.signal });
    if (!versions.ok) return versions;
    const available = await versions.json() as { versions: string[]; default: string };
    const current = service.rum?.faro_version ?? service.faro_version ?? null;
    return Response.json({
      available: available.versions,
      current,
      latest: available.default,
      update_available: current !== null && current !== available.default,
    });
  }
  const config = service[match[2] as "rum" | "cmcd"];
  return Response.json({
    ...config,
    enabled: config?.enabled === true || match[2] === "rum" && service.rum_enabled === true,
  });
}
