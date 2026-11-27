import { fetchCloudMetadata } from "./metadata-cache";

export interface CloudServiceStatusOptions {
  role?: "admin" | "analyst" | "analyst-no-pii";
  isAnalyst?: boolean;
}

export async function cloudServiceStatus(
  request: Request,
  options?: CloudServiceStatusOptions,
): Promise<Response | null> {
  const path = new URL(request.url).pathname;
  const match = path.match(/^\/api\/services\/([^/]+)\/(rum|cmcd)\/(status|versions)$/);
  if (!match || request.method !== "GET") return null;
  // Bootstrap and the page's scans already read the services list; share that answer.
  const response = await fetchCloudMetadata("/api/services", request.signal);
  if (!response.ok) return response;
  const payload = await response.json() as { services?: Array<{
    service_id: string;
    rum?: Record<string, unknown>;
    cmcd?: Record<string, unknown>;
    scoring?: Record<string, unknown>;
    rum_enabled?: boolean;
    faro_version?: string;
  }> };
  let targetServiceId: string;
  try {
    targetServiceId = decodeURIComponent(match[1]);
  } catch {
    return Response.json({ detail: { error: "invalid_service_id" } }, { status: 400 });
  }
  const service = payload.services?.find((item) => item.service_id === targetServiceId);
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
  const enabled = config?.enabled === true || match[2] === "rum" && service.rum_enabled === true;
  const role = options?.role ?? request.headers.get("x-analyst-role") ?? undefined;
  const isAnalyst = Boolean(options?.isAnalyst || role === "analyst" || role === "analyst-no-pii");
  if (match[2] === "rum") {
    const enabledAt = (config?.enabled_at as string | null | undefined) ?? null;
    if (isAnalyst) {
      return Response.json({
        enabled,
        enabled_at: enabledAt,
      });
    }
    return Response.json({
      ...config,
      enabled,
      enabled_at: enabledAt,
      supports_storage_purge: false,
    });
  }
  if (isAnalyst) {
    return Response.json({
      enabled,
    });
  }
  return Response.json({
    ...config,
    enabled,
  });
}
