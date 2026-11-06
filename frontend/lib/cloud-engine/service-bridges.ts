export async function cloudServiceStatus(request: Request): Promise<Response | null> {
  const path = new URL(request.url).pathname;
  const match = path.match(/^\/api\/services\/([^/]+)\/(rum|cmcd)\/(status|versions)$/);
  if (!match || request.method !== "GET") return null;
  const response = await fetch("/api/services", { signal: request.signal });
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
