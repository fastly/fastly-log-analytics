// Cloud bridge for the per-service Log Settings modal. The modal speaks the
// self-hosted routes (`/api/services/{id}/logging-settings`, `/log-fields`);
// Cloud keeps the same values on the service record, edited through a
// generation-checked PATCH and activated by a reconcile job.

type RunJob = (jobId: string, signal: AbortSignal) => Response;

type CloudService = {
  fos_prefix?: string;
  log_period?: number;
  sample_rate?: number;
  edge_only?: boolean;
  custom_condition?: string;
  log_fields?: Record<string, unknown>;
  cmcd?: { enabled?: boolean; mode?: string; version?: number };
  last_activated_version?: number | null;
};

const ROUTE = /^\/api\/services\/([^/]+)\/(logging-settings|logging-settings\/update|log-fields)$/;

function failure(message: string, status = 502): Response {
  return Response.json({ detail: { error: message } }, { status });
}

async function readService(serviceId: string, signal: AbortSignal) {
  const response = await fetch(`/api/admin/services/${encodeURIComponent(serviceId)}`, { credentials: "include", signal });
  if (!response.ok) return { response };
  const { service, generation } = await response.json() as { service?: CloudService; generation?: string };
  if (!service || !generation) return { response: failure("Missing native service configuration or generation") };
  return { service, generation };
}

async function patchService(serviceId: string, body: Record<string, unknown>, signal: AbortSignal) {
  const response = await fetch(`/api/admin/services/${encodeURIComponent(serviceId)}`, {
    method: "PATCH",
    credentials: "include",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body),
    signal,
  });
  if (!response.ok) return { response };
  const { service, generation } = await response.json() as { service?: CloudService; generation?: string };
  if (!service || !generation) return { response: failure("Missing native service configuration or generation") };
  return { service, generation };
}

function loggingSettings(service: CloudService) {
  return {
    ok: true,
    _is_cached: false,
    prefix: service.fos_prefix ?? "",
    period: service.log_period,
    sample_rate: service.sample_rate,
    edge_only: service.edge_only,
    custom_condition: service.custom_condition ?? "",
    format_match: true,
    version: service.last_activated_version ?? null,
    cmcd: {
      enabled: service.cmcd?.enabled ?? false,
      mode: service.cmcd?.mode ?? "query_string",
      version: service.cmcd?.version ?? 1,
    },
  };
}

function logFields(service: CloudService) {
  return { _is_cached: false, log_fields: service.log_fields ?? {}, waf_warning: false, history: [], estimate: 0 };
}

function parseInteger(value: string | null, name: string): number | Response {
  if (value === null || value === "") return failure(`${name}_required`, 400);
  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed < 0) return failure(`invalid_${name}`, 400);
  return parsed;
}

export async function cloudLoggingSettings(request: Request, runJob: RunJob): Promise<Response | null> {
  const url = new URL(request.url);
  const match = url.pathname.match(ROUTE);
  if (!match) return null;
  const [, serviceId, route] = match;
  const { signal } = request;

  if (route === "logging-settings" && request.method === "GET") {
    const read = await readService(serviceId, signal);
    if (read.response) return read.response;
    return Response.json(loggingSettings(read.service));
  }

  if (route === "log-fields" && request.method === "GET") {
    const read = await readService(serviceId, signal);
    if (read.response) return read.response;
    return Response.json(logFields(read.service));
  }

  if (route === "log-fields" && request.method === "POST") {
    let input: { log_fields?: unknown };
    try {
      input = JSON.parse(await request.text()) as { log_fields?: unknown };
    } catch (error) {
      if (!(error instanceof SyntaxError)) throw error;
      return failure("invalid_json", 400);
    }
    if (!input?.log_fields || typeof input.log_fields !== "object" || Array.isArray(input.log_fields)) {
      return failure("invalid_input", 400);
    }
    const read = await readService(serviceId, signal);
    if (read.response) return read.response;
    const patched = await patchService(serviceId, { generation: read.generation, log_fields: input.log_fields }, signal);
    if (patched.response) return patched.response;
    return Response.json({ ok: true, ...logFields(patched.service) });
  }

  if (route === "logging-settings/update" && request.method === "GET") {
    const period = parseInteger(url.searchParams.get("period"), "period");
    if (period instanceof Response) return period;
    const sampleRate = parseInteger(url.searchParams.get("sample_rate"), "sample_rate");
    if (sampleRate instanceof Response) return sampleRate;
    const read = await readService(serviceId, signal);
    if (read.response) return read.response;
    const cmcd = { ...read.service.cmcd };
    if (url.searchParams.has("cmcd_enabled")) cmcd.enabled = url.searchParams.get("cmcd_enabled") === "true";
    if (url.searchParams.has("cmcd_mode")) cmcd.mode = url.searchParams.get("cmcd_mode") ?? cmcd.mode;
    if (url.searchParams.has("cmcd_version")) {
      const version = parseInteger(url.searchParams.get("cmcd_version"), "cmcd_version");
      if (version instanceof Response) return version;
      cmcd.version = version;
    }
    const patched = await patchService(serviceId, {
      generation: read.generation,
      log_period: period,
      sample_rate: sampleRate,
      edge_only: url.searchParams.get("edge_only") !== "false",
      custom_condition: url.searchParams.get("custom_condition") ?? "",
      cmcd,
    }, signal);
    if (patched.response) return patched.response;
    const reconciled = await fetch(`/api/admin/services/${encodeURIComponent(serviceId)}/reconcile`, {
      method: "POST",
      credentials: "include",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ generation: patched.generation, activate: true, dry_run: false }),
      signal,
    });
    if (!reconciled.ok) return reconciled;
    const { job } = await reconciled.json() as { job?: { id?: string } };
    if (!job?.id) return failure("Missing native reconcile job");
    return runJob(job.id, signal);
  }

  return null;
}
