// Cloud bridge for the provisioning wizard's execute step. The wizard posts
// the self-hosted body (CDN proxy, cron sync and flat `cmcd_*` keys, numeric
// strings); Cloud's `Provision` input rejects unknown fields and reads the
// stream from a job, so this translates the body and steps the job.

type RunJob = (jobId: string, signal: AbortSignal) => Response;

const SELF_HOSTED_ONLY = new Set(["cdn_service_name", "cdn_service_id", "cdn_shield", "cdn_url", "cdn_prefix", "enable_cron_sync", "ngwaf_workspace_id"]);

function failure(message: string, status = 502): Response {
  return Response.json({ detail: { error: message } }, { status });
}

function optionalNumber(value: unknown): number | undefined {
  if (value === null || value === undefined || value === "") return undefined;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : undefined;
}

/** Maps the wizard's deploy body onto Cloud's `Provision` input. */
export function cloudProvisionBody(input: Record<string, unknown>): Record<string, unknown> {
  const body: Record<string, unknown> = {};
  for (const [key, value] of Object.entries(input)) {
    if (SELF_HOSTED_ONLY.has(key) || key.startsWith("cmcd_")) continue;
    body[key] = value;
  }
  if ("rum_log_period" in input) {
    const period = optionalNumber(input.rum_log_period);
    if (period === undefined) delete body.rum_log_period;
    else body.rum_log_period = period;
  }
  if (typeof input.log_fields === "string") {
    try {
      body.log_fields = JSON.parse(input.log_fields);
    } catch (error) {
      if (!(error instanceof SyntaxError)) throw error;
    }
  }
  if (input.cmcd_enabled !== undefined || input.cmcd_mode !== undefined || input.cmcd_version !== undefined) {
    body.cmcd = {
      enabled: input.cmcd_enabled === true,
      mode: typeof input.cmcd_mode === "string" && input.cmcd_mode ? input.cmcd_mode : "query_string",
      version: optionalNumber(input.cmcd_version) ?? 1,
    };
  }
  return body;
}

export async function cloudProvisionExecute(request: Request, runJob: RunJob): Promise<Response | null> {
  if (new URL(request.url).pathname !== "/api/provision/execute" || request.method !== "POST") return null;
  let input: Record<string, unknown>;
  try {
    input = JSON.parse(await request.text()) as Record<string, unknown>;
  } catch (error) {
    if (!(error instanceof SyntaxError)) throw error;
    return failure("invalid_json", 400);
  }
  if (!input || typeof input !== "object" || Array.isArray(input)) return failure("invalid_input", 400);
  if (typeof input.token !== "string" || !input.token) return failure("token_required", 400);
  if (typeof input.service_id !== "string" || !input.service_id) return failure("service_id_required", 400);
  const { signal } = request;

  const started = await fetch("/api/provision/execute", {
    method: "POST",
    credentials: "include",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(cloudProvisionBody(input)),
    signal,
  });
  if (!started.ok) return started;
  const { job } = await started.json() as { job?: { id?: string } };
  if (!job?.id) return failure("Missing native provisioning job");
  return runJob(job.id, signal);
}
