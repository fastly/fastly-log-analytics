type RunJob = (jobId: string, signal: AbortSignal) => Response;

function failure(message: string, status = 502): Response {
  return Response.json({ detail: { error: message } }, { status });
}

export async function cloudRumMutation(request: Request, runJob: RunJob): Promise<Response | null> {
  const match = new URL(request.url).pathname.match(/^\/api\/services\/([^/]+)\/rum\/(enable|disable|upgrade|settings)$/);
  if (!match || request.method !== "POST") return null;
  const [, serviceId, action] = match;
  const optionalBody = action === "enable" || action === "disable";
  const body = await request.text();
  let input: Record<string, unknown> | null;
  try {
    input = body === "" && optionalBody ? {} : JSON.parse(body) as Record<string, unknown> | null;
  } catch (error) {
    if (!(error instanceof SyntaxError)) throw error;
    return failure("invalid_json", 400);
  }
  if (input === null && optionalBody) input = {};
  if (!input || typeof input !== "object" || Array.isArray(input)) return failure("invalid_input", 400);
  if (action === "disable" && (input.remove_cloud_files || input.remove_bucket)) {
    return failure("Cloud RUM disable preserves retained data and does not delete buckets", 400);
  }
  if (action === "upgrade" && (typeof input.version !== "string" || !input.version)) {
    return failure("version_required", 400);
  }
  const path = `/api/admin/services/${serviceId}`;
  const current = await fetch(path, { credentials: "include", signal: request.signal });
  if (!current.ok) return current;
  const { service, generation } = await current.json() as {
    service?: { rum?: Record<string, unknown> };
    generation?: string;
  };
  if (!service?.rum || !generation) return failure("Missing native RUM configuration or generation");
  const rum = { ...service.rum };
  if (action === "enable" || action === "disable") rum.enabled = action === "enable";
  if (action === "upgrade") rum.faro_version = input.version;
  if (action === "settings") {
    for (const key of ["capture_vitals", "capture_performance", "capture_errors", "capture_events", "custom_condition"]) {
      if (key in input) rum[key] = input[key];
    }
  }
  const reconciled = await fetch(new Request(new URL(`${path}/reconcile`, request.url), {
    method: "POST",
    credentials: "include",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ generation, rum, activate: input.activate !== false, dry_run: input.dry_run === true }),
    signal: request.signal,
  }));
  if (!reconciled.ok) return reconciled;
  const { job } = await reconciled.json() as { job?: { id?: string } };
  if (!job?.id) return failure("Missing native RUM reconcile job");
  const stream = runJob(job.id, request.signal);
  if (action !== "settings") return stream;
  const events = (await stream.text()).split("\n")
    .filter((line) => line.startsWith("data: "))
    .map((line) => JSON.parse(line.slice(6)) as {
      type?: string; error?: string; message?: string; job?: { status?: string };
    });
  const error = events.find((event) => event.type === "error");
  if (error) return failure(error.error ?? error.message ?? "RUM reconciliation failed");
  if (!events.some((event) => event.type === "done" && event.job?.status === "complete")) {
    return failure("RUM reconciliation did not complete");
  }
  return Response.json({ ok: true });
}
