// Cloud bridge for the admin Teardown dialog. The dialog posts the self-hosted
// body (`remove_logging`, `remove_rum`, `remove_bucket`, `remove_cloud_files`,
// `remove_cache`, `token`) and expects a progress stream; Cloud's teardown route
// takes its own option names plus the service generation and typed
// confirmation, rejects unknown fields, and returns a job to step.

type RunJob = (jobId: string, signal: AbortSignal) => Response;

type SelfHostedTeardown = {
  service_id?: unknown;
  token?: unknown;
  remove_logging?: unknown;
  remove_rum?: unknown;
  remove_bucket?: unknown;
  remove_cloud_files?: unknown;
};

function failure(message: string, status = 502): Response {
  return Response.json({ detail: { error: message } }, { status });
}

/** Maps the dialog's flags onto Cloud's `Teardown` options. */
export function cloudTeardownOptions(input: SelfHostedTeardown) {
  const removeLogging = input.remove_logging === true;
  const removeSnippets = removeLogging || input.remove_rum === true;
  const removeConfig = removeLogging && removeSnippets;
  return {
    remove_logging: removeLogging,
    remove_snippets: removeSnippets,
    delete_fos_keys: removeConfig,
    delete_data: removeConfig && input.remove_cloud_files === true,
    delete_fos_bucket: removeConfig && input.remove_bucket === true,
  };
}

export async function cloudTeardown(request: Request, runJob: RunJob): Promise<Response | null> {
  if (new URL(request.url).pathname !== "/api/provision/teardown" || request.method !== "POST") return null;
  let input: SelfHostedTeardown;
  try {
    input = JSON.parse(await request.text()) as SelfHostedTeardown;
  } catch (error) {
    if (!(error instanceof SyntaxError)) throw error;
    return failure("invalid_json", 400);
  }
  if (typeof input?.service_id !== "string" || !input.service_id) return failure("service_id_required", 400);
  if (typeof input.token !== "string" || !input.token) return failure("token_required", 400);
  const serviceId = input.service_id;
  const { signal } = request;

  const read = await fetch(`/api/admin/services/${encodeURIComponent(serviceId)}`, { credentials: "include", signal });
  if (!read.ok) return read;
  const { generation } = await read.json() as { generation?: string };
  if (!generation) return failure("Missing native service generation");

  const started = await fetch("/api/provision/teardown", {
    method: "POST",
    credentials: "include",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      service_id: serviceId,
      confirm_service_id: serviceId,
      token: input.token,
      generation,
      ...cloudTeardownOptions(input),
    }),
    signal,
  });
  if (!started.ok) return started;
  const { job } = await started.json() as { job?: { id?: string } };
  if (!job?.id) return failure("Missing native teardown job");
  return runJob(job.id, signal);
}
