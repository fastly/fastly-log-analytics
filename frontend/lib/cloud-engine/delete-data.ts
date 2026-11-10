// Cloud bridge for the admin Delete Data dialog. The dialog streams the
// self-hosted route (`POST /api/admin/reset-logs?service_id=…`); Cloud runs
// Delete Data as a job that the edge clock advances, so this starts the job
// through the generation-checked native route and streams its status until it
// reaches a terminal state.

type DeleteDataStatus = {
  status: "none" | "fencing" | "resetting" | "cleanup" | "complete" | "failed" | "cancelled";
  error?: string | null;
  messages?: { at: number; text: string }[];
  deleted?: Record<string, number>;
  bytes?: number;
};

const TERMINAL = new Set(["complete", "failed", "cancelled", "none"]);

let pollMs = 5000;

/** Test hook: how often the job status is read while streaming. */
export function setDeleteDataPollInterval(ms: number): void {
  pollMs = ms;
}

function failure(message: string, status = 502): Response {
  return Response.json({ detail: { error: message } }, { status });
}

function sleep(ms: number, signal: AbortSignal): Promise<void> {
  return new Promise((resolve) => {
    const timer = setTimeout(done, ms);
    function done() {
      signal.removeEventListener("abort", done);
      clearTimeout(timer);
      resolve();
    }
    signal.addEventListener("abort", done, { once: true });
  });
}

function summarize(job: DeleteDataStatus): string {
  const deleted = Object.entries(job.deleted ?? {}).map(([kind, count]) => `${count} ${kind}`).join(", ");
  return deleted ? `Deleted ${deleted}.` : "Delete Data complete.";
}

export async function cloudDeleteData(request: Request): Promise<Response | null> {
  const url = new URL(request.url);
  if (url.pathname !== "/api/admin/reset-logs" || request.method !== "POST") return null;
  const serviceId = url.searchParams.get("service_id");
  if (!serviceId) return failure("service_id_required", 400);
  let input: { confirm?: unknown };
  try {
    input = JSON.parse(await request.text()) as { confirm?: unknown };
  } catch (error) {
    if (!(error instanceof SyntaxError)) throw error;
    return failure("invalid_json", 400);
  }
  if (input?.confirm !== serviceId) return failure("confirm_mismatch", 400);
  const { signal } = request;
  const base = `/api/admin/services/${encodeURIComponent(serviceId)}`;

  const read = await fetch(base, { credentials: "include", signal });
  if (!read.ok) return read;
  const { generation } = await read.json() as { generation?: string };
  if (!generation) return failure("Missing native service generation");

  const started = await fetch(`${base}/delete-data`, {
    method: "POST",
    credentials: "include",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ confirm_service_id: serviceId, generation }),
    signal,
  });
  if (!started.ok) return started;
  const { job: initial } = await started.json() as { job?: DeleteDataStatus };
  if (!initial) return failure("Missing native Delete Data job");

  const encoder = new TextEncoder();
  return new Response(new ReadableStream<Uint8Array>({
    async start(controller) {
      const emit = (payload: Record<string, unknown>) => {
        controller.enqueue(encoder.encode(`data: ${JSON.stringify(payload)}\n\n`));
      };
      try {
        let job = initial;
        let seen = 0;
        let lastStatus = "";
        for (;;) {
          for (const entry of (job.messages ?? []).slice(seen)) emit({ type: "status", message: entry.text });
          seen = job.messages?.length ?? seen;
          if (job.status !== lastStatus && !TERMINAL.has(job.status)) {
            emit({ type: "status", status: job.status, message: `${job.status}: waiting for the edge clock to advance the job` });
            lastStatus = job.status;
          }
          if (job.status === "complete") {
            emit({ type: "done", status: "complete", message: summarize(job) });
            break;
          }
          if (TERMINAL.has(job.status)) {
            const message = job.error ?? (job.status === "cancelled" ? "Delete Data was cancelled." : "Delete Data job disappeared.");
            emit({ type: "error", status: job.status, error: message, message });
            break;
          }
          await sleep(pollMs, signal);
          if (signal.aborted) break;
          const poll = await fetch(`${base}/delete-data`, { credentials: "include", signal });
          if (!poll.ok) {
            const message = `Delete Data status read failed (${poll.status})`;
            emit({ type: "error", status: "failed", error: message, message });
            break;
          }
          job = await poll.json() as DeleteDataStatus;
        }
        controller.close();
      } catch (error) {
        controller.error(error);
      }
    },
  }), { headers: { "content-type": "text/event-stream" } });
}
