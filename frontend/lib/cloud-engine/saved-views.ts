// Saved views used to live in this browser's localStorage under `flp-saved-views-v2:<service>`.
// They now live on the Compute origin (`/api/views`, one KV record per service) so every user
// sees the same list, as on the self-hosted backend. The first listing of a service moves any
// local views to the server, then forgets them.
const LEGACY_KEY = "flp-saved-views-v2:";

interface LegacyView {
  id?: unknown;
  service_id?: unknown;
  name?: unknown;
  filters_json?: unknown;
  time_range_type?: unknown;
  start_time?: unknown;
  end_time?: unknown;
  page?: unknown;
  created_at?: unknown;
}

const optional = (value: unknown) => (typeof value === "string" ? value : undefined);

const pending = new Map<string, Promise<void>>();

export function migrateLegacySavedViews(serviceId: string): Promise<void> {
  const inFlight = pending.get(serviceId);
  if (inFlight) return inFlight;
  const run = (async () => {
    if (typeof localStorage === "undefined") return;
    const key = LEGACY_KEY + encodeURIComponent(serviceId);
    const raw = localStorage.getItem(key);
    if (!raw) return;
    let parsed: unknown;
    try { parsed = JSON.parse(raw); } catch { parsed = []; }
    const views = Array.isArray(parsed) ? (parsed as LegacyView[]) : [];
    for (const view of views) {
      if (view?.service_id !== serviceId || typeof view.name !== "string" ||
          typeof view.filters_json !== "string") continue;
      const body = {
        id: optional(view.id),
        service_id: serviceId,
        name: view.name,
        filters_json: view.filters_json,
        time_range_type: optional(view.time_range_type),
        start_time: optional(view.start_time),
        end_time: optional(view.end_time),
        page: optional(view.page),
        created_at: optional(view.created_at),
      };
      const response = await fetch("/api/views/", {
        method: "POST",
        headers: { "content-type": "application/json" },
        credentials: "include",
        body: JSON.stringify(body),
      });
      // A rejected view stays local so a later session can retry.
      if (!response.ok && response.status !== 400 && response.status !== 404) return;
    }
    localStorage.removeItem(key);
  })().finally(() => pending.delete(serviceId));
  pending.set(serviceId, run);
  return run;
}
