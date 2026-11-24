// @vitest-environment node
import { afterEach, expect, it, vi } from "vitest";
import { SNAPSHOT_REUSE_MS } from "./dashboard/catalog-manifest";

const END = "2026-11-06T00:00:00.000Z";
const SERVICE = "svc";

type Call = { path: string; params: URLSearchParams };

/** Startup requests each page issues for its default 24h view. */
const PAGES: Record<string, Array<[string, string, Record<string, unknown>?]>> = {
  "/dashboard": [
    ["POST", "/api/dashboard/bundle", {
      chart_metric: "requests", chart_interval: "5 minutes", sections: ["core", "topten", "bots"],
    }],
  ],
  "/insights": [
    ["POST", "/api/insights", { window_size_hrs: 24, baseline_hours: 168, filters: {}, force_refresh: false }],
    ["GET", "/api/insight-availability"],
  ],
  "/network": [
    ["POST", "/api/network/bundle", {}],
    ["POST", "/api/network-health", {}],
    ["POST", "/api/network-quality", {}],
  ],
  "/security": [
    ["POST", "/api/security/aggregates", {}],
    ["POST", "/api/security/threat-intel", {}],
  ],
  "/origin": [["POST", "/api/origin/aggregates", {}]],
  "/performance": [["POST", "/api/performance/aggregates", {}]],
  "/assets-shield": [["POST", "/api/assets/aggregates", { sections: ["summary"] }]],
  "/fastly-value": [["POST", "/api/value/summary", { sections: ["overview", "network"] }]],
};

function fakeNetwork(calls: Call[]) {
  return vi.fn(async (input: RequestInfo | URL) => {
    const url = new URL(String(input instanceof Request ? input.url : input), "http://localhost");
    calls.push({ path: url.pathname, params: url.searchParams });
    if (url.pathname === "/api/log-extents")
      return Response.json({ latest_log_at: END, earliest_log_at: "2026-10-01T00:00:00Z" });
    if (url.pathname === "/api/services")
      return Response.json({ services: [{ service_id: SERVICE, log_fields: { custom_fields: [] } }] });
    if (url.pathname === "/api/log-fields/catalog") return Response.json({ fields: [], groups: [] });
    if (url.pathname === "/api/files") {
      // The production trace: one raw page, three summary pages for 24h.
      const page = Number(url.searchParams.get("cursor") ?? 0);
      const summary = url.searchParams.get("view") === "summary";
      const last = summary ? page >= 2 : true;
      return Response.json({
        files: [], ...url.searchParams.get("summaries") === "1" ? { summary_files: [] } : {},
        complete: last, next_cursor: last ? null : String(page + 1), catalog_generation: 100,
        ...url.searchParams.has("since_generation")
          ? { base_generation: Number(url.searchParams.get("since_generation")), changes: [], summary_partitions: [] }
          : {},
      });
    }
    return Response.json({});
  });
}

async function loadPage(page: string, handle: (request: Request) => Promise<Response | null>) {
  await Promise.all(PAGES[page].map(async ([method, path, body]) => {
    const response = await handle(new Request(`http://localhost${path}`, {
      method,
      headers: { "content-type": "application/json", "x-service-id": SERVICE },
      ...method === "POST" ? { body: JSON.stringify(body) } : {},
    }));
    await response?.text();
  }));
}

function fakeCacheStorage() {
  const stores = new Map<string, Map<string, Response>>();
  const open = async (name: string) => {
    const entries = stores.get(name) ?? new Map<string, Response>();
    stores.set(name, entries);
    return {
      match: async (url: string) => entries.get(String(url))?.clone(),
      put: async (url: string, value: Response) => void entries.set(String(url), value.clone()),
      delete: async (url: string) => entries.delete(String(url)),
      keys: async () => [...entries.keys()].map((url) => new Request(new URL(url, "http://localhost"))),
    } as unknown as Cache;
  };
  return { open, delete: async (name: string) => stores.delete(name), has: async (name: string) => stores.has(name), keys: async () => [...stores.keys()] };
}

/**
 * `/api/files` requests per page for a 24h view with empty data: cold, after
 * an in-app revisit (in-memory snapshots), and after a reload (persisted
 * snapshots). Rollups fall back to raw scans on empty data, so these bound
 * the listings from below. Every listing is one request: no summary-view chain.
 */
const EXPECTED: Record<string, [cold: number, revisit: number, reload: number]> = {
  "/dashboard": [1, 1, 1],
  // Availability reads the log field settings, so it lists no files.
  "/insights": [3, 3, 3],
  "/network": [1, 1, 1],
  "/security": [1, 1, 1],
  "/origin": [1, 1, 1],
  "/performance": [1, 1, 1],
  "/assets-shield": [1, 1, 1],
  "/fastly-value": [1, 1, 1],
};

afterEach(() => {
  vi.useRealTimers();
  vi.unstubAllGlobals();
});

it.each(Object.keys(PAGES))("lists %s in one request per range", async (page) => {
  vi.useFakeTimers({ toFake: ["Date"] });
  vi.setSystemTime(new Date("2026-11-06T00:05:00.000Z"));
  const calls: Call[] = [];
  vi.stubGlobal("fetch", fakeNetwork(calls));
  vi.stubGlobal("caches", fakeCacheStorage());
  const stored = new Map<string, string>();
  vi.stubGlobal("localStorage", {
    getItem: (key: string) => stored.get(key) ?? null,
    setItem: (key: string, value: string) => void stored.set(key, value),
    removeItem: (key: string) => void stored.delete(key),
  });
  const listings = () => calls.filter((call) => call.path === "/api/files");
  const load = async () => {
    const { handleCloudAnalyticalRequest } = await import("./adapter");
    const before = listings().length;
    await loadPage(page, handleCloudAnalyticalRequest);
    return listings().slice(before);
  };
  vi.resetModules();
  const cold = await load();
  // Within the snapshot reuse window, an in-app revisit reuses the cached manifest with zero /api/files calls.
  const reused = await load();
  expect(reused).toHaveLength(0);
  // Past the snapshot reuse window, a revisit lists again using since_generation.
  vi.setSystemTime(Date.now() + SNAPSHOT_REUSE_MS + 1);
  const revisit = await load();
  vi.resetModules();
  const reload = await load();
  expect([cold.length, revisit.length, reload.length]).toEqual(EXPECTED[page]);
  for (const call of listings()) expect(call.params.get("view")).not.toBe("summary");
  for (const call of [...revisit, ...reload]) expect(call.params.get("since_generation")).toBe("100");
}, 60_000);
