import { afterEach, expect, it, vi } from "vitest";
import { measureCloudQuery, publishCloudWorkerTelemetry } from "./readiness";
import { setCloudModeRuntime } from "./mode";

afterEach(() => setCloudModeRuntime(false));

it("captures the originating service through a late query completion without requiring window", async () => {
  setCloudModeRuntime(true);
  const events: unknown[] = [];
  const listener = (event: Event) => events.push((event as CustomEvent).detail);
  const scope = window;
  scope.addEventListener("fla:cloud-query", listener);
  try {
    vi.stubGlobal("window", undefined);
    let finish!: (data: { total_rows: number }) => void;
    const result = measureCloudQuery("dashboard.bundle",
      () => new Promise(resolve => { finish = resolve; }), undefined, "service-a");
    expect(events).toMatchObject([{ serviceId: "service-a", state: "pending" }]);
    finish({ total_rows: 0 });
    await result;
    expect(events).toMatchObject([
      { serviceId: "service-a", state: "pending" },
      { serviceId: "service-a", state: "success", totalRows: 0 },
    ]);
  } finally {
    vi.unstubAllGlobals();
    scope.removeEventListener("fla:cloud-query", listener);
  }
});

it("reports real completion only after a pending query resolves, preserving zero rows", async () => {
  setCloudModeRuntime(true);
  const events: unknown[] = [];
  const listener = (event: Event) => events.push((event as CustomEvent).detail);
  window.addEventListener("fla:cloud-query", listener);
  try {
    let finish!: (data: { aggregates: { total_rows: number } }) => void;
    const result = measureCloudQuery("dashboard.bundle", () => new Promise(resolve => { finish = resolve; }));
    expect(events).toMatchObject([{ state: "pending", section: "dashboard.bundle" }]);
    finish({ aggregates: { total_rows: 0 } });
    expect(await result).toEqual({ aggregates: { total_rows: 0 } });
    expect(events).toMatchObject([
      { state: "pending" }, { state: "success", totalRows: 0 },
    ]);
  } finally {
    window.removeEventListener("fla:cloud-query", listener);
  }
});

it("does not turn stale-view exceptions or canceled queries into successful readiness", async () => {
  setCloudModeRuntime(true);
  const events: unknown[] = [];
  const listener = (event: Event) => events.push((event as CustomEvent).detail);
  window.addEventListener("fla:cloud-query", listener);
  try {
    const error = new Error("sentinel-private-query-token");
    await expect(measureCloudQuery("dashboard.bundle", async () => { throw error; })).rejects.toBe(error);
    expect(events).toMatchObject([{ state: "pending" }, { state: "error", totalRows: null }]);
    expect(JSON.stringify(events)).not.toContain(error.message);
  } finally {
    window.removeEventListener("fla:cloud-query", listener);
  }
});

it("reports failed local API responses as errors without exposing response bodies", async () => {
  setCloudModeRuntime(true);
  const events: unknown[] = [];
  const listener = (event: Event) => events.push((event as CustomEvent).detail);
  window.addEventListener("fla:cloud-query", listener);
  try {
    const response = Response.json({ secret: "sentinel-private-response" }, { status: 503 });
    expect(await measureCloudQuery("api./api/origin/aggregates", async () => response)).toBe(response);
    expect(events).toMatchObject([{ state: "pending" }, { state: "error", status: 503 }]);
    expect(JSON.stringify(events)).not.toContain("sentinel");
  } finally {
    window.removeEventListener("fla:cloud-query", listener);
  }
});

it("keeps High-Scale silent and allowlists worker statistics without SQL or tokens", async () => {
  const events: unknown[] = [];
  const listener = (event: Event) => events.push((event as CustomEvent).detail);
  window.addEventListener("fla:cloud-worker", listener);
  try {
    await measureCloudQuery("dashboard.bundle", async () => ({ aggregates: { total_rows: 42 } }));
    publishCloudWorkerTelemetry({ files_fetched: 3, sql: "sentinel-private-sql" });
    expect(events).toEqual([]);
    setCloudModeRuntime(true);
    publishCloudWorkerTelemetry({ files_fetched: 3, files_cached: 0, sql: "sentinel-private-sql" });
    expect(events).toMatchObject([{ filesFetched: 3, filesCached: 0 }]);
    expect(JSON.stringify(events)).not.toContain("sentinel");
  } finally {
    window.removeEventListener("fla:cloud-worker", listener);
  }
});
