import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { client } from "@/lib/api";
import { useDebugStore, useServiceStore } from "@/stores/filterStore";
import { setCloudModeRuntime } from "./mode";

beforeEach(() => {
  setCloudModeRuntime(true);
});

afterEach(() => {
  setCloudModeRuntime(false);
  vi.unstubAllGlobals();
  useDebugStore.setState({ apiCallsEnabled: true, apiCalls: [] });
  useServiceStore.setState({ activeServiceId: "svc-1" });
});

describe("API telemetry hook", () => {
  it("publishes the actual intercepted API status, including failures, without response bodies", async () => {
    const events: unknown[] = [];
    const listener = (event: Event) => events.push((event as CustomEvent).detail);
    window.addEventListener("fla:cloud-query", listener);
    try {
      vi.stubGlobal("fetch", vi.fn(async () => Response.json(
        { secret: "sentinel-private-response" }, { status: 503 },
      )));
      const { handleCloudAnalyticalRequest } = await import("./adapter");
      const response = await handleCloudAnalyticalRequest(new Request("http://localhost/api/services", {
        headers: { "x-service-id": "origin-service" },
      }));
      expect(response?.status).toBe(503);
      expect(events).toMatchObject([
        { section: "api./api/services", serviceId: "origin-service", state: "pending" },
        { section: "api./api/services", serviceId: "origin-service", state: "error", status: 503 },
      ]);
      expect(JSON.stringify(events)).not.toContain("sentinel");
    } finally {
      window.removeEventListener("fla:cloud-query", listener);
    }
  });

  it("binds real worker-result telemetry to its originating service after a selection change", async () => {
    const events: unknown[] = [];
    const listener = (event: Event) => events.push((event as CustomEvent).detail);
    window.addEventListener("fla:cloud-worker", listener);
    try {
      vi.stubGlobal("fetch", vi.fn(async () => Response.json({ ok: true })));
      useServiceStore.setState({ activeServiceId: "service-b" });
      const { postCloudQueryTelemetry } = await import("./adapter");
      await postCloudQueryTelemetry({ files_fetched: 3, files_cached: 1 }, "service-a",
        { route: "/dashboard", search: "?range=1h" });
      expect(events).toMatchObject([{
        route: "/dashboard", search: "?range=1h",
        serviceId: "service-a", filesFetched: 3, filesCached: 1,
      }]);
    } finally {
      window.removeEventListener("fla:cloud-worker", listener);
    }
  });

  it("records vcpu_ms from the x-flp-vcpu-ms response header on openapi-fetch calls", async () => {
    useServiceStore.setState({ activeServiceId: "svc-1" });
    useDebugStore.setState({ apiCallsEnabled: true, apiCalls: [] });
    vi.stubGlobal(
      "fetch",
      vi.fn(async () =>
        new Response(JSON.stringify({ status: "ok" }), {
          status: 200,
          headers: {
            "content-type": "application/json",
            "x-flp-vcpu-ms": "17",
          },
        }),
      ),
    );
    // Use a real Compute passthrough route. /api/health is Standard-only.
    await client.GET("/api/services");
    const calls = useDebugStore.getState().apiCalls;
    expect(calls).toHaveLength(1);
    expect(calls[0]).toMatchObject({
      source: "Compute",
      method: "GET",
      path: "/api/services",
      status: 200,
      vcpu_ms: 17,
    });
  });

  it("leaves vcpu_ms null when the header is absent", async () => {
    useServiceStore.setState({ activeServiceId: "svc-1" });
    useDebugStore.setState({ apiCallsEnabled: true, apiCalls: [] });
    vi.stubGlobal(
      "fetch",
      vi.fn(async () =>
        new Response(JSON.stringify({ status: "ok" }), {
          status: 200,
          headers: { "content-type": "application/json" },
        }),
      ),
    );
    await client.GET("/api/services");
    expect(useDebugStore.getState().apiCalls[0]?.vcpu_ms).toBeNull();
  });
});
