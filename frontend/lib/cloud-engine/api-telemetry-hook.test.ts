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
