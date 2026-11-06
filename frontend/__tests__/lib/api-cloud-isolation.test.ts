import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { useSessionRoleStore } from "@/stores/filterStore";
import { setCloudModeRuntime } from "@/lib/cloud-engine/mode";

const adapter = vi.hoisted(() => ({ evaluated: vi.fn(), handle: vi.fn() }));
vi.mock("@/lib/cloud-engine/adapter", () => {
  adapter.evaluated();
  return { handleCloudAnalyticalRequest: adapter.handle };
});

import { adminFetch, adminJsonResult, client } from "@/lib/api";

const replace = vi.fn();
const location = window.location;

beforeEach(() => {
  setCloudModeRuntime(false);
  useSessionRoleStore.setState({ isRemoteAnalyst: false });
  adapter.handle.mockResolvedValue(null);
  replace.mockReset();
  Object.defineProperty(window, "location", {
    configurable: true,
    value: { origin: "https://analytics.example.test:8443", pathname: "/dashboard", search: "?service=test", replace },
  });
});

afterEach(() => {
  setCloudModeRuntime(false);
  vi.unstubAllGlobals();
  Object.defineProperty(window, "location", { configurable: true, value: location });
});

describe("API Cloud isolation", () => {
  it("does not evaluate the Cloud adapter on import or Standard requests", async () => {
    const fetch = vi.fn(async () => Response.json({ status: "ok" }));
    vi.stubGlobal("fetch", fetch);
    await client.GET("/api/health");
    await adminFetch("/api/health");
    expect(fetch).toHaveBeenCalledTimes(2);
    expect(adapter.evaluated).not.toHaveBeenCalled();
    expect(adapter.handle).not.toHaveBeenCalled();
  });

  it("loads the adapter only when Cloud requests need it", async () => {
    setCloudModeRuntime(true);
    adapter.handle.mockResolvedValue(Response.json({ status: "cloud" }));
    const fetch = vi.fn();
    vi.stubGlobal("fetch", fetch);
    const result = await client.GET("/api/health");
    expect(result.data).toMatchObject({ status: "cloud" });
    expect(adapter.evaluated).toHaveBeenCalledOnce();
    expect(fetch).not.toHaveBeenCalled();
  });

  it.each([
    ["/api/services?view=all", "https://analytics.example.test:8443/api/services?view=all"],
    ["api/services", "https://analytics.example.test:8443/api/services"],
    [new URL("https://upstream.example.test/api/services"), "https://upstream.example.test/api/services"],
    [new Request("https://upstream.example.test/api/services"), "https://upstream.example.test/api/services"],
  ])("preserves the real origin through native Cloud passthrough for %s", async (input, expectedUrl) => {
    setCloudModeRuntime(true);
    const fetch = vi.fn(async () => Response.json({ services: [] }));
    vi.stubGlobal("fetch", fetch);
    adapter.handle.mockImplementation((request: Request) => globalThis.fetch(request));
    expect((await adminFetch(input)).ok).toBe(true);
    expect(fetch).toHaveBeenCalledOnce();
    const [request] = vi.mocked(globalThis.fetch).mock.calls[0];
    expect(request).toBeInstanceOf(Request);
    expect((request as Request).url).toBe(expectedUrl);
  });

  it("uses the localhost fallback only outside a browser", async () => {
    setCloudModeRuntime(true);
    vi.stubGlobal("window", undefined);
    const fetch = vi.fn(async () => Response.json({ services: [] }));
    vi.stubGlobal("fetch", fetch);
    adapter.handle.mockImplementation((request: Request) => globalThis.fetch(request));
    await adminFetch("/api/services");
    const [request] = vi.mocked(globalThis.fetch).mock.calls[0];
    expect((request as Request).url).toBe("http://localhost/api/services");
  });
});

describe("Cloud subresource 401 session verification", () => {
  beforeEach(() => {
    setCloudModeRuntime(true);
  });

  it.each(["client", "adminFetch", "adminJsonResult"] as const)(
    "keeps a valid session on %s and preserves the resource failure",
    async (transport) => {
      const fetch = vi.fn(async (input: RequestInfo | URL) =>
        String(input instanceof Request ? input.url : input).endsWith("/api/auth/session")
          ? Response.json({ user: { id: "valid-user" } })
          : Response.json({ error: "cdn_access_denied" }, { status: 401 }),
      );
      vi.stubGlobal("fetch", fetch);
      if (transport === "client") {
        await expect(client.GET("/api/health")).rejects.toMatchObject({ status: 401, message: "cdn_access_denied" });
      } else if (transport === "adminFetch") {
        expect((await adminFetch("/api/health")).status).toBe(401);
      } else {
        expect(await adminJsonResult("/api/health", "/api/health")).toMatchObject({ ok: false, status: 401 });
      }
      expect(fetch).toHaveBeenCalledWith(
        "http://127.0.0.1:8000/api/auth/session",
        { credentials: "include", cache: "no-store" },
      );
      expect(replace).not.toHaveBeenCalled();
    },
  );

  it.each([401, 403, 200])("redirects only after the session is confirmed expired (%s)", async (status) => {
    vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL) =>
      String(input instanceof Request ? input.url : input).endsWith("/api/auth/session")
        ? Response.json({ user: null }, { status })
        : Response.json({ error: "unauthenticated" }, { status: 401 }),
    ));
    await expect(client.GET("/api/health")).rejects.toMatchObject({ status: 401 });
    expect(replace).toHaveBeenCalledWith("/share-login?return=%2Fdashboard%3Fservice%3Dtest");
  });

  it.each(["network", "server", "malformed"] as const)("does not log out on an inconclusive %s probe", async (failure) => {
    vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL) => {
      if (!String(input instanceof Request ? input.url : input).endsWith("/api/auth/session")) {
        return Response.json({ error: "cdn_access_denied" }, { status: 401 });
      }
      if (failure === "network") throw new TypeError("Network unavailable");
      if (failure === "malformed") return new Response("not JSON");
      return Response.json({ error: "busy" }, { status: 503 });
    }));
    await expect(client.GET("/api/health")).rejects.toMatchObject({ status: 401 });
    expect(replace).not.toHaveBeenCalled();
  });

  it("shares an in-flight session probe across concurrent resource failures", async () => {
    let finishProbe!: (response: Response) => void;
    const probe = new Promise<Response>((resolve) => { finishProbe = resolve; });
    const fetch = vi.fn(async (input: RequestInfo | URL) =>
      String(input instanceof Request ? input.url : input).endsWith("/api/auth/session")
        ? probe
        : Response.json({ error: "cdn_access_denied" }, { status: 401 }),
    );
    vi.stubGlobal("fetch", fetch);
    const requests = Promise.all([adminFetch("/api/health"), adminFetch("/api/health")]);
    await vi.waitFor(() => expect(fetch.mock.calls.filter(
      ([input]) => String(input).endsWith("/api/auth/session"),
    )).toHaveLength(1));
    finishProbe(Response.json({ user: { id: "valid-user" } }));
    expect((await requests).map((response) => response.status)).toEqual([401, 401]);
    expect(fetch).toHaveBeenCalledTimes(3);
    expect(replace).not.toHaveBeenCalled();
  });

  it("does not probe or redirect again while already on the login page", async () => {
    Object.defineProperty(window, "location", {
      configurable: true,
      value: { pathname: "/share-login", search: "", replace },
    });
    const fetch = vi.fn(async () => Response.json({ error: "unauthenticated" }, { status: 401 }));
    vi.stubGlobal("fetch", fetch);
    await expect(client.GET("/api/health")).rejects.toMatchObject({ status: 401 });
    expect(fetch).toHaveBeenCalledOnce();
    expect(replace).not.toHaveBeenCalled();
  });

  it("retains the Standard shared-session redirect without a Cloud auth probe", async () => {
    setCloudModeRuntime(false);
    useSessionRoleStore.setState({ isRemoteAnalyst: true });
    const fetch = vi.fn(async () => Response.json({ error: "unauthenticated" }, { status: 401 }));
    vi.stubGlobal("fetch", fetch);
    await expect(client.GET("/api/health")).rejects.toMatchObject({ status: 401 });
    expect(fetch).toHaveBeenCalledOnce();
    expect(replace).toHaveBeenCalledOnce();
  });
});
