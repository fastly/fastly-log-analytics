import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

const browserSource = vi.fn();
const runDashboard = vi.fn(async () => ({ ok: true }));

vi.mock("./sources", () => ({ browserSource }));
vi.mock("./engine", () => ({
  clearDashboardMemo: vi.fn(),
  runDashboard,
  searchFieldSummaries: vi.fn(),
  searchFieldValues: vi.fn(),
}));
vi.mock("./manifest", () => ({ clearManifestCache: vi.fn() }));
vi.mock("./duckdb", () => ({ getDuckDBEngine: vi.fn() }));

type Host = { onmessage?: (event: { data: unknown }) => void; postMessage: ReturnType<typeof vi.fn> };

describe("engine.worker cache scope", () => {
  let host: Host;
  const withOptions = vi.fn<(opts: unknown) => void>();

  beforeEach(() => {
    vi.resetModules();
    browserSource.mockReset();
    runDashboard.mockClear();
    withOptions.mockClear();
    browserSource.mockImplementation((_base: string, _opts: unknown, limits: { cacheScope?: string }) => {
      const source = {
        cacheScope: limits?.cacheScope,
        withOptions: (opts: unknown) => {
          withOptions(opts);
          return source;
        },
        clearMemoryCache: vi.fn(),
      };
      return source;
    });
    host = { postMessage: vi.fn() };
    vi.stubGlobal("self", Object.assign(host, { location: { origin: "https://flp.example" } }));
  });

  afterEach(() => vi.unstubAllGlobals());

  it("reads and writes browser caches under the scope the page passes and forwards serviceId via withOptions", async () => {
    await import("./engine.worker");
    const pageScope = JSON.stringify(["session-1", "user-1"]);
    host.onmessage!({ data: { id: 1, type: "run", options: { serviceId: "svc-1" }, cacheScope: pageScope } });
    await vi.waitFor(() => expect(runDashboard).toHaveBeenCalled());
    const scopes = browserSource.mock.calls.map((call) => (call[2] as { cacheScope?: string } | undefined)?.cacheScope);
    expect(scopes).toContain(pageScope);
    expect(withOptions).toHaveBeenCalledWith(expect.objectContaining({ serviceId: "svc-1" }));
    const [source] = runDashboard.mock.calls[0] as unknown as [{ cacheScope?: string }];
    expect(source.cacheScope).toBe(pageScope);

    host.onmessage!({ data: { id: 2, type: "run", options: { serviceId: "svc-2" }, cacheScope: pageScope } });
    await vi.waitFor(() => expect(runDashboard).toHaveBeenCalledTimes(2));
    expect(withOptions).toHaveBeenLastCalledWith(expect.objectContaining({ serviceId: "svc-2" }));
    const [again] = runDashboard.mock.calls[1] as unknown as [{ cacheScope?: string }];
    expect(again).toBe(source);
  });
});
