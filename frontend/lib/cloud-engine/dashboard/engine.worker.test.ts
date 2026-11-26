import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

const browserSource = vi.fn();
const runDashboard = vi.fn(async () => ({ ok: true }));
const clearDashboardMemo = vi.fn();
const clearManifestCache = vi.fn();
const invalidateManifestRecent = vi.fn();
const observeManifestCommit = vi.fn();
const clearRollupQueryCache = vi.fn();
const clearSecurityRollupCache = vi.fn();
const answerFromRollup = vi.fn(
  async (_path?: unknown, _params?: unknown, _source?: unknown, _signal?: AbortSignal): Promise<unknown> => ({
    ok: true,
  }),
);

vi.mock("./sources", () => ({ browserSource }));
vi.mock("./engine", () => ({
  clearDashboardMemo,
  runDashboard,
  searchFieldSummaries: vi.fn(),
  searchFieldValues: vi.fn(),
}));
vi.mock("./manifest", () => ({
  clearManifestCache,
  invalidateManifestRecent,
  observeManifestCommit,
}));
vi.mock("./rollup-query", () => ({ clearRollupQueryCache }));
vi.mock("../security-rollup", () => ({ clearSecurityRollupCache }));
vi.mock("../rollup-answer", () => ({ answerFromRollup }));
vi.mock("./duckdb", async (importOriginal) => ({
  ...(await importOriginal<typeof import("./duckdb")>()),
  getDuckDBEngine: vi.fn(() => ({ reset: vi.fn(async () => {}), prewarm: vi.fn(async () => {}) })),
}));

type Host = { onmessage?: (event: { data: unknown }) => void; postMessage: ReturnType<typeof vi.fn> };

describe("engine.worker cache scope", () => {
  let host: Host;
  const withOptions = vi.fn<(opts: unknown) => void>();
  const clearMemoryCache = vi.fn();

  beforeEach(() => {
    vi.resetModules();
    browserSource.mockReset();
    runDashboard.mockClear();
    clearDashboardMemo.mockClear();
    clearManifestCache.mockClear();
    invalidateManifestRecent.mockClear();
    observeManifestCommit.mockClear();
    clearRollupQueryCache.mockClear();
    clearSecurityRollupCache.mockClear();
    answerFromRollup.mockReset().mockResolvedValue({ ok: true });
    withOptions.mockClear();
    clearMemoryCache.mockClear();
    browserSource.mockImplementation((_base: string, _opts: unknown, limits: { cacheScope?: string }) => {
      const source = {
        cacheScope: limits?.cacheScope,
        fetchJson: vi.fn(),
        withOptions: (opts: unknown) => {
          withOptions(opts);
          return source;
        },
        clearMemoryCache,
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

  it("observes catalog commits without wiping rollup caches or aborting in-flight rollup queries", async () => {
    await import("./engine.worker");
    let resolveRollup!: (val: unknown) => void;
    let aborted = false;
    answerFromRollup.mockImplementationOnce(
      (_path, _params, _source, signal?: AbortSignal) =>
        new Promise((resolve) => {
          signal?.addEventListener("abort", () => {
            aborted = true;
          });
          resolveRollup = resolve;
        }),
    );

    const pageScope = JSON.stringify(["session-1", "user-1"]);
    host.onmessage!({
      data: {
        id: 1,
        type: "rollup",
        path: "/api/origin/aggregates",
        params: {},
        serviceId: "svc-1",
        cacheScope: pageScope,
      },
    });
    await vi.waitFor(() => expect(answerFromRollup).toHaveBeenCalledTimes(1));

    const commitEvent = { type: "commit", generation: 42, manifest: "processed/_catalog/current.json" };
    host.onmessage!({
      data: { id: 2, type: "catalogChanged", cacheScope: pageScope, serviceId: "svc-1", event: commitEvent },
    });
    await vi.waitFor(() => expect(observeManifestCommit).toHaveBeenCalledWith(expect.any(Function), commitEvent));

    expect(aborted).toBe(false);
    expect(clearManifestCache).not.toHaveBeenCalled();
    expect(clearRollupQueryCache).not.toHaveBeenCalled();
    expect(clearSecurityRollupCache).not.toHaveBeenCalled();
    expect(clearDashboardMemo).not.toHaveBeenCalled();
    expect(clearMemoryCache).not.toHaveBeenCalled();

    host.onmessage!({
      data: {
        id: 3,
        type: "catalogChanged",
        cacheScope: pageScope,
        serviceId: "svc-1",
        event: { type: "resync", generation: 50 },
      },
    });
    await vi.waitFor(() => expect(clearDashboardMemo).toHaveBeenCalledTimes(1));
    expect(aborted).toBe(false);
    expect(clearRollupQueryCache).not.toHaveBeenCalled();
    expect(clearMemoryCache).not.toHaveBeenCalled();

    host.onmessage!({ data: { id: 4, type: "catalogChanged" } });
    await vi.waitFor(() => expect(invalidateManifestRecent).toHaveBeenCalledTimes(1));
    expect(aborted).toBe(false);
    expect(clearRollupQueryCache).not.toHaveBeenCalled();

    resolveRollup({ served: "worker" });
    await vi.waitFor(() =>
      expect(host.postMessage).toHaveBeenCalledWith({
        id: 1,
        type: "rollupResult",
        result: { served: "worker" },
      }),
    );

    host.onmessage!({ data: { id: 5, type: "clearCache" } });
    await vi.waitFor(() => expect(clearRollupQueryCache).toHaveBeenCalledTimes(1));
    expect(clearManifestCache).toHaveBeenCalledTimes(1);
    expect(clearSecurityRollupCache).toHaveBeenCalledTimes(1);
    expect(clearMemoryCache).toHaveBeenCalled();
  });
});
