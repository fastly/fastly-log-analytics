// @vitest-environment node
import { afterEach, describe, expect, it, vi } from "vitest";
import { cachedDuckDBModule } from "./duckdb-module-cache";

afterEach(() => vi.unstubAllGlobals());

describe("cached DuckDB module", () => {
  it("falls back to the module URL, without downloading it, when Cache Storage fails", async () => {
    vi.stubGlobal("caches", { open: async () => { throw new DOMException("blocked", "SecurityError"); } });
    const fetches = vi.fn();
    vi.stubGlobal("fetch", fetches);
    const cached = await cachedDuckDBModule("https://app.test/duckdb/2/duckdb-eh.wasm", "2");
    expect(cached.url).toBe("https://app.test/duckdb/2/duckdb-eh.wasm");
    expect(fetches).not.toHaveBeenCalled();
  });

  it("keeps only the current version's module, and caches nothing from a failed download", async () => {
    const names = new Set(["flp-duckdb-module-1", "flp-parquet-v1"]);
    const puts: string[] = [];
    vi.stubGlobal("caches", {
      open: async (name: string) => {
        names.add(name);
        return { match: async () => undefined, put: async (url: string) => { puts.push(url); } };
      },
      keys: async () => [...names],
      delete: async (name: string) => names.delete(name),
    });
    vi.stubGlobal("fetch", async () => new Response("missing", { status: 404 }));
    await expect(cachedDuckDBModule("https://app.test/duckdb/2/duckdb-eh.wasm", "2")).rejects.toThrow("(404)");
    expect(puts).toEqual([]);
    vi.stubGlobal("fetch", async () => new Response(new Uint8Array([0, 97, 115, 109])));
    (await cachedDuckDBModule("https://app.test/duckdb/2/duckdb-eh.wasm", "2")).release();
    expect(puts).toEqual(["https://app.test/duckdb/2/duckdb-eh.wasm"]);
    expect([...names].sort()).toEqual(["flp-duckdb-module-2", "flp-parquet-v1"]);
  });
});
