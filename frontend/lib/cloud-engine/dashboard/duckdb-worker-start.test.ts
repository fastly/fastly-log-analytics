// @vitest-environment node
import { expect, it, vi } from "vitest";
import { createBrowserBackend, DUCKDB_WASM_VERSION } from "./duckdb";

const gate = vi.hoisted(() => {
  let release!: () => void;
  return { opened: new Promise<void>((resolve) => { release = resolve; }), release: () => release() };
});

vi.mock("@duckdb/duckdb-wasm", async () => {
  await gate.opened;
  return {
    PACKAGE_VERSION: DUCKDB_WASM_VERSION,
    VoidLogger: class {},
    AsyncDuckDB: class {
      async instantiate() {}
      async open() {}
      async connect() { return { query: async () => ({}) }; }
      async terminate() {}
    },
  };
});

it("starts the DuckDB worker while the module import is still loading", async () => {
  const workers: string[] = [];
  vi.stubGlobal("self", { location: new URL("https://flp.test/") });
  vi.stubGlobal("Worker", class extends EventTarget {
    constructor(url: string) { super(); workers.push(url); }
    terminate() {}
  });
  const backend = createBrowserBackend();
  await vi.waitFor(() => expect(workers).toEqual([
    `https://flp.test/duckdb/${DUCKDB_WASM_VERSION}/duckdb-browser-eh.worker.js`,
  ]));
  gate.release();
  await (await backend).dispose();
  vi.unstubAllGlobals();
});
