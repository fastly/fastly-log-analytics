// @vitest-environment node
import { existsSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { describe, expect, it, vi } from "vitest";
import {
  DUCKDB_WORKER_PATCH_MARKER,
  patchDuckDBBrowserWorker,
} from "../../../scripts/duckdb-worker-patch.mjs";

const REAL_WORKER_PATH = join(
  process.cwd(),
  "node_modules",
  "@duckdb",
  "duckdb-wasm",
  "dist",
  "duckdb-browser-eh.worker.js",
);

describe("patchDuckDBBrowserWorker", () => {
  it("patches the vendored duckdb-browser-eh worker idempotently and rejects unexpected inputs", () => {
    expect(existsSync(REAL_WORKER_PATH)).toBe(true);
    const raw = readFileSync(REAL_WORKER_PATH, "utf8");
    const patched = patchDuckDBBrowserWorker(raw);
    expect(patched).toContain(DUCKDB_WORKER_PATCH_MARKER);
    expect(patchDuckDBBrowserWorker(patched)).toBe(patched);
    expect(() => patchDuckDBBrowserWorker("function unrelated() {}")).toThrow(
      /instantiateStreaming/,
    );
  });

  it("retries non-streaming fetch with cache no-store when instantiateStreaming aborts, and surfaces double failures", async () => {
    const raw = readFileSync(REAL_WORKER_PATH, "utf8");
    const patched = patchDuckDBBrowserWorker(raw);
    const start = patched.indexOf("instantiateWasm(e,t){");
    const end = patched.indexOf("else if(typeof XMLHttpRequest", start);
    expect(start).toBeGreaterThan(-1);
    expect(end).toBeGreaterThan(start);

    // Isolate the patched instantiateStreaming body as a callable function on a fake worker binding.
    const inner = patched.slice(
      patched.indexOf("if(WebAssembly.instantiateStreaming)", start),
      end,
    );
    const invoke = new Function(
      "e",
      "t",
      "WebAssembly",
      "fetch",
      "TransformStream",
      "Request",
      "Response",
      inner,
    ) as (
      e: Record<string, unknown>,
      t: (instance: unknown, module: unknown) => void,
      wasm: unknown,
      fetchFn: unknown,
      tsCtor: unknown,
      reqCtor: unknown,
      resCtor: unknown,
    ) => void;

    const fakeInstance = { id: "inst" };
    const fakeModule = { id: "mod" };
    const fetchCalls: Array<[unknown, RequestInit | undefined]> = [];
    const fetchMock = vi.fn(async (input: unknown, init?: RequestInit) => {
      fetchCalls.push([input, init]);
      return {
        ok: true,
        status: 200,
        headers: new Headers({ "content-length": "4" }),
        body: { pipeThrough: () => "piped" },
        arrayBuffer: async () => new Uint8Array([0, 97, 115, 109]).buffer,
      };
    });
    const instantiateStreamingMock = vi.fn(async () => {
      throw new TypeError(
        "WebAssembly compilation aborted: Network error: Failed to read from a ReadableStream.",
      );
    });
    const instantiateMock = vi.fn(async () => ({
      instance: fakeInstance,
      module: fakeModule,
    }));
    const onResolved = vi.fn();
    const ctx = {
      mainModuleURL: "https://flp.test/duckdb/1.33.1-dev57.0/duckdb-eh.wasm",
      onInstantiationProgress: [],
    };
    const fakeWasm = {
      instantiateStreaming: instantiateStreamingMock,
      instantiate: instantiateMock,
    };

    invoke.call(
      ctx,
      { env: {} },
      onResolved,
      fakeWasm,
      fetchMock,
      class {
        constructor() {}
      },
      class {
        constructor(public url: string) {}
      },
      class {
        constructor(public body: unknown) {}
      },
    );

    await vi.waitFor(() => expect(onResolved).toHaveBeenCalledTimes(1));
    expect(onResolved).toHaveBeenCalledWith(fakeInstance, fakeModule);
    expect(fetchCalls).toHaveLength(2);
    expect(fetchCalls[1]).toEqual([
      "https://flp.test/duckdb/1.33.1-dev57.0/duckdb-eh.wasm",
      { credentials: "same-origin", cache: "no-store" },
    ]);

    // Double failure: fallback fetch returns 503 -> error is scheduled to throw rather than hanging quietly.
    const uncaught: unknown[] = [];
    const timerErrors = vi.spyOn(globalThis, "setTimeout").mockImplementation(((
      fn: (...args: unknown[]) => void,
    ) => {
      try {
        fn();
      } catch (err) {
        uncaught.push(err);
      }
      return 0 as unknown as ReturnType<typeof setTimeout>;
    }) as typeof setTimeout);
    try {
      const fetch503 = vi.fn(async () => ({
        ok: false,
        status: 503,
        headers: new Headers(),
        body: null,
        arrayBuffer: async () => new ArrayBuffer(0),
      }));
      invoke.call(
        ctx,
        { env: {} },
        vi.fn(),
        fakeWasm,
        fetch503,
        undefined,
        class {
          constructor(public url: string) {}
        },
        class {
          constructor(public body: unknown) {}
        },
      );
      await vi.waitFor(() => expect(uncaught).toHaveLength(1));
      expect(String(uncaught[0])).toContain("DuckDB Wasm HTTP 503");
    } finally {
      timerErrors.mockRestore();
    }
  });
});
