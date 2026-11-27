// @vitest-environment node
import { createHash } from "node:crypto";
import { mkdtempSync, mkdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import {
  DUCKDB_HARDENED_WORKER_FILE,
  DUCKDB_LEGACY_WORKER_SHIM,
  installDuckdbWorkerHardening,
  renderHardenedDuckdbWorker,
  syncDuckdbAssets,
} from "../../../scripts/copy-duckdb-assets.mjs";

type Listener = (event: { reason?: unknown; preventDefault?: () => void }) => void;

function makeWorkerScope(fetchImpl: typeof fetch) {
  const listeners = new Map<string, Listener[]>();
  const scope = {
    location: { origin: "https://flp.test", href: "https://flp.test/duckdb/1.33.1-dev57.0/duckdb-browser-eh.v2.worker.js" },
    fetch: fetchImpl,
    addEventListener(type: string, listener: Listener) {
      const existing = listeners.get(type) ?? [];
      existing.push(listener);
      listeners.set(type, existing);
    },
    dispatchUnhandledRejection(reason: unknown) {
      let prevented = false;
      const event = { reason, preventDefault: () => { prevented = true; } };
      for (const listener of listeners.get("unhandledrejection") ?? []) {
        listener(event);
      }
      return prevented;
    },
  };
  return { scope, listeners };
}

const tempDirs: string[] = [];

afterEach(() => {
  vi.useRealTimers();
  while (tempDirs.length > 0) {
    const dir = tempDirs.pop();
    if (dir) rmSync(dir, { recursive: true, force: true });
  }
});

describe("DuckDB worker preamble hardening", () => {
  it("buffers same-origin GET .wasm responses and installs idempotently", async () => {
    const wasmBytes = new Uint8Array([0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00]);
    const calls: Array<{ input: string; init?: RequestInit }> = [];
    const origFetch = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      calls.push({ input: input instanceof Request ? input.url : String(input), init });
      return new Response(wasmBytes, {
        status: 200,
        statusText: "OK",
        headers: { "content-type": "text/plain" },
      });
    }) as unknown as typeof fetch;

    const { scope, listeners } = makeWorkerScope(origFetch);
    installDuckdbWorkerHardening(scope);
    const wrappedFetch = scope.fetch;
    installDuckdbWorkerHardening(scope);
    expect(scope.fetch).toBe(wrappedFetch);
    expect(listeners.get("unhandledrejection")).toHaveLength(1);

    const res1 = await scope.fetch(new Request("https://flp.test/duckdb/1.33.1-dev57.0/duckdb-eh.wasm"));
    expect(res1.status).toBe(200);
    expect(res1.headers.get("content-type")).toBe("application/wasm");
    expect(res1.headers.get("content-length")).toBe(String(wasmBytes.byteLength));
    expect(new Uint8Array(await res1.arrayBuffer())).toEqual(wasmBytes);

    const res2 = await scope.fetch("/duckdb/1.33.1-dev57.0/duckdb-eh.wasm");
    expect(res2.status).toBe(200);
    expect(new Uint8Array(await res2.arrayBuffer())).toEqual(wasmBytes);
    expect(calls).toHaveLength(2);
  });

  it("retries same-origin .wasm once with cache no-store on mid-stream ReadableStream failure, fetch throw, or 5xx", async () => {
    const wasmBytes = new Uint8Array([0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00]);

    for (const mode of ["stream-error", "fetch-throw", "http-503"] as const) {
      const seen: Array<{ input: string; init?: RequestInit }> = [];
      let attempt = 0;
      const origFetch = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
        attempt += 1;
        seen.push({ input: input instanceof Request ? input.url : String(input), init });
        if (attempt === 1) {
          if (mode === "fetch-throw") {
            throw new TypeError("Failed to fetch");
          }
          if (mode === "http-503") {
            return new Response("upstream unavailable", { status: 503 });
          }
          const brokenStream = new ReadableStream<Uint8Array>({
            pull(controller) {
              controller.error(new TypeError("Failed to read from a ReadableStream."));
            },
          });
          return new Response(brokenStream, {
            status: 200,
            headers: { "content-type": "application/wasm" },
          });
        }
        return new Response(wasmBytes, {
          status: 200,
          headers: { "content-type": "application/wasm" },
        });
      }) as unknown as typeof fetch;

      const { scope } = makeWorkerScope(origFetch);
      installDuckdbWorkerHardening(scope);

      const response = await scope.fetch("https://flp.test/duckdb/1.33.1-dev57.0/duckdb-eh.wasm");
      expect(response.status).toBe(200);
      expect(response.headers.get("content-type")).toBe("application/wasm");
      expect(new Uint8Array(await response.arrayBuffer())).toEqual(wasmBytes);
      expect(seen).toHaveLength(2);
      expect(seen[1]).toEqual({
        input: "https://flp.test/duckdb/1.33.1-dev57.0/duckdb-eh.wasm",
        init: { method: "GET", cache: "no-store", credentials: "same-origin" },
      });
    }
  });

  it("does not retry 404 .wasm responses and passes through non-.wasm, POST, and cross-origin fetches", async () => {
    const origFetch = vi.fn(async (input: RequestInfo | URL) => {
      const url = input instanceof Request ? input.url : String(input);
      if (url.endsWith("/missing.wasm")) {
        return new Response("not found", { status: 404 });
      }
      return new Response("ok", { status: 200 });
    }) as unknown as typeof fetch;

    const { scope } = makeWorkerScope(origFetch);
    installDuckdbWorkerHardening(scope);

    const notFound = await scope.fetch("https://flp.test/duckdb/1.33.1-dev57.0/missing.wasm");
    expect(notFound.status).toBe(404);
    expect(origFetch).toHaveBeenCalledTimes(1);

    const rangeInit: RequestInit = { headers: { Range: "bytes=0-15" } };
    await scope.fetch("https://flp.test/fos/svc/a.parquet", rangeInit);
    await scope.fetch("https://flp.test/api/files", { method: "POST", body: "{}" });
    await scope.fetch("https://other.example/x.wasm");
    expect(origFetch).toHaveBeenCalledTimes(4);
    expect(origFetch).toHaveBeenNthCalledWith(2, "https://flp.test/fos/svc/a.parquet", rangeInit);
    expect(origFetch).toHaveBeenNthCalledWith(3, "https://flp.test/api/files", { method: "POST", body: "{}" });
    expect(origFetch).toHaveBeenNthCalledWith(4, "https://other.example/x.wasm", undefined);
  });

  it("converts worker unhandledrejection events into async worker errors", () => {
    vi.useFakeTimers();
    const { scope } = makeWorkerScope(vi.fn() as unknown as typeof fetch);
    installDuckdbWorkerHardening(scope);

    const boom = new TypeError("WebAssembly compilation aborted");
    expect(scope.dispatchUnhandledRejection(boom)).toBe(true);
    expect(() => vi.runAllTimers()).toThrow(boom);

    expect(scope.dispatchUnhandledRejection("string failure")).toBe(true);
    expect(() => vi.runAllTimers()).toThrowError("string failure");
  });

  it("syncDuckdbAssets writes v2 hardened worker, legacy shim, extensions, and skips unchanged runs", () => {
    const root = mkdtempSync(join(tmpdir(), "duckdb-sync-"));
    tempDirs.push(root);
    const srcDir = join(root, "src");
    const vendorDir = join(root, "vendor");
    const publicDir = join(root, "public/duckdb");
    const version = "1.33.1-dev57.0";
    mkdirSync(srcDir, { recursive: true });
    mkdirSync(join(vendorDir, version, "wasm_eh"), { recursive: true });

    const wasmData = "fake-wasm-bytes";
    const workerData = "/* upstream duckdb worker */\nself.postMessage('ready');\n";
    const extData = "fake-parquet-extension";
    writeFileSync(join(srcDir, "duckdb-eh.wasm"), wasmData);
    writeFileSync(join(srcDir, "duckdb-browser-eh.worker.js"), workerData);
    const extRel = `${version}/wasm_eh/parquet.duckdb_extension.wasm`;
    writeFileSync(join(vendorDir, extRel), extData);

    // Pre-populate stale v1-only destination directory
    const staleVersionDir = join(publicDir, version);
    mkdirSync(staleVersionDir, { recursive: true });
    writeFileSync(join(staleVersionDir, "duckdb-eh.wasm"), wasmData);
    writeFileSync(join(staleVersionDir, "duckdb-browser-eh.worker.js"), workerData);

    const expectedSha = createSha256Hex(extData);
    writeFileSync(join(vendorDir, "manifest.json"), JSON.stringify({ files: { [extRel]: expectedSha } }));

    const first = syncDuckdbAssets({ srcDir, vendorDir, publicDir, version });
    expect(first.copied).toBe(true);
    const destDir = join(publicDir, version);
    expect(readFileSync(join(destDir, "duckdb-eh.wasm"), "utf8")).toBe(wasmData);
    expect(readFileSync(join(destDir, DUCKDB_HARDENED_WORKER_FILE), "utf8")).toBe(
      renderHardenedDuckdbWorker(workerData),
    );
    expect(readFileSync(join(destDir, "duckdb-browser-eh.worker.js"), "utf8")).toBe(DUCKDB_LEGACY_WORKER_SHIM);
    expect(readFileSync(join(destDir, "extensions", extRel), "utf8")).toBe(extData);

    const second = syncDuckdbAssets({ srcDir, vendorDir, publicDir, version });
    expect(second.copied).toBe(false);
  });
});

function createSha256Hex(content: string): string {
  return createHash("sha256").update(content).digest("hex");
}
