// @vitest-environment node
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { createBrowserBackend, DuckDBAnalyticalEngine } from "./duckdb";

type MockReader = AsyncIterable<never> & { schema: { fields: never[] } };
const state = vi.hoisted(() => ({
  fail: "", cleanupFails: false, factories: 0, terminations: 0,
  queries: [] as string[],
  send: null as ((sql: string) => Promise<MockReader>) | null,
  terminate: null as (() => Promise<void>) | null,
}));

function emptyReader(): MockReader {
  return { schema: { fields: [] }, async *[Symbol.asyncIterator]() { yield* []; } };
}

vi.mock("@duckdb/duckdb-wasm", () => ({
  PACKAGE_VERSION: "test",
  VoidLogger: class {},
  AsyncDuckDB: class {
    constructor() {
      state.factories++;
      if (state.fail === "constructor") throw new Error("synthetic constructor failure");
    }
    async instantiate() { if (state.fail === "instantiate") throw new Error("synthetic instantiate failure"); }
    async open() { if (state.fail === "open") throw new Error("synthetic open failure"); }
    async connect() {
      if (state.fail === "connect") throw new Error("synthetic connect failure");
      return {
        send: async (sql: string) => {
          state.queries.push(sql);
          return state.send ? state.send(sql) : emptyReader();
        },
        cancelSent: async () => {},
        query: async () => {
        if (state.fail === "extension") throw new Error("synthetic extension failure");
        return { numRows: 0, schema: { fields: [] }, getChild: () => null };
        },
      };
    }
    async terminate() {
      state.terminations++;
      if (state.cleanupFails) throw new Error("synthetic termination failure");
      await state.terminate?.();
    }
  },
}));

beforeEach(() => {
  Object.assign(state, {
    fail: "", cleanupFails: false, factories: 0, terminations: 0,
    queries: [], send: null, terminate: null,
  });
  vi.stubGlobal("Worker", class { terminate() { state.terminations++; } });
});

describe("browser native rejection and cancellation", () => {
  it.each([
    ["send", "Out of Memory Error: synthetic native failure"],
    ["send", "FATAL Error: database has been invalidated"],
    ["stream", "Out of Memory Error: synthetic native failure"],
    ["stream", "FATAL Error: database has been invalidated"],
  ])("quarantines an aborted native rejection from %s: %s", async (phase, message) => {
    const engine = new DuckDBAnalyticalEngine({ backendFactory: createBrowserBackend });
    await engine.prewarm();
    let entered!: () => void, rejectNative!: (error: Error) => void;
    const started = new Promise<void>((resolve) => { entered = resolve; });
    const pending = new Promise<never>((_, reject) => { rejectNative = reject; });
    state.send = async (sql) => {
      if (sql !== "SELECT pending") return emptyReader();
      if (phase === "send") { entered(); return pending; }
      return { schema: { fields: [] }, async *[Symbol.asyncIterator]() {
        entered();
        await pending;
        yield* [];
      } };
    };
    const old = engine.readContext();
    const controller = new AbortController();
    const failure = new Error(message);
    try {
      const failing = old.executeSql("SELECT pending", [], controller.signal);
      const queued = old.executeSql("SELECT queued");
      const settled = Promise.allSettled([failing, queued]);
      await started;
      controller.abort();
      rejectNative(failure);
      const results = await settled;
      expect(results).toEqual([
        { status: "rejected", reason: failure },
        { status: "rejected", reason: failure },
      ]);
      expect(state.queries).not.toContain("SELECT queued");
      await expect(old.getPresentColumns()).rejects.toBe(failure);
      expect(state).toMatchObject({ factories: 1, terminations: 0 });

      let finishCleanup!: () => void, cleanupEntered!: () => void;
      const cleanupStarted = new Promise<void>((resolve) => { cleanupEntered = resolve; });
      state.terminate = () => new Promise<void>((resolve) => {
        finishCleanup = resolve;
        cleanupEntered();
      });
      const recovery = engine.beginRequest();
      await cleanupStarted;
      try { expect(state.factories).toBe(1); }
      finally { finishCleanup(); await recovery; state.terminate = null; }
      state.send = null;
      await engine.prewarm();
      expect(state).toMatchObject({ factories: 2, terminations: 1 });
      await expect(old.executeSql("SELECT stale")).rejects.toThrow(/generation|expired/i);
      await expect(engine.executeSql("SELECT recovered")).resolves.toEqual([]);
    } finally {
      state.send = null;
      state.terminate = null;
      await engine.reset();
    }
  });

  it.each(["send", "stream"])("preserves an aborted ordinary rejection from %s without quarantine", async (phase) => {
    const engine = new DuckDBAnalyticalEngine({ backendFactory: createBrowserBackend });
    await engine.prewarm();
    let entered!: () => void, rejectNative!: (error: Error) => void;
    const started = new Promise<void>((resolve) => { entered = resolve; });
    const pending = new Promise<never>((_, reject) => { rejectNative = reject; });
    state.send = async (sql) => {
      if (sql !== "SELECT pending") return emptyReader();
      if (phase === "send") { entered(); return pending; }
      return { schema: { fields: [] }, async *[Symbol.asyncIterator]() {
        entered(); await pending; yield* [];
      } };
    };
    const controller = new AbortController();
    const aborted = new DOMException("Ordinary cancellation", "AbortError");
    try {
      const failing = engine.executeSql("SELECT pending", [], controller.signal);
      const rejected = expect(failing).rejects.toBe(aborted);
      await started;
      controller.abort(aborted);
      rejectNative(new Error("Conversion Error: quoted Out of Memory Error is ordinary input"));
      await rejected;
      state.send = null;
      await engine.beginRequest();
      await expect(engine.executeSql("SELECT healthy")).resolves.toEqual([]);
      expect(state).toMatchObject({ factories: 1, terminations: 0 });
    } finally {
      state.send = null;
      await engine.reset();
    }
  });

  it("does not poison a new generation with an old aborted native rejection", async () => {
    const engine = new DuckDBAnalyticalEngine({ backendFactory: createBrowserBackend });
    await engine.prewarm();
    let entered!: () => void, rejectNative!: (error: Error) => void;
    const started = new Promise<void>((resolve) => { entered = resolve; });
    const pending = new Promise<never>((_, reject) => { rejectNative = reject; });
    state.send = async (sql) => {
      if (sql !== "SELECT pending") return emptyReader();
      entered();
      return pending;
    };
    const old = engine.readContext();
    const controller = new AbortController();
    const failure = new Error("Out of Memory Error: old generation");
    try {
      const rejected = expect(old.executeSql("SELECT pending", [], controller.signal)).rejects.toBe(failure);
      await started;
      const resetting = engine.reset();
      controller.abort();
      rejectNative(failure);
      await rejected;
      await resetting;
      state.send = null;
      await expect(engine.executeSql("SELECT fresh")).resolves.toEqual([]);
      expect(() => old.getCustomColumnTypes()).toThrow(/generation|expired/i);
      expect(state).toMatchObject({ factories: 1, terminations: 0 });
    } finally {
      state.send = null;
      await engine.reset();
    }
  });
});
afterEach(() => vi.unstubAllGlobals());

describe("owned browser backend initialization", () => {
  it.each(["constructor", "instantiate", "open", "connect", "extension"])(
    "terminates a failed %s backend and recreates only at the next request", async (phase) => {
      state.fail = phase;
      const engine = new DuckDBAnalyticalEngine({ backendFactory: createBrowserBackend });
      await expect(engine.prewarm()).rejects.toThrow(`synthetic ${phase} failure`);
      expect(state).toMatchObject({ factories: 1, terminations: 1 });
      await expect(engine.executeSql("SELECT 1")).rejects.toThrow(`synthetic ${phase} failure`);
      expect(state.factories).toBe(1);
      await engine.beginRequest();
      state.fail = "";
      await engine.prewarm();
      expect(state.factories).toBe(2);
      await engine.reset();
    },
  );

  it("does not replace an initialization failure whose worker cleanup was not confirmed", async () => {
    state.fail = "instantiate";
    state.cleanupFails = true;
    const engine = new DuckDBAnalyticalEngine({ backendFactory: createBrowserBackend });
    await expect(engine.prewarm()).rejects.toThrow(/instantiate failure.*cleanup was not confirmed/);
    state.fail = "";
    state.cleanupFails = false;
    await expect(engine.beginRequest()).rejects.toThrow(/cleanup failed.*replacement was not started/);
    expect(state.factories).toBe(1);
  });
});
