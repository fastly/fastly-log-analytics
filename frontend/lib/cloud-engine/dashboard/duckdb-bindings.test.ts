// @vitest-environment node
import { describe, expect, it, vi } from "vitest";
import { parquetWriteBuffer } from "hyparquet-writer";
import { createNodeBackend, DuckDBAnalyticalEngine, FIELD_DUCKDB_TYPES } from "./duckdb";
import { parquetFixture } from "./test-fixtures";
import { Aggregator } from "./engine";

describe("bounded complete Parquet bindings", () => {
  it("answers the complete filtered range without materializing unused wide columns", async () => {
    const engine = new DuckDBAnalyticalEngine();
    await engine.executeSql("SET memory_limit = '8MB'");
    const rows = 64;
    const bytes = parquetWriteBuffer({
      codec: "SNAPPY",
      columnData: [
        { name: "timestamp", type: "TIMESTAMP", data: Array.from({ length: rows }, (_, i) => new Date(1700000000000 + i)) },
        { name: "status", type: "INT32", data: Array.from({ length: rows }, (_, i) => i % 2 ? 500 : 200) },
        ...Object.entries(FIELD_DUCKDB_TYPES).filter(([, type]) => type === "VARCHAR")
          .map(([name]) => ({ name, type: "STRING" as const, data: Array.from({ length: rows }, () => name.repeat(1024)) })),
      ],
    });
    const original = new Uint8Array(bytes).slice();
    for (let i = 0; i < 4; i++) {
      await engine.ingestFile(`wide-${i}`, `wide-${i}.parquet`, bytes);
    }
    expect(await engine.executeSql("SELECT count(*) AS count, count(*) FILTER (WHERE status = 500) AS errors FROM logs"))
      .toEqual([{ count: rows * 4, errors: rows * 2 }]);
    expect(new Uint8Array(bytes)).toEqual(original);
    await engine.reset();
  });

  it("keeps empty datasets typed and rejects a read context after reset", async () => {
    const engine = new DuckDBAnalyticalEngine();
    await engine.ingestFile("one", "one.parquet", parquetFixture([
      { timestamp: 1700000000000, status: 200, cache: "HIT", resp_bytes: 1 },
    ]));
    const read = engine.readContext();
    await engine.reset();
    await expect(read.executeSql("SELECT count(*) FROM logs")).rejects.toThrow(/generation|expired/i);
    await expect(read.getPresentColumns()).rejects.toThrow(/generation|expired/i);
    await expect(engine.queryIntoAggregator([], 0, 1, [], new Aggregator(0, 1), { context: read }))
      .rejects.toThrow(/generation|expired/i);
    expect(await engine.executeSql("SELECT count(*) AS count FROM client_vitals WHERE metric_value > 0"))
      .toEqual([{ count: 0 }]);
    expect(await engine.executeSql("SELECT count(*) AS count FROM logs")).toEqual([{ count: 0 }]);
  });

  it("replaces a file without losing nullable and mixed timestamp/type semantics", async () => {
    const engine = new DuckDBAnalyticalEngine();
    const integer = parquetWriteBuffer({ columnData: [
      { name: "timestamp", type: "INT64", data: [BigInt(1700000000000)] },
      { name: "status", type: "INT32", data: [201] },
      { name: "ttl", type: "DOUBLE", data: [NaN] },
    ] });
    await engine.ingestFile("one", "one.parquet", integer);
    await engine.ingestFile("two", "two.parquet", parquetFixture([
      { timestamp: 1700000000001, status: 202, cache: "HIT", resp_bytes: 2 },
    ]));
    expect(await engine.executeSql("SELECT status, ts_ms, ttl, cache FROM requests ORDER BY status"))
      .toEqual([
        { status: 201, ts_ms: 1700000000000, ttl: null, cache: null },
        { status: 202, ts_ms: 1700000000001, ttl: null, cache: "HIT" },
      ]);
    await engine.retainFiles({ request: ["two"], vitals: [], errors: [] });
    expect(await engine.executeSql("SELECT status FROM logs")).toEqual([{ status: 202 }]);
    await engine.reset();
  });

  it("quarantines old queued SQL and recreates only on the next request", async () => {
    let factories = 0, disposed = 0;
    let fail = false;
    const engine = new DuckDBAnalyticalEngine({ backendFactory: async () => {
      factories++;
      const backend = await createNodeBackend();
      return {
        ...backend,
        query: async (sql: string, signal?: AbortSignal) => {
          if (fail && sql === "SELECT status FROM requests") {
            fail = false;
            throw new Error("Out of Memory Error: could not allocate block");
          }
          return backend.query(sql, signal);
        },
        dispose: async () => { disposed++; await backend.dispose(); },
      };
    } });
    const bytes = parquetFixture([{ timestamp: 1700000000000, status: 200, cache: "HIT", resp_bytes: 1 }]);
    await engine.ingestFile("one", "one.parquet", bytes);
    const old = engine.readContext();
    fail = true;
    const results = await Promise.allSettled([
      old.executeSql("SELECT status FROM requests"),
      old.executeSql("SELECT count(*) AS count FROM logs"),
    ]);
    expect(results.map((result) => result.status)).toEqual(["rejected", "rejected"]);
    expect(factories).toBe(1);
    expect(disposed).toBe(0);
    await engine.beginRequest();
    await engine.ingestFile("one", "one.parquet", bytes);
    expect(await engine.executeSql("SELECT status FROM requests")).toEqual([{ status: 200 }]);
    expect(factories).toBe(2);
    expect(disposed).toBe(1);
    await expect(old.getPresentColumns()).rejects.toThrow(/generation|expired/i);
    await engine.reset();
  });

  it("does not poison ordinary SQL errors or replace an unconfirmed old backend", async () => {
    let factories = 0, refuseCleanup = true;
    let fail = false;
    const engine = new DuckDBAnalyticalEngine({ backendFactory: async () => {
      factories++;
      const backend = await createNodeBackend();
      return {
        ...backend,
        query: async (sql: string, signal?: AbortSignal) => {
          if (fail && sql === "SELECT 123 AS value") throw new Error("FATAL Error: database has been invalidated");
          return backend.query(sql, signal);
        },
        dispose: async () => {
          if (refuseCleanup) throw new Error("worker termination unconfirmed");
          await backend.dispose();
        },
      };
    } });
    await expect(engine.executeSql("SELECT missing_column FROM requests")).rejects.toThrow();
    expect(await engine.executeSql("SELECT 1 AS value")).toEqual([{ value: 1 }]);
    fail = true;
    await expect(engine.executeSql("SELECT 123 AS value")).rejects.toThrow(/FATAL/);
    await expect(engine.beginRequest()).rejects.toThrow(/cleanup|termination/i);
    expect(factories).toBe(1);
    await expect(engine.executeSql("SELECT 1 AS value")).rejects.toThrow();
    refuseCleanup = false;
    fail = false;
    await engine.beginRequest();
    expect(await engine.executeSql("SELECT 1 AS value")).toEqual([{ value: 1 }]);
    expect(factories).toBe(2);
    await engine.reset();
  });

  it("does not commit an old registration after reset or release replacement pins", async () => {
    let resume!: () => void, entered!: () => void;
    const waiting = new Promise<void>((resolve) => { resume = resolve; });
    const started = new Promise<void>((resolve) => { entered = resolve; });
    let delay = true;
    const engine = new DuckDBAnalyticalEngine({ backendFactory: async () => {
      const backend = await createNodeBackend();
      return { ...backend, registerFileBuffer: async (name: string, buffer: Uint8Array) => {
        if (delay) { entered(); await waiting; }
        await backend.registerFileBuffer(name, buffer);
      } };
    } });
    const bytes = parquetFixture([{ timestamp: 1700000000000, status: 200, cache: "HIT", resp_bytes: 1 }]);
    const oldLease = engine.lease();
    const pending = oldLease.ingestFile("one", "one.parquet", bytes);
    const rejected = expect(pending).rejects.toThrow(/generation|expired/i);
    await started;
    const reset = engine.reset();
    delay = false;
    resume();
    await rejected;
    await reset;
    const fresh = engine.lease();
    await fresh.ingestFile("one", "one.parquet", bytes);
    oldLease.release();
    await engine.retainFiles({ request: [], vitals: [], errors: [] });
    expect(await fresh.readContext().executeSql("SELECT count(*) AS count FROM requests")).toEqual([{ count: 1 }]);
    fresh.release();
    await engine.reset();
  });

  it("owns registration transfers and keeps the cached compressed bytes reusable", async () => {
    const engine = new DuckDBAnalyticalEngine({ backendFactory: async () => {
      const backend = await createNodeBackend();
      return { ...backend, registerFileBuffer: async (name, buffer) => {
        const delivered = buffer.slice();
        structuredClone(buffer.buffer, { transfer: [buffer.buffer] });
        await backend.registerFileBuffer(name, delivered);
      } };
    } });
    const bytes = parquetFixture([{ timestamp: 1700000000000, status: 201, cache: "HIT", resp_bytes: 3 }]);
    const original = new Uint8Array(bytes).slice();
    await engine.ingestFile("service-a:v1", "one.parquet", bytes);
    expect(new Uint8Array(bytes)).toEqual(original);
    await engine.reset();
    await engine.ingestFile("service-b:v2", "one.parquet", bytes);
    expect(await engine.executeSql("SELECT scoped_key, status FROM logs"))
      .toEqual([{ scoped_key: "service-b:v2", status: 201 }]);
    expect(new Uint8Array(bytes)).toEqual(original);
    await engine.reset();
  });

  it("keeps the original publication OOM visible without running unsafe cleanup or replay", async () => {
    let fail = false, drops = 0, factories = 0;
    const oom = new Error("Out of Memory Error: synthetic view publication");
    const engine = new DuckDBAnalyticalEngine({ backendFactory: async () => {
      factories++;
      const backend = await createNodeBackend();
      return { ...backend,
        query: async (sql, signal) => {
          if (fail && sql.includes("read_parquet") && sql.startsWith("CREATE")) throw oom;
          return backend.query(sql, signal);
        },
        dropFile: async (name) => {
          drops++;
          if (fail) throw new Error("FATAL Error: database has been invalidated");
          await backend.dropFile(name);
        },
      };
    } });
    await engine.prewarm();
    fail = true;
    const bytes = parquetFixture([{ timestamp: 1700000000000, status: 201, cache: "HIT", resp_bytes: 3 }]);
    await expect(engine.ingestFile("one", "one.parquet", bytes)).rejects.toBe(oom);
    expect(drops).toBe(0);
    expect(factories).toBe(1);
    expect(() => engine.getFileMeta("one")).toThrow(oom);
    fail = false;
    await engine.beginRequest();
    await engine.ingestFile("one", "one.parquet", bytes);
    expect(await engine.executeSql("SELECT count(*) AS count FROM logs")).toEqual([{ count: 1 }]);
    expect(factories).toBe(2);
    await engine.reset();
  });

  it("retains typed empty custom fields and dataset-specific RUM aliases", async () => {
    const engine = new DuckDBAnalyticalEngine();
    const lease = engine.lease([{ id: "custom_duration", type: "number" }]);
    expect(await lease.readContext().executeSql("SELECT count(*) AS count FROM requests WHERE custom_duration > 0"))
      .toEqual([{ count: 0 }]);
    const vitals = parquetWriteBuffer({ columnData: [
      { name: "timestamp", type: "STRING", data: ["2023-11-14T22:13:20Z"] },
      { name: "req_id", type: "STRING", data: ["event-a"] },
      { name: "cid", type: "STRING", data: ["client-a"] },
      { name: "metric_rating", type: "STRING", data: ["good"] },
      { name: "metric_value", type: "DOUBLE", data: [NaN] },
      { name: "pathname", type: "STRING", data: ["/home"] },
      { name: "browser", type: "STRING", data: ["Example Browser"] },
    ] });
    const errors = parquetWriteBuffer({ columnData: [
      { name: "ts_ms", type: "INT64", data: [BigInt(1700000000001)] },
      { name: "error_file", type: "STRING", data: ["app.js"] },
      { name: "error_line", type: "INT32", data: [17] },
      { name: "error_col", type: "INT32", data: [4] },
    ] });
    await engine.syncRumFiles([{ key: "vitals", bytes: vitals }], [{ key: "errors", bytes: errors }]);
    expect(await engine.executeSql("SELECT ts_ms, request_event_id, client_id, session_id, rating, metric_value, page_url, browser_name FROM client_vitals"))
      .toEqual([{ ts_ms: 1700000000000, request_event_id: "event-a", client_id: "client-a", session_id: "client-a",
        rating: "good", metric_value: null, page_url: "/home", browser_name: "Example Browser" }]);
    expect(await engine.executeSql("SELECT ts_ms, filename, lineno, colno FROM client_errors"))
      .toEqual([{ ts_ms: 1700000000001, filename: "app.js", lineno: 17, colno: 4 }]);
    expect(await engine.getPresentColumns()).toEqual(new Set());
    lease.release();
    await engine.reset();
  });

  it("does not evict a binding pinned while compressed-budget admission awaits publication", async () => {
    let enter!: () => void, resume!: () => void, delay = false;
    const entered = new Promise<void>((resolve) => { enter = resolve; });
    const waiting = new Promise<void>((resolve) => { resume = resolve; });
    const engine = new DuckDBAnalyticalEngine({ backendFactory: async () => {
      const backend = await createNodeBackend();
      return { ...backend, query: async (sql, signal) => {
        if (delay && sql.startsWith("CREATE") && !sql.includes("read_parquet")) {
          delay = false;
          enter();
          await waiting;
        }
        return backend.query(sql, signal);
      } };
    } });
    const weighted = (status: number) => {
      const bytes = parquetFixture([{ timestamp: 1700000000000, status, cache: "HIT", resp_bytes: 1 }]);
      // Exercise admission accounting with real Parquet without a large allocation.
      Object.defineProperty(bytes, "byteLength", { value: 40 * 1024 * 1024 });
      return bytes;
    };
    await engine.ingestFile("one", "one.parquet", weighted(201));
    const old = engine.readContext();
    delay = true;
    const pending = engine.ingestFile("two", "two.parquet", weighted(202));
    const rejected = expect(pending).rejects.toThrow(/byte budget/);
    await entered;
    const queued = old.executeSql("SELECT status FROM logs");
    resume();
    await rejected;
    expect(await queued).toEqual([{ status: 201 }]);
    expect(engine.hasFile("one")).toBe(true);
    expect(engine.hasFile("two")).toBe(false);
    await engine.ingestFile("two", "two.parquet", weighted(202));
    expect(await engine.executeSql("SELECT status FROM logs")).toEqual([{ status: 202 }]);
    await expect(old.getPresentColumns()).rejects.toThrow(/expired/i);
    await engine.reset();
  });

  it("rejects old queued cancellation without poisoning a replacement range", async () => {
    let enter!: () => void, resume!: () => void, delay = false;
    const entered = new Promise<void>((resolve) => { enter = resolve; });
    const waiting = new Promise<void>((resolve) => { resume = resolve; });
    const engine = new DuckDBAnalyticalEngine({ backendFactory: async () => {
      const backend = await createNodeBackend();
      return { ...backend, query: async (sql, signal) => {
        if (delay && sql === "SELECT status FROM logs") {
          enter();
          await waiting;
        }
        return backend.query(sql, signal);
      } };
    } });
    const bytes = parquetFixture([{ timestamp: 1700000000000, status: 201, cache: "HIT", resp_bytes: 1 }]);
    await engine.ingestFile("one", "one.parquet", bytes);
    const old = engine.readContext();
    delay = true;
    const running = old.executeSql("SELECT status FROM logs");
    const runningRejected = expect(running).rejects.toThrow(/generation|expired/i);
    await entered;
    const controller = new AbortController();
    const queued = old.executeSql("SELECT count(*) FROM logs", [], controller.signal);
    const queuedRejected = expect(queued).rejects.toMatchObject({ name: "AbortError" });
    controller.abort();
    const reset = engine.reset();
    delay = false;
    resume();
    await runningRejected;
    await queuedRejected;
    await reset;
    await engine.ingestFile("one", "one.parquet", bytes);
    expect(await engine.executeSql("SELECT status FROM logs")).toEqual([{ status: 201 }]);
    await engine.reset();
  });

  it("does not start a replacement after the owned backend cleanup deadline", async () => {
    let owned!: Awaited<ReturnType<typeof createNodeBackend>>, factories = 0;
    const engine = new DuckDBAnalyticalEngine({ backendFactory: async () => {
      factories++;
      owned = await createNodeBackend();
      return { ...owned,
        query: async (sql, signal) => {
          if (sql === "SELECT 123") throw new Error("Out of Memory Error: synthetic cleanup timeout");
          return owned.query(sql, signal);
        },
        dispose: () => new Promise<void>(() => {}),
      };
    } });
    await expect(engine.executeSql("SELECT 123")).rejects.toThrow(/Out of Memory/);
    vi.useFakeTimers();
    try {
      const pending = engine.beginRequest();
      const rejected = expect(pending).rejects.toThrow(/cleanup failed.*replacement was not started/);
      await vi.advanceTimersByTimeAsync(2001);
      await rejected;
      expect(factories).toBe(1);
      await expect(engine.prewarm()).rejects.toThrow(/Out of Memory/);
    } finally {
      vi.useRealTimers();
      await owned.dispose();
    }
  });

  it("restores the complete old views before dropping a failed partial publication", async () => {
    let fail = false;
    const engine = new DuckDBAnalyticalEngine({ backendFactory: async () => {
      const backend = await createNodeBackend();
      return { ...backend, query: async (sql, signal) => {
        if (fail && sql.startsWith("CREATE") && sql.includes("UNION ALL")) {
          fail = false;
          await backend.query(sql.split(";")[0], signal);
          throw new Error("Invalid Input Error: synthetic partial view publication");
        }
        return backend.query(sql, signal);
      } };
    } });
    const bytes = parquetFixture([{ timestamp: 1700000000000, status: 201, cache: "HIT", resp_bytes: 1 }]);
    await engine.ingestFile("one", "one.parquet", bytes);
    fail = true;
    await expect(engine.ingestFile("two", "two.parquet", bytes)).rejects.toThrow(/partial view publication/);
    expect(await engine.executeSql("SELECT count(*) AS count FROM logs")).toEqual([{ count: 1 }]);
    expect(engine.hasFile("one")).toBe(true);
    expect(engine.hasFile("two")).toBe(false);
    await engine.reset();
  });

  it("quarantines a refused selective-file cleanup before reusing the backend", async () => {
    let fail = false, factories = 0, disposals = 0;
    const engine = new DuckDBAnalyticalEngine({ backendFactory: async () => {
      factories++;
      const backend = await createNodeBackend();
      return { ...backend,
        dropFile: async (name) => {
          if (fail) throw new Error("Invalid Input Error: synthetic drop refusal");
          await backend.dropFile(name);
        },
        dispose: async () => { disposals++; await backend.dispose(); },
      };
    } });
    const bytes = parquetFixture([{ timestamp: 1700000000000, status: 201, cache: "HIT", resp_bytes: 1 }]);
    await engine.prewarm();
    fail = true;
    await expect(engine.queryParquetIntoAggregator(
      { kind: "buffer", bytes }, 0, 1700000000001, [], new Aggregator(0, 1700000000001, ["status"]),
    )).rejects.toThrow(/drop refusal/);
    expect(() => engine.getCustomColumnTypes()).toThrow(/cleanup/i);
    expect(factories).toBe(1);
    await engine.beginRequest();
    fail = false;
    await engine.ingestFile("one", "one.parquet", bytes);
    expect(await engine.executeSql("SELECT count(*) AS count FROM logs")).toEqual([{ count: 1 }]);
    expect(factories).toBe(2);
    expect(disposals).toBe(1);
    await engine.reset();
  });

  it("retains the query error when selective-file cleanup also fails", async () => {
    let fail = false, queryFailures = 0;
    const engine = new DuckDBAnalyticalEngine({ backendFactory: async () => {
      const backend = await createNodeBackend();
      return { ...backend,
        query: async (sql, signal) => {
          if (fail && !sql.startsWith("DESCRIBE") && sql.includes("selective_")) {
            queryFailures++;
            throw new Error("Invalid Input Error: synthetic query failure");
          }
          return backend.query(sql, signal);
        },
        dropFile: async (name) => {
          if (fail) throw new Error("synthetic drop refusal");
          await backend.dropFile(name);
        },
      };
    } });
    await engine.prewarm();
    fail = true;
    const bytes = parquetFixture([{ timestamp: 1700000000000, status: 201, cache: "HIT", resp_bytes: 1 }]);
    await expect(engine.queryParquetIntoAggregator(
      { kind: "buffer", bytes }, 0, 1700000000001, [], new Aggregator(0, 1700000000001, ["status"]),
    )).rejects.toThrow(/query failure.*drop refusal/);
    expect(queryFailures).toBe(1);
    fail = false;
    await engine.beginRequest();
  });

  it("holds RUM bindings through the same scoped lease and fences released reads", async () => {
    const engine = new DuckDBAnalyticalEngine();
    const bytes = parquetFixture([{ timestamp: 1700000000000, status: 201, cache: "HIT", resp_bytes: 1 }]);
    await engine.syncRumFiles([{ key: "vitals", bytes }]);
    const lease = engine.lease();
    expect(lease.getFileMeta("vitals", "vitals")).toBeDefined();
    const read = lease.readContext();
    await engine.retainFiles({ request: [], vitals: [], errors: [] });
    expect(await read.executeSql("SELECT count(*) AS count FROM client_vitals")).toEqual([{ count: 1 }]);
    lease.release();
    await expect(read.executeSql("SELECT count(*) FROM client_vitals")).rejects.toThrow(/expired/i);
    await engine.reset();
  });

  it("isolates selected service files and custom schemas while both leases remain live", async () => {
    const engine = new DuckDBAnalyticalEngine();
    const first = engine.lease([{ id: "custom_duration", type: "number" }]);
    await first.ingestFile("service-a:v1", "one.parquet", parquetWriteBuffer({ columnData: [
      { name: "timestamp", type: "TIMESTAMP", data: [new Date(1700000000000)] },
      { name: "custom_duration", type: "DOUBLE", data: [3.5] },
    ] }));
    const firstRead = first.readContext();
    const second = engine.lease();
    await second.ingestFile("service-b:v2", "two.parquet", parquetFixture([
      { timestamp: 1700000000000, status: 202, cache: "HIT", resp_bytes: 1 },
    ]));
    const secondRead = second.readContext();
    expect(secondRead.getCustomColumnTypes().has("custom_duration")).toBe(false);
    await expect(secondRead.executeSql("SELECT custom_duration FROM logs")).rejects.toThrow(/column/i);
    expect(await secondRead.executeSql("SELECT scoped_key, status FROM logs"))
      .toEqual([{ scoped_key: "service-b:v2", status: 202 }]);
    expect(await firstRead.executeSql("SELECT custom_duration FROM logs")).toEqual([{ custom_duration: 3.5 }]);
    first.release();
    second.release();
    await engine.reset();
  });

  it("publishes a lease's deferred files with one view rebuild instead of one per file", async () => {
    let views = 0;
    const engine = new DuckDBAnalyticalEngine({ backendFactory: async () => {
      const backend = await createNodeBackend();
      return { ...backend, query: async (sql, signal) => {
        if (sql.startsWith("CREATE OR REPLACE VIEW") && sql.includes("read_parquet")) views++;
        return backend.query(sql, signal);
      } };
    } });
    const lease = engine.lease();
    for (let i = 0; i < 20; i++) {
      await lease.ingestFile(`f${i}`, `f${i}.parquet`, parquetFixture([
        { timestamp: 1700000000000 + i, status: 200 + i, cache: "HIT", resp_bytes: 1 },
      ]), undefined, undefined, "request", { deferView: true });
    }
    expect(views).toBe(0);
    await lease.publishView();
    expect(views).toBe(1);
    const read = lease.readContext();
    expect(await read.executeSql("SELECT count(*) AS count, count(DISTINCT status) AS statuses FROM logs"))
      .toEqual([{ count: 20, statuses: 20 }]);
    expect(views).toBe(1);
    lease.release();
    await engine.reset();
  });

  it("retires a deferred batch whose view publication fails and keeps earlier files readable", async () => {
    let fail = false;
    const engine = new DuckDBAnalyticalEngine({ backendFactory: async () => {
      const backend = await createNodeBackend();
      return { ...backend, query: async (sql, signal) => {
        if (fail && sql.startsWith("CREATE") && sql.includes("UNION ALL")) {
          fail = false;
          throw new Error("Invalid Input Error: synthetic batch view publication");
        }
        return backend.query(sql, signal);
      } };
    } });
    const bytes = parquetFixture([{ timestamp: 1700000000000, status: 201, cache: "HIT", resp_bytes: 1 }]);
    await engine.ingestFile("one", "one.parquet", bytes);
    const lease = engine.lease();
    await lease.ingestFile("two", "two.parquet", bytes, undefined, undefined, "request", { deferView: true });
    await lease.ingestFile("three", "three.parquet", bytes, undefined, undefined, "request", { deferView: true });
    fail = true;
    await expect(lease.publishView()).rejects.toThrow(/batch view publication/);
    lease.release();
    expect(engine.hasFile("two")).toBe(false);
    expect(engine.hasFile("three")).toBe(false);
    expect(await engine.executeSql("SELECT count(*) AS count FROM logs")).toEqual([{ count: 1 }]);
    await engine.reset();
  });

  it("does not mistake SQL input quoted in an ordinary error for a fatal database error", async () => {
    const engine = new DuckDBAnalyticalEngine();
    await expect(engine.executeSql("SELECT CAST('Out of Memory Error: user input' AS INTEGER)"))
      .rejects.toThrow(/Conversion Error/);
    expect(await engine.executeSql("SELECT 1 AS value")).toEqual([{ value: 1 }]);
    await engine.reset();
  });
});
