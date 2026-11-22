// @vitest-environment node
import { parquetWriteBuffer } from "hyparquet-writer";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { getDuckDBEngine } from "./dashboard/duckdb";
import {
  evaluateMatrixAuc,
  normalizeScoringRoute,
  retrainCloudScoring,
  trainCloudScoringMatrix,
} from "./scoring-train";

const engine = getDuckDBEngine();

beforeEach(async () => {
  await engine.reset();
});

afterEach(() => {
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
});

describe("normalizeScoringRoute", () => {
  it("normalizes numeric IDs, UUIDs, hashes, prefixed IDs, and query strings", () => {
    expect(normalizeScoringRoute("/Products/12345?sort=asc")).toEqual({
      path: "/products/*",
      category: "product",
    });
    expect(
      normalizeScoringRoute("/users/550e8400-e29b-41d4-a716-446655440000/profile"),
    ).toEqual({
      path: "/users/*/profile",
      category: "account",
    });
    expect(normalizeScoringRoute("/orders/ORD-998877/status")).toEqual({
      path: "/orders/*/status",
      category: "checkout",
    });
    expect(normalizeScoringRoute("/")).toEqual({
      path: "/",
      category: "home",
    });
  });
});

describe("trainCloudScoringMatrix", () => {
  it("sessionizes Parquet request logs, drops short/fast sessions, and builds FSM1-ready matrix", async () => {
    const base = Date.parse("2026-11-18T12:00:00Z");
    const rows = [
      // Good human-like session (3 events, 10s apart)
      { ts: base, ip: "192.0.2.10", ua: "Mozilla/5.0", url: "/", edge_sid: "sid-good" },
      { ts: base + 10_000, ip: "192.0.2.10", ua: "Mozilla/5.0", url: "/products/42", edge_sid: "sid-good" },
      { ts: base + 20_000, ip: "192.0.2.10", ua: "Mozilla/5.0", url: "/cart", edge_sid: "sid-good" },
      // Too short (2 events) -> dropped
      { ts: base, ip: "192.0.2.11", ua: "Bot/1.0", url: "/a", edge_sid: "sid-short" },
      { ts: base + 5_000, ip: "192.0.2.11", ua: "Bot/1.0", url: "/b", edge_sid: "sid-short" },
      // Too fast (3 events in 100ms) -> dropped
      { ts: base, ip: "192.0.2.12", ua: "Fast/1.0", url: "/x", edge_sid: "sid-fast" },
      { ts: base + 50, ip: "192.0.2.12", ua: "Fast/1.0", url: "/y", edge_sid: "sid-fast" },
      { ts: base + 100, ip: "192.0.2.12", ua: "Fast/1.0", url: "/z", edge_sid: "sid-fast" },
    ];
    const parquet = parquetWriteBuffer({
      columnData: [
        { name: "timestamp", type: "TIMESTAMP", data: rows.map((r) => new Date(r.ts)) },
        { name: "ip", type: "STRING", data: rows.map((r) => r.ip) },
        { name: "ua", type: "STRING", data: rows.map((r) => r.ua) },
        { name: "url", type: "STRING", data: rows.map((r) => r.url) },
        { name: "edge_sid", type: "STRING", data: rows.map((r) => r.edge_sid) },
      ],
    });
    await engine.syncWindow([{ key: "req-1", bytes: parquet }]);

    const result = await trainCloudScoringMatrix(engine.readContext(), {
      sinceDays: 7,
      endMs: base + 60_000,
      version: "2026-11-18-test01",
    });

    expect(result.matrix.version).toBe("2026-11-18-test01");
    expect(result.matrix.session_count).toBe(1);
    expect(result.matrix.transition_count).toBe(2);
    expect(result.matrix.counts).toEqual({
      "/": { "/products/*": 1 },
      "/products/*": { "/cart": 1 },
    });
    expect(result.matrix.row_totals).toEqual({
      "/": 1,
      "/products/*": 1,
    });
    expect(result.stats).toMatchObject({
      sessions_in: 3,
      sessions_kept: 1,
      sessions_dropped_short: 1,
      sessions_dropped_fast: 1,
      transitions: 2,
    });
  });

  it("evaluates ROC-AUC when >=5 good and >=5 bad labels are present", () => {
    const matrix = {
      version: "2026-11-18-test",
      vocab_size: 3,
      session_count: 10,
      transition_count: 20,
      counts: { "/": { "/products/*": 18, "/cart": 2 } },
      row_totals: { "/": 20 },
      categories: { "/": "home", "/products/*": "product", "/cart": "cart" },
      anchors: [],
    };
    const labeled = [
      ...Array.from({ length: 5 }, (_, i) => ({
        edge_sid: `g${i}`,
        label: "good" as const,
        urls: ["/", "/products/1"],
      })),
      ...Array.from({ length: 5 }, (_, i) => ({
        edge_sid: `b${i}`,
        label: "bad" as const,
        urls: ["/", "/admin/secret"],
      })),
    ];
    const auc = evaluateMatrixAuc(matrix, labeled);
    expect(auc).not.toBeNull();
    expect(auc!).toBeGreaterThan(0.8);
  });

  it("posts the trained matrix to /api/services/{id}/scoring/retrain", async () => {
    const base = Date.parse("2026-11-18T12:00:00Z");
    const parquet = parquetWriteBuffer({
      columnData: [
        { name: "timestamp", type: "TIMESTAMP", data: [new Date(base), new Date(base + 5000), new Date(base + 10000)] },
        { name: "ip", type: "STRING", data: ["192.0.2.1", "192.0.2.1", "192.0.2.1"] },
        { name: "ua", type: "STRING", data: ["Mozilla/5.0", "Mozilla/5.0", "Mozilla/5.0"] },
        { name: "url", type: "STRING", data: ["/", "/products/1", "/cart"] },
      ],
    });
    await engine.syncWindow([{ key: "req-2", bytes: parquet }]);
    const fetcher = vi.fn(async (input: RequestInfo | URL) => {
      const url = String(input);
      if (url.endsWith("/labels")) {
        return Response.json({ labels: [], counts: { good: 0, bad: 0, total: 0 } });
      }
      return Response.json({ ok: true, version: "2026-11-18-000001" });
    });
    vi.stubGlobal("fetch", fetcher);

    const resp = await retrainCloudScoring("svc123", {
      readContext: engine.readContext(),
      sinceDays: 7,
      endMs: base + 60_000,
    });
    expect(resp.ok).toBe(true);
    const retrainCall = fetcher.mock.calls.find(([u]) => String(u).endsWith("/scoring/retrain"));
    expect(retrainCall).toBeDefined();
  });
});
