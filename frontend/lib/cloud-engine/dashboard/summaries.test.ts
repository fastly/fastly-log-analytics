// @vitest-environment node
import { parquetMetadata } from "hyparquet";
import { parquetWriteBuffer } from "hyparquet-writer";
import { readFileSync } from "node:fs";
import path from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import {
  CARD_FIELDS,
  customCards,
  decodeSummary,
  fingerprint,
  normalizeDashboardValue,
  planCoarsestFirst,
  planCompactsFromWindows,
  planSummaries,
  SLICE_GROUPS,
  summaryValue,
} from "./summaries";
import {
  classifySummaryAgainstFilters,
  clearDashboardMemo,
  DASHBOARD_FILTERED_SUMMARY_SPEC,
  DASHBOARD_SUMMARY_SPEC,
  resolveSummaryFromSlices,
  runDashboard,
  searchFieldSummaries,
  searchFieldValues,
  type DashboardResult,
} from "./engine";
import type { DashboardFilter } from "./filters";
import { fnv1a32 } from "../parquet-key";
import { Hll } from "./hll";
import type { ManifestFile } from "./manifest";
import { readColumns } from "./parquet";
import { editSummaryRows, LAYOUT_B, withSummaryLayout } from "./summary-layout-fixture";
import { defineSummarySpec, readSummaryProjection, summaryProjectionLimits } from "./summary-projection";
import { fixtureSource, parquetFixture, projectingSource } from "./test-fixtures";

// Written by `cargo test --test summary -- --ignored write_dashboard_fixtures`:
// raw windows of 2027-01-15 08:00-10:00Z, their summaries and the bundles the
// server builds from them (hour 08 urls exceed the bundle top K).
const DIR = path.join(__dirname, "__fixtures__", "summaries");
type IndexEntry = {
  key: string;
  file: string;
  size: number;
  kind: "raw" | "window" | "hour" | "day";
  start: number;
  end: number;
  rows: number;
  sources?: number;
  early?: boolean;
};
const INDEX: IndexEntry[] = JSON.parse(
  readFileSync(path.join(DIR, "index.json"), "utf8"),
);
const BUFFERS: Record<string, ArrayBuffer> = Object.fromEntries(
  INDEX.map((entry) => {
    const bytes = readFileSync(path.join(DIR, entry.file));
    return [
      entry.key,
      bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.length),
    ];
  }),
);
const iso = (s: number) => new Date(s * 1000).toISOString();
const entry = (e: IndexEntry): ManifestFile => ({
  key: e.key,
  start: iso(e.start),
  end: iso(e.end),
  size: e.size,
  rows: e.rows,
  ...(e.kind === "raw" ? {} : { kind: e.kind, sources: e.sources }),
});
const RAW = INDEX.filter((e) => e.kind === "raw").map(entry);
const pick = (
  kind: IndexEntry["kind"],
  pred: (e: IndexEntry) => boolean = () => true,
) => INDEX.filter((e) => e.kind === kind && pred(e)).map(entry);
const HOUR_09 = 1_800_003_600;
const page = (files: ManifestFile[]) => ({
  files,
  next_cursor: null,
  complete: true,
});

async function run(
  from: string,
  to: string,
  raw: ManifestFile[],
  summaries: ManifestFile[],
  extra: Record<string, ArrayBuffer> = {},
) {
  const source = fixtureSource(
    [page(raw)],
    { ...BUFFERS, ...extra },
    { json: [], files: [] },
    [page(summaries)],
  );
  const result = await runDashboard(source, { from, to });
  return { result, files: source.log.files };
}

/** A raw listing page as a server that omits inline summaries returns it. */
function withoutInlineSummaries(body: unknown): unknown {
  const { summary_files: _omitted, ...page } = body as Record<string, unknown>;
  return page;
}

/** Summary answers equal raw answers, except the documented approximations. */
function expectMatchesRaw(summary: DashboardResult, raw: DashboardResult) {
  expect(summary.totalRequests).toBe(raw.totalRequests);
  expect(summary.hasBytes).toBe(raw.hasBytes);
  expect(summary.buckets).toEqual(raw.buckets);
  const approximate: string[] = [];
  for (const field of CARD_FIELDS) {
    const card = summary.cards[field];
    const exact = raw.cards[field];
    if (!card.approximate) {
      // `all` feeds the map, so its order carries no meaning.
      const sorted = (c: typeof card) => ({
        ...c,
        all: c.all?.toSorted((x, y) =>
          String(x.value).localeCompare(String(y.value)),
        ),
      });
      expect(sorted(card), field).toEqual(sorted(exact));
      continue;
    }
    approximate.push(field);
    expect(card.total, field).toBe(exact.total);
    // Kept values may miss counts a source cut: lower bounds, never more.
    expect(card.top[0].count, field).toBeLessThanOrEqual(exact.top[0].count);
    // No count is low by more than the rows left outside every kept list.
    const slack = card.unattributed ?? NaN;
    expect(slack, field).toBeGreaterThanOrEqual(0);
    expect(slack, field).toBeLessThanOrEqual(card.total);
    const shown = new Map(card.top.map((e) => [e.value, e.count]));
    for (const entry of exact.top) {
      const seen = shown.get(entry.value);
      if (seen !== undefined) {
        expect(entry.count - seen, field).toBeLessThanOrEqual(slack);
      }
    }
    if (field === "ip") {
      expect(
        Math.abs(card.distinct - exact.distinct) / exact.distinct,
        field,
      ).toBeLessThan(0.15);
    } else {
      expect(card.distinct, field).toBeLessThanOrEqual(exact.distinct);
    }
  }
  return approximate;
}

describe("summary readers", () => {
  it("reserves whole-file budget for actual raw reads before summary-covered fallback windows", async () => {
    const from = "2027-01-15T07:00:00Z";
    const to = "2027-01-15T10:00:00Z";
    const compactKey = "historical-compact-without-summary";
    const compactBytes = await parquetFixture([
      {
        timestamp: Date.parse("2027-01-15T07:00:01Z"),
        host: "historical.example",
        status: 200,
        cache: "HIT",
        resp_bytes: 7,
      },
    ]);
    const files = [
      ...RAW.map((file) => ({ ...file, size: 1024 * 1024 })),
      {
        key: compactKey,
        start: "2027-01-15T07:00:00Z",
        end: "2027-01-15T07:01:00Z",
        rows: 1,
        size: compactBytes.byteLength,
      },
    ];
    expect(RAW.length).toBeGreaterThanOrEqual(8);
    const downloaded = new Set<string>();
    const persisted = new Set<string>();
    const source = {
      cacheScope: "summary-covered-prefetch-budget",
      fetchJson: async () => {
        throw new Error("unexpected manifest request");
      },
      fetchFile: async (key: string) => {
        const cached = downloaded.has(key);
        downloaded.add(key);
        return { bytes: BUFFERS[key], cached };
      },
      persistFile: async (file: ManifestFile) => {
        const downloaded = !persisted.has(file.key);
        persisted.add(file.key);
        return { downloaded };
      },
      openParquet: async (file: ManifestFile) => {
        expect(file.key).toBe(compactKey);
        expect(persisted.has(file.key)).toBe(true);
        return {
          kind: "buffer" as const,
          bytes: compactBytes.slice(0),
          cached: true,
        };
      },
    };
    const options = {
      from,
      to,
      wallNow: () => Date.parse(to),
      manifest: { files, summaryFiles: pick("window"), complete: true },
    };
    const cold = await runDashboard(source, options);
    const warm = await runDashboard(source, options);
    const baseline = await run(from, to, files, [], {
      [compactKey]: compactBytes,
    });
    expectMatchesRaw(cold, baseline.result);
    expect(warm.totalRequests).toBe(cold.totalRequests);
    expect(warm.cards).toEqual(cold.cards);
    expect([...persisted]).toEqual([compactKey]);
    expect(warm.stats.fetched).toBe(0);
    expect(warm.stats.cached).toBe(warm.stats.listed);
  });

  it("serves warm summaries from decodedSummaries memory cache without re-reading storage when openParquet is present", async () => {
    const from = "2027-01-15T07:00:00Z";
    const to = "2027-01-15T10:00:00Z";
    const files = RAW.map((file) => ({ ...file, size: 1024 * 1024 }));
    const summaryFiles = pick("window");
    const fetchCounts = new Map<string, number>();
    const hasFileCalls: string[] = [];
    const source = {
      ...fixtureSource([page(files)], BUFFERS, undefined, [page(summaryFiles)]),
      cacheScope: "summary-warm-memory-cache",
      hasFile: async (key: string) => {
        hasFileCalls.push(key);
        return fetchCounts.has(key);
      },
      fetchFile: async (key: string) => {
        const prev = fetchCounts.get(key) ?? 0;
        fetchCounts.set(key, prev + 1);
        return { bytes: BUFFERS[key], cached: prev > 0 };
      },
      openParquet: async (file: ManifestFile) => ({
        kind: "buffer" as const,
        bytes: BUFFERS[file.key].slice(0),
        cached: true,
      }),
    };
    const options = {
      from,
      to,
      wallNow: () => Date.parse(to),
      manifest: { files, summaryFiles, complete: true },
    };
    const cold = await runDashboard(source, options);
    const summaryKeys = summaryFiles.map((f) => f.key);
    const coldSummaryFetches = summaryKeys.reduce(
      (sum, k) => sum + (fetchCounts.get(k) ?? 0),
      0,
    );
    expect(coldSummaryFetches).toBeGreaterThan(0);

    hasFileCalls.length = 0;
    const warm = await runDashboard(source, options);
    const warmSummaryFetches = summaryKeys.reduce(
      (sum, k) => sum + (fetchCounts.get(k) ?? 0),
      0,
    );
    expect(warmSummaryFetches).toBe(coldSummaryFetches);
    expect(hasFileCalls.filter((k) => summaryKeys.includes(k))).toEqual([]);
    expect(warm.totalRequests).toBe(cold.totalRequests);
    expect(warm.cards).toEqual(cold.cards);
    expect(warm.stats.fetched).toBe(0);
    expect(warm.stats.cached).toBe(warm.stats.listed);
  });

  it("restores each card field's raw type from the catalog", () => {
    const catalog = JSON.parse(
      readFileSync(
        path.join(
          __dirname,
          "../../../../compute/cloud/src/logging/catalog.json",
        ),
        "utf8",
      ),
    ) as { fields: { id: string; duckdb_type: string }[] };
    const types = new Map(catalog.fields.map((f) => [f.id, f.duckdb_type]));
    for (const field of CARD_FIELDS) {
      const type = types.get(field);
      const expected =
        type === "VARCHAR"
          ? "x"
          : type === "BOOLEAN"
            ? true
            : 12.5;
      const text =
        type === "BOOLEAN" ? "true" : type === "VARCHAR" ? "x" : "12.5";
      expect(summaryValue(field, text), field).toBe(expected);
    }
  });

  it("normalizes v2-compatible dashboard card values", () => {
    expect(summaryValue("host", "")).toBeNull();
    expect(summaryValue("io_input_format", "")).toBeNull();
    expect(normalizeDashboardValue("ttl", 3600.027)).toBe(3600);
    expect(normalizeDashboardValue("age", 0.5)).toBe(1);
    expect(normalizeDashboardValue("conn_requests", 1)).toBe("1");
    expect(normalizeDashboardValue("conn_requests", 2)).toBe("2–5");
    expect(normalizeDashboardValue("conn_requests", 6)).toBe("6–20");
    expect(normalizeDashboardValue("conn_requests", 21)).toBe("21+");
    expect(normalizeDashboardValue("conn_requests", 101)).toBe("21+");
  });

  it("fingerprints names like bundle.rs", () => {
    expect(fingerprint([])).toBe(BigInt("0xcbf29ce484222325"));
    const day = pick("day")[0].key;
    const hours = pick("hour", (e) => !e.early).map((f) =>
      f.key.slice(f.key.lastIndexOf("/") + 1),
    );
    expect(day).toContain(
      `-h${fingerprint(hours.sort()).toString(16).padStart(16, "0")}.parquet`,
    );
  });

  it("plans the day bundle, hour bundles with late windows, and raw boundaries", () => {
    const day = pick("day");
    const dayStart = Date.parse("2027-01-15T00:00:00Z");
    const all = planSummaries(RAW, day, dayStart, dayStart + 86_400_000);
    expect(all.summaries.map((s) => s.file.key)).toEqual([day[0].key]);
    expect(all.summaries[0].covers).toHaveLength(8);
    expect(all.raw).toEqual([]);

    // Hour 09's early bundle covers three windows; its late window is listed too.
    const early = pick("hour", (e) => e.start === HOUR_09 && Boolean(e.early));
    const late = pick("window", (e) => e.start === HOUR_09 + 2700);
    const hour = planSummaries(
      RAW,
      [...early, ...late],
      HOUR_09 * 1000,
      (HOUR_09 + 3600) * 1000,
    );
    expect(hour.summaries.map((s) => s.covers.length)).toEqual([1, 3]);
    expect(hour.raw.map((f) => f.key)).toEqual(
      RAW.filter((f) => Date.parse(f.start) < HOUR_09 * 1000).map((f) => f.key),
    );

    // Without its late window the early bundle proves nothing; all of hour 09 reads raw.
    const missing = planSummaries(
      RAW,
      early,
      HOUR_09 * 1000,
      (HOUR_09 + 3600) * 1000,
    );
    expect(missing.summaries).toEqual([]);

    // Compacted hours (including multi-pass compacts) match their hour and day bundles by row count.
    const compact08: ManifestFile = {
      key: "svc/processed/request/year=2027/month=01/day=15/hour=08/catalog1/67833a44755110bf1c8a9d63cf1b51f33610f7fb58e7657d76f83685ce42fc01/compact=1800000000-1800002700-c2-n800-027f2a0d05906a11.parquet",
      start: "2027-01-15T08:00:00Z",
      end: "2027-01-15T08:47:00Z",
      rows: 800,
      size: 4096,
      sources: 2,
    };
    const rawWithCompact08 = [
      compact08,
      ...RAW.filter((f) => Date.parse(f.start) >= HOUR_09 * 1000),
    ];
    const hour08Bundle = pick("hour", (e) => e.start < HOUR_09);
    const plannedHour08 = planSummaries(
      rawWithCompact08,
      hour08Bundle,
      dayStart,
      dayStart + 86_400_000,
    );
    expect(plannedHour08.summaries.map((s) => s.file.key)).toEqual([
      hour08Bundle[0].key,
    ]);
    expect(plannedHour08.summaries[0].covers).toEqual([compact08]);
    const mismatchedCompact08: ManifestFile = {
      ...compact08,
      key: compact08.key.replace("-n800-", "-n81-"),
      rows: 81,
    };
    expect(
      planSummaries(
        [mismatchedCompact08],
        hour08Bundle,
        dayStart,
        dayStart + 86_400_000,
      ).summaries,
    ).toEqual([]);
    const plannedDayWithCompact = planSummaries(
      rawWithCompact08,
      day,
      dayStart,
      dayStart + 86_400_000,
    );
    expect(plannedDayWithCompact.summaries.map((s) => s.file.key)).toEqual([
      day[0].key,
    ]);
    expect(plannedDayWithCompact.raw).toEqual([]);
  });

  it("plan uses the open bundle and only later windows", () => {
    const windows09 = pick("window", (e) => e.start >= HOUR_09).sort(
      (a, b) => Date.parse(a.start) - Date.parse(b.start),
    );
    expect(windows09).toHaveLength(4);
    const covered = windows09.slice(0, 3);
    const later = windows09.slice(3);
    const endMs = Date.parse(covered[covered.length - 1].end);
    const rows = covered.reduce((sum, w) => sum + w.rows, 0);
    const fp = fingerprint(
      covered.map((w) => w.key.slice(w.key.lastIndexOf("/") + 1)).sort(),
    )
      .toString(16)
      .padStart(16, "0");
    const openKey = `processed/summary_bundle/request/year=2027/month=01/day=15/open_bundle=09-${endMs}-c${covered.length}-n${rows}-h${fp}.parquet`;
    const openFile: ManifestFile = {
      key: openKey,
      start: covered[0].start,
      end: covered[covered.length - 1].end,
      size: 1024,
      rows,
      kind: "hour",
      sources: covered.length,
    };
    const plan = planSummaries(
      RAW,
      [openFile, ...windows09],
      HOUR_09 * 1000,
      (HOUR_09 + 3600) * 1000,
    );
    expect(plan.summaries.map((s) => s.file.key)).toEqual([
      openKey,
      ...later.map((w) => w.key),
    ]);
    expect(plan.summaries.map((s) => s.covers.length)).toEqual([3, 1]);
    expect(
      plan.raw.filter((f) => Date.parse(f.start) >= HOUR_09 * 1000),
    ).toEqual([]);
  });

  it("plan uses an open bundle covering a set while a late window it does not cover is listed", () => {
    const windows09 = pick("window", (e) => e.start >= HOUR_09).sort(
      (a, b) => Date.parse(a.start) - Date.parse(b.start),
    );
    const covered = [windows09[0], windows09[2]];
    const listed = [windows09[1], windows09[3]];
    const rows = covered.reduce((sum, w) => sum + w.rows, 0);
    const fp = fingerprint(
      covered.map((w) => w.key.slice(w.key.lastIndexOf("/") + 1)).sort(),
    )
      .toString(16)
      .padStart(16, "0");
    const openKey = `processed/summary_bundle/request/year=2027/month=01/day=15/open_bundle=09-${Date.parse(windows09[2].end)}-c2-n${rows}-h${fp}.parquet`;
    const openFile: ManifestFile = {
      key: openKey,
      start: covered[0].start,
      end: covered[1].end,
      size: 1024,
      rows,
      kind: "hour",
      sources: 2,
    };
    const from = HOUR_09 * 1000;
    const to = (HOUR_09 + 3600) * 1000;
    for (const plan of [
      planSummaries(RAW, [openFile, ...listed], from, to),
      planCoarsestFirst(RAW, [openFile, ...listed], from, to),
    ]) {
      expect(
        plan.summaries.map((s) => [s.file.key, s.covers.length]).sort(),
      ).toEqual(
        [[openKey, 2], ...listed.map((w) => [w.key, 1])].sort(),
      );
      expect(plan.raw.filter((f) => Date.parse(f.start) >= from)).toEqual([]);
    }
    // A window neither listed nor in the bundle's set keeps the bundle out.
    const missing = planSummaries(RAW, [openFile, windows09[3]], from, to);
    expect(missing.summaries.map((s) => s.file.key)).toEqual([windows09[3].key]);
    expect(
      missing.raw.filter((f) => Date.parse(f.start) >= from),
    ).toHaveLength(3);
  });

  it("fingerprint mismatch falls back to windows", () => {
    const windows09 = pick("window", (e) => e.start >= HOUR_09).sort(
      (a, b) => Date.parse(a.start) - Date.parse(b.start),
    );
    const covered = windows09.slice(0, 3);
    const endMs = Date.parse(covered[covered.length - 1].end);
    const rows = covered.reduce((sum, w) => sum + w.rows, 0);
    const openKey = `processed/summary_bundle/request/year=2027/month=01/day=15/open_bundle=09-${endMs}-c${covered.length}-n${rows}-h0000000000000000.parquet`;
    const badOpenFile: ManifestFile = {
      key: openKey,
      start: covered[0].start,
      end: covered[covered.length - 1].end,
      size: 1024,
      rows,
      kind: "hour",
      sources: covered.length,
    };
    const plan = planSummaries(
      RAW,
      [badOpenFile, ...windows09],
      HOUR_09 * 1000,
      (HOUR_09 + 3600) * 1000,
    );
    expect(plan.summaries.map((s) => s.file.key)).toEqual(
      windows09.map((w) => w.key),
    );
    expect(
      plan.raw.filter((f) => Date.parse(f.start) >= HOUR_09 * 1000),
    ).toEqual([]);
  });

  it("merges the Rust IP sketches into what TS builds from the raw IPs", async () => {
    const day = await decodeSummary(BUFFERS[pick("day")[0].key], 0, 4e12);
    const hll = new Hll();
    for (const file of RAW) {
      const { columns } = await readColumns(BUFFERS[file.key], [
        "ip",
        "timestamp",
      ]);
      const timestamps = Array.from(columns.get("timestamp") ?? []);
      // A row without a timestamp is not a request, so its IP is not sketched.
      Array.from(columns.get("ip") ?? []).forEach((ip, i) => {
        if (typeof ip === "string" && timestamps[i] != null) hll.add(ip);
      });
    }
    expect(day?.ipSketch?.toBytes()).toEqual(hll.toBytes());
  });

  it("decodes typed custom fields and rejects a summary without one", async () => {
    const t = Date.parse("2027-01-15T08:00:00Z");
    const rows: [string, string | null, string | null, number][] = [
      ["time_series", null, null, 6],
      ["field_totals", "tenant", null, 6],
      ["all_fields", "tenant", "acme", 4],
      ["all_fields", "tenant", "beta", 2],
      ["field_totals", "score", null, 6],
      ["all_fields", "score", "1", 6],
      ["field_totals", "flag", null, 2],
      ["all_fields", "flag", "true", 2],
      ["field_totals", "cmcd_br", null, 6],
      ["all_fields", "cmcd_br", "3000", 6],
    ];
    const write = (version: string) =>
      parquetWriteBuffer({
        columnData: [
          { name: "bundle", type: "STRING", data: rows.map((r) => r[0]) },
          {
            name: "bucket",
            type: "TIMESTAMP",
            data: rows.map(() => new Date(t)),
          },
          { name: "field", type: "STRING", data: rows.map((r) => r[1]) },
          { name: "value", type: "STRING", data: rows.map((r) => r[2]) },
          {
            name: "count",
            type: "INT64",
            data: rows.map((r) =>
              r[0] === "time_series" ? null : BigInt(r[3]),
            ),
          },
          {
            name: "requests",
            type: "INT64",
            data: rows.map((r) =>
              r[0] === "time_series" ? BigInt(r[3]) : null,
            ),
          },
        ],
        kvMetadata: [{ key: "flp_summary", value: version }],
      });
    const summary = write("3");
    const custom = [
      { id: "tenant", type: "string" },
      { id: "score", type: "number" },
      { id: "flag", type: "boolean" },
    ] as const;
    const decoded = await decodeSummary(summary, t, t + 3_600_000, custom);
    expect(decoded?.rollup.counts.tenant).toEqual([
      ["acme", 4],
      ["beta", 2],
    ]);
    expect(decoded?.rollup.counts.score).toEqual([[1, 6]]);
    expect(decoded?.rollup.counts.flag).toEqual([[true, 2]]);
    expect(decoded?.rollup.nonNull).toMatchObject({ tenant: 6, flag: 2 });
    expect(decoded?.rollup.counts.cmcd_br).toBeUndefined();
    expect(
      (await decodeSummary(summary, t, t + 3_600_000))?.rollup.counts.tenant,
    ).toBeUndefined();
    // A summary written before the field existed would undercount it.
    expect(
      await decodeSummary(summary, t, t + 3_600_000, [
        ...custom,
        { id: "region_tag", type: "string" },
      ]),
    ).toBeNull();
    // A v2 summary falls back to raw.
    expect(
      await decodeSummary(write("2"), t, t + 3_600_000, custom),
    ).toBeNull();
  });

  it("rejects a summary of another version or reaching outside the range", async () => {
    const key = pick("hour", (e) => e.start === HOUR_09 && !e.early)[0].key;
    const decoded = await decodeSummary(
      BUFFERS[key],
      HOUR_09 * 1000,
      (HOUR_09 + 3600) * 1000,
    );
    // Four of its 800 rows have a null timestamp, which is not a request.
    expect(decoded?.rollup.totalRequests).toBe(796);
    expect(decoded?.ipSketch).toBeInstanceOf(Hll);
    expect(
      await decodeSummary(
        BUFFERS[key],
        HOUR_09 * 1000,
        (HOUR_09 + 1800) * 1000,
      ),
    ).toBeNull();
    expect(
      await decodeSummary(BUFFERS[RAW[0].key], 0, Number.MAX_SAFE_INTEGER),
    ).toBeNull();
  });

  it("answers a whole day from its bundle as raw Parquet would", async () => {
    const from = "2027-01-15T00:00:00Z";
    const to = "2027-01-16T00:00:00Z";
    const raw = await run(from, to, RAW, []);
    const summary = await run(from, to, RAW, pick("day"));
    expect(summary.files).toEqual(pick("day").map((f) => f.key));
    expect(summary.result.stats.listed).toBe(1);
    expect(summary.result.stats.rows).toBe(raw.result.stats.rows);
    const approximate = expectMatchesRaw(summary.result, raw.result);
    expect(approximate).toContain("url");
    expect(approximate.length).toBeLessThan(CARD_FIELDS.length / 2);
  });

  it("answers closed hours from hour bundles and late window summaries", async () => {
    const from = "2027-01-15T08:00:00Z";
    const to = "2027-01-15T10:00:00Z";
    const raw = await run(from, to, RAW, []);
    const hours = await run(
      from,
      to,
      RAW,
      pick("hour", (e) => !e.early),
    );
    expect(hours.files).toHaveLength(2);
    expect(expectMatchesRaw(hours.result, raw.result)).toEqual(["ip"]);

    const lateWindow = pick("window", (e) => e.start === HOUR_09 + 2700);
    const early = await run(from, to, RAW, [
      ...pick("hour", (e) => e.start < HOUR_09 && !e.early),
      ...pick("hour", (e) => e.start === HOUR_09 && Boolean(e.early)),
      ...lateWindow,
    ]);
    expect(early.files).toHaveLength(3);
    expectMatchesRaw(early.result, raw.result);
  });

  it("reads raw Parquet for partial windows and exactly matches window summaries", async () => {
    const from = "2027-01-15T08:07:00Z";
    const to = "2027-01-15T09:30:00Z";
    const raw = await run(from, to, RAW, []);
    const mixed = await run(
      from,
      to,
      RAW,
      pick(
        "window",
        (e) => e.start >= 1_800_000_900 && e.end <= HOUR_09 + 1800,
      ),
    );
    expect(mixed.files.filter((k) => k.includes("/window="))).toEqual([
      RAW[0].key,
    ]);
    expect(expectMatchesRaw(mixed.result, raw.result)).toEqual([]);
  });

  it("reads raw Parquet for an hour whose bundle misses a window", async () => {
    const from = "2027-01-15T08:00:00Z";
    const to = "2027-01-15T10:00:00Z";
    const extraKey =
      "processed/request/year=2027/month=01/day=15/hour=09/window=1800006300-1800007200-p2s0-n2.parquet";
    const extra = {
      [extraKey]: parquetFixture([
        {
          timestamp: (HOUR_09 + 3000) * 1000,
          status: 503,
          cache: "MISS",
          resp_bytes: 7,
          host: "late.example",
        },
        {
          timestamp: (HOUR_09 + 3001) * 1000,
          status: 200,
          cache: "HIT",
          resp_bytes: 9,
          host: "late.example",
        },
      ]),
    };
    const files = [
      ...RAW,
      {
        key: extraKey,
        start: iso(HOUR_09 + 2700),
        end: iso(HOUR_09 + 3600),
        size: 1,
        rows: 2,
      },
    ];
    const raw = await run(from, to, files, [], extra);
    const summary = await run(
      from,
      to,
      files,
      pick("hour", (e) => !e.early),
      extra,
    );
    expect(
      summary.files.filter((k) => k.includes("hour=09/window=")),
    ).toHaveLength(5);
    expect(summary.result.cards.host.top[0]).toBeDefined();
    expectMatchesRaw(summary.result, raw.result);
  });

  it("falls back to raw Parquet when the summary listing fails", async () => {
    const from = "2027-01-15T08:00:00Z";
    const to = "2027-01-15T10:00:00Z";
    const raw = await run(from, to, RAW, []);
    const source = fixtureSource([page(RAW)], BUFFERS);
    const failing = {
      ...source,
      async fetchJson(url: string, signal?: AbortSignal) {
        if (url.includes("view=summary")) throw new Error("503");
        return withoutInlineSummaries(await source.fetchJson(url, signal));
      },
    };
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    try {
      const result = await runDashboard(failing, { from, to });
      expect(expectMatchesRaw(result, raw.result)).toEqual([]);
      expect(result.stats).toMatchObject({
        summaryListing: "failed",
        summaryFiles: 0,
      });
      expect(warn).toHaveBeenCalledWith(
        "flp: summary listing failed; reading raw Parquet",
        expect.any(Error),
      );
    } finally {
      warn.mockRestore();
    }
  });

  it("reports an incomplete summary listing and how many summaries a run used", async () => {
    const from = "2027-01-15T08:00:00Z";
    const to = "2027-01-15T10:00:00Z";
    const raw = await run(from, to, RAW, []);
    const incomplete = fixtureSource(
      [page(RAW)],
      BUFFERS,
      { json: [], files: [] },
      [
        {
          files: pick("hour", (e) => !e.early),
          next_cursor: null,
          complete: false,
        },
      ],
    );
    const legacy = {
      ...incomplete,
      async fetchJson(url: string, signal?: AbortSignal) {
        return withoutInlineSummaries(await incomplete.fetchJson(url, signal));
      },
    };
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    try {
      const result = await runDashboard(legacy, { from, to });
      expect(expectMatchesRaw(result, raw.result)).toEqual([]);
      expect(result.stats).toMatchObject({
        summaryListing: "incomplete",
        summaryFiles: 0,
      });
      expect(warn).toHaveBeenCalledWith(
        "flp: summary listing incomplete; reading raw Parquet",
      );
    } finally {
      warn.mockRestore();
    }
    const listed = await run(
      from,
      to,
      RAW,
      pick("hour", (e) => !e.early),
    );
    expect(listed.result.stats).toMatchObject({
      summaryListing: "inline",
      summaryFiles: 2,
    });
  });

  it("lists summaries when the raw listing omits them", async () => {
    const from = "2027-01-15T08:00:00Z";
    const to = "2027-01-15T10:00:00Z";
    const hours = pick("hour", (e) => !e.early);
    const source = fixtureSource(
      [page(RAW)],
      BUFFERS,
      { json: [], files: [] },
      [page(hours)],
    );
    const legacy = {
      ...source,
      async fetchJson(url: string, signal?: AbortSignal) {
        const body = await source.fetchJson(url, signal);
        return url.includes("view=summary") ? body : withoutInlineSummaries(body);
      },
    };
    const result = await runDashboard(legacy, { from, to });
    expect(source.log.summary).toHaveLength(1);
    expect(result.stats).toMatchObject({
      summaryListing: "listed",
      summaryFiles: 2,
    });
    const listed = await run(from, to, RAW, hours);
    expect(source.log.files.toSorted()).toEqual(listed.files.toSorted());
  });

  it("takes summaries from the raw listing instead of a separate summary listing", async () => {
    const from = "2027-01-15T08:00:00Z";
    const to = "2027-01-15T10:00:00Z";
    const hours = pick("hour", (e) => !e.early);
    const listed = await run(from, to, RAW, hours);
    for (const search of [false, true]) {
      const source = fixtureSource(
        [page(RAW)],
        BUFFERS,
        { json: [], files: [] },
        [page(hours)],
      );
      if (search) {
        await searchFieldSummaries(source, { from, to, column: "host", query: "" });
      } else {
        const result = await runDashboard(source, { from, to });
        expect(result.stats).toMatchObject({ summaryListing: "inline", summaryFiles: 2 });
        expect(source.log.files.toSorted()).toEqual(listed.files.toSorted());
      }
      expect(source.log.summary).toBeUndefined();
      expect(source.log.json).toHaveLength(1);
      expect(new URL(source.log.json[0], "http://x").searchParams.get("summaries")).toBe("1");
    }
  });

  it("classifies summaries against filters and edgeOnly while respecting truncated fields", async () => {
    const dayKey = pick("day")[0].key;
    const h08Key = pick("hour", (e) => e.start < HOUR_09 && !e.early)[0].key;
    const h09Key = pick("hour", (e) => e.start === HOUR_09 && !e.early)[0].key;
    const day = (await decodeSummary(BUFFERS[dayKey], 0, 4e12))!;
    const h08 = (await decodeSummary(BUFFERS[h08Key], 0, 4e12))!;
    const h09 = (await decodeSummary(BUFFERS[h09Key], 0, 4e12))!;
    expect(day.truncated).toContain("url");
    expect(h08.truncated).toContain("ip");
    expect(h09.truncated).not.toContain("url");

    const f = (
      mode: DashboardFilter["mode"],
      field: string,
      value: string,
    ): DashboardFilter => ({ mode, field, value });

    // Non-truncated field with 0 matches -> "none"; 0 excluded matches -> "all".
    expect(
      classifySummaryAgainstFilters(h08, [f("include", "country", "FR")]),
    ).toBe("none");
    expect(
      classifySummaryAgainstFilters(h08, [f("exclude", "country", "FR")]),
    ).toBe("all");
    expect(
      classifySummaryAgainstFilters(h08, [f("include", "country", "US")]),
    ).toBe("partial");

    // In hour 09, `url` is NOT truncated and only has `/q/*` and `/hot/*` URLs.
    expect(
      classifySummaryAgainstFilters(h09, [f("include", "url", "/p/0")]),
    ).toBe("none");
    expect(
      classifySummaryAgainstFilters(h09, [f("exclude", "url", "/p/*")]),
    ).toBe("all");

    // In the day bundle, `url` IS truncated (`unattributed > 0`), so a value missing
    // from the kept top-K list must NOT be pruned as "none" or merged as "all".
    const keptDayUrls = new Set(day.rollup.counts.url.map(([v]) => String(v)));
    const droppedUrl = Array.from({ length: 620 }, (_, i) => `/p/${i}`).find(
      (u) => !keptDayUrls.has(u),
    )!;
    expect(droppedUrl).toBeDefined();
    expect(
      classifySummaryAgainstFilters(day, [f("include", "url", droppedUrl)]),
    ).toBe("partial");
    expect(
      classifySummaryAgainstFilters(day, [f("exclude", "url", droppedUrl)]),
    ).toBe("partial");

    // edgeOnly classification: mixed edge/non-edge is "partial", all-edge is "all", all-shield is "none".
    expect(classifySummaryAgainstFilters(h08, [], true)).toBe("partial");
    const allEdgeSummary = {
      ...h09,
      rollup: {
        ...h09.rollup,
        counts: {
          ...h09.rollup.counts,
          edge: [[true, h09.rollup.totalRequests] as [boolean, number]],
        },
        nonNull: { ...h09.rollup.nonNull, edge: h09.rollup.totalRequests },
      },
    };
    expect(classifySummaryAgainstFilters(allEdgeSummary, [], true)).toBe("all");
    const allShieldSummary = {
      ...h09,
      rollup: {
        ...h09.rollup,
        counts: {
          ...h09.rollup.counts,
          edge: [[false, h09.rollup.totalRequests] as [boolean, number]],
        },
        nonNull: { ...h09.rollup.nonNull, edge: h09.rollup.totalRequests },
      },
    };
    expect(classifySummaryAgainstFilters(allShieldSummary, [], true)).toBe(
      "none",
    );
  });

  it("reuses cached summaries from the prior load to prune 0%-match and merge 100%-match hours on filtered runs", async () => {
    const from = "2027-01-15T08:00:00Z";
    const to = "2027-01-15T10:00:00Z";
    const hourBundles = pick("hour", (e) => !e.early);
    const source = fixtureSource(
      [page(RAW)],
      BUFFERS,
      { json: [], files: [] },
      [page(hourBundles)],
    );

    // 1. Initial unfiltered load fetches only the two hour bundles.
    const initial = await runDashboard(source, { from, to });
    expect(initial.stats).toMatchObject({ listed: 2, fetched: 2, cached: 0 });
    expect(source.log.files).toHaveLength(2);

    const runRawFiltered = async (filters: DashboardFilter[]) => {
      const rawSource = fixtureSource([page(RAW)], BUFFERS);
      return runDashboard(rawSource, { from, to, filters });
    };

    // 2. 0%-match filter across both hours: prunes both cached hour bundles with 0 raw fetches.
    const noneFilters: DashboardFilter[] = [
      { mode: "include", field: "country", value: "FR" },
    ];
    const noneResult = await runDashboard(source, {
      from,
      to,
      filters: noneFilters,
    });
    expect(noneResult.stats).toMatchObject({
      listed: 2,
      fetched: 0,
      cached: 2,
    });
    expect(source.log.files).toHaveLength(2);
    expectMatchesRaw(noneResult, await runRawFiltered(noneFilters));

    // 3. 100%-match filter across both hours: merges both cached hour bundles with 0 raw fetches.
    const allFilters: DashboardFilter[] = [
      { mode: "exclude", field: "country", value: "FR" },
    ];
    const allResult = await runDashboard(source, {
      from,
      to,
      filters: allFilters,
    });
    expect(allResult.stats).toMatchObject({ listed: 2, fetched: 0, cached: 2 });
    expect(source.log.files).toHaveLength(2);
    expectMatchesRaw(allResult, await runRawFiltered(allFilters));

    // 4. Filter that matches 100% of hour 09 ("all") and partial in hour 08:
    // merges hour 09 from its cached bundle and fetches only hour 08's 4 raw windows.
    const excludePFilters: DashboardFilter[] = [
      { mode: "exclude", field: "url", value: "/p/*" },
    ];
    const excludePResult = await runDashboard(source, {
      from,
      to,
      filters: excludePFilters,
    });
    expect(excludePResult.stats).toMatchObject({
      listed: 5,
      fetched: 4,
      cached: 1,
    });
    expect(
      source.log.files.slice(2).every((k) => k.includes("hour=08/window=")),
    ).toBe(true);
    expectMatchesRaw(excludePResult, await runRawFiltered(excludePFilters));

    // 5. Subsequent filter on hour 08 (`/p/0`) prunes hour 09 from its cached bundle ("none")
    // and answers hour 08 from its 4 raw windows already ingested in DuckDB (0 new fetches).
    const includeP0Filters: DashboardFilter[] = [
      { mode: "include", field: "url", value: "/p/0" },
    ];
    const includeP0Result = await runDashboard(source, {
      from,
      to,
      filters: includeP0Filters,
    });
    expect(includeP0Result.totalRequests).toBeGreaterThan(0);
    expect(includeP0Result.stats).toMatchObject({
      listed: 5,
      fetched: 0,
      cached: 5,
    });
    expect(source.log.files).toHaveLength(6);
    expectMatchesRaw(includeP0Result, await runRawFiltered(includeP0Filters));
  });

  it("falls back from a partial cached day bundle to cached hour bundles before fetching raw windows", async () => {
    const from = "2027-01-15T00:00:00Z";
    const to = "2027-01-16T00:00:00Z";
    const dayBundles = pick("day");
    const hourBundles = pick("hour", (e) => !e.early);
    const day = (await decodeSummary(BUFFERS[dayBundles[0].key], 0, 4e12))!;
    const h08 = (await decodeSummary(
      BUFFERS[hourBundles.find((f) => f.key.includes("hour_bundle=08"))!.key],
      0,
      4e12,
    ))!;
    const keptDayUrls = new Set(day.rollup.counts.url.map(([v]) => String(v)));
    const droppedFromDayUrl = h08.rollup.counts.url
      .map(([v]) => String(v))
      .find((u) => u.startsWith("/p/") && !keptDayUrls.has(u))!;
    expect(droppedFromDayUrl).toBeDefined();

    const source = fixtureSource([page(RAW)], BUFFERS);

    // Warm the hour bundles (2h range) and the day bundle (24h range).
    await runDashboard(source, {
      from: "2027-01-15T08:00:00Z",
      to: "2027-01-15T10:00:00Z",
      manifest: { complete: true, files: RAW, summaryFiles: hourBundles },
    });
    await runDashboard(source, {
      from,
      to,
      manifest: { complete: true, files: RAW, summaryFiles: dayBundles },
    });
    const filesBeforeFilter = source.log.files.length;
    expect(filesBeforeFilter).toBe(3);

    // Filter on 24h range for a `/p/*` URL truncated from `day_bundle` (`unattributed > 0`):
    // - `day_bundle` must NOT be falsely pruned as "none"; it classifies as "partial"
    // - falls back to `hour_bundle=08` ("partial") and `hour_bundle=09` ("none", pruned!)
    // - fetches only hour 08's 4 raw windows.
    const filters: DashboardFilter[] = [
      { mode: "include", field: "url", value: droppedFromDayUrl },
    ];
    const filtered = await runDashboard(source, {
      from,
      to,
      filters,
      manifest: { complete: true, files: RAW, summaryFiles: dayBundles },
    });
    expect(filtered.totalRequests).toBeGreaterThan(0);
    const newlyFetched = source.log.files.slice(filesBeforeFilter);
    expect(newlyFetched).toHaveLength(4);
    expect(newlyFetched.every((k) => k.includes("hour=08/window="))).toBe(true);

    const rawSource = fixtureSource([page(RAW)], BUFFERS);
    const rawFiltered = await runDashboard(rawSource, { from, to, filters });
    expectMatchesRaw(filtered, rawFiltered);
  });
});

describe("pivot slices", () => {
  afterEach(() => clearDashboardMemo());
  const from = "2027-01-15T08:00:00Z";
  const to = "2027-01-15T10:00:00Z";
  const rawFiltered = (filters: DashboardFilter[], edgeOnly = false) =>
    runDashboard(fixtureSource([page(RAW)], BUFFERS), {
      from,
      to,
      filters,
      edgeOnly,
    });
  const coldFiltered = async (
    summaries: ManifestFile[],
    filters: DashboardFilter[],
    edgeOnly = false,
    range = { from, to },
  ) => {
    const source = fixtureSource(
      [page(RAW)],
      BUFFERS,
      { json: [], files: [] },
      [page(summaries)],
    );
    const result = await runDashboard(source, { ...range, filters, edgeOnly });
    return { result, files: source.log.files };
  };
  const status = (value: string, mode: DashboardFilter["mode"] = "include") =>
    ({ field: "status", value, mode }) as DashboardFilter;

  it("splits every summary's time-series rows across its status slices", async () => {
    for (const file of INDEX.filter((e) => e.kind !== "raw").map(entry)) {
      const decoded = (await decodeSummary(BUFFERS[file.key], 0, 4e12, [], {
        slices: true,
      }))!;
      const pivot = decoded.slices?.get("status");
      expect(pivot, file.key).toBeDefined();
      expect(pivot!.truncated).toBe(false);
      const slices = [...pivot!.values.values()];
      expect(
        slices.reduce((sum, s) => sum + s.rollup.totalRequests, 0),
        file.key,
      ).toBe(decoded.rollup.totalRequests);
      expect(
        (await decodeSummary(BUFFERS[file.key], 0, 4e12))!.slices,
      ).toBeUndefined();
    }
  });

  it.each([
    ["one value", [status("200")]],
    ["several values", [status("200"), status("304")]],
    ["a wildcard", [status("5*")]],
    ["an exclude", [status("200", "exclude")]],
    ["include and exclude", [status("*"), status("404", "exclude")]],
    ["no value present", [status("418")]],
  ])(
    "answers a status filter with %s from cold hour bundles without raw reads",
    async (_, filters) => {
      const hours = pick("hour", (e) => !e.early);
      const { result, files } = await coldFiltered(hours, filters);
      expect(files.sort()).toEqual(hours.map((f) => f.key).sort());
      expect(result.stats).toMatchObject({ listed: 2, fetched: 2, cached: 0 });
      const raw = await rawFiltered(filters);
      expectMatchesRaw(result, raw);
      expect(result.cards.ip.distinct).toBeGreaterThanOrEqual(
        filters[0].value === "418" ? 0 : 1,
      );
    },
  );

  it("answers from windows and from the day bundle as from raw", async () => {
    const filters = [status("200"), status("503")];
    const windows = pick("window");
    const fromWindows = await coldFiltered(windows, filters);
    expect(fromWindows.files.every((k) => k.includes("/summary="))).toBe(true);
    // Windows are summarized whole, so even approximate cards are exact.
    expect(expectMatchesRaw(fromWindows.result, await rawFiltered(filters))).toEqual(
      [],
    );
    const range = { from: "2027-01-15T00:00:00Z", to: "2027-01-16T00:00:00Z" };
    const day = await coldFiltered(pick("day"), filters, false, range);
    expect(day.files).toEqual(pick("day").map((f) => f.key));
    expectMatchesRaw(
      day.result,
      await runDashboard(fixtureSource([page(RAW)], BUFFERS), {
        ...range,
        filters,
      }),
    );
  });

  it("narrows by other filters within a slice, and reads raw when the slice only partly matches them", async () => {
    const hours = pick("hour", (e) => !e.early);
    // Every 404 row is `GET`, so the slice matches the method filter whole.
    const whole = [status("404"), { field: "method", value: "GET", mode: "include" } as const];
    const sliced = await coldFiltered(hours, whole);
    expect(sliced.files.every((k) => k.includes("hour_bundle="))).toBe(true);
    expectMatchesRaw(sliced.result, await rawFiltered(whole));
    const none = [status("404"), { field: "method", value: "POST", mode: "include" } as const];
    const pruned = await coldFiltered(hours, none);
    expect(pruned.result.totalRequests).toBe(0);
    expect(pruned.files.every((k) => k.includes("hour_bundle="))).toBe(true);
    const partial = [status("200"), { field: "country", value: "US", mode: "include" } as const];
    const mixed = await coldFiltered(hours, partial);
    expect(mixed.files.some((k) => k.includes("/window="))).toBe(true);
    expectMatchesRaw(mixed.result, await rawFiltered(partial));
    const edge = await coldFiltered(hours, [status("200")], true);
    expectMatchesRaw(edge.result, await rawFiltered([status("200")], true));
  });

  it("leaves cut pivots, and excludes over rows no slice holds, to the whole summary", async () => {
    const [hour] = pick("hour", (e) => !e.early);
    const decoded = (await decodeSummary(BUFFERS[hour.key], 0, 4e12, [], {
      slices: true,
    }))!;
    const pivot = decoded.slices!.get("status")!;
    const ok = resolveSummaryFromSlices(decoded, [status("200")]);
    expect(ok).not.toBeNull();
    expect(ok).not.toBe("none");
    const cut = { ...decoded, slices: new Map([["status", { ...pivot, truncated: true }]]) };
    expect(resolveSummaryFromSlices(cut, [status("200")])).toBeNull();
    // As if the 304 rows had no status: includes still answer, excludes keep them.
    const values = new Map(pivot.values);
    values.delete("304");
    const nulls = { ...decoded, slices: new Map([["status", { ...pivot, values }]]) };
    expect(resolveSummaryFromSlices(nulls, [status("200")])).toEqual(ok);
    expect(resolveSummaryFromSlices(nulls, [status("200", "exclude")])).toBeNull();
    expect(resolveSummaryFromSlices(decoded, [status("200", "exclude")])).not.toBeNull();
  });

  it("answers an open hour's compacts from the window summaries they name", async () => {
    // The ingest worker compacts an open hour's windows before its bundle catches up; their
    // summaries stay listed and the compact's name tags them.
    const windows09 = RAW.filter((f) => f.key.includes("/hour=09/"));
    const compacts = windows09.slice(0, 2).map((window): ManifestFile => {
      const name = /window=(\d+)-(\d+)-(.+)-n(\d+)\.parquet$/.exec(window.key)!;
      const tag = fnv1a32(`window=${name[1]}-${name[2]}-${name[3]}-n${name[4]}.parquet`)
        .toString(16)
        .padStart(8, "0");
      return {
        ...window,
        key: window.key.replace(/window=.*$/, `compact=${name[1]}-${name[2]}-c1-n${name[4]}-${tag}.parquet`),
        sources: 1,
      };
    });
    const raw = [...RAW.filter((f) => !windows09.slice(0, 2).includes(f)), ...compacts];
    const buffers = { ...BUFFERS };
    compacts.forEach((compact, i) => (buffers[compact.key] = BUFFERS[windows09[i].key]));
    const summaries = [
      ...pick("hour", (e) => !e.early && e.key.includes("hour_bundle=08")),
      ...pick("window", (e) => e.key.includes("/hour=09/")),
    ];
    const filters = [status("200"), status("304")];
    const source = fixtureSource([page(raw)], buffers, { json: [], files: [] }, [page(summaries)]);
    const result = await runDashboard(source, { from, to, filters });
    expect(source.log.files.filter((k) => !k.includes("summary"))).toEqual([]);
    expectMatchesRaw(result, await rawFiltered(filters));

    // One compact of two windows, one of whose summaries predates slices: the compact reads
    // raw and neither summary counts its rows.
    const pair = windows09.slice(0, 2);
    const names = pair.map((w) => /window=(.+)\.parquet$/.exec(w.key)![1]);
    const tags = names.map((n) => fnv1a32(`window=${n}.parquet`).toString(16).padStart(8, "0"));
    const compact: ManifestFile = {
      ...pair[0],
      key: pair[0].key.replace(/window=.*$/, `compact=1800003600-1800005400-c2-n${pair[0].rows! + pair[1].rows!}-${tags.join("")}.parquet`),
      end: pair[1].end,
      rows: pair[0].rows! + pair[1].rows!,
      sources: 2,
    };
    let second: Record<string, unknown>[] = [];
    await editSummaryRows(BUFFERS[pair[1].key], (rows) => (second = rows));
    const mixed = { ...BUFFERS, [compact.key]: await editSummaryRows(BUFFERS[pair[0].key], (rows) => [...rows, ...second]) };
    const [, unsliced] = summaries.filter((f) => f.key.includes("/hour=09/"));
    mixed[unsliced.key] = await editSummaryRows(BUFFERS[unsliced.key], (rows) =>
      rows.filter((row) => !SLICE_GROUPS.includes(String(row.bundle))),
    );
    const mixedSource = fixtureSource(
      [page([...RAW.filter((f) => !pair.includes(f)), compact])],
      mixed,
      { json: [], files: [] },
      [page(summaries.map((f) => (f.key === unsliced.key ? { ...f, size: mixed[f.key].byteLength } : f)))],
    );
    const fallback = await runDashboard(mixedSource, { from, to, filters });
    expect(mixedSource.log.files.filter((k) => !k.includes("summary"))).toEqual([compact.key]);
    expectMatchesRaw(fallback, await rawFiltered(filters));
  });

  it("answers an open hour's compacts from their window summaries unfiltered", async () => {
    // The open bundle lags the compacts, so only the window summaries the compact names hold
    // their rows; a group with an unreadable summary reads its compact raw and counts nothing else.
    const pair = RAW.filter((f) => f.key.includes("/hour=09/")).slice(0, 2);
    const names = pair.map((w) => /window=(.+)\.parquet$/.exec(w.key)![1]);
    const tags = names.map((n) => fnv1a32(`window=${n}.parquet`).toString(16).padStart(8, "0"));
    const compact: ManifestFile = {
      ...pair[0],
      key: pair[0].key.replace(/window=.*$/, `compact=1800003600-1800005400-c2-n${pair[0].rows! + pair[1].rows!}-${tags.join("")}.parquet`),
      end: pair[1].end,
      rows: pair[0].rows! + pair[1].rows!,
      sources: 2,
    };
    let second: Record<string, unknown>[] = [];
    await editSummaryRows(BUFFERS[pair[1].key], (rows) => (second = rows));
    const buffers = { ...BUFFERS, [compact.key]: await editSummaryRows(BUFFERS[pair[0].key], (rows) => [...rows, ...second]) };
    const raw = [...RAW.filter((f) => !pair.includes(f)), compact];
    const summaries = [
      ...pick("hour", (e) => !e.early && e.key.includes("hour_bundle=08")),
      ...pick("window", (e) => e.key.includes("/hour=09/")),
    ];
    const exact = await runDashboard(fixtureSource([page(raw)], buffers), { from, to });
    clearDashboardMemo();

    const source = fixtureSource([page(raw)], buffers, { json: [], files: [] }, [page(summaries)]);
    const cold = await runDashboard(source, { from, to });
    expect(source.log.files.filter((k) => !k.includes("summary"))).toEqual([]);
    expectMatchesRaw(cold, exact);
    const warm = await runDashboard(source, { from, to });
    expect(source.log.files.filter((k) => !k.includes("summary"))).toEqual([]);
    expectMatchesRaw(warm, exact);
    clearDashboardMemo();

    const [unreadable] = summaries.filter((f) => f.key.includes(names[1]));
    const broken = fixtureSource([page(raw)], { ...buffers, [unreadable.key]: new ArrayBuffer(8) }, { json: [], files: [] }, [page(summaries)]);
    const fallback = await runDashboard(broken, { from, to });
    expect(broken.log.files.filter((k) => !k.includes("summary"))).toEqual([compact.key]);
    expectMatchesRaw(fallback, exact);
  });

  it("fetches the summaries of a filtered run and of an open hour's compacts in parallel", async () => {
    // Under load an hour is many small window summaries; a serial fetch chain
    // costs a round trip each.
    const overlapping = <S extends { fetchFile: (key: string, ...a: never[]) => Promise<unknown> }>(
      source: S,
      watched: (key: string) => boolean = () => true,
    ) => {
      let inFlight = 0;
      const seen = { peak: 0 };
      const fetchFile = source.fetchFile.bind(source);
      source.fetchFile = (async (key: string, ...args: never[]) => {
        if (!watched(key)) return fetchFile(key, ...args);
        seen.peak = Math.max(seen.peak, ++inFlight);
        await new Promise((resolve) => setTimeout(resolve, 5));
        try {
          return await fetchFile(key, ...args);
        } finally {
          inFlight--;
        }
      }) as S["fetchFile"];
      return seen;
    };
    const filters = [status("200"), status("503")];
    const windows = pick("window");
    const filteredSource = fixtureSource([page(RAW)], BUFFERS, { json: [], files: [] }, [page(windows)]);
    const filtered = overlapping(filteredSource);
    const fromWindows = await runDashboard(filteredSource, { from, to, filters });
    expect(filteredSource.log.files.every((k) => k.includes("/summary="))).toBe(true);
    expect(filtered.peak).toBeGreaterThan(1);
    expectMatchesRaw(fromWindows, await rawFiltered(filters));
    clearDashboardMemo();

    const pair = RAW.filter((f) => f.key.includes("/hour=09/")).slice(0, 2);
    const names = pair.map((w) => /window=(.+)\.parquet$/.exec(w.key)![1]);
    const tags = names.map((n) => fnv1a32(`window=${n}.parquet`).toString(16).padStart(8, "0"));
    const compact: ManifestFile = {
      ...pair[0],
      key: pair[0].key.replace(/window=.*$/, `compact=1800003600-1800005400-c2-n${pair[0].rows! + pair[1].rows!}-${tags.join("")}.parquet`),
      end: pair[1].end,
      rows: pair[0].rows! + pair[1].rows!,
      sources: 2,
    };
    let second: Record<string, unknown>[] = [];
    await editSummaryRows(BUFFERS[pair[1].key], (rows) => (second = rows));
    const buffers = { ...BUFFERS, [compact.key]: await editSummaryRows(BUFFERS[pair[0].key], (rows) => [...rows, ...second]) };
    const raw = [...RAW.filter((f) => !pair.includes(f)), compact];
    const summaries = pick("window", (e) => e.key.includes("/hour=09/"));
    const exact = await runDashboard(fixtureSource([page(raw)], buffers), { from, to });
    clearDashboardMemo();
    const source = fixtureSource([page(raw)], buffers, { json: [], files: [] }, [page(summaries)]);
    // The compact's two summaries are read before any other file.
    const unfiltered = overlapping(source, (key) => names.some((n) => key.includes(`summary=${n}`)));
    const cold = await runDashboard(source, { from, to });
    expect(source.log.files.filter((k) => !k.includes("summary"))).not.toContain(compact.key);
    expect(unfiltered.peak).toBeGreaterThan(1);
    expectMatchesRaw(cold, exact);
  });

  it("reads raw for summaries written before slices", async () => {
    const hours = pick("hour", (e) => !e.early);
    const buffers = { ...BUFFERS };
    const old = await Promise.all(
      hours.map(async (file) => {
        buffers[file.key] = await editSummaryRows(BUFFERS[file.key], (rows) =>
          rows.filter((row) => !SLICE_GROUPS.includes(String(row.bundle))),
        );
        return { ...file, size: buffers[file.key].byteLength };
      }),
    );
    const source = fixtureSource([page(RAW)], buffers, { json: [], files: [] }, [
      page(old),
    ]);
    const filters = [status("200")];
    const result = await runDashboard(source, { from, to, filters });
    expect(source.log.files.filter((k) => k.includes("/window="))).toHaveLength(8);
    expectMatchesRaw(result, await rawFiltered(filters));
  });
});

describe("custom cards", () => {
  it("drops system-managed and load-test fields", () => {
    const ids = customCards(
      [
        "tenant",
        "cmcd_br",
        "edge_x",
        "rum_lcp",
        "lt_run_id",
        "lt_request_id",
        "fastly_req_id",
      ].map((id) => ({ id, label: id, type: "string" as const })),
    ).map((card) => card.id);
    expect(ids).toEqual(["tenant"]);
  });

  it("drops underscore fields, which summaries never carry", () => {
    // The dashboard sends the `_bot_name` card; one unsummarized field sends every summary to raw.
    const ids = customCards(
      ["tenant", "_bot_name", "_ngwaf_bot_name"].map((id) => ({
        id,
        label: id,
        type: "string" as const,
      })),
    ).map((card) => card.id);
    expect(ids).toEqual(["tenant"]);
  });
});

describe("summary field search", () => {
  const search = async (
    from: string,
    to: string,
    summaries: ManifestFile[],
    column: "host" | "url",
    query = "",
    filters: DashboardFilter[] = [],
  ) => {
    const source = fixtureSource(
      [page(RAW)],
      BUFFERS,
      { json: [], files: [] },
      [page(summaries)],
    );
    const fast = await searchFieldSummaries(source, {
      from,
      to,
      column,
      query,
      filters,
    });
    const exact = await searchFieldValues(fixtureSource([page(RAW)], BUFFERS), {
      from,
      to,
      column,
      query,
      filters,
    });
    return { fast, exact, files: source.log.files };
  };

  it("answers an untruncated field exactly from hour bundles without raw reads", async () => {
    const hours = pick("hour", (e) => !e.early);
    const { fast, exact, files } = await search(
      "2027-01-15T08:00:00Z",
      "2027-01-15T10:00:00Z",
      hours,
      "host",
      "EXAMPLE",
    );
    expect(exact.length).toBeGreaterThan(0);
    expect(fast).toEqual({ entries: exact, partial: false });
    expect(files).toEqual(hours.map((f) => f.key));
  });

  it("marks a field a bundle cut as partial with lower-bound counts", async () => {
    const { fast, exact, files } = await search(
      "2027-01-15T00:00:00Z",
      "2027-01-16T00:00:00Z",
      pick("day"),
      "url",
    );
    expect(fast.partial).toBe(true);
    expect(files).toEqual(pick("day").map((f) => f.key));
    const exactCounts = new Map(exact.map((e) => [e.value, e.count]));
    expect(fast.entries.length).toBeGreaterThan(0);
    for (const entry of fast.entries) {
      expect(entry.count).toBeLessThanOrEqual(
        exactCounts.get(entry.value) ?? 0,
      );
    }
  });

  it("marks windows without a summary or cached rollup as partial without reading them", async () => {
    const { fast, files } = await search(
      "2027-01-15T08:07:00Z",
      "2027-01-15T09:30:00Z",
      pick(
        "window",
        (e) => e.start >= 1_800_000_900 && e.end <= HOUR_09 + 1800,
      ),
      "host",
    );
    expect(fast.partial).toBe(true);
    expect(fast.entries.length).toBeGreaterThan(0);
    expect(files.some((k) => RAW.some((f) => f.key === k))).toBe(false);
  });

  it("leaves searches with other filters to the exact scan", async () => {
    const { fast, files } = await search(
      "2027-01-15T08:00:00Z",
      "2027-01-15T10:00:00Z",
      pick("hour", (e) => !e.early),
      "host",
      "",
      [{ mode: "include", field: "status", value: "200" }],
    );
    expect(fast).toEqual({ entries: [], partial: true });
    expect(files).toEqual([]);
  });
});

describe("projected dashboard summaries", () => {
  const LIMITS = { ...summaryProjectionLimits };
  // The fixtures are a few KiB and mostly footer, which a projection would otherwise read whole.
  beforeEach(() => Object.assign(summaryProjectionLimits, { wholeBelowBytes: 0, wholeAboveShare: Infinity }));
  afterEach(() => {
    Object.assign(summaryProjectionLimits, LIMITS);
    clearDashboardMemo();
  });

  const SUMMARIES = INDEX.filter((e) => e.kind !== "raw").map(entry);
  const BASE = new Set(["bundle", "bucket", "field", "value", "truncated"]);
  const projected = async (file: ManifestFile, bytes: ArrayBuffer, spec = DASHBOARD_SUMMARY_SPEC) => {
    const buffers = { [file.key]: bytes };
    const source = projectingSource(fixtureSource([], buffers), buffers);
    const read = await readSummaryProjection(source, { ...file, size: bytes.byteLength }, spec);
    expect(read.whole).toBe(false);
    expect(source.log.files).toEqual([]);
    return read.parts;
  };

  it("decodes every summary from its projection as from the whole file, today's layout or the announced one", async () => {
    for (const layout of [false, true]) {
      for (const file of SUMMARIES) {
        const bytes = layout ? await withSummaryLayout(BUFFERS[file.key]) : BUFFERS[file.key];
        const whole = await decodeSummary(bytes, 0, 4e12);
        expect(whole?.ipSketch, file.key).toBeTruthy();
        expect(await decodeSummary(await projected(file, bytes), 0, 4e12), file.key).toEqual(whole);
        const sliced = await decodeSummary(bytes, 0, 4e12, [], { slices: true });
        expect(sliced?.slices?.get("status")?.values.size, file.key).toBeGreaterThan(1);
        expect(
          await decodeSummary(await projected(file, bytes, DASHBOARD_FILTERED_SUMMARY_SPEC), 0, 4e12, [], { slices: true }),
          file.key,
        ).toEqual(sliced);
      }
    }
  });

  it("needs every group and column of its spec", async () => {
    const spec = DASHBOARD_SUMMARY_SPEC;
    // One row group per group, so a spec without a group skips its rows.
    const split = Object.fromEntries(await Promise.all(SUMMARIES.map(async (file) =>
      [file.key, await withSummaryLayout(BUFFERS[file.key], LAYOUT_B.flat().map((group) => [group]))] as const)));
    const only = Object.fromEntries([...spec.only].map(([column, of]) => [column, [...of]]));
    const reduced = [
      ...[...spec.groups].map((group) => [`group ${group}`, defineSummarySpec({
        groups: [...spec.groups].filter((g) => g !== group), columns: [...spec.columns], only,
      })] as const),
      ...[...spec.columns].filter((column) => !BASE.has(column)).map((column) => [`column ${column}`, defineSummarySpec({
        groups: [...spec.groups],
        columns: [...spec.columns].filter((c) => c !== column),
        only: Object.fromEntries(Object.entries(only).filter(([c]) => c !== column)),
      })] as const),
    ];
    const unchanged: string[] = [];
    for (const [name, without] of reduced) {
      let changed = false;
      for (const file of SUMMARIES) {
        const bytes = split[file.key];
        const got = await decodeSummary(await projected(file, bytes, without), 0, 4e12).catch(() => "threw");
        if (JSON.stringify(got, (_, v) => (v instanceof Hll ? [...v.toBytes()] : v))
          !== JSON.stringify(await decodeSummary(bytes, 0, 4e12), (_, v) => (v instanceof Hll ? [...v.toBytes()] : v))) {
          changed = true;
          break;
        }
      }
      if (!changed) unchanged.push(name);
    }
    expect(unchanged).toEqual([]);
  });

  it("reads under half the data bytes from a file announcing its layout as from today's", async () => {
    const [file] = pick("hour");
    const bytes = await withSummaryLayout([...pick("hour"), ...pick("day")].map((f) => BUFFERS[f.key]));
    const buffers = { [file.key]: bytes };
    const source = projectingSource(fixtureSource([], buffers), buffers);
    const legacy = await withSummaryLayout([...pick("hour"), ...pick("day")].map((f) => BUFFERS[f.key]), null);
    const legacyBuffers = { [file.key]: legacy };
    const legacySource = projectingSource(fixtureSource([], legacyBuffers), legacyBuffers);
    const sized = (data: ArrayBuffer) => ({ ...file, size: data.byteLength });
    const read = await readSummaryProjection(source, sized(bytes), DASHBOARD_SUMMARY_SPEC);
    const readLegacy = await readSummaryProjection(legacySource, sized(legacy), DASHBOARD_SUMMARY_SPEC);
    expect(await decodeSummary(read.parts, 0, 4e12)).toEqual(await decodeSummary(bytes, 0, 4e12));
    // A footer is read once and cached; one row group per layout entry makes it larger.
    const pastFooter = (downloaded: number, data: ArrayBuffer) => downloaded - parquetMetadata(data).metadata_length - 8;
    expect(pastFooter(read.downloadedBytes, bytes)).toBeLessThan(pastFooter(readLegacy.downloadedBytes, legacy) / 2);
  });

  it("answers the dashboard from projections, from their cache once the memo is gone, and filtered runs from them", async () => {
    const from = "2027-01-15T08:00:00Z";
    const to = "2027-01-15T10:00:00Z";
    const buffers = { ...BUFFERS };
    const hours = await Promise.all(pick("hour", (e) => !e.early).map(async (file) => {
      buffers[file.key] = await withSummaryLayout(BUFFERS[file.key]);
      return { ...file, size: buffers[file.key].byteLength };
    }));
    const whole = await runDashboard(fixtureSource([page(RAW)], buffers, { json: [], files: [] }, [page(hours)]), { from, to });
    clearDashboardMemo();
    const source = projectingSource(fixtureSource([page(RAW)], buffers, { json: [], files: [] }, [page(hours)]), buffers);
    const result = await runDashboard(source, { from, to });
    expect({ ...result, stats: undefined }).toEqual({ ...whole, stats: undefined });
    expect(result.stats).toMatchObject({ listed: 2, fetched: 2, cached: 0 });
    expect(source.log.files).toEqual([]);
    expect(new Set(source.ranges)).toEqual(new Set(hours.map((file) => file.key)));

    clearDashboardMemo();
    const ranges = source.ranges.length;
    const warm = await runDashboard(source, { from, to });
    expect({ ...warm, stats: undefined }).toEqual({ ...whole, stats: undefined });
    expect(warm.stats).toMatchObject({ listed: 2, fetched: 0, cached: 2 });
    expect(source.ranges).toHaveLength(ranges);

    const filters: DashboardFilter[] = [{ mode: "include", field: "country", value: "FR" }];
    const filtered = await runDashboard(source, { from, to, filters });
    expect(filtered.stats).toMatchObject({ listed: 2, fetched: 0, cached: 2 });
    expect(source.log.files).toEqual([]);
    expectMatchesRaw(filtered, await runDashboard(fixtureSource([page(RAW)], BUFFERS), { from, to, filters }));
  });

  it("reports no cached bytes for a cold filtered run that downloads projected summaries", async () => {
    const from = "2027-01-15T08:00:00Z";
    const to = "2027-01-15T10:00:00Z";
    // Hour 08 has no summaries, so its raw windows are fetched and progress is reported.
    const windows = pick("window", (e) => e.key.includes("/hour=09/"));
    const source = projectingSource(fixtureSource([page(RAW)], BUFFERS, { json: [], files: [] }, [page(windows)]), BUFFERS);
    const reports: { cachedBytes: number; downloadedBytes: number }[] = [];
    const filters: DashboardFilter[] = [{ mode: "include", field: "status", value: "200" }];
    const result = await runDashboard(source, { from, to, filters, onProgress: (p) => reports.push({ ...p }) });
    expect(result.stats).toMatchObject({ cached: 0 });
    expect(reports.length).toBeGreaterThan(0);
    for (const report of reports) {
      expect(report.cachedBytes).toBe(0);
      expect(report.downloadedBytes).toBeGreaterThan(0);
    }
  });

  it("covers open-hour compacts via open_bundle + window summaries even when an in-progress tail RAW_WINDOW crosses toMs", () => {
    const fromMs = Date.parse("2026-11-21T00:00:00Z");
    const toMs = Date.parse("2026-11-21T01:21:00Z");
    const dayDir = "year=2026/month=11/day=21/";
    const hourDir = `processed/request/${dayDir}hour=01/`;
    const sumDir = `processed/summary/request/${dayDir}hour=01/`;
    const bundleDir = `processed/summary_bundle/request/${dayDir}`;

    const compactA: ManifestFile = {
      key: `${hourDir}compact=1795222800-1795223880-c10-n1000-deadbeef.parquet`,
      start: "2026-11-21T01:00:00Z",
      end: "2026-11-21T01:18:00Z",
      size: 50_000,
      rows: 1000,
      sources: 10,
    };
    const tailLiveWindow: ManifestFile = {
      key: `${hourDir}window=1795224000-1795224120-aaa-n50.parquet`,
      start: "2026-11-21T01:20:00Z",
      end: "2026-11-21T01:22:00Z", // crosses toMs (01:21:00Z)
      size: 4_000,
      rows: 50,
    };
    const openBundle: ManifestFile = {
      key: `${bundleDir}open_bundle=01-1795223640-c8-n800-h0123456789abcdef.parquet`,
      start: "2026-11-21T01:00:00Z",
      end: "2026-11-21T01:14:00Z",
      size: 20_000,
      rows: 800,
      kind: "hour",
    };
    const compactedWin1: ManifestFile = {
      key: `${sumDir}summary=1795223640-1795223760-w1-n100.parquet`,
      start: "2026-11-21T01:14:00Z",
      end: "2026-11-21T01:16:00Z",
      size: 4_000,
      rows: 100,
      kind: "window",
    };
    const compactedWin2: ManifestFile = {
      key: `${sumDir}summary=1795223760-1795223880-w2-n100.parquet`,
      start: "2026-11-21T01:16:00Z",
      end: "2026-11-21T01:18:00Z",
      size: 4_000,
      rows: 100,
      kind: "window",
    };
    // Window summary corresponding to the still-live uncompacted tail window!
    const tailLiveWinSummary: ManifestFile = {
      key: `${sumDir}summary=1795224000-1795224120-aaa-n50.parquet`,
      start: "2026-11-21T01:20:00Z",
      end: "2026-11-21T01:22:00Z",
      size: 3_000,
      rows: 50,
      kind: "window",
    };

    const plan = planCompactsFromWindows(
      [compactA, tailLiveWindow],
      [openBundle, compactedWin1, compactedWin2, tailLiveWinSummary],
      fromMs,
      toMs,
    );
    expect(plan.raw.map((f) => f.key)).toEqual([tailLiveWindow.key]);
    expect(plan.summaries.map((s) => s.file.key).sort()).toEqual(
      [openBundle.key, compactedWin1.key, compactedWin2.key].sort(),
    );
  });

  it("answers an open hour's compacts and bundled windows from its open_bundle while late windows keep their own summaries", () => {
    // Live shape under ingest: the bundle holds the hour's compacts and two windows
    // without listed summaries; three later windows ending at the same slot have their own.
    const fromMs = Date.parse("2026-11-22T18:00:00Z");
    const toMs = Date.parse("2026-11-22T18:57:00Z");
    const hourDir = "processed/request/year=2026/month=11/day=22/hour=18/";
    const sumDir = "processed/summary/request/year=2026/month=11/day=22/hour=18/";
    const raw = (name: string, start: string, end: string, rows: number, sources?: number): ManifestFile => ({
      key: `${hourDir}${name}`, start: `2026-11-22T${start}:00Z`, end: `2026-11-22T${end}:00Z`, size: 10_000, rows,
      ...sources ? { sources } : {},
    });
    const compacts = [
      raw("compact=1795370400-1795371600-c32-n300-aa.parquet", "18:00", "18:20", 300, 32),
      raw("compact=1795371600-1795373040-c13-n200-bb.parquet", "18:20", "18:44", 200, 13),
    ];
    const bundled = [
      raw("window=1795373280-1795373400-a6s1-n30.parquet", "18:48", "18:50", 30),
      raw("window=1795373280-1795373400-a6s2-n20.parquet", "18:48", "18:50", 20),
    ];
    const late = [
      raw("window=1795373280-1795373400-b1s0-n1.parquet", "18:48", "18:50", 1),
      raw("window=1795373280-1795373400-c1s0-n41.parquet", "18:48", "18:50", 41),
    ];
    const openBundle: ManifestFile = {
      key: "processed/summary_bundle/request/year=2026/month=11/day=22/open_bundle=18-1795373400000-c47-n550-h0123456789abcdef.parquet",
      start: "2026-11-22T18:00:00Z", end: "2026-11-22T18:50:00Z", size: 20_000, rows: 550, kind: "hour",
    };
    const lateSummaries = late.map((file): ManifestFile => ({
      ...file, key: file.key.replace(hourDir, sumDir).replace("/window=", "/summary="), kind: "window",
    }));

    const plan = planSummaries([...compacts, ...bundled, ...late], [openBundle, ...lateSummaries], fromMs, toMs);

    expect(plan.raw).toEqual([]);
    expect(plan.summaries.map((use) => use.file.key).sort()).toEqual(
      [openBundle.key, ...lateSummaries.map((file) => file.key)].sort());
    expect(plan.summaries.find((use) => use.file === openBundle)?.covers.map((file) => file.key).sort())
      .toEqual([...compacts, ...bundled].map((file) => file.key).sort());

    // One more row than the bundle names: its holdings are not proven, so the hour reads raw.
    const short = planSummaries([...compacts, ...bundled, ...late], [
      { ...openBundle, key: openBundle.key.replace("-n550-", "-n549-") }, ...lateSummaries,
    ], fromMs, toMs);
    expect(short.raw.map((file) => file.key).sort()).toEqual([...compacts, ...bundled].map((file) => file.key).sort());
  });
});
