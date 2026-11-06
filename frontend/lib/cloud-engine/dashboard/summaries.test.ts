// @vitest-environment node
import { parquetWriteBuffer } from "hyparquet-writer";
import { readFileSync } from "node:fs";
import path from "node:path";
import { describe, expect, it } from "vitest";
import {
  CARD_FIELDS,
  decodeSummary,
  fingerprint,
  planSummaries,
  summaryValue,
} from "./summaries";
import {
  classifySummaryAgainstFilters,
  runDashboard,
  searchFieldSummaries,
  searchFieldValues,
  type DashboardResult,
} from "./engine";
import type { DashboardFilter } from "./filters";
import { Hll } from "./hll";
import type { ManifestFile } from "./manifest";
import { readColumns } from "./parquet";
import { fixtureSource, parquetFixture } from "./test-fixtures";

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

  it("restores each card field's raw type from the catalog", () => {
    const catalog = JSON.parse(
      readFileSync(
        path.join(__dirname, "../../../../compute/cloud/src/logging/catalog.json"),
        "utf8",
      ),
    ) as { fields: { id: string; duckdb_type: string }[] };
    const types = new Map(catalog.fields.map((f) => [f.id, f.duckdb_type]));
    for (const field of CARD_FIELDS) {
      const type = types.get(field);
      const expected =
        type === "VARCHAR" ? "x" : type === "BOOLEAN" ? true : 12.5;
      const text =
        type === "BOOLEAN" ? "true" : type === "VARCHAR" ? "x" : "12.5";
      expect(summaryValue(field, text), field).toBe(expected);
    }
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

  it("merges the Rust IP sketches into what TS builds from the raw IPs", async () => {
    const day = await decodeSummary(BUFFERS[pick("day")[0].key], 0, 4e12);
    const hll = new Hll();
    for (const file of RAW) {
      const { columns } = await readColumns(BUFFERS[file.key], ["ip"]);
      for (const ip of Array.from(columns.get("ip") ?? [])) {
        if (typeof ip === "string") hll.add(ip);
      }
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
    const summary = parquetWriteBuffer({
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
          data: rows.map((r) => (r[0] === "time_series" ? null : BigInt(r[3]))),
        },
        {
          name: "requests",
          type: "INT64",
          data: rows.map((r) => (r[0] === "time_series" ? BigInt(r[3]) : null)),
        },
      ],
      kvMetadata: [{ key: "flp_summary", value: "2" }],
    });
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
        return source.fetchJson(url, signal);
      },
    };
    const result = await runDashboard(failing, { from, to });
    expect(expectMatchesRaw(result, raw.result)).toEqual([]);
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
