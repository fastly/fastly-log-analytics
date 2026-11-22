// @vitest-environment node
import { describe, expect, it } from "vitest";
import { Histogram } from "./histogram";
import type { ManifestFile } from "./manifest";
import { readColumns } from "./parquet";
import { fnv1a32 } from "../parquet-key";
import {
  decodeRollup,
  mergeRollups,
  planRollup,
  queryRollup,
  rebucketSeries,
  type Rollup,
  type RollupAgg,
} from "./rollup-query";
import {
  FIXTURE_START,
  SUMMARY_FIXTURE_BUFFERS as BUFFERS,
  fixtureFiles,
  fixturePage,
} from "./summary-fixtures";
import { fixtureSource, projectingSource } from "./test-fixtures";
import { INSIGHTS_RESIDUAL } from "../insights-residual";

const HOUR = 3_600_000;
const T0 = FIXTURE_START * 1000;
const RAW = fixtureFiles("raw");
const WINDOWS = fixtureFiles("window");
const HOURS = fixtureFiles("hour", (e) => e.sources === 4);
const DAY = fixtureFiles("day");
const at = (file: ManifestFile) => Date.parse(file.start);

async function decodeAll(files: ManifestFile[], stepMs = 300_000) {
  const decoded = await Promise.all(
    files.map((file) => decodeRollup(BUFFERS[file.key], stepMs)),
  );
  return mergeRollups(decoded.map((d) => d!));
}

const dimCounts = (rollup: Rollup, dim: string) =>
  Object.fromEntries(
    [...(rollup.dims.get(dim) ?? new Map())].map(([value, agg]) => [
      value,
      [agg.requests, agg.status5xx, agg.respBytes, agg.rttSum, agg.rttCount],
    ]),
  );

const overall = (rollup: Rollup) =>
  [...(rollup.series.get("overall")?.get("")?.values() ?? [])].reduce(
    (sum, agg) => sum + agg.requests,
    0,
  );

describe("decodeRollup and mergeRollups", () => {
  it("adds up to the timed raw rows and agrees across windows, hour and day bundles", async () => {
    const raw = await Promise.all(
      RAW.map((file) => readColumns(BUFFERS[file.key], ["pop", "timestamp"])),
    );
    const byPop = new Map<string, number>();
    let rows = 0;
    for (const { rows: n, columns } of raw) {
      const pops = columns.get("pop")!;
      const ts = columns.get("timestamp")!;
      for (let i = 0; i < n; i++) {
        // Summaries, like the raw queries' time bounds, skip untimed rows.
        if (ts[i] == null) continue;
        const pop = pops[i] as string | null;
        if (pop) byPop.set(pop, (byPop.get(pop) ?? 0) + 1);
        rows++;
      }
    }

    const windows = await decodeAll(WINDOWS);
    const hours = await decodeAll(HOURS);
    const day = await decodeAll(DAY, HOUR);

    expect(overall(windows)).toBe(rows);
    expect(
      Object.fromEntries(
        [...windows.dims.get("pop")!].map(([pop, agg]) => [pop, agg.requests]),
      ),
    ).toEqual(Object.fromEntries(byPop));
    for (const dim of ["pop", "country", "proto"]) {
      expect(dimCounts(hours, dim), dim).toEqual(dimCounts(windows, dim));
      expect(dimCounts(day, dim), dim).toEqual(dimCounts(windows, dim));
    }
    expect(overall(hours)).toBe(rows);
    expect(overall(day)).toBe(rows);
    expect(windows.seriesStepMs).toBe(300_000);
    expect(day.seriesStepMs).toBe(HOUR);
    const rtt = (r: Rollup) =>
      r.series.get("overall")!.get("")!.values().reduce(
        (h, agg) => (agg.hists.rtt ? (h.merge(agg.hists.rtt), h) : h),
        new Histogram(),
      );
    expect(rtt(day).count()).toBe(rtt(windows).count());
    expect(rtt(day).quantile(0.5)).toBe(rtt(windows).quantile(0.5));
  });

  it("keeps the all_fields value counts of the timed raw rows, merged across windows and bundles", async () => {
    const raw = await Promise.all(
      RAW.map((file) => readColumns(BUFFERS[file.key], ["cache", "asn", "timestamp"])),
    );
    const expected = { cache: new Map<string, number>(), asn: new Map<string, number>() };
    for (const { rows: n, columns } of raw) {
      const ts = columns.get("timestamp")!;
      for (let i = 0; i < n; i++) {
        if (ts[i] == null) continue;
        for (const field of ["cache", "asn"] as const) {
          const value = columns.get(field)![i];
          if (value == null) continue;
          const counts = expected[field];
          counts.set(String(value), (counts.get(String(value)) ?? 0) + 1);
        }
      }
    }
    const counts = (rollup: Rollup, field: string) => Object.fromEntries(rollup.fields.get(field) ?? []);
    for (const rollup of [await decodeAll(WINDOWS), await decodeAll(HOURS), await decodeAll(DAY, HOUR)]) {
      expect(counts(rollup, "cache")).toEqual(Object.fromEntries(expected.cache));
      expect(counts(rollup, "asn")).toEqual(Object.fromEntries(expected.asn));
      expect([...rollup.truncated].filter((key) => key.startsWith("all_fields/"))).toEqual([]);
    }
  });

  it("returns null for a file that is not summary v3", async () => {
    expect(await decodeRollup(BUFFERS[RAW[0].key], 300_000)).toBeNull();
  });

  it("merges into fresh aggregates, leaving its inputs untouched", async () => {
    const a = (await decodeRollup(BUFFERS[WINDOWS[0].key], 300_000))!;
    const before = a.dims.get("pop")!.values().next().value!.requests;
    const rttHist = () =>
      [...a.series.get("overall")!.get("")!.values()].find((agg) => agg.hists.rtt)!
        .hists.rtt!;
    const histBefore = rttHist().count();
    mergeRollups([a, a]);
    expect(a.dims.get("pop")!.values().next().value!.requests).toBe(before);
    expect(rttHist().count()).toBe(histBefore);
  });
});

describe("rebucketSeries", () => {
  it("sums steps into coarser buckets and refuses buckets finer than or misaligned with the step", async () => {
    const windows = await decodeAll(WINDOWS);
    const hourly = rebucketSeries(windows, "asn", HOUR)!;
    for (const [asn, buckets] of windows.series.get("asn")!) {
      const byHour = new Map<number, number>();
      for (const [t, agg] of buckets) {
        const h = Math.floor(t / HOUR) * HOUR;
        byHour.set(h, (byHour.get(h) ?? 0) + agg.requests);
      }
      expect(
        Object.fromEntries([...hourly.get(asn)!].map(([t, agg]) => [t, agg.requests])),
        asn,
      ).toEqual(Object.fromEntries(byHour));
    }
    const rttCount = (series: Map<string, Map<number, RollupAgg>>) =>
      [...series.values()].flatMap((b) => [...b.values()]).reduce((n, agg) => n + (agg.hists.rtt?.count() ?? 0), 0);
    expect(rttCount(hourly)).toBe(rttCount(windows.series.get("asn")!));
    expect(rebucketSeries(windows, "asn", 60_000)).toBeNull();
    expect(rebucketSeries(windows, "asn", 450_000)).toBeNull();
    expect(rebucketSeries(await decodeAll(DAY, HOUR), "asn", 900_000)).toBeNull();
    expect(rebucketSeries(windows, "missing", HOUR)).toEqual(new Map());
  });
});

describe("planRollup", () => {
  const all = [...WINDOWS, ...HOURS, ...DAY];

  it("answers a fully covered range from the coarsest summaries", () => {
    const plan = planRollup(RAW, all, T0, T0 + 2 * HOUR)!;
    expect(plan.files.map((use) => use.file.key)).toEqual(DAY.map((f) => f.key));
    expect(plan.dataFromMs).toBe(T0);
    expect(plan.dataThroughMs).toBe(T0 + 2 * HOUR);
  });

  it("omits an open tail of windows without summaries and reports data through", () => {
    const summaries = [
      ...HOURS.filter((f) => at(f) === T0),
      ...WINDOWS.filter((f) => at(f) < T0 + HOUR + 45 * 60_000),
    ];
    const plan = planRollup(RAW, summaries, T0, T0 + 2 * HOUR)!;
    expect(plan.dataThroughMs).toBe(T0 + HOUR + 45 * 60_000);
    expect(plan.files).toHaveLength(4);
  });

  it("trims a head window that starts before the range", () => {
    const plan = planRollup(RAW, all, T0 + 5 * 60_000, T0 + 2 * HOUR)!;
    expect(plan.dataFromMs).toBe(T0 + 15 * 60_000);
    expect(plan.dataThroughMs).toBe(T0 + 2 * HOUR);
    expect(
      plan.files.flatMap((use) => use.covers).map((f) => f.key).toSorted(),
    ).toEqual(RAW.slice(1).map((f) => f.key).toSorted());
  });

  it("falls back to raw for a gap before the newest summary", () => {
    const summaries = WINDOWS.filter(
      (f) => at(f) !== T0 + HOUR + 15 * 60_000,
    );
    expect(planRollup(RAW, summaries, T0, T0 + 2 * HOUR)).toBeNull();
  });

  describe("with a residual budget", () => {
    const keys = (files: readonly ManifestFile[] | undefined) => (files ?? []).map((f) => f.key);
    const cutAt = (file: ManifestFile) => RAW.find((r) => r.start === file.start)!;

    it("plans a window crossing the range start as residual, data from the range start", () => {
      const fromMs = T0 + 5 * 60_000;
      const plan = planRollup(RAW, all, fromMs, T0 + 2 * HOUR, { residualBytes: Infinity })!;
      expect(keys(plan.residual)).toEqual([RAW[0].key]);
      expect(plan.dataFromMs).toBe(fromMs);
      expect(plan.dataThroughMs).toBe(T0 + 2 * HOUR);
      expect(plan.files.flatMap((use) => use.covers).map((f) => f.key)).not.toContain(RAW[0].key);
    });

    it("plans a gap and a long open tail as residual instead of null", () => {
      const gap = WINDOWS.find((f) => at(f) === T0 + 15 * 60_000)!;
      const summaries = WINDOWS.filter((f) => f !== gap && at(f) < T0 + HOUR);
      expect(planRollup(RAW, summaries, T0, T0 + 2 * HOUR)).toBeNull();
      const plan = planRollup(RAW, summaries, T0, T0 + 2 * HOUR, { residualBytes: Infinity })!;
      expect(keys(plan.residual).toSorted()).toEqual(
        [cutAt(gap), ...RAW.filter((f) => at(f) >= T0 + HOUR)].map((f) => f.key).toSorted(),
      );
      expect(plan.dataThroughMs).toBe(T0 + 2 * HOUR);
    });

    it("clips a residual window crossing the range end", () => {
      const toMs = T0 + 2 * HOUR - 5 * 60_000;
      const plan = planRollup(RAW, all, T0, toMs, { residualBytes: Infinity })!;
      expect(keys(plan.residual)).toEqual([RAW.at(-1)!.key]);
      expect(plan.dataThroughMs).toBe(toMs);
    });

    it("is null when the residual's listed bytes pass the budget", () => {
      const fromMs = T0 + 5 * 60_000;
      expect(planRollup(RAW, all, fromMs, T0 + 2 * HOUR, { residualBytes: RAW[0].size })).not.toBeNull();
      expect(planRollup(RAW, all, fromMs, T0 + 2 * HOUR, { residualBytes: RAW[0].size - 1 })).toBeNull();
    });
  });

  it("falls back to raw when the omitted tail is longer than one raw window", () => {
    const summaries = WINDOWS.filter((f) => at(f) < T0 + HOUR + 30 * 60_000);
    expect(planRollup(RAW, summaries, T0, T0 + 2 * HOUR)).toBeNull();
  });

  it("falls back to raw when nothing is covered", () => {
    expect(planRollup(RAW, [], T0, T0 + 2 * HOUR)).toBeNull();
  });

  // `compact={a}-{b}-c{n}-n{rows}-{tags}` names its sources by the sorted
  // FNV-1a hashes of their file names (`telemetry/compact.rs` `compact_key`).
  const fileName = (f: ManifestFile) => f.key.slice(f.key.lastIndexOf("/") + 1);
  const compactOf = (sources: ManifestFile[]): ManifestFile => {
    const dir = sources[0].key.replace(/(hour=\d{2}\/).*$/, "$1");
    const tags = [...new Set(sources.map((f) => fnv1a32(fileName(f))))]
      .sort((x, y) => x - y)
      .map((t) => t.toString(16).padStart(8, "0"))
      .join("");
    const a = Math.min(...sources.map((f) => Date.parse(f.start) / 1000));
    const b = Math.max(...sources.map((f) => Date.parse(f.end) / 1000));
    const rows = sources.reduce((sum, f) => sum + (f.rows ?? 0), 0);
    return {
      key: `${dir}catalog1/${"0".repeat(64)}/compact=${a}-${b}-c${sources.length}-n${rows}-${tags}.parquet`,
      start: new Date(a * 1000).toISOString(),
      end: new Date(b * 1000).toISOString(),
      rows,
      size: 1,
    };
  };
  const hour1Raw = RAW.filter((f) => at(f) >= T0 + HOUR);
  const openHour = [...HOURS.filter((f) => at(f) === T0), ...WINDOWS];

  it("answers a compacted open hour from the summaries of the windows the compact names", () => {
    const compact = compactOf(hour1Raw.slice(0, 3));
    const raw = [...RAW.filter((f) => at(f) < T0 + HOUR), compact, hour1Raw[3]];
    const plan = planRollup(raw, openHour, T0, T0 + 2 * HOUR)!;
    expect(plan).not.toBeNull();
    expect(plan.dataThroughMs).toBe(T0 + 2 * HOUR);
    const answering = plan.files
      .filter((use) => use.covers.includes(compact))
      .map((use) => use.file.key);
    expect(answering.toSorted()).toEqual(
      WINDOWS.filter(
        (f) => at(f) >= T0 + HOUR && at(f) < T0 + HOUR + 45 * 60_000,
      )
        .map((f) => f.key)
        .toSorted(),
    );
  });

  it("answers compacts merged from compacts only when the hour's window summaries add up", () => {
    const merged = compactOf([
      compactOf(hour1Raw.slice(0, 2)),
      compactOf(hour1Raw.slice(2, 4)),
    ]);
    const raw = [...RAW.filter((f) => at(f) < T0 + HOUR), merged];
    const plan = planRollup(raw, openHour, T0, T0 + 2 * HOUR)!;
    expect(plan).not.toBeNull();
    expect(plan.dataThroughMs).toBe(T0 + 2 * HOUR);
    expect(
      plan.files.filter((use) => use.covers.includes(merged)),
    ).toHaveLength(4);

    const missing = openHour.filter(
      (f) => at(f) !== T0 + HOUR + 15 * 60_000 || !WINDOWS.includes(f),
    );
    expect(planRollup(raw, missing, T0, T0 + 2 * HOUR)).toBeNull();
  });

  it("reads a compact raw when a window it names has no summary", () => {
    const compact = compactOf(hour1Raw.slice(0, 3));
    const raw = [...RAW.filter((f) => at(f) < T0 + HOUR), compact, hour1Raw[3]];
    const missing = openHour.filter(
      (f) => at(f) !== T0 + HOUR + 15 * 60_000 || !WINDOWS.includes(f),
    );
    expect(planRollup(raw, missing, T0, T0 + 2 * HOUR)).toBeNull();
  });

  it("answers a compacted open hour from its open_bundle plus trailing window summaries", () => {
    const compact = compactOf(hour1Raw);
    const raw = [...RAW.filter((f) => at(f) < T0 + HOUR), compact];
    const openRows = hour1Raw
      .slice(0, 3)
      .reduce((sum, f) => sum + (f.rows ?? 0), 0);
    const openEndMs = Date.parse(hour1Raw[2].end);
    const openBundle: ManifestFile = {
      key: `processed/summary_bundle/request/sha256=${"a".repeat(64)}/year=2027/month=01/day=15/open_bundle=09-${openEndMs}-c3-n${openRows}-h0123456789abcdef.parquet`,
      start: hour1Raw[0].start,
      end: hour1Raw[2].end,
      rows: openRows,
      size: 1,
    };
    const trailingSummary = WINDOWS.find(
      (f) => at(f) === Date.parse(hour1Raw[3].start),
    )!;
    const summaries = [
      ...HOURS.filter((f) => at(f) === T0),
      openBundle,
      trailingSummary,
    ];

    const plan = planRollup(raw, summaries, T0, T0 + 2 * HOUR)!;
    expect(plan).not.toBeNull();
    expect(plan.dataThroughMs).toBe(T0 + 2 * HOUR);
    const answering = plan.files
      .filter((use) => use.covers.includes(compact))
      .map((use) => use.file.key);
    expect(answering.toSorted()).toEqual(
      [openBundle.key, trailingSummary.key].toSorted(),
    );

    // If the trailing summary is missing, rows no longer add up -> must fall back to raw.
    expect(
      planRollup(
        raw,
        [...HOURS.filter((f) => at(f) === T0), openBundle],
        T0,
        T0 + 2 * HOUR,
      ),
    ).toBeNull();

    // If the requested range starts mid-hour, the open hour is partial -> must fall back to raw.
    expect(
      planRollup(raw, summaries, T0 + HOUR + 5 * 60_000, T0 + 2 * HOUR),
    ).toBeNull();
  });

  it("uses a compacted open hour's open_bundle when the live tail window crosses the range end", () => {
    // Live shape: a compact and a still-live window both inside the open bundle,
    // and the newest window (with its summary) ending after `toMs`.
    const compact = compactOf(hour1Raw.slice(0, 2));
    const raw = [...RAW.filter((f) => at(f) < T0 + HOUR), compact, hour1Raw[2], hour1Raw[3]];
    const openRows = hour1Raw.slice(0, 3).reduce((sum, f) => sum + (f.rows ?? 0), 0);
    const openEndMs = Date.parse(hour1Raw[2].end);
    const openBundle: ManifestFile = {
      key: `processed/summary_bundle/request/sha256=${"a".repeat(64)}/year=2027/month=01/day=15/open_bundle=09-${openEndMs}-c3-n${openRows}-h0123456789abcdef.parquet`,
      start: hour1Raw[0].start,
      end: hour1Raw[2].end,
      rows: openRows,
      size: 1,
    };
    const tailSummary = WINDOWS.find((f) => at(f) === at(hour1Raw[3]))!;
    const summaries = [...HOURS.filter((f) => at(f) === T0), openBundle, tailSummary];
    const toMs = at(hour1Raw[3]) + 5 * 60_000;

    const plan = planRollup(raw, summaries, T0, toMs)!;
    expect(plan).not.toBeNull();
    expect(plan.dataThroughMs).toBe(openEndMs);
    const open = plan.files.find((use) => use.file.key === openBundle.key)!;
    expect(open.covers.toSorted((a, b) => a.key.localeCompare(b.key))).toEqual(
      [compact, hour1Raw[2]].toSorted((a, b) => a.key.localeCompare(b.key)),
    );

    // A bundle reaching past the range end could hold rows after it.
    const late = Date.parse(hour1Raw[3].end);
    const longBundle = {
      ...openBundle,
      key: openBundle.key.replace(`open_bundle=09-${openEndMs}-`, `open_bundle=09-${late}-`),
      end: hour1Raw[3].end,
    };
    expect(planRollup(raw, [...HOURS.filter((f) => at(f) === T0), longBundle], T0, toMs)).toBeNull();
    // A raw file of the hour that starts before the range still makes the hour partial.
    expect(planRollup(raw, summaries, T0 + HOUR + 5 * 60_000, toMs)).toBeNull();
  });
});

describe("queryRollup", () => {
  const source = (
    summaries: ManifestFile[],
    buffers: Record<string, ArrayBuffer> = BUFFERS,
  ) =>
    fixtureSource(
      [fixturePage(RAW)],
      buffers,
      { json: [], files: [] },
      [fixturePage(summaries)],
    );

  it("reads, decodes and merges the planned summaries once for concurrent callers", async () => {
    const s = source([...WINDOWS, ...HOURS]);
    const range = { fromMs: T0, toMs: T0 + 2 * HOUR };
    const [a, b] = await Promise.all([
      queryRollup(s, range),
      queryRollup(s, range),
    ]);
    expect(a).toBe(b);
    expect(a!.dataFromMs).toBe(T0);
    expect(a!.dataThroughMs).toBe(T0 + 2 * HOUR);
    expect(overall(a!.rollup)).toBe(overall(await decodeAll(WINDOWS)));
    expect(s.log.files.toSorted()).toEqual(HOURS.map((f) => f.key).toSorted());
  });

  it("lists raw files and their summaries in one request", async () => {
    const s = source([...WINDOWS, ...HOURS]);
    const result = await queryRollup(s, { fromMs: T0, toMs: T0 + 2 * HOUR });
    expect(result!.dataThroughMs).toBe(T0 + 2 * HOUR);
    expect(s.log.summary).toBeUndefined();
    expect(s.log.json).toHaveLength(1);
    expect(new URL(s.log.json[0], "http://x").searchParams.get("summaries")).toBe("1");
  });

  it("lists summaries separately when the raw listing omits them", async () => {
    const s = source([...WINDOWS, ...HOURS]);
    const fetchJson = s.fetchJson;
    s.fetchJson = async (url, signal) => {
      const { summary_files: _omitted, ...page } = (await fetchJson(url, signal)) as Record<string, unknown>;
      return page;
    };
    const result = await queryRollup(s, { fromMs: T0, toMs: T0 + 2 * HOUR });
    expect(overall(result!.rollup)).toBe(overall(await decodeAll(WINDOWS)));
    expect(s.log.summary).toHaveLength(1);
  });

  it("rolls up the raw rows no summary answers, exactly through the range end", async () => {
    const fromMs = T0 + 5 * 60_000;
    const toMs = T0 + 2 * HOUR - 7 * 60_000;
    const gap = WINDOWS.find((f) => at(f) === T0 + HOUR)!;
    const s = projectingSource(source(WINDOWS.filter((f) => f !== gap)), BUFFERS);
    expect(await queryRollup(s, { fromMs, toMs })).toBeNull();
    const result = (await queryRollup(s, { fromMs, toMs, residual: INSIGHTS_RESIDUAL }))!;
    let rows = 0;
    for (const file of RAW) {
      const { rows: n, columns } = await readColumns(BUFFERS[file.key], ["timestamp"]);
      const ts = columns.get("timestamp")!;
      for (let i = 0; i < n; i++) {
        const t = ts[i] instanceof Date ? (ts[i] as Date).getTime() : Number(ts[i]);
        if (ts[i] != null && t >= fromMs && t < toMs) rows++;
      }
    }
    expect(overall(result.rollup)).toBe(rows);
    expect([result.dataFromMs, result.dataThroughMs]).toEqual([fromMs, toMs]);
    expect(s.ranges.toSorted()).toEqual(
      [RAW[0], RAW.find((f) => at(f) === T0 + HOUR)!, RAW.at(-1)!].map((f) => f.key).toSorted(),
    );
    expect([...result.rollup.dims.keys()]).toEqual(["city"]);
    expect(result.rollup.present!.has("ins_url/url")).toBe(true);
    expect(result.missingRanges!.size).toBe(0);
  });

  it("returns null, never zero, when a planned summary is not v3", async () => {
    const stale = WINDOWS[0];
    const s = source(WINDOWS, { ...BUFFERS, [stale.key]: BUFFERS[RAW[0].key] });
    expect(await queryRollup(s, { fromMs: T0, toMs: T0 + 2 * HOUR })).toBeNull();
  });

  it("aborts in-flight queryRollup and readSummaryProjection across clearRollupQueryCache without writing derived CacheStorage or retaining decoded Rollups in RAM", async () => {
    const rollupMod = await import("./rollup-query");
    const { defineSummarySpec, summaryProjectionLimits } = await import("./summary-projection");

    const spec = defineSummarySpec({ groups: ["dims"], columns: ["requests", "status5xx", "resp_bytes"] });
    const prevLimit = summaryProjectionLimits.wholeBelowBytes;
    summaryProjectionLimits.wholeBelowBytes = 128;

    try {
      let releaseDerivedRead: () => void = () => {};
      let notifyDerivedPaused: () => void = () => {};
      let pauseFirstDerivedRead = true;
      let derivedWrites = 0;
      let rangeReads = 0;
      let wholeFetches = 0;

      const paused = new Promise<void>((r) => {
        notifyDerivedPaused = r;
      });
      const base = source(HOURS);
      const trackedSource = {
        ...base,
        cacheScope: "alice-scope:svc-alice",
        fetchFile: async (key: string, sig?: AbortSignal) => {
          wholeFetches++;
          sig?.throwIfAborted();
          return base.fetchFile(key, sig);
        },
        fetchRange: async (file: ManifestFile, start: number, end: number, sig?: AbortSignal) => {
          rangeReads++;
          sig?.throwIfAborted();
          return BUFFERS[file.key].slice(start, end);
        },
        readDerivedFile: async () => {
          if (pauseFirstDerivedRead) {
            pauseFirstDerivedRead = false;
            notifyDerivedPaused?.();
            await new Promise<void>((r) => {
              releaseDerivedRead = r;
            });
          }
          return null;
        },
        writeDerivedFile: async () => {
          derivedWrites++;
        },
      };

      const caller = new AbortController();
      const pending = queryRollup(trackedSource, {
        fromMs: T0,
        toMs: T0 + 2 * HOUR,
        spec,
        signal: caller.signal,
      }).then(
        () => "RESOLVED",
        () => "REJECTED",
      );

      await paused;
      caller.abort(new DOMException("Unmounted on logout", "AbortError"));
      const clearFn = (rollupMod as { clearRollupQueryCache?: () => void }).clearRollupQueryCache;
      clearFn?.();
      releaseDerivedRead?.();

      expect(await pending).toBe("REJECTED");
      await new Promise((r) => setTimeout(r, 10));

      expect(rangeReads).toBe(0);
      expect(derivedWrites).toBe(0);
      expect(wholeFetches).toBe(0);
      expect(typeof (rollupMod as Record<string, unknown>).clearRollupQueryCache).toBe("function");

      // Subsequent post-clear replay must NOT hit stale decoded RAM from before logout:
      const replay = await queryRollup(trackedSource, {
        fromMs: T0,
        toMs: T0 + 2 * HOUR,
        spec,
      });
      expect(replay).not.toBeNull();
      expect(rangeReads + wholeFetches).toBeGreaterThan(0);

      // Auditor Challenge 1: un-signaled queryRollup must ALSO abort and evict decoded solely via clearRollupQueryCache():
      clearFn?.();
      rangeReads = 0;
      wholeFetches = 0;
      derivedWrites = 0;
      pauseFirstDerivedRead = true;
      const pausedUnsignaled = new Promise<void>((r) => {
        notifyDerivedPaused = r;
      });
      const unsignaledPending = queryRollup(trackedSource, {
        fromMs: T0,
        toMs: T0 + 2 * HOUR,
        spec,
      }).then(
        () => "RESOLVED",
        () => "REJECTED",
      );
      await pausedUnsignaled;
      clearFn?.();
      releaseDerivedRead?.();
      expect(await unsignaledPending).toBe("REJECTED");
      expect(rangeReads).toBe(0);
      expect(derivedWrites).toBe(0);
    } finally {
      summaryProjectionLimits.wholeBelowBytes = prevLimit;
    }
  });
});
