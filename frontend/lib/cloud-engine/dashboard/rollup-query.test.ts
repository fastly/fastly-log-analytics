// @vitest-environment node
import { describe, expect, it } from "vitest";
import { Histogram } from "./histogram";
import type { ManifestFile } from "./manifest";
import { readColumns } from "./parquet";
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
import { fixtureSource } from "./test-fixtures";

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

  it("falls back to raw when the omitted tail is longer than allowed", () => {
    const summaries = WINDOWS.filter((f) => at(f) < T0 + HOUR);
    expect(planRollup(RAW, summaries, T0, T0 + 2 * HOUR)).not.toBeNull();
    expect(
      planRollup(RAW, summaries, T0, T0 + 2 * HOUR, 30 * 60_000),
    ).toBeNull();
  });

  it("falls back to raw when nothing is covered", () => {
    expect(planRollup(RAW, [], T0, T0 + 2 * HOUR)).toBeNull();
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

  it("returns null, never zero, when a planned summary is not v3", async () => {
    const stale = WINDOWS[0];
    const s = source(WINDOWS, { ...BUFFERS, [stale.key]: BUFFERS[RAW[0].key] });
    expect(await queryRollup(s, { fromMs: T0, toMs: T0 + 2 * HOUR })).toBeNull();
  });
});
