// @vitest-environment node
import { afterAll, beforeAll, describe, expect, it, vi } from "vitest";
import { ASSETS_SUMMARY_SPEC, assetsFromRollup, type AssetsRollupBody } from "./assets-rollup";
import type { ManifestFile } from "./dashboard/manifest";
import { decodeRollup, mergeRollups, rollupGroup } from "./dashboard/rollup-query";
import { FIXTURE_START, fixturePage, loadSummaryFixtures } from "./dashboard/summary-fixtures";
import { editSummaryRows, withSummaryLayout } from "./dashboard/summary-layout-fixture";
import {
  defineSummarySpec, readSummaryColumns, readSummaryProjection, summaryProjectionLimits, type SummarySpec,
} from "./dashboard/summary-projection";
import { fixtureSource, projectingSource } from "./dashboard/test-fixtures";
import type { InsightsRequest } from "./insights";
import {
  INSIGHTS_SUMMARY_SPEC, insightsBaselineFromRollup, insightsBaselineRollup, rollupInsightsBaseline,
} from "./insights-rollup";
import { NETWORK_SUMMARY_SPEC, networkFromRollup } from "./network-rollup";
import { ORIGIN_SUMMARY_SPEC, originFromRollup, type OriginRollupBody } from "./origin-rollup";
import { PERFORMANCE_SUMMARY_SPEC, performanceFromRollup, type PerformanceRollupBody } from "./performance-rollup";
import type { RollupAnswerSource } from "./rollup-answer";
import { SECURITY_ROLLUP_SECTIONS, VALUE_ROLLUP_SECTIONS } from "./rollup-sections";
import {
  SECURITY_THREAT_INTEL_SUMMARY_SPEC, securityFromRollup, type SecurityRollupBody, securitySummarySpec,
} from "./security-rollup";
import { VALUE_SUMMARY_SPEC, valueFromRollup, type ValueRollupBody } from "./value-rollup";

const HOUR = 3_600_000;
const T0 = FIXTURE_START * 1000;
const T1 = T0 + 2 * HOUR;
const range = { start_time: new Date(T0).toISOString(), end_time: new Date(T1).toISOString() };
const FIXTURES = {
  network: loadSummaryFixtures("network-summaries"),
  origin: loadSummaryFixtures("origin-summaries"),
  dashboard: loadSummaryFixtures("../dashboard/__fixtures__/summaries"),
  securityValue: loadSummaryFixtures("security-value-summaries"),
  preValue: loadSummaryFixtures("network-summaries"),
};
type FixtureName = keyof typeof FIXTURES;
/** Summary rows as written before the `value` group, for the reads that fall back without it. */
const EDITS: Partial<Record<FixtureName, (rows: Record<string, unknown>[]) => Record<string, unknown>[]>> = {
  preValue: (rows) => rows.filter((row) => row.bundle !== "value" && !(row.bundle === "dim_totals" && row.value === "value")),
};

type Answer = (source: RollupAnswerSource, spec: SummarySpec | null) => Promise<unknown>;
/**
 * `inert`: columns the page reads that every fixture leaves null or zero in the
 * named groups, so no answer here can tell them missing; the test checks that.
 * `elsewhere`: spec groups or columns (as the needs test names them) only rows
 * no fixture here holds can show missing, each shown by its own test.
 */
type Page = {
  spec: SummarySpec;
  fixtures: FixtureName[];
  answers: Answer[];
  inert?: Record<string, string[]>;
  elsewhere?: string[];
};

const HEALTH = ["summary", "leaderboard", "metro_leaderboard", "heatmap", "buckets"];
const network = (path: string, params: Record<string, unknown>): Answer => (source, spec) =>
  networkFromRollup(path, { ...range, bucket_seconds: 900, top_n: 30, ...params }, source, undefined, spec);
const origin = (body: Partial<OriginRollupBody>): Answer => (source, spec) =>
  originFromRollup("/api/origin/aggregates", {
    ...range, bucket_minutes: 60, timeseries_metric: "ttlb", timeseries_percentile: "p95", ip_health_limit: 30,
    sections: ["summary", "timeseries", "slow_urls", "status_codes", "path_breakdown", "pop_latency", "ip_health"],
    ...body,
  } as OriginRollupBody, source, undefined, spec);
const performance = (body: Partial<PerformanceRollupBody>): Answer => (source, spec) =>
  performanceFromRollup("/api/performance/aggregates", {
    ...range, sections: ["ttl_dist", "waterfall", "top_urls", "top_asns", "scatter"], ...body,
  } as PerformanceRollupBody, source, undefined, spec);
const assets = (body: Partial<AssetsRollupBody>): Answer => (source, spec) =>
  assetsFromRollup("/api/assets/aggregates", { ...range, sections: ["summary"], ...body } as AssetsRollupBody,
    source, undefined, spec);
const value = (body: Partial<ValueRollupBody>): Answer => (source, spec) =>
  valueFromRollup("/api/value/summary", {
    ...range, sections: ["overview", "network"], chart_interval: "1 day", ...body,
  } as ValueRollupBody, source, undefined, spec);
const security = (path: string, body: Partial<SecurityRollupBody> = {}): Answer => (source, spec) =>
  securityFromRollup(path, { ...range, ...body }, source, undefined, spec);
const SECURITY_ALL = { sections: [...SECURITY_ROLLUP_SECTIONS], bucket_seconds: 3600 };
const insights: Answer = async (source, spec) => {
  const body = { service_id: "example", window_size_hrs: 1, baseline_hours: 1, clamp_end: new Date(T1).toISOString() };
  vi.useFakeTimers({ toFake: ["Date"], now: T1 });
  const result = await insightsBaselineFromRollup(body as InsightsRequest, [], source, undefined, spec)
    .finally(() => vi.useRealTimers());
  if (!result) return null;
  // Window rows naming every baseline city and ASN, so each `b_*` column is filled.
  const city = [...(result.rollup.dims.get("city")?.keys() ?? [])].map((key) => {
    const [country, name] = JSON.parse(key) as [string, string];
    return { country, city: name };
  });
  const asn = [...(result.rollup.fields.get("asn")?.keys() ?? [])].map((value) => ({ asn: value }));
  return rollupInsightsBaseline(insightsBaselineRollup(result), { city, dimension: [], asn, scalar: [{}] });
};

const PAGES: Record<string, Page> = {
  network: {
    spec: NETWORK_SUMMARY_SPEC,
    fixtures: ["network", "dashboard"],
    inert: Object.fromEntries(["cwnd_sum", "cwnd_count", "label"].map((column) => [column, [...NETWORK_SUMMARY_SPEC.groups]])),
    answers: [
      network("/api/network/bundle", { sections: [...HEALTH, "pop_health", "transport"] }),
      network("/api/network-health", { sections: [...HEALTH, "cities", "map_buckets"] }),
      network("/api/network-health", { sections: ["cities", "map_buckets"] }),
      network("/api/network-quality", { region_country: "US" }),
      network("/api/network/pop-health", {}),
    ],
  },
  origin: {
    spec: ORIGIN_SUMMARY_SPEC,
    fixtures: ["origin", "dashboard"],
    answers: [
      origin({}),
      origin({ timeseries_metric: "ttfb", timeseries_percentile: "p50", split_by_leg: true, bucket_minutes: 5 }),
      origin({ slow_urls_min_requests: 1, slow_urls_limit: 100, sections: ["slow_urls", "summary"] }),
    ],
  },
  performance: {
    spec: PERFORMANCE_SUMMARY_SPEC,
    fixtures: ["origin", "dashboard"],
    answers: [performance({}), performance({ sort_by: "avg" } as Partial<PerformanceRollupBody>)],
  },
  assets: {
    spec: ASSETS_SUMMARY_SPEC,
    fixtures: ["origin", "dashboard"],
    inert: { origin_wait_sum: ["assets"], origin_requests: ["assets"] },
    answers: [assets({}), assets({ filters: { cache: { mode: "include", values: ["HIT"] } } } as Partial<AssetsRollupBody>)],
  },
  value: {
    spec: VALUE_SUMMARY_SPEC,
    fixtures: ["network", "origin", "securityValue", "preValue"],
    answers: [
      value({}),
      value({ chart_interval: "1 hour" } as Partial<ValueRollupBody>),
      value({ sections: [...VALUE_ROLLUP_SECTIONS] }),
    ],
  },
  security: {
    spec: securitySummarySpec(SECURITY_ROLLUP_SECTIONS),
    fixtures: ["securityValue"],
    answers: [
      security("/api/security/aggregates", SECURITY_ALL),
      security("/api/security/bundle", { ...SECURITY_ALL, mask_ips: true }),
    ],
  },
  threatIntel: {
    spec: SECURITY_THREAT_INTEL_SUMMARY_SPEC,
    fixtures: ["securityValue"],
    answers: [security("/api/security/threat-intel")],
    // WAF rule and bot columns; see security-rollup.test.ts "reads the field totals ...".
    elsewhere: ["group field_totals"],
  },
  insights: { spec: INSIGHTS_SUMMARY_SPEC, fixtures: ["network", "dashboard"], answers: [insights] },
};

/** One fixture's raw windows and a summary set, as the server lists them, with its files' bytes. */
type Listing = { raw: ManifestFile[]; summaries: ManifestFile[]; buffers: Record<string, ArrayBuffer> };

/** The fixture's window summaries, or its fullest hour bundle of each hour, today's layout or the announced one. */
async function listing(name: FixtureName, set: "window" | "hour", layout: boolean): Promise<Listing> {
  const fixture = FIXTURES[name];
  const hours = new Map<string, ManifestFile>();
  for (const file of fixture.files("hour")) {
    const held = hours.get(file.start);
    if (!held || (file.sources ?? 0) > (held.sources ?? 0)) hours.set(file.start, file);
  }
  const chosen = set === "window" ? fixture.files("window") : [...hours.values()];
  const buffers = { ...fixture.buffers };
  const edit = EDITS[name];
  const summaries = await Promise.all(chosen.map(async (file) => {
    if (!layout && !edit) return file;
    const bytes = edit ? await editSummaryRows(fixture.buffers[file.key], edit) : fixture.buffers[file.key];
    buffers[file.key] = layout ? await withSummaryLayout(bytes) : bytes;
    return { ...file, size: buffers[file.key].byteLength };
  }));
  return { raw: fixture.files("raw"), summaries, buffers };
}

const sourceOf = (listing: Listing) => projectingSource(
  fixtureSource([fixturePage(listing.raw)], listing.buffers, { json: [], files: [] }, [fixturePage(listing.summaries)]),
  listing.buffers,
);

type Outcome = { value?: unknown; error?: string; wholeFiles: string[] };

async function answer(page: Page, listing: Listing, spec: SummarySpec | null): Promise<Outcome[]> {
  const out: Outcome[] = [];
  for (const run of page.answers) {
    const source = sourceOf(listing);
    try {
      out.push({ value: await run(source, spec), wholeFiles: source.log.files });
    } catch (error) {
      out.push({ error: String(error), wholeFiles: source.log.files });
    }
  }
  return out;
}

const same = (a: unknown, b: unknown) => {
  try {
    expect(a).toEqual(b);
    return true;
  } catch {
    return false;
  }
};

const LISTINGS: Record<string, Listing> = {};
beforeAll(async () => {
  for (const name of Object.keys(FIXTURES) as FixtureName[]) {
    for (const set of ["window", "hour"] as const) {
      for (const layout of [false, true]) LISTINGS[`${name}/${set}/${layout ? "layout" : "legacy"}`] = await listing(name, set, layout);
    }
  }
  // The fixtures are a few KiB and mostly footer, which a projection would otherwise read whole.
  Object.assign(summaryProjectionLimits, { wholeBelowBytes: 0, wholeAboveShare: Infinity });
}, 120_000);
const LIMITS = { ...summaryProjectionLimits };
afterAll(() => Object.assign(summaryProjectionLimits, LIMITS));

const BASE = new Set(["bundle", "bucket", "field", "value", "truncated"]);

describe.each(Object.entries(PAGES))("%s from projected summaries", (_name, page) => {
  const listings = page.fixtures.flatMap((name) => ["window", "hour"].flatMap((set) =>
    ["legacy", "layout"].map((layout) => `${name}/${set}/${layout}`)));

  it.each(listings)("equals the whole-file answer on %s", async (key) => {
    const whole = await answer(page, LISTINGS[key], null);
    expect(whole.map((outcome) => outcome.error)).toEqual(whole.map(() => undefined));
    expect(whole.some((outcome) => outcome.value !== null)).toBe(true);
    const projected = await answer(page, LISTINGS[key], page.spec);
    expect(projected.map((outcome) => outcome.error)).toEqual(whole.map(() => undefined));
    expect(projected.map((outcome) => outcome.value)).toEqual(whole.map((outcome) => outcome.value));
    expect(projected.flatMap((outcome) => outcome.wholeFiles)).toEqual([]);
  });

  /** A spec without one group or column must change some answer, or throw: the spec holds nothing a page does not read. */
  it("needs every group and column of its spec", async () => {
    const differs: Record<string, boolean> = {};
    const reduced = [
      ...[...page.spec.groups].map((group) => ({
        name: `group ${group}`,
        spec: defineSummarySpec({
          groups: [...page.spec.groups].filter((g) => g !== group),
          columns: [...page.spec.columns],
          only: Object.fromEntries([...page.spec.only].map(([column, of]) => [column, [...of]])),
        }),
      })),
      ...[...page.spec.columns].filter((column) => !BASE.has(column) && !(column in (page.inert ?? {}))).map((column) => ({
        name: `column ${column}`,
        spec: defineSummarySpec({
          groups: [...page.spec.groups],
          columns: [...page.spec.columns].filter((c) => c !== column),
          only: Object.fromEntries([...page.spec.only].filter(([c]) => c !== column).map(([c, of]) => [c, [...of]])),
        }),
      })),
    ];
    for (const { name } of reduced) differs[name] = page.elsewhere?.includes(name) ?? false;
    expect((page.elsewhere ?? []).filter((name) => !(name in differs))).toEqual([]);
    for (const fixture of page.fixtures) {
      for (const set of ["window", "hour"]) {
        const listing = LISTINGS[`${fixture}/${set}/legacy`];
        const whole = await answer(page, listing, null);
        for (const { name, spec } of reduced) {
          if (differs[name]) continue;
          const got = await answer(page, listing, spec);
          differs[name] = got.some((outcome, i) => outcome.error !== undefined || !same(outcome.value, whole[i].value));
        }
      }
    }
    expect(Object.entries(differs).filter(([, changed]) => !changed).map(([name]) => name)).toEqual([]);
  }, 120_000);

  it.runIf(page.inert)("holds only null or zero in its inert columns", async () => {
    for (const fixture of page.fixtures) {
      for (const set of ["window", "hour"]) {
        const listing = LISTINGS[`${fixture}/${set}/legacy`];
        for (const file of listing.summaries) {
          const inert = Object.entries(page.inert!);
          const { rows, columns } = await readSummaryColumns([listing.buffers[file.key]], ["bundle", ...Object.keys(page.inert!)]);
          for (const [column, groups] of inert) {
            const values = new Set(Array.from({ length: rows }, (_, i) => i)
              .filter((i) => groups.includes(String(columns.get("bundle")![i])))
              .map((i) => columns.get(column)?.[i] ?? null));
            expect([...values].filter((v) => v !== null && Number(v) !== 0), `${file.key} ${column}`).toEqual([]);
          }
        }
      }
    }
  });
});

describe("a rollup decoded for a spec", () => {
  const fixture = FIXTURES.network;
  const file = fixture.files("hour")[0];
  const spec = defineSummarySpec({ groups: ["dims", "dim_totals"], columns: ["requests"] });

  it("throws on any group it did not load, and reads the ones it did", async () => {
    const source = projectingSource(fixtureSource([], fixture.buffers), fixture.buffers);
    const { parts } = await readSummaryProjection(source, file, spec);
    const decoded = (await decodeRollup(parts, 300_000, spec))!;
    const whole = (await decodeRollup(fixture.buffers[file.key], 300_000))!;
    for (const rollup of [decoded, mergeRollups([decoded, decoded])]) {
      expect(rollup.dims.get("country")?.size).toBe(whole.dims.get("country")?.size);
      expect(rollup.present!.has("dims/country")).toBe(true);
      expect(() => rollup.series.get("overall")).toThrow(/series5/);
      expect(() => [...rollup.fields]).toThrow(/all_fields/);
      expect(() => rollup.present!.has("series5/city")).toThrow(/series5/);
      expect(() => rollup.truncated.has("all_fields/ua")).toThrow(/all_fields/);
      expect(() => rollupGroup(rollup, "quality", "overall")).toThrow(/quality/);
      expect(() => rollup.overview).toThrow(/overview/);
      expect(() => rollup.samples).toThrow(/sample/);
      expect(() => rollup.assets).toThrow(/assets/);
      expect(() => rollup.waterfall).toThrow(/overview/);
      expect(rollup.dims.get("country")!.values().next().value!.errors).toBeNaN();
      expect(() => structuredClone(rollup)).not.toThrow();
    }
  });
});
