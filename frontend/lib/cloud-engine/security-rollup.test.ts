// @vitest-environment node
import { parquetMetadata } from "hyparquet";
import { parquetWriteBuffer } from "hyparquet-writer";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { DuckDBAnalyticalEngine as ParquetEngine } from "./dashboard/duckdb";
import { Hll } from "./dashboard/hll";
import type { ManifestFile } from "./dashboard/manifest";
import { FIXTURE_START, fixturePage, loadSummaryFixtures } from "./dashboard/summary-fixtures";
import { withSummaryLayout } from "./dashboard/summary-layout-fixture";
import { defineSummarySpec, summaryProjectionLimits, type SummarySpec } from "./dashboard/summary-projection";
import { fixtureSource, projectingSource } from "./dashboard/test-fixtures";
import { SECURITY_ROLLUP_SECTIONS } from "./rollup-sections";
import { executeSecurityAggregates, type SecurityAggregatesResponse, type SecuritySectionName } from "./security";
import {
  decodeSecuritySummary,
  SECURITY_SUMMARY_SPEC,
  SECURITY_TLS_SUMMARY_SPEC,
  type SecurityRollup,
  securityAggregatesFromRollup,
  securityFromRollup,
  securityRollupEligible,
} from "./security-rollup";

const HOUR = 3_600_000;
const T0 = FIXTURE_START * 1000;
const T1 = T0 + 2 * HOUR;
const START = new Date(T0).toISOString();
const END = new Date(T1).toISOString();
const PATH = "/api/security/aggregates";

/** The Rust-written dashboard fixture: raw windows (with `ip` and `is_ipv6`), their summaries and bundles. */
const RUST = loadSummaryFixtures("../dashboard/__fixtures__/summaries");
const RUST_RAW = RUST.files("raw");
const RUST_WINDOWS = RUST.files("window");
const RUST_HOURS = RUST.files("hour", (e) => e.key.includes("-c4-"));
const RUST_DAY = RUST.files("day");

type Row = {
  ts: number | null;
  ip: string | null;
  tls: string | null;
  hdr: number | null;
  conn: number | null;
  v6: boolean | null;
  ptype: string | null;
  waf: string | null;
};

/** Values with distinct totals (no ties in a ranking), spread over the rows by a fixed permutation. */
function spread<T>(counts: [T, number][], n: number): T[] {
  const list = counts.flatMap(([value, count]) => Array<T>(count).fill(value));
  expect(list).toHaveLength(n);
  return list.map((_, i) => list[(i * 97) % n]);
}

/**
 * Security rows for the dashboard fixture's raw windows (same names and row
 * counts, so the Rust bundle names plan over them), with every bucket and
 * corner the sections read: null and '' values, case-mixed verified bots and
 * rows without a timestamp.
 */
function syntheticRows(windows: ManifestFile[]): Row[][] {
  const n = windows.reduce((sum, file) => sum + Number(/-n(\d+)\.parquet$/.exec(file.key)![1]), 0);
  const tls = spread<string | null>([
    ...Array.from({ length: 22 }, (_, k) => [`fp-${String(k).padStart(2, "0")}`, (22 - k) * 5] as [string, number]),
    ["", 150], [null, 185],
  ], n);
  const ptype = spread<string | null>([["vpn", 500], ["tor", 330], ["hosting", 210], ["public", 120], ["", 140], [null, 300]], n);
  const hdr = [0, 256, 257, 1024, 1025, 4096, 5000, 8192, 9000, 32768, 40000, null];
  const conn = [0, 1, 1, 3, 5, 7, 20, 21, 100, 101, 500, null, -2];
  const waf = (i: number) => (i % 9 === 0 ? "VERIFIED-BOT.googlebot" : i % 9 === 3 ? "verified-bot.Bingbot"
    : i % 9 === 5 ? "SQLI" : i % 18 === 4 ? "Search VERIFIED-BOT.duckbot" : null);
  let i = 0;
  return windows.map((file) => {
    const start = Date.parse(file.start);
    const rows = Number(/-n(\d+)\.parquet$/.exec(file.key)![1]);
    return Array.from({ length: rows }, (_, j) => {
      const row: Row = {
        ts: i % 53 === 7 ? null : start + Math.floor((j * 900_000) / rows),
        ip: i % 61 === 0 ? null : `10.0.${i % 40}.${(i >> 3) % 9}`,
        tls: tls[i],
        hdr: hdr[i % hdr.length],
        conn: conn[i % conn.length],
        v6: i % 17 === 0 ? null : i % 5 === 0 || (i % 7 === 0 && start >= T0 + HOUR),
        ptype: ptype[i],
        waf: waf(i),
      };
      i++;
      return row;
    });
  });
}

function rawParquet(rows: Row[], customProxyType = false): ArrayBuffer {
  return parquetWriteBuffer({
    columnData: [
      { name: "timestamp", type: "TIMESTAMP", data: rows.map((r) => (r.ts === null ? null : new Date(r.ts))) },
      { name: "ts_ms", type: "INT64", data: rows.map((r) => (r.ts === null ? null : BigInt(r.ts))) },
      { name: "ip", type: "STRING", data: rows.map((r) => r.ip) },
      { name: "tls_ciphers_sha", type: "STRING", data: rows.map((r) => r.tls) },
      { name: "req_header_bytes", type: "INT64", data: rows.map((r) => (r.hdr === null ? null : BigInt(r.hdr))) },
      { name: "conn_requests", type: "INT64", data: rows.map((r) => (r.conn === null ? null : BigInt(r.conn))) },
      { name: "is_ipv6", type: "BOOLEAN", data: rows.map((r) => r.v6) },
      { name: "p_type", type: "STRING", data: rows.map((r) => r.ptype) },
      { name: "waf_sig", type: "STRING", data: rows.map((r) => r.waf) },
      ...customProxyType ? [{ name: "proxy_type", type: "STRING" as const, data: rows.map(() => "relay") }] : [],
    ],
  });
}

const headerBin = (b: number) =>
  b <= 256 ? "0-256B" : b <= 1024 ? "256B-1KB" : b <= 4096 ? "1KB-4KB" : b <= 8192 ? "4KB-8KB" : b <= 32768 ? "8KB-32KB" : ">32KB";

type SummaryOptions = {
  stepMs?: number;
  /** Keep only the top K values of each `all_fields` field, flagging the cut fields. */
  topK?: number;
  /** Extra `field_totals` rows, as for custom columns. */
  customFields?: string[];
  binsCut?: boolean;
};

/**
 * A v3 summary of these rows as `summary.rs` writes the rows /security reads:
 * live rows only, per-minute `time_series`, step `bins`, `field_totals`,
 * `all_fields` and the `tls_ciphers_sha` IP sketches.
 */
function summaryParquet(rows: Row[], options: SummaryOptions = {}): ArrayBuffer {
  const { stepMs = 300_000, topK = Infinity, customFields = [], binsCut = false } = options;
  const live = rows.filter((r) => r.ts !== null);
  type Out = [string, number, string | null, string | null, boolean | null, number | null, number | null, Uint8Array | null];
  const out: Out[] = [["overview", T0, null, null, null, null, live.length, null]];
  const add = <K>(map: Map<K, number>, key: K) => map.set(key, (map.get(key) ?? 0) + 1);
  const minutes = new Map<number, number>();
  const bins = new Map<string, number>();
  for (const r of live) {
    add(minutes, Math.floor(r.ts! / 60_000) * 60_000);
    const step = Math.floor(r.ts! / stepMs) * stepMs;
    if (r.hdr !== null) add(bins, JSON.stringify(["req_header_size", step, headerBin(r.hdr)]));
    if (r.waf && r.waf.toUpperCase().includes("VERIFIED-BOT.")) {
      add(bins, JSON.stringify(["verified_bot", step, r.waf.replaceAll("VERIFIED-BOT.", "")]));
    }
  }
  for (const [minute, n] of minutes) out.push(["time_series", minute, null, null, null, null, n, null]);
  for (const [key, n] of bins) {
    const [field, step, value] = JSON.parse(key) as [string, number, string];
    out.push(["bins", step, field, value, null, null, n, null]);
  }
  out.push(["dim_totals", T0, "req_header_size", "bins", binsCut, null, null, null]);
  const fields: [string, (r: Row) => string | null][] = [
    ["ip", (r) => r.ip],
    ["tls_ciphers_sha", (r) => r.tls],
    ["conn_requests", (r) => (r.conn === null ? null : String(r.conn))],
    ["is_ipv6", (r) => (r.v6 === null ? null : String(r.v6))],
    ["p_type", (r) => r.ptype],
  ];
  for (const [field, get] of fields) {
    const counts = new Map<string, number>();
    const sketches = new Map<string, Hll>();
    for (const r of live) {
      const value = get(r);
      if (value === null) continue;
      add(counts, value);
      if (field === "tls_ciphers_sha" && r.ip !== null) {
        const hll = sketches.get(value) ?? new Hll();
        hll.add(r.ip);
        sketches.set(value, hll);
      }
    }
    const kept = [...counts].sort((a, b) => b[1] - a[1] || (a[0] < b[0] ? -1 : 1)).slice(0, topK);
    out.push(["field_totals", T0, field, null, kept.length < counts.size, [...counts.values()].reduce((s, n) => s + n, 0), null, null]);
    for (const [value, n] of kept) out.push(["all_fields", T0, field, value, null, n, null, null]);
    for (const [value, hll] of sketches) out.push(["all_fields_ip", T0, field, value, null, null, null, hll.toBytes()]);
  }
  for (const field of customFields) out.push(["field_totals", T0, field, null, false, live.length, null, null]);
  return parquetWriteBuffer({
    columnData: [
      { name: "bundle", type: "STRING", data: out.map((r) => r[0]) },
      { name: "bucket", type: "TIMESTAMP", data: out.map((r) => new Date(r[1])) },
      { name: "field", type: "STRING", data: out.map((r) => r[2]) },
      { name: "value", type: "STRING", data: out.map((r) => r[3]) },
      { name: "truncated", type: "BOOLEAN", data: out.map((r) => r[4]) },
      { name: "count", type: "INT64", data: out.map((r) => (r[5] === null ? null : BigInt(r[5]))) },
      { name: "requests", type: "INT64", data: out.map((r) => (r[6] === null ? null : BigInt(r[6]))) },
      { name: "ip_sketch", type: "BYTE_ARRAY", data: out.map((r) => r[7]) },
    ],
    kvMetadata: [{ key: "flp_summary", value: "3" }],
  });
}

const WINDOW_ROWS = syntheticRows(RUST_RAW);
const hourRows = (h: number) => WINDOW_ROWS.filter((_, w) => Date.parse(RUST_RAW[w].start) < T0 + (h + 1) * HOUR &&
  Date.parse(RUST_RAW[w].start) >= T0 + h * HOUR).flat();
const SYNTH: Record<string, ArrayBuffer> = {
  ...Object.fromEntries(RUST_RAW.map((file, w) => [file.key, rawParquet(WINDOW_ROWS[w])])),
  ...Object.fromEntries(RUST_WINDOWS.map((file, w) => [file.key, summaryParquet(WINDOW_ROWS[w])])),
  ...Object.fromEntries(RUST_HOURS.map((file, h) => [file.key, summaryParquet(hourRows(h))])),
  [RUST_DAY[0].key]: summaryParquet(WINDOW_ROWS.flat(), { stepMs: HOUR }),
};

async function rollupOf(list: ManifestFile[], buffers = SYNTH): Promise<SecurityRollup> {
  const files = await Promise.all(list.map(async (f) => ({ key: f.key, summary: (await decodeSecuritySummary(buffers[f.key]))! })));
  return { files, dataFromMs: T0, dataThroughMs: T1 };
}

const SECTIONS = [...SECURITY_ROLLUP_SECTIONS];
const body = (extra: Record<string, unknown> = {}) => ({ start_time: START, end_time: END, sections: SECTIONS, ...extra });

async function rawAnswer(buffers: Record<string, ArrayBuffer>, raw: ManifestFile[], sections: string[], bucketSeconds = 300) {
  const engine = new ParquetEngine();
  const lease = engine.lease([]);
  try {
    for (const file of raw) {
      await lease.ingestFile(file.key, `${file.key}.parquet`, buffers[file.key], undefined, undefined, "request", { deferView: true });
    }
    await lease.publishView();
    return await executeSecurityAggregates({
      start_time: START, end_time: END, bucket_seconds: bucketSeconds, sections: sections as SecuritySectionName[],
    }, lease.readContext());
  } finally {
    lease.release();
    await engine.reset();
  }
}

/** Raw and rollup answers agree exactly, but fingerprint IP counts within HLL error. */
function expectParity(got: SecurityAggregatesResponse | null, want: SecurityAggregatesResponse) {
  expect(got).not.toBeNull();
  const { rollup: _meta, tls_fingerprints: gotFp, ...rest } = got as SecurityAggregatesResponse & { rollup: unknown };
  const { tls_fingerprints: wantFp, ...wantRest } = want;
  expect(rest).toEqual(wantRest);
  expect(gotFp?.map(({ ip_count: _, ...r }) => r)).toEqual(wantFp?.map(({ ip_count: _, ...r }) => r));
  gotFp?.forEach((row, i) => expect(Math.abs(row.ip_count - wantFp![i].ip_count)).toBeLessThanOrEqual(1));
}

describe("security rollup vs raw DuckDB over the same rows", () => {
  let want: SecurityAggregatesResponse;
  beforeAll(async () => {
    want = await rawAnswer(SYNTH, RUST_RAW, [...SECTIONS], 3600);
  }, 60_000);

  it("covers every section with data, so a parity check can fail", () => {
    expect(want.tls_fingerprints).toHaveLength(20);
    expect(want.fingerprint_coverage!.tls_ciphers_sha).toBeGreaterThan(0);
    expect(want.req_size_dist).toHaveLength(6);
    expect(want.conn_reuse_dist).toHaveLength(5);
    expect(want.ipv6_adoption).toHaveLength(2);
    expect(want.proxy_dist).toHaveLength(4);
    expect(new Set(want.verified_bots_ts!.map((p) => p.bot_type)))
      .toEqual(new Set(["googlebot", "verified-bot.Bingbot", "Search duckbot"]));
    expect(want.ngwaf_configured).toBe(false);
  });

  it("matches every summary-backed section from windows and hour bundles", async () => {
    for (const list of [RUST_WINDOWS, RUST_HOURS]) {
      expectParity(securityAggregatesFromRollup(await rollupOf(list), body({ bucket_seconds: 3600 })), want);
    }
  });

  it("matches all but the hourly IPv6 share from the day bundle, and refuses that", async () => {
    const day = await rollupOf(RUST_DAY);
    const { ipv6_adoption: _, ...rest } = want;
    const sections = SECTIONS.filter((s) => s !== "ipv6_adoption");
    expectParity(securityAggregatesFromRollup(day, body({ sections, bucket_seconds: 3600 })), rest);
    expect(securityAggregatesFromRollup(day, body())).toBeNull();
  });

  it("matches the 5-minute verified-bot series from windows and refuses buckets the summaries can't tile", async () => {
    const raw = await rawAnswer(SYNTH, RUST_RAW, ["verified_bots_ts"], 600);
    const windows = await rollupOf(RUST_WINDOWS);
    expect(securityAggregatesFromRollup(windows, body({ sections: ["verified_bots_ts"], bucket_seconds: 600 }))?.verified_bots_ts)
      .toEqual(raw.verified_bots_ts);
    for (const bucket of [450, 7 * 3600]) {
      expect(securityAggregatesFromRollup(windows, body({ sections: ["verified_bots_ts"], bucket_seconds: bucket }))).toBeNull();
    }
    expect(securityAggregatesFromRollup(await rollupOf(RUST_DAY), body({ sections: ["verified_bots_ts"], bucket_seconds: 600 })))
      .toBeNull();
  });
});

describe("securityAggregatesFromRollup", () => {
  it("marks fingerprint IP counts approximate, and nothing a lower bound when no field was cut", async () => {
    const out = securityAggregatesFromRollup(await rollupOf(RUST_WINDOWS), body());
    expect(out?.rollup).toEqual({
      data_from: START, data_through: END, approximate: ["tls_fingerprints.ip_count"], lower_bound: [],
    });
  });

  it("marks counts of cut fields lower bounds, and refuses coverage once '' may have been cut", async () => {
    const noEmpty = WINDOW_ROWS[0].map((r) => (r.tls === "" ? { ...r, tls: null } : r));
    const cut = { [RUST_WINDOWS[0].key]: summaryParquet(noEmpty, { topK: 3, binsCut: true }) };
    const rollup = await rollupOf(RUST_WINDOWS, { ...SYNTH, ...cut });
    expect(securityAggregatesFromRollup(rollup, body())).toBeNull();
    const sections = SECTIONS.filter((s) => s !== "tls_fingerprints" && s !== "fingerprint_coverage");
    expect(securityAggregatesFromRollup(rollup, body({ sections }))?.rollup.lower_bound)
      .toEqual(["req_size_dist", "conn_reuse_dist", "proxy_dist"]);
    // tls_ciphers_sha keeps '' among its top 3 when '' is common enough.
    const rows = WINDOW_ROWS[0].map((r, i) => ({ ...r, tls: i % 3 === 0 ? "" : r.tls }));
    const keptEmpty = await rollupOf([RUST_WINDOWS[0]], { [RUST_WINDOWS[0].key]: summaryParquet(rows, { topK: 3 }) });
    expect(securityAggregatesFromRollup(keptEmpty, body({ sections: ["tls_fingerprints"] }))?.rollup.lower_bound)
      .toEqual(["tls_fingerprints.request_count"]);
  });

  it("refuses custom proxy_type and client_ip columns, which raw reads instead of p_type and ip", async () => {
    for (const [field, section] of [["proxy_type", "proxy_dist"], ["client_ip", "tls_fingerprints"]]) {
      const custom = { [RUST_WINDOWS[2].key]: summaryParquet(WINDOW_ROWS[2], { customFields: [field] }) };
      const rollup = await rollupOf(RUST_WINDOWS, { ...SYNTH, ...custom });
      expect(securityAggregatesFromRollup(rollup, body({ sections: [section] })), field).toBeNull();
      expect(securityAggregatesFromRollup(rollup, body({ sections: ["req_size_dist"] }))).not.toBeNull();
    }
  });

  it("refuses the IPv6 share when a window's requests span two hours", async () => {
    const rows = WINDOW_ROWS[3].map((r, i) => (i === 0 ? { ...r, ts: T0 + HOUR + 60_000 } : r));
    const rollup = await rollupOf(RUST_WINDOWS, { ...SYNTH, [RUST_WINDOWS[3].key]: summaryParquet(rows) });
    expect(securityAggregatesFromRollup(rollup, body({ sections: ["ipv6_adoption"] }))).toBeNull();
  });

  it("refuses a connection count that isn't a plain integer", async () => {
    const rows = WINDOW_ROWS[1].map((r, i) => (i === 0 ? { ...r, conn: 2.5 } : r));
    const rollup = await rollupOf(RUST_WINDOWS, { ...SYNTH, [RUST_WINDOWS[1].key]: summaryParquet(rows) });
    expect(securityAggregatesFromRollup(rollup, body({ sections: ["conn_reuse_dist"] }))).toBeNull();
  });

  it("answers only the requested sections, with their raw companions", async () => {
    const out = securityAggregatesFromRollup(await rollupOf(RUST_WINDOWS), body({ sections: ["tls_fingerprints", "ngwaf_verified_bots"] }));
    expect(Object.keys(out!).sort()).toEqual([
      "fingerprint_coverage", "ngwaf_configured", "ngwaf_verified_bots", "ngwaf_verified_bots_ts", "rollup", "tls_fingerprints",
    ]);
  });
});

describe("IPv6 share from Rust-written summaries", () => {
  it("matches raw from windows and hour bundles, and leaves a day-bundle-only range to raw", async () => {
    const want = await rawAnswer(RUST.buffers, RUST_RAW, ["ipv6_adoption"]);
    expect(want.ipv6_adoption).toHaveLength(2);
    for (const list of [RUST_WINDOWS, RUST_HOURS]) {
      const got = securityAggregatesFromRollup(await rollupOf(list, RUST.buffers), body({ sections: ["ipv6_adoption"] }));
      expect(got?.ipv6_adoption).toEqual(want.ipv6_adoption);
    }
    const dayOnly = fixtureSource([fixturePage(RUST_RAW)], RUST.buffers, { json: [], files: [] }, [fixturePage(RUST_DAY)]);
    expect(await securityFromRollup(PATH, body({ sections: ["ipv6_adoption"] }), dayOnly)).toBeNull();
    expect(await securityFromRollup(PATH, body({ sections: ["proxy_dist"] }), dayOnly)).not.toBeNull();
    expect(dayOnly.log.files).toEqual(RUST_DAY.map((f) => f.key));
  }, 60_000);
});

describe("securityFromRollup", () => {
  const listed = [...RUST_WINDOWS, ...RUST_DAY];
  const source = () => fixtureSource([fixturePage(RUST_RAW)], SYNTH, { json: [], files: [] }, [fixturePage(listed)]);

  it("answers unfiltered reads from summaries alone, without day bundles when IPv6 is asked", async () => {
    const s = source();
    const served = await securityFromRollup(PATH, body(), s);
    expect(served).toEqual(securityAggregatesFromRollup(await rollupOf(RUST_WINDOWS), body()));
    expect(s.log.files.sort()).toEqual(RUST_WINDOWS.map((f) => f.key).sort());
    const noIpv6 = source();
    const sections = SECTIONS.filter((x) => x !== "ipv6_adoption");
    expect(await securityFromRollup("/api/security/bundle", body({ sections, bucket_seconds: 3600, filters: { pop: { values: [] } } }), noIpv6))
      .not.toBeNull();
    expect(noIpv6.log.files).toEqual(RUST_DAY.map((f) => f.key));
  });

  it("leaves filtered reads, row-level sections, unnamed sections, bad ranges and other routes to the raw scan", async () => {
    for (const params of [
      body({ filters: { pop: { values: ["IAD"], mode: "include" } } }),
      body({ filters: { country: ["US"] } }),
      body({ sections: [...SECTIONS, "wellknown_bots"] }),
      body({ sections: ["bot_efficiency"] }),
      body({ sections: ["top_ips_header"] }),
      body({ sections: ["proxies"] }),
      body({ sections: ["top_bots"] }),
      body({ sections: undefined }),
      body({ sections: [] }),
      body({ start_time: undefined }),
      body({ end_time: START }),
    ]) {
      expect(securityRollupEligible(PATH, params) && await securityFromRollup(PATH, params, source()), JSON.stringify(params))
        .toBeFalsy();
    }
    for (const path of ["/api/security/proxies", "/api/security/threat-intel", "/api/security/top-bots"]) {
      expect(securityRollupEligible(path, body())).toBe(false);
    }
  });
});

describe("securityFromRollup on projected summaries", () => {
  const LIMITS = { ...summaryProjectionLimits };
  // The fixtures are a few KiB and mostly footer, which a projection would otherwise read whole.
  beforeAll(() => Object.assign(summaryProjectionLimits, { wholeBelowBytes: 0, wholeAboveShare: Infinity }));
  afterAll(() => Object.assign(summaryProjectionLimits, LIMITS));

  const NO_TLS = SECTIONS.filter((section) => section !== "tls_fingerprints");
  // One window's bins cut to their top K, so `dim_totals` is read.
  const CUT = { ...SYNTH, [RUST_WINDOWS[0].key]: summaryParquet(WINDOW_ROWS[0], { binsCut: true }) };
  type Listing = { listed: ManifestFile[]; buffers: Record<string, ArrayBuffer> };
  const listing = async (list: ManifestFile[], layout: boolean, buffers = CUT): Promise<Listing> => {
    const out = { ...buffers };
    const listed = await Promise.all(list.map(async (file) => {
      if (layout) out[file.key] = await withSummaryLayout(buffers[file.key]);
      return { ...file, size: out[file.key].byteLength };
    }));
    return { listed, buffers: out };
  };
  const ANSWERS = [
    { path: PATH, body: body() },
    { path: PATH, body: body({ sections: NO_TLS }) },
    { path: "/api/security/bundle", body: body({ sections: SECTIONS.filter((x) => x !== "ipv6_adoption"), bucket_seconds: 3600 }) },
    { path: PATH, body: body({ sections: ["tls_fingerprints"] }) },
  ];
  const run = async ({ listed, buffers }: Listing, path: string, params: ReturnType<typeof body>, spec?: SummarySpec | null) => {
    const source = projectingSource(fixtureSource([fixturePage(RUST_RAW)], buffers, { json: [], files: [] }, [fixturePage(listed)]), buffers);
    // The column names of each stored projection; these files fit the footer probe, so ranges can't tell.
    const projections: string[][] = [];
    const write = source.writeDerivedFile;
    source.writeDerivedFile = async (key, identity, bytes, meta) => {
      const lengths = (JSON.parse(meta) as { parts?: number[] }).parts;
      let offset = 0;
      for (const length of lengths ?? []) {
        projections.push(parquetMetadata(bytes.slice(offset, (offset += length))).schema.slice(1).map((f) => f.name));
      }
      return write(key, identity, bytes, meta);
    };
    const value = await securityFromRollup(path, params, source, undefined, spec);
    return { value, files: source.log.files, projections };
  };

  it.each([false, true])("answers as from whole files, reading the IP sketches only for the TLS fingerprints (layout %s)", async (layout) => {
    for (const list of [[...RUST_WINDOWS, ...RUST_DAY], RUST_HOURS]) {
      const files = await listing(list, layout);
      for (const { path, body: params } of ANSWERS) {
        const whole = await run(files, path, params, null);
        expect(whole.value, JSON.stringify(params.sections)).not.toBeNull();
        const projected = await run(files, path, params);
        expect(projected.value, JSON.stringify(params.sections)).toEqual(whole.value);
        expect(projected.files).toEqual([]);
        expect(projected.projections.length).toBeGreaterThan(0);
        expect(projected.projections.some((columns) => columns.includes("ip_sketch")), JSON.stringify(params.sections))
          .toBe(params.sections.includes("tls_fingerprints"));
      }
    }
  });

  it.each([
    ["with", SECURITY_TLS_SUMMARY_SPEC, SECTIONS],
    ["without", SECURITY_SUMMARY_SPEC, NO_TLS],
  ] as const)("needs every group and column of its spec %s the TLS fingerprints", async (_name, spec, sections) => {
    const files = await listing(RUST_WINDOWS, false);
    const whole = await run(files, PATH, body({ sections }), null);
    const only = Object.fromEntries([...spec.only].map(([column, of]) => [column, [...of]]));
    const BASE = new Set(["bundle", "bucket", "field", "value", "truncated"]);
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
      const got = await run(files, PATH, body({ sections }), without).catch((error) => ({ value: String(error) }));
      try {
        expect(got.value).toEqual(whole.value);
        unchanged.push(name);
      } catch {
        // Changed, as it must.
      }
    }
    expect(unchanged).toEqual([]);
  });
});
