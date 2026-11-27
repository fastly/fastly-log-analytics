// @vitest-environment node
import { parquetMetadata } from "hyparquet";
import { parquetWriteBuffer } from "hyparquet-writer";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { type DuckDBReadContext, DuckDBAnalyticalEngine as ParquetEngine } from "./dashboard/duckdb";
import { Hll } from "./dashboard/hll";
import type { ManifestFile } from "./dashboard/manifest";
import { FIXTURE_START, fixturePage, loadSummaryFixtures } from "./dashboard/summary-fixtures";
import { MissingCloudFileError } from "./dashboard/file-errors";
import { editSummaryRows, withSummaryLayout } from "./dashboard/summary-layout-fixture";
import { defineSummarySpec, summaryProjectionLimits, type SummarySpec } from "./dashboard/summary-projection";
import { fixtureSource, projectingSource } from "./dashboard/test-fixtures";
import { SECURITY_ROLLUP_SECTIONS } from "./rollup-sections";
import {
  executeSecurityAggregates,
  executeSecurityThreatIntel,
  type SecurityAggregatesResponse,
  type SecuritySectionName,
} from "./security";
import {
  decodeSecuritySummary,
  SECURITY_SUMMARY_SPEC,
  SECURITY_THREAT_INTEL_SUMMARY_SPEC,
  SECURITY_TLS_SUMMARY_SPEC,
  securitySummarySpec,
  type SecurityRollup,
  securityAggregatesFromRollup,
  securityFromRollup,
  securityRollupEligible,
  threatIntelFromRollup,
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

/** The sections the `all_fields`, `bins` and `time_series` rows answer, which these synthetic summaries carry. */
const SECTIONS: SecuritySectionName[] = [
  "ngwaf_verified_bots", "ngwaf_verified_bots_ts", "tls_fingerprints", "fingerprint_coverage", "req_size_dist",
  "ipv6_adoption", "proxy_dist", "conn_reuse_dist", "verified_bots_ts",
];
/** The sections the Rust-written `header_ip`, `bot_ua`, `proxy` rows and the `ua` and `_ref_stem` fields answer. */
const ROW_SECTIONS: SecuritySectionName[] = ["top_ips_header", "wellknown_bots", "bot_efficiency", "top_bots", "proxies"];
const body = (extra: Record<string, unknown> = {}) => ({ start_time: START, end_time: END, sections: SECTIONS, ...extra });

async function withRaw<T>(buffers: Record<string, ArrayBuffer>, raw: ManifestFile[], run: (read: DuckDBReadContext) => Promise<T>) {
  const engine = new ParquetEngine();
  const lease = engine.lease([]);
  try {
    for (const file of raw) {
      await lease.ingestFile(file.key, `${file.key}.parquet`, buffers[file.key], undefined, undefined, "request", { deferView: true });
    }
    await lease.publishView();
    return await run(lease.readContext());
  } finally {
    lease.release();
    await engine.reset();
  }
}

function rawAnswer(buffers: Record<string, ArrayBuffer>, raw: ManifestFile[], sections: string[], bucketSeconds = 300, maskIps = false) {
  return withRaw(buffers, raw, (read) => executeSecurityAggregates({
    start_time: START, end_time: END, bucket_seconds: bucketSeconds, sections: sections as SecuritySectionName[], mask_ips: maskIps,
  }, read));
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
      expectParity(await securityAggregatesFromRollup(await rollupOf(list), body({ bucket_seconds: 3600 })), want);
    }
  });

  it("matches all but the hourly IPv6 share from the day bundle, and refuses that", async () => {
    const day = await rollupOf(RUST_DAY);
    const { ipv6_adoption: _, ...rest } = want;
    const sections = SECTIONS.filter((s) => s !== "ipv6_adoption");
    expectParity(await securityAggregatesFromRollup(day, body({ sections, bucket_seconds: 3600 })), rest);
    expect(await securityAggregatesFromRollup(day, body())).toBeNull();
  });

  it("matches the 5-minute verified-bot series from windows and refuses buckets the summaries can't tile", async () => {
    const raw = await rawAnswer(SYNTH, RUST_RAW, ["verified_bots_ts"], 600);
    const windows = await rollupOf(RUST_WINDOWS);
    expect((await securityAggregatesFromRollup(windows, body({ sections: ["verified_bots_ts"], bucket_seconds: 600 })))?.verified_bots_ts)
      .toEqual(raw.verified_bots_ts);
    for (const bucket of [450, 7 * 3600]) {
      expect(await securityAggregatesFromRollup(windows, body({ sections: ["verified_bots_ts"], bucket_seconds: bucket }))).toBeNull();
    }
    expect(await securityAggregatesFromRollup(await rollupOf(RUST_DAY), body({ sections: ["verified_bots_ts"], bucket_seconds: 600 })))
      .toBeNull();
  });
});

describe("securityAggregatesFromRollup", () => {
  it("marks fingerprint IP counts approximate, and nothing a lower bound when no field was cut", async () => {
    const out = await securityAggregatesFromRollup(await rollupOf(RUST_WINDOWS), body());
    expect(out?.rollup).toEqual({
      data_from: START, data_through: END, approximate: ["tls_fingerprints.ip_count"], lower_bound: [],
    });
  });

  it("marks counts of cut fields lower bounds, and refuses coverage once '' may have been cut", async () => {
    const noEmpty = WINDOW_ROWS[0].map((r) => (r.tls === "" ? { ...r, tls: null } : r));
    const cut = { [RUST_WINDOWS[0].key]: summaryParquet(noEmpty, { topK: 3, binsCut: true }) };
    const rollup = await rollupOf(RUST_WINDOWS, { ...SYNTH, ...cut });
    expect(await securityAggregatesFromRollup(rollup, body())).toBeNull();
    const sections = SECTIONS.filter((s) => s !== "tls_fingerprints" && s !== "fingerprint_coverage");
    expect((await securityAggregatesFromRollup(rollup, body({ sections })))?.rollup.lower_bound)
      .toEqual(["req_size_dist", "conn_reuse_dist", "proxy_dist"]);
    // tls_ciphers_sha keeps '' among its top 3 when '' is common enough.
    const rows = WINDOW_ROWS[0].map((r, i) => ({ ...r, tls: i % 3 === 0 ? "" : r.tls }));
    const keptEmpty = await rollupOf([RUST_WINDOWS[0]], { [RUST_WINDOWS[0].key]: summaryParquet(rows, { topK: 3 }) });
    expect((await securityAggregatesFromRollup(keptEmpty, body({ sections: ["tls_fingerprints"] })))?.rollup.lower_bound)
      .toEqual(["tls_fingerprints.request_count"]);
  });

  it("refuses custom proxy_type and client_ip columns, which raw reads instead of p_type and ip", async () => {
    for (const [field, section] of [["proxy_type", "proxy_dist"], ["client_ip", "tls_fingerprints"]]) {
      const custom = { [RUST_WINDOWS[2].key]: summaryParquet(WINDOW_ROWS[2], { customFields: [field] }) };
      const rollup = await rollupOf(RUST_WINDOWS, { ...SYNTH, ...custom });
      expect(await securityAggregatesFromRollup(rollup, body({ sections: [section] })), field).toBeNull();
      expect(await securityAggregatesFromRollup(rollup, body({ sections: ["req_size_dist"] }))).not.toBeNull();
    }
  });

  it("refuses the IPv6 share when a window's requests span two hours", async () => {
    const rows = WINDOW_ROWS[3].map((r, i) => (i === 0 ? { ...r, ts: T0 + HOUR + 60_000 } : r));
    const rollup = await rollupOf(RUST_WINDOWS, { ...SYNTH, [RUST_WINDOWS[3].key]: summaryParquet(rows) });
    expect(await securityAggregatesFromRollup(rollup, body({ sections: ["ipv6_adoption"] }))).toBeNull();
  });

  it("refuses a connection count that isn't a plain integer", async () => {
    const rows = WINDOW_ROWS[1].map((r, i) => (i === 0 ? { ...r, conn: 2.5 } : r));
    const rollup = await rollupOf(RUST_WINDOWS, { ...SYNTH, [RUST_WINDOWS[1].key]: summaryParquet(rows) });
    expect(await securityAggregatesFromRollup(rollup, body({ sections: ["conn_reuse_dist"] }))).toBeNull();
  });

  it("answers only the requested sections, with their raw companions", async () => {
    const out = await securityAggregatesFromRollup(await rollupOf(RUST_WINDOWS), body({ sections: ["tls_fingerprints", "ngwaf_verified_bots"] }));
    expect(Object.keys(out!).sort()).toEqual([
      "fingerprint_coverage", "ngwaf_configured", "ngwaf_verified_bots", "ngwaf_verified_bots_ts", "rdns_verification_available", "rollup", "tls_fingerprints",
    ]);
  });
});

describe("IPv6 share from Rust-written summaries", () => {
  it("matches raw from windows and hour bundles, and leaves a day-bundle-only range to raw", async () => {
    const want = await rawAnswer(RUST.buffers, RUST_RAW, ["ipv6_adoption"]);
    expect(want.ipv6_adoption).toHaveLength(2);
    for (const list of [RUST_WINDOWS, RUST_HOURS]) {
      const got = await securityAggregatesFromRollup(await rollupOf(list, RUST.buffers), body({ sections: ["ipv6_adoption"] }));
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
    expect(served).toEqual(await securityAggregatesFromRollup(await rollupOf(RUST_WINDOWS), body()));
    expect(s.log.files.sort()).toEqual(RUST_WINDOWS.map((f) => f.key).sort());
    const noIpv6 = source();
    const sections = SECTIONS.filter((x) => x !== "ipv6_adoption");
    expect(await securityFromRollup("/api/security/bundle", body({ sections, bucket_seconds: 3600, filters: { pop: { values: [] } } }), noIpv6))
      .not.toBeNull();
    expect(noIpv6.log.files).toEqual(RUST_DAY.map((f) => f.key));
  });

  it("plans around a listed day bundle whose object is gone, exactly from the windows", async () => {
    const s = source();
    const fetchFile = s.fetchFile;
    let goneReads = 0;
    s.fetchFile = async (key, signal, options) => {
      if (key === RUST_DAY[0].key && ++goneReads) throw new MissingCloudFileError(key);
      return fetchFile(key, signal, options);
    };
    const sections = SECTIONS.filter((x) => x !== "ipv6_adoption");
    const params = body({ sections, bucket_seconds: 3600 });
    expect(await securityFromRollup(PATH, params, s))
      .toEqual(await securityAggregatesFromRollup(await rollupOf(RUST_WINDOWS), params));
    expect(goneReads).toBe(1);
  });

  it("leaves filtered reads, unknown or unnamed sections, bad ranges and other routes to the raw scan", async () => {
    for (const params of [
      body({ filters: { pop: { values: ["IAD"], mode: "include" } } }),
      body({ filters: { country: ["US"] } }),
      body({ sections: [...SECTIONS, "unknown"] }),
      body({ sections: undefined }),
      body({ sections: [] }),
      body({ start_time: undefined }),
      body({ end_time: START }),
    ]) {
      expect(securityRollupEligible(PATH, params) && await securityFromRollup(PATH, params, source()), JSON.stringify(params))
        .toBeFalsy();
    }
    for (const path of ["/api/security/proxies", "/api/security/top-bots", "/api/security/proxies/export"]) {
      expect(securityRollupEligible(path, body())).toBe(false);
    }
    expect(securityRollupEligible("/api/security/threat-intel", { start_time: START, end_time: END })).toBe(true);
    expect(securityRollupEligible("/api/security/threat-intel", body({ filters: { pop: { values: ["IAD"] } } }))).toBe(false);
  });
});

/** Rust-written summaries of rows with user agents, referers, header sizes, RTT minimums, JA3/JA4 and countries. */
const SV = loadSummaryFixtures("security-value-summaries");
const SV_RAW = SV.files("raw");
const SV_LISTS: [string, ManifestFile[]][] = [
  ["windows", SV.files("window")],
  ["hours", SV.files("hour", (e) => e.key.includes("-c4-"))],
  ["day", SV.files("day")],
];

type Summary = SecurityRollup["files"][number]["summary"];

/** The rollup with its first file's totals and group sets copied and patched. */
function patchFirst(rollup: SecurityRollup, patch: (summary: Summary) => void): SecurityRollup {
  return {
    ...rollup,
    files: rollup.files.map(({ key, summary }, i) => {
      if (i > 0) return { key, summary };
      const copy = { ...summary, totals: new Map(summary.totals), groups: new Set(summary.groups),
        groupsCut: new Set(summary.groupsCut) };
      patch(copy);
      return { key, summary: copy };
    }),
  };
}

const byKey = <T>(rows: T[] | undefined, key: (row: T) => string) =>
  [...rows ?? []].sort((a, b) => (key(a) < key(b) ? -1 : key(a) > key(b) ? 1 : 0));

/** Rows ordered by value, so ties a raw `ORDER BY ... LIMIT` breaks arbitrarily compare equal. */
function rowSections(out: SecurityAggregatesResponse) {
  return {
    wellknown_bots: byKey(out.wellknown_bots, (r) => r.id),
    bot_efficiency: byKey(out.bot_efficiency, (r) => r.bot_name)
      .map(({ referrals: _r, requests_per_referral: _q, bytes_per_referral: _b, ...rest }) => rest),
    bots: byKey(out.bots, (r) => r.id),
    ngwaf_bots: out.ngwaf_bots,
    active_proxies_count: out.active_proxies_count,
    tunnel_requests_count: out.tunnel_requests_count,
    distance_mismatches_count: out.distance_mismatches_count,
    traffic_quality: byKey(out.traffic_quality, (r) => r.label),
    suspicious_isps: byKey(out.suspicious_isps, (r) => `${r.isp}|${r.asn}`),
    // Clients past the first 100 by risk then distance are cut, so ties there keep either row.
    active_clients: (out.active_clients ?? []).map((r) => `${r.risk_level}|${r.distance_km}`).sort(),
  };
}

/** The top header IPs agree on their sizes, and on the IPs above the last (possibly tied) size. */
function expectTopIps(got: SecurityAggregatesResponse["top_ips_header"], want: SecurityAggregatesResponse["top_ips_header"]) {
  expect(got!.map((r) => r.max_header)).toEqual(want!.map((r) => r.max_header));
  const last = want!.at(-1)!.max_header;
  const above = (rows: typeof want) => rows!.filter((r) => r.max_header > last).map((r) => r.ip).sort();
  expect(above(got)).toEqual(above(want));
}

describe("security row sections vs raw DuckDB over the same rows", () => {
  let want: SecurityAggregatesResponse;
  let masked: SecurityAggregatesResponse;
  beforeAll(async () => {
    want = await rawAnswer(SV.buffers, SV_RAW, ROW_SECTIONS);
    masked = await rawAnswer(SV.buffers, SV_RAW, ["top_ips_header", "proxies"], 300, true);
  }, 60_000);

  it("covers every row section with data, so a parity check can fail", () => {
    expect(want.top_ips_header).toHaveLength(10);
    expect(want.wellknown_bots!.length).toBeGreaterThan(2);
    expect(want.bot_efficiency!.length).toBeGreaterThan(1);
    expect(want.bot_efficiency!.some((r) => r.referrals > 0)).toBe(true);
    expect(want.bots!.length).toBeGreaterThan(2);
    expect(want.active_clients!.length).toBeGreaterThan(0);
    expect(want.suspicious_isps!.length).toBeGreaterThan(0);
    expect(masked.top_ips_header![0].ip).toMatch(/\.xxx$/);
  });

  it("matches every row section from windows, hour and day bundles, referrals within sketch error", async () => {
    for (const [label, list] of SV_LISTS) {
      const got = (await securityAggregatesFromRollup(await rollupOf(list, SV.buffers), body({ sections: ROW_SECTIONS })))!;
      expect(got, label).not.toBeNull();
      expectTopIps(got.top_ips_header, want.top_ips_header);
      // A day bundle keeps 300 of the RTT rows; raw reads 5,000 of them, here every one.
      const { active_proxies_count: _a, tunnel_requests_count: _t, distance_mismatches_count: _d, traffic_quality: _q,
        suspicious_isps: _s, active_clients: _c, ...counts } = rowSections(got);
      const { active_proxies_count: _A, tunnel_requests_count: _T, distance_mismatches_count: _D, traffic_quality: _Q,
        suspicious_isps: _S, active_clients: _C, ...wantCounts } = rowSections(want);
      expect(counts, label).toEqual(wantCounts);
      if (label === "day") {
        expect(got.rollup.approximate, label).toEqual(["bot_efficiency.referrals", "proxies"]);
        expect(got.tunnel_requests_count!).toBeLessThanOrEqual(want.tunnel_requests_count!);
      } else {
        expect(rowSections(got), label).toEqual(rowSections(want));
        expect(got.rollup.approximate, label).toEqual(["bot_efficiency.referrals"]);
      }
      expect(got.rollup.lower_bound, label).toEqual([]);
      const referrals = (out: SecurityAggregatesResponse) => byKey(out.bot_efficiency, (r) => r.bot_name).map((r) => r.referrals);
      referrals(got).forEach((n, i) => expect(Math.abs(n - referrals(want)[i]), label).toBeLessThanOrEqual(1));
    }
  });

  it("masks IPs as the raw scan does", async () => {
    const [, windows] = SV_LISTS[0];
    const got = (await securityAggregatesFromRollup(await rollupOf(windows, SV.buffers),
      body({ sections: ["top_ips_header", "proxies"], mask_ips: true })))!;
    expectTopIps(got.top_ips_header, masked.top_ips_header);
    expect(rowSections(got).active_clients).toEqual(rowSections(masked).active_clients);
    expect(new Set(got.active_clients!.map((r) => r.ip.replace(/\.xxx$/, "")))).toEqual(
      new Set(masked.active_clients!.map((r) => r.ip.replace(/\.xxx$/, ""))));
  });

  it("marks cut user agents lower bounds, and leaves custom client_ip and summaries without the rows to the raw scan", async () => {
    const result = await rollupOf(SV_LISTS[0][1], SV.buffers);
    const files = (patch: (summary: Summary) => void) => patchFirst(result, patch);
    const cut = files((s) => {
      s.groupsCut.add("bot_ua/ua");
      s.totals.set("ua", { ...s.totals.get("ua")!, truncated: true });
    });
    expect((await securityAggregatesFromRollup(cut, body({ sections: ROW_SECTIONS })))!.rollup.lower_bound)
      .toEqual(["wellknown_bots", "bot_efficiency", "top_bots"]);
    const clientIp = files((s) => s.totals.set("client_ip", { count: 1, truncated: false }));
    for (const section of ["top_ips_header", "wellknown_bots", "bot_efficiency", "proxies"]) {
      expect(await securityAggregatesFromRollup(clientIp, body({ sections: [section] })), section).toBeNull();
    }
    expect(await securityAggregatesFromRollup(clientIp, body({ sections: ["top_bots"] }))).not.toBeNull();
    for (const [group, section] of [
      ["header_ip/ip", "top_ips_header"], ["bot_ua/ua", "wellknown_bots"], ["bot_ua/ua", "bot_efficiency"],
      ["proxy/sample", "proxies"],
    ]) {
      expect(await securityAggregatesFromRollup(files((s) => s.groups.delete(group)), body({ sections: [section] })), group)
        .toBeNull();
    }
    expect(await securityAggregatesFromRollup(files((s) => s.totals.delete("_ref_stem")), body({ sections: ["bot_efficiency"] })))
      .toBeNull();
  });

  it("omits unrelated groups and scopes per-group columns in securitySummarySpec for row sections", () => {
    const rawSpec = securitySummarySpec(["wellknown_bots", "bot_efficiency", "top_ips_header", "proxies"]);
    expect(rawSpec.groups.has("all_fields")).toBe(false);
    expect(rawSpec.groups.has("bins")).toBe(false);
    expect(rawSpec.groups.has("time_series")).toBe(false);
    expect([...(rawSpec.only.get("ip_sketch") ?? [])]).toEqual(["all_fields_ip"]);
    expect([...(rawSpec.only.get("value_max") ?? [])]).toEqual(["header_ip"]);
    expect([...(rawSpec.only.get("resp_bytes_sum") ?? [])]).toEqual(["bot_ua"]);
    expect([...(rawSpec.only.get("total_bandwidth_bytes") ?? [])]).toEqual(["overview"]);
    expect([...(rawSpec.only.get("ip") ?? [])]).toEqual(["proxy"]);
  });
});

describe("threat intel from summaries", () => {
  let want: Awaited<ReturnType<typeof executeSecurityThreatIntel>>;
  /** Each fingerprint's countries with the most requests, any of which `MODE` may pick. */
  const modes = new Map<string, string[]>();
  beforeAll(async () => {
    want = await withRaw(SV.buffers, SV_RAW, async (read) => {
      const rows = await read.querySql(`
        WITH c AS (
          SELECT COALESCE(NULLIF(CAST(ja3 AS VARCHAR), ''), NULLIF(CAST(ja4 AS VARCHAR), '')) AS fp,
                 CAST(country AS VARCHAR) AS country, COUNT(*) AS n
          FROM logs WHERE country IS NOT NULL GROUP BY 1, 2)
        SELECT fp, country FROM c WHERE fp IS NOT NULL
          AND n = (SELECT MAX(n) FROM c AS d WHERE d.fp = c.fp)`);
      for (const r of rows) modes.set(String(r.fp), [...modes.get(String(r.fp)) ?? [], String(r.country || "Unknown")]);
      return executeSecurityThreatIntel({ start_time: START, end_time: END }, read);
    });
  }, 60_000);

  /** Rows by fingerprint, each country checked against the tied modes and then dropped. */
  const sorted = (rows: typeof want.data) => byKey(rows, (r) => r.tls_fingerprint).map(({ top_country, ...rest }) => {
    expect(modes.get(rest.tls_fingerprint) ?? ["Unknown"], rest.tls_fingerprint).toContain(top_country);
    return rest;
  });

  it("matches raw from windows, hour and day bundles", async () => {
    expect(want.data.length).toBeGreaterThan(3);
    expect(want.data.some((r) => r.is_anonymized_proxy)).toBe(true);
    expect(want.data.some((r) => !r.is_anonymized_proxy)).toBe(true);
    expect(new Set(want.data.map((r) => r.top_country)).size).toBeGreaterThan(1);
    expect([...modes.values()].some((tied) => tied.length === 1)).toBe(true);
    for (const [label, list] of SV_LISTS) {
      const got = threatIntelFromRollup(await rollupOf(list, SV.buffers))!;
      expect(sorted(got.data), label).toEqual(sorted(want.data));
      expect(got.rollup, label).toEqual({ data_from: START, data_through: END, approximate: [], lower_bound: [] });
    }
  });

  it("marks cut fingerprints and countries, and leaves WAF rule, bot columns and older summaries to the raw scan", async () => {
    const result = await rollupOf(SV_LISTS[0][1], SV.buffers);
    const first = (patch: (summary: Summary) => void) => patchFirst(result, patch);
    expect(threatIntelFromRollup(first((s) => {
      s.groupsCut.add("tls_fp/fp");
      s.groupsCut.add("tls_fp/fp_country");
    }))!.rollup).toMatchObject({ approximate: ["threat_intel.top_country"], lower_bound: ["threat_intel.requests"] });
    for (const field of ["waf_logged_rules", "waf_blocked_rules", "is_bot"]) {
      expect(threatIntelFromRollup(first((s) => s.totals.set(field, { count: 0, truncated: false }))), field).toBeNull();
    }
    expect(threatIntelFromRollup(first((s) => s.groups.delete("tls_fp/fp_country")))).toBeNull();
  });

  it("reads the field totals that name WAF rule columns, which no fixture has, to leave them to the raw scan", async () => {
    const list = SV_LISTS[0][1];
    const buffers = { ...SV.buffers };
    for (const file of list) {
      buffers[file.key] = await editSummaryRows(SV.buffers[file.key], (rows) => {
        const total = rows.find((row) => row.bundle === "field_totals");
        if (!total) throw new Error(`${file.key} has no field totals`);
        return [...rows, { ...total, field: "waf_logged_rules" }];
      });
    }
    const listed = list.map((file) => ({ ...file, size: buffers[file.key].byteLength }));
    const read = (spec: SummarySpec) => {
      const source = fixtureSource([fixturePage(SV_RAW)], buffers, { json: [], files: [] }, [fixturePage(listed)]);
      return securityFromRollup("/api/security/threat-intel", { start_time: START, end_time: END },
        projectingSource(source, buffers), undefined, spec);
    };
    expect(await read(SECURITY_THREAT_INTEL_SUMMARY_SPEC)).toBeNull();
    expect(await read(defineSummarySpec({ groups: ["tls_fp", "dim_totals"], columns: ["count", "requests"] }))).not.toBeNull();
  });

  it("answers the page's unfiltered read from summaries without raw files", async () => {
    const s = fixtureSource([fixturePage(SV_RAW)], SV.buffers, { json: [], files: [] }, [fixturePage(SV_LISTS[0][1])]);
    const served = await securityFromRollup("/api/security/threat-intel", { start_time: START, end_time: END }, s);
    expect(served).toEqual(threatIntelFromRollup(await rollupOf(SV_LISTS[0][1], SV.buffers)));
    expect(s.log.files.every((key) => SV_LISTS[0][1].some((f) => f.key === key))).toBe(true);
    const all = await securityFromRollup(PATH, body({ sections: [...SECURITY_ROLLUP_SECTIONS], bucket_seconds: 3600 }), s);
    expect(all).not.toBeNull();
    expect(s.log.files.every((key) => SV_LISTS[0][1].some((f) => f.key === key))).toBe(true);
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

  it("clears decoded SecuritySummary RAM and aborts in-flight summary projection across clearSecurityRollupCache and clearDashboardData", async () => {
    const secMod = await import("./security-rollup");
    const { clearDashboardData } = await import("./dashboard/client");
    const clearFn = (secMod as Record<string, unknown>).clearSecurityRollupCache as (() => void) | undefined;
    expect(typeof clearFn).toBe("function");

    const { listed, buffers } = await listing(RUST_WINDOWS, true);
    let rangeReads = 0;
    let derivedWrites = 0;
    let pauseFirstDerived = false;
    let notifyPaused!: () => void;
    let releasePaused!: () => void;

    const base = projectingSource(
      fixtureSource([fixturePage(RUST_RAW)], buffers, { json: [], files: [] }, [fixturePage(listed)]),
      buffers,
    );
    const tracked = {
      ...base,
      cacheScope: "alice-sec-scope:svc-alice",
      fetchRange: async (file: ManifestFile, start: number, end: number, sig?: AbortSignal) => {
        sig?.throwIfAborted();
        rangeReads += 1;
        return base.fetchRange(file, start, end, sig);
      },
      readDerivedFile: async (key: string, id: string) => {
        if (pauseFirstDerived) {
          pauseFirstDerived = false;
          notifyPaused?.();
          await new Promise<void>((r) => { releasePaused = r; });
        }
        return base.readDerivedFile?.(key, id) ?? null;
      },
      writeDerivedFile: async () => {
        derivedWrites += 1;
      },
    };

    // 1. Populate decoded RAM, then clearDashboardData() must evict decoded RAM so replay re-fetches:
    const initial = await secMod.querySecurityRollup(tracked, {
      fromMs: T0,
      toMs: T1,
      hourly: false,
      spec: SECURITY_SUMMARY_SPEC,
    });
    expect(initial).not.toBeNull();
    expect(rangeReads).toBeGreaterThan(0);

    rangeReads = 0;
    await clearDashboardData();
    const replay = await secMod.querySecurityRollup(tracked, {
      fromMs: T0,
      toMs: T1,
      hourly: false,
      spec: SECURITY_SUMMARY_SPEC,
    });
    expect(replay).not.toBeNull();
    expect(rangeReads).toBeGreaterThan(0);

    // 2. Caller signal abort mid-projection stops range fetches and derived writes:
    clearFn!();
    rangeReads = 0;
    derivedWrites = 0;
    pauseFirstDerived = true;
    const paused1 = new Promise<void>((r) => { notifyPaused = r; });
    const caller = new AbortController();
    const pendingCallerAbort = secMod.querySecurityRollup(tracked, {
      fromMs: T0,
      toMs: T1,
      hourly: false,
      spec: SECURITY_SUMMARY_SPEC,
      signal: caller.signal,
    }).then(() => "RESOLVED", () => "REJECTED");
    await paused1;
    caller.abort(new DOMException("Caller aborted", "AbortError"));
    releasePaused();
    expect(await pendingCallerAbort).toBe("REJECTED");
    expect(rangeReads).toBe(0);
    expect(derivedWrites).toBe(0);

    // 3. Un-signaled querySecurityRollup aborted by clearSecurityRollupCache() alone:
    pauseFirstDerived = true;
    const paused2 = new Promise<void>((r) => { notifyPaused = r; });
    const pendingClearAbort = secMod.querySecurityRollup(tracked, {
      fromMs: T0,
      toMs: T1,
      hourly: false,
      spec: SECURITY_SUMMARY_SPEC,
    }).then(() => "RESOLVED", () => "REJECTED");
    await paused2;
    clearFn!();
    releasePaused();
    expect(await pendingClearAbort).toBe("REJECTED");
    expect(rangeReads).toBe(0);
    expect(derivedWrites).toBe(0);
  });

  it("rolls up unsummarized residual raw files alongside summaries with full parity", async () => {
    // Drop the summary for the last raw window so active/fresh traffic sits in a raw residual file.
    const partialSummaries = RUST_WINDOWS.slice(0, -1);
    const partialSource = fixtureSource(
      [fixturePage(RUST_RAW)],
      RUST.buffers,
      { json: [], files: [] },
      [fixturePage(partialSummaries)],
    );
    const got = await securityFromRollup(PATH, body({ sections: SECTIONS }), partialSource);
    const raw = await rawAnswer(RUST.buffers, RUST_RAW, SECTIONS);
    expectParity(got as SecurityAggregatesResponse | null, raw);
    expect(partialSource.log.files).toContain(RUST_RAW[RUST_RAW.length - 1].key);
    expect(partialSource.log.files).toContain(partialSummaries[0].key);
  });

  it("answers threat-intel from summaries plus unsummarized residual raw files with parity", async () => {
    const partialSummaries = SV_LISTS[0][1].slice(0, -1);
    const partialSource = fixtureSource(
      [fixturePage(SV_RAW)],
      SV.buffers,
      { json: [], files: [] },
      [fixturePage(partialSummaries)],
    );
    const got = await securityFromRollup("/api/security/threat-intel", body(), partialSource);
    expect(got).not.toBeNull();
    const full = await securityFromRollup("/api/security/threat-intel", body(),
      fixtureSource([fixturePage(SV_RAW)], SV.buffers, { json: [], files: [] }, [fixturePage(SV_LISTS[0][1])]));
    expect(got).toMatchObject({ data: (full as { data: unknown }).data });
  });
});
