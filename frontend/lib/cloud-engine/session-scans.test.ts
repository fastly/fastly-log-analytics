// @vitest-environment node
import { parquetWriteBuffer } from "hyparquet-writer";
import { beforeEach, describe, expect, it } from "vitest";
import { getDuckDBEngine } from "./dashboard/duckdb";
import { handleSessionsDetail, handleSessionsList } from "./sessions";

const engine = getDuckDBEngine();
const start = "2026-11-01T00:00:00Z";
const from = Date.parse(start);
const end = "2026-11-01T02:00:00Z";
beforeEach(async () => { await engine.reset(); });

describe("Cloud session scans against actual Parquet", () => {
  it("sessionizes TIMESTAMPTZ rows at the strict inactivity boundary and exposes stream drill-down", async () => {
    const bytes = parquetWriteBuffer({ columnData: [
      { name: "timestamp", type: "TIMESTAMP", data: [from, from + 1800000, from + 3600001].map((at) => new Date(at)) },
      { name: "ip", type: "STRING", data: ["192.0.2.10", "192.0.2.10", "192.0.2.10"] },
      { name: "cmcd_sid", type: "STRING", data: ["stream", "stream", "stream"] },
      { name: "url", type: "STRING", data: ["/a.m4s", "/b.m4s", "/c.m4s"] },
      { name: "status", type: "INT32", data: [200, 200, 503] },
    ] });
    await engine.syncWindow([{ key: "session-fixture", bytes }]);
    const context = {
      serviceId: "fixture-service", availableColumns: await engine.getPresentColumns(),
      runSql: (sql: string) => engine.executeSql(sql),
    };
    const list = await handleSessionsList({
      start_time: start, end_time: end, sort_by: "session_start", sort_dir: "asc",
    }, context);
    if (!("sessions" in list.body)) throw new Error("Session scan failed");
    expect(list.body.sessions).toMatchObject([
      { req_count: 2, streaming_reqs: 2, is_streaming: true, unique_urls: 2 },
      { req_count: 1, streaming_reqs: 1, is_streaming: true, unique_urls: 1 },
    ]);
    const detail = await handleSessionsDetail({ session_token: list.body.sessions[0].session_token }, context);
    expect(detail.body).toMatchObject({ data: [
      { url: "/a.m4s", timestamp: "2026-11-01T00:00:00.000Z" },
      { url: "/b.m4s", timestamp: "2026-11-01T00:30:00.000Z" },
    ] });
    const singleton = await handleSessionsDetail({ session_token: list.body.sessions[1].session_token }, context);
    expect(singleton.body).toMatchObject({ data: [{ url: "/c.m4s" }] });
  });

  type Row = {
    ts: number; ip: string; ja4: string | null; cmcd_sid: string | null; edge_sid: string | null; edge: boolean | null;
    asn: number | null; country: string | null; status: number | null; resp_bytes: number | null;
    tcp_rtt: number | null; ua: string | null; url: string | null;
  };
  const writeRows = (rows: Row[]) => parquetWriteBuffer({ columnData: [
    { name: "timestamp", type: "TIMESTAMP", data: rows.map((row) => new Date(row.ts)) },
    ...(["ip", "ja4", "cmcd_sid", "edge_sid", "country", "ua", "url"] as const).map((name) =>
      ({ name, type: "STRING" as const, data: rows.map((row) => row[name]) })),
    { name: "edge", type: "BOOLEAN", data: rows.map((row) => row.edge) },
    ...(["asn", "status", "resp_bytes", "tcp_rtt"] as const).map((name) =>
      ({ name, type: "DOUBLE" as const, data: rows.map((row) => row[name]) })),
  ] });
  const minOf = <T,>(values: (T | null)[]) =>
    values.filter((value): value is T => value != null).reduce<T | null>((a, b) => a == null || b < a ? b : a, null);
  // The sessions definition, in plain code: per ip and ja4, a gap of more than 30 minutes starts a new session.
  function referenceSessions(rows: Row[]) {
    const byKey = new Map<string, Row[]>();
    for (const row of rows) byKey.set(`${row.ip}|${row.ja4}`, [...byKey.get(`${row.ip}|${row.ja4}`) ?? [], row]);
    return [...byKey.values()].flatMap((keyed) => {
      const sorted = keyed.sort((a, b) => a.ts - b.ts);
      const sessions: Row[][] = [];
      sorted.forEach((row, i) => {
        if (i === 0 || row.ts - sorted[i - 1].ts > 1_800_000) sessions.push([]);
        sessions[sessions.length - 1].push(row);
      });
      return sessions.map((session) => {
        const rtts = session.map((row) => row.tcp_rtt).filter((rtt): rtt is number => rtt != null).sort((a, b) => a - b);
        const mid = (rtts.length - 1) / 2;
        const reqs4xx = session.filter((row) => row.status != null && row.status >= 400 && row.status < 500).length;
        return {
          ip: session[0].ip, ja4: session[0].ja4,
          session_start: new Date(session[0].ts).toISOString(),
          session_end: new Date(session[session.length - 1].ts).toISOString(),
          req_count: session.length,
          streaming_reqs: session.filter((row) => row.cmcd_sid != null && row.cmcd_sid !== "").length,
          edge_sid: session.map((row) => row.edge_sid).filter((sid) => sid != null).sort().pop() ?? null,
          edge_count: session.filter((row) => row.edge === true).length,
          shield_count: session.filter((row) => row.edge === false).length,
          asn: minOf(session.map((row) => row.asn)), country: minOf(session.map((row) => row.country)),
          reqs_4xx: reqs4xx, reqs_5xx: session.filter((row) => row.status != null && row.status >= 500).length,
          total_bytes: session.reduce((sum, row) => sum + (row.resp_bytes ?? 0), 0),
          median_rtt_ms: rtts.length ? (rtts[Math.floor(mid)] + rtts[Math.ceil(mid)]) / 2 / 1000 : null,
          ua: minOf(session.map((row) => row.ua)),
          unique_urls: new Set(session.map((row) => row.url).filter((url) => url != null)).size,
          flagged: session.length >= 40 || reqs4xx * 100 / session.length >= 25,
        };
      });
    });
  }

  it("matches the inactivity-gap sessions definition for every sort, filter and page", async () => {
    let seed = 7;
    const rand = () => (seed = (seed * 1_103_515_245 + 12_345) % 2_147_483_648) / 2_147_483_648;
    const pick = <T,>(values: T[]) => values[Math.floor(rand() * values.length)];
    const rows: Row[] = [];
    for (const ip of ["192.0.2.1", "192.0.2.2", "192.0.2.3", "198.51.100.7"]) {
      for (const ja4 of ["t13d_a", "t13d_b", null]) {
        let ts = from + Math.floor(rand() * 600_000);
        const count = 5 + Math.floor(rand() * 70);
        for (let i = 0; i < count && ts < Date.parse(end); i++) {
          rows.push({
            ts, ip, ja4,
            cmcd_sid: pick([null, "", "s1", "s2"]), edge_sid: pick([null, "e1", "e9", "e5"]), edge: pick([true, false, null]),
            asn: pick([null, 64_500, 64_501, 13_335]), country: pick([null, "US", "DE", "BR"]),
            status: pick([200, 200, 304, 404, 429, 503, null]), resp_bytes: pick([null, 512, 2048, 90_000]),
            tcp_rtt: pick([null, 9_000, 12_500, 30_000, 31_000]), ua: pick([null, "curl/8", "Mozilla/5.0", "bot"]),
            url: pick([null, "/a", "/b", "/c", "/d"]),
          });
          // Inactivity gaps straddle the strict 30-minute boundary, and same-minute bursts share timestamps.
          ts += pick([0, 1, 59_999, 60_000, 61_000, 300_000, 1_800_000, 1_800_001, 1_860_000, 2_400_000]);
        }
      }
    }
    await engine.syncWindow([{ key: "session-equivalence", bytes: writeRows(rows) }]);
    const context = {
      serviceId: "fixture-service", availableColumns: await engine.getPresentColumns(),
      runSql: (sql: string) => engine.executeSql(sql),
    };
    const list = async (body: Record<string, unknown>) => {
      const result = await handleSessionsList({ start_time: start, end_time: end, min_reqs_flag: 40, min_4xx_pct_flag: 25,
        ...body }, context);
      if (!("sessions" in result.body)) throw new Error("Session scan failed");
      return result.body.sessions.map(({ session_token: _token, is_streaming: _streaming, ...session }) => session);
    };
    const reference = referenceSessions(rows);
    expect(reference.length).toBeGreaterThan(30);
    expect(reference.filter((session) => session.req_count === 1).length).toBeGreaterThan(0);
    const byKey = (a: { ip: string; ja4?: string | null; session_start: string }, b: typeof a) =>
      `${a.ip}|${a.ja4}|${a.session_start}`.localeCompare(`${b.ip}|${b.ja4}|${b.session_start}`);
    const normalized = <T extends { ip: string; ja4?: string | null; session_start: string; median_rtt_ms?: number | null }>(
      sessions: T[],
    ) => sessions
      .map((session) => ({ ...session, median_rtt_ms: session.median_rtt_ms == null ? null : Number(session.median_rtt_ms.toFixed(6)) }))
      .sort(byKey);
    for (const sort_by of ["req_count", "session_start", "median_rtt_ms", "unique_urls", "total_bytes"]) {
      expect(normalized(await list({ sort_by, limit: 500 })), sort_by).toEqual(normalized(reference));
    }
    expect(normalized(await list({ flagged_only: true, limit: 500 })))
      .toEqual(normalized(reference.filter((session) => session.flagged)));
    expect(normalized(await list({ streaming_only: true, sort_by: "unique_urls", limit: 500 })))
      .toEqual(normalized(reference.filter((session) => session.streaming_reqs > 0)));
    const filtered = rows.filter((row) => row.country === "US");
    expect(normalized(await list({ filters: { country: { mode: "include", values: ["US"] } }, limit: 500 })))
      .toEqual(normalized(referenceSessions(filtered)));
    // Pages of distinct session starts partition the whole ordered list.
    const ordered = [...reference].sort((a, b) => b.session_start.localeCompare(a.session_start) || byKey(a, b));
    expect(new Set(ordered.map((session) => session.session_start)).size).toBe(ordered.length);
    const pages = [];
    for (let page = 1; page <= Math.ceil(ordered.length / 7); page++) {
      pages.push(...await list({ sort_by: "session_start", sort_dir: "desc", limit: 7, page }));
    }
    expect(pages.map((session) => ({ ...session, median_rtt_ms: session.median_rtt_ms == null ? null
      : Number(session.median_rtt_ms.toFixed(6)) }))).toEqual(normalized(ordered).sort((a, b) =>
      b.session_start.localeCompare(a.session_start)));
  });

  it("sessionizes a busy range without exhausting DuckDB memory", async () => {
    const rows: Row[] = [];
    const ua = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0 Safari/537.36";
    for (let i = 0; i < 150_000; i++) {
      const key = i % 3_000;
      rows.push({
        // Each client requests every 90 seconds, so (almost) no two of its requests share a minute.
        ts: from + Math.floor(i / 3_000) * 90_000 + (key * 13) % 60_000, ip: `10.0.${key >> 8}.${key & 255}`,
        ja4: `t13d1516h2_8daaf6152771_${(key % 3).toString().padStart(12, "0")}`, cmcd_sid: i % 4 ? null : `stream-${i % 9}`,
        edge_sid: `edge-${i % 11}`, edge: i % 5 !== 0, asn: 64_500 + (i % 17), country: ["US", "DE", "BR"][i % 3],
        status: i % 9 ? 200 : 404, resp_bytes: 1_000 + (i % 1_000), tcp_rtt: 10_000 + (i % 5_000), ua: `${ua} ${i % 40}`,
        url: `/videos/segment/${i % 2_000}/chunk-${(i * 7) % 997}.m4s?token=${"x".repeat(40)}`,
      });
    }
    await engine.syncWindow([{ key: "session-busy", bytes: writeRows(rows) }]);
    const context = {
      serviceId: "fixture-service", availableColumns: await engine.getPresentColumns(),
      runSql: (sql: string) => engine.executeSql(sql),
    };
    // Sessionizing raw rows exhausted this scaled-down limit, as a busy day exhausted the page's 256MB.
    await engine.executeSql("SET memory_limit = '64MB'");
    try {
      const list = await handleSessionsList({ start_time: start, end_time: end, limit: 500 }, context);
      if (!("sessions" in list.body)) throw new Error("Session scan failed");
      const reference = referenceSessions(rows);
      expect(list.body.sessions).toHaveLength(Math.min(500, reference.length));
      expect(list.body.sessions.reduce((sum, session) => sum + session.req_count, 0))
        .toBe(reference.sort((a, b) => b.req_count - a.req_count).slice(0, 500).reduce((sum, session) => sum + session.req_count, 0));
    } finally {
      await engine.executeSql("SET memory_limit = '256MB'");
    }
  });
});
