// @vitest-environment node
import { parquetWriteBuffer } from "hyparquet-writer";
import { beforeEach, describe, expect, it, vi } from "vitest";
vi.mock("./active-log-fields", () => ({
  cloudActiveLogFieldIds: async () => new Set(["tcp_rtt", "ja4", "edge", "edge_sid", "cmcd_sid"]),
}));
import { getDuckDBEngine } from "./dashboard/duckdb";
import { handleSessionsDetail, handleSessionsList, sessionsListColumns, type SessionsListRequest } from "./sessions";
import { sessionsEnrichSpans, sessionsLoadFrom } from "./sessions-window";

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

  const dayEnd = new Date(from + 86_400_000).toISOString();
  const steadyRow = (ts: number, key: number, i: number): Row => ({
    ts, ip: `10.${key >> 16 & 255}.${key >> 8 & 255}.${key & 255}`, ja4: `t13d_${key % 2}`, cmcd_sid: null,
    edge_sid: null, edge: i % 3 !== 0, asn: 64_500 + (key % 17), country: ["US", "DE", "BR"][key % 3],
    status: i % 9 ? 200 : 404, resp_bytes: 1_000 + (i % 1_000), tcp_rtt: 10_000 + (key % 5_000), ua: `agent ${key % 40}`,
    url: `/p/${(key * 7 + i) % 500}`,
  });
  const newestFirst = (sessions: ReturnType<typeof referenceSessions>) => [...sessions].sort((a, b) =>
    cmp(b.session_start, a.session_start) || cmp(a.ip, b.ip) || cmp(String(a.ja4), String(b.ja4)));
  const cmp = (a: string, b: string) => a < b ? -1 : a > b ? 1 : 0;
  const withoutToken = <T extends { session_token: string; is_streaming: boolean }>(sessions: T[]) =>
    sessions.map(({ session_token: _token, is_streaming: _streaming, ...session }) => session);

  it("lists the newest sessions of a busy day without sessionizing the whole range", async () => {
    // A steady day: each minute 200 clients are active, each for 4 consecutive minutes.
    const rows: Row[] = [];
    for (let minute = 0; minute < 1_440; minute++) {
      for (let j = 0; j < 200; j++) {
        const key = (minute - (j % 4)) * 50 + Math.floor(j / 4);
        if (key >= 0) rows.push(steadyRow(from + minute * 60_000 + (j * 571) % 60_000, key, rows.length));
      }
    }
    await engine.syncWindow([{ key: "session-steady-day", bytes: writeRows(rows) }]);
    const context = {
      serviceId: "fixture-service", availableColumns: await engine.getPresentColumns(),
      runSql: (sql: string) => engine.executeSql(sql),
    };
    // The whole day's client-minutes exceed this scaled-down limit, as a busy week exceeded the page's 256MB.
    await engine.executeSql("SET memory_limit = '64MB'");
    try {
      const list = await handleSessionsList({ start_time: start, end_time: dayEnd, page: 1, limit: 100,
        sort_by: "session_start", sort_dir: "desc", min_reqs_flag: 40, min_4xx_pct_flag: 25 }, context);
      if (!("sessions" in list.body)) throw new Error(`Session scan failed: ${JSON.stringify(list.body)}`);
      const expected = newestFirst(referenceSessions(rows)).slice(0, 100);
      expect(withoutToken(list.body.sessions).map((session) => ({ ...session, median_rtt_ms: Number(session.median_rtt_ms) })))
        .toEqual(expected.map((session) => ({ ...session, median_rtt_ms: Number(session.median_rtt_ms) })));
    } finally {
      await engine.executeSql("SET memory_limit = '256MB'");
    }
  });

  it("reports newest sessions exactly when they began before the newest slice", async () => {
    const rows: Row[] = [];
    const add = (ip: string, offsets: number[]) => offsets.forEach((offset) => rows.push({ ...steadyRow(from + offset, 1, rows.length), ip }));
    const hour = 3_600_000, min = 60_000;
    // Requests every 10 minutes for the whole day: one session that began at the range start.
    add("192.0.2.1", Array.from({ length: 144 }, (_, i) => i * 10 * min));
    // Exactly 30 minutes apart is still one session; 1ms more starts a new one.
    add("192.0.2.2", [24 * hour - 61 * min, 24 * hour - 31 * min, 24 * hour - min]);
    add("192.0.2.3", [24 * hour - 61 * min - 1, 24 * hour - 31 * min, 24 * hour - min]);
    // Quiet for hours, then active again just before the end.
    add("192.0.2.4", [hour, 2 * hour, 24 * hour - 2 * min]);
    // Sparse older sessions, so a page of 7 needs most of the day.
    for (let i = 0; i < 12; i++) add(`198.51.100.${i}`, [i * 2 * hour + 5 * min, i * 2 * hour + 9 * min]);
    await engine.syncWindow([{ key: "session-straddle", bytes: writeRows(rows) }]);
    const context = {
      serviceId: "fixture-service", availableColumns: await engine.getPresentColumns(),
      runSql: (sql: string) => engine.executeSql(sql),
    };
    const reference = newestFirst(referenceSessions(rows));
    for (const [limit, page] of [[3, 1], [7, 1], [7, 2], [7, 3], [100, 1]]) {
      const list = await handleSessionsList({ start_time: start, end_time: dayEnd, page, limit,
        sort_by: "session_start", sort_dir: "desc", min_reqs_flag: 40, min_4xx_pct_flag: 25 }, context);
      if (!("sessions" in list.body)) throw new Error("Session scan failed");
      expect(withoutToken(list.body.sessions), `limit ${limit} page ${page}`)
        .toEqual(reference.slice((page - 1) * limit, page * limit));
    }
    const flagged = await handleSessionsList({ start_time: start, end_time: dayEnd, limit: 5, flagged_only: true,
      sort_by: "session_start", sort_dir: "desc", min_reqs_flag: 40, min_4xx_pct_flag: 25 }, context);
    if (!("sessions" in flagged.body)) throw new Error("Session scan failed");
    expect(withoutToken(flagged.body.sessions)).toEqual(reference.filter((session) => session.flagged).slice(0, 5));
  });

  describe("bounded loading from 5-minute window files", () => {
    const ALL_COLUMNS = ["timestamp", "ip", "ja4", "cmcd_sid", "edge_sid", "country", "ua", "url", "edge", "asn", "status",
      "resp_bytes", "tcp_rtt"];
    const writeColumns = (rows: Row[], columns: ReadonlySet<string> | null) => parquetWriteBuffer({ columnData: [
      { name: "timestamp", type: "TIMESTAMP" as const, data: rows.map((row) => new Date(row.ts)) },
      ...(["ip", "ja4", "cmcd_sid", "edge_sid", "country", "ua", "url"] as const).map((name) =>
        ({ name, type: "STRING" as const, data: rows.map((row) => row[name]) })),
      { name: "edge", type: "BOOLEAN" as const, data: rows.map((row) => row.edge) },
      ...(["asn", "status", "resp_bytes", "tcp_rtt"] as const).map((name) =>
        ({ name, type: "DOUBLE" as const, data: rows.map((row) => row[name]) })),
    ].filter((column) => !columns || columns.has(column.name)) });
    type Load = { fromMs: number; toMs: number; columns: readonly string[] | null; files: number; rows: number; bytes: number;
      release: ReturnType<typeof vi.fn> };

    // A day of window files and a loader that registers only the files overlapping a span, with only the asked columns.
    function windowFiles(rows: Row[], fileMs = 5 * 60_000) {
      const files = Array.from({ length: 86_400_000 / fileMs }, (_, i) => ({ fromMs: from + i * fileMs,
        toMs: from + (i + 1) * fileMs, rows: rows.filter((row) => row.ts >= from + i * fileMs && row.ts < from + (i + 1) * fileMs) }));
      const loads: Load[] = [];
      const buffers = new Map<string, ArrayBuffer>();
      const written = (file: typeof files[number], columns: readonly string[] | null) => {
        const key = `w${file.fromMs}:${columns?.join(",")}`;
        if (!buffers.has(key)) buffers.set(key, writeColumns(file.rows, columns ? new Set(columns) : null));
        return { key, bytes: buffers.get(key)! };
      };
      const loadRange = async (fromMs: number, toMs: number, columns: readonly string[] | null) => {
        const selected = files.filter((file) => file.toMs > fromMs && file.fromMs < toMs && file.rows.length);
        await engine.reset();
        const window = selected.map((file) => written(file, columns));
        await engine.syncWindow(window);
        const release = vi.fn();
        loads.push({ fromMs, toMs, columns, files: selected.length, release,
          rows: selected.reduce((sum, file) => sum + file.rows.length, 0),
          bytes: window.reduce((sum, file) => sum + file.bytes.byteLength, 0) });
        return { runSql: (sql: string) => engine.executeSql(sql), release };
      };
      // As the adapter does: the first load is the page's first window with the list columns.
      const list = async (body: Partial<SessionsListRequest>) => {
        const request: SessionsListRequest = { start_time: start, end_time: dayEnd, page: 1, limit: 100, min_reqs_flag: 40,
          min_4xx_pct_flag: 25, ...body };
        const loadedFromMs = sessionsLoadFrom(request, from, Date.parse(dayEnd));
        const first = await loadRange(loadedFromMs, Date.parse(dayEnd), sessionsListColumns(request.filters, request.sort_by));
        const result = await handleSessionsList(request, {
          serviceId: "fixture-service", availableColumns: new Set([...ALL_COLUMNS, "ts_ms"]),
          runSql: first.runSql, loadedFromMs, loadRange,
        });
        if (!("sessions" in result.body)) throw new Error(`Session scan failed: ${JSON.stringify(result.body)}`);
        return withoutToken(result.body.sessions);
      };
      const wholeDayBytes = () => files.reduce((sum, file) => sum + (file.rows.length ? written(file, null).bytes.byteLength : 0), 0);
      return { loads, list, wholeDayBytes };
    }
    // Each minute `perMinute` clients start a 4-minute session, until `untilMinute`.
    function steadyDay(perMinute: number, untilMinute = 1_440) {
      const rows: Row[] = [];
      for (let minute = 0; minute < untilMinute; minute++) {
        for (let j = 0; j < perMinute * 4; j++) {
          const key = (minute - (j % 4)) * perMinute + Math.floor(j / 4);
          if (key >= 0) rows.push(steadyRow(from + minute * 60_000 + (j * 571) % 60_000, key, rows.length));
        }
      }
      return rows;
    }
    const exactRtt = <T extends { median_rtt_ms?: number | null }>(sessions: T[]) =>
      sessions.map((session) => ({ ...session, median_rtt_ms: session.median_rtt_ms == null ? null : Number(session.median_rtt_ms) }));

    it("lists a 24h page 1 newest-first from the last 45 minutes of files, exactly", async () => {
      const rows = steadyDay(10);
      const dayEndMs = Date.parse(dayEnd), min = 60_000;
      const add = (ip: string, at: number[]) => at.forEach((ts) => rows.push({ ...steadyRow(ts, 3, rows.length), ip }));
      // Exactly 1800 s of silence keeps one session that began before the page; 1801 s starts one inside it.
      add("192.0.2.30", [dayEndMs - 35 * min, dayEndMs - 5 * min]);
      add("192.0.2.31", [dayEndMs - 35 * min - 1_000, dayEndMs - 5 * min]);
      // A session that began before the first load, its rows continuing into the newest slice.
      add("192.0.2.32", [dayEndMs - 46 * min, dayEndMs - 16 * min, dayEndMs - 5 * min]);
      // A client active all day: one session from the range start.
      add("192.0.2.33", Array.from({ length: 144 }, (_, i) => from + i * 10 * min));
      const { loads, list, wholeDayBytes } = windowFiles(rows);
      const sessions = await list({ sort_by: "session_start", sort_dir: "desc" });

      const expected = newestFirst(referenceSessions(rows)).slice(0, 100);
      expect(exactRtt(sessions)).toEqual(exactRtt(expected));
      expect(sessions.some((session) => session.ip === "192.0.2.31")).toBe(true);
      expect(sessions.some((session) => ["192.0.2.30", "192.0.2.32", "192.0.2.33"].includes(session.ip))).toBe(false);
      // Pass 1: the first window only, without ua or url. Pass 2: only the page's own span, for ua and url.
      expect(loads.map((load) => load.fromMs - dayEndMs)).toEqual([-45 * min, Date.parse(expected.at(-1)!.session_start) - dayEndMs]);
      expect(loads[0].columns).not.toEqual(expect.arrayContaining(["ua"]));
      expect(loads[0].columns).not.toEqual(expect.arrayContaining(["url"]));
      expect(loads[1].columns).toEqual(expect.arrayContaining(["ua", "url"]));
      expect(loads[1].fromMs).toBeGreaterThanOrEqual(dayEndMs - 15 * min);
      expect(loads[1].release).toHaveBeenCalledTimes(1);
      // Peak rows and bytes held stay a small slice of the day's.
      const loadedRows = loads.reduce((sum, load) => sum + load.rows, 0);
      const loadedBytes = loads.reduce((sum, load) => sum + load.bytes, 0);
      expect(loadedRows).toBeLessThan(rows.length / 20);
      expect(loadedBytes).toBeLessThan(wholeDayBytes() / 20);
    });

    it("widens to older files only until the page fills", async () => {
      // Traffic stops three hours before the range end.
      const rows = steadyDay(10, 1_440 - 180);
      const { loads, list } = windowFiles(rows);
      const sessions = await list({ sort_by: "session_start", sort_dir: "desc" });
      expect(exactRtt(sessions)).toEqual(exactRtt(newestFirst(referenceSessions(rows)).slice(0, 100)));
      const min = 60_000, dayEndMs = Date.parse(dayEnd);
      expect(loads.slice(0, 3).map((load) => load.fromMs - dayEndMs)).toEqual([-45 * min, -90 * min, -270 * min]);
      expect(loads).toHaveLength(4);
      // Each wider pass-1 read is released once the next replaces it, and the last once the list is built.
      expect(loads.slice(1).map((load) => load.release.mock.calls.length)).toEqual([1, 1, 1]);
    });

    it("lists other sorts from the whole range without ua or url, reading those only for the page", async () => {
      const rows = steadyDay(2);
      // Hourly files: registering a whole day of 5-minute files is slow in tests.
      const { loads, list } = windowFiles(rows, 3_600_000);
      const sessions = await list({ sort_by: "req_count", sort_dir: "desc", limit: 50 });
      const reference = referenceSessions(rows);
      const order = (a: typeof reference[number], b: typeof reference[number]) => b.req_count - a.req_count ||
        cmp(a.ip, b.ip) || cmp(String(a.ja4), String(b.ja4)) || cmp(a.session_start, b.session_start);
      expect(exactRtt(sessions)).toEqual(exactRtt([...reference].sort(order).slice(0, 50)));
      expect(loads[0]).toMatchObject({ fromMs: from, files: 24 });
      expect(loads[0].columns).not.toEqual(expect.arrayContaining(["ua"]));
      expect(loads.length).toBeGreaterThan(1);
      expect(loads.length).toBeLessThanOrEqual(9);
      for (const load of loads.slice(1)) expect(load.columns).toEqual(expect.arrayContaining(["ua", "url"]));
    });

    it("merges enrichment spans within the same UTC hour so hourly catalog1 files are not loaded twice", () => {
      const hourStart = Date.parse("2026-11-25T07:00:00Z");
      const spans = sessionsEnrichSpans([
        { startMs: hourStart + 5 * 60_000, endMs: hourStart + 6 * 60_000 },
        { startMs: hourStart + 45 * 60_000, endMs: hourStart + 47 * 60_000 },
        { startMs: hourStart + 3 * 3_600_000 + 10 * 60_000, endMs: hourStart + 3 * 3_600_000 + 12 * 60_000 },
      ]);
      expect(spans).toEqual([
        { startMs: hourStart + 5 * 60_000, endMs: hourStart + 47 * 60_000 },
        { startMs: hourStart + 3 * 3_600_000 + 10 * 60_000, endMs: hourStart + 3 * 3_600_000 + 12 * 60_000 },
      ]);
    });
  });
});
