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
});
