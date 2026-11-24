// @vitest-environment node
import { describe, expect, it } from "vitest";
import { getDuckDBEngine } from "./dashboard/duckdb";
import {
  executeCloudQuery,
  getQueryPresets,
  getQuerySchema,
  planCloudQueryScan,
  redactQueryRowsForPii,
  rewriteCloudQuerySql,
} from "./query";

describe("cloud-engine query module", () => {
  it("returns 26 presets across 6 categories starting with canonical presets", () => {
    const presets = getQueryPresets("svc-1");
    expect(presets).toHaveLength(26);
    expect(presets[0].name).toBe("Sample rows");
    expect(presets[1].name).toBe("Row count");
    expect(presets[2].name).toBe("Column stats");

    const categories = new Set(presets.map((p) => p.category));
    expect(categories).toEqual(
      new Set([
        "Traffic",
        "Performance",
        "Cache",
        "Origin",
        "Security",
        "Network",
      ]),
    );
    expect(getQueryPresets("")).toEqual([]);
  });

  it("hides cookie_session in getQuerySchema when maskIps is true", () => {
    const unmasked = getQuerySchema("svc-1", false);
    expect(unmasked.schema.some((c) => c.column === "cookie_session")).toBe(
      true,
    );

    const masked = getQuerySchema("svc-1", true);
    expect(masked.schema.some((c) => c.column === "cookie_session")).toBe(
      false,
    );
  });

  it("rewrites FROM logs to FROM requests and appends default LIMIT 1000", () => {
    const { rewrittenSql, hasExplicitLimit } = rewriteCloudQuerySql(
      "SELECT status, count(*) FROM logs GROUP BY 1",
    );
    expect(hasExplicitLimit).toBe(false);
    expect(rewrittenSql).toBe(
      "SELECT status, count(*) FROM requests GROUP BY 1 LIMIT 1000",
    );
  });

  it("rejects forbidden SQL in executeCloudQuery with 403 sql_not_permitted", async () => {
    const res = await executeCloudQuery({
      sql: "SELECT * FROM read_parquet('/etc/passwd')",
    });
    expect(res.status).toBe(403);
    expect(res.body).toEqual({
      detail: {
        error: "sql_not_permitted",
        message: expect.stringContaining("read_parquet"),
      },
    });
  });

  it("executes valid SQL, detects truncation, and masks PII for analyst-no-pii", async () => {
    const executedSql: string[] = [];
    const res = await executeCloudQuery(
      {
        sql: "SELECT ip, url, cookie_session FROM logs",
        max_rows: 2,
      },
      {
        role: "analyst-no-pii",
        runSql: async (sql) => {
          executedSql.push(sql);
          return [
            { ip: "203.0.113.5", url: "/login", cookie_session: "sess-1" },
            { ip: "203.0.113.6", url: "/home", cookie_session: "sess-2" },
            { ip: "203.0.113.7", url: "/extra", cookie_session: "sess-3" },
          ];
        },
      },
    );

    expect(res.status).toBe(200);
    if ("data" in res.body) {
      expect(res.body.truncated).toBe(true);
      expect(res.body.row_count).toBe(2);
      expect(res.body.total_rows).toBe(-1);
      expect(res.body.data[0]).toEqual({
        ip: "0.0.0.0",
        url: "/login",
        cookie_session: "[redacted]",
      });
      expect(executedSql[0]).toContain("FROM requests");
    }
  });

  it("redacts IP and session columns via redactQueryRowsForPii", () => {
    const rows = [{ ip: "198.51.100.1", cmcd_sid: "sid-123", status: 200 }];
    expect(redactQueryRowsForPii(rows, false)).toEqual(rows);
    expect(redactQueryRowsForPii(rows, true)).toEqual([
      { ip: "0.0.0.0", cmcd_sid: "[redacted]", status: 200 },
    ]);
  });
});

describe("Cloud query scan planning", () => {
  const plan = async (sql: string, customColumns = {}) => {
    const read = await getDuckDBEngine().beginRequest();
    return planCloudQueryScan({ sql, max_rows: 1000 }, {
      tableName: "requests",
      allowedTables: ["logs", "requests", "client_vitals", "client_errors"],
      customColumns,
      runSql: (query) => read.executeSql(query),
    });
  };

  it("loads the newest window first for the default and builder queries", async () => {
    expect(await plan("SELECT * FROM logs LIMIT 100")).toEqual({
      columns: null, newestFirst: { table: "requests", ordered: false, limit: 100 },
    });
    expect(await plan("SELECT *\nFROM logs\nWHERE status >= 500\nORDER BY timestamp DESC\nLIMIT 100")).toEqual({
      columns: null, newestFirst: { table: "requests", ordered: true, limit: 100 },
    });
    const picked = await plan("SELECT url, \"Status\" FROM logs r WHERE r.cache = 'MISS' ORDER BY r.timestamp DESC LIMIT 5");
    expect(picked.newestFirst).toEqual({ table: "requests", ordered: true, limit: 5 });
    expect(picked.columns?.sort()).toEqual(["cache", "status", "timestamp", "url"]);
  });

  it("scans the whole range, projected, for aggregates and other orders", async () => {
    expect(await plan("SELECT status, count(*) AS requests FROM logs GROUP BY status ORDER BY requests DESC"))
      .toEqual({ columns: ["timestamp", "status"], newestFirst: null });
    for (const sql of [
      "SELECT count(*) FROM logs LIMIT 1",
      "SELECT url FROM logs ORDER BY status DESC LIMIT 5",
      "SELECT url AS timestamp FROM logs ORDER BY timestamp DESC LIMIT 5",
      "SELECT url \"timestamp\" FROM logs ORDER BY timestamp DESC LIMIT 5",
      "SELECT DISTINCT url FROM logs LIMIT 5",
      "SELECT url FROM logs LIMIT 5 OFFSET 10",
    ]) expect((await plan(sql)).newestFirst, sql).toBeNull();
    const custom = await plan("SELECT my_field, count(*) FROM logs GROUP BY 1", { my_field: "VARCHAR" });
    expect(custom.columns?.sort()).toEqual(["my_field", "timestamp"]);
    for (const preset of getQueryPresets("svc-1")) {
      const { columns } = await plan(preset.sql);
      if (preset.sql.includes("*") && !preset.sql.includes("count(*)") || /^SUMMARIZE/.test(preset.sql)) {
        expect(columns, preset.name).toBeNull();
      } else {
        expect(columns, preset.name).not.toBeNull();
        expect(columns!.length, preset.name).toBeLessThan(10);
      }
    }
  });

  it("falls back to every column when whole rows or unknown names are referenced", async () => {
    for (const sql of [
      "SELECT hash(r) FROM logs r LIMIT 5",
      "SELECT r.* FROM logs r",
      "SELECT count(*) FROM (SELECT DISTINCT * FROM logs)",
      "SELECT count(*) FROM (SELECT * FROM logs) AS q WHERE q IS NOT NULL",
      "SELECT md5(requests::VARCHAR) FROM requests",
      "SELECT COLUMNS('stat.*') FROM logs",
      "SELECT no_such_column FROM logs",
    ]) expect((await plan(sql)).columns, sql).toBeNull();
  });

  it("restricts a newest-window query to rows at or after the window start", async () => {
    const executed: string[] = [];
    const read = await getDuckDBEngine().beginRequest();
    const res = await executeCloudQuery(
      { sql: "SELECT * FROM logs ORDER BY timestamp DESC LIMIT 3", max_rows: 10 },
      { since: { table: "requests", ms: 1234 }, runSql: async (sql) => {
        executed.push(sql);
        return read.executeSql(sql);
      } },
    );
    expect(res.status).toBe(200);
    expect(executed[0]).toMatch(/^WITH "requests" AS \(SELECT \* FROM main\."requests" WHERE ts_ms >= 1234\) /);
  });

  it("strips internal DuckDB scoped_key column from SELECT * FROM logs results", async () => {
    const res = await executeCloudQuery(
      { sql: "SELECT * FROM logs LIMIT 10", max_rows: 10 },
      {
        runSql: async () => [
          {
            scoped_key: '["session-uuid","user:admin"]|processed/request/a.parquet|["status"]',
            timestamp: "2026-11-22T00:00:00Z",
            status: 200,
          },
        ],
      },
    );
    expect(res.status).toBe(200);
    if ("columns" in res.body) {
      expect(res.body.columns).not.toContain("scoped_key");
      expect(res.body.data[0]).not.toHaveProperty("scoped_key");
      expect(res.body.data[0]).toMatchObject({ status: 200 });
    }
  });
});
