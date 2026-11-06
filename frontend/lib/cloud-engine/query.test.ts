import { describe, expect, it } from "vitest";
import {
  executeCloudQuery,
  getQueryPresets,
  getQuerySchema,
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
