import { describe, expect, it } from "vitest";
import {
  BLOCKED_FUNCTIONS,
  FORBIDDEN_KEYWORDS,
  MAX_INPUT_BYTES,
  SAFE_FUNCTIONS,
  SQLValidationError,
  escapeSqlLiteral,
  isSafeIdent,
  isSimpleSelectStatement,
  stripSqlComments,
  validateSqlQuery,
  validateUserSql,
} from "./sql-validator";

describe("cloud-engine sql-validator", () => {
  it("exports 64 KiB MAX_INPUT_BYTES and security sets", () => {
    expect(MAX_INPUT_BYTES).toBe(65_536);
    expect(BLOCKED_FUNCTIONS).toContain("read_parquet");
    expect(BLOCKED_FUNCTIONS).toContain("glob");
    expect(FORBIDDEN_KEYWORDS.has("drop")).toBe(true);
    expect(SAFE_FUNCTIONS.has("count")).toBe(true);
  });

  it("accepts valid SELECT, WITH, and SUMMARIZE queries", () => {
    expect(() =>
      validateUserSql("SELECT status, count(*) FROM logs GROUP BY 1", ["logs"]),
    ).not.toThrow();
    expect(() =>
      validateUserSql(
        "WITH top_urls AS (SELECT url, count(*) AS c FROM logs GROUP BY 1) SELECT * FROM top_urls LIMIT 10",
        ["logs"],
      ),
    ).not.toThrow();
    expect(() => validateUserSql("SUMMARIZE logs", ["logs"])).not.toThrow();
    expect(isSimpleSelectStatement("SELECT * FROM logs")).toBe(true);
    expect(isSimpleSelectStatement("SUMMARIZE logs")).toBe(false);
  });

  it("enforces exact 65,536-byte UTF-8 boundary", () => {
    const prefix = "SELECT '";
    const suffix = "' AS x FROM logs";
    const padOk = "a".repeat(65_536 - prefix.length - suffix.length);
    const sqlOk = prefix + padOk + suffix;
    expect(new TextEncoder().encode(sqlOk).byteLength).toBe(65_536);
    expect(() => validateUserSql(sqlOk, ["logs"])).not.toThrow();

    const sqlTooBig = prefix + padOk + "a" + suffix;
    expect(new TextEncoder().encode(sqlTooBig).byteLength).toBe(65_537);
    expect(() => validateUserSql(sqlTooBig, ["logs"])).toThrow(SQLValidationError);
  });

  it.each([
    "DROP TABLE logs",
    "DELETE FROM logs WHERE 1=1",
    "INSERT INTO logs VALUES (1)",
    "UPDATE logs SET status = 200",
    "COPY logs TO '/tmp/pwn.parquet'",
    "ATTACH '/tmp/other.db' AS other",
    "PRAGMA database_list",
    "INSTALL httpfs",
    "LOAD httpfs",
    "SELECT * FROM read_parquet('/etc/passwd')",
    "SELECT * FROM parquet_scan('s3://bucket/key.parquet')",
    "SELECT * FROM read_csv('/etc/hosts')",
    "SELECT * FROM read_json('/tmp/secrets.json')",
    "SELECT * FROM glob('/Users/*')",
    "SELECT current_setting('secret') FROM logs",
    "SELECT getenv('FASTLY_API_TOKEN') FROM logs",
    "SELECT * FROM information_schema.tables",
    "SELECT * FROM pg_catalog.pg_tables",
    "SELECT * FROM duckdb_secrets()",
    "SELECT * FROM pragma_database_list()",
    "SELECT 1 FROM logs; DROP TABLE logs",
    "SELECT * FROM '/etc/passwd'",
    "SELECT * FROM \"../secret.parquet\"",
  ])("rejects adversarial SQL: %s", (adversarialSql) => {
    expect(() => validateUserSql(adversarialSql, ["logs"])).toThrow(
      SQLValidationError,
    );
    const res = validateSqlQuery(adversarialSql, { allowedTables: ["logs"] });
    expect(res.valid).toBe(false);
    expect(res.code).toBe("sql_not_permitted");
  });

  it("strips SQL comments while preserving string literals", () => {
    const stripped = stripSqlComments(
      "SELECT '-- not a comment', /* block */ status FROM logs -- trailing\nWHERE status = 200",
    );
    expect(stripped).toContain("'-- not a comment'");
    expect(stripped).not.toContain("block");
    expect(stripped).not.toContain("trailing");
  });

  it("allows keywords inside single-quoted string literals", () => {
    expect(() =>
      validateUserSql(
        "SELECT * FROM logs WHERE url = '/drop/delete/read_parquet(1);'",
        ["logs"],
      ),
    ).not.toThrow();
  });

  it("escapes SQL literals and validates identifiers", () => {
    expect(escapeSqlLiteral("O'Reilly")).toBe("O''Reilly");
    expect(isSafeIdent("logs_2026")).toBe(true);
    expect(isSafeIdent("logs; DROP")).toBe(false);
  });
});
