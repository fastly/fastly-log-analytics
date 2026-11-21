// @vitest-environment node
import { afterAll, beforeAll, describe, expect, it } from 'vitest'
import { DuckDBAnalyticalEngine } from '../dashboard/duckdb'
import { BLOCKED_FUNCTIONS, SQLValidationError, validateSqlQuery, validateUserSql } from '../sql-validator'

const OPERATIONAL_TABLE_FUNCTIONS = [
  'arrow_scan',
  'arrow_scan_dumb',
  'seq_scan',
  'enable_logging',
  'disable_logging',
  'enable_profiling',
  'disable_profiling',
  'truncate_duckdb_logs',
  'force_checkpoint',
  'summary',
  'test_all_types',
  'test_vector_types',
] as const

describe('T5-FE-04: cloud-engine SQL validator adversarial replacement-scan, schema, and table-function hardening', () => {
  let engine: DuckDBAnalyticalEngine

  beforeAll(async () => {
    engine = new DuckDBAnalyticalEngine()
    await engine.executeSql('DROP VIEW IF EXISTS main.logs')
    await engine.executeSql('CREATE TABLE main.logs (status BIGINT, url VARCHAR, pop VARCHAR)')
    await engine.executeSql("INSERT INTO main.logs VALUES (200, '/ok', 'SJC')")
    await engine.executeSql('CREATE TABLE main.ops_secret (secret_col VARCHAR)')
    await engine.executeSql("INSERT INTO main.ops_secret VALUES ('TOP_SECRET_OPS_ROW')")
  })

  afterAll(async () => {
    await engine.executeSql('DROP TABLE IF EXISTS main.logs')
    await engine.executeSql('DROP TABLE IF EXISTS main.ops_secret')
    await engine.reset()
  })

  it('includes all 12 operational/scan DuckDB table functions in BLOCKED_FUNCTIONS', () => {
    for (const fn of OPERATIONAL_TABLE_FUNCTIONS) {
      expect(BLOCKED_FUNCTIONS, fn).toContain(fn)
    }
  })

  it.each([
    "SELECT b.* FROM range(1) AS a, 'bound_g1_1.parquet' AS b",
    "SELECT * FROM logs, 'https://evil.example/exfil.parquet'",
    "SUMMARIZE 'bound_g1_1.parquet'",
    "DESCRIBE 'https://evil.example/a.parquet'",
    'SELECT * FROM main.ops_secret',
    'SELECT * FROM "main"."ops_secret"',
    'SELECT * FROM memory.main.ops_secret',
    'SELECT * FROM other_schema.logs',
    'SELECT b.* FROM range(1) AS a, ops_secret AS b',
    'SUMMARIZE ops_secret',
    'DESCRIBE ops_secret',
    'SELECT * FROM main.read_parquet(\'a.parquet\')',
    'SELECT * FROM (FROM ops_secret)',
    'SELECT * FROM (TABLE ops_secret)',
    'SELECT * FROM (ops_secret CROSS JOIN logs)',
    'SELECT * FROM (logs CROSS JOIN ops_secret)',
    'SELECT * FROM (logs JOIN ops_secret ON true)',
    'SELECT * FROM ((logs JOIN ops_secret ON true))',
    "SELECT * FROM ('https://evil.example/exfil.parquet' CROSS JOIN logs)",
    "SELECT * FROM (logs JOIN 'https://evil.example/exfil.parquet' ON true)",
    "SELECT * FROM (FROM 'bound_g1_1.parquet')",
    'WITH p AS (FROM ops_secret SELECT *) SELECT * FROM p',
    'WITH p AS (TABLE ops_secret) SELECT * FROM p',
    'WITH p AS (UNPIVOT ops_secret ON secret_col) SELECT * FROM p',
    'WITH p AS (PIVOT ops_secret ON secret_col) SELECT * FROM p',
    'SELECT secret_col FROM logs UNION ALL TABLE ops_secret',
    'TABLE ops_secret',
    'FROM ops_secret SELECT *',
    'SHOW TABLES',
    'SHOW COLUMNS',
    'SHOW COLUMNS FROM ops_secret',
    'SUMMARIZE (ops_secret CROSS JOIN logs)',
    'DESCRIBE (logs JOIN ops_secret ON true)',
    ...OPERATIONAL_TABLE_FUNCTIONS.flatMap((fn) => [
      `SELECT * FROM ${fn}()`,
      `SELECT * FROM range(1), ${fn}()`,
      `SELECT * FROM (logs CROSS JOIN ${fn}())`,
    ]),
  ])('rejects adversarial query before DuckDB execution: %s', async (sql) => {
    expect(() => validateUserSql(sql, ['logs', 'requests'])).toThrow(SQLValidationError)
    const res = validateSqlQuery(sql, { allowedTables: ['logs', 'requests'] })
    expect(res.valid).toBe(false)
    expect(res.code).toBe('sql_not_permitted')
  })

  it('continues allowing legitimate SELECT, WITH CTE, JOIN, main.logs, SUMMARIZE, and allowed table functions on real DuckDB-Wasm', async () => {
    for (const validSql of [
      'SELECT status, count(*) AS n FROM logs GROUP BY status',
      'SELECT status FROM main.logs WHERE status = 200',
      "WITH c AS (SELECT url, pop FROM logs WHERE pop = 'SJC') SELECT a.url, b.pop FROM c AS a JOIN logs AS b ON a.url = b.url",
      'SELECT x FROM unnest([1, 2, 3]) AS t(x)',
      'SELECT * FROM range(2) AS r, logs WHERE logs.status = 200',
      'SELECT a.status, b.url FROM (logs AS a JOIN logs AS b ON (a.status = b.status))',
      'SUMMARIZE logs',
      'DESCRIBE logs',
    ]) {
      expect(() => validateUserSql(validSql, ['logs', 'requests'])).not.toThrow()
      const rows = await engine.executeSql(validSql)
      expect(Array.isArray(rows)).toBe(true)
    }
    expect(() => validateUserSql('SHOW COLUMNS FROM logs', ['logs', 'requests'])).not.toThrow()
    expect(() => validateUserSql('SHOW COLUMNS FROM main.logs', ['logs', 'requests'])).not.toThrow()
    expect(() => validateUserSql('SHOW TABLES')).not.toThrow()
  })
})
