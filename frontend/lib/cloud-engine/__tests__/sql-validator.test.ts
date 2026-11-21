// @vitest-environment node
import { afterAll, beforeAll, describe, expect, it } from 'vitest'
import { DuckDBAnalyticalEngine } from '../dashboard/duckdb'
import {
  BLOCKED_FUNCTIONS,
  SQLValidationError,
  stripSqlComments,
  validateSqlQuery,
  validateUserSql,
} from '../sql-validator'

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
    'SHOW ALL TABLES',
    'SHOW COLUMNS',
    'SHOW COLUMNS FROM ops_secret',
    'SUMMARIZE (ops_secret CROSS JOIN logs)',
    'DESCRIBE (logs JOIN ops_secret ON true)',
    // Iteration-3 Wave-2 lexer, comment-stripper, DISTINCT, DESCRIBE, and multi-paren vectors:
    "SELECT E'\\'', secret_col FROM ops_secret -- '\nLIMIT 5",
    "SELECT e'\\'', secret_col FROM ops_secret -- '\nLIMIT 5",
    "SELECT E'x\\'' AS pad, secret_col FROM ops_secret /*'*/",
    "SELECT E'a'\n'\\'', secret_col FROM ops_secret -- '\nLIMIT 5",
    "SELECT E'a'/*c*/\n'\\'', secret_col FROM ops_secret -- '\nLIMIT 5",
    "VALUES (E'\\''); CREATE TABLE pwned_by_values AS SELECT secret_col FROM ops_secret; VALUES ('ok' -- '\n)",
    "SELECT E'unterminated\\",
    'SELECT $a$ -- /* $a$, secret_col FROM ops_secret',
    'SELECT $1$ FROM ops_secret $1$',
    'SELECT $tag$unterminated FROM logs',
    'SELECT $é$ok$é$, secret_col FROM ops_secret',
    'SELECT url FROM logs --x\r UNION ALL SELECT secret_col FROM ops_secret',
    'SELECT /* outer /* inner */ url FROM logs',
    "SELECT U&'\\0061' FROM logs",
    "SELECT u&'\\0061' FROM logs",
    'SELECT U&"url" FROM logs',
    'SELECT u&"url" FROM logs',
    "SELECT 'a' UESCAPE '!' FROM logs",
    'SELECT secret_col AS "distinct" FROM ops_secret',
    'SELECT 1 AS "distinct" FROM \'https://evil.example/exfil.parquet\'',
    'DESCRIBE',
    'DESCRIBE;',
    'SUMMARIZE',
    'SUMMARIZE;',
    'SELECT * FROM (DESCRIBE)',
    'SELECT * FROM (SUMMARIZE)',
    'SELECT * FROM ()',
    'SELECT * FROM (logs, )',
    'SELECT * FROM logs,',
    'SELECT * FROM logs JOIN',
    'SELECT * FROM WHERE 1 = 1',
    "WITH p AS ((SELECT 'a' AS s) UNION ALL SELECT secret_col FROM ops_secret) SELECT * FROM p",
    "SELECT * FROM logs WHERE url IN ((SELECT 'a') UNION ALL SELECT * FROM 'https://evil.example/exfil.parquet')",
    'SELECT * FROM ((SELECT 1) AS a, ops_secret AS b)',
    'SELECT TRIM((FROM ops_secret)) FROM logs',
    'SELECT TRIM(FROM (SELECT secret_col FROM ops_secret)) FROM logs',
    // Iteration-4 Wave-3 Unicode space (StripUnicodeSpaces), control char, keyword, catalog, and CTE-scoping vectors:
    'SELECT secret_col FROM\u200bops_secret',
    'SELECT secret_col FROM\u2060ops_secret',
    'SELECT secret_col FROM\ufeffops_secret',
    'SELECT secret_col FROM\u00a0ops_secret',
    "SELECT current_setting\u200b('threads') FROM logs",
    "SELECT E'a'\u00a0\n'\\'' AS pad, secret_col FROM ops_secret WHERE 'x' != E'\\''",
    "SELECT E'a'\u200b\n'\\'' AS pad, secret_col FROM ops_secret WHERE 'x' != E'\\''",
    "SELECT 1,\u00a0$a$'$a$ AS a, secret_col FROM ops_secret WHERE $b$'$b$ != $b$y$b$",
    "SELECT 1e'\\'', (SELECT secret_col FROM ops_secret LIMIT 1) AS leaked -- '\nFROM logs",
    "SELECT $1e'\\'', (SELECT secret_col FROM ops_secret LIMIT 1) AS leaked -- '\nFROM logs",
    'SELECT status\vFROM logs',
    'SELECT status\u1680FROM logs',
    'SELECT status\u2028FROM logs',
    'SELECT status\x01FROM logs',
    'WITH p AS (PIVOT_LONGER memory.main.requests ON status) SELECT * FROM p',
    'WITH p AS (PIVOT_WIDER memory.main.requests ON status) SELECT * FROM p',
    'WITH p AS (PIVOT_LONGER ops_secret ON secret_col) SELECT * FROM p',
    'WITH p AS (PIVOT_WIDER ops_secret ON secret_col) SELECT * FROM p',
    "WITH p AS (PIVOT_LONGER 'https://evil.example/exfil.parquet' ON x) SELECT * FROM p",
    'SELECT * FROM memory.main.logs',
    'SELECT * FROM "memory"."main"."logs"',
    'SELECT * FROM memory.logs',
    'SELECT memory.logs.status FROM logs',
    'SELECT "memory"."logs"."status" FROM logs',
    'SELECT ARRAY(SHOW TABLES) AS all_tbls',
    "SELECT status FROM logs WHERE 'ops_secret' IN (SHOW TABLES)",
    'SELECT status FROM logs WHERE EXISTS (SHOW ops_secret)',
    'SELECT status FROM logs WHERE EXISTS (DESC ops_secret)',
    'SELECT status FROM logs WHERE EXISTS (DESC)',
    'DESC logs',
    'SELECT * FROM (DESCRIBE logs)',
    'SELECT * FROM (SUMMARIZE logs)',
    'WITH d AS (DESCRIBE logs) SELECT * FROM d',
    'SELECT * FROM logs UNION DISTINCT DESCRIBE ops_secret',
    'SELECT secret_col FROM ops_secret WINDOW w AS (), ops_secret AS ()',
    'SELECT client_id FROM client_vitals WINDOW w AS (), client_vitals AS ()',
    'WITH ops_secret AS (SELECT secret_col FROM ops_secret) SELECT * FROM ops_secret',
    'WITH a AS (SELECT secret_col FROM ops_secret), ops_secret AS (SELECT 1) SELECT * FROM a',
    'WITH RECURSIVE ops_secret AS (SELECT secret_col FROM ops_secret UNION ALL SELECT secret_col FROM ops_secret WHERE 1=0) SELECT * FROM ops_secret',
    'SELECT secret_col, (WITH ops_secret AS (SELECT 1) SELECT * FROM ops_secret) AS dummy FROM ops_secret',
    'SELECT secret_col FROM ops_secret WHERE 1 = (WITH ops_secret AS (SELECT 1) SELECT * FROM ops_secret)',
    "SELECT secret_col FROM ops_secret UNION ALL (WITH ops_secret AS (SELECT 'x' AS secret_col) SELECT secret_col FROM ops_secret)",
    "(WITH ops_secret AS (SELECT 'x' AS secret_col) SELECT secret_col FROM ops_secret) UNION ALL SELECT secret_col FROM ops_secret",
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
      'SELECT TRIM(FROM url) AS trimmed_url FROM logs',
      'SELECT status IS DISTINCT FROM 200 AS d1, status IS NOT DISTINCT FROM 404 AS d2 FROM logs',
      "SELECT status FROM logs WHERE'ok'='ok'",
      "SELECT E'safe\\'quote' AS q, E'back\\\\slash' AS bs, $é$-- keep /* me */$é$ AS dq FROM logs",
      'WITH c1 AS (SELECT status, url FROM logs), c2 AS (SELECT * FROM c1) SELECT * FROM c2 ORDER BY status DESC',
      'WITH c(x) AS (SELECT status FROM logs) SELECT x FROM c',
      'WITH c AS MATERIALIZED (SELECT status FROM logs), d AS NOT MATERIALIZED (SELECT * FROM c) SELECT * FROM d',
      'SELECT status, count(*) OVER w AS n FROM logs WINDOW w AS (PARTITION BY status)',
      "SELECT 1 AS a1e, 'ok' AS s FROM logs WHERE'ok'='ok'",
      'SELECT $\u00a0$ok$\u00a0$ AS s, status\u00a0FROM\u200blogs',
      'DESCRIBE TABLE logs',
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

  it('maintains stripSqlComments parity on dollar quotes, E-strings, CR line comments, and unclosed comments/quotes', async () => {
    const preservedDollar = stripSqlComments(
      'SELECT $tag$ -- keep /* inside */ $tag$ AS s, $é$/* keep */$é$ AS u /* drop */ FROM logs -- end\rWHERE status = 200',
    )
    expect(preservedDollar).toContain('$tag$ -- keep /* inside */ $tag$')
    expect(preservedDollar).toContain('$é$/* keep */$é$')
    expect(preservedDollar).not.toContain('/* drop */')
    expect(preservedDollar).toContain('\rWHERE status = 200')

    const preservedEscape = stripSqlComments("SELECT E'it\\'s -- not a comment' AS s /* cut */ FROM logs")
    expect(preservedEscape).toContain("E'it\\'s -- not a comment'")
    expect(preservedEscape).not.toContain('cut')

    expect(() => stripSqlComments('SELECT $a$unclosed FROM logs')).toThrow(SQLValidationError)
    expect(() => stripSqlComments("SELECT E'unclosed\\")).toThrow(SQLValidationError)
    expect(() => stripSqlComments('SELECT /* outer /* inner */ FROM logs')).toThrow(SQLValidationError)
    expect(() => stripSqlComments("SELECT U&'\\0061' FROM logs")).toThrow(SQLValidationError)
    expect(() => stripSqlComments('SELECT status\vFROM logs')).toThrow(SQLValidationError)

    const unicodeSpaceStripped = stripSqlComments(
      'SELECT $\u00a0$keep$\u00a0$ AS dq, status\u00a0FROM\u200blogs\u2060WHERE\ufeffstatus = 200',
    )
    expect(unicodeSpaceStripped).toContain('$\u00a0$keep$\u00a0$')
    expect(unicodeSpaceStripped).toContain('status FROM logs WHERE status = 200')

    // Wave-2 comment-splice vectors: stripSqlComments preserves $a$...$a$ verbatim so ops_secret is never resurrected
    const splice1 = "SELECT $a$ -- $a$ /*\n$a$, secret_col FROM ops_secret -- */ FROM logs"
    const norm1 = validateUserSql(splice1, ['logs', 'requests']).normalizedSql
    expect(norm1).not.toContain('ops_secret')
    const rows1 = await engine.executeSql(norm1)
    expect(JSON.stringify(rows1)).not.toContain('TOP_SECRET_OPS_ROW')

    const splice2 = "SELECT $a$ /* $a$ FROM logs WHERE $b$ */ $a$ AS pad, secret_col FROM ops_secret -- $b$"
    const norm2 = validateUserSql(splice2, ['logs', 'requests']).normalizedSql
    await expect(engine.executeSql(norm2)).rejects.toThrow()
  })

  it('rejects FROM ONLY / JOIN ONLY disallowed tables even when quoted CTE "only" is in scope while preserving valid quoted "only" CTE and FROM ONLY logs', async () => {
    for (const blockedOnlySql of [
      'WITH "only" AS (SELECT 1 AS x) SELECT secret_col FROM ONLY ops_secret',
      'WITH "only" AS (SELECT 1 AS x) SELECT b.secret_col FROM logs AS a CROSS JOIN ONLY ops_secret AS b',
      'WITH "only" AS (SELECT 1 AS x) SELECT b.secret_col FROM logs AS a, ONLY ops_secret AS b',
      'WITH "only" AS (SELECT 1 AS x) SELECT secret_col FROM ONLY main.ops_secret',
      'WITH "only" AS (SELECT 1 AS x) SELECT secret_col FROM ONLY (ops_secret)',
      'WITH "only" AS (SELECT 1 AS x) SELECT count(*) AS n FROM ONLY client_vitals',
      'SELECT * FROM ONLY WHERE 1=1',
      'SELECT secret_col FROM ONLY ops_secret',
    ]) {
      expect(() => validateUserSql(blockedOnlySql, ['logs', 'requests']), blockedOnlySql).toThrow(
        SQLValidationError,
      )
    }

    const quotedCteSql = 'WITH "only" AS (SELECT 1 AS x) SELECT * FROM "only"'
    expect(() => validateUserSql(quotedCteSql, ['logs', 'requests'])).not.toThrow()
    const cteRows = await engine.executeSql(quotedCteSql)
    expect(cteRows).toEqual([{ x: 1 }])

    expect(() => validateUserSql('SELECT status FROM ONLY logs WHERE status = 200', ['logs', 'requests'])).not.toThrow()
    expect(() => validateUserSql('SELECT status FROM ONLY main.logs WHERE status = 200', ['logs', 'requests'])).not.toThrow()
  })

  it('closes SEC-T5-ITER7-FE-01: blocks numeric-underscore/radix dollar-quote smuggling and non-keyword ./::/AS clause transitions while preserving valid DuckDB numbers and typecasts', async () => {
    for (const smuggledSql of [
      'SELECT 1_$$, (SELECT secret_col FROM ops_secret) AS leaked, 2_$$ FROM logs',
      'SELECT 1.5_$$, (SELECT secret_col FROM ops_secret) AS leaked, 2.5_$$ FROM logs',
      'SELECT 1e2_$$, (SELECT secret_col FROM ops_secret) AS leaked, 2e2_$$ FROM logs',
      'SELECT 0x1$$, (SELECT secret_col FROM ops_secret) AS leaked, 0x2$$ FROM logs',
      'SELECT 0b1$$, (SELECT secret_col FROM ops_secret) AS leaked, 0b0$$ FROM logs',
      'SELECT 0o1$$, (SELECT secret_col FROM ops_secret) AS leaked, 0o2$$ FROM logs',
      'SELECT 1_$t$, (SELECT secret_col FROM ops_secret) AS leaked, 2_$t$ FROM logs',
      "SELECT 1_$$, current_setting('threads') AS leaked, 2_$$ FROM logs",
      "SELECT 1_$$, (SELECT table_name FROM information_schema.tables WHERE table_name = 'ops_secret') AS leaked, 2_$$ FROM logs",
      'SELECT c.secret_col FROM logs AS a JOIN logs AS b ON 1::union(x INT) IS NOT NULL, ops_secret AS c',
      'SELECT c.secret_col FROM (SELECT 1 AS "where") AS a JOIN logs AS b ON a.where = 1, ops_secret AS c',
      'SELECT c.secret_col FROM (SELECT 1 AS "select") AS a JOIN logs AS b ON a.select = 1, ops_secret AS c',
      'SELECT c.secret_col FROM (SELECT 1 AS "order") AS a JOIN logs AS b ON a.order = 1, ops_secret AS c',
      'SELECT c.secret_col FROM (SELECT 1 AS "limit") AS a JOIN logs AS b ON a.limit = 1, ops_secret AS c',
      'SELECT c.secret_col FROM (SELECT 1 AS "qualify") AS a JOIN logs AS b ON a.qualify = 1, ops_secret AS c',
      'SELECT c.secret_col FROM (SELECT 1 AS "union") AS a JOIN logs AS b ON a.union = 1, ops_secret AS c',
      'SELECT c.secret_col FROM (SELECT 1 AS "except") AS a JOIN logs AS b ON a.except = 1, ops_secret AS c',
      'SELECT c.secret_col FROM (SELECT 1 AS "intersect") AS a JOIN logs AS b ON a.intersect = 1, ops_secret AS c',
      'SELECT b.secret_col FROM logs AS sample, ops_secret AS b',
      'SELECT b.secret_col FROM logs sample, ops_secret AS b',
      'SELECT b.secret_col FROM logs prewhere, ops_secret AS b',
      'SELECT b.secret_col FROM logs settings, ops_secret AS b',
      'SELECT b.secret_col FROM logs format, ops_secret AS b',
    ]) {
      expect(() => validateUserSql(smuggledSql, ['logs', 'requests']), smuggledSql).toThrow(
        SQLValidationError,
      )
    }

    for (const benignSql of [
      'SELECT 1_000_000 AS n, 1.5_0e+1_0 AS f FROM logs LIMIT 1',
      'SELECT a.where FROM (SELECT 1 AS "where") AS a JOIN logs AS b ON a.where = 1 LIMIT 1',
      'SELECT 1::union(x INT) AS u FROM logs LIMIT 1',
    ]) {
      expect(() => validateUserSql(benignSql, ['logs', 'requests']), benignSql).not.toThrow()
      const rows = await engine.executeSql(benignSql)
      expect(rows.length, benignSql).toBe(1)
    }
  })
})
