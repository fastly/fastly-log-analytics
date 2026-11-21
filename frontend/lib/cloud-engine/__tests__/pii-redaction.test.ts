// @vitest-environment node
import { afterAll, beforeAll, describe, expect, it } from 'vitest'
import { DuckDBAnalyticalEngine } from '../dashboard/duckdb'
import { executeCloudQuery } from '../query'
import { handleSessionsDetail, sealSessionToken } from '../sessions'

const SHARED_18_PII_KEYS = [
  'ip',
  'client_ip',
  'c_ip',
  'ip_address',
  'remote_addr',
  'oip',
  'x-forwarded-for',
  'x_forwarded_for',
  'true-client-ip',
  'true_client_ip',
  'fastly-client-ip',
  'fastly_client_ip',
  'cookie_session',
  'cid',
  'client_id',
  'edge_sid',
  'sid',
  'session_id',
] as const

describe('T5-FE-05: cloud-engine executeCloudQuery shadow-masked CTE and handleSessionsDetail PII redaction', () => {
  let engine: DuckDBAnalyticalEngine

  beforeAll(async () => {
    engine = new DuckDBAnalyticalEngine()
    const extraCols = [
      ...SHARED_18_PII_KEYS,
      'xff',
      'origin_ip',
      'cmcd_sid',
      'cmcd_cid',
    ]
    const colDefs = [
      '"timestamp" TIMESTAMPTZ',
      '"ts_ms" BIGINT',
      '"url" VARCHAR',
      '"status" BIGINT',
      '"ja4" VARCHAR',
      ...extraCols.map((c) => `"${c}" VARCHAR`),
    ].join(', ')
    await engine.executeSql('DROP VIEW IF EXISTS main.logs')
    await engine.executeSql('DROP VIEW IF EXISTS main.requests')
    await engine.executeSql('DROP VIEW IF EXISTS main.client_vitals')
    await engine.executeSql(`CREATE OR REPLACE TABLE main.requests (${colDefs})`)

    const vals = [
      "'2026-11-20T12:00:00Z'::TIMESTAMPTZ",
      '1795176000000',
      "'/checkout'",
      '200',
      "'t13d1516h2_8daaf6152771_b0da82dd1658'",
      ...extraCols.map((c, idx) =>
        c.includes('sid') || c.includes('cid') || c.includes('session') || c === 'client_id'
          ? `'CANARY_SESS_${idx}_${c}'`
          : `'203.0.113.${40 + idx}'`,
      ),
    ].join(', ')
    await engine.executeSql(`INSERT INTO main.requests VALUES (${vals})`)

    await engine.executeSql(
      'CREATE OR REPLACE TABLE main.client_vitals ("timestamp" TIMESTAMPTZ, "client_id" VARCHAR, "session_id" VARCHAR, "url" VARCHAR)',
    )
    await engine.executeSql(
      "INSERT INTO main.client_vitals VALUES ('2026-11-20T12:00:00Z'::TIMESTAMPTZ, 'CANARY_RUM_CID_789', 'CANARY_RUM_SID_012', '/home')",
    )
  })

  afterAll(async () => {
    await engine.executeSql('DROP TABLE IF EXISTS main.requests')
    await engine.executeSql('DROP TABLE IF EXISTS main.client_vitals')
    await engine.reset()
  })

  it('prevents alias/expression PII leaks, WHERE ip boolean oracles, CTE bypasses, SUMMARIZE leaks, and RUM ID leaks under maskIps', async () => {
    const runSql = (sql: string) => engine.executeSql(sql)

    // 1. Aliasing and scalar expressions over PII columns cannot recover raw canary values
    const aliasRes = await executeCloudQuery(
      {
        sql: 'SELECT ip AS alias_ip, concat(client_ip, \'\') AS expr_ip, cookie_session AS s, origin_ip, "x-forwarded-for" AS xff_hdr, cmcd_sid FROM logs',
      },
      { maskIps: true, tableName: 'requests', allowedTables: ['logs', 'requests'], runSql },
    )
    expect(aliasRes.status).toBe(200)
    const serializedAlias = JSON.stringify(aliasRes.body)
    expect(serializedAlias).not.toContain('203.0.113.')
    expect(serializedAlias).not.toContain('CANARY_SESS_')

    // 2. WHERE ip = '<raw_ip>' oracle returns count = 0 because shadow-masked CTE runs BEFORE user WHERE
    const oracleRes = await executeCloudQuery(
      { sql: "SELECT count(*) AS n FROM logs WHERE ip = '203.0.113.40'" },
      { maskIps: true, tableName: 'requests', allowedTables: ['logs', 'requests'], runSql },
    )
    expect(oracleRes.status).toBe(200)
    if ('data' in oracleRes.body) {
      expect(Number(oracleRes.body.data[0].n)).toBe(0)
    }

    // 3. User queries starting with WITH ... merge cleanly with shadow-masked CTE
    const withRes = await executeCloudQuery(
      { sql: 'WITH sub AS (SELECT ip AS sub_ip, url FROM logs) SELECT sub_ip, url FROM sub' },
      { maskIps: true, tableName: 'requests', allowedTables: ['logs', 'requests'], runSql },
    )
    expect(withRes.status).toBe(200)
    expect(JSON.stringify(withRes.body)).not.toContain('203.0.113.')

    // 4. SUMMARIZE logs does not leak canary min/max IPs or session IDs
    const sumRes = await executeCloudQuery(
      { sql: 'SUMMARIZE logs' },
      { maskIps: true, tableName: 'requests', allowedTables: ['logs', 'requests'], runSql },
    )
    expect(sumRes.status).toBe(200)
    const serializedSum = JSON.stringify(sumRes.body)
    expect(serializedSum).not.toContain('203.0.113.')
    expect(serializedSum).not.toContain('CANARY_SESS_')

    // 5. client_vitals client_id and session_id are redacted
    const rumRes = await executeCloudQuery(
      { sql: 'SELECT client_id, session_id, url FROM client_vitals' },
      {
        maskIps: true,
        tableName: 'client_vitals',
        allowedTables: ['client_vitals'],
        runSql,
      },
    )
    expect(rumRes.status).toBe(200)
    const serializedRum = JSON.stringify(rumRes.body)
    expect(serializedRum).not.toContain('CANARY_RUM_CID_789')
    expect(serializedRum).not.toContain('CANARY_RUM_SID_012')
  })

  it('redacts all canonical PII columns in handleSessionsDetail and rejects raw non-placeholder ip filters when maskIps is true', async () => {
    const runSql = (sql: string) => engine.executeSql(sql)

    // Raw non-placeholder IP without token is blocked with 403
    const rawIpRes = await handleSessionsDetail(
      {
        ip: '203.0.113.40',
        start_time: '2026-11-20T11:55:00Z',
        end_time: '2026-11-20T12:05:00Z',
      },
      { maskIps: true, tableName: 'requests', availableColumns: new Set(['ts_ms']), runSql },
    )
    expect(rawIpRes.status).toBe(403)

    // Valid session_token drill-down redacts all 18 shared PII keys + origin_ip + xff + cmcd_sid + cmcd_cid
    const token = sealSessionToken({
      ip: '203.0.113.40',
      ja4: 't13d1516h2_8daaf6152771_b0da82dd1658',
      s: '2026-11-20T11:55:00Z',
      e: '2026-11-20T12:05:00Z',
      svc: 'svc-1',
    })
    const detailRes = await handleSessionsDetail(
      { session_token: token },
      { maskIps: true, tableName: 'requests', availableColumns: new Set(['ts_ms']), runSql },
    )
    expect(detailRes.status).toBe(200)
    const text = JSON.stringify(detailRes.body)
    expect(text).not.toContain('203.0.113.')
    expect(text).not.toContain('CANARY_SESS_')
  })
})
