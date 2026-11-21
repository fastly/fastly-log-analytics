// @vitest-environment node
import { afterAll, beforeAll, describe, expect, it } from 'vitest'
import { DuckDBAnalyticalEngine } from '../dashboard/duckdb'
import { executeCloudQuery, getQuerySchema, redactQueryRowsForPii, rewriteCloudQuerySql } from '../query'
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
      '"ua" VARCHAR',
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
      "'/checkout?client_ip=203.0.113.40&v6=2001:db8::dead:beef'",
      "'Mozilla/5.0 Chrome/131.0.6778.85 (XFF 203.0.113.40)'",
      '200',
      "'t13d1516h2_8daaf6152771_b0da82dd1658'",
      ...extraCols.map((c, idx) =>
        c.includes('sid') || c.includes('cid') || c.includes('session') || c === 'client_id'
          ? `'CANARY_SESS_${idx}_${c}'`
          : `'203.0.113.${40 + idx}'`,
      ),
    ].join(', ')
    await engine.executeSql(`INSERT INTO main.requests VALUES (${vals})`)
    await engine.executeSql('CREATE OR REPLACE VIEW main.logs AS SELECT * FROM main.requests')

    await engine.executeSql(
      'CREATE OR REPLACE TABLE main.client_vitals ("timestamp" TIMESTAMPTZ, "client_id" VARCHAR, "session_id" VARCHAR, "url" VARCHAR)',
    )
    await engine.executeSql(
      "INSERT INTO main.client_vitals VALUES ('2026-11-20T12:00:00Z'::TIMESTAMPTZ, 'CANARY_RUM_CID_789', 'CANARY_RUM_SID_012', '/home')",
    )
    await engine.executeSql('CREATE OR REPLACE TABLE main.ops_secret (secret_col VARCHAR)')
    await engine.executeSql("INSERT INTO main.ops_secret VALUES ('TOP_SECRET_CANARY')")
  })

  afterAll(async () => {
    await engine.executeSql('DROP VIEW IF EXISTS main.logs')
    await engine.executeSql('DROP TABLE IF EXISTS main.requests')
    await engine.executeSql('DROP TABLE IF EXISTS main.client_vitals')
    await engine.executeSql('DROP TABLE IF EXISTS main.ops_secret')
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

    // 5. client_vitals client_id and session_id are redacted (including main.client_vitals and multi-table joins)
    for (const rumSql of [
      'SELECT client_id AS leaked_cid, session_id AS leaked_sid, url FROM client_vitals',
      'SELECT client_id AS leaked_cid, session_id AS leaked_sid, url FROM main.client_vitals',
      'SELECT client_id AS leaked_cid, session_id AS leaked_sid, url FROM "main"."client_vitals"',
    ]) {
      const rumRes = await executeCloudQuery(
        { sql: rumSql },
        {
          maskIps: true,
          tableName: 'client_vitals',
          allowedTables: ['client_vitals'],
          runSql,
        },
      )
      expect(rumRes.status, rumSql).toBe(200)
      const serializedRum = JSON.stringify(rumRes.body)
      expect(serializedRum, rumSql).not.toContain('CANARY_RUM_CID_789')
      expect(serializedRum, rumSql).not.toContain('CANARY_RUM_SID_012')
    }

    // 6. SEC-T5-ITER1-03: Quoted "main"."requests", main."requests", "logs", comma-joins, parenthesized joins, and multi-table queries
    for (const bypassSql of [
      'SELECT b.ip AS leaked_ip, b.cookie_session AS leaked_sess FROM range(1) AS a, main.requests AS b',
      'SELECT b.ip AS leaked_ip, b.cookie_session AS leaked_sess FROM range(1) AS a, "main"."requests" AS b',
      'SELECT ip AS leaked_ip, cookie_session AS leaked_sess FROM main."requests"',
      'SELECT ip AS leaked_ip, cookie_session AS leaked_sess FROM "main"."requests"',
      'SELECT ip AS leaked_ip, cookie_session AS leaked_sess FROM "logs"',
      'SELECT r.ip AS leaked_ip FROM (main.requests AS r CROSS JOIN range(1))',
      'SELECT v.client_id AS leaked_cid, r.ip AS leaked_ip FROM requests AS r CROSS JOIN client_vitals AS v',
      'SELECT v.client_id AS leaked_cid, r.ip AS leaked_ip FROM main.requests AS r, main.client_vitals AS v',
      'SUMMARIZE main."requests"',
      'SUMMARIZE "main"."logs"',
      'SUMMARIZE main.client_vitals',
      'DESCRIBE "requests"',
    ]) {
      const res = await executeCloudQuery(
        { sql: bypassSql },
        {
          maskIps: true,
          tableName: 'requests',
          allowedTables: ['logs', 'requests', 'client_vitals'],
          runSql,
        },
      )
      expect(res.status, bypassSql).toBe(200)
      const serialized = JSON.stringify(res.body)
      expect(serialized, bypassSql).not.toContain('203.0.113.')
      expect(serialized, bypassSql).not.toContain('CANARY_SESS_')
      expect(serialized, bypassSql).not.toContain('CANARY_RUM_')
    }

    // Boolean WHERE count oracles via comma-join and quoted main."requests" return 0
    for (const countOracleSql of [
      "SELECT count(*) AS n FROM range(1) AS a, main.requests AS b WHERE b.ip = '203.0.113.40'",
      'SELECT count(*) AS n FROM "main"."requests" WHERE ip = \'203.0.113.40\'',
      'SELECT count(*) AS n FROM "logs" WHERE ip = \'203.0.113.40\'',
    ]) {
      const cRes = await executeCloudQuery(
        { sql: countOracleSql },
        { maskIps: true, tableName: 'requests', allowedTables: ['logs', 'requests'], runSql },
      )
      expect(cRes.status, countOracleSql).toBe(200)
      if ('data' in cRes.body) {
        expect(Number(cRes.body.data[0].n), countOracleSql).toBe(0)
      }
    }

    // String literals containing 'FROM main.requests' are preserved verbatim while table refs are rewritten
    const strLitRes = await executeCloudQuery(
      { sql: 'SELECT \'FROM main.requests\' AS note, ip AS alias_ip, url, ua FROM "main"."requests"' },
      { maskIps: true, tableName: 'requests', allowedTables: ['logs', 'requests'], runSql },
    )
    expect(strLitRes.status).toBe(200)
    if ('data' in strLitRes.body) {
      expect(strLitRes.body.data[0].note).toBe('FROM main.requests')
      expect(strLitRes.body.data[0].alias_ip).toBe('0.0.0.0')
      expect(String(strLitRes.body.data[0].url)).not.toContain('203.0.113.40')
      expect(String(strLitRes.body.data[0].url)).not.toContain('2001:db8::dead:beef')
      expect(String(strLitRes.body.data[0].ua)).toContain('Chrome/131.0.6778.85')
      expect(String(strLitRes.body.data[0].ua)).not.toContain('203.0.113.40')
    }

    // User CTE colliding with reserved/allowed table names is rejected with 403 sql_not_permitted under maskIps
    for (const collidingCteSql of [
      "WITH requests AS (SELECT 'x' AS ip) SELECT * FROM requests",
      'WITH "logs" AS (SELECT 1 AS x) SELECT * FROM "logs"',
      'WITH client_vitals AS (SELECT 1 AS x) SELECT * FROM client_vitals',
    ]) {
      const cteErr = await executeCloudQuery(
        { sql: collidingCteSql },
        {
          maskIps: true,
          tableName: 'requests',
          allowedTables: ['logs', 'requests', 'client_vitals'],
          runSql,
        },
      )
      expect(cteErr.status, collidingCteSql).toBe(403)
    }

    // 7. getQuerySchema strips all 18 SHARED_18_PII_KEYS + origin_ip + cmcd_* when maskIps is true
    const defaultMaskedSchema = getQuerySchema('svc-1', true)
    expect(defaultMaskedSchema.schema.map((c) => c.column)).not.toContain('ip')
    expect(defaultMaskedSchema.schema.map((c) => c.column)).not.toContain('cookie_session')
    const customMaskedSchema = getQuerySchema('svc-1', true, [
      ...SHARED_18_PII_KEYS.map((column) => ({ column, type: 'VARCHAR' })),
      { column: 'origin_ip', type: 'VARCHAR' },
      { column: 'xff', type: 'VARCHAR' },
      { column: 'cmcd_session', type: 'VARCHAR' },
      { column: 'cmcd_sid', type: 'VARCHAR' },
      { column: 'cmcd_cid', type: 'VARCHAR' },
      { column: 'url', type: 'VARCHAR' },
      { column: 'status', type: 'BIGINT' },
    ])
    expect(customMaskedSchema.schema.map((c) => c.column)).toEqual(['url', 'status'])
  })

  it('redacts all canonical PII columns and embedded URL/UA IPs in handleSessionsDetail when maskIps is true', async () => {
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
    expect(text).not.toContain('2001:db8::dead:beef')
    expect(text).not.toContain('CANARY_SESS_')
    expect(text).toContain('Chrome/131.0.6778.85')
  })

  it('closes SEC-T5-ITER2-01..04: nested LIST/STRUCT/MAP/BLOB scrubbing, pre-query split_part/LIKE embedded-IP scrubbing, AS "distinct" rewriter parity, and SHOW COLUMNS / SUMMARIZE SELECT support under maskIps', async () => {
    const runSql = (sql: string) => engine.executeSql(sql)

    // 1. Disallowed E-string table, bare/(DESCRIBE) catalog enumeration, and AS "distinct" disallowed-table vectors return 403
    for (const blockedSql of [
      "SELECT e'\\'', ip FROM ops_secret -- '\nLIMIT 5",
      'DESCRIBE',
      'SELECT * FROM (DESCRIBE)',
      'SELECT 1 AS "distinct" FROM \'nonexistent_probe.parquet\'',
    ]) {
      const r = await executeCloudQuery(
        { sql: blockedSql },
        { maskIps: true, tableName: 'requests', allowedTables: ['logs', 'requests'], runSql },
      )
      expect(r.status, blockedSql).toBe(403)
    }

    // 2. Rewriter parity on E'\'' + main.requests, AS "distinct" FROM logs, TRIM(FROM url), and multi-parenthesized subqueries
    for (const paritySql of [
      "SELECT E'\\'', replace(ip, '.', '_') AS raw_ip, cookie_session AS raw_sess FROM main.requests -- '\nLIMIT 5",
      "SELECT E'x\\'' AS pad, ip AS leaked_ip, cookie_session AS leaked_sess FROM main.requests /*'*/",
      'SELECT cookie_session AS "distinct", replace(ip, \'.\', \'_\') AS ip_u FROM logs',
      'SELECT TRIM(FROM url) AS trimmed_url, cookie_session AS s FROM logs',
      'WITH p AS ((SELECT cookie_session AS s FROM logs) UNION ALL SELECT cookie_session AS s FROM main.requests) SELECT * FROM p',
    ]) {
      const r = await executeCloudQuery(
        { sql: paritySql },
        { maskIps: true, tableName: 'requests', allowedTables: ['logs', 'requests'], runSql },
      )
      expect(r.status, paritySql).toBe(200)
      const dumped = JSON.stringify(r.body)
      expect(dumped, paritySql).not.toContain('CANARY_SESS_')
      expect(dumped, paritySql).not.toContain('203_0_113')
      expect(dumped, paritySql).not.toContain('203.0.113.')
      expect(dumped, paritySql).not.toContain('2001:db8::dead:beef')
    }

    expect(
      rewriteCloudQuerySql('SELECT url AS "distinct" FROM logs', 'svc_table').rewrittenSql,
    ).toBe('SELECT url AS "distinct" FROM svc_table LIMIT 1000')
    expect(
      rewriteCloudQuerySql(
        'WITH p AS ((SELECT url FROM logs) UNION ALL SELECT url FROM logs) SELECT * FROM p',
        'svc_table',
      ).rewrittenSql,
    ).toBe(
      'WITH p AS ((SELECT url FROM svc_table) UNION ALL SELECT url FROM svc_table) SELECT * FROM p LIMIT 1000',
    )

    // 3. Nested containers ([url], {'u': url}, MAP {'u': url}, encode(url)) + split_part(url, '.', 1..4) + WHERE url LIKE '%<ip>%'
    const containerRes = await executeCloudQuery(
      {
        sql: "SELECT [url] AS url_list, {'u': url, 'ua': ua} AS url_struct, MAP {'u': url} AS url_map, encode(url) AS url_bytes FROM requests",
      },
      { maskIps: true, tableName: 'requests', allowedTables: ['logs', 'requests'], runSql },
    )
    expect(containerRes.status).toBe(200)
    const containerText = JSON.stringify(containerRes.body)
    expect(containerText).not.toContain('203.0.113.')
    expect(containerText).not.toContain('2001:db8::dead:beef')
    expect(containerText).toContain('Chrome/131.0.6778.85')
    if ('data' in containerRes.body) {
      const rawBytes = containerRes.body.data[0].url_bytes as Uint8Array
      expect(rawBytes).toBeInstanceOf(Uint8Array)
      const decodedBytes = new TextDecoder().decode(rawBytes)
      expect(decodedBytes).not.toContain('203.0.113.40')
      expect(decodedBytes).not.toContain('2001:db8::dead:beef')
      expect(decodedBytes).toContain('0.0.0.0')
    }

    const splitRes = await executeCloudQuery(
      {
        sql: "SELECT split_part(url, '.', 1) AS p1, split_part(url, '.', 2) AS p2, split_part(url, '.', 3) AS p3, split_part(url, '.', 4) AS p4 FROM logs",
      },
      { maskIps: true, tableName: 'requests', allowedTables: ['logs', 'requests'], runSql },
    )
    expect(splitRes.status).toBe(200)
    if ('data' in splitRes.body) {
      const row = splitRes.body.data[0]
      expect(`${row.p1}.${row.p2}.${row.p3}.${row.p4}`).not.toContain('203.0.113.40')
      expect(String(row.p1)).not.toContain('203')
      expect(String(row.p3)).not.toBe('113')
    }

    for (const likeProbe of [
      "SELECT count(*) AS hit FROM requests WHERE url LIKE '%203.0.113.40%'",
      "SELECT count(*) AS hit FROM requests WHERE url LIKE '%2001:db8::dead:beef%'",
      "SELECT count(*) AS hit FROM requests WHERE ua LIKE '%203.0.113.40%'",
    ]) {
      const likeRes = await executeCloudQuery(
        { sql: likeProbe },
        { maskIps: true, tableName: 'requests', allowedTables: ['logs', 'requests'], runSql },
      )
      expect(likeRes.status, likeProbe).toBe(200)
      if ('data' in likeRes.body) {
        expect(Number(likeRes.body.data[0].hit), likeProbe).toBe(0)
      }
    }

    // UA version token and ISO timestamp remain intact pre-query inside shadow CTE
    const uaKeepRes = await executeCloudQuery(
      {
        sql: "SELECT count(*) AS hit, CAST(timestamp AS VARCHAR) AS ts_str FROM requests WHERE ua LIKE '%Chrome/131.0.6778.85%' GROUP BY timestamp",
      },
      { maskIps: true, tableName: 'requests', allowedTables: ['logs', 'requests'], runSql },
    )
    expect(uaKeepRes.status).toBe(200)
    if ('data' in uaKeepRes.body) {
      expect(Number(uaKeepRes.body.data[0].hit)).toBe(1)
      expect(String(uaKeepRes.body.data[0].ts_str)).toContain('12:00:00')
    }

    // 4. SHOW COLUMNS FROM logs & DESCRIBE logs strip PII columns; SUMMARIZE SELECT ... executes cleanly under maskIps
    for (const schemaSql of ['SHOW COLUMNS FROM logs', 'SHOW COLUMNS FROM main.requests', 'DESCRIBE logs']) {
      const showRes = await executeCloudQuery(
        { sql: schemaSql },
        { maskIps: true, tableName: 'requests', allowedTables: ['logs', 'requests'], runSql },
      )
      expect(showRes.status, schemaSql).toBe(200)
      if ('data' in showRes.body) {
        const colNames = showRes.body.data.map((r) => String(r.column_name))
        expect(colNames, schemaSql).toContain('url')
        expect(colNames, schemaSql).toContain('status')
        expect(colNames, schemaSql).not.toContain('ip')
        expect(colNames, schemaSql).not.toContain('cookie_session')
        expect(colNames, schemaSql).not.toContain('x-forwarded-for')
      }
    }

    for (const sumSelectSql of [
      'SUMMARIZE SELECT status, count(*) AS c FROM logs GROUP BY status',
      'SUMMARIZE (SELECT ip, cookie_session, url FROM logs)',
      'DESCRIBE SELECT status, url FROM logs',
    ]) {
      const sumSelRes = await executeCloudQuery(
        { sql: sumSelectSql },
        { maskIps: true, tableName: 'requests', allowedTables: ['logs', 'requests'], runSql },
      )
      expect(sumSelRes.status, sumSelectSql).toBe(200)
      const sumText = JSON.stringify(sumSelRes.body)
      expect(sumText, sumSelectSql).not.toContain('203.0.113.')
      expect(sumText, sumSelectSql).not.toContain('CANARY_SESS_')
    }

    // 5. Direct redactQueryRowsForPii recursive container + undecodable binary Uint8Array scrubbing
    const directRedacted = redactQueryRowsForPii(
      [
        {
          arr: ['ip=203.0.113.40', { deep: 'v6=2001:db8::dead:beef', ua: 'Chrome/131.0.6778.85' }],
          mp: new Map<string, unknown>([
            ['u', '/p?ip=203.0.113.40'],
            ['cookie_session', 'RAW_SESS'],
          ]),
          utf8Bytes: new TextEncoder().encode('hit 203.0.113.40 ok'),
          badBinary: new Uint8Array([0xff, 0xfe, 0xfd, 50, 48, 51]),
        },
      ],
      true,
    )
    expect(JSON.stringify(directRedacted[0].arr)).not.toContain('203.0.113.40')
    expect(JSON.stringify(directRedacted[0].arr)).not.toContain('2001:db8::dead:beef')
    expect(JSON.stringify(directRedacted[0].arr)).toContain('Chrome/131.0.6778.85')
    const outMap = directRedacted[0].mp as Map<string, unknown>
    expect(outMap.get('u')).toBe('/p?ip=0.0.0.0')
    expect(outMap.get('cookie_session')).toBe('[redacted]')
    expect(new TextDecoder().decode(directRedacted[0].utf8Bytes as Uint8Array)).toBe('hit 0.0.0.0 ok')
    expect((directRedacted[0].badBinary as Uint8Array).byteLength).toBe(0)
  })

  it('closes SEC-T5-ITER3-01..03 & Unicode-space differentials: nested subquery WITH collisions, DESCRIBE TABLE PII column stripping, and WINDOW/PIVOT/SHOW/Unicode-ws isolation under maskIps', async () => {
    const runSql = (sql: string) => engine.executeSql(sql)

    // 1. Nested subquery WITH colliding with reserved/protected tables inside an outer WITH body is rejected with 403
    for (const nestedCollisionSql of [
      "WITH outer_cte AS (SELECT * FROM (WITH requests AS (SELECT 'LEAK' AS ip) SELECT * FROM requests)) SELECT * FROM outer_cte",
      'WITH outer_cte AS (SELECT * FROM logs WHERE EXISTS (WITH "logs" AS (SELECT 1) SELECT * FROM "logs")) SELECT * FROM outer_cte',
      'WITH outer_cte AS (SELECT * FROM (WITH client_vitals AS (SELECT 1 AS x) SELECT * FROM client_vitals)) SELECT * FROM outer_cte',
      'WITH outer_cte AS (SELECT * FROM (WITH client_errors AS (SELECT 1 AS x) SELECT * FROM client_errors)) SELECT * FROM outer_cte',
    ]) {
      const r = await executeCloudQuery(
        { sql: nestedCollisionSql },
        { maskIps: true, tableName: 'requests', allowedTables: ['logs', 'requests'], runSql },
      )
      expect(r.status, nestedCollisionSql).toBe(403)
    }

    // 2. DESCRIBE TABLE/FROM/(logs)/(TABLE logs)/(FROM logs)/SELECT * strips PII column names under maskIps: true
    for (const descTableSql of [
      'DESCRIBE TABLE logs',
      'DESCRIBE TABLE main.requests',
      'DESCRIBE TABLE "requests"',
      'DESCRIBE FROM logs',
      'DESCRIBE (logs)',
      'DESCRIBE (TABLE logs)',
      'DESCRIBE (FROM logs)',
      'DESCRIBE SELECT * FROM logs',
    ]) {
      const descRes = await executeCloudQuery(
        { sql: descTableSql },
        { maskIps: true, tableName: 'requests', allowedTables: ['logs', 'requests'], runSql },
      )
      expect(descRes.status, descTableSql).toBe(200)
      if ('data' in descRes.body) {
        const colNames = descRes.body.data.map((r) => String(r.column_name))
        expect(colNames, descTableSql).toContain('url')
        expect(colNames, descTableSql).toContain('status')
        expect(colNames, descTableSql).not.toContain('ip')
        expect(colNames, descTableSql).not.toContain('cookie_session')
        expect(colNames, descTableSql).not.toContain('x-forwarded-for')
      }
    }

    // 2b. Whitespace-less WITH"c" explain merges cleanly with shadow CTEs under maskIps: true
    const explainWithQuotedRes = await executeCloudQuery(
      { sql: 'WITH"c" AS (SELECT status FROM logs) SELECT * FROM "c"', explain: true },
      { maskIps: true, tableName: 'requests', allowedTables: ['logs', 'requests'], runSql },
    )
    expect(explainWithQuotedRes.status).toBe(200)
    if ('explain_plan' in explainWithQuotedRes.body) {
      expect(explainWithQuotedRes.body.explain_plan).toBeTruthy()
    }

    // 3. Wave-3 PIVOT_LONGER/PIVOT_WIDER, memory catalog, WINDOW fake-CTE, self/sibling/subquery CTE, and subquery SHOW/DESC/DESCRIBE all return 403
    for (const wave3BlockedSql of [
      'WITH p AS (PIVOT_LONGER memory.main.requests ON cookie_session) SELECT value AS leaked_sess FROM p',
      'WITH p AS (PIVOT_WIDER memory.main.requests ON cookie_session) SELECT * FROM p',
      'SELECT client_id AS leaked_cid, session_id AS leaked_sid FROM client_vitals WINDOW w AS (), client_vitals AS ()',
      'WITH client_vitals AS (SELECT client_id FROM client_vitals) SELECT * FROM client_vitals',
      'WITH a AS (SELECT client_id FROM client_vitals), client_vitals AS (SELECT 1) SELECT * FROM a',
      'SELECT client_id, (WITH client_vitals AS (SELECT 1) SELECT * FROM client_vitals) FROM client_vitals',
      'SELECT ARRAY(SHOW TABLES) AS all_tbls',
      "SELECT status FROM logs WHERE 'client_vitals' IN (SHOW TABLES)",
      'SELECT status FROM logs WHERE EXISTS (DESC client_vitals)',
      'SELECT * FROM (DESCRIBE logs)',
      'SELECT * FROM (SUMMARIZE logs)',
      "SELECT E'a'\u00a0\n'\\'' AS pad, client_id FROM client_vitals WHERE 'x' != E'\\''",
      "SELECT 1,\u00a0$a$'$a$ AS a, client_id FROM client_vitals WHERE $b$'$b$ != $b$y$b$",
    ]) {
      const r = await executeCloudQuery(
        { sql: wave3BlockedSql },
        { maskIps: true, tableName: 'requests', allowedTables: ['logs', 'requests'], runSql },
      )
      expect(r.status, wave3BlockedSql).toBe(403)
    }

    // 4. Unicode ZWSP/NBSP around main.requests still rewrites and masks all PII columns cleanly
    const unicodeMaskedRes = await executeCloudQuery(
      { sql: "SELECT replace(ip, '.', '_') AS raw_ip, cookie_session AS s FROM\u200bmain.requests" },
      { maskIps: true, tableName: 'requests', allowedTables: ['logs', 'requests'], runSql },
    )
    expect(unicodeMaskedRes.status).toBe(200)
    const unicodeText = JSON.stringify(unicodeMaskedRes.body)
    expect(unicodeText).not.toContain('203_0_113')
    expect(unicodeText).not.toContain('CANARY_SESS_')

    // 5. Benign chained WITH CTEs, ORDER BY ... DESC, and WINDOW clauses execute cleanly with PII masked
    for (const benignSql of [
      'WITH c1 AS (SELECT status, ip, cookie_session FROM logs), c2 AS (SELECT * FROM c1) SELECT * FROM c2 ORDER BY status DESC',
      'SELECT status, count(*) OVER w AS n FROM logs WINDOW w AS (PARTITION BY status)',
    ]) {
      const okRes = await executeCloudQuery(
        { sql: benignSql },
        { maskIps: true, tableName: 'requests', allowedTables: ['logs', 'requests'], runSql },
      )
      expect(okRes.status, benignSql).toBe(200)
      const okText = JSON.stringify(okRes.body)
      expect(okText, benignSql).not.toContain('203.0.113.')
      expect(okText, benignSql).not.toContain('CANARY_SESS_')
    }

    // 6. SEC-T5-ITER5-01: WITH "only" AS (...) SELECT ... FROM ONLY <disallowed> returns 403,
    // while quoted CTE SELECT * FROM "only" and FROM ONLY logs / FROM ONLY main.logs work with PII masked
    for (const blockedOnlySql of [
      'WITH "only" AS (SELECT 1 AS x) SELECT client_id FROM ONLY client_vitals',
      'WITH "only" AS (SELECT 1 AS x) SELECT b.client_id FROM logs AS a CROSS JOIN ONLY client_vitals AS b',
      'WITH "only" AS (SELECT 1 AS x) SELECT b.client_id FROM logs AS a, ONLY client_vitals AS b',
      'WITH "only" AS (SELECT 1 AS x) SELECT client_id FROM ONLY main.client_vitals',
      'WITH "only" AS (SELECT 1 AS x) SELECT client_id FROM ONLY (client_vitals)',
      'SELECT * FROM ONLY WHERE 1=1',
    ]) {
      const r = await executeCloudQuery(
        { sql: blockedOnlySql },
        { maskIps: true, tableName: 'requests', allowedTables: ['logs', 'requests'], runSql },
      )
      expect(r.status, blockedOnlySql).toBe(403)
    }

    const quotedOnlyRes = await executeCloudQuery(
      { sql: 'WITH "only" AS (SELECT 1 AS x) SELECT * FROM "only"' },
      { maskIps: true, tableName: 'requests', allowedTables: ['logs', 'requests'], runSql },
    )
    expect(quotedOnlyRes.status).toBe(200)

    for (const allowedOnlySql of [
      'SELECT status, ip, cookie_session FROM ONLY logs',
      'SELECT status, ip, cookie_session FROM ONLY main.logs',
    ]) {
      const onlyLogsRes = await executeCloudQuery(
        { sql: allowedOnlySql },
        { maskIps: true, tableName: 'requests', allowedTables: ['logs', 'requests'], runSql },
      )
      expect(onlyLogsRes.status, allowedOnlySql).toBe(200)
      const onlyLogsText = JSON.stringify(onlyLogsRes.body)
      expect(onlyLogsText, allowedOnlySql).not.toContain('203.0.113.')
      expect(onlyLogsText, allowedOnlySql).not.toContain('CANARY_SESS_')
    }
  })

  it('closes SEC-T5-ITER7-FE-01: blocks numeric-underscore/radix dollar-quote smuggling and non-keyword ./::/AS comma-joins while masking main.requests PII', async () => {
    const runSql = (sql: string) => engine.executeSql(sql)

    for (const blockedSql of [
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
    ]) {
      const res = await executeCloudQuery(
        { sql: blockedSql },
        {
          role: 'analyst-no-pii',
          maskIps: true,
          tableName: 'requests',
          allowedTables: ['logs', 'requests'],
          runSql,
        },
      )
      expect(res.status, blockedSql).toBe(403)
      expect(JSON.stringify(res.body), blockedSql).not.toContain('TOP_SECRET_CANARY')
    }

    for (const maskedDolqSql of [
      "SELECT 1_$$, (SELECT reverse(ip) || '|' || reverse(cookie_session) FROM main.requests LIMIT 1) AS raw_pii, 2_$$ FROM logs",
      "SELECT 0x1$$, (SELECT reverse(ip) || '|' || reverse(cookie_session) FROM main.requests LIMIT 1) AS raw_pii, 0x2$$ FROM logs",
    ]) {
      const res = await executeCloudQuery(
        { sql: maskedDolqSql },
        {
          role: 'analyst-no-pii',
          maskIps: true,
          tableName: 'requests',
          allowedTables: ['logs', 'requests'],
          runSql,
        },
      )
      expect(res.status, maskedDolqSql).toBe(200)
      const bodyStr = JSON.stringify(res.body)
      expect(bodyStr, maskedDolqSql).not.toContain('04.311.0.302')
      expect(bodyStr, maskedDolqSql).not.toContain('CANARY_SESS_')
      expect(bodyStr, maskedDolqSql).not.toContain('_SSES_YRANAC')
      if ('data' in res.body) {
        expect(res.body.data[0].raw_pii, maskedDolqSql).toBe('0.0.0.0|]detcader[')
      }
    }

    for (const benignSql of [
      'SELECT 1_000_000 AS n, 1.5_0e+1_0 AS f FROM logs LIMIT 1',
      'SELECT a.where FROM (SELECT 1 AS "where") AS a JOIN logs AS b ON a.where = 1 LIMIT 1',
      'SELECT 1::union(x INT) AS u FROM logs LIMIT 1',
      'SELECT c.ip, c.cookie_session FROM (SELECT 1 AS "where") AS a JOIN logs AS b ON a.where = 1, main.requests AS c LIMIT 1',
    ]) {
      const res = await executeCloudQuery(
        { sql: benignSql },
        {
          role: 'analyst-no-pii',
          maskIps: true,
          tableName: 'requests',
          allowedTables: ['logs', 'requests'],
          runSql,
        },
      )
      expect(res.status, benignSql).toBe(200)
      const bodyStr = JSON.stringify(res.body)
      expect(bodyStr, benignSql).not.toContain('203.0.113.')
      expect(bodyStr, benignSql).not.toContain('CANARY_SESS_')
    }
  })

  it('closes SEC-T5-ITER8-FE-01: blocks AS-shield cascades and {where: 1}/[x FOR x IN [1]] comma-joins on live executeCloudQuery while preserving benign AS/1./[]/{} queries with PII masked', async () => {
    const runSql = (sql: string) => engine.executeSql(sql)

    for (const blockedSql of [
      'SELECT secret_col AS as FROM ops_secret',
      "SELECT secret_col, {'as': 1}.as FROM ops_secret",
      "SELECT {'as': secret_col}.as FROM ops_secret",
      "SELECT secret_col, {'as': 1}.as AS as FROM ops_secret",
      'SELECT c.secret_col FROM (SELECT 200 AS "as") AS a JOIN logs AS b ON b.status = a.as JOIN ops_secret AS c ON true',
      "SELECT status AS as FROM main.'requests'",
      'SELECT c.secret_col FROM logs AS a JOIN logs AS b ON [x FOR x IN [1]] IS NOT NULL, ops_secret AS c',
      'SELECT c.secret_col FROM logs AS a JOIN logs AS b ON {where: 1} IS NOT NULL, ops_secret AS c',
      'SELECT * FROM [ops_secret]',
      'SELECT * FROM {ops_secret: 1}',
      'SELECT [1) FROM logs',
      "SELECT {'a': 1] FROM logs",
      'SELECT (SELECT c.secret_col FROM logs AS x JOIN logs AS y ON mode() WITHIN GROUP (ORDER BY outer_logs.status) = 200, ops_secret AS c LIMIT 1) AS leaked FROM logs AS outer_logs',
    ]) {
      const res = await executeCloudQuery(
        { sql: blockedSql },
        {
          role: 'analyst-no-pii',
          maskIps: true,
          tableName: 'requests',
          allowedTables: ['logs', 'requests'],
          runSql,
        },
      )
      expect(res.status, blockedSql).toBe(403)
      expect(JSON.stringify(res.body), blockedSql).not.toContain('TOP_SECRET_CANARY')
    }

    expect(rewriteCloudQuerySql('SELECT status AS as FROM logs', 'requests').rewrittenSql).toBe(
      'SELECT status AS as FROM requests LIMIT 1000',
    )
    expect(
      rewriteCloudQuerySql("SELECT {'as': status}.as FROM logs", 'requests').rewrittenSql,
    ).toBe("SELECT {'as': status}.as FROM requests LIMIT 1000")
    expect(
      rewriteCloudQuerySql(
        'SELECT c.status FROM logs AS a JOIN logs AS b ON {where: 1} IS NOT NULL, logs AS c',
        'requests',
      ).rewrittenSql,
    ).toContain(', requests AS c')
    expect(
      rewriteCloudQuerySql(
        'SELECT (SELECT c.ip FROM logs AS x JOIN logs AS y ON mode() WITHIN GROUP (ORDER BY outer_logs.status) = 200, logs AS c LIMIT 1) AS masked_ip FROM logs AS outer_logs',
        'requests',
      ).rewrittenSql,
    ).toContain(', requests AS c')

    for (const benignSql of [
      'SELECT status AS as, url, ip, cookie_session FROM logs',
      "SELECT {'as': status}.as AS where, url, ip, cookie_session FROM logs",
      'SELECT status, 1., ip, cookie_session FROM logs',
      'SELECT a.status, a.ip FROM logs AS a JOIN logs AS b ON [a.status, 1] = [b.status, 1]',
      "SELECT [x FOR x IN [1, 2] IF x > 0] AS xs, {'a': 1} AS st, ip, cookie_session FROM logs",
      'SELECT (SELECT c.ip FROM logs AS x JOIN logs AS y ON mode() WITHIN GROUP (ORDER BY outer_logs.status) = 200, logs AS c LIMIT 1) AS masked_ip FROM logs AS outer_logs',
      'SELECT a.within, count(*) AS cnt FROM (SELECT status AS within FROM logs) AS a GROUP BY within',
    ]) {
      const res = await executeCloudQuery(
        { sql: benignSql },
        {
          role: 'analyst-no-pii',
          maskIps: true,
          tableName: 'requests',
          allowedTables: ['logs', 'requests'],
          runSql,
        },
      )
      expect(res.status, benignSql).toBe(200)
      const bodyStr = JSON.stringify(res.body)
      expect(bodyStr, benignSql).not.toContain('203.0.113.')
      expect(bodyStr, benignSql).not.toContain('CANARY_SESS_')
    }
    const wgMaskedRes = await executeCloudQuery(
      {
        sql: 'SELECT (SELECT c.ip FROM logs AS x JOIN logs AS y ON mode() WITHIN GROUP (ORDER BY outer_logs.status) = 200, logs AS c LIMIT 1) AS masked_ip FROM logs AS outer_logs LIMIT 1',
      },
      {
        role: 'analyst-no-pii',
        maskIps: true,
        tableName: 'requests',
        allowedTables: ['logs', 'requests'],
        runSql,
      },
    )
    expect(wgMaskedRes.status).toBe(200)
    expect(JSON.stringify(wgMaskedRes.body)).toContain('0.0.0.0')
  })
})
