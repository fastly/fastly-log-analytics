import { expect, it } from 'vitest'

it('models disabled ClickHouse without inventing publication health or replay success', async () => {
  const status = await fetch('http://127.0.0.1:8000/api/admin/clickhouse/status?service_id=svc-test')
  expect(await status.json()).toMatchObject({
    service_id: 'svc-test',
    enabled: false,
    health: 'disabled',
    oldest_pending_age_seconds: null,
  })
  const replay = await fetch('http://127.0.0.1:8000/api/admin/clickhouse/replay', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ service_id: 'svc-test', dataset_id: 'dataset' }),
  })
  expect(replay.status).toBe(409)
  expect(await replay.json()).toMatchObject({ detail: { error: 'clickhouse_disabled' } })
})
