/**
 * R-3c journey: provision-wizard SSE stream and provision-teardown confirm dialog + bootstrap removal.
 */
import { expect, test, type Page } from '@playwright/test'

async function provisionFreshService(page: Page, sid: string) {
  await page.goto('/admin')
  const messages = await page.evaluate(async (deployBody: Record<string, unknown>) => {
    const resp = await fetch('/api/provision/execute', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(deployBody),
    })
    if (!resp.body) {
      throw new Error(`provision/execute returned no body (status=${resp.status})`)
    }
    const reader = resp.body.getReader()
    const decoder = new TextDecoder()
    const out: string[] = []
    let buf = ''
    const deadline = Date.now() + 90_000
    const SEP = /\r\n\r\n|\n\n|\r\r/
    while (Date.now() < deadline) {
      const { value, done } = await reader.read()
      if (value) {
        buf += decoder.decode(value, { stream: true })
      }
      let m = SEP.exec(buf)
      while (m) {
        const chunk = buf.slice(0, m.index)
        buf = buf.slice(m.index + m[0].length)
        for (const line of chunk.split(/\r\n|\n|\r/)) {
          if (line.startsWith('data: ')) out.push(line.slice(6))
        }
        m = SEP.exec(buf)
      }
      if (done) {
        break
      }
    }
    if (buf.trim()) {
      for (const line of buf.split(/\r\n|\n|\r/)) {
        if (line.startsWith('data: ')) {
          out.push(line.slice(6))
        }
      }
    }
    return out
  }, {
    token: 'mock-fastly-token',
    service_id: sid,
    service_name: `PW Teardown Target ${sid}`,
    endpoint_name: 'PW Teardown Logger',
    fos_region: 'us-east-1',
    fos_bucket_name: `pw-teardown-bucket-${Date.now()}`,
    fos_prefix: '',
    sample_rate: '100',
    edge_only: true,
    custom_condition: '',
    log_period: '60',
    cdn_service_name: 'PW Teardown CDN',
    cdn_url: `https://pw-teardown-${Date.now()}.example`,
    cdn_shield: 'none',
    enable_cron_sync: false,
    delete_after: true,
    commit_interval_mins: 5,
    enable_cron_compact: false,
    log_retention_days: 30,
    log_fields: null,
  })
  const done = messages.some((raw: string) => {
    try {
      const m = JSON.parse(raw) as { type?: string }
      return m.type === 'done'
    } catch {
      return false
    }
  })
  console.log(`RECEIVED ${messages.length} TEARDOWN PROVISION MESSAGES:`, messages)
  expect(done, `orchestrator did not emit 'done' for ${sid}; got ${messages.length} events`).toBe(true)
}

test('admin shell reaches the provision-wizard entry', async ({ page }) => {
  await page.goto('/admin')
  await expect(page).toHaveURL(/\/admin/)
  await expect(page.getByRole('button', { name: /add service/i })).toBeVisible({
    timeout: 30_000,
  })
})

test('POST /api/provision/execute streams all 8 Step banners and persists the new service', async ({
  page,
}) => {
  test.setTimeout(180_000)
  const sid = `svc-pw-wizard-${Date.now()}`
  const body = {
    token: 'mock-fastly-token',
    service_id: sid,
    service_name: 'Playwright Wizard E2E',
    endpoint_name: 'PW Wizard Logger',
    fos_region: 'us-east-1',
    fos_bucket_name: `pw-wizard-bucket-${Date.now()}`,
    fos_prefix: '',
    sample_rate: '100',
    edge_only: true,
    custom_condition: '',
    log_period: '60',
    cdn_service_name: 'PW Wizard CDN',
    cdn_url: `https://pw-wizard-${Date.now()}.example`,
    cdn_shield: 'none',
    enable_cron_sync: false,
    delete_after: true,
    commit_interval_mins: 5,
    enable_cron_compact: false,
    log_retention_days: 30,
    log_fields: null,
  }

  await page.goto('/admin')

  const messages = await page.evaluate(async (deployBody: Record<string, unknown>) => {
    const resp = await fetch('/api/provision/execute', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(deployBody),
    })
    if (!resp.body) {
      throw new Error(`provision/execute returned no body (status=${resp.status})`)
    }
    const reader = resp.body.getReader()
    const decoder = new TextDecoder()
    const out: string[] = []
    let buf = ''
    const deadline = Date.now() + 90_000
    const SEP = /\r\n\r\n|\n\n|\r\r/
    while (Date.now() < deadline) {
      const { value, done } = await reader.read()
      if (value) {
        buf += decoder.decode(value, { stream: true })
      }
      let m = SEP.exec(buf)
      while (m) {
        const chunk = buf.slice(0, m.index)
        buf = buf.slice(m.index + m[0].length)
        for (const line of chunk.split(/\r\n|\n|\r/)) {
          if (line.startsWith('data: ')) {
            out.push(line.slice(6))
          }
        }
        m = SEP.exec(buf)
      }
      if (done) {
        break
      }
    }
    if (buf.trim()) {
      for (const line of buf.split(/\r\n|\n|\r/)) {
        if (line.startsWith('data: ')) {
          out.push(line.slice(6))
        }
      }
    }
    return out
  }, body)

  console.log("ALL RAW SSE MESSAGES RECEIVED:", messages)
  const stepBanners = messages
    .map((raw) => {
      try {
        return JSON.parse(raw) as { type?: string; message?: string }
      } catch {
        return null
      }
    })
    .filter((m): m is { type: string; message: string } => !!m && typeof m.message === 'string')
    .filter((m) => /Step \d+\/8/.test(m.message))

  console.log("STEP BANNERS RECEIVED:", stepBanners.map(m => m.message))
  expect(stepBanners.length).toBeGreaterThanOrEqual(8)
  for (let i = 1; i <= 8; i++) {
    expect(stepBanners.some((m) => m.message.includes(`Step ${i}/8`))).toBe(true)
  }

  const bootstrapResp = await page.request.get('/api/bootstrap')
  expect(bootstrapResp.ok()).toBe(true)
  const bootstrap = (await bootstrapResp.json()) as {
    services?: { service_id: string }[]
  }
  expect(bootstrap.services?.some((s) => s.service_id === sid)).toBe(true)
})

test('teardown CTA opens a confirm dialog and removes the service from /api/bootstrap', async ({
  page,
}) => {
  test.setTimeout(180_000)
  const sid = `svc-pw-teardown-${Date.now()}`

  await provisionFreshService(page, sid)

  const beforeResp = await page.request.get('/api/bootstrap')
  const before = (await beforeResp.json()) as { services?: { service_id: string }[] }
  expect(before.services?.some((s) => s.service_id === sid)).toBe(true)

  await page.goto('/admin')

  await page.getByRole('button', { name: /refresh status/i }).click()

  const row = page
    .getByRole('row')
    .filter({ hasText: `PW Teardown Target ${sid}` })
    .first()
  await expect(row).toBeVisible({ timeout: 30_000 })

  await row.getByRole('button', { name: /manage/i }).click()
  await page.getByRole('menuitem', { name: /teardown service/i }).click()

  await expect(page.getByRole('dialog')).toBeVisible({ timeout: 15_000 })
  await expect(page.getByText(new RegExp(`Teardown:.*${sid}`, 'i'))).toBeVisible()

  const tokenField = page.getByLabel(/Paste a Fastly token/i)
  await tokenField.focus()
  await tokenField.pressSequentially('mock-teardown-token', { delay: 5 })

  const executeBtn = page.getByRole('button', { name: /execute teardown/i })
  await expect(executeBtn).toBeEnabled({ timeout: 10_000 })
  await page.waitForTimeout(250)
  await executeBtn.click({ timeout: 30_000, force: true })

  await expect(
    page.getByRole('button', { name: /^close$/i }).first(),
  ).toBeVisible({ timeout: 60_000 })

  await expect
    .poll(
      async () => {
        const r = await page.request.get('/api/bootstrap')
        const j = (await r.json()) as { services?: { service_id: string }[] }
        return j.services?.some((s) => s.service_id === sid) ?? false
      },
      { timeout: 8_000, intervals: [250, 500, 1000, 1500] },
    )
    .toBe(false)
})
