/**
 * R-3c journey: provision-teardown confirm dialog + bootstrap removal.
 *
 * Pins two halves of the teardown flow:
 *   1. The destructive UI gates on a confirm dialog before tearing down
 *      anything (TeardownDialog with "Teardown: <name>" header).
 *   2. After executing the teardown SSE stream to completion, the
 *      service no longer appears in `/api/bootstrap`.
 *
 * The journey provisions its own fresh service first via a direct
 * POST to /api/provision/execute so the test is self-contained — we
 * never tear down the global-setup seed (other tests depend on it).
 * Test-fixture service ids only, per `infra-stays-local`.
 */
import { expect, test } from '@playwright/test'

async function provisionFreshService(page: any, sid: string) {
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
    // sse-starlette emits \r\n\r\n separators; parse all three spec
    // separators to match the production useServiceStream parser.
    const SEP = /\r\n\r\n|\n\n|\r\r/
    while (Date.now() < deadline) {
      const { value, done } = await reader.read()
      if (done) break
      buf += decoder.decode(value, { stream: true })
      let m = SEP.exec(buf)
      while (m) {
        const chunk = buf.slice(0, m.index)
        buf = buf.slice(m.index + m[0].length)
        for (const line of chunk.split(/\r\n|\n|\r/)) {
          if (line.startsWith('data: ')) out.push(line.slice(6))
        }
        m = SEP.exec(buf)
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
  // Sanity: the orchestrator emitted a terminal 'done' event.
  const done = messages.some((raw: string) => {
    try {
      const m = JSON.parse(raw) as { type?: string }
      return m.type === 'done'
    } catch {
      return false
    }
  })
  expect(done, `orchestrator did not emit 'done' for ${sid}; got ${messages.length} events`).toBe(true)
}

test('teardown CTA opens a confirm dialog and removes the service from /api/bootstrap', async ({
  page,
}) => {
  const sid = `svc-pw-teardown-${Date.now()}`

  await provisionFreshService(page, sid)

  // /api/bootstrap should now see it.
  const beforeResp = await page.request.get('/api/bootstrap')
  const before = (await beforeResp.json()) as { services?: { service_id: string }[] }
  expect(before.services?.some((s) => s.service_id === sid)).toBe(true)

  // Reload /admin so the freshly provisioned service appears in the
  // ServicesTable (the table reads from /api/services via useQuery).
  await page.goto('/admin')

  // The teardown button lives on the per-service row; use the row's
  // accessible name as a scope so we click the right one if there are
  // multiple services in the table.
  const row = page
    .getByRole('row')
    .filter({ hasText: `PW Teardown Target ${sid}` })
    .first()
  await expect(row).toBeVisible({ timeout: 30_000 })

  // Row actions collapsed into a single "Manage" dropdown (Delete Data +
  // Teardown Service live inside it). The menu content is portaled to
  // document.body by base-ui's Menu primitive, so only the trigger is
  // scoped to the row — the opened item is queried from `page`.
  await row.getByRole('button', { name: /manage/i }).click()
  await page.getByRole('menuitem', { name: /teardown service/i }).click()

  // TeardownDialog opens with "Teardown: <name>" title.
  await expect(page.getByRole('dialog')).toBeVisible({ timeout: 15_000 })
  await expect(page.getByText(new RegExp(`Teardown:.*${sid}`, 'i'))).toBeVisible()

  // The destructive button is disabled until the admin pastes a
  // Fastly token. Mock-mode short-circuits the token's real use, but
  // the UI still gates the click; provide any non-empty value.
  // The label reads "Paste a Fastly token with the global scope" —
  // match by `Paste a Fastly token` to stay tolerant of copy tweaks.
  //
  // pressSequentially instead of fill: fill() does focus → selectAll →
  // dispatch('') → dispatch(value), and the intermediate empty
  // dispatch sets apiToken='' for one React render → canExecute=false
  // → button disabled. On webkit's microtask scheduling the
  // Execute-Teardown button can latch into the disabled state long
  // enough for the subsequent click() to time out chasing actionability.
  // pressSequentially fires per-character keydown/input events with
  // no empty-state intermediate, so canExecute never flips back.
  const tokenField = page.getByLabel(/Paste a Fastly token/i)
  await tokenField.focus()
  await tokenField.pressSequentially('mock-teardown-token', { delay: 5 })

  const executeBtn = page.getByRole('button', { name: /execute teardown/i })
  await expect(executeBtn).toBeEnabled({ timeout: 10_000 })
  // One render-frame settle so React's commit phase finishes before
  // the click action checks actionability. 30s click timeout absorbs
  // any residual webkit variance.
  await page.waitForTimeout(250)
  await executeBtn.click({ timeout: 30_000 })

  // The SSE stream emits 'done' when the orchestrator finishes; the
  // dialog renders a "Close" button only at that point. After close
  // the dialog reloads the page (window.location.reload), so we
  // wait for the navigation to settle before asserting bootstrap.
  // Use .first() to disambiguate from the dialog's "X" close icon
  // (also exposed as a "Close"-named button).
  await expect(
    page.getByRole('button', { name: /^close$/i }).first(),
  ).toBeVisible({ timeout: 60_000 })

  // Poll rather than a single-shot read: the loopback-admin /api/bootstrap
  // response is memoized for a few seconds. The backend now invalidates that
  // cache the moment teardown removes the config (backend/routers/provision.py
  // → CacheRegistry.clear), but polling keeps this assertion robust against any
  // residual TTL/timing instead of racing the cache window.
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
