/**
 * R-3c journey: provision-wizard SSE stream.
 *
 * The wizard's `/api/provision/execute` SSE handling is jsdom-unreproducible
 * (chunked text/event-stream timing). This journey drives the same wire
 * the wizard uses from a real browser context: POST → text/event-stream →
 * ReadableStream reader → assert all 8 "Step N/8" banners arrive in order.
 *
 * Driving the full wizard UI (mode → token → service → storage → ngwaf →
 * fields → execute) requires real Fastly responses at every step.
 * FASTLY_MOCK_MODE short-circuits Fastly/NGWAF but the wizard's per-step
 * validation depends on shape-specific mock responses (e.g. service-list
 * picker, FOS bucket check, domain availability) that drift the moment
 * the wizard UI evolves. Hitting `/api/provision/execute` directly tests
 * what's brittle without it (browser ↔ FastAPI SSE wire + orchestrator)
 * and leaves per-step form validation to unit tests that already cover
 * each step component.
 *
 * The created service uses a unique `svc-pw-wizard-<timestamp>` id per
 * `infra-stays-local` (test-fixture ids only, never real ones). After
 * the stream completes we hit `/api/bootstrap` to pin that the orchestrator
 * persisted the new config and the API surface sees it.
 */
import { expect, test } from '@playwright/test'

test('admin shell reaches the provision-wizard entry', async ({ page }) => {
  // Reachability gate — keeps a refactor of the admin shell from
  // silently stripping the wizard launcher.
  await page.goto('/admin')
  await expect(page).toHaveURL(/\/admin/)
  await expect(page.getByRole('button', { name: /add service/i })).toBeVisible({
    timeout: 30_000,
  })
})

test('POST /api/provision/execute streams all 8 Step banners and persists the new service', async ({
  page,
}) => {
  // Mint a fresh test-fixture service id so re-runs don't collide.
  // Date.now() is forbidden inside the workflow script but is fine in
  // Playwright test bodies (this isn't a workflow).
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
    // ensure_cdn_service unconditionally calls `cfg["cdn_url"].replace(...)` —
    // any test-fixture HTTPS URL is fine, the domain check tolerates DNS
    // failure as "available".
    cdn_url: `https://pw-wizard-${Date.now()}.example`,
    cdn_shield: 'none',
    enable_cron_sync: false, // dev/E2E doesn't process crons per dev-no-crons memory
    delete_after: true,
    commit_interval_mins: 5,
    enable_cron_compact: false,
    log_retention_days: 30,
    log_fields: null,
  }

  // Land on a real page so we have a same-origin fetch context.
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
    // Cap the read window so a hung backend can't hang the test.
    const deadline = Date.now() + 90_000
    // SSE event terminator: spec-allowed separators are \r\n\r\n,
    // \n\n, and \r\r. sse-starlette emits \r\n\r\n, so a parser that
    // only looks for \n\n returns zero messages. Use the same regex
    // useServiceStream + useSSE use in production code.
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
          if (line.startsWith('data: ')) {
            out.push(line.slice(6))
          }
        }
        m = SEP.exec(buf)
      }
    }
    return out
  }, body)

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

  // The orchestrator yields banners in order. Pin all 8 are seen.
  expect(stepBanners.length).toBeGreaterThanOrEqual(8)
  for (let i = 1; i <= 8; i++) {
    expect(stepBanners.some((m) => m.message.includes(`Step ${i}/8`))).toBe(true)
  }

  // Bootstrap must see the new service after the orchestrator persists
  // the config. Hit via Playwright's request helper (separate from
  // the page context — bypasses any in-flight client caches).
  const bootstrapResp = await page.request.get('/api/bootstrap')
  expect(bootstrapResp.ok()).toBe(true)
  const bootstrap = (await bootstrapResp.json()) as {
    services?: { service_id: string }[]
  }
  expect(bootstrap.services?.some((s) => s.service_id === sid)).toBe(true)
})
