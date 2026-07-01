/**
 * Pins the SSE consolidation: an admin page must hold exactly ONE
 * multiplexed event-stream connection (``/api/admin/events/stream``) and
 * MUST NOT open the three legacy per-purpose streams that used to each
 * consume an HTTP/1.1 connection (the cause of tunnel connection-pool
 * starvation / "pending" requests). A regression that re-splits the
 * streams would reopen the starvation window and fail here.
 */
import { expect, test } from '@playwright/test'

const LEGACY_STREAMS = [
  '/api/sync-status/stream',
  '/api/cron-runs/stream',
  '/api/admin/system-metrics/stream',
  // The /admin/share dashboard used to mount a SECOND always-on stream on
  // top of the merged one; it's now folded into the `share` channel.
  '/api/admin/share/stream',
]

function recordStreams(page: import('@playwright/test').Page): string[] {
  const streamRequests: string[] = []
  page.on('response', (resp) => {
    const url = resp.url()
    if (url.includes('/stream')) streamRequests.push(new URL(url).pathname + new URL(url).search)
  })
  return streamRequests
}

test('admin page opens one multiplexed SSE stream, not the three legacy ones', async ({ page }) => {
  const streamRequests = recordStreams(page)

  await page.goto('/admin')
  // AppLayout holds the merged stream open, so the network is never idle —
  // wait for main + a fixed settle (same pattern as admin-login.spec.ts).
  await page.locator('main').first().waitFor({ state: 'visible', timeout: 30_000 })
  await page.waitForTimeout(3_000)

  const merged = streamRequests.filter((p) => p.startsWith('/api/admin/events/stream'))
  const legacy = streamRequests.filter((p) => LEGACY_STREAMS.some((s) => p.startsWith(s)))

  expect(legacy, `legacy per-purpose streams must not open; saw: ${legacy.join(', ')}`).toHaveLength(0)
  expect(
    merged.length,
    `expected the merged /api/admin/events/stream to open; saw stream reqs: ${streamRequests.join(', ')}`,
  ).toBeGreaterThanOrEqual(1)
  // The merged URL must carry the channel set (system-metrics added on /admin).
  expect(merged.some((p) => p.includes('channels=') && p.includes('system-metrics'))).toBe(true)
})

test('admin/share opens one multiplexed stream (share channel folded in), not a second stream', async ({ page }) => {
  const streamRequests = recordStreams(page)

  await page.goto('/admin/share')
  await page.locator('main').first().waitFor({ state: 'visible', timeout: 30_000 })
  await page.waitForTimeout(3_000)

  const merged = streamRequests.filter((p) => p.startsWith('/api/admin/events/stream'))
  const legacy = streamRequests.filter((p) => LEGACY_STREAMS.some((s) => p.startsWith(s)))

  expect(
    legacy,
    `no dedicated share/legacy stream may open on /admin/share; saw: ${legacy.join(', ')}`,
  ).toHaveLength(0)
  expect(
    merged.length,
    `expected the merged /api/admin/events/stream on /admin/share; saw: ${streamRequests.join(', ')}`,
  ).toBeGreaterThanOrEqual(1)
  // The share channel must be folded into the merged connection here.
  expect(merged.some((p) => p.includes('channels=') && p.includes('share'))).toBe(true)
})
