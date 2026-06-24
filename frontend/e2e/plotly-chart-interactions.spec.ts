/**
 * R-3c journey: plotly chart interactions.
 *
 * Pins the loading-state flash regression originally fixed in commit
 * a506e0a (no flash of empty state after data lands). Plotly mounts
 * lazily under React.memo + dynamic import; this test mounts the
 * dashboard, waits for the cards to settle, then asserts no
 * intermediate "no data" placeholder is visible alongside the chart.
 */
import { expect, test } from '@playwright/test'

test('dashboard renders charts without flashing empty state', async ({ page }) => {
  await page.goto('/dashboard')
  await expect(page.getByRole('heading', { name: 'Dashboard' })).toBeVisible({ timeout: 30_000 })

  // "Traffic over Time" is the first card. Wait for its container.
  const trafficCard = page.getByText('Traffic over Time').first()
  await expect(trafficCard).toBeVisible({ timeout: 30_000 })

  // Wait for Plotly to actually render — the chart inserts an `.js-plotly-plot`
  // wrapper div with an inner SVG once Plotly.newPlot resolves. Polling
  // for that selector is the deterministic "chart is on screen" signal
  // (replaces a fixed 2 s settle).
  await page.locator('.js-plotly-plot, .plotly').first().waitFor({ state: 'visible', timeout: 30_000 })

  // Empty-state copy should NOT linger after the chart wrapper is up.
  // The flash regression would surface as the placeholder being visible
  // after Plotly attached.
  const emptyStateVisible = await page.getByText(/no data/i).isVisible().catch(() => false)
  expect(emptyStateVisible).toBe(false)
})
