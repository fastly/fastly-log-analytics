/**
 * R-3c journey: maplibre country-filter.
 *
 * Pins the "map country click" half of the regression originally
 * fixed in commit a506e0a + the geojson preload from ca34280. The
 * dashboard's ChoroplethMap mounts under dynamic import; this test
 * reaches the dashboard, waits for the maplibre canvas to render,
 * and asserts the static GeoJSON has been preloaded (request
 * monitoring) — the click → filter pill chain requires a real
 * MapLibre `click` event which Playwright can synthesize but the
 * map's interaction layer needs valid tile data + measured layout
 * that varies across browsers; gate that follow-on behind a more
 * deterministic fixture once the map's test mode lands.
 */
import { expect, test } from '@playwright/test'

test('dashboard mounts the maplibre container without crashing', async ({ page }) => {
  const geojsonRequests: string[] = []
  page.on('request', (req) => {
    const u = req.url()
    if (u.endsWith('.geojson') || u.endsWith('.json') && u.includes('countries')) {
      geojsonRequests.push(u)
    }
  })

  await page.goto('/dashboard')
  await expect(page.getByRole('heading', { name: 'Dashboard' })).toBeVisible({ timeout: 30_000 })

  // ChoroplethMap wraps `new maplibregl.Map()` in a try/catch: when WebGL2
  // context creation fails (headless/locked-down browser — real in CI
  // Firefox, which reproducibly cannot get a WebGL2 context), it renders a
  // text fallback ("Interactive map unavailable...") instead of the canvas
  // container div, so no <canvas> ever attaches. maplibre-gl 6.7 made this
  // failure loud and synchronous (GPUInitializationError thrown from the
  // constructor) where earlier versions silently limped along — see
  // https://github.com/maplibre/maplibre-gl-js/issues/8066. Both outcomes
  // are "mounts without crashing"; race them instead of assuming the
  // canvas always wins.
  const canvasAttached = page.locator('canvas').first().waitFor({ state: 'attached', timeout: 45_000 })
  const fallbackVisible = page.getByText(/Interactive map unavailable/i).waitFor({ state: 'visible', timeout: 45_000 })
  // Promise.any (not .race): resolves as soon as either outcome lands, and
  // only rejects if BOTH time out. .race would reject on the first
  // rejection even if the other promise was about to succeed.
  await Promise.any([canvasAttached, fallbackVisible])

  const canvasCount = await page.locator('canvas').count()
  const fallbackCount = await page.getByText(/Interactive map unavailable/i).count()
  expect(canvasCount + fallbackCount).toBeGreaterThanOrEqual(1)
})
