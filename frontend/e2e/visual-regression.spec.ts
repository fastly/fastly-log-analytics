/**
 * Visual-regression baseline for the chart + map components that DOM
 * assertions can't catch (audit §2.2 "Visual regression for the chart
 * + map components" — landed as Playwright's built-in toHaveScreenshot
 * rather than a Chromatic / lost-pixel SaaS or self-host).
 *
 * Why these specific surfaces:
 *   - PlotlyChart: theme-sensitive `theme === 'dark'` branches in
 *     PlotlyChart.tsx + usage/page.tsx + charts/page.tsx. The most
 *     fragile when next-themes ships a breaking change or Tailwind
 *     rolls a token rename.
 *   - ChoroplethMap (GeoMap): maplibre canvas with custom layer
 *     opacity tied to muted-foreground — same theme fragility.
 *
 * ── Why this is gated behind RUN_VISUAL_REGRESSION=1 ─────────────────
 *
 * Playwright snapshots are per-platform-per-browser (the on-disk
 * filename embeds chromium/firefox/webkit + darwin/linux). A fresh
 * baseline generated on macOS will fail the first time CI runs it on
 * Linux because font hinting, sub-pixel anti-aliasing, and even the
 * pixel buffer alignment of Chromium-on-Mesa-vs-darwin all differ
 * enough to bust strict pixel-match.
 *
 * Two options to handle this cleanly:
 *   1. Generate baselines IN CI, commit, then enforce.
 *   2. Generate locally per-platform, commit all variants, enforce
 *      everywhere.
 *
 * Both require an explicit "I'm running visual now" gate during the
 * bootstrap window. RUN_VISUAL_REGRESSION=1 is that gate. Until the
 * baselines are committed for the target platforms, the test is a
 * skip everywhere — the spec body is the on-disk documentation for
 * future-you about how to invoke + what to assert against.
 *
 * Bootstrap flow:
 *
 *   1. Locally, generate first-pass baselines for chromium-darwin:
 *      RUN_VISUAL_REGRESSION=1 npx playwright test --project=chromium \
 *          e2e/visual-regression.spec.ts --update-snapshots
 *
 *   2. Commit the new files under e2e/visual-regression.spec.ts-snapshots/
 *
 *   3. CI run on linux fails the first time — re-run with
 *      RUN_VISUAL_REGRESSION=1 + --update-snapshots to add linux
 *      baselines, then commit those too.
 *
 *   4. Once baselines for the platforms you care about are in tree,
 *      flip the workflow env var on so the gate enforces.
 */
import { expect, test } from '@playwright/test'

const ENABLED = process.env.RUN_VISUAL_REGRESSION === '1'

test.skip(
  !ENABLED,
  'Visual regression is opt-in via RUN_VISUAL_REGRESSION=1 ' +
    '(see file header for the bootstrap flow + per-platform snapshot story).',
)

// Pixel-ratio tolerance: 2% absorbs anti-aliasing + chart-data jitter
// while still failing on a real theme break (background swap, axis
// label color flip, removed legend). Tune per-assertion if a card
// proves naturally noisier; this is the default that worked for the
// audit's reference comparators (Storybook + lost-pixel).
const DEFAULT_TOLERANCE = { maxDiffPixelRatio: 0.02 }

test.describe('dashboard chart + map render', () => {
  test('TrafficChart (Plotly) — light theme', async ({ page }) => {
    await page.emulateMedia({ colorScheme: 'light' })
    await page.goto('/dashboard')
    await expect(page.getByRole('heading', { name: 'Dashboard' })).toBeVisible({ timeout: 30_000 })
    await page.waitForLoadState('networkidle', { timeout: 30_000 })
    // Plotly inserts .js-plotly-plot once newPlot resolves.
    await page.locator('.js-plotly-plot, .plotly').first().waitFor({ state: 'visible', timeout: 30_000 })

    // Target the chart's wrapping card so anti-aliasing on adjacent
    // borders / shadows doesn't bleed into the diff.
    const trafficCard = page.locator('div').filter({ hasText: /^Traffic over Time/ }).first()
    await expect(trafficCard).toHaveScreenshot('traffic-chart-light.png', DEFAULT_TOLERANCE)
  })

  test('TrafficChart (Plotly) — dark theme', async ({ page }) => {
    await page.emulateMedia({ colorScheme: 'dark' })
    await page.goto('/dashboard')
    await expect(page.getByRole('heading', { name: 'Dashboard' })).toBeVisible({ timeout: 30_000 })
    await page.waitForLoadState('networkidle', { timeout: 30_000 })
    await page.locator('.js-plotly-plot, .plotly').first().waitFor({ state: 'visible', timeout: 30_000 })

    const trafficCard = page.locator('div').filter({ hasText: /^Traffic over Time/ }).first()
    await expect(trafficCard).toHaveScreenshot('traffic-chart-dark.png', DEFAULT_TOLERANCE)
  })

  test('GeoMap (maplibre choropleth) — light theme', async ({ page }) => {
    await page.emulateMedia({ colorScheme: 'light' })
    await page.goto('/dashboard')
    await expect(page.getByRole('heading', { name: 'Dashboard' })).toBeVisible({ timeout: 30_000 })
    await page.waitForLoadState('networkidle', { timeout: 30_000 })
    await page.locator('canvas').first().waitFor({ state: 'visible', timeout: 30_000 })

    const geoMapCard = page.locator('div').filter({ hasText: /^Requests by Country/ }).first()
    await expect(geoMapCard).toHaveScreenshot('geo-map-light.png', DEFAULT_TOLERANCE)
  })

  test('GeoMap (maplibre choropleth) — dark theme', async ({ page }) => {
    await page.emulateMedia({ colorScheme: 'dark' })
    await page.goto('/dashboard')
    await expect(page.getByRole('heading', { name: 'Dashboard' })).toBeVisible({ timeout: 30_000 })
    await page.waitForLoadState('networkidle', { timeout: 30_000 })
    await page.locator('canvas').first().waitFor({ state: 'visible', timeout: 30_000 })

    const geoMapCard = page.locator('div').filter({ hasText: /^Requests by Country/ }).first()
    await expect(geoMapCard).toHaveScreenshot('geo-map-dark.png', DEFAULT_TOLERANCE)
  })
})
