/**
 * R-3c journey: dashboard card visibility persistence.
 *
 * Pins that the dashboard's card visibility state survives a page
 * reload. The audit referenced a `user_dashboard_layout` SQLite table
 * but the current implementation persists to localStorage under
 * `dashboard_cards` (see hooks/useCardVisibility.ts). Same contract,
 * different storage layer — the test still validates the toggle →
 * reload → still-hidden chain end-to-end, which jsdom couldn't
 * reproduce reliably across the popover (base-ui) + checkbox stack.
 *
 * Workaround note: base-ui's Popover uses pointerdown-based detection
 * that didn't always cooperate with userEvent in jsdom unit tests
 * (see frontend/__tests__/components/FilterValueCell.test.tsx for
 * the documented why). Playwright drives the real browser, so a
 * plain `.click()` works without dispatch-event hacks.
 */
import { expect, test } from '@playwright/test'

test('dashboard card grid mounts and the Cards toggle is reachable', async ({ page }) => {
  await page.goto('/dashboard')
  await expect(page.getByRole('heading', { name: 'Dashboard' })).toBeVisible({ timeout: 30_000 })
  // Cards button is in DashboardHeader.
  await expect(page.getByRole('button', { name: /cards/i }).first()).toBeVisible({
    timeout: 15_000,
  })
})

test('toggling a card off persists across page reload', async ({ page }) => {
  await page.goto('/dashboard')
  await expect(page.getByRole('heading', { name: 'Dashboard' })).toBeVisible({ timeout: 30_000 })

  // Open the Cards popover.
  const cardsBtn = page.getByRole('button', { name: /cards/i }).first()
  await cardsBtn.click()

  // The popover is portaled — wait for its "Visible cards" header
  // to appear before reading its contents. Otherwise we race the
  // floating-tree mount and the input#card-<id> locator below sees
  // an empty DOM.
  await expect(page.getByText(/visible cards/i).first()).toBeVisible({ timeout: 15_000 })

  // The popover lists every dashboard card as a checkbox with id
  // ``card-<id>`` and a label that mirrors the card's display name.
  // Pick the first CHECKED checkbox so we know we're toggling a card
  // that's currently visible — toggling an already-hidden one would
  // make the reload assertion ambiguous.
  const firstChecked = page.locator('input[type="checkbox"][id^="card-"]:checked').first()
  await expect(firstChecked).toBeVisible({ timeout: 15_000 })
  const cardId = await firstChecked.getAttribute('id')
  expect(cardId).toBeTruthy()
  const cardKey = cardId!.replace('card-', '')

  // Click the associated <label> rather than the input itself: the
  // checkbox component renders a custom indicator and the label is
  // the documented click target (htmlFor=card-<id>).
  const label = page.locator(`label[for="${cardId}"]`)
  await label.click()

  // After the toggle, the checkbox state should flip to unchecked.
  await expect(page.locator(`input#${cardId}`)).not.toBeChecked()

  // Persistence happens through localStorage (`dashboard_cards` key,
  // see frontend/hooks/useCardVisibility.ts). The audit referenced a
  // `user_dashboard_layout` SQLite table that doesn't exist on this
  // branch — same persistence contract, different storage layer.
  const storedBefore = await page.evaluate(() => localStorage.getItem('dashboard_cards'))
  expect(storedBefore, 'dashboard_cards must be written after toggle').toBeTruthy()
  const visibleBefore = JSON.parse(storedBefore!) as string[]
  expect(visibleBefore.includes(cardKey)).toBe(false)

  // Reload the page. localStorage survives `page.reload()` in the
  // same browser context — pin that the hook reads the prior value
  // back on mount instead of resetting to defaults.
  await page.reload()
  await expect(page.getByRole('heading', { name: 'Dashboard' })).toBeVisible({ timeout: 30_000 })

  const storedAfter = await page.evaluate(() => localStorage.getItem('dashboard_cards'))
  expect(storedAfter, 'dashboard_cards must survive reload').toBeTruthy()
  const visibleAfter = JSON.parse(storedAfter!) as string[]
  expect(visibleAfter.includes(cardKey)).toBe(false)
})
