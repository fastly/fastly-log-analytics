import { expect, test } from '@playwright/test'

test('an unconfigured service never shows NGWAF cards, even with saved selections or Show all', async ({ page }) => {
  await page.addInitScript(() => {
    localStorage.setItem('dashboard_cards', JSON.stringify([
      '_bot_name', '_ngwaf_bot_name', 'waf_sig_ind',
    ]))
  })
  await page.goto('/dashboard')
  await page.getByRole('heading', { name: 'Security', exact: true }).scrollIntoViewIfNeeded()
  await expect(page.getByRole('heading', { name: /^Fastly Bots/ })).toBeVisible()
  await expect(page.getByRole('heading', { name: /NGWAF/ })).toHaveCount(0)

  const cards = page.getByRole('button', { name: 'Toggle visible dashboard cards' })
  await expect(cards).toHaveText(/^Cards\s*1$/)
  await cards.click()
  await expect(page.getByLabel('NGWAF Verified Bots', { exact: true })).toHaveCount(0)
  await expect(page.getByLabel('NGWAF Signals', { exact: true })).toHaveCount(0)
  await page.getByRole('button', { name: 'Show all', exact: true }).click()
  await page.keyboard.press('Escape')
  await expect(page.getByRole('heading', { name: /NGWAF/ })).toHaveCount(0)
  await page.getByRole('heading', { name: 'Security', exact: true }).scrollIntoViewIfNeeded()
  await expect(page.getByRole('heading', { name: /^Fastly Bots/ })).toBeVisible()
})
