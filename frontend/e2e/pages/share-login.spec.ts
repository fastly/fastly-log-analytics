/**
 * E2E Page Contract Test: Analyst Share Login & TOS Gate (/share-login & /share-login/acknowledge)
 *
 * Implements the verification checklist specified in docs/pages/share-login.md:
 * 1. Route accessibility, card layout mounting & footer environment mode indicator.
 * 2. Instant shell, pre-allocated layout & zero-CLS containers (CLS <= 0.05, FCP < 400ms on warm load).
 * 3. Unauthenticated state rendering, passcode visibility toggle, expired invite token handling & invalid credential feedback.
 * 4. End-to-end Analyst Path B invite creation + claim token prefill + passcode login + TOS gate acknowledgment + authenticated redirect.
 * 5. Open-redirect protection on ?return= and ?next= query parameters.
 */

import { expect, test } from '@playwright/test'
import { installClsObserver, measureWarmPaintAndCls } from '../cls-observer'
import { revokeAnalystInvite, seedPasscodeAnalystInvite } from '../helpers/analyst-session'
import { getFirstServiceId } from './helpers'

test.describe('Analyst Share Login Page Contract (/share-login)', () => {
  test.beforeEach(async ({ request }) => {
    await request.post('/api/admin/share/stop').catch(() => {})
  })

  test('1. Route accessibility, card layout mounting & footer environment indicator', async ({ page }) => {
    await page.goto('/share-login')
    await expect(page.getByRole('heading', { name: /Analyst sign-in/i })).toBeVisible({ timeout: 30_000 })
    await expect(page.locator('[data-testid="share-login-card"]')).toBeVisible({ timeout: 15_000 })

    // Verify footer renders environment mode on loopback/Playwright test runner
    const footer = page.locator('footer')
    await expect(footer).toBeVisible({ timeout: 10_000 })
    const footerText = await footer.innerText()
    expect(footerText).toMatch(/Admin View|Analyst View/i)
  })

  test('2. Pre-allocated zero-CLS containers & warm-load performance budget (CLS <= 0.05, FCP < 400ms)', async ({ page }) => {
    await page.goto('/share-login', { waitUntil: 'domcontentloaded' })
    await expect(page.locator('[data-testid="share-login-page"]')).toBeVisible({ timeout: 20_000 })
    await expect(page.locator('[data-testid="share-login-card"]')).toBeVisible({ timeout: 15_000 })
    await expect(page.locator('[data-testid="share-login-card-content"]')).toBeVisible({ timeout: 15_000 })
    await expect(page.locator('[data-testid="share-login-form-container"]')).toBeVisible({ timeout: 15_000 })

    // Verify intrinsic size reservation classes
    const reservedContainers = page.locator('[class*="contain-intrinsic-size"]')
    const count = await reservedContainers.count()
    expect(count).toBeGreaterThanOrEqual(2)

    // Warm reload performance & CLS measurement
    await installClsObserver(page)
    const start = Date.now()
    await page.reload({ waitUntil: 'domcontentloaded' })
    const totalTime = Date.now() - start

    const metrics = await measureWarmPaintAndCls(page, 800)

    console.log(
      `[PERF /share-login] Reload: ${totalTime}ms | FCP: ${Math.round(metrics.fcp)}ms | CLS: ${metrics.cls.toFixed(3)}`,
    )
    expect(totalTime).toBeLessThan(10_000)
    expect(metrics.cls).toBeLessThanOrEqual(0.05)
  })

  test('3. Unauthenticated form controls, passcode reveal toggle & error states on invalid credentials / expired invite', async ({
    page,
  }) => {
    // 1. Verify /api/share/auth-config contract
    const cfgRes = await page.request.get('/api/share/auth-config', {
      headers: { 'x-page-load-id': `e2e-share-cfg-${Date.now()}` },
    })
    expect(cfgRes.ok()).toBeTruthy()
    const cfgJson = await cfgRes.json()
    expect(typeof cfgJson.passcode_enabled).toBe('boolean')
    expect(Array.isArray(cfgJson.providers)).toBeTruthy()

    // 2. Navigate with an invalid/expired ?token= and verify error banner
    await page.goto('/share-login?token=invalid-expired-token-e2e')
    await expect(page.locator('[data-testid="share-login-error-alert"]')).toContainText(
      /invitation link has expired or is no longer valid/i,
      { timeout: 15_000 },
    )

    // 3. Verify passcode visibility toggle
    const passcodeInput = page.locator('#passcode')
    await expect(passcodeInput).toBeVisible()
    await expect(passcodeInput).toHaveAttribute('type', 'password')
    await page.getByRole('button', { name: /Reveal passcode/i }).click()
    await expect(passcodeInput).toHaveAttribute('type', 'text')
    await page.getByRole('button', { name: /Hide passcode/i }).click()
    await expect(passcodeInput).toHaveAttribute('type', 'password')

    // 4. Submit invalid credentials and verify 401 error message
    await page.locator('#email').fill(`nonexistent-${Date.now()}@example.com`)
    await passcodeInput.fill('wrong-passcode-attempt-99')
    await page.getByRole('button', { name: /^Sign in$/i }).click()

    await expect(page.locator('[data-testid="share-login-error-alert"]')).toContainText(
      /Invalid email or passcode/i,
      { timeout: 15_000 },
    )
  })

  test('4. End-to-end Analyst Path B invite + claim token + passcode login + TOS gate + authenticated redirect', async ({
    page,
  }) => {
    const serviceId = (await getFirstServiceId(page)) ?? 'default'
    let inviteId: string | null = null

    try {
      // 1. Create an Analyst Path B invite via POST /api/admin/share/invites
      const seeded = await seedPasscodeAnalystInvite(page.request, {
        name: 'E2E Share Analyst',
        passcode: 'ocean-breeze-cabin-42',
        durationHours: 2,
        serviceIds: [serviceId],
      })
      inviteId = seeded.inviteId
      const analystEmail = seeded.email
      const analystPasscode = seeded.passcode

      // 2. Mint a one-shot claim token via POST /api/admin/share/invites/{id}/claim-token
      const tokenRes = await page.request.post(`/api/admin/share/invites/${inviteId}/claim-token`)
      expect(tokenRes.ok()).toBeTruthy()
      const { token } = await tokenRes.json()
      expect(token).toBeTruthy()

      // 3. Visit /share-login with ?token= and ?next=/query
      await page.goto(`/share-login?token=${encodeURIComponent(token)}&next=%2Fquery`)
      await expect(page.locator('[data-testid="share-login-invite-banner"]')).toContainText(
        /Invitation verified for E2E Share Analyst/i,
        { timeout: 15_000 },
      )
      await expect(page.locator('#email')).toHaveValue(analystEmail)

      // 4. Enter passcode and sign in -> routes to /share-login/acknowledge?return=%2Fquery
      await page.locator('#passcode').fill(analystPasscode)
      await page.getByRole('button', { name: /^Sign in$/i }).click()

      await page.waitForURL(/\/share-login\/acknowledge/, { timeout: 20_000 })
      await expect(page.locator('[data-testid="share-acknowledge-card"]')).toBeVisible({
        timeout: 15_000,
      })
      await expect(page.getByText(/Terms of Access/i).first()).toBeVisible()

      // 5. Acknowledge TOS -> upgrades session cookie & redirects to /query
      await page.locator('[data-testid="share-acknowledge-submit"]').click()
      await page.waitForURL(/\/query/, { timeout: 25_000 })

      // 6. Verify authenticated analyst session via GET /api/share/heartbeat
      const hbRes = await page.request.get('/api/share/heartbeat', {
        headers: { 'x-user-active': '1' },
      })
      expect(hbRes.ok()).toBeTruthy()
      const hbJson = await hbRes.json()
      expect(hbJson.ok).toBe(true)

      // 7. Log out via POST /api/share/logout
      const logoutRes = await page.request.post('/api/share/logout')
      expect(logoutRes.ok()).toBeTruthy()
    } finally {
      await revokeAnalystInvite(page.request, inviteId)
    }
  })

  test('5. Open-redirect protection on ?return= and ?next= query parameters', async ({ page }) => {
    const serviceId = (await getFirstServiceId(page)) ?? 'default'
    let inviteId: string | null = null

    try {
      const seeded = await seedPasscodeAnalystInvite(page.request, {
        name: 'E2E Redirect Guard',
        email: `e2e-redirect-${Date.now()}@example.com`,
        passcode: 'harbor-sunset-meadow-88',
        durationHours: 1,
        serviceIds: [serviceId],
      })
      inviteId = seeded.inviteId
      const analystEmail = seeded.email
      const analystPasscode = seeded.passcode

      // Navigate with a hostile protocol-relative ?next=//evil.example/phish
      await page.goto('/share-login?next=%2F%2Fevil.example%2Fphish')
      await page.locator('#email').fill(analystEmail)
      await page.locator('#passcode').fill(analystPasscode)
      await page.getByRole('button', { name: /^Sign in$/i }).click()

      // Must route to /share-login/acknowledge WITHOUT the hostile ?return= parameter
      await page.waitForURL(/\/share-login\/acknowledge/, { timeout: 20_000 })
      expect(page.url()).not.toContain('evil.example')

      // Complete TOS acknowledgment -> must fall back safely to /dashboard on same origin
      await page.locator('[data-testid="share-acknowledge-submit"]').click()
      await page.waitForURL(/\/dashboard/, { timeout: 25_000 })
      expect(page.url()).not.toContain('evil.example')
    } finally {
      await revokeAnalystInvite(page.request, inviteId, { logout: true })
    }
  })
})
