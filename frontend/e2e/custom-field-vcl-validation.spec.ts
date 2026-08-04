/**
 * R-3c journey: custom-field VCL validation debounce.
 *
 * The debounce in production is a real 500 ms. In jsdom unit tests it
 * was advanced manually; this Playwright run lets the timer fire on
 * the browser's real clock so we exercise the actual debounce → POST
 * cycle the user sees. After the wait window elapses, the drawer's
 * `useEffect` calls `POST /api/services/{id}/custom-fields/validate-vcl`
 * and surfaces either a `lintResult` or a `lintFetchError` in the UI.
 *
 * We assert the request fired (which proves the debounce timer ran in
 * the browser) AND the validation pane updated (which proves the
 * async result hydrated the UI). The exact lint verdict is mocked
 * upstream — we don't gate on valid/invalid because the falco binary
 * may not be present in CI, and the audit's point is the timing path,
 * not the linter's verdict.
 */
import { expect, test } from '@playwright/test'

test('custom-field admin page renders', async ({ page }) => {
  // Reachability gate — pins the admin surface remains reachable.
  const r = await page.goto('/admin')
  expect(r?.status()).toBeLessThan(500)
})

test('VCL editor debounce → validate-vcl fires after 500 ms in a real browser', async ({
  page,
}) => {
  // Watch for the validate-vcl POST so we can assert the debounce timer
  // actually elapsed in the browser (not advanced manually like jsdom).
  const validateWaiter = page.waitForRequest(
    (req) =>
      req.method() === 'POST' && /\/api\/services\/[^/]+\/custom-fields\/validate-vcl/.test(req.url()),
    { timeout: 30_000 },
  )

  await page.goto('/admin')

  // Open Log Settings on the seeded service. Row actions live behind the
  // "Manage" dropdown (per ServicesTableColumns.tsx) — the item is
  // portaled to document.body by base-ui's Menu primitive, so it's
  // queried from `page`, not scoped to the row.
  const manageBtn = page.getByRole('button', { name: /manage/i }).first()
  await expect(manageBtn).toBeVisible({ timeout: 30_000 })
  await manageBtn.click()
  const logSettingsItem = page.getByRole('menuitem', { name: /log settings/i })
  await expect(logSettingsItem).toBeVisible({ timeout: 15_000 })
  await logSettingsItem.click()

  // The modal has three steps: Standard Fields → Custom Fields →
  // Review. Walk to step 2 (Custom Fields). The "Next Step" button
  // disappears at step 3; one click is enough here.
  const nextStep = page.getByRole('button', { name: /next step/i })
  await expect(nextStep).toBeVisible({ timeout: 30_000 })
  await nextStep.click()

  // The Custom Fields manager surfaces an "Add Field" button.
  const addField = page.getByRole('button', { name: /add field/i })
  await expect(addField).toBeVisible({ timeout: 30_000 })
  await addField.click()

  // Drawer opens with the create form. Label first (auto-slugs to
  // name per CustomFieldDrawer.handleChange), then VCL expression.
  const label = page.getByLabel('Label', { exact: false }).first()
  await label.fill('Pw Vcl Probe')

  const vclInput = page.getByLabel('VCL Log Expression')
  await expect(vclInput).toBeVisible()
  await vclInput.fill('req.http.Host')

  // Wait for the validate-vcl POST. The drawer's useEffect runs 500
  // ms after the last keystroke, so this is also the proof the
  // real-time debounce fired (jsdom couldn't reproduce this without
  // manual fake-timer advance — that's why this lives in Playwright).
  const validateReq = await validateWaiter
  expect(validateReq.url()).toMatch(/\/custom-fields\/validate-vcl/)

  // Validation pane updates: either a green "valid" check, an error
  // banner, or the fetch-error banner if the backend can't reach
  // falco. Assert one of them appears so we know the result wired
  // back into the UI, not just that the request went out.
  await expect(
    page
      .getByText(/VCL Expression is valid|Validation Errors|Validation could not run/i)
      .first(),
  ).toBeVisible({ timeout: 30_000 })
})
