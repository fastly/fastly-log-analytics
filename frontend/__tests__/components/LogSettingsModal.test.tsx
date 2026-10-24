import { screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { afterEach, expect, test, vi } from 'vitest'
import { axe } from 'vitest-axe'
import { LogSettingsModal } from '@/components/LogSettingsModal/LogSettingsModal'
import * as apiLib from '@/lib/api'
import type { ServiceConfig } from '@/types/api'
import React from 'react'
import { getOk } from '@/__tests__/helpers/api-result'
import { stubMatchMedia } from '@/__tests__/helpers/browser-stubs'
import { createUseSSEMock } from '@/__tests__/helpers/sse-mock'
import { renderWithQuery } from '../helpers/query'

stubMatchMedia()
window.HTMLElement.prototype.scrollIntoView = vi.fn()

const { state: sseState } = createUseSSEMock()
vi.mock('@/hooks/useSSE', () => ({ useSSE: () => sseState }))

afterEach(() => {
  vi.useRealTimers()
  vi.restoreAllMocks()
})

test('LogSettingsModal navigates through wizard steps', async () => {
  const user = userEvent.setup()
  const onOpenChange = vi.fn()

  vi.spyOn(apiLib.client, 'GET').mockImplementation(async (url: string) => {
    if (url.includes('/api/log-fields/catalog')) {
      return getOk({ groups: [{ id: 'core', label: 'Core', fields: [] }], fields: [], presets: {} })
    }
    if (url.includes('/logging-settings')) {
      return getOk({ log_period: '60', sample_rate: 100, edge_only: true, custom_condition: 'req.url ~ "test"' })
    }
    if (url.includes('/log-fields')) {
      return getOk({ groups: ['core'] })
    }
    return getOk({})
  })

  renderWithQuery(
    <LogSettingsModal
      service={{ service_id: "test-svc", name: "Test Service" } as ServiceConfig}
      open={true}
      onOpenChange={onOpenChange}
    />
  )

  // Step 1: Wait for it to load and render "General Settings"
  await waitFor(() => expect(screen.getByText('Log Period')).toBeDefined())
  expect(screen.getByText('1. Standard Fields')).toBeDefined()

  // Verify custom condition is loaded.
  // LabelWithInfo renders both a <Label> AND an info Button with
  // aria-label="More info: Optional Log Condition", so a loose regex
  // matches both. Anchor with ^...$ to scope to the input's label only.
  const customConditionInput = screen.getByLabelText(/^Optional Log Condition$/i) as HTMLInputElement
  expect(customConditionInput.value).toBe('req.url ~ "test"')

  // Modify custom condition. clear()+type() simulates select-all+overtype
  // the way a user does it; fireEvent.change skipped the focus chain.
  await user.clear(customConditionInput)
  await user.type(customConditionInput, 'req.url ~ "updated"')
  expect(customConditionInput.value).toBe('req.url ~ "updated"')

  // Role-based query — wizard has multiple buttons; "Next Step" is the
  // only <button> whose accessible name matches.
  await user.click(screen.getByRole('button', { name: /Next Step/i }))

  // Step 2: Custom Fields
  await waitFor(() => expect(screen.getByText('2. Custom Fields')).toBeDefined())
  expect(screen.getByText('Define Custom Log Fields')).toBeDefined()

  await user.click(screen.getByRole('button', { name: /Next Step/i }))

  // Step 3: Review
  await waitFor(() => expect(screen.getByText('3. Review')).toBeDefined())
  expect(screen.getByText('Review Log Configuration Changes')).toBeDefined()

  // Verify custom condition appears in review step
  expect(screen.getByText('req.url ~ "updated"')).toBeDefined()
  expect(screen.getByText(/Custom Condition:/)).toBeDefined()

  // Verify Deploy button is present
  expect(screen.getByText('Deploy to Fastly')).toBeDefined()

  // Click Back
  await user.click(screen.getByRole('button', { name: /^Back$/i }))

  // Back to Step 2
  await waitFor(() => expect(screen.getByText('Define Custom Log Fields')).toBeDefined())
})

test('LogSettingsModal shows custom fields in review step', async () => {
  const user = userEvent.setup()
  const onOpenChange = vi.fn()

  vi.spyOn(apiLib.client, 'GET').mockImplementation(async (url: string) => {
    if (url.includes('/api/log-fields/catalog')) {
      return getOk({
          groups: [{ id: 'core', label: 'Core', fields: ['ip'] }],
          fields: [
            { id: 'ip', label: 'IP Address', group: 'core', is_custom: false },
            { id: 'x_custom', label: 'My Custom Field', group: 'custom', is_custom: true }
          ],
          presets: {}
      })
    }
    return getOk({ log_fields: { groups: ['core'] } })
  })

  renderWithQuery(
    <LogSettingsModal
      service={{ service_id: "test-svc", name: "Test Service" } as ServiceConfig}
      open={true}
      onOpenChange={onOpenChange}
    />
  )

  // Navigate to Step 2
  await waitFor(() => expect(screen.getAllByText('1. Standard Fields').length).toBeGreaterThan(0))
  await user.click(screen.getAllByRole('button', { name: /Next Step/i })[0])

  // Wait for Step 2 content
  await waitFor(() => expect(screen.getAllByText('Define Custom Log Fields').length).toBeGreaterThan(0))

  // Click Next Step to go to Step 3
  await user.click(screen.getAllByRole('button', { name: /Next Step/i })[0])

  // Wait for Review Step header
  await waitFor(() => expect(screen.getByText('Review Log Configuration Changes')).toBeDefined())

  // Verify custom field is listed in the summary
  // Use a more specific regex to avoid matching the "2. Custom Fields" step indicator
  await waitFor(() => expect(screen.getByText(/Custom Fields \(\d+\)/)).toBeDefined())
  expect(screen.getByText('My Custom Field')).toBeDefined()
  expect(screen.getByText('(x_custom)')).toBeDefined()
})

// Contract test: pins the POST body shape so a future "send bare config"
// regression (cf. commit 4805391 → "log_fields is required") trips here
// before it ever lands on a user. The backend handler at
// backend/routers/services/core.py extracts body.log_fields; anything
// other than `{ log_fields: { groups, field_overrides, ... } }` is a
// contract break.
test('LogSettingsModal Deploy POSTs body wrapped in { log_fields }', async () => {
  const user = userEvent.setup()
  const onOpenChange = vi.fn()

  vi.spyOn(apiLib.client, 'GET').mockImplementation(async (url: string) => {
    if (url.includes('/api/log-fields/catalog')) {
      return getOk({
          groups: [{ id: 'core', label: 'Core', fields: ['ip'] }],
          fields: [{ id: 'ip', label: 'IP Address', group: 'core', is_custom: false }],
          presets: {},
      })
    }
    return getOk({ log_fields: { groups: ['core'], field_overrides: {} } })
  })

  const postSpy = vi.fn<(url: unknown, opts: { body: { log_fields: object } }) => Promise<{ data: { ok: boolean } }>>(
    async () => ({ data: { ok: true } }),
  )
  vi.spyOn(apiLib.client, 'POST').mockImplementation(postSpy)

  renderWithQuery(
    <LogSettingsModal
      service={{ service_id: 'test-svc', name: 'Test Service' } as ServiceConfig}
      open={true}
      onOpenChange={onOpenChange}
    />
  )

  // Walk wizard: Step 1 → 2 → 3 → Deploy. Pattern mirrors the existing
  // "shows custom fields in review step" test which is known to traverse
  // all three steps cleanly.
  await waitFor(() => expect(screen.getAllByText('1. Standard Fields').length).toBeGreaterThan(0))
  await user.click(screen.getAllByRole('button', { name: /Next Step/i })[0])
  await waitFor(() => expect(screen.getAllByText('Define Custom Log Fields').length).toBeGreaterThan(0))
  await user.click(screen.getAllByRole('button', { name: /Next Step/i })[0])
  await waitFor(() => expect(screen.getAllByText('Review Log Configuration Changes').length).toBeGreaterThan(0))
  await user.click(screen.getAllByRole('button', { name: /Deploy to Fastly/i })[0])

  await waitFor(() => {
    const logFieldsCall = postSpy.mock.calls.find(([u]) =>
      typeof u === 'string' && u.includes('/log-fields')
    )
    expect(logFieldsCall).toBeDefined()
  })

  const logFieldsCall = postSpy.mock.calls.find(([u]) =>
    typeof u === 'string' && u.includes('/log-fields')
  )!
  const [url, opts] = logFieldsCall
  expect(url).toBe('/api/services/{service_id}/log-fields')
  expect(opts).toHaveProperty('body.log_fields')
  expect(opts.body.log_fields).toHaveProperty('groups')
  expect(opts.body.log_fields).toHaveProperty('field_overrides')
})

// TESTING_PLAN_3 item 19. Pin the modal's a11y contract — labels, dialog
// role, focus management. Disable color-contrast (jsdom can't compute it)
// and region (Dialog renders into a portal, not into <main>).
test('LogSettingsModal has no axe-detectable a11y violations', async () => {
  // Radix Checkbox settles its indicator state on an effect that fires during
  // axe's async DOM traversal, after the test's awaited renders — producing a
  // benign, intermittent "not wrapped in act(...)" warning for
  // CheckboxIndicator. The modal is correct (axe + the wizard-walk tests pass);
  // filter ONLY that specific React message so this test stays quiet while any
  // genuine console.error still surfaces.
  const realError = console.error
  const actFilter = vi.spyOn(console, 'error').mockImplementation((...args: unknown[]) => {
    if (typeof args[0] === 'string' && args[0].includes('not wrapped in act')) return
    realError(...args)
  })

  vi.spyOn(apiLib.client, 'GET').mockImplementation(async (url: string) => {
    if (url.includes('/api/log-fields/catalog')) {
      return getOk({
          groups: [{ id: 'core', label: 'Core', fields: ['ip'] }],
          fields: [{ id: 'ip', label: 'IP Address', group: 'core', is_custom: false }],
          presets: {},
      })
    }
    return getOk({ log_fields: { groups: ['core'], field_overrides: {} } })
  })

  const { container } = renderWithQuery(
    <LogSettingsModal
      service={{ service_id: 'test-svc', name: 'Test Service' } as ServiceConfig}
      open={true}
      onOpenChange={vi.fn()}
    />,
  )

  await waitFor(() => expect(screen.getByText('Log Period')).toBeDefined())

  const results = await axe(container, {
    rules: {
      'color-contrast': { enabled: false },
      region: { enabled: false },
    },
  })
  expect(results).toHaveNoViolations()
  actFilter.mockRestore()
})
