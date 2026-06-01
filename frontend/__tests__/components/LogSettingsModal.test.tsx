import { render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { expect, test, vi } from 'vitest'
import { axe } from 'vitest-axe'
import { LogSettingsModal } from '@/components/LogSettingsModal/LogSettingsModal'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import * as apiLib from '@/lib/api'
import React from 'react'

// Mock matchMedia
Object.defineProperty(window, 'matchMedia', {
  writable: true,
  value: vi.fn().mockImplementation(query => ({
    matches: false,
    media: query,
    onchange: null,
    addListener: vi.fn(),
    removeListener: vi.fn(),
    addEventListener: vi.fn(),
    removeEventListener: vi.fn(),
    dispatchEvent: vi.fn(),
  })),
})

// Mock scrollIntoView
window.HTMLElement.prototype.scrollIntoView = vi.fn()

// Mock useSSE
vi.mock('@/hooks/useSSE', () => ({
  useSSE: () => ({
    lines: [],
    status: 'idle',
    isDone: false,
    error: null,
    start: vi.fn(),
    stop: vi.fn(),
    reset: vi.fn()
  })
}))

test('LogSettingsModal navigates through wizard steps', async () => {
  const user = userEvent.setup()
  const queryClient = new QueryClient()
  const onOpenChange = vi.fn()
  
  // Mock API requests
  vi.spyOn(apiLib.client, 'GET').mockImplementation(async (url: any) => {
    if (url.includes('/api/log-fields/catalog')) {
      return { data: { groups: [{ id: 'core', label: 'Core', fields: [] }], fields: [], presets: {} } } as any
    }
    if (url.includes('/logging-settings')) {
      return { data: { log_period: '60', sample_rate: 100, edge_only: true, custom_condition: 'req.url ~ "test"' } } as any
    }
    if (url.includes('/log-fields')) {
      return { data: { groups: ['core'] } } as any
    }
    return { data: {} } as any
  })

  render(
    <QueryClientProvider client={queryClient}>
      <LogSettingsModal
        service={{ service_id: "test-svc", name: "Test Service" } as any}
        open={true}
        onOpenChange={onOpenChange}
      />
    </QueryClientProvider>
  )

  // Step 1: Wait for it to load and render "General Settings"
  await waitFor(() => expect(screen.getByText('Log Period')).toBeDefined())
  expect(screen.getByText('1. Standard Fields')).toBeDefined()
  
  // Verify custom condition is loaded
  const customConditionInput = screen.getByLabelText(/Optional Log Condition/i) as HTMLInputElement
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
  const queryClient = new QueryClient()
  const onOpenChange = vi.fn()
  
  // Mock API requests with a custom field
  vi.spyOn(apiLib.client, 'GET').mockImplementation(async (url: any) => {
    if (url.includes('/api/log-fields/catalog')) {
      return { 
        data: { 
          groups: [{ id: 'core', label: 'Core', fields: ['ip'] }], 
          fields: [
            { id: 'ip', label: 'IP Address', group: 'core', is_custom: false },
            { id: 'x_custom', label: 'My Custom Field', group: 'custom', is_custom: true }
          ], 
          presets: {} 
        } 
      } as any
    }
    return { data: { log_fields: { groups: ['core'] } } } as any
  })

  render(
    <QueryClientProvider client={queryClient}>
      <LogSettingsModal
        service={{ service_id: "test-svc", name: "Test Service" } as any}
        open={true}
        onOpenChange={onOpenChange}
      />
    </QueryClientProvider>
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
  const queryClient = new QueryClient()
  const onOpenChange = vi.fn()

  vi.spyOn(apiLib.client, 'GET').mockImplementation(async (url: any) => {
    if (url.includes('/api/log-fields/catalog')) {
      return {
        data: {
          groups: [{ id: 'core', label: 'Core', fields: ['ip'] }],
          fields: [{ id: 'ip', label: 'IP Address', group: 'core', is_custom: false }],
          presets: {},
        },
      } as any
    }
    return { data: { log_fields: { groups: ['core'], field_overrides: {} } } } as any
  })

  const postSpy: any = vi.fn(async () => ({ data: { ok: true } }))
  vi.spyOn(apiLib.client, 'POST').mockImplementation(postSpy)

  render(
    <QueryClientProvider client={queryClient}>
      <LogSettingsModal
        service={{ service_id: 'test-svc', name: 'Test Service' } as any}
        open={true}
        onOpenChange={onOpenChange}
      />
    </QueryClientProvider>
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
    const logFieldsCall = postSpy.mock.calls.find(([u]: any[]) =>
      typeof u === 'string' && u.includes('/log-fields')
    )
    expect(logFieldsCall).toBeDefined()
  })

  const logFieldsCall = postSpy.mock.calls.find(([u]: any[]) =>
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
  const queryClient = new QueryClient()

  vi.spyOn(apiLib.client, 'GET').mockImplementation(async (url: any) => {
    if (url.includes('/api/log-fields/catalog')) {
      return {
        data: {
          groups: [{ id: 'core', label: 'Core', fields: ['ip'] }],
          fields: [{ id: 'ip', label: 'IP Address', group: 'core', is_custom: false }],
          presets: {},
        },
      } as any
    }
    return { data: { log_fields: { groups: ['core'], field_overrides: {} } } } as any
  })

  const { container } = render(
    <QueryClientProvider client={queryClient}>
      <LogSettingsModal
        service={{ service_id: 'test-svc', name: 'Test Service' } as any}
        open={true}
        onOpenChange={vi.fn()}
      />
    </QueryClientProvider>,
  )

  await waitFor(() => expect(screen.getByText('Log Period')).toBeDefined())

  const results = await axe(container, {
    rules: {
      'color-contrast': { enabled: false },
      region: { enabled: false },
    },
  })
  expect(results).toHaveNoViolations()
})
