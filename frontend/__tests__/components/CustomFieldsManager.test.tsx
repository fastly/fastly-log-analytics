import { render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { expect, test, vi } from 'vitest'
import { CustomFieldsManager } from '@/components/CustomFields/CustomFieldsManager'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import * as customFieldsApi from '@/lib/api/custom-fields'
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

const queryClient = new QueryClient()

test('renders custom fields and handles deletion', async () => {
  const user = userEvent.setup()
  // Mock API to return a fake field
  vi.spyOn(customFieldsApi.customFieldsApi, 'listCustomFields').mockResolvedValue({
    fields: [
      {
        name: 'test_field_1',
        label: 'Test Field One',
        description: 'A test field',
        vcl_log_expression: 'req.http.Host',
        collection_stage: 'edge',
        duckdb_type: 'VARCHAR',
        value_type: 'string',
        bytes_estimate: 20,
        nullable: true,
        enabled: true,
        show_in_dashboard: true,
        show_in_logs: true,
        filterable: true
      }
    ]
  } as any)

  const deleteSpy = vi.spyOn(customFieldsApi.customFieldsApi, 'deleteCustomField').mockResolvedValue({} as any)

  render(
    <QueryClientProvider client={queryClient}>
      <CustomFieldsManager serviceId="test-svc" />
    </QueryClientProvider>
  )

  // Wait for the field to load
  await waitFor(() => expect(screen.getByText('Test Field One')).toBeDefined())
  expect(screen.getByText('test_field_1')).toBeDefined()
  expect(screen.getByText('VARCHAR')).toBeDefined()

  // Mock window.confirm to return true before the click — userEvent
  // fires the click synchronously enough that a confirm() called from
  // the click handler would race with a later spy install.
  vi.spyOn(window, 'confirm').mockImplementation(() => true)

  // Role-based query when possible, but title is the only accessible
  // anchor on the trash-icon button without bringing in an aria-label.
  const deleteBtn = screen.getByTitle('Delete')
  await user.click(deleteBtn)

  // Verify API was called
  await waitFor(() => expect(deleteSpy).toHaveBeenCalledWith('test-svc', 'test_field_1'))
})
