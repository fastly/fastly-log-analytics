/**
 * Consolidated CustomFields tests: CustomFieldDrawer and CustomFieldsManager.
 */
import { screen, fireEvent, waitFor, act } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { afterEach, expect, test, vi } from 'vitest'
import { axe } from 'vitest-axe'
import React from 'react'
import { CustomFieldDrawer } from '@/components/CustomFields/CustomFieldDrawer'
import { CustomFieldsManager } from '@/components/CustomFields/CustomFieldsManager'
import * as customFieldsApi from '@/lib/api'
import type { components } from '@/types/api.generated'
import { renderWithQuery } from '../helpers/query'
import { stubMatchMedia, stubPointerCaptureAndScroll } from '../helpers/browser-stubs'

vi.mock('@tanstack/react-virtual', async () => (await import('../helpers/browser-stubs')).reactVirtualModuleMock())

// monaco-editor breaks under jsdom; replace with a plain textarea.
vi.mock('@monaco-editor/react', () => ({
  default: ({ value, onChange }: { value?: string; onChange: (next: string) => void }) => (
    <textarea
      data-testid="mock-monaco"
      value={value}
      onChange={(e) => onChange(e.target.value)}
    />
  ),
}))

// jsdom shims required by the Drawer's underlying primitives.
stubMatchMedia()
stubPointerCaptureAndScroll()

afterEach(() => {
  vi.useRealTimers()
  vi.restoreAllMocks()
})

test('submits valid custom field and displays validation errors when invalid', async () => {
  const realError = console.error
  const actFilter = vi
    .spyOn(console, 'error')
    .mockImplementation((...args: unknown[]) => {
      if (typeof args[0] === 'string' && args[0].includes('not wrapped in act')) return
      realError(...args)
    })
  const restoreActFilter = () => actFilter.mockRestore()

  vi.useFakeTimers({ shouldAdvanceTime: true })
  const user = userEvent.setup({ advanceTimers: vi.advanceTimersByTime })

  const onSave = vi.fn()
  const onOpenChange = vi.fn()

  vi.spyOn(customFieldsApi.customFieldsApi, 'validateCustomVcl').mockResolvedValue({
    valid: true,
    errors: [],
    warnings: [],
    format_length_limit: 8000,
  })

  vi.spyOn(customFieldsApi.customFieldsApi, 'createCustomField').mockRejectedValueOnce(
    new Error('Field name must be lowercase alphanumeric + underscore'),
  )

  renderWithQuery(
    <CustomFieldDrawer
      serviceId="test-svc"
      open={true}
      onOpenChange={onOpenChange}
      onSave={onSave}
      field={null}
    />,
  )

  await waitFor(() => expect(screen.getByText('Basic Information')).toBeDefined())

  const labelInput = screen.getByLabelText(/^Label/i)
  const nameInput = screen.getByLabelText(/^Field Name/i)
  const vclInput = screen.getByLabelText('VCL Log Expression')

  await user.type(labelInput, 'My Bad Field!')
  fireEvent.change(nameInput, { target: { value: 'BAD_NAME_!@#' } })
  await user.type(vclInput, 'req.http.Host')

  await act(async () => {
    await vi.advanceTimersByTimeAsync(500)
  })

  const saveBtn = screen.getByRole('button', { name: /save field/i })
  await waitFor(() => expect((saveBtn as HTMLButtonElement).disabled).toBe(false), {
    timeout: 500,
  })

  await user.click(saveBtn)

  await waitFor(() => {
    expect(
      screen.getByText('Field name must be lowercase alphanumeric + underscore'),
    ).toBeDefined()
  })

  fireEvent.change(nameInput, { target: { value: 'valid_name' } })

  vi.spyOn(customFieldsApi.customFieldsApi, 'createCustomField').mockResolvedValueOnce(
    {
      _is_cached: false,
      field: {
        name: 'valid_name',
        label: 'My Bad Field!',
        description: '',
        vcl_log_expression: 'req.http.Host',
        collection_stage: 'edge',
        origin_log_frequency: 'all',
        duckdb_type: 'VARCHAR',
        value_type: 'string',
        bytes_estimate: 32,
        nullable: true,
        enabled: true,
        show_in_dashboard: false,
        show_in_logs: false,
        filterable: false,
      },
      warnings: [],
    },
  )

  await user.click(saveBtn)

  await waitFor(() => {
    expect(onSave).toHaveBeenCalled()
  })

  vi.useRealTimers()
  restoreActFilter()
})

test('CustomFieldDrawer has no axe-detectable a11y violations', async () => {
  vi.spyOn(customFieldsApi.customFieldsApi, 'validateCustomVcl').mockResolvedValue({
    valid: true,
    errors: [],
    warnings: [],
    format_length_limit: 8000,
  })

  const { container } = renderWithQuery(
    <CustomFieldDrawer
      serviceId="test-svc"
      open={true}
      onOpenChange={vi.fn()}
      onSave={vi.fn()}
      field={null}
    />,
  )
  await waitFor(() => expect(screen.getByLabelText(/^Field Name/i)).toBeDefined())

  const results = await axe(container, {
    rules: {
      'color-contrast': { enabled: false },
      region: { enabled: false },
    },
  })
  expect(results).toHaveNoViolations()
})

test('renders custom fields and handles deletion', async () => {
  const user = userEvent.setup()
  vi.spyOn(customFieldsApi.customFieldsApi, 'listCustomFields').mockResolvedValue({
    _is_cached: false,
    fields: [
      {
        name: 'test_field_1',
        label: 'Test Field One',
        description: 'A test field',
        vcl_log_expression: 'req.http.Host',
        collection_stage: 'edge',
        origin_log_frequency: 'all',
        duckdb_type: 'VARCHAR',
        value_type: 'string',
        bytes_estimate: 20,
        nullable: true,
        enabled: true,
        show_in_dashboard: true,
        show_in_logs: true,
        filterable: true,
      },
    ],
  })

  const deleteSpy = vi
    .spyOn(customFieldsApi.customFieldsApi, 'deleteCustomField')
    .mockResolvedValue({} as components['schemas']['CustomFieldResponse'])

  renderWithQuery(<CustomFieldsManager serviceId="test-svc" />)

  await waitFor(() => expect(screen.getByText('Test Field One')).toBeDefined())
  expect(screen.getByText('test_field_1')).toBeDefined()
  expect(screen.getByText('VARCHAR')).toBeDefined()

  vi.spyOn(window, 'confirm').mockImplementation(() => true)

  const deleteBtn = screen.getByTitle('Delete')
  await user.click(deleteBtn)

  await waitFor(() => expect(deleteSpy).toHaveBeenCalledWith('test-svc', 'test_field_1'))
})
