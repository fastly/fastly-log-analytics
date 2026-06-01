/**
 * Migration (TESTING_PLAN_3 item 10): swap fireEvent → userEvent and
 * placeholder-text → role/label selectors.
 *
 * Why this matters: ``fireEvent.change(input, { target: { value }})``
 * sets the DOM value directly and skips every step a real user makes
 * (focus, keystrokes, the controlled-component's onChange chain). It
 * also blasts through ``disabled``. userEvent.type / userEvent.click
 * model real interactions, so tests fail when the UI is genuinely
 * broken — not just when the assertion is wrong.
 *
 * ``getByRole``/``getByLabelText`` describe the form the way users (and
 * screen readers) see it. ``getByPlaceholderText`` couples tests to
 * marketing copy.
 *
 * One exception kept here: the Field Name input is ``disabled`` (it's
 * auto-derived from Label). userEvent refuses to type into disabled
 * inputs, which is correct for real users. The test still needs to
 * inject an invalid name to exercise the API-error path, so that one
 * step uses fireEvent.change as a documented escape hatch.
 */
import { render, screen, fireEvent, waitFor, act } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { afterEach, beforeEach, expect, test, vi } from 'vitest'
import { axe } from 'vitest-axe'
import { CustomFieldDrawer } from '@/components/CustomFields/CustomFieldDrawer'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import * as customFieldsApi from '@/lib/api/custom-fields'
import React from 'react'

// monaco-editor breaks under jsdom; replace with a plain textarea.
vi.mock('@monaco-editor/react', () => ({
  default: ({ value, onChange }: any) => (
    <textarea
      data-testid="mock-monaco"
      value={value}
      onChange={(e) => onChange(e.target.value)}
    />
  ),
}))

// jsdom shims required by the Drawer's underlying primitives.
Object.defineProperty(window, 'matchMedia', {
  writable: true,
  value: vi.fn().mockImplementation((query) => ({
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
window.HTMLElement.prototype.scrollIntoView = vi.fn()
window.HTMLElement.prototype.hasPointerCapture = vi.fn()
window.HTMLElement.prototype.releasePointerCapture = vi.fn()

const queryClient = new QueryClient()

test('submits valid custom field and displays validation errors when invalid', async () => {
  // Fake timers must be installed BEFORE render so the drawer's two
  // ``useDebounce`` hooks (vcl_log_expression + collection_stage)
  // schedule their setTimeouts under fake time. shouldAdvanceTime lets
  // userEvent's internal setTimeout polling keep working.
  vi.useFakeTimers({ shouldAdvanceTime: true })
  // userEvent v14 needs to be told about the fake-timer setup.
  const user = userEvent.setup({ advanceTimers: vi.advanceTimersByTime })

  const onSave = vi.fn()
  const onOpenChange = vi.fn()

  vi.spyOn(customFieldsApi.customFieldsApi, 'validateCustomVcl').mockResolvedValue({
    valid: true,
    errors: [],
    warnings: [],
    format_length_limit: 8000,
  } as any)

  vi.spyOn(customFieldsApi.customFieldsApi, 'createCustomField').mockRejectedValueOnce(
    new Error('Field name must be lowercase alphanumeric + underscore'),
  )

  render(
    <QueryClientProvider client={queryClient}>
      <CustomFieldDrawer
        serviceId="test-svc"
        open={true}
        onOpenChange={onOpenChange}
        onSave={onSave}
        field={null}
      />
    </QueryClientProvider>,
  )

  await waitFor(() => expect(screen.getByText('Basic Information')).toBeDefined())

  // Selectors now describe the form the way users see it.
  const labelInput = screen.getByLabelText(/^Label/i)
  const nameInput = screen.getByLabelText(/^Field Name/i)
  const vclInput = screen.getByLabelText('VCL Log Expression')

  // Real typing. userEvent fires focus / keydown / input / change /
  // keyup per character — so any onChange-controlled-input bug shows up.
  await user.type(labelInput, 'My Bad Field!')

  // Field Name is disabled in normal use (auto-derived from Label).
  // userEvent correctly refuses to type into a disabled input; use
  // fireEvent here to force the controlled state for this one assertion
  // path. Removing this escape hatch would mean adding a backdoor in
  // the component, which is worse.
  fireEvent.change(nameInput, { target: { value: 'BAD_NAME_!@#' } })

  await user.type(vclInput, 'req.http.Host')

  // Drive the 500 ms lint debounce + drain microtasks so
  // validateCustomVcl resolves and the React state update lands.
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

  // Repair: same escape hatch — controlled-and-disabled input.
  fireEvent.change(nameInput, { target: { value: 'valid_name' } })

  vi.spyOn(customFieldsApi.customFieldsApi, 'createCustomField').mockResolvedValueOnce(
    {} as any,
  )

  await user.click(saveBtn)

  await waitFor(() => {
    expect(onSave).toHaveBeenCalled()
  })

  vi.useRealTimers()
})

// TESTING_PLAN_3 item 19. The drawer has many form inputs that screen-
// reader users rely on labels for; this pins that contract. Disabling
// color-contrast (jsdom can't compute it) and region (Drawer renders into
// a portal that lives outside <main> by design, not an accessibility bug).
test('CustomFieldDrawer has no axe-detectable a11y violations', async () => {
  vi.spyOn(customFieldsApi.customFieldsApi, 'validateCustomVcl').mockResolvedValue({
    valid: true,
    errors: [],
    warnings: [],
    format_length_limit: 8000,
  } as any)

  const { container } = render(
    <QueryClientProvider client={queryClient}>
      <CustomFieldDrawer
        serviceId="test-svc"
        open={true}
        onOpenChange={vi.fn()}
        onSave={vi.fn()}
        field={null}
      />
    </QueryClientProvider>,
  )
  // Field Name input is the most stable anchor — present as soon as the
  // drawer's basic-info section renders.
  await waitFor(() => expect(screen.getByLabelText(/^Field Name/i)).toBeDefined())

  const results = await axe(container, {
    rules: {
      'color-contrast': { enabled: false },
      region: { enabled: false },
    },
  })
  expect(results).toHaveNoViolations()
})
