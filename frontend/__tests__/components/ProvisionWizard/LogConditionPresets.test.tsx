/**
 * @vitest-environment jsdom
 */
import * as React from 'react'
import {
  renderWithQuery, stubMatchMedia, stubPointerCaptureAndScroll,
} from '@/__tests__/helpers/page-smoke'
import { screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { describe, expect, it, vi } from 'vitest'
import {
  INITIAL_CONFIG, type ProvisionConfig, type WizardState,
} from '@/components/ProvisionWizard/ProvisionWizard'
import { StorageStep } from '@/components/ProvisionWizard/steps/StorageStep'

stubMatchMedia()
stubPointerCaptureAndScroll()
window.HTMLElement.prototype.scrollIntoView = vi.fn()

function StorageHarness({ initialConfig }: { initialConfig?: Partial<ProvisionConfig> }) {
  const [config, setConfig] = React.useState<ProvisionConfig>({
    ...INITIAL_CONFIG,
    logging_enabled: true,
    ...initialConfig,
  })
  const state = {
    step: 'storage',
    config,
    setConfig,
    domainStatus: 'idle',
    domainMessage: '',
    checkDomain: vi.fn(),
  } as unknown as WizardState

  return <StorageStep s={state} />
}

async function selectLogConditionPreset(user: ReturnType<typeof userEvent.setup>, optionName: RegExp | string) {
  const trigger = screen.getByRole('combobox', { name: /log condition/i })
  await user.click(trigger)
  const option = await screen.findByRole('option', { name: optionName })
  await user.click(option)
}

describe('ProvisionWizard Log Condition preset dropdown', () => {
  it('defaults to "All requests" with no freeform input shown', () => {
    renderWithQuery(<StorageHarness />)
    const trigger = screen.getByRole('combobox', { name: /log condition/i })
    expect(trigger).toHaveTextContent(/all requests/i)
    expect(screen.queryByRole('textbox', { name: /custom vcl condition/i })).not.toBeInTheDocument()
  })

  it('populates custom_condition when selecting each preset', async () => {
    const user = userEvent.setup()
    renderWithQuery(<StorageHarness initialConfig={{ sample_rate: 25 }} />)

    await selectLogConditionPreset(user, /errors only \(4xx & 5xx\)/i)
    expect(screen.getByRole('combobox', { name: /log condition/i })).toHaveTextContent(/errors only \(4xx & 5xx\)/i)
    expect(screen.getByText('resp.status >= 400')).toBeInTheDocument()

    await selectLogConditionPreset(user, /server errors only \(5xx\)/i)
    expect(screen.getByText('resp.status >= 500')).toBeInTheDocument()

    await selectLogConditionPreset(user, /page loads only/i)
    expect(
      screen.getByText('req.url.ext !~ "^(?i)(js|css|png|jpg|jpeg|gif|svg|ico|woff2?|ttf|eot|webp|avif|map)$"'),
    ).toBeInTheDocument()

    await selectLogConditionPreset(user, /sample plus all errors/i)
    expect(screen.getByText('resp.status >= 400 || randombool(10, 100)')).toBeInTheDocument()
    const sampleRateInput = screen.getByRole('spinbutton') as HTMLInputElement
    expect(sampleRateInput.value).toBe('100')

    await selectLogConditionPreset(user, /all requests/i)
    expect(screen.getByRole('combobox', { name: /log condition/i })).toHaveTextContent(/all requests/i)
    expect(screen.queryByText('resp.status >= 400 || randombool(10, 100)')).not.toBeInTheDocument()
  })

  it('shows editable freeform input when switching to Custom and hides it when switching back', async () => {
    const user = userEvent.setup()
    renderWithQuery(<StorageHarness />)

    await selectLogConditionPreset(user, /^custom$/i)
    const input = screen.getByRole('textbox', { name: /custom vcl condition/i }) as HTMLInputElement
    expect(input).toBeVisible()
    expect(input.value).toBe('')

    await user.type(input, 'req.url.path ~ "^/api/"')
    expect(input.value).toBe('req.url.path ~ "^/api/"')

    // Clearing the input while in Custom mode keeps Custom selected and input visible.
    await user.clear(input)
    expect(screen.getByRole('combobox', { name: /log condition/i })).toHaveTextContent(/custom/i)
    expect(screen.getByRole('textbox', { name: /custom vcl condition/i })).toBeVisible()

    // Switching back to a preset hides the freeform input.
    await selectLogConditionPreset(user, /server errors only \(5xx\)/i)
    expect(screen.queryByRole('textbox', { name: /custom vcl condition/i })).not.toBeInTheDocument()
    expect(screen.getByText('resp.status >= 500')).toBeInTheDocument()

    // Switching from a preset to Custom preserves the preset expression for editing.
    await selectLogConditionPreset(user, /^custom$/i)
    const editableInput = screen.getByRole('textbox', { name: /custom vcl condition/i }) as HTMLInputElement
    expect(editableInput.value).toBe('resp.status >= 500')
  })

  it('auto-selects matching preset or Custom on load', () => {
    const { unmount } = renderWithQuery(
      <StorageHarness initialConfig={{ custom_condition: 'resp.status >= 500' }} />,
    )
    expect(screen.getByRole('combobox', { name: /log condition/i })).toHaveTextContent(/server errors only \(5xx\)/i)
    expect(screen.queryByRole('textbox', { name: /custom vcl condition/i })).not.toBeInTheDocument()
    unmount()

    renderWithQuery(
      <StorageHarness initialConfig={{ custom_condition: 'req.http.Host == "example.com"' }} />,
    )
    expect(screen.getByRole('combobox', { name: /log condition/i })).toHaveTextContent(/custom/i)
    const input = screen.getByRole('textbox', { name: /custom vcl condition/i }) as HTMLInputElement
    expect(input.value).toBe('req.http.Host == "example.com"')
  })
})
