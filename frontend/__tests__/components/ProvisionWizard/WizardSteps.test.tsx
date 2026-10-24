/**
 * @vitest-environment jsdom
 *
 * Consolidated ProvisionWizard component tests: TerraformStep,
 * topology capability (ModeStep + JoinStep), WizardFooter, and RumVersionPicker.
 */
import * as React from 'react'
import { act, fireEvent, render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { afterEach, describe, expect, it, test, vi } from 'vitest'
import { http, HttpResponse } from 'msw'

import { TerraformStep } from '@/components/ProvisionWizard/steps/TerraformStep'
import { ModeStep } from '@/components/ProvisionWizard/steps/ModeStep'
import { JoinStep } from '@/components/ProvisionWizard/steps/JoinStep'
import { WizardFooter } from '@/components/ProvisionWizard/WizardFooter'
import { RumVersionPicker } from '@/components/ProvisionWizard/RumVersionPicker'
import {
  ANALYST_PATH_A_UNSUPPORTED_REASON,
  INITIAL_CONFIG,
  type WizardState,
} from '@/components/ProvisionWizard/types'
import { renderWithQuery } from '../../helpers/query'
import { server } from '@/__tests__/helpers/msw/server'
import { getApiBase } from '@/lib/api'

const API_BASE = getApiBase()
const SVC = 'svc-new-1'

afterEach(() => {
  server.resetHandlers()
})

describe('TerraformStep', () => {
  const tfState = {
    terraformFiles: { 'main.tf': 'resource "x" "y" {}' },
    selectedTfFile: 'main.tf',
    setSelectedTfFile: vi.fn(),
    isFetchingTerraform: false,
    handleExportTerraform: vi.fn(),
  } as unknown as WizardState

  test('copy button writes the selected file to the clipboard', async () => {
    const writeText = vi.fn().mockResolvedValue(undefined)
    Object.defineProperty(navigator, 'clipboard', { value: { writeText }, configurable: true })
    render(<TerraformStep s={tfState} />)
    await act(async () => {
      fireEvent.click(screen.getByRole('button', { name: 'Copy main.tf to clipboard' }))
    })
    expect(writeText).toHaveBeenCalledWith('resource "x" "y" {}')
  })

  test('a rejected clipboard write is swallowed instead of surfacing as an unhandled rejection', async () => {
    const writeText = vi.fn().mockRejectedValue(new Error('denied'))
    Object.defineProperty(navigator, 'clipboard', { value: { writeText }, configurable: true })
    render(<TerraformStep s={tfState} />)
    await act(async () => {
      fireEvent.click(screen.getByRole('button', { name: 'Copy main.tf to clipboard' }))
    })
    await new Promise((r) => setTimeout(r, 0))
    expect(writeText).toHaveBeenCalledTimes(1)
  })
})

function makeTopologyState(overrides: Partial<WizardState> = {}): WizardState {
  return {
    step: 'mode',
    setStep: vi.fn(),
    mode: 'join',
    setMode: vi.fn(),
    analystPathASupported: true,
    analystPathAReason: null,
    joinPhase: 'form',
    config: INITIAL_CONFIG,
    setConfig: vi.fn(),
    isAnalyzing: false,
    lines: [],
    status: 'idle',
    sseError: null,
    icebergMetadataLocation: '',
    setIcebergMetadataLocation: vi.fn(),
    handleCheckFos: vi.fn(),
    fosStatus: 'idle',
    fosError: '',
    selectedService: null,
    selectedCdnService: null,
    servicesData: [],
    configStatus: null,
    isCheckingConfig: false,
    handleCheckConfig: vi.fn(),
    ...overrides,
  } as WizardState
}

describe('ProvisionWizard topology capability', () => {
  it('keeps Analyst: Join enabled for supported sync topology', () => {
    render(<ModeStep s={makeTopologyState({ analystPathASupported: true })} />)
    expect(screen.getByRole('button', { name: /analyst: join/i })).toBeEnabled()
  })

  it('disables Analyst: Join for unsupported scalable topology', () => {
    render(
      <ModeStep
        s={makeTopologyState({
          analystPathASupported: false,
          analystPathAReason: ANALYST_PATH_A_UNSUPPORTED_REASON,
        })}
      />,
    )
    expect(screen.getByRole('button', { name: /analyst: join/i })).toBeDisabled()
    expect(screen.getByText(ANALYST_PATH_A_UNSUPPORTED_REASON)).toBeVisible()
  })

  it('warns when a stale invite or manual entry reaches JoinStep', () => {
    render(
      <JoinStep
        s={makeTopologyState({
          analystPathASupported: false,
          analystPathAReason: ANALYST_PATH_A_UNSUPPORTED_REASON,
        })}
      />,
    )
    expect(screen.getByText(/independent analyst join is unavailable/i)).toBeVisible()
    expect(screen.getByText(/old invite or entered credentials manually/i)).toBeVisible()
    expect(screen.getByText(/Path B/i)).toBeVisible()
  })
})

function makeFooterState(overrides: Partial<WizardState> = {}): WizardState {
  return {
    step: 'terraform',
    setStep: vi.fn(),
    mode: 'provision',
    isDeploying: false,
    status: 'idle',
    isDone: false,
    handleModalClose: vi.fn(),
    isAnalyzing: false,
    token: 'tok',
    isLoadingServices: false,
    handleTokenSubmit: vi.fn(),
    selectedService: { id: 'svc', name: 'Svc' },
    validateMutation: { isPending: false },
    domainStatus: 'available',
    config: { fos_bucket_name: 'bucket', ngwaf_workspace_id: '' },
    fetchTerraformPreview: vi.fn(),
    handleDeploy: vi.fn(),
    handleAdminIngest: vi.fn(),
    fosStatus: 'idle',
    handleAnalyzeLake: vi.fn(),
    importMode: 'all',
    importRange: { start: '', end: '' },
    handleJoin: vi.fn(),
    joinPhase: 'form',
    stop: vi.fn(),
    ...overrides,
  } as WizardState
}

describe('WizardFooter — terraform (Terraform & VCL preview) step', () => {
  it("provision and ingest modes: 'Back to Review' returns to Review without deploying or ingesting", async () => {
    for (const mode of ['provision', 'ingest'] as const) {
      const setStep = vi.fn()
      const handleDeploy = vi.fn()
      const handleAdminIngest = vi.fn()
      const s = makeFooterState({ mode, setStep, handleDeploy, handleAdminIngest })
      const { unmount } = render(<WizardFooter s={s} />)

      expect(screen.queryByRole('button', { name: /deploy to fastly/i })).not.toBeInTheDocument()
      expect(screen.queryByRole('button', { name: /complete setup/i })).not.toBeInTheDocument()

      await userEvent.click(screen.getByRole('button', { name: /back to review/i }))
      expect(setStep).toHaveBeenCalledWith('execute')
      expect(handleDeploy).not.toHaveBeenCalled()
      expect(handleAdminIngest).not.toHaveBeenCalled()
      unmount()
    }
  })
})

function renderPicker(onChange: (v: string | null) => void, value: string | null = null) {
  return renderWithQuery(<RumVersionPicker serviceId={SVC} value={value} onChange={onChange} />)
}

describe('RumVersionPicker', () => {
  it('renders the loading state without crashing while the fetch is in flight', () => {
    server.use(
      http.get(`${API_BASE}/api/services/:service_id/rum/versions`, async () => {
        await new Promise(() => {})
        return HttpResponse.json({ available: [] })
      }),
    )
    const { container } = renderPicker(() => {})
    expect(screen.getByText(/faro web sdk version/i)).toBeInTheDocument()
    expect(container.querySelector('[data-slot="skeleton"]')).toBeTruthy()
  })

  it('lists available versions, marks latest, and reports a selection', async () => {
    server.use(
      http.get(`${API_BASE}/api/services/:service_id/rum/versions`, () =>
        HttpResponse.json({
          available: ['1.9.0', '1.8.0', '1.7.0'],
          current: null,
          latest: '1.9.0',
          update_available: false,
        }),
      ),
    )
    const seen: (string | null)[] = []
    const user = userEvent.setup()
    renderPicker((v) => seen.push(v))

    await waitFor(() => expect(seen).toContain('1.9.0'))

    await user.click(screen.getByRole('combobox', { name: /faro web sdk version/i }))
    await user.click(await screen.findByRole('option', { name: /1\.7\.0/ }))
    expect(seen).toContain('1.7.0')
  })

  it('degrades to an inline message + retry on a registry 503 or empty version list', async () => {
    server.use(
      http.get(`${API_BASE}/api/services/:service_id/rum/versions`, () =>
        HttpResponse.json(
          { error: 'faro_registry_unavailable', message: 'registry down' },
          { status: 503 },
        ),
      ),
    )
    const { unmount } = renderPicker(() => {
      throw new Error('onChange must not fire on a degraded fetch')
    })

    expect(await screen.findByText(/couldn't reach the npm registry/i)).toBeInTheDocument()
    expect(screen.getByText(/default self-hosted version/i)).toBeInTheDocument()
    expect(screen.queryByRole('combobox')).not.toBeInTheDocument()
    expect(screen.getByRole('button', { name: /retry/i })).toBeEnabled()
    unmount()

    server.use(
      http.get(`${API_BASE}/api/services/:service_id/rum/versions`, () =>
        HttpResponse.json({ available: [], current: null, latest: null, update_available: false }),
      ),
    )
    renderPicker(() => {
      throw new Error('onChange must not fire when there is nothing to default to')
    })
    expect(await screen.findByText(/no versions available/i)).toBeInTheDocument()
    expect(screen.queryByRole('combobox')).not.toBeInTheDocument()
  })

  it('retry re-fires the request after a registry outage clears', async () => {
    let calls = 0
    server.use(
      http.get(`${API_BASE}/api/services/:service_id/rum/versions`, () => {
        calls += 1
        if (calls === 1) {
          return HttpResponse.json({ error: 'faro_registry_unavailable' }, { status: 503 })
        }
        return HttpResponse.json({
          available: ['2.0.0'],
          current: null,
          latest: '2.0.0',
          update_available: false,
        })
      }),
    )
    const seen: (string | null)[] = []
    const user = userEvent.setup()
    renderPicker((v) => seen.push(v))

    await screen.findByText(/couldn't reach the npm registry/i)
    await user.click(screen.getByRole('button', { name: /retry/i }))

    await waitFor(() => expect(seen).toContain('2.0.0'))
    expect(calls).toBe(2)
  })
})
