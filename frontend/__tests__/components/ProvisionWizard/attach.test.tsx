/**
 * @vitest-environment jsdom
 */
import { fireEvent, render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { afterEach, describe, expect, it, vi } from 'vitest'
import { http, HttpResponse } from 'msw/http'
import { getApiBase } from '@/lib/api'
import { setCloudModeRuntime } from '@/lib/cloud-engine/mode'
import { server } from '@/__tests__/helpers/msw/handlers'
import { createTestQueryClient, createUseSSEMock, renderWithQuery } from '@/__tests__/helpers/page-smoke'
import { ATTACH_ENDPOINT_NAME, applyAttach, attachUnavailableMessage, clearAttach, resolveAttachOffer } from '@/components/ProvisionWizard/attach'
import { REGION_LABELS } from '@/lib/utils'
import { ProvisionWizard } from '@/components/ProvisionWizard/ProvisionWizard'
import { getStepsForMode, INITIAL_CONFIG, type WizardState } from '@/components/ProvisionWizard/types'
import { WizardFooter } from '@/components/ProvisionWizard/WizardChrome'
import { ServiceStep } from '@/components/ProvisionWizard/steps/CredentialsStep'
import { FeaturesStep } from '@/components/ProvisionWizard/steps/DomainStep'
import { FieldsStep } from '@/components/ProvisionWizard/steps/FieldsStep'
import { runDeploy } from '@/components/ProvisionWizard/wizardActions'

const API_BASE = getApiBase()
const managed = Object.assign(new Error('service_managed_by_other_install'), { status: 409 })
const noFields = { groups: [], field_overrides: {} }
const { state: sseState, start: mockStart } = createUseSSEMock()

vi.mock('@/hooks/useSSE', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/hooks/useSSE')>()),
  useSSE: () => sseState,
}))

afterEach(() => setCloudModeRuntime(false))

describe('resolveAttachOffer', () => {
  it('asks Cloud to validate in attached mode and returns the fields the existing VCL never sets', async () => {
    setCloudModeRuntime(true)
    let received: Record<string, unknown> | null = null
    server.use(http.post(`${API_BASE}/api/provision/validate`, async ({ request }) => {
      received = await request.json() as Record<string, unknown>
      return HttpResponse.json({ valid: true, capture: { mode: 'attached', missing_fields: ['server_region'] } })
    }))
    const fields = { groups: ['A', 'C'], field_overrides: {} }
    const offer = await resolveAttachOffer(managed, 'TOK', 'svc-1', fields)
    expect(received).toEqual({ token: 'TOK', service_id: 'svc-1', capture: 'attached', log_fields: fields })
    expect(offer).toMatchObject({ serviceId: 'svc-1', available: true, missingFields: ['server_region'] })
  })

  it('leaves log_fields to the server default when no groups are chosen yet', async () => {
    setCloudModeRuntime(true)
    let received: Record<string, unknown> | null = null
    server.use(http.post(`${API_BASE}/api/provision/validate`, async ({ request }) => {
      received = await request.json() as Record<string, unknown>
      return HttpResponse.json({ valid: true, capture: { mode: 'attached', missing_fields: [] } })
    }))
    await resolveAttachOffer(Object.assign(new Error('managed_resources_not_owned'), { status: 409 }), 'TOK', 'svc-1', noFields)
    expect(received).not.toHaveProperty('log_fields')
  })

  it('reports why attaching is impossible when the other install has no capture VCL', async () => {
    setCloudModeRuntime(true)
    server.use(http.post(`${API_BASE}/api/provision/validate`, () =>
      HttpResponse.json({ detail: 'capture_vcl_missing', error: 'capture_vcl_missing' }, { status: 409 })))
    const offer = await resolveAttachOffer(managed, 'TOK', 'svc-1', noFields)
    expect(offer).toEqual({ serviceId: 'svc-1', available: false, reason: 'capture_vcl_missing' })
    expect(attachUnavailableMessage('capture_vcl_missing')).toMatch(/capture VCL snippets are missing/)
  })

  it('offers nothing outside Cloud or for unrelated validation errors', async () => {
    let calls = 0
    server.use(http.post(`${API_BASE}/api/provision/validate`, () => {
      calls += 1
      return HttpResponse.json({ valid: true })
    }))
    expect(await resolveAttachOffer(managed, 'TOK', 'svc-1', noFields)).toBeNull()
    setCloudModeRuntime(true)
    expect(await resolveAttachOffer(new Error('service_not_found'), 'TOK', 'svc-1', noFields)).toBeNull()
    expect(calls).toBe(0)
  })
})

describe('attach config', () => {
  it('sets capture attached with the reserved endpoint name while preserving rum_enabled and cmcd_enabled', () => {
    const attached = applyAttach({ ...INITIAL_CONFIG, rum_enabled: true, cmcd_enabled: true, endpoint_name: 'Mine' })
    expect(attached).toMatchObject({
      capture: 'attached', endpoint_name: ATTACH_ENDPOINT_NAME, logging_enabled: true, rum_enabled: true, cmcd_enabled: true,
    })
    let posted: Record<string, unknown> | undefined
    runDeploy({
      token: 'TOK', selectedService: { id: 'svc-1', name: 'Demo', provisioned: false }, config: attached,
      setIsDeploying: () => {}, start: (_path, body) => { posted = body },
    })
    expect(posted).toMatchObject({
      capture: 'attached', endpoint_name: ATTACH_ENDPOINT_NAME, rum_enabled: true, cmcd_enabled: true,
    })
  })

  it('a managed deploy sends no capture key, and choosing another service clears attach mode', () => {
    let posted: Record<string, unknown> | undefined
    runDeploy({
      token: 'TOK', selectedService: { id: 'svc-1', name: 'Demo', provisioned: false }, config: INITIAL_CONFIG,
      setIsDeploying: () => {}, start: (_path, body) => { posted = body },
    })
    expect(posted).not.toHaveProperty('capture')
    expect(clearAttach(applyAttach(INITIAL_CONFIG))).toMatchObject({ capture: 'managed', endpoint_name: '' })
    const custom = { ...applyAttach(INITIAL_CONFIG), endpoint_name: 'Team logs' }
    expect(clearAttach(custom).endpoint_name).toBe('Team logs')
  })
})

describe('attach UI', () => {
  const service = { id: 'svc-1', name: 'Demo', provisioned: false }
  const state = (overrides: Partial<WizardState>) => ({
    search: '', setSearch: vi.fn(), tokenInfo: null, isLoadingServices: false, filteredServices: [service],
    selectedService: service, validateMutation: { isPending: false }, handleServiceSelect: vi.fn(),
    attachOffer: null, isCheckingAttach: false, acceptAttach: vi.fn(), config: INITIAL_CONFIG, setConfig: vi.fn(),
    STEPS: [{ id: 'features', label: 'Features' }], ...overrides,
  }) as unknown as WizardState

  it('offers to attach, lists fields that will be blank, and accepts on click', () => {
    const s = state({ attachOffer: { serviceId: 'svc-1', available: true, missingFields: ['server_region'], data: {} } })
    render(<ServiceStep s={s} />)
    expect(screen.getByText(/server_region/)).toBeInTheDocument()
    expect(screen.getByText(new RegExp(ATTACH_ENDPOINT_NAME))).toBeInTheDocument()
    expect(screen.queryByText(/Request logs only: RUM and CMCD stay with the other install/)).toBeNull()
    expect(screen.getByText(/RUM/i)).toBeInTheDocument()
    expect(screen.getByText(/CMCD/i)).toBeInTheDocument()
    fireEvent.click(screen.getByRole('button', { name: 'Attach alongside the existing install' }))
    expect(s.acceptAttach).toHaveBeenCalledOnce()
  })

  it('explains a missing capture VCL instead of offering to attach', () => {
    render(<ServiceStep s={state({ attachOffer: { serviceId: 'svc-1', available: false, reason: 'capture_vcl_missing' } })} />)
    expect(screen.getByRole('alert')).toHaveTextContent(/capture VCL snippets are missing/)
    expect(screen.queryByRole('button', { name: /Attach alongside/ })).toBeNull()
  })

  it('keeps the RUM feature choices enabled once attached', () => {
    render(<FeaturesStep s={state({ config: applyAttach(INITIAL_CONFIG) })} />)
    const buttons = screen.getAllByRole('button')
    expect(buttons.filter((b) => b.hasAttribute('disabled'))).toHaveLength(0)
  })

  it('shows the CMCD configuration section once attached', () => {
    render(<FieldsStep s={state({
      config: applyAttach(INITIAL_CONFIG),
      isLoadingCatalog: false,
      catalog: { groups: [], presets: {} },
      estimatedBytes: 100,
    })} />)
    expect(screen.getByText(/CMCD/i)).toBeInTheDocument()
  })

  it('auto-matches rum_enabled, cmcd_enabled, and log_fields from the attach offer', () => {
    const offer = {
      serviceId: 'svc-1',
      available: true as const,
      missingFields: ['server_region'],
      data: {
        capture: { mode: 'attached', missing_fields: ['server_region'], rum_present: true, cmcd_present: true },
      },
    }
    const attached = applyAttach(INITIAL_CONFIG, offer)
    expect(attached).toMatchObject({
      capture: 'attached',
      endpoint_name: ATTACH_ENDPOINT_NAME,
      logging_enabled: true,
      rum_enabled: true,
      cmcd_enabled: true,
    })
    expect(attached.log_fields.groups).toEqual(['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M'])
    expect(attached.log_fields.field_overrides).toEqual({ server_region: false })
  })

  it('skips features and fields steps in attach mode', () => {
    setCloudModeRuntime(true)
    expect(
      getStepsForMode('provision', { logging_enabled: true, capture: 'attached' }).map((s) => s.id),
    ).toEqual(['token', 'service', 'storage', 'execute'])

    const setStep = vi.fn()
    render(
      <WizardFooter
        s={state({
          step: 'storage',
          mode: 'provision',
          setStep,
          domainStatus: 'idle',
          config: applyAttach(INITIAL_CONFIG),
        })}
      />,
    )
    fireEvent.click(screen.getByRole('button', { name: 'Continue' }))
    expect(setStep).toHaveBeenCalledWith('execute')
  })

  it('populates fos_bucket_name, fos_region, fos_prefix, and tokenInfo when accepting attach', async () => {
    setCloudModeRuntime(true)
    Element.prototype.getAnimations ??= () => []
    const user = userEvent.setup()
    mockStart.mockClear()
    server.use(
      http.post(`${API_BASE}/api/provision/services`, () =>
        HttpResponse.json({ services: [{ id: 'svc-1', name: 'Demo', provisioned: false }] }),
      ),
      http.post(`${API_BASE}/api/provision/validate`, async ({ request }) => {
        const body = (await request.json()) as { capture?: string }
        if (body.capture === 'attached') {
          return HttpResponse.json({
            valid: true,
            service_name: 'Demo',
            token_info: { id: 'tok-1', name: 'CI Token', type: 'automation' },
            defaults: {
              endpoint_name: 'Fastly Object Storage Logs',
              fos_region: 'eu-central-1',
              fos_bucket_name: 'fos-svc-1-logs',
              fos_prefix: 'logs/',
            },
            capture: {
              mode: 'attached',
              missing_fields: ['server_region'],
              rum_present: true,
              cmcd_present: false,
            },
          })
        }
        return HttpResponse.json(
          { detail: 'service_managed_by_other_install', error: 'service_managed_by_other_install' },
          { status: 409 },
        )
      }),
    )

    renderWithQuery(<ProvisionWizard open={true} onOpenChange={vi.fn()} />, {
      queryClient: createTestQueryClient(),
    })

    await user.type(screen.getByLabelText(/api token/i), 'TOK')
    await user.click(screen.getByRole('button', { name: /fetch services/i }))
    await user.click(await screen.findByRole('button', { name: /Select service Demo, id svc-1/i }))
    await user.click(await screen.findByRole('button', { name: 'Attach alongside the existing install' }))

    expect(await screen.findByDisplayValue('fos-svc-1-logs')).toBeInTheDocument()
    expect(screen.getByDisplayValue('FLP Cloud Logs')).toBeInTheDocument()
    expect(screen.getByText(REGION_LABELS['eu-central-1'])).toBeInTheDocument()
    expect(screen.queryByText(/Select features to deploy/i)).toBeNull()

    await user.click(screen.getByRole('button', { name: 'Continue' }))
    expect(await screen.findByText(/fos-svc-1-logs/)).toBeInTheDocument()

    await user.click(screen.getByRole('button', { name: /Back/i }))
    await user.click(screen.getByRole('button', { name: /Back/i }))
    expect(await screen.findByText('CI Token')).toBeInTheDocument()

    await user.click(screen.getByRole('button', { name: /Select service Demo, id svc-1/i }))
    await user.click(await screen.findByRole('button', { name: 'Attach alongside the existing install' }))
    await user.click(await screen.findByRole('button', { name: 'Continue' }))
    await user.click(await screen.findByRole('button', { name: 'Deploy to Fastly' }))
    expect(mockStart).toHaveBeenCalledWith(
      '/api/provision/execute',
      expect.objectContaining({
        capture: 'attached',
        endpoint_name: 'FLP Cloud Logs',
        fos_bucket_name: 'fos-svc-1-logs',
        fos_region: 'eu-central-1',
        fos_prefix: 'logs/',
        logging_enabled: true,
        rum_enabled: true,
        cmcd_enabled: false,
        log_fields: JSON.stringify({
          groups: ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M'],
          field_overrides: { server_region: false },
        }),
      }),
    )
  })
})
