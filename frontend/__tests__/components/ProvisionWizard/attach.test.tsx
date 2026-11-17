/**
 * @vitest-environment jsdom
 */
import { fireEvent, render, screen } from '@testing-library/react'
import { afterEach, describe, expect, it, vi } from 'vitest'
import { http, HttpResponse } from 'msw/http'
import { getApiBase } from '@/lib/api'
import { setCloudModeRuntime } from '@/lib/cloud-engine/mode'
import { server } from '@/__tests__/helpers/msw/handlers'
import {
  ATTACH_ENDPOINT_NAME, applyAttach, attachUnavailableMessage, clearAttach, resolveAttachOffer,
} from '@/components/ProvisionWizard/attach'
import { INITIAL_CONFIG, type WizardState } from '@/components/ProvisionWizard/types'
import { ServiceStep } from '@/components/ProvisionWizard/steps/CredentialsStep'
import { FeaturesStep } from '@/components/ProvisionWizard/steps/DomainStep'
import { runDeploy } from '@/components/ProvisionWizard/wizardActions'

const API_BASE = getApiBase()
const managed = Object.assign(new Error('service_managed_by_other_install'), { status: 409 })
const noFields = { groups: [], field_overrides: {} }

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
  it('forces logging only under the reserved endpoint name and deploys with capture attached', () => {
    const attached = applyAttach({ ...INITIAL_CONFIG, rum_enabled: true, cmcd_enabled: true, endpoint_name: 'Mine' })
    expect(attached).toMatchObject({
      capture: 'attached', endpoint_name: ATTACH_ENDPOINT_NAME, logging_enabled: true, rum_enabled: false, cmcd_enabled: false,
    })
    let posted: Record<string, unknown> | undefined
    runDeploy({
      token: 'TOK', selectedService: { id: 'svc-1', name: 'Demo', provisioned: false }, config: attached,
      setIsDeploying: () => {}, start: (_path, body) => { posted = body },
    })
    expect(posted).toMatchObject({ capture: 'attached', endpoint_name: ATTACH_ENDPOINT_NAME, rum_enabled: false })
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
    fireEvent.click(screen.getByRole('button', { name: 'Attach alongside the existing install' }))
    expect(s.acceptAttach).toHaveBeenCalledOnce()
  })

  it('explains a missing capture VCL instead of offering to attach', () => {
    render(<ServiceStep s={state({ attachOffer: { serviceId: 'svc-1', available: false, reason: 'capture_vcl_missing' } })} />)
    expect(screen.getByRole('alert')).toHaveTextContent(/capture VCL snippets are missing/)
    expect(screen.queryByRole('button', { name: /Attach alongside/ })).toBeNull()
  })

  it('disables the RUM feature choices once attached', () => {
    render(<FeaturesStep s={state({ config: applyAttach(INITIAL_CONFIG) })} />)
    const buttons = screen.getAllByRole('button')
    expect(buttons.filter((b) => b.hasAttribute('disabled'))).toHaveLength(2)
  })
})
