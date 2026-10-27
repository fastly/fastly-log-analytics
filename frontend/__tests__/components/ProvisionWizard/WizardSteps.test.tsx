/**
 * @vitest-environment jsdom
 *
 * Consolidated ProvisionWizard component tests: TerraformStep,
 * ModeStep / step ordering / ConnectStep, WizardFooter, RumVersionPicker, and ResumeBanner.
 */
import * as React from 'react'
import {
  FULL_CONFIG, without, createTestQueryClient, renderWithQuery, createUseSSEMock,
  getOk, stubMatchMedia, stubPointerCaptureAndScroll, makeQueryWrapper,
} from '@/__tests__/helpers/page-smoke'
import * as apiLib from '@/lib/api'
import { toggleLogFieldGroup, toggleLogFieldOverride, toggleLogFieldPreset, updateLogFieldLimit, getApiBase } from '@/lib/api'
import { act, fireEvent, render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { afterEach, beforeEach, describe, expect, it, test, vi } from 'vitest'
import { http, HttpResponse } from 'msw'
import { axe } from 'vitest-axe'
import { DeleteDataDialog, TeardownDialog } from '@/components/TeardownDialog'
import { LogSettingsModal } from '@/components/LogSettingsModal/LogSettingsModal'
import { CustomFieldDrawer, CustomFieldsManager } from '@/components/CustomFields/CustomFieldDrawer'
import type { ServiceConfig } from '@/types/api'
import type { components } from '@/types/api.generated'
import {
  ProvisionWizard, TerraformStep, ModeStep, ResumeBanner, WizardFooter,
  INITIAL_CONFIG, WIZARD_DRAFT_VERSION, getStepsForMode,
  type LogFieldsCatalog, type WizardDraft, type WizardState,
  ConnectStep,
} from '@/components/ProvisionWizard/ProvisionWizard'
import { RumVersionPicker } from '@/components/ProvisionWizard/steps/StorageStep'
import {
  WIZARD_DRAFT_KEY, buildHandleModalClose, buildValidateOnSuccess, clearDraft, loadDraft,
  mergePersistedConfig, runAdminIngest, runCheckConfig, runCheckDomain,
  runCheckFos, runDeploy, runFetchTerraformPreview, saveDraft, stripSecretsFromConfig,
  validateMutationFn,
} from '@/components/ProvisionWizard/useWizardState'
import { useServiceStore } from '@/stores/filterStore'
import { server } from '@/__tests__/helpers/msw/handlers'

vi.mock('@tanstack/react-virtual', async () => (await import('../../helpers/page-smoke')).reactVirtualModuleMock())

vi.mock('@monaco-editor/react', () => ({
  default: ({ value, onChange }: { value?: string; onChange: (next: string) => void }) => (
    <textarea
      data-testid="mock-monaco"
      value={value}
      onChange={(e) => onChange(e.target.value)}
    />
  ),
}))

stubMatchMedia()
stubPointerCaptureAndScroll()
window.HTMLElement.prototype.scrollIntoView = vi.fn()

vi.mock('@/components/ui/scroll-area', () => ({
  ScrollArea: ({ children }: { children: React.ReactNode }) => <div>{children}</div>,
}))

vi.mock('@/stores/filterStore', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/stores/filterStore')>()),
  useTimezoneStore: () => ({ timezone: 'UTC' }),
}))

const { state: sseState, start: mockStart, stop: mockStop, resetState } = createUseSSEMock()
vi.mock('@/hooks/useSSE', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/hooks/useSSE')>()), useSSE: () => sseState,
}))

const API_BASE = getApiBase()
const SVC = 'svc-new-1'

afterEach(() => {
  server.resetHandlers()
  vi.useRealTimers()
  vi.restoreAllMocks()
})

describe('TerraformStep', () => {
  const tfState = {
    terraformFiles: { 'main.tf': 'resource "x" "y" {}' }, selectedTfFile: 'main.tf',
    setSelectedTfFile: vi.fn(), isFetchingTerraform: false, handleExportTerraform: vi.fn(),
  } as unknown as WizardState

  test('copy button writes the selected file to the clipboard', async () => {
    const writeText = vi.fn().mockResolvedValue(undefined)
    Object.defineProperty(navigator, 'clipboard', { value: { writeText }, configurable: true })
    render(<TerraformStep s={tfState} />)
    await act(async () => { fireEvent.click(screen.getByRole('button', { name: 'Copy main.tf to clipboard' })) })
    expect(writeText).toHaveBeenCalledWith('resource "x" "y" {}')
  })

  test('a rejected clipboard write is swallowed instead of surfacing as an unhandled rejection', async () => {
    const writeText = vi.fn().mockRejectedValue(new Error('denied'))
    Object.defineProperty(navigator, 'clipboard', { value: { writeText }, configurable: true })
    render(<TerraformStep s={tfState} />)
    await act(async () => { fireEvent.click(screen.getByRole('button', { name: 'Copy main.tf to clipboard' })) })
    await new Promise((r) => setTimeout(r, 0))
    expect(writeText).toHaveBeenCalledTimes(1)
  })
})

function makeConnectState(overrides: Partial<WizardState> = {}): WizardState {
  return {
    step: 'connect', setStep: vi.fn(), setMode: vi.fn(), mode: 'ingest', setupPhase: 'form',
    config: INITIAL_CONFIG, setConfig: vi.fn(),
    lines: [], status: 'idle', sseError: null,
    handleCheckFos: vi.fn(), fosStatus: 'idle', fosError: '', selectedService: null, selectedCdnService: null,
    setSelectedService: vi.fn(), setSelectedCdnService: vi.fn(),
    servicesData: [], configStatus: null, isCheckingConfig: false, handleCheckConfig: vi.fn(),
    ...overrides,
  } as WizardState
}

describe('ProvisionWizard modes and ingest connect step', () => {
  it('offers exactly the two admin modes and no analyst join', () => {
    render(<ModeStep s={makeConnectState({ step: 'mode', mode: null })} />)
    expect(screen.getAllByRole('button')).toHaveLength(2)
    expect(screen.getByRole('button', { name: /admin: provision/i })).toBeEnabled()
    expect(screen.getByRole('button', { name: /admin: connect terraform/i })).toBeEnabled()
    expect(screen.queryByRole('button', { name: /analyst/i })).not.toBeInTheDocument()
  })

  it.each([
    ['provision', 'Admin: Provision'],
    ['ingest', 'Admin: Connect Terraform'],
  ] as const)('selecting %s starts at the token step', async (mode, label) => {
    const s = makeConnectState({ step: 'mode', mode: null })
    render(<ModeStep s={s} />)
    await userEvent.click(screen.getByRole('button', { name: new RegExp(label, 'i') }))
    expect(s.setMode).toHaveBeenCalledWith(mode)
    expect(s.setStep).toHaveBeenCalledWith('token')
  })

  it('renders the ingest connect form without DuckLake or analyst-invite inputs', () => {
    render(<ConnectStep s={makeConnectState()} />)
    expect(screen.getByText(/connect to existing storage/i)).toBeVisible()
    expect(screen.getByText('Logging Service', { selector: 'label, span, div' })).toBeVisible()
    expect(screen.queryByText(/metadata location/i)).not.toBeInTheDocument()
    expect(screen.queryByText(/import config from admin/i)).not.toBeInTheDocument()
    expect(screen.queryByText(/read-only mode/i)).not.toBeInTheDocument()
  })

  it('shows the completion panel once setup is done', () => {
    render(<ConnectStep s={makeConnectState({ setupPhase: 'done' })} />)
    expect(screen.getByText(/setup complete/i)).toBeVisible()
    expect(screen.queryByText(/connect to existing storage/i)).not.toBeInTheDocument()
  })
})

describe('getStepsForMode', () => {
  const ids = (mode: Parameters<typeof getStepsForMode>[0], cfg?: { logging_enabled?: boolean }) =>
    getStepsForMode(mode, cfg).map((x) => x.id)

  it('provision mode skips the connect step', () => {
    expect(ids('provision')).toEqual(['mode', 'token', 'service', 'features', 'storage', 'ngwaf', 'fields', 'execute'])
  })

  it('ingest mode inserts connect between service and features', () => {
    expect(ids('ingest')).toEqual(['mode', 'token', 'service', 'connect', 'features', 'storage', 'ngwaf', 'fields', 'execute'])
  })

  it('drops the NGWAF and log-field steps when logging is disabled', () => {
    expect(ids('ingest', { logging_enabled: false })).toEqual(['mode', 'token', 'service', 'connect', 'features', 'storage', 'execute'])
    expect(ids('provision', { logging_enabled: false })).toEqual(['mode', 'token', 'service', 'features', 'storage', 'execute'])
  })

  it('never contains the removed analyst-join steps', () => {
    for (const mode of ['provision', 'ingest', null] as const) {
      for (const removed of ['join', 'analyze', 'settings', 'confirm']) expect(ids(mode)).not.toContain(removed)
    }
  })
})

describe('WizardFooter — ingest connect step', () => {
  it('Continue stays disabled until storage access is verified and both services are chosen', async () => {
    const setStep = vi.fn()
    const { rerender } = render(<WizardFooter s={makeFooterState({ step: 'connect', mode: 'ingest', setStep, setupPhase: 'form', fosStatus: 'idle', config: { endpoint_name: 'EP', cdn_service_name: 'cdn' } as never })} />)
    expect(screen.getByRole('button', { name: /^continue$/i })).toBeDisabled()
    rerender(<WizardFooter s={makeFooterState({ step: 'connect', mode: 'ingest', setStep, setupPhase: 'form', fosStatus: 'success', config: { endpoint_name: 'EP', cdn_service_name: '' } as never })} />)
    expect(screen.getByRole('button', { name: /^continue$/i })).toBeDisabled()
    rerender(<WizardFooter s={makeFooterState({ step: 'connect', mode: 'ingest', setStep, setupPhase: 'form', fosStatus: 'success', config: { endpoint_name: 'EP', cdn_service_name: 'cdn' } as never })} />)
    await userEvent.click(screen.getByRole('button', { name: /^continue$/i }))
    expect(setStep).toHaveBeenCalledWith('features')
  })

  it('Back from connect returns to the service step; Back from features (ingest) returns to connect', async () => {
    const setStep = vi.fn()
    const { unmount } = render(<WizardFooter s={makeFooterState({ step: 'connect', mode: 'ingest', setStep, setupPhase: 'form' })} />)
    await userEvent.click(screen.getByRole('button', { name: /back/i }))
    expect(setStep).toHaveBeenCalledWith('service')
    unmount()
    const setStep2 = vi.fn()
    render(<WizardFooter s={makeFooterState({ step: 'features', mode: 'ingest', setStep: setStep2, setupPhase: 'form' })} />)
    await userEvent.click(screen.getByRole('button', { name: /back/i }))
    expect(setStep2).toHaveBeenCalledWith('connect')
  })

  it('execute step in ingest mode finishes with Complete Setup (handleAdminIngest)', async () => {
    const handleAdminIngest = vi.fn()
    render(<WizardFooter s={makeFooterState({ step: 'execute', mode: 'ingest', handleAdminIngest })} />)
    await userEvent.click(screen.getByRole('button', { name: /complete setup/i }))
    expect(handleAdminIngest).toHaveBeenCalled()
  })
})

function makeFooterState(overrides: Partial<WizardState> = {}): WizardState {
  return {
    step: 'terraform', setStep: vi.fn(), mode: 'provision', isDeploying: false, status: 'idle', isDone: false,
    handleModalClose: vi.fn(), token: 'tok', isLoadingServices: false, handleTokenSubmit: vi.fn(),
    selectedService: { id: 'svc', name: 'Svc' }, validateMutation: { isPending: false }, domainStatus: 'available',
    config: { fos_bucket_name: 'bucket', ngwaf_workspace_id: '' }, fetchTerraformPreview: vi.fn(),
    handleDeploy: vi.fn(), handleAdminIngest: vi.fn(), fosStatus: 'idle', setupPhase: 'form', stop: vi.fn(),
    ...overrides,
  } as WizardState
}

describe('WizardFooter — terraform (Terraform & VCL preview) step', () => {
  it("provision and ingest modes: 'Back to Review' returns to Review without deploying or ingesting", async () => {
    for (const mode of ['provision', 'ingest'] as const) {
      const setStep = vi.fn(), handleDeploy = vi.fn(), handleAdminIngest = vi.fn()
      const { unmount } = render(<WizardFooter s={makeFooterState({ mode, setStep, handleDeploy, handleAdminIngest })} />)
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

const renderPicker = (onChange: (v: string | null) => void, value: string | null = null) =>
  renderWithQuery(<RumVersionPicker serviceId={SVC} value={value} onChange={onChange} />)

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
        HttpResponse.json({ available: ['1.9.0', '1.8.0', '1.7.0'], current: null, latest: '1.9.0', update_available: false }),
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
        HttpResponse.json({ error: 'faro_registry_unavailable', message: 'registry down' }, { status: 503 }),
      ),
    )
    const { unmount } = renderPicker(() => { throw new Error('onChange must not fire on a degraded fetch') })
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
    renderPicker(() => { throw new Error('onChange must not fire when there is nothing to default to') })
    expect(await screen.findByText(/no versions available/i)).toBeInTheDocument()
    expect(screen.queryByRole('combobox')).not.toBeInTheDocument()
  })

  it('retry re-fires the request after a registry outage clears', async () => {
    let calls = 0
    server.use(
      http.get(`${API_BASE}/api/services/:service_id/rum/versions`, () => {
        calls += 1
        if (calls === 1) return HttpResponse.json({ error: 'faro_registry_unavailable' }, { status: 503 })
        return HttpResponse.json({ available: ['2.0.0'], current: null, latest: '2.0.0', update_available: false })
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

const draftQueryClient = createTestQueryClient()

function makeDraft(overrides: Partial<WizardDraft> = {}): WizardDraft {
  return {
    version: WIZARD_DRAFT_VERSION, draftId: 'draft-1', mode: 'provision', step: 'fields', currentStep: 'fields',
    selectedServiceId: 'svc-1', selectedServiceName: 'My Service', selectedCdnServiceId: null, selectedCdnServiceName: null,
    tokenInfo: { id: 'tok-1', name: 'tok', type: 'user' },
    config: stripSecretsFromConfig({
      ...INITIAL_CONFIG, endpoint_name: 'My EP', fos_bucket_name: 'b',
      fos_access_key: 'AK-LEAK', fos_secret_key: 'SK-LEAK', cdn_secret: 'CDN-LEAK',
    }),
    updatedAt: new Date().toISOString(), createdAt: new Date().toISOString(),
    ...overrides,
  }
}

describe('ResumeBanner', () => {
  beforeEach(() => window.localStorage.removeItem(WIZARD_DRAFT_KEY))
  afterEach(() => window.localStorage.removeItem(WIZARD_DRAFT_KEY))

  it('renders the step label from the saved draft and fires the action callbacks', async () => {
    const user = userEvent.setup()
    const onResume = vi.fn(), onStartFresh = vi.fn()
    render(<ResumeBanner draft={makeDraft({ currentStep: 'fields' })} onResume={onResume} onStartFresh={onStartFresh} />)
    expect(screen.getByText(/Resume previous wizard/i)).toBeInTheDocument()
    expect(screen.getByTestId('resume-banner-resume')).toHaveTextContent(/Resume from Log Fields/i)
    await user.click(screen.getByTestId('resume-banner-resume'))
    expect(onResume).toHaveBeenCalledOnce()
    await user.click(screen.getByTestId('resume-banner-start-fresh'))
    expect(onStartFresh).toHaveBeenCalledOnce()
  })

  it('hydrates the saved draft from localStorage when the wizard opens', () => {
    saveDraft(makeDraft({ currentStep: 'fields' }))
    renderWithQuery(<ProvisionWizard open={true} onOpenChange={vi.fn()} />, { queryClient: draftQueryClient })
    expect(screen.getByText(/Resume previous wizard/i)).toBeInTheDocument()
  })
})

const buildToggleGroup = (catalog: LogFieldsCatalog | null | undefined) => (
  prev: typeof INITIAL_CONFIG, groupId: string, checked: boolean,
) => ({ ...prev, log_fields: toggleLogFieldGroup(prev.log_fields, catalog, groupId, checked) })

const applyToggleField = (
  prev: typeof INITIAL_CONFIG, fieldId: string, checked: boolean, defaultEnabledByGroup: boolean,
) => ({ ...prev, log_fields: toggleLogFieldOverride(prev.log_fields, fieldId, checked, defaultEnabledByGroup) })

const applyUpdateFieldLimit = (
  prev: typeof INITIAL_CONFIG, fieldId: string, limit?: number,
) => ({ ...prev, log_fields: updateLogFieldLimit(prev.log_fields, fieldId, limit) })

const buildTogglePreset = (
  catalog: LogFieldsCatalog | null | undefined, isPresetActive: (groups: string[]) => boolean,
) => (prev: typeof INITIAL_CONFIG, presetGroups: string[]) => ({
  ...prev, log_fields: toggleLogFieldPreset(prev.log_fields, catalog, presetGroups, isPresetActive),
})

const group = (id: string, label: string, requires: string | null = null) => ({
  id, label, description: '', locked: false, requires, note: null, recommended_with: null, total_bytes: 0, fields: [],
})

const PRESETS = {
  minimal: { label: 'Minimal', description: '', groups: ['core'] },
  standard: { label: 'Standard', description: '', groups: ['core', 'http'] },
  full: { label: 'Full', description: '', groups: ['core', 'http', 'tls', 'security'] },
}

const CATALOG: LogFieldsCatalog = {
  groups: [
    group('core', 'Core'), group('http', 'HTTP', 'core'), group('tls', 'TLS', 'core'),
    group('security', 'Security', 'http'), group('detail', 'Detail', 'http'),
  ],
  presets: PRESETS,
}

const configWithGroups = (groups: string[]) => ({ ...INITIAL_CONFIG, log_fields: { ...INITIAL_CONFIG.log_fields, groups } })

describe('buildToggleGroup', () => {
  it('adds a group with no dependencies and auto-adds required ancestor groups', () => {
    const toggle = buildToggleGroup(CATALOG)
    expect(toggle(configWithGroups([]), 'core', true).log_fields.groups).toEqual(['core'])
    expect(new Set(toggle(configWithGroups([]), 'security', true).log_fields.groups)).toEqual(new Set(['security', 'http', 'core']))
    expect(new Set(toggle(configWithGroups(['http']), 'security', true).log_fields.groups)).toEqual(new Set(['http', 'core', 'security']))
  })

  it('removes a group without touching dependencies of others and is idempotent', () => {
    const toggle = buildToggleGroup(CATALOG)
    const next = toggle(configWithGroups(['core', 'http', 'security']), 'security', false)
    expect(new Set(next.log_fields.groups)).toEqual(new Set(['core', 'http']))
    expect(toggle(configWithGroups(['core']), 'core', true).log_fields.groups).toEqual(['core'])
  })

  it('does not mutate input config and tolerates null catalog or missing groups', () => {
    const toggle = buildToggleGroup(CATALOG)
    const input = configWithGroups(['core'])
    const inputGroupsRef = input.log_fields.groups
    toggle(input, 'http', true)
    expect(input.log_fields.groups).toBe(inputGroupsRef)
    expect(buildToggleGroup(null)(configWithGroups([]), 'core', true).log_fields.groups).toEqual(['core'])
    const config: typeof INITIAL_CONFIG = {
      ...INITIAL_CONFIG, log_fields: without(INITIAL_CONFIG.log_fields, 'groups') as typeof INITIAL_CONFIG.log_fields,
    }
    expect(toggle(config, 'core', true).log_fields.groups).toEqual(['core'])
  })
})

describe('applyToggleField & applyUpdateFieldLimit', () => {
  it('sets or clears field overrides based on group default', () => {
    const c1 = { ...INITIAL_CONFIG, log_fields: { ...INITIAL_CONFIG.log_fields, field_overrides: {} } }
    expect(applyToggleField(c1, 'cookie_id', true, false).log_fields.field_overrides).toEqual({ cookie_id: true })
    const c2 = { ...INITIAL_CONFIG, log_fields: { ...INITIAL_CONFIG.log_fields, field_overrides: { cookie_id: true, ip: false } } }
    expect(applyToggleField(c2, 'cookie_id', true, true).log_fields.field_overrides).toEqual({ ip: false })
    const c3 = { ...INITIAL_CONFIG, log_fields: { ...without(INITIAL_CONFIG.log_fields, 'field_overrides') } }
    expect(applyToggleField(c3, 'cookie_id', false, true).log_fields.field_overrides).toEqual({ cookie_id: false })
  })

  it('sets, overwrites, and removes numeric field limits', () => {
    const c1 = { ...INITIAL_CONFIG, log_fields: { ...INITIAL_CONFIG.log_fields, field_limits: {} } }
    expect(applyUpdateFieldLimit(c1, 'url', 2048).log_fields.field_limits).toEqual({ url: 2048 })
    const c2 = { ...INITIAL_CONFIG, log_fields: { ...INITIAL_CONFIG.log_fields, field_limits: { url: 2048, ua: 512 } } }
    expect(applyUpdateFieldLimit(c2, 'url', undefined).log_fields.field_limits).toEqual({ ua: 512 })
    expect(applyUpdateFieldLimit(c2, 'url', 1024).log_fields.field_limits).toEqual({ url: 1024, ua: 512 })
    const c3 = { ...INITIAL_CONFIG, log_fields: { ...without(INITIAL_CONFIG.log_fields, 'field_limits') } }
    expect(applyUpdateFieldLimit(c3, 'url', 4096).log_fields.field_limits).toEqual({ url: 4096 })
  })
})

describe('buildTogglePreset', () => {
  const isPresetActiveFor = (groups: string[]) => {
    const set = new Set(groups)
    return (presetGroups: string[]) => presetGroups.every((g) => set.has(g))
  }

  it('adds preset groups, removes unprotected groups on toggle-off, and cascades dependents', () => {
    const toggle = buildTogglePreset(CATALOG, isPresetActiveFor([]))
    expect(new Set(toggle(configWithGroups([]), PRESETS.full.groups).log_fields.groups)).toEqual(new Set(['core', 'http', 'tls', 'security']))
    const isolatedCatalog = { ...CATALOG, presets: { standard: PRESETS.standard } }
    const startingGroups = ['core', 'http', 'detail']
    const toggleOff = buildTogglePreset(isolatedCatalog, isPresetActiveFor(startingGroups))
    expect(toggleOff(configWithGroups(startingGroups), PRESETS.standard.groups).log_fields.groups).toEqual([])
  })

  it('keeps groups protected by other active presets and tolerates missing/null catalog', () => {
    const startingStd = ['core', 'http']
    expect(buildTogglePreset(CATALOG, isPresetActiveFor(startingStd))(configWithGroups(startingStd), PRESETS.standard.groups).log_fields.groups).toEqual(['core'])
    const startingFull = ['core', 'http', 'tls', 'security']
    const toggleFull = buildTogglePreset(CATALOG, isPresetActiveFor(startingFull))
    expect(new Set(toggleFull(configWithGroups(startingFull), PRESETS.standard.groups).log_fields.groups)).toEqual(new Set(['core', 'http', 'tls', 'security']))
    expect(buildTogglePreset({ groups: CATALOG.groups }, isPresetActiveFor([]))(configWithGroups([]), ['core']).log_fields.groups).toEqual(['core'])
    expect(buildTogglePreset(null, isPresetActiveFor([]))(configWithGroups([]), ['core']).log_fields.groups).toEqual(['core'])
  })
})

describe('wizard-draft persistence', () => {
  beforeEach(() => window.localStorage.clear())
  afterEach(() => window.localStorage.clear())

  it('strips secrets and mergePersistedConfig always returns blank secret fields', () => {
    const stripped = stripSecretsFromConfig({
      ...INITIAL_CONFIG, endpoint_name: 'My EP', sample_rate: 50,
      fos_access_key: 'AK', fos_secret_key: 'SK', cdn_secret: 'CDN',
    })
    for (const k of ['fos_access_key', 'fos_secret_key', 'cdn_secret']) expect(stripped).not.toHaveProperty(k)
    expect(stripped).toMatchObject({ endpoint_name: 'My EP', sample_rate: 50 })

    const leaked = {
      ...stripSecretsFromConfig(INITIAL_CONFIG), fos_access_key: 'LEAK', fos_secret_key: 'LEAK', cdn_secret: 'LEAK',
    } as unknown as Parameters<typeof mergePersistedConfig>[0]
    const merged = mergePersistedConfig(leaked)
    expect(merged.fos_access_key).toBe('')
    expect(merged.fos_secret_key).toBe('')
    expect(merged.cdn_secret).toBe('')
  })

  it('roundtrips a non-secret draft with zero secret keys or values in localStorage', () => {
    saveDraft(makeDraft())
    const loaded = loadDraft()
    expect(loaded).not.toBeNull()
    expect(loaded!.draftId).toBe('draft-1')
    expect(loaded!.config.endpoint_name).toBe('My EP')
    const raw = window.localStorage.getItem(WIZARD_DRAFT_KEY)!
    for (const s of ['AK-LEAK', 'SK-LEAK', 'CDN-LEAK']) expect(raw).not.toContain(s)
  })

  it('discards a v1 draft, including one left under the legacy v1 key', () => {
    expect(WIZARD_DRAFT_VERSION).toBe(2)
    window.localStorage.setItem(WIZARD_DRAFT_KEY, JSON.stringify({ ...makeDraft(), version: 1 }))
    expect(loadDraft()).toBeNull()
    expect(window.localStorage.getItem(WIZARD_DRAFT_KEY)).toBeNull()

    window.localStorage.setItem('provision-wizard-draft-v1', JSON.stringify({ ...makeDraft(), version: 1, mode: 'join' }))
    expect(loadDraft()).toBeNull()
    expect(window.localStorage.getItem('provision-wizard-draft-v1')).toBeNull()
  })

  it('rejects a draft whose mode is the removed analyst join mode', () => {
    window.localStorage.setItem(WIZARD_DRAFT_KEY, JSON.stringify({ ...makeDraft(), mode: 'join' }))
    expect(loadDraft()).toBeNull()
  })

  it('persists no analyst-import fields', () => {
    saveDraft(makeDraft())
    const raw = window.localStorage.getItem(WIZARD_DRAFT_KEY)!
    for (const k of ['importMode', 'importRange', 'syncEnabled', 'syncIntervalMins']) expect(raw).not.toContain(k)
  })

  it('handles version mismatch, mode step, invalid JSON, clearDraft, and broken localStorage', () => {
    saveDraft(makeDraft({ version: WIZARD_DRAFT_VERSION + 1 }))
    expect(loadDraft()).toBeNull()
    expect(window.localStorage.getItem(WIZARD_DRAFT_KEY)).toBeNull()

    saveDraft(makeDraft({ step: 'mode', currentStep: 'mode' }))
    expect(loadDraft()).toBeNull()

    window.localStorage.setItem(WIZARD_DRAFT_KEY, '{not json')
    expect(loadDraft()).toBeNull()

    clearDraft()
    expect(loadDraft()).toBeNull()

    const origSet = window.localStorage.setItem
    window.localStorage.setItem = () => { throw new Error('quota') }
    try { expect(() => saveDraft(makeDraft())).not.toThrow() } finally { window.localStorage.setItem = origSet }
  })
})

describe('validateMutationFn', () => {
  it('POSTs to /api/provision/validate and returns the data body', async () => {
    let bodyReceived: unknown = null
    server.use(
      http.post(`${API_BASE}/api/provision/validate`, async ({ request }) => {
        bodyReceived = await request.json()
        return HttpResponse.json({ token_info: { id: 't1', name: 'me', type: 'user' } })
      }),
    )
    const result = await validateMutationFn('TOKEN_XYZ')('service-123')
    expect(bodyReceived).toMatchObject({ token: 'TOKEN_XYZ', service_id: 'service-123' })
    expect(result).toEqual({ token_info: { id: 't1', name: 'me', type: 'user' } })
  })

  it('fresh install (no active service): the validate call is NOT aborted', async () => {
    const prev = useServiceStore.getState().activeServiceId
    ;(useServiceStore.getState() as { activeServiceId: string | null }).activeServiceId = null
    try {
      let reached = false
      server.use(
        http.post(`${API_BASE}/api/provision/validate`, async () => {
          reached = true
          return HttpResponse.json({ token_info: { id: 't1', name: 'me', type: 'user' } })
        }),
      )
      const result = await validateMutationFn('TOKEN_XYZ')('service-123')
      expect(reached).toBe(true)
      expect(result).toEqual({ token_info: { id: 't1', name: 'me', type: 'user' } })
    } finally {
      ;(useServiceStore.getState() as { activeServiceId: string | null }).activeServiceId = prev
    }
  })
})

describe('buildValidateOnSuccess', () => {
  const makeDeps = (mode: 'provision' | 'ingest' = 'provision') => ({
    token: 'TOK', mode, setTokenInfo: vi.fn(), setConfig: vi.fn(), setStep: vi.fn(),
  })

  it('writes token_info via setTokenInfo when present and skips when absent', () => {
    const deps = makeDeps()
    buildValidateOnSuccess(deps)({ token_info: { id: 't1', name: 'me', type: 'user' } })
    expect(deps.setTokenInfo).toHaveBeenCalledWith({ id: 't1', name: 'me', type: 'user' })
    const emptyDeps = makeDeps()
    buildValidateOnSuccess(emptyDeps)({})
    expect(emptyDeps.setTokenInfo).not.toHaveBeenCalled()
    expect(emptyDeps.setConfig).not.toHaveBeenCalled()
    expect(emptyDeps.setStep).toHaveBeenCalled()
  })

  it('applies defaults via setConfig and honours fallback vs explicit values', () => {
    const deps = makeDeps()
    buildValidateOnSuccess(deps)({ service_name: 'my-svc', defaults: { fos_bucket_name: 'MyBucket-Logs-Prod', fos_prefix: 'logs/' } })
    expect(deps.setConfig.mock.calls[0][0](INITIAL_CONFIG)).toMatchObject({
      endpoint_name: 'Fastly Object Storage Logs', fos_region: 'us-east-1',
      fos_bucket_name: 'mybucket-logs-prod', cdn_service_name: 'my-svc (CDN)', cdn_prefix: 'fos-mybucket-logs',
    })

    const explicitDeps = makeDeps()
    buildValidateOnSuccess(explicitDeps)({
      service_name: 'svc',
      defaults: { endpoint_name: 'Custom EP', fos_region: 'us-west-2', fos_bucket_name: 'override', cdn_service_name: 'explicit cdn', cdn_prefix: 'EXPLICIT' },
    })
    expect(explicitDeps.setConfig.mock.calls[0][0](INITIAL_CONFIG)).toMatchObject({
      endpoint_name: 'Custom EP', fos_region: 'us-west-2', cdn_service_name: 'explicit cdn', cdn_prefix: 'explicit',
    })
  })

  it.each([
    ['ingest' as const, 'connect'],
    ['provision' as const, 'features'],
  ])('routes %s mode to the %s step', (mode, expectedStep) => {
    const deps = makeDeps(mode)
    buildValidateOnSuccess(deps)({})
    expect(deps.setStep).toHaveBeenCalledWith(expectedStep)
  })
})

describe('runCheckConfig', () => {
  const makeArgs = (overrides: Partial<Parameters<typeof runCheckConfig>[0]> = {}): Parameters<typeof runCheckConfig>[0] => ({
    token: 'TOK', selectedService: { id: 's1', name: 'svc', provisioned: false },
    selectedCdnService: { id: 'c1', name: 'cdn', provisioned: false }, config: { ...INITIAL_CONFIG, fos_bucket_name: 'b' },
    setIsCheckingConfig: vi.fn(), setConfigStatus: vi.fn(), ...overrides,
  })

  it('returns early when prerequisites are missing, and updates status when present', async () => {
    const missing = makeArgs({ selectedService: null })
    await runCheckConfig(missing)
    expect(missing.setIsCheckingConfig).not.toHaveBeenCalled()

    const body = { logging_service: { ok: true, details: 'fine' }, cdn_service: { ok: true, details: 'fine' } }
    server.use(http.get(`${API_BASE}/api/provision/check-config`, () => HttpResponse.json(body)))
    const args = makeArgs()
    await runCheckConfig(args)
    expect(args.setIsCheckingConfig).toHaveBeenNthCalledWith(1, true)
    expect(args.setConfigStatus).toHaveBeenCalledWith(body)
    expect(args.setIsCheckingConfig).toHaveBeenLastCalledWith(false)
  })
})

describe('runCheckFos', () => {
  const makeArgs = (overrides: Partial<Parameters<typeof runCheckFos>[0]> = {}): Parameters<typeof runCheckFos>[0] => ({
    vals: { bucket: 'b', region: 'us-east-1', access_key: 'a', secret_key: 's' },
    config: INITIAL_CONFIG, setFosStatus: vi.fn(), setFosError: vi.fn(), ...overrides,
  })

  it('does nothing when any credential is missing', async () => {
    const args = makeArgs({ vals: { bucket: 'b' } })
    await runCheckFos(args)
    expect(args.setFosStatus).not.toHaveBeenCalled()
  })

  it.each([
    ['ok:true', () => HttpResponse.json({ ok: true }), 'success', ''],
    ['ok:false', () => HttpResponse.json({ ok: false, error: 'no creds' }), 'error', 'no creds'],
    ['network error', () => HttpResponse.error(), 'error', null],
  ])('sets status on %s', async (_label, respFn, expectedStatus, expectedErr) => {
    server.use(http.post(`${API_BASE}/api/provision/check-fos`, respFn))
    const args = makeArgs()
    await runCheckFos(args)
    expect(args.setFosStatus).toHaveBeenCalledWith('checking')
    expect(args.setFosStatus).toHaveBeenLastCalledWith(expectedStatus)
    if (expectedErr !== null) expect(args.setFosError).toHaveBeenCalledWith(expectedErr)
    else expect(args.setFosError).toHaveBeenCalled()
  })

  it('falls back to config values when vals is not supplied', async () => {
    let received: unknown = null
    server.use(
      http.post(`${API_BASE}/api/provision/check-fos`, async ({ request }) => {
        received = await request.json()
        return HttpResponse.json({ ok: true })
      }),
    )
    const args = makeArgs({
      vals: undefined,
      config: { ...INITIAL_CONFIG, fos_bucket_name: 'cfg-bucket', fos_region: 'cfg-region', fos_access_key: 'cfg-ak', fos_secret_key: 'cfg-sk' },
    })
    await runCheckFos(args)
    expect(received).toMatchObject({ bucket: 'cfg-bucket', region: 'cfg-region', access_key: 'cfg-ak', secret_key: 'cfg-sk' })
  })
})

describe('runCheckDomain', () => {
  const makeArgs = (overrides: Partial<Parameters<typeof runCheckDomain>[0]> = {}): Parameters<typeof runCheckDomain>[0] => ({
    prefix: 'myco', setDomainStatus: vi.fn(), setDomainMessage: vi.fn(), ...overrides,
  })

  it.each(['no', ''])('returns early for short/empty prefix (%j)', async (prefix) => {
    const args = makeArgs({ prefix })
    await runCheckDomain(args)
    expect(args.setDomainStatus).not.toHaveBeenCalled()
  })

  it.each([
    ['available', () => HttpResponse.json({ available: true }), 'available', 'Domain available!'],
    ['taken', () => HttpResponse.json({ available: false }), 'taken', 'This domain prefix is already in use.'],
    ['error', () => HttpResponse.error(), 'error', null],
  ])('handles %s domain response', async (_label, respFn, expectedStatus, expectedMsg) => {
    server.use(http.get(`${API_BASE}/api/provision/check-domain`, respFn))
    const args = makeArgs()
    await runCheckDomain(args)
    expect(args.setDomainStatus).toHaveBeenNthCalledWith(1, 'checking')
    expect(args.setDomainStatus).toHaveBeenLastCalledWith(expectedStatus)
    if (expectedMsg) expect(args.setDomainMessage).toHaveBeenCalledWith(expectedMsg)
  })
})

describe('runFetchTerraformPreview', () => {
  const makeArgs = (overrides: Partial<Parameters<typeof runFetchTerraformPreview>[0]> = {}): Parameters<typeof runFetchTerraformPreview>[0] => ({
    token: 'TOK', selectedService: { id: 'svc-1', name: 'svc', provisioned: false }, config: FULL_CONFIG,
    setIsFetchingTerraform: vi.fn(), setTerraformFiles: vi.fn(), setSelectedTfFile: vi.fn(), ...overrides,
  })

  it('returns early when no service is selected', async () => {
    const args = makeArgs({ selectedService: null })
    await runFetchTerraformPreview(args)
    expect(args.setIsFetchingTerraform).not.toHaveBeenCalled()
  })

  it.each([
    ['selects main.tf when present', { 'main.tf': 'resource "..." {}', 'variables.tf': 'variable "..." {}' }, 'main.tf'],
    ['falls back to first file when main.tf is absent', { 'logging.tf': '...', 'cdn.tf': '...' }, 'logging.tf'],
  ])('%s', async (_label, files, expectedFile) => {
    server.use(http.post(`${API_BASE}/api/provision/terraform/preview`, () => HttpResponse.json(files)))
    const args = makeArgs()
    await runFetchTerraformPreview(args)
    expect(args.setIsFetchingTerraform).toHaveBeenNthCalledWith(1, true)
    expect(args.setTerraformFiles).toHaveBeenCalledWith(files)
    expect(args.setSelectedTfFile).toHaveBeenCalledWith(expectedFile)
    expect(args.setIsFetchingTerraform).toHaveBeenLastCalledWith(false)
  })

  it('still flips loading off if the request errors', async () => {
    const errSpy = vi.spyOn(console, 'error').mockImplementation(() => {})
    server.use(http.post(`${API_BASE}/api/provision/terraform/preview`, () => HttpResponse.error()))
    const args = makeArgs()
    await runFetchTerraformPreview(args)
    expect(args.setIsFetchingTerraform).toHaveBeenLastCalledWith(false)
    expect(args.setTerraformFiles).not.toHaveBeenCalled()
    expect(errSpy).toHaveBeenCalled()
    errSpy.mockRestore()
  })
})

describe('buildHandleModalClose', () => {
  const makeDeps = (overrides: Partial<Parameters<typeof buildHandleModalClose>[0]> = {}) => ({
    status: 'idle', isDone: false, onOpenChange: vi.fn(), selectedService: null, setActiveServiceId: vi.fn(),
    queryClient: { invalidateQueries: vi.fn() }, setStep: vi.fn(), setMode: vi.fn(), setSearch: vi.fn(),
    setSelectedService: vi.fn(), setIsDeploying: vi.fn(), setFosStatus: vi.fn(), setFosError: vi.fn(),
    setSetupPhase: vi.fn(),
    reset: vi.fn(), resetConfig: vi.fn(), setNgwafWorkspaces: vi.fn(), setNgwafFetching: vi.fn(), setNgwafFetchError: vi.fn(),
    ...overrides,
  })

  it('refuses to close while streaming and propagates open=true unchanged', () => {
    const streaming = makeDeps({ status: 'streaming' })
    buildHandleModalClose(streaming)(false)
    expect(streaming.onOpenChange).not.toHaveBeenCalled()
    const opening = makeDeps()
    buildHandleModalClose(opening)(true)
    expect(opening.onOpenChange).toHaveBeenCalledWith(true)
    expect(opening.setStep).not.toHaveBeenCalled()
  })

  it('on close after done/isDone: switches active service + invalidates bootstrap', () => {
    const reload = vi.fn()
    Object.defineProperty(window, 'location', { configurable: true, value: { reload } })
    const deps = makeDeps({ status: 'done', selectedService: { id: 'svc-new', name: 'new', provisioned: false } })
    buildHandleModalClose(deps)(false)
    expect(deps.onOpenChange).toHaveBeenCalledWith(false)
    expect(deps.setActiveServiceId).toHaveBeenCalledWith('svc-new')
    expect(deps.queryClient.invalidateQueries).toHaveBeenCalledWith({ queryKey: ['bootstrap'] })
    expect(reload).toHaveBeenCalled()
  })

  it('on close mid-flight: defers a reset chain via setTimeout(300)', () => {
    vi.useFakeTimers()
    const deps = makeDeps({ status: 'idle' })
    buildHandleModalClose(deps)(false)
    expect(deps.setStep).not.toHaveBeenCalled()
    vi.advanceTimersByTime(300)
    expect(deps.setStep).toHaveBeenCalledWith('mode')
    expect(deps.setMode).toHaveBeenCalledWith(null)
    expect(deps.setFosStatus).toHaveBeenCalledWith('idle')
    expect(deps.setSetupPhase).toHaveBeenCalledWith('form')
    expect(deps.reset).toHaveBeenCalled()
    expect(deps.resetConfig).toHaveBeenCalled()
  })
})

describe('runDeploy', () => {
  const makeArgs = (overrides: Partial<Parameters<typeof runDeploy>[0]> = {}) => ({
    token: 'TOK', selectedService: { id: 'svc-1', name: 'svc', provisioned: false },
    config: FULL_CONFIG, setIsDeploying: vi.fn(), start: vi.fn(), ...overrides,
  })

  it('skips when no service is selected', () => {
    const args = makeArgs({ selectedService: null })
    runDeploy(args)
    expect(args.setIsDeploying).not.toHaveBeenCalled()
    expect(args.start).not.toHaveBeenCalled()
  })

  it('starts the execute SSE with the full deploy body and handles optional config variations', () => {
    const args = makeArgs()
    runDeploy(args)
    expect(args.setIsDeploying).toHaveBeenCalledWith(true)
    const [path, body] = (args.start as ReturnType<typeof vi.fn>).mock.calls[0]
    expect(path).toBe('/api/provision/execute')
    expect(body).toMatchObject({
      token: 'TOK', service_id: 'svc-1', service_name: 'svc', endpoint_name: 'My EP',
      fos_bucket_name: 'my-bucket', sample_rate: '1', log_period: '60', commit_interval_mins: 5,
      cdn_url: 'https://mycdn.global.ssl.fastly.net',
    })

    const noCdn = makeArgs({ config: { ...FULL_CONFIG, cdn_prefix: '', faro_version: '1.9.0', log_fields: { groups: ['core', 'http'] } } })
    runDeploy(noCdn)
    const [, bodyNoCdn] = (noCdn.start as ReturnType<typeof vi.fn>).mock.calls[0]
    expect(bodyNoCdn).not.toHaveProperty('cdn_url')
    expect(bodyNoCdn.faro_version).toBe('1.9.0')
    expect(bodyNoCdn.log_fields).toBe('{"groups":["core","http"]}')

    const nullFields = makeArgs({ config: { ...(without(FULL_CONFIG, 'log_fields') as typeof FULL_CONFIG), faro_version: null } })
    runDeploy(nullFields)
    const [, bodyNull] = (nullFields.start as ReturnType<typeof vi.fn>).mock.calls[0]
    expect(bodyNull.faro_version).toBeNull()
    expect(bodyNull.log_fields).toBeNull()
  })
})

describe('runAdminIngest', () => {
  const makeArgs = (overrides: Partial<Parameters<typeof runAdminIngest>[0]> = {}) => ({
    token: 'TOK', selectedService: { id: 'svc-1', name: 'svc', provisioned: false },
    selectedCdnService: { id: 'cdn-1', name: 'cdn', provisioned: false }, config: FULL_CONFIG,
    services: [{ id: 'svc-other', name: 'other' }],
    setIsDeploying: vi.fn(), setActiveServiceId: vi.fn(),
    setServices: vi.fn(), queryClient: { invalidateQueries: vi.fn() },
    setSetupPhase: vi.fn(), setStep: vi.fn(), ...overrides,
  })

  it('skips when no service is selected', async () => {
    const args = makeArgs({ selectedService: null })
    await runAdminIngest(args)
    expect(args.setIsDeploying).not.toHaveBeenCalled()
  })

  it('on success registers the ingested service (deduping existing) and marks setup done', async () => {
    server.use(http.post(`${API_BASE}/api/provision/ingest`, () => HttpResponse.json({ ok: true })))
    const args = makeArgs()
    await runAdminIngest(args)
    expect(args.setIsDeploying).toHaveBeenNthCalledWith(1, true)
    expect(args.setActiveServiceId).toHaveBeenCalledWith('svc-1')
    expect(args.setServices).toHaveBeenCalledOnce()
    const newServices = (args.setServices as ReturnType<typeof vi.fn>).mock.calls[0][0]
    expect(newServices).toHaveLength(2)
    expect(newServices[1]).toEqual({ id: 'svc-1', name: 'svc' })
    expect(args.queryClient.invalidateQueries).toHaveBeenCalledWith({ queryKey: ['bootstrap'] })
    expect(args.setSetupPhase).toHaveBeenCalledWith('done')
    expect(args.setStep).toHaveBeenCalledWith('connect')
    expect(args.setIsDeploying).toHaveBeenLastCalledWith(false)

    const existingArgs = makeArgs({ services: [{ id: 'svc-1', name: 'svc' }] })
    await runAdminIngest(existingArgs)
    expect(existingArgs.setServices).not.toHaveBeenCalled()
    expect(existingArgs.setSetupPhase).toHaveBeenCalledWith('done')
  })

  it.each([
    ['ok:false', () => HttpResponse.json({ ok: false, error: 'nope' }), false],
    ['network error', () => HttpResponse.error(), true],
  ])('does not advance phase on %s and still flips loading off', async (_label, respFn, logsErr) => {
    const errSpy = vi.spyOn(console, 'error').mockImplementation(() => {})
    server.use(http.post(`${API_BASE}/api/provision/ingest`, respFn))
    const args = makeArgs()
    await runAdminIngest(args)
    expect(args.setSetupPhase).not.toHaveBeenCalled()
    expect(args.setStep).not.toHaveBeenCalled()
    expect(args.setIsDeploying).toHaveBeenLastCalledWith(false)
    if (logsErr) expect(errSpy).toHaveBeenCalled()
    errSpy.mockRestore()
  })
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

  await waitFor(() => expect(screen.getByText('Log Period')).toBeDefined())
  expect(screen.getByText('1. Standard Fields')).toBeDefined()

  const customConditionInput = screen.getByLabelText(/^Optional Log Condition$/i) as HTMLInputElement
  expect(customConditionInput.value).toBe('req.url ~ "test"')

  await user.clear(customConditionInput)
  await user.type(customConditionInput, 'req.url ~ "updated"')
  expect(customConditionInput.value).toBe('req.url ~ "updated"')

  await user.click(screen.getByRole('button', { name: /Next Step/i }))

  await waitFor(() => expect(screen.getByText('2. Custom Fields')).toBeDefined())
  expect(screen.getByText('Define Custom Log Fields')).toBeDefined()

  await user.click(screen.getByRole('button', { name: /Next Step/i }))

  await waitFor(() => expect(screen.getByText('3. Review')).toBeDefined())
  expect(screen.getByText('Review Log Configuration Changes')).toBeDefined()

  expect(screen.getByText('req.url ~ "updated"')).toBeDefined()
  expect(screen.getByText(/Custom Condition:/)).toBeDefined()

  expect(screen.getByText('Deploy to Fastly')).toBeDefined()

  await user.click(screen.getByRole('button', { name: /^Back$/i }))

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

  await waitFor(() => expect(screen.getAllByText('1. Standard Fields').length).toBeGreaterThan(0))
  await user.click(screen.getAllByRole('button', { name: /Next Step/i })[0])

  await waitFor(() => expect(screen.getAllByText('Define Custom Log Fields').length).toBeGreaterThan(0))

  await user.click(screen.getAllByRole('button', { name: /Next Step/i })[0])

  await waitFor(() => expect(screen.getByText('Review Log Configuration Changes')).toBeDefined())

  await waitFor(() => expect(screen.getByText(/Custom Fields \(\d+\)/)).toBeDefined())
  expect(screen.getByText('My Custom Field')).toBeDefined()
  expect(screen.getByText('(x_custom)')).toBeDefined()
})

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

test('LogSettingsModal has no axe-detectable a11y violations', async () => {
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

  vi.spyOn(apiLib.customFieldsApi, 'validateCustomVcl').mockResolvedValue({
    valid: true,
    errors: [],
    warnings: [],
    format_length_limit: 8000,
  })

  vi.spyOn(apiLib.customFieldsApi, 'createCustomField').mockRejectedValueOnce(
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

  vi.spyOn(apiLib.customFieldsApi, 'createCustomField').mockResolvedValueOnce(
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
  vi.spyOn(apiLib.customFieldsApi, 'validateCustomVcl').mockResolvedValue({
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
  vi.spyOn(apiLib.customFieldsApi, 'listCustomFields').mockResolvedValue({
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
    .spyOn(apiLib.customFieldsApi, 'deleteCustomField')
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

describe('DeleteDataDialog & TeardownDialog', () => {
  beforeEach(() => {
    resetState()
    useServiceStore.setState({ activeServiceId: 'some-other-active-service', isInitialized: true })
  })

  function renderDialog(props: Partial<React.ComponentProps<typeof DeleteDataDialog>> = {}) {
    const qc = createTestQueryClient()
    return render(
      <DeleteDataDialog
        service={{
          service_id: 'test-svc',
          name: 'Test Service',
          duckdb_size_bytes: 5_000_000,
          cache_file_count: 12,
        } as ServiceConfig}
        open={true}
        onOpenChange={vi.fn()}
        {...props}
      />,
      { wrapper: makeQueryWrapper(qc) },
    )
  }

  test('DeleteDataDialog shows local + cloud file counts and sizes before confirming', async () => {
    sseState.status = 'idle'
    server.use(
      http.get(`${API_BASE}/api/admin/storage-info`, ({ request }) => {
        const url = new URL(request.url)
        if (url.searchParams.get('service_id') !== 'test-svc') {
          return HttpResponse.json({ error: 'wrong service targeted' }, { status: 400 })
        }
        return HttpResponse.json({ data_files: 42, size_bytes: 123_456_789, snapshots: 3 })
      }),
    )

    renderDialog()

    expect(screen.getByText('Delete Data: Test Service')).toBeDefined()
    expect(await screen.findByText('12 files')).toBeInTheDocument()

    await waitFor(() => {
      expect(screen.getByText('42 files')).toBeInTheDocument()
    })
  })

  test('DeleteDataDialog keeps Delete Data disabled on a near-miss confirmation', async () => {
    sseState.status = 'idle'
    const user = userEvent.setup()

    renderDialog()

    const deleteBtn = screen.getByRole('button', { name: /^delete data$/i })
    const confirmInput = screen.getByLabelText(/type.*to confirm/i)

    await user.type(confirmInput, 'test service')
    expect((deleteBtn as HTMLButtonElement).disabled).toBe(true)

    await user.clear(confirmInput)
    await user.type(confirmInput, 'Test Servic')
    expect((deleteBtn as HTMLButtonElement).disabled).toBe(true)

    await user.type(confirmInput, 'e')
    expect((deleteBtn as HTMLButtonElement).disabled).toBe(false)
  })

  test('DeleteDataDialog explains the destructive scope and starts the SSE stream on confirm', async () => {
    sseState.status = 'idle'
    const user = userEvent.setup()
    const onOpenChange = vi.fn()
    const onComplete = vi.fn()

    renderDialog({ onOpenChange, onComplete })

    expect(screen.getByText('Delete Data: Test Service')).toBeDefined()
    expect(screen.getByText(/local.*and cloud-stored/i)).toBeDefined()
    expect(screen.getByText(/preserved:/i)).toBeDefined()

    const deleteBtn = screen.getByRole('button', { name: /^delete data$/i })
    expect((deleteBtn as HTMLButtonElement).disabled).toBe(true)
    await user.type(screen.getByLabelText(/type.*to confirm/i), 'Test Service')
    expect((deleteBtn as HTMLButtonElement).disabled).toBe(false)
    await user.click(deleteBtn)

    await waitFor(() => {
      expect(mockStart).toHaveBeenCalledWith(
        '/api/admin/reset-logs?service_id=test-svc',
        { confirm: 'test-svc' },
      )
    })
  })

  test('DeleteDataDialog blocks the Cancel/Close path while streaming and only offers Stop', async () => {
    sseState.status = 'idle'
    const user = userEvent.setup()
    const onOpenChange = vi.fn()

    const qc = createTestQueryClient()
    const { rerender } = render(
      <DeleteDataDialog
        service={{ service_id: 'test-svc', name: 'Test Service' } as ServiceConfig}
        open={true}
        onOpenChange={onOpenChange}
      />,
      { wrapper: makeQueryWrapper(qc) },
    )

    await user.type(screen.getByLabelText(/type.*to confirm/i), 'Test Service')
    await user.click(screen.getByRole('button', { name: /^delete data$/i }))
    sseState.status = 'streaming'
    rerender(
      <DeleteDataDialog
        service={{ service_id: 'test-svc', name: 'Test Service' } as ServiceConfig}
        open={true}
        onOpenChange={onOpenChange}
      />,
    )

    expect(screen.queryByRole('button', { name: /cancel/i })).toBeNull()
    const stopBtn = screen.getByRole('button', { name: /^stop$/i })
    await user.click(stopBtn)
    expect(mockStop).toHaveBeenCalled()
  })

  test('TeardownDialog starts the SSE stream when the user enters a token and clicks Execute Teardown', async () => {
    const user = userEvent.setup()
    const onOpenChange = vi.fn()
    const onComplete = vi.fn()

    render(
      <TeardownDialog
        service={{ service_id: 'test-svc', name: 'Test Service', rum_enabled: true } as ServiceConfig}
        open={true}
        onOpenChange={onOpenChange}
        onComplete={onComplete}
      />,
    )

    expect(screen.getByText('Teardown: Test Service')).toBeDefined()

    const teardownBtn = screen.getByRole('button', { name: /execute teardown/i })
    expect((teardownBtn as HTMLButtonElement).disabled).toBe(true)

    const tokenInput = screen.getByLabelText(/fastly token with the/i)
    await user.type(tokenInput, 'test-admin-token-value')

    expect((teardownBtn as HTMLButtonElement).disabled).toBe(false)
    await user.click(teardownBtn)

    await waitFor(() => {
      expect(mockStart).toHaveBeenCalledWith(
        '/api/provision/teardown',
        {
          service_id: 'test-svc',
          remove_logging: true,
          remove_rum: true,
          remove_cdn: true,
          remove_bucket: true,
          remove_cloud_files: true,
          remove_cache: true,
          token: 'test-admin-token-value',
        },
      )
    })
  })

})
