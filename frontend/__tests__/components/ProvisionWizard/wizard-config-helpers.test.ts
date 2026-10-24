/**
 * @vitest-environment jsdom
 *
 * Consolidated tests for log-field config ops and wizard draft persistence.
 */
import { without } from '@/__tests__/helpers/partial'
import { afterEach, beforeEach, describe, it, expect } from 'vitest'

import {
  toggleLogFieldGroup,
  toggleLogFieldOverride,
  toggleLogFieldPreset,
  updateLogFieldLimit,
} from '@/lib/log-field-config-ops'
import {
  WIZARD_DRAFT_KEY,
  clearDraft,
  loadDraft,
  mergePersistedConfig,
  saveDraft,
  stripSecretsFromConfig,
} from '@/components/ProvisionWizard/wizard-draft'
import {
  INITIAL_CONFIG,
  WIZARD_DRAFT_VERSION,
  type LogFieldsCatalog,
  type WizardDraft,
} from '@/components/ProvisionWizard/types'

const buildToggleGroup = (catalog: LogFieldsCatalog | null | undefined) => (
  prev: typeof INITIAL_CONFIG,
  groupId: string,
  checked: boolean,
) => ({
  ...prev,
  log_fields: toggleLogFieldGroup(prev.log_fields, catalog, groupId, checked),
})

const applyToggleField = (
  prev: typeof INITIAL_CONFIG,
  fieldId: string,
  checked: boolean,
  defaultEnabledByGroup: boolean,
) => ({
  ...prev,
  log_fields: toggleLogFieldOverride(prev.log_fields, fieldId, checked, defaultEnabledByGroup),
})

const applyUpdateFieldLimit = (
  prev: typeof INITIAL_CONFIG,
  fieldId: string,
  limit?: number,
) => ({
  ...prev,
  log_fields: updateLogFieldLimit(prev.log_fields, fieldId, limit),
})

const buildTogglePreset = (
  catalog: LogFieldsCatalog | null | undefined,
  isPresetActive: (groups: string[]) => boolean,
) => (prev: typeof INITIAL_CONFIG, presetGroups: string[]) => ({
  ...prev,
  log_fields: toggleLogFieldPreset(prev.log_fields, catalog, presetGroups, isPresetActive),
})

const group = (id: string, label: string, requires: string | null = null) => ({
  id,
  label,
  description: '',
  locked: false,
  requires,
  note: null,
  recommended_with: null,
  total_bytes: 0,
  fields: [],
})

const PRESETS = {
  minimal: { label: 'Minimal', description: '', groups: ['core'] },
  standard: { label: 'Standard', description: '', groups: ['core', 'http'] },
  full: { label: 'Full', description: '', groups: ['core', 'http', 'tls', 'security'] },
}

const CATALOG: LogFieldsCatalog = {
  groups: [
    group('core', 'Core'),
    group('http', 'HTTP', 'core'),
    group('tls', 'TLS', 'core'),
    group('security', 'Security', 'http'),
    group('detail', 'Detail', 'http'),
  ],
  presets: PRESETS,
}

function configWithGroups(groups: string[]) {
  return {
    ...INITIAL_CONFIG,
    log_fields: { ...INITIAL_CONFIG.log_fields, groups },
  }
}

describe('buildToggleGroup', () => {
  it('adds a group with no dependencies and auto-adds required ancestor groups', () => {
    const toggle = buildToggleGroup(CATALOG)
    expect(toggle(configWithGroups([]), 'core', true).log_fields.groups).toEqual(['core'])
    expect(new Set(toggle(configWithGroups([]), 'security', true).log_fields.groups)).toEqual(
      new Set(['security', 'http', 'core']),
    )
    expect(new Set(toggle(configWithGroups(['http']), 'security', true).log_fields.groups)).toEqual(
      new Set(['http', 'core', 'security']),
    )
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
      ...INITIAL_CONFIG,
      log_fields: without(INITIAL_CONFIG.log_fields, 'groups') as typeof INITIAL_CONFIG.log_fields,
    }
    expect(toggle(config, 'core', true).log_fields.groups).toEqual(['core'])
  })
})

describe('applyToggleField & applyUpdateFieldLimit', () => {
  it('sets or clears field overrides based on group default', () => {
    const c1 = { ...INITIAL_CONFIG, log_fields: { ...INITIAL_CONFIG.log_fields, field_overrides: {} } }
    expect(applyToggleField(c1, 'cookie_id', true, false).log_fields.field_overrides).toEqual({ cookie_id: true })

    const c2 = {
      ...INITIAL_CONFIG,
      log_fields: { ...INITIAL_CONFIG.log_fields, field_overrides: { cookie_id: true, ip: false } },
    }
    expect(applyToggleField(c2, 'cookie_id', true, true).log_fields.field_overrides).toEqual({ ip: false })

    const c3 = { ...INITIAL_CONFIG, log_fields: { ...without(INITIAL_CONFIG.log_fields, 'field_overrides') } }
    expect(applyToggleField(c3, 'cookie_id', false, true).log_fields.field_overrides).toEqual({ cookie_id: false })
  })

  it('sets, overwrites, and removes numeric field limits', () => {
    const c1 = { ...INITIAL_CONFIG, log_fields: { ...INITIAL_CONFIG.log_fields, field_limits: {} } }
    expect(applyUpdateFieldLimit(c1, 'url', 2048).log_fields.field_limits).toEqual({ url: 2048 })

    const c2 = {
      ...INITIAL_CONFIG,
      log_fields: { ...INITIAL_CONFIG.log_fields, field_limits: { url: 2048, ua: 512 } },
    }
    expect(applyUpdateFieldLimit(c2, 'url', undefined).log_fields.field_limits).toEqual({ ua: 512 })
    expect(applyUpdateFieldLimit(c2, 'url', 1024).log_fields.field_limits).toEqual({ url: 1024, ua: 512 })

    const c3 = { ...INITIAL_CONFIG, log_fields: { ...without(INITIAL_CONFIG.log_fields, 'field_limits') } }
    expect(applyUpdateFieldLimit(c3, 'url', 4096).log_fields.field_limits).toEqual({ url: 4096 })
  })
})

describe('buildTogglePreset', () => {
  function isPresetActiveFor(groups: string[]) {
    const set = new Set(groups)
    return (presetGroups: string[]) => presetGroups.every((g) => set.has(g))
  }

  it('adds preset groups, removes unprotected groups on toggle-off, and cascades dependents', () => {
    const toggle = buildTogglePreset(CATALOG, isPresetActiveFor([]))
    expect(new Set(toggle(configWithGroups([]), PRESETS.full.groups).log_fields.groups)).toEqual(
      new Set(['core', 'http', 'tls', 'security']),
    )

    const isolatedCatalog = { ...CATALOG, presets: { standard: PRESETS.standard } }
    const startingGroups = ['core', 'http', 'detail']
    const toggleOff = buildTogglePreset(isolatedCatalog, isPresetActiveFor(startingGroups))
    expect(toggleOff(configWithGroups(startingGroups), PRESETS.standard.groups).log_fields.groups).toEqual([])
  })

  it('keeps groups protected by other active presets and tolerates missing/null catalog', () => {
    const startingStd = ['core', 'http']
    const toggleStd = buildTogglePreset(CATALOG, isPresetActiveFor(startingStd))
    expect(toggleStd(configWithGroups(startingStd), PRESETS.standard.groups).log_fields.groups).toEqual(['core'])

    const startingFull = ['core', 'http', 'tls', 'security']
    const toggleFull = buildTogglePreset(CATALOG, isPresetActiveFor(startingFull))
    expect(new Set(toggleFull(configWithGroups(startingFull), PRESETS.standard.groups).log_fields.groups)).toEqual(
      new Set(['core', 'http', 'tls', 'security']),
    )

    expect(buildTogglePreset({ groups: CATALOG.groups }, isPresetActiveFor([]))(configWithGroups([]), ['core']).log_fields.groups).toEqual(['core'])
    expect(buildTogglePreset(null, isPresetActiveFor([]))(configWithGroups([]), ['core']).log_fields.groups).toEqual(['core'])
  })
})

function makeDraft(overrides: Partial<WizardDraft> = {}): WizardDraft {
  return {
    version: WIZARD_DRAFT_VERSION,
    draftId: 'draft-1',
    mode: 'provision',
    step: 'fields',
    currentStep: 'fields',
    selectedServiceId: 'svc-1',
    selectedServiceName: 'svc',
    selectedCdnServiceId: null,
    selectedCdnServiceName: null,
    tokenInfo: { id: 'tok-1', name: 'tok', type: 'user' },
    config: stripSecretsFromConfig({
      ...INITIAL_CONFIG,
      endpoint_name: 'My EP',
      fos_bucket_name: 'b',
      fos_access_key: 'AK-LEAK',
      fos_secret_key: 'SK-LEAK',
      cdn_secret: 'CDN-LEAK',
    }),
    importMode: 'all',
    importRange: { start: '', end: '' },
    syncEnabled: true,
    syncIntervalMins: '2',
    icebergMetadataLocation: '',
    updatedAt: new Date().toISOString(),
    createdAt: new Date().toISOString(),
    ...overrides,
  }
}

describe('wizard-draft persistence', () => {
  beforeEach(() => window.localStorage.clear())
  afterEach(() => window.localStorage.clear())

  it('strips secrets and mergePersistedConfig always returns blank secret fields', () => {
    const stripped = stripSecretsFromConfig({
      ...INITIAL_CONFIG,
      endpoint_name: 'My EP',
      sample_rate: 50,
      fos_access_key: 'AK',
      fos_secret_key: 'SK',
      cdn_secret: 'CDN',
    })
    expect(stripped).not.toHaveProperty('fos_access_key')
    expect(stripped).not.toHaveProperty('fos_secret_key')
    expect(stripped).not.toHaveProperty('cdn_secret')
    expect(stripped).toMatchObject({ endpoint_name: 'My EP', sample_rate: 50 })

    const dirty = {
      ...stripSecretsFromConfig(INITIAL_CONFIG),
      fos_access_key: 'LEAK',
      fos_secret_key: 'LEAK',
      cdn_secret: 'LEAK',
    }
    const merged = mergePersistedConfig(dirty)
    expect(merged.fos_access_key).toBe('')
    expect(merged.fos_secret_key).toBe('')
    expect(merged.cdn_secret).toBe('')
  })

  it('roundtrips a non-secret draft with zero secret keys or values in localStorage', () => {
    const draft = makeDraft()
    saveDraft(draft)
    const loaded = loadDraft()
    expect(loaded).not.toBeNull()
    expect(loaded!.draftId).toBe('draft-1')
    expect(loaded!.config.endpoint_name).toBe('My EP')

    const raw = window.localStorage.getItem(WIZARD_DRAFT_KEY)!
    expect(raw).not.toContain('AK-LEAK')
    expect(raw).not.toContain('SK-LEAK')
    expect(raw).not.toContain('CDN-LEAK')
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
    try {
      expect(() => saveDraft(makeDraft())).not.toThrow()
    } finally {
      window.localStorage.setItem = origSet
    }
  })
})
