/**
 * @vitest-environment jsdom
 *
 * Tests for the wizard's localStorage draft persistence at
 * [components/ProvisionWizard/wizard-draft.ts](../../../components/ProvisionWizard/wizard-draft.ts).
 *
 * The critical invariant is that NO secret-shaped field ever lands in
 * localStorage. The strip-on-write happens both at the type level (the
 * PersistedConfig Omit<>) and at runtime in saveDraft(); this test fixes
 * both with a regex sweep over the serialized payload.
 */
import { afterEach, beforeEach, describe, expect, it } from 'vitest'

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
  type WizardDraft,
} from '@/components/ProvisionWizard/types'

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

beforeEach(() => {
  window.localStorage.clear()
})

afterEach(() => {
  window.localStorage.clear()
})

describe('stripSecretsFromConfig', () => {
  it('removes fos_access_key, fos_secret_key, and cdn_secret', () => {
    const stripped = stripSecretsFromConfig({
      ...INITIAL_CONFIG,
      fos_access_key: 'AK',
      fos_secret_key: 'SK',
      cdn_secret: 'CDN',
    })
    expect(stripped).not.toHaveProperty('fos_access_key')
    expect(stripped).not.toHaveProperty('fos_secret_key')
    expect(stripped).not.toHaveProperty('cdn_secret')
  })

  it('preserves non-secret config fields', () => {
    const stripped = stripSecretsFromConfig({
      ...INITIAL_CONFIG,
      endpoint_name: 'My EP',
      sample_rate: 50,
    })
    expect(stripped).toMatchObject({
      endpoint_name: 'My EP',
      sample_rate: 50,
    })
  })
})

describe('mergePersistedConfig', () => {
  it('always returns blank secret fields, even if the persisted blob somehow contains them', () => {
    const dirty = {
      ...stripSecretsFromConfig(INITIAL_CONFIG),
      // simulate a tampered or legacy payload
      fos_access_key: 'LEAK',
      fos_secret_key: 'LEAK',
      cdn_secret: 'LEAK',
    } as never
    const merged = mergePersistedConfig(dirty)
    expect(merged.fos_access_key).toBe('')
    expect(merged.fos_secret_key).toBe('')
    expect(merged.cdn_secret).toBe('')
  })
})

describe('saveDraft + loadDraft roundtrip', () => {
  it('roundtrips a non-secret draft', () => {
    const draft = makeDraft()
    saveDraft(draft)
    const loaded = loadDraft()
    expect(loaded).not.toBeNull()
    expect(loaded!.draftId).toBe('draft-1')
    expect(loaded!.mode).toBe('provision')
    expect(loaded!.currentStep).toBe('fields')
    expect(loaded!.config.endpoint_name).toBe('My EP')
  })

  it('the serialized payload contains NO secret-shaped keys', () => {
    const draft = makeDraft()
    saveDraft(draft)
    const raw = window.localStorage.getItem(WIZARD_DRAFT_KEY)
    expect(raw).not.toBeNull()
    // No key matching /secret|token|password/ AND no key ending in _key.
    // (We allow draftId because the regex looks for 'key' as a whole-word
    // suffix on snake-cased config keys like 'fos_access_key'.)
    const parsed = JSON.parse(raw!)
    const allKeys = new Set<string>()
    function walk(node: unknown) {
      if (!node || typeof node !== 'object') return
      for (const [k, v] of Object.entries(node as Record<string, unknown>)) {
        allKeys.add(k)
        walk(v)
      }
    }
    walk(parsed)
    const offenders: string[] = []
    for (const k of allKeys) {
      if (/secret/i.test(k)) offenders.push(k)
      else if (/password/i.test(k)) offenders.push(k)
      else if (/_key$/i.test(k)) offenders.push(k)
    }
    expect(offenders).toEqual([])
    // Also a raw substring check on the serialized payload for the literal
    // secret values seeded by makeDraft.
    expect(raw).not.toContain('AK-LEAK')
    expect(raw).not.toContain('SK-LEAK')
    expect(raw).not.toContain('CDN-LEAK')
  })

  it('returns null for a version mismatch and clears the stale entry', () => {
    saveDraft(makeDraft({ version: WIZARD_DRAFT_VERSION + 1 }))
    const loaded = loadDraft()
    expect(loaded).toBeNull()
    expect(window.localStorage.getItem(WIZARD_DRAFT_KEY)).toBeNull()
  })

  it('returns null when the stored blob has step="mode" (nothing to resume)', () => {
    saveDraft(makeDraft({ step: 'mode', currentStep: 'mode' }))
    expect(loadDraft()).toBeNull()
  })

  it('returns null when JSON parse fails', () => {
    window.localStorage.setItem(WIZARD_DRAFT_KEY, '{not json')
    expect(loadDraft()).toBeNull()
  })

  it('returns null when localStorage is empty', () => {
    expect(loadDraft()).toBeNull()
  })
})

describe('clearDraft', () => {
  it('removes the draft key', () => {
    saveDraft(makeDraft())
    expect(window.localStorage.getItem(WIZARD_DRAFT_KEY)).not.toBeNull()
    clearDraft()
    expect(window.localStorage.getItem(WIZARD_DRAFT_KEY)).toBeNull()
  })

  it('is a no-op when nothing is stored', () => {
    expect(() => clearDraft()).not.toThrow()
  })
})

describe('saveDraft / loadDraft fallback on broken localStorage', () => {
  it('save is a no-op when localStorage.setItem throws', () => {
    const original = window.localStorage.setItem
    window.localStorage.setItem = () => {
      throw new Error('quota')
    }
    try {
      expect(() => saveDraft(makeDraft())).not.toThrow()
    } finally {
      window.localStorage.setItem = original
    }
  })

  it('load returns null when localStorage.getItem throws', () => {
    const original = window.localStorage.getItem
    window.localStorage.getItem = () => {
      throw new Error('blocked')
    }
    try {
      expect(loadDraft()).toBeNull()
    } finally {
      window.localStorage.getItem = original
    }
  })
})
