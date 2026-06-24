/**
 * @vitest-environment jsdom
 *
 * Tests for the pure-transformation helpers at
 * [components/ProvisionWizard/wizard-config-helpers.ts](../../../components/ProvisionWizard/wizard-config-helpers.ts).
 *
 * The wizard's log-fields step is the part most likely to silently drop
 * data on a refactor — the dependency-chain expansion in `buildToggleGroup`
 * and the preset toggle in `buildTogglePreset` both have non-trivial
 * iterative logic. The 4 helpers in this file own all the state mutation
 * for that step; the rest of the wizard just dispatches into them.
 *
 * Coverage of these helpers is also the next cheap win on the §10.14
 * frontend-coverage path (file was 1.8% covered pre-test) — pure
 * functions, no React, no MSW.
 */
import { describe, it, expect } from 'vitest'

import {
  applyToggleField,
  applyUpdateFieldLimit,
  buildToggleGroup,
  buildTogglePreset,
} from '@/components/ProvisionWizard/wizard-config-helpers'
import { INITIAL_CONFIG } from '@/components/ProvisionWizard/types'

// Catalog fixture mirroring the shape the API returns. `requires` is the
// dependency edge the toggleGroup helper auto-expands.
const CATALOG = {
  groups: [
    { id: 'core', label: 'Core' },
    { id: 'http', label: 'HTTP', requires: 'core' },
    { id: 'tls', label: 'TLS', requires: 'core' },
    { id: 'security', label: 'Security', requires: 'http' },
    // Cycle-free chain: detail → http → core
    { id: 'detail', label: 'Detail', requires: 'http' },
  ],
  presets: {
    minimal: { label: 'Minimal', groups: ['core'] },
    standard: { label: 'Standard', groups: ['core', 'http'] },
    full: { label: 'Full', groups: ['core', 'http', 'tls', 'security'] },
  },
}

function configWithGroups(groups: string[]) {
  return {
    ...INITIAL_CONFIG,
    log_fields: { ...INITIAL_CONFIG.log_fields, groups },
  }
}

describe('buildToggleGroup', () => {
  it('adds a group with no dependencies', () => {
    const toggle = buildToggleGroup(CATALOG)
    const next = toggle(configWithGroups([]), 'core', true)
    expect(next.log_fields.groups).toEqual(['core'])
  })

  it('auto-adds required ancestor groups when adding a dependent', () => {
    const toggle = buildToggleGroup(CATALOG)
    const next = toggle(configWithGroups([]), 'security', true)
    // security requires http → http requires core; both pulled in.
    expect(new Set(next.log_fields.groups)).toEqual(new Set(['security', 'http', 'core']))
  })

  it('handles transitive deps with intermediate already present', () => {
    const toggle = buildToggleGroup(CATALOG)
    const next = toggle(configWithGroups(['http']), 'security', true)
    expect(new Set(next.log_fields.groups)).toEqual(new Set(['http', 'core', 'security']))
  })

  it('removes a group without touching dependencies of others', () => {
    const toggle = buildToggleGroup(CATALOG)
    const next = toggle(configWithGroups(['core', 'http', 'security']), 'security', false)
    // Removing security must not cascade — http + core stay.
    expect(new Set(next.log_fields.groups)).toEqual(new Set(['core', 'http']))
  })

  it('is idempotent when adding a group that is already present', () => {
    const toggle = buildToggleGroup(CATALOG)
    const next = toggle(configWithGroups(['core']), 'core', true)
    expect(next.log_fields.groups).toEqual(['core'])
  })

  it('does not mutate the input config', () => {
    const toggle = buildToggleGroup(CATALOG)
    const input = configWithGroups(['core'])
    const inputGroupsRef = input.log_fields.groups
    toggle(input, 'http', true)
    expect(input.log_fields.groups).toBe(inputGroupsRef)
    expect(input.log_fields.groups).toEqual(['core'])
  })

  it('tolerates a null catalog when only the group toggle is needed', () => {
    const toggle = buildToggleGroup(null)
    const next = toggle(configWithGroups([]), 'core', true)
    expect(next.log_fields.groups).toEqual(['core'])
  })

  it('tolerates a config with no groups array set yet', () => {
    const toggle = buildToggleGroup(CATALOG)
    const config = {
      ...INITIAL_CONFIG,
      log_fields: { ...INITIAL_CONFIG.log_fields, groups: undefined as unknown as string[] },
    }
    const next = toggle(config, 'core', true)
    expect(next.log_fields.groups).toEqual(['core'])
  })
})

describe('applyToggleField', () => {
  it('sets an override when state differs from group default', () => {
    const config = {
      ...INITIAL_CONFIG,
      log_fields: { ...INITIAL_CONFIG.log_fields, field_overrides: {} },
    }
    const next = applyToggleField(config, 'cookie_id', true, false)
    expect(next.log_fields.field_overrides).toEqual({ cookie_id: true })
  })

  it('clears the override when state matches the group default', () => {
    const config = {
      ...INITIAL_CONFIG,
      log_fields: {
        ...INITIAL_CONFIG.log_fields,
        field_overrides: { cookie_id: true, ip: false },
      },
    }
    const next = applyToggleField(config, 'cookie_id', true, true)
    // Match — entry should be deleted to avoid leaking a redundant override.
    expect(next.log_fields.field_overrides).toEqual({ ip: false })
  })

  it('handles a config with no field_overrides yet', () => {
    const config = {
      ...INITIAL_CONFIG,
      log_fields: {
        ...INITIAL_CONFIG.log_fields,
        field_overrides: undefined as unknown as Record<string, boolean>,
      },
    }
    const next = applyToggleField(config, 'cookie_id', false, true)
    expect(next.log_fields.field_overrides).toEqual({ cookie_id: false })
  })
})

describe('applyUpdateFieldLimit', () => {
  it('sets a numeric limit', () => {
    const config = {
      ...INITIAL_CONFIG,
      log_fields: { ...INITIAL_CONFIG.log_fields, field_limits: {} },
    }
    const next = applyUpdateFieldLimit(config, 'url', 2048)
    expect(next.log_fields.field_limits).toEqual({ url: 2048 })
  })

  it('removes a limit when undefined is passed', () => {
    const config = {
      ...INITIAL_CONFIG,
      log_fields: {
        ...INITIAL_CONFIG.log_fields,
        field_limits: { url: 2048, ua: 512 },
      },
    }
    const next = applyUpdateFieldLimit(config, 'url', undefined)
    expect(next.log_fields.field_limits).toEqual({ ua: 512 })
  })

  it('overwrites an existing limit', () => {
    const config = {
      ...INITIAL_CONFIG,
      log_fields: {
        ...INITIAL_CONFIG.log_fields,
        field_limits: { url: 2048 },
      },
    }
    const next = applyUpdateFieldLimit(config, 'url', 1024)
    expect(next.log_fields.field_limits).toEqual({ url: 1024 })
  })

  it('handles a config with no field_limits yet', () => {
    const config = {
      ...INITIAL_CONFIG,
      log_fields: {
        ...INITIAL_CONFIG.log_fields,
        field_limits: undefined as unknown as Record<string, number>,
      },
    }
    const next = applyUpdateFieldLimit(config, 'url', 4096)
    expect(next.log_fields.field_limits).toEqual({ url: 4096 })
  })
})

describe('buildTogglePreset', () => {
  // isPresetActive is the helper the component normally supplies; for
  // these tests we derive it from the current config's groups set.
  function isPresetActiveFor(groups: string[]) {
    const set = new Set(groups)
    return (presetGroups: string[]) => presetGroups.every((g) => set.has(g))
  }

  it('adds all groups in a preset (and their dependencies)', () => {
    const toggle = buildTogglePreset(CATALOG, isPresetActiveFor([]))
    const next = toggle(configWithGroups([]), CATALOG.presets.standard.groups)
    expect(new Set(next.log_fields.groups)).toEqual(new Set(['core', 'http']))
  })

  it('pulls in transitive dependencies when activating a preset', () => {
    const toggle = buildTogglePreset(CATALOG, isPresetActiveFor([]))
    const next = toggle(configWithGroups([]), CATALOG.presets.full.groups)
    expect(new Set(next.log_fields.groups)).toEqual(
      new Set(['core', 'http', 'tls', 'security']),
    )
  })

  it('removes the preset groups when toggling an already-active preset off', () => {
    // Use a catalog with only the `standard` preset so deactivation isn't
    // protected by another preset (e.g. `minimal=['core']`) that would
    // otherwise keep `core` alive.
    const isolatedCatalog = {
      ...CATALOG,
      presets: { standard: CATALOG.presets.standard },
    }
    const startingGroups = ['core', 'http']
    const toggle = buildTogglePreset(isolatedCatalog, isPresetActiveFor(startingGroups))
    const next = toggle(
      configWithGroups(startingGroups),
      isolatedCatalog.presets.standard.groups,
    )
    expect(next.log_fields.groups).toEqual([])
  })

  it('keeps groups held by a different preset (minimal protects core)', () => {
    // With both standard and minimal in the catalog, deactivating standard
    // must keep `core` because minimal=['core'] is still satisfied.
    const startingGroups = ['core', 'http']
    const toggle = buildTogglePreset(CATALOG, isPresetActiveFor(startingGroups))
    const next = toggle(configWithGroups(startingGroups), CATALOG.presets.standard.groups)
    expect(next.log_fields.groups).toEqual(['core'])
  })

  it('keeps groups required by ANOTHER active preset when toggling one off', () => {
    // standard (core+http) AND full (core+http+tls+security) are both active.
    // Toggling standard off must NOT remove core/http because full needs them.
    const startingGroups = ['core', 'http', 'tls', 'security']
    const toggle = buildTogglePreset(CATALOG, isPresetActiveFor(startingGroups))
    const next = toggle(configWithGroups(startingGroups), CATALOG.presets.standard.groups)
    // All four still present — full preset is still on.
    expect(new Set(next.log_fields.groups)).toEqual(
      new Set(['core', 'http', 'tls', 'security']),
    )
  })

  it('cascades the removal to direct dependents when toggling a preset off', () => {
    // Use an isolated catalog so `minimal=['core']` doesn't preserve core.
    // Starting set: core, http, detail. Toggling standard (core+http) off
    // should drop all three — http drops because no other preset holds it,
    // core drops because no other preset holds it, and detail drops because
    // its `requires: 'http'` edge fires the catalog.groups.forEach cascade
    // inside the helper.
    const isolatedCatalog = {
      ...CATALOG,
      presets: { standard: CATALOG.presets.standard },
    }
    const startingGroups = ['core', 'http', 'detail']
    const toggle = buildTogglePreset(isolatedCatalog, isPresetActiveFor(startingGroups))
    const next = toggle(
      configWithGroups(startingGroups),
      isolatedCatalog.presets.standard.groups,
    )
    expect(next.log_fields.groups).toEqual([])
  })

  it('tolerates a catalog with no presets', () => {
    const toggle = buildTogglePreset({ groups: CATALOG.groups }, isPresetActiveFor([]))
    const next = toggle(configWithGroups([]), ['core'])
    expect(next.log_fields.groups).toEqual(['core'])
  })

  it('tolerates a null catalog', () => {
    const toggle = buildTogglePreset(null, isPresetActiveFor([]))
    const next = toggle(configWithGroups([]), ['core'])
    expect(next.log_fields.groups).toEqual(['core'])
  })
})
