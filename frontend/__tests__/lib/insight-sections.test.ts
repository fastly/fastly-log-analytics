import { describe, it, expect } from 'vitest'
import {
  INSIGHT_SECTIONS,
  OTHER_SECTION,
  categoryToSection,
  sortInsights,
  groupInsightsBySection,
  summarizeSeverities,
} from '@/lib/insight-sections'
import {
  pickInsightsDefault,
  historyHoursFromExtents,
  WINDOW_OPTIONS,
  BASELINE_OPTIONS,
} from '@/lib/insights-defaults'

describe('categoryToSection', () => {
  it('maps each known category to its section', () => {
    expect(categoryToSection('security').key).toBe('security')
    expect(categoryToSection('origin').key).toBe('origin')
    expect(categoryToSection('edge').key).toBe('edge')
    expect(categoryToSection('network').key).toBe('network')
    expect(categoryToSection('traffic').key).toBe('traffic')
  })

  it('falls back to Other for unknown / missing category', () => {
    expect(categoryToSection('does_not_exist').key).toBe(OTHER_SECTION.key)
    expect(categoryToSection('').key).toBe(OTHER_SECTION.key)
    expect(categoryToSection(null).key).toBe(OTHER_SECTION.key)
    expect(categoryToSection(undefined).key).toBe(OTHER_SECTION.key)
  })
})

describe('sortInsights', () => {
  it('orders by severity (critical→error→warning→info→clean) then title', () => {
    const cards = [
      { title: 'Zeta', severity: 'clean' },
      { title: 'Alpha', severity: 'warning' },
      { title: 'Beta', severity: 'critical' },
      { title: 'Gamma', severity: 'warning' },
      { title: 'Delta', severity: 'error' },
      { title: 'Epsilon', severity: 'info' },
    ]
    expect(sortInsights(cards).map((c) => c.title)).toEqual([
      'Beta',
      'Delta',
      'Alpha',
      'Gamma',
      'Epsilon',
      'Zeta',
    ])
  })

  it('sorts unknown / missing severities last, then by title', () => {
    const cards = [
      { title: 'B', severity: undefined },
      { title: 'A', severity: 'critical' },
      { title: 'C', severity: 'mystery' },
    ]
    expect(sortInsights(cards).map((c) => c.title)).toEqual(['A', 'B', 'C'])
  })

  it('does not mutate its input', () => {
    const cards = [
      { title: 'B', severity: 'clean' },
      { title: 'A', severity: 'critical' },
    ]
    const snapshot = [...cards]
    sortInsights(cards)
    expect(cards).toEqual(snapshot)
  })
})

describe('groupInsightsBySection', () => {
  it('returns sections in fixed order, dropping empty ones', () => {
    const cards = [
      { id: '1', title: 'T', severity: 'info', category: 'traffic' },
      { id: '2', title: 'S', severity: 'critical', category: 'security' },
    ]
    const groups = groupInsightsBySection(cards)
    expect(groups.map((g) => g.section.key)).toEqual(['security', 'traffic'])
  })

  it('files each card into its section and sorts within the section', () => {
    const cards = [
      { id: '1', title: 'Warn', severity: 'warning', category: 'edge' },
      { id: '2', title: 'Crit', severity: 'critical', category: 'edge' },
      { id: '3', title: 'Sec', severity: 'info', category: 'security' },
    ]
    const groups = groupInsightsBySection(cards)
    const edge = groups.find((g) => g.section.key === 'edge')!
    expect(edge.cards.map((c) => c.id)).toEqual(['2', '1'])
    const sec = groups.find((g) => g.section.key === 'security')!
    expect(sec.cards.map((c) => c.id)).toEqual(['3'])
  })

  it('files network-category cards into the Network section, in fixed order', () => {
    const cards = [
      { id: '1', title: 'Traf', severity: 'info', category: 'traffic' },
      { id: '2', title: 'Net', severity: 'warning', category: 'network' },
      { id: '3', title: 'Edge', severity: 'info', category: 'edge' },
    ]
    const groups = groupInsightsBySection(cards)
    expect(groups.map((g) => g.section.key)).toEqual(['edge', 'network', 'traffic'])
    const net = groups.find((g) => g.section.key === 'network')!
    expect(net.cards.map((c) => c.id)).toEqual(['2'])
    expect(net.section.label).toBe('Network & Routing Path')
  })

  it('buckets unknown / missing categories into the Other section, ordered last', () => {
    const cards = [
      { id: '1', title: 'Known', severity: 'info', category: 'security' },
      { id: '2', title: 'Unknown', severity: 'info', category: 'does_not_exist' },
      { id: '3', title: 'Missing', severity: 'info', category: null },
    ]
    const groups = groupInsightsBySection(cards)
    expect(groups.map((g) => g.section.key)).toEqual(['security', OTHER_SECTION.key])
    const other = groups.find((g) => g.section.key === OTHER_SECTION.key)!
    expect(other.cards.map((c) => c.id).sort()).toEqual(['2', '3'])
  })

  it('groups skeleton-shaped entries (no severity) by title', () => {
    const skeletons = [
      { id: 'b', title: 'Beta', category: 'origin' },
      { id: 'a', title: 'Alpha', category: 'origin' },
    ]
    const groups = groupInsightsBySection(skeletons)
    expect(groups).toHaveLength(1)
    expect(groups[0].section.key).toBe('origin')
    expect(groups[0].cards.map((c) => c.id)).toEqual(['a', 'b'])
  })

  it('every declared section key is a distinct backend category', () => {
    const keys = INSIGHT_SECTIONS.map((s) => s.key)
    expect(new Set(keys).size).toBe(keys.length)
    expect(keys).toEqual(['security', 'origin', 'edge', 'network', 'traffic'])
  })
})

describe('summarizeSeverities', () => {
  it('counts per severity, worst-first, omitting zero counts', () => {
    const cards = [
      { severity: 'warning' },
      { severity: 'critical' },
      { severity: 'warning' },
      { severity: 'warning' },
      { severity: 'critical' },
      { severity: 'clean' },
    ]
    expect(summarizeSeverities(cards)).toEqual([
      { severity: 'critical', count: 2 },
      { severity: 'warning', count: 3 },
      { severity: 'clean', count: 1 },
    ])
  })

  it('orders chips by the severity ramp regardless of input order', () => {
    const cards = [
      { severity: 'clean' },
      { severity: 'info' },
      { severity: 'error' },
      { severity: 'critical' },
    ]
    expect(summarizeSeverities(cards).map((s) => s.severity)).toEqual([
      'critical',
      'error',
      'info',
      'clean',
    ])
  })

  it('ignores unknown and missing severities (skeleton entries → [])', () => {
    expect(summarizeSeverities([{ severity: 'mystery' }, { severity: null }, {}])).toEqual([])
    expect(summarizeSeverities([{ title: 'Alpha', category: 'origin' }])).toEqual([])
  })

  it('returns [] for an empty section', () => {
    expect(summarizeSeverities([])).toEqual([])
  })
})

const STATIC_DEFAULT = { window: '24', baseline: '168' } as const

const WIN = new Set(WINDOW_OPTIONS.map((o) => o.value))
const BASE = new Set(BASELINE_OPTIONS.map((o) => o.value))

describe('pickInsightsDefault', () => {
  it('returns the static default when history is null', () => {
    expect(pickInsightsDefault(null)).toEqual({
      window: STATIC_DEFAULT.window,
      baseline: STATIC_DEFAULT.baseline,
    })
  })

  it.each([
    [0.5, '0.25', '1'],
    [2, '1', '1'],
    [12, '4', '1'],
    [36, '4', '24'],
    [96, '24', '24'],
    [200, '24', '168'],
    [800, '24', '168'],
  ])('history %sh -> window %s / baseline %s', (h, w, b) => {
    expect(pickInsightsDefault(h)).toEqual({ window: w, baseline: b })
  })

  it.each([
    [0.999, '0.25', '1'],
    [1, '1', '1'],
    [3.999, '1', '1'],
    [4, '4', '1'],
    [23.999, '4', '1'],
    [24, '4', '24'],
    [47.999, '4', '24'],
    [48, '24', '24'],
    [167.999, '24', '24'],
    [168, '24', '168'],
    [719.999, '24', '168'],
    [720, '24', '168'],
  ])('boundary %sh -> %s / %s', (h, w, b) => {
    expect(pickInsightsDefault(h)).toEqual({ window: w, baseline: b })
  })

  it('only ever returns real dropdown option values', () => {
    for (const h of [null, 0, 0.5, 1, 2, 4, 12, 24, 36, 96, 168, 200, 720, 2000, NaN]) {
      const r = pickInsightsDefault(h as number | null)
      expect(WIN.has(r.window)).toBe(true)
      expect(BASE.has(r.baseline)).toBe(true)
    }
  })

  it('treats NaN as no-data (static default)', () => {
    expect(pickInsightsDefault(NaN)).toEqual({
      window: STATIC_DEFAULT.window,
      baseline: STATIC_DEFAULT.baseline,
    })
  })
})

describe('historyHoursFromExtents', () => {
  const NOW = Date.parse('2026-06-15T12:00:00.000Z')

  it('returns null for null/undefined/empty earliest', () => {
    expect(historyHoursFromExtents(null, NOW)).toBeNull()
    expect(historyHoursFromExtents(undefined, NOW)).toBeNull()
    expect(historyHoursFromExtents('', NOW)).toBeNull()
  })

  it('computes hours from a full ISO earliest', () => {
    expect(historyHoursFromExtents('2026-06-15T10:00:00.000Z', NOW)).toBeCloseTo(2, 6)
  })

  it('widens a date-only earliest to UTC start-of-day', () => {
    expect(historyHoursFromExtents('2026-06-14', NOW)).toBeCloseTo(36, 6)
  })

  it('returns null for an unparseable earliest', () => {
    expect(historyHoursFromExtents('not-a-date', NOW)).toBeNull()
  })

  it('clamps a future earliest to 0 (never negative)', () => {
    expect(historyHoursFromExtents('2026-06-15T13:00:00.000Z', NOW)).toBe(0)
  })
})
