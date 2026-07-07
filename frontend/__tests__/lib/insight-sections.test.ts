import { describe, it, expect } from 'vitest'
import {
  INSIGHT_SECTIONS,
  OTHER_SECTION,
  categoryToSection,
  sortInsights,
  groupInsightsBySection,
  summarizeSeverities,
} from '@/lib/insight-sections'

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
      'Beta', // critical
      'Delta', // error
      'Alpha', // warning, A before G
      'Gamma', // warning
      'Epsilon', // info
      'Zeta', // clean
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
    // Provide traffic + security cards only → order must still be
    // security before traffic (fixed INSIGHT_SECTIONS order), no origin/edge.
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
    expect(edge.cards.map((c) => c.id)).toEqual(['2', '1']) // critical before warning
    const sec = groups.find((g) => g.section.key === 'security')!
    expect(sec.cards.map((c) => c.id)).toEqual(['3'])
  })

  it('files network-category cards into the Network section, in fixed order', () => {
    // network sits after edge, before traffic (see INSIGHT_SECTIONS order).
    const cards = [
      { id: '1', title: 'Traf', severity: 'info', category: 'traffic' },
      { id: '2', title: 'Net', severity: 'warning', category: 'network' },
      { id: '3', title: 'Edge', severity: 'info', category: 'edge' },
    ]
    const groups = groupInsightsBySection(cards)
    expect(groups.map((g) => g.section.key)).toEqual(['edge', 'network', 'traffic'])
    const net = groups.find((g) => g.section.key === 'network')!
    expect(net.cards.map((c) => c.id)).toEqual(['2'])
    expect(net.section.label).toBe('Network Path')
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
    // Availability entries have id/title/category but no severity.
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
    // Fixed triage order: attack → broken → slow → network path → shifting.
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
    // Availability skeletons carry title/category but no severity.
    expect(summarizeSeverities([{ title: 'Alpha', category: 'origin' }])).toEqual([])
  })

  it('returns [] for an empty section', () => {
    expect(summarizeSeverities([])).toEqual([])
  })
})
