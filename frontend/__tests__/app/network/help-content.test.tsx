import { render, screen } from '@testing-library/react'
import { describe, expect, test, vi } from 'vitest'
import React from 'react'

// SHIELDING_COLUMNS renders FilterValueCell, which reads next/navigation's
// usePathname and the filterStore. Stub both so column cells render in isolation.
vi.mock('next/navigation', () => ({
  usePathname: vi.fn(() => '/network'),
}))
vi.mock('@/stores/filterStore', () => ({
  useFilterStore: vi.fn((selector?: (s: any) => any) => {
    const state = { filters: [], addFilter: vi.fn(), removeFilter: vi.fn(), clearFilters: vi.fn() }
    return selector ? selector(state) : state
  }),
}))

import {
  GlobalHealthHelp, AvgRttHelp, WorstAsnHelp, WorstRegionHelp, HeatmapHelp,
  AsnLeaderboardHelp, MetroLeaderboardHelp, ShieldingHelp,
  HealthBadge, SHIELDING_COLUMNS, getShieldingLabels,
} from '@/app/network/help-content'

describe('HealthBadge', () => {
  test('null → em-dash; ≥80 → green; 50-79 → yellow; <50 → destructive (no color leak)', () => {
    render(<HealthBadge score={null} />)
    expect(screen.getByText('—')).toBeInTheDocument()
    const hi = render(<HealthBadge score={92} />)
    expect(hi.getByText('92/100')).toBeInTheDocument()
    expect(hi.container.querySelector('[class*="text-green"]')).not.toBeNull()
    hi.unmount()
    const mid = render(<HealthBadge score={65} />)
    expect(mid.getByText('65/100')).toBeInTheDocument()
    expect(mid.container.querySelector('[class*="text-yellow"]')).not.toBeNull()
    mid.unmount()
    const lo = render(<HealthBadge score={20} />)
    expect(lo.getByText('20/100')).toBeInTheDocument()
    expect(lo.container.querySelector('[class*="text-green"]')).toBeNull()
    expect(lo.container.querySelector('[class*="text-yellow"]')).toBeNull()
    lo.unmount()
  })
})

describe('SHIELDING_COLUMNS', () => {
  // The columns are plain config objects (no react-table runtime needed).
  // Call each `cell` factory directly with a minimal info-shape mock.
  const makeInfo = (value: any, original: any = {}) => ({ getValue: () => value, row: { original } })
  const colById = (id: string) => SHIELDING_COLUMNS.find((c) => c.id === id)!

  test('every column exposes id + accessorKey + meta.label + header + cell', () => {
    expect(SHIELDING_COLUMNS.length).toBeGreaterThanOrEqual(8)
    for (const col of SHIELDING_COLUMNS) {
      expect(col.id).toBeTruthy()
      expect(col.accessorKey).toBe(col.id)
      expect((col.meta as any).label).toBeTruthy()
      expect(typeof col.header).toBe('function')
      expect(typeof col.cell).toBe('function')
    }
  })

  test('edge_pop cell renders value + destructive class when anomaly_static', () => {
    const { container } = render(<>{colById('edge_pop').cell(makeInfo('SFO', { anomaly_static: true }))}</>)
    expect(container.textContent).toMatch(/SFO/)
    expect(container.querySelector('[class*="text-destructive"]')).not.toBeNull()
  })

  test('requests cell uses locale separators; p50/p95/p99 use fixed-point ms', () => {
    const reqs = render(<>{colById('requests').cell(makeInfo(1234567))}</>)
    expect(reqs.container.textContent).toMatch(/1[.,]234[.,]567/)
    reqs.unmount()
    for (const id of ['p50_ms', 'p95_ms', 'p99_ms'] as const) {
      const r = render(<>{colById(id).cell(makeInfo(12.345))}</>)
      expect(r.container.textContent).toMatch(/12\.3ms/)
      r.unmount()
    }
  })

  test('light_speed_rtt_ms: em-dash when null, fixed-point ms when present', () => {
    const a = render(<>{colById('light_speed_rtt_ms').cell(makeInfo(null))}</>)
    expect(a.container.textContent).toMatch(/—/); a.unmount()
    const b = render(<>{colById('light_speed_rtt_ms').cell(makeInfo(4.2))}</>)
    expect(b.container.textContent).toMatch(/4\.2ms/); b.unmount()
  })

  test('efficiency_ratio: null → em-dash; <1.5 → green; 1.5-3 → yellow; ≥3 → destructive', () => {
    const col = colById('efficiency_ratio')
    const nul = render(<>{col.cell(makeInfo(null))}</>)
    expect(nul.container.textContent).toMatch(/—/); nul.unmount()
    const lo = render(<>{col.cell(makeInfo(1.2))}</>)
    expect(lo.container.querySelector('[class*="text-green"]')).not.toBeNull(); lo.unmount()
    const mid = render(<>{col.cell(makeInfo(2.4))}</>)
    expect(mid.container.querySelector('[class*="text-yellow"]')).not.toBeNull(); mid.unmount()
    const hi = render(<>{col.cell(makeInfo(5.0))}</>)
    expect(hi.container.querySelector('[class*="text-destructive"]')).not.toBeNull(); hi.unmount()
  })
})

describe('getShieldingLabels', () => {
  test('maps known ids, falls back to id for unknown, preserves order', () => {
    expect(getShieldingLabels(['edge_pop', 'requests'])).toEqual([
      { id: 'edge_pop', label: 'Edge POP' },
      { id: 'requests', label: 'Requests' },
    ])
    expect(getShieldingLabels(['mystery_field'])).toEqual([
      { id: 'mystery_field', label: 'mystery_field' },
    ])
    expect(getShieldingLabels(['p99_ms', 'p50_ms', 'p95_ms']).map((o) => o.id))
      .toEqual(['p99_ms', 'p50_ms', 'p95_ms'])
  })
})

describe('stateless help components', () => {
  test('GlobalHealthHelp mentions the 0–100 composite score', () => {
    render(<GlobalHealthHelp />)
    expect(screen.getByText(/0[–-]100 score/i)).toBeInTheDocument()
  })
  test('AvgRttHelp mentions TCP Round Trip Time', () => {
    render(<AvgRttHelp />)
    expect(screen.getByText(/TCP Round Trip Time/i)).toBeInTheDocument()
  })
  test('WorstAsnHelp mentions Autonomous System Number', () => {
    render(<WorstAsnHelp />)
    expect(screen.getByText(/Autonomous System Number/i)).toBeInTheDocument()
  })
  test('WorstRegionHelp mentions country-level network health', () => {
    const { container } = render(<WorstRegionHelp />)
    expect(container.textContent).toMatch(/country experiencing the lowest/i)
  })
  test('HeatmapHelp explains ISP rows + time-bucket columns', () => {
    const { container } = render(<HeatmapHelp />)
    expect(container.textContent).toMatch(/row is an ISP/i)
    expect(container.textContent).toMatch(/column is a time bucket/i)
  })
  test('AsnLeaderboardHelp surfaces P95/P99 tail-latency context', () => {
    const { container } = render(<AsnLeaderboardHelp />)
    expect(container.textContent).toMatch(/P95 \/ P99 RTT/i)
  })
  test('MetroLeaderboardHelp mentions city / metro area segmentation', () => {
    const { container } = render(<MetroLeaderboardHelp />)
    expect(container.textContent).toMatch(/city or metro area/i)
  })
  test('ShieldingHelp includes the efficiency legend buckets', () => {
    const { container } = render(<ShieldingHelp />)
    expect(container.textContent).toMatch(/Excellent/i)
    expect(container.textContent).toMatch(/Moderate/i)
    expect(container.textContent).toMatch(/Investigate/i)
  })
})
