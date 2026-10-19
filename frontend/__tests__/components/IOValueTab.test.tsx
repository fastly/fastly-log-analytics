/**
 * IOValueTab renders its URL-opportunity and format-conversion lists through
 * the shared DataTableReadonly. Pins headers and cell formatting for both the
 * IO-enabled view (conversion pairs + opportunities) and the upsell view.
 */
import { stubResizeObserver } from '@/__tests__/helpers/browser-stubs'
import * as React from 'react'
import { describe, it, expect, beforeAll, vi } from 'vitest'
import { render, screen } from '@testing-library/react'

import IOValueTab from '@/app/fastly-value/_sections/tabs/IOValueTab'
import type { components } from '@/types/api.generated'

vi.mock('@tanstack/react-virtual', async () => (await import('../helpers/virtual-mock')).reactVirtualModuleMock())
vi.mock('@/components/PlotlyChart', () => ({ PlotlyChart: () => <div data-testid="plotly" /> }))
vi.mock('@/components/FilterValueCell', () => ({
  FilterValueCell: ({ display }: { display: string }) => <span>{display}</span>,
}))

beforeAll(() => {
  stubResizeObserver()
})

type IOData = components['schemas']['IOMetrics']

const base = {
  io_time_series: [],
  format_distribution: [],
  io_bandwidth_time_series: [],
  io_compression_time_series: [],
  optimization_opportunities: [
    { url: '/img/hero.png', request_count: 1500, avg_kb: 812.34, total_bytes: 2_000_000 },
    { url: '/img/nokb.png', request_count: 3, avg_kb: null, total_bytes: 100 },
  ],
  io_format_conversion_pairs: [],
} as unknown as IOData

describe('IOValueTab tables', () => {
  it('upsell view lists the top image URLs', () => {
    render(<IOValueTab data={base} />)
    for (const name of ['URL', 'Requests', 'Avg KB', 'Total Bandwidth']) {
      expect(screen.getByRole('columnheader', { name })).toBeTruthy()
    }
    expect(screen.getByText('/img/hero.png')).toBeTruthy()
    expect(screen.getByText('812.3')).toBeTruthy()
    expect(screen.getAllByText('—').length).toBeGreaterThan(0)
  })

  it('enabled view lists format conversions', () => {
    const data = {
      ...base,
      io_transforms: 10,
      io_format_conversion_pairs: [{ input_format: 'png', output_format: 'webp', count: 2500, avg_ratio: 3.2 }],
    } as unknown as IOData
    render(<IOValueTab data={data} />)
    for (const name of ['From', 'To', 'Count', 'Avg Ratio']) {
      expect(screen.getByRole('columnheader', { name })).toBeTruthy()
    }
    expect(screen.getByText('png')).toBeTruthy()
    expect(screen.getByText('webp')).toBeTruthy()
    expect(screen.getByText('3.2x')).toBeTruthy()
  })
})
