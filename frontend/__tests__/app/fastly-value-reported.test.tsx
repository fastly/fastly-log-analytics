/**
 * @vitest-environment jsdom
 */
import { cleanup, render, screen, within } from '@testing-library/react'
import { afterEach, describe, expect, it } from 'vitest'
import { SummaryValueTab, type ValueData } from '@/app/fastly-value/_sections/ValueDomainTabs'
import { formatBytes, formatCompactCount } from '@/lib/utils'

afterEach(cleanup)

const reported = {
  window_start: '2026-11-05T10:00:00Z',
  window_end: '2026-11-05T12:00:00Z',
  requests: 200,
  bandwidth_bytes: 2_000_000,
  hit_requests: 170,
  origin_offload_pct: 85,
  sample_rate: 100,
  edge_only: false,
  logged_requests: 185,
  logged_bandwidth_bytes: 1_900_000,
}

const data = (fastly_reported: ValueData['fastly_reported']): ValueData => ({
  overview: {
    total_requests: 190,
    total_bandwidth_bytes: 1_950_000,
    origin_offload_pct: 84.5,
    cache_acceleration_factor: null,
    threats_blocked: null,
  },
  caching: { origin_offload_pct: 84.5, bandwidth_saved_bytes: 1_500_000 },
  fastly_reported,
} as ValueData)

const card = (title: string) => screen.getByText(title).closest('[data-slot="card"]') as HTMLElement

describe('Fastly Value summary cards', () => {
  it("shows Fastly's reported totals beside the logged ones, with how far the logs are off", () => {
    render(<SummaryValueTab data={data(reported)} />)
    expect(within(card('Total Requests')).getByText(`Fastly reported ${formatCompactCount(200)} · logs −7.5%`)).toBeInTheDocument()
    expect(within(card('Bandwidth Delivered')).getByText(`Fastly reported ${formatBytes(2_000_000)} · logs −5.0%`))
      .toBeInTheDocument()
    expect(within(card('Origin Offload')).getByText('Fastly reported 85.0% · logs −0.5 pts')).toBeInTheDocument()
    expect(card('Total Requests').querySelector('[title]')?.getAttribute('title'))
      .toMatch(/2026-11-05 10:00.*12:00 UTC/)
  })

  it('compares sampled logs with the sampled share of the Fastly figure', () => {
    render(<SummaryValueTab data={data({ ...reported, requests: 2000, sample_rate: 10 })} />)
    const line = within(card('Total Requests')).getByText(`Fastly reported ${formatCompactCount(2000)} · logs −7.5%`)
    expect(line.getAttribute('title')).toMatch(/10% of requests/)
  })

  it('leaves the comparison out without Fastly figures, and labels bandwidth saved as an estimate', () => {
    render(<SummaryValueTab data={data(null)} />)
    expect(screen.queryByText(/Fastly reported/)).not.toBeInTheDocument()
    expect(within(card('Bandwidth Saved (est.)')).getByText(/^Estimate:/)).toBeInTheDocument()
  })
})
