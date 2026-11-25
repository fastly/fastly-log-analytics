/**
 * @vitest-environment jsdom
 */
import { cleanup, render, screen } from '@testing-library/react'
import { afterEach, describe, expect, it } from 'vitest'
import { Aggregates } from '@/app/origin/_sections/Aggregates'

afterEach(cleanup)

describe('origin KPI cards', () => {
  it('show loading, not undefined or zero values, before the summary query has started', () => {
    render(<Aggregates summary={{ data: undefined, isLoading: false, isFetching: false, error: null }} />)
    expect(screen.queryByText(/undefined/)).toBeNull()
    expect(screen.queryByText('0.00%')).toBeNull()
    expect(screen.getByText('Loading Origin TTFB (P50)...')).toBeTruthy()
  })

  it('renders full origin timing breakdown and miss/pass split from shared summary metrics', () => {
    render(
      <Aggregates
        summary={{
          data: {
            has_data: true,
            total_misses: 120,
            total_passes: 30,
            ottfb_p50_ms: 42.5,
            ottfb_p75_ms: 88.2,
            ottfb_p95_ms: 210.4,
            ottfb_p99_ms: 480.9,
            ottlb_p50_ms: 65.0,
            ottlb_p95_ms: 340.2,
            cdn_overhead_p50_ms: 6.4,
            origin_error_rate: 0.0025,
            obytes_p50: 4096,
          },
          isLoading: false,
          isFetching: false,
          error: null,
        }}
      />,
    )
    expect(screen.getByText('Origin TTFB (P50)')).toBeTruthy()
    expect(screen.getByText('Origin TTFB (P95)')).toBeTruthy()
    expect(screen.getByText('Origin TTFB (P99)')).toBeTruthy()
    expect(screen.getByText('480.9ms')).toBeTruthy()
    expect(screen.getByText('Origin TTLB (P95)')).toBeTruthy()
    expect(screen.getByText('340.2ms')).toBeTruthy()
    expect(screen.getByText('Median (P50): 65.0ms')).toBeTruthy()
    expect(screen.getByText('CDN Overhead (P50)')).toBeTruthy()
    expect(screen.getByText('6.4ms')).toBeTruthy()
    expect(screen.getByText('120 misses · 30 passes')).toBeTruthy()
    expect(screen.getByText('Origin Payload (P50)')).toBeTruthy()
    expect(screen.getByText('4 KB')).toBeTruthy()
  })
})
