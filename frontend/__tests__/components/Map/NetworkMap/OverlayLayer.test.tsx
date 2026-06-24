/**
 * OverlayLayer hosts two purely-presentational helpers consumed by the
 * NetworkMap tooltip layer: ``formatMetricValue`` (a typed switchboard
 * that scales raw metric numbers into human-readable units) and
 * ``MapTooltip`` (a fixed-position tooltip card whose horizontal anchor
 * flips when the cursor is in the right 30% of the viewport).
 *
 * The component itself was 0%-covered. These tests pin (1) every
 * branch of formatMetricValue — including the null/undefined fallback,
 * the per-metric unit conversions, and the throughput tier ladder
 * (Gbps/Mbps/Kbps/bps) — and (2) MapTooltip's render output: city +
 * country + requests row, the flipLeft viewport-edge branch, and the
 * "hide redundant health-score row when metric === health_score"
 * conditional.
 *
 * @vitest-environment jsdom
 */
import { render, screen } from '@testing-library/react'
import { describe, it, expect } from 'vitest'
import React from 'react'

import { formatMetricValue, MapTooltip, type TooltipInfo } from '@/components/Map/NetworkMap/OverlayLayer'

describe('formatMetricValue', () => {
  it('returns em dash for null/undefined values', () => {
    expect(formatMetricValue(null, 'health_score')).toBe('—')
    expect(formatMetricValue(undefined, 'rtt_med_us')).toBe('—')
  })

  it('formats health_score as NN/100', () => {
    expect(formatMetricValue(87.4, 'health_score')).toBe('87/100')
    expect(formatMetricValue(100, 'health_score')).toBe('100/100')
  })

  it('converts rtt_med_us microseconds to milliseconds with one decimal', () => {
    // 12_345 us / 1000 = 12.345 → toFixed(1) → "12.3 ms"
    expect(formatMetricValue(12345, 'rtt_med_us')).toBe('12.3 ms')
    expect(formatMetricValue(0, 'rtt_med_us')).toBe('0.0 ms')
  })

  it('formats avg_ploss as a percent with two decimals', () => {
    // 0.0123 → 1.23%
    expect(formatMetricValue(0.0123, 'avg_ploss')).toBe('1.23%')
    expect(formatMetricValue(0, 'avg_ploss')).toBe('0.00%')
  })

  it('formats error_pct directly with two decimals', () => {
    expect(formatMetricValue(4.5678, 'error_pct')).toBe('4.57%')
  })

  it('scales throughput_bps across the Gbps/Mbps/Kbps/bps ladder', () => {
    expect(formatMetricValue(2.5e9, 'throughput_bps')).toBe('2.5 Gbps')
    expect(formatMetricValue(3.4e6, 'throughput_bps')).toBe('3.4 Mbps')
    expect(formatMetricValue(7.8e3, 'throughput_bps')).toBe('7.8 Kbps')
    expect(formatMetricValue(500, 'throughput_bps')).toBe('500 bps')
  })

  it('falls back to String(val) for unknown metric ids', () => {
    expect(formatMetricValue(42, 'unknown_metric')).toBe('42')
  })
})

describe('MapTooltip', () => {
  const baseInfo: TooltipInfo = {
    clientX: 100,
    clientY: 200,
    city: 'Sydney',
    country: 'Australia',
    cityData: { reqs: 12345, health_score: 92, rtt_med_us: 18000 },
  }

  it('renders city, country, and a localized requests count', () => {
    // Default window width in jsdom is 1024; clientX=100 stays on the
    // right-anchored (non-flipped) side, which is fine for this case.
    render(<MapTooltip info={baseInfo} metric="rtt_med_us" />)

    expect(screen.getByText('Sydney')).toBeInTheDocument()
    expect(screen.getByText('Australia')).toBeInTheDocument()
    expect(screen.getByText('Median RTT')).toBeInTheDocument()
    expect(screen.getByText('18.0 ms')).toBeInTheDocument()
    // toLocaleString in en-US groups with commas.
    expect(screen.getByText('12,345')).toBeInTheDocument()
    // Non-health-score metric → the extra Health Score row also renders.
    expect(screen.getByText('Health Score')).toBeInTheDocument()
    expect(screen.getByText('92/100')).toBeInTheDocument()
  })

  it('flips the tooltip to the left when cursor is in the right 30% of viewport', () => {
    // jsdom defaults to innerWidth 1024 → threshold = 716.8.
    // 900 > 716.8 triggers flipLeft.
    const info: TooltipInfo = { ...baseInfo, clientX: 900 }
    const { container } = render(<MapTooltip info={info} metric="error_pct" />)

    const tooltip = container.firstChild as HTMLElement
    expect(tooltip).not.toBeNull()
    // flipLeft branch: left = clientX - 14 (= 886), transform translate -100%/-100%.
    expect(tooltip.style.left).toBe('886px')
    expect(tooltip.style.transform).toBe('translate(-100%, -100%)')

    // Sanity-check the non-flipped branch on the same metric for contrast.
    const { container: c2 } = render(<MapTooltip info={{ ...info, clientX: 100 }} metric="error_pct" />)
    const tooltip2 = c2.firstChild as HTMLElement
    // non-flipped: left = clientX + 14 (= 114), transform translateY only.
    expect(tooltip2.style.left).toBe('114px')
    expect(tooltip2.style.transform).toBe('translateY(-100%)')
  })

  it('hides the duplicate health-score row when the selected metric IS health_score', () => {
    render(<MapTooltip info={baseInfo} metric="health_score" />)

    // The metric label row is "Health Score", which appears once as the
    // primary metric — the conditional secondary row must NOT render,
    // so getAllByText should yield exactly one match.
    const labels = screen.getAllByText('Health Score')
    expect(labels).toHaveLength(1)
    // The primary value row prints "92/100" via formatMetricValue.
    expect(screen.getByText('92/100')).toBeInTheDocument()
  })
})
