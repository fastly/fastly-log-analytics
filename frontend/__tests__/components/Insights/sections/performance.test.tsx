import { render } from '@testing-library/react'
import { describe, expect, test } from 'vitest'
import React, { isValidElement } from 'react'
import { getPerformanceContent } from '@/components/Insights/InsightHelpModal/sections/performance'

// IDs grepped from the performance section's switch — each corresponds
// to a distinct branch of the case statement.
const KNOWN_IDS = [
  'city_latency_regressions',
  'asn_metro_performance',
  'latency_regression',
  'network_asn_health',
  'pop_latency_regression',
  'metro_delivery_degradation',
  'connection_type_mix',
  'http3_fallback',
  'payload_compression_regression',
  'timeout_split',
] as const

describe('getPerformanceContent', () => {
  test('returns null for an unknown id', () => {
    expect(getPerformanceContent('nope_not_a_real_insight')).toBeNull()
  })

  test('returns null for empty id', () => {
    expect(getPerformanceContent('')).toBeNull()
  })

  test.each(KNOWN_IDS)('returns a fully-shaped InsightContent for %s', (id) => {
    const content = getPerformanceContent(id)
    expect(content).not.toBeNull()
    if (!content) return

    expect(typeof content.title).toBe('string')
    expect(content.title.length).toBeGreaterThan(0)

    expect(isValidElement(content.icon)).toBe(true)

    expect(Array.isArray(content.fields)).toBe(true)
    expect(content.fields.length).toBeGreaterThan(0)
    for (const f of content.fields) expect(typeof f).toBe('string')
    expect(new Set(content.fields).size).toBe(content.fields.length)

    expect(content.description).toBeTruthy()
    const { container, unmount } = render(<>{content.description}</>)
    expect(container.textContent?.trim().length ?? 0).toBeGreaterThan(0)
    unmount()
  })

  test('performance arms do not require a diagram', () => {
    for (const id of KNOWN_IDS) {
      expect(getPerformanceContent(id)?.diagram).toBeUndefined()
    }
  })

  test('each known arm exposes a distinct title', () => {
    const titles = KNOWN_IDS.map((id) => getPerformanceContent(id)!.title)
    expect(new Set(titles).size).toBe(titles.length)
  })

  test('asn_metro_performance lists ASN/metro fields', () => {
    const content = getPerformanceContent('asn_metro_performance')!
    expect(content.fields).toContain('asn')
    expect(content.fields).toContain('metro')
    expect(content.fields).toContain('tcp_rtt')
  })

  test('network_asn_health surfaces low-level kernel metrics', () => {
    const content = getPerformanceContent('network_asn_health')!
    const { container, unmount } = render(<>{content.description}</>)
    expect(container.textContent).toMatch(/Packet Loss/i)
    expect(container.textContent).toMatch(/Jitter/i)
    unmount()
  })
})
