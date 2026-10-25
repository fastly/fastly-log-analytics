import { render } from '@testing-library/react'
import { describe, expect, test } from 'vitest'
import React, { isValidElement } from 'react'
import { getPerformanceContent } from '@/components/Insights/InsightHelpModal/sections/performance'
import { getSecurityContent } from '@/components/Insights/InsightHelpModal/sections/security'
import { getTrafficContent } from '@/components/Insights/InsightHelpModal'

const PERFORMANCE_IDS = [
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

const SECURITY_IDS = [
  'impossible_distance',
  'ua_monoculture',
  'new_probe_urls',
  'waf_signal_spikes',
  'proxy_surge',
  'botnet_grouping',
  'low_and_slow',
  'credential_enumeration',
  'content_discovery',
  'session_harvesting',
  'repeated_patterns',
] as const

const ARMS_WITH_DIAGRAM = new Set<string>(['impossible_distance', 'repeated_patterns'])

const TRAFFIC_IDS = [
  'city_surges',
  'new_city_traffic',
  'new_country_traffic',
  'asn_concentration',
  'referer_monoculture',
  'method_drift',
  'new_asn_traffic',
  'asn_hosting_shift',
] as const

describe('getPerformanceContent', () => {
  test('returns null for an unknown or empty id', () => {
    expect(getPerformanceContent('nope_not_a_real_insight')).toBeNull()
    expect(getPerformanceContent('')).toBeNull()
  })

  test.each(PERFORMANCE_IDS)('returns a fully-shaped InsightContent for %s', (id) => {
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

  test('performance arms do not require a diagram and expose distinct titles', () => {
    for (const id of PERFORMANCE_IDS) {
      expect(getPerformanceContent(id)?.diagram).toBeUndefined()
    }
    const titles = PERFORMANCE_IDS.map((id) => getPerformanceContent(id)!.title)
    expect(new Set(titles).size).toBe(titles.length)
  })

  test('asn_metro_performance lists ASN/metro fields and network_asn_health surfaces kernel metrics', () => {
    const content = getPerformanceContent('asn_metro_performance')!
    expect(content.fields).toContain('asn')
    expect(content.fields).toContain('metro')
    expect(content.fields).toContain('tcp_rtt')

    const health = getPerformanceContent('network_asn_health')!
    const { container, unmount } = render(<>{health.description}</>)
    expect(container.textContent).toMatch(/Packet Loss/i)
    expect(container.textContent).toMatch(/Jitter/i)
    unmount()
  })
})

describe('getSecurityContent', () => {
  test('returns null for an unknown or empty id', () => {
    expect(getSecurityContent('nope_not_a_real_insight')).toBeNull()
    expect(getSecurityContent('')).toBeNull()
  })

  test.each(SECURITY_IDS)('returns a fully-shaped InsightContent for %s', (id) => {
    const content = getSecurityContent(id)
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

  test('impossible_distance and repeated_patterns include diagrams; only documented arms ship one', () => {
    const idist = getSecurityContent('impossible_distance')
    expect(idist?.diagram).toBeTruthy()
    const { container: c1, unmount: u1 } = render(<>{idist!.diagram}</>)
    expect(c1.textContent).toMatch(/Speed of Light/i)
    u1()

    const rpat = getSecurityContent('repeated_patterns')
    expect(rpat?.diagram).toBeTruthy()
    expect(rpat?.fields).toEqual(['ip', 'timestamp'])
    const { container: c2, unmount: u2 } = render(<>{rpat!.diagram}</>)
    expect(c2.textContent).toMatch(/Automated script/i)
    expect(c2.textContent).toMatch(/Human browsing/i)
    u2()

    for (const id of SECURITY_IDS) {
      const diagram = getSecurityContent(id)?.diagram
      if (ARMS_WITH_DIAGRAM.has(id)) {
        expect(diagram).toBeTruthy()
      } else {
        expect(diagram).toBeUndefined()
      }
    }
    const titles = SECURITY_IDS.map((id) => getSecurityContent(id)!.title)
    expect(new Set(titles).size).toBe(titles.length)
  })
})

describe('getTrafficContent', () => {
  test('returns null for an unknown or empty id', () => {
    expect(getTrafficContent('nope_not_a_real_insight')).toBeNull()
    expect(getTrafficContent('')).toBeNull()
  })

  test.each(TRAFFIC_IDS)('returns a fully-shaped InsightContent for %s', (id) => {
    const content = getTrafficContent(id)
    expect(content).not.toBeNull()
    if (!content) return

    expect(typeof content.title).toBe('string')
    expect(content.title.length).toBeGreaterThan(0)
    expect(isValidElement(content.icon)).toBe(true)
    expect(Array.isArray(content.fields)).toBe(true)
    expect(content.fields.length).toBeGreaterThan(0)
    for (const f of content.fields) expect(typeof f).toBe('string')

    expect(content.description).toBeTruthy()
    const { container, unmount } = render(<>{content.description}</>)
    expect(container.textContent?.trim().length ?? 0).toBeGreaterThan(0)
    unmount()
  })

  test('traffic arms do not require a diagram and branch new_city_traffic vs new_country_traffic', () => {
    for (const id of TRAFFIC_IDS) {
      expect(getTrafficContent(id)?.diagram).toBeUndefined()
    }
    const city = getTrafficContent('new_city_traffic')!
    const country = getTrafficContent('new_country_traffic')!
    expect(city.title).toMatch(/City/i)
    expect(country.title).toMatch(/Country/i)
    expect(city.title).not.toBe(country.title)
    expect(city.fields).toContain('city')
    expect(country.fields).toContain('country')
    expect(getTrafficContent('city_surges')!.fields).toEqual(['city'])
    expect(getTrafficContent('asn_concentration')!.fields).toEqual(['asn'])
  })
})
