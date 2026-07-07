import { render } from '@testing-library/react'
import { describe, expect, test } from 'vitest'
import React, { isValidElement } from 'react'
import { getTrafficContent } from '@/components/Insights/InsightHelpModal/sections/traffic'

// IDs grepped from the traffic section's switch. Note that
// 'new_city_traffic' and 'new_country_traffic' share a case fall-through
// arm — we cover both because the returned content differs (title +
// fields branch on the id at runtime).
const KNOWN_IDS = [
  'city_surges',
  'new_city_traffic',
  'new_country_traffic',
  'asn_concentration',
  'referer_monoculture',
  'method_drift',
  'new_asn_traffic',
] as const

describe('getTrafficContent', () => {
  test('returns null for an unknown id', () => {
    expect(getTrafficContent('nope_not_a_real_insight')).toBeNull()
  })

  test('returns null for empty id', () => {
    expect(getTrafficContent('')).toBeNull()
  })

  test.each(KNOWN_IDS)('returns a fully-shaped InsightContent for %s', (id) => {
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

  test('traffic arms do not require a diagram', () => {
    for (const id of KNOWN_IDS) {
      expect(getTrafficContent(id)?.diagram).toBeUndefined()
    }
  })

  test('new_city_traffic vs new_country_traffic — title and field branch', () => {
    const city = getTrafficContent('new_city_traffic')!
    const country = getTrafficContent('new_country_traffic')!

    expect(city.title).toMatch(/City/i)
    expect(country.title).toMatch(/Country/i)
    expect(city.title).not.toBe(country.title)

    expect(city.fields).toContain('city')
    expect(country.fields).toContain('country')
  })

  test('city_surges uses the city field', () => {
    expect(getTrafficContent('city_surges')!.fields).toEqual(['city'])
  })

  test('asn_concentration uses the asn field', () => {
    expect(getTrafficContent('asn_concentration')!.fields).toEqual(['asn'])
  })
})
