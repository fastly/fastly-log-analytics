import { render } from '@testing-library/react'
import { describe, expect, test } from 'vitest'
import React, { isValidElement } from 'react'
import { getSecurityContent } from '@/components/Insights/InsightHelpModal/sections/security'

// IDs grepped from the security section's switch — each is a distinct
// branch we exercise to assert the shape returned by getSecurityContent.
const KNOWN_IDS = [
  'impossible_distance',
  'ua_monoculture',
  'new_probe_urls',
  'waf_signal_spikes',
  'proxy_surge',
  'botnet_grouping',
  'low_and_slow',
  'repeated_patterns',
] as const

// Arms that intentionally ship an explanatory diagram. Every other arm
// must leave `diagram` undefined (asserted below).
const ARMS_WITH_DIAGRAM = new Set<string>(['impossible_distance', 'repeated_patterns'])

describe('getSecurityContent', () => {
  test('returns null for an unknown id', () => {
    expect(getSecurityContent('nope_not_a_real_insight')).toBeNull()
  })

  test('returns null for empty id', () => {
    expect(getSecurityContent('')).toBeNull()
  })

  test.each(KNOWN_IDS)('returns a fully-shaped InsightContent for %s', (id) => {
    const content = getSecurityContent(id)
    expect(content).not.toBeNull()
    if (!content) return // narrow for TS

    // .title — non-empty string
    expect(typeof content.title).toBe('string')
    expect(content.title.length).toBeGreaterThan(0)

    // .icon — React element (lucide icon)
    expect(isValidElement(content.icon)).toBe(true)

    // .fields — non-empty array of strings, no duplicates
    expect(Array.isArray(content.fields)).toBe(true)
    expect(content.fields.length).toBeGreaterThan(0)
    for (const f of content.fields) expect(typeof f).toBe('string')
    expect(new Set(content.fields).size).toBe(content.fields.length)

    // .description — React node that renders SOMETHING when mounted
    expect(content.description).toBeTruthy()
    const { container, unmount } = render(<>{content.description}</>)
    expect(container.textContent?.trim().length ?? 0).toBeGreaterThan(0)
    unmount()
  })

  test('impossible_distance includes a diagram', () => {
    const content = getSecurityContent('impossible_distance')
    expect(content?.diagram).toBeTruthy()
    const { container, unmount } = render(<>{content!.diagram}</>)
    expect(container.textContent).toMatch(/Speed of Light/i)
    unmount()
  })

  test('repeated_patterns includes a cadence diagram contrasting script vs human', () => {
    const content = getSecurityContent('repeated_patterns')
    expect(content?.diagram).toBeTruthy()
    expect(content?.fields).toEqual(['ip', 'timestamp'])
    const { container, unmount } = render(<>{content!.diagram}</>)
    expect(container.textContent).toMatch(/Automated script/i)
    expect(container.textContent).toMatch(/Human browsing/i)
    unmount()
  })

  test('only documented arms ship a diagram', () => {
    // The diagram is documented as optional on InsightContent — make
    // sure no branch accidentally starts (or stops) setting it without
    // an accompanying test update.
    for (const id of KNOWN_IDS) {
      const diagram = getSecurityContent(id)?.diagram
      if (ARMS_WITH_DIAGRAM.has(id)) {
        expect(diagram).toBeTruthy()
      } else {
        expect(diagram).toBeUndefined()
      }
    }
  })

  test('each known arm exposes a distinct title', () => {
    const titles = KNOWN_IDS.map((id) => getSecurityContent(id)!.title)
    expect(new Set(titles).size).toBe(titles.length)
  })
})
