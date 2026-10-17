import { describe, expect, test } from 'vitest'
import { suggestionsFromData, type ContentSecurityData } from '@/app/streaming/_sections/tabs/securityData'

const data = (top_countries: { value: string; requests: number }[]) => ({ top_countries }) as unknown as ContentSecurityData

describe('country suggestions', () => {
  test('lists every country, even with no data', () => {
    const list = suggestionsFromData(undefined).country
    expect(list.length).toBeGreaterThanOrEqual(249)
    expect(list.map((c) => c.value)).toEqual(expect.arrayContaining(['US', 'CN', 'BR', 'AX']))
  })

  test('is alphabetical by name and carries each country\'s requests', () => {
    const list = suggestionsFromData(
      data([
        { value: 'BR', requests: 50 },
        { value: 'US', requests: 900 },
        { value: 'FR', requests: 50 },
      ]),
    ).country
    const names = list.map((c) => c.label as string)
    expect(names).toEqual([...names].sort((a, b) => a.localeCompare(b, 'en')))
    expect(list.find((c) => c.value === 'US')).toMatchObject({ label: 'United States', requests: 900 })
    expect(list.find((c) => c.value === 'BR')?.requests).toBe(50)
    expect(list.find((c) => c.value === 'NZ')?.requests).toBeUndefined()
  })

  test('keeps an observed code that is not in the ISO list', () => {
    const list = suggestionsFromData(data([{ value: 'XK', requests: 5 }])).country
    expect(list.find((c) => c.value === 'XK')).toMatchObject({ requests: 5 })
  })

  test('does not list a code twice', () => {
    const values = suggestionsFromData(data([{ value: 'US', requests: 1 }])).country.map((c) => c.value)
    expect(new Set(values).size).toBe(values.length)
  })
})
