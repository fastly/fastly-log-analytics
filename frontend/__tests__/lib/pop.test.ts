import { describe, it, expect } from 'vitest'

import { formatPopGeo } from '@/lib/pop'

describe('formatPopGeo', () => {
  it('formats a US PoP as "City, ST - USA"', () => {
    expect(formatPopGeo({ city: 'Denver', region: 'co', country: 'us' })).toBe('Denver, CO - USA')
    expect(formatPopGeo({ city: 'Ashburn (Metro)', region: 'va', country: 'us' })).toBe('Ashburn, VA - USA')
  })

  it('omits the state for non-US PoPs (2-part shield)', () => {
    expect(formatPopGeo({ city: 'London', region: '', country: 'uk' })).toBe('London - UK')
    expect(formatPopGeo({ city: 'Frankfurt', region: '', country: 'de' })).toBe('Frankfurt - DE')
  })

  it('drops a region that just repeats the city', () => {
    expect(formatPopGeo({ city: 'Hyderabad', region: 'hyderabad', country: 'in' })).toBe('Hyderabad - IN')
    expect(formatPopGeo({ city: 'Sao Paulo', region: 'saopaulo', country: 'br' })).toBe('Sao Paulo - BR')
  })

  it('keeps a real state that differs from the city', () => {
    expect(formatPopGeo({ city: 'New York City', region: 'ny', country: 'us' })).toBe('New York City, NY - USA')
  })

  it('degrades gracefully for missing / partial data', () => {
    expect(formatPopGeo(undefined)).toBe('')
    expect(formatPopGeo(null)).toBe('')
    expect(formatPopGeo({ city: '', region: 'ca', country: 'us' })).toBe('')
    expect(formatPopGeo({ city: 'Palo Alto', region: '', country: '' })).toBe('Palo Alto')
  })
})
