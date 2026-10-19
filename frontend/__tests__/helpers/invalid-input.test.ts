import { describe, expect, it } from 'vitest'
import { invalidInput } from './invalid-input'

function describeFilters(filters: string[]): string {
  return typeof filters
}

describe('invalidInput', () => {
  it('hands the exact malformed value through to the callee', () => {
    const bad = { not: 'an array' }
    expect(invalidInput(bad)).toBe(bad)
    expect(describeFilters(invalidInput(bad))).toBe('object')
    expect(describeFilters(invalidInput('str'))).toBe('string')
  })
})
