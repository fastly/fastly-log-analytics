import { afterEach, describe, expect, it } from 'vitest'
import { setCloudModeRuntime } from '@/lib/cloud-engine/mode'
import { allowedLogPeriods, effectiveLogPeriod } from './serviceDefaults'

const OPTIONS = ['1', '5', '10', '30', '60', '120', '300'] as const

afterEach(() => setCloudModeRuntime(false))

describe('log period floor', () => {
  it('keeps sub-minute periods for self-hosted installs', () => {
    expect(allowedLogPeriods(OPTIONS)).toEqual(OPTIONS)
    expect(effectiveLogPeriod(10)).toBe(10)
    expect(effectiveLogPeriod(undefined)).toBe(10)
  })

  it('offers and sends nothing below one minute in Cloud mode', () => {
    setCloudModeRuntime(true)
    expect(allowedLogPeriods(OPTIONS)).toEqual(['60', '120', '300'])
    expect(effectiveLogPeriod(10)).toBe(60)
    expect(effectiveLogPeriod(undefined)).toBe(60)
    expect(effectiveLogPeriod(300)).toBe(300)
  })
})
