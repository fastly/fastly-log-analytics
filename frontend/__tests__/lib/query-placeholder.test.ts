import { describe, expect, it } from 'vitest'
import type { QueryKey } from '@tanstack/react-query'
import { keepPreviousWithinService } from '@/lib/query-placeholder'

const prev = (key: QueryKey) => ({ queryKey: key })

describe('keepPreviousWithinService', () => {
  it('keeps previous data when only non-service key parts change', () => {
    const fn = keepPreviousWithinService(['logs', 'query', 'svc-a', 'p2'])
    expect(fn('rows-a', prev(['logs', 'query', 'svc-a', 'p1']))).toBe('rows-a')
  })

  it('drops previous data when the service id changes', () => {
    const fn = keepPreviousWithinService(['logs', 'query', 'svc-b'])
    expect(fn('rows-a', prev(['logs', 'query', 'svc-a']))).toBeUndefined()
  })

  it('returns undefined with no previous query', () => {
    expect(keepPreviousWithinService(['a', 'b', 'c'])(undefined, undefined)).toBeUndefined()
  })

  it('keeps previous data when keys are too short to carry a service id', () => {
    const fn = keepPreviousWithinService(['a', 'b'])
    expect(fn('x', prev(['a', 'c']))).toBe('x')
  })

  it('honours a custom service id index', () => {
    const fn = keepPreviousWithinService(['a', 'svc-b'], 1)
    expect(fn('x', prev(['a', 'svc-a']))).toBeUndefined()
  })
})
