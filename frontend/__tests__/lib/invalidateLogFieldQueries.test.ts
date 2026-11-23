import { QueryClient } from '@tanstack/react-query'
import { describe, expect, it } from 'vitest'
import { invalidateLogFieldQueries, queryKeys } from '@/lib/apiQueryKeys'

describe('invalidateLogFieldQueries', () => {
  it('refreshes everything derived from the log field settings, bootstrap included, and nothing else', () => {
    const client = new QueryClient()
    const keys = {
      bootstrap: queryKeys.bootstrap(),
      services: queryKeys.services(),
      customFields: ['custom-fields', 'svc-1'],
      catalog: queryKeys.logFieldsCatalog('svc-1'),
      unrelated: ['assets', 'aggregates', 'svc-1'],
    }
    for (const key of Object.values(keys)) client.setQueryData(key, { seeded: true })

    invalidateLogFieldQueries(client, 'svc-1')

    const invalidated = (key: readonly unknown[]) => client.getQueryState(key)?.isInvalidated
    expect(invalidated(keys.bootstrap)).toBe(true)
    expect(invalidated(keys.services)).toBe(true)
    expect(invalidated(keys.customFields)).toBe(true)
    expect(invalidated(keys.catalog)).toBe(true)
    expect(invalidated(keys.unrelated)).toBe(false)
  })
})
