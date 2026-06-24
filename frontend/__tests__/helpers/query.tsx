import * as React from 'react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import type { QueryClientConfig } from '@tanstack/react-query'

/**
 * Shared QueryClient factory for tests. Forces `queries.retry = false` (tests
 * must not retry on the first failed fetch) and lets each call site layer on
 * its own gcTime/staleTime/mutations via `overrides`, so the resulting client
 * is identical to the hand-rolled ones these replaced.
 *
 * The spread order keeps any caller-supplied `mutations` (and other
 * defaultOptions) while merging `retry: false` ahead of the caller's own
 * `queries` keys.
 */
export function createTestQueryClient(overrides?: QueryClientConfig['defaultOptions']): QueryClient {
  return new QueryClient({
    defaultOptions: {
      ...overrides,
      queries: { retry: false, ...overrides?.queries },
    },
  })
}

/** A QueryClientProvider wrapper bound to `qc`, for renderHook/render. */
export function makeQueryWrapper(qc: QueryClient) {
  return function QueryWrapper({ children }: { children: React.ReactNode }) {
    return React.createElement(QueryClientProvider, { client: qc }, children)
  }
}
