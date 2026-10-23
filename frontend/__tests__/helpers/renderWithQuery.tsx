import * as React from 'react'
import { render, type RenderOptions, type RenderResult } from '@testing-library/react'
import type { QueryClientConfig } from '@tanstack/react-query'
import { QueryClientProvider } from '@tanstack/react-query'

import { createTestQueryClient, makeQueryWrapper } from './query'

export { createTestQueryClient, makeQueryWrapper }

export interface RenderWithQueryOptions extends Omit<RenderOptions, 'wrapper'> {
  queryOptions?: QueryClientConfig['defaultOptions']
  withNuqs?: boolean
}

/**
 * Shared helper that renders `ui` inside a test `QueryClientProvider` with
 * `queries.retry = false` and `mutations.retry = false` by default.
 */
export function renderWithQuery(
  ui: React.ReactElement,
  options?: RenderWithQueryOptions
): RenderResult & { queryClient: ReturnType<typeof createTestQueryClient> } {
  const { queryOptions, withNuqs = false, ...renderOptions } = options ?? {}
  const queryClient = createTestQueryClient({
    mutations: { retry: false, ...queryOptions?.mutations },
    ...queryOptions,
    queries: { retry: false, gcTime: 0, ...queryOptions?.queries },
  })
  if (withNuqs) {
    const Wrapper = makeQueryWrapper(queryClient)
    return {
      queryClient,
      ...render(ui, { wrapper: Wrapper, ...renderOptions }),
    }
  }
  return {
    queryClient,
    ...render(<QueryClientProvider client={queryClient}>{ui}</QueryClientProvider>, renderOptions),
  }
}

export const renderWithQueryClient = renderWithQuery
