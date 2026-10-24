import * as React from 'react'
import { render, type RenderOptions, type RenderResult } from '@testing-library/react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import type { QueryClientConfig } from '@tanstack/react-query'
import { NuqsTestingAdapter } from 'nuqs/adapters/testing'

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

/**
 * A QueryClientProvider wrapper bound to `qc`, for renderHook/render. Includes a
 * nuqs testing adapter (seeded from the jsdom URL) so components that mount
 * nuqs hooks, e.g. ReportLayout's view/metric deep-link consumer, render.
 */
export function makeQueryWrapper(qc: QueryClient) {
  return function QueryWrapper({ children }: { children: React.ReactNode }) {
    return (
      <NuqsTestingAdapter searchParams={window.location.search}>
        <QueryClientProvider client={qc}>{children}</QueryClientProvider>
      </NuqsTestingAdapter>
    )
  }
}

interface RenderWithQueryOptions extends RenderOptions {
  queryClient?: QueryClient
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
): RenderResult & { queryClient: QueryClient } {
  const {
    queryClient: providedClient,
    queryOptions,
    withNuqs = false,
    wrapper: InnerWrapper,
    ...renderOptions
  } = options ?? {}
  const queryClient =
    providedClient ??
    createTestQueryClient({
      mutations: { retry: false, ...queryOptions?.mutations },
      ...queryOptions,
      queries: { retry: false, gcTime: 0, ...queryOptions?.queries },
    })
  const Wrapper = ({ children }: { children: React.ReactNode }) => {
    const content = InnerWrapper ? <InnerWrapper>{children}</InnerWrapper> : children
    if (withNuqs) {
      return (
        <NuqsTestingAdapter searchParams={window.location.search}>
          <QueryClientProvider client={queryClient}>{content}</QueryClientProvider>
        </NuqsTestingAdapter>
      )
    }
    return <QueryClientProvider client={queryClient}>{content}</QueryClientProvider>
  }
  return {
    queryClient,
    ...render(ui, { wrapper: Wrapper, ...renderOptions }),
  }
}

export const renderWithQueryClient = renderWithQuery
