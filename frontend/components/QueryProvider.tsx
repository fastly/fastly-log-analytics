'use client'

import { HydrationBoundary, QueryClient, QueryClientProvider } from '@tanstack/react-query'
import type { DehydratedState } from '@tanstack/react-query'
import { useState } from 'react'
import dynamic from 'next/dynamic'
import { NuqsAdapter } from 'nuqs/adapters/next/app'
import { hydrateFilterStoreFromUrl } from '@/lib/urlFilterHydration'

const ReactQueryDevtools = dynamic(
  () => import('@tanstack/react-query-devtools').then(m => ({ default: m.ReactQueryDevtools })),
  { ssr: false }
)

interface QueryProviderProps {
  children: React.ReactNode
  // Optional React Query dehydrated state from a server component
  // (typically app/layout.tsx). When present, the client cache is
  // seeded on first mount so hooks like useBootstrap find data
  // already cached and skip their first network fetch entirely.
  dehydratedState?: DehydratedState | null
}

export default function QueryProvider({ children, dehydratedState }: QueryProviderProps) {
  // Lazy initializer runs synchronously on first render — i.e. BEFORE
  // child components render. By the time any page-level hook reads
  // filterStore, the URL params have been written. Without this, the
  // URL→store sync lives in useFilterUrlSync's useEffect (post-render),
  // so the first React Query keys use store defaults and any SSR'd
  // cache misses. See [lib/urlFilterHydration.ts](lib/urlFilterHydration.ts).
  useState(() => {
    hydrateFilterStoreFromUrl()
    return null
  })

  const [queryClient] = useState(() => new QueryClient({
    defaultOptions: {
      queries: {
        // staleTime: queries stay "fresh" for 30s after fetch. Repeat
        // navigations to a route within 30s skip the network entirely
        // — that's the difference between "click → instant snapshot"
        // vs "click → spinner → repaint" for revisits.
        staleTime: 30 * 1000,
        // gcTime: keep cached data in memory for 5 min after the last
        // subscriber unmounts. Without this React Query drops the
        // cache the moment a page unmounts, so navigating away and
        // back pays a cold fetch even within seconds. 5 min covers
        // typical click-back behaviour without bloating memory.
        gcTime: 5 * 60 * 1000,
        refetchOnWindowFocus: false,
        // Skip retries on 4xx (caller error — retrying just amplifies
        // the same failure into 3-4x traffic per the React Query
        // default of `retry: 3`). Allow up to 2 retries on 5xx / network
        // errors where a retry can plausibly succeed. The /api/sessions
        // CORS preflight failure used to fan one user click into 4
        // identical /api/sessions POSTs visible in HAR.
        retry: (failureCount: number, error: unknown) => {
          const status = (error as { response?: { status?: number } } | null)?.response?.status
          if (status !== undefined && status >= 400 && status < 500) return false
          return failureCount < 2
        },
      },
      mutations: {
        // Same retry-on-4xx-is-amplification rule for mutations (the
        // /api/sessions POST that powers session list refresh is a
        // mutation, not a query).
        retry: (failureCount: number, error: unknown) => {
          const status = (error as { response?: { status?: number } } | null)?.response?.status
          if (status !== undefined && status >= 400 && status < 500) return false
          return failureCount < 1
        },
      },
    },
  }))

  return (
    <QueryClientProvider client={queryClient}>
      <NuqsAdapter>
        <HydrationBoundary state={dehydratedState}>
          {children}
        </HydrationBoundary>
      </NuqsAdapter>
      {process.env.NODE_ENV === 'development' && <ReactQueryDevtools initialIsOpen={false} />}
    </QueryClientProvider>
  )
}
