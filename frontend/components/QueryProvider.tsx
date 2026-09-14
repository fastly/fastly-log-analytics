'use client'

import { HydrationBoundary, QueryClient, QueryClientProvider } from '@tanstack/react-query'
import type { DehydratedState } from '@tanstack/react-query'
import { useState } from 'react'
import dynamic from 'next/dynamic'
import { NuqsAdapter } from 'nuqs/adapters/next/app'
import { hydrateFilterStoreFromUrl } from '@/lib/urlFilterHydration'

// Run URL→filterStore hydration at MODULE LOAD on the client, before
// any React component renders. The previous useState-initializer
// approach ran inside QueryProvider's first render — same render tick
// as the page's first useFilterStore() call. On routes like /security
// that's a race: the page subscribed to the default 24h startTime,
// fired its first /api/security/aggregates against that key, then the
// hydrate's write-to-store notified the subscriber and a SECOND fetch
// fired against the URL-derived window — visible in HARs as
// hits_per_load = 2 with the first request returning stale data.
// Module-load hydration writes the store before any Zustand subscriber
// is established.
if (typeof window !== 'undefined') {
  hydrateFilterStoreFromUrl()
}

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

  const [queryClient] = useState(() => new QueryClient({
    defaultOptions: {
      queries: {
        // staleTime: queries stay "fresh" for 60s after fetch. Repeat
        // navigations to a route within 60s skip the network entirely
        // — that's the difference between "click → instant snapshot"
        // vs "click → spinner → repaint" for revisits.
        staleTime: 5 * 60 * 1000,
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
        //
        // E-5 (audit): 503 with err.busy = true (DuckDB pool saturation)
        // gets one extra retry (3 total) because the failure mode is
        // explicitly transient — the backend is telling us "try again
        // shortly" rather than "this is broken". Exponential backoff
        // (1s → 2s → 4s, 8s cap) gives the pool room to drain instead
        // of hammering it with three retries in <300ms.
        retry: (failureCount: number, error: unknown) => {
          // NO_SERVICE sentinel from lib/api.ts: retrying re-throws the
          // same sentinel until the activeService store transitions and
          // the queryKey changes (which is what refires the query). Retry
          // is pure waste and visibly extends the "loading" interval on
          // service-switch.
          if ((error as { code?: string } | null)?.code === 'NO_SERVICE') return false
          const status = (error as { response?: { status?: number } } | null)?.response?.status
          if (status !== undefined && status >= 400 && status < 500) return false
          const busy = (error as { busy?: boolean } | null)?.busy === true
          if (busy) return failureCount < 3
          return failureCount < 2
        },
        retryDelay: (attemptIndex: number) =>
          Math.min(1000 * 2 ** attemptIndex, 8000),
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
