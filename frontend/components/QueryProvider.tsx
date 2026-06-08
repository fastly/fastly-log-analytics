'use client'

import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { useState } from 'react'
import dynamic from 'next/dynamic'

const ReactQueryDevtools = dynamic(
  () => import('@tanstack/react-query-devtools').then(m => ({ default: m.ReactQueryDevtools })),
  { ssr: false }
)

export default function QueryProvider({ children }: { children: React.ReactNode }) {
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
      },
    },
  }))

  return (
    <QueryClientProvider client={queryClient}>
      {children}
      {process.env.NODE_ENV === 'development' && <ReactQueryDevtools initialIsOpen={false} />}
    </QueryClientProvider>
  )
}
