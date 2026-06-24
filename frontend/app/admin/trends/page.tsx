import { HydrationBoundary } from '@tanstack/react-query'

import { fetchMetricHistoryBatchServerSide } from '@/lib/ssr/admin_trends'
import { seedDehydratedState } from '@/lib/ssr/seed'
import AdminTrendsClient from './_sections/AdminTrendsClient'

// Per-request RSC shell for /admin/trends. Pre-fetches the default 1-hour
// metric-history batch server-side and dehydrates it into a HydrationBoundary
// so the 7 sparkline cards render from cache on first paint instead of paying
// the client round-trip + skeleton flash on cold load.
//
// The seed is consumed 100% of the time: AdminTrendsClient's window selector
// is useState('1h') with no searchParams, so its cold queryKey is always
// ['admin','metric-history-batch','1h'] (see the SEED-KEY PIN comment there).
//
// force-dynamic because the fetcher reads cookies + the Caddy marker via
// next/headers — the seed must be per-request and audience-scoped.
//
// Failure path mirrors alerts/logs: the SSR helper returns null on
// timeout / non-2xx (incl. the analyst-branch 401/403) / malformed body,
// and AdminTrendsClient's useQuery picks up unchanged.
export const dynamic = 'force-dynamic'

export default async function AdminTrendsPage() {
  const batch = await fetchMetricHistoryBatchServerSide()

  // Must match AdminTrendsClient's useQuery key for the cold '1h' window.
  const dehydratedState = seedDehydratedState(['admin', 'metric-history-batch', '1h'], batch)

  return (
    <HydrationBoundary state={dehydratedState}>
      <AdminTrendsClient />
    </HydrationBoundary>
  )
}
