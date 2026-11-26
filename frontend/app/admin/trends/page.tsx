import { Suspense } from 'react'
import { HydrationBoundary } from '@tanstack/react-query'

import { fetchMetricHistoryBatchServerSide, seedDehydratedState } from '@/lib/ssr/upstream'
import AdminTrendsLoading from './loading'
import { AdminTrendsClient } from '../_sections/OperationsOverview'
import { queryKeys } from '@/lib/api'

// Per-request RSC shell for /admin/trends. Streams the static AdminTrendsLoading
// shell immediately via <Suspense> while pre-fetching the default 1-hour
// metric-history batch server-side inside AdminTrendsPageContent and dehydrating
// it into a HydrationBoundary so the sparkline cards hydrate from cache instead
// of paying an extra client round-trip once the stream completes.
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

export default function AdminTrendsPage() {
  return (
    <Suspense fallback={<AdminTrendsLoading />}>
      <AdminTrendsPageContent />
    </Suspense>
  )
}

async function AdminTrendsPageContent() {
  const batch = await fetchMetricHistoryBatchServerSide()

  // Must match AdminTrendsClient's useQuery key for the cold '1h' window.
  const dehydratedState = seedDehydratedState(queryKeys.admin.metricHistoryBatch('1h'), batch)

  return (
    <HydrationBoundary state={dehydratedState}>
      <AdminTrendsClient />
    </HydrationBoundary>
  )
}
