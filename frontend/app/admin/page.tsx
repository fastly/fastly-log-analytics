'use client'
import React from 'react'
import Link from 'next/link'
import { useQueryClient } from '@tanstack/react-query'
import { client } from '@/lib/api'
import { useServiceStore } from '@/stores/serviceStore'
import { SystemHealthCard } from "@/components/SystemHealthCard"
import { buttonVariants } from '@/components/ui/button'
import { PageHeader } from '@/components/ui/page-header'
import { UserPlus, ShieldCheck, Activity } from 'lucide-react'

import { ServicesTable } from './_sections/ServicesTable'
import { GlobalSettings, PricingSettings } from './_sections/GlobalSettings'
import { OperationsOverview } from './_sections/OperationsOverview'

export default function AdminPage() {
  const queryClient = useQueryClient()
  const { activeServiceId } = useServiceStore()

  return (
    <div className="space-y-6">
      <PageHeader
        title="Admin"
        description="Manage your global settings, Fastly services, and log ingestion pipelines."
      >
        {/* Navigation chips for sibling admin pages. These used to live
            next to the "Add Service" button in the Service Management
            section, which conflated "act on this service list" with
            "go somewhere else" — and the cluster of three buttons made
            it ambiguous which one performed the destructive action.
            Moving them up to the PageHeader's action slot establishes
            "here's where you switch between admin sub-pages" as a
            top-of-page navigation pattern. */}
        {/* `secondary` variant gives these a visible filled background so
            they read as obviously-clickable nav buttons on a white page.
            The previous `outline` variant rendered as white-on-white and
            only revealed itself on hover, making the slot look empty. */}
        <Link
          href="/admin/share"
          // Drop the mount-time RSC prefetch — paired with the two
          // sibling nav Links it fires 3 unsolicited `?_rsc=` round-trips
          // every time /admin renders. Next still prefetches the route
          // automatically on hover (the user's mouse is over the Link by
          // then anyway), and the onMouseEnter hook below already warms
          // the destination's data query in parallel — so click-latency
          // is unchanged in practice.
          prefetch={false}
          onMouseEnter={() => {
            // Warm the share-status query so by the time the click
            // resolves, /admin/share's useQuery hits a fresh cache
            // entry instead of paying a ~300ms fetch round-trip.
            // staleTime=5s on the destination's useQuery means the
            // prefetched payload counts as fresh for the click that
            // immediately follows.
            queryClient.prefetchQuery({
              queryKey: ['admin', 'share', 'status'],
              queryFn: async ({ signal }) => {
                const { data, response } = await client.GET('/api/admin/share/status' as any, { signal, })
                if (!response.ok) throw new Error(`status ${response.status}`)
                return data
              },
            })
          }}
          data-testid="open-share-dialog"
          className={buttonVariants({ variant: 'secondary', size: 'sm' })}
        >
          <UserPlus className="h-4 w-4 mr-1" /> Share Dashboard
        </Link>
        <Link
          href="/admin/session-scoring"
          prefetch={false}
          onMouseEnter={() => {
            if (!activeServiceId) return
            // Warm the two composite queries the destination page fires
            // on mount. Pre-fix this warmed ['scoring-status', ...], but
            // the page actually reads scoring-status via the config
            // composite — so the prefetch was overwritten before any
            // panel could use it, and the page showed `compositesLoading`
            // skeleton on click. Matching the composite keys + default
            // since_hours=24 (the page's initial useState) means the
            // composites are warm on mount → no skeleton flash, same
            // pattern as the Share Dashboard link above.
            queryClient.prefetchQuery({
              queryKey: ['scoring-analytics-composite', activeServiceId, 24],
              queryFn: async ({ signal }) => {
                const { data, response } = await client.GET(
                  '/api/services/{service_id}/scoring/analytics' as any,
                  {
                    params: {
                      path: { service_id: activeServiceId },
                      query: { since_hours: 24 },
                    },
                    signal,
                  } as any,
                )
                if (!response.ok) throw new Error(`status ${response.status}`)
                return data
              },
            })
            queryClient.prefetchQuery({
              queryKey: ['scoring-config-composite', activeServiceId],
              queryFn: async ({ signal }) => {
                const { data, response } = await client.GET(
                  '/api/services/{service_id}/scoring/config' as any,
                  {
                    params: { path: { service_id: activeServiceId }, signal } as any,
                  } as any,
                )
                if (!response.ok) throw new Error(`status ${response.status}`)
                return data
              },
            })
          }}
          className={buttonVariants({ variant: 'secondary', size: 'sm' })}
        >
          <ShieldCheck className="h-4 w-4 mr-1" /> Session Scoring
        </Link>
        <Link
          href="/admin/queries"
          prefetch={false}
          className={buttonVariants({ variant: 'secondary', size: 'sm' })}
        >
          <Activity className="h-4 w-4 mr-1" /> Live Queries
        </Link>
      </PageHeader>

      <OperationsOverview />

      <ServicesTable />

      <SystemHealthCard />

      <GlobalSettings />

      <PricingSettings />
    </div>
  )
}
