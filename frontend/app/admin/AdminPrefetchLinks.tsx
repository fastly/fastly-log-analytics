'use client'

import React from 'react'
import Link from 'next/link'
import { useQueryClient } from '@tanstack/react-query'
import { client } from '@/lib/api'
import { unwrapClient } from '@/lib/api-client'
import { scoringAnalyticsCompositeOptions, scoringConfigCompositeOptions } from '@/lib/queries/scoring'
import { useMounted } from '@/hooks/useMounted'
import { useServiceStore } from '@/stores/serviceStore'
import { buttonVariants } from '@/components/ui/button'
import { UserPlus, ShieldCheck, Activity, TrendingUp, Layers, Eye, Database } from 'lucide-react'
import { queryKeys } from '@/lib/query-keys'

export function AdminPrefetchLinks({ isHighScale }: { isHighScale?: boolean } = {}) {
  const queryClient = useQueryClient()
  const mounted = useMounted()
  const storeServiceId = useServiceStore(s => s.activeServiceId)
  const services = useServiceStore(s => s.services)
  const activeServiceId = mounted ? storeServiceId : null
  const showClickHouse = isHighScale ?? services.some(s => Boolean(s.is_high_scale))

  return (
    <>
      <Link
        href={activeServiceId ? `/admin/share?service=${activeServiceId}` : '/admin/share'}
        prefetch={false}
        onMouseEnter={() => {
          queryClient.prefetchQuery({
            queryKey: queryKeys.admin.shareStatus(),
            queryFn: async ({ signal }) =>
              unwrapClient(client.GET('/api/admin/share/status', { signal })),
          })
        }}
        data-testid="open-share-dialog"
        className={buttonVariants({ variant: 'secondary', size: 'sm' })}
      >
        <UserPlus className="h-4 w-4 mr-1" /> Share Dashboard
      </Link>
      <Link
        href={activeServiceId ? `/admin/session-scoring?service=${activeServiceId}` : '/admin/session-scoring'}
        prefetch={false}
        onMouseEnter={() => {
          if (!activeServiceId) return
          queryClient.prefetchQuery(scoringAnalyticsCompositeOptions(activeServiceId, 24))
          queryClient.prefetchQuery(scoringConfigCompositeOptions(activeServiceId))
        }}
        className={buttonVariants({ variant: 'secondary', size: 'sm' })}
      >
        <ShieldCheck className="h-4 w-4 mr-1" /> Session Scoring
      </Link>
      <Link
        href={activeServiceId ? `/admin/queries?service=${activeServiceId}` : '/admin/queries'}
        prefetch={false}
        className={buttonVariants({ variant: 'secondary', size: 'sm' })}
      >
        <Activity className="h-4 w-4 mr-1" /> Live Queries
      </Link>
      <Link
        href={activeServiceId ? `/admin/trends?service=${activeServiceId}` : '/admin/trends'}
        prefetch={false}
        onMouseEnter={() => {
          queryClient.prefetchQuery({
            queryKey: queryKeys.admin.metricHistoryBatch('1h'),
            queryFn: async ({ signal }) =>
              unwrapClient(
                client.GET('/api/admin/metric-history/batch', {
                  params: { query: { since: '1h' } },
                  signal,
                }),
              ),
          })
        }}
        className={buttonVariants({ variant: 'secondary', size: 'sm' })}
      >
        <TrendingUp className="h-4 w-4 mr-1" /> Trends
      </Link>
      <Link
        href={activeServiceId ? `/admin/queue?service=${activeServiceId}` : '/admin/queue'}
        prefetch={false}
        onMouseEnter={() => {
          queryClient.prefetchQuery({
            queryKey: queryKeys.admin.celeryStatus(),
            queryFn: async ({ signal }) =>
              unwrapClient(client.GET('/api/admin/celery/status', { signal })),
          })
        }}
        className={buttonVariants({ variant: 'secondary', size: 'sm' })}
      >
        <Layers className="h-4 w-4 mr-1" /> Queue
      </Link>
      <Link
        href={activeServiceId ? `/admin/rum?service=${activeServiceId}` : '/admin/rum'}
        prefetch={false}
        className={buttonVariants({ variant: 'secondary', size: 'sm' })}
      >
        <Eye className="h-4 w-4 mr-1" /> RUM
      </Link>
      {showClickHouse && (
        <Link
          href={activeServiceId ? `/admin/clickhouse?service=${activeServiceId}` : '/admin/clickhouse'}
          prefetch={false}
          onMouseEnter={() => {
            if (!activeServiceId) return
            queryClient.prefetchQuery({
              queryKey: queryKeys.admin.clickhouseStatus(activeServiceId),
              queryFn: async ({ signal }) => {
                const { data, response } = await client.GET('/api/admin/clickhouse/status', {
                  params: { query: { service_id: activeServiceId } },
                  signal,
                })
                if (!response.ok && response.status !== 503) throw new Error(`status ${response.status}`)
                return data
              },
            })
          }}
          className={buttonVariants({ variant: 'secondary', size: 'sm' })}
        >
          <Database className="h-4 w-4 mr-1" /> ClickHouse
        </Link>
      )}
    </>
  )
}
