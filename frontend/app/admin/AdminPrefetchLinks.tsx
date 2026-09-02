'use client'

import React from 'react'
import Link from 'next/link'
import { useQueryClient } from '@tanstack/react-query'
import { client } from '@/lib/api'
import { useServiceStore } from '@/stores/serviceStore'
import { buttonVariants } from '@/components/ui/button'
import { UserPlus, ShieldCheck, Activity, TrendingUp, Layers } from 'lucide-react'

export function AdminPrefetchLinks() {
  const queryClient = useQueryClient()
  const activeServiceId = useServiceStore(s => s.activeServiceId)

  return (
    <>
      <Link
        href={activeServiceId ? `/admin/share?service=${activeServiceId}` : '/admin/share'}
        prefetch={false}
        onMouseEnter={() => {
          queryClient.prefetchQuery({
            queryKey: ['admin', 'share', 'status'],
            queryFn: async ({ signal }) => {
              const { data, response } = await client.GET('/api/admin/share/status', { signal })
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
        href={activeServiceId ? `/admin/session-scoring?service=${activeServiceId}` : '/admin/session-scoring'}
        prefetch={false}
        onMouseEnter={() => {
          if (!activeServiceId) return
          queryClient.prefetchQuery({
            queryKey: ['scoring-analytics-composite', activeServiceId, 24],
            queryFn: async ({ signal }) => {
              const { data, response } = await client.GET(
                '/api/services/{service_id}/scoring/analytics',
                {
                  params: {
                    path: { service_id: activeServiceId },
                    query: { since_hours: 24 },
                  },
                  signal,
                },
              )
              if (!response.ok) throw new Error(`status ${response.status}`)
              return data
            },
          })
          queryClient.prefetchQuery({
            queryKey: ['scoring-config-composite', activeServiceId],
            queryFn: async ({ signal }) => {
              const { data, response } = await client.GET(
                '/api/services/{service_id}/scoring/config',
                {
                  params: { path: { service_id: activeServiceId } },
                  signal,
                },
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
            queryKey: ['admin', 'metric-history-batch', '1h'],
            queryFn: async ({ signal }) => {
              const { data, response } = await client.GET('/api/admin/metric-history/batch', {
                params: { query: { since: '1h' } },
                signal,
              })
              if (!response.ok) throw new Error(`status ${response.status}`)
              return data
            },
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
            queryKey: ['admin', 'celery-status'],
            queryFn: async ({ signal }) => {
              const { data, response } = await client.GET('/api/admin/celery/status', { signal })
              if (!response.ok) throw new Error(`status ${response.status}`)
              return data
            },
          })
        }}
        className={buttonVariants({ variant: 'secondary', size: 'sm' })}
      >
        <Layers className="h-4 w-4 mr-1" /> Queue
      </Link>
    </>
  )
}
