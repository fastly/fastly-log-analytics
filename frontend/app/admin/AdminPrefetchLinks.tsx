'use client'

import React from 'react'
import Link from 'next/link'
import { useQueryClient } from '@tanstack/react-query'
import { client } from '@/lib/api'
import { useServiceStore } from '@/stores/serviceStore'
import { buttonVariants } from '@/components/ui/button'
import { UserPlus, ShieldCheck, Activity } from 'lucide-react'

export function AdminPrefetchLinks() {
  const queryClient = useQueryClient()
  const { activeServiceId } = useServiceStore()

  return (
    <>
      <Link
        href="/admin/share"
        prefetch={false}
        onMouseEnter={() => {
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
    </>
  )
}
