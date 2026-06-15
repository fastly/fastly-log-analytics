'use client'

import React, { useState } from 'react'
import { useQuery, useQueryClient } from '@tanstack/react-query'
import { Users } from 'lucide-react'

import { client } from '@/lib/api'
import { useIsDataReady } from '@/hooks/useIsDataReady'
import { useScoringLabels } from '@/hooks/useScoringLabels'
import { ReportLayout } from '@/components/ReportLayout'
import { UpdatingBadge } from '@/components/UpdatingBadge'
import type { components } from '@/types/api.generated'

import { ScoringControls } from './_sections/ScoringControls'
import { SessionsTable } from './_sections/SessionsTable'
import { SessionDetail } from './_sections/SessionDetail'

type SessionRow = components['schemas']['Session']
type SessionsResponse = components['schemas']['SessionsResponse']

export default function SessionsPage() {
  const [selectedSession, setSelectedSession] = useState<SessionRow | null>(null)

  // ── Filter state ─────────────────────────────────────────────────────────
  const [flaggedOnly, setFlaggedOnly] = useState(false)
  const [minReqs, setMinReqs] = useState<number | ''>('')
  const [min4xxPct, setMin4xxPct] = useState<number | ''>('')

  return (
    <ReportLayout
      title="User Sessions"
      description="Track IP addresses and JA4 fingerprints generating high request volumes or errors."
      icon={Users}
    >
      {({
        startTime,
        endTime,
        activeServiceId,
        filterPayload,
      }) => {
        const isReady = useIsDataReady()

        // Mirror backend's 7-day guard client-side so the request never
        // fires on a too-wide range. Backend rejects with a 400 either
        // way, but the round-trip costs the user a flash of error toast
        // + (on the analyst path) 1-2 timed-out pyiceberg CDN GETs per
        // attempt. Inline empty-state below explains the limit instead.
        const rangeExceedsSevenDays = React.useMemo(() => {
          if (!startTime || !endTime) return false
          const s = Date.parse(startTime)
          const e = Date.parse(endTime)
          if (!Number.isFinite(s) || !Number.isFinite(e)) return false
          return (e - s) / 86_400_000 > 7
        }, [startTime, endTime])

        const qc = useQueryClient()
        const { labelBySid, idBySid, labels } = useScoringLabels(activeServiceId || '', {
          enabled: !!activeServiceId,
        })
        const onFlagged = React.useCallback(() => {
          qc.invalidateQueries({ queryKey: ['scoring-labels', activeServiceId] })
        }, [qc, activeServiceId])

        const { data, isLoading, isFetching, refetch } = useQuery({
          queryKey: ['sessions', 'list', activeServiceId, startTime, endTime, filterPayload, flaggedOnly, minReqs, min4xxPct],
          queryFn: async ({ signal }) => {
            const { data } = await client.POST("/api/sessions", {
              signal,
              body: {
                start_time: startTime,
                end_time: endTime,
                filters: filterPayload,
                page: 1,
                limit: 100,
                sort_by: 'session_start',
                sort_dir: 'desc',
                flagged_only: flaggedOnly,
                min_reqs_flag: minReqs !== '' ? minReqs : undefined,
                min_4xx_pct_flag: min4xxPct !== '' ? min4xxPct : undefined,
              }
            })
            return data as SessionsResponse | undefined
          },
          enabled: isReady && !rangeExceedsSevenDays
        })

        if (rangeExceedsSevenDays) {
          return (
            <>
              <div className="flex flex-col sm:flex-row items-start sm:items-center gap-2 sm:gap-4 shrink-0 mb-4 justify-end">
                <UpdatingBadge />
              </div>
              <div className="rounded-md border border-amber-300 bg-amber-50 p-4 text-sm text-amber-900">
                Sessions view is limited to a 7-day window. Narrow the date range above to see results.
              </div>
            </>
          )
        }

        const isLoadingInitial = isLoading || (isFetching && !data)

        return (
          <>
            <div className="flex flex-col sm:flex-row items-start sm:items-center gap-2 sm:gap-4 shrink-0 mb-4 justify-end">
              <UpdatingBadge />
            </div>

            <ScoringControls
              flaggedOnly={flaggedOnly}
              setFlaggedOnly={setFlaggedOnly}
              minReqs={minReqs}
              setMinReqs={setMinReqs}
              min4xxPct={min4xxPct}
              setMin4xxPct={setMin4xxPct}
              data={data}
              isFetching={isFetching}
              isLoadingInitial={isLoadingInitial}
              refetch={refetch}
            />

            <SessionsTable
              data={data}
              activeServiceId={activeServiceId}
              isLoadingInitial={isLoadingInitial}
              isFetching={isFetching}
              labels={labels}
              labelBySid={labelBySid}
              idBySid={idBySid}
              onFlagged={onFlagged}
              onRowClick={setSelectedSession}
            />

            <SessionDetail
              selectedSession={selectedSession}
              setSelectedSession={setSelectedSession}
              activeServiceId={activeServiceId}
              data={data}
              labels={labels}
              labelBySid={labelBySid}
              onFlagged={onFlagged}
            />
          </>
        )
      }}
    </ReportLayout>
  )
}
