'use client'

import React, { useState } from 'react'
import { useQuery, useQueryClient } from '@tanstack/react-query'
import { Users } from 'lucide-react'
import dynamic from 'next/dynamic'

import { client, extractApiError } from '@/lib/api'
import { useIsDataReady } from '@/hooks/useIsDataReady'
import { useScoringLabels } from '@/hooks/useScoringLabels'
import { ReportLayout } from '@/components/ReportLayout'
import { UpdatingBadge } from '@/components/UpdatingBadge'
import type { components } from '@/types/api.generated'

import { ScoringControls } from './_sections/ScoringControls'
import { SessionsTable } from './_sections/SessionsTable'

// SessionDetail is only rendered after a row click. Lazy-loading it
// (with the DataTable + 7 sub-components it pulls in) keeps the
// initial sessions chunk smaller; the dialog's chunk only fetches on
// first row interaction.
const SessionDetail = dynamic(
  () => import('./_sections/SessionDetail').then(m => m.SessionDetail),
  { ssr: false },
)

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

        const { data, isLoading, isFetching, isError, error, refetch } = useQuery({
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

        // E-7 (audit): React Query v5 auto-aborts the previous queryFn's
        // signal when the observer's queryKey changes — and activeServiceId
        // is in the key above — so the bare service-switch is already safe
        // against the new key's data being beaten to the screen by a stale
        // svc1 response. The explicit cancelQueries below covers the
        // remaining sibling cache entries (other filter combinations
        // mid-flight under the OLD service id) that are not the active
        // observer and would otherwise complete and write to the cache
        // unnecessarily after the switch. Cancelling on the old id (via a
        // ref of the previous value) is the only way to target them — the
        // current effect run already sees the new id.
        const prevServiceRef = React.useRef<string | null>(activeServiceId)
        React.useEffect(() => {
          const prev = prevServiceRef.current
          if (prev && prev !== activeServiceId) {
            void qc.cancelQueries({ queryKey: ['sessions', 'list', prev] })
            void qc.cancelQueries({ queryKey: ['scoring-labels', prev] })
          }
          prevServiceRef.current = activeServiceId
        }, [activeServiceId, qc])

        if (rangeExceedsSevenDays) {
          return (
            <>
              <div className="flex flex-col sm:flex-row items-start sm:items-center gap-2 sm:gap-4 shrink-0 mb-4 justify-end">
                <UpdatingBadge />
              </div>
              <div className="rounded-md border border-amber-300 bg-amber-50 p-4 text-sm text-amber-900 dark:border-amber-700 dark:bg-amber-950 dark:text-amber-100">
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

            {/* E-6 (audit): surface query failures inline instead of
                rendering an empty SessionsTable that looks like a
                successful zero-result query. */}
            {isError ? (
              <div
                role="alert"
                className="mb-4 flex flex-col items-start gap-2 rounded-md border border-red-300 bg-red-50 p-4 text-sm text-red-900 dark:border-red-700 dark:bg-red-950 dark:text-red-100"
              >
                <div className="font-semibold">Failed to load sessions.</div>
                <div className="font-mono text-xs opacity-80 break-all">{extractApiError(error)}</div>
                <button
                  type="button"
                  onClick={() => { void refetch() }}
                  className="mt-1 rounded border border-red-400 px-2 py-1 text-xs hover:bg-red-100 dark:hover:bg-red-900"
                >
                  Retry
                </button>
              </div>
            ) : null}

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
