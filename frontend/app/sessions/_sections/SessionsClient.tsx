'use client'

import React, { useEffect, useState } from 'react'
import { useQuery, useQueryClient } from '@tanstack/react-query'
import { parseAsBoolean, parseAsInteger, useQueryState } from 'nuqs'
import { ChevronLeft, ChevronRight, Users } from 'lucide-react'

import { client, extractApiError, unwrapClient, queryKeys } from '@/lib/api'
import { formatCount } from '@/lib/utils'
import { useIsDataReady, useServiceSwitchReset } from '@/hooks/useBootstrap'
import { useReportCancelQueries } from '@/hooks/useReportPageState'
import { useScoringLabels } from '@/components/SessionScoring/ScoringAdminPanels'
import { useServiceStore } from '@/stores/filterStore'
import { ReportLayout } from '@/components/ReportLayout'
import { Button } from '@/components/ui/button'
import { UpdatingBadge } from '@/components/AnalyticsCard'
import { RawWindowNote, readRawWindow } from '@/components/network/rollupLabels'
import {
  SessionDetail,
  SessionDetailModal,
  type SessionDetailProps,
} from '@/components/SessionDetailModal'
import {
  ScoringControls,
  SessionsTable,
  type ScoringControlsProps,
  type SessionsTableProps,
} from './SessionsTable'
import type { components } from '@/types/api.generated'
import type { BootstrapResponse, Session, SessionsResponse } from '@/types/api'

export {
  SessionDetail,
  SessionDetailModal,
  SessionsTable,
  ScoringControls,
  type SessionDetailProps,
  type SessionsTableProps,
  type ScoringControlsProps,
}

type SessionsFilters = components['schemas']['SessionsRequest']['filters']

const SESSIONS_CANCEL_KEYS: ReadonlyArray<readonly unknown[]> = [
  ['sessions'],
  queryKeys.scoringLabelsAll(),
]

function useActiveServiceCmcdEnabled(activeServiceId: string | null): boolean {
  const isInitialized = useServiceStore((s) => s.isInitialized)
  const services = useServiceStore((s) => s.services)
  const queryClient = useQueryClient()
  if (isInitialized) return services.find((s) => s.id === activeServiceId)?.cmcdEnabled ?? false
  const bootstrapServices = queryClient.getQueryData<BootstrapResponse>(queryKeys.bootstrap())?.services
  return Array.isArray(bootstrapServices) ? !!bootstrapServices.find((s) => s.service_id === activeServiceId)?.cmcd_enabled : false
}

function SessionsBody({
  startTime, endTime, activeServiceId, filterPayload,
}: {
  startTime: string | null; endTime: string | null; activeServiceId: string | null; filterPayload: SessionsFilters
}) {
  const [selectedSession, setSelectedSession] = useState<Session | null>(null)
  const [flaggedOnly, setFlaggedOnly] = useQueryState('flagged', parseAsBoolean.withDefault(false))
  const [streamingOnly, setStreamingOnly] = useQueryState('streaming', parseAsBoolean.withDefault(false))
  const [minReqsParam, setMinReqsParam] = useQueryState('min_reqs', parseAsInteger)
  const [min4xxPctParam, setMin4xxPctParam] = useQueryState('min_4xx_pct', parseAsInteger)
  const [pageParam, setPageParam] = useQueryState('page', parseAsInteger.withDefault(1))
  const page = Math.max(1, pageParam || 1)
  const minReqs: number | '' = minReqsParam ?? ''
  const min4xxPct: number | '' = min4xxPctParam ?? ''
  const setMinReqs = React.useCallback((v: number | '') => {
    void setMinReqsParam(v === '' ? null : v)
    void setPageParam(1)
  }, [setMinReqsParam, setPageParam])
  const setMin4xxPct = React.useCallback((v: number | '') => {
    void setMin4xxPctParam(v === '' ? null : v)
    void setPageParam(1)
  }, [setMin4xxPctParam, setPageParam])
  const filterKey = JSON.stringify(filterPayload)
  useEffect(() => {
    void setPageParam((prev) => (prev && prev > 1 ? 1 : prev))
  }, [activeServiceId, startTime, endTime, filterKey, setPageParam])
  const isReady = useIsDataReady()
  const cmcdEnabled = useActiveServiceCmcdEnabled(activeServiceId)
  const qc = useQueryClient()

  useReportCancelQueries(SESSIONS_CANCEL_KEYS)

  const rangeExceedsSevenDays = React.useMemo(() => {
    if (!startTime || !endTime) return false
    const s = Date.parse(startTime)
    const e = Date.parse(endTime)
    return Number.isFinite(s) && Number.isFinite(e) && (e - s) / 86_400_000 > 7
  }, [startTime, endTime])

  const { labelBySid, idBySid, labels } = useScoringLabels(activeServiceId || '', { enabled: !!activeServiceId })
  const onFlagged = React.useCallback(() => {
    qc.invalidateQueries({ queryKey: queryKeys.scoringLabels(activeServiceId) })
  }, [qc, activeServiceId])

  const { data, isLoading, isFetching, isError, error, refetch } = useQuery({
    queryKey: ['sessions', 'list', activeServiceId, startTime, endTime, filterPayload, flaggedOnly, streamingOnly, minReqs, min4xxPct, page],
    queryFn: async ({ signal }) =>
      (await unwrapClient(
        client.POST('/api/sessions', {
          signal,
          body: {
            start_time: startTime, end_time: endTime, filters: filterPayload, page, limit: 100,
            sort_by: 'session_start', sort_dir: 'desc', flagged_only: flaggedOnly, streaming_only: streamingOnly,
            min_reqs_flag: minReqs !== '' ? minReqs : undefined,
            min_4xx_pct_flag: min4xxPct !== '' ? min4xxPct : undefined,
          },
        }),
      )) as SessionsResponse | undefined,
    enabled: isReady && !rangeExceedsSevenDays,
  })

  useServiceSwitchReset(activeServiceId, (prev) => {
    void qc.cancelQueries({ queryKey: ['sessions', 'list', prev] })
    void qc.cancelQueries({ queryKey: queryKeys.scoringLabels(prev) })
  })

  if (rangeExceedsSevenDays) {
    return (
      <div className="rounded-md border border-amber-300 bg-amber-50 p-4 text-sm text-amber-900 dark:border-amber-700 dark:bg-amber-950 dark:text-amber-100">
        Sessions view is limited to a 7-day window. Narrow the date range above to see results.
      </div>
    )
  }

  const isLoadingInitial = isLoading || (isFetching && !data)

  return (
    <div data-testid="user-sessions-explorer" className="space-y-4">
      {isFetching && !isLoadingInitial ? (
        <div className="flex flex-col sm:flex-row items-start sm:items-center gap-2 sm:gap-4 shrink-0 mb-4 justify-end">
          <UpdatingBadge />
        </div>
      ) : null}

      {isError ? (
        <div role="alert" className="mb-4 flex flex-col items-start gap-2 rounded-md border border-red-300 bg-red-50 p-4 text-sm text-red-900 dark:border-red-700 dark:bg-red-950 dark:text-red-100">
          <div className="font-semibold">Failed to load sessions.</div>
          <div className="font-mono text-xs opacity-80 break-all">{extractApiError(error)}</div>
          <button
            type="button" onClick={() => { void refetch() }}
            className="mt-1 rounded border border-red-400 px-2 py-1 text-xs hover:bg-red-100 dark:hover:bg-red-900"
          >
            Retry
          </button>
        </div>
      ) : null}

      <RawWindowNote window={readRawWindow(data)} />

      <ScoringControls
        cmcdEnabled={cmcdEnabled}
        flaggedOnly={flaggedOnly}
        setFlaggedOnly={(v) => { void setFlaggedOnly(v); void setPageParam(1) }}
        streamingOnly={streamingOnly}
        setStreamingOnly={(v) => { void setStreamingOnly(v); void setPageParam(1) }}
        minReqs={minReqs} setMinReqs={setMinReqs} min4xxPct={min4xxPct} setMin4xxPct={setMin4xxPct}
        data={data} isFetching={isFetching} isLoadingInitial={isLoadingInitial} refetch={refetch}
      />

      <SessionsTable
        data={data} activeServiceId={activeServiceId} isLoadingInitial={isLoadingInitial} isFetching={isFetching}
        labels={labels} labelBySid={labelBySid} idBySid={idBySid} onFlagged={onFlagged} onRowClick={setSelectedSession}
      />

      {(data?.total ?? 0) > (data?.limit ?? 100) && (() => {
        const limit = data?.limit ?? 100
        const total = data?.total ?? 0
        const totalPages = Math.max(1, Math.ceil(total / limit))
        const rangeStart = (page - 1) * limit + 1
        const rangeEnd = Math.min(page * limit, total)
        return (
          <div className="flex flex-wrap items-center justify-between gap-2 rounded-md border px-3 py-2 text-xs text-muted-foreground">
            <span className="tabular-nums">
              Showing {formatCount(rangeStart)}–{formatCount(rangeEnd)} of {formatCount(total)} sessions (Page {page} of {totalPages})
            </span>
            <div className="flex items-center gap-2">
              <Button
                type="button"
                variant="outline"
                size="sm"
                className="h-7 gap-1 px-2 text-xs"
                disabled={page <= 1 || isFetching}
                onClick={() => void setPageParam(Math.max(1, page - 1))}
              >
                <ChevronLeft className="h-3.5 w-3.5" />
                Previous
              </Button>
              <Button
                type="button"
                variant="outline"
                size="sm"
                className="h-7 gap-1 px-2 text-xs"
                disabled={page >= totalPages || isFetching}
                onClick={() => void setPageParam(Math.min(totalPages, page + 1))}
              >
                Next
                <ChevronRight className="h-3.5 w-3.5" />
              </Button>
            </div>
          </div>
        )
      })()}

      <SessionDetail
        selectedSession={selectedSession} setSelectedSession={setSelectedSession} activeServiceId={activeServiceId}
        data={data} labels={labels} labelBySid={labelBySid} onFlagged={onFlagged}
      />
    </div>
  )
}

export function SessionsClient() {
  return (
    <ReportLayout
      title="User Sessions"
      description="Track IP addresses and JA4 fingerprints generating high request volumes or errors."
      icon={Users}
    >
      {({ startTime, endTime, activeServiceId, filterPayload }) => (
        <SessionsBody startTime={startTime} endTime={endTime} activeServiceId={activeServiceId} filterPayload={filterPayload} />
      )}
    </ReportLayout>
  )
}

export default SessionsClient
