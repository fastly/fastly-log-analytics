'use client'

import React, { useState } from 'react'
import { useQuery, useQueryClient } from '@tanstack/react-query'
import { parseAsBoolean, parseAsInteger, useQueryState } from 'nuqs'
import { Users } from 'lucide-react'

import { client, extractApiError, unwrapClient, queryKeys } from '@/lib/api'
import { useIsDataReady, useServiceSwitchReset } from '@/hooks/useBootstrap'
import { useReportCancelQueries } from '@/hooks/useReportPageState'
import { useScoringLabels } from '@/components/SessionScoring/ScoringAdminPanels'
import { useServiceStore } from '@/stores/filterStore'
import { ReportLayout } from '@/components/ReportLayout'
import { UpdatingBadge } from '@/components/AnalyticsCard'
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
  const minReqs: number | '' = minReqsParam ?? ''
  const min4xxPct: number | '' = min4xxPctParam ?? ''
  const setMinReqs = React.useCallback((v: number | '') => { void setMinReqsParam(v === '' ? null : v) }, [setMinReqsParam])
  const setMin4xxPct = React.useCallback((v: number | '') => { void setMin4xxPctParam(v === '' ? null : v) }, [setMin4xxPctParam])
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
    queryKey: ['sessions', 'list', activeServiceId, startTime, endTime, filterPayload, flaggedOnly, streamingOnly, minReqs, min4xxPct],
    queryFn: async ({ signal }) =>
      (await unwrapClient(
        client.POST('/api/sessions', {
          signal,
          body: {
            start_time: startTime, end_time: endTime, filters: filterPayload, page: 1, limit: 100,
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
    <div data-testid="user-sessions-explorer" className="space-y-4">
      <div className="flex flex-col sm:flex-row items-start sm:items-center gap-2 sm:gap-4 shrink-0 mb-4 justify-end">
        <UpdatingBadge />
      </div>

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

      <ScoringControls
        cmcdEnabled={cmcdEnabled} flaggedOnly={flaggedOnly} setFlaggedOnly={(v) => void setFlaggedOnly(v)}
        streamingOnly={streamingOnly} setStreamingOnly={(v) => void setStreamingOnly(v)}
        minReqs={minReqs} setMinReqs={setMinReqs} min4xxPct={min4xxPct} setMin4xxPct={setMin4xxPct}
        data={data} isFetching={isFetching} isLoadingInitial={isLoadingInitial} refetch={refetch}
      />

      <SessionsTable
        data={data} activeServiceId={activeServiceId} isLoadingInitial={isLoadingInitial} isFetching={isFetching}
        labels={labels} labelBySid={labelBySid} idBySid={idBySid} onFlagged={onFlagged} onRowClick={setSelectedSession}
      />

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
