'use client'

import React, { useState, useEffect } from 'react'
import { useQuery, useQueryClient } from '@tanstack/react-query'
import { ColumnDef } from '@tanstack/react-table'
import { AlertTriangle, Clock, ExternalLink, Film, Users } from 'lucide-react'
import dynamic from 'next/dynamic'

import { client, extractApiError } from '@/lib/api'
import { unwrapClient } from '@/lib/api'
import { queryKeys } from '@/lib/query-keys'
import { useIsDataReady } from '@/hooks/useBootstrap'
import { useScoringLabels, type LabelRow, type LabelValue } from '@/components/SessionScoring/help-content'
import { FlagSessionPopover } from '@/components/SessionScoring/TopFlaggedTable'
import { useServiceStore } from '@/stores/filterStore'
import { useServiceSwitchReset } from '@/hooks/useBootstrap'
import { useDateFormat } from '@/hooks/useSSE'
import { ReportLayout } from '@/components/ReportLayout'
import { DataTable } from '@/components/DataTable'
import { UpdatingBadge } from '@/components/AnalyticsCard'
import { Button } from '@/components/ui/button'
import { Switch } from '@/components/ui/switch'
import { Label } from '@/components/ui/label'
import { Input } from '@/components/ui/input'
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from '@/components/ui/tooltip'
import { cn } from '@/lib/utils'
import { formatCountMaybe } from '@/lib/format'
import type { components } from '@/types/api.generated'
import type { BootstrapResponse, Session, SessionsResponse } from '@/types/api'

interface SessionsTableProps {
  data: SessionsResponse | undefined
  activeServiceId: string | null
  isLoadingInitial: boolean
  isFetching: boolean
  labels: LabelRow[]
  labelBySid: Map<string, LabelValue>
  idBySid: Map<string, string>
  onFlagged: () => void
  onRowClick: (row: Session) => void
}

export function SessionsTable({
  data,
  activeServiceId,
  isLoadingInitial,
  isFetching,
  labels,
  labelBySid,
  idBySid,
  onFlagged,
  onRowClick,
}: SessionsTableProps) {
  const { full, relative, abbr } = useDateFormat()

  const columns: ColumnDef<Session>[] = React.useMemo(() => {
    const cols: ColumnDef<Session>[] = [
      {
        accessorKey: 'ip',
        header: 'IP Address',
        cell: ({ row }) => (
          <div className="flex items-center gap-1.5">
            <span className="font-medium">{row.getValue('ip') as string}</span>
            {row.original.is_streaming && (
              <span title="Streaming Session">
                <Film className="h-3.5 w-3.5 text-blue-500 shrink-0" />
              </span>
            )}
            {row.original.flagged && (
              <span title="Flagged Session">
                <AlertTriangle className="h-3.5 w-3.5 text-yellow-500 shrink-0" />
              </span>
            )}
          </div>
        )
      },
      { accessorKey: 'country', header: 'Country' },
      {
        accessorKey: 'session_start',
        header: 'Started',
        cell: ({ row }) => {
          const val = row.getValue('session_start') as string
          return (
            <TooltipProvider>
              <Tooltip>
                <TooltipTrigger render={
                  <span className="whitespace-nowrap text-xs  border-b border-dotted border-muted-foreground/30">
                    {relative(val)}
                  </span>
                } />
                <TooltipContent className="text-xs">
                  {full(val)} {abbr()}
                </TooltipContent>
              </Tooltip>
            </TooltipProvider>
          )
        },
      },
      { accessorKey: 'req_count', header: 'Requests' },
      {
        accessorKey: 'unique_urls',
        header: 'Unique URLs',
        cell: ({ row }) => formatCountMaybe(row.getValue('unique_urls') as number) ?? '—',
      },
      {
        accessorKey: 'asn',
        header: 'ASN',
        cell: ({ row }) => {
          const r = row.original as { asn?: number | null; asn_label?: string | null }
          const asnMap = (data?.asn_names ?? {}) as Record<string, string>
          const mapped = r.asn != null ? asnMap[String(r.asn)] : undefined
          if (mapped) return mapped
          if (r.asn_label) return r.asn_label
          return r.asn ? `AS${r.asn}` : '—'
        },
      },
      {
        accessorKey: 'reqs_4xx',
        header: '4xx%',
        cell: ({ row }) => {
          const n4xx = (row.getValue('reqs_4xx') as number) ?? 0
          const total = (row.original.req_count as number) ?? 0
          if (!total) return '—'
          const pct = (n4xx / total) * 100
          return (
            <span className={pct >= 20 ? 'text-yellow-800 dark:text-yellow-400 font-semibold' : ''}>
              {pct.toFixed(1)}%
            </span>
          )
        },
      },
    ]

    if (data?.has_rtt) {
      cols.push({
        accessorKey: 'median_rtt_ms',
        header: 'Med. RTT',
        cell: ({ row }) => {
          const val = row.getValue('median_rtt_ms') as number
          return val ? `${val.toFixed(1)}ms` : '—'
        },
      })
    }

    if (data?.has_ja4) {
      cols.push({
        accessorKey: 'ja4',
        header: 'JA4',
        cell: ({ row }) => {
          const val = row.getValue('ja4') as string | undefined
          return val ? <span className="text-xs font-mono">{val.slice(0, 16)}…</span> : '—'
        },
      })
    }

    if (data?.has_edge) {
      cols.push({
        id: 'edge',
        header: 'Edge / Shield',
        cell: ({ row }) => `${row.original.edge_count ?? 0} / ${row.original.shield_count ?? 0}`,
      })
    }

    cols.push({
      accessorKey: 'ua',
      header: 'User-Agent',
      cell: ({ row }) => {
        const ua = row.getValue('ua') as string | undefined
        return ua ? <span className="text-xs truncate max-w-[200px] inline-block" title={ua}>{ua}</span> : '—'
      },
    })

    if (data?.has_edge_sid) {
      cols.push({
        id: '__flag',
        header: 'Flag',
        cell: ({ row }) => {
          const sid = row.original.edge_sid ?? undefined
          if (!sid) return null
          return (
            <FlagSessionPopover
              serviceId={activeServiceId || ''}
              sid={sid}
              sampleIp={row.original.ip}
              sampleUa={row.original.ua ?? undefined}
              currentLabel={labelBySid.get(sid) ?? null}
              currentLabelId={idBySid.get(sid) ?? null}
              onFlagged={onFlagged}
            />
          )
        },
      })
    }

    cols.push({
      id: 'actions',
      header: '',
      cell: ({ row }) => (
        <Button variant="ghost" size="sm" className="h-7" onClick={() => onRowClick(row.original)}>
          Details <ExternalLink className="ml-1.5 h-3 w-3" />
        </Button>
      ),
    })

    return cols
  }, [data, relative, full, abbr, labels, labelBySid, idBySid, activeServiceId, onFlagged, onRowClick])

  return (
    <div
      data-testid="sessions-table-container"
      className={cn(
        "border rounded-lg transition-opacity duration-100 min-h-[500px] [contain-intrinsic-size:500px]",
        isFetching && !isLoadingInitial && "opacity-40 pointer-events-none"
      )}
    >
      <DataTable
        columns={columns}
        data={data?.sessions || []}
        isLoading={isLoadingInitial}
        searchKey="ip"
        onRowClick={onRowClick}
      />
    </div>
  )
}

interface ScoringControlsProps {
  cmcdEnabled: boolean
  flaggedOnly: boolean
  setFlaggedOnly: (v: boolean) => void
  streamingOnly: boolean
  setStreamingOnly: (v: boolean) => void
  minReqs: number | ''
  setMinReqs: (v: number | '') => void
  min4xxPct: number | ''
  setMin4xxPct: (v: number | '') => void
  data: Pick<components['schemas']['SessionsResponse'], 'min_reqs_flag' | 'min_4xx_pct_flag'> | undefined
  isFetching: boolean
  isLoadingInitial: boolean
  refetch: () => void
}

export function ScoringControls({
  cmcdEnabled,
  flaggedOnly,
  setFlaggedOnly,
  streamingOnly,
  setStreamingOnly,
  minReqs,
  setMinReqs,
  min4xxPct,
  setMin4xxPct,
  data,
  isFetching,
  isLoadingInitial,
  refetch,
}: ScoringControlsProps) {
  return (
    <div
      data-testid="sessions-scoring-controls"
      className={cn("flex flex-wrap items-center gap-4 p-3 border rounded-lg bg-muted/30 transition-opacity duration-100", isFetching && !isLoadingInitial && "opacity-40 pointer-events-none")}
    >
      <div className="flex items-center gap-2">
        <Switch
          id="flagged-only"
          data-testid="flagged-only-switch"
          checked={flaggedOnly}
          onCheckedChange={setFlaggedOnly}
        />
        <Label htmlFor="flagged-only" className="text-sm cursor-pointer flex items-center gap-1">
          <AlertTriangle className="h-3.5 w-3.5 text-yellow-500" /> Flagged only
        </Label>
      </div>

      {cmcdEnabled && (
        <div className="flex items-center gap-2">
          <Switch
            id="streaming-only"
            data-testid="streaming-only-switch"
            checked={streamingOnly}
            onCheckedChange={setStreamingOnly}
          />
          <Label htmlFor="streaming-only" className="text-sm cursor-pointer flex items-center gap-1">
            <Film className="h-3.5 w-3.5 text-blue-500" /> Streaming sessions
          </Label>
        </div>
      )}

      <div className="flex items-center gap-2">
        <Label className="text-xs text-muted-foreground whitespace-nowrap">Min. requests</Label>
        <Input
          type="number"
          min={0}
          value={minReqs}
          onChange={e => setMinReqs(e.target.value === '' ? '' : Number(e.target.value))}
          placeholder={`≥ ${data?.min_reqs_flag ?? 1000}`}
          data-testid="min-reqs-input"
          className="h-8 w-24 text-sm text-right"
        />
      </div>

      <div className="flex items-center gap-2">
        <Label className="text-xs text-muted-foreground whitespace-nowrap">Min. 4xx%</Label>
        <Input
          type="number"
          min={0}
          max={100}
          value={min4xxPct}
          onChange={e => setMin4xxPct(e.target.value === '' ? '' : Number(e.target.value))}
          placeholder={`≥ ${data?.min_4xx_pct_flag ?? 20}`}
          data-testid="min-4xx-input"
          className="h-8 w-24 text-sm text-right"
        />
      </div>

      {(flaggedOnly || streamingOnly || minReqs !== '' || min4xxPct !== '') && (
        <Button
          variant="ghost"
          size="sm"
          className="h-8 text-xs ml-auto"
          onClick={() => { setFlaggedOnly(false); setStreamingOnly(false); setMinReqs(''); setMin4xxPct('') }}
        >
          Clear filters
        </Button>
      )}

      <Button
        variant="outline"
        size="sm"
        className="h-8 ml-auto"
        onClick={() => refetch()}
        disabled={isFetching}
      >
        {isFetching ? <Clock className="h-3.5 w-3.5 mr-2 animate-spin" /> : <Clock className="h-3.5 w-3.5 mr-2" />}
        Refresh
      </Button>
    </div>
  )
}

function useActiveServiceCmcdEnabled(activeServiceId: string | null): boolean {
  const isInitialized = useServiceStore(s => s.isInitialized)
  const services = useServiceStore(s => s.services)
  const queryClient = useQueryClient()

  if (isInitialized) {
    const activeService = services.find(s => s.id === activeServiceId)
    return activeService?.cmcdEnabled ?? false
  }

  const bootstrap = queryClient.getQueryData<BootstrapResponse>(queryKeys.bootstrap())
  const bootstrapServices = bootstrap?.services
  if (Array.isArray(bootstrapServices)) {
    const activeService = bootstrapServices.find((s) => s.service_id === activeServiceId)
    return !!activeService?.cmcd_enabled
  }

  return false
}

// SessionDetail is only rendered after a row click. Lazy-loading it
// (with the DataTable + 7 sub-components it pulls in) keeps the
// initial sessions chunk smaller; the dialog's chunk only fetches on
// first row interaction.
const SessionDetail = dynamic(
  () => import('./_sections/SessionDetail').then(m => m.SessionDetail),
  { ssr: false },
)

// Lifted out of the ReportLayout render-prop so the (large) hook list —
// useIsDataReady, useMemo, useQueryClient, useScoringLabels, useCallback,
// useQuery, useRef, useEffect — lives at the top of a STABLE component
// instead of inside the `{(ctx) => { …hooks…; if (x) return …; … }}`
// callback. Mirrors InsightsBody (app/insights/page.tsx) and SecurityBody.
// The render-prop here even early-returns (rangeExceedsSevenDays) AFTER its
// hooks; that's the classic #310 fragility — a conditional appearing above
// the hooks (or ReportShell swapping the callback's mount with a skeleton on
// `!isReady`) changes hook order across renders and throws #310. Hooks at the
// top of a dedicated child are immune.
type SessionsFilters = components['schemas']['SessionsRequest']['filters']

interface SessionsBodyProps {
  startTime: string | null
  endTime: string | null
  activeServiceId: string | null
  filterPayload: SessionsFilters
  selectedSession: Session | null
  setSelectedSession: React.Dispatch<React.SetStateAction<Session | null>>
  flaggedOnly: boolean
  setFlaggedOnly: React.Dispatch<React.SetStateAction<boolean>>
  streamingOnly: boolean
  setStreamingOnly: React.Dispatch<React.SetStateAction<boolean>>
  minReqs: number | ''
  setMinReqs: React.Dispatch<React.SetStateAction<number | ''>>
  min4xxPct: number | ''
  setMin4xxPct: React.Dispatch<React.SetStateAction<number | ''>>
}

function SessionsBody({
  startTime,
  endTime,
  activeServiceId,
  filterPayload,
  selectedSession,
  setSelectedSession,
  flaggedOnly,
  setFlaggedOnly,
  streamingOnly,
  setStreamingOnly,
  minReqs,
  setMinReqs,
  min4xxPct,
  setMin4xxPct,
}: SessionsBodyProps) {
  const isReady = useIsDataReady()
  const cmcdEnabled = useActiveServiceCmcdEnabled(activeServiceId)
  const queryClient = useQueryClient()

  useEffect(() => {
    return () => {
      queryClient.cancelQueries({ queryKey: ['sessions'] })
      queryClient.cancelQueries({ queryKey: queryKeys.scoringLabelsAll() })
    }
  }, [queryClient])

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
    qc.invalidateQueries({ queryKey: queryKeys.scoringLabels(activeServiceId) })
  }, [qc, activeServiceId])

  const { data, isLoading, isFetching, isError, error, refetch } = useQuery({
    queryKey: ['sessions', 'list', activeServiceId, startTime, endTime, filterPayload, flaggedOnly, streamingOnly, minReqs, min4xxPct],
    queryFn: async ({ signal }) => {
      return (await unwrapClient(
        client.POST("/api/sessions", {
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
            streaming_only: streamingOnly,
            min_reqs_flag: minReqs !== '' ? minReqs : undefined,
            min_4xx_pct_flag: min4xxPct !== '' ? min4xxPct : undefined,
          },
        }),
      )) as SessionsResponse | undefined
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
        cmcdEnabled={cmcdEnabled}
        flaggedOnly={flaggedOnly}
        setFlaggedOnly={setFlaggedOnly}
        streamingOnly={streamingOnly}
        setStreamingOnly={setStreamingOnly}
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
    </div>
  )
}


export default function SessionsPage() {
  const [selectedSession, setSelectedSession] = useState<Session | null>(null)

  // ── Filter state ─────────────────────────────────────────────────────────
  const [flaggedOnly, setFlaggedOnly] = useState(false)
  const [streamingOnly, setStreamingOnly] = useState(false)
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
      }) => (
        <SessionsBody
          startTime={startTime}
          endTime={endTime}
          activeServiceId={activeServiceId}
          filterPayload={filterPayload}
          selectedSession={selectedSession}
          setSelectedSession={setSelectedSession}
          flaggedOnly={flaggedOnly}
          setFlaggedOnly={setFlaggedOnly}
          streamingOnly={streamingOnly}
          setStreamingOnly={setStreamingOnly}
          minReqs={minReqs}
          setMinReqs={setMinReqs}
          min4xxPct={min4xxPct}
          setMin4xxPct={setMin4xxPct}
        />
      )}
    </ReportLayout>
  )
}
