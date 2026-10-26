'use client'

import React, { useState, useEffect } from 'react'
import Link from 'next/link'
import { useQuery, useQueryClient } from '@tanstack/react-query'
import { ColumnDef } from '@tanstack/react-table'
import { AlertTriangle, Clock, ExternalLink, Film, Globe, Shield, Users } from 'lucide-react'

import { client, extractApiError, unwrapClient, queryKeys } from '@/lib/api'
import { useIsDataReady, useFieldLabel, useServiceSwitchReset } from '@/hooks/useBootstrap'
import { useScoringLabels, type LabelRow, type LabelValue } from '@/components/SessionScoring/ScoringAdminPanels'
import { FlagSessionPopover } from '@/components/SessionScoring/TopFlaggedTable'
import { useServiceStore } from '@/stores/filterStore'
import { useDateFormat } from '@/hooks/useSSE'
import { ReportLayout } from '@/components/ReportLayout'
import { DataTable } from '@/components/DataTable'
import { UpdatingBadge } from '@/components/AnalyticsCard'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Dialog, DialogContent, DialogDescription, DialogHeader, DialogTitle } from '@/components/ui/dialog'
import { Switch } from '@/components/ui/switch'
import { Label } from '@/components/ui/label'
import { Input } from '@/components/ui/input'
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from '@/components/ui/tooltip'
import { cn, buildSessionDashboardUrl, buildStreamDetailUrl, formatCount, formatCountMaybe } from '@/lib/utils'
import { OBJECT_TYPE_LABELS, STREAM_TYPE_LABELS, STREAMING_FORMAT_LABELS, cmcdLabel } from '@/app/streaming/_sections/StreamingClient'
import type { components } from '@/types/api.generated'
import type { BootstrapResponse, Session, SessionDetailResponse, SessionsResponse } from '@/types/api'

type SessionDetailRow = SessionDetailResponse['data'][number]
type SessionsFilters = components['schemas']['SessionsRequest']['filters']

const CMCD_DETAIL_COLS = ['cmcd_ot', 'cmcd_br', 'cmcd_bl', 'cmcd_sf', 'cmcd_st']
const DEFAULT_VISIBLE_DETAIL_COLS = ['timestamp', 'host', 'url', 'method', 'edge', 'status', 'cache', 'ua', 'pop']
const INITIAL_DETAIL_COLUMN_ORDER = [...DEFAULT_VISIBLE_DETAIL_COLS, ...CMCD_DETAIL_COLS]

function MetadataItem({ label, children, className }: { label: string; children: React.ReactNode; className?: string }) {
  return (
    <div className={cn('flex flex-col gap-0.5', className)}>
      <span className="text-[11px] sm:text-[10px] uppercase font-bold text-muted-foreground">{label}</span>
      <div className="text-xs">{children}</div>
    </div>
  )
}

function RelativeTimeTooltip({
  timestamp, relative, full, abbr, className, icon,
}: {
  timestamp: string; relative: (ts: string) => string; full: (ts: string) => string; abbr: () => string; className?: string; icon?: React.ReactNode
}) {
  return (
    <TooltipProvider>
      <Tooltip>
        <TooltipTrigger render={<span className={className}>{icon}{relative(timestamp)}</span>} />
        <TooltipContent className="text-xs">{full(timestamp)} {abbr()}</TooltipContent>
      </Tooltip>
    </TooltipProvider>
  )
}

interface SessionDetailProps {
  selectedSession: Session | null
  setSelectedSession: (s: Session | null) => void
  activeServiceId: string | null
  data: SessionsResponse | undefined
  labels: LabelRow[]
  labelBySid: Map<string, LabelValue>
  onFlagged: () => void
}

export function SessionDetail({
  selectedSession, setSelectedSession, activeServiceId, data, labels, labelBySid, onFlagged,
}: SessionDetailProps) {
  const { full, relative, abbr } = useDateFormat()
  const getFieldLabel = useFieldLabel()
  const [detailEdgeOnly, setDetailEdgeOnly] = useState(false)

  const { data: detailData, isLoading: isLoadingDetail, isError: isErrorDetail, error: detailError, refetch: refetchDetail } = useQuery({
    queryKey: ['sessions', 'detail', activeServiceId, selectedSession?.session_token ?? selectedSession?.ip, selectedSession?.ja4, selectedSession?.session_start],
    queryFn: async ({ signal }) => {
      if (!selectedSession) return undefined
      const body = selectedSession.session_token
        ? { session_token: selectedSession.session_token }
        : { ip: selectedSession.ip, ja4: selectedSession.ja4, start_time: selectedSession.session_start, end_time: selectedSession.session_end }
      return unwrapClient(client.POST('/api/sessions/detail', { signal, body }))
    },
    enabled: !!activeServiceId && !!selectedSession,
  })

  const detailColumnNames = detailData?.columns
  const detailColumns: ColumnDef<SessionDetailRow>[] = React.useMemo(() => {
    if (!detailColumnNames) return []
    return detailColumnNames.map((col) => ({
      id: col,
      accessorFn: (row) => row[col],
      meta: { label: getFieldLabel(col) },
      header: getFieldLabel(col),
      cell: ({ row }) => {
        const value = row.original[col]
        const text = value == null ? '' : String(value)
        if (col === 'timestamp') {
          return (
            <RelativeTimeTooltip
              timestamp={text} relative={relative} full={full} abbr={abbr}
              className="whitespace-nowrap font-mono text-xs border-b border-dotted border-muted-foreground/30"
            />
          )
        }
        if (col === 'status') {
          const s = Number(value)
          return <Badge variant={s >= 500 ? 'destructive' : s >= 400 ? 'outline' : 'secondary'}>{s}</Badge>
        }
        if (col === 'resp_bytes' || col === 'elapsed') {
          return <span className="text-xs font-mono tabular-nums">{formatCount(Number(value))}</span>
        }
        if (col === 'cmcd_ot') return <span className="text-xs">{cmcdLabel(OBJECT_TYPE_LABELS, text)}</span>
        if (col === 'cmcd_sf') return <span className="text-xs">{cmcdLabel(STREAMING_FORMAT_LABELS, text)}</span>
        if (col === 'cmcd_st') return <span className="text-xs">{cmcdLabel(STREAM_TYPE_LABELS, text)}</span>
        return <span className="text-xs truncate max-w-[220px] inline-block">{text}</span>
      },
    }))
  }, [detailColumnNames, relative, full, abbr, getFieldLabel])

  const initialDetailVisibility = React.useMemo(() => {
    if (!detailData?.columns) return {}
    const streamingVisible = selectedSession?.is_streaming ? CMCD_DETAIL_COLS.filter((c) => detailData.columns.includes(c)) : []
    const allVisible = new Set([...DEFAULT_VISIBLE_DETAIL_COLS, ...streamingVisible])
    return Object.fromEntries(detailData.columns.map((col) => [col, allVisible.has(col)]))
  }, [detailData?.columns, selectedSession?.is_streaming])

  const filteredDetailData = React.useMemo(() => {
    const arr = detailData?.data || []
    return detailEdgeOnly ? arr.filter((row) => row.edge === 1 || row.edge === true || row.edge === '1') : arr
  }, [detailData?.data, detailEdgeOnly])

  const renderIdentifierLink = (label: string, field: 'ip' | 'ja4' | 'ua', value: string | null | undefined, itemClass = 'min-w-0', textClass = '') =>
    value ? (
      <MetadataItem label={label} className={itemClass}>
        <Link
          href={buildSessionDashboardUrl(activeServiceId || '', field, value, selectedSession?.session_start, selectedSession?.session_end)}
          className={cn('flex gap-1.5 text-sm hover:underline group', field === 'ua' ? 'items-start' : 'items-center')}
          title="View in Dashboard" target="_blank" rel="noopener noreferrer"
        >
          <span className={textClass}>{value}</span>
          <ExternalLink className={cn('h-3 w-3 text-muted-foreground group-hover:text-primary transition-colors shrink-0', field === 'ua' && 'mt-0.5')} />
        </Link>
      </MetadataItem>
    ) : null

  return (
    <Dialog open={!!selectedSession} onOpenChange={(open) => !open && setSelectedSession(null)}>
      <DialogContent data-testid="session-detail-dialog" className="max-w-6xl max-h-[85vh] flex flex-col p-4 md:p-6 overflow-hidden">
        <DialogHeader className="shrink-0 mb-2">
          <DialogTitle className="flex items-center gap-2 text-base">
            <Users className="h-4 w-4" />
            Session: {selectedSession?.ip}
            {selectedSession?.is_streaming && <Film className="h-4 w-4 text-blue-500" />}
            {selectedSession?.is_streaming && selectedSession.session_token && (
              <Link
                href={buildStreamDetailUrl(selectedSession.session_token, activeServiceId)}
                className="inline-flex items-center gap-1.5 text-xs font-semibold px-2.5 py-1 rounded-md bg-blue-500/10 text-blue-600 dark:text-blue-400 border border-blue-500/20 hover:bg-blue-500/20 transition-colors"
              >
                <Film className="h-3 w-3" />Stream Details<ExternalLink className="h-3 w-3" />
              </Link>
            )}
            {selectedSession?.flagged && <AlertTriangle className="h-4 w-4 text-yellow-500" />}
            {selectedSession?.edge_sid && (
              <FlagSessionPopover
                serviceId={activeServiceId || ''}
                sid={selectedSession.edge_sid}
                sampleIp={selectedSession.ip}
                sampleUa={selectedSession.ua ?? undefined}
                currentLabel={labelBySid.get(selectedSession.edge_sid) ?? null}
                currentLabelId={labels.find((l) => l.sid === selectedSession.edge_sid)?.id ?? null}
                onFlagged={onFlagged}
              />
            )}
          </DialogTitle>
          <DialogDescription className="sr-only">
            Detailed metadata and per-request records for the selected client session.
          </DialogDescription>
        </DialogHeader>

        <div className="grid grid-cols-2 md:grid-cols-4 gap-3 p-3 bg-muted/30 rounded-lg shrink-0">
          <MetadataItem label="Start">
            {selectedSession && (
              <RelativeTimeTooltip
                timestamp={selectedSession.session_start} relative={relative} full={full} abbr={abbr}
                className="flex items-center gap-1" icon={<Clock className="h-3 w-3 shrink-0" />}
              />
            )}
          </MetadataItem>
          <MetadataItem label="End">
            {selectedSession && <RelativeTimeTooltip timestamp={selectedSession.session_end} relative={relative} full={full} abbr={abbr} />}
          </MetadataItem>
          <MetadataItem label="Country">
            <span className="flex items-center gap-1"><Globe className="h-3 w-3" /> {selectedSession?.country || '—'}</span>
          </MetadataItem>
          <MetadataItem label="ASN">{selectedSession?.asn ? `AS${selectedSession.asn}` : '—'}</MetadataItem>
          <MetadataItem label="Requests"><span className="font-semibold">{formatCountMaybe(selectedSession?.req_count)}</span></MetadataItem>
          <MetadataItem label="Unique URLs">{selectedSession?.unique_urls ?? '—'}</MetadataItem>
          <MetadataItem label="Edge / Shield">
            <span className="flex items-center gap-1"><Shield className="h-3 w-3" />{selectedSession?.edge_count ?? 0} / {selectedSession?.shield_count ?? 0}</span>
          </MetadataItem>
          <MetadataItem label="Med. RTT">
            {selectedSession?.median_rtt_ms ? `${selectedSession.median_rtt_ms.toFixed(1)}ms` : '—'}
          </MetadataItem>
        </div>

        <div className="flex flex-col gap-3 px-1 shrink-0">
          <div className="flex flex-wrap items-start gap-x-6 gap-y-3">
            {renderIdentifierLink('IP Address', 'ip', selectedSession?.ip)}
            {renderIdentifierLink('JA4 Fingerprint', 'ja4', selectedSession?.ja4, 'min-w-0', 'truncate max-w-[300px]')}
            {renderIdentifierLink('User-Agent', 'ua', selectedSession?.ua, 'min-w-0 flex-1 basis-full md:basis-0', 'break-all line-clamp-2 leading-tight')}
          </div>
        </div>

        <div className="flex-1 overflow-auto min-h-0 flex flex-col">
          {isErrorDetail && (
            <div role="alert" className="mb-3 flex flex-col items-start gap-2 rounded-md border border-destructive/50 bg-destructive/10 p-3 text-sm text-destructive">
              <div className="font-medium">Failed to load session detail.</div>
              <div className="font-mono text-xs opacity-80 break-all">{extractApiError(detailError)}</div>
              <button
                type="button" onClick={() => { void refetchDetail() }}
                className="mt-1 rounded border border-destructive/50 px-2 py-1 text-xs hover:bg-destructive/20"
              >
                Retry
              </button>
            </div>
          )}
          <DataTable
            title={
              <div className="flex items-center gap-4">
                <h4 className="text-sm font-semibold">Request Timeline</h4>
                {data?.has_edge && (
                  <div className="flex items-center gap-2">
                    <Switch id="detail-edge-only" checked={detailEdgeOnly} onCheckedChange={setDetailEdgeOnly} className="scale-75" />
                    <Label htmlFor="detail-edge-only" className="text-xs font-normal cursor-pointer text-muted-foreground hover:text-foreground transition-colors">
                      Edge only
                    </Label>
                  </div>
                )}
              </div>
            }
            compactToolbar={true}
            columns={detailColumns}
            data={filteredDetailData}
            isLoading={isLoadingDetail}
            initialVisibility={initialDetailVisibility}
            initialColumnOrder={INITIAL_DETAIL_COLUMN_ORDER}
            initialSorting={[{ id: 'timestamp', desc: true }]}
          />
        </div>
      </DialogContent>
    </Dialog>
  )
}

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
  data, activeServiceId, isLoadingInitial, isFetching, labels, labelBySid, idBySid, onFlagged, onRowClick,
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
            {row.original.is_streaming && <span title="Streaming Session"><Film className="h-3.5 w-3.5 text-blue-500 shrink-0" /></span>}
            {row.original.flagged && <span title="Flagged Session"><AlertTriangle className="h-3.5 w-3.5 text-yellow-500 shrink-0" /></span>}
          </div>
        ),
      },
      { accessorKey: 'country', header: 'Country' },
      {
        accessorKey: 'session_start',
        header: 'Started',
        cell: ({ row }) => (
          <RelativeTimeTooltip
            timestamp={row.getValue('session_start') as string} relative={relative} full={full} abbr={abbr}
            className="whitespace-nowrap text-xs border-b border-dotted border-muted-foreground/30"
          />
        ),
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
          return mapped || r.asn_label || (r.asn ? `AS${r.asn}` : '—')
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
          return <span className={pct >= 20 ? 'text-yellow-800 dark:text-yellow-400 font-semibold' : ''}>{pct.toFixed(1)}%</span>
        },
      },
    ]

    if (data?.has_rtt) {
      cols.push({
        accessorKey: 'median_rtt_ms', header: 'Med. RTT',
        cell: ({ row }) => {
          const val = row.getValue('median_rtt_ms') as number
          return val ? `${val.toFixed(1)}ms` : '—'
        },
      })
    }
    if (data?.has_ja4) {
      cols.push({
        accessorKey: 'ja4', header: 'JA4',
        cell: ({ row }) => {
          const val = row.getValue('ja4') as string | undefined
          return val ? <span className="text-xs font-mono">{val.slice(0, 16)}…</span> : '—'
        },
      })
    }
    if (data?.has_edge) {
      cols.push({
        id: 'edge', header: 'Edge / Shield',
        cell: ({ row }) => `${row.original.edge_count ?? 0} / ${row.original.shield_count ?? 0}`,
      })
    }

    cols.push({
      accessorKey: 'ua', header: 'User-Agent',
      cell: ({ row }) => {
        const ua = row.getValue('ua') as string | undefined
        return ua ? <span className="text-xs truncate max-w-[200px] inline-block" title={ua}>{ua}</span> : '—'
      },
    })

    if (data?.has_edge_sid) {
      cols.push({
        id: '__flag', header: 'Flag',
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
      id: 'actions', header: '',
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
        'border rounded-lg transition-opacity duration-100 min-h-[500px] [contain-intrinsic-size:500px]',
        isFetching && !isLoadingInitial && 'opacity-40 pointer-events-none',
      )}
    >
      <DataTable columns={columns} data={data?.sessions || []} isLoading={isLoadingInitial} searchKey="ip" onRowClick={onRowClick} />
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
  cmcdEnabled, flaggedOnly, setFlaggedOnly, streamingOnly, setStreamingOnly,
  minReqs, setMinReqs, min4xxPct, setMin4xxPct, data, isFetching, isLoadingInitial, refetch,
}: ScoringControlsProps) {
  return (
    <div
      data-testid="sessions-scoring-controls"
      className={cn(
        'flex flex-wrap items-center gap-4 p-3 border rounded-lg bg-muted/30 transition-opacity duration-100',
        isFetching && !isLoadingInitial && 'opacity-40 pointer-events-none',
      )}
    >
      <div className="flex items-center gap-2">
        <Switch id="flagged-only" data-testid="flagged-only-switch" checked={flaggedOnly} onCheckedChange={setFlaggedOnly} />
        <Label htmlFor="flagged-only" className="text-sm cursor-pointer flex items-center gap-1">
          <AlertTriangle className="h-3.5 w-3.5 text-yellow-500" /> Flagged only
        </Label>
      </div>

      {cmcdEnabled && (
        <div className="flex items-center gap-2">
          <Switch id="streaming-only" data-testid="streaming-only-switch" checked={streamingOnly} onCheckedChange={setStreamingOnly} />
          <Label htmlFor="streaming-only" className="text-sm cursor-pointer flex items-center gap-1">
            <Film className="h-3.5 w-3.5 text-blue-500" /> Streaming sessions
          </Label>
        </div>
      )}

      <div className="flex items-center gap-2">
        <Label className="text-xs text-muted-foreground whitespace-nowrap">Min. requests</Label>
        <Input
          type="number" min={0} value={minReqs}
          onChange={(e) => setMinReqs(e.target.value === '' ? '' : Number(e.target.value))}
          placeholder={`≥ ${data?.min_reqs_flag ?? 1000}`} data-testid="min-reqs-input" className="h-8 w-24 text-sm text-right"
        />
      </div>

      <div className="flex items-center gap-2">
        <Label className="text-xs text-muted-foreground whitespace-nowrap">Min. 4xx%</Label>
        <Input
          type="number" min={0} max={100} value={min4xxPct}
          onChange={(e) => setMin4xxPct(e.target.value === '' ? '' : Number(e.target.value))}
          placeholder={`≥ ${data?.min_4xx_pct_flag ?? 20}`} data-testid="min-4xx-input" className="h-8 w-24 text-sm text-right"
        />
      </div>

      {(flaggedOnly || streamingOnly || minReqs !== '' || min4xxPct !== '') && (
        <Button
          variant="ghost" size="sm" className="h-8 text-xs ml-auto"
          onClick={() => { setFlaggedOnly(false); setStreamingOnly(false); setMinReqs(''); setMin4xxPct('') }}
        >
          Clear filters
        </Button>
      )}

      <Button variant="outline" size="sm" className="h-8 ml-auto" onClick={() => refetch()} disabled={isFetching}>
        <Clock className={cn('h-3.5 w-3.5 mr-2', isFetching && 'animate-spin')} />Refresh
      </Button>
    </div>
  )
}

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
  const [flaggedOnly, setFlaggedOnly] = useState(false)
  const [streamingOnly, setStreamingOnly] = useState(false)
  const [minReqs, setMinReqs] = useState<number | ''>('')
  const [min4xxPct, setMin4xxPct] = useState<number | ''>('')
  const isReady = useIsDataReady()
  const cmcdEnabled = useActiveServiceCmcdEnabled(activeServiceId)
  const qc = useQueryClient()

  useEffect(() => {
    return () => {
      qc.cancelQueries({ queryKey: ['sessions'] })
      qc.cancelQueries({ queryKey: queryKeys.scoringLabelsAll() })
    }
  }, [qc])

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
        cmcdEnabled={cmcdEnabled} flaggedOnly={flaggedOnly} setFlaggedOnly={setFlaggedOnly}
        streamingOnly={streamingOnly} setStreamingOnly={setStreamingOnly}
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

export default function SessionsPage() {
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
