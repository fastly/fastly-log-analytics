'use client'

import React from 'react'
import Link from 'next/link'
import { useQuery } from '@tanstack/react-query'
import { parseAsBoolean, useQueryState } from 'nuqs'
import { ColumnDef } from '@tanstack/react-table'
import { AlertTriangle, Clock, ExternalLink, Film, Globe, Shield, Users } from 'lucide-react'

import { client, extractApiError, unwrapClient } from '@/lib/api'
import { useFieldLabel } from '@/hooks/useBootstrap'
import { type LabelRow, type LabelValue } from '@/components/SessionScoring/ScoringAdminPanels'
import { FlagSessionPopover } from '@/components/SessionScoring/TopFlaggedTable'
import { useDateFormat } from '@/hooks/useSSE'
import { DataTable } from '@/components/DataTable'
import { Badge } from '@/components/ui/badge'
import { Dialog, DialogContent, DialogDescription, DialogHeader, DialogTitle } from '@/components/ui/dialog'
import { Switch } from '@/components/ui/switch'
import { Label } from '@/components/ui/label'
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from '@/components/ui/tooltip'
import { cn, buildSessionDashboardUrl, buildStreamDetailUrl, formatCount, formatCountMaybe } from '@/lib/utils'
import { OBJECT_TYPE_LABELS, STREAM_TYPE_LABELS, STREAMING_FORMAT_LABELS, cmcdLabel } from '@/app/streaming/_sections/StreamingClient'
import type { Session, SessionDetailResponse, SessionsResponse } from '@/types/api'

type SessionDetailRow = SessionDetailResponse['data'][number]

const CMCD_DETAIL_COLS = ['cmcd_ot', 'cmcd_br', 'cmcd_bl', 'cmcd_sf', 'cmcd_st']
const DEFAULT_VISIBLE_DETAIL_COLS = ['timestamp', 'host', 'url', 'method', 'edge', 'status', 'cache', 'ua', 'pop']
const INITIAL_DETAIL_COLUMN_ORDER = [...DEFAULT_VISIBLE_DETAIL_COLS, ...CMCD_DETAIL_COLS]

export function MetadataItem({ label, children, className }: { label: string; children: React.ReactNode; className?: string }) {
  return (
    <div className={cn('flex flex-col gap-0.5', className)}>
      <span className="text-[11px] sm:text-[10px] uppercase font-bold text-muted-foreground">{label}</span>
      <div className="text-xs">{children}</div>
    </div>
  )
}

export function RelativeTimeTooltip({
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

export interface SessionDetailProps {
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
  const [detailEdgeOnly, setDetailEdgeOnly] = useQueryState('edge_only', parseAsBoolean.withDefault(false))

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
    const cols = detailData?.columns
    if (!cols) return {}
    const streamingVisible = selectedSession?.is_streaming ? CMCD_DETAIL_COLS.filter((c) => cols.includes(c)) : []
    const allVisible = new Set([...DEFAULT_VISIBLE_DETAIL_COLS, ...streamingVisible])
    return Object.fromEntries(cols.map((col) => [col, allVisible.has(col)]))
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
                    <Switch id="detail-edge-only" checked={detailEdgeOnly} onCheckedChange={(v) => void setDetailEdgeOnly(v)} className="scale-75" />
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

export const SessionDetailModal = SessionDetail
export default SessionDetailModal
