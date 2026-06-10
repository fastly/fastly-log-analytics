'use client'

import React, { useState } from 'react'
import Link from 'next/link'
import { useQuery } from '@tanstack/react-query'
import { ColumnDef } from '@tanstack/react-table'
import { AlertTriangle, Clock, ExternalLink, Globe, Shield, Users } from 'lucide-react'

import { client } from '@/lib/api'
import { DataTable } from '@/components/DataTable'
import { FlagSessionPopover } from '@/components/SessionScoring/FlagSessionPopover'
import { Badge } from '@/components/ui/badge'
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog'
import { Label } from '@/components/ui/label'
import { MetadataItem } from '@/components/ui/metadata-item'
import { Switch } from '@/components/ui/switch'
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from '@/components/ui/tooltip'
import { useDateFormat } from '@/hooks/useDateFormat'
import { useFieldLabel } from '@/hooks/useFieldLabel'
import type { LabelRow, LabelValue } from '@/hooks/useScoringLabels'
import { buildSessionDashboardUrl } from '@/lib/session-urls'

interface SessionDetailProps {
  selectedSession: any | null
  setSelectedSession: (s: any | null) => void
  activeServiceId: string | null
  data: any
  labels: LabelRow[]
  labelBySid: Map<string, LabelValue>
  onFlagged: () => void
}

export function SessionDetail({
  selectedSession,
  setSelectedSession,
  activeServiceId,
  data,
  labels,
  labelBySid,
  onFlagged,
}: SessionDetailProps) {
  const { full, relative, abbr } = useDateFormat()
  const getFieldLabel = useFieldLabel()
  const [detailEdgeOnly, setDetailEdgeOnly] = useState(false)

  const { data: detailData, isLoading: isLoadingDetail } = useQuery({
    queryKey: ['sessions', 'detail', activeServiceId, selectedSession?.ip, selectedSession?.ja4, selectedSession?.session_start],
    queryFn: async ({ signal }) => {
      const { data } = await client.POST("/api/sessions/detail", {
        signal,
        body: {
          ip: selectedSession.ip,
          ja4: selectedSession.ja4,
          start_time: selectedSession.session_start,
          end_time: selectedSession.session_end,
        }
      })
      return data
    },
    enabled: !!activeServiceId && !!selectedSession
  })

  const detailColumns: ColumnDef<any>[] = React.useMemo(() => {
    if (!detailData?.columns) return []
    return detailData.columns.map(col => ({
      id: col,
      accessorFn: (row) => row[col],
      meta: { label: getFieldLabel(col) },
      header: getFieldLabel(col),
      cell: ({ row }: { row: any }) => {
        const value = row.original[col]
        if (col === 'timestamp') return (
          <TooltipProvider>
            <Tooltip>
              <TooltipTrigger render={
                <span className="whitespace-nowrap font-mono text-xs  border-b border-dotted border-muted-foreground/30">
                  {relative(value as string)}
                </span>
              } />
              <TooltipContent className="text-xs">
                {full(value as string)} {abbr()}
              </TooltipContent>
            </Tooltip>
          </TooltipProvider>
        )
        if (col === 'status') {
          const s = Number(value)
          return <Badge variant={s >= 500 ? 'destructive' : s >= 400 ? 'outline' : 'secondary'}>{s}</Badge>
        }
        if (col === 'resp_bytes' || col === 'elapsed') {
          return <span className="text-xs font-mono tabular-nums">{Number(value).toLocaleString()}</span>
        }
        return <span className="text-xs truncate max-w-[220px] inline-block">{String(value ?? '')}</span>
      }
    }))
  }, [detailData?.columns, relative, full, abbr, getFieldLabel])

  const initialDetailVisibility = React.useMemo(() => {
    if (!detailData?.columns) return {}
    const defaultVisible = ['timestamp', 'host', 'url', 'method', 'edge', 'status', 'cache', 'ua', 'pop']
    const visibility: Record<string, boolean> = {}
    detailData.columns.forEach(col => {
      visibility[col] = defaultVisible.includes(col)
    })
    return visibility
  }, [detailData?.columns])

  const initialDetailColumnOrder = ['timestamp', 'host', 'url', 'method', 'edge', 'status', 'cache', 'ua', 'pop']

  const filteredDetailData = React.useMemo(() => {
    const arr = detailData?.data || []
    return detailEdgeOnly ? arr.filter(row => row.edge === 1 || row.edge === true || row.edge === '1') : arr
  }, [detailData?.data, detailEdgeOnly])

  return (
    <Dialog open={!!selectedSession} onOpenChange={(open) => !open && setSelectedSession(null)}>
      <DialogContent className="max-w-6xl max-h-[85vh] flex flex-col p-4 md:p-6 overflow-hidden">
        <DialogHeader className="shrink-0 mb-2">            <DialogTitle className="flex items-center gap-2 text-base">
            <Users className="h-4 w-4" />
            Session: {selectedSession?.ip}
            {selectedSession?.flagged && <AlertTriangle className="h-4 w-4 text-yellow-500" />}
            {/* TODO: drop `as any` cast once openapi types include edge_sid. */}
            {(selectedSession as any)?.edge_sid && (
              <FlagSessionPopover
                serviceId={activeServiceId || ''}
                sid={(selectedSession as any).edge_sid}
                sampleIp={selectedSession?.ip}
                sampleUa={selectedSession?.ua}
                currentLabel={labelBySid.get((selectedSession as any).edge_sid) ?? null}
                currentLabelId={
                  labels.find((l) => l.sid === (selectedSession as any).edge_sid)?.id ?? null
                }
                onFlagged={onFlagged}
              />
            )}
          </DialogTitle>
        </DialogHeader>

        {/* Session metadata grid */}
        <div className="grid grid-cols-2 md:grid-cols-4 gap-3 p-3 bg-muted/30 rounded-lg shrink-0">
          <MetadataItem label="Start">
            {selectedSession && (
              <TooltipProvider>
                <Tooltip>
                  <TooltipTrigger render={
                    <span className="flex items-center gap-1 ">
                      <Clock className="h-3 w-3 shrink-0" />
                      {relative(selectedSession.session_start)}
                    </span>
                  } />
                  <TooltipContent className="text-xs">
                    {full(selectedSession.session_start)} {abbr()}
                  </TooltipContent>
                </Tooltip>
              </TooltipProvider>
            )}
          </MetadataItem>
          <MetadataItem label="End">
            {selectedSession && (
              <TooltipProvider>
                <Tooltip>
                  <TooltipTrigger render={
                    <span className="">
                      {relative(selectedSession.session_end)}
                    </span>
                  } />
                  <TooltipContent className="text-xs">
                    {full(selectedSession.session_end)} {abbr()}
                  </TooltipContent>
                </Tooltip>
              </TooltipProvider>
            )}
          </MetadataItem>
          <MetadataItem label="Country">
            <span className="flex items-center gap-1">
              <Globe className="h-3 w-3" /> {selectedSession?.country || '—'}
            </span>
          </MetadataItem>
          <MetadataItem label="ASN">
            {selectedSession?.asn ? `AS${selectedSession.asn}` : '—'}
          </MetadataItem>
          <MetadataItem label="Requests">
            <span className="font-semibold">{selectedSession?.req_count?.toLocaleString()}</span>
          </MetadataItem>
          <MetadataItem label="Unique URLs">
            {selectedSession?.unique_urls ?? '—'}
          </MetadataItem>
          <MetadataItem label="Edge / Shield">
            <span className="flex items-center gap-1">
              <Shield className="h-3 w-3" />
              {selectedSession?.edge_count ?? 0} / {selectedSession?.shield_count ?? 0}
            </span>
          </MetadataItem>
          <MetadataItem label="Med. RTT">
            {selectedSession?.median_rtt_ms ? `${selectedSession.median_rtt_ms.toFixed(1)}ms` : '—'}
          </MetadataItem>
        </div>

        {/* Identifiers */}
        <div className="flex flex-col gap-3 px-1 shrink-0">
          <div className="flex flex-wrap items-start gap-x-6 gap-y-3">
            <MetadataItem label="IP Address" className="min-w-0">
              <Link
                href={buildSessionDashboardUrl(activeServiceId || '', 'ip', selectedSession?.ip, selectedSession?.session_start, selectedSession?.session_end)}
                className="flex items-center gap-1.5 text-sm hover:underline group"
                title="View in Dashboard"
                target="_blank"
                rel="noopener noreferrer"
              >
                <span>{selectedSession?.ip}</span>
                <ExternalLink className="h-3 w-3 text-muted-foreground group-hover:text-primary transition-colors shrink-0" />
              </Link>
            </MetadataItem>

            {selectedSession?.ja4 && (
              <MetadataItem label="JA4 Fingerprint" className="min-w-0">
                <Link
                  href={buildSessionDashboardUrl(activeServiceId || '', 'ja4', selectedSession.ja4, selectedSession?.session_start, selectedSession?.session_end)}
                  className="flex items-center gap-1.5 text-sm hover:underline group"
                  title="View in Dashboard"
                  target="_blank"
                  rel="noopener noreferrer"
                >
                  <span className="truncate max-w-[300px]">{selectedSession.ja4}</span>
                  <ExternalLink className="h-3 w-3 text-muted-foreground group-hover:text-primary transition-colors shrink-0" />
                </Link>
              </MetadataItem>
            )}

            {selectedSession?.ua && (
              <MetadataItem label="User-Agent" className="min-w-0 flex-1 basis-full md:basis-0">
                <Link
                  href={buildSessionDashboardUrl(activeServiceId || '', 'ua', selectedSession.ua, selectedSession?.session_start, selectedSession?.session_end)}
                  className="flex items-start gap-1.5 text-sm hover:underline group"
                  title="View in Dashboard"
                  target="_blank"
                  rel="noopener noreferrer"
                >
                  <span className="break-all line-clamp-2 leading-tight">{selectedSession.ua}</span>
                  <ExternalLink className="h-3 w-3 text-muted-foreground group-hover:text-primary transition-colors shrink-0 mt-0.5" />
                </Link>
              </MetadataItem>
            )}            </div>
        </div>

        {/* Timeline */}
        <div className="flex-1 overflow-auto min-h-0 flex flex-col">
          <DataTable
            title={
              <div className="flex items-center gap-4">
                <h4 className="text-sm font-semibold">Request Timeline</h4>
                {data?.has_edge && (
                  <div className="flex items-center gap-2">
                    <Switch
                      id="detail-edge-only"
                      checked={detailEdgeOnly}
                      onCheckedChange={setDetailEdgeOnly}
                      className="scale-75"
                    />
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
            initialColumnOrder={initialDetailColumnOrder}
            initialSorting={[{ id: 'timestamp', desc: true }]}
          />
        </div>
      </DialogContent>
    </Dialog>
  )
}
