'use client'

import React, { useState } from 'react'
import Link from 'next/link'
import { useQuery, useQueryClient } from '@tanstack/react-query'
import { client } from '@/lib/api'
import { useIsDataReady } from '@/hooks/useIsDataReady'
import { useFieldLabel } from '@/hooks/useFieldLabel'
import { useScoringLabels } from '@/hooks/useScoringLabels'
import { FlagSessionPopover } from '@/components/SessionScoring/FlagSessionPopover'
import { DataTable } from '@/components/DataTable'
import { ColumnDef } from '@tanstack/react-table'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Switch } from '@/components/ui/switch'
import { Label } from '@/components/ui/label'
import { Input } from '@/components/ui/input'
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle
} from '@/components/ui/dialog'
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "@/components/ui/tooltip"
import { useDateFormat } from '@/hooks/useDateFormat'
import { Users, AlertTriangle, ExternalLink, Clock, Globe, Shield } from 'lucide-react'
import { MetadataItem } from '@/components/ui/metadata-item'
import { cn } from '@/lib/utils'
import { ReportLayout } from '@/components/ReportLayout'
import { UpdatingBadge } from '@/components/UpdatingBadge'
import { buildSessionDashboardUrl } from '@/lib/session-urls'

export default function SessionsPage() {
  const [selectedSession, setSelectedSession] = useState<any | null>(null)
  const { full, relative, abbr } = useDateFormat()

  // ── Filter state ─────────────────────────────────────────────────────────
  const [flaggedOnly, setFlaggedOnly] = useState(false)
  const [minReqs, setMinReqs] = useState<number | ''>('')
  const [min4xxPct, setMin4xxPct] = useState<number | ''>('')
  const [detailEdgeOnly, setDetailEdgeOnly] = useState(false)

  const getFieldLabel = useFieldLabel()

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

        const qc = useQueryClient()
        const { labelBySid, labels } = useScoringLabels(activeServiceId || '', {
          enabled: !!activeServiceId,
        })
        const onFlagged = React.useCallback(() => {
          qc.invalidateQueries({ queryKey: ['scoring-labels', activeServiceId] })
        }, [qc, activeServiceId])

        const { data, isLoading, isFetching, refetch } = useQuery({
    queryKey: ['sessions', 'list', activeServiceId, startTime, endTime, filterPayload, flaggedOnly, minReqs, min4xxPct],
    queryFn: async ({ signal }) => {
      const { data } = await client.POST("/api/sessions", { signal, 
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
      return data as any
    },
    enabled: isReady
  })

  const isLoadingInitial = isLoading || (isFetching && !data)


  const { data: detailData, isLoading: isLoadingDetail } = useQuery({
    queryKey: ['sessions', 'detail', activeServiceId, selectedSession?.ip, selectedSession?.ja4, selectedSession?.session_start],
    queryFn: async ({ signal }) => {
      const { data } = await client.POST("/api/sessions/detail", { signal, 
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

  // ── Main table columns ────────────────────────────────────────────────────

  const columns: ColumnDef<any>[] = React.useMemo(() => {
    const cols: ColumnDef<any>[] = [
      { 
        accessorKey: 'ip', 
        header: 'IP Address',
        cell: ({ row }) => (
          <div className="flex items-center gap-1.5">
            <span className="font-medium">{row.getValue('ip') as string}</span>
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
        cell: ({ row }) => (row.getValue('unique_urls') as number)?.toLocaleString() ?? '—',
      },
      {
        accessorKey: 'asn',
        header: 'ASN',
        cell: ({ row }) => {
          const asn = row.getValue('asn') as number | undefined
          return asn ? `AS${asn}` : '—'
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
            <span className={pct >= 20 ? 'text-yellow-600 dark:text-yellow-400 font-semibold' : ''}>
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

    // TODO: drop `as any` once openapi types regenerate to include has_edge_sid/edge_sid.
    if ((data as any)?.has_edge_sid) {
      cols.push({
        id: '__flag',
        header: 'Flag',
        cell: ({ row }) => {
          const sid = (row.original as any).edge_sid as string | undefined
          if (!sid) return null
          const labelRow = labels.find((l) => l.sid === sid)
          return (
            <FlagSessionPopover
              serviceId={activeServiceId || ''}
              sid={sid}
              sampleIp={row.original.ip}
              sampleUa={row.original.ua}
              currentLabel={labelBySid.get(sid) ?? null}
              currentLabelId={labelRow?.id ?? null}
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
        <Button variant="ghost" size="sm" className="h-7" onClick={() => setSelectedSession(row.original)}>
          Details <ExternalLink className="ml-1.5 h-3 w-3" />
        </Button>
      ),
    })

    return cols
  }, [data, relative, full, abbr, labels, labelBySid, activeServiceId, onFlagged])

  // ── Detail dialog columns (all available from backend) ───────────────────

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
    <>
      <div className="flex flex-col sm:flex-row items-start sm:items-center gap-2 sm:gap-4 shrink-0 mb-4 justify-end">
        <UpdatingBadge />
      </div>
      {/* ── Filter bar ── */}
      <div className={cn("flex flex-wrap items-center gap-4 p-3 border rounded-lg bg-muted/30 transition-opacity duration-100", isFetching && !isLoadingInitial && "opacity-40 pointer-events-none")}>
        <div className="flex items-center gap-2">
          <Switch
            id="flagged-only"
            checked={flaggedOnly}
            onCheckedChange={setFlaggedOnly}
          />
          <Label htmlFor="flagged-only" className="text-sm cursor-pointer flex items-center gap-1">
            <AlertTriangle className="h-3.5 w-3.5 text-yellow-500" /> Flagged only
          </Label>
        </div>

        <div className="flex items-center gap-2">
          <Label className="text-xs text-muted-foreground whitespace-nowrap">Min. requests</Label>
          <Input
            type="number"
            min={0}
            value={minReqs}
            onChange={e => setMinReqs(e.target.value === '' ? '' : Number(e.target.value))}
            placeholder={data?.min_reqs_flag?.toString() ?? "1000"}
            className="h-8 w-20 text-sm text-right"
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
            placeholder={data?.min_4xx_pct_flag?.toString() ?? "20"}
            className="h-8 w-20 text-sm text-right"
          />
        </div>

        {(flaggedOnly || minReqs !== '' || min4xxPct !== '') && (
          <Button
            variant="ghost"
            size="sm"
            className="h-8 text-xs ml-auto"
            onClick={() => { setFlaggedOnly(false); setMinReqs(''); setMin4xxPct('') }}
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

      {/* ── Sessions table ── */}
      <div className={cn("border rounded-lg transition-opacity duration-100", isFetching && !isLoadingInitial && "opacity-40 pointer-events-none")}>
        <DataTable
          columns={columns}
          data={data?.sessions || []}
          isLoading={isLoadingInitial}
          searchKey="ip"
          onRowClick={setSelectedSession}
        />
      </div>
      {/* ── Session detail dialog ── */}
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
      </>
    )
  }}
  </ReportLayout>
)
}
