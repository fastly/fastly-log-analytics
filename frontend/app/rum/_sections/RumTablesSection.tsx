'use client'

import React, { useMemo, useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import type { ColumnDef } from '@/lib/table'
import { Activity, Globe, Monitor, Server, ShieldAlert, Terminal } from 'lucide-react'
import { AnalyticsCard } from '@/components/AnalyticsCard'
import { DataTable, DataTableReadonly, PopLabel } from '@/components/DataTable'
import { Button } from '@/components/ui/button'
import { Dialog, DialogContent, DialogDescription, DialogHeader, DialogTitle } from '@/components/ui/dialog'
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select'
import { adminJsonResult } from '@/lib/api'
import { formatCount } from '@/lib/format'
import { formatDate } from '@/lib/utils'
import type { StripIndex, components } from '@/types/api'

export type RumWorstPage = components['schemas']['RumWorstPage']
export type RumJsError = components['schemas']['RumJsError']
export type RumSlowestUrlItem = components['schemas']['RumSlowestUrlItem']
export type RumSlowestNetworkItem = components['schemas']['RumSlowestNetworkItem']
export type RumRequestLogResponse = components['schemas']['RumRequestLogResponse']

export interface RumRawLog {
  city?: string
  region?: string
  country?: string
  pop?: string
  tls?: string
  ttfb?: number | string | null
  req_id?: string | null
  resource_url?: string | null
  faro_payload?: unknown
  meta?: unknown
  measurements?: unknown
  events?: unknown
  [key: string]: unknown
}

export type RumLiveEvent = Omit<StripIndex<components['schemas']['RumLiveEventItem']>, 'browser' | 'os' | 'raw_log'> & {
  browser: components['schemas']['RumLiveEventItem']['browser'] | { name?: string }
  os: components['schemas']['RumLiveEventItem']['os'] | { name?: string }
  req_id?: string | null
  resource_url?: string | null
  metric_name?: string | null
  metric_value?: number | null
  raw_log?: RumRawLog
}

const worstPageCol = (
  id: string,
  header: string,
  size: number,
  accessorFn: (p: RumWorstPage) => string | number,
  render: (p: RumWorstPage) => React.ReactNode,
): ColumnDef<RumWorstPage> => ({ id, header, size, accessorFn, cell: ({ row }) => render(row.original) })

export const WORST_PAGE_COLUMNS: ColumnDef<RumWorstPage>[] = [
  worstPageCol('path', 'Path', 260, (p) => p.path, (p) => <span className="font-semibold font-mono text-xs">{p.path}</span>),
  worstPageCol('views', 'Pageviews', 110, (p) => p.views, (p) => <span className="block text-right text-sm">{p.views}</span>),
  worstPageCol('avg_load_time', 'Avg Load (s)', 120, (p) => p.avg_load_time ?? -1, (p) => (
    <span className="block text-right text-sm">{p.avg_load_time != null ? `${p.avg_load_time.toFixed(1)}s` : '—'}</span>
  )),
  worstPageCol('lcp_p75', 'LCP p75', 100, (p) => p.lcp_p75 ?? -1, (p) => (
    <span className="block text-right text-sm text-amber-500">{p.lcp_p75 != null ? `${p.lcp_p75.toFixed(2)}s` : '—'}</span>
  )),
  worstPageCol('cls_p75', 'CLS p75', 100, (p) => p.cls_p75 ?? -1, (p) => (
    <span className="block text-right text-sm">{p.cls_p75 != null ? p.cls_p75.toFixed(2) : '—'}</span>
  )),
]

export function cleanRumField(v: string | null | undefined): string {
  if (!v) return ''
  const trimmed = String(v).trim()
  if (!trimmed || /^(unknown|\?|null|none|-|—)$/i.test(trimmed)) return ''
  return trimmed
}

export function envName(v: string | { name?: string } | null | undefined): string {
  const raw = typeof v === 'object' && v ? v.name : v
  return cleanRumField(raw) || 'Unknown'
}

export function formatRumEnvironment(
  browser: string | { name?: string } | null | undefined,
  os: string | { name?: string } | null | undefined,
): string {
  const b = cleanRumField(typeof browser === 'object' && browser ? browser.name : browser)
  const o = cleanRumField(typeof os === 'object' && os ? os.name : os)
  if (b && o) return `${b} on ${o}`
  return b || o || '—'
}

const liveCol = (
  id: string,
  label: string,
  cell: (r: RumLiveEvent) => React.ReactNode,
  accessorFn?: (r: RumLiveEvent) => unknown,
): ColumnDef<RumLiveEvent> => ({
  id,
  ...(accessorFn ? { accessorFn } : { accessorKey: id }),
  meta: { label },
  header: label,
  cell: ({ row }) => cell(row.original),
})

export type RumSortMetric = 'p99' | 'p95' | 'p50' | 'avg'

const fmtMs = (val: number | null | undefined) => (val != null && Number.isFinite(val) ? `${val.toFixed(1)}ms` : '—')

const makeResourceColumns = <T extends RumSlowestUrlItem | RumSlowestNetworkItem>(
  labelHeader: string,
  getLabel: (row: T) => string,
): ColumnDef<T>[] => [
  {
    id: 'name',
    header: labelHeader,
    accessorFn: getLabel,
    cell: ({ row }) => (
      <span className="font-mono text-xs block max-w-[240px] truncate" title={getLabel(row.original)}>
        {getLabel(row.original)}
      </span>
    ),
  },
  {
    id: 'requests',
    header: 'Reqs',
    accessorKey: 'requests',
    cell: ({ row }) => <span className="font-mono text-xs tabular-nums">{formatCount(row.original.requests)}</span>,
  },
  {
    id: 'avg',
    header: 'Client Avg',
    accessorKey: 'avg',
    cell: ({ row }) => <span className="font-mono text-xs tabular-nums">{fmtMs(row.original.avg)}</span>,
  },
  {
    id: 'p50',
    header: 'P50',
    accessorKey: 'p50',
    cell: ({ row }) => <span className="font-mono text-xs tabular-nums">{fmtMs(row.original.p50)}</span>,
  },
  {
    id: 'p95',
    header: 'P95',
    accessorKey: 'p95',
    cell: ({ row }) => <span className="font-mono text-xs tabular-nums">{fmtMs(row.original.p95)}</span>,
  },
  {
    id: 'p99',
    header: 'P99',
    accessorKey: 'p99',
    cell: ({ row }) => <span className="font-mono text-xs tabular-nums font-semibold">{fmtMs(row.original.p99)}</span>,
  },
  {
    id: 'edge_avg',
    header: 'Edge Avg',
    accessorKey: 'edge_avg',
    cell: ({ row }) => <span className="font-mono text-xs tabular-nums text-muted-foreground">{fmtMs(row.original.edge_avg)}</span>,
  },
  {
    id: 'edge_p95',
    header: 'Edge P95',
    accessorKey: 'edge_p95',
    cell: ({ row }) => <span className="font-mono text-xs tabular-nums text-muted-foreground">{fmtMs(row.original.edge_p95)}</span>,
  },
]

const CLIENT_URL_COLUMNS = makeResourceColumns<RumSlowestUrlItem>('URL', (r) => r.url)
const CLIENT_NETWORK_COLUMNS = makeResourceColumns<RumSlowestNetworkItem>('Network', (r) => r.label || r.asn)

export function RumClientResourceTablesSection({
  slowestUrls = [],
  slowestNetworks = [],
}: {
  slowestUrls?: RumSlowestUrlItem[]
  slowestNetworks?: RumSlowestNetworkItem[]
}) {
  const [sortMetric, setSortMetric] = useState<RumSortMetric>('p99')

  const sortedUrls = useMemo(
    () => [...slowestUrls].sort((a, b) => Number(b[sortMetric] ?? 0) - Number(a[sortMetric] ?? 0) || b.requests - a.requests),
    [slowestUrls, sortMetric],
  )
  const sortedNetworks = useMemo(
    () => [...slowestNetworks].sort((a, b) => Number(b[sortMetric] ?? 0) - Number(a[sortMetric] ?? 0) || b.requests - a.requests),
    [slowestNetworks, sortMetric],
  )

  const sortSelector = (
    <Select value={sortMetric} onValueChange={(val) => setSortMetric(val as RumSortMetric)}>
      <SelectTrigger className="h-7 w-[110px] text-xs" aria-label="Sort client latency">
        <SelectValue placeholder="Sort by" />
      </SelectTrigger>
      <SelectContent>
        <SelectItem value="p99">Sort: P99</SelectItem>
        <SelectItem value="p95">Sort: P95</SelectItem>
        <SelectItem value="p50">Sort: P50</SelectItem>
        <SelectItem value="avg">Sort: Avg</SelectItem>
      </SelectContent>
    </Select>
  )

  return (
    <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
      <AnalyticsCard
        title="Slowest URLs (Client TTLB vs Edge)"
        icon={<Server className="h-4 w-4" />}
        headerAction={sortSelector}
        isEmpty={sortedUrls.length === 0}
        contentClassName="p-0"
      >
        <DataTableReadonly
          columns={CLIENT_URL_COLUMNS}
          data={sortedUrls}
          hideToolbar
          showPagination={false}
          tableCaption="Slowest URLs by client TTLB"
          emptyMessage="No client resource timing data in this window"
        />
      </AnalyticsCard>

      <AnalyticsCard
        title="Slowest Networks (Client TTLB vs Edge)"
        icon={<Globe className="h-4 w-4" />}
        headerAction={sortSelector}
        isEmpty={sortedNetworks.length === 0}
        contentClassName="p-0"
      >
        <DataTableReadonly
          columns={CLIENT_NETWORK_COLUMNS}
          data={sortedNetworks}
          hideToolbar
          showPagination={false}
          tableCaption="Slowest networks by client TTLB"
          emptyMessage="No client network timing data in this window"
        />
      </AnalyticsCard>
    </div>
  )
}

export function RumWorstPagesAndErrorsSection({
  worstPages,
  errors,
}: {
  worstPages: RumWorstPage[]
  errors: RumJsError[]
}) {
  return (
    <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
      <AnalyticsCard title="Worst Performing Pages" icon={<Monitor />}>
        <DataTableReadonly
          columns={WORST_PAGE_COLUMNS}
          data={worstPages}
          hideToolbar
          showPagination={false}
          tableCaption="Worst performing pages"
          emptyMessage="No page data in this window"
        />
      </AnalyticsCard>

      <AnalyticsCard title="JavaScript Runtime Exceptions" icon={<ShieldAlert className="text-rose-500" />}>
        <div className="space-y-4">
          {errors.length === 0 ? (
            <p className="text-sm text-muted-foreground py-4 text-center">No JS errors captured in this window</p>
          ) : (
            errors.map((e: RumJsError) => (
              <div key={`${e.message}-${e.file}-${e.line}-${e.col}`} className="p-3 bg-rose-500/5 rounded-lg border border-rose-500/10 space-y-1.5">
                <div className="flex justify-between items-start gap-4">
                  <p className="text-xs font-semibold text-rose-600 font-mono break-all">{e.message}</p>
                  <span className="bg-rose-500/10 text-rose-600 px-2 py-0.5 rounded text-[10px] font-bold">{e.count}x</span>
                </div>
                <p className="text-[10px] text-muted-foreground font-mono">File: {e.file}:{e.line}:{e.col}</p>
              </div>
            ))
          )}
        </div>
      </AnalyticsCard>
    </div>
  )
}

export function RumLiveActivitySection({
  liveEvents,
  isLiveEventsLoading,
  timezone,
  serviceId,
}: {
  liveEvents: RumLiveEvent[] | undefined
  isLiveEventsLoading: boolean
  timezone: string
  serviceId?: string | null
}) {
  const [selectedEvent, setSelectedEvent] = useState<RumLiveEvent | null>(null)
  const selectedReqId = selectedEvent
    ? String(selectedEvent.req_id || selectedEvent.raw_log?.req_id || '').trim()
    : ''

  const { data: matchedLog, isLoading: isMatchedLogLoading } = useQuery<RumRequestLogResponse | null>({
    queryKey: ['rum-request-log', serviceId, selectedReqId, selectedEvent?.time],
    queryFn: async ({ signal }) => {
      if (!serviceId || !selectedReqId) return null
      const params = new URLSearchParams({ req_id: selectedReqId })
      if (selectedEvent?.time) params.set('timestamp', selectedEvent.time)
      const res = await adminJsonResult<'/api/services/{service_id}/rum/request-log', RumRequestLogResponse>(
        '/api/services/{service_id}/rum/request-log',
        `/api/services/${encodeURIComponent(serviceId)}/rum/request-log?${params.toString()}`,
        { signal },
      )
      if (!res.ok) return null
      return res.data ?? null
    },
    enabled: !!serviceId && !!selectedReqId && selectedEvent !== null,
    retry: false,
  })

  const columns = React.useMemo<ColumnDef<RumLiveEvent>[]>(() => [
    liveCol('type', 'Status', (r) => {
      const isErr = r.type === 'error'
      return (
        <span className={`inline-flex items-center gap-1.5 px-2 py-1 rounded-full text-[10px] font-bold ${
          isErr ? 'bg-rose-500/10 text-rose-500 border border-rose-500/20' : 'bg-emerald-500/10 text-emerald-500 border border-emerald-500/20'
        }`}>
          <span className={`h-1.5 w-1.5 rounded-full ${isErr ? 'bg-rose-500' : 'bg-emerald-500'}`} />
          {isErr ? 'Error' : 'Success'}
        </span>
      )
    }),
    liveCol('time', 'Time', (r) => <span className="font-mono text-xs text-muted-foreground whitespace-nowrap">{formatDate(r.time, timezone, 'HH:mm:ss')}</span>),
    liveCol('path', 'Page Path', (r) => <span className="font-semibold font-mono text-xs text-zinc-300 block max-w-[200px] truncate" title={r.path}>{r.path}</span>),
    liveCol('desc', 'Description', (r) => <span className="text-muted-foreground text-xs block max-w-[250px] truncate" title={r.desc}>{r.desc}</span>),
    liveCol('environment', 'Environment', (r) => {
      const label = formatRumEnvironment(r.browser, r.os)
      return <span className="text-muted-foreground text-xs block max-w-[150px] truncate" title={label === '—' ? undefined : label}>{label}</span>
    }, (r) => formatRumEnvironment(r.browser, r.os)),
    liveCol('geo', 'Location', (r) => {
      const raw = r.raw_log || {}
      const location = [cleanRumField(raw.city), cleanRumField(raw.region), cleanRumField(raw.country)].filter(Boolean).join(', ')
      return <span className="text-muted-foreground text-xs block max-w-[150px] truncate" title={location || undefined}>{location || '—'}</span>
    }, (r) => [cleanRumField(r.raw_log?.city), cleanRumField(r.raw_log?.region), cleanRumField(r.raw_log?.country)].filter(Boolean).join(', ')),
    liveCol('pop', 'Edge POP', (r) => {
      const pop = cleanRumField(r.raw_log?.pop)
      return <PopLabel code={pop || null} className="font-mono text-xs" />
    }, (r) => cleanRumField(r.raw_log?.pop)),
    liveCol('tls', 'TLS', (r) => {
      const tls = cleanRumField(r.raw_log?.tls)
      return <span className="font-mono text-xs text-muted-foreground">{tls ? `TLS ${tls.replace(/^TLSv?/i, '')}` : '—'}</span>
    }, (r) => cleanRumField(r.raw_log?.tls)),
    liveCol('ttfb', 'TTFB (Edge)', (r) => {
      const val = r.raw_log?.ttfb != null ? Number(r.raw_log.ttfb) : NaN
      return <span className="font-mono text-xs text-muted-foreground">{Number.isFinite(val) ? `${Math.round(val)}ms` : '—'}</span>
    }, (r) => {
      const val = r.raw_log?.ttfb != null ? Number(r.raw_log.ttfb) : NaN
      return Number.isFinite(val) ? val : null
    }),
    {
      id: 'actions',
      header: () => <span className="sr-only">Actions</span>,
      cell: ({ row }) => (
        <div className="text-right">
          <Button variant="outline" size="sm" className="h-8 text-xs font-medium" onClick={(e) => { e.stopPropagation(); setSelectedEvent(row.original) }}>
            Details
          </Button>
        </div>
      ),
    },
  ], [timezone])

  return (
    <>
      <AnalyticsCard title="Live Activity Monitor" icon={<Terminal className="h-4 w-4" />}>
        <div className="w-full">
          <DataTable
            columns={columns}
            data={liveEvents || []}
            searchKey="path"
            isLoading={isLiveEventsLoading}
            showPagination={false}
            emptyMessage="Waiting for real-time RUM user events..."
            onRowClick={(row) => setSelectedEvent(row)}
          />
        </div>
      </AnalyticsCard>

      <Dialog open={selectedEvent !== null} onOpenChange={(open) => { if (!open) setSelectedEvent(null) }}>
        <DialogContent className="max-w-2xl">
          <DialogHeader>
            <DialogTitle className="text-lg font-bold flex items-center gap-2">
              <Activity className={selectedEvent?.type === 'error' ? 'text-rose-500 h-5 w-5' : 'text-emerald-500 h-5 w-5'} />
              RUM Beacon Details
            </DialogTitle>
            <DialogDescription>Extracted client characteristics and complete raw payload captured at the Fastly edge.</DialogDescription>
          </DialogHeader>

          {selectedEvent && (() => {
            let faroPayload: unknown = null
            let edgeMetadata: Record<string, unknown> | null = null
            const rawLog = selectedEvent.raw_log
            if (rawLog) {
              if (rawLog.faro_payload) {
                faroPayload = rawLog.faro_payload
                edgeMetadata = { ...rawLog }
                delete edgeMetadata.faro_payload
              } else if (rawLog.meta && (rawLog.measurements || rawLog.events)) {
                faroPayload = rawLog
                edgeMetadata = {
                  time: selectedEvent.time,
                  type: selectedEvent.type,
                  path: selectedEvent.path,
                  desc: selectedEvent.desc,
                  browser: selectedEvent.browser,
                  os: selectedEvent.os,
                }
              } else {
                edgeMetadata = rawLog
              }
            }

            return (
              <div className="space-y-4 my-2 text-xs">
                <div className="grid grid-cols-2 gap-4 bg-muted/20 p-4 rounded-lg border border-border">
                  {[
                    ['Timestamp', <span key="t" className="font-mono">{formatDate(selectedEvent.time, timezone, 'yyyy-MM-dd HH:mm:ss (zzz)')}</span>],
                    ['Event Type', <span key="e" className={`capitalize font-semibold ${selectedEvent.type === 'error' ? 'text-rose-500' : 'text-emerald-500'}`}>{selectedEvent.type}</span>],
                    ['Url Path', <span key="p" className="font-mono font-semibold">{selectedEvent.path}</span>],
                    ['Client Platform', <span key="c">{formatRumEnvironment(selectedEvent.browser, selectedEvent.os)}</span>],
                  ].map(([label, val]) => (
                    <div key={String(label)}><span className="text-muted-foreground block mb-1">{label}</span>{val}</div>
                  ))}
                  <div className="col-span-2">
                    <span className="text-muted-foreground block mb-1">Description</span>
                    <span className={`font-mono block p-2 rounded ${selectedEvent.type === 'error' ? 'bg-rose-500/10 text-rose-500 border border-rose-500/20' : 'bg-muted/40 text-foreground'}`}>
                      {selectedEvent.desc}
                    </span>
                  </div>
                </div>

                {faroPayload != null && (
                  <div className="space-y-2">
                    <span className="text-muted-foreground font-semibold block">Client Faro JSON Payload (Full)</span>
                    <div className="bg-emerald-950/5 dark:bg-emerald-950/15 border border-emerald-500/15 rounded-lg p-4 overflow-auto max-h-[250px] font-mono text-[11px] leading-relaxed text-foreground">
                      <pre>{JSON.stringify(faroPayload, null, 2)}</pre>
                    </div>
                  </div>
                )}

                <div className="space-y-2">
                  <span className="text-muted-foreground font-semibold block">
                    {faroPayload ? 'Edge Log Representation (Extracted Row Metadata)' : 'Raw Beacon Log JSON'}
                  </span>
                  <div className="bg-muted/30 border border-border rounded-lg p-4 overflow-auto max-h-[250px] font-mono text-[11px] leading-relaxed text-foreground">
                    <pre>{JSON.stringify(edgeMetadata || rawLog || {}, null, 2)}</pre>
                  </div>
                </div>

                {selectedReqId && (
                  <div className="space-y-2">
                    <div className="flex items-center justify-between">
                      <span className="text-muted-foreground font-semibold block">Matching Edge Request Log</span>
                      <span className="font-mono text-[11px] text-muted-foreground">req_id: {selectedReqId}</span>
                    </div>
                    {isMatchedLogLoading ? (
                      <div className="bg-muted/20 border border-border rounded-lg p-3 text-muted-foreground">
                        Looking up edge request log line…
                      </div>
                    ) : matchedLog?.found && matchedLog.request ? (
                      <div className="bg-muted/20 border border-border rounded-lg p-3 space-y-2">
                        <div className="grid grid-cols-2 sm:grid-cols-3 gap-2 text-[11px]">
                          <div>
                            <span className="text-muted-foreground block">Method &amp; Status</span>
                            <span className="font-mono font-semibold">
                              {String(matchedLog.request.method ?? '—')} · {String(matchedLog.request.status ?? '—')}
                            </span>
                          </div>
                          <div>
                            <span className="text-muted-foreground block">Edge Elapsed / Client TTLB</span>
                            <span className="font-mono font-semibold">
                              {matchedLog.request.elapsed_ms != null ? `${matchedLog.request.elapsed_ms}ms` : '—'}
                              {selectedEvent.metric_value != null && selectedEvent.metric_name === 'ttlb'
                                ? ` / ${Math.round(selectedEvent.metric_value)}ms`
                                : ''}
                            </span>
                          </div>
                          <div>
                            <span className="text-muted-foreground block">Cache / POP</span>
                            <span className="font-mono">
                              {String(matchedLog.request.cache ?? '—')} · {String(matchedLog.request.pop ?? '—')}
                            </span>
                          </div>
                        </div>
                        <div className="bg-background/60 border border-border rounded p-2 overflow-auto max-h-[180px] font-mono text-[11px]">
                          <pre>{JSON.stringify(matchedLog.request, null, 2)}</pre>
                        </div>
                      </div>
                    ) : (
                      <div className="bg-muted/20 border border-border rounded-lg p-3 text-muted-foreground">
                        No matching edge request log line found for this request ID in the retention window.
                      </div>
                    )}
                  </div>
                )}
              </div>
            )
          })()}
        </DialogContent>
      </Dialog>
    </>
  )
}
