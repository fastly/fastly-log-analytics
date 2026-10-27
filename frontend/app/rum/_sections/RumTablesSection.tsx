'use client'

import React, { useState } from 'react'
import { ColumnDef } from '@tanstack/react-table'
import { Activity, Monitor, ShieldAlert, Terminal } from 'lucide-react'
import { AnalyticsCard } from '@/components/AnalyticsCard'
import { DataTable, DataTableReadonly } from '@/components/DataTable'
import { Button } from '@/components/ui/button'
import { Dialog, DialogContent, DialogDescription, DialogHeader, DialogTitle } from '@/components/ui/dialog'
import { formatDate } from '@/lib/utils'
import type { StripIndex, components } from '@/types/api'

export type RumWorstPage = components['schemas']['RumWorstPage']
export type RumJsError = components['schemas']['RumJsError']

export interface RumRawLog {
  city?: string
  region?: string
  country?: string
  pop?: string
  tls?: string
  ttfb?: number | string | null
  faro_payload?: unknown
  meta?: unknown
  measurements?: unknown
  events?: unknown
  [key: string]: unknown
}

export type RumLiveEvent = Omit<StripIndex<components['schemas']['RumLiveEventItem']>, 'browser' | 'os' | 'raw_log'> & {
  browser: components['schemas']['RumLiveEventItem']['browser'] | { name?: string }
  os: components['schemas']['RumLiveEventItem']['os'] | { name?: string }
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

export function envName(v: string | { name?: string } | null | undefined): string {
  return (typeof v === 'object' && v ? v.name : v) || 'Unknown'
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
}: {
  liveEvents: RumLiveEvent[] | undefined
  isLiveEventsLoading: boolean
  timezone: string
}) {
  const [selectedEvent, setSelectedEvent] = useState<RumLiveEvent | null>(null)

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
      const label = `${envName(r.browser)} on ${envName(r.os)}`
      return <span className="text-muted-foreground text-xs block max-w-[150px] truncate" title={label}>{label}</span>
    }, (r) => `${envName(r.browser)} on ${envName(r.os)}`),
    liveCol('geo', 'Location', (r) => {
      const raw = r.raw_log || {}
      const location = [raw.city, raw.region, raw.country].filter(Boolean).join(', ')
      return <span className="text-muted-foreground text-xs block max-w-[150px] truncate" title={location}>{location || '—'}</span>
    }, (r) => `${r.raw_log?.city || ''}, ${r.raw_log?.country || ''}`),
    liveCol('pop', 'Edge POP', (r) => <span className="font-mono text-xs text-muted-foreground uppercase">{r.raw_log?.pop || '—'}</span>, (r) => r.raw_log?.pop || ''),
    liveCol('tls', 'TLS', (r) => <span className="font-mono text-xs text-muted-foreground">{r.raw_log?.tls ? `TLS ${r.raw_log.tls}` : '—'}</span>, (r) => r.raw_log?.tls || ''),
    liveCol('ttfb', 'TTFB (Edge)', (r) => <span className="font-mono text-xs text-muted-foreground">{r.raw_log?.ttfb != null ? `${r.raw_log.ttfb}ms` : '—'}</span>, (r) => (r.raw_log?.ttfb != null ? parseFloat(String(r.raw_log.ttfb)) : null)),
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
                    ['Client Platform', <span key="c">{envName(selectedEvent.browser)} on {envName(selectedEvent.os)}</span>],
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
              </div>
            )
          })()}
        </DialogContent>
      </Dialog>
    </>
  )
}
