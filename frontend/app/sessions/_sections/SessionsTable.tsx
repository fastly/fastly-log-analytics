'use client'

import React from 'react'
import { ColumnDef } from '@tanstack/react-table'
import { AlertTriangle, ExternalLink, Film } from 'lucide-react'
import { DataTable } from '@/components/DataTable'
import { Button } from '@/components/ui/button'
import { FlagSessionPopover } from '@/components/SessionScoring/FlagSessionPopover'
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from '@/components/ui/tooltip'
import { useDateFormat } from '@/hooks/useDateFormat'
import type { LabelRow, LabelValue } from '@/hooks/useScoringLabels'
import { cn } from '@/lib/utils'
import type { components } from '@/types/api.generated'

type SessionsResponse = components['schemas']['SessionsResponse']
type SessionRow = components['schemas']['Session']

interface SessionsTableProps {
  data: SessionsResponse | undefined
  activeServiceId: string | null
  isLoadingInitial: boolean
  isFetching: boolean
  labels: LabelRow[]
  labelBySid: Map<string, LabelValue>
  idBySid: Map<string, string>
  onFlagged: () => void
  onRowClick: (row: SessionRow) => void
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

  const columns: ColumnDef<SessionRow>[] = React.useMemo(() => {
    const cols: ColumnDef<SessionRow>[] = [
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
        cell: ({ row }) => (row.getValue('unique_urls') as number)?.toLocaleString() ?? '—',
      },
      {
        accessorKey: 'asn',
        header: 'ASN',
        cell: ({ row }) => {
          const r = row.original as { asn?: number | null; asn_label?: string | null }
          // JSON serialises Python int dict keys as strings; look up the
          // hoisted asn_names map by the stringified asn. Falls back to the
          // row-level asn_label during the deploy window before backends
          // populate the map, and to a bare "AS<n>" otherwise.
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
    <div className={cn("border rounded-lg transition-opacity duration-100", isFetching && !isLoadingInitial && "opacity-40 pointer-events-none")}>
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
