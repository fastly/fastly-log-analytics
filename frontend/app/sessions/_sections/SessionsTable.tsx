'use client'

import React from 'react'
import { ColumnDef } from '@tanstack/react-table'
import { AlertTriangle, Clock, ExternalLink, Film } from 'lucide-react'

import { type LabelRow, type LabelValue } from '@/components/SessionScoring/ScoringAdminPanels'
import { FlagSessionPopover } from '@/components/SessionScoring/TopFlaggedTable'
import { useDateFormat } from '@/hooks/useSSE'
import { DataTable } from '@/components/DataTable'
import { Button } from '@/components/ui/button'
import { Switch } from '@/components/ui/switch'
import { Label } from '@/components/ui/label'
import { Input } from '@/components/ui/input'
import { RelativeTimeTooltip } from '@/components/SessionDetailModal'
import { cn, formatCountMaybe } from '@/lib/utils'
import type { components } from '@/types/api.generated'
import type { Session, SessionsResponse } from '@/types/api'

export interface SessionsTableProps {
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
  }, [data, relative, full, abbr, labelBySid, idBySid, activeServiceId, onFlagged, onRowClick])

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

export interface ScoringControlsProps {
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
