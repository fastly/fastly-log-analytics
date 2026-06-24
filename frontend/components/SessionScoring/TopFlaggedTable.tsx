'use client'

import * as React from 'react'
import { useQueryClient } from '@tanstack/react-query'
import { ColumnDef } from '@tanstack/react-table'

import { AnalyticsCard } from '@/components/AnalyticsCard'
import { TopFlaggedHelp } from '@/components/SessionScoring/help-content'
import { DataTable, ColumnVisibilityDropdown, DateTimeCell } from '@/components/DataTable'
import { Badge } from '@/components/ui/badge'
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select'
import { useScoringLabels } from '@/hooks/useScoringLabels'

import { FlagSessionPopover, type LabelValue } from './FlagSessionPopover'
import { SessionEventsDialog } from './SessionEventsDialog'
import { useScoringQuery } from './useScoringQuery'

interface TopFlaggedTableProps {
  serviceId: string
  sinceHours?: number
  onSinceHoursChange?: (hours: number) => void
}

interface FlaggedRow {
  timestamp: string
  edge_sid: string
  edge_score: number
  edge_score_l1?: number
  edge_score_l2?: number
  edge_cookie_compliance?: string
  edge_score_reason?: string
  ip?: string
  ua?: string
  url?: string
  status?: number
  country?: string
}

function scoreBadge(score: number) {
  if (score >= 75) return <Badge className="bg-rose-600 hover:bg-rose-600">{score}</Badge>
  if (score >= 50) return <Badge className="bg-amber-500 hover:bg-amber-500">{score}</Badge>
  if (score >= 25) return <Badge className="bg-yellow-400 text-black hover:bg-yellow-400">{score}</Badge>
  return <Badge variant="secondary">{score}</Badge>
}

function complianceBadge(c?: string) {
  if (!c) return null
  if (c === 'ok') return <Badge variant="secondary" className="text-emerald-700">ok</Badge>
  return <Badge variant="outline" className="text-rose-700 border-rose-300">{c}</Badge>
}

const TIME_WINDOWS: { label: string; hours: number }[] = [
  { label: '1h', hours: 1 },
  { label: '6h', hours: 6 },
  { label: '24h', hours: 24 },
  { label: '3d', hours: 72 },
  { label: '7d', hours: 168 },
]

const COLUMN_LABELS: Record<string, string> = {
  timestamp: 'Time',
  edge_score: 'Score',
  edge_score_l1: 'L1',
  edge_score_l2: 'L2',
  edge_cookie_compliance: 'Cookie',
  url: 'URL',
  ip: 'IP',
  ua: 'UA',
  edge_sid: 'SID',
  __flag: 'Flag',
}

// Hidden by default — UA strings are long + L1/L2 are detail rarely needed.
// Matches the dashboard's "compact by default, opt-in detail" convention.
const DEFAULT_HIDDEN: Record<string, boolean> = {
  edge_score_l1: false,
  edge_score_l2: false,
  ua: false,
}

export function TopFlaggedTable({
  serviceId,
  sinceHours = 24,
  onSinceHoursChange,
}: TopFlaggedTableProps) {
  const qc = useQueryClient()
  const [complianceFilter, setComplianceFilter] = React.useState<string>('all')
  const [minScore, setMinScore] = React.useState<string>('0')
  const [visibility, setVisibility] = React.useState<Record<string, boolean>>(DEFAULT_HIDDEN)
  // Uncontrolled time-window when the parent doesn't manage it.
  const [localHours, setLocalHours] = React.useState(sinceHours)
  const effectiveHours = onSinceHoursChange ? sinceHours : localHours
  const setHours = onSinceHoursChange ?? setLocalHours

  const flagged = useScoringQuery<{ rows: FlaggedRow[]; since_hours: number }>(
    ['scoring-top-flagged', serviceId, effectiveHours],
    serviceId,
    'top-flagged',
    { since_hours: effectiveHours, limit: 200 },
  )

  const { labelBySid } = useScoringLabels(serviceId)

  const onFlagged = () => {
    qc.invalidateQueries({ queryKey: ['scoring-labels', serviceId] })
    qc.invalidateQueries({ queryKey: ['scoring-top-flagged', serviceId] })
  }

  // Distinct compliance values present in the result — populates the
  // filter dropdown without hardcoding a list that might drift from
  // what the scorer actually emits.
  const complianceValues = React.useMemo(() => {
    const s = new Set<string>()
    for (const r of flagged.data?.rows ?? []) {
      if (r.edge_cookie_compliance) s.add(r.edge_cookie_compliance)
    }
    return Array.from(s).sort()
  }, [flagged.data])

  const filteredData = React.useMemo(() => {
    const rows = flagged.data?.rows ?? []
    const min = parseInt(minScore, 10) || 0
    return rows.filter((r) => {
      if (r.edge_score < min) return false
      if (complianceFilter !== 'all' && r.edge_cookie_compliance !== complianceFilter) return false
      return true
    })
  }, [flagged.data, complianceFilter, minScore])

  const columns = React.useMemo<ColumnDef<FlaggedRow>[]>(() => [
    {
      accessorKey: 'timestamp',
      header: COLUMN_LABELS.timestamp,
      enableSorting: true,
      cell: ({ row }) => (
        <DateTimeCell iso={row.original.timestamp} className="text-xs font-mono whitespace-nowrap" />
      ),
    },
    {
      accessorKey: 'edge_score',
      header: COLUMN_LABELS.edge_score,
      enableSorting: true,
      cell: ({ row }) => scoreBadge(row.getValue<number>('edge_score')),
    },
    {
      accessorKey: 'edge_score_l1',
      header: COLUMN_LABELS.edge_score_l1,
      enableSorting: true,
      cell: ({ row }) => <span className="text-xs">{row.getValue<number>('edge_score_l1') ?? '—'}</span>,
    },
    {
      accessorKey: 'edge_score_l2',
      header: COLUMN_LABELS.edge_score_l2,
      enableSorting: true,
      cell: ({ row }) => <span className="text-xs">{row.getValue<number>('edge_score_l2') ?? '—'}</span>,
    },
    {
      accessorKey: 'edge_cookie_compliance',
      header: COLUMN_LABELS.edge_cookie_compliance,
      enableSorting: true,
      cell: ({ row }) => complianceBadge(row.getValue<string>('edge_cookie_compliance')),
    },
    {
      accessorKey: 'url',
      header: COLUMN_LABELS.url,
      enableSorting: true,
      cell: ({ row }) => {
        const url = row.getValue<string>('url') ?? ''
        return (
          <span className="text-xs max-w-[320px] truncate inline-block" title={url}>{url}</span>
        )
      },
    },
    {
      accessorKey: 'ip',
      header: COLUMN_LABELS.ip,
      enableSorting: true,
      cell: ({ row }) => (
        <span className="text-xs font-mono">{row.getValue<string>('ip') ?? '—'}</span>
      ),
    },
    {
      accessorKey: 'ua',
      header: COLUMN_LABELS.ua,
      enableSorting: false,
      cell: ({ row }) => {
        const ua = row.getValue<string>('ua') ?? ''
        return (
          <span className="text-xs max-w-[260px] truncate inline-block" title={ua}>{ua}</span>
        )
      },
    },
    {
      accessorKey: 'edge_sid',
      header: COLUMN_LABELS.edge_sid,
      enableSorting: true,
      cell: ({ row }) => {
        const sid = row.getValue<string>('edge_sid') ?? ''
        if (!sid) return <span className="text-xs font-mono">—</span>
        return (
          <SessionEventsDialog
            serviceId={serviceId}
            sid={sid}
            label={labelBySid.get(sid) ?? undefined}
            trigger={
              <button
                type="button"
                className="text-xs font-mono hover:underline text-primary"
                title="View session events"
              >
                {sid}
              </button>
            }
          />
        )
      },
    },
    {
      id: '__flag',
      header: () => <span className="text-right block">{COLUMN_LABELS.__flag}</span>,
      enableSorting: false,
      cell: ({ row }) => {
        const r = row.original
        return (
          <div className="text-right">
            <FlagSessionPopover
              serviceId={serviceId}
              sid={r.edge_sid}
              sampleIp={r.ip}
              sampleUa={r.ua}
              sampleUrl={r.url}
              currentLabel={labelBySid.get(r.edge_sid) ?? null}
              onFlagged={onFlagged}
            />
          </div>
        )
      },
    },
  ], [serviceId, labelBySid])

  const columnIds = React.useMemo(() => columns.map((c) => (c.id ?? (c as any).accessorKey) as string), [columns])

  const since = effectiveHours >= 24 ? `${effectiveHours / 24}d` : `${effectiveHours}h`

  return (
    <AnalyticsCard
      title={`Top flagged sessions — last ${since}`}
      description="Sortable + filterable. Use the Flag column to label sessions for matrix evaluation."
      isLoading={flagged.isLoading}
      isFetching={flagged.isFetching}
      error={flagged.error as (Error & { status?: number }) | null}
      contentClassName="p-0"
      helpContent={<TopFlaggedHelp />}
      helpTitle="About Top Flagged Sessions"
      headerAction={
        <div className="flex items-center gap-2">
          <Select value={String(effectiveHours)} onValueChange={(v) => v && setHours(parseInt(v, 10))}>
            <SelectTrigger className="h-7 text-xs w-20">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              {TIME_WINDOWS.map((w) => (
                <SelectItem key={w.hours} value={String(w.hours)} className="text-xs">
                  {w.label}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
          <Select value={complianceFilter} onValueChange={(v) => v && setComplianceFilter(v)}>
            <SelectTrigger className="h-7 text-xs w-32">
              <SelectValue placeholder="Cookie: any" />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="all" className="text-xs">Cookie: any</SelectItem>
              {complianceValues.map((v) => (
                <SelectItem key={v} value={v} className="text-xs">Cookie: {v}</SelectItem>
              ))}
            </SelectContent>
          </Select>
          <Select value={minScore} onValueChange={(v) => v && setMinScore(v)}>
            <SelectTrigger className="h-7 text-xs w-28">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="0" className="text-xs">Score: any</SelectItem>
              <SelectItem value="25" className="text-xs">Score ≥ 25</SelectItem>
              <SelectItem value="50" className="text-xs">Score ≥ 50</SelectItem>
              <SelectItem value="75" className="text-xs">Score ≥ 75</SelectItem>
            </SelectContent>
          </Select>
          <ColumnVisibilityDropdown
            columns={columnIds
              .filter((id) => id !== '__flag')
              .map((id) => ({ id, label: COLUMN_LABELS[id] ?? id }))}
            visibility={visibility}
            onChange={(id, visible) => setVisibility((v) => ({ ...v, [id]: visible }))}
          />
        </div>
      }
    >
      <DataTable
        columns={columns}
        data={filteredData}
        searchKey="url"
        initialSorting={[{ id: 'timestamp', desc: true }]}
        hideToolbar
        columnVisibility={visibility}
        onColumnVisibilityChange={setVisibility}
        emptyMessage={flagged.isLoading ? '' : `No scored requests match the current filters in the last ${since}.`}
        showPagination
      />
    </AnalyticsCard>
  )
}
