'use client'

import React from 'react'
import { useMutation, useQueryClient } from '@tanstack/react-query'
import { Activity, ChevronRight, Flag, Loader2 } from 'lucide-react'
import { ColumnDef, flexRender, getCoreRowModel, useReactTable } from '@tanstack/react-table'
import { AnalyticsCard } from '@/components/AnalyticsCard'
import {
  ColumnVisibilityDropdown,
  columnIdOf,
  DataTable,
  DateTimeCell,
} from '@/components/DataTable'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from '@/components/ui/dialog'
import { Popover, PopoverContent, PopoverTrigger } from '@/components/ui/popover'
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select'
import { SkeletonGrid } from '@/components/ui/skeleton'
import { Table, TableBody, TableCell, TableRow } from '@/components/ui/table'
import { Textarea } from '@/components/ui/textarea'
import { client, extractApiError, unwrapClient, queryKeys } from '@/lib/api'
import { formatLocalDateTime, keyByOccurrence } from '@/lib/utils'
import type { components } from '@/types/api'
import { TopFlaggedHelp } from './help-content'
import { useScoringLabels, type LabelValue } from './LabelsTab'
import { useScoringQuery } from './ScoringAdminPanels'

export interface FlagSessionPopoverProps {
  serviceId: string
  // ``null`` for a cookieless request — the popover renders disabled.
  sid: string | null
  // Optional metadata captured at flag time — denormalized into the label
  // row so the Labels tab can show "what this session looked like" without
  // re-querying the logs.
  sampleIp?: string | null
  sampleUa?: string | null
  sampleUrl?: string | null
  // Currently-applied label (from the labels API). Undefined means we
  // don't know / haven't checked.
  currentLabel?: LabelValue | null
  // Id of the currently-applied label row, required to issue the DELETE
  // that un-flags the session. Optional — when absent the "Clear Label"
  // affordance is hidden.
  currentLabelId?: string | null
  trigger?: React.ReactNode
  onFlagged?: () => void
}

const LABEL_STYLES: Record<LabelValue, string> = {
  good: 'border-emerald-500 text-emerald-700 hover:bg-emerald-50',
  bad: 'border-rose-500 text-rose-700 hover:bg-rose-50',
  neutral: 'border-slate-400 text-slate-700 hover:bg-slate-50',
}

/**
 * Reusable popover for labeling a session as good / bad / neutral. Used
 * from both the SessionScoring admin page's TopFlaggedTable and from the
 * dashboard raw-logs table's Flag column.
 *
 * Always posts to /api/services/{serviceId}/scoring/labels, which upserts
 * on (service_id, sid), so re-flagging the same session overwrites
 * cleanly. The `currentLabel` prop is purely cosmetic (visual hint of
 * existing state) — the upsert semantics on the backend mean we never
 * need to choose between POST and PATCH from the client.
 */
export function FlagSessionPopover({
  serviceId,
  sid,
  sampleIp = '',
  sampleUa = '',
  sampleUrl = '',
  currentLabel,
  currentLabelId,
  trigger,
  onFlagged,
}: FlagSessionPopoverProps) {
  const [open, setOpen] = React.useState(false)
  const [notes, setNotes] = React.useState('')
  const [error, setError] = React.useState('')
  const disabled = !sid

  const submitMutation = useMutation({
    mutationFn: async (label: LabelValue) => {
      await unwrapClient(
        client.POST('/api/services/{service_id}/scoring/labels', {
          params: { path: { service_id: serviceId } },
          body: {
            sid: sid!,
            label,
            notes,
            flagged_by: 'admin',
            sample_ip: sampleIp ?? '',
            sample_ua: sampleUa ?? '',
            sample_url: sampleUrl ?? '',
          },
        }),
      )
    },
    onMutate: () => setError(''),
    onSuccess: () => {
      setOpen(false)
      setNotes('')
      onFlagged?.()
    },
    onError: (e) => setError(extractApiError(e) || 'Failed to save label'),
  })
  const busy = submitMutation.isPending ? (submitMutation.variables ?? null) : null
  const submit = (label: LabelValue) => {
    if (sid) submitMutation.mutate(label)
  }

  const clearMutation = useMutation({
    mutationFn: async () => {
      await unwrapClient(
        client.DELETE('/api/services/{service_id}/scoring/labels/{label_id}', {
          params: { path: { service_id: serviceId, label_id: currentLabelId! } },
        }),
      )
    },
    onMutate: () => setError(''),
    onSuccess: () => {
      setOpen(false)
      onFlagged?.()
    },
    onError: (e) => setError(extractApiError(e) || 'Failed to clear label'),
  })
  const clearing = clearMutation.isPending
  const handleClearLabel = () => {
    if (currentLabelId) clearMutation.mutate()
  }

  return (
    <Popover open={open} onOpenChange={setOpen}>
      <PopoverTrigger
        render={(props: React.ComponentPropsWithRef<'button'>) =>
          trigger ? (
            <button {...props}>{trigger}</button>
          ) : (
            <Button
              {...props}
              variant="ghost"
              size="icon"
              className="h-7 w-7"
              disabled={disabled}
              aria-label={disabled ? 'No session id (cookieless request)' : 'Flag this session'}
              title={disabled ? 'No session id (cookieless request)' : 'Flag this session'}
            >
              <Flag
                className={
                  currentLabel === 'bad'
                    ? 'h-3.5 w-3.5 text-rose-600 fill-rose-600'
                    : currentLabel === 'good'
                      ? 'h-3.5 w-3.5 text-emerald-600 fill-emerald-600'
                      : currentLabel === 'neutral'
                        ? 'h-3.5 w-3.5 text-slate-500 fill-slate-500'
                        : 'h-3.5 w-3.5 text-muted-foreground'
                }
              />
            </Button>
          )
        }
      />
      <PopoverContent className="w-80" align="end">
        <div className="space-y-3">
          <div>
            <p className="text-sm font-medium">Label session</p>
            <p className="text-xs text-muted-foreground font-mono break-all mt-0.5">
              sid: {sid || '(none)'}
            </p>
            {currentLabel && (
              <p className="text-xs text-muted-foreground mt-1">
                Currently: <span className="font-medium">{currentLabel}</span> · re-flagging
                overwrites
              </p>
            )}
          </div>
          <Textarea
            value={notes}
            onChange={(e) => setNotes(e.target.value)}
            placeholder="Notes (optional) — why this session?"
            className="text-xs min-h-[60px]"
            disabled={!!busy}
          />
          {error && <p className="text-xs text-rose-600">{error}</p>}
          <div className="grid grid-cols-3 gap-2">
            {(['good', 'neutral', 'bad'] as const).map((lbl) => (
              <Button
                key={lbl}
                variant="outline"
                size="sm"
                className={`text-xs ${LABEL_STYLES[lbl]}`}
                disabled={disabled || !!busy || clearing}
                onClick={() => submit(lbl)}
              >
                {busy === lbl ? <Loader2 className="h-3 w-3 animate-spin" /> : lbl}
              </Button>
            ))}
          </div>
          {currentLabel && currentLabelId && (
            <div className="pt-1 border-t">
              <Button
                variant="ghost"
                size="sm"
                className="w-full text-xs text-rose-600 hover:text-rose-700 hover:bg-rose-50"
                disabled={!!busy || clearing}
                onClick={handleClearLabel}
              >
                {clearing ? <Loader2 className="h-3 w-3 animate-spin" /> : 'Clear Label (Un-flag)'}
              </Button>
            </div>
          )}
        </div>
      </PopoverContent>
    </Popover>
  )
}

type SessionEvent = components['schemas']['ScoringSessionEvent']
type SessionEventRow = {
  key: string
  event: SessionEvent
  index: number
  prev: SessionEvent | null
}

const EVENT_COLUMNS: ColumnDef<SessionEventRow>[] = [
  {
    id: 'index',
    cell: ({ row }) => <span className="text-muted-foreground font-mono">{row.original.index + 1}.</span>,
  },
  {
    id: 'details',
    cell: ({ row }) => {
      const { event, prev } = row.original
      const ts = event.ts ? formatLocalDateTime(event.ts) : '—'
      return (
        <div className="space-y-1">
          <div className="flex items-center gap-2">
            {prev && (
              <span className="text-muted-foreground inline-flex items-center gap-0.5">
                <code className="text-[10px] truncate max-w-[200px]">{prev.url}</code>
                <ChevronRight className="h-3 w-3" />
              </span>
            )}
            <code className="font-mono text-xs truncate">{event.url}</code>
          </div>
          <div className="flex items-center gap-3 text-[10px] text-muted-foreground">
            <span>{ts}</span>
            {event.status != null && <span>HTTP {event.status}</span>}
            {event.edge_cookie_compliance && <span>cookie:{event.edge_cookie_compliance}</span>}
            {event.edge_score_reason && (
              <span className="text-amber-600">{event.edge_score_reason}</span>
            )}
          </div>
        </div>
      )
    },
  },
  {
    id: 'score',
    cell: ({ row }) => {
      const score = row.original.event.edge_score
      if (score == null) return null
      const tone =
        score >= 75 ? 'text-destructive' : score >= 50 ? 'text-amber-600' : 'text-muted-foreground'
      return <div className={`text-right font-mono font-semibold ${tone}`}>{score}</div>
    },
  },
]

export interface SessionEventsDialogProps {
  serviceId: string
  sid: string
  label?: string
  trigger?: React.ReactElement
}

export function SessionEventsDialog({ serviceId, sid, label, trigger }: SessionEventsDialogProps) {
  const [open, setOpen] = React.useState(false)
  const { data, isLoading, isError, error } = useScoringQuery(
    ['scoring-session-events', serviceId, sid],
    () =>
      client.GET('/api/services/{service_id}/scoring/sessions/{sid}/events', {
        params: { path: { service_id: serviceId, sid }, query: { since_days: 30 } },
      }),
    { enabled: open, staleTime: 30_000 },
  )
  const eventRows = React.useMemo<SessionEventRow[]>(
    () =>
      keyByOccurrence(data?.events ?? [], (ev) => `${ev.ts ?? ''}|${ev.edge_score ?? ''}`).map(
        ({ key, item: event }, index, arr) => ({
          key,
          event,
          index,
          prev: index > 0 ? arr[index - 1].item : null,
        }),
      ),
    [data?.events],
  )
  const eventsTable = useReactTable({
    data: eventRows,
    columns: EVENT_COLUMNS,
    getCoreRowModel: getCoreRowModel(),
  })

  return (
    <Dialog open={open} onOpenChange={setOpen}>
      <DialogTrigger
        render={
          trigger ?? (
            <button
              type="button"
              className="text-xs text-primary hover:underline inline-flex items-center gap-0.5"
              title="View session events"
            >
              <Activity className="h-3 w-3" />
              events
            </button>
          )
        }
      />
      <DialogContent className="max-w-3xl max-h-[80vh] overflow-hidden flex flex-col">
        <DialogHeader>
          <DialogTitle className="flex items-center gap-2">
            <Activity className="h-4 w-4" />
            <span>Session</span>
            <code className="font-mono text-sm">{sid}</code>
            {label && (
              <Badge
                variant={
                  label === 'good' ? 'success' : label === 'bad' ? 'destructive' : 'secondary'
                }
                className="uppercase text-[10px]"
              >
                {label}
              </Badge>
            )}
          </DialogTitle>
          <DialogDescription>
            URL sequence the session walked, oldest first. The L2 scorer evaluates each
            transition (prev URL → current URL) against the trained matrix — high-score
            transitions are the ones the matrix flagged as unusual.
          </DialogDescription>
        </DialogHeader>
        <div className="flex-1 overflow-y-auto -mx-6 px-6">
          {isLoading && (
            <div className="space-y-2" role="status" aria-busy="true">
              <span className="sr-only">Loading session events</span>
              <SkeletonGrid count={6} height="3rem" className="rounded-md" />
            </div>
          )}
          {isError && (
            <div className="text-sm text-destructive p-3 border rounded-md">
              Failed to load events: {(error as Error)?.message ?? 'unknown'}
            </div>
          )}
          {data && data.event_count === 0 && (
            <div className="text-sm text-muted-foreground p-6 text-center border rounded-md">
              No events found for this sid in the last {data.since_days} days. The
              session may have rotated its cookie, or the rows haven&apos;t been
              ingested yet (cron ticks every ~30s; FOS-to-parquet lag is ~1–2 min).
            </div>
          )}
          {data && (data.event_count ?? 0) > 0 && (
            <div className="space-y-1">
              <div className="text-xs text-muted-foreground mb-2">
                {data.event_count} event{data.event_count === 1 ? '' : 's'} in last {data.since_days}
                d
              </div>
              <Table className="text-xs">
                <TableBody>
                  {eventsTable.getRowModel().rows.map((row) => (
                    <TableRow key={row.original.key} className="align-top">
                      {row.getVisibleCells().map((cell) => (
                        <TableCell key={cell.id} className="py-2">
                          {flexRender(cell.column.columnDef.cell, cell.getContext())}
                        </TableCell>
                      ))}
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </div>
          )}
        </div>
      </DialogContent>
    </Dialog>
  )
}

export interface TopFlaggedTableProps {
  serviceId: string
  sinceHours?: number
}

type FlaggedRow = components['schemas']['ScoringTopFlaggedRow']

function scoreBadge(score: number) {
  if (score >= 75) return <Badge className="bg-rose-600 hover:bg-rose-600">{score}</Badge>
  if (score >= 50) return <Badge className="bg-amber-500 hover:bg-amber-500">{score}</Badge>
  if (score >= 25) {
    return <Badge className="bg-yellow-400 text-black hover:bg-yellow-400">{score}</Badge>
  }
  return <Badge variant="secondary">{score}</Badge>
}

function complianceBadge(c?: string) {
  if (!c) return null
  if (c === 'ok') {
    return (
      <Badge variant="secondary" className="text-emerald-700">
        ok
      </Badge>
    )
  }
  return (
    <Badge variant="outline" className="text-rose-700 border-rose-300">
      {c}
    </Badge>
  )
}

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

export function TopFlaggedTable({ serviceId, sinceHours = 24 }: TopFlaggedTableProps) {
  const qc = useQueryClient()
  const [complianceFilter, setComplianceFilter] = React.useState<string>('all')
  const [minScore, setMinScore] = React.useState<string>('0')
  const [visibility, setVisibility] = React.useState<Record<string, boolean>>(DEFAULT_HIDDEN)

  const flagged = useScoringQuery(['scoring-top-flagged', serviceId, sinceHours], () =>
    client.GET('/api/services/{service_id}/scoring/top-flagged', {
      params: { path: { service_id: serviceId }, query: { since_hours: sinceHours, limit: 200 } },
    }),
  )
  const { labelBySid } = useScoringLabels(serviceId)
  const onFlagged = () => {
    qc.invalidateQueries({ queryKey: queryKeys.scoringLabels(serviceId) })
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
    const min = parseInt(minScore, 10) || 0
    return (flagged.data?.rows ?? []).filter(
      (r) =>
        r.edge_score >= min &&
        (complianceFilter === 'all' || r.edge_cookie_compliance === complianceFilter),
    )
  }, [flagged.data, complianceFilter, minScore])

  const columns = React.useMemo<ColumnDef<FlaggedRow>[]>(
    () => [
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
        cell: ({ row }) => (
          <span className="text-xs">{row.getValue<number>('edge_score_l1') ?? '—'}</span>
        ),
      },
      {
        accessorKey: 'edge_score_l2',
        header: COLUMN_LABELS.edge_score_l2,
        enableSorting: true,
        cell: ({ row }) => (
          <span className="text-xs">{row.getValue<number>('edge_score_l2') ?? '—'}</span>
        ),
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
            <span className="text-xs max-w-[320px] truncate inline-block" title={url}>
              {url}
            </span>
          )
        },
      },
      {
        accessorKey: 'ip',
        header: COLUMN_LABELS.ip,
        enableSorting: true,
        cell: ({ row }) => <span className="text-xs font-mono">{row.getValue<string>('ip') ?? '—'}</span>,
      },
      {
        accessorKey: 'ua',
        header: COLUMN_LABELS.ua,
        enableSorting: false,
        cell: ({ row }) => {
          const ua = row.getValue<string>('ua') ?? ''
          return (
            <span className="text-xs max-w-[260px] truncate inline-block" title={ua}>
              {ua}
            </span>
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
                currentLabel={r.edge_sid ? (labelBySid.get(r.edge_sid) ?? null) : null}
                onFlagged={onFlagged}
              />
            </div>
          )
        },
      },
    ],
    [serviceId, labelBySid],
  )

  const columnIds = React.useMemo(() => columns.map(columnIdOf), [columns])
  const since = sinceHours >= 24 ? `${sinceHours / 24}d` : `${sinceHours}h`

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
          <Select value={complianceFilter} onValueChange={(v) => v && setComplianceFilter(v)}>
            <SelectTrigger className="h-7 text-xs w-32">
              <SelectValue placeholder="Cookie: any" />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="all" className="text-xs">
                Cookie: any
              </SelectItem>
              {complianceValues.map((v) => (
                <SelectItem key={v} value={v} className="text-xs">
                  Cookie: {v}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
          <Select value={minScore} onValueChange={(v) => v && setMinScore(v)}>
            <SelectTrigger className="h-7 text-xs w-28">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="0" className="text-xs">
                Score: any
              </SelectItem>
              <SelectItem value="25" className="text-xs">
                Score ≥ 25
              </SelectItem>
              <SelectItem value="50" className="text-xs">
                Score ≥ 50
              </SelectItem>
              <SelectItem value="75" className="text-xs">
                Score ≥ 75
              </SelectItem>
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
        emptyMessage={
          flagged.isLoading
            ? ''
            : `No scored requests match the current filters in the last ${since}.`
        }
        showPagination
      />
    </AnalyticsCard>
  )
}
