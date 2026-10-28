'use client'

import React from 'react'
import { useMutation } from '@tanstack/react-query'
import { Activity, ChevronRight, Flag, Loader2 } from 'lucide-react'
import { ColumnDef, flexRender, getCoreRowModel, useReactTable } from '@tanstack/react-table'
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
import { SkeletonGrid } from '@/components/ui/skeleton'
import { Table, TableBody, TableCell, TableRow } from '@/components/ui/table'
import { Textarea } from '@/components/ui/textarea'
import { client, extractApiError, unwrapClient } from '@/lib/api'
import { formatLocalDateTime, keyByOccurrence } from '@/lib/utils'
import type { components } from '@/types/api'
import type { LabelValue } from './LabelsTab'
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
