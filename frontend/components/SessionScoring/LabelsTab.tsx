'use client'

import * as React from 'react'
import { useQueryClient } from '@tanstack/react-query'
import { Pencil, Trash2, Loader2 } from 'lucide-react'

import { AnalyticsCard } from '@/components/AnalyticsCard'
import { LabelsHelp } from '@/components/SessionScoring/help-content'
import { DateTimeCell } from '@/components/DataTable/DateTimeCell'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { ConfirmDialog } from '@/components/ui/confirm-dialog'
import { Popover, PopoverContent, PopoverTrigger } from '@/components/ui/popover'
import { Textarea } from '@/components/ui/textarea'
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table'
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select'
import { useScoringLabels, type LabelRow, type LabelValue } from '@/hooks/useScoringLabels'
import { client, extractApiError } from '@/lib/api'
import { showToast, showToastWithAction } from '@/lib/toast'

import { SessionEventsDialog } from './SessionEventsDialog'

interface LabelsTabProps {
  serviceId: string
}

function labelBadge(label: LabelValue) {
  if (label === 'bad') return <Badge className="bg-rose-600 hover:bg-rose-600">bad</Badge>
  if (label === 'good') return <Badge className="bg-emerald-600 hover:bg-emerald-600">good</Badge>
  return <Badge variant="secondary">neutral</Badge>
}

export function LabelsTab({ serviceId }: LabelsTabProps) {
  const qc = useQueryClient()
  const [editingId, setEditingId] = React.useState<string | null>(null)
  const [editLabel, setEditLabel] = React.useState<LabelValue>('neutral')
  const [editNotes, setEditNotes] = React.useState('')
  const [busyId, setBusyId] = React.useState<string | null>(null)
  const [error, setError] = React.useState('')
  const [pendingDelete, setPendingDelete] = React.useState<LabelRow | null>(null)
  // IDs hidden from the table while the post-confirm undo window is open.
  // The DELETE fires after the toast duration unless Undo cancels the timer.
  const [pendingDeleteIds, setPendingDeleteIds] = React.useState<Set<string>>(() => new Set())
  const pendingTimersRef = React.useRef<Map<string, ReturnType<typeof setTimeout>>>(new Map())
  React.useEffect(() => {
    const timers = pendingTimersRef.current
    return () => {
      for (const [, t] of timers) clearTimeout(t)
      timers.clear()
    }
  }, [])

  const labels = useScoringLabels(serviceId)

  const openEdit = (row: LabelRow) => {
    setEditingId(row.id)
    setEditLabel(row.label)
    setEditNotes(row.notes ?? '')
    setError('')
  }

  const saveEdit = async (row: LabelRow) => {
    setBusyId(row.id)
    setError('')
    try {
      await client.PATCH('/api/services/{service_id}/scoring/labels/{label_id}' as any, {
        params: { path: { service_id: serviceId, label_id: row.id } },
        body: { label: editLabel, notes: editNotes },
      } as any)
      setEditingId(null)
      qc.invalidateQueries({ queryKey: ['scoring-labels', serviceId] })
    } catch (e: any) {
      setError(extractApiError(e) || 'Failed to update label')
    } finally {
      setBusyId(null)
    }
  }

  const remove = (row: LabelRow) => {
    setError('')
    setPendingDelete(null)
    const id = row.id
    setPendingDeleteIds((prev) => {
      const next = new Set(prev)
      next.add(id)
      return next
    })
    const timer = setTimeout(async () => {
      pendingTimersRef.current.delete(id)
      try {
        await client.DELETE('/api/services/{service_id}/scoring/labels/{label_id}' as any, {
          params: { path: { service_id: serviceId, label_id: id } },
        } as any)
        qc.invalidateQueries({ queryKey: ['scoring-labels', serviceId] })
      } catch (e: any) {
        setPendingDeleteIds((prev) => {
          const next = new Set(prev)
          next.delete(id)
          return next
        })
        showToast(extractApiError(e) || 'Failed to delete label', 'error')
      }
    }, 5500)
    pendingTimersRef.current.set(id, timer)
    showToastWithAction(`Deleted label for ${row.sid}`, {
      actionLabel: 'Undo',
      onAction: () => {
        const t = pendingTimersRef.current.get(id)
        if (t) {
          clearTimeout(t)
          pendingTimersRef.current.delete(id)
        }
        setPendingDeleteIds((prev) => {
          const next = new Set(prev)
          next.delete(id)
          return next
        })
      },
      durationMs: 5500,
    })
  }

  const rows = React.useMemo(
    () => labels.labels.filter((r) => !pendingDeleteIds.has(r.id)),
    [labels.labels, pendingDeleteIds],
  )
  const counts = labels.counts

  return (
    <AnalyticsCard
      title="Session labels"
      description="Human-applied labels feeding the matrix-quality evaluator (ROC-AUC). Labels are NOT used for active blocking — that's VCL's job — they only tell us whether the trained matrix can separate good from bad."
      helpContent={<LabelsHelp />}
      helpTitle="About Session Labels"
      isLoading={labels.isLoading}
      isFetching={labels.isFetching}
      error={labels.error}
      contentClassName="p-0"
      headerAction={
        <div className="flex items-center gap-2 text-xs">
          <span className="text-emerald-700">good: {counts.good}</span>
          <span className="text-slate-500">·</span>
          <span className="text-slate-700">neutral: {counts.neutral}</span>
          <span className="text-slate-500">·</span>
          <span className="text-rose-700">bad: {counts.bad}</span>
        </div>
      }
    >
      <p className="text-[11px] text-muted-foreground italic px-4 py-2 border-b">
        Labels are keyed on <span className="font-mono">sid</span>, which rotates when
        the encrypted cookie hits its idle-expire or hard-cap (security feature). If a
        previously-labeled session disappears from view, the underlying visitor likely
        got a fresh sid — label the new one, and treat each labeled row as a snapshot
        of one session, not an identity across all of a visitor&apos;s activity.
      </p>
      {error && <p className="text-xs text-rose-600 px-4 py-2">{error}</p>}
      <div className="max-h-[480px] overflow-auto">
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead>SID</TableHead>
              <TableHead>Label</TableHead>
              <TableHead>Notes</TableHead>
              <TableHead className="hidden lg:table-cell">Sample URL</TableHead>
              <TableHead className="hidden xl:table-cell">IP</TableHead>
              <TableHead>Updated</TableHead>
              <TableHead className="text-right">Actions</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {rows.length === 0 && !labels.isLoading && (
              <TableRow>
                <TableCell colSpan={7} className="text-center text-xs text-muted-foreground py-6">
                  No labels yet. Flag a session from the dashboard or the Top Flagged table to add one.
                </TableCell>
              </TableRow>
            )}
            {rows.map((r) => (
              <TableRow key={r.id}>
                <TableCell className="text-xs font-mono">
                  <SessionEventsDialog
                    serviceId={serviceId}
                    sid={r.sid}
                    label={r.label}
                    trigger={
                      <button
                        type="button"
                        className="text-xs font-mono hover:underline text-primary"
                        title="View session events"
                      >
                        {r.sid}
                      </button>
                    }
                  />
                </TableCell>
                <TableCell>{labelBadge(r.label)}</TableCell>
                <TableCell className="text-xs max-w-[260px] truncate" title={r.notes}>{r.notes || '—'}</TableCell>
                <TableCell className="hidden lg:table-cell text-xs max-w-[260px] truncate" title={r.sample_url}>{r.sample_url || '—'}</TableCell>
                <TableCell className="hidden xl:table-cell text-xs font-mono">{r.sample_ip || '—'}</TableCell>
                <TableCell className="text-xs whitespace-nowrap"><DateTimeCell iso={r.updated_at} /></TableCell>
                <TableCell className="text-right">
                  <div className="flex items-center justify-end gap-1">
                    <Popover
                      open={editingId === r.id}
                      onOpenChange={(o) => (o ? openEdit(r) : setEditingId(null))}
                    >
                      <PopoverTrigger
                        render={(props: React.ComponentPropsWithRef<'button'>) => (
                          <Button {...props} variant="ghost" size="icon" aria-label="Edit label" className="h-7 w-7" title="Edit label">
                            <Pencil className="h-3.5 w-3.5" />
                          </Button>
                        )}
                      />
                      <PopoverContent className="w-80" align="end">
                        <div className="space-y-3">
                          <p className="text-sm font-medium">Edit label</p>
                          <Select value={editLabel} onValueChange={(v) => setEditLabel(v as LabelValue)}>
                            <SelectTrigger className="text-xs">
                              <SelectValue />
                            </SelectTrigger>
                            <SelectContent>
                              <SelectItem value="good">good</SelectItem>
                              <SelectItem value="neutral">neutral</SelectItem>
                              <SelectItem value="bad">bad</SelectItem>
                            </SelectContent>
                          </Select>
                          <Textarea
                            value={editNotes}
                            onChange={(e) => setEditNotes(e.target.value)}
                            placeholder="Notes (optional)"
                            className="text-xs min-h-[60px]"
                          />
                          <div className="flex justify-end gap-2">
                            <Button variant="ghost" size="sm" onClick={() => setEditingId(null)}>
                              Cancel
                            </Button>
                            <Button size="sm" disabled={busyId === r.id} onClick={() => saveEdit(r)}>
                              {busyId === r.id ? <Loader2 className="h-3 w-3 animate-spin" /> : 'Save'}
                            </Button>
                          </div>
                        </div>
                      </PopoverContent>
                    </Popover>
                    <Button
                      variant="ghost"
                      size="icon"
                      aria-label="Delete label"
                      className="h-7 w-7 text-rose-600 hover:text-rose-700"
                      title="Delete label"
                      onClick={() => {
                        setError('')
                        setPendingDelete(r)
                      }}
                      disabled={busyId === r.id}
                    >
                      <Trash2 className="h-3.5 w-3.5" />
                    </Button>
                  </div>
                </TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </div>
      <ConfirmDialog
        open={pendingDelete !== null}
        onOpenChange={(open) => {
          if (!open) setPendingDelete(null)
        }}
        isDangerous
        title="Delete label"
        description={
          pendingDelete ? (
            <>
              Delete label for sid <span className="font-mono">{pendingDelete.sid}</span>?
            </>
          ) : null
        }
        confirmLabel="Delete"
        onConfirm={() => {
          if (pendingDelete) remove(pendingDelete)
        }}
      />
    </AnalyticsCard>
  )
}
