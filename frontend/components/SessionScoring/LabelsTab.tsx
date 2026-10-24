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
import type { ColumnDef } from '@tanstack/react-table'
import { DataTableReadonly } from '@/components/DataTable'
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select'
import { useScoringLabels, type LabelRow, type LabelValue } from '@/hooks/useScoringLabels'
import { useDeferredDeleteWithUndo } from '@/hooks/useDeferredDeleteWithUndo'
import { client, extractApiError } from '@/lib/api'
import { unwrapClient } from '@/lib/api-client'
import { queryKeys } from '@/lib/query-keys'

import { SessionEventsDialog } from './SessionEventsDialog'

interface LabelsTabProps {
  serviceId: string
}

function labelBadge(label: LabelValue) {
  if (label === 'bad') return <Badge className="bg-rose-600 hover:bg-rose-600">bad</Badge>
  if (label === 'good') return <Badge className="bg-emerald-600 hover:bg-emerald-600">good</Badge>
  return <Badge variant="secondary">neutral</Badge>
}

interface LabelActionsProps {
  row: LabelRow
  onSave: (row: LabelRow, label: LabelValue, notes: string) => Promise<boolean>
  onDelete: (row: LabelRow) => void
}

// Edit state lives here (not in LabelsTab) so typing in the popover does not
// invalidate the table's column defs, which would remount the cell mid-edit.
function LabelActions({ row: r, onSave, onDelete }: LabelActionsProps) {
  const [open, setOpen] = React.useState(false)
  const [busy, setBusy] = React.useState(false)
  const [editLabel, setEditLabel] = React.useState<LabelValue>(r.label)
  const [editNotes, setEditNotes] = React.useState(r.notes ?? '')

  const save = async () => {
    setBusy(true)
    const ok = await onSave(r, editLabel, editNotes)
    setBusy(false)
    if (ok) setOpen(false)
  }

  return (
    <div className="flex items-center justify-end gap-1">
      <Popover
        open={open}
        onOpenChange={(o) => {
          if (o) {
            setEditLabel(r.label)
            setEditNotes(r.notes ?? '')
          }
          setOpen(o)
        }}
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
              <Button variant="ghost" size="sm" onClick={() => setOpen(false)}>
                Cancel
              </Button>
              <Button size="sm" disabled={busy} onClick={save}>
                {busy ? <Loader2 className="h-3 w-3 animate-spin" /> : 'Save'}
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
        onClick={() => onDelete(r)}
        disabled={busy}
      >
        <Trash2 className="h-3.5 w-3.5" />
      </Button>
    </div>
  )
}

export function LabelsTab({ serviceId }: LabelsTabProps) {
  const qc = useQueryClient()
  const [error, setError] = React.useState('')
  const [pendingDelete, setPendingDelete] = React.useState<LabelRow | null>(null)
  const { pendingDeleteIds, scheduleDelete } = useDeferredDeleteWithUndo()

  const labels = useScoringLabels(serviceId)

  // Stable identities: the column defs below depend on them, and flexRender
  // treats cell closures as components (a new identity remounts the cell).
  const saveEdit = React.useCallback(async (row: LabelRow, label: LabelValue, notes: string): Promise<boolean> => {
    setError('')
    try {
      await unwrapClient(
        client.PATCH('/api/services/{service_id}/scoring/labels/{label_id}', {
          params: { path: { service_id: serviceId, label_id: row.id } },
          body: { label, notes },
        }),
      )
      qc.invalidateQueries({ queryKey: queryKeys.scoringLabels(serviceId) })
      return true
    } catch (e) {
      setError(extractApiError(e) || 'Failed to update label')
      return false
    }
  }, [qc, serviceId])

  const requestDelete = React.useCallback((row: LabelRow) => {
    setError('')
    setPendingDelete(row)
  }, [])

  const remove = (row: LabelRow) => {
    setError('')
    setPendingDelete(null)
    const id = row.id
    scheduleDelete({
      id,
      toastMessage: `Deleted label for ${row.sid}`,
      performDelete: async () => {
        await unwrapClient(
          client.DELETE('/api/services/{service_id}/scoring/labels/{label_id}', {
            params: { path: { service_id: serviceId, label_id: id } },
          }),
        )
        qc.invalidateQueries({ queryKey: queryKeys.scoringLabels(serviceId) })
      },
      errorMessage: (e) => extractApiError(e) || 'Failed to delete label',
    })
  }

  const rows = React.useMemo(
    () => labels.labels.filter((r) => !pendingDeleteIds.has(r.id)),
    [labels.labels, pendingDeleteIds],
  )
  const counts = labels.counts

  const columns = React.useMemo<ColumnDef<LabelRow>[]>(
    () => [
      {
        id: 'sid',
        header: 'SID',
        accessorFn: (r) => r.sid,
        size: 160,
        enableSorting: false,
        cell: ({ row }) => (
          <SessionEventsDialog
            serviceId={serviceId}
            sid={row.original.sid}
            label={row.original.label}
            trigger={
              <button
                type="button"
                className="text-xs font-mono hover:underline text-primary"
                title="View session events"
              >
                {row.original.sid}
              </button>
            }
          />
        ),
      },
      {
        id: 'label',
        header: 'Label',
        accessorFn: (r) => r.label,
        size: 90,
        enableSorting: false,
        cell: ({ row }) => labelBadge(row.original.label),
      },
      {
        id: 'notes',
        header: 'Notes',
        accessorFn: (r) => r.notes ?? '',
        size: 260,
        enableSorting: false,
        cell: ({ row }) => (
          <span className="text-xs" title={row.original.notes ?? undefined}>{row.original.notes || '—'}</span>
        ),
      },
      {
        id: 'sample_url',
        header: 'Sample URL',
        accessorFn: (r) => r.sample_url ?? '',
        size: 260,
        enableSorting: false,
        cell: ({ row }) => (
          <span className="text-xs" title={row.original.sample_url ?? undefined}>{row.original.sample_url || '—'}</span>
        ),
      },
      {
        id: 'sample_ip',
        header: 'IP',
        accessorFn: (r) => r.sample_ip ?? '',
        size: 140,
        enableSorting: false,
        cell: ({ row }) => <span className="text-xs font-mono">{row.original.sample_ip || '—'}</span>,
      },
      {
        id: 'updated_at',
        header: 'Updated',
        accessorFn: (r) => r.updated_at ?? '',
        size: 170,
        enableSorting: false,
        cell: ({ row }) => (
          <span className="text-xs whitespace-nowrap"><DateTimeCell iso={row.original.updated_at} /></span>
        ),
      },
      {
        id: 'actions',
        header: 'Actions',
        size: 100,
        enableSorting: false,
        cell: ({ row }) => (
          <LabelActions
            row={row.original}
            onSave={saveEdit}
            onDelete={requestDelete}
          />
        ),
      },
    ],
    [serviceId, saveEdit, requestDelete],
  )


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
      <DataTableReadonly
        columns={columns}
        data={rows}
        hideToolbar
        showPagination={rows.length > 50}
        tableCaption="Session labels"
        isLoading={labels.isLoading}
        emptyMessage="No labels yet. Flag a session from the dashboard or the Top Flagged table to add one."
      />
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
