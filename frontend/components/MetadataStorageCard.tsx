'use client'

import * as React from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { AnalyticsCard } from '@/components/AnalyticsCard'
import { SSEModal } from '@/components/SSEModal/SSEModal'
import { Button } from '@/components/ui/button'
import { Badge } from '@/components/ui/badge'
import { Input } from '@/components/ui/input'
import { Trash2, Database, Pencil, Check, X, Lock } from 'lucide-react'
import { client } from '@/lib/api'

type TableStat = { rows: number; bytes: number | null }
type StorageResponse = {
  tables: Record<string, TableStat>
  db_bytes: number | null
  db_path: string
  retention: {
    usage_log_days: number
    ingested_files_days: number
    cron_runs_days: number
  }
  // Set when cron_sync.delete_after is False on this service: the
  // ingested_files table is the dedup gate against full_sync re-LIST →
  // re-ingest, so the cleanup helper force-disables its trimming. The UI
  // disables the input + shows a tooltip explaining the override.
  ingested_files_locked?: boolean
}
// Tables we surface in the table list. Order matters — the first three
// are the trimmable ones, shown first; the rest are reference data.
const TRIMMABLE = ['usage_log', 'ingested_files', 'cron_runs'] as const
const REFERENCE = ['alerts', 'saved_views', 'audit_log', 'in_flight_buffers', 'locally_compacted_files'] as const

// Human-readable name + one-line description per table. The raw SQLite
// table name is still shown beneath so an operator who knows the schema
// can match them up.
const TABLE_LABELS: Record<string, { label: string; sub: string }> = {
  usage_log: { label: 'FOS / CDN usage log', sub: 'every Class A/B + CDN read recorded for cost analytics' },
  ingested_files: { label: 'Ingested files index', sub: 'one row per parsed .gz so we never re-ingest' },
  cron_runs: { label: 'Cron run history', sub: 'audit trail for sync, optimize, cleanup, etc.' },
  alerts: { label: 'Saved alerts', sub: 'user-configured alert rules' },
  saved_views: { label: 'Saved dashboard views', sub: 'user-saved filter combinations' },
  audit_log: { label: 'Admin audit log', sub: 'admin actions (settings, teardown, share invites)' },
  in_flight_buffers: { label: 'In-flight ingest buffers', sub: 'pending parquet writes mid-sync' },
  locally_compacted_files: { label: 'Local-compaction registry', sub: 'tracks compacted source files to block re-download' },
}

function labelFor(rawName: string): { label: string; sub: string } {
  return TABLE_LABELS[rawName] ?? { label: rawName, sub: '' }
}

function fmtBytes(n: number | null | undefined): string {
  if (n == null) return '–'
  if (n < 1024) return `${n} B`
  if (n < 1024 * 1024) return `${(n / 1024).toFixed(1)} KB`
  if (n < 1024 * 1024 * 1024) return `${(n / 1024 / 1024).toFixed(1)} MB`
  return `${(n / 1024 / 1024 / 1024).toFixed(2)} GB`
}

function fmtRows(n: number | null | undefined): string {
  if (n == null) return '–'
  return n.toLocaleString()
}

function fmtDays(n: number): string {
  if (n <= 0) return 'disabled'
  if (n === 1) return '1 day'
  return `${n} days`
}

export function MetadataStorageCard() {
  const qc = useQueryClient()

  // Service resolution flows through the client middleware (x-service-id
  // header injected from useServiceStore) and the backend's get_source
  // fallback. We don't gate on activeServiceId here — the other admin
  // cards (SystemHealthCard etc.) follow the same pattern, and gating
  // means the card sits in loading forever when the store hasn't hydrated.
  const { data, isLoading, isFetching, error } = useQuery<StorageResponse>({
    queryKey: ['admin', 'metadata-storage'],
    queryFn: async () => {
      const { data } = await client.GET('/api/admin/metadata-storage' as any, {} as any)
      return data as StorageResponse
    },
    staleTime: 30_000,
    refetchInterval: 60_000,
    refetchIntervalInBackground: false,
    retry: 1,
  })

  // Edit mode for the retention summary. Closed by default; clicking the
  // pencil opens 3 number inputs + Save/Cancel. Save PATCHes the cfg block
  // through /api/admin/metadata-retention and invalidates the storage query
  // so the new policy reflects on the card immediately.
  const [editing, setEditing] = React.useState(false)
  const [draft, setDraft] = React.useState<{ usage_log_days: string; ingested_files_days: string; cron_runs_days: string }>({
    usage_log_days: '',
    ingested_files_days: '',
    cron_runs_days: '',
  })

  const beginEdit = () => {
    if (!data) return
    setDraft({
      usage_log_days: String(data.retention.usage_log_days),
      ingested_files_days: String(data.retention.ingested_files_days),
      cron_runs_days: String(data.retention.cron_runs_days),
    })
    setEditing(true)
  }

  const saveRetention = useMutation({
    mutationFn: async () => {
      const body = {
        usage_log_days: Math.max(0, parseInt(draft.usage_log_days, 10) || 0),
        ingested_files_days: Math.max(0, parseInt(draft.ingested_files_days, 10) || 0),
        cron_runs_days: Math.max(0, parseInt(draft.cron_runs_days, 10) || 0),
      }
      const { data } = await client.PATCH('/api/admin/metadata-retention' as any, { body } as any)
      return data
    },
    onSuccess: () => {
      setEditing(false)
      qc.invalidateQueries({ queryKey: ['admin', 'metadata-storage'] })
    },
  })

  if (error) {
    return (
      <AnalyticsCard
        title="Metadata Storage"
        icon={<Database className="h-4 w-4" />}
        description="Per-table row counts + on-disk size for this service's metadata.db."
      >
        <div className="text-sm p-3 rounded-lg border border-destructive/50 bg-destructive/5 text-destructive">
          Failed to load metadata storage stats: {(error as Error).message}
        </div>
      </AnalyticsCard>
    )
  }

  if (isLoading || !data) {
    return (
      <AnalyticsCard
        title="Metadata Storage"
        icon={<Database className="h-4 w-4" />}
        description="Per-table row counts + on-disk size for this service's metadata.db."
        isLoading
      >
        <div className="h-32 animate-pulse rounded-lg bg-muted" />
      </AnalyticsCard>
    )
  }

  const rows = [
    ...TRIMMABLE.map((name) => ({ name, trimmable: true, stat: data.tables[name] })),
    ...REFERENCE.map((name) => ({ name, trimmable: false, stat: data.tables[name] })),
  ].filter((r) => r.stat)

  const headerAction = (
    <SSEModal
      title="Run metadata cleanup now"
      description={
        <div className="space-y-2 text-sm">
          <p>
            Trims rows from <code className="text-xs">usage_log</code>,{' '}
            <code className="text-xs">ingested_files</code>, and{' '}
            <code className="text-xs">cron_runs</code> older than the active retention windows
            ({fmtDays(data.retention.usage_log_days)} / {fmtDays(data.retention.ingested_files_days)} /{' '}
            {fmtDays(data.retention.cron_runs_days)}), then VACUUMs the SQLite file so the
            on-disk size drops to reflect the deletions.
          </p>
          <p className="text-muted-foreground">
            Current file: <span className="tabular-nums font-medium">{fmtBytes(data.db_bytes)}</span>.
            VACUUM is single-threaded and rewrites the whole file — on a multi-GB DB it can take
            several minutes. The run is also written to <code className="text-xs">cron_runs</code>
            and surfaces on the Data Management history grid.
          </p>
        </div>
      }
      endpoint="/api/admin/metadata-cleanup"
      body={{}}
      onClose={() => qc.invalidateQueries({ queryKey: ['admin', 'metadata-storage'] })}
      trigger={
        <Button size="sm" variant="outline">
          <Trash2 className="h-3.5 w-3.5 mr-1.5" />
          Cleanup now
        </Button>
      }
    />
  )

  return (
    <AnalyticsCard
      title="Metadata Storage"
      icon={<Database className="h-4 w-4" />}
      description="Per-table row counts + on-disk size for this service's metadata.db. Daily auto-cleanup at 03:15 UTC."
      isFetching={isFetching}
      headerAction={headerAction}
    >
      <div className="space-y-4">
        {/* Retention summary — view or edit */}
        <div className="p-3 border rounded-lg bg-muted/30 space-y-2">
          <div className="flex items-center justify-between">
            <div className="text-[10px] uppercase tracking-wider text-muted-foreground font-medium">Retention policy</div>
            {editing ? (
              <div className="flex items-center gap-1">
                <Button
                  size="sm"
                  variant="ghost"
                  className="h-6 px-2 text-xs"
                  onClick={() => saveRetention.mutate()}
                  disabled={saveRetention.isPending}
                >
                  <Check className="h-3 w-3 mr-1" />
                  {saveRetention.isPending ? 'Saving…' : 'Save'}
                </Button>
                <Button
                  size="sm"
                  variant="ghost"
                  className="h-6 px-2 text-xs"
                  onClick={() => setEditing(false)}
                  disabled={saveRetention.isPending}
                >
                  <X className="h-3 w-3 mr-1" />
                  Cancel
                </Button>
              </div>
            ) : (
              <Button size="sm" variant="ghost" className="h-6 px-2 text-xs" onClick={beginEdit}>
                <Pencil className="h-3 w-3 mr-1" />
                Edit
              </Button>
            )}
          </div>
          <div className="grid grid-cols-1 md:grid-cols-3 gap-3">
            <RetentionField
              label="usage_log retention"
              editing={editing}
              value={data.retention.usage_log_days}
              draft={draft.usage_log_days}
              onDraftChange={(v) => setDraft((d) => ({ ...d, usage_log_days: v }))}
            />
            <RetentionField
              label="ingested_files retention"
              editing={editing}
              value={data.retention.ingested_files_days}
              draft={draft.ingested_files_days}
              onDraftChange={(v) => setDraft((d) => ({ ...d, ingested_files_days: v }))}
              locked={data.ingested_files_locked}
              lockedReason="cron_sync.delete_after=false on this service. Raw .gz files stay in FOS forever, so ingested_files is the only thing stopping the daily full_sync from re-ingesting every aged-out file. Cleanup of this table is force-disabled regardless of the configured retention."
            />
            <RetentionField
              label="cron_runs retention"
              editing={editing}
              value={data.retention.cron_runs_days}
              draft={draft.cron_runs_days}
              onDraftChange={(v) => setDraft((d) => ({ ...d, cron_runs_days: v }))}
            />
          </div>
          {saveRetention.isError && (
            <div className="text-xs text-destructive pt-1">
              Save failed: {(saveRetention.error as Error)?.message ?? 'unknown error'}
            </div>
          )}
          {editing && (
            <div className="text-[10px] text-muted-foreground pt-1">
              Set a value to 0 to disable cleanup for that table.
            </div>
          )}
        </div>

        {/* Per-table breakdown */}
        <div className="border rounded-lg overflow-hidden">
          <table className="w-full text-sm">
            <thead className="bg-muted/50">
              <tr className="text-left">
                <th className="px-3 py-2 font-medium">Table</th>
                <th className="px-3 py-2 font-medium text-right">Rows</th>
                <th className="px-3 py-2 font-medium text-right">Size</th>
                <th className="px-3 py-2 font-medium text-center">Trim policy</th>
              </tr>
            </thead>
            <tbody>
              {rows.map((r) => {
                const { label, sub } = labelFor(r.name)
                return (
                  <tr key={r.name} className="border-t">
                    <td className="px-3 py-2">
                      <div className="text-sm font-medium">{label}</div>
                      <div className="text-[10px] text-muted-foreground font-mono">{r.name}</div>
                      {sub && <div className="text-[10px] text-muted-foreground mt-0.5">{sub}</div>}
                    </td>
                    <td className="px-3 py-2 text-right tabular-nums align-top">{fmtRows(r.stat.rows)}</td>
                    <td className="px-3 py-2 text-right tabular-nums align-top">{fmtBytes(r.stat.bytes)}</td>
                    <td className="px-3 py-2 text-center align-top">
                      {r.trimmable ? (
                        <Badge variant="secondary" className="text-[10px]">
                          {fmtDays(
                            r.name === 'usage_log'
                              ? data.retention.usage_log_days
                              : r.name === 'ingested_files'
                                ? data.retention.ingested_files_days
                                : data.retention.cron_runs_days,
                          )}
                        </Badge>
                      ) : (
                        <span className="text-xs text-muted-foreground" title="Not subject to retention cleanup — these are user/admin data, not append-only telemetry.">
                          —
                        </span>
                      )}
                    </td>
                  </tr>
                )
              })}
            </tbody>
            <tfoot className="bg-muted/30 border-t">
              <tr>
                <td className="px-3 py-2 font-medium text-xs uppercase tracking-wider">Total DB file</td>
                <td className="px-3 py-2"></td>
                <td className="px-3 py-2 text-right font-semibold tabular-nums">{fmtBytes(data.db_bytes)}</td>
                <td className="px-3 py-2"></td>
              </tr>
            </tfoot>
          </table>
        </div>


      </div>
    </AnalyticsCard>
  )
}

function RetentionField({
  label,
  editing,
  value,
  draft,
  onDraftChange,
  locked,
  lockedReason,
}: {
  label: string
  editing: boolean
  value: number
  draft: string
  onDraftChange: (v: string) => void
  locked?: boolean
  lockedReason?: string
}) {
  return (
    <div>
      <div className="text-[10px] uppercase tracking-wider text-muted-foreground flex items-center gap-1">
        {label}
        {locked && (
          <span title={lockedReason} className="inline-flex items-center text-amber-500" aria-label="locked">
            <Lock className="h-3 w-3" />
          </span>
        )}
      </div>
      {editing ? (
        <div className="flex items-center gap-1.5 mt-1">
          <Input
            type="number"
            min={0}
            value={locked ? '0' : draft}
            onChange={(e) => onDraftChange(e.target.value)}
            disabled={locked}
            className="h-7 w-20 text-sm tabular-nums"
            title={locked ? lockedReason : undefined}
          />
          <span className="text-xs text-muted-foreground">days</span>
        </div>
      ) : (
        <div
          className={`text-sm font-semibold tabular-nums ${locked ? 'text-muted-foreground italic' : ''}`}
          title={locked ? lockedReason : undefined}
        >
          {locked ? 'disabled (delete_after=false)' : fmtDays(value)}
        </div>
      )}
    </div>
  )
}
