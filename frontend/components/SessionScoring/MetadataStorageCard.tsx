'use client'

import React from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { Check, Database, Lock, Pencil, Trash2, X } from 'lucide-react'
import { ColumnDef, flexRender, getCoreRowModel, useReactTable } from '@tanstack/react-table'
import { AnalyticsCard, type AnalyticsCardError } from '@/components/AnalyticsCard'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table'
import { SSEModal } from '@/components/SSEModal'
import { useMounted } from '@/hooks/useBootstrap'
import { client, unwrapClient, queryKeys } from '@/lib/api'
import { formatBytesTiered, formatCountMaybe } from '@/lib/utils'
import type { components, Expect, WireParity } from '@/types/api'

type TableStat = { rows: number; bytes: number | null }
type StorageResponse = {
  tables: Record<string, TableStat>
  db_bytes: number | null
  db_path: string
  retention: { usage_log_days: number; ingested_files_days: number; cron_runs_days: number }
  ingested_files_locked?: boolean
}
type _TableStatParity = Expect<WireParity<TableStat, components['schemas']['MetadataTableStat']>>
type _RetentionParity = Expect<
  WireParity<StorageResponse['retention'], components['schemas']['MetadataRetentionValues']>
>
type _StorageParity = Expect<
  WireParity<StorageResponse, components['schemas']['MetadataStorageResponse']>
>

const TRIMMABLE = ['usage_log', 'ingested_files', 'cron_runs'] as const
const REFERENCE = [
  'alerts',
  'saved_views',
  'audit_log',
  'in_flight_buffers',
  'locally_compacted_files',
] as const

const TABLE_LABELS: Record<string, { label: string; sub: string }> = {
  usage_log: {
    label: 'FOS / CDN usage log',
    sub: 'every Class A/B + CDN read recorded for cost analytics',
  },
  ingested_files: {
    label: 'Ingested files index',
    sub: 'one row per parsed .gz so we never re-ingest',
  },
  cron_runs: {
    label: 'Cron run history',
    sub: 'audit trail for sync, optimize, cleanup, etc.',
  },
  alerts: { label: 'Saved alerts', sub: 'user-configured alert rules' },
  saved_views: { label: 'Saved dashboard views', sub: 'user-saved filter combinations' },
  audit_log: { label: 'Admin audit log', sub: 'admin actions (settings, teardown, share invites)' },
  in_flight_buffers: { label: 'In-flight ingest buffers', sub: 'pending parquet writes mid-sync' },
  locally_compacted_files: {
    label: 'Local-compaction registry',
    sub: 'tracks compacted source files to block re-download',
  },
}

const fmtRetentionDays = (n: number): string =>
  n <= 0 ? 'disabled' : n === 1 ? '1 day' : `${n} days`

const LOCKED_REASON =
  'cron_sync.delete_after=false on this service. Raw .gz files stay in FOS forever, so ingested_files is the only thing stopping the daily full_sync from re-ingesting every aged-out file. Cleanup of this table is force-disabled regardless of the configured retention.'

type MetadataStorageRow = { name: string; trimmable: boolean; stat: TableStat }

function MetadataStorageTable({
  rows,
  retention,
  dbBytes,
}: {
  rows: MetadataStorageRow[]
  retention: StorageResponse['retention']
  dbBytes: number | null
}) {
  const columns = React.useMemo<ColumnDef<MetadataStorageRow>[]>(
    () => [
      {
        id: 'table',
        header: 'Table',
        cell: ({ row }) => {
          const r = row.original
          const { label, sub } = TABLE_LABELS[r.name] ?? { label: r.name, sub: '' }
          return (
            <>
              <div className="text-sm font-medium">{label}</div>
              <div className="text-[11px] sm:text-[10px] text-muted-foreground font-mono">
                {r.name}
              </div>
              {sub && (
                <div className="text-[11px] sm:text-[10px] text-muted-foreground mt-0.5">{sub}</div>
              )}
            </>
          )
        },
      },
      { id: 'rows', header: 'Rows', cell: ({ row }) => formatCountMaybe(row.original.stat.rows) ?? '–' },
      {
        id: 'size',
        header: 'Size',
        cell: ({ row }) => formatBytesTiered(row.original.stat.bytes, { maxUnit: 'GB' }),
      },
      {
        id: 'policy',
        header: 'Trim policy',
        cell: ({ row }) => {
          const r = row.original
          return r.trimmable ? (
            <Badge variant="secondary" className="text-[11px] sm:text-[10px]">
              {fmtRetentionDays(
                r.name === 'usage_log'
                  ? retention.usage_log_days
                  : r.name === 'ingested_files'
                    ? retention.ingested_files_days
                    : retention.cron_runs_days,
              )}
            </Badge>
          ) : (
            <span
              className="text-xs text-muted-foreground"
              title="Not subject to retention cleanup — these are user/admin data, not append-only telemetry."
            >
              —
            </span>
          )
        },
      },
    ],
    [retention],
  )
  const table = useReactTable({ data: rows, columns, getCoreRowModel: getCoreRowModel() })
  return (
    <Table className="w-full text-sm">
      <TableHeader className="bg-muted/50">
        {table.getHeaderGroups().map((hg) => (
          <TableRow key={hg.id} className="text-left">
            {hg.headers.map((header) => (
              <TableHead
                key={header.id}
                className={
                  header.column.id === 'rows' || header.column.id === 'size'
                    ? 'px-3 py-2 font-medium text-right'
                    : header.column.id === 'policy'
                      ? 'px-3 py-2 font-medium text-center'
                      : 'px-3 py-2 font-medium'
                }
              >
                {flexRender(header.column.columnDef.header, header.getContext())}
              </TableHead>
            ))}
          </TableRow>
        ))}
      </TableHeader>
      <TableBody>
        {table.getRowModel().rows.map((row) => (
          <TableRow key={row.original.name} className="border-t">
            {row.getVisibleCells().map((cell) => (
              <TableCell
                key={cell.id}
                className={
                  cell.column.id === 'rows' || cell.column.id === 'size'
                    ? 'px-3 py-2 text-right tabular-nums align-top'
                    : cell.column.id === 'policy'
                      ? 'px-3 py-2 text-center align-top'
                      : 'px-3 py-2'
                }
              >
                {flexRender(cell.column.columnDef.cell, cell.getContext())}
              </TableCell>
            ))}
          </TableRow>
        ))}
      </TableBody>
      <tfoot className="bg-muted/30 border-t">
        <TableRow>
          <TableCell className="px-3 py-2 font-medium text-xs uppercase tracking-wider">
            Total DB file
          </TableCell>
          <TableCell className="px-3 py-2" />
          <TableCell className="px-3 py-2 text-right font-semibold tabular-nums">
            {formatBytesTiered(dbBytes, { maxUnit: 'GB' })}
          </TableCell>
          <TableCell className="px-3 py-2" />
        </TableRow>
      </tfoot>
    </Table>
  )
}

export function MetadataStorageCard() {
  const qc = useQueryClient()
  const mounted = useMounted()
  const { data, isLoading, isFetching, error } = useQuery<StorageResponse>({
    queryKey: queryKeys.admin.metadataStorage(),
    queryFn: async () => (await unwrapClient(client.GET('/api/admin/metadata-storage'))) as StorageResponse,
    staleTime: 30_000,
    refetchInterval: 5 * 60_000,
    refetchIntervalInBackground: false,
    retry: 1,
  })
  const [editing, setEditing] = React.useState(false)
  const [draft, setDraft] = React.useState({
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
    mutationFn: async () =>
      unwrapClient(
        client.PATCH('/api/admin/metadata-retention', {
          body: {
            usage_log_days: Math.max(0, parseInt(draft.usage_log_days, 10) || 0),
            ingested_files_days: Math.max(0, parseInt(draft.ingested_files_days, 10) || 0),
            cron_runs_days: Math.max(0, parseInt(draft.cron_runs_days, 10) || 0),
          },
        }),
      ),
    onSuccess: () => {
      setEditing(false)
      qc.invalidateQueries({ queryKey: queryKeys.admin.metadataStorage() })
    },
  })

  const rows = data
    ? [
        ...TRIMMABLE.map((name) => ({ name, trimmable: true, stat: data.tables[name] })),
        ...REFERENCE.map((name) => ({ name, trimmable: false, stat: data.tables[name] })),
      ].filter((r) => r.stat)
    : []

  const headerAction = data ? (
    <SSEModal
      title="Run metadata cleanup now"
      endpoint="/api/admin/metadata-cleanup"
      body={{}}
      onClose={() => qc.invalidateQueries({ queryKey: queryKeys.admin.metadataStorage() })}
      description={
        <div className="space-y-2 text-sm">
          <p>
            Trims rows from <code className="text-xs">usage_log</code>,{' '}
            <code className="text-xs">ingested_files</code>, and{' '}
            <code className="text-xs">cron_runs</code> older than the active retention windows (
            {fmtRetentionDays(data.retention.usage_log_days)} /{' '}
            {fmtRetentionDays(data.retention.ingested_files_days)} /{' '}
            {fmtRetentionDays(data.retention.cron_runs_days)}).
          </p>
          <p className="text-muted-foreground">
            Current size:{' '}
            <span className="tabular-nums font-medium">
              {formatBytesTiered(data.db_bytes, { maxUnit: 'GB' })}
            </span>
            . The run is also written to <code className="text-xs">cron_runs</code>{' '}
            and surfaces on the Data Management history grid.
          </p>
        </div>
      }
      trigger={
        <Button size="sm" variant="outline">
          <Trash2 className="h-3.5 w-3.5 mr-1.5" /> Cleanup now
        </Button>
      }
    />
  ) : undefined

  const retentionFields = data
    ? ([
        { key: 'usage_log_days', label: 'usage_log retention', value: data.retention.usage_log_days },
        {
          key: 'ingested_files_days',
          label: 'ingested_files retention',
          value: data.retention.ingested_files_days,
          locked: data.ingested_files_locked,
          lockedReason: LOCKED_REASON,
        },
        { key: 'cron_runs_days', label: 'cron_runs retention', value: data.retention.cron_runs_days },
      ] as const)
    : []

  return (
    <AnalyticsCard
      title="Metadata Storage"
      icon={<Database className="h-4 w-4" />}
      description="Per-table row counts + on-disk size for this service's metadata tables. Daily auto-cleanup at 03:15 UTC."
      isLoading={!mounted || isLoading}
      isFetching={mounted && isFetching}
      error={mounted ? (error as AnalyticsCardError | null) : null}
      headerAction={headerAction}
    >
      {mounted && data ? (
        <div className="space-y-4">
          <div className="p-3 border rounded-lg bg-muted/30 space-y-2">
            <div className="flex items-center justify-between">
              <div className="text-[11px] sm:text-[10px] uppercase tracking-wider text-muted-foreground font-medium">
                Retention policy
              </div>
              {editing ? (
                <div className="flex items-center gap-1">
                  <Button
                    size="sm"
                    variant="ghost"
                    className="h-6 px-2 text-xs"
                    onClick={() => saveRetention.mutate()}
                    disabled={saveRetention.isPending}
                  >
                    <Check className="h-3 w-3 mr-1" /> {saveRetention.isPending ? 'Saving…' : 'Save'}
                  </Button>
                  <Button
                    size="sm"
                    variant="ghost"
                    className="h-6 px-2 text-xs"
                    onClick={() => setEditing(false)}
                    disabled={saveRetention.isPending}
                  >
                    <X className="h-3 w-3 mr-1" /> Cancel
                  </Button>
                </div>
              ) : (
                <Button
                  size="sm"
                  variant="ghost"
                  className="h-6 px-2 text-xs"
                  onClick={beginEdit}
                >
                  <Pencil className="h-3 w-3 mr-1" /> Edit
                </Button>
              )}
            </div>
            <div className="grid grid-cols-1 md:grid-cols-3 gap-3">
              {retentionFields.map((f) => (
                <RetentionField
                  key={f.key}
                  label={f.label}
                  editing={editing}
                  value={f.value}
                  draft={draft[f.key]}
                  onDraftChange={(v) => setDraft((d) => ({ ...d, [f.key]: v }))}
                  locked={'locked' in f ? f.locked : undefined}
                  lockedReason={'lockedReason' in f ? f.lockedReason : undefined}
                />
              ))}
            </div>
            {saveRetention.isError && (
              <div className="text-xs text-destructive pt-1">
                Save failed: {(saveRetention.error as Error)?.message ?? 'unknown error'}
              </div>
            )}
            {editing && (
              <div className="text-[11px] sm:text-[10px] text-muted-foreground pt-1">
                Set a value to 0 to disable cleanup for that table.
              </div>
            )}
          </div>
          <div className="border rounded-lg overflow-x-auto">
            <MetadataStorageTable rows={rows} retention={data.retention} dbBytes={data.db_bytes} />
          </div>
        </div>
      ) : null}
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
      <div className="text-[11px] sm:text-[10px] uppercase tracking-wider text-muted-foreground flex items-center gap-1">
        {label}
        {locked && (
          <span
            title={lockedReason}
            className="inline-flex items-center text-amber-500"
            aria-label="locked"
          >
            <Lock className="h-3 w-3" aria-hidden="true" />
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
            aria-label={`${label} retention in days`}
          />
          <span className="text-xs text-muted-foreground">days</span>
        </div>
      ) : (
        <div
          className={`text-sm font-semibold tabular-nums ${locked ? 'text-muted-foreground italic' : ''}`}
          title={locked ? lockedReason : undefined}
        >
          {locked ? 'disabled (delete_after=false)' : fmtRetentionDays(value)}
        </div>
      )}
    </div>
  )
}
