'use client'

import React from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import {
  Archive,
  Check,
  Clock,
  Database,
  FileCode,
  Info,
  Layers,
  Loader2,
  Lock,
  Pencil,
  Trash2,
  X,
} from 'lucide-react'
import { ColumnDef, flexRender, getCoreRowModel, useReactTable } from '@tanstack/react-table'
import { AnalyticsCard, CardErrorState, type AnalyticsCardError } from '@/components/AnalyticsCard'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import { LabelWithInfo } from '@/components/ui/confirm-dialog'
import { Input } from '@/components/ui/input'
import { Skeleton } from '@/components/ui/skeleton'
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table'
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from '@/components/ui/tooltip'
import { SSEModal } from '@/components/SSEModal'
import { useMounted } from '@/hooks/useBootstrap'
import { useDateFormat } from '@/hooks/useSSE'
import { client, unwrapClient, queryKeys } from '@/lib/api'
import { cn, formatBytes, formatBytesTiered, formatCountMaybe, formatRelative } from '@/lib/utils'
import { useServiceStore } from '@/stores/filterStore'
import type { components, Expect, WireParity } from '@/types/api'

export function IcebergStatus({ accessLevel }: { accessLevel?: string }) {
  const activeServiceId = useServiceStore((s) => s.activeServiceId)
  const { full, abbr } = useDateFormat()
  const { data: info, isLoading, error } = useQuery({
    queryKey: queryKeys.admin.iceberg(activeServiceId),
    queryFn: async () => unwrapClient(client.GET('/api/admin/iceberg-info')),
    enabled: !!activeServiceId,
    staleTime: 60_000,
    refetchInterval: 5 * 60_000,
    refetchIntervalInBackground: false,
  })

  if (!activeServiceId) return null
  if (error) {
    return (
      <CardErrorState
        icon={<Info className="h-4 w-4" />}
        title="Failed to load DuckLake table info"
        message={error?.message || 'Unknown error'}
      />
    )
  }

  const stats = [
    {
      label: 'Snapshots',
      value: info?.snapshots ?? 0,
      icon: Layers,
      tooltip: 'Total number of atomic commits in the DuckLake table history.',
    },
    {
      label: 'Data Files',
      value: info?.data_files ?? 0,
      icon: FileCode,
      tooltip: 'Number of Parquet data files currently in the DuckLake table.',
    },
    {
      label: 'Total Size',
      value: formatBytes(info?.size_bytes ?? 0),
      icon: Database,
      tooltip: 'Cumulative size of all data files in the DuckLake table.',
    },
    {
      label: 'Latest Commit',
      value: info?.latest_snapshot_at ? formatRelative(info.latest_snapshot_at) : 'Never',
      icon: Clock,
      tooltip: info?.latest_snapshot_at
        ? `${full(info.latest_snapshot_at)} ${abbr()}`
        : 'No snapshots yet.',
    },
  ]

  return (
    <div className="space-y-4">
      <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
        {stats.map((stat) => (
          <Card key={stat.label} className="shadow-none border-muted/60">
            <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
              <CardTitle className="text-[10px] font-bold uppercase tracking-wider text-muted-foreground flex items-center gap-1.5">
                <LabelWithInfo
                  label={stat.label}
                  info={stat.tooltip}
                  className="gap-1.5"
                  labelClassName="text-[10px] font-bold uppercase tracking-wider text-muted-foreground"
                />
              </CardTitle>
              <stat.icon className="h-3.5 w-3.5 text-muted-foreground/60" />
            </CardHeader>
            <CardContent>
              {isLoading ? (
                <Skeleton className="h-7 w-20" />
              ) : (
                <TooltipProvider>
                  <Tooltip>
                    <TooltipTrigger
                      render={
                        <div
                          className="text-xl font-mono font-bold tracking-tight rounded-sm focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
                          tabIndex={0}
                          role="button"
                          aria-label={`${stat.label}: ${stat.value}`}
                        >
                          {stat.value}
                        </div>
                      }
                    />
                    <TooltipContent className="text-xs">{stat.tooltip}</TooltipContent>
                  </Tooltip>
                </TooltipProvider>
              )}
            </CardContent>
          </Card>
        ))}
      </div>
      {accessLevel !== 'read_only' && (
        <Card className="shadow-none border-muted/60 bg-muted/5">
          <CardContent className="p-4 flex flex-col md:flex-row md:items-center justify-between gap-4">
            <div className="flex items-center gap-3">
              <div className="p-2 bg-blue-500/10 rounded-full">
                <Archive className="h-4 w-4 text-blue-500" />
              </div>
              <div>
                <h4 className="text-sm font-semibold">Local Buffer Status</h4>
                <p className="text-xs text-muted-foreground">
                  Data ingested but not yet committed to the DuckLake table.
                </p>
              </div>
            </div>
            <div className="flex items-center gap-6">
              {(
                [
                  ['Pending Files', info?.buffer_files ?? 0, 'w-12'],
                  ['Buffer Size', formatBytes(info?.buffer_size_bytes ?? 0), 'w-16'],
                ] as const
              ).map(([lbl, val, wCls]) => (
                <div key={lbl} className="flex flex-col">
                  <span className="text-[10px] font-bold uppercase text-muted-foreground">
                    {lbl}
                  </span>
                  {isLoading ? (
                    <Skeleton className={`h-5 ${wCls} mt-1`} />
                  ) : (
                    <span className="text-sm font-mono font-bold">{val}</span>
                  )}
                </div>
              ))}
              <div className="flex items-center gap-2 ml-4">
                {info?.buffer_files && info.buffer_files > 0 ? (
                  <Badge
                    variant="outline"
                    className="h-6 px-2 text-[10px] bg-blue-500/10 text-blue-600 border-blue-500/20 animate-pulse shadow-none uppercase font-bold"
                  >
                    Pending Commit
                  </Badge>
                ) : null}
              </div>
            </div>
          </CardContent>
        </Card>
      )}
      {info?.table_location && (
        <div className="px-4 py-2 bg-muted/30 rounded-md border border-dashed flex items-center justify-between">
          <div className="flex items-center gap-2 overflow-hidden">
            <Database className="h-3 w-3 text-muted-foreground shrink-0" />
            <span className="text-[10px] text-muted-foreground font-mono truncate">
              {info.table_location}
            </span>
          </div>
          <span className="text-[10px] font-bold text-muted-foreground uppercase tracking-widest ml-4 shrink-0">
            Hadoop Catalog (Fastly Object Storage {info.region ? `- ${info.region}` : ''})
          </span>
        </div>
      )}
    </div>
  )
}

export function IcebergCalendar() {
  const activeServiceId = useServiceStore((s) => s.activeServiceId)
  const { data: calendar, isLoading } = useQuery({
    queryKey: queryKeys.admin.icebergCalendar(activeServiceId),
    queryFn: async () => unwrapClient(client.GET('/api/admin/iceberg-calendar')),
    enabled: !!activeServiceId,
    staleTime: 60_000,
    refetchInterval: 5 * 60_000,
    refetchIntervalInBackground: false,
  })

  const days = React.useMemo(() => {
    const today = new Date()
    today.setHours(0, 0, 0, 0)
    const pad = (n: number) => n.toString().padStart(2, '0')
    return Array.from({ length: 90 }, (_, i) => {
      const d = new Date(today)
      d.setDate(d.getDate() - i)
      return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}`
    }).reverse()
  }, [])

  if (!activeServiceId) return null

  return (
    <Card className="shadow-none border-muted/60">
      <CardHeader className="pb-2">
        <CardTitle className="text-sm font-semibold">DuckLake Storage Distribution</CardTitle>
        <CardDescription className="text-xs">
          Physical data file counts and sizes partitioned by day.
        </CardDescription>
      </CardHeader>
      <CardContent>
        {isLoading ? (
          <div className="flex items-center gap-2 py-6 text-sm text-muted-foreground animate-pulse">
            <Loader2 className="h-3 w-3 animate-spin" /> Loading distribution data...
          </div>
        ) : (
          <div className="flex flex-wrap gap-1">
            {days.map((date) => {
              const entry = calendar?.[date]
              const dayData = Array.isArray(entry) ? undefined : entry
              const hasData = !!dayData
              return (
                <TooltipProvider key={date}>
                  <Tooltip>
                    <TooltipTrigger
                      render={
                        <span
                          className="inline-block rounded-sm focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
                          tabIndex={0}
                          role="button"
                          aria-label={
                            hasData
                              ? `${date}: ${dayData.data_files} files, ${formatBytes(dayData.size_bytes)}`
                              : `${date}: no data`
                          }
                        />
                      }
                    >
                      <div
                        className={cn(
                          'h-4 w-4 rounded-sm transition-colors border',
                          hasData
                            ? 'bg-blue-500 border-blue-600/20 hover:bg-blue-400'
                            : 'bg-muted/30 border-border/50 hover:bg-muted/50',
                        )}
                      />
                    </TooltipTrigger>
                    <TooltipContent className="text-xs max-w-[200px] p-2 space-y-1">
                      <div className="font-bold border-b border-muted/20 pb-1 mb-1">{date}</div>
                      {hasData ? (
                        <>
                          <div className="flex justify-between gap-4">
                            <span className="text-muted-foreground">Files:</span>
                            <span className="font-mono">{dayData.data_files}</span>
                          </div>
                          <div className="flex justify-between gap-4">
                            <span className="text-muted-foreground">Size:</span>
                            <span className="font-mono">{formatBytes(dayData.size_bytes)}</span>
                          </div>
                        </>
                      ) : (
                        <div className="text-muted-foreground italic">No data files</div>
                      )}
                    </TooltipContent>
                  </Tooltip>
                </TooltipProvider>
              )
            })}
          </div>
        )}
        <div className="mt-4 flex items-center gap-4 text-[10px] text-muted-foreground uppercase font-bold tracking-wider">
          <div className="flex items-center gap-1.5">
            <div className="h-2 w-2 rounded-sm bg-blue-500" />
            <span>Committed Data</span>
          </div>
          <div className="flex items-center gap-1.5">
            <div className="h-2 w-2 rounded-sm bg-muted/30 border" />
            <span>No Data</span>
          </div>
        </div>
      </CardContent>
    </Card>
  )
}

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
            {fmtRetentionDays(data.retention.cron_runs_days)}), then VACUUMs the SQLite file so the
            on-disk size drops to reflect the deletions.
          </p>
          <p className="text-muted-foreground">
            Current file:{' '}
            <span className="tabular-nums font-medium">
              {formatBytesTiered(data.db_bytes, { maxUnit: 'GB' })}
            </span>
            . VACUUM is single-threaded and rewrites the whole file — on a multi-GB DB it can take
            several minutes. The run is also written to <code className="text-xs">cron_runs</code>{' '}
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
      description="Per-table row counts + on-disk size for this service's metadata.db. Daily auto-cleanup at 03:15 UTC."
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
