'use client'

import React from 'react'
import { useQueryClient } from '@tanstack/react-query'
import { RefreshCw } from 'lucide-react'
import type { ColumnDef } from '@tanstack/react-table'
import { DataTableReadonly } from '@/components/DataTable'
import { ColumnVisibilityDropdown } from '@/components/DataTable/ColumnVisibilityDropdown'
import { Button } from "@/components/ui/button"
import { Input } from '@/components/ui/input'
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select"
import { formatCountMaybe } from '@/lib/format'
import { queryKeys } from '@/lib/query-keys'
import type { components } from '@/types/api.generated'

type SchemaColumn = components['schemas']['SchemaResponse']['schema'][number]
type IngestedFile = components['schemas']['IngestedFile']

const hasStats = (col: SchemaColumn) => col.null_percentage !== undefined
const populatedPct = (col: SchemaColumn) =>
  col.null_percentage != null ? Math.max(0, 100 - col.null_percentage) : null

const SCHEMA_COLUMNS: ColumnDef<SchemaColumn>[] = [
  {
    id: 'name',
    header: 'Column Name',
    accessorFn: (c) => c.name,
    size: 220,
    cell: ({ row }) => <span className="font-mono text-xs font-bold">{row.original.name}</span>,
  },
  {
    id: 'type',
    header: 'DuckDB Type',
    accessorFn: (c) => c.type,
    size: 160,
    cell: ({ row }) => <span className="font-mono text-xs text-muted-foreground">{row.original.type}</span>,
  },
  {
    id: 'populated',
    header: 'Populated %',
    accessorFn: (c) => populatedPct(c) ?? -1,
    size: 130,
    cell: ({ row }) => {
      const pct = populatedPct(row.original)
      const text = pct != null ? pct.toFixed(1) : '—'
      return (
        <span className="block font-mono text-xs text-right tabular-nums">
          {hasStats(row.original) ? (
            <span className={text === '0.0' ? 'text-muted-foreground/30' : ''}>{text}%</span>
          ) : '—'}
        </span>
      )
    },
  },
  {
    id: 'approx_unique',
    header: 'Approx Unique',
    accessorFn: (c) => c.approx_unique ?? -1,
    size: 140,
    cell: ({ row }) => (
      <span className="block font-mono text-xs text-right text-muted-foreground tabular-nums">
        {hasStats(row.original) ? formatCountMaybe(row.original.approx_unique) ?? '—' : '—'}
      </span>
    ),
  },
  {
    id: 'min',
    header: 'Min Value',
    accessorFn: (c) => c.min ?? '',
    size: 200,
    cell: ({ row }) => (
      <span className="font-mono text-xs text-muted-foreground" title={row.original.min ?? undefined}>
        {hasStats(row.original) ? row.original.min ?? '—' : '—'}
      </span>
    ),
  },
  {
    id: 'max',
    header: 'Max Value',
    accessorFn: (c) => c.max ?? '',
    size: 200,
    cell: ({ row }) => (
      <span className="font-mono text-xs text-muted-foreground" title={row.original.max ?? undefined}>
        {hasStats(row.original) ? row.original.max ?? '—' : '—'}
      </span>
    ),
  },
]

export function SchemaTab({
  schemaData,
  isLoadingSchema,
}: {
  schemaData: components['schemas']['SchemaResponse'] | undefined
  isLoadingSchema: boolean
}) {
  return (
    <>
      <div className="p-4 border-b flex justify-between items-center">
        <div>
          <h3 className="text-sm font-medium">DuckDB Table Schema & Statistics</h3>
          <p className="text-xs text-muted-foreground mt-1">Based on a fast statistical sample of your logs.</p>
        </div>
      </div>
      <DataTableReadonly
        columns={SCHEMA_COLUMNS}
        data={schemaData?.schema ?? []}
        isLoading={isLoadingSchema}
        hideToolbar
        showPagination={false}
        tableCaption="DuckDB Table Schema and Statistics"
      />
    </>
  )
}

export function IngestionTab({
  ingestedColumns,
  ingestedFiles,
  isLoadingIngested,
}: {
  ingestedColumns: ColumnDef<IngestedFile>[]
  ingestedFiles: components['schemas']['IngestedFilesResponse'] | undefined
  isLoadingIngested: boolean
}) {
  return (
    <DataTableReadonly
      columns={ingestedColumns}
      data={ingestedFiles?.files || []}
      isLoading={isLoadingIngested}
      searchKey="file_name"
      initialSorting={[{ id: 'ingested_at', desc: true }]}
      renderToolbar={(table) => (
        <div className="p-4 border-b flex flex-wrap items-center justify-between gap-4">
          <h3 className="text-sm font-medium">Log Ingestion History</h3>
          <div className="flex items-center gap-2 ml-auto">
            <Input
              placeholder="Filter by filename..."
              value={(table.getColumn('file_name')?.getFilterValue() as string) ?? ''}
              onChange={(event) => table.getColumn('file_name')?.setFilterValue(event.target.value)}
              className="max-w-sm h-8"
            />
            <ColumnVisibilityDropdown table={table} compact />
          </div>
        </div>
      )}
    />
  )
}

export function ServiceHistoryTab({
  auditColumns,
  auditLogs,
  isLoadingAudit,
  isFetchingAudit,
  eventFilter,
  setEventFilter,
  activeServiceId,
}: {
  auditColumns: ColumnDef<components['schemas']['AuditLogEntry']>[]
  auditLogs: components['schemas']['AuditLogsResponse'] | undefined
  isLoadingAudit: boolean
  isFetchingAudit: boolean
  eventFilter: string
  setEventFilter: (v: string) => void
  activeServiceId: string | null | undefined
}) {
  const queryClient = useQueryClient()

  return (
    <div className="p-0">
      <DataTableReadonly
        columns={auditColumns}
        data={auditLogs?.entries || []}
        isLoading={isLoadingAudit}
        initialSorting={[{ id: 'timestamp', desc: true }]}
        renderToolbar={(table) => (
          <div className="flex flex-col sm:flex-row sm:items-center justify-between p-4 border-b gap-4">
            <div className="flex items-center gap-4">
              <h3 className="text-sm font-medium whitespace-nowrap">Service History</h3>
              <div className="flex items-center gap-2">
                <Select value={eventFilter} onValueChange={(v) => setEventFilter(v || 'all')}>
                  <SelectTrigger className="h-8 w-[200px] text-xs">
                    <SelectValue placeholder="All events" />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="all">All events</SelectItem>
                    <SelectItem value="provision">Provision</SelectItem>
                    <SelectItem value="fastly_activation">Fastly Activation</SelectItem>
                    <SelectItem value="cron_settings_update">Cron Settings Update</SelectItem>
                    <SelectItem value="logging_settings_update">Log Settings Updated</SelectItem>
                    <SelectItem value="log_format_update">Log Format Update</SelectItem>
                  </SelectContent>
                </Select>
              </div>
            </div>
            <div className="flex items-center gap-2">
              <ColumnVisibilityDropdown table={table} />
              <Button
                variant="outline"
                size="sm"
                onClick={() => queryClient.invalidateQueries({ queryKey: queryKeys.admin.auditLogsFor(activeServiceId) })}
                disabled={isFetchingAudit}
                className="h-8 text-xs"
              >
                <RefreshCw className={`h-3 w-3 mr-1.5 ${isFetchingAudit ? 'animate-spin' : ''}`} />
                Refresh
              </Button>
            </div>
          </div>
        )}
      />
    </div>
  )
}
