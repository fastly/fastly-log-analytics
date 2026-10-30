'use client'

import React from 'react'
import type { ColumnDef, SortingState } from '@tanstack/react-table'
import { useDateFormat } from '@/hooks/useSSE'
import { Button, buttonVariants } from '@/components/ui/button'
import { Label } from '@/components/ui/label'
import { Switch } from '@/components/ui/switch'
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select'
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
  DropdownMenuSeparator,
  DropdownMenuLabel,
  DropdownMenuGroup,
} from '@/components/ui/dropdown-menu'
import { DataTable } from '@/components/DataTable'
import { Database, History, Bookmark, Download, X, Clock } from 'lucide-react'
import { PageHeader } from '@/components/ui/confirm-dialog'
import { formatCount, cn } from '@/lib/utils'
import { Skeleton } from '@/components/ui/skeleton'
import type { components } from '@/types/api.generated'
import type { QueryMode } from './QueryEditorSection'

export type QueryResponse = components['schemas']['QueryResponse']
export type PresetQuery = components['schemas']['PresetQuery']

export interface HistoryEntry {
  sql: string
  ts: number
}

export interface QueryToolbarProps {
  presets: PresetQuery[] | undefined
  history: HistoryEntry[]
  mode: QueryMode
  onModeChange: (next: QueryMode) => void
  onSelectSql: (sql: string) => void
  onRemoveHistoryItem: (e: React.MouseEvent, index: number) => void
  explain: boolean
  onExplainChange: (next: boolean) => void
  maxRows: number
  onMaxRowsChange: (next: number) => void
  canExport: boolean
  onExportCsv: () => void
}

const ROW_LIMIT_OPTIONS = [
  [100, 'Fetch 100 rows'],
  [500, 'Fetch 500 rows'],
  [1000, 'Fetch 1,000 rows'],
  [5000, 'Fetch 5,000 rows'],
  [10000, 'Fetch 10,000 rows'],
] as const

export function QueryToolbar({
  presets,
  history,
  mode,
  onModeChange,
  onSelectSql,
  onRemoveHistoryItem,
  explain,
  onExplainChange,
  maxRows,
  onMaxRowsChange,
  canExport,
  onExportCsv,
}: QueryToolbarProps) {
  const { timeAgo } = useDateFormat()
  const selectRawSql = (sqlText: string) => {
    if (mode !== 'raw') onModeChange('raw')
    onSelectSql(sqlText)
  }

  return (
    <div className="flex items-center justify-between p-2 border-b bg-muted/30 flex-wrap gap-2">
      <div className="flex items-center gap-2">
        <DropdownMenu>
          <DropdownMenuTrigger className={buttonVariants({ variant: 'outline', size: 'sm', className: 'h-8' })}>
            <span className="flex items-center">
              <Bookmark className="w-3.5 h-3.5 mr-2 text-muted-foreground" />
              Presets
            </span>
          </DropdownMenuTrigger>
          <DropdownMenuContent align="start" className="w-[300px]">
            <DropdownMenuGroup>
              <DropdownMenuLabel>Recommended Queries</DropdownMenuLabel>
              <DropdownMenuSeparator />
              {presets?.length ? (
                presets.map((p) => (
                  <DropdownMenuItem
                    key={p.name}
                    className="flex-col items-start cursor-pointer py-2"
                    onClick={() => selectRawSql(p.sql)}
                  >
                    <div className="font-semibold text-sm">{p.name}</div>
                    <div className="text-xs text-muted-foreground mt-0.5">{p.description}</div>
                  </DropdownMenuItem>
                ))
              ) : (
                <div className="p-4 text-xs text-muted-foreground text-center italic">No presets available.</div>
              )}
            </DropdownMenuGroup>
          </DropdownMenuContent>
        </DropdownMenu>

        <DropdownMenu>
          <DropdownMenuTrigger className={buttonVariants({ variant: 'outline', size: 'sm', className: 'h-8' })}>
            <span className="flex items-center">
              <History className="w-3.5 h-3.5 mr-2 text-muted-foreground" />
              History
            </span>
          </DropdownMenuTrigger>
          <DropdownMenuContent align="start" className="w-[400px] max-h-[400px] overflow-y-auto">
            <DropdownMenuGroup>
              <DropdownMenuLabel>Recent Queries</DropdownMenuLabel>
              <DropdownMenuSeparator />
              {history.length ? (
                history.map((h, i) => (
                  <DropdownMenuItem
                    key={h.ts}
                    className="flex items-start justify-between cursor-pointer py-2 group"
                    onClick={() => selectRawSql(h.sql)}
                  >
                    <div className="overflow-hidden flex-1 mr-4">
                      <div className="text-xs font-mono truncate">{h.sql.replace(/\s+/g, ' ').trim()}</div>
                      <div className="text-[10px] text-muted-foreground mt-1">{timeAgo(new Date(h.ts))}</div>
                    </div>
                    <Button
                      variant="ghost"
                      size="icon"
                      aria-label="Remove from query history"
                      className="h-5 w-5 opacity-0 group-hover:opacity-100 shrink-0"
                      onClick={(e) => onRemoveHistoryItem(e, i)}
                    >
                      <X className="h-3 w-3" />
                    </Button>
                  </DropdownMenuItem>
                ))
              ) : (
                <div className="p-4 text-xs text-muted-foreground text-center italic">No history found.</div>
              )}
            </DropdownMenuGroup>
          </DropdownMenuContent>
        </DropdownMenu>

        {canExport && (
          <Button variant="outline" size="sm" className="h-8" onClick={onExportCsv} data-testid="query-csv-export-btn">
            <Download className="w-3.5 h-3.5 mr-2" />
            Export
          </Button>
        )}
      </div>

      <div className="flex items-center gap-4">
        <div className="flex items-center space-x-2">
          <Switch id="explain" checked={explain} onCheckedChange={onExplainChange} />
          <Label htmlFor="explain" className="text-xs cursor-pointer text-muted-foreground">
            Plan
          </Label>
        </div>

        <Select value={maxRows.toString()} onValueChange={(v) => onMaxRowsChange(Number(v))}>
          <SelectTrigger className="h-8 w-[140px] text-xs">
            <SelectValue placeholder="Row limit" />
          </SelectTrigger>
          <SelectContent>
            {ROW_LIMIT_OPTIONS.map(([val, label]) => (
              <SelectItem key={val} value={String(val)}>
                {label}
              </SelectItem>
            ))}
          </SelectContent>
        </Select>
      </div>
    </div>
  )
}

export function ResultsTable({
  data,
  isPending,
  isStructured,
  columns,
  structuredSorting,
  onStructuredSortingChange,
}: {
  data: QueryResponse
  isPending: boolean
  isStructured: boolean
  columns: ColumnDef<Record<string, unknown>>[]
  structuredSorting: SortingState
  onStructuredSortingChange: (sorting: SortingState) => void
}) {
  return (
    <div
      className={cn('space-y-4 transition-opacity duration-150', isPending && 'opacity-60')}
      data-testid="query-results-table"
    >
      <div className="flex items-center gap-4 text-xs text-muted-foreground px-1">
        <span className="flex items-center gap-1">
          <Database className="h-3 w-3" />
          {data.data?.length || 0} rows returned
          {data.truncated && (
            <span className="text-amber-500 font-semibold ml-1">
              {data.total_rows && data.total_rows > 0
                ? `(Truncated to ${data.data?.length} of ${formatCount(data.total_rows)})`
                : `(Truncated to ${data.data?.length} — more available; add LIMIT to count)`}
            </span>
          )}
        </span>
        <span className="flex items-center gap-1">
          <Clock className="h-3 w-3" />
          {data.elapsed_ms}ms execution time
        </span>
      </div>

      <div className="border rounded-lg bg-card overflow-hidden">
        <DataTable
          tableCaption="Log Explorer Data Results"
          columns={columns}
          data={data.data || []}
          isLoading={isPending}
          {...(isStructured
            ? { sorting: structuredSorting, onSortingChange: onStructuredSortingChange }
            : { initialSorting: [{ id: 'timestamp', desc: true }] })}
        />
      </div>
    </div>
  )
}

export const SKELETON_ROWS = Array.from({ length: 8 }, (_, i) => `query-skel-row-${i}`)

export function QueryInitializingSkeleton({ controls }: { controls?: React.ReactNode }) {
  return (
    <div
      className="space-y-6 min-h-[820px] [contain-intrinsic-size:auto_820px] [content-visibility:auto]"
      data-testid="query-engine-initializing"
    >
      <PageHeader
        title="Query Explorer"
        description="Browse raw request logs using the global filter bar — column headers sort server-side."
      >
        <Button disabled size="lg" data-testid="query-run-btn">
          <Database className="h-4 w-4 mr-2 animate-spin" />
          Initializing DuckDB analytical engine...
        </Button>
      </PageHeader>
      {controls ?? <Skeleton className="h-9 w-full rounded-lg" />}
      <Skeleton className="h-[235px] w-full rounded-lg" />
      <Skeleton className="h-[380px] w-full rounded-lg" />
    </div>
  )
}
