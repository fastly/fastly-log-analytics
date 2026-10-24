'use client'

import React from 'react'
import type { ColumnDef, SortingState } from '@tanstack/react-table'
import { Button, buttonVariants } from '@/components/ui/button'
import { Label } from '@/components/ui/label'
import { Switch } from '@/components/ui/switch'
import { Tabs, TabsList, TabsTrigger } from '@/components/ui/tabs'
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select'
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
import { History, Bookmark, Download, X, Filter, Code2, Clock, Database } from 'lucide-react'
import { useDateFormat } from '@/hooks/useDateFormat'
import { useMounted } from '@/hooks/useMounted'
import { formatCount } from '@/lib/format'
import { cn } from '@/lib/utils'
import type { QueryMode } from '../_sql_builder'
import type { components } from '@/types/api.generated'

type PresetQuery = components['schemas']['PresetQuery']

interface HistoryEntry {
  sql: string
  ts: number
}

interface QueryToolbarProps {
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

/**
 * Toolbar above the editor/preview: Presets + History dropdowns on the left,
 * Plan toggle + row-limit select + Export button on the right. All state is
 * owned by the page shell; this component is presentational.
 */
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
              {presets?.length ? presets.map((p) => (
                <DropdownMenuItem
                  key={p.name}
                  className="flex-col items-start cursor-pointer py-2"
                  onClick={() => {
                    if (mode !== 'raw') onModeChange('raw')
                    onSelectSql(p.sql)
                  }}
                >
                  <div className="font-semibold text-sm">{p.name}</div>
                  <div className="text-xs text-muted-foreground mt-0.5">{p.description}</div>
                </DropdownMenuItem>
              )) : (
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
              {history.length ? history.map((h, i) => (
                <DropdownMenuItem
                  key={h.ts}
                  className="flex items-start justify-between cursor-pointer py-2 group"
                  onClick={() => {
                    if (mode !== 'raw') onModeChange('raw')
                    onSelectSql(h.sql)
                  }}
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
              )) : (
                <div className="p-4 text-xs text-muted-foreground text-center italic">No history found.</div>
              )}
            </DropdownMenuGroup>
          </DropdownMenuContent>
        </DropdownMenu>
      </div>

      <div className="flex items-center gap-4">
        <div className="flex items-center space-x-2">
          <Switch id="explain" checked={explain} onCheckedChange={onExplainChange} />
          <Label htmlFor="explain" className="text-xs cursor-pointer text-muted-foreground">Plan</Label>
        </div>

        <Select value={maxRows.toString()} onValueChange={v => onMaxRowsChange(Number(v))}>
          <SelectTrigger className="h-8 w-[140px] text-xs">
            <SelectValue placeholder="Row limit" />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="100">Fetch 100 rows</SelectItem>
            <SelectItem value="500">Fetch 500 rows</SelectItem>
            <SelectItem value="1000">Fetch 1,000 rows</SelectItem>
            <SelectItem value="5000">Fetch 5,000 rows</SelectItem>
            <SelectItem value="10000">Fetch 10,000 rows</SelectItem>
          </SelectContent>
        </Select>

        {canExport && (
          <Button
            variant="outline"
            size="sm"
            className="h-8"
            onClick={onExportCsv}
            data-testid="query-csv-export-btn"
          >
            <Download className="w-3.5 h-3.5 mr-2" />
            Export
          </Button>
        )}
      </div>
    </div>
  )
}

export function ModeToggle({ mode, onModeChange }: { mode: QueryMode; onModeChange: (next: QueryMode) => void }) {
  return (
    <Tabs value={mode} onValueChange={(v) => onModeChange(v as QueryMode)}>
      <TabsList>
        <TabsTrigger value="structured">
          <Filter className="h-3.5 w-3.5" />
          Structured
        </TabsTrigger>
        <TabsTrigger value="raw">
          <Code2 className="h-3.5 w-3.5" />
          Edit Raw SQL
        </TabsTrigger>
      </TabsList>
    </Tabs>
  )
}

export function StructuredMode({ structuredSql }: { structuredSql: string }) {
  const mounted = useMounted()
  return (
    <div className="p-4 bg-muted/10">
      <div className="text-[10px] uppercase tracking-wider text-muted-foreground mb-2 font-semibold">
        Generated SQL (sync&apos;d with filter bar)
      </div>
      <pre className="text-xs font-mono whitespace-pre-wrap break-words text-foreground/90 bg-background border rounded p-3 overflow-x-auto min-h-[106px]">
        {mounted
          ? structuredSql
          : 'SELECT *\nFROM logs\nWHERE -- building query preview…\nORDER BY timestamp DESC\nLIMIT 100'}
      </pre>
      <div className="text-[10px] text-muted-foreground mt-2">
        Edit the date range or filters in the header bar above to refine.
        Click column headers below to change sort order — the query re-runs server-side.
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
  data: components['schemas']['QueryResponse']
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
        {isStructured ? (
          <DataTable
            tableCaption="Log Explorer Data Results"
            columns={columns}
            data={data.data || []}
            isLoading={isPending}
            sorting={structuredSorting}
            onSortingChange={onStructuredSortingChange}
          />
        ) : (
          <DataTable
            tableCaption="Log Explorer Data Results"
            columns={columns}
            data={data.data || []}
            isLoading={isPending}
            initialSorting={[{ id: 'timestamp', desc: true }]}
          />
        )}
      </div>
    </div>
  )
}
