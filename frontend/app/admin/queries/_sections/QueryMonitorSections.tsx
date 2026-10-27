'use client'

import * as React from 'react'
import {
  Check,
  Copy,
  Group,
  Pause,
  Play,
  Search,
} from 'lucide-react'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Input } from '@/components/ui/input'
import { Skeleton } from '@/components/ui/skeleton'
import { formatCount } from '@/lib/utils'
import {
  type ActiveOrPromotedRow,
  type ActiveRow,
  type AttributionKind,
  type CompletedRow,
  type DbFilter,
  type GroupedCompletedRow,
  type SlowHistoryMode,
  DB_FILTER_OPTIONS,
  KIND_FILTER_OPTIONS,
  SLOW_HISTORY_META,
  SLOW_HISTORY_MODES,
  formatThresholdMs,
} from './useQueryMonitorState'
import {
  ActiveTable,
  CompletedTable,
  FilterChipRow,
  PollingIndicator,
} from './QueryMonitorComponents'

export function formatSlowQueriesReport(
  queries: readonly CompletedRow[],
  slowHistoryMode: SlowHistoryMode,
  slowThresholdMs: number,
): string {
  const toIso = (ts?: number | null) =>
    ts ? new Date(ts < 10_000_000_000 ? ts * 1000 : ts).toISOString() : '—'
  const lines = [
    '==================================================',
    'NOTABLE SLOW QUERIES REPORT',
    '==================================================',
    `Time Window:    ${SLOW_HISTORY_META[slowHistoryMode].reportLabel}`,
    `Threshold:      ≥ ${slowThresholdMs} ms`,
    `Total Queries:  ${queries.length}`,
    `Exported At:    ${new Date().toISOString()}`,
    '',
  ]
  queries.forEach((row, i) => {
    const attr = row.attribution
    lines.push(
      '--------------------------------------------------',
      `[Query #${i + 1}] ID: ${row.query_id} | DB: ${row.db_type} | Duration: ${formatCount(row.duration_ms)} ms | Outcome: ${row.outcome}`,
      `Timestamp:      Started ${toIso(row.started_at_utc)} · Ended ${toIso(row.ended_at_utc)}`,
      `Attribution:    Kind: ${attr.kind} · Caller: ${attr.caller_qualname} (${attr.caller_file})`,
    )
    if (attr.cron_job) {
      lines.push(
        `Cron:           ${attr.cron_job}${attr.cron_run_id ? ` (Run: ${attr.cron_run_id})` : ''}`,
      )
    }
    if (attr.request_path) {
      lines.push(
        `HTTP Request:   ${attr.request_path}${attr.request_id ? ` (ID: ${attr.request_id})` : ''}`,
      )
    }
    if (row.service_id) lines.push(`Service:        ${row.service_id}`)
    if (row.peak_memory_mb != null) {
      lines.push(`Peak Memory:    ${row.peak_memory_mb.toFixed(1)} MB`)
    }
    if (row.error_message) lines.push(`Error:          ${row.error_message}`)
    lines.push('', 'SQL:', row.sql || row.sql_preview, '')
  })
  lines.push('==================================================', '')
  return lines.join('\n')
}

export function ActiveQueriesCard({
  filteredActive,
  visible,
  isFetching,
  isError,
  paused,
  onTogglePaused,
  dbFilter,
  onDbFilterChange,
  kindFilter,
  onKindFilterChange,
  groupCrons,
  onToggleGroupCrons,
  search,
  onSearchChange,
  searchInputRef,
  isRefreshingActive,
  isInitialLoading,
  onSelectRow,
  onKill,
  cancellingQid,
  onToggleGroup,
}: {
  filteredActive: ActiveOrPromotedRow[]
  visible: boolean
  isFetching: boolean
  isError: boolean
  paused: boolean
  onTogglePaused: () => void
  dbFilter: DbFilter
  onDbFilterChange: (v: DbFilter) => void
  kindFilter: AttributionKind | 'all'
  onKindFilterChange: (v: AttributionKind | 'all') => void
  groupCrons: boolean
  onToggleGroupCrons: () => void
  search: string
  onSearchChange: (v: string) => void
  searchInputRef: React.RefObject<HTMLInputElement | null>
  isRefreshingActive: boolean
  isInitialLoading: boolean
  onSelectRow: (row: ActiveOrPromotedRow) => void
  onKill: (row: ActiveRow) => void
  cancellingQid: number | null
  onToggleGroup: (runId: string) => void
}) {
  return (
    <div
      data-testid="admin-queries-active-reservation"
      className="min-h-[480px] [contain-intrinsic-size:auto_480px] [content-visibility:auto]"
    >
      <Card className="min-h-[480px]">
        <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-3 gap-2">
          <CardTitle className="text-base flex items-center gap-2 whitespace-nowrap shrink-0">
            Active &amp; Just-Finished
            <Badge variant="secondary" className="whitespace-nowrap">
              {filteredActive.filter((r) => !r._completed).length} active
              {filteredActive.some((r) => r._completed) &&
                ` + ${filteredActive.filter((r) => r._completed).length} just-finished`}
            </Badge>
            <PollingIndicator
              visible={visible}
              isFetching={isFetching}
              isError={isError}
              paused={paused}
            />
          </CardTitle>
          <div className="flex items-center gap-2 shrink-0">
            <Button
              variant={paused ? 'default' : 'outline'}
              size="sm"
              className="h-7 px-2 text-xs"
              onClick={onTogglePaused}
              title={paused ? 'Resume polling (.)' : 'Pause polling (.)'}
            >
              {paused ? (
                <>
                  <Play className="h-3 w-3 mr-1" /> Resume
                </>
              ) : (
                <>
                  <Pause className="h-3 w-3 mr-1" /> Pause
                </>
              )}
            </Button>
            <FilterChipRow
              value={dbFilter}
              onChange={onDbFilterChange}
              options={DB_FILTER_OPTIONS}
            />
            <FilterChipRow
              value={kindFilter}
              onChange={onKindFilterChange}
              options={KIND_FILTER_OPTIONS}
            />
            <Button
              variant={groupCrons ? 'default' : 'outline'}
              size="sm"
              className="h-7 px-2 text-xs"
              onClick={onToggleGroupCrons}
              title={
                groupCrons
                  ? 'Cron rows from the same run are collapsed. Click to expand.'
                  : 'Cron rows are shown individually. Click to group by run.'
              }
            >
              <Group className="h-3 w-3 mr-1" />
              Group crons
            </Button>
            <div className="relative">
              <Search className="absolute left-2 top-1/2 -translate-y-1/2 h-3.5 w-3.5 text-muted-foreground" />
              <Input
                ref={searchInputRef}
                placeholder="Filter by SQL or caller…"
                value={search}
                onChange={(e) => onSearchChange(e.target.value)}
                className="h-8 w-48 pl-7 text-sm"
              />
            </div>
          </div>
        </CardHeader>
        <CardContent
          className={`p-0 transition-opacity duration-150 ${
            isRefreshingActive ? 'opacity-40 pointer-events-none' : ''
          }`}
        >
          {isInitialLoading ? (
            <div data-testid="admin-queries-active-skeleton">
              <Skeleton className="h-10 w-full rounded-none" />
              <div className="h-[400px] flex items-center justify-center text-xs text-muted-foreground">
                Inspecting DuckDB connection pool...
              </div>
            </div>
          ) : (
            <ActiveTable
              rows={filteredActive}
              onRowClick={onSelectRow}
              onKill={onKill}
              cancellingQid={cancellingQid}
              onToggleGroup={onToggleGroup}
            />
          )}
        </CardContent>
      </Card>
    </div>
  )
}

export function SlowQueriesCard({
  slowRows,
  slowThresholdMs,
  onSlowThresholdChange,
  slowHistoryMode,
  onSlowHistoryModeChange,
  isFetchingHistorical,
  isRefreshingSlow,
  isInitialLoading,
  copiedQueries,
  onCopyQueries,
  onSelectRow,
  onToggleGroup,
}: {
  slowRows: GroupedCompletedRow[]
  slowThresholdMs: number
  onSlowThresholdChange: (ms: number) => void
  slowHistoryMode: SlowHistoryMode
  onSlowHistoryModeChange: (mode: SlowHistoryMode) => void
  isFetchingHistorical: boolean
  isRefreshingSlow: boolean
  isInitialLoading: boolean
  copiedQueries: boolean
  onCopyQueries: () => void
  onSelectRow: (row: GroupedCompletedRow) => void
  onToggleGroup: (runId: string) => void
}) {
  return (
    <div
      data-testid="admin-queries-slow-reservation"
      className="min-h-[200px] [contain-intrinsic-size:auto_200px] [content-visibility:auto]"
    >
      <Card>
        <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-3">
          <CardTitle className="text-base flex items-center gap-2">
            Notable Slow Queries
            <Badge variant="outline">{slowRows.length}</Badge>
            <span className="text-xs text-muted-foreground font-normal">
              ≥ {formatThresholdMs(slowThresholdMs, slowThresholdMs < 1000)},
              sorted slowest first
            </span>
            {slowHistoryMode !== 'recent' && isFetchingHistorical && (
              <span className="text-xs text-muted-foreground italic">
                Loading query telemetry...
              </span>
            )}
          </CardTitle>
          <div className="flex items-center gap-1">
            <div className="flex items-center gap-1 mr-2">
              {SLOW_HISTORY_MODES.map((m) => (
                <Button
                  key={m}
                  variant={slowHistoryMode === m ? 'default' : 'outline'}
                  size="sm"
                  className="h-7 px-2 text-xs"
                  onClick={() => onSlowHistoryModeChange(m)}
                  title={SLOW_HISTORY_META[m].title}
                >
                  {SLOW_HISTORY_META[m].buttonLabel}
                </Button>
              ))}
            </div>
            {[100, 500, 1000, 2000, 5000].map((ms) => (
              <Button
                key={ms}
                variant={slowThresholdMs === ms ? 'default' : 'outline'}
                size="sm"
                className="h-7 px-2 text-xs"
                onClick={() => onSlowThresholdChange(ms)}
              >
                {formatThresholdMs(ms)}
              </Button>
            ))}
            <Button
              variant="outline"
              size="sm"
              className="h-7 px-2 text-xs ml-2 font-medium shrink-0"
              disabled={slowRows.length === 0}
              onClick={onCopyQueries}
              title="Copy slow queries report to clipboard"
            >
              {copiedQueries ? (
                <>
                  <Check className="h-3 w-3 mr-1 text-emerald-500 animate-in fade-in zoom-in-95 duration-150" />
                  Copied!
                </>
              ) : (
                <>
                  <Copy className="h-3 w-3 mr-1" />
                  Copy queries
                </>
              )}
            </Button>
          </div>
        </CardHeader>
        <CardContent
          className={`p-0 transition-opacity duration-150 ${
            isRefreshingSlow ? 'opacity-40 pointer-events-none' : ''
          }`}
        >
          {isInitialLoading ? (
            <div data-testid="admin-queries-slow-skeleton">
              <Skeleton className="h-10 w-full rounded-none" />
              <div className="h-24 flex items-center justify-center text-xs text-muted-foreground">
                Loading query telemetry...
              </div>
            </div>
          ) : (
            <CompletedTable
              rows={slowRows}
              onRowClick={onSelectRow}
              initialSorting={[{ id: 'duration_ms', desc: true }]}
              onToggleGroup={onToggleGroup}
              emptyMessage={
                slowHistoryMode === 'recent'
                  ? `No queries ≥ ${formatThresholdMs(slowThresholdMs, true)} in recent history.`
                  : isFetchingHistorical
                    ? 'Loading query telemetry...'
                    : `No persisted queries ≥ ${formatThresholdMs(slowThresholdMs, true)} in the last ${SLOW_HISTORY_META[slowHistoryMode].emptyWindow}.`
              }
            />
          )}
        </CardContent>
      </Card>
    </div>
  )
}

export function CompletedQueriesCard({
  completed,
  isRefreshingActive,
  onSelectRow,
  onToggleGroup,
}: {
  completed: GroupedCompletedRow[]
  isRefreshingActive: boolean
  onSelectRow: (row: GroupedCompletedRow) => void
  onToggleGroup: (runId: string) => void
}) {
  return (
    <div
      data-testid="admin-queries-completed-reservation"
      className="min-h-[200px] [contain-intrinsic-size:auto_200px] [content-visibility:auto]"
    >
      <Card>
        <CardHeader className="pb-3">
          <CardTitle className="text-base flex items-center gap-2">
            Recently Completed
            <Badge variant="outline">{completed.length}</Badge>
          </CardTitle>
        </CardHeader>
        <CardContent
          className={`p-0 transition-opacity duration-150 ${
            isRefreshingActive ? 'opacity-40 pointer-events-none' : ''
          }`}
        >
          <CompletedTable
            rows={completed}
            onRowClick={onSelectRow}
            onToggleGroup={onToggleGroup}
          />
        </CardContent>
      </Card>
    </div>
  )
}
