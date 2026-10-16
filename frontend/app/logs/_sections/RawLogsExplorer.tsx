'use client'

import React, { useState, useMemo } from 'react'
import { useQuery, keepPreviousData } from '@tanstack/react-query'
import { useDebounce } from '@/hooks/useDebounce'
import {
  Search,
  X,
  ChevronLeft,
  ChevronRight,
  Columns,
  Copy,
  Check,
  Loader2,
  RefreshCw,
  Database,
  ArrowUpDown,
  FileCode,
  Table as TableIcon,
  AlertTriangle,
} from 'lucide-react'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Badge } from '@/components/ui/badge'
import {
  DropdownMenu,
  DropdownMenuCheckboxItem,
  DropdownMenuContent,
  DropdownMenuGroup,
  DropdownMenuLabel,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from '@/components/ui/dropdown-menu'
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogDescription,
} from '@/components/ui/dialog'
import { Tabs, TabsList, TabsTrigger, TabsContent } from '@/components/ui/tabs'
import { ScrollArea } from '@/components/ui/scroll-area'
import { client, extractApiError } from '@/lib/api'
import { showToast } from '@/lib/toast'
import { useTimeRange } from '@/hooks/useTimeRange'
import { useFilterStore } from '@/stores/filterStore'
import { quantizeAnchor } from '@/lib/time-window'
import { resolveRangeWire } from '@/lib/range-wire'
import { useDebouncedFilterPayload } from '@/hooks/useFilterPayload'
import { cn } from '@/lib/utils'
import { useCopyToClipboard } from '@/hooks/useCopyToClipboard'
import { statusBadgeClass } from '@/lib/status-style'

// Wire rows are flat scalar records (backend `rows: list[dict[str, Any]]`).
type LogRow = Record<string, string | number | null | undefined>

const SKELETON_ROW_IDS = ['s0', 's1', 's2', 's3', 's4', 's5', 's6', 's7']

const DEFAULT_COLUMNS = [
  'timestamp',
  'status',
  'request_method',
  'url',
  'client_ip',
  'pop',
  'origin',
  'response_time_ms',
]

const COMMON_OPTIONAL_COLUMNS = [
  'content_type',
  'cache_status',
  'asn',
  'geo_country_code',
  'tls_protocol',
  'tls_cipher',
  'fastly_service_id',
  'server_datacenter',
]

function getMethodBadgeVariant(method: string | number | null | undefined): string {
  const m = String(method || '').toUpperCase()
  if (m === 'GET') return 'bg-sky-500/10 text-sky-600 dark:text-sky-400 border-sky-500/20'
  if (m === 'POST') return 'bg-emerald-500/10 text-emerald-600 dark:text-emerald-400 border-emerald-500/20'
  if (m === 'PUT' || m === 'PATCH') return 'bg-amber-500/10 text-amber-600 dark:text-amber-400 border-amber-500/20'
  if (m === 'DELETE') return 'bg-destructive/10 text-destructive border-destructive/20'
  return 'bg-muted text-muted-foreground'
}

export function RawLogsExplorer({
  activeServiceId,
  isAnalyst = false,
}: {
  activeServiceId: string | null
  isAnalyst?: boolean
}) {
  const { startTime, endTime } = useTimeRange()
  const relativeRange = useFilterStore((s) => s.relativeRange)
  const isAutoRange = useFilterStore((s) => s.isAutoRange)
  const storeEndTime = useFilterStore((s) => s.endTime)

  const anchor = useMemo(() => {
    return quantizeAnchor(storeEndTime)
  }, [storeEndTime])

  const filterPayload = useDebouncedFilterPayload(true)

  const rangeWire = useMemo(() => {
    return resolveRangeWire({
      relativeRange,
      isAutoRange,
      startTime,
      endTime,
      anchor,
    })
  }, [relativeRange, isAutoRange, startTime, endTime, anchor])

  const [searchInput, setSearchInput] = useState('')
  const debouncedSearch = useDebounce(searchInput.trim(), 300)
  const [limit, setLimit] = useState(50)
  // Pagination is keyed to the filter/range/search it was set under; a key
  // mismatch reads as page 0, so a filter change never fires a fetch with the
  // previous filter's stale offset (no reset effect needed).
  const pageKey = JSON.stringify([filterPayload, rangeWire.rangeKey, debouncedSearch])
  const [page, setPage] = useState({ offset: 0, key: pageKey })
  const offset = page.key === pageKey ? page.offset : 0
  const setOffset = (next: number | ((prev: number) => number)) =>
    setPage({ offset: typeof next === 'function' ? next(offset) : next, key: pageKey })
  const [sortDir, setSortDir] = useState<'desc' | 'asc'>('desc')
  const [selectedColumns, setSelectedColumns] = useState<string[]>(DEFAULT_COLUMNS)
  const [selectedRow, setSelectedRow] = useState<LogRow | null>(null)
  const { copied, copy } = useCopyToClipboard(2000)
  const [detailFilter, setDetailFilter] = useState('')

  // Query logs
  const {
    data,
    isLoading,
    isFetching,
    error,
    refetch,
  } = useQuery({
    queryKey: [
      'logs',
      'query',
      activeServiceId,
      rangeWire.rangeKey,
      anchor,
      filterPayload,
      debouncedSearch,
      limit,
      offset,
      sortDir,
      selectedColumns.join(','),
    ],
    queryFn: async ({ signal }) => {
      const { data, error } = await client.POST('/api/logs/query', {
        signal,
        body: {
          filters: filterPayload,
          search: debouncedSearch || undefined,
          columns: selectedColumns,
          limit,
          offset,
          sort_dir: sortDir,
          ...rangeWire.rangeBody,
        },
      })
      if (error) {
        throw new Error(extractApiError(error))
      }
      return data
    },
    enabled: !!activeServiceId,
    placeholderData: keepPreviousData,
    staleTime: 10_000,
  })

  // Discover all available columns from response or fallback list
  const schemaColumns = data?.schema_columns
  const availableColumns = useMemo(() => {
    const cols = new Set<string>([...DEFAULT_COLUMNS, ...COMMON_OPTIONAL_COLUMNS])
    schemaColumns?.forEach((col) => cols.add(col.name))
    return Array.from(cols)
  }, [schemaColumns])

  const toggleColumn = (col: string) => {
    setSelectedColumns((prev) => {
      if (prev.includes(col)) {
        if (prev.length <= 1) return prev // Keep at least one column
        return prev.filter((c) => c !== col)
      } else {
        return [...prev, col]
      }
    })
  }

  const handleCopyJson = () => {
    if (!selectedRow) return
    copy(JSON.stringify(selectedRow, null, 2))
    showToast('Log record JSON copied to clipboard', 'success')
  }

  const totalCount = data?.total_count ?? 0
  const rows = (data?.rows ?? []) as LogRow[]
  const hasMore = data?.has_more ?? false

  const filteredDetailKeys = useMemo(() => {
    if (!selectedRow) return []
    const keys = Object.keys(selectedRow).sort()
    if (!detailFilter.trim()) return keys
    const term = detailFilter.toLowerCase()
    return keys.filter(
      (k) =>
        k.toLowerCase().includes(term) ||
        String(selectedRow[k] ?? '').toLowerCase().includes(term),
    )
  }, [selectedRow, detailFilter])

  return (
    <div
      data-testid="raw-logs-explorer"
      className="space-y-4 [contain-intrinsic-size:600px] [content-visibility:auto]"
    >
      {/* Controls & Search Toolbar */}
      <div className="flex flex-wrap items-center justify-between gap-3 bg-card p-3 rounded-lg border">
        {/* Search Input */}
        <div className="relative flex-1 min-w-[260px] max-w-md">
          <Search className="absolute left-2.5 top-2.5 h-4 w-4 text-muted-foreground" />
          <Input
            data-testid="raw-logs-search-input"
            type="text"
            placeholder="Search URLs, IPs, status, POP..."
            value={searchInput}
            onChange={(e) => setSearchInput(e.target.value)}
            className="pl-9 pr-8 h-9 text-xs"
          />
          {searchInput && (
            <button
              onClick={() => setSearchInput('')}
              className="absolute right-2.5 top-2.5 text-muted-foreground hover:text-foreground"
              aria-label="Clear search"
            >
              <X className="h-4 w-4" />
            </button>
          )}
        </div>

        {/* Right Controls */}
        <div className="flex items-center gap-2">
          {/* Sort Direction Toggle */}
          <Button
            variant="outline"
            size="sm"
            onClick={() => setSortDir((prev) => (prev === 'desc' ? 'asc' : 'desc'))}
            className="h-9 text-xs gap-1.5"
            title={`Sort order: ${sortDir.toUpperCase()}`}
          >
            <ArrowUpDown className="h-3.5 w-3.5" />
            <span>{sortDir === 'desc' ? 'Newest First' : 'Oldest First'}</span>
          </Button>

          {/* Column Selector Dropdown */}
          <DropdownMenu>
            <DropdownMenuTrigger
              data-testid="raw-logs-column-selector"
              className="inline-flex items-center justify-center gap-1.5 h-9 px-3 text-xs font-medium border rounded-md bg-background hover:bg-accent hover:text-accent-foreground cursor-pointer focus:outline-none"
            >
              <Columns className="h-3.5 w-3.5" />
              <span>Columns ({selectedColumns.length})</span>
            </DropdownMenuTrigger>
            <DropdownMenuContent align="end" className="w-56 max-h-80 overflow-y-auto">
              <DropdownMenuGroup>
                <DropdownMenuLabel className="text-xs">Display Columns</DropdownMenuLabel>
                <DropdownMenuSeparator />
                {availableColumns.map((col) => (
                  <DropdownMenuCheckboxItem
                    key={col}
                    checked={selectedColumns.includes(col)}
                    onCheckedChange={() => toggleColumn(col)}
                    className="text-xs cursor-pointer"
                  >
                    {col}
                  </DropdownMenuCheckboxItem>
                ))}
              </DropdownMenuGroup>
            </DropdownMenuContent>
          </DropdownMenu>

          {/* Rows per page selector */}
          <div className="flex items-center border rounded-md overflow-hidden bg-background">
            <button
              onClick={() => {
                setLimit(50)
                setOffset(0)
              }}
              className={cn(
                'px-2.5 py-1 text-xs font-medium transition-colors',
                limit === 50 ? 'bg-primary text-primary-foreground' : 'hover:bg-muted text-muted-foreground',
              )}
            >
              50
            </button>
            <button
              onClick={() => {
                setLimit(100)
                setOffset(0)
              }}
              className={cn(
                'px-2.5 py-1 text-xs font-medium transition-colors',
                limit === 100 ? 'bg-primary text-primary-foreground' : 'hover:bg-muted text-muted-foreground',
              )}
            >
              100
            </button>
          </div>

          {/* Manual Refetch Button */}
          <Button
            variant="outline"
            size="sm"
            onClick={() => refetch()}
            disabled={isFetching}
            className="h-9 px-2.5 text-xs"
            title="Refresh logs"
          >
            <RefreshCw className={cn('h-3.5 w-3.5', isFetching && 'animate-spin')} />
          </Button>
        </div>
      </div>

      {/* Logs Table Container */}
      <div
        data-testid="raw-logs-table-container"
        className={cn(
          'border rounded-lg overflow-hidden bg-card transition-opacity duration-150 min-h-[500px] flex flex-col justify-between',
          isFetching && !isLoading && 'opacity-60 pointer-events-none',
        )}
      >
        <div className="overflow-x-auto w-full">
          {error ? (
            <div className="flex flex-col items-center justify-center p-12 text-center space-y-3">
              <AlertTriangle className="h-10 w-10 text-destructive" />
              <h4 className="text-base font-semibold">Error Querying Raw Logs</h4>
              <p className="text-sm text-muted-foreground max-w-md">
                {(error as Error).message || 'Failed to fetch log records.'}
              </p>
              <Button size="sm" variant="outline" onClick={() => refetch()}>
                Retry Query
              </Button>
            </div>
          ) : isLoading ? (
            <div className="space-y-3 p-4">
              <div className="flex items-center justify-center py-16 gap-2 text-sm text-muted-foreground">
                <Loader2 className="h-4 w-4 animate-spin text-primary" />
                <span>Querying edge log records...</span>
              </div>
              <div className="space-y-2">
                {SKELETON_ROW_IDS.map((id) => (
                  <div key={id} className="h-10 w-full bg-muted/40 rounded animate-pulse" />
                ))}
              </div>
            </div>
          ) : rows.length === 0 ? (
            <div className="flex flex-col items-center justify-center p-16 text-center space-y-3">
              <Database className="h-10 w-10 text-muted-foreground/60" />
              <h4 className="text-base font-medium">No Log Records Found</h4>
              <p className="text-sm text-muted-foreground max-w-sm">
                No access log events matched the active time range, search term, or filters.
              </p>
            </div>
          ) : (
            <table data-testid="raw-logs-table" className="w-full text-left border-collapse text-xs">
              <thead>
                <tr className="border-b bg-muted/40 text-muted-foreground font-medium">
                  {selectedColumns.map((col) => (
                    <th key={col} className="p-2.5 whitespace-nowrap font-semibold">
                      {col === 'timestamp'
                        ? 'Timestamp'
                        : col === 'status'
                        ? 'Status'
                        : col === 'request_method'
                        ? 'Method'
                        : col === 'url'
                        ? 'URL Path'
                        : col === 'client_ip'
                        ? 'Client IP'
                        : col === 'pop'
                        ? 'POP'
                        : col === 'origin'
                        ? 'Origin'
                        : col === 'response_time_ms'
                        ? 'Duration'
                        : col}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody className="divide-y divide-border/50">
                {rows.map((row, idx) => (
                  <tr
                    key={row.timestamp ? `${row.timestamp}-${idx}` : idx}
                    onClick={() => setSelectedRow(row)}
                    className="hover:bg-muted/50 cursor-pointer transition-colors group"
                  >
                    {selectedColumns.map((col) => {
                      const val = row[col]

                      if (col === 'timestamp') {
                        const ts = val ? String(val).replace('T', ' ').replace('Z', '') : '—'
                        return (
                          <td key={col} className="p-2.5 whitespace-nowrap font-mono text-muted-foreground">
                            {ts}
                          </td>
                        )
                      }

                      if (col === 'status') {
                        return (
                          <td key={col} className="p-2.5 whitespace-nowrap">
                            <span
                              className={cn(
                                'px-2 py-0.5 rounded text-[11px] font-semibold border',
                                statusBadgeClass(val),
                              )}
                            >
                              {val ?? '—'}
                            </span>
                          </td>
                        )
                      }

                      if (col === 'request_method') {
                        return (
                          <td key={col} className="p-2.5 whitespace-nowrap">
                            <span
                              className={cn(
                                'px-1.5 py-0.5 rounded text-[10px] font-mono font-bold border',
                                getMethodBadgeVariant(val),
                              )}
                            >
                              {val ?? 'GET'}
                            </span>
                          </td>
                        )
                      }

                      if (col === 'url') {
                        return (
                          <td
                            key={col}
                            className="p-2.5 max-w-md truncate font-mono text-[11px] text-foreground/90 group-hover:text-primary transition-colors"
                            title={String(val ?? '')}
                          >
                            {val ?? '—'}
                          </td>
                        )
                      }

                      if (col === 'client_ip') {
                        return (
                          <td key={col} className="p-2.5 whitespace-nowrap font-mono text-foreground/80">
                            {val ?? '—'}
                          </td>
                        )
                      }

                      if (col === 'pop') {
                        return (
                          <td key={col} className="p-2.5 whitespace-nowrap">
                            {val ? (
                              <Badge variant="outline" className="text-[10px] font-mono px-1.5 py-0">
                                {val}
                              </Badge>
                            ) : (
                              '—'
                            )}
                          </td>
                        )
                      }

                      if (col === 'response_time_ms') {
                        const num = typeof val === 'number' ? Math.round(val) : val
                        return (
                          <td key={col} className="p-2.5 whitespace-nowrap font-mono text-muted-foreground">
                            {num != null ? `${num}ms` : '—'}
                          </td>
                        )
                      }

                      return (
                        <td key={col} className="p-2.5 whitespace-nowrap max-w-xs truncate text-muted-foreground">
                          {val != null ? String(val) : '—'}
                        </td>
                      )
                    })}
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </div>

        {/* Pagination Bar */}
        <div className="flex flex-wrap items-center justify-between gap-2 p-3 border-t bg-muted/20 text-xs">
          <div className="text-muted-foreground">
            {totalCount > 0 ? (
              <span>
                Showing <strong className="text-foreground">{offset + 1}</strong> to{' '}
                <strong className="text-foreground">{Math.min(offset + limit, totalCount)}</strong> of{' '}
                <strong className="text-foreground">{totalCount.toLocaleString()}</strong> events
              </span>
            ) : (
              <span>0 events</span>
            )}
          </div>

          <div className="flex items-center gap-1.5">
            <Button
              data-testid="raw-logs-pagination-prev"
              variant="outline"
              size="sm"
              onClick={() => setOffset((prev) => Math.max(0, prev - limit))}
              disabled={offset === 0 || isLoading}
              className="h-8 px-2.5 text-xs gap-1"
            >
              <ChevronLeft className="h-3.5 w-3.5" />
              <span>Previous</span>
            </Button>
            <Button
              data-testid="raw-logs-pagination-next"
              variant="outline"
              size="sm"
              onClick={() => setOffset((prev) => prev + limit)}
              disabled={!hasMore || isLoading}
              className="h-8 px-2.5 text-xs gap-1"
            >
              <span>Next</span>
              <ChevronRight className="h-3.5 w-3.5" />
            </Button>
          </div>
        </div>
      </div>

      {/* Row Inspection Detail Modal */}
      <Dialog open={!!selectedRow} onOpenChange={(open) => !open && setSelectedRow(null)}>
        <DialogContent
          data-testid="raw-logs-row-detail-modal"
          className="max-w-3xl max-h-[85vh] flex flex-col p-6 overflow-hidden"
        >
          {selectedRow && (
            <>
              <DialogHeader className="border-b pb-4">
                <div className="flex items-start justify-between gap-4">
                  <div className="space-y-1">
                    <DialogTitle className="text-base font-semibold flex items-center gap-2">
                      <span
                        className={cn(
                          'px-2 py-0.5 rounded text-xs font-mono font-bold border',
                          getMethodBadgeVariant(selectedRow.request_method),
                        )}
                      >
                        {selectedRow.request_method || 'GET'}
                      </span>
                      <span
                        className={cn(
                          'px-2 py-0.5 rounded text-xs font-semibold border',
                          statusBadgeClass(selectedRow.status),
                        )}
                      >
                        {selectedRow.status || '—'}
                      </span>
                      <span className="font-mono text-xs text-muted-foreground truncate max-w-sm">
                        {selectedRow.url || '—'}
                      </span>
                    </DialogTitle>
                    <DialogDescription className="text-xs text-muted-foreground font-mono">
                      Timestamp: {selectedRow.timestamp || '—'} | POP: {selectedRow.pop || '—'} | IP:{' '}
                      {selectedRow.client_ip || '—'}
                    </DialogDescription>
                  </div>

                  <Button
                    data-testid="raw-logs-copy-json"
                    variant="outline"
                    size="sm"
                    onClick={handleCopyJson}
                    className="h-8 text-xs gap-1.5 shrink-0"
                  >
                    {copied ? <Check className="h-3.5 w-3.5 text-emerald-500" /> : <Copy className="h-3.5 w-3.5" />}
                    <span>{copied ? 'Copied' : 'Copy JSON'}</span>
                  </Button>
                </div>
              </DialogHeader>

              <Tabs defaultValue="formatted" className="flex-1 flex flex-col min-h-0 pt-2">
                <div className="flex items-center justify-between pb-2 border-b">
                  <TabsList className="h-8">
                    <TabsTrigger value="formatted" className="text-xs gap-1.5">
                      <TableIcon className="h-3.5 w-3.5" /> Formatted
                    </TabsTrigger>
                    <TabsTrigger value="raw" className="text-xs gap-1.5">
                      <FileCode className="h-3.5 w-3.5" /> Raw JSON
                    </TabsTrigger>
                  </TabsList>

                  <Input
                    type="text"
                    placeholder="Filter fields..."
                    value={detailFilter}
                    onChange={(e) => setDetailFilter(e.target.value)}
                    className="h-7 w-48 text-xs"
                  />
                </div>

                <TabsContent value="formatted" className="flex-1 min-h-0 pt-2">
                  <ScrollArea className="h-[52vh] pr-4">
                    <table className="w-full text-xs font-mono border-collapse">
                      <thead>
                        <tr className="border-b text-muted-foreground">
                          <th className="py-2 text-left font-semibold w-1/3">Field</th>
                          <th className="py-2 text-left font-semibold w-2/3">Value</th>
                        </tr>
                      </thead>
                      <tbody className="divide-y divide-border/40">
                        {filteredDetailKeys.map((key) => {
                          const val = selectedRow[key]
                          const displayVal =
                            val === null || val === undefined
                              ? 'null'
                              : typeof val === 'object'
                              ? JSON.stringify(val)
                              : String(val)

                          return (
                            <tr key={key} className="hover:bg-muted/30">
                              <td className="py-1.5 font-semibold text-primary/80 align-top select-all">
                                {key}
                              </td>
                              <td className="py-1.5 text-foreground/90 break-all select-all">
                                {displayVal}
                              </td>
                            </tr>
                          )
                        })}
                      </tbody>
                    </table>
                  </ScrollArea>
                </TabsContent>

                <TabsContent value="raw" className="flex-1 min-h-0 pt-2">
                  <ScrollArea className="h-[52vh]">
                    <pre className="p-4 rounded-lg bg-muted/40 text-xs font-mono text-foreground/90 overflow-x-auto select-all">
                      {JSON.stringify(selectedRow, null, 2)}
                    </pre>
                  </ScrollArea>
                </TabsContent>
              </Tabs>
            </>
          )}
        </DialogContent>
      </Dialog>
    </div>
  )
}
