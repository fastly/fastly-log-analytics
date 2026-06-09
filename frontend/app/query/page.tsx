'use client'

import React, { Suspense, useState, useEffect, useMemo, useCallback } from 'react'
import { useRouter, useSearchParams } from 'next/navigation'
import { useQuery, useMutation } from '@tanstack/react-query'
import { useShallow } from 'zustand/react/shallow'
import type { ColumnDef, SortingState } from '@tanstack/react-table'
import { client } from '@/lib/api'
import { useServiceStore } from '@/stores/serviceStore'
import { useFilterStore } from '@/stores/filterStore'
import { useFilterPayload } from '@/hooks/useFilterPayload'
import { useDateFormat } from '@/hooks/useDateFormat'
import { useFieldLabel } from '@/hooks/useFieldLabel'
import { CodeEditor } from '@/components/CodeEditor'
import { DataTable } from '@/components/DataTable'
import { Button, buttonVariants } from '@/components/ui/button'
import { Alert, AlertDescription, AlertTitle } from '@/components/ui/alert'
import { Label } from '@/components/ui/label'
import { Switch } from '@/components/ui/switch'
import { Tabs, TabsList, TabsTrigger } from '@/components/ui/tabs'
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue
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
import { Play, Search, AlertCircle, Clock, Database, ArrowUp, ArrowDown, History, Bookmark, Download, X, Filter, Code2 } from 'lucide-react'
import { NoServiceSelected } from '@/components/NoServiceSelected'
import { PageHeader } from '@/components/ui/page-header'
import { downloadAsCsv } from '@/lib/utils'
import type { FiltersPayload } from '@/types/filters'

const HISTORY_KEY = 'fastly_qe_history'

type QueryMode = 'structured' | 'raw'

/** Escape a string literal for safe SQL embedding (single-quote doubling). */
function sqlEscape(v: string): string {
  return v.replace(/'/g, "''")
}

/** Quote a column identifier for DuckDB (double-quote, escape inner quotes). */
function quoteIdent(name: string): string {
  return `"${name.replace(/"/g, '""')}"`
}

/**
 * Build a WHERE clause fragment from a FiltersPayload + date range.
 * Returns an empty string when nothing is constrained.
 */
function buildWhereClause(
  filters: FiltersPayload,
  startTime: string | null,
  endTime: string | null,
): string {
  const parts: string[] = []

  if (startTime) parts.push(`timestamp >= '${sqlEscape(startTime)}'`)
  if (endTime) parts.push(`timestamp <= '${sqlEscape(endTime)}'`)

  for (const [rawCol, spec] of Object.entries(filters)) {
    if (!spec || !Array.isArray(spec.values) || spec.values.length === 0) continue
    // FilterStore appends `_<n>` to dedupe same-column same-mode buckets; the
    // real column name is everything before the trailing `_<digits>`.
    const col = rawCol.replace(/_\d+$/, '')
    const ident = quoteIdent(col)
    const literals = spec.values.map(v => `'${sqlEscape(String(v))}'`).join(', ')
    const op = spec.mode === 'exclude' ? 'NOT IN' : 'IN'
    parts.push(`${ident} ${op} (${literals})`)
  }

  return parts.length > 0 ? `WHERE ${parts.join(' AND ')}` : ''
}

/**
 * Generate the canonical Structured-Mode SQL. Sort comes from the table's
 * SortingState so column-header clicks round-trip to the server.
 */
function buildStructuredSql(
  filters: FiltersPayload,
  startTime: string | null,
  endTime: string | null,
  sorting: SortingState,
  maxRows: number,
): string {
  const where = buildWhereClause(filters, startTime, endTime)
  const sort = sorting[0]
  const orderBy = sort
    ? `ORDER BY ${quoteIdent(sort.id)} ${sort.desc ? 'DESC' : 'ASC'}`
    : 'ORDER BY timestamp DESC'
  return [
    'SELECT *',
    'FROM logs',
    where,
    orderBy,
    `LIMIT ${maxRows}`,
  ].filter(Boolean).join('\n')
}

function QueryPageInner() {
  const router = useRouter()
  const searchParams = useSearchParams()
  const { activeServiceId } = useServiceStore()
  const { full, abbr, timeAgo } = useDateFormat()
  const getFieldLabel = useFieldLabel()

  // Mode comes from ?mode=raw; defaults to structured. AppLayout's
  // RawQueryModeProbe reads the same param to toggle the global FilterBar.
  const urlMode: QueryMode = searchParams.get('mode') === 'raw' ? 'raw' : 'structured'
  const [mode, setMode] = useState<QueryMode>(urlMode)

  // Keep local mode in sync if the URL changes underneath us (e.g. back/forward).
  useEffect(() => {
    setMode(urlMode)
  }, [urlMode])

  // Filter store drives Structured Mode. We pull primitives so we can compose
  // the generated SQL purely from filter-bar state.
  const { startTime, endTime, addFilter, setRange, clearFilters } = useFilterStore(
    useShallow(state => ({
      startTime: state.startTime,
      endTime: state.endTime,
      addFilter: state.addFilter,
      setRange: state.setRange,
      clearFilters: state.clearFilters,
    })),
  )
  const filterPayload = useFilterPayload()

  // ── One-shot URL hydration ────────────────────────────────────────────────
  // The dashboard "See Raw Logs" CTA links here with ?start_time, ?end_time,
  // and ?filters=<json>. Apply them once into the filter store so the
  // structured mode picks them up, then strip the params so subsequent
  // FilterBar edits aren't fighting a stale URL.
  const [hasHydratedFromUrl, setHasHydratedFromUrl] = useState(false)
  useEffect(() => {
    if (hasHydratedFromUrl) return
    if (typeof window === 'undefined') return

    const params = new URLSearchParams(window.location.search)
    const qsStart = params.get('start_time')
    const qsEnd = params.get('end_time')
    const qsFilters = params.get('filters')

    let mutated = false

    if (qsStart && qsEnd) {
      setRange(qsStart, qsEnd)
      mutated = true
    }

    if (qsFilters) {
      try {
        const parsed = JSON.parse(qsFilters) as FiltersPayload
        if (parsed && typeof parsed === 'object') {
          clearFilters()
          for (const [rawCol, spec] of Object.entries(parsed)) {
            if (!spec || !Array.isArray(spec.values)) continue
            const col = rawCol.replace(/_\d+$/, '')
            for (const v of spec.values) {
              addFilter(col, String(v), spec.mode === 'exclude' ? 'exclude' : 'include')
            }
          }
          mutated = true
        }
      } catch {
        // Malformed ?filters= — ignore silently rather than break the page.
      }
    }

    if (mutated) {
      const url = new URL(window.location.href)
      url.searchParams.delete('start_time')
      url.searchParams.delete('end_time')
      url.searchParams.delete('filters')
      window.history.replaceState({}, '', url.toString())
    }

    setHasHydratedFromUrl(true)
  }, [hasHydratedFromUrl, addFilter, clearFilters, setRange])

  // ── SQL editor + run controls ─────────────────────────────────────────────
  const [rawSql, setRawSql] = useState('SELECT * FROM logs LIMIT 100')
  const [maxRows, setMaxRows] = useState<number>(10000)
  const [explain, setExplain] = useState<boolean>(false)
  const [history, setHistory] = useState<{ sql: string; ts: number }[]>([])

  // Structured-mode sort lives here so the generated SQL can ORDER BY it
  // server-side. Raw-mode sort is owned uncontrolled by DataTable (client side)
  // to keep custom SQL queries from being silently rewritten.
  const [structuredSorting, setStructuredSorting] = useState<SortingState>([
    { id: 'timestamp', desc: true },
  ])

  useEffect(() => {
    try {
      const stored = localStorage.getItem(HISTORY_KEY)
      if (stored) {
        setHistory(JSON.parse(stored))
      }
    } catch { /* ignore */ }
  }, [])

  // The Structured-mode SQL preview/payload — recomputed whenever filter state
  // or sort changes. Raw mode ignores this entirely.
  const structuredSql = useMemo(
    () => buildStructuredSql(filterPayload, startTime, endTime, structuredSorting, maxRows),
    [filterPayload, startTime, endTime, structuredSorting, maxRows],
  )

  const effectiveSql = mode === 'structured' ? structuredSql : rawSql

  const { data: schemaData } = useQuery({
    queryKey: ['admin', 'schema', activeServiceId],
    queryFn: async ({ signal }) => {
      const { data } = await client.GET('/api/schema', { signal })
      return data as any
    },
    enabled: !!activeServiceId,
  })

  const { data: presets } = useQuery({
    queryKey: ['query', 'presets', activeServiceId],
    queryFn: async ({ signal }) => {
      const { data } = await client.GET('/api/presets', { signal })
      return data as any
    },
    enabled: !!activeServiceId,
  })

  const queryMutation = useMutation({
    mutationFn: async (params: { sql: string; max_rows: number; explain: boolean }) => {
      const { data } = await client.POST('/api/query', { body: params })
      return data as any
    },
  })

  const pushHistory = useCallback((sqlToRun: string) => {
    setHistory(prev => {
      const updated = [
        { sql: sqlToRun, ts: Date.now() },
        ...prev.filter(h => h.sql !== sqlToRun),
      ].slice(0, 20)
      try {
        localStorage.setItem(HISTORY_KEY, JSON.stringify(updated))
      } catch { /* ignore */ }
      return updated
    })
  }, [])

  const handleRun = useCallback(() => {
    const sqlToRun = effectiveSql.trim()
    if (!sqlToRun) return
    pushHistory(sqlToRun)
    queryMutation.mutate({ sql: sqlToRun, max_rows: maxRows, explain })
  }, [effectiveSql, maxRows, explain, pushHistory, queryMutation])

  // In Structured Mode, re-run whenever the generated SQL changes (filter,
  // sort, range, row-cap edits) so the result table tracks the FilterBar
  // live. We deliberately don't auto-run in Raw Mode — the user has typed
  // a custom query and shouldn't see it re-execute on every keystroke.
  useEffect(() => {
    if (mode !== 'structured') return
    if (!activeServiceId) return
    if (!hasHydratedFromUrl) return
    pushHistory(structuredSql)
    queryMutation.mutate({ sql: structuredSql, max_rows: maxRows, explain })
    // queryMutation/pushHistory are stable from useMutation/useCallback; we
    // only want to re-fire when the generated SQL or run-time inputs change.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [structuredSql, mode, activeServiceId, hasHydratedFromUrl, maxRows, explain])

  const handleExportCSV = useCallback(() => {
    if (!queryMutation.data?.data?.length) return
    const data = queryMutation.data.data
    const cols = queryMutation.data.columns || []
    downloadAsCsv(data, cols, 'query_results.csv')
  }, [queryMutation.data])

  const removeHistoryItem = useCallback((e: React.MouseEvent, index: number) => {
    e.stopPropagation()
    setHistory(prev => {
      const updated = [...prev]
      updated.splice(index, 1)
      try {
        localStorage.setItem(HISTORY_KEY, JSON.stringify(updated))
      } catch { /* ignore */ }
      return updated
    })
  }, [])

  // Switching modes is reflected in the URL so AppLayout's filter-bar
  // visibility (driven by ?mode=raw) stays in sync without a hard reload.
  const handleModeChange = useCallback((next: QueryMode) => {
    setMode(next)
    const params = new URLSearchParams(window.location.search)
    if (next === 'raw') {
      params.set('mode', 'raw')
      // Seed the raw editor with whatever the structured view currently
      // resolves to, so toggling to "Edit Raw SQL" gives the user a usable
      // starting point rather than the stale placeholder.
      setRawSql(prev => (prev === 'SELECT * FROM logs LIMIT 100' ? structuredSql : prev))
    } else {
      params.delete('mode')
    }
    const qs = params.toString()
    router.replace(qs ? `/query?${qs}` : '/query')
  }, [router, structuredSql])

  const columns: ColumnDef<any>[] = useMemo(() => {
    if (!queryMutation.data?.columns) return []
    return queryMutation.data.columns.map((col: any) => ({
      id: col,
      accessorFn: (row: any) => row[col],
      meta: { label: getFieldLabel(col) },
      header: ({ column }: any) => {
        const isSorted = column.getIsSorted()
        return (
          <Button
            variant="ghost"
            onClick={() => column.toggleSorting(isSorted === 'asc')}
            className="h-8 px-2 data-[state=open]:bg-accent hover:bg-muted font-mono text-xs flex items-center whitespace-nowrap"
          >
            {getFieldLabel(col)}
            {isSorted === 'desc' ? (
              <ArrowDown className="ml-2 h-3 w-3" />
            ) : isSorted === 'asc' ? (
              <ArrowUp className="ml-2 h-3 w-3" />
            ) : null}
          </Button>
        )
      },
      cell: ({ row }: { row: any }) => {
        const value = row.original[col]
        if ((col === 'timestamp' || col.endsWith('_at')) && value && typeof value === 'string' && value.includes('T')) {
          try {
            return <span className="text-xs font-mono">{full(value)} {abbr()}</span>
          } catch {
            // fallback when value is not a valid date string
          }
        }
        return <span className="text-xs font-mono">{value !== null && value !== undefined ? String(value) : 'null'}</span>
      },
    }))
  }, [queryMutation.data?.columns, full, abbr, getFieldLabel])

  if (!activeServiceId) {
    return <NoServiceSelected icon={Search} message="Please select a service from the header to run queries." />
  }

  const isStructured = mode === 'structured'

  return (
    <div className="space-y-6">
      <PageHeader
        title="Query Explorer"
        description={
          isStructured
            ? 'Browse raw request logs using the global filter bar — column headers sort server-side.'
            : 'Write custom SQL against your local DuckDB log cache. Header sorting is client-side only.'
        }
      >
        <Button
          onClick={handleRun}
          disabled={queryMutation.isPending || !effectiveSql.trim()}
          size="lg"
        >
          {queryMutation.isPending ? (
            <Database className="h-4 w-4 mr-2 animate-spin" />
          ) : (
            <Play className="h-4 w-4 mr-2" />
          )}
          Run Query
        </Button>
      </PageHeader>

      {/* Mode toggle. Renders above the editor/preview so it's the first thing
          a user sees on the page and stays in a predictable spot when toggling. */}
      <Tabs value={mode} onValueChange={(v) => handleModeChange(v as QueryMode)}>
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

      <div className="border rounded-lg bg-card shadow-sm">
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
                  {presets?.length ? presets.map((p: any, i: number) => (
                    <DropdownMenuItem
                      key={i}
                      className="flex-col items-start cursor-pointer py-2"
                      onClick={() => {
                        // Selecting a preset implies the user wants raw SQL —
                        // jump them into Raw Mode and pre-fill the editor.
                        if (mode !== 'raw') handleModeChange('raw')
                        setRawSql(p.sql)
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
                      key={i}
                      className="flex items-start justify-between cursor-pointer py-2 group"
                      onClick={() => {
                        if (mode !== 'raw') handleModeChange('raw')
                        setRawSql(h.sql)
                      }}
                    >
                      <div className="overflow-hidden flex-1 mr-4">
                        <div className="text-xs font-mono truncate">{h.sql.replace(/\s+/g, ' ').trim()}</div>
                        <div className="text-[10px] text-muted-foreground mt-1">{timeAgo(new Date(h.ts))}</div>
                      </div>
                      <Button
                        variant="ghost"
                        size="icon"
                        className="h-5 w-5 opacity-0 group-hover:opacity-100 shrink-0"
                        onClick={(e) => removeHistoryItem(e, i)}
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
              <Switch id="explain" checked={explain} onCheckedChange={setExplain} />
              <Label htmlFor="explain" className="text-xs cursor-pointer text-muted-foreground">Plan</Label>
            </div>

            <Select value={maxRows.toString()} onValueChange={v => setMaxRows(Number(v))}>
              <SelectTrigger className="h-8 w-[140px] text-xs">
                <SelectValue placeholder="Row limit" />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="100">Fetch 100 rows</SelectItem>
                <SelectItem value="500">Fetch 500 rows</SelectItem>
                <SelectItem value="1000">Fetch 1,000 rows</SelectItem>
                <SelectItem value="5000">Fetch 5,000 rows</SelectItem>
                <SelectItem value="10000">Fetch 10,000 rows</SelectItem>
                <SelectItem value="50000">Fetch 50,000 rows</SelectItem>
              </SelectContent>
            </Select>

            {queryMutation.data?.data && queryMutation.data.data.length > 0 && (
              <Button variant="outline" size="sm" className="h-8" onClick={handleExportCSV}>
                <Download className="w-3.5 h-3.5 mr-2" />
                Export
              </Button>
            )}
          </div>
        </div>

        {isStructured ? (
          // Structured mode: show the generated SQL read-only so users can
          // see exactly what they're about to run. CodeEditor isn't wired
          // for read-only display, so we render a styled <pre> instead.
          <div className="p-4 bg-muted/10">
            <div className="text-[10px] uppercase tracking-wider text-muted-foreground mb-2 font-semibold">
              Generated SQL (sync'd with filter bar)
            </div>
            <pre className="text-xs font-mono whitespace-pre-wrap break-words text-foreground/90 bg-background border rounded p-3 overflow-x-auto">
              {structuredSql}
            </pre>
            <div className="text-[10px] text-muted-foreground mt-2">
              Edit the date range or filters in the header bar above to refine.
              Click column headers below to change sort order — the query re-runs server-side.
            </div>
          </div>
        ) : (
          <CodeEditor
            value={rawSql}
            onChange={setRawSql}
            schema={schemaData?.schema}
            tableName={schemaData?.table_name}
            height="400px"
          />
        )}
      </div>

      {queryMutation.error && (
        <Alert variant="destructive">
          <AlertCircle className="h-4 w-4" />
          <AlertTitle>Query Error</AlertTitle>
          <AlertDescription className="font-mono text-xs whitespace-pre-wrap break-words mt-2">
            {queryMutation.error instanceof Error ? queryMutation.error.message : 'An unknown error occurred'}
          </AlertDescription>
        </Alert>
      )}

      {queryMutation.data?.explain_plan && (
        <Alert>
          <AlertTitle className="text-xs font-bold uppercase tracking-wider text-muted-foreground mb-2">Query Plan</AlertTitle>
          <AlertDescription>
            <pre className="text-xs font-mono bg-muted/50 p-4 rounded-md overflow-x-auto">
              {queryMutation.data.explain_plan}
            </pre>
          </AlertDescription>
        </Alert>
      )}

      {queryMutation.data && (
        <div className="space-y-4">
          <div className="flex items-center gap-4 text-xs text-muted-foreground px-1">
            <span className="flex items-center gap-1">
              <Database className="h-3 w-3" />
              {queryMutation.data.data?.length || 0} rows returned
              {queryMutation.data.truncated && (
                <span className="text-amber-500 font-semibold ml-1">
                  {queryMutation.data.total_rows && queryMutation.data.total_rows > 0
                    ? `(Truncated to ${queryMutation.data.data?.length} of ${queryMutation.data.total_rows.toLocaleString()})`
                    : `(Truncated to ${queryMutation.data.data?.length} — more available; add LIMIT to count)`}
                </span>
              )}
            </span>
            <span className="flex items-center gap-1">
              <Clock className="h-3 w-3" />
              {queryMutation.data.elapsed_ms}ms execution time
            </span>
          </div>

          <div className="border rounded-lg bg-card overflow-hidden">
            {/* Structured mode is server-sorted (the SortingState is the SQL
                ORDER BY input), so we control DataTable's sorting prop.
                Raw mode owns its own sort state internally — clicking a
                column header re-orders the already-fetched rows client side
                without rewriting the user's SQL. */}
            {isStructured ? (
              <DataTable
                columns={columns}
                data={queryMutation.data.data || []}
                isLoading={queryMutation.isPending}
                sorting={structuredSorting}
                onSortingChange={setStructuredSorting}
              />
            ) : (
              <DataTable
                columns={columns}
                data={queryMutation.data.data || []}
                isLoading={queryMutation.isPending}
                initialSorting={[{ id: 'timestamp', desc: true }]}
              />
            )}
          </div>
        </div>
      )}
    </div>
  )
}

// useSearchParams() requires a Suspense boundary above it in Next.js's
// static-generation path. Wrapping the inner component lets the rest of
// the route render eagerly while the search-params subtree streams in.
export default function QueryPage() {
  return (
    <Suspense fallback={null}>
      <QueryPageInner />
    </Suspense>
  )
}
