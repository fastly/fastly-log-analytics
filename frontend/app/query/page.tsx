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
import { useLogFieldsCatalog } from '@/hooks/useLogFieldsCatalog'
import { Button } from '@/components/ui/button'
import { Alert, AlertDescription, AlertTitle } from '@/components/ui/alert'
import { Play, Search, AlertCircle, Database, ArrowUp, ArrowDown } from 'lucide-react'
import { NoServiceSelected } from '@/components/NoServiceSelected'
import { PageHeader } from '@/components/ui/page-header'
import { downloadAsCsv } from '@/lib/utils'
import { Skeleton } from '@/components/ui/skeleton'
import type { FiltersPayload } from '@/types/filters'
import { buildStructuredSql, type QueryMode } from './_sql_builder'
import { ModeToggle } from './_sections/ModeToggle'
import { StructuredMode } from './_sections/StructuredMode'
import { RawSqlMode } from './_sections/RawSqlMode'
import { ResultsTable } from './_sections/ResultsTable'
import { QueryToolbar } from './_sections/QueryToolbar'

const HISTORY_KEY = 'fastly_qe_history'

function QueryPageInner() {
  const router = useRouter()
  const searchParams = useSearchParams()
  const { activeServiceId } = useServiceStore()
  const { full, abbr } = useDateFormat()
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

  // fieldTypes drives unquoted IN-list literals for numeric columns
  // (so `edge_score IN (50)` instead of `IN ('50')`). Sourced from the
  // catalog's per-field duckdb_type. Missing catalog → falls back to
  // all-quoted, which still works via DuckDB's implicit cast.
  const { data: catalog } = useLogFieldsCatalog()
  const fieldTypes = useMemo<Record<string, string>>(() => {
    const out: Record<string, string> = {}
    for (const f of catalog?.fields ?? []) {
      if (f.id && f.duckdb_type) out[f.id] = f.duckdb_type
    }
    return out
  }, [catalog])

  // The Structured-mode SQL preview/payload — recomputed whenever filter state
  // or sort changes. Raw mode ignores this entirely.
  const structuredSql = useMemo(
    () => buildStructuredSql(filterPayload, startTime, endTime, structuredSorting, maxRows, fieldTypes),
    [filterPayload, startTime, endTime, structuredSorting, maxRows, fieldTypes],
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

      <ModeToggle mode={mode} onModeChange={handleModeChange} />

      <div className="border rounded-lg bg-card shadow-sm">
        <QueryToolbar
          presets={presets}
          history={history}
          mode={mode}
          onModeChange={handleModeChange}
          onSelectSql={setRawSql}
          onRemoveHistoryItem={removeHistoryItem}
          explain={explain}
          onExplainChange={setExplain}
          maxRows={maxRows}
          onMaxRowsChange={setMaxRows}
          canExport={!!(queryMutation.data?.data && queryMutation.data.data.length > 0)}
          onExportCsv={handleExportCSV}
        />

        {isStructured ? (
          <StructuredMode structuredSql={structuredSql} />
        ) : (
          <RawSqlMode
            rawSql={rawSql}
            onRawSqlChange={setRawSql}
            schema={schemaData?.schema}
            tableName={schemaData?.table_name}
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

      {/* First-run loading state: backend returns ``elapsed_ms`` BUT the
          browser still pays JSON parse + ColumnDef rebuild + first render
          (perceptible on 10k-row responses). Without this skeleton the
          results region is empty between the click and the table paint,
          and the only loading hint is the button's spinner. */}
      {queryMutation.isPending && !queryMutation.data && (
        <div className="space-y-3">
          <div className="flex items-center gap-2 text-xs text-muted-foreground">
            <Database className="h-3 w-3 animate-spin" />
            <span>Running query…</span>
          </div>
          <Skeleton className="h-9 w-full rounded-md" />
          {Array.from({ length: 8 }).map((_, i) => (
            <Skeleton key={i} className="h-8 w-full rounded-md opacity-60" />
          ))}
        </div>
      )}

      {queryMutation.data && (
        <div className="relative">
          {/* Re-run overlay: keeps the prior data visible (preserves scroll
              + sort context) while indicating that fresh results are on
              the way. Pointer-events-none so the user can still scroll. */}
          {queryMutation.isPending && (
            <div className="pointer-events-none absolute inset-0 z-10 flex items-start justify-end p-3">
              <div className="flex items-center gap-2 rounded-md border bg-background/90 px-3 py-1.5 text-xs text-muted-foreground shadow-sm backdrop-blur">
                <Database className="h-3 w-3 animate-spin" />
                <span>Re-running…</span>
              </div>
            </div>
          )}
          <ResultsTable
            data={queryMutation.data}
            isPending={queryMutation.isPending}
            isStructured={isStructured}
            columns={columns}
            structuredSorting={structuredSorting}
            onStructuredSortingChange={setStructuredSorting}
          />
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
