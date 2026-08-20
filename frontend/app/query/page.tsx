'use client'

import React, { Suspense, useState, useEffect, useMemo, useCallback } from 'react'
import { useRouter, useSearchParams } from 'next/navigation'
import { useQuery, useMutation } from '@tanstack/react-query'
import { useShallow } from 'zustand/react/shallow'
import type { ColumnDef, SortingState } from '@tanstack/react-table'
import { client } from '@/lib/api'
import { useFilterStore } from '@/stores/filterStore'
import { useEffectiveServiceId, useBootstrapResolved } from '@/hooks/useIsDataReady'
import { useDebouncedFilterPayload } from '@/hooks/useFilterPayload'
import { useFilterUrlWriteback } from '@/hooks/useFilterUrlWriteback'
import { useDateFormat } from '@/hooks/useDateFormat'
import { useFieldLabel } from '@/hooks/useFieldLabel'
import { useLogFieldsCatalog } from '@/hooks/useLogFieldsCatalog'
import { Button } from '@/components/ui/button'
import { Alert, AlertDescription, AlertTitle } from '@/components/ui/alert'
import { Play, Search, AlertCircle, Database, Terminal, Activity, Bug } from 'lucide-react'
import { Tabs, TabsList, TabsTrigger } from '@/components/ui/tabs'
import { NoServiceSelected } from '@/components/NoServiceSelected'
import { PageHeader } from '@/components/ui/page-header'
import { downloadAsCsv } from '@/lib/utils'
import { Skeleton } from '@/components/ui/skeleton'
import type { FiltersPayload } from '@/types/filters'
import { buildStructuredSql, type QueryMode } from './_sql_builder'
import { parseJsonAsync } from '@/lib/workers/parseJson'
import { ModeToggle } from './_sections/ModeToggle'
import { StructuredMode } from './_sections/StructuredMode'
import { RawSqlMode } from './_sections/RawSqlMode'
import { ResultsTable } from './_sections/ResultsTable'
import { QueryToolbar } from './_sections/QueryToolbar'

const HISTORY_KEY = 'fastly_qe_history'

function QueryPageInner() {
  const router = useRouter()
  const searchParams = useSearchParams()
  // Read via the bootstrap-aware fallback so a cold load (empty
  // localStorage) doesn't flash NoServiceSelected before useBootstrap
  // populates the persisted store.
  const activeServiceId = useEffectiveServiceId() ?? null
  const bootstrapResolved = useBootstrapResolved()
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
  // Pass `true` so the FilterBar's "Edge only" toggle reaches the query
  // — Structured Mode delegates to the same aggregates path the reports use.
  const filterPayload = useDebouncedFilterPayload(true)

  // Write store → URL on every filter / range mutation so /query becomes
  // shareable. URL → store hydration is owned at module load by
  // hydrateFilterStoreFromUrl in QueryProvider; this hook only handles the
  // write-back loop the audit identified as missing here.
  useFilterUrlWriteback()

  // ── One-shot URL hydration ────────────────────────────────────────────────
  // The dashboard "See Raw Logs" CTA links here with ?start_time, ?end_time,
  // and ?filters=<json>. Apply them once into the filter store so the
  // structured mode picks them up, then strip the params so subsequent
  // FilterBar edits aren't fighting a stale URL.
  const [hasHydratedFromUrl, setHasHydratedFromUrl] = useState(false)
  // Gate the Structured-mode auto-run on user intent. A fresh /query
  // visit (no deep-link params, no Run click) should NOT immediately
  // fire a backend query — the previous behaviour issued a default
  // ``SELECT * FROM logs LIMIT 100`` against the cold view on every
  // navigation. ``hasUserRun`` becomes true on a deep-link hydration
  // (See Raw Logs CTA) and on an explicit Run button click.
  const [hasUserRun, setHasUserRun] = useState(false)
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
      // Deep-link landings are an explicit user intent — auto-run is
      // the entire point of the See Raw Logs CTA. useFilterUrlWriteback owns
      // the canonical URL state from the next effect tick onward, so we
      // intentionally do NOT strip the consumed params here.
      setHasUserRun(true)
    }

    setHasHydratedFromUrl(true)
  }, [hasHydratedFromUrl, addFilter, clearFilters, setRange])

  // ── SQL editor + run controls ─────────────────────────────────────────────
  type Dataset = 'logs' | 'client_vitals' | 'client_errors'
  const [dataset, setDataset] = useState<Dataset>('logs')

  const RUM_SCHEMAS = useMemo<Record<string, { name: string; type: string }[]>>(() => ({
    client_vitals: [
      { name: 'timestamp', type: 'timestamp' },
      { name: 'metric_name', type: 'string' },
      { name: 'metric_value', type: 'double' },
      { name: 'metric_rating', type: 'string' },
      { name: 'pathname', type: 'string' },
      { name: 'browser', type: 'string' },
      { name: 'os', type: 'string' },
      { name: 'device', type: 'string' },
      { name: 'cid', type: 'string' },
      { name: 'req_id', type: 'string' },
    ],
    client_errors: [
      { name: 'timestamp', type: 'timestamp' },
      { name: 'error_message', type: 'string' },
      { name: 'error_file', type: 'string' },
      { name: 'error_line', type: 'integer' },
      { name: 'error_col', type: 'integer' },
      { name: 'pathname', type: 'string' },
      { name: 'browser', type: 'string' },
      { name: 'os', type: 'string' },
      { name: 'device', type: 'string' },
      { name: 'cid', type: 'string' },
      { name: 'req_id', type: 'string' },
    ],
  }), [])

  const [rawSql, setRawSql] = useState('SELECT * FROM logs LIMIT 100')

  const handleDatasetChange = useCallback((nextDataset: Dataset) => {
    setDataset(nextDataset)
    setRawSql(`SELECT * FROM ${nextDataset} LIMIT 100`)
  }, [])

  // Default maxRows 100, not 10000. The previous default forced the page
  // to fetch up to 19 MB of JSON on cold load (analyst-30d clocked 17.9 s
  // p50 + occasional Fastly 503 on the synthesized timeout), and the
  // overwhelming majority of users never scroll past the first few
  // rows. Power users can still type 10000 (or higher) in the
  // SQL Controls input; the DataTable's scroll-fetch (when added) can
  // also page in more rows on demand.
  const [maxRows, setMaxRows] = useState<number>(100)
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

  const { data: schemaData } = useQuery({
    queryKey: ['admin', 'schema', activeServiceId],
    queryFn: async ({ signal }) => {
      const { data } = await client.GET('/api/schema', { signal })
      return data as any
    },
    enabled: !!activeServiceId,
    // Schema only changes when admin adds a custom field — never within
    // an interactive session. 5 min staleTime removes the RTT (and the
    // 10-s cold-cache tail variance) from every cross-page navigation.
    staleTime: 5 * 60_000,
  })

  // fieldTypes drives unquoted IN-list literals for numeric columns
  // (so `edge_score IN (50)` instead of `IN ('50')`). Sourced from the
  // catalog's per-field duckdb_type. Missing catalog → falls back to
  // all-quoted, which still works via DuckDB's implicit cast.
  const { data: catalog } = useLogFieldsCatalog()
  const activeFieldTypes = useMemo<Record<string, string>>(() => {
    const out: Record<string, string> = {}
    if (dataset === 'logs') {
      for (const f of catalog?.fields ?? []) {
        if (f.id && f.duckdb_type) out[f.id] = f.duckdb_type
      }
    } else {
      const fields = RUM_SCHEMAS[dataset] || []
      for (const f of fields) {
        out[f.name] = f.type
      }
    }
    return out
  }, [dataset, catalog, RUM_SCHEMAS])

  const activeSchema = useMemo(() => {
    if (dataset === 'client_vitals') return RUM_SCHEMAS.client_vitals
    if (dataset === 'client_errors') return RUM_SCHEMAS.client_errors
    return schemaData?.schema
  }, [dataset, schemaData, RUM_SCHEMAS])

  const activeTableName = useMemo(() => {
    if (dataset === 'client_vitals') return 'client_vitals'
    if (dataset === 'client_errors') return 'client_errors'
    return schemaData?.table_name || 'logs'
  }, [dataset, schemaData])

  // The Structured-mode SQL preview/payload — recomputed whenever filter state
  // or sort changes. Raw mode ignores this entirely.
  const structuredSql = useMemo(
    () => buildStructuredSql(filterPayload, startTime, endTime, structuredSorting, maxRows, activeFieldTypes, activeTableName),
    [filterPayload, startTime, endTime, structuredSorting, maxRows, activeFieldTypes, activeTableName],
  )

  const effectiveSql = mode === 'structured' ? structuredSql : rawSql

  const { data: presets } = useQuery({
    queryKey: ['query', 'presets', activeServiceId],
    queryFn: async ({ signal }) => {
      const { data } = await client.GET('/api/presets', { signal })
      return data as any
    },
    enabled: !!activeServiceId,
    // Same rationale as schema above — presets list only changes when
    // someone edits service config. The 45-s analyst-30d outlier this
    // call has occasionally hit goes away once it's cached past first.
    staleTime: 5 * 60_000,
  })

  const queryMutation = useMutation({
    mutationFn: async (params: { sql: string; max_rows: number; explain: boolean; dataset?: Dataset }) => {
      const { data, error } = await client.POST('/api/query', { body: params as any, parseAs: 'text' })
      if (error) {
        if (typeof error === 'string') {
          try {
            throw JSON.parse(error)
          } catch (e) {
            throw new Error(error)
          }
        }
        throw error
      }
      if (!data) throw new Error('No data')
      if (typeof data !== 'string') return data as any
      return await parseJsonAsync<any>(data as string)
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
    // Mark first-paint auto-run as wanted — subsequent filter / sort /
    // range edits become live again once the user has explicitly run
    // once.
    setHasUserRun(true)
    pushHistory(sqlToRun)
    queryMutation.mutate({ sql: sqlToRun, max_rows: maxRows, explain, dataset })
  }, [effectiveSql, maxRows, explain, dataset, pushHistory, queryMutation])

  // In Structured Mode, re-run whenever the generated SQL changes (filter,
  // sort, range, row-cap edits) so the result table tracks the FilterBar
  // live. We deliberately don't auto-run in Raw Mode — the user has typed
  // a custom query and shouldn't see it re-execute on every keystroke.
  // We also skip the very first paint unless the user signalled intent
  // (deep-link via See Raw Logs OR explicit Run click) — see hasUserRun.
  useEffect(() => {
    if (mode !== 'structured') return
    if (!activeServiceId) return
    if (!hasHydratedFromUrl) return
    if (!hasUserRun) return
    pushHistory(structuredSql)
    queryMutation.mutate({ sql: structuredSql, max_rows: maxRows, explain, dataset })
    // queryMutation/pushHistory are stable from useMutation/useCallback; we
    // only want to re-fire when the generated SQL or run-time inputs change.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [structuredSql, mode, activeServiceId, hasHydratedFromUrl, hasUserRun, maxRows, explain, dataset])

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
      // Plain-label header — the DataTable header supplies the sort
      // button + arrow. Returning a <Button> here nested a button inside
      // that button: invalid HTML the browser restructures on parse,
      // breaking hydration (React #418). A <span> keeps the monospace
      // field-name styling without the interactive nesting.
      header: () => <span className="font-mono text-xs">{getFieldLabel(col)}</span>,
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

  if (!activeServiceId && bootstrapResolved) {
    return <NoServiceSelected icon={Search} message="Please select a service from the header to run queries." />
  }
  if (!activeServiceId) {
    // Bootstrap in flight on cold load — render nothing rather than
    // flash the fallback. The page will re-render with real content
    // as soon as bootstrap commits.
    return null
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

      <div className="flex flex-col gap-4 sm:flex-row sm:items-center sm:justify-between">
        <ModeToggle mode={mode} onModeChange={handleModeChange} />
        <DatasetToggle dataset={dataset} onDatasetChange={handleDatasetChange} />
      </div>

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
            schema={activeSchema}
            tableName={activeTableName}
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
            <Skeleton key={`skeleton-row-${i}`} className="h-8 w-full rounded-md opacity-60" />
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

interface DatasetToggleProps {
  dataset: 'logs' | 'client_vitals' | 'client_errors'
  onDatasetChange: (next: 'logs' | 'client_vitals' | 'client_errors') => void
}

export function DatasetToggle({ dataset, onDatasetChange }: DatasetToggleProps) {
  return (
    <Tabs value={dataset} onValueChange={(v) => onDatasetChange(v as any)}>
      <TabsList className="grid w-full grid-cols-3 max-w-[480px]">
        <TabsTrigger value="logs" className="flex items-center gap-1.5">
          <Terminal className="h-3.5 w-3.5" />
          <span className="truncate">CDN Request Logs</span>
        </TabsTrigger>
        <TabsTrigger value="client_vitals" className="flex items-center gap-1.5">
          <Activity className="h-3.5 w-3.5" />
          <span className="truncate">RUM Web Vitals</span>
        </TabsTrigger>
        <TabsTrigger value="client_errors" className="flex items-center gap-1.5">
          <Bug className="h-3.5 w-3.5" />
          <span className="truncate">RUM JS Errors</span>
        </TabsTrigger>
      </TabsList>
    </Tabs>
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
