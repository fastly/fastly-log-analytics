'use client'

import React, { Suspense, useState, useEffect, useEffectEvent, useMemo, useCallback } from 'react'
import { parseAsString, parseAsStringLiteral, useQueryStates } from 'nuqs'
import { useQuery, useMutation } from '@tanstack/react-query'
import { useShallow } from 'zustand/react/shallow'
import type { ColumnDef, SortingState } from '@tanstack/react-table'
import { client, unwrapClient, queryKeys } from '@/lib/api'
import { useFilterStore } from '@/stores/filterStore'
import {
  useEffectiveServiceId,
  useBootstrapResolved,
  useFieldLabel,
  useLogFieldsCatalog,
} from '@/hooks/useBootstrap'
import { useDebouncedFilterPayload, useFilterUrlWriteback } from '@/hooks/useDashboardCards'
import { useDateFormat, useDerivedState, useLocalStorageString } from '@/hooks/useSSE'
import { Button } from '@/components/ui/button'
import { Alert, AlertDescription, AlertTitle } from '@/components/ui/alert'
import { Play, Search, AlertCircle, Database, Terminal } from 'lucide-react'
import { NoServiceSelected } from '@/components/ReportLayout'
import { PageHeader } from '@/components/ui/confirm-dialog'
import { applyUrlFiltersJsonToStore, downloadAsCsv } from '@/lib/utils'
import { Skeleton } from '@/components/ui/skeleton'
import { buildStructuredSql } from '@/lib/sqlBuilder'
import type { components } from '@/types/api.generated'
import {
  CodeEditor,
  RawSqlMode,
  ModeToggle,
  StructuredMode,
  DatasetToggle,
  RUM_SCHEMAS,
  DEFAULT_LOG_SCHEMA,
  type CodeEditorHandle,
  type QueryMode,
} from './QueryEditorSection'
import {
  QueryToolbar,
  ResultsTable,
  QueryInitializingSkeleton,
  SKELETON_ROWS,
  type QueryResponse,
} from './QueryToolbarAndResults'

export {
  buildStructuredSql,
  CodeEditor,
  RawSqlMode,
  QueryToolbar,
  ModeToggle,
  StructuredMode,
  ResultsTable,
  type CodeEditorHandle,
  type QueryMode,
}

export const queryPageUrlParsers = {
  q: parseAsString,
  mode: parseAsStringLiteral(['raw'] as const),
  start_time: parseAsString,
  end_time: parseAsString,
  filters: parseAsString,
}

interface ErrorBody {
  detail?: string | { message?: string; error?: string }
  message?: string
}

function errorBodyMessage(body: ErrorBody | null): string | undefined {
  const detail = body?.detail
  return typeof detail === 'string' ? detail : detail?.message || detail?.error || body?.message
}

const HISTORY_KEY = 'fastly_qe_history'

function applyDeepLinkToFilterStore(
  qsStart: string | null,
  qsEnd: string | null,
  qsFilters: string | null,
): boolean {
  if (typeof window === 'undefined') return false
  const store = useFilterStore.getState()
  let mutated = false
  if (qsStart && qsEnd) {
    store.setRange(qsStart, qsEnd)
    mutated = true
  }
  if (applyUrlFiltersJsonToStore(qsFilters, store)) {
    mutated = true
  }
  return mutated
}

function parseHistory(raw: string | null): { sql: string; ts: number }[] {
  if (!raw) return []
  try {
    return JSON.parse(raw)
  } catch {
    return []
  }
}

function QueryPageInner() {
  const [urlParams, setUrlParams] = useQueryStates(queryPageUrlParsers, { history: 'replace', shallow: true })
  const activeServiceId = useEffectiveServiceId() ?? null
  const bootstrapResolved = useBootstrapResolved()
  const { full, abbr } = useDateFormat()
  const getFieldLabel = useFieldLabel()

  const urlQueryParam = urlParams.q
  const urlMode: QueryMode = urlParams.mode === 'raw' || Boolean(urlQueryParam) ? 'raw' : 'structured'
  const [mode, setMode] = useDerivedState<QueryMode>(urlMode)

  const [deepLinked] = useState(() =>
    applyDeepLinkToFilterStore(urlParams.start_time, urlParams.end_time, urlParams.filters),
  )
  const { startTime, endTime } = useFilterStore(
    useShallow((state) => ({ startTime: state.startTime, endTime: state.endTime })),
  )
  const filterPayload = useDebouncedFilterPayload(true)
  useFilterUrlWriteback()

  const [hasUserRun, setHasUserRun] = useState(deepLinked)
  type Dataset = components['schemas']['QueryRequest']['dataset']
  const [dataset, setDataset] = useState<Dataset>('logs')
  const [rawSql, setRawSql] = useState(() => urlQueryParam || 'SELECT * FROM logs LIMIT 100')

  const handleDatasetChange = useCallback((nextDataset: Dataset) => {
    setDataset(nextDataset)
    setRawSql(`SELECT * FROM ${nextDataset} LIMIT 100`)
  }, [])

  const [maxRows, setMaxRows] = useState<number>(100)
  const [explain, setExplain] = useState<boolean>(false)
  const [historyRaw, writeHistoryRaw] = useLocalStorageString(HISTORY_KEY)
  const history = useMemo(() => parseHistory(historyRaw), [historyRaw])
  const [structuredSorting, setStructuredSorting] = useState<SortingState>([{ id: 'timestamp', desc: true }])

  const { data: schemaData } = useQuery({
    queryKey: queryKeys.admin.schema(activeServiceId),
    queryFn: async ({ signal }) => unwrapClient(client.GET('/api/schema', { signal })),
    enabled: !!activeServiceId,
    staleTime: 5 * 60_000,
  })

  const { data: catalog } = useLogFieldsCatalog()
  const activeFieldTypes = useMemo<Record<string, string>>(() => {
    const out: Record<string, string> = {}
    if (dataset === 'logs') {
      for (const f of catalog?.fields ?? []) if (f.id && f.duckdb_type) out[f.id] = f.duckdb_type
    } else {
      for (const f of RUM_SCHEMAS[dataset] || []) out[f.name] = f.type
    }
    return out
  }, [dataset, catalog])

  const activeSchema = useMemo<{ name: string; type: string }[]>(() => {
    if (dataset === 'client_vitals' || dataset === 'client_errors') return RUM_SCHEMAS[dataset]
    const rawList = schemaData?.schema ?? []
    if (rawList.length > 0) {
      return rawList
        .map((item) => ({ name: item.name || '', type: item.type || 'VARCHAR' }))
        .filter((item) => Boolean(item.name))
    }
    if (catalog?.fields?.length) {
      return catalog.fields
        .filter((f) => Boolean(f.id))
        .map((f) => ({ name: f.id, type: f.duckdb_type || 'VARCHAR' }))
    }
    return DEFAULT_LOG_SCHEMA
  }, [dataset, schemaData, catalog])

  const activeTableName = useMemo(() => {
    if (dataset === 'client_vitals' || dataset === 'client_errors') return dataset
    return schemaData?.table_name || 'logs'
  }, [dataset, schemaData])

  const structuredTableName = dataset === 'client_vitals' || dataset === 'client_errors' ? dataset : 'logs'

  const structuredSql = useMemo(
    () =>
      buildStructuredSql(
        filterPayload,
        startTime,
        endTime,
        structuredSorting,
        maxRows,
        activeFieldTypes,
        structuredTableName,
      ),
    [filterPayload, startTime, endTime, structuredSorting, maxRows, activeFieldTypes, structuredTableName],
  )

  const effectiveSql = mode === 'structured' ? structuredSql : rawSql

  const { data: presets } = useQuery({
    queryKey: ['query', 'presets', activeServiceId],
    queryFn: async ({ signal }) => (await unwrapClient(client.GET('/api/presets', { signal }))) ?? [],
    enabled: !!activeServiceId,
    staleTime: 5 * 60_000,
  })

  const queryMutation = useMutation({
    mutationFn: async (params: { sql: string; max_rows: number; explain: boolean; dataset?: Dataset }) => {
      const { data, error } = await client.POST('/api/query', { body: { dataset: 'logs', ...params }, parseAs: 'text' })
      if (error) {
        if (typeof error === 'string') {
          let parsed: ErrorBody | null = null
          try {
            parsed = JSON.parse(error)
          } catch {
            throw new Error(error)
          }
          throw new Error(errorBodyMessage(parsed) || error)
        }
        if (error instanceof Error) throw error
        throw new Error(errorBodyMessage(error as ErrorBody) || 'Query execution failed')
      }
      if (!data) throw new Error('No data')
      return typeof data !== 'string' ? (data as QueryResponse) : (JSON.parse(data) as QueryResponse)
    },
  })

  const pushHistory = useCallback(
    (sqlToRun: string) => {
      const updated = [{ sql: sqlToRun, ts: Date.now() }, ...history.filter((h) => h.sql !== sqlToRun)].slice(0, 20)
      writeHistoryRaw(JSON.stringify(updated))
    },
    [history, writeHistoryRaw],
  )

  const handleRun = useCallback(() => {
    const sqlToRun = effectiveSql.trim()
    if (!sqlToRun) return
    setHasUserRun(true)
    pushHistory(sqlToRun)
    queryMutation.mutate({ sql: sqlToRun, max_rows: maxRows, explain, dataset })
  }, [effectiveSql, maxRows, explain, dataset, pushHistory, queryMutation])

  const runStructured = useEffectEvent(() => {
    pushHistory(structuredSql)
    queryMutation.mutate({ sql: structuredSql, max_rows: maxRows, explain, dataset })
  })
  useEffect(() => {
    if (mode !== 'structured' || !activeServiceId || !hasUserRun) return
    runStructured()
  }, [structuredSql, mode, activeServiceId, hasUserRun, maxRows, explain, dataset])

  const handleExportCSV = useCallback(() => {
    if (!queryMutation.data?.data?.length) return
    downloadAsCsv(queryMutation.data.data, queryMutation.data.columns || [], 'query_results.csv')
  }, [queryMutation.data])

  const removeHistoryItem = useCallback(
    (e: React.MouseEvent, index: number) => {
      e.stopPropagation()
      const updated = [...history]
      updated.splice(index, 1)
      writeHistoryRaw(JSON.stringify(updated))
    },
    [history, writeHistoryRaw],
  )

  const handleModeChange = useCallback(
    (next: QueryMode) => {
      setMode(next)
      if (next === 'raw') setRawSql((prev) => (prev === 'SELECT * FROM logs LIMIT 100' ? structuredSql : prev))
      void setUrlParams({ mode: next === 'raw' ? 'raw' : null })
    },
    [setUrlParams, structuredSql, setMode],
  )

  const resultColumns = queryMutation.data?.columns
  const columns: ColumnDef<Record<string, unknown>>[] = useMemo(() => {
    if (!resultColumns) return []
    return resultColumns.map((col: string) => ({
      id: col,
      accessorFn: (row: Record<string, unknown>) => row[col],
      meta: { label: getFieldLabel(col) },
      header: () => <span className="font-mono text-xs">{getFieldLabel(col)}</span>,
      cell: ({ row }: { row: { original: Record<string, unknown> } }) => {
        const value = row.original[col]
        if (
          (col === 'timestamp' || col.endsWith('_at')) &&
          value &&
          typeof value === 'string' &&
          value.includes('T')
        ) {
          try {
            return (
              <span className="text-xs font-mono">
                {full(value)} {abbr()}
              </span>
            )
          } catch {
            /* fallback */
          }
        }
        return <span className="text-xs font-mono">{value !== null && value !== undefined ? String(value) : 'null'}</span>
      },
    }))
  }, [resultColumns, full, abbr, getFieldLabel])

  if (!activeServiceId && bootstrapResolved) {
    return <NoServiceSelected icon={Search} message="Please select a service from the header to run queries." />
  }
  if (!activeServiceId) {
    return (
      <QueryInitializingSkeleton
        controls={
          <div className="flex flex-col gap-4 sm:flex-row sm:items-center sm:justify-between">
            <ModeToggle mode={mode} onModeChange={handleModeChange} />
            <DatasetToggle dataset={dataset} onDatasetChange={handleDatasetChange} />
          </div>
        }
      />
    )
  }

  const isStructured = mode === 'structured'

  return (
    <div
      className="space-y-6 min-h-[820px] [contain-intrinsic-size:auto_820px] [content-visibility:auto]"
      data-testid="query-page-root"
    >
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
          data-testid="query-run-btn"
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

      <div
        className="border rounded-lg bg-card shadow-sm [contain-intrinsic-size:auto_420px] [content-visibility:auto]"
        data-testid="query-editor-container"
      >
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
          <RawSqlMode rawSql={rawSql} onRawSqlChange={setRawSql} schema={activeSchema} tableName={activeTableName} />
        )}
      </div>

      {queryMutation.error && (
        <Alert variant="destructive" data-testid="query-error-alert">
          <AlertCircle className="h-4 w-4" />
          <AlertTitle>Query Error</AlertTitle>
          <AlertDescription className="font-mono text-xs whitespace-pre-wrap break-words mt-2">
            {queryMutation.error instanceof Error ? queryMutation.error.message : 'An unknown error occurred'}
          </AlertDescription>
        </Alert>
      )}

      {queryMutation.data?.explain_plan && (
        <Alert>
          <AlertTitle className="text-xs font-bold uppercase tracking-wider text-muted-foreground mb-2">
            Query Plan
          </AlertTitle>
          <AlertDescription>
            <pre className="text-xs font-mono bg-muted/50 p-4 rounded-md overflow-x-auto">
              {queryMutation.data.explain_plan}
            </pre>
          </AlertDescription>
        </Alert>
      )}

      {!queryMutation.data && !queryMutation.isPending && (
        <div
          className="flex flex-col items-center justify-center rounded-lg border border-dashed p-8 text-center min-h-[380px] bg-muted/10 [contain-intrinsic-size:auto_380px] [content-visibility:auto]"
          data-testid="query-results-placeholder"
        >
          <div className="mx-auto flex max-w-[420px] flex-col items-center justify-center text-center">
            <Terminal className="h-10 w-10 text-muted-foreground/60 mb-4" />
            <h3 className="text-lg font-semibold">No query results yet</h3>
            <p className="mb-4 mt-2 text-sm text-muted-foreground">
              Configure filters or write custom SQL above and click &quot;Run Query&quot; to fetch logs.
            </p>
          </div>
        </div>
      )}

      {queryMutation.isPending && !queryMutation.data && (
        <div
          className="space-y-3 min-h-[380px] [contain-intrinsic-size:auto_380px] [content-visibility:auto]"
          data-testid="query-results-loading"
        >
          <div className="flex items-center gap-2 text-xs text-muted-foreground">
            <Database className="h-3 w-3 animate-spin" />
            <span>Running query…</span>
          </div>
          <Skeleton className="h-9 w-full rounded-md" />
          {SKELETON_ROWS.map((k) => (
            <Skeleton key={k} className="h-8 w-full rounded-md opacity-60" />
          ))}
        </div>
      )}

      {queryMutation.data && (
        <div
          className="relative min-h-[380px] [contain-intrinsic-size:auto_380px] [content-visibility:auto]"
          data-testid="query-results-container"
        >
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

export function QueryClient() {
  return (
    <Suspense fallback={<QueryInitializingSkeleton />}>
      <QueryPageInner />
    </Suspense>
  )
}

export default QueryClient
