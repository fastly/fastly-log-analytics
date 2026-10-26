'use client'

import React, { Suspense, useState, useEffect, useEffectEvent, useMemo, useCallback } from 'react'
import CodeMirror, { type ReactCodeMirrorRef } from '@uiw/react-codemirror'
import { sql } from '@codemirror/lang-sql'
import { keymap, EditorView } from '@codemirror/view'
import { indentWithTab } from '@codemirror/commands'
import { useTheme } from 'next-themes'
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
  useMounted,
} from '@/hooks/useBootstrap'
import { useDebouncedFilterPayload, useFilterUrlWriteback } from '@/hooks/useDashboardCards'
import { useDateFormat, useDerivedState, useLocalStorageString } from '@/hooks/useSSE'
import { Button, buttonVariants } from '@/components/ui/button'
import { Label } from '@/components/ui/label'
import { Switch } from '@/components/ui/switch'
import { Alert, AlertDescription, AlertTitle } from '@/components/ui/alert'
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
import {
  Play,
  Search,
  AlertCircle,
  Database,
  Terminal,
  Activity,
  Bug,
  History,
  Bookmark,
  Download,
  X,
  Filter,
  Code2,
  Clock,
  ChevronRight,
  Columns3,
} from 'lucide-react'
import { Tabs, TabsList, TabsTrigger } from '@/components/ui/tabs'
import { NoServiceSelected } from '@/components/ReportLayout'
import { PageHeader } from '@/components/ui/confirm-dialog'
import { downloadAsCsv, formatCount, cn } from '@/lib/utils'
import { Skeleton } from '@/components/ui/skeleton'
import type { FiltersPayload, Schema } from '@/types/api'
import type { components } from '@/types/api.generated'

type QueryResponse = components['schemas']['QueryResponse']
type PresetQuery = components['schemas']['PresetQuery']

export interface CodeEditorHandle {
  insertAtCursor: (text: string) => void
}

interface CodeEditorProps {
  value: string
  onChange: (value: string) => void
  schema?: { name: string; type: string }[]
  tableName?: string
  className?: string
  height?: string
  ariaLabel?: string
}

export const CodeEditor = React.forwardRef<CodeEditorHandle, CodeEditorProps>(function CodeEditor(
  {
    value,
    onChange,
    schema,
    tableName = 'logs',
    className,
    height = '300px',
    ariaLabel = 'SQL query editor',
  },
  ref,
) {
  const { theme } = useTheme()
  const isDark = theme === 'dark'
  const cmRef = React.useRef<ReactCodeMirrorRef | null>(null)

  React.useImperativeHandle(
    ref,
    () => ({
      insertAtCursor: (text: string) => {
        const view = cmRef.current?.view
        if (!view) return
        const { from, to } = view.state.selection.main
        view.dispatch({
          changes: { from, to, insert: text },
          selection: { anchor: from + text.length },
          scrollIntoView: true,
        })
        view.focus()
      },
    }),
    [],
  )

  const a11yExtensions = React.useMemo(
    () => [
      keymap.of([
        indentWithTab,
        {
          key: 'Escape',
          run: (view) => {
            view.contentDOM.blur()
            return true
          },
        },
      ]),
      EditorView.contentAttributes.of({ 'aria-label': ariaLabel }),
    ],
    [ariaLabel],
  )

  const sqlExtension = React.useMemo(() => {
    const schemaObj: Record<string, string[]> = {}
    if (schema) {
      const cols = schema.map((s) => s.name)
      schemaObj[tableName] = cols
      if (tableName !== 'logs') schemaObj['logs'] = cols
    }
    return sql({ schema: schemaObj, defaultTable: tableName })
  }, [schema, tableName])

  return (
    <div className={className}>
      <CodeMirror
        ref={cmRef}
        value={value}
        height={height}
        theme={isDark ? 'dark' : 'light'}
        extensions={[sqlExtension, ...a11yExtensions]}
        onChange={onChange}
        className="border rounded-md overflow-hidden"
      />
      <p className="sr-only" id="code-editor-keyboard-hint">
        Press Escape, then Tab, to move focus out of the editor. Press Control-Space to trigger column autocomplete.
      </p>
    </div>
  )
})

export const queryPageUrlParsers = {
  q: parseAsString,
  mode: parseAsStringLiteral(['raw'] as const),
  start_time: parseAsString,
  end_time: parseAsString,
  filters: parseAsString,
}

function sqlEscape(v: string): string {
  return v.replace(/'/g, "''")
}

const SIMPLE_IDENT_RE = /^[a-zA-Z_][a-zA-Z0-9_]*$/

function quoteIdent(name: string): string {
  if (SIMPLE_IDENT_RE.test(name)) return name
  return `"${name.replace(/"/g, '""')}"`
}

const NUMERIC_DUCKDB_TYPES = new Set([
  'TINYINT', 'SMALLINT', 'INTEGER', 'BIGINT', 'HUGEINT',
  'UTINYINT', 'USMALLINT', 'UINTEGER', 'UBIGINT', 'UHUGEINT',
  'FLOAT', 'DOUBLE', 'REAL', 'DECIMAL',
])

function isNumericType(t: string | undefined): boolean {
  if (!t) return false
  const base = t.toUpperCase().split('(')[0].trim()
  return NUMERIC_DUCKDB_TYPES.has(base)
}

const NUMERIC_LITERAL_RE = /^-?\d+(\.\d+)?$/

function toCanonicalSqlTs(ts: string): string {
  const d = new Date(ts)
  return isNaN(d.getTime()) ? ts : d.toISOString()
}

function buildWhereClause(
  filters: FiltersPayload,
  startTime: string | null,
  endTime: string | null,
  fieldTypes?: Record<string, string>,
): string {
  const parts: string[] = []

  if (startTime) parts.push(`timestamp >= '${sqlEscape(toCanonicalSqlTs(startTime))}'`)
  if (endTime) parts.push(`timestamp <= '${sqlEscape(toCanonicalSqlTs(endTime))}'`)

  for (const [rawCol, spec] of Object.entries(filters)) {
    if (!spec || !Array.isArray(spec.values) || spec.values.length === 0) continue
    const col = rawCol.replace(/_\d+$/, '')
    const ident = quoteIdent(col)
    const colIsNumeric = isNumericType(fieldTypes?.[col])
    const literals = spec.values
      .map((v) => {
        const s = String(v)
        if (colIsNumeric && NUMERIC_LITERAL_RE.test(s)) return s
        return `'${sqlEscape(s)}'`
      })
      .join(', ')
    const op = spec.mode === 'exclude' ? 'NOT IN' : 'IN'
    parts.push(`${ident} ${op} (${literals})`)
  }

  return parts.length > 0 ? `WHERE ${parts.join(' AND ')}` : ''
}

export function buildStructuredSql(
  filters: FiltersPayload,
  startTime: string | null,
  endTime: string | null,
  sorting: SortingState,
  maxRows: number,
  fieldTypes?: Record<string, string>,
  tableName: string = 'logs',
): string {
  const where = buildWhereClause(filters, startTime, endTime, fieldTypes)
  const sort = sorting[0]
  const orderBy = sort
    ? `ORDER BY ${quoteIdent(sort.id)} ${sort.desc ? 'DESC' : 'ASC'}`
    : 'ORDER BY timestamp DESC'
  return ['SELECT *', `FROM ${tableName}`, where, orderBy, `LIMIT ${maxRows}`].filter(Boolean).join('\n')
}

export type QueryMode = 'structured' | 'raw'

type SchemaColumn = {
  name?: Schema<'SchemaColumn'>['name']
  column?: string
  type: Schema<'SchemaColumn'>['type']
}

interface RawSqlModeProps {
  rawSql: string
  onRawSqlChange: (sql: string) => void
  schema?: SchemaColumn[]
  tableName?: string
}

export function RawSqlMode({ rawSql, onRawSqlChange, schema, tableName }: RawSqlModeProps) {
  const editorRef = React.useRef<CodeEditorHandle>(null)
  const [browserOpen, setBrowserOpen] = React.useState(true)
  const resolvedTable = tableName || 'logs'

  const normalizedSchema = React.useMemo(
    () =>
      (schema ?? [])
        .map((col) => ({
          name: col.name || col.column || '',
          type: col.type,
        }))
        .filter((col) => Boolean(col.name)),
    [schema],
  )

  const handleInsert = React.useCallback((col: string) => {
    editorRef.current?.insertAtCursor(col)
  }, [])

  return (
    <div className="grid gap-3 p-3 md:grid-cols-[1fr_240px]">
      <CodeEditor
        ref={editorRef}
        value={rawSql}
        onChange={onRawSqlChange}
        schema={normalizedSchema}
        tableName={tableName}
        height="400px"
      />
      <SchemaBrowser
        tableName={resolvedTable}
        schema={normalizedSchema}
        open={browserOpen}
        onOpenChange={setBrowserOpen}
        onInsert={handleInsert}
      />
    </div>
  )
}

interface SchemaBrowserProps {
  tableName: string
  schema?: { name: string; type: string }[]
  open: boolean
  onOpenChange: (next: boolean) => void
  onInsert: (col: string) => void
}

function SchemaBrowser({ tableName, schema, open, onOpenChange, onInsert }: SchemaBrowserProps) {
  return (
    <aside
      className="rounded-md border bg-muted/20 text-xs flex flex-col min-h-0"
      aria-label="Schema browser"
      data-testid="query-schema-sidebar"
    >
      <button
        type="button"
        onClick={() => onOpenChange(!open)}
        aria-expanded={open}
        className="flex items-center gap-2 w-full px-3 py-2 border-b text-left font-medium hover:bg-muted/40 transition-colors"
      >
        <ChevronRight className={cn('h-3.5 w-3.5 text-muted-foreground transition-transform', open && 'rotate-90')} />
        <Columns3 className="h-3.5 w-3.5 text-muted-foreground" />
        <span className="flex-1 truncate">{tableName}</span>
        <span className="text-[10px] text-muted-foreground tabular-nums">{schema?.length ?? 0}</span>
      </button>
      {open && (
        <div className="overflow-y-auto" style={{ maxHeight: '360px' }}>
          {schema && schema.length > 0 ? (
            <ul className="py-1">
              {schema.map((col) => (
                <li key={col.name}>
                  <button
                    type="button"
                    onClick={() => onInsert(col.name)}
                    title={`Insert ${col.name} at cursor`}
                    className="flex w-full items-baseline justify-between gap-2 px-3 py-1 text-left font-mono text-[11px] hover:bg-muted/60 focus:bg-muted/60 focus:outline-none"
                  >
                    <span className="truncate">{col.name}</span>
                    <span className="shrink-0 text-[10px] uppercase tracking-wide text-muted-foreground">
                      {col.type}
                    </span>
                  </button>
                </li>
              ))}
            </ul>
          ) : (
            <p className="p-3 italic text-muted-foreground">No columns available.</p>
          )}
        </div>
      )}
    </aside>
  )
}

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
              {presets?.length ? (
                presets.map((p) => (
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
                ))
              ) : (
                <div className="p-4 text-xs text-muted-foreground text-center italic">No history found.</div>
              )}
            </DropdownMenuGroup>
          </DropdownMenuContent>
        </DropdownMenu>
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

interface ErrorBody {
  detail?: string | { message?: string; error?: string }
  message?: string
}

function errorBodyMessage(body: ErrorBody | null): string | undefined {
  const detail = body?.detail
  if (typeof detail === 'string') return detail
  return detail?.message || detail?.error || body?.message
}

const HISTORY_KEY = 'fastly_qe_history'

function applyDeepLinkToFilterStore(qsStart: string | null, qsEnd: string | null, qsFilters: string | null): boolean {
  if (typeof window === 'undefined') return false
  const { setRange, clearFilters, addFilter } = useFilterStore.getState()

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

const SKELETON_ROWS = Array.from({ length: 8 }, (_, i) => `query-skel-row-${i}`)

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
    useShallow((state) => ({
      startTime: state.startTime,
      endTime: state.endTime,
    })),
  )
  const filterPayload = useDebouncedFilterPayload(true)

  useFilterUrlWriteback()

  const [hasUserRun, setHasUserRun] = useState(deepLinked)

  type Dataset = components['schemas']['QueryRequest']['dataset']
  const [dataset, setDataset] = useState<Dataset>('logs')

  const RUM_SCHEMAS = useMemo<Record<string, { name: string; type: string }[]>>(
    () => ({
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
    }),
    [],
  )

  const DEFAULT_LOG_SCHEMA = useMemo<{ name: string; type: string }[]>(
    () => [
      { name: 'timestamp', type: 'TIMESTAMPTZ' },
      { name: 'ip', type: 'VARCHAR' },
      { name: 'url', type: 'VARCHAR' },
      { name: 'host', type: 'VARCHAR' },
      { name: 'method', type: 'VARCHAR' },
      { name: 'status', type: 'INTEGER' },
      { name: 'cache', type: 'VARCHAR' },
      { name: 'country', type: 'VARCHAR' },
      { name: 'pop', type: 'VARCHAR' },
      { name: 'asn', type: 'INTEGER' },
      { name: 'ua', type: 'VARCHAR' },
    ],
    [],
  )

  const [rawSql, setRawSql] = useState(() => urlQueryParam || 'SELECT * FROM logs LIMIT 100')

  const handleDatasetChange = useCallback((nextDataset: Dataset) => {
    setDataset(nextDataset)
    setRawSql(`SELECT * FROM ${nextDataset} LIMIT 100`)
  }, [])

  const [maxRows, setMaxRows] = useState<number>(100)
  const [explain, setExplain] = useState<boolean>(false)
  const [historyRaw, writeHistoryRaw] = useLocalStorageString(HISTORY_KEY)
  const history = useMemo(() => parseHistory(historyRaw), [historyRaw])

  const [structuredSorting, setStructuredSorting] = useState<SortingState>([
    { id: 'timestamp', desc: true },
  ])

  const { data: schemaData } = useQuery({
    queryKey: queryKeys.admin.schema(activeServiceId),
    queryFn: async ({ signal }) => {
      return unwrapClient(client.GET('/api/schema', { signal }))
    },
    enabled: !!activeServiceId,
    staleTime: 5 * 60_000,
  })

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

  const activeSchema = useMemo<{ name: string; type: string }[]>(() => {
    if (dataset === 'client_vitals') return RUM_SCHEMAS.client_vitals
    if (dataset === 'client_errors') return RUM_SCHEMAS.client_errors
    const rawList = schemaData?.schema ?? []
    if (rawList.length > 0) {
      return rawList
        .map((item) => ({
          name: item.name || '',
          type: item.type || 'VARCHAR',
        }))
        .filter((item) => Boolean(item.name))
    }
    if (catalog?.fields?.length) {
      return catalog.fields
        .filter((f) => Boolean(f.id))
        .map((f) => ({
          name: f.id,
          type: f.duckdb_type || 'VARCHAR',
        }))
    }
    return DEFAULT_LOG_SCHEMA
  }, [dataset, schemaData, catalog, RUM_SCHEMAS, DEFAULT_LOG_SCHEMA])

  const activeTableName = useMemo(() => {
    if (dataset === 'client_vitals') return 'client_vitals'
    if (dataset === 'client_errors') return 'client_errors'
    return schemaData?.table_name || 'logs'
  }, [dataset, schemaData])

  const structuredSql = useMemo(
    () => buildStructuredSql(filterPayload, startTime, endTime, structuredSorting, maxRows, activeFieldTypes, activeTableName),
    [filterPayload, startTime, endTime, structuredSorting, maxRows, activeFieldTypes, activeTableName],
  )

  const effectiveSql = mode === 'structured' ? structuredSql : rawSql

  const { data: presets } = useQuery({
    queryKey: ['query', 'presets', activeServiceId],
    queryFn: async ({ signal }) => {
      const data = await unwrapClient(client.GET('/api/presets', { signal }))
      return data ?? []
    },
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
      if (typeof data !== 'string') return data as QueryResponse
      return JSON.parse(data) as QueryResponse
    },
  })

  const pushHistory = useCallback(
    (sqlToRun: string) => {
      const updated = [
        { sql: sqlToRun, ts: Date.now() },
        ...history.filter((h) => h.sql !== sqlToRun),
      ].slice(0, 20)
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
    if (mode !== 'structured') return
    if (!activeServiceId) return
    if (!hasUserRun) return
    runStructured()
  }, [structuredSql, mode, activeServiceId, hasUserRun, maxRows, explain, dataset])

  const handleExportCSV = useCallback(() => {
    if (!queryMutation.data?.data?.length) return
    const data = queryMutation.data.data
    const cols = queryMutation.data.columns || []
    downloadAsCsv(data, cols, 'query_results.csv')
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
      if (next === 'raw') {
        setRawSql((prev) => (prev === 'SELECT * FROM logs LIMIT 100' ? structuredSql : prev))
      }
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
        if ((col === 'timestamp' || col.endsWith('_at')) && value && typeof value === 'string' && value.includes('T')) {
          try {
            return (
              <span className="text-xs font-mono">
                {full(value)} {abbr()}
              </span>
            )
          } catch {
            // fallback when value is not a valid date string
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
      <div
        className="space-y-6 [contain-intrinsic-size:auto_600px] [content-visibility:auto]"
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
        <div className="flex flex-col gap-4 sm:flex-row sm:items-center sm:justify-between">
          <ModeToggle mode={mode} onModeChange={handleModeChange} />
          <DatasetToggle dataset={dataset} onDatasetChange={handleDatasetChange} />
        </div>
        <Skeleton className="h-[238px] w-full rounded-lg" />
        <Skeleton className="h-[380px] w-full rounded-lg" />
      </div>
    )
  }

  const isStructured = mode === 'structured'

  return (
    <div
      className="space-y-6 [contain-intrinsic-size:auto_600px] [content-visibility:auto]"
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
          <RawSqlMode
            rawSql={rawSql}
            onRawSqlChange={setRawSql}
            schema={activeSchema}
            tableName={activeTableName}
          />
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

type ToggleDataset = 'logs' | 'client_vitals' | 'client_errors'

interface DatasetToggleProps {
  dataset: ToggleDataset
  onDatasetChange: (next: ToggleDataset) => void
}

function DatasetToggle({ dataset, onDatasetChange }: DatasetToggleProps) {
  return (
    <Tabs value={dataset} onValueChange={(v) => onDatasetChange(v as ToggleDataset)}>
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

export default function QueryPage() {
  return (
    <Suspense
      fallback={
        <div
          className="space-y-6 [contain-intrinsic-size:auto_600px] [content-visibility:auto]"
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
          <Skeleton className="h-9 w-full rounded-lg" />
          <Skeleton className="h-[238px] w-full rounded-lg" />
          <Skeleton className="h-[380px] w-full rounded-lg" />
        </div>
      }
    >
      <QueryPageInner />
    </Suspense>
  )
}
