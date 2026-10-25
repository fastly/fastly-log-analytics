'use client'

import React from 'react'
import CodeMirror, { type ReactCodeMirrorRef } from '@uiw/react-codemirror'
import { sql } from '@codemirror/lang-sql'
import { keymap, EditorView } from '@codemirror/view'
import { indentWithTab } from '@codemirror/commands'
import { useTheme } from 'next-themes'
import { parseAsString, parseAsStringLiteral } from 'nuqs'
import type { ColumnDef, SortingState } from '@tanstack/react-table'
import type { FiltersPayload } from '@/types/filters'
import type { Schema } from '@/types/api'
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
import { History, Bookmark, Download, X, Filter, Code2, Clock, Database, ChevronRight, Columns3 } from 'lucide-react'
import { useDateFormat } from '@/hooks/useSSE'
import { useMounted } from '@/hooks/useBootstrap'
import { formatCount } from '@/lib/format'
import { cn } from '@/lib/utils'
import type { components } from '@/types/api.generated'

export interface CodeEditorHandle {
  /**
   * Insert the given text at the current cursor position, focus the editor,
   * and scroll the insertion into view. Powers the schema-browser
   * click-to-insert affordance on /query raw mode.
   */
  insertAtCursor: (text: string) => void
}

interface CodeEditorProps {
  value: string
  onChange: (value: string) => void
  schema?: { name: string; type: string }[]
  tableName?: string
  className?: string
  height?: string
  /** Accessible label announced to screen readers for the editor surface. */
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

  // a11y: WCAG 2.1.1 / 2.1.2 — without this, Tab inside CodeMirror inserts a
  // tab character and Esc has no binding, so keyboard-only users get trapped
  // in the editor. We:
  //   1. Bind Tab to `indentWithTab` (Shift+Tab dedents) so indentation still
  //      works when the user is actively editing.
  //   2. Bind Escape to blur the contentDOM so keyboard users can always
  //      escape to the next focusable element via Tab afterwards.
  //   3. Expose an aria-label on the contenteditable surface (WCAG 4.1.2).
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

  // defaultTable lets unqualified column names (e.g. `SELECT sta|`)
  // surface column completions without requiring `logs.` prefix —
  // matches what users get in Metabase / Snowsight.
  const sqlExtension = React.useMemo(
    () => {
      const schemaObj: Record<string, string[]> = {}
      if (schema) {
        const cols = schema.map(s => s.name)
        schemaObj[tableName] = cols
        if (tableName !== 'logs') schemaObj['logs'] = cols
      }
      return sql({ schema: schemaObj, defaultTable: tableName })
    },
    [schema, tableName],
  )

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

/**
 * /query URL contract (shared links and the dashboard "See Raw Logs" CTA):
 *   ?mode=raw            raw-SQL editor (also implied by ?q=)
 *   ?q=<sql>             seeds the raw editor
 *   ?start_time&end_time absolute range, applied once to the filter store
 *   ?filters=<json>      filter payload, applied once to the filter store
 * Unrecognised `mode` values mean structured mode, as before.
 */
export const queryPageUrlParsers = {
  q: parseAsString,
  mode: parseAsStringLiteral(['raw'] as const),
  start_time: parseAsString,
  end_time: parseAsString,
  filters: parseAsString,
}

/** Escape a string literal for safe SQL embedding (single-quote doubling). */
function sqlEscape(v: string): string {
  return v.replace(/'/g, "''")
}

const SIMPLE_IDENT_RE = /^[a-zA-Z_][a-zA-Z0-9_]*$/

/** Quote a column identifier only when required (unquoted simple identifiers stay compatible with High-Scale ClickHouse console validation). */
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
      .map(v => {
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
  return [
    'SELECT *',
    `FROM ${tableName}`,
    where,
    orderBy,
    `LIMIT ${maxRows}`,
  ].filter(Boolean).join('\n')
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

/**
 * Raw mode: a CodeEditor for free-form SQL. The user has full control of the
 * query string; results are sorted client-side so we don't silently rewrite
 * their SQL.
 *
 * Renders an inline schema browser to the right of the editor (table → columns,
 * click to insert at cursor) so users don't have to memorise column names or
 * tab away to another surface. Autocomplete (Ctrl+Space) is provided by the
 * CodeEditor's @codemirror/lang-sql configuration.
 */
export function RawSqlMode({ rawSql, onRawSqlChange, schema, tableName }: RawSqlModeProps) {
  const editorRef = React.useRef<CodeEditorHandle>(null)
  const [browserOpen, setBrowserOpen] = React.useState(true)
  const resolvedTable = tableName || 'logs'

  const normalizedSchema = React.useMemo(
    () =>
      (schema ?? []).map(col => ({
        name: col.name || col.column || '',
        type: col.type,
      })).filter(col => Boolean(col.name)),
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
        <ChevronRight
          className={cn('h-3.5 w-3.5 text-muted-foreground transition-transform', open && 'rotate-90')}
        />
        <Columns3 className="h-3.5 w-3.5 text-muted-foreground" />
        <span className="flex-1 truncate">{tableName}</span>
        <span className="text-[10px] text-muted-foreground tabular-nums">
          {schema?.length ?? 0}
        </span>
      </button>
      {open && (
        <div className="overflow-y-auto" style={{ maxHeight: '360px' }}>
          {schema && schema.length > 0 ? (
            <ul className="py-1">
              {schema.map(col => (
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
