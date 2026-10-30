'use client'

import React, { useMemo } from 'react'
import CodeMirror, { type ReactCodeMirrorRef } from '@uiw/react-codemirror'
import { sql } from '@codemirror/lang-sql'
import { keymap, EditorView } from '@codemirror/view'
import { indentWithTab } from '@codemirror/commands'
import { useTheme } from 'next-themes'
import { flexRender, getCoreRowModel, useReactTable, type ColumnDef } from '@tanstack/react-table'
import { useMounted } from '@/hooks/useBootstrap'
import { Table, TableBody, TableCell, TableRow } from '@/components/ui/table'
import { Terminal, Activity, Bug, Filter, Code2, ChevronRight, Columns3 } from 'lucide-react'
import { Tabs, TabsList, TabsTrigger } from '@/components/ui/tabs'
import { cn } from '@/lib/utils'
import type { Schema } from '@/types/api'

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
  { value, onChange, schema, tableName = 'logs', className, height = '300px', ariaLabel = 'SQL query editor' },
  ref,
) {
  const { theme } = useTheme()
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
        theme={theme === 'dark' ? 'dark' : 'light'}
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

export type QueryMode = 'structured' | 'raw'

type SchemaColumn = { name?: Schema<'SchemaColumn'>['name']; column?: string; type: Schema<'SchemaColumn'>['type'] }

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
        .map((col) => ({ name: col.name || col.column || '', type: col.type }))
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

type SchemaCol = { name: string; type: string }

interface SchemaBrowserProps {
  tableName: string
  schema?: SchemaCol[]
  open: boolean
  onOpenChange: (next: boolean) => void
  onInsert: (col: string) => void
}

function SchemaBrowser({ tableName, schema, open, onOpenChange, onInsert }: SchemaBrowserProps) {
  const rows = useMemo(() => schema ?? [], [schema])
  const columns = useMemo<ColumnDef<SchemaCol>[]>(
    () => [
      {
        id: 'column',
        cell: ({ row: { original: col } }) => (
          <button
            type="button"
            onClick={() => onInsert(col.name)}
            title={`Insert ${col.name} at cursor`}
            className="flex w-full items-baseline justify-between gap-2 px-3 py-1 text-left font-mono text-[11px] hover:bg-muted/60 focus:bg-muted/60 focus:outline-none"
          >
            <span className="truncate">{col.name}</span>
            <span className="shrink-0 text-[10px] uppercase tracking-wide text-muted-foreground">{col.type}</span>
          </button>
        ),
      },
    ],
    [onInsert],
  )
  const table = useReactTable({ data: rows, columns, getCoreRowModel: getCoreRowModel() })

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
          {rows.length > 0 ? (
            <Table className="py-1">
              <TableBody>
                {table.getRowModel().rows.map((row) => (
                  <TableRow key={row.original.name} className="border-0 hover:bg-transparent">
                    {row.getVisibleCells().map((cell) => (
                      <TableCell key={cell.id} className="p-0">
                        {flexRender(cell.column.columnDef.cell, cell.getContext())}
                      </TableCell>
                    ))}
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          ) : (
            <p className="p-3 italic text-muted-foreground">No columns available.</p>
          )}
        </div>
      )}
    </aside>
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
        Edit the date range or filters in the header bar above to refine. Click column headers below to change sort
        order — the query re-runs server-side.
      </div>
    </div>
  )
}

export type ToggleDataset = 'logs' | 'client_vitals' | 'client_errors'

const DATASET_OPTIONS: readonly {
  value: ToggleDataset
  label: string
  Icon: React.ComponentType<{ className?: string }>
}[] = [
  { value: 'logs', label: 'CDN Request Logs', Icon: Terminal },
  { value: 'client_vitals', label: 'RUM Web Vitals', Icon: Activity },
  { value: 'client_errors', label: 'RUM JS Errors', Icon: Bug },
]

export function DatasetToggle({
  dataset,
  onDatasetChange,
}: {
  dataset: ToggleDataset
  onDatasetChange: (next: ToggleDataset) => void
}) {
  return (
    <Tabs value={dataset} onValueChange={(v) => onDatasetChange(v as ToggleDataset)}>
      <TabsList className="grid w-full grid-cols-3 max-w-[480px]">
        {DATASET_OPTIONS.map(({ value, label, Icon }) => (
          <TabsTrigger key={value} value={value} className="flex items-center gap-1.5">
            <Icon className="h-3.5 w-3.5" />
            <span className="truncate">{label}</span>
          </TabsTrigger>
        ))}
      </TabsList>
    </Tabs>
  )
}

export const RUM_SCHEMAS: Record<string, { name: string; type: string }[]> = {
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
}

export const DEFAULT_LOG_SCHEMA: { name: string; type: string }[] = [
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
]
