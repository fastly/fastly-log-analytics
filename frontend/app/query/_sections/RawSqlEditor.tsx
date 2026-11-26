'use client'

import React, { useMemo } from 'react'
import CodeMirror, { type ReactCodeMirrorRef } from '@uiw/react-codemirror'
import { sql } from '@codemirror/lang-sql'
import { keymap, EditorView } from '@codemirror/view'
import { indentWithTab } from '@codemirror/commands'
import { useTheme } from 'next-themes'
import { flexRender, useTable } from '@tanstack/react-table'
import { basicTableFeatures, type BasicColumnDef } from '@/lib/table'
import { Table, TableBody, TableCell, TableRow } from '@/components/ui/table'
import { ChevronRight, Columns3 } from 'lucide-react'
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
  const columns = useMemo<BasicColumnDef<SchemaCol>[]>(
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
  const table = useTable({ features: basicTableFeatures, data: rows, columns })

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
