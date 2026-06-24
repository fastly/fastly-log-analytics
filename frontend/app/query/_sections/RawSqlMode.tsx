'use client'

import React from 'react'
import dynamic from 'next/dynamic'
import { ChevronRight, Columns3 } from 'lucide-react'
import type { CodeEditorHandle } from '@/components/CodeEditor/CodeEditor'
import { cn } from '@/lib/utils'

interface SchemaColumn {
  name: string
  type: string
}

interface RawSqlModeProps {
  rawSql: string
  onRawSqlChange: (sql: string) => void
  schema?: SchemaColumn[]
  tableName?: string
}

// CodeEditor pulls in CodeMirror + @codemirror/lang-sql (~200KB parsed).
// Structured Mode is the default landing experience on /query, so we keep
// the editor off the initial bundle and only load it when the user actually
// switches to Raw mode (this section mounts). ssr:false skips the SSR pass
// since CodeMirror touches `window` on hydration anyway.
//
// next/dynamic strips refs by default. We promote the underlying component
// to a forwardRef-aware wrapper so the schema browser can dispatch
// insert-at-cursor transactions through the CodeEditorHandle ref.
const CodeEditor = dynamic(
  () =>
    import('@/components/CodeEditor/CodeEditor').then(m => {
      const Inner = m.CodeEditor
      const Forwarded = React.forwardRef<CodeEditorHandle, React.ComponentProps<typeof Inner>>(
        (props, ref) => <Inner {...props} ref={ref} />,
      )
      Forwarded.displayName = 'DynamicCodeEditor'
      return { default: Forwarded }
    }),
  {
    ssr: false,
    loading: () => (
      // Match the editor's rendered footprint (height=400px + border + rounded)
      // so the layout doesn't shift when the real component swaps in.
      <div
        className="border rounded-md bg-muted/30 animate-pulse"
        style={{ height: '400px' }}
        aria-label="Loading SQL editor"
        role="status"
      />
    ),
  },
)

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

  const handleInsert = React.useCallback((col: string) => {
    editorRef.current?.insertAtCursor(col)
  }, [])

  return (
    <div className="grid gap-3 p-3 md:grid-cols-[1fr_240px]">
      <CodeEditor
        ref={editorRef}
        value={rawSql}
        onChange={onRawSqlChange}
        schema={schema}
        tableName={tableName}
        height="400px"
      />
      <SchemaBrowser
        tableName={resolvedTable}
        schema={schema}
        open={browserOpen}
        onOpenChange={setBrowserOpen}
        onInsert={handleInsert}
      />
    </div>
  )
}

interface SchemaBrowserProps {
  tableName: string
  schema?: SchemaColumn[]
  open: boolean
  onOpenChange: (next: boolean) => void
  onInsert: (col: string) => void
}

function SchemaBrowser({ tableName, schema, open, onOpenChange, onInsert }: SchemaBrowserProps) {
  return (
    <aside
      className="rounded-md border bg-muted/20 text-xs flex flex-col min-h-0"
      aria-label="Schema browser"
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
